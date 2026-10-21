"""Failure injection with real processes: SIGKILL, SIGSTOP, injected crashes, DB outages.

Each test asserts on persisted state and attempt history. These scenarios show
the tested behaviour only; they do not prove every distributed failure is safe.
"""

from __future__ import annotations

import asyncio
import json
import signal
import time

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from flowforge.faults import CRASH_EXIT_CODE
from flowforge.handlers.builtin import read_ledger
from tests.conftest import TEST_DB_URL
from tests.support.helpers import (
    attempts_for,
    events_for,
    start_run,
    task,
    tasks_by_key,
    wait_for,
    wait_for_run,
)
from tests.support.processes import TcpProxy, process_env, process_group

pytestmark = [pytest.mark.integration, pytest.mark.process]


@pytest.fixture
def env(tmp_path):
    return process_env(TEST_DB_URL, tmp_path)


async def _file(path) -> bool:
    return path.exists()


async def test_worker_crash_immediately_after_claim(db, registry, tmp_path, env):
    run_id = await start_run(db, registry, [task("a", "echo", max_attempts=3, initial_delay=0)])
    with process_group(tmp_path, env) as pg:
        crasher = pg.worker(
            "crasher",
            FLOWFORGE_ENABLE_FAULT_INJECTION="true",
            FLOWFORGE_FAULT_POINTS="crash_after_claim",
        )
        assert await crasher.wait_exit() == CRASH_EXIT_CODE, crasher.output()
        t = (await tasks_by_key(db, run_id))["a"]
        assert t["status"] == "RUNNING" and t["lease_owner"] == "crasher"

        pg.scheduler()
        pg.worker("healthy")
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}, timeout=20) == "SUCCEEDED"
    attempts = await attempts_for(db, run_id)
    assert [(a["worker_id"], a["outcome"]) for a in attempts] == [
        ("crasher", "LEASE_EXPIRED"),
        ("healthy", "SUCCEEDED"),
    ]


async def test_worker_sigkill_during_execution_is_recovered(db, registry, tmp_path, env):
    run_id = await start_run(
        db,
        registry,
        [task("long", "test.wait_for_release", input={"key": "k"}, initial_delay=0)],
    )
    with process_group(tmp_path, env) as pg:
        pg.scheduler()
        victim = pg.worker("victim")
        await wait_for(lambda: _file(tmp_path / "k.started"), timeout=20)
        killed_at = time.monotonic()
        victim.kill()
        (tmp_path / "k.started").unlink()

        pg.worker("rescuer")
        await wait_for(lambda: _file(tmp_path / "k.started"), timeout=20)
        recovery_seconds = time.monotonic() - killed_at
        (tmp_path / "k.release").touch()
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}, timeout=20) == "SUCCEEDED"

    attempts = await attempts_for(db, run_id)
    assert [(a["worker_id"], a["outcome"]) for a in attempts] == [
        ("victim", "LEASE_EXPIRED"),
        ("rescuer", "SUCCEEDED"),
    ]
    # Lease 2s + scheduler interval 0.2s + poll; generous bound for CI noise.
    assert recovery_seconds < 6, recovery_seconds
    lease_events = [e for e in await events_for(db, run_id) if e["event_type"] == "lease_expired"]
    assert lease_events[0]["data"]["outcome_known"] is False


@pytest.mark.parametrize("use_operation_key", [False, True])
async def test_crash_after_side_effect_before_commit(
    db, registry, tmp_path, env, use_operation_key
):
    """The ambiguous case: the effect happened, the success was never recorded.

    The engine cannot know the effect happened, so it re-executes the task.
    Without downstream de-duplication the effect is duplicated; with the
    engine's stable operation key the downstream system can de-duplicate.
    """
    ledger = tmp_path / "ledger.jsonl"
    env = {**env, "FLOWFORGE_DEMO_LEDGER_PATH": str(ledger)}
    run_id = await start_run(
        db,
        registry,
        [
            task(
                "charge",
                "record_effect",
                input={"amount": 10, "use_operation_key": use_operation_key},
                initial_delay=0,
            )
        ],
    )
    with process_group(tmp_path, env) as pg:
        crasher = pg.worker(
            "crasher",
            FLOWFORGE_ENABLE_FAULT_INJECTION="true",
            FLOWFORGE_FAULT_POINTS="crash_after_handler",
        )
        assert await crasher.wait_exit() == CRASH_EXIT_CODE, crasher.output()
        assert len(read_ledger(ledger)) == 1  # the side effect DID happen
        assert (await tasks_by_key(db, run_id))["charge"]["status"] == "RUNNING"

        pg.scheduler()
        pg.worker("rescuer")
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}, timeout=20) == "SUCCEEDED"

    attempts = await attempts_for(db, run_id)
    assert [a["outcome"] for a in attempts] == ["LEASE_EXPIRED", "SUCCEEDED"]
    entries = read_ledger(ledger)
    output = (await tasks_by_key(db, run_id))["charge"]["output_json"]
    if use_operation_key:
        assert len(entries) == 1, "downstream de-duplication on the operation key failed"
        assert output == {"applied": False, "duplicate_of_existing": True}
    else:
        # Documented at-least-once behaviour: the effect was applied twice.
        assert len(entries) == 2
        assert {e["operation_key"] for e in entries} == {f"{run_id}/charge"}
        assert [e["attempt"] for e in entries] == [1, 2]


async def test_paused_worker_loses_lease_and_its_late_result_is_rejected(
    db, registry, tmp_path, env
):
    run_id = await start_run(
        db,
        registry,
        [task("t", "test.wait_for_release", input={"key": "p"}, initial_delay=0)],
    )
    with process_group(tmp_path, env) as pg:
        pg.scheduler()
        paused = pg.worker("paused")
        await wait_for(lambda: _file(tmp_path / "p.started"), timeout=20)
        paused.signal(signal.SIGSTOP)  # e.g. a long GC pause or VM freeze
        (tmp_path / "p.started").unlink()

        pg.worker("successor")
        await wait_for(lambda: _file(tmp_path / "p.started"), timeout=20)
        (tmp_path / "p.release").touch()
        assert await wait_for_run(db, run_id, {"SUCCEEDED"}, timeout=20) == "SUCCEEDED"
        accepted = (await tasks_by_key(db, run_id))["t"]["output_json"]

        paused.signal(signal.SIGCONT)  # the old worker resumes, still "executing"

        async def stale_rejected() -> bool:
            return any(
                e["event_type"] == "stale_result_rejected" for e in await events_for(db, run_id)
            )

        await wait_for(stale_rejected, timeout=20)

    t = (await tasks_by_key(db, run_id))["t"]
    assert t["output_json"] == accepted == {"released": "p", "attempt": 2}
    attempts = await attempts_for(db, run_id)
    assert [(a["worker_id"], a["outcome"]) for a in attempts] == [
        ("paused", "LEASE_EXPIRED"),
        ("successor", "SUCCEEDED"),
    ]


async def test_worker_without_heartbeats_cannot_commit_after_takeover(db, registry, tmp_path, env):
    """Delayed/absent heartbeats: the worker keeps running but its lease lapses."""
    run_id = await start_run(
        db,
        registry,
        [task("t", "sleep", input={"seconds": 3.5}, initial_delay=0, max_attempts=3)],
    )
    with process_group(tmp_path, env) as pg:
        pg.scheduler()
        # One slot: while the silent worker executes attempt 1 (3.5s) it cannot
        # re-claim the task itself after the lease (2s) is recovered.
        pg.worker(
            "silent",
            concurrency=1,
            FLOWFORGE_ENABLE_FAULT_INJECTION="true",
            FLOWFORGE_FAULT_POINTS="skip_heartbeats",
        )

        async def owned_by_silent() -> bool:
            return (await tasks_by_key(db, run_id))["t"]["lease_owner"] == "silent"

        async def stale_rejected() -> bool:
            return any(
                e["event_type"] == "stale_result_rejected" for e in await events_for(db, run_id)
            )

        await wait_for(owned_by_silent, timeout=20)
        pg.worker("healthy")
        assert await wait_for_run(db, run_id, {"SUCCEEDED"}, timeout=20) == "SUCCEEDED"
        await wait_for(stale_rejected, timeout=20)

    attempts = await attempts_for(db, run_id)
    healthy = [a for a in attempts if a["worker_id"] == "healthy"]
    assert [a["outcome"] for a in healthy] == ["SUCCEEDED"]
    assert all(a["outcome"] == "LEASE_EXPIRED" for a in attempts if a["worker_id"] == "silent")
    assert sum(a["outcome"] == "SUCCEEDED" for a in attempts) == 1
    events = [e["event_type"] for e in await events_for(db, run_id)]
    assert "stale_result_rejected" in events


async def test_graceful_shutdown_finishes_inflight_and_stops_claiming(db, registry, tmp_path, env):
    run_a = await start_run(db, registry, [task("a", "sleep", input={"seconds": 1.0})])
    with process_group(tmp_path, env) as pg:
        w = pg.worker("graceful", concurrency=1)

        async def running() -> bool:
            return (await tasks_by_key(db, run_a))["a"]["status"] == "RUNNING"

        await wait_for(running, timeout=20)
        run_b = await start_run(db, registry, [task("b", "noop")])
        w.signal(signal.SIGTERM)
        assert await w.wait_exit(timeout=20) == 0, w.output()

    assert (await tasks_by_key(db, run_a))["a"]["status"] == "SUCCEEDED"
    assert (await tasks_by_key(db, run_b))["b"]["status"] == "READY"  # never claimed
    async with db.connect() as conn:
        stopped = (
            await conn.execute(text("SELECT stopped_at FROM workers WHERE worker_id='graceful'"))
        ).scalar_one()
    assert stopped is not None


async def test_scheduler_sigkill_and_restart_with_retry_wait(db, registry, tmp_path, env):
    run_id = await start_run(
        db,
        registry,
        [
            task(
                "t",
                "fail_until",
                input={"fail_times": 2},
                initial_delay=1,
                max_delay=1,
                max_attempts=3,
            )
        ],
    )
    with process_group(tmp_path, env) as pg:
        sched = pg.scheduler("scheduler-1")
        pg.worker("w")

        async def in_retry_wait() -> bool:
            return (await tasks_by_key(db, run_id))["t"]["status"] == "RETRY_WAIT"

        await wait_for(in_retry_wait, timeout=20)
        sched.kill()
        await asyncio.sleep(1.5)
        assert (await tasks_by_key(db, run_id))["t"]["status"] == "RETRY_WAIT"
        pg.scheduler("scheduler-2")
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}, timeout=20) == "SUCCEEDED"
    assert [a["outcome"] for a in await attempts_for(db, run_id)] == [
        "FAILED",
        "FAILED",
        "SUCCEEDED",
    ]


def _proxied_url(proxy: TcpProxy) -> str:
    url = make_url(TEST_DB_URL).set(host="127.0.0.1", port=proxy.port)
    return url.render_as_string(hide_password=False)


async def test_short_database_outage_is_survived(db, registry, tmp_path, env):
    """Connectivity drops for less than the lease: the worker rides it out."""
    target = make_url(TEST_DB_URL)
    proxy = TcpProxy(target.host or "localhost", target.port or 5432)
    await proxy.start()
    run_id = await start_run(
        db, registry, [task("t", "sleep", input={"seconds": 2.5}, max_attempts=1)]
    )
    try:
        with process_group(tmp_path, env) as pg:
            w = pg.worker(
                "w",
                FLOWFORGE_DATABASE_URL=_proxied_url(proxy),
                FLOWFORGE_LEASE_SECONDS="6",
                FLOWFORGE_HEARTBEAT_INTERVAL_SECONDS="0.5",
            )

            async def running() -> bool:
                return (await tasks_by_key(db, run_id))["t"]["status"] == "RUNNING"

            await wait_for(running, timeout=20)
            proxy.cut()
            await asyncio.sleep(1.5)
            proxy.restore()
            assert (
                await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}, timeout=20) == "SUCCEEDED"
            )
            # The worker keeps working after the outage: claim a new task.
            run2 = await start_run(db, registry, [task("t2", "noop")])
            assert await wait_for_run(db, run2, {"SUCCEEDED"}, timeout=20) == "SUCCEEDED"
            # The outage was really observed by the worker, not slept through.
            assert "lease renewal failed" in w.output()
    finally:
        await proxy.close()
    attempts = await attempts_for(db, run_id)
    assert [(a["worker_id"], a["outcome"]) for a in attempts] == [("w", "SUCCEEDED")]


async def test_long_database_outage_leads_to_safe_takeover(db, registry, tmp_path, env):
    """The worker is partitioned from PostgreSQL longer than its lease.

    The scheduler (which can still reach the database) recovers the task and a
    second worker completes it. When the partition heals, the first worker's
    result is rejected by the fenced commit.
    """
    target = make_url(TEST_DB_URL)
    proxy = TcpProxy(target.host or "localhost", target.port or 5432)
    await proxy.start()
    run_id = await start_run(
        db,
        registry,
        [
            task(
                "t",
                "test.ignore_cancel_sleep",
                input={"seconds": 3},
                initial_delay=0,
                max_attempts=3,
            )
        ],
    )
    try:
        with process_group(tmp_path, env) as pg:
            pg.worker("partitioned", FLOWFORGE_DATABASE_URL=_proxied_url(proxy))
            pg.scheduler()

            async def running() -> bool:
                return (await tasks_by_key(db, run_id))["t"]["status"] == "RUNNING"

            await wait_for(running, timeout=20)
            proxy.cut()
            pg.worker("direct")
            assert await wait_for_run(db, run_id, {"SUCCEEDED"}, timeout=30) == "SUCCEEDED"
            proxy.restore()

            async def stale() -> bool:
                return any(
                    e["event_type"] == "stale_result_rejected" for e in await events_for(db, run_id)
                )

            await wait_for(stale, timeout=30)
    finally:
        await proxy.close()
    attempts = await attempts_for(db, run_id)
    assert attempts[0]["worker_id"] == "partitioned"
    assert attempts[0]["outcome"] == "LEASE_EXPIRED"
    assert [a["worker_id"] for a in attempts if a["outcome"] == "SUCCEEDED"] == ["direct"]
    event = next(
        e for e in await events_for(db, run_id) if e["event_type"] == "stale_result_rejected"
    )
    assert json.loads(json.dumps(event["data"]))["worker_id"] == "partitioned"
