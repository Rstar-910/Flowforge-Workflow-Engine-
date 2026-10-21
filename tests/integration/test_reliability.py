"""Leases, stale workers, cancellation, manual retry, scheduler restart, reconciliation."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from sqlalchemy import text

from flowforge.domain.errors import FailureInfo
from flowforge.engine.control import cancel_run, retry_failed_task
from flowforge.engine.execution import claim_tasks, complete_attempt, renew_leases
from flowforge.engine.recovery import recover_expired_leases
from flowforge.engine.submission import ConflictError
from flowforge.engine.types import Failed, Succeeded
from flowforge.scheduler.service import Scheduler
from tests.support.helpers import (
    TENANT,
    Cluster,
    attempts_for,
    events_for,
    run_status,
    start_run,
    task,
    tasks_by_key,
    wait_for,
    wait_for_run,
)

pytestmark = pytest.mark.integration


async def expire_lease(db, task_id: uuid.UUID) -> None:
    async with db.begin() as conn:
        await conn.execute(
            text("UPDATE tasks SET lease_expires_at = now() - interval '1 second' WHERE id=:i"),
            {"i": task_id},
        )


async def claim_one(db, worker: str, handler: str = "noop"):
    got = await claim_tasks(db, worker_id=worker, handlers=[handler], limit=1, lease_seconds=30)
    assert len(got) == 1
    return got[0]


async def test_stale_worker_cannot_overwrite_newer_attempt(db, registry):
    run_id = await start_run(db, registry, [task("a", max_attempts=3, initial_delay=0)])
    old = await claim_one(db, "worker-old")
    await expire_lease(db, old.task_id)
    recovered = await recover_expired_leases(db, limit=10)
    assert [r.new_status for r in recovered] == ["READY"]  # zero backoff -> READY directly

    new = await claim_one(db, "worker-new")
    assert new.attempt_number == 2 and new.attempt_id != old.attempt_id

    # The old worker wakes up and tries to renew and to report success: both rejected.
    renewal = await renew_leases(
        db, worker_id="worker-old", attempt_ids=[old.attempt_id], lease_seconds=30
    )
    assert renewal.renewed == set()
    stale = await complete_attempt(
        db, claimed=old, worker_id="worker-old", result=Succeeded({"by": "old"}), duration_ms=5
    )
    assert not stale.accepted

    fresh = await complete_attempt(
        db, claimed=new, worker_id="worker-new", result=Succeeded({"by": "new"}), duration_ms=5
    )
    assert fresh.accepted and fresh.run_status == "SUCCEEDED"

    # And after the new result is accepted, the stale worker still cannot overwrite it.
    late = await complete_attempt(
        db,
        claimed=old,
        worker_id="worker-old",
        result=Failed(FailureInfo("X", "x", True)),
        duration_ms=5,
    )
    assert not late.accepted
    t = (await tasks_by_key(db, run_id))["a"]
    assert t["status"] == "SUCCEEDED" and t["output_json"] == {"by": "new"}
    attempts = await attempts_for(db, run_id)
    assert [(a["attempt_number"], a["outcome"]) for a in attempts] == [
        (1, "LEASE_EXPIRED"),
        (2, "SUCCEEDED"),
    ]
    stale_events = [
        e for e in await events_for(db, run_id) if e["event_type"] == "stale_result_rejected"
    ]
    assert [e["data"]["reported"] for e in stale_events] == ["Succeeded", "Failed"]


async def test_lease_expiry_consumes_attempts_and_terminates(db, registry):
    """A task that 'crashes its worker' every time must still end, after max_attempts."""
    run_id = await start_run(
        db,
        registry,
        [task("poison", max_attempts=3, initial_delay=0), task("after", "noop", ["poison"])],
    )
    for n in range(1, 4):
        c = await claim_one(db, f"w{n}")
        assert c.attempt_number == n
        await expire_lease(db, c.task_id)
        await recover_expired_leases(db, limit=10)
    tasks = await tasks_by_key(db, run_id)
    assert tasks["poison"]["status"] == "FAILED"
    assert tasks["poison"]["last_error_type"] == "LeaseExpired"
    assert tasks["after"]["status"] == "UPSTREAM_FAILED"
    assert await run_status(db, run_id) == "FAILED"
    assert [a["outcome"] for a in await attempts_for(db, run_id)] == ["LEASE_EXPIRED"] * 3
    # Nothing left to claim.
    assert await claim_tasks(db, worker_id="w", handlers=["noop"], limit=5, lease_seconds=5) == []


async def test_renewal_keeps_lease_and_recovery_ignores_live_leases(db, registry):
    await start_run(db, registry, [task("a")])
    c = await claim_one(db, "w")
    renewal = await renew_leases(db, worker_id="w", attempt_ids=[c.attempt_id], lease_seconds=30)
    assert renewal.renewed == {c.attempt_id}
    assert await recover_expired_leases(db, limit=10) == []
    # A different worker id cannot renew someone else's lease.
    other = await renew_leases(db, worker_id="x", attempt_ids=[c.attempt_id], lease_seconds=30)
    assert other.renewed == set()


async def test_lease_expiry_recovered_by_scheduler_and_rerun(db, settings, registry):
    """A worker dies right after claiming; the scheduler recovers; another worker finishes."""
    run_id = await start_run(db, registry, [task("a", "echo", max_attempts=2, initial_delay=0)])
    dead = await claim_tasks(db, worker_id="dead", handlers=["echo"], limit=1, lease_seconds=0.5)
    assert dead
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}) == "SUCCEEDED"
    attempts = await attempts_for(db, run_id)
    assert [(a["worker_id"], a["outcome"]) for a in attempts] == [
        ("dead", "LEASE_EXPIRED"),
        ("w0", "SUCCEEDED"),
    ]


async def test_cancel_run_with_ready_blocked_and_running_tasks(
    db, settings, registry, tmp_path, monkeypatch
):
    monkeypatch.setenv("FLOWFORGE_TEST_SIGNAL_DIR", str(tmp_path))
    run_id = await start_run(
        db,
        registry,
        [
            task("running", "test.wait_for_release", input={"key": "r"}),
            task("blocked", "noop", ["running"]),
            task("ready_later", "sleep", input={"seconds": 0}),
        ],
    )
    async with db.begin() as conn:  # keep 'ready_later' unclaimable by our worker's handler set
        await conn.execute(text("UPDATE tasks SET handler='echo' WHERE task_key='ready_later'"))
    from flowforge.worker.registry import HandlerRegistry

    only_wait = HandlerRegistry()
    only_wait.register("test.wait_for_release", registry.get("test.wait_for_release").func)
    async with Cluster(db, settings, only_wait):
        await wait_for(lambda: _exists(tmp_path / "r.started"))
        status = await cancel_run(db, tenant_id=TENANT, run_id=run_id)
        assert status == "CANCELLING"
        tasks = await tasks_by_key(db, run_id)
        assert tasks["blocked"]["status"] == "CANCELLED"
        assert tasks["ready_later"]["status"] == "CANCELLED"
        assert tasks["running"]["status"] == "RUNNING"
        assert await run_status(db, run_id) == "CANCELLING"
        requested = [
            e for e in await events_for(db, run_id) if e["event_type"] == "task_cancel_requested"
        ]
        assert [e["data"]["task_key"] for e in requested] == ["running"]
        # The worker sees the request on its next heartbeat; the handler cooperates.
        assert await wait_for_run(db, run_id, {"CANCELLED"}, timeout=10) == "CANCELLED"
    tasks = await tasks_by_key(db, run_id)
    assert tasks["running"]["status"] == "CANCELLED"
    assert [a["outcome"] for a in await attempts_for(db, run_id)] == ["CANCELLED"]
    # Cancelling again is a conflict (terminal), not an error that changes state.
    with pytest.raises(ConflictError):
        await cancel_run(db, tenant_id=TENANT, run_id=run_id)


async def test_cancel_with_uncooperative_handler_waits_for_completion(db, settings, registry):
    run_id = await start_run(
        db, registry, [task("stubborn", "test.ignore_cancel_sleep", input={"seconds": 1.5})]
    )
    async with Cluster(db, settings, registry):
        await wait_for(lambda: _task_status_is(db, run_id, "stubborn", "RUNNING"))
        assert await cancel_run(db, tenant_id=TENANT, run_id=run_id) == "CANCELLING"
        assert await wait_for_run(db, run_id, {"CANCELLED"}, timeout=15) == "CANCELLED"
    # A thread cannot be interrupted: the engine waits and records what really
    # happened (the handler finished), while the run itself ends CANCELLED.
    attempts = await attempts_for(db, run_id)
    assert [a["outcome"] for a in attempts] == ["SUCCEEDED"]
    assert (await tasks_by_key(db, run_id))["stubborn"]["status"] == "SUCCEEDED"


async def test_cancel_pending_run_is_immediate(db, registry):
    run_id = await start_run(db, registry, [task("a"), task("b", "noop", ["a"])])
    assert await cancel_run(db, tenant_id=TENANT, run_id=run_id) == "CANCELLED"
    assert {t["status"] for t in (await tasks_by_key(db, run_id)).values()} == {"CANCELLED"}
    assert await claim_tasks(db, worker_id="w", handlers=["noop"], limit=5, lease_seconds=5) == []


async def test_cancel_other_tenant_is_not_found(db, registry):
    from flowforge.engine.submission import NotFoundError

    run_id = await start_run(db, registry, [task("a")])
    with pytest.raises(NotFoundError):
        await cancel_run(db, tenant_id="tenant-b", run_id=run_id)


async def test_manual_retry_reopens_failed_task_and_dependents(db, settings, registry):
    run_id = await start_run(
        db,
        registry,
        [
            task("a", "fail_until", input={"fail_times": 3}, max_attempts=2),
            task("b", "noop", ["a"]),
        ],
    )
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"FAILED", "SUCCEEDED"}) == "FAILED"
        tasks = await tasks_by_key(db, run_id)
        assert tasks["b"]["status"] == "UPSTREAM_FAILED"
        with pytest.raises(ConflictError):  # only FAILED tasks are retryable
            await retry_failed_task(
                db, tenant_id=TENANT, task_id=tasks["b"]["id"], requested_by="admin"
            )
        await retry_failed_task(
            db, tenant_id=TENANT, task_id=tasks["a"]["id"], requested_by="admin"
        )
        assert await wait_for_run(db, run_id, {"FAILED", "SUCCEEDED"}) == "SUCCEEDED"
    attempts = [a for a in await attempts_for(db, run_id) if a["task_key"] == "a"]
    assert [a["outcome"] for a in attempts] == ["FAILED", "FAILED", "FAILED", "SUCCEEDED"]
    assert [a["attempt_number"] for a in attempts] == [1, 2, 3, 4]  # fencing stays monotonic
    events = [e["event_type"] for e in await events_for(db, run_id)]
    assert "task_manual_retry" in events


async def test_scheduler_restart_with_retry_wait_tasks(db, settings, registry):
    """Retry timing lives in the database; a brand-new scheduler picks it up."""
    run_id = await start_run(
        db,
        registry,
        [task("a", "fail_until", input={"fail_times": 1}, initial_delay=0.5, max_delay=0.5)],
    )
    async with Cluster(db, settings, registry, scheduler=False):
        await wait_for(lambda: _task_status_is(db, run_id, "a", "RETRY_WAIT"))
        await asyncio.sleep(0.8)
        # No scheduler: the task stays in RETRY_WAIT although it is due.
        assert (await tasks_by_key(db, run_id))["a"]["status"] == "RETRY_WAIT"
        # "Restart" = a fresh scheduler object with no memory of the past.
        stats = await Scheduler(db, settings).cycle()
        assert stats.retries_promoted == 1
        assert await wait_for_run(db, run_id, {"SUCCEEDED"}) == "SUCCEEDED"


async def test_retry_backoff_respects_next_attempt_at(db, registry):
    run_id = await start_run(
        db, registry, [task("a", "noop", initial_delay=5, max_delay=5, max_attempts=2)]
    )
    c = await claim_one(db, "w")
    out = await complete_attempt(
        db,
        claimed=c,
        worker_id="w",
        result=Failed(FailureInfo("Boom", "boom", retryable=True)),
        duration_ms=1,
    )
    assert out.task_status == "RETRY_WAIT" and out.retry_delay_seconds == 5
    assert await Scheduler(db, _settings()).cycle() is not None
    assert (await tasks_by_key(db, run_id))["a"]["status"] == "RETRY_WAIT"  # not due yet
    async with db.begin() as conn:
        delta = (
            await conn.execute(
                text(
                    "SELECT extract(epoch FROM next_attempt_at - completed_at_or_now) FROM "
                    "(SELECT next_attempt_at, statement_timestamp() AS completed_at_or_now "
                    "FROM tasks WHERE run_id=:r) s"
                ),
                {"r": run_id},
            )
        ).scalar_one()
    assert 3.5 < float(delta) <= 5.0


async def test_reconciler_repairs_progression_after_manual_db_edit(db, settings, registry):
    run_id = await start_run(db, registry, [task("a"), task("b", "noop", ["a"])])
    async with db.begin() as conn:  # bypass the engine entirely
        await conn.execute(
            text(
                "UPDATE tasks SET status='SUCCEEDED', completed_at=now(), ready_at=NULL "
                "WHERE run_id=:r AND task_key='a'"
            ),
            {"r": run_id},
        )
    stats = await Scheduler(db, settings).cycle(reconcile=True)
    assert stats.runs_reconciled == 1
    assert (await tasks_by_key(db, run_id))["b"]["status"] == "READY"


def _settings():
    from tests.conftest import make_settings

    return make_settings()


async def _exists(path) -> bool:
    return path.exists()


async def _task_status_is(db, run_id, key, status) -> bool:
    return (await tasks_by_key(db, run_id))[key]["status"] == status
