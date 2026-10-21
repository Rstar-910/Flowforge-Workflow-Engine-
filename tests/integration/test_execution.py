"""End-to-end execution through the engine with in-process workers and scheduler."""

from __future__ import annotations

import pytest

from tests.support.helpers import (
    Cluster,
    attempts_for,
    events_for,
    start_run,
    task,
    tasks_by_key,
    wait_for_run,
)

pytestmark = pytest.mark.integration


async def test_linear_workflow_succeeds_and_persists_outputs(db, settings, registry):
    run_id = await start_run(
        db,
        registry,
        [
            task("fetch", "fetch_data"),
            task("sum", "aggregate", ["fetch"], input={"source": "fetch", "op": "sum"}),
            task("report", "combine", ["sum"]),
        ],
        run_input={"rows": 5},
    )
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}) == "SUCCEEDED"

    tasks = await tasks_by_key(db, run_id)
    assert {t["status"] for t in tasks.values()} == {"SUCCEEDED"}
    values = [(i * 37) % 101 for i in range(5)]
    assert tasks["fetch"]["output_json"] == {"values": values}
    assert tasks["sum"]["output_json"] == {"op": "sum", "value": sum(values)}
    # Dependency ordering: each task started after its dependency completed.
    assert tasks["sum"]["started_at"] >= tasks["fetch"]["completed_at"]
    assert tasks["report"]["started_at"] >= tasks["sum"]["completed_at"]
    attempts = await attempts_for(db, run_id)
    assert [a["outcome"] for a in attempts] == ["SUCCEEDED"] * 3
    assert all(a["ended_at"] is not None and a["duration_ms"] is not None for a in attempts)
    event_types = [e["event_type"] for e in await events_for(db, run_id)]
    assert event_types[0] == "run_created"
    assert event_types.count("attempt_started") == 3
    assert event_types[-1] == "run_status_changed"


async def test_diamond_dependencies_and_run_output(db, settings, registry):
    run_id = await start_run(
        db,
        registry,
        [
            task("a", "test.record_order"),
            task("b", "test.record_order", ["a"], input={"delay": 0.2}),
            task("c", "test.record_order", ["a"], input={"delay": 0.2}),
            task("d", "test.record_order", ["b", "c"]),
        ],
    )
    async with Cluster(db, settings, registry, workers=2, concurrency=2):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}) == "SUCCEEDED"
    tasks = await tasks_by_key(db, run_id)
    out = {k: v["output_json"] for k, v in tasks.items()}
    assert out["d"]["deps"] == ["b", "c"]
    assert out["b"]["t"] > out["a"]["t"] and out["c"]["t"] > out["a"]["t"]
    assert out["d"]["t"] > max(out["b"]["t"], out["c"]["t"])
    # b and c ran in parallel: their execution windows overlap.
    assert tasks["b"]["started_at"] < tasks["c"]["completed_at"]
    assert tasks["c"]["started_at"] < tasks["b"]["completed_at"]
    from tests.support.helpers import run_status  # noqa: F401

    async with db.connect() as conn:
        from sqlalchemy import text

        output = (
            await conn.execute(
                text("SELECT output_json FROM workflow_runs WHERE id=:id"), {"id": run_id}
            )
        ).scalar_one()
    assert set(output) == {"d"}  # run output = outputs of sink tasks


async def test_wide_fan_out_runs_in_parallel(db, settings, registry):
    n = 12
    tasks = [task("root")] + [
        task(f"leaf{i:02d}", "sleep", ["root"], input={"seconds": 0.3}) for i in range(n)
    ]
    run_id = await start_run(db, registry, tasks)
    import time

    t0 = time.monotonic()
    async with Cluster(db, settings, registry, workers=3, concurrency=4):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}) == "SUCCEEDED"
    elapsed = time.monotonic() - t0
    # 12 x 0.3s serially would take 3.6s; with 12 slots it should be far less.
    assert elapsed < 2.5, elapsed


async def test_transient_failures_are_retried_until_success(db, settings, registry):
    run_id = await start_run(
        db,
        registry,
        [task("flaky", "fail_until", input={"fail_times": 2}, max_attempts=3)],
    )
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}) == "SUCCEEDED"
    attempts = await attempts_for(db, run_id)
    assert [a["outcome"] for a in attempts] == ["FAILED", "FAILED", "SUCCEEDED"]
    assert [a["attempt_number"] for a in attempts] == [1, 2, 3]
    assert all(a["retryable"] for a in attempts[:2])
    t = (await tasks_by_key(db, run_id))["flaky"]
    assert t["attempt_count"] == 3
    assert t["output_json"] == {"succeeded_on_attempt": 3}


async def test_retries_are_bounded_then_failure_propagates(db, settings, registry):
    run_id = await start_run(
        db,
        registry,
        [
            task("a", "fail_until", input={"fail_times": 10}, max_attempts=3),
            task("b", "noop", ["a"]),
            task("c", "noop", ["b"]),
            task("independent", "noop"),
        ],
    )
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}) == "FAILED"
    tasks = await tasks_by_key(db, run_id)
    assert tasks["a"]["status"] == "FAILED"
    assert tasks["a"]["attempt_count"] == 3
    assert tasks["b"]["status"] == "UPSTREAM_FAILED"
    assert tasks["c"]["status"] == "UPSTREAM_FAILED"
    assert tasks["independent"]["status"] == "SUCCEEDED"  # independent branches still run
    assert len([a for a in await attempts_for(db, run_id) if a["task_key"] == "a"]) == 3
    assert not [a for a in await attempts_for(db, run_id) if a["task_key"] in ("b", "c")]


async def test_permanent_errors_are_not_retried(db, settings, registry):
    run_id = await start_run(
        db,
        registry,
        [
            task("perm", "always_fail", max_attempts=5),
            task(
                "value_err",
                "test.raise_value_error",
                max_attempts=5,
                non_retryable_errors=["ValueError"],
            ),
            task("bad_input", "sleep", input={"seconds": "not-a-number"}, max_attempts=5),
            task("bad_output", "test.unserialisable", max_attempts=5),
        ],
    )
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}) == "FAILED"
    attempts = await attempts_for(db, run_id)
    by_key = {a["task_key"]: a for a in attempts}
    assert len(attempts) == 4  # exactly one attempt each
    assert by_key["perm"]["error_type"] == "PermanentTaskError"
    assert by_key["value_err"]["error_type"] == "ValueError"
    assert by_key["bad_input"]["error_type"] == "InputValidationError"
    assert by_key["bad_output"]["error_type"] == "PermanentTaskError"
    assert all(a["retryable"] is False for a in attempts)


async def test_execution_timeout_async_and_process(db, settings, registry):
    run_id = await start_run(
        db,
        registry,
        [
            task("async_slow", "sleep", input={"seconds": 30}, timeout=0.3, max_attempts=2),
            task("proc_slow", "process_sleep", input={"seconds": 30}, timeout=1.0, max_attempts=1),
        ],
    )
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}, timeout=30) == "FAILED"
    attempts = await attempts_for(db, run_id)
    assert [a["outcome"] for a in attempts if a["task_key"] == "async_slow"] == [
        "TIMED_OUT",
        "TIMED_OUT",
    ]
    proc = [a for a in attempts if a["task_key"] == "proc_slow"]
    assert [a["outcome"] for a in proc] == ["TIMED_OUT"]
    assert proc[0]["duration_ms"] < 5000  # the subprocess was killed, not awaited for 30s


async def test_retry_on_timeout_can_be_disabled(db, settings, registry):
    run_id = await start_run(
        db,
        registry,
        [
            task(
                "t",
                "sleep",
                input={"seconds": 30},
                timeout=0.2,
                max_attempts=3,
                retry_on_timeout=False,
            )
        ],
    )
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}) == "FAILED"
    assert [a["outcome"] for a in await attempts_for(db, run_id)] == ["TIMED_OUT"]


async def test_handler_not_registered_on_worker_is_never_claimed(db, settings, registry):
    """A worker only claims tasks whose handler it has; the task waits for a capable worker."""
    from flowforge.worker.registry import HandlerRegistry

    narrow = HandlerRegistry()
    narrow.register("noop", registry.get("noop").func)
    run_id = await start_run(db, registry, [task("a", "echo")])
    async with Cluster(db, settings, narrow):
        import asyncio

        await asyncio.sleep(0.5)
        assert (await tasks_by_key(db, run_id))["a"]["status"] == "READY"
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"SUCCEEDED"}) == "SUCCEEDED"
