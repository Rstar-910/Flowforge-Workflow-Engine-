"""Workflow deadlines and task queue timeouts (spec section 11)."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from flowforge.engine.control import retry_failed_task
from flowforge.engine.execution import claim_tasks
from flowforge.engine.submission import ConflictError
from flowforge.engine.timeouts import fail_queue_timeouts
from flowforge.scheduler.service import Scheduler
from flowforge.worker.registry import HandlerRegistry
from tests.support.helpers import (
    TENANT,
    Cluster,
    attempts_for,
    events_for,
    start_run,
    task,
    tasks_by_key,
    wait_for_run,
)

pytestmark = pytest.mark.integration


async def run_row(db, run_id):
    async with db.connect() as conn:
        return (
            (await conn.execute(text("SELECT * FROM workflow_runs WHERE id=:r"), {"r": run_id}))
            .mappings()
            .one()
        )


async def test_deadline_stops_a_running_workflow_and_fails_it(db, settings, registry):
    run_id = await start_run(
        db,
        registry,
        [
            task("slow", "sleep", input={"seconds": 30}),
            task("after", "noop", ["slow"]),
            task("quick", "noop"),
        ],
        deadline=1.0,
    )
    run = await run_row(db, run_id)
    assert 0.9 < (run["deadline_at"] - run["created_at"]).total_seconds() < 1.1
    async with Cluster(db, settings, registry):
        assert (
            await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED", "CANCELLED"}, timeout=15)
            == "FAILED"
        )
    run = await run_row(db, run_id)
    assert run["failure_reason"] == "workflow deadline exceeded"
    assert run["deadline_exceeded_at"] is not None
    tasks = await tasks_by_key(db, run_id)
    assert tasks["quick"]["status"] == "SUCCEEDED"  # finished before the deadline
    assert tasks["slow"]["status"] == "CANCELLED"  # running handler was told to stop
    assert tasks["after"]["status"] == "CANCELLED"  # queued work cancelled at once
    assert [a["outcome"] for a in await attempts_for(db, run_id) if a["task_key"] == "slow"] == [
        "CANCELLED"
    ]
    events = [e["event_type"] for e in await events_for(db, run_id)]
    assert "run_deadline_exceeded" in events
    # The deadline is enforced within about one scheduler interval, plus a heartbeat.
    assert (run["completed_at"] - run["deadline_at"]).total_seconds() < 3


async def test_deadline_with_nothing_running_fails_immediately(db, settings, registry):
    run_id = await start_run(db, registry, [task("a"), task("b", "noop", ["a"])], deadline=0.2)
    await asyncio.sleep(0.4)  # no worker: nothing ever ran
    stats = await Scheduler(db, settings).cycle()
    assert stats.deadlines_exceeded == 1
    run = await run_row(db, run_id)
    assert run["status"] == "FAILED" and run["failure_reason"] == "workflow deadline exceeded"
    assert {t["status"] for t in (await tasks_by_key(db, run_id)).values()} == {"CANCELLED"}
    assert await claim_tasks(db, worker_id="w", handlers=["noop"], limit=5, lease_seconds=5) == []


async def test_runs_meeting_their_deadline_are_unaffected(db, settings, registry):
    run_id = await start_run(db, registry, [task("a"), task("b", "noop", ["a"])], deadline=60)
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}) == "SUCCEEDED"
    assert (await run_row(db, run_id))["deadline_exceeded_at"] is None


async def test_user_cancel_still_ends_cancelled_and_deadline_runs_cannot_be_retried(
    db, settings, registry
):
    from flowforge.engine.control import cancel_run

    cancelled = await start_run(db, registry, [task("a")], deadline=60)
    assert await cancel_run(db, tenant_id=TENANT, run_id=cancelled) == "CANCELLED"

    run_id = await start_run(
        db,
        registry,
        [task("bad", "always_fail", max_attempts=1), task("slow", "sleep", input={"seconds": 30})],
        deadline=1.0,
    )
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"FAILED"}, timeout=15) == "FAILED"
    bad = (await tasks_by_key(db, run_id))["bad"]
    assert bad["status"] == "FAILED"
    with pytest.raises(ConflictError, match="deadline"):
        await retry_failed_task(db, tenant_id=TENANT, task_id=bad["id"], requested_by="admin")


async def test_queue_timeout_fails_a_task_no_worker_can_run(db, settings, registry):
    narrow = HandlerRegistry()  # this worker cannot run "echo"
    narrow.register("noop", registry.get("noop").func)
    run_id = await start_run(
        db,
        registry,
        [
            task("orphan", "echo", queue_timeout=0.5),
            task("next", "noop", ["orphan"]),
            task("fine", "noop", queue_timeout=0.5),
        ],
    )
    async with Cluster(db, settings, narrow):
        assert await wait_for_run(db, run_id, {"FAILED", "SUCCEEDED"}, timeout=15) == "FAILED"
    tasks = await tasks_by_key(db, run_id)
    assert tasks["orphan"]["status"] == "FAILED"
    assert tasks["orphan"]["last_error_type"] == "QueueTimeout"
    assert "with handler echo" in tasks["orphan"]["last_error_message"]
    assert tasks["orphan"]["attempt_count"] == 0  # failed without ever running
    assert tasks["next"]["status"] == "UPSTREAM_FAILED"
    assert tasks["fine"]["status"] == "SUCCEEDED"  # claimed in time
    ev = next(e for e in await events_for(db, run_id) if e["event_type"] == "task_queue_timeout")
    assert ev["data"]["waited_seconds"] >= 0.5


async def test_queue_timeout_never_fires_for_a_claimed_task(db, registry):
    run_id = await start_run(db, registry, [task("a", queue_timeout=0.2)])
    claimed = await claim_tasks(db, worker_id="w", handlers=["noop"], limit=1, lease_seconds=60)
    assert claimed
    async with db.begin() as conn:  # pretend it waited far longer than the timeout
        await conn.execute(
            text("UPDATE tasks SET ready_at = now() - interval '1 hour' WHERE run_id = :r"),
            {"r": run_id},
        )
    assert await fail_queue_timeouts(db, limit=10) == 0
    assert (await tasks_by_key(db, run_id))["a"]["status"] == "RUNNING"


async def test_queue_timeout_restarts_after_retry_wait(db, registry):
    """Time spent in RETRY_WAIT (or any earlier READY period) is not queue time: promoting
    a retry restarts the clock. Deterministic: the scheduler steps are called directly, so
    no worker can claim the task in between and hide a wrong clock."""
    from flowforge.domain.errors import FailureInfo
    from flowforge.engine.execution import complete_attempt
    from flowforge.engine.recovery import promote_due_retries
    from flowforge.engine.types import Failed

    run_id = await start_run(db, registry, [task("t", queue_timeout=5)])
    c = (await claim_tasks(db, worker_id="w", handlers=["noop"], limit=1, lease_seconds=60))[0]
    await complete_attempt(
        db, claimed=c, worker_id="w", result=Failed(FailureInfo("X", "x", True)), duration_ms=1
    )
    async with db.begin() as conn:  # first READY was an hour ago; the retry is due now
        await conn.execute(
            text(
                "UPDATE tasks SET ready_at = now() - interval '1 hour',"
                " next_attempt_at = now() - interval '1 second' WHERE run_id = :r"
            ),
            {"r": run_id},
        )
    assert await promote_due_retries(db, limit=10) == 1
    assert await fail_queue_timeouts(db, limit=10) == 0  # fresh clock: 5 s not yet used up
    t = (await tasks_by_key(db, run_id))["t"]
    assert t["status"] == "READY"
    async with db.connect() as conn:
        waited = (
            await conn.execute(
                text("SELECT extract(epoch FROM now() - ready_at) FROM tasks WHERE run_id = :r"),
                {"r": run_id},
            )
        ).scalar_one()
    assert float(waited) < 5


async def test_queue_timeout_with_retries_end_to_end(db, settings, registry):
    """End-to-end companion: a retry that waits longer than the queue timeout still runs."""
    run_id = await start_run(
        db,
        registry,
        [
            task(
                "t",
                "fail_until",
                input={"fail_times": 1},
                queue_timeout=0.8,
                initial_delay=1.2,
                max_delay=1.2,
            )
        ],
    )
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}, timeout=15) == "SUCCEEDED"
    assert [a["outcome"] for a in await attempts_for(db, run_id)] == ["FAILED", "SUCCEEDED"]


async def test_queue_timeout_races_with_claims_safely(db, registry):
    """Many claimers vs. the queue-timeout sweep: every task ends up either claimed or
    timed out, never both, and never lost."""
    run_id = await start_run(
        db, registry, [task(f"t{i:02d}", queue_timeout=0.05) for i in range(40)]
    )
    await asyncio.sleep(0.1)  # every task is now past its queue timeout

    async def claimer(i: int) -> int:
        n = 0
        while got := await claim_tasks(
            db, worker_id=f"c{i}", handlers=["noop"], limit=2, lease_seconds=60
        ):
            n += len(got)
        return n

    results = await asyncio.gather(
        *(claimer(i) for i in range(6)), fail_queue_timeouts(db, limit=100)
    )
    claimed, timed_out = sum(results[:-1]), results[-1]
    tasks = await tasks_by_key(db, run_id)
    statuses = [t["status"] for t in tasks.values()]
    assert claimed + timed_out == 40
    assert statuses.count("RUNNING") == claimed and statuses.count("FAILED") == timed_out
    for t in tasks.values():  # a timed-out task never got an attempt
        assert (t["status"] == "FAILED") == (t["attempt_count"] == 0)


async def test_definitions_stored_before_new_fields_can_be_reposted(db, registry):
    """Adding optional fields must not make an identical re-post look like a conflict."""
    from flowforge.domain.definition import WorkflowDefinitionSpec
    from flowforge.engine.submission import canonical_hash, create_definition

    spec = WorkflowDefinitionSpec.model_validate(
        {"name": "legacy", "version": 1, "tasks": [task("a")]}
    )
    def_id, created = await create_definition(
        db, tenant_id=TENANT, created_by="t", spec=spec, known_handlers=registry.names()
    )
    assert created
    async with db.begin() as conn:  # simulate a row written by the previous schema
        await conn.execute(
            text("ALTER TABLE workflow_definitions DISABLE TRIGGER workflow_definitions_immutable")
        )
        await conn.execute(
            text(
                "UPDATE workflow_definitions SET definition_json = (definition_json"
                " - 'deadline_seconds') || jsonb_build_object('tasks', (SELECT jsonb_agg(t -"
                " 'queue_timeout_seconds') FROM jsonb_array_elements(definition_json->'tasks') t))"
                " WHERE id = :d"
            ),
            {"d": def_id},
        )
        # ...including the hash the old code computed over that older JSON shape.
        old_json = (
            await conn.execute(
                text("SELECT definition_json FROM workflow_definitions WHERE id = :d"),
                {"d": def_id},
            )
        ).scalar_one()
        assert "deadline_seconds" not in old_json
        await conn.execute(
            text("UPDATE workflow_definitions SET spec_hash = :h WHERE id = :d"),
            {"h": canonical_hash(old_json), "d": def_id},
        )
        await conn.execute(
            text("ALTER TABLE workflow_definitions ENABLE TRIGGER workflow_definitions_immutable")
        )
    again, created = await create_definition(
        db, tenant_id=TENANT, created_by="t", spec=spec, known_handlers=registry.names()
    )
    assert again == def_id and not created
