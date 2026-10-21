"""Retention: event pruning, run deletion, worker registry cleanup, and their safety rules."""

from __future__ import annotations

import uuid

import httpx
import pytest
from sqlalchemy import text

from flowforge.api.app import create_app
from flowforge.engine.control import retry_failed_task
from flowforge.engine.execution import register_worker
from flowforge.engine.retention import apply_retention
from flowforge.engine.submission import submit_run
from flowforge.scheduler.service import Scheduler
from tests.conftest import make_settings
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


async def finished_runs(db, settings, registry, n: int, **kw) -> list[uuid.UUID]:
    ids = [
        await start_run(db, registry, [task("a"), task("b", "noop", ["a"])], **kw) for _ in range(n)
    ]
    async with Cluster(db, settings, registry):
        for rid in ids:
            await wait_for_run(db, rid, {"SUCCEEDED", "FAILED"})
    return ids


async def age_run(db, run_id: uuid.UUID, days: float) -> None:
    async with db.begin() as conn:
        await conn.execute(
            text(
                "UPDATE workflow_runs SET completed_at = now() - make_interval(secs => :s)"
                " WHERE id = :r"
            ),
            {"s": days * 86400, "r": run_id},
        )


async def count(db, table: str, run_id: uuid.UUID) -> int:
    column = "id" if table == "workflow_runs" else "run_id"
    async with db.connect() as conn:
        return int(
            (
                await conn.execute(
                    text(f"SELECT count(*) FROM {table} WHERE {column} = :r"), {"r": run_id}
                )
            ).scalar_one()
        )


async def dependency_rows(db, run_id: uuid.UUID) -> int:
    async with db.connect() as conn:
        return int(
            (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM task_dependencies d JOIN tasks t ON t.id = d.task_id"
                        " WHERE t.run_id = :r"
                    ),
                    {"r": run_id},
                )
            ).scalar_one()
        )


async def test_retention_is_off_by_default(db, settings, registry):
    (old,) = await finished_runs(db, settings, registry, 1)
    await age_run(db, old, days=3650)
    stats = await apply_retention(db, make_settings())
    assert (stats.runs_deleted, stats.events_deleted) == (0, 0)
    assert await count(db, "workflow_runs", old) == 1
    assert await count(db, "task_events", old) > 0


async def test_event_pruning_keeps_runs_tasks_and_attempts(db, settings, registry):
    old, recent = await finished_runs(db, settings, registry, 2)
    await age_run(db, old, days=10)
    attempts_before = await attempts_for(db, old)
    cfg = make_settings(event_retention_days=7)

    stats = await apply_retention(db, cfg)
    assert stats.runs_events_pruned == 1 and stats.events_deleted > 0
    assert await events_for(db, old) == []
    assert await events_for(db, recent) != []  # too young
    # History that matters for debugging is kept.
    assert await attempts_for(db, old) == attempts_before
    assert {t["status"] for t in (await tasks_by_key(db, old)).values()} == {"SUCCEEDED"}

    again = await apply_retention(db, cfg)  # idempotent: the run is marked pruned
    assert (again.runs_events_pruned, again.events_deleted) == (0, 0)

    app = create_app(settings, engine=db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        run = (await c.get(f"/v1/runs/{old}", headers={"X-API-Key": "user-key"})).json()
        evs = (await c.get(f"/v1/runs/{old}/events", headers={"X-API-Key": "user-key"})).json()
    assert run["events_pruned_at"] is not None and evs["items"] == []


async def test_run_deletion_cascades_and_frees_the_idempotency_key(db, settings, registry):
    old, recent = await finished_runs(
        db, settings, registry, 2, name="retained", idempotency_key=None
    )
    async with db.connect() as conn:
        def_id = (
            await conn.execute(
                text("SELECT definition_id FROM workflow_runs WHERE id=:r"), {"r": old}
            )
        ).scalar_one()
    keyed = (
        await submit_run(
            db,
            tenant_id=TENANT,
            created_by="t",
            definition_id=def_id,
            run_input={"x": 1},
            idempotency_key="order-1",
            max_input_bytes=65536,
        )
    ).run_id
    async with Cluster(db, settings, registry):
        await wait_for_run(db, keyed, {"SUCCEEDED"})
    for rid in (old, keyed):
        await age_run(db, rid, days=40)

    stats = await apply_retention(db, make_settings(run_retention_days=30))
    assert stats.runs_deleted == 2
    for rid in (old, keyed):
        for table in ("workflow_runs", "tasks", "task_attempts", "task_events"):
            assert await count(db, table, rid) == 0, table
        assert await dependency_rows(db, rid) == 0
    async with db.connect() as conn:
        idem = (await conn.execute(text("SELECT count(*) FROM idempotency_records"))).scalar_one()
        defs = (await conn.execute(text("SELECT count(*) FROM workflow_definitions"))).scalar_one()
    assert idem == 0
    assert defs >= 1  # definitions are never deleted by retention
    assert await count(db, "workflow_runs", recent) == 1  # too young

    # The idempotency window equals the run retention period: the key is reusable now.
    again = await submit_run(
        db,
        tenant_id=TENANT,
        created_by="t",
        definition_id=def_id,
        run_input={"x": 1},
        idempotency_key="order-1",
        max_input_bytes=65536,
    )
    assert again.created and again.run_id != keyed


async def test_active_runs_are_never_deleted_however_old(db, registry):
    active = await start_run(db, registry, [task("a")])  # READY, no worker running
    async with db.begin() as conn:
        await conn.execute(
            text("UPDATE workflow_runs SET created_at = now() - interval '5 years' WHERE id = :r"),
            {"r": active},
        )
    stats = await apply_retention(db, make_settings(run_retention_days=1, event_retention_days=1))
    assert stats.runs_deleted == 0 and stats.events_deleted == 0
    assert await count(db, "workflow_runs", active) == 1
    assert await count(db, "task_events", active) > 0


async def test_runs_locked_by_another_transaction_are_skipped(db, settings, registry):
    (old,) = await finished_runs(db, settings, registry, 1)
    await age_run(db, old, days=40)
    cfg = make_settings(run_retention_days=30)
    async with db.connect() as holder:
        await holder.execute(
            text("SELECT id FROM workflow_runs WHERE id = :r FOR NO KEY UPDATE"), {"r": old}
        )
        # e.g. an admin's manual retry holds the run: retention must not wait or delete it.
        assert (await apply_retention(db, cfg)).runs_deleted == 0
        await holder.rollback()
    assert (await apply_retention(db, cfg)).runs_deleted == 1


async def test_large_backlog_is_deleted_in_bounded_batches(db, settings, registry):
    ids = await finished_runs(db, settings, registry, 7)
    for rid in ids:
        await age_run(db, rid, days=40)
    stats = await apply_retention(db, make_settings(run_retention_days=30, retention_batch_size=3))
    assert stats.runs_deleted == 7  # 3 + 3 + 1, each its own short transaction


async def test_manual_retry_clears_the_pruned_marker(db, settings, registry):
    run_id = await start_run(db, registry, [task("a", "always_fail", max_attempts=1)])
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, run_id, {"FAILED"}) == "FAILED"
    await age_run(db, run_id, days=10)
    await apply_retention(db, make_settings(event_retention_days=7))
    a = (await tasks_by_key(db, run_id))["a"]
    await retry_failed_task(db, tenant_id=TENANT, task_id=a["id"], requested_by="admin")
    async with db.connect() as conn:
        marker = (
            await conn.execute(
                text("SELECT events_pruned_at FROM workflow_runs WHERE id=:r"), {"r": run_id}
            )
        ).scalar_one()
    assert marker is None
    assert next(e["event_type"] for e in await events_for(db, run_id)) == "task_manual_retry"


async def test_stale_workers_are_removed_from_the_registry(db):
    await register_worker(db, "gone", pid=1, concurrency=1, handlers=["noop"])
    await register_worker(db, "alive", pid=2, concurrency=1, handlers=["noop"])
    async with db.begin() as conn:
        await conn.execute(
            text(
                "UPDATE workers SET last_heartbeat_at = now() - interval '8 days'"
                " WHERE worker_id = 'gone'"
            )
        )
    stats = await apply_retention(db, make_settings())  # default: one week
    assert stats.workers_deleted == 1
    async with db.connect() as conn:
        left = [r[0] for r in await conn.execute(text("SELECT worker_id FROM workers"))]
    assert left == ["alive"]


async def test_scheduler_applies_retention_on_its_own_interval(db, settings, registry):
    (old,) = await finished_runs(db, settings, registry, 1)
    await age_run(db, old, days=40)
    sched = Scheduler(db, make_settings(run_retention_days=30, retention_interval_seconds=3600))
    first = await sched.cycle()
    assert first.retention is not None and first.retention.runs_deleted == 1
    second = await sched.cycle()
    assert second.retention is None  # not due again for an hour


async def test_stale_result_for_a_run_deleted_by_retention_is_dropped(db, registry):
    """A worker that comes back after its run was deleted gets a clean rejection,
    not a foreign-key error it would retry for a minute."""
    from flowforge.engine.execution import claim_tasks, complete_attempt
    from flowforge.engine.types import Succeeded

    run_id = await start_run(db, registry, [task("a")])
    claimed = (await claim_tasks(db, worker_id="w", handlers=["noop"], limit=1, lease_seconds=30))[
        0
    ]
    async with db.begin() as conn:  # the run was finished and later deleted by retention
        await conn.execute(text("DELETE FROM workflow_runs WHERE id = :r"), {"r": run_id})
    outcome = await complete_attempt(
        db, claimed=claimed, worker_id="w", result=Succeeded({}), duration_ms=1
    )
    assert not outcome.accepted and outcome.reason == "lease_lost"
    async with db.connect() as conn:
        assert (await conn.execute(text("SELECT count(*) FROM task_events"))).scalar_one() == 0
