"""Concurrency tests: verified against persisted state and attempt history."""

from __future__ import annotations

import asyncio
import random
from collections import Counter

import pytest
from sqlalchemy import text

from flowforge.engine.execution import claim_tasks, complete_attempt
from flowforge.engine.submission import IdempotencyConflictError, submit_run
from flowforge.engine.types import Succeeded
from tests.support.helpers import (
    TENANT,
    Cluster,
    start_run,
    task,
    tasks_by_key,
    wait_for_run,
)

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("trial", range(3))
async def test_concurrent_claimers_never_share_a_task(db, settings, registry, trial):
    """30 claimers race over 300 READY tasks: every task is claimed exactly once."""
    tasks = [task(f"t{i:03d}") for i in range(300)]
    run_id = await start_run(db, registry, tasks)
    claims: list = []

    async def claimer(i: int) -> None:
        rng = random.Random(trial * 100 + i)
        while True:
            got = await claim_tasks(
                db,
                worker_id=f"claimer-{i}",
                handlers=["noop"],
                limit=rng.randint(1, 7),
                lease_seconds=60,
            )
            if not got:
                return
            claims.extend((f"claimer-{i}", c) for c in got)

    await asyncio.gather(*(claimer(i) for i in range(30)))

    task_ids = Counter(c.task_id for _, c in claims)
    assert len(task_ids) == 300
    assert set(task_ids.values()) == {1}, "a task was claimed twice"
    # Persisted state agrees: one RUNNING attempt per task, owner matches the claimer.
    async with db.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT t.lease_owner, a.worker_id, t.attempt_count, a.attempt_number, "
                    "t.current_attempt_id = a.id AS current "
                    "FROM tasks t JOIN task_attempts a ON a.task_id = t.id WHERE t.run_id = :r"
                ),
                {"r": run_id},
            )
        ).all()
    assert len(rows) == 300
    assert all(r.lease_owner == r.worker_id and r.current for r in rows)
    assert {(r.attempt_count, r.attempt_number) for r in rows} == {(1, 1)}


async def test_many_workers_complete_many_runs_without_duplicate_success(db, settings, registry):
    run_ids = [
        await start_run(
            db,
            registry,
            [
                task("a"),
                task("b", "noop", ["a"]),
                task("c", "noop", ["a"]),
                task("d", "noop", ["b", "c"]),
            ],
            name="shared-shape" if i == 0 else None,
        )
        for i in range(25)
    ]
    async with Cluster(db, settings, registry, workers=5, concurrency=4):
        for rid in run_ids:
            assert await wait_for_run(db, rid, {"SUCCEEDED", "FAILED"}, timeout=60) == "SUCCEEDED"
    async with db.connect() as conn:
        dup = (
            await conn.execute(
                text(
                    "SELECT task_id FROM task_attempts WHERE outcome = 'SUCCEEDED' "
                    "GROUP BY task_id HAVING count(*) > 1"
                )
            )
        ).all()
        total = (await conn.execute(text("SELECT count(*) FROM task_attempts"))).scalar_one()
    assert dup == []
    assert total == 100  # 25 runs x 4 tasks, no retries, no duplicate attempts


@pytest.mark.parametrize("trial", range(3))
async def test_fan_in_promotion_has_no_write_skew(db, settings, registry, trial):
    """20 parents finishing concurrently must promote their shared child exactly once."""
    parents = [task(f"p{i:02d}") for i in range(20)]
    run_id = await start_run(
        db, registry, [*parents, task("child", "noop", [p["id"] for p in parents])]
    )
    claimed = await claim_tasks(db, worker_id="w", handlers=["noop"], limit=20, lease_seconds=60)
    assert len(claimed) == 20
    results = await asyncio.gather(
        *(
            complete_attempt(db, claimed=c, worker_id="w", result=Succeeded({}), duration_ms=1)
            for c in claimed
        )
    )
    assert all(r.accepted for r in results)
    child = (await tasks_by_key(db, run_id))["child"]
    assert child["status"] == "READY"
    async with db.connect() as conn:
        n = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM task_events"
                    " WHERE task_id = :t AND event_type='task_ready'"
                ),
                {"t": child["id"]},
            )
        ).scalar_one()
    assert n == 1


async def test_concurrent_idempotent_submissions_create_one_run(db, settings, registry):
    first = await start_run(db, registry, [task("a")], name="idem", idempotency_key="seed")
    async with db.connect() as conn:
        def_id = (
            await conn.execute(
                text("SELECT definition_id FROM workflow_runs WHERE id=:i"), {"i": first}
            )
        ).scalar_one()

    async def submit(i: int):
        return await submit_run(
            db,
            tenant_id=TENANT,
            created_by="t",
            definition_id=def_id,
            run_input={"order": 42},
            idempotency_key="order-42",
            max_input_bytes=65536,
        )

    results = await asyncio.gather(*(submit(i) for i in range(25)))
    assert len({r.run_id for r in results}) == 1
    assert sum(r.created for r in results) == 1
    async with db.connect() as conn:
        runs = (
            await conn.execute(
                text("SELECT count(*) FROM workflow_runs WHERE idempotency_key='order-42'")
            )
        ).scalar_one()
        records = (
            await conn.execute(
                text("SELECT count(*) FROM idempotency_records WHERE idempotency_key='order-42'")
            )
        ).scalar_one()
    assert runs == 1 and records == 1

    # Same key, different request -> conflict, under concurrency too.
    async def conflicting(i: int):
        return await submit_run(
            db,
            tenant_id=TENANT,
            created_by="t",
            definition_id=def_id,
            run_input={"order": 43 + i},
            idempotency_key="order-42",
            max_input_bytes=65536,
        )

    outcomes = await asyncio.gather(*(conflicting(i) for i in range(5)), return_exceptions=True)
    assert all(isinstance(o, IdempotencyConflictError) for o in outcomes)

    # Keys are scoped per tenant: another tenant may use the same key.
    other = await start_run(
        db, registry, [task("a")], name="idem", tenant="tenant-b", idempotency_key="order-42"
    )
    assert other not in {r.run_id for r in results}
