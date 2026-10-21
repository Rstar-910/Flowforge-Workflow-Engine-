"""Race tests: two valid operations on the same task or run at the same moment.

Two techniques:

* **Forced interleavings.** An external connection holds the run row lock while
  both operations are started; we wait until both are queued behind it in
  PostgreSQL, then release. PostgreSQL grants the queued lock requests in
  order, which pins the dangerous ordering deterministically (for example: a
  completion has already written its RETRY_WAIT decision, and a cancellation
  commits before the completion re-reads the run).
* **Randomised repetition.** Both operations are launched together many times
  with small random offsets.

Every test ends with the global invariant check (see tests/support/invariants.py,
applied by the ``db`` fixture), so collateral damage is caught too.
"""

from __future__ import annotations

import asyncio
import random
import uuid
from collections import Counter

import pytest
from sqlalchemy import text

from flowforge.domain.errors import FailureInfo
from flowforge.engine.control import cancel_run, retry_failed_task
from flowforge.engine.execution import claim_tasks, complete_attempt, renew_leases
from flowforge.engine.recovery import promote_due_retries, recover_expired_leases
from flowforge.engine.submission import ConflictError
from flowforge.engine.timeouts import enforce_run_deadlines
from flowforge.engine.types import Failed, Succeeded
from flowforge.scheduler.service import Scheduler
from tests.conftest import make_settings
from tests.support.helpers import (
    TENANT,
    attempts_for,
    events_for,
    run_status,
    start_run,
    task,
    tasks_by_key,
)
from tests.support.invariants import INVARIANTS, violations

pytestmark = pytest.mark.integration

RETRYABLE = Failed(FailureInfo("Boom", "transient", retryable=True))


async def claim_one(db, worker: str = "w", handler: str = "noop", run_id=None):
    got = await claim_tasks(db, worker_id=worker, handlers=[handler], limit=1, lease_seconds=60)
    assert len(got) == 1
    if run_id is not None:
        assert got[0].run_id == run_id, "claimed a task of another run (test isolation bug)"
    return got[0]


async def fresh(db) -> None:
    """Start a repetition from an empty database, so trials cannot interfere."""
    from tests.conftest import TABLES

    async with db.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE"))


async def expire(db, task_id: uuid.UUID) -> None:
    async with db.begin() as conn:
        await conn.execute(
            text("UPDATE tasks SET lease_expires_at = now() - interval '1 second' WHERE id=:i"),
            {"i": task_id},
        )


_RUN_LOCK_WAITERS = text(
    "SELECT pid FROM pg_stat_activity WHERE datname = current_database()"
    " AND wait_event_type = 'Lock' AND query LIKE '%FROM workflow_runs%FOR NO KEY UPDATE%'"
)


async def run_lock_waiters(db) -> set[int]:
    async with db.connect() as conn:
        return {r[0] for r in (await conn.execute(_RUN_LOCK_WAITERS)).all()}


class RunLockHolder:
    """Hold a run's lock in an external transaction to stage an interleaving."""

    def __init__(self, db, run_id: uuid.UUID) -> None:
        self.db, self.run_id = db, run_id

    async def __aenter__(self) -> RunLockHolder:
        stray = await run_lock_waiters(self.db)
        assert not stray, f"sessions already waiting on a run lock (leaked by a test?): {stray}"
        self.queued: list[int] = []
        self.conn = await self.db.connect()
        self.tx = await self.conn.begin()
        await self.conn.execute(
            text("SELECT 1 FROM workflow_runs WHERE id = :r FOR NO KEY UPDATE"), {"r": self.run_id}
        )
        return self

    async def queued_next(self) -> int:
        """Wait until one more session is queued for the run lock; return its pid.

        Tracks backend pids rather than counting, so the staging order is exact even
        if unrelated sessions wait on other locks.
        """
        for _ in range(500):
            new = await run_lock_waiters(self.db) - set(self.queued)
            if len(new) == 1:
                self.queued.append(new.pop())
                return self.queued[-1]
            assert len(new) < 2, f"two sessions queued at once: {new}"
            await asyncio.sleep(0.01)
        raise AssertionError("no new session queued for the run lock")

    async def release(self) -> None:
        if self.tx.is_active:
            await self.tx.rollback()

    async def __aexit__(self, *exc: object) -> None:
        await self.release()
        await self.conn.close()


class TaskRowHolder:
    """Hold a task row lock so a completion/recovery blocks at its first statement.

    Staging on the *task* row (not the run row) gives a deterministic order: the
    blocked operation holds no lock on the run yet, so it cannot use PostgreSQL's
    lock-upgrade queue jumping (a transaction already holding FOR KEY SHARE on the
    run via a foreign key may skip the run-lock queue).
    """

    _WAITING = text(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
        " AND wait_event_type = 'Lock' AND query LIKE '%WITH t AS%UPDATE tasks%'"
    )

    def __init__(self, db, task_id: uuid.UUID) -> None:
        self.db, self.task_id = db, task_id

    async def __aenter__(self) -> TaskRowHolder:
        self.conn = await self.db.connect()
        self.tx = await self.conn.begin()
        await self.conn.execute(
            text("SELECT 1 FROM tasks WHERE id = :t FOR NO KEY UPDATE"), {"t": self.task_id}
        )
        return self

    async def blocked(self) -> None:
        """Wait until the fenced UPDATE of the task is blocked on our lock."""
        for _ in range(500):
            async with self.db.connect() as c:
                if (await c.execute(self._WAITING)).scalar_one() == 1:
                    return
            await asyncio.sleep(0.01)
        raise AssertionError("the operation never blocked on the task row")

    async def release(self) -> None:
        if self.tx.is_active:
            await self.tx.rollback()

    async def __aexit__(self, *exc: object) -> None:
        await self.release()
        await self.conn.close()


async def jitter() -> None:
    await asyncio.sleep(random.random() * 0.004)


async def event_types(db, run_id, task_id=None) -> list[str]:
    return [
        e["event_type"]
        for e in await events_for(db, run_id)
        if task_id is None or e["task_id"] == task_id
    ]


# --------------------------------------------------------------- the checker itself


@pytest.mark.no_invariants
async def test_invariant_checker_detects_corruption(db, registry):
    """The checker is only useful if it can fail: corrupt the DB in five ways."""
    run_id = await start_run(db, registry, [task("a"), task("b", "noop", ["a"])])
    c = await claim_one(db)
    corruptions = {
        "open attempt on a task that is not RUNNING it": (
            "UPDATE task_attempts SET ended_at = NULL WHERE id = :a; "
            "UPDATE tasks SET status = 'READY', lease_owner = NULL, lease_expires_at = NULL,"
            " current_attempt_id = NULL WHERE id = :t"
        ),
        "task READY/RUNNING although a dependency has not succeeded": (
            "UPDATE tasks SET status = 'READY' WHERE run_id = :r AND task_key = 'b'"
        ),
        "runnable task in a stopping or finished run": (
            "UPDATE workflow_runs SET status = 'CANCELLING' WHERE id = :r"
        ),
        "attempt numbers not dense 1..attempt_count": (
            "UPDATE tasks SET attempt_count = 2, attempt_limit = 3 WHERE id = :t"
        ),
    }
    async with db.begin() as conn:
        for sql in corruptions.values():
            for stmt in sql.split("; "):
                await conn.execute(text(stmt), {"a": c.attempt_id, "t": c.task_id, "r": run_id})
    found = await violations(db)
    for name in corruptions:
        assert name in found, f"checker missed: {name}"
    assert set(found) <= set(INVARIANTS)


# --------------------------------------------------------------- ownership races


async def test_completion_vs_lease_recovery_exactly_one_wins(db, registry):
    """The worker reports just as the scheduler recovers its expired lease."""
    outcomes: Counter[str] = Counter()
    for _ in range(30):
        await fresh(db)
        run_id = await start_run(db, registry, [task("t", max_attempts=3, initial_delay=0)])
        c = await claim_one(db, run_id=run_id)
        await expire(db, c.task_id)

        async def complete(c=c):
            await jitter()
            return await complete_attempt(
                db, claimed=c, worker_id="w", result=Succeeded({"ok": 1}), duration_ms=1
            )

        async def recover():
            await jitter()
            return await recover_expired_leases(db, limit=10)

        done, recovered = await asyncio.gather(complete(), recover())
        assert done.accepted != bool(recovered), "both or neither won"
        t = (await tasks_by_key(db, run_id))["t"]
        attempt = (await attempts_for(db, run_id))[0]
        if done.accepted:
            outcomes["completion"] += 1
            assert t["status"] == "SUCCEEDED" and attempt["outcome"] == "SUCCEEDED"
            assert "lease_expired" not in await event_types(db, run_id)
        else:
            outcomes["recovery"] += 1
            assert t["status"] in ("READY", "RETRY_WAIT") and t["output_json"] is None
            assert attempt["outcome"] == "LEASE_EXPIRED"
            assert "stale_result_rejected" in await event_types(db, run_id)
    print("completion vs recovery outcomes:", dict(outcomes))


async def test_renewal_vs_lease_recovery_never_both(db, registry):
    outcomes: Counter[str] = Counter()
    for _ in range(30):
        await fresh(db)
        run_id = await start_run(db, registry, [task("t", max_attempts=3)])
        c = await claim_one(db, run_id=run_id)
        await expire(db, c.task_id)

        async def renew(c=c):
            await jitter()
            return await renew_leases(
                db, worker_id="w", attempt_ids=[c.attempt_id], lease_seconds=60
            )

        async def recover():
            await jitter()
            return await recover_expired_leases(db, limit=10)

        renewal, recovered = await asyncio.gather(renew(), recover())
        renewed = c.attempt_id in renewal.renewed
        assert renewed != bool(recovered), "renewed AND recovered (or neither)"
        t = (await tasks_by_key(db, run_id))["t"]
        if renewed:
            outcomes["renewed"] += 1
            assert t["status"] == "RUNNING" and t["current_attempt_id"] == c.attempt_id
        else:
            outcomes["recovered"] += 1
            assert t["current_attempt_id"] is None
    print("renewal vs recovery outcomes:", dict(outcomes))


async def test_stale_worker_reusing_its_worker_id_cannot_touch_the_new_attempt(db, registry):
    """A restarted worker often keeps its configured id; fencing is by attempt, not worker."""
    run_id = await start_run(db, registry, [task("t", max_attempts=3, initial_delay=0)])
    old = await claim_one(db, worker="worker-1")
    await expire(db, old.task_id)
    await recover_expired_leases(db, limit=10)
    new = await claim_one(db, worker="worker-1")  # same id, new process, new attempt
    renewal = await renew_leases(
        db, worker_id="worker-1", attempt_ids=[old.attempt_id], lease_seconds=60
    )
    assert renewal.renewed == set()
    late = await complete_attempt(
        db, claimed=old, worker_id="worker-1", result=Succeeded({"by": "old"}), duration_ms=1
    )
    assert not late.accepted
    t = (await tasks_by_key(db, run_id))["t"]
    assert t["status"] == "RUNNING" and t["current_attempt_id"] == new.attempt_id
    done = await complete_attempt(
        db, claimed=new, worker_id="worker-1", result=Succeeded({"by": "new"}), duration_ms=1
    )
    assert done.accepted
    assert (await tasks_by_key(db, run_id))["t"]["output_json"] == {"by": "new"}


async def test_stale_attempt_cannot_overwrite_a_manually_retried_task(db, registry):
    """Fencing survives a FAILED -> manual retry -> new attempt cycle."""
    run_id = await start_run(db, registry, [task("t", max_attempts=1)])
    first = await claim_one(db, worker="A")
    await expire(db, first.task_id)
    await recover_expired_leases(db, limit=10)  # budget exhausted -> FAILED
    t = (await tasks_by_key(db, run_id))["t"]
    assert t["status"] == "FAILED" and await run_status(db, run_id) == "FAILED"
    await retry_failed_task(db, tenant_id=TENANT, task_id=t["id"], requested_by="admin")
    second = await claim_one(db, worker="B")
    assert second.attempt_number == 2
    late = await complete_attempt(
        db, claimed=first, worker_id="A", result=Succeeded({"by": "A"}), duration_ms=1
    )
    assert not late.accepted
    assert (await tasks_by_key(db, run_id))["t"]["current_attempt_id"] == second.attempt_id
    assert (
        await complete_attempt(
            db, claimed=second, worker_id="B", result=Succeeded({"by": "B"}), duration_ms=1
        )
    ).accepted
    assert await run_status(db, run_id) == "SUCCEEDED"


# --------------------------------------------------------------- cancellation races


async def test_cancel_commits_before_a_failing_completion_is_applied(db, registry):
    """Forced: the worker has already decided 'retry' (in Python) and its fenced UPDATE is
    blocked; the run is cancelled and committed meanwhile. The completion then writes
    READY for the retry, takes the run lock, sees CANCELLING and must cancel the retry."""
    run_id = await start_run(db, registry, [task("t", max_attempts=3, initial_delay=0)])
    c = await claim_one(db, run_id=run_id)
    async with TaskRowHolder(db, c.task_id) as holder:
        complete = asyncio.create_task(
            complete_attempt(db, claimed=c, worker_id="w", result=RETRYABLE, duration_ms=1)
        )
        await holder.blocked()
        assert await cancel_run(db, tenant_id=TENANT, run_id=run_id) == "CANCELLING"
        await holder.release()
        outcome = await complete
    assert outcome.accepted and outcome.task_status == "CANCELLED"
    t = (await tasks_by_key(db, run_id))["t"]
    assert t["status"] == "CANCELLED"
    assert await run_status(db, run_id) == "CANCELLED"
    assert [a["outcome"] for a in await attempts_for(db, run_id)] == ["FAILED"]
    assert "task_cancel_requested" in await event_types(db, run_id, t["id"])
    assert await claim_tasks(db, worker_id="x", handlers=["noop"], limit=5, lease_seconds=5) == []


async def test_cancel_commits_before_a_lease_recovery_is_applied(db, registry):
    run_id = await start_run(db, registry, [task("t", max_attempts=3, initial_delay=0)])
    c = await claim_one(db, run_id=run_id)
    await expire(db, c.task_id)
    async with TaskRowHolder(db, c.task_id) as holder:
        recover = asyncio.create_task(recover_expired_leases(db, limit=10))
        await holder.blocked()
        assert await cancel_run(db, tenant_id=TENANT, run_id=run_id) == "CANCELLING"
        await holder.release()
        recovered = await recover
    assert [r.new_status for r in recovered] == ["CANCELLED"]
    assert (await tasks_by_key(db, run_id))["t"]["status"] == "CANCELLED"
    assert await run_status(db, run_id) == "CANCELLED"


async def test_retry_promotion_vs_cancel_never_leaves_claimable_work(db, registry):
    for _ in range(25):
        await fresh(db)
        run_id = await start_run(db, registry, [task("t", max_attempts=3)])
        c = await claim_one(db, run_id=run_id)
        await complete_attempt(db, claimed=c, worker_id="w", result=RETRYABLE, duration_ms=1)
        async with db.begin() as conn:  # the retry is due now
            await conn.execute(
                text(
                    "UPDATE tasks SET next_attempt_at = now() - interval '1 second'"
                    " WHERE run_id = :r"
                ),
                {"r": run_id},
            )

        async def promote():
            await jitter()
            return await promote_due_retries(db, limit=10)

        async def cancel(run_id=run_id):
            await jitter()
            return await cancel_run(db, tenant_id=TENANT, run_id=run_id)

        await asyncio.gather(promote(), cancel())
        assert (await tasks_by_key(db, run_id))["t"]["status"] == "CANCELLED"
        assert await run_status(db, run_id) == "CANCELLED"
    assert await claim_tasks(db, worker_id="x", handlers=["noop"], limit=50, lease_seconds=5) == []


async def test_claim_vs_cancel_then_drain(db, registry):
    """Whatever a claim grabbed before the cancel must still drain the run to CANCELLED."""
    claimed_counts: list[int] = []
    for _ in range(15):
        await fresh(db)
        run_id = await start_run(db, registry, [task(f"t{i}") for i in range(8)])

        async def claim():
            await jitter()
            return await claim_tasks(
                db, worker_id="w", handlers=["noop"], limit=8, lease_seconds=60
            )

        async def cancel(run_id=run_id):
            await jitter()
            return await cancel_run(db, tenant_id=TENANT, run_id=run_id)

        claimed, status = await asyncio.gather(claim(), cancel())
        claimed_counts.append(len(claimed))
        assert status == ("CANCELLING" if claimed else "CANCELLED")
        tasks = await tasks_by_key(db, run_id)
        assert all(t["status"] in ("RUNNING", "CANCELLED") for t in tasks.values())
        assert sum(t["status"] == "RUNNING" for t in tasks.values()) == len(claimed)
        for c in claimed:  # the workers finish what they had (or obey the cancel)
            await complete_attempt(
                db, claimed=c, worker_id="w", result=Succeeded({}), duration_ms=1
            )
        assert await run_status(db, run_id) == "CANCELLED"
    print("tasks claimed before the cancel, per trial:", claimed_counts)


async def test_manual_retry_vs_cancel_are_serialised(db, registry):
    outcomes: Counter[str] = Counter()
    for _ in range(20):
        await fresh(db)
        run_id = await start_run(db, registry, [task("t", max_attempts=1)])
        c = await claim_one(db, run_id=run_id)
        await complete_attempt(
            db, claimed=c, worker_id="w", result=Failed(FailureInfo("X", "x", False)), duration_ms=1
        )
        assert await run_status(db, run_id) == "FAILED"
        tid = c.task_id

        async def retry(tid=tid):
            await jitter()
            return await retry_failed_task(db, tenant_id=TENANT, task_id=tid, requested_by="admin")

        async def cancel(run_id=run_id):
            await jitter()
            return await cancel_run(db, tenant_id=TENANT, run_id=run_id)

        r, x = await asyncio.gather(retry(), cancel(), return_exceptions=True)
        status = await run_status(db, run_id)
        if isinstance(x, ConflictError):  # cancel saw FAILED first; retry re-opened it
            outcomes["cancel-first"] += 1
            assert r == "READY" and status == "RUNNING"
        else:  # retry first, then the cancel stopped the re-opened run
            outcomes["retry-first"] += 1
            assert r == "READY" and x == "CANCELLED" and status == "CANCELLED"
    print("manual retry vs cancel outcomes:", dict(outcomes))


# --------------------------------------------------------------- deadline races


async def test_last_completion_vs_deadline_both_orders_are_consistent(db, registry):
    async def overdue_run():
        run_id = await start_run(db, registry, [task("t")], deadline=0.2)
        c = await claim_one(db)
        await asyncio.sleep(0.3)
        return run_id, c

    # Order A (forced): the deadline stops the run while the completion's fenced UPDATE is
    # blocked; the task still finishes successfully and the run ends FAILED.
    run_id, c = await overdue_run()
    async with TaskRowHolder(db, c.task_id) as holder:
        done = asyncio.create_task(
            complete_attempt(db, claimed=c, worker_id="w", result=Succeeded({}), duration_ms=1)
        )
        await holder.blocked()
        assert await enforce_run_deadlines(db, limit=10) == 1
        await holder.release()
        assert (await done).accepted
    assert await run_status(db, run_id) == "FAILED"  # the deadline passed before completion
    assert (await tasks_by_key(db, run_id))["t"]["status"] == "SUCCEEDED"  # work is recorded

    # Order B: the completion commits first; the sweep then finds nothing to do.
    # (Not forceable with a lock holder: the completion's event insert already took a
    # FOR KEY SHARE lock on the run through the foreign key, so its run-lock request is an
    # upgrade that does not keep its queue position.)
    run_id, c = await overdue_run()
    assert (
        await complete_attempt(db, claimed=c, worker_id="w", result=Succeeded({}), duration_ms=1)
    ).accepted
    assert await enforce_run_deadlines(db, limit=10) == 0
    assert await run_status(db, run_id) == "SUCCEEDED"

    # Concurrent: whichever wins, the outcome is one of the two consistent ones.
    outcomes: Counter[str] = Counter()
    for _ in range(15):
        await fresh(db)
        run_id, c = await overdue_run()

        async def finish(c=c):
            await jitter()
            return await complete_attempt(
                db, claimed=c, worker_id="w", result=Succeeded({}), duration_ms=1
            )

        async def sweep():
            await jitter()
            return await enforce_run_deadlines(db, limit=10)

        done, stopped = await asyncio.gather(finish(), sweep())
        assert done.accepted
        status = await run_status(db, run_id)
        assert (await tasks_by_key(db, run_id))["t"]["status"] == "SUCCEEDED"
        if stopped:
            outcomes["deadline first"] += 1
            assert status == "FAILED"
        else:
            outcomes["completion first"] += 1
            assert status == "SUCCEEDED"
    print("last completion vs deadline outcomes:", dict(outcomes))


async def test_deadline_vs_user_cancel_one_consistent_outcome(db, registry):
    outcomes: Counter[str] = Counter()
    for _ in range(20):
        await fresh(db)
        run_id = await start_run(db, registry, [task("t"), task("u", "noop", ["t"])], deadline=0.05)
        await asyncio.sleep(0.08)

        async def deadline():
            await jitter()
            return await enforce_run_deadlines(db, limit=10)

        async def cancel(run_id=run_id):
            await jitter()
            return await cancel_run(db, tenant_id=TENANT, run_id=run_id)

        await asyncio.gather(deadline(), cancel(), return_exceptions=True)
        async with db.connect() as conn:
            row = (
                await conn.execute(
                    text("SELECT status, deadline_exceeded_at FROM workflow_runs WHERE id=:r"),
                    {"r": run_id},
                )
            ).one()
        if row.status == "CANCELLED":
            outcomes["user cancel"] += 1
            assert row.deadline_exceeded_at is None
        else:
            outcomes["deadline"] += 1
            assert row.status == "FAILED" and row.deadline_exceeded_at is not None
    print("deadline vs cancel outcomes:", dict(outcomes))

    # Forced: the deadline sweep is queued for the run lock before the user's cancel.
    await fresh(db)
    run_id = await start_run(db, registry, [task("t")], deadline=0.05)
    await asyncio.sleep(0.08)
    async with RunLockHolder(db, run_id) as holder:
        sweep = asyncio.create_task(enforce_run_deadlines(db, limit=10))
        await holder.queued_next()
        cancel = asyncio.create_task(cancel_run(db, tenant_id=TENANT, run_id=run_id))
        await holder.queued_next()
        await holder.release()
        assert await sweep == 1
        with pytest.raises(ConflictError):  # the run is already FAILED by its deadline
            await cancel
    assert await run_status(db, run_id) == "FAILED"


async def test_manual_retry_refused_once_the_deadline_has_passed(db, registry):
    """Even if the run failed for another reason, re-opening it after its deadline would
    only let the deadline sweep stop it again a moment later."""
    run_id = await start_run(db, registry, [task("t", max_attempts=1)], deadline=0.3)
    c = await claim_one(db)
    await complete_attempt(
        db, claimed=c, worker_id="w", result=Failed(FailureInfo("X", "x", False)), duration_ms=1
    )
    assert await run_status(db, run_id) == "FAILED"  # failed before its deadline
    await asyncio.sleep(0.4)
    with pytest.raises(ConflictError, match="deadline"):
        await retry_failed_task(db, tenant_id=TENANT, task_id=c.task_id, requested_by="admin")


# --------------------------------------------------------------- several schedulers


async def test_two_schedulers_apply_each_effect_exactly_once(db, registry):
    """Expired leases, due retries, overdue deadlines and queue timeouts, swept by two
    scheduler instances at the same moment: every effect happens exactly once."""
    lease_runs = [
        await start_run(db, registry, [task("t", max_attempts=3, initial_delay=0)])
        for _ in range(8)
    ]
    for _ in lease_runs:
        c = await claim_one(db)
        await expire(db, c.task_id)
    retry_runs = [await start_run(db, registry, [task("t", max_attempts=3)]) for _ in range(8)]
    for _ in retry_runs:
        c = await claim_one(db)
        await complete_attempt(db, claimed=c, worker_id="w", result=RETRYABLE, duration_ms=1)
    async with db.begin() as conn:
        await conn.execute(
            text(
                "UPDATE tasks SET next_attempt_at = now() - interval '1 s'"
                " WHERE status = 'RETRY_WAIT'"
            )
        )
    deadline_runs = [await start_run(db, registry, [task("t")], deadline=0.05) for _ in range(5)]
    queue_runs = [
        await start_run(db, registry, [task("q", "echo", queue_timeout=0.05)]) for _ in range(5)
    ]
    await asyncio.sleep(0.1)

    settings = make_settings()
    stats = await asyncio.gather(Scheduler(db, settings).cycle(), Scheduler(db, settings).cycle())
    assert sum(s.leases_recovered for s in stats) == 8
    assert sum(s.retries_promoted for s in stats) == 8
    assert sum(s.deadlines_exceeded for s in stats) == 5
    assert sum(s.queue_timeouts for s in stats) == 5
    for rid in lease_runs:
        assert (await event_types(db, rid)).count("lease_expired") == 1
    for rid in deadline_runs:
        assert (await event_types(db, rid)).count("run_deadline_exceeded") == 1
        assert await run_status(db, rid) == "FAILED"
    for rid in queue_runs:
        assert (await event_types(db, rid)).count("task_queue_timeout") == 1
    for rid in retry_runs:
        ready = [
            e
            for e in await events_for(db, rid)
            if e["event_type"] == "task_ready" and e["data"].get("reason") == "retry_due"
        ]
        assert len(ready) == 1
