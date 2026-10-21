"""Scheduler-side operations: retry promotion, lease recovery, reconciliation.

Every operation is safe to run concurrently from several scheduler processes:
rows are selected with SKIP LOCKED or re-checked after locking, and every state
change is conditional on the state that was observed.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from flowforge.domain.errors import FailureInfo
from flowforge.domain.retry import RetryPolicy
from flowforge.domain.states import AttemptOutcome, TaskStatus
from flowforge.engine.execution import finish_attempt, plan_result, settle_run
from flowforge.engine.progression import advance_run
from flowforge.engine.sql import lock_run
from flowforge.engine.types import Failed
from flowforge.observability import metrics
from flowforge.observability.logging import log_event

logger = logging.getLogger(__name__)

LEASE_EXPIRED_MESSAGE = (
    "worker stopped renewing its lease (crash, pause or partition); the handler's "
    "outcome and any external side effects are unknown"
)

_PROMOTE_RETRIES_SQL = text(
    """
    WITH due AS (
        SELECT id FROM tasks
         WHERE status = 'RETRY_WAIT' AND next_attempt_at <= statement_timestamp()
         ORDER BY next_attempt_at
         LIMIT :limit
           FOR UPDATE SKIP LOCKED
    ), promoted AS (
        UPDATE tasks t
           SET status = 'READY',
               ready_at = statement_timestamp(),
               updated_at = statement_timestamp()
          FROM due
         WHERE t.id = due.id
        RETURNING t.id, t.run_id, t.task_key, t.attempt_count
    ), ev AS (
        INSERT INTO task_events (run_id, task_id, event_type, data)
        SELECT run_id, id, 'task_ready',
               jsonb_build_object('task_key', task_key, 'reason', 'retry_due',
                                  'next_attempt_number', attempt_count + 1)
          FROM promoted
    )
    SELECT count(*) FROM promoted
    """
)

_EXPIRED_SQL = text(
    """
    SELECT id, run_id, task_key, current_attempt_id, attempt_count, attempt_limit,
           retry_policy, lease_owner
      FROM tasks
     WHERE status = 'RUNNING' AND lease_expires_at < statement_timestamp()
     ORDER BY lease_expires_at
     LIMIT :limit
    """
)

_RECONCILE_CANDIDATES_SQL = text(
    """
    SELECT r.id FROM workflow_runs r
     WHERE r.status IN ('PENDING', 'RUNNING', 'CANCELLING')
       AND (
            NOT EXISTS (SELECT 1 FROM tasks t WHERE t.run_id = r.id
                         AND t.status IN ('BLOCKED', 'READY', 'RUNNING', 'RETRY_WAIT'))
         OR (r.status = 'CANCELLING' AND NOT EXISTS (
                SELECT 1 FROM tasks t WHERE t.run_id = r.id AND t.status = 'RUNNING'))
         OR (r.status <> 'CANCELLING' AND EXISTS (
                SELECT 1 FROM tasks t WHERE t.run_id = r.id AND t.status = 'BLOCKED'
                   AND NOT EXISTS (
                       SELECT 1 FROM task_dependencies d JOIN tasks p ON p.id = d.depends_on_task_id
                        WHERE d.task_id = t.id AND p.status <> 'SUCCEEDED')))
         OR (r.status <> 'CANCELLING' AND EXISTS (
                SELECT 1 FROM tasks t WHERE t.run_id = r.id AND t.status = 'BLOCKED'
                   AND EXISTS (
                       SELECT 1 FROM task_dependencies d JOIN tasks p ON p.id = d.depends_on_task_id
                        WHERE d.task_id = t.id
                          AND p.status IN ('FAILED', 'UPSTREAM_FAILED', 'CANCELLED'))))
       )
     LIMIT :limit
    """
)


async def promote_due_retries(engine: AsyncEngine, *, limit: int) -> int:
    with metrics.time_db_operation("promote_retries"):
        async with engine.begin() as conn:
            return int((await conn.execute(_PROMOTE_RETRIES_SQL, {"limit": limit})).scalar_one())


@dataclass(frozen=True, slots=True)
class RecoveredLease:
    task_id: uuid.UUID
    run_id: uuid.UUID
    new_status: TaskStatus


async def recover_expired_leases(engine: AsyncEngine, *, limit: int) -> list[RecoveredLease]:
    """Treat attempts whose lease expired as failed with an unknown outcome.

    A lease expiry consumes an attempt (it was counted at claim time), so a task
    that crashes its worker every time still terminates after ``max_attempts``.
    """
    async with engine.connect() as conn:
        candidates = (await conn.execute(_EXPIRED_SQL, {"limit": limit})).all()
    recovered: list[RecoveredLease] = []
    for cand in candidates:
        result = await _recover_one(engine, cand)
        if result is not None:
            recovered.append(result)
    return recovered


async def _recover_one(engine: AsyncEngine, cand: Any) -> RecoveredLease | None:
    policy = RetryPolicy.model_validate(cand.retry_policy)
    failure = FailureInfo("LeaseExpired", LEASE_EXPIRED_MESSAGE, retryable=True)
    task_status, _, _, retry_delay = plan_result(
        Failed(failure),
        attempt_number=cand.attempt_count,
        attempt_limit=cand.attempt_limit,
        policy=policy,
    )
    with metrics.time_db_operation("recover_lease"):
        async with engine.begin() as conn:
            # Fenced on the attempt id AND on the lease still being expired *now*:
            # if the worker renewed or finished in the meantime, nothing happens.
            run_id = await finish_attempt(
                conn,
                task_id=cand.id,
                attempt_id=cand.current_attempt_id,
                task_status=task_status,
                outcome=AttemptOutcome.LEASE_EXPIRED,
                failure=failure,
                retry_delay=retry_delay,
                output=None,
                duration_ms=None,
                event_type="lease_expired",
                event_data={
                    "task_key": cand.task_key,
                    "attempt_number": cand.attempt_count,
                    "worker_id": cand.lease_owner,
                    "task_status": task_status.value,
                    "retry_delay_seconds": retry_delay,
                    "outcome_known": False,
                },
                require_expired=True,
            )
            if run_id is None:
                return None
            task_status, _ = await settle_run(conn, run_id, cand.id, task_status)
    metrics.EXPIRED_LEASES.inc()
    metrics.TASKS_COMPLETED.labels(AttemptOutcome.LEASE_EXPIRED.value).inc()
    if task_status in (TaskStatus.RETRY_WAIT, TaskStatus.READY):
        metrics.TASK_RETRIES.inc()
    log_event(
        logger,
        logging.WARNING,
        "lease expired; task recovered",
        event_type="lease_expired",
        task_id=str(cand.id),
        workflow_run_id=str(cand.run_id),
        worker_id=cand.lease_owner,
        task_status=task_status.value,
    )
    return RecoveredLease(task_id=cand.id, run_id=cand.run_id, new_status=task_status)


async def reconcile_runs(engine: AsyncEngine, *, limit: int) -> int:
    """Safety net: repair runs whose progression was not applied.

    In normal operation progression happens inside the transaction that changes
    task state, so this finds nothing. It exists so that a bug or an operator's
    manual database edit cannot leave a run stuck forever.
    """
    async with engine.begin() as conn:
        run_ids = [r.id for r in (await conn.execute(_RECONCILE_CANDIDATES_SQL, {"limit": limit}))]
    repaired = 0
    for run_id in run_ids:
        async with engine.begin() as conn:
            run = await lock_run(conn, run_id)
            if run is None:
                continue
            before = run["status"]
            after = await advance_run(conn, run)
            repaired += 1
            log_event(
                logger,
                logging.INFO,
                "run reconciled",
                event_type="run_reconciled",
                workflow_run_id=str(run_id),
                before=before,
                after=after.value,
            )
    return repaired


_STATUS_COUNTS_SQL = text(
    """
    SELECT status, count(*) FROM tasks
     WHERE status IN ('BLOCKED', 'READY', 'RUNNING', 'RETRY_WAIT')
     GROUP BY status
    """
)
_WORKER_STATS_SQL = text(
    """
    SELECT count(*) FILTER (
               WHERE last_heartbeat_at > statement_timestamp() - make_interval(secs => :lease))
               AS active,
           coalesce(max(extract(epoch FROM (statement_timestamp() - last_heartbeat_at))), 0)::float8
               AS max_age
      FROM workers WHERE stopped_at IS NULL
    """
)


async def sample_gauges(engine: AsyncEngine, *, lease_seconds: float) -> dict[str, int]:
    async with engine.begin() as conn:
        counts = {s: int(n) for s, n in (await conn.execute(_STATUS_COUNTS_SQL)).all()}
        workers = (await conn.execute(_WORKER_STATS_SQL, {"lease": lease_seconds})).one()
    for status in ("BLOCKED", "READY", "RUNNING", "RETRY_WAIT"):
        metrics.TASKS_BY_STATUS.labels(status).set(counts.get(status, 0))
    metrics.READY_QUEUE_DEPTH.set(counts.get("READY", 0))
    metrics.ACTIVE_WORKERS.set(workers.active)
    metrics.WORKER_HEARTBEAT_AGE.set(workers.max_age)
    return counts
