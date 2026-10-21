"""Worker-facing engine operations: claim, lease renewal and fenced result commit.

Transaction boundaries:

* ``claim_tasks``      - ONE autocommit statement: lock READY rows (SKIP LOCKED),
                         mark them RUNNING with a new attempt id, insert attempt
                         rows and events, activate PENDING runs (SKIP LOCKED) and
                         return run input + dependency outputs. Never waits on a lock.
* ``renew_leases``     - extend leases still owned by this worker's current attempts.
* ``complete_attempt`` - one short transaction: fenced UPDATE of the task (the
                         WHERE clause *is* the lease check), attempt + event in the
                         same statement, then lock the run and advance it.

Lock order (deadlock freedom): a transaction may lock its *own RUNNING* task row,
then the run row, then non-running task rows of that run. Transactions that
start from the run row (cancel, manual retry, reconcile) never touch RUNNING
rows; claims and retry promotion only use SKIP LOCKED and never wait.
"""

from __future__ import annotations

import json
import logging
import socket
import uuid
from collections.abc import Sequence
from typing import Any

from sqlalchemy import String, bindparam, select, text, update
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from flowforge import __version__
from flowforge.domain.errors import FailureInfo
from flowforge.domain.retry import RetryPolicy, compute_retry_delay
from flowforge.domain.states import AttemptOutcome, RunStatus, TaskStatus
from flowforge.engine.progression import advance_after_success, advance_run, set_run_status
from flowforge.engine.sql import append_events, attempts_t, lock_run, workers_t
from flowforge.engine.types import (
    Cancelled,
    ClaimedTask,
    CompletionOutcome,
    Failed,
    LeaseRenewal,
    Succeeded,
    TaskResult,
)
from flowforge.observability import metrics
from flowforge.observability.logging import log_event

logger = logging.getLogger(__name__)

_UUIDS = ARRAY(UUID(as_uuid=True))

_CLAIM_SQL = text(
    """
    WITH picked AS (
        SELECT id
          FROM tasks
         WHERE status = 'READY' AND handler = ANY(:handlers)
         ORDER BY ready_at
         LIMIT :limit
           FOR UPDATE SKIP LOCKED
    ), claimed AS (
        UPDATE tasks t
           SET status = 'RUNNING',
               attempt_count = t.attempt_count + 1,
               current_attempt_id = gen_random_uuid(),
               lease_owner = :worker_id,
               lease_expires_at = statement_timestamp() + make_interval(secs => :lease_seconds),
               started_at = coalesce(t.started_at, statement_timestamp()),
               updated_at = statement_timestamp()
          FROM picked
         WHERE t.id = picked.id
        RETURNING t.id, t.run_id, t.task_key, t.handler, t.current_attempt_id,
                  t.attempt_count, t.attempt_limit, t.timeout_seconds, t.retry_policy,
                  t.input_json, t.lease_expires_at,
                  greatest(0, extract(epoch FROM (statement_timestamp() - t.ready_at)))::float8
                      AS queue_wait
    ), new_attempts AS (
        INSERT INTO task_attempts
               (id, task_id, run_id, attempt_number, worker_id, outcome, queue_wait_ms,
                started_at)
        SELECT current_attempt_id, id, run_id, attempt_count, :worker_id, 'RUNNING',
               (queue_wait * 1000)::int, statement_timestamp()
          FROM claimed
    ), attempt_events AS (
        INSERT INTO task_events (run_id, task_id, attempt_id, event_type, data)
        SELECT run_id, id, current_attempt_id, 'attempt_started',
               jsonb_build_object('task_key', task_key, 'attempt_number', attempt_count,
                                  'worker_id', CAST(:worker_id AS text))
          FROM claimed
    ), activated AS (
        -- SKIP LOCKED: a claim never waits for a run lock. If the run row is busy,
        -- the completion path performs the PENDING -> RUNNING transition instead.
        UPDATE workflow_runs r
           SET status = 'RUNNING', started_at = statement_timestamp(),
               updated_at = statement_timestamp()
         WHERE r.id IN (SELECT w.id FROM workflow_runs w
                         WHERE w.id IN (SELECT run_id FROM claimed) AND w.status = 'PENDING'
                           FOR NO KEY UPDATE SKIP LOCKED)
        RETURNING r.id
    ), activation_events AS (
        INSERT INTO task_events (run_id, event_type, data)
        SELECT id, 'run_status_changed', '{"from": "PENDING", "to": "RUNNING"}'::jsonb
          FROM activated
    )
    SELECT c.*, r.input_json AS run_input, r.traceparent,
           -- ANY(ARRAY(...)): statistics-independent primary-key lookups (ADR 0012).
           coalesce((SELECT jsonb_object_agg(p.task_key, p.output_json)
                       FROM tasks p
                      WHERE p.id = ANY(ARRAY(SELECT d.depends_on_task_id
                                               FROM task_dependencies d
                                              WHERE d.task_id = c.id))),
                    '{}'::jsonb) AS dependency_outputs
      FROM claimed c JOIN workflow_runs r ON r.id = c.run_id
    """
).bindparams(bindparam("handlers", type_=ARRAY(String)))

_RENEW_SQL = text(
    """
    UPDATE tasks t
       SET lease_expires_at = statement_timestamp() + make_interval(secs => :lease_seconds)
      FROM workflow_runs r
     WHERE r.id = t.run_id
       AND t.current_attempt_id = ANY(:attempt_ids)
       AND t.status = 'RUNNING'
       AND t.lease_owner = :worker_id
    RETURNING t.current_attempt_id, r.status = 'CANCELLING' AS cancel_requested
    """
).bindparams(bindparam("attempt_ids", type_=_UUIDS))

# The fenced transition out of RUNNING. Its WHERE clause is the lease check:
# PostgreSQL re-evaluates it after acquiring the row lock, so of two concurrent
# writers (a worker's completion, the scheduler's lease recovery) exactly one
# matches. The attempt row and the audit event are written in the same statement.
FINISH_ATTEMPT_SQL = text(
    """
    WITH t AS (
        UPDATE tasks
           SET status = CAST(:task_status AS varchar),
               output_json = CASE WHEN CAST(:task_status AS varchar) = 'SUCCEEDED'
                                  THEN CAST(:output AS jsonb) ELSE output_json END,
               lease_owner = NULL, lease_expires_at = NULL, current_attempt_id = NULL,
               last_error_type = coalesce(CAST(:error_type AS varchar), last_error_type),
               last_error_message = coalesce(CAST(:error_message AS text), last_error_message),
               completed_at = CASE WHEN CAST(:task_status AS varchar)
                                        IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
                                   THEN statement_timestamp() END,
               next_attempt_at = CASE WHEN CAST(:task_status AS varchar) = 'RETRY_WAIT'
                                      THEN statement_timestamp()
                                           + make_interval(secs => CAST(:retry_delay AS float8))
                                      ELSE next_attempt_at END,
               ready_at = CASE WHEN CAST(:task_status AS varchar) = 'READY'
                               THEN statement_timestamp() ELSE ready_at END,
               updated_at = statement_timestamp()
         WHERE id = :task_id
           AND status = 'RUNNING'
           AND current_attempt_id = :attempt_id
           AND (NOT CAST(:require_expired AS boolean)
                OR lease_expires_at < statement_timestamp())
        RETURNING id, run_id
    ), a AS (
        UPDATE task_attempts
           SET outcome = CAST(:outcome AS varchar),
               ended_at = statement_timestamp(),
               duration_ms = coalesce(CAST(:duration_ms AS int),
                   (extract(epoch FROM (statement_timestamp() - started_at)) * 1000)::int),
               error_type = CAST(:error_type AS varchar),
               error_message = CAST(:error_message AS text),
               retryable = CAST(:retryable AS boolean)
         WHERE id = :attempt_id AND EXISTS (SELECT 1 FROM t)
    ), e AS (
        INSERT INTO task_events (run_id, task_id, attempt_id, event_type, data)
        SELECT run_id, id, :attempt_id, CAST(:event_type AS varchar),
               CAST(:event_data AS jsonb)
          FROM t
    )
    SELECT run_id FROM t
    """
)

_CANCEL_FIXUP_SQL = text(
    """
    UPDATE tasks
       SET status = 'CANCELLED', completed_at = statement_timestamp(),
           updated_at = statement_timestamp()
     WHERE id = :task_id AND status IN ('READY', 'RETRY_WAIT')
    """
)

_OWNER_OUTCOMES = frozenset(
    {
        AttemptOutcome.SUCCEEDED.value,
        AttemptOutcome.FAILED.value,
        AttemptOutcome.TIMED_OUT.value,
        AttemptOutcome.CANCELLED.value,
    }
)


async def register_worker(
    engine: AsyncEngine, worker_id: str, *, pid: int, concurrency: int, handlers: Sequence[str]
) -> None:
    values: dict[str, Any] = {
        "worker_id": worker_id,
        "hostname": socket.gethostname()[:255],
        "pid": pid,
        "version": __version__,
        "concurrency": concurrency,
        "handlers": sorted(handlers),
    }
    async with engine.begin() as conn:
        await conn.execute(
            pg_insert(workers_t)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["worker_id"],
                set_={
                    **values,
                    "last_heartbeat_at": text("statement_timestamp()"),
                    "stopped_at": None,
                },
            )
        )


async def mark_worker_stopped(engine: AsyncEngine, worker_id: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            update(workers_t)
            .where(workers_t.c.worker_id == worker_id)
            .values(
                stopped_at=text("statement_timestamp()"),
                last_heartbeat_at=text("statement_timestamp()"),
            )
        )


async def claim_tasks(
    engine: AsyncEngine,
    *,
    worker_id: str,
    handlers: Sequence[str],
    limit: int,
    lease_seconds: float,
) -> list[ClaimedTask]:
    if limit <= 0 or not handlers:
        return []
    with metrics.time_db_operation("claim"):
        async with engine.connect() as raw:
            # A single statement is atomic on its own: no BEGIN/COMMIT round trips.
            conn = await raw.execution_options(isolation_level="AUTOCOMMIT")
            rows = (
                await conn.execute(
                    _CLAIM_SQL,
                    {
                        "handlers": list(handlers),
                        "limit": limit,
                        "worker_id": worker_id,
                        "lease_seconds": lease_seconds,
                    },
                )
            ).all()
    claimed = [
        ClaimedTask(
            task_id=r.id,
            run_id=r.run_id,
            task_key=r.task_key,
            handler=r.handler,
            attempt_id=r.current_attempt_id,
            attempt_number=r.attempt_count,
            attempt_limit=r.attempt_limit,
            timeout_seconds=r.timeout_seconds,
            retry_policy=RetryPolicy.model_validate(r.retry_policy),
            input=r.input_json,
            run_input=r.run_input,
            dependency_outputs=r.dependency_outputs,
            lease_expires_at=r.lease_expires_at,
            queue_wait_seconds=r.queue_wait,
            traceparent=r.traceparent,
        )
        for r in rows
    ]
    metrics.TASKS_CLAIMED.inc(len(claimed))
    for c in claimed:
        metrics.TASK_QUEUE_WAIT.observe(c.queue_wait_seconds)
    return claimed


async def renew_leases(
    engine: AsyncEngine,
    *,
    worker_id: str,
    attempt_ids: Sequence[uuid.UUID],
    lease_seconds: float,
) -> LeaseRenewal:
    """Extend leases for attempts this worker still owns; also refreshes the worker heartbeat.

    Also reports which attempts belong to runs being cancelled, so the worker can
    signal those handlers (cancellation never writes to RUNNING task rows; see the
    lock-order note at the top of this module).
    """
    with metrics.time_db_operation("renew_leases"):
        async with engine.begin() as conn:
            await conn.execute(
                update(workers_t)
                .where(workers_t.c.worker_id == worker_id)
                .values(last_heartbeat_at=text("statement_timestamp()"))
            )
            if not attempt_ids:
                return LeaseRenewal()
            rows = (
                await conn.execute(
                    _RENEW_SQL,
                    {
                        "attempt_ids": list(attempt_ids),
                        "worker_id": worker_id,
                        "lease_seconds": lease_seconds,
                    },
                )
            ).all()
    return LeaseRenewal(
        renewed={r.current_attempt_id for r in rows},
        cancel_requested={r.current_attempt_id for r in rows if r.cancel_requested},
    )


def plan_result(
    result: TaskResult, *, attempt_number: int, attempt_limit: int, policy: RetryPolicy
) -> tuple[TaskStatus, AttemptOutcome, FailureInfo | None, float | None]:
    """Decide the next task state for a reported result (cancellation applied later)."""
    match result:
        case Succeeded():
            return TaskStatus.SUCCEEDED, AttemptOutcome.SUCCEEDED, None, None
        case Cancelled(reason=reason):
            info = FailureInfo("Cancelled", reason, retryable=False)
            return TaskStatus.CANCELLED, AttemptOutcome.CANCELLED, info, None
        case Failed(failure=f):
            outcome = AttemptOutcome.TIMED_OUT if f.timed_out else AttemptOutcome.FAILED
            if f.retryable and attempt_number < attempt_limit:
                delay = compute_retry_delay(
                    policy, attempt_number, retry_after_seconds=f.retry_after_seconds
                )
                status = TaskStatus.RETRY_WAIT if delay > 0 else TaskStatus.READY
                return status, outcome, f, delay
            return TaskStatus.FAILED, outcome, f, None
    raise TypeError(f"unknown task result {result!r}")


async def finish_attempt(
    conn: AsyncConnection,
    *,
    task_id: uuid.UUID,
    attempt_id: uuid.UUID,
    task_status: TaskStatus,
    outcome: AttemptOutcome,
    failure: FailureInfo | None,
    retry_delay: float | None,
    output: Any,
    duration_ms: int | None,
    event_type: str,
    event_data: dict[str, Any],
    require_expired: bool = False,
) -> uuid.UUID | None:
    """Run the fenced transition. Returns the run id, or None if the fence rejected it."""
    row = (
        await conn.execute(
            FINISH_ATTEMPT_SQL,
            {
                "task_id": task_id,
                "attempt_id": attempt_id,
                "task_status": task_status.value,
                "output": json.dumps(output) if task_status is TaskStatus.SUCCEEDED else None,
                "error_type": failure.error_type if failure else None,
                "error_message": failure.message if failure else None,
                "retryable": failure.retryable if failure else None,
                "retry_delay": retry_delay,
                "outcome": outcome.value,
                "duration_ms": duration_ms,
                "event_type": event_type,
                "event_data": json.dumps(event_data),
                "require_expired": require_expired,
            },
        )
    ).first()
    return None if row is None else row.run_id


async def settle_run(
    conn: AsyncConnection, run_id: uuid.UUID, task_id: uuid.UUID, task_status: TaskStatus
) -> tuple[TaskStatus, str]:
    """After a task left RUNNING: lock the run, apply cancellation, advance the run.

    Returns the task's final status (a pending retry becomes CANCELLED if the run
    is being cancelled) and the run status.
    """
    run = await lock_run(conn, run_id)
    assert run is not None  # tasks cascade-delete with their run
    if run["status"] == RunStatus.CANCELLING and task_status in (
        TaskStatus.RETRY_WAIT,
        TaskStatus.READY,
    ):
        # Cancellation committed after this task's state was decided: the
        # cancelling transaction skipped this row (it was still RUNNING), so
        # cancel the pending retry here, under the run lock.
        await conn.execute(_CANCEL_FIXUP_SQL, {"task_id": task_id})
        task_status = TaskStatus.CANCELLED
    if run["status"] == RunStatus.PENDING:
        # The claim skipped activation because the run row was locked at the time.
        await set_run_status(conn, run, RunStatus.RUNNING, started_at=text("statement_timestamp()"))
        run = {**run, "status": RunStatus.RUNNING.value}
    if task_status in (TaskStatus.RETRY_WAIT, TaskStatus.READY):
        return task_status, str(run["status"])  # task still active: nothing to advance
    if task_status is TaskStatus.SUCCEEDED and run["status"] == RunStatus.RUNNING:
        return task_status, (await advance_after_success(conn, run, task_id)).value
    return task_status, (await advance_run(conn, run)).value


async def complete_attempt(
    engine: AsyncEngine,
    *,
    claimed: ClaimedTask,
    worker_id: str,
    result: TaskResult,
    duration_ms: int,
) -> CompletionOutcome:
    """Persist an attempt's result if, and only if, the attempt still holds the lease."""
    task_status, outcome, failure, retry_delay = plan_result(
        result,
        attempt_number=claimed.attempt_number,
        attempt_limit=claimed.attempt_limit,
        policy=claimed.retry_policy,
    )
    output = result.output if isinstance(result, Succeeded) else None
    with metrics.time_db_operation("complete_attempt"):
        async with engine.begin() as conn:
            run_id = await finish_attempt(
                conn,
                task_id=claimed.task_id,
                attempt_id=claimed.attempt_id,
                task_status=task_status,
                outcome=outcome,
                failure=failure,
                retry_delay=retry_delay,
                output=output,
                duration_ms=duration_ms,
                event_type="attempt_finished",
                event_data={
                    "task_key": claimed.task_key,
                    "attempt_number": claimed.attempt_number,
                    "outcome": outcome.value,
                    "task_status": task_status.value,
                    "error_type": failure.error_type if failure else None,
                    "retry_delay_seconds": retry_delay,
                    "duration_ms": duration_ms,
                },
            )
            if run_id is not None:
                task_status, run_status = await settle_run(
                    conn, run_id, claimed.task_id, task_status
                )
            else:
                # A retry after an ambiguous commit (committed, but the ack was
                # lost) finds its own attempt already finished. Only the owning
                # worker writes these outcomes; LEASE_EXPIRED is the recovery path.
                prior = (
                    await conn.execute(
                        select(attempts_t.c.outcome).where(attempts_t.c.id == claimed.attempt_id)
                    )
                ).scalar_one_or_none()
                if prior in _OWNER_OUTCOMES:
                    return CompletionOutcome(accepted=True, reason="already_recorded")
    if run_id is None:
        await _record_stale_result(engine, claimed, worker_id, result)
        return CompletionOutcome(accepted=False, reason="lease_lost")

    metrics.TASKS_COMPLETED.labels(outcome.value).inc()
    if retry_delay is not None and task_status is not TaskStatus.CANCELLED:
        metrics.TASK_RETRIES.inc()
    log_event(
        logger,
        logging.INFO,
        "attempt finished",
        event_type="attempt_finished",
        outcome=outcome.value,
        task_status=task_status.value,
        duration_ms=duration_ms,
        error_type=failure.error_type if failure else None,
    )
    return CompletionOutcome(
        accepted=True,
        task_status=task_status.value,
        run_status=run_status,
        retry_delay_seconds=retry_delay if task_status is not TaskStatus.CANCELLED else None,
    )


async def _record_stale_result(
    engine: AsyncEngine, claimed: ClaimedTask, worker_id: str, result: TaskResult
) -> None:
    """Keep evidence of what a worker that lost its lease reported.

    The result itself is discarded, but recording it matters: a stale "success"
    is evidence that the handler's side effects probably happened.
    """
    metrics.STALE_RESULTS_REJECTED.inc()
    reported = type(result).__name__
    log_event(
        logger,
        logging.WARNING,
        "stale result rejected: lease no longer held",
        event_type="stale_result_rejected",
        reported=reported,
    )
    try:
        await _append_stale_event(engine, claimed, worker_id, reported)
    except IntegrityError:
        # The run was deleted by retention in the meantime: nothing left to annotate.
        logger.info("stale result for a run that no longer exists; not recorded")


async def _append_stale_event(
    engine: AsyncEngine, claimed: ClaimedTask, worker_id: str, reported: str
) -> None:
    async with engine.begin() as conn:
        await append_events(
            conn,
            [
                {
                    "run_id": claimed.run_id,
                    "task_id": claimed.task_id,
                    "attempt_id": claimed.attempt_id,
                    "event_type": "stale_result_rejected",
                    "data": {
                        "task_key": claimed.task_key,
                        "attempt_number": claimed.attempt_number,
                        "worker_id": worker_id,
                        "reported": reported,
                    },
                }
            ],
        )
