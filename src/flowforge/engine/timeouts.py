"""Workflow deadlines and task queue timeouts (spec section 11), enforced by the scheduler.

* **Workflow deadline**: a run still PENDING/RUNNING after ``deadline_at`` is
  stopped exactly like a cancellation (queued work cancelled at once, running
  handlers signalled on their next heartbeat) and finalises as FAILED with
  ``failure_reason = "workflow deadline exceeded"``.
* **Queue timeout**: a task that stayed READY (unclaimed) longer than its
  ``queue_timeout_seconds`` fails with ``QueueTimeout`` without any attempt;
  normal failure propagation follows. The clock restarts whenever the task
  becomes READY again (time in RETRY_WAIT does not count).

Both follow the engine lock order: run row first, then non-running task rows.
A claim racing a queue timeout is settled by the conditional UPDATE: if the
claim committed first, the row is no longer READY and nothing happens; if the
timeout committed first, the claim's SKIP LOCKED pick never sees the task.
"""

from __future__ import annotations

import logging
import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from flowforge.domain.states import RunStatus
from flowforge.engine.control import begin_stopping
from flowforge.engine.progression import advance_run
from flowforge.engine.sql import append_events, lock_run
from flowforge.observability import metrics
from flowforge.observability.logging import log_event

logger = logging.getLogger(__name__)

_OVERDUE_RUNS_SQL = text(
    """
    SELECT id FROM workflow_runs
     WHERE status IN ('PENDING', 'RUNNING')
       AND deadline_at IS NOT NULL
       AND deadline_at < statement_timestamp()
     ORDER BY deadline_at
     LIMIT :limit
    """
)

_QUEUE_EXPIRED_SQL = text(
    """
    SELECT id, run_id FROM tasks
     WHERE status = 'READY'
       AND queue_timeout_seconds IS NOT NULL
       AND ready_at < statement_timestamp() - make_interval(secs => queue_timeout_seconds)
     ORDER BY ready_at
     LIMIT :limit
    """
)

_FAIL_QUEUED_SQL = text(
    """
    UPDATE tasks
       SET status = 'FAILED',
           completed_at = statement_timestamp(),
           updated_at = statement_timestamp(),
           last_error_type = 'QueueTimeout',
           last_error_message = 'not claimed by any worker within '
                                || queue_timeout_seconds || ' s (no capacity, or no worker '
                                || 'with handler ' || handler || ')'
     WHERE id = :task_id
       AND status = 'READY'
       AND ready_at < statement_timestamp() - make_interval(secs => queue_timeout_seconds)
    RETURNING task_key, queue_timeout_seconds,
              extract(epoch FROM (statement_timestamp() - ready_at))::float8 AS waited
    """
)


async def enforce_run_deadlines(engine: AsyncEngine, *, limit: int) -> int:
    async with engine.connect() as conn:
        run_ids = [r.id for r in await conn.execute(_OVERDUE_RUNS_SQL, {"limit": limit})]
    stopped = 0
    for run_id in run_ids:
        if await _stop_overdue_run(engine, run_id):
            stopped += 1
    return stopped


async def _stop_overdue_run(engine: AsyncEngine, run_id: uuid.UUID) -> bool:
    async with engine.begin() as conn:
        run = await lock_run(conn, run_id)
        if (
            run is None
            or run["status"] not in (RunStatus.PENDING, RunStatus.RUNNING)
            or run["deadline_at"] is None
        ):
            return False
        overdue = (
            await conn.execute(text("SELECT :d < statement_timestamp()"), {"d": run["deadline_at"]})
        ).scalar_one()
        if not overdue:
            return False
        final = await begin_stopping(conn, run, deadline=True)
    metrics.DEADLINES_EXCEEDED.labels("run").inc()
    log_event(
        logger,
        logging.WARNING,
        "workflow deadline exceeded; stopping run",
        event_type="run_deadline_exceeded",
        workflow_run_id=str(run_id),
        run_status=final.value,
    )
    return True


async def fail_queue_timeouts(engine: AsyncEngine, *, limit: int) -> int:
    async with engine.connect() as conn:
        candidates = (await conn.execute(_QUEUE_EXPIRED_SQL, {"limit": limit})).all()
    failed = 0
    for cand in candidates:
        if await _fail_queued_task(engine, cand.id, cand.run_id):
            failed += 1
    return failed


async def _fail_queued_task(engine: AsyncEngine, task_id: uuid.UUID, run_id: uuid.UUID) -> bool:
    async with engine.begin() as conn:
        run = await lock_run(conn, run_id)
        if run is None or run["status"] not in (RunStatus.PENDING, RunStatus.RUNNING):
            return False
        row = (await conn.execute(_FAIL_QUEUED_SQL, {"task_id": task_id})).first()
        if row is None:  # claimed (or otherwise moved on) in the meantime
            return False
        await append_events(
            conn,
            [
                {
                    "run_id": run_id,
                    "task_id": task_id,
                    "event_type": "task_queue_timeout",
                    "data": {
                        "task_key": row.task_key,
                        "queue_timeout_seconds": row.queue_timeout_seconds,
                        "waited_seconds": round(row.waited, 3),
                    },
                }
            ],
        )
        await advance_run(conn, run)
    metrics.DEADLINES_EXCEEDED.labels("queue").inc()
    log_event(
        logger,
        logging.WARNING,
        "task queue timeout",
        event_type="task_queue_timeout",
        workflow_run_id=str(run_id),
        task_id=str(task_id),
        task_key=row.task_key,
    )
    return True
