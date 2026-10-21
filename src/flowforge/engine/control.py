"""Operator controls: run cancellation and authorised manual retry."""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import and_, select, text, update
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from flowforge.domain.retry import RetryPolicy
from flowforge.domain.states import TERMINAL_RUN_STATUSES, RunStatus, TaskStatus
from flowforge.engine.progression import advance_run, set_run_status
from flowforge.engine.sql import append_events, lock_run, runs_t, tasks_t
from flowforge.engine.submission import ConflictError, NotFoundError

_CANCEL_PENDING_SQL = text(
    """
    UPDATE tasks
       SET status = 'CANCELLED',
           completed_at = statement_timestamp(),
           updated_at = statement_timestamp()
     WHERE run_id = :run_id AND status IN ('BLOCKED', 'READY', 'RETRY_WAIT')
    RETURNING id, task_key
    """
)
_RUNNING_TASKS_SQL = text(
    "SELECT id, task_key FROM tasks WHERE run_id = :run_id AND status = 'RUNNING'"
)
_REBLOCK_SQL = text(
    """
    UPDATE tasks
       SET status = 'BLOCKED', completed_at = NULL, updated_at = statement_timestamp()
     WHERE run_id = :run_id AND status = 'UPSTREAM_FAILED'
    RETURNING id
    """
)


async def cancel_run(engine: AsyncEngine, *, tenant_id: str, run_id: uuid.UUID) -> str:
    """Request cancellation.

    Not-yet-running tasks are cancelled immediately and atomically. RUNNING
    tasks are not written to (that would invert the engine's lock order, see
    engine/execution.py): their workers learn that the run is CANCELLING on the
    next heartbeat and signal the handler cooperatively. The run stays
    CANCELLING until no task is RUNNING (handler stopped, finished, timed out,
    or its lease expired).
    """
    async with engine.begin() as conn:
        run = await lock_run(conn, run_id)
        if run is None or run["tenant_id"] != tenant_id:
            raise NotFoundError("run not found")
        status = RunStatus(run["status"])
        if status is RunStatus.CANCELLING:
            return status.value
        if status in TERMINAL_RUN_STATUSES:
            raise ConflictError(f"run is already {status.value}")
        return (await begin_stopping(conn, run, deadline=False)).value


async def begin_stopping(conn: AsyncConnection, run: Any, *, deadline: bool) -> RunStatus:
    """Move a locked PENDING/RUNNING run to CANCELLING and stop its queued work.

    Shared by user cancellation and the workflow deadline. With ``deadline=True``
    the run records ``deadline_exceeded_at`` and finalises as FAILED (reason
    "deadline exceeded") instead of CANCELLED.
    """
    run_id = run["id"]
    extra: dict[str, Any] = {"cancellation_requested_at": text("statement_timestamp()")}
    if deadline:
        extra["deadline_exceeded_at"] = text("statement_timestamp()")
        extra["failure_reason"] = "workflow deadline exceeded"
    await set_run_status(conn, run, RunStatus.CANCELLING, **extra)
    cancelled = (await conn.execute(_CANCEL_PENDING_SQL, {"run_id": run_id})).all()
    requested = (await conn.execute(_RUNNING_TASKS_SQL, {"run_id": run_id})).all()
    reason = {"reason": "deadline_exceeded"} if deadline else {}
    await append_events(
        conn,
        (
            [
                {
                    "run_id": run_id,
                    "event_type": "run_deadline_exceeded",
                    "data": {"deadline_at": str(run["deadline_at"])},
                }
            ]
            if deadline
            else []
        )
        + [
            {
                "run_id": run_id,
                "task_id": r.id,
                "event_type": "task_cancelled",
                "data": {"task_key": r.task_key, **reason},
            }
            for r in cancelled
        ]
        + [
            {
                "run_id": run_id,
                "task_id": r.id,
                "event_type": "task_cancel_requested",
                "data": {"task_key": r.task_key, **reason},
            }
            for r in requested
        ],
    )
    stopping = {
        **run,
        "status": RunStatus.CANCELLING.value,
        "deadline_exceeded_at": True if deadline else run.get("deadline_exceeded_at"),
    }
    return await advance_run(conn, stopping)


async def retry_failed_task(
    engine: AsyncEngine, *, tenant_id: str, task_id: uuid.UUID, requested_by: str
) -> str:
    """Re-open a terminally FAILED task with a fresh attempt budget.

    Its UPSTREAM_FAILED descendants become BLOCKED again (and are re-marked if
    some *other* failed dependency still blocks them). Attempt history is kept;
    attempt numbers keep increasing so fencing tokens stay monotonic.
    """
    async with engine.begin() as conn:
        task_row = (
            await conn.execute(
                select(tasks_t.c.run_id)
                .join(runs_t, runs_t.c.id == tasks_t.c.run_id)
                .where(and_(tasks_t.c.id == task_id, runs_t.c.tenant_id == tenant_id))
            )
        ).first()
        if task_row is None:
            raise NotFoundError("task not found")
        run = await lock_run(conn, task_row.run_id)
        assert run is not None
        task = (
            (await conn.execute(select(tasks_t).where(tasks_t.c.id == task_id).with_for_update()))
            .mappings()
            .one()
        )
        if task["status"] != TaskStatus.FAILED:
            raise ConflictError(f"only FAILED tasks can be retried (task is {task['status']})")
        if run["status"] not in (RunStatus.RUNNING, RunStatus.FAILED):
            raise ConflictError(f"cannot retry a task of a {run['status']} run")
        if run["deadline_exceeded_at"] is not None:
            raise ConflictError(
                "the run failed its workflow deadline; start a new run instead of retrying"
            )
        policy = RetryPolicy.model_validate(task["retry_policy"])
        await conn.execute(
            update(tasks_t)
            .where(tasks_t.c.id == task_id)
            .values(
                status=TaskStatus.READY.value,
                ready_at=text("statement_timestamp()"),
                completed_at=None,
                next_attempt_at=None,
                attempt_limit=task["attempt_count"] + policy.max_attempts,
                updated_at=text("statement_timestamp()"),
            )
        )
        await conn.execute(_REBLOCK_SQL, {"run_id": run["id"]})
        await append_events(
            conn,
            [
                {
                    "run_id": run["id"],
                    "task_id": task_id,
                    "event_type": "task_manual_retry",
                    "data": {
                        "task_key": task["task_key"],
                        "requested_by": requested_by,
                        "new_attempt_limit": task["attempt_count"] + policy.max_attempts,
                    },
                }
            ],
        )
        if run["status"] == RunStatus.FAILED:
            await set_run_status(
                conn,
                run,
                RunStatus.RUNNING,
                completed_at=None,
                failure_reason=None,
                events_pruned_at=None,  # new events get normal retention again
            )
            run = {**run, "status": RunStatus.RUNNING.value}
        await advance_run(conn, run)
        return TaskStatus.READY.value
