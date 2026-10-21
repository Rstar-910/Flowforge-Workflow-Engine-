"""Run progression: dependency promotion, failure propagation and run finalisation.

All functions here must be called inside a transaction that already holds the
run row lock (``sql.lock_run``). Because every terminal task transition in a run
takes that lock first, two tasks finishing concurrently cannot both miss the
fact that a shared dependent is now unblocked (the classic write-skew between
sibling completions) and cannot both try to finalise the run.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Mapping
from typing import Any

from sqlalchemy import text, update
from sqlalchemy.ext.asyncio import AsyncConnection

from flowforge.domain.states import (
    ACTIVE_TASK_STATUSES,
    RunStatus,
    TaskStatus,
    check_run_transition,
)
from flowforge.engine.sql import append_events, runs_t
from flowforge.observability import metrics
from flowforge.observability.logging import log_event

logger = logging.getLogger(__name__)

_PROMOTE_SQL = text(
    """
    UPDATE tasks t
       SET status = 'READY', ready_at = statement_timestamp(), updated_at = statement_timestamp()
     WHERE t.run_id = :run_id
       AND t.status = 'BLOCKED'
       AND NOT EXISTS (
             SELECT 1
               FROM task_dependencies d
               JOIN tasks p ON p.id = d.depends_on_task_id
              WHERE d.task_id = t.id AND p.status <> 'SUCCEEDED')
    RETURNING t.id, t.task_key
    """
)

_PROPAGATE_SQL = text(
    """
    UPDATE tasks t
       SET status = 'UPSTREAM_FAILED',
           completed_at = statement_timestamp(),
           updated_at = statement_timestamp(),
           last_error_type = 'UpstreamFailed',
           last_error_message = 'dependency ' || bad.task_key || ' ended in ' || bad.status
      FROM (
            SELECT DISTINCT ON (d.task_id) d.task_id, p.task_key, p.status
              FROM task_dependencies d
              JOIN tasks p ON p.id = d.depends_on_task_id
             WHERE p.run_id = :run_id
               AND p.status IN ('FAILED', 'UPSTREAM_FAILED', 'CANCELLED')
             ORDER BY d.task_id, p.task_key
           ) bad
     WHERE t.id = bad.task_id AND t.status = 'BLOCKED'
    RETURNING t.id, t.task_key, bad.task_key AS upstream
    """
)

_COUNTS_SQL = text("SELECT status, count(*) FROM tasks WHERE run_id = :run_id GROUP BY status")

_SINK_OUTPUTS_SQL = text(
    """
    SELECT t.task_key, t.output_json
      FROM tasks t
     WHERE t.run_id = :run_id
       AND NOT EXISTS (SELECT 1 FROM task_dependencies d WHERE d.depends_on_task_id = t.id)
     ORDER BY t.task_key
    """
)

_FAILED_KEYS_SQL = text(
    "SELECT task_key FROM tasks WHERE run_id = :run_id AND status = 'FAILED' ORDER BY task_key"
)


async def propagate_upstream_failures(conn: AsyncConnection, run_id: uuid.UUID) -> int:
    """Mark BLOCKED tasks that can never run (a dependency failed) as UPSTREAM_FAILED."""
    total = 0
    while True:  # one level of the DAG per iteration; depth is bounded by MAX_TASKS
        rows = (await conn.execute(_PROPAGATE_SQL, {"run_id": run_id})).all()
        if not rows:
            return total
        total += len(rows)
        await append_events(
            conn,
            [
                {
                    "run_id": run_id,
                    "task_id": r.id,
                    "event_type": "task_upstream_failed",
                    "data": {"task_key": r.task_key, "upstream": r.upstream},
                }
                for r in rows
            ],
        )


async def promote_ready_tasks(conn: AsyncConnection, run_id: uuid.UUID) -> int:
    rows = (await conn.execute(_PROMOTE_SQL, {"run_id": run_id})).all()
    await append_events(
        conn,
        [
            {
                "run_id": run_id,
                "task_id": r.id,
                "event_type": "task_ready",
                "data": {"task_key": r.task_key, "reason": "dependencies_succeeded"},
            }
            for r in rows
        ],
    )
    return len(rows)


async def set_run_status(
    conn: AsyncConnection,
    run: Mapping[str, Any],
    new_status: RunStatus,
    **values: Any,
) -> None:
    current = RunStatus(run["status"])
    check_run_transition(current, new_status)
    await conn.execute(
        update(runs_t)
        .where(runs_t.c.id == run["id"])
        .values(status=new_status.value, updated_at=text("statement_timestamp()"), **values)
    )
    await append_events(
        conn,
        [
            {
                "run_id": run["id"],
                "event_type": "run_status_changed",
                "data": {"from": current.value, "to": new_status.value},
            }
        ],
    )
    if new_status in (RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED):
        metrics.RUNS_COMPLETED.labels(new_status.value).inc()
        log_event(
            logger,
            logging.INFO,
            "workflow run finished",
            event_type="run_finished",
            workflow_run_id=str(run["id"]),
            outcome=new_status.value,
        )


async def finalize_run_if_done(conn: AsyncConnection, run: Mapping[str, Any]) -> RunStatus:
    status = RunStatus(run["status"])
    run_id = run["id"]
    counts = {
        TaskStatus(s): n for s, n in (await conn.execute(_COUNTS_SQL, {"run_id": run_id})).all()
    }
    active = sum(n for s, n in counts.items() if s in ACTIVE_TASK_STATUSES)

    if status is RunStatus.CANCELLING:
        if counts.get(TaskStatus.RUNNING, 0) == 0:
            if run.get("deadline_exceeded_at") is not None:
                # Stopped by the workflow deadline, not by a user: the run failed.
                await set_run_status(
                    conn,
                    run,
                    RunStatus.FAILED,
                    completed_at=text("statement_timestamp()"),
                    started_at=run["started_at"] or text("statement_timestamp()"),
                )
                return RunStatus.FAILED
            await set_run_status(
                conn, run, RunStatus.CANCELLED, completed_at=text("statement_timestamp()")
            )
            return RunStatus.CANCELLED
        return status

    if status not in (RunStatus.PENDING, RunStatus.RUNNING) or active > 0:
        return status

    total = sum(counts.values())
    if counts.get(TaskStatus.SUCCEEDED, 0) == total:
        outputs = {
            r.task_key: r.output_json
            for r in (await conn.execute(_SINK_OUTPUTS_SQL, {"run_id": run_id})).all()
        }
        await set_run_status(
            conn,
            run,
            RunStatus.SUCCEEDED,
            completed_at=text("statement_timestamp()"),
            output_json=outputs,
            started_at=run["started_at"] or text("statement_timestamp()"),
        )
        return RunStatus.SUCCEEDED

    failed = [r.task_key for r in (await conn.execute(_FAILED_KEYS_SQL, {"run_id": run_id})).all()]
    reason = "tasks failed: " + ", ".join(failed) if failed else "no runnable tasks remain"
    await set_run_status(
        conn,
        run,
        RunStatus.FAILED,
        completed_at=text("statement_timestamp()"),
        failure_reason=reason,
        started_at=run["started_at"] or text("statement_timestamp()"),
    )
    return RunStatus.FAILED


_PROMOTE_CHILDREN_SQL = text(
    """
    WITH promoted AS (
        UPDATE tasks c
           SET status = 'READY', ready_at = statement_timestamp(),
               updated_at = statement_timestamp()
         -- id = ANY(ARRAY(...)) pins the plan to primary-key lookups driven by
         -- the (few) dependency edges, independent of planner statistics; with
         -- a plain join/IN, a never-analyzed table got a 60-100x slower plan
         -- inside the run lock (ADR 0012).
         WHERE c.id = ANY(ARRAY(
                 SELECT task_id FROM task_dependencies WHERE depends_on_task_id = :done))
           AND c.status = 'BLOCKED'
           AND NOT EXISTS (
                 SELECT 1
                   FROM tasks p
                  WHERE p.id = ANY(ARRAY(
                          SELECT d.depends_on_task_id FROM task_dependencies d
                           WHERE d.task_id = c.id))
                    AND p.status <> 'SUCCEEDED')
        RETURNING c.id, c.task_key
    ), ev AS (
        INSERT INTO task_events (run_id, task_id, event_type, data)
        SELECT :run_id, id, 'task_ready',
               jsonb_build_object('task_key', task_key, 'reason', 'dependencies_succeeded')
          FROM promoted
    )
    SELECT EXISTS (
        SELECT 1 FROM tasks
         WHERE run_id = :run_id AND status IN ('BLOCKED', 'READY', 'RUNNING', 'RETRY_WAIT')
    ) AS active
    """
)


async def advance_after_success(
    conn: AsyncConnection, run: Mapping[str, Any], succeeded_task_id: Any
) -> RunStatus:
    """Fast path after a task SUCCEEDED in a RUNNING run (caller holds the run lock).

    A success cannot cause failure propagation and can only unblock the task's
    direct children, so one statement promotes them and reports whether the run
    still has active tasks; the full finalisation runs only when it has none.
    The statement starts after the run lock was granted, so (READ COMMITTED) it
    sees every sibling completion that committed before us.
    """
    active = (
        await conn.execute(_PROMOTE_CHILDREN_SQL, {"done": succeeded_task_id, "run_id": run["id"]})
    ).scalar_one()
    if active:
        return RunStatus(run["status"])
    return await finalize_run_if_done(conn, run)


async def advance_run(conn: AsyncConnection, run: Mapping[str, Any]) -> RunStatus:
    """Bring a locked run's dependent state up to date after any task state change."""
    if run["status"] in (RunStatus.PENDING, RunStatus.RUNNING):
        await propagate_upstream_failures(conn, run["id"])
        await promote_ready_tasks(conn, run["id"])
    return await finalize_run_if_done(conn, run)
