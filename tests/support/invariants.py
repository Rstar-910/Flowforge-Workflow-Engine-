"""Global consistency invariants, checked directly against PostgreSQL.

Race and failure tests call ``assert_invariants(db)`` after the system has
settled. Each query returns offending rows; an empty result means the invariant
holds. The invariants are deliberately stronger than any single test's
assertions: they catch damage to *other* rows that a test did not look at.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

INVARIANTS: dict[str, str] = {
    # Work that could still be claimed or promoted must belong to a live run.
    "runnable task in a stopping or finished run": """
        SELECT t.id, t.status, r.status AS run_status FROM tasks t
          JOIN workflow_runs r ON r.id = t.run_id
         WHERE t.status IN ('READY', 'RETRY_WAIT', 'BLOCKED')
           AND r.status IN ('CANCELLING', 'CANCELLED', 'SUCCEEDED', 'FAILED')""",
    "RUNNING task in a finished run": """
        SELECT t.id FROM tasks t JOIN workflow_runs r ON r.id = t.run_id
         WHERE t.status = 'RUNNING' AND r.status IN ('CANCELLED', 'SUCCEEDED', 'FAILED')""",
    # Exactly one open attempt per RUNNING task, and it is the task's current lease token.
    "RUNNING task without exactly one matching RUNNING attempt": """
        SELECT t.id FROM tasks t
         WHERE t.status = 'RUNNING'
           AND (SELECT count(*) FROM task_attempts a
                 WHERE a.task_id = t.id AND a.outcome = 'RUNNING'
                   AND a.id = t.current_attempt_id AND a.attempt_number = t.attempt_count) <> 1""",
    "open attempt on a task that is not RUNNING it": """
        SELECT a.id FROM task_attempts a JOIN tasks t ON t.id = a.task_id
         WHERE a.outcome = 'RUNNING'
           AND (t.status <> 'RUNNING' OR t.current_attempt_id IS DISTINCT FROM a.id)""",
    # Attempt numbering is the fencing generation: dense, 1..attempt_count.
    "attempt numbers not dense 1..attempt_count": """
        SELECT t.id FROM tasks t
         WHERE t.attempt_count <> (SELECT count(*) FROM task_attempts a WHERE a.task_id = t.id)
            OR EXISTS (SELECT 1 FROM task_attempts a WHERE a.task_id = t.id
                        AND (a.attempt_number < 1 OR a.attempt_number > t.attempt_count))""",
    "more than one accepted success for a task": """
        SELECT task_id FROM task_attempts WHERE outcome = 'SUCCEEDED'
         GROUP BY task_id HAVING count(*) > 1""",
    "SUCCEEDED task whose last attempt did not succeed": """
        SELECT t.id FROM tasks t
         WHERE t.status = 'SUCCEEDED' AND t.attempt_count > 0
           AND NOT EXISTS (SELECT 1 FROM task_attempts a WHERE a.task_id = t.id
                            AND a.attempt_number = t.attempt_count AND a.outcome = 'SUCCEEDED')""",
    "terminal run with active tasks": """
        SELECT r.id FROM workflow_runs r
         WHERE r.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
           AND EXISTS (SELECT 1 FROM tasks t WHERE t.run_id = r.id
                        AND t.status IN ('BLOCKED', 'READY', 'RUNNING', 'RETRY_WAIT'))""",
    "SUCCEEDED run with a task that did not succeed": """
        SELECT r.id FROM workflow_runs r
         WHERE r.status = 'SUCCEEDED'
           AND EXISTS (SELECT 1 FROM tasks t WHERE t.run_id = r.id AND t.status <> 'SUCCEEDED')""",
    "READY task without ready_at (queue age unknown)": """
        SELECT id FROM tasks WHERE status = 'READY' AND ready_at IS NULL""",
    "task READY/RUNNING although a dependency has not succeeded": """
        SELECT t.id FROM tasks t
          JOIN task_dependencies d ON d.task_id = t.id
          JOIN tasks p ON p.id = d.depends_on_task_id
         WHERE t.status IN ('READY', 'RUNNING', 'SUCCEEDED') AND p.status <> 'SUCCEEDED'""",
    "deadline-failed run not marked FAILED": """
        SELECT id FROM workflow_runs
         WHERE deadline_exceeded_at IS NOT NULL AND status IN ('CANCELLED', 'SUCCEEDED')""",
}


async def violations(db: AsyncEngine) -> dict[str, list[tuple[object, ...]]]:
    found: dict[str, list[tuple[object, ...]]] = {}
    async with db.connect() as conn:
        for name, sql in INVARIANTS.items():
            rows = [tuple(r) for r in (await conn.execute(text(sql))).all()]
            if rows:
                found[name] = rows[:5]
    return found


async def assert_invariants(db: AsyncEngine) -> None:
    found = await violations(db)
    assert not found, "invariant violations:\n" + "\n".join(
        f"  - {name}: {rows}" for name, rows in found.items()
    )
