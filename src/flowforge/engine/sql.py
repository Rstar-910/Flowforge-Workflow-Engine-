"""Shared SQL helpers: table handles, events, run locking."""

from __future__ import annotations

import uuid
from typing import Any, cast

from sqlalchemy import Table, insert, select, text
from sqlalchemy.ext.asyncio import AsyncConnection

from flowforge.db import models

runs_t = cast(Table, models.WorkflowRun.__table__)
tasks_t = cast(Table, models.Task.__table__)
deps_t = cast(Table, models.TaskDependency.__table__)
attempts_t = cast(Table, models.TaskAttempt.__table__)
events_t = cast(Table, models.TaskEvent.__table__)
defs_t = cast(Table, models.WorkflowDefinition.__table__)
idem_t = cast(Table, models.IdempotencyRecord.__table__)
workers_t = cast(Table, models.Worker.__table__)


async def append_events(conn: AsyncConnection, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    normalized = [
        {
            "run_id": r["run_id"],
            "task_id": r.get("task_id"),
            "attempt_id": r.get("attempt_id"),
            "event_type": r["event_type"],
            "data": r.get("data") or {},
        }
        for r in rows
    ]
    await conn.execute(insert(events_t), normalized)


async def lock_run(conn: AsyncConnection, run_id: uuid.UUID) -> Any | None:
    """Lock a run row. Every task terminal transition in a run happens under this lock.

    FOR NO KEY UPDATE (not FOR UPDATE): it is mutually exclusive with itself,
    but compatible with the FOR KEY SHARE locks that foreign-key checks take
    when a transaction inserts events/attempts referencing the run. With plain
    FOR UPDATE, two sibling completions that each inserted an event (KEY SHARE)
    and then requested the run lock would deadlock (found by
    test_fan_in_promotion_has_no_write_skew).
    """
    result = await conn.execute(
        select(runs_t).where(runs_t.c.id == run_id).with_for_update(key_share=True)
    )
    return result.mappings().first()


async def db_now(conn: AsyncConnection) -> Any:
    return (await conn.execute(text("SELECT statement_timestamp()"))).scalar_one()
