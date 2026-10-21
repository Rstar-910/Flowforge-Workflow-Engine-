"""Read-only queries backing the inspection API. Every query is tenant-scoped."""

from __future__ import annotations

import base64
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import and_, func, or_, select, text, tuple_
from sqlalchemy.ext.asyncio import AsyncEngine

from flowforge.engine.sql import attempts_t, defs_t, deps_t, events_t, runs_t, tasks_t


class InvalidCursorError(ValueError):
    pass


def encode_cursor(ts: datetime, ident: uuid.UUID) -> str:
    return base64.urlsafe_b64encode(f"{ts.isoformat()}|{ident}".encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        ts, ident = base64.urlsafe_b64decode(cursor.encode()).decode().split("|")
        return datetime.fromisoformat(ts), uuid.UUID(ident)
    except Exception as exc:
        raise InvalidCursorError("invalid cursor") from exc


async def list_definitions(
    engine: AsyncEngine, tenant_id: str, *, limit: int, cursor: str | None, name: str | None
) -> tuple[list[dict[str, Any]], str | None]:
    q = select(defs_t).where(defs_t.c.tenant_id == tenant_id)
    if name:
        q = q.where(defs_t.c.name == name)
    if cursor:
        ts, ident = decode_cursor(cursor)
        q = q.where(tuple_(defs_t.c.created_at, defs_t.c.id) < tuple_(ts, ident))
    q = q.order_by(defs_t.c.created_at.desc(), defs_t.c.id.desc()).limit(limit + 1)
    async with engine.connect() as conn:
        rows = [dict(r) for r in (await conn.execute(q)).mappings()]
    nxt = (
        encode_cursor(rows[limit - 1]["created_at"], rows[limit - 1]["id"])
        if len(rows) > limit
        else None
    )
    return rows[:limit], nxt


async def get_definition(
    engine: AsyncEngine, tenant_id: str, definition_id: uuid.UUID
) -> dict[str, Any] | None:
    async with engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    select(defs_t).where(
                        and_(defs_t.c.id == definition_id, defs_t.c.tenant_id == tenant_id)
                    )
                )
            )
            .mappings()
            .first()
        )
    return dict(row) if row else None


async def list_runs(
    engine: AsyncEngine,
    tenant_id: str,
    *,
    limit: int,
    cursor: str | None,
    status: str | None,
    definition_id: uuid.UUID | None,
) -> tuple[list[dict[str, Any]], str | None]:
    q = select(runs_t).where(runs_t.c.tenant_id == tenant_id)
    if status:
        q = q.where(runs_t.c.status == status)
    if definition_id:
        q = q.where(runs_t.c.definition_id == definition_id)
    if cursor:
        ts, ident = decode_cursor(cursor)
        q = q.where(tuple_(runs_t.c.created_at, runs_t.c.id) < tuple_(ts, ident))
    q = q.order_by(runs_t.c.created_at.desc(), runs_t.c.id.desc()).limit(limit + 1)
    async with engine.connect() as conn:
        rows = [dict(r) for r in (await conn.execute(q)).mappings()]
    nxt = (
        encode_cursor(rows[limit - 1]["created_at"], rows[limit - 1]["id"])
        if len(rows) > limit
        else None
    )
    return rows[:limit], nxt


async def get_run(engine: AsyncEngine, tenant_id: str, run_id: uuid.UUID) -> dict[str, Any] | None:
    async with engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    select(runs_t).where(
                        and_(runs_t.c.id == run_id, runs_t.c.tenant_id == tenant_id)
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        counts = {
            s: n
            for s, n in (
                await conn.execute(
                    select(tasks_t.c.status, func.count())
                    .where(tasks_t.c.run_id == run_id)
                    .group_by(tasks_t.c.status)
                )
            ).all()
        }
    return {**dict(row), "task_counts": counts}


async def list_tasks(
    engine: AsyncEngine, tenant_id: str, run_id: uuid.UUID
) -> list[dict[str, Any]] | None:
    async with engine.connect() as conn:
        owned = (
            await conn.execute(
                select(runs_t.c.id).where(
                    and_(runs_t.c.id == run_id, runs_t.c.tenant_id == tenant_id)
                )
            )
        ).first()
        if owned is None:
            return None
        rows = [
            dict(r)
            for r in (
                await conn.execute(
                    select(tasks_t)
                    .where(tasks_t.c.run_id == run_id)
                    .order_by(tasks_t.c.created_at, tasks_t.c.task_key)
                )
            ).mappings()
        ]
        dep_rows = (
            await conn.execute(
                select(deps_t.c.task_id, tasks_t.c.task_key)
                .join(tasks_t, tasks_t.c.id == deps_t.c.depends_on_task_id)
                .where(tasks_t.c.run_id == run_id)
            )
        ).all()
    deps: dict[uuid.UUID, list[str]] = {}
    for d in dep_rows:
        deps.setdefault(d.task_id, []).append(d.task_key)
    for r in rows:
        r["depends_on"] = sorted(deps.get(r["id"], []))
    return rows


async def get_task(
    engine: AsyncEngine, tenant_id: str, task_id: uuid.UUID
) -> dict[str, Any] | None:
    async with engine.connect() as conn:
        row = (
            (
                await conn.execute(
                    select(tasks_t)
                    .join(runs_t, runs_t.c.id == tasks_t.c.run_id)
                    .where(and_(tasks_t.c.id == task_id, runs_t.c.tenant_id == tenant_id))
                )
            )
            .mappings()
            .first()
        )
    return dict(row) if row else None


async def list_attempts(
    engine: AsyncEngine, tenant_id: str, task_id: uuid.UUID
) -> list[dict[str, Any]] | None:
    if await get_task(engine, tenant_id, task_id) is None:
        return None
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                select(attempts_t)
                .where(attempts_t.c.task_id == task_id)
                .order_by(attempts_t.c.attempt_number)
            )
        ).mappings()
        return [dict(r) for r in rows]


async def list_events(
    engine: AsyncEngine,
    tenant_id: str,
    run_id: uuid.UUID,
    *,
    after: int,
    limit: int,
    task_id: uuid.UUID | None = None,
) -> list[dict[str, Any]] | None:
    async with engine.connect() as conn:
        owned = (
            await conn.execute(
                select(runs_t.c.id).where(
                    and_(runs_t.c.id == run_id, runs_t.c.tenant_id == tenant_id)
                )
            )
        ).first()
        if owned is None:
            return None
        q = (
            select(events_t)
            .where(and_(events_t.c.run_id == run_id, events_t.c.id > after))
            .order_by(events_t.c.id)
            .limit(limit)
        )
        if task_id is not None:
            q = q.where(or_(events_t.c.task_id == task_id))
        return [dict(r) for r in (await conn.execute(q)).mappings()]


_RUN_COUNTS_SQL = text(
    "SELECT status, count(*) FROM workflow_runs WHERE tenant_id = :tenant GROUP BY status"
)
_ACTIVE_TASK_COUNTS_SQL = text(
    """
    SELECT t.status, count(*)
      FROM tasks t JOIN workflow_runs r ON r.id = t.run_id
     WHERE r.tenant_id = :tenant
       AND r.status IN ('PENDING', 'RUNNING', 'CANCELLING')
       AND t.status IN ('BLOCKED', 'READY', 'RUNNING', 'RETRY_WAIT')
     GROUP BY t.status
    """
)


async def tenant_stats(engine: AsyncEngine, tenant_id: str) -> dict[str, dict[str, int]]:
    """Run counts by status and active-task counts by status, for one tenant."""
    async with engine.connect() as conn:
        runs = {s: int(n) for s, n in (await conn.execute(_RUN_COUNTS_SQL, {"tenant": tenant_id}))}
        tasks = {
            s: int(n)
            for s, n in (await conn.execute(_ACTIVE_TASK_COUNTS_SQL, {"tenant": tenant_id}))
        }
    return {"runs": runs, "active_tasks": tasks}


_WORKERS_SQL = text(
    """
    SELECT w.worker_id, w.hostname, w.pid, w.version, w.concurrency, w.handlers,
           w.started_at, w.last_heartbeat_at, w.stopped_at,
           extract(epoch FROM (statement_timestamp() - w.last_heartbeat_at))::float8
               AS heartbeat_age_seconds,
           (SELECT count(*) FROM tasks t
             WHERE t.lease_owner = w.worker_id AND t.status = 'RUNNING') AS running_tasks
      FROM workers w
     ORDER BY w.stopped_at IS NOT NULL, w.started_at DESC
     LIMIT :limit
    """
)


async def list_workers(engine: AsyncEngine, *, limit: int) -> list[dict[str, Any]]:
    """Worker registry (infrastructure-wide, not tenant data: admin only)."""
    async with engine.connect() as conn:
        return [dict(r) for r in (await conn.execute(_WORKERS_SQL, {"limit": limit})).mappings()]
