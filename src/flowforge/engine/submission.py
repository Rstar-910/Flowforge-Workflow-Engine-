"""Workflow definition storage and idempotent run submission."""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from sqlalchemy import and_, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from flowforge.domain.definition import (
    ValidatedDefinition,
    WorkflowDefinitionSpec,
    json_size,
    validate_definition,
)
from flowforge.domain.states import RunStatus, TaskStatus
from flowforge.engine.sql import (
    append_events,
    db_now,
    defs_t,
    deps_t,
    idem_t,
    runs_t,
    tasks_t,
)
from flowforge.observability import metrics


class NotFoundError(Exception):
    pass


class ConflictError(Exception):
    pass


class IdempotencyConflictError(ConflictError):
    pass


class PayloadTooLargeError(Exception):
    pass


def canonical_hash(value: Any) -> str:
    """SHA-256 of a canonical JSON encoding (sorted keys, no whitespace)."""
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class SubmitResult:
    run_id: uuid.UUID
    created: bool


async def create_definition(
    engine: AsyncEngine,
    *,
    tenant_id: str,
    created_by: str,
    spec: WorkflowDefinitionSpec,
    known_handlers: Iterable[str],
) -> tuple[uuid.UUID, bool]:
    """Validate and store a definition. Returns (id, created).

    Re-posting an identical (name, version) is idempotent; posting different
    content under an existing (name, version) is a conflict, since definitions
    are immutable once stored.
    """
    validated = validate_definition(spec, known_handlers)
    doc = validated.spec.model_dump(mode="json")
    spec_hash = canonical_hash(doc)
    def_id = uuid.uuid4()
    async with engine.begin() as conn:
        row = (
            await conn.execute(
                pg_insert(defs_t)
                .values(
                    id=def_id,
                    tenant_id=tenant_id,
                    name=spec.name,
                    version=spec.version,
                    description=spec.description,
                    definition_json=doc,
                    spec_hash=spec_hash,
                    created_by=created_by,
                )
                .on_conflict_do_nothing(index_elements=["tenant_id", "name", "version"])
                .returning(defs_t.c.id)
            )
        ).first()
        if row is not None:
            return row.id, True
        existing = (
            await conn.execute(
                select(defs_t.c.id, defs_t.c.spec_hash).where(
                    and_(
                        defs_t.c.tenant_id == tenant_id,
                        defs_t.c.name == spec.name,
                        defs_t.c.version == spec.version,
                    )
                )
            )
        ).one()
        if existing.spec_hash != spec_hash:
            raise ConflictError(
                f"definition {spec.name!r} version {spec.version} already exists with "
                "different content; publish a new version"
            )
        return existing.id, False


async def _load_definition(
    conn: AsyncConnection, tenant_id: str, definition_id: uuid.UUID
) -> ValidatedDefinition:
    row = (
        await conn.execute(
            select(defs_t.c.definition_json).where(
                and_(defs_t.c.id == definition_id, defs_t.c.tenant_id == tenant_id)
            )
        )
    ).first()
    if row is None:
        raise NotFoundError("workflow definition not found")
    spec = WorkflowDefinitionSpec.model_validate(row.definition_json)
    # Handlers were checked when the definition was stored; the worker re-checks
    # at execution time, so do not fail submission if this process lacks one.
    return validate_definition(spec, {t.handler for t in spec.tasks})


async def submit_run(
    engine: AsyncEngine,
    *,
    tenant_id: str,
    created_by: str,
    definition_id: uuid.UUID,
    run_input: dict[str, Any],
    idempotency_key: str | None,
    max_input_bytes: int,
    traceparent: str | None = None,
) -> SubmitResult:
    """Create a run and all of its tasks atomically.

    Idempotency protocol (single transaction, READ COMMITTED):

    1. ``INSERT .. ON CONFLICT DO NOTHING`` into ``idempotency_records``. If a
       concurrent transaction holds the same key uncommitted, PostgreSQL makes
       this insert wait for it; afterwards exactly one of them owns the key.
    2. If our insert did not take effect, read the winner's record: same request
       hash -> return its run; different hash -> conflict.
    3. Otherwise insert the run (the FK from the record to the run is DEFERRED,
       so it is checked at commit).
    """
    if json_size(run_input) > max_input_bytes:
        raise PayloadTooLargeError(f"run input exceeds {max_input_bytes} bytes")
    request_hash = canonical_hash({"definition_id": str(definition_id), "input": run_input})
    run_id = uuid.uuid4()

    async with engine.begin() as conn:
        definition = await _load_definition(conn, tenant_id, definition_id)

        if idempotency_key is not None:
            inserted = (
                await conn.execute(
                    pg_insert(idem_t)
                    .values(
                        tenant_id=tenant_id,
                        idempotency_key=idempotency_key,
                        request_hash=request_hash,
                        run_id=run_id,
                    )
                    .on_conflict_do_nothing(index_elements=["tenant_id", "idempotency_key"])
                    .returning(idem_t.c.run_id)
                )
            ).first()
            if inserted is None:
                existing = (
                    await conn.execute(
                        select(idem_t.c.request_hash, idem_t.c.run_id).where(
                            and_(
                                idem_t.c.tenant_id == tenant_id,
                                idem_t.c.idempotency_key == idempotency_key,
                            )
                        )
                    )
                ).one()
                if existing.request_hash != request_hash:
                    raise IdempotencyConflictError(
                        "idempotency key was already used with a different request"
                    )
                return SubmitResult(run_id=existing.run_id, created=False)

        await _insert_run(
            conn,
            run_id=run_id,
            tenant_id=tenant_id,
            created_by=created_by,
            definition_id=definition_id,
            definition=definition,
            run_input=run_input,
            idempotency_key=idempotency_key,
            traceparent=traceparent,
        )
    metrics.RUNS_SUBMITTED.inc()
    return SubmitResult(run_id=run_id, created=True)


async def _insert_run(
    conn: AsyncConnection,
    *,
    run_id: uuid.UUID,
    tenant_id: str,
    created_by: str,
    definition_id: uuid.UUID,
    definition: ValidatedDefinition,
    run_input: dict[str, Any],
    idempotency_key: str | None,
    traceparent: str | None,
) -> None:
    now = await db_now(conn)  # one timestamp for all root tasks of this run
    await conn.execute(
        insert(runs_t).values(
            id=run_id,
            tenant_id=tenant_id,
            definition_id=definition_id,
            status=RunStatus.PENDING.value,
            input_json=run_input,
            idempotency_key=idempotency_key,
            created_by=created_by,
            traceparent=traceparent,
        )
    )
    task_ids = {key: uuid.uuid4() for key in definition.topological_order}
    task_rows = []
    dep_rows: list[dict[str, uuid.UUID]] = []
    for key in definition.topological_order:
        spec = definition.task(key)
        is_root = not spec.depends_on
        task_rows.append(
            {
                "id": task_ids[key],
                "run_id": run_id,
                "task_key": key,
                "handler": spec.handler,
                "status": (TaskStatus.READY if is_root else TaskStatus.BLOCKED).value,
                "input_json": spec.input,
                "timeout_seconds": spec.timeout_seconds,
                "retry_policy": spec.retry.model_dump(mode="json"),
                "attempt_limit": spec.retry.max_attempts,
                "ready_at": now if is_root else None,
            }
        )
        dep_rows.extend(
            {"task_id": task_ids[key], "depends_on_task_id": task_ids[d]} for d in spec.depends_on
        )
    await conn.execute(insert(tasks_t), task_rows)
    if dep_rows:
        await conn.execute(insert(deps_t), dep_rows)
    await append_events(
        conn,
        [
            {
                "run_id": run_id,
                "event_type": "run_created",
                "data": {
                    "definition_id": str(definition_id),
                    "task_count": len(task_rows),
                    "idempotency_key": idempotency_key,
                },
            }
        ],
    )
