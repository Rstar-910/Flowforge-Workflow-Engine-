"""Helpers for building workflows and inspecting persisted state in tests."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from flowforge.config import Settings
from flowforge.domain.definition import WorkflowDefinitionSpec
from flowforge.engine.submission import create_definition, submit_run
from flowforge.scheduler.service import Scheduler
from flowforge.worker.registry import HandlerRegistry
from flowforge.worker.runtime import Worker

TENANT = "tenant-a"


def task(
    task_id: str,
    handler: str = "noop",
    depends_on: list[str] | None = None,
    *,
    input: dict[str, Any] | None = None,
    timeout: float = 30,
    max_attempts: int = 3,
    initial_delay: float = 0.05,
    max_delay: float = 0.2,
    queue_timeout: float | None = None,
    **retry: Any,
) -> dict[str, Any]:
    extra = {"queue_timeout_seconds": queue_timeout} if queue_timeout is not None else {}
    return {
        **extra,
        "id": task_id,
        "handler": handler,
        "depends_on": depends_on or [],
        "input": input or {},
        "timeout_seconds": timeout,
        "retry": {
            "max_attempts": max_attempts,
            "initial_delay_seconds": initial_delay,
            "max_delay_seconds": max_delay,
            "jitter_ratio": 0.0,
            **retry,
        },
    }


async def start_run(
    engine: AsyncEngine,
    registry: HandlerRegistry,
    tasks: list[dict[str, Any]],
    *,
    run_input: dict[str, Any] | None = None,
    name: str | None = None,
    idempotency_key: str | None = None,
    tenant: str = TENANT,
    deadline: float | None = None,
) -> uuid.UUID:
    doc: dict[str, Any] = {
        "name": name or f"wf-{uuid.uuid4().hex[:8]}",
        "version": 1,
        "tasks": tasks,
    }
    if deadline is not None:
        doc["deadline_seconds"] = deadline
    spec = WorkflowDefinitionSpec.model_validate(doc)
    def_id, _ = await create_definition(
        engine, tenant_id=tenant, created_by="test", spec=spec, known_handlers=registry.names()
    )
    result = await submit_run(
        engine,
        tenant_id=tenant,
        created_by="test",
        definition_id=def_id,
        run_input=run_input or {},
        idempotency_key=idempotency_key,
        max_input_bytes=65536,
    )
    return result.run_id


async def run_status(engine: AsyncEngine, run_id: uuid.UUID) -> str:
    async with engine.connect() as conn:
        return str(
            (
                await conn.execute(
                    text("SELECT status FROM workflow_runs WHERE id = :id"), {"id": run_id}
                )
            ).scalar_one()
        )


async def tasks_by_key(engine: AsyncEngine, run_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text("SELECT * FROM tasks WHERE run_id = :id ORDER BY task_key"), {"id": run_id}
            )
        ).mappings()
        return {r["task_key"]: dict(r) for r in rows}


async def attempts_for(engine: AsyncEngine, run_id: uuid.UUID) -> list[dict[str, Any]]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT a.*, t.task_key FROM task_attempts a JOIN tasks t ON t.id = a.task_id"
                    " WHERE a.run_id = :id ORDER BY t.task_key, a.attempt_number"
                ),
                {"id": run_id},
            )
        ).mappings()
        return [dict(r) for r in rows]


async def events_for(engine: AsyncEngine, run_id: uuid.UUID) -> list[dict[str, Any]]:
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text("SELECT * FROM task_events WHERE run_id = :id ORDER BY id"), {"id": run_id}
            )
        ).mappings()
        return [dict(r) for r in rows]


async def wait_for(
    predicate: Callable[[], Awaitable[bool]], *, timeout: float = 20, interval: float = 0.05
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


async def wait_for_run(
    engine: AsyncEngine, run_id: uuid.UUID, statuses: set[str], *, timeout: float = 20
) -> str:
    async def done() -> bool:
        return await run_status(engine, run_id) in statuses

    await wait_for(done, timeout=timeout)
    return await run_status(engine, run_id)


class Cluster:
    """In-process workers and scheduler sharing one event loop (for fast tests)."""

    def __init__(
        self,
        engine: AsyncEngine,
        settings: Settings,
        registry: HandlerRegistry,
        *,
        workers: int = 1,
        concurrency: int = 4,
        scheduler: bool = True,
    ) -> None:
        self.stop = asyncio.Event()
        self.workers = [
            Worker(engine, settings, registry, concurrency=concurrency, worker_id=f"w{i}")
            for i in range(workers)
        ]
        self.scheduler = Scheduler(engine, settings, reconcile_every=1) if scheduler else None
        self._tasks: list[asyncio.Task[None]] = []

    async def __aenter__(self) -> Cluster:
        for w in self.workers:
            self._tasks.append(asyncio.create_task(w.run(self.stop)))
        if self.scheduler:
            self._tasks.append(asyncio.create_task(self.scheduler.run(self.stop)))
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.stop.set()
        await asyncio.wait_for(asyncio.gather(*self._tasks), timeout=30)
