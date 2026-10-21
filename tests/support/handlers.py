"""Handlers used only by tests. Loaded by worker subprocesses via FLOWFORGE_HANDLER_MODULES."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any

from flowforge.worker.context import TaskContext
from flowforge.worker.registry import task_handler


def _signal_dir() -> Path:
    return Path(os.environ.get("FLOWFORGE_TEST_SIGNAL_DIR", "/tmp/flowforge-test-signals"))


@task_handler("test.wait_for_release")
async def wait_for_release(ctx: TaskContext) -> dict[str, Any]:
    """Create <dir>/<key>.started, then wait (cancellably) until <dir>/<key>.release exists."""
    key = ctx.input["key"]
    d = _signal_dir()
    (d / f"{key}.started").write_text(f"{os.getpid()} {ctx.attempt_number}")
    deadline = time.monotonic() + float(ctx.input.get("max_wait", 60))
    while not (d / f"{key}.release").exists():
        if time.monotonic() > deadline:
            raise TimeoutError("release file never appeared")
        await ctx.sleep(0.02)
    return {"released": key, "attempt": ctx.attempt_number}


@task_handler("test.ignore_cancel_sleep")
def ignore_cancel_sleep(ctx: TaskContext) -> dict[str, Any]:
    """Blocking sleep that never checks for cancellation."""
    time.sleep(float(ctx.input.get("seconds", 1)))
    return {"done": True}


@task_handler("test.record_order")
async def record_order(ctx: TaskContext) -> dict[str, Any]:
    """Return a monotonic timestamp so tests can verify ordering across tasks."""
    delay = float(ctx.input.get("delay", 0))
    if delay:
        await ctx.sleep(delay)
    return {"t": time.time(), "deps": sorted(ctx.dependency_outputs)}


@task_handler("test.raise_value_error")
async def raise_value_error(ctx: TaskContext) -> None:
    raise ValueError("bad input value")


@task_handler("test.unserialisable")
async def unserialisable(ctx: TaskContext) -> object:
    return {"x": object()}
