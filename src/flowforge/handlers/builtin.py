"""Built-in handlers used by examples, demos and benchmarks.

They are deliberately small and deterministic. ``record_effect`` simulates an
external side effect (e.g. charging a card) so the at-least-once ambiguity can
be demonstrated; it writes to a local append-only ledger file whose path is
operator-configured (``FLOWFORGE_DEMO_LEDGER_PATH``), never taken from task input.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from flowforge.domain.errors import PermanentTaskError, RetryableTaskError
from flowforge.worker.context import TaskContext
from flowforge.worker.registry import task_handler


@task_handler("noop")
async def noop(ctx: TaskContext) -> dict[str, Any]:
    """Return immediately (used to measure engine overhead)."""
    return {"task": ctx.task_key}


@task_handler("echo")
async def echo(ctx: TaskContext) -> dict[str, Any]:
    """Return the task input, run input and dependency outputs."""
    return {"input": ctx.input, "run_input": ctx.run_input, "deps": ctx.dependency_outputs}


class SleepInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    seconds: float = Field(ge=0, le=3600)


@task_handler("sleep", input_model=SleepInput)
async def sleep(ctx: TaskContext) -> dict[str, Any]:
    """Cancellation-aware asynchronous sleep."""
    await ctx.sleep(ctx.input.seconds)
    return {"slept": ctx.input.seconds}


@task_handler("blocking_sleep", input_model=SleepInput)
def blocking_sleep(ctx: TaskContext) -> dict[str, Any]:
    """Synchronous sleep in a worker thread that ignores cancellation (timeout demo)."""
    time.sleep(ctx.input.seconds)
    return {"slept": ctx.input.seconds}


@task_handler("process_sleep", isolation="process", input_model=None)
def process_sleep(ctx: TaskContext) -> dict[str, Any]:
    """Synchronous sleep in a subprocess; can be killed on timeout."""
    seconds = float(ctx.input.get("seconds", 1))
    time.sleep(seconds)
    return {"slept": seconds, "pid": os.getpid()}


class FailInput(BaseModel):
    model_config = ConfigDict(extra="allow")
    fail_times: int = Field(default=1, ge=0, le=100)
    retry_after_seconds: float | None = Field(default=None, ge=0)


@task_handler("fail_until", input_model=FailInput)
async def fail_until(ctx: TaskContext) -> dict[str, Any]:
    """Fail with a retryable error on the first ``fail_times`` attempts, then succeed."""
    if ctx.attempt_number <= ctx.input.fail_times:
        raise RetryableTaskError(
            f"simulated transient failure {ctx.attempt_number}/{ctx.input.fail_times}",
            retry_after_seconds=ctx.input.retry_after_seconds,
        )
    return {"succeeded_on_attempt": ctx.attempt_number}


@task_handler("always_fail")
async def always_fail(ctx: TaskContext) -> None:
    """Fail permanently (non-retryable)."""
    raise PermanentTaskError(str(ctx.input.get("message", "simulated permanent failure")))


@task_handler("flaky")
async def flaky(ctx: TaskContext) -> dict[str, Any]:
    """Fail transiently with probability ``p``, decided by a hash of (operation_key, attempt)."""
    p = float(ctx.input.get("p", 0.2))
    digest = hashlib.sha256(f"{ctx.operation_key}:{ctx.attempt_number}".encode()).digest()
    if digest[0] / 255 < p:
        raise RetryableTaskError("simulated flaky failure")
    return {"attempt": ctx.attempt_number}


@task_handler("cpu_burn", isolation="process")
def cpu_burn(ctx: TaskContext) -> dict[str, Any]:
    """CPU-bound work (hash chain) run in a subprocess."""
    rounds = int(ctx.input.get("rounds", 100_000))
    h = b"flowforge"
    for _ in range(rounds):
        h = hashlib.sha256(h).digest()
    return {"digest": h.hex()[:16]}


# ---------------------------------------------------------------- reporting


@task_handler("fetch_data")
async def fetch_data(ctx: TaskContext) -> dict[str, Any]:
    """Produce a deterministic dataset from the run input."""
    n = int(ctx.run_input.get("rows", 10))
    return {"values": [(i * 37) % 101 for i in range(n)]}


@task_handler("aggregate")
async def aggregate(ctx: TaskContext) -> dict[str, Any]:
    """Aggregate the values produced by the dependency named in ``input.source``."""
    source = ctx.input.get("source", "fetch")
    values = ctx.dependency_outputs[source]["values"]
    op = ctx.input.get("op", "sum")
    result: float
    if op == "sum":
        result = sum(values)
    elif op == "max":
        result = max(values)
    elif op == "mean":
        result = sum(values) / len(values) if values else 0.0
    else:
        raise PermanentTaskError(f"unknown aggregation {op!r}")
    return {"op": op, "value": result}


@task_handler("combine")
async def combine(ctx: TaskContext) -> dict[str, Any]:
    """Merge all dependency outputs into one report."""
    return {"report": {k: v.get("value", v) for k, v in sorted(ctx.dependency_outputs.items())}}


# ---------------------------------------------------------- side effects

LEDGER_ENV = "FLOWFORGE_DEMO_LEDGER_PATH"


def _ledger_path() -> Path:
    path = os.environ.get(LEDGER_ENV)
    if not path:
        raise PermanentTaskError(f"{LEDGER_ENV} is not configured on this worker")
    return Path(path)


def read_ledger(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


@task_handler("record_effect", isolation="thread", idempotent=False)
def record_effect(ctx: TaskContext) -> dict[str, Any]:
    """Append an entry to an external ledger, simulating a side effect such as a payment.

    With ``input.use_operation_key=true`` the "external system" de-duplicates on
    the engine's stable operation key, so retries after an ambiguous crash do
    not repeat the effect. Without it every attempt that reaches this point
    writes a new entry - exactly the duplicate an at-least-once engine can cause.
    """
    path = _ledger_path()
    use_key = bool(ctx.input.get("use_operation_key", True))
    entry = {
        "operation_key": ctx.operation_key,
        "attempt": ctx.attempt_number,
        "amount": ctx.input.get("amount", 1),
    }
    with path.open("a+") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        try:
            fh.seek(0)
            if use_key:
                for line in fh.read().splitlines():
                    if line.strip() and json.loads(line)["operation_key"] == ctx.operation_key:
                        return {"applied": False, "duplicate_of_existing": True}
            fh.seek(0, os.SEEK_END)
            fh.write(json.dumps(entry) + "\n")
            fh.flush()
            os.fsync(fh.fileno())
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
    return {"applied": True}
