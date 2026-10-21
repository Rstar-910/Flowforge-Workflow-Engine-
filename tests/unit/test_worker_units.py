"""Handler registry and executor isolation, without a database."""

from __future__ import annotations

import asyncio
import time
import uuid

import pytest
from pydantic import BaseModel

from flowforge.domain.errors import RetryableTaskError, TaskCancelledError, TaskTimeoutError
from flowforge.handlers import builtin
from flowforge.worker.context import TaskContext
from flowforge.worker.executor import HandlerExecutor
from flowforge.worker.registry import HandlerRegistrationError, HandlerRegistry


def ctx(input=None, attempt=1, timeout=5.0) -> TaskContext:
    return TaskContext(
        task_id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        task_key="k",
        attempt_id=uuid.uuid4(),
        attempt_number=attempt,
        input=input or {},
        run_input={},
        dependency_outputs={},
        timeout_seconds=timeout,
    )


def test_registry_rejects_invalid_registrations():
    reg = HandlerRegistry()

    async def a(c):
        return 1

    def s(c):
        return 1

    reg.register("ok", a)
    with pytest.raises(HandlerRegistrationError, match="already registered"):
        reg.register("ok", a)
    with pytest.raises(HandlerRegistrationError, match="invalid handler name"):
        reg.register("Bad Name", a)
    with pytest.raises(HandlerRegistrationError, match="needs async def"):
        reg.register("x1", s, isolation="async")
    with pytest.raises(HandlerRegistrationError, match="synchronous"):
        reg.register("x2", a, isolation="thread")
    with pytest.raises(HandlerRegistrationError, match="importable"):
        reg.register("x3", s, isolation="process")  # local function cannot be spawned
    with pytest.raises(HandlerRegistrationError, match="BaseModel"):
        reg.register("x4", a, input_model=dict)  # type: ignore[arg-type]
    with pytest.raises(HandlerRegistrationError, match="not callable"):
        reg.register("x5", 42)  # type: ignore[arg-type]
    assert reg.register("sync_default", s).isolation == "thread"
    assert reg.names() == ["ok", "sync_default"]


def test_handler_names_double_as_operation_key_and_fencing_token():
    c = ctx(attempt=3)
    assert c.operation_key == f"{c.run_id}/k" and c.fencing_token == 3


async def test_async_timeout_and_cancel():
    ex = HandlerExecutor(2)
    reg = HandlerRegistry()
    spec = reg.register("s", builtin.sleep)

    class In(BaseModel):
        seconds: float

    c = ctx()
    c.input = In(seconds=10)
    with pytest.raises(TaskTimeoutError):
        await ex.run(spec, c, timeout=0.1)
    c2 = ctx()
    c2.input = In(seconds=10)
    asyncio.get_running_loop().call_later(0.1, c2.request_cancel, "cancel_requested")
    with pytest.raises(TaskCancelledError):
        await ex.run(spec, c2, timeout=5)
    ex.shutdown()


async def test_thread_timeout_counts_zombie_until_thread_returns():
    ex = HandlerExecutor(2)
    reg = HandlerRegistry()

    def slow(c):
        time.sleep(0.5)
        return "done"

    spec = reg.register("slow", slow)
    with pytest.raises(TaskTimeoutError):
        await ex.run(spec, ctx(), timeout=0.05)
    assert ex.zombies.count == 1
    await asyncio.sleep(0.7)
    assert ex.zombies.count == 0
    ex.shutdown()


async def test_process_isolation_kills_on_timeout_and_propagates_errors():
    ex = HandlerExecutor(1)
    reg = HandlerRegistry()
    spec = reg.register("ps", builtin.process_sleep, isolation="process")
    t0 = time.monotonic()
    with pytest.raises(TaskTimeoutError):
        await ex.run(spec, ctx({"seconds": 30}), timeout=1.0)
    assert time.monotonic() - t0 < 5
    ok = await ex.run(spec, ctx({"seconds": 0}), timeout=10)
    assert ok["slept"] == 0
    burn = reg.register("burn", builtin.cpu_burn, isolation="process")
    assert len((await ex.run(burn, ctx({"rounds": 10}), timeout=10))["digest"]) == 16


async def test_fail_until_handler_raises_retryable():
    c = ctx(attempt=1)
    c.input = builtin.FailInput(fail_times=1, retry_after_seconds=2)
    with pytest.raises(RetryableTaskError) as exc:
        await builtin.fail_until(c)
    assert exc.value.retry_after_seconds == 2
    c2 = ctx(attempt=2)
    c2.input = builtin.FailInput(fail_times=1)
    assert await builtin.fail_until(c2) == {"succeeded_on_attempt": 2}


async def test_cancelled_process_handler_never_outlives_cancellation():
    """Regression: a second cancellation during cleanup used to orphan the child process."""
    import multiprocessing

    ex = HandlerExecutor(1)
    spec = HandlerRegistry().register("ps", builtin.process_sleep, isolation="process")
    task = asyncio.create_task(ex.run(spec, ctx({"seconds": 30}), timeout=60))
    for _ in range(100):  # wait until the child is running
        if multiprocessing.active_children():
            break
        await asyncio.sleep(0.05)
    assert multiprocessing.active_children(), "handler process never started"
    task.cancel()  # e.g. worker shutdown
    await asyncio.sleep(0)  # cleanup has started
    task.cancel()  # e.g. a teardown timeout cancels again
    await asyncio.gather(task, return_exceptions=True)
    await asyncio.sleep(1.5)  # shielded reaping finishes in the background
    assert [p for p in multiprocessing.active_children() if p.is_alive()] == []
