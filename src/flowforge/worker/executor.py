"""Run one handler invocation under its isolation mode with a timeout.

* ``async``  : awaited on the event loop; timeout/cancel interrupt it at its next
               ``await``. A handler that blocks the loop defeats this (and
               starves heartbeats, so its lease will expire) - don't do that.
* ``thread`` : run in a bounded thread pool. Python cannot kill a thread:
               cancellation is cooperative only, and on timeout the attempt is
               recorded as TIMED_OUT, the cancel flag is set, and the thread is
               counted as a *zombie* against worker capacity until it returns.
* ``process``: run in a fresh ``spawn`` subprocess that is terminated (SIGTERM,
               then SIGKILL) on timeout or cancellation - the only mode that
               can forcibly stop arbitrary synchronous code.
"""

from __future__ import annotations

import asyncio
import importlib
import multiprocessing
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from multiprocessing.connection import Connection
from typing import Any

from flowforge.domain.errors import (
    PermanentTaskError,
    RetryableTaskError,
    TaskCancelledError,
    TaskError,
    TaskTimeoutError,
)
from flowforge.worker.context import TaskContext
from flowforge.worker.registry import HandlerSpec


@dataclass
class ZombieTracker:
    count: int = 0


def _rebuild_remote_error(
    error_type: str, message: str, retryable: bool, retry_after: float | None
) -> BaseException:
    exc: TaskError
    if retryable:
        exc = RetryableTaskError(message, retry_after_seconds=retry_after)
    else:
        exc = PermanentTaskError(message)
    # Preserve the original class name for classification and attempt history.
    exc.__class__ = type(error_type, (type(exc),), {})
    return exc


def _process_entry(module: str, qualname: str, ctx_data: dict[str, Any], conn: Connection) -> None:
    """Runs in the child process."""
    try:
        obj: Any = importlib.import_module(module)
        for part in qualname.split("."):
            obj = getattr(obj, part)
        ctx = TaskContext(**ctx_data)
        result = obj(ctx)
        conn.send(("ok", result))
    except BaseException as exc:
        conn.send(
            (
                "error",
                (
                    type(exc).__name__,
                    str(exc)[:2000],
                    bool(getattr(exc, "retryable", True)),
                    getattr(exc, "retry_after_seconds", None),
                    traceback.format_exc(limit=5),
                ),
            )
        )
    finally:
        conn.close()


class HandlerExecutor:
    def __init__(self, max_threads: int) -> None:
        self._threads = ThreadPoolExecutor(max_workers=max_threads, thread_name_prefix="ff-handler")
        self._mp = multiprocessing.get_context("spawn")
        self.zombies = ZombieTracker()

    def shutdown(self) -> None:
        self._threads.shutdown(wait=False, cancel_futures=True)

    async def run(self, spec: HandlerSpec, ctx: TaskContext, timeout: float) -> Any:
        if spec.isolation == "async":
            return await self._run_async(spec, ctx, timeout)
        if spec.isolation == "thread":
            return await self._run_thread(spec, ctx, timeout)
        return await self._run_process(spec, ctx, timeout)

    async def _run_async(self, spec: HandlerSpec, ctx: TaskContext, timeout: float) -> Any:
        task = asyncio.ensure_future(spec.func(ctx))
        watcher = asyncio.ensure_future(_wait_cancel(ctx))
        try:
            done, _ = await asyncio.wait(
                {task, watcher}, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            if task in done:
                return task.result()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if watcher in done:
                raise TaskCancelledError(ctx.cancel_reason or "cancelled")
            raise TaskTimeoutError(f"handler exceeded timeout of {timeout:g}s")
        finally:
            watcher.cancel()

    async def _run_thread(self, spec: HandlerSpec, ctx: TaskContext, timeout: float) -> Any:
        # Cancellation is purely cooperative here (the handler may call
        # ctx.check_cancelled()); we keep waiting so that the recorded outcome
        # is what the thread actually did. Only the timeout makes us stop waiting.
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(self._threads, spec.func, ctx)
        done, _ = await asyncio.wait({fut}, timeout=timeout)
        if fut in done:
            return fut.result()
        ctx.request_cancel("timeout")
        self.zombies.count += 1

        def _released(_: Any) -> None:
            self.zombies.count -= 1

        fut.add_done_callback(_released)
        raise TaskTimeoutError(f"handler exceeded timeout of {timeout:g}s (thread still running)")

    async def _run_process(self, spec: HandlerSpec, ctx: TaskContext, timeout: float) -> Any:
        parent, child = self._mp.Pipe(duplex=False)
        proc = self._mp.Process(
            target=_process_entry,
            args=(spec.func.__module__, spec.func.__qualname__, ctx.portable(), child),
            daemon=True,
        )
        proc.start()
        child.close()
        deadline = time.monotonic() + timeout
        try:
            while True:
                if parent.poll():
                    kind, payload = parent.recv()
                    break
                if not proc.is_alive() and not parent.poll():
                    raise RetryableTaskError(
                        f"handler process exited with code {proc.exitcode} without a result"
                    )
                if ctx.is_cancelled():
                    raise TaskCancelledError(ctx.cancel_reason or "cancelled")
                if time.monotonic() >= deadline:
                    raise TaskTimeoutError(f"handler exceeded timeout of {timeout:g}s (killed)")
                await asyncio.sleep(0.02)
        finally:
            parent.close()
            # Signal the child *before* any await: if this task is cancelled
            # again while cleaning up (worker shutdown, a teardown timeout), an
            # await here would be interrupted and the child would keep running,
            # side effects included. Reaping is shielded for the same reason.
            if proc.is_alive():
                proc.terminate()
            await asyncio.shield(asyncio.to_thread(_reap_process, proc))
        if kind == "ok":
            return payload
        error_type, message, retryable, retry_after, _tb = payload
        raise _rebuild_remote_error(error_type, message, retryable, retry_after)


async def _wait_cancel(ctx: TaskContext) -> None:
    # The flag is a threading.Event (shared with handler threads), so poll it.
    while not ctx.is_cancelled():  # noqa: ASYNC110
        await asyncio.sleep(0.05)


def _reap_process(proc: Any) -> None:
    """Wait for a terminated child; escalate to SIGKILL if it ignores SIGTERM."""
    proc.join(1.0)
    if proc.is_alive():
        proc.kill()
        proc.join(5.0)
