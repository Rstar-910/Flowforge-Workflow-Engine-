"""The object passed to every handler."""

from __future__ import annotations

import asyncio
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from flowforge.domain.errors import TaskCancelledError


@dataclass
class TaskContext:
    """Execution context for one attempt.

    * ``operation_key`` is stable across retries of the same task in the same
      run: pass it to downstream systems as their idempotency key.
    * ``fencing_token`` (the attempt number) increases with every attempt: a
      downstream system that remembers the highest token it has seen can reject
      writes from a stale attempt.
    * Long-running handlers should call ``check_cancelled()`` (sync) or use
      ``await ctx.sleep()`` (async) so cancellation and lease loss are honoured.
    """

    task_id: uuid.UUID
    run_id: uuid.UUID
    task_key: str
    attempt_id: uuid.UUID
    attempt_number: int
    input: Any
    run_input: dict[str, Any]
    dependency_outputs: dict[str, Any]
    timeout_seconds: float
    started_monotonic: float = field(default_factory=time.monotonic)
    _cancel: threading.Event = field(default_factory=threading.Event, repr=False)
    cancel_reason: str | None = None

    @property
    def operation_key(self) -> str:
        return f"{self.run_id}/{self.task_key}"

    @property
    def fencing_token(self) -> int:
        return self.attempt_number

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self.timeout_seconds - (time.monotonic() - self.started_monotonic))

    def request_cancel(self, reason: str) -> None:
        if not self._cancel.is_set():
            self.cancel_reason = reason
            self._cancel.set()

    def is_cancelled(self) -> bool:
        return self._cancel.is_set()

    def check_cancelled(self) -> None:
        if self._cancel.is_set():
            raise TaskCancelledError(self.cancel_reason or "cancelled")

    async def sleep(self, seconds: float) -> None:
        """Cancellation-aware sleep for async handlers."""
        deadline = time.monotonic() + seconds
        while True:
            self.check_cancelled()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(remaining, 0.05))

    def sleep_sync(self, seconds: float) -> None:
        """Cancellation-aware sleep for thread handlers."""
        if self._cancel.wait(seconds):
            self.check_cancelled()

    def portable(self) -> dict[str, Any]:
        """Picklable subset for process-isolated handlers."""
        return {
            "task_id": self.task_id,
            "run_id": self.run_id,
            "task_key": self.task_key,
            "attempt_id": self.attempt_id,
            "attempt_number": self.attempt_number,
            "input": self.input,
            "run_input": self.run_input,
            "dependency_outputs": self.dependency_outputs,
            "timeout_seconds": self.timeout_seconds,
        }
