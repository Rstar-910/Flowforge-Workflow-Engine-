"""Errors raised by task handlers and their retry classification."""

from __future__ import annotations

from dataclasses import dataclass

from flowforge.domain.retry import RetryPolicy


class TaskError(Exception):
    """Base class for errors a handler raises deliberately."""

    retryable: bool = True


class RetryableTaskError(TaskError):
    """A transient failure (network error, rate limit, dependency unavailable)."""

    retryable = True

    def __init__(self, message: str, *, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class PermanentTaskError(TaskError):
    """A deterministic failure that will not succeed on retry (bad input, auth failure)."""

    retryable = False


class InputValidationError(PermanentTaskError):
    """The task's input does not match the handler's declared input model."""


class HandlerNotFoundError(PermanentTaskError):
    """The worker has no handler registered under the task's handler name."""


class TaskTimeoutError(Exception):
    """Execution exceeded the task's timeout."""


class TaskCancelledError(Exception):
    """Execution stopped because cancellation was requested or the lease was lost."""


@dataclass(frozen=True, slots=True)
class FailureInfo:
    error_type: str
    message: str
    retryable: bool
    timed_out: bool = False
    retry_after_seconds: float | None = None


MAX_ERROR_MESSAGE_CHARS = 2000


def classify_exception(exc: BaseException, policy: RetryPolicy) -> FailureInfo:
    """Decide whether a handler failure may be retried under ``policy``."""
    error_type = type(exc).__name__
    message = str(exc)[:MAX_ERROR_MESSAGE_CHARS] or error_type
    if isinstance(exc, TaskTimeoutError):
        return FailureInfo(error_type, message, retryable=policy.retry_on_timeout, timed_out=True)
    retry_after = getattr(exc, "retry_after_seconds", None)
    if error_type in policy.non_retryable_errors:
        return FailureInfo(error_type, message, retryable=False)
    if isinstance(exc, TaskError):
        return FailureInfo(error_type, message, exc.retryable, retry_after_seconds=retry_after)
    # Unknown exceptions are treated as transient by default; deterministic
    # failures should raise PermanentTaskError or be listed in non_retryable_errors.
    # The retry budget is bounded either way.
    return FailureInfo(error_type, message, retryable=True)
