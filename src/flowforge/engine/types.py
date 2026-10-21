"""Value objects exchanged between the engine and its callers."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from flowforge.domain.errors import FailureInfo
from flowforge.domain.retry import RetryPolicy


@dataclass(frozen=True, slots=True)
class ClaimedTask:
    """Everything a worker needs to execute one attempt.

    ``attempt_id`` is the lease token: every renewal and result write is
    conditioned on ``tasks.current_attempt_id = attempt_id``. ``attempt_number``
    is the monotonically increasing fencing generation that handlers may pass to
    downstream systems that support fencing.
    """

    task_id: uuid.UUID
    run_id: uuid.UUID
    task_key: str
    handler: str
    attempt_id: uuid.UUID
    attempt_number: int
    attempt_limit: int
    timeout_seconds: float
    retry_policy: RetryPolicy
    input: dict[str, Any]
    run_input: dict[str, Any]
    dependency_outputs: dict[str, Any]
    lease_expires_at: datetime
    queue_wait_seconds: float
    traceparent: str | None = None

    @property
    def operation_key(self) -> str:
        """Stable across attempts of the same logical task: use as a downstream idempotency key."""
        return f"{self.run_id}/{self.task_key}"


@dataclass(frozen=True, slots=True)
class Succeeded:
    output: Any


@dataclass(frozen=True, slots=True)
class Failed:
    failure: FailureInfo


@dataclass(frozen=True, slots=True)
class Cancelled:
    reason: str


TaskResult = Succeeded | Failed | Cancelled


@dataclass(frozen=True, slots=True)
class CompletionOutcome:
    accepted: bool
    task_status: str | None = None
    run_status: str | None = None
    retry_delay_seconds: float | None = None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class LeaseRenewal:
    renewed: set[uuid.UUID] = field(default_factory=set)
    cancel_requested: set[uuid.UUID] = field(default_factory=set)
