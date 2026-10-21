"""Request and response models of the public API (v1)."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

IDEMPOTENCY_KEY_PATTERN = r"^[A-Za-z0-9._:\-]{1,200}$"


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str | None = None
    details: list[dict[str, Any]] | None = None


class ErrorResponse(BaseModel):
    error: ErrorBody


class DefinitionOut(BaseModel):
    id: uuid.UUID
    name: str
    version: int
    description: str | None
    definition: dict[str, Any]
    created_by: str
    created_at: datetime

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> DefinitionOut:
        return cls(**row, definition=row["definition_json"])


class DefinitionList(BaseModel):
    items: list[DefinitionOut]
    next_cursor: str | None


class StartRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    input: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = Field(default=None, pattern=IDEMPOTENCY_KEY_PATTERN)


class RunOut(BaseModel):
    id: uuid.UUID
    definition_id: uuid.UUID
    status: str
    input: dict[str, Any]
    output: dict[str, Any] | None
    failure_reason: str | None
    idempotency_key: str | None
    created_by: str
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    cancellation_requested_at: datetime | None
    events_pruned_at: datetime | None = None
    deadline_at: datetime | None = None
    deadline_exceeded_at: datetime | None = None
    task_counts: dict[str, int] | None = None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> RunOut:
        return cls(**row, input=row["input_json"], output=row["output_json"])


class RunList(BaseModel):
    items: list[RunOut]
    next_cursor: str | None


class TaskOut(BaseModel):
    id: uuid.UUID
    run_id: uuid.UUID
    task_key: str
    handler: str
    status: str
    depends_on: list[str] | None = None
    input: dict[str, Any]
    output: Any | None
    attempt_count: int
    attempt_limit: int
    timeout_seconds: float
    queue_timeout_seconds: float | None = None
    retry_policy: dict[str, Any]
    lease_owner: str | None
    lease_expires_at: datetime | None
    next_attempt_at: datetime | None
    ready_at: datetime | None
    last_error_type: str | None
    last_error_message: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> TaskOut:
        return cls(**row, input=row["input_json"], output=row["output_json"])


class TaskList(BaseModel):
    items: list[TaskOut]


class AttemptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    task_id: uuid.UUID
    attempt_number: int
    worker_id: str
    outcome: str
    retryable: bool | None
    error_type: str | None
    error_message: str | None
    queue_wait_ms: int | None
    duration_ms: int | None
    started_at: datetime
    ended_at: datetime | None


class AttemptList(BaseModel):
    items: list[AttemptOut]


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    task_id: uuid.UUID | None
    attempt_id: uuid.UUID | None
    event_type: str
    data: dict[str, Any]
    created_at: datetime


class EventList(BaseModel):
    items: list[EventOut]
    next_after: int | None


class StatusOut(BaseModel):
    id: uuid.UUID
    status: str


class HealthOut(BaseModel):
    status: str
    checks: dict[str, str] = Field(default_factory=dict)


class StatsOut(BaseModel):
    runs: dict[str, int]
    active_tasks: dict[str, int]


class WorkerOut(BaseModel):
    worker_id: str
    hostname: str
    pid: int
    version: str
    concurrency: int
    handlers: list[str]
    started_at: datetime
    last_heartbeat_at: datetime
    stopped_at: datetime | None
    heartbeat_age_seconds: float
    running_tasks: int
    # "live": heartbeat within one lease period; "silent": registered but not
    # heartbeating (crashed, paused or partitioned); "stopped": shut down cleanly.
    health: str


class WorkerList(BaseModel):
    items: list[WorkerOut]
