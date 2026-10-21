"""SQLAlchemy table definitions.

These models describe the schema; the authoritative DDL lives in Alembic
migrations (``migrations/versions``) and a test asserts that the two agree.
Business logic does not live here.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Identity,
    Index,
    Integer,
    MetaData,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from flowforge.domain.states import AttemptOutcome, RunStatus, TaskStatus

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


def _in(column: str, values: Any) -> str:
    return f"{column} IN (" + ", ".join(f"'{v.value}'" for v in values) + ")"


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


def _ts(nullable: bool = True, default_now: bool = False) -> Any:
    return mapped_column(
        DateTime(timezone=True),
        nullable=nullable,
        server_default=func.now() if default_now else None,
    )


class WorkflowDefinition(Base):
    __tablename__ = "workflow_definitions"
    __table_args__ = (UniqueConstraint("tenant_id", "name", "version"),)

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(100), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    definition_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    spec_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_by: Mapped[str] = mapped_column(String(200), nullable=False)
    created_at: Mapped[datetime] = _ts(nullable=False, default_now=True)


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"
    __table_args__ = (
        CheckConstraint(_in("status", RunStatus), name="status_valid"),
        CheckConstraint(
            "(status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')) = (completed_at IS NOT NULL)",
            name="completed_at_iff_terminal",
        ),
        Index("ix_workflow_runs_tenant_created", "tenant_id", "created_at", "id"),
        Index(
            "ix_workflow_runs_active",
            "status",
            postgresql_where=text("status IN ('PENDING', 'RUNNING', 'CANCELLING')"),
        ),
        # Retention scans: oldest finished runs first.
        Index(
            "ix_workflow_runs_finished",
            "completed_at",
            postgresql_where=text("status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')"),
        ),
        Index(
            "ix_workflow_runs_events_unpruned",
            "completed_at",
            postgresql_where=text(
                "status IN ('SUCCEEDED', 'FAILED', 'CANCELLED') AND events_pruned_at IS NULL"
            ),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(100), nullable=False)
    definition_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("workflow_definitions.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    input_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    output_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    idempotency_key: Mapped[str | None] = mapped_column(String(200))
    failure_reason: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(200), nullable=False)
    traceparent: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = _ts(nullable=False, default_now=True)
    started_at: Mapped[datetime | None] = _ts()
    completed_at: Mapped[datetime | None] = _ts()
    cancellation_requested_at: Mapped[datetime | None] = _ts()
    # Set when retention deleted this run's events (attempts and tasks are kept).
    events_pruned_at: Mapped[datetime | None] = _ts()
    updated_at: Mapped[datetime] = _ts(nullable=False, default_now=True)


class Task(Base):
    __tablename__ = "tasks"
    __table_args__ = (
        UniqueConstraint("run_id", "task_key"),
        CheckConstraint(_in("status", TaskStatus), name="status_valid"),
        CheckConstraint("attempt_count >= 0", name="attempt_count_nonneg"),
        CheckConstraint("attempt_count <= attempt_limit", name="attempts_bounded"),
        CheckConstraint("timeout_seconds > 0", name="timeout_positive"),
        CheckConstraint(
            "(status = 'RUNNING') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL"
            " AND current_attempt_id IS NOT NULL)",
            name="lease_iff_running",
        ),
        CheckConstraint(
            "status <> 'RETRY_WAIT' OR next_attempt_at IS NOT NULL",
            name="retry_wait_has_time",
        ),
        CheckConstraint(
            "(status IN ('SUCCEEDED', 'FAILED', 'UPSTREAM_FAILED', 'CANCELLED'))"
            " = (completed_at IS NOT NULL)",
            name="completed_at_iff_terminal",
        ),
        Index("ix_tasks_ready", "ready_at", postgresql_where=text("status = 'READY'")),
        Index(
            "ix_tasks_retry_due",
            "next_attempt_at",
            postgresql_where=text("status = 'RETRY_WAIT'"),
        ),
        Index(
            "ix_tasks_lease_expiry",
            "lease_expires_at",
            postgresql_where=text("status = 'RUNNING'"),
        ),
        Index("ix_tasks_run_status", "run_id", "status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workflow_runs.id", ondelete="CASCADE"), nullable=False
    )
    task_key: Mapped[str] = mapped_column(String(100), nullable=False)
    handler: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    input_json: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    output_json: Mapped[Any | None] = mapped_column(JSONB)
    timeout_seconds: Mapped[float] = mapped_column(Float, nullable=False)
    retry_policy: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    # attempt_count doubles as the lease generation / fencing token: it is
    # incremented by every successful claim and never decreases.
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    attempt_limit: Mapped[int] = mapped_column(Integer, nullable=False)
    current_attempt_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    lease_owner: Mapped[str | None] = mapped_column(String(200))
    lease_expires_at: Mapped[datetime | None] = _ts()
    next_attempt_at: Mapped[datetime | None] = _ts()
    ready_at: Mapped[datetime | None] = _ts()
    last_error_type: Mapped[str | None] = mapped_column(String(200))
    last_error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = _ts(nullable=False, default_now=True)
    started_at: Mapped[datetime | None] = _ts()
    completed_at: Mapped[datetime | None] = _ts()
    updated_at: Mapped[datetime] = _ts(nullable=False, default_now=True)


class TaskDependency(Base):
    __tablename__ = "task_dependencies"
    __table_args__ = (
        PrimaryKeyConstraint("task_id", "depends_on_task_id"),
        CheckConstraint("task_id <> depends_on_task_id", name="no_self_dependency"),
        Index("ix_task_dependencies_depends_on", "depends_on_task_id"),
    )

    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False
    )
    depends_on_task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False
    )


class TaskAttempt(Base):
    __tablename__ = "task_attempts"
    __table_args__ = (
        UniqueConstraint("task_id", "attempt_number"),
        CheckConstraint(_in("outcome", AttemptOutcome), name="outcome_valid"),
        CheckConstraint("attempt_number >= 1", name="attempt_number_positive"),
        CheckConstraint("(outcome = 'RUNNING') = (ended_at IS NULL)", name="ended_at_iff_finished"),
        Index("ix_task_attempts_run", "run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("workflow_runs.id", ondelete="CASCADE"), nullable=False
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(200), nullable=False)
    outcome: Mapped[str] = mapped_column(String(20), nullable=False)
    retryable: Mapped[bool | None] = mapped_column(Boolean)
    error_type: Mapped[str | None] = mapped_column(String(200))
    error_message: Mapped[str | None] = mapped_column(Text)
    queue_wait_ms: Mapped[int | None] = mapped_column(Integer)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    started_at: Mapped[datetime] = _ts(nullable=False, default_now=True)
    ended_at: Mapped[datetime | None] = _ts()


class TaskEvent(Base):
    """Append-only audit trail of state changes. Rows are never updated."""

    __tablename__ = "task_events"
    __table_args__ = (
        Index("ix_task_events_run_id", "run_id", "id"),
        ForeignKeyConstraint(["run_id"], ["workflow_runs.id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["task_id"], ["tasks.id"], ondelete="CASCADE"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(always=True), primary_key=True)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    task_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), index=True)
    attempt_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    data: Mapped[dict[str, Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    created_at: Mapped[datetime] = _ts(nullable=False, default_now=True)


class Worker(Base):
    __tablename__ = "workers"

    worker_id: Mapped[str] = mapped_column(String(200), primary_key=True)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)
    version: Mapped[str] = mapped_column(String(50), nullable=False)
    concurrency: Mapped[int] = mapped_column(Integer, nullable=False)
    handlers: Mapped[list[str]] = mapped_column(JSONB, nullable=False)
    started_at: Mapped[datetime] = _ts(nullable=False, default_now=True)
    last_heartbeat_at: Mapped[datetime] = _ts(nullable=False, default_now=True)
    stopped_at: Mapped[datetime | None] = _ts()


class IdempotencyRecord(Base):
    __tablename__ = "idempotency_records"
    __table_args__ = (
        PrimaryKeyConstraint("tenant_id", "idempotency_key"),
        # Deferred so the key can be reserved before the run row is inserted in
        # the same transaction; see services/runs.py.
        ForeignKeyConstraint(
            ["run_id"],
            ["workflow_runs.id"],
            ondelete="CASCADE",
            deferrable=True,
            initially="DEFERRED",
        ),
    )

    tenant_id: Mapped[str] = mapped_column(String(100), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(200), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False, unique=True)
    created_at: Mapped[datetime] = _ts(nullable=False, default_now=True)
