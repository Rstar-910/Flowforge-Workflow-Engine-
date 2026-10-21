"""Initial schema: definitions, runs, tasks, dependencies, attempts, events, workers,
idempotency records.

Revision ID: 0001
Revises:
Create Date: 2026-10-21 17:22:25.193072
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "workers",
        sa.Column("worker_id", sa.String(length=200), nullable=False),
        sa.Column("hostname", sa.String(length=255), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("version", sa.String(length=50), nullable=False),
        sa.Column("concurrency", sa.Integer(), nullable=False),
        sa.Column("handlers", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "last_heartbeat_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("worker_id", name=op.f("pk_workers")),
    )
    op.create_table(
        "workflow_definitions",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("tenant_id", sa.String(length=100), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("definition_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("spec_hash", sa.String(length=64), nullable=False),
        sa.Column("created_by", sa.String(length=200), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflow_definitions")),
        sa.UniqueConstraint(
            "tenant_id",
            "name",
            "version",
            name=op.f("uq_workflow_definitions_tenant_id_name_version"),
        ),
    )
    op.create_table(
        "workflow_runs",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("tenant_id", sa.String(length=100), nullable=False),
        sa.Column("definition_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("input_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("output_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("idempotency_key", sa.String(length=200), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(length=200), nullable=False),
        sa.Column("traceparent", sa.String(length=64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancellation_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')) = (completed_at IS NOT NULL)",
            name=op.f("ck_workflow_runs_completed_at_iff_terminal"),
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLING', 'CANCELLED')",
            name=op.f("ck_workflow_runs_status_valid"),
        ),
        sa.ForeignKeyConstraint(
            ["definition_id"],
            ["workflow_definitions.id"],
            name=op.f("fk_workflow_runs_definition_id_workflow_definitions"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_workflow_runs")),
    )
    op.create_index(
        "ix_workflow_runs_active",
        "workflow_runs",
        ["status"],
        unique=False,
        postgresql_where=sa.text("status IN ('PENDING', 'RUNNING', 'CANCELLING')"),
    )
    op.create_index(
        op.f("ix_workflow_runs_definition_id"), "workflow_runs", ["definition_id"], unique=False
    )
    op.create_index(
        "ix_workflow_runs_tenant_created",
        "workflow_runs",
        ["tenant_id", "created_at", "id"],
        unique=False,
    )
    op.create_table(
        "idempotency_records",
        sa.Column("tenant_id", sa.String(length=100), nullable=False),
        sa.Column("idempotency_key", sa.String(length=200), nullable=False),
        sa.Column("request_hash", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["workflow_runs.id"],
            name=op.f("fk_idempotency_records_run_id_workflow_runs"),
            ondelete="CASCADE",
            initially="DEFERRED",
            deferrable=True,
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id", "idempotency_key", name=op.f("pk_idempotency_records")
        ),
        sa.UniqueConstraint("run_id", name=op.f("uq_idempotency_records_run_id")),
    )
    op.create_table(
        "tasks",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("task_key", sa.String(length=100), nullable=False),
        sa.Column("handler", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("input_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("output_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("timeout_seconds", sa.Float(), nullable=False),
        sa.Column("retry_policy", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("attempt_limit", sa.Integer(), nullable=False),
        sa.Column("current_attempt_id", sa.UUID(), nullable=True),
        sa.Column("lease_owner", sa.String(length=200), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ready_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_type", sa.String(length=200), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "(status = 'RUNNING') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL AND current_attempt_id IS NOT NULL)",
            name=op.f("ck_tasks_lease_iff_running"),
        ),
        sa.CheckConstraint(
            "(status IN ('SUCCEEDED', 'FAILED', 'UPSTREAM_FAILED', 'CANCELLED')) = (completed_at IS NOT NULL)",
            name=op.f("ck_tasks_completed_at_iff_terminal"),
        ),
        sa.CheckConstraint(
            "status <> 'RETRY_WAIT' OR next_attempt_at IS NOT NULL",
            name=op.f("ck_tasks_retry_wait_has_time"),
        ),
        sa.CheckConstraint(
            "status IN ('BLOCKED', 'READY', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'FAILED', 'UPSTREAM_FAILED', 'CANCELLED')",
            name=op.f("ck_tasks_status_valid"),
        ),
        sa.CheckConstraint(
            "attempt_count <= attempt_limit", name=op.f("ck_tasks_attempts_bounded")
        ),
        sa.CheckConstraint("attempt_count >= 0", name=op.f("ck_tasks_attempt_count_nonneg")),
        sa.CheckConstraint("timeout_seconds > 0", name=op.f("ck_tasks_timeout_positive")),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["workflow_runs.id"],
            name=op.f("fk_tasks_run_id_workflow_runs"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_tasks")),
        sa.UniqueConstraint("run_id", "task_key", name=op.f("uq_tasks_run_id_task_key")),
    )
    op.create_index(
        "ix_tasks_lease_expiry",
        "tasks",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("status = 'RUNNING'"),
    )
    op.create_index(
        "ix_tasks_ready",
        "tasks",
        ["ready_at"],
        unique=False,
        postgresql_where=sa.text("status = 'READY'"),
    )
    op.create_index(
        "ix_tasks_retry_due",
        "tasks",
        ["next_attempt_at"],
        unique=False,
        postgresql_where=sa.text("status = 'RETRY_WAIT'"),
    )
    op.create_index("ix_tasks_run_status", "tasks", ["run_id", "status"], unique=False)
    op.create_table(
        "task_attempts",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(length=200), nullable=False),
        sa.Column("outcome", sa.String(length=20), nullable=False),
        sa.Column("retryable", sa.Boolean(), nullable=True),
        sa.Column("error_type", sa.String(length=200), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("queue_wait_ms", sa.Integer(), nullable=True),
        sa.Column("duration_ms", sa.Integer(), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "(outcome = 'RUNNING') = (ended_at IS NULL)",
            name=op.f("ck_task_attempts_ended_at_iff_finished"),
        ),
        sa.CheckConstraint(
            "outcome IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'TIMED_OUT', 'LEASE_EXPIRED', 'CANCELLED')",
            name=op.f("ck_task_attempts_outcome_valid"),
        ),
        sa.CheckConstraint(
            "attempt_number >= 1", name=op.f("ck_task_attempts_attempt_number_positive")
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["workflow_runs.id"],
            name=op.f("fk_task_attempts_run_id_workflow_runs"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            name=op.f("fk_task_attempts_task_id_tasks"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_task_attempts")),
        sa.UniqueConstraint(
            "task_id", "attempt_number", name=op.f("uq_task_attempts_task_id_attempt_number")
        ),
    )
    op.create_index("ix_task_attempts_run", "task_attempts", ["run_id"], unique=False)
    op.create_table(
        "task_dependencies",
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("depends_on_task_id", sa.UUID(), nullable=False),
        sa.CheckConstraint(
            "task_id <> depends_on_task_id", name=op.f("ck_task_dependencies_no_self_dependency")
        ),
        sa.ForeignKeyConstraint(
            ["depends_on_task_id"],
            ["tasks.id"],
            name=op.f("fk_task_dependencies_depends_on_task_id_tasks"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["task_id"],
            ["tasks.id"],
            name=op.f("fk_task_dependencies_task_id_tasks"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("task_id", "depends_on_task_id", name=op.f("pk_task_dependencies")),
    )
    op.create_index(
        "ix_task_dependencies_depends_on", "task_dependencies", ["depends_on_task_id"], unique=False
    )
    op.create_table(
        "task_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(always=True), nullable=False),
        sa.Column("run_id", sa.UUID(), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=True),
        sa.Column("attempt_id", sa.UUID(), nullable=True),
        sa.Column("event_type", sa.String(length=50), nullable=False),
        sa.Column(
            "data",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["run_id"],
            ["workflow_runs.id"],
            name=op.f("fk_task_events_run_id_workflow_runs"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["task_id"], ["tasks.id"], name=op.f("fk_task_events_task_id_tasks"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_task_events")),
    )
    op.create_index("ix_task_events_run_id", "task_events", ["run_id", "id"], unique=False)
    op.create_index(op.f("ix_task_events_task_id"), "task_events", ["task_id"], unique=False)

    # task_events is an append-only audit trail; definitions are immutable once
    # stored (publish a new version instead). Enforced in the database so that
    # no code path can rewrite history.
    op.execute(
        """
        CREATE FUNCTION flowforge_reject_update() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION '% rows are immutable', TG_TABLE_NAME
                USING ERRCODE = 'restrict_violation';
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    op.execute(
        "CREATE TRIGGER task_events_append_only BEFORE UPDATE ON task_events "
        "FOR EACH ROW EXECUTE FUNCTION flowforge_reject_update()"
    )
    op.execute(
        "CREATE TRIGGER workflow_definitions_immutable BEFORE UPDATE ON workflow_definitions "
        "FOR EACH ROW EXECUTE FUNCTION flowforge_reject_update()"
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS workflow_definitions_immutable ON workflow_definitions")
    op.execute("DROP TRIGGER IF EXISTS task_events_append_only ON task_events")
    op.execute("DROP FUNCTION IF EXISTS flowforge_reject_update()")
    op.drop_index(op.f("ix_task_events_task_id"), table_name="task_events")
    op.drop_index("ix_task_events_run_id", table_name="task_events")
    op.drop_table("task_events")
    op.drop_index("ix_task_dependencies_depends_on", table_name="task_dependencies")
    op.drop_table("task_dependencies")
    op.drop_index("ix_task_attempts_run", table_name="task_attempts")
    op.drop_table("task_attempts")
    op.drop_index("ix_tasks_run_status", table_name="tasks")
    op.drop_index(
        "ix_tasks_retry_due", table_name="tasks", postgresql_where=sa.text("status = 'RETRY_WAIT'")
    )
    op.drop_index(
        "ix_tasks_ready", table_name="tasks", postgresql_where=sa.text("status = 'READY'")
    )
    op.drop_index(
        "ix_tasks_lease_expiry", table_name="tasks", postgresql_where=sa.text("status = 'RUNNING'")
    )
    op.drop_table("tasks")
    op.drop_table("idempotency_records")
    op.drop_index("ix_workflow_runs_tenant_created", table_name="workflow_runs")
    op.drop_index(op.f("ix_workflow_runs_definition_id"), table_name="workflow_runs")
    op.drop_index(
        "ix_workflow_runs_active",
        table_name="workflow_runs",
        postgresql_where=sa.text("status IN ('PENDING', 'RUNNING', 'CANCELLING')"),
    )
    op.drop_table("workflow_runs")
    op.drop_table("workflow_definitions")
    op.drop_table("workers")
