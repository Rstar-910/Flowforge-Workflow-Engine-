"""Workflow deadlines and task queue timeouts.

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("workflow_runs", sa.Column("deadline_at", sa.DateTime(timezone=True)))
    op.add_column("workflow_runs", sa.Column("deadline_exceeded_at", sa.DateTime(timezone=True)))
    op.add_column("tasks", sa.Column("queue_timeout_seconds", sa.Float()))
    op.create_check_constraint(
        op.f("ck_tasks_queue_timeout_positive"),
        "tasks",
        "queue_timeout_seconds IS NULL OR queue_timeout_seconds > 0",
    )
    op.create_index(
        "ix_workflow_runs_deadline",
        "workflow_runs",
        ["deadline_at"],
        postgresql_where=sa.text("status IN ('PENDING', 'RUNNING') AND deadline_at IS NOT NULL"),
    )
    op.create_index(
        "ix_tasks_queue_timeout",
        "tasks",
        ["ready_at"],
        postgresql_where=sa.text("status = 'READY' AND queue_timeout_seconds IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_tasks_queue_timeout", table_name="tasks")
    op.drop_index("ix_workflow_runs_deadline", table_name="workflow_runs")
    op.drop_constraint(op.f("ck_tasks_queue_timeout_positive"), "tasks", type_="check")
    op.drop_column("tasks", "queue_timeout_seconds")
    op.drop_column("workflow_runs", "deadline_exceeded_at")
    op.drop_column("workflow_runs", "deadline_at")
