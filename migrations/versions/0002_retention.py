"""Retention support: events_pruned_at marker and indexes for finding old finished runs.

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-21
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

FINISHED = "status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')"


def upgrade() -> None:
    op.add_column(
        "workflow_runs",
        sa.Column("events_pruned_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_workflow_runs_finished",
        "workflow_runs",
        ["completed_at"],
        unique=False,
        postgresql_where=sa.text(FINISHED),
    )
    op.create_index(
        "ix_workflow_runs_events_unpruned",
        "workflow_runs",
        ["completed_at"],
        unique=False,
        postgresql_where=sa.text(f"{FINISHED} AND events_pruned_at IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_workflow_runs_events_unpruned", table_name="workflow_runs")
    op.drop_index("ix_workflow_runs_finished", table_name="workflow_runs")
    op.drop_column("workflow_runs", "events_pruned_at")
