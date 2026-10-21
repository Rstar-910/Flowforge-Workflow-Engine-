"""Retention: bound the growth of finished-run history and the worker registry.

Run by the scheduler every ``retention_interval_seconds``. Every operation is
one short statement over at most ``batch`` runs, so locks are held briefly and a
large backlog is worked off over several cycles instead of in one long
transaction.

Only *finished* runs (SUCCEEDED, FAILED, CANCELLED) are ever touched, however
old an active run is. Run rows are locked with ``FOR NO KEY UPDATE SKIP LOCKED``
(the engine's run-lock mode, ADR 0005), so retention never waits for, and never
races with, a transaction that holds the run, e.g. an admin's manual retry
re-opening a FAILED run.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from flowforge.config import Settings
from flowforge.observability import metrics
from flowforge.observability.logging import log_event

logger = logging.getLogger(__name__)

DAY = 86_400.0
MAX_BATCHES_PER_CYCLE = 20  # bound the work (and lock churn) of one scheduler cycle

_DELETE_RUNS_SQL = text(
    """
    WITH victims AS (
        SELECT id FROM workflow_runs
         WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
           AND completed_at < statement_timestamp() - make_interval(secs => :age)
         ORDER BY completed_at
         LIMIT :batch
           FOR NO KEY UPDATE SKIP LOCKED
    )
    -- ON DELETE CASCADE removes tasks, dependencies, attempts, events and
    -- idempotency records of these runs in the same statement.
    DELETE FROM workflow_runs r USING victims v WHERE r.id = v.id
    RETURNING r.id
    """
)

_PRUNE_EVENTS_SQL = text(
    """
    WITH victims AS (
        SELECT id FROM workflow_runs
         WHERE status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
           AND events_pruned_at IS NULL
           AND completed_at < statement_timestamp() - make_interval(secs => :age)
         ORDER BY completed_at
         LIMIT :batch
           FOR NO KEY UPDATE SKIP LOCKED
    ), deleted AS (
        DELETE FROM task_events e USING victims v WHERE e.run_id = v.id RETURNING 1
    ), marked AS (
        UPDATE workflow_runs r SET events_pruned_at = statement_timestamp()
          FROM victims v WHERE r.id = v.id
        RETURNING 1
    )
    SELECT (SELECT count(*) FROM marked) AS runs, (SELECT count(*) FROM deleted) AS events
    """
)

_DELETE_WORKERS_SQL = text(
    """
    DELETE FROM workers
     WHERE last_heartbeat_at < statement_timestamp() - make_interval(secs => :age)
    RETURNING worker_id
    """
)


@dataclass
class RetentionStats:
    runs_deleted: int = 0
    runs_events_pruned: int = 0
    events_deleted: int = 0
    workers_deleted: int = 0

    @property
    def total(self) -> int:
        return self.runs_deleted + self.events_deleted + self.workers_deleted


async def delete_finished_runs(engine: AsyncEngine, *, older_than_days: float, batch: int) -> int:
    deleted = 0
    for _ in range(MAX_BATCHES_PER_CYCLE):
        with metrics.time_db_operation("retention_delete_runs"):
            async with engine.begin() as conn:
                n = len(
                    (
                        await conn.execute(
                            _DELETE_RUNS_SQL, {"age": older_than_days * DAY, "batch": batch}
                        )
                    ).all()
                )
        deleted += n
        if n < batch:
            break
    return deleted


async def prune_events(
    engine: AsyncEngine, *, older_than_days: float, batch: int
) -> tuple[int, int]:
    runs = events = 0
    for _ in range(MAX_BATCHES_PER_CYCLE):
        with metrics.time_db_operation("retention_prune_events"):
            async with engine.begin() as conn:
                row = (
                    await conn.execute(
                        _PRUNE_EVENTS_SQL, {"age": older_than_days * DAY, "batch": batch}
                    )
                ).one()
        runs += row.runs
        events += row.events
        if row.runs < batch:
            break
    return runs, events


async def delete_stale_workers(engine: AsyncEngine, *, older_than_hours: float) -> int:
    """Remove registry rows of workers that stopped, or went silent, long ago.

    A worker that is still alive re-registers on restart and refreshes its row on
    every heartbeat, so only genuinely gone workers age out.
    """
    async with engine.begin() as conn:
        return len(
            (await conn.execute(_DELETE_WORKERS_SQL, {"age": older_than_hours * 3600})).all()
        )


async def apply_retention(engine: AsyncEngine, settings: Settings) -> RetentionStats:
    stats = RetentionStats()
    batch = settings.retention_batch_size
    if settings.run_retention_days is not None:
        stats.runs_deleted = await delete_finished_runs(
            engine, older_than_days=settings.run_retention_days, batch=batch
        )
    if settings.event_retention_days is not None:
        stats.runs_events_pruned, stats.events_deleted = await prune_events(
            engine, older_than_days=settings.event_retention_days, batch=batch
        )
    stats.workers_deleted = await delete_stale_workers(
        engine, older_than_hours=settings.worker_retention_hours
    )

    metrics.RETENTION_DELETED.labels("runs").inc(stats.runs_deleted)
    metrics.RETENTION_DELETED.labels("events").inc(stats.events_deleted)
    metrics.RETENTION_DELETED.labels("workers").inc(stats.workers_deleted)
    if stats.total:
        log_event(
            logger,
            logging.INFO,
            "retention applied",
            event_type="retention_applied",
            runs_deleted=stats.runs_deleted,
            runs_events_pruned=stats.runs_events_pruned,
            events_deleted=stats.events_deleted,
            workers_deleted=stats.workers_deleted,
        )
    return stats
