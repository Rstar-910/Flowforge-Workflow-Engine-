"""The scheduler / recovery process.

Each cycle (all state lives in PostgreSQL; the scheduler keeps none):

1. RETRY_WAIT tasks whose ``next_attempt_at`` passed -> READY.
2. RUNNING tasks whose lease expired -> RETRY_WAIT / READY / FAILED / CANCELLED.
3. Every ``reconcile_every`` cycles: repair runs whose progression is out of
   date (safety net; normally a no-op).
4. Sample queue-depth and worker-health gauges.

Running several schedulers is safe (see engine/recovery.py) but not useful.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass

from sqlalchemy.ext.asyncio import AsyncEngine

from flowforge.config import Settings
from flowforge.engine.recovery import (
    promote_due_retries,
    reconcile_runs,
    recover_expired_leases,
    sample_gauges,
)
from flowforge.observability import metrics
from flowforge.observability.logging import log_event
from flowforge.worker.runtime import DB_ERRORS

logger = logging.getLogger(__name__)


@dataclass
class CycleStats:
    retries_promoted: int = 0
    leases_recovered: int = 0
    runs_reconciled: int = 0


class Scheduler:
    def __init__(self, engine: AsyncEngine, settings: Settings, *, reconcile_every: int = 5):
        self.engine = engine
        self.settings = settings
        self.reconcile_every = max(1, reconcile_every)
        self.cycles = 0

    async def cycle(self, *, reconcile: bool | None = None) -> CycleStats:
        start = time.perf_counter()
        stats = CycleStats()
        batch = self.settings.scheduler_batch_size
        stats.retries_promoted = await promote_due_retries(self.engine, limit=batch)
        stats.leases_recovered = len(await recover_expired_leases(self.engine, limit=batch))
        if reconcile if reconcile is not None else self.cycles % self.reconcile_every == 0:
            stats.runs_reconciled = await reconcile_runs(self.engine, limit=batch)
        await sample_gauges(self.engine, lease_seconds=self.settings.lease_seconds)
        self.cycles += 1
        metrics.SCHEDULER_CYCLE.observe(time.perf_counter() - start)
        if stats.retries_promoted or stats.leases_recovered or stats.runs_reconciled:
            log_event(
                logger,
                logging.INFO,
                "scheduler cycle",
                event_type="scheduler_cycle",
                retries_promoted=stats.retries_promoted,
                leases_recovered=stats.leases_recovered,
                runs_reconciled=stats.runs_reconciled,
                duration_ms=int((time.perf_counter() - start) * 1000),
            )
        return stats

    async def run(self, stop: asyncio.Event) -> None:
        log_event(logger, logging.INFO, "scheduler started", event_type="scheduler_started")
        interval = self.settings.scheduler_interval_seconds
        error_streak = 0
        while not stop.is_set():
            try:
                stats = await self.cycle()
                error_streak = 0
                # Stay busy while there is a backlog; otherwise sleep.
                busy = (
                    stats.retries_promoted >= self.settings.scheduler_batch_size
                    or stats.leases_recovered >= self.settings.scheduler_batch_size
                )
                delay = 0.0 if busy else interval
            except DB_ERRORS as exc:
                error_streak += 1
                metrics.DB_ERRORS.labels("scheduler").inc()
                delay = min(interval * 2**error_streak, 15.0)
                log_event(
                    logger,
                    logging.WARNING,
                    "scheduler cycle failed; backing off",
                    event_type="scheduler_cycle_failed",
                    error_type=type(exc).__name__,
                    backoff_seconds=delay,
                )
            if delay:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), delay)
        log_event(logger, logging.INFO, "scheduler stopped", event_type="scheduler_stopped")
