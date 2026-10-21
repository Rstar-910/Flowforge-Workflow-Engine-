"""The worker process: claim loop, heartbeat loop, execution and graceful shutdown."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import json
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass, field

from pydantic import ValidationError
from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine

from flowforge.config import Settings
from flowforge.db.session import retry_if_connection_was_dead
from flowforge.domain.definition import json_size
from flowforge.domain.errors import (
    FailureInfo,
    HandlerNotFoundError,
    InputValidationError,
    PermanentTaskError,
    TaskCancelledError,
    classify_exception,
)
from flowforge.engine.execution import (
    claim_tasks,
    complete_attempt,
    mark_worker_stopped,
    register_worker,
    renew_leases,
)
from flowforge.engine.types import Cancelled, ClaimedTask, CompletionOutcome, Failed, Succeeded
from flowforge.engine.types import TaskResult as EngineResult
from flowforge.faults import FaultInjector
from flowforge.observability import metrics
from flowforge.observability.logging import bind_log_context, log_context, log_event
from flowforge.observability.tracing import context_from_traceparent, tracer
from flowforge.worker.context import TaskContext
from flowforge.worker.executor import HandlerExecutor
from flowforge.worker.registry import HandlerRegistry

logger = logging.getLogger(__name__)

# Errors that indicate the database is unreachable or the connection broke.
DB_ERRORS: tuple[type[BaseException], ...] = (
    OperationalError,
    InterfaceError,
    DBAPIError,
    OSError,
    ConnectionError,
    asyncio.TimeoutError,
)

REASON_CANCEL_REQUESTED = "cancel_requested"
REASON_LEASE_LOST = "lease_lost"
REASON_SHUTDOWN = "worker_shutdown"


@dataclass
class InFlight:
    claimed: ClaimedTask
    ctx: TaskContext
    lease_deadline: float  # local monotonic time; conservative (see heartbeat)
    lease_lost: bool = False
    task: asyncio.Task[None] | None = field(default=None, repr=False)


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


class Worker:
    def __init__(
        self,
        engine: AsyncEngine,
        settings: Settings,
        registry: HandlerRegistry,
        *,
        worker_id: str | None = None,
        concurrency: int = 4,
        faults: FaultInjector | None = None,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be >= 1")
        self.engine = engine
        self.settings = settings
        self.registry = registry
        self.worker_id = worker_id or default_worker_id()
        self.concurrency = concurrency
        self.faults = faults or FaultInjector(settings.fault_point_set())
        self.executor = HandlerExecutor(max_threads=concurrency)
        self.inflight: dict[uuid.UUID, InFlight] = {}
        self._wakeup = asyncio.Event()
        self.completed = 0

    # ------------------------------------------------------------------ main
    async def run(self, stop: asyncio.Event) -> None:
        bind_log_context(worker_id=self.worker_id)
        await self._register(stop)
        log_event(
            logger,
            logging.INFO,
            "worker started",
            event_type="worker_started",
            concurrency=self.concurrency,
            handlers=self.registry.names(),
        )
        heartbeat = asyncio.create_task(self._heartbeat_loop(), name="heartbeat")
        try:
            await self._claim_loop(stop)
        finally:
            await self._drain()
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            try:
                await mark_worker_stopped(self.engine, self.worker_id)
            except DB_ERRORS:
                logger.warning("could not mark worker stopped (database unavailable)")
            self.executor.shutdown()
            log_event(logger, logging.INFO, "worker stopped", event_type="worker_stopped")

    async def _register(self, stop: asyncio.Event) -> None:
        delay = self.settings.worker_poll_interval_seconds
        while not stop.is_set():
            try:
                await retry_if_connection_was_dead(
                    lambda: register_worker(
                        self.engine,
                        self.worker_id,
                        pid=os.getpid(),
                        concurrency=self.concurrency,
                        handlers=self.registry.names(),
                    ),
                    "worker",
                )
                return
            except DB_ERRORS as exc:
                metrics.DB_ERRORS.labels("worker").inc()
                logger.warning("worker registration failed, retrying: %s", type(exc).__name__)
                await _wait(stop, delay)
                delay = min(delay * 2, 10.0)

    def capacity(self) -> int:
        return self.concurrency - len(self.inflight) - self.executor.zombies.count

    async def _claim_loop(self, stop: asyncio.Event) -> None:
        base = self.settings.worker_poll_interval_seconds
        idle_delay = base
        error_streak = 0
        while not stop.is_set():
            free = self.capacity()
            if free <= 0:
                self._wakeup.clear()
                await _wait_any(stop, self._wakeup, timeout=base)
                continue
            claim_started = time.monotonic()
            try:
                claimed = await retry_if_connection_was_dead(
                    functools.partial(
                        claim_tasks,
                        self.engine,
                        worker_id=self.worker_id,
                        handlers=self.registry.names(),
                        limit=free,
                        lease_seconds=self.settings.lease_seconds,
                    ),
                    "worker",
                )
                error_streak = 0
            except DB_ERRORS as exc:
                error_streak += 1
                metrics.DB_ERRORS.labels("worker").inc()
                backoff = min(base * 2**error_streak, 10.0)
                log_event(
                    logger,
                    logging.WARNING,
                    "claim failed; backing off",
                    event_type="claim_failed",
                    error_type=type(exc).__name__,
                    backoff_seconds=backoff,
                )
                await _wait(stop, backoff)
                continue
            if claimed:
                self.faults.crash_if("crash_after_claim")
                for c in claimed:
                    self._start(c, claim_started)
                idle_delay = base
                continue
            await _wait_any(stop, self._wakeup, timeout=idle_delay)
            self._wakeup.clear()
            idle_delay = min(idle_delay * 1.5, self.settings.worker_max_poll_interval_seconds)

    def _start(self, claimed: ClaimedTask, claim_started: float) -> None:
        ctx = TaskContext(
            task_id=claimed.task_id,
            run_id=claimed.run_id,
            task_key=claimed.task_key,
            attempt_id=claimed.attempt_id,
            attempt_number=claimed.attempt_number,
            input=claimed.input,
            run_input=claimed.run_input,
            dependency_outputs=claimed.dependency_outputs,
            timeout_seconds=claimed.timeout_seconds,
        )
        # The database computed expiry as now()+lease when the claim executed,
        # which is no earlier than claim_started on our clock: a conservative bound.
        entry = InFlight(claimed, ctx, lease_deadline=claim_started + self.settings.lease_seconds)
        self.inflight[claimed.attempt_id] = entry
        metrics.RUNNING_TASKS.set(len(self.inflight))
        entry.task = asyncio.create_task(self._execute(entry), name=f"task-{claimed.task_key}")

    # ------------------------------------------------------------- execution
    async def _execute(self, entry: InFlight) -> None:
        c = entry.claimed
        with (
            log_context(
                workflow_run_id=c.run_id,
                task_id=c.task_id,
                task_key=c.task_key,
                attempt_id=c.attempt_id,
            ),
            tracer.start_as_current_span(
                "flowforge.task.execute",
                context=context_from_traceparent(c.traceparent),
                attributes={
                    "flowforge.handler": c.handler,
                    "flowforge.task_key": c.task_key,
                    "flowforge.run_id": str(c.run_id),
                    "flowforge.attempt": c.attempt_number,
                },
            ),
        ):
            try:
                log_event(
                    logger,
                    logging.INFO,
                    "attempt started",
                    event_type="attempt_started",
                    handler=c.handler,
                    attempt_number=c.attempt_number,
                    queue_wait_ms=int(c.queue_wait_seconds * 1000),
                )
                started = time.monotonic()
                result = await self._invoke(entry)
                duration = time.monotonic() - started
                metrics.TASK_EXECUTION.labels(c.handler).observe(duration)
                # Side effects (if any) have happened; the result is not yet durable.
                self.faults.crash_if("crash_after_handler")
                outcome = await self._commit(entry, result, int(duration * 1000))
                if outcome is not None and outcome.reason == "already_recorded":
                    log_event(
                        logger,
                        logging.INFO,
                        "commit acknowledgement was lost; the result was already recorded",
                        event_type="commit_already_recorded",
                    )
                if outcome is not None and not outcome.accepted:
                    log_event(
                        logger,
                        logging.WARNING,
                        "result rejected: lease no longer held by this attempt",
                        event_type="stale_result_rejected",
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # never let one task kill the worker
                logger.exception("unexpected error while executing task")
            finally:
                self.inflight.pop(c.attempt_id, None)
                metrics.RUNNING_TASKS.set(len(self.inflight))
                self.completed += 1
                self._wakeup.set()

    async def _invoke(self, entry: InFlight) -> EngineResult:
        c, ctx = entry.claimed, entry.ctx
        spec = self.registry.get(c.handler)
        try:
            if spec is None:
                raise HandlerNotFoundError(f"no handler registered for {c.handler!r}")
            if spec.input_model is not None:
                try:
                    ctx.input = spec.input_model.model_validate(c.input)
                except ValidationError as exc:
                    raise InputValidationError(
                        f"input does not match {spec.input_model.__name__}: "
                        f"{exc.error_count()} error(s)"
                    ) from exc
            output = await self.executor.run(spec, ctx, c.timeout_seconds)
            return Succeeded(self._check_output(output))
        except TaskCancelledError as exc:
            reason = ctx.cancel_reason or str(exc)
            if reason == REASON_CANCEL_REQUESTED:
                return Cancelled(reason)
            # Lease loss or shutdown is not a business failure: record a retryable
            # failure. If the lease is truly gone the fenced commit rejects it.
            name = "WorkerShutdown" if reason == REASON_SHUTDOWN else "LeaseLost"
            return Failed(FailureInfo(name, reason, retryable=True))
        except Exception as exc:
            return Failed(classify_exception(exc, c.retry_policy))

    def _check_output(self, output: object) -> object:
        try:
            json.dumps(output)
        except (TypeError, ValueError) as exc:
            raise PermanentTaskError(f"handler output is not JSON serialisable: {exc}") from exc
        if json_size(output) > self.settings.max_json_value_bytes:
            raise PermanentTaskError(
                f"handler output exceeds {self.settings.max_json_value_bytes} bytes"
            )
        return output

    async def _commit(
        self, entry: InFlight, result: EngineResult, duration_ms: int
    ) -> CompletionOutcome | None:
        """Persist the result, retrying through transient database failures.

        The commit is fenced on the attempt id inside the database, so retrying
        is safe; if the lease was recovered in the meantime the result is
        rejected and recorded as stale.
        """
        delay = 0.2
        give_up_at = time.monotonic() + max(60.0, self.settings.lease_seconds * 4)
        while True:
            try:
                return await retry_if_connection_was_dead(
                    lambda: complete_attempt(
                        self.engine,
                        claimed=entry.claimed,
                        worker_id=self.worker_id,
                        result=result,
                        duration_ms=duration_ms,
                    ),
                    "worker",
                )
            except DB_ERRORS as exc:
                metrics.DB_ERRORS.labels("worker").inc()
                if time.monotonic() > give_up_at:
                    log_event(
                        logger,
                        logging.ERROR,
                        "giving up persisting result; lease recovery will re-run the task",
                        event_type="commit_abandoned",
                        error_type=type(exc).__name__,
                    )
                    return None
                logger.warning("commit failed (%s); retrying in %.1fs", type(exc).__name__, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)

    # ------------------------------------------------------------- heartbeat
    async def _heartbeat_loop(self) -> None:
        interval = self.settings.heartbeat_interval_seconds
        while True:
            await asyncio.sleep(interval)
            if self.faults.active("skip_heartbeats"):
                continue
            await self.heartbeat_once()

    async def heartbeat_once(self) -> None:
        entries = list(self.inflight.values())
        sent_at = time.monotonic()
        try:
            renewal = await retry_if_connection_was_dead(
                lambda: renew_leases(
                    self.engine,
                    worker_id=self.worker_id,
                    attempt_ids=[e.claimed.attempt_id for e in entries],
                    lease_seconds=self.settings.lease_seconds,
                ),
                "worker",
            )
        except DB_ERRORS as exc:
            metrics.LEASE_RENEWAL_FAILURES.labels("db_error").inc()
            metrics.DB_ERRORS.labels("worker").inc()
            logger.warning("lease renewal failed: %s", type(exc).__name__)
            now = time.monotonic()
            for e in entries:
                if now >= e.lease_deadline and not e.lease_lost:
                    # We can no longer prove ownership; stop doing work. The
                    # commit stays fenced, so this is about wasted effort only.
                    e.lease_lost = True
                    e.ctx.request_cancel(REASON_LEASE_LOST)
            return
        for e in entries:
            if e.claimed.attempt_id in renewal.renewed:
                e.lease_deadline = sent_at + self.settings.lease_seconds
                if e.claimed.attempt_id in renewal.cancel_requested:
                    e.ctx.request_cancel(REASON_CANCEL_REQUESTED)
            elif e.claimed.attempt_id in self.inflight and not e.lease_lost:
                e.lease_lost = True
                metrics.LEASE_RENEWAL_FAILURES.labels("lost").inc()
                log_event(
                    logger,
                    logging.WARNING,
                    "lease lost; signalling handler to stop",
                    event_type="lease_lost",
                    attempt_id=str(e.claimed.attempt_id),
                    task_key=e.claimed.task_key,
                )
                e.ctx.request_cancel(REASON_LEASE_LOST)

    # -------------------------------------------------------------- shutdown
    async def _drain(self) -> None:
        if not self.inflight:
            return
        grace = self.settings.worker_shutdown_grace_seconds
        log_event(
            logger,
            logging.INFO,
            "draining in-flight tasks",
            event_type="worker_draining",
            inflight=len(self.inflight),
            grace_seconds=grace,
        )
        tasks = [e.task for e in self.inflight.values() if e.task is not None]
        _, pending = await asyncio.wait(tasks, timeout=grace) if tasks else (set(), set())
        if not pending:
            return
        for e in list(self.inflight.values()):
            e.ctx.request_cancel(REASON_SHUTDOWN)
        _, pending = await asyncio.wait(pending, timeout=10)
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


async def _wait(stop: asyncio.Event, timeout: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout)


async def _wait_any(stop: asyncio.Event, other: asyncio.Event, *, timeout: float) -> None:
    waiters = [asyncio.ensure_future(stop.wait()), asyncio.ensure_future(other.wait())]
    try:
        await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for w in waiters:
            w.cancel()
