"""Async engine and transaction helpers."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

from sqlalchemy import event
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from flowforge.config import Settings


def create_engine(
    settings: Settings,
    *,
    pool_size: int | None = None,
    pre_ping: bool = True,
    narrow_lookups_only: bool = False,
) -> AsyncEngine:
    """Create the async engine.

    ``pre_ping`` costs one round trip per connection checkout. The API keeps it,
    so the first request after a database restart does not fail. Workers and the
    scheduler turn it off: every loop already retries on connection errors, and
    SQLAlchemy discards a connection once it reports a disconnect.

    ``narrow_lookups_only`` sets ``enable_bitmapscan = off`` for the session.
    Worker and scheduler queries only ever fetch a few rows (primary key,
    small index range, or ORDER BY ... LIMIT on a partial index). With stale or
    missing statistics (a new or truncated database, before autoanalyze) the
    planner chose bitmap scans for the claim query; a bitmap scan visits every
    dead index entry and, unlike a plain index scan, never marks them dead, so
    its cost grew with churn: about 100x slower than the index scan (ADR 0012).
    """
    server_settings = {
        "application_name": settings.service_name,
        "statement_timeout": str(settings.db_statement_timeout_ms),
        # Guard against forgotten open transactions holding locks.
        "idle_in_transaction_session_timeout": "60000",
        "timezone": "UTC",
    }
    if narrow_lookups_only:
        server_settings["enable_bitmapscan"] = "off"
    engine = create_async_engine(
        settings.database_url.get_secret_value(),
        pool_size=pool_size or settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=pre_ping,
        pool_recycle=1800,
        pool_timeout=settings.db_connect_timeout_seconds * 2,
        connect_args={
            "timeout": settings.db_connect_timeout_seconds,
            "command_timeout": settings.db_statement_timeout_ms / 1000 + 5,
            "server_settings": server_settings,
        },
    )
    _install_query_timing(engine)
    return engine


def _install_query_timing(engine: AsyncEngine) -> None:
    import time

    from flowforge.observability.metrics import DB_QUERY_DURATION

    @event.listens_for(engine.sync_engine, "before_cursor_execute")
    def _before(conn, cursor, statement, parameters, context, executemany):  # type: ignore[no-untyped-def]
        conn.info.setdefault("_ff_query_start", []).append(time.perf_counter())

    @event.listens_for(engine.sync_engine, "after_cursor_execute")
    def _after(conn, cursor, statement, parameters, context, executemany):  # type: ignore[no-untyped-def]
        starts = conn.info.get("_ff_query_start")
        if starts:
            DB_QUERY_DURATION.observe(time.perf_counter() - starts.pop())


@asynccontextmanager
async def transaction(engine: AsyncEngine) -> AsyncIterator[AsyncConnection]:
    """A short READ COMMITTED transaction on a pooled connection."""
    async with engine.begin() as conn:
        yield conn


async def retry_if_connection_was_dead[T](op: Callable[[], Awaitable[T]], component: str) -> T:
    """Run ``op``; if it failed only because its pooled connection was already dead, retry once.

    Workers and the scheduler run without pool pre-ping (ADR 0011). A connection the
    server closed while it sat idle in the pool (PostgreSQL restart, an idle timeout,
    a dropped TCP connection, pg_terminate_backend) therefore fails on its next use.
    SQLAlchemy flags that case with ``connection_invalidated`` and discards the
    connection; it is not evidence that the database is down, so one immediate retry
    on a fresh connection is right, instead of entering exponential backoff. Any
    other error, or a second failure, propagates to the caller's backoff.

    Only use for operations that are safe to repeat: claims (one atomic statement),
    lease renewal (idempotent), fenced completion (recognises its own committed
    result), and scheduler sweeps (each step re-checks state under lock).
    """
    from flowforge.observability import metrics

    try:
        return await op()
    except DBAPIError as exc:
        if not exc.connection_invalidated:
            raise
        metrics.DB_RECONNECTS.labels(component).inc()
        return await op()
