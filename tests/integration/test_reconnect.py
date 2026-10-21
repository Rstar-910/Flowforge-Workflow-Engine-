"""Pooled connections closed by the server while idle (restart, idle timeout, kill)."""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from flowforge.db.session import create_engine, retry_if_connection_was_dead
from flowforge.engine.execution import claim_tasks
from flowforge.observability import metrics
from tests.conftest import make_settings
from tests.support.helpers import start_run, task

pytestmark = pytest.mark.integration


async def kill_sessions(db, app_name: str) -> int:
    async with db.connect() as conn:
        return int(
            (
                await conn.execute(
                    text(
                        "SELECT count(*) FILTER (WHERE pg_terminate_backend(pid))"
                        " FROM pg_stat_activity WHERE application_name = :n"
                    ),
                    {"n": app_name},
                )
            ).scalar_one()
        )


async def test_first_use_of_a_server_killed_connection_is_retried_once(db, registry):
    run_id = await start_run(db, registry, [task("a")])
    worker_db = create_engine(
        make_settings(service_name="reconnect-probe"), pool_size=1, pre_ping=False
    )
    try:
        async with worker_db.connect() as conn:  # leave one idle connection in the pool
            await conn.execute(text("SELECT 1"))
        assert await kill_sessions(db, "reconnect-probe") == 1
        before = metrics.DB_RECONNECTS.labels("test")._value.get()

        async def claim():
            return await claim_tasks(
                worker_db, worker_id="w", handlers=["noop"], limit=1, lease_seconds=30
            )

        # Without the helper, this first use fails on the dead pooled connection...
        with pytest.raises(DBAPIError) as exc:
            await claim()
        assert exc.value.connection_invalidated
        async with worker_db.connect() as conn:
            await conn.execute(text("SELECT 1"))
        await kill_sessions(db, "reconnect-probe")
        # ...with it, the same situation costs one immediate retry, not an error.
        claimed = await retry_if_connection_was_dead(claim, "test")
        assert [c.run_id for c in claimed] == [run_id]
        assert metrics.DB_RECONNECTS.labels("test")._value.get() == before + 1
    finally:
        await worker_db.dispose()


async def test_other_database_errors_are_not_retried(db):
    calls = 0

    async def failing():
        nonlocal calls
        calls += 1
        async with db.connect() as conn:
            await conn.execute(text("SELECT 1/0"))

    with pytest.raises(DBAPIError):
        await retry_if_connection_was_dead(failing, "test")
    assert calls == 1  # a real error goes straight to the caller's backoff
