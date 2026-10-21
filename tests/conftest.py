"""Shared fixtures.

Integration tests run against a dedicated database (default ``flowforge_test``
on the docker-compose PostgreSQL at localhost:5433). The database is dropped
and recreated once per session and only ever if its name starts with
``flowforge_test``, so a developer's working database is never touched.
Tables are truncated before each test.

If PostgreSQL is unreachable, integration tests are skipped with an explicit
reason - unless ``FLOWFORGE_REQUIRE_DB=1`` (set in CI), in which case they fail.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from flowforge.config import Settings
from flowforge.db.session import create_engine

ROOT = Path(__file__).resolve().parents[1]
TEST_DB_URL = os.environ.get(
    "FLOWFORGE_TEST_DATABASE_URL",
    "postgresql+asyncpg://flowforge:flowforge@localhost:5433/flowforge_test",
)
REQUIRE_DB = os.environ.get("FLOWFORGE_REQUIRE_DB") == "1"
TABLES = (
    "task_events, task_attempts, task_dependencies, tasks, idempotency_records, "
    "workflow_runs, workflow_definitions, workers"
)


def make_settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "database_url": TEST_DB_URL,
        "api_keys": "admin-key:tenant-a:admin,user-key:tenant-a:user,other-key:tenant-b:user",
        "lease_seconds": 2.0,
        "heartbeat_interval_seconds": 0.5,
        "worker_poll_interval_seconds": 0.05,
        "worker_max_poll_interval_seconds": 0.2,
        "scheduler_interval_seconds": 0.2,
        "worker_shutdown_grace_seconds": 5.0,
        "log_format": "text",
        "db_pool_size": 10,
        "handler_modules": "flowforge.handlers.builtin,tests.support.handlers",
    }
    base.update(overrides)
    return Settings(_env_file=None, **base)  # type: ignore[call-arg]


async def _prepare_database() -> str | None:
    url = make_url(TEST_DB_URL)
    db_name = url.database or ""
    if not db_name.startswith("flowforge_test"):
        raise RuntimeError("test database name must start with 'flowforge_test'")
    admin = create_async_engine(
        url.set(database="postgres").render_as_string(hide_password=False),
        isolation_level="AUTOCOMMIT",
    )
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)'))
            await conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    except (OSError, ConnectionError) as exc:
        return f"PostgreSQL not reachable at {url.host}:{url.port}: {exc}"
    finally:
        await admin.dispose()

    from alembic import command
    from alembic.config import Config

    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.attributes["database_url"] = TEST_DB_URL
    cfg.attributes["configure_logger"] = False
    # Alembic's env.py calls asyncio.run(); give it its own thread and loop.
    await asyncio.to_thread(command.upgrade, cfg, "head")
    return None


@pytest.fixture(scope="session")
async def db_ready() -> AsyncIterator[None]:
    reason = await _prepare_database()
    if reason is not None:
        if REQUIRE_DB:
            pytest.fail(reason)
        pytest.skip(reason)
    yield


@pytest.fixture(scope="session")
def settings() -> Settings:
    return make_settings()


@pytest.fixture(scope="session")
async def engine(db_ready: None, settings: Settings) -> AsyncIterator[AsyncEngine]:
    eng = create_engine(settings, pool_size=20)
    yield eng
    await eng.dispose()


@pytest.fixture
async def db(engine: AsyncEngine) -> AsyncEngine:
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE"))
    return engine


@pytest.fixture(scope="session")
def registry():  # type: ignore[no-untyped-def]
    import flowforge.handlers.builtin  # noqa: F401
    import tests.support.handlers  # noqa: F401
    from flowforge.worker.registry import default_registry

    return default_registry


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Opt-in diagnostic for a rare hang during interpreter exit (seen locally, not in CI).

    With FLOWFORGE_TEST_EXIT_WATCHDOG=<seconds>, a process still alive that long after
    the session ends dumps every thread's stack to stderr. It is armed from atexit
    because pytest's own faulthandler plugin cancels pending dumps at unconfigure.
    """
    seconds = os.environ.get("FLOWFORGE_TEST_EXIT_WATCHDOG")
    if not seconds:
        return
    import atexit
    import faulthandler
    import sys

    def arm() -> None:
        faulthandler.enable(file=sys.__stderr__, all_threads=True)
        faulthandler.dump_traceback_later(float(seconds), repeat=True, file=sys.__stderr__)

    atexit.register(arm)
