"""Migrations and database-level constraints."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.conftest import ROOT, TEST_DB_URL
from tests.support.helpers import start_run, task, tasks_by_key

pytestmark = pytest.mark.integration


def _alembic_cfg():
    from alembic.config import Config

    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.attributes["database_url"] = TEST_DB_URL
    cfg.attributes["configure_logger"] = False
    return cfg


async def test_models_match_migrations(db):
    """`alembic check` fails if the ORM models drift from the migration history."""
    from alembic import command

    await asyncio.to_thread(command.check, _alembic_cfg())


async def _exec(db, sql, params=None):
    async with db.begin() as conn:
        await conn.execute(text(sql), params or {})


async def test_task_constraints(db, registry):
    run_id = await start_run(db, registry, [task("a"), task("b", "noop", ["a"])])
    tasks = await tasks_by_key(db, run_id)
    a = tasks["a"]["id"]
    bad_updates = [
        "UPDATE tasks SET status='WHATEVER' WHERE id=:id",
        "UPDATE tasks SET status='RUNNING' WHERE id=:id",  # no lease
        "UPDATE tasks SET status='RETRY_WAIT' WHERE id=:id",  # no next_attempt_at
        "UPDATE tasks SET status='SUCCEEDED' WHERE id=:id",  # no completed_at
        "UPDATE tasks SET attempt_count = attempt_limit + 1 WHERE id=:id",
        "UPDATE tasks SET timeout_seconds = 0 WHERE id=:id",
        "UPDATE tasks SET task_key='b' WHERE id=:id",  # unique (run_id, task_key)
    ]
    for sql in bad_updates:
        with pytest.raises(IntegrityError):
            await _exec(db, sql, {"id": a})
    with pytest.raises(IntegrityError):
        await _exec(db, "INSERT INTO task_dependencies VALUES (:id, :id)", {"id": a})
    with pytest.raises(IntegrityError):
        await _exec(
            db, "INSERT INTO task_dependencies VALUES (:id, :x)", {"id": a, "x": uuid.uuid4()}
        )


@pytest.mark.no_invariants  # inserts a bare RUNNING attempt to probe constraints
async def test_attempt_and_run_constraints(db, registry):
    run_id = await start_run(db, registry, [task("a")])
    a = (await tasks_by_key(db, run_id))["a"]["id"]
    insert = (
        "INSERT INTO task_attempts (id, task_id, run_id, attempt_number, worker_id, outcome)"
        " VALUES (:i, :t, :r, :n, 'w', :o)"
    )
    await _exec(db, insert, {"i": uuid.uuid4(), "t": a, "r": run_id, "n": 1, "o": "RUNNING"})
    with pytest.raises(IntegrityError):  # duplicate attempt number
        await _exec(db, insert, {"i": uuid.uuid4(), "t": a, "r": run_id, "n": 1, "o": "RUNNING"})
    with pytest.raises(IntegrityError):  # finished outcome needs ended_at
        await _exec(db, insert, {"i": uuid.uuid4(), "t": a, "r": run_id, "n": 2, "o": "SUCCEEDED"})
    with pytest.raises(IntegrityError):
        await _exec(db, "UPDATE workflow_runs SET status='SUCCEEDED' WHERE id=:r", {"r": run_id})
    with pytest.raises(IntegrityError):  # definitions referenced by runs cannot be deleted
        await _exec(db, "DELETE FROM workflow_definitions")


async def test_audit_trail_and_definitions_are_immutable(db, registry):
    await start_run(db, registry, [task("a")])
    with pytest.raises(DBAPIError, match="immutable"):
        await _exec(db, "UPDATE task_events SET event_type='forged'")
    with pytest.raises(DBAPIError, match="immutable"):
        await _exec(db, "UPDATE workflow_definitions SET definition_json='{}'")


async def test_idempotency_record_must_reference_a_run(db):
    with pytest.raises(IntegrityError):  # deferred FK is checked at commit
        await _exec(
            db,
            "INSERT INTO idempotency_records (tenant_id, idempotency_key, request_hash, run_id)"
            " VALUES ('t', 'k', 'h', :r)",
            {"r": uuid.uuid4()},
        )


async def test_downgrade_and_upgrade_round_trip(db):
    from alembic import command

    cfg = _alembic_cfg()
    await asyncio.to_thread(command.downgrade, cfg, "base")
    async with db.connect() as conn:
        tables = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM pg_tables WHERE schemaname='public' "
                    "AND tablename <> 'alembic_version'"
                )
            )
        ).scalar_one()
    assert tables == 0
    await asyncio.to_thread(command.upgrade, cfg, "head")
    await asyncio.to_thread(command.check, cfg)
