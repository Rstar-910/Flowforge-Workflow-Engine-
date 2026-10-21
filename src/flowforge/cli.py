"""Command-line entry point: ``flowforge {api,worker,scheduler,migrate,inspect}``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import uuid
from pathlib import Path

from flowforge.config import Settings, get_settings
from flowforge.observability.logging import configure_logging
from flowforge.observability.tracing import configure_tracing

logger = logging.getLogger("flowforge.cli")


def _install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    count = {"n": 0}

    def handle(signame: str) -> None:
        count["n"] += 1
        if count["n"] == 1:
            logger.info("received %s; shutting down gracefully (repeat to force)", signame)
            stop.set()
        else:
            logger.warning("received %s again; exiting immediately", signame)
            os._exit(130)

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle, sig.name)


def _start_metrics_server(settings: Settings) -> None:
    if settings.metrics_port:
        from prometheus_client import start_http_server

        start_http_server(settings.metrics_port)
        logger.info("metrics on :%s/metrics", settings.metrics_port)


async def _run_worker(settings: Settings, concurrency: int, worker_id: str | None) -> None:
    from flowforge.db.session import create_engine
    from flowforge.worker.registry import load_handler_modules
    from flowforge.worker.runtime import Worker

    registry = load_handler_modules(settings.handler_module_list())
    # Pool: one connection per concurrent completion plus claim + heartbeat.
    engine = create_engine(
        settings, pool_size=concurrency + 2, pre_ping=False, narrow_lookups_only=True
    )
    stop = asyncio.Event()
    _install_signal_handlers(stop)
    try:
        await Worker(engine, settings, registry, concurrency=concurrency, worker_id=worker_id).run(
            stop
        )
    finally:
        await engine.dispose()


async def _run_scheduler(settings: Settings) -> None:
    from flowforge.db.session import create_engine
    from flowforge.scheduler.service import Scheduler

    engine = create_engine(settings, pool_size=3, pre_ping=False, narrow_lookups_only=True)
    stop = asyncio.Event()
    _install_signal_handlers(stop)
    try:
        await Scheduler(engine, settings).run(stop)
    finally:
        await engine.dispose()


def _migrate(settings: Settings, revision: str) -> None:
    from alembic import command
    from alembic.config import Config

    root = Path(__file__).resolve().parents[2]
    ini = root / "alembic.ini"
    if not ini.exists():  # installed package (e.g. container): use cwd
        ini = Path.cwd() / "alembic.ini"
    cfg = Config(str(ini))
    cfg.attributes["database_url"] = settings.database_url.get_secret_value()
    cfg.attributes["configure_logger"] = False  # keep our structured logging
    logging.getLogger("alembic.runtime.plugins").setLevel(logging.WARNING)
    command.upgrade(cfg, revision)


async def _inspect(settings: Settings, run_id: uuid.UUID) -> int:
    """Operator view of a run straight from the database (bypasses API tenancy)."""
    from sqlalchemy import text

    from flowforge.db.session import create_engine

    engine = create_engine(settings, pool_size=1)
    try:
        async with engine.connect() as conn:
            run = (
                (await conn.execute(text("SELECT * FROM workflow_runs WHERE id=:i"), {"i": run_id}))
                .mappings()
                .first()
            )
            if run is None:
                print("run not found", file=sys.stderr)
                return 1
            print(f"run {run['id']}  status={run['status']}  tenant={run['tenant_id']}")
            print(f"  created={run['created_at']}  completed={run['completed_at']}")
            if run["failure_reason"]:
                print(f"  failure: {run['failure_reason']}")
            rows = (
                await conn.execute(
                    text(
                        "SELECT t.task_key, t.status, t.attempt_count, t.attempt_limit, "
                        "a.attempt_number, a.worker_id, a.outcome, a.duration_ms, a.error_type "
                        "FROM tasks t LEFT JOIN task_attempts a ON a.task_id = t.id "
                        "WHERE t.run_id = :i ORDER BY t.created_at, t.task_key, a.attempt_number"
                    ),
                    {"i": run_id},
                )
            ).all()
            print(
                f"\n{'task':<20}{'status':<17}{'att':<7}{'#':<3}{'worker':<26}"
                f"{'outcome':<15}{'ms':>7}  error"
            )
            for r in rows:
                print(
                    f"{r.task_key:<20}{r.status:<17}{f'{r.attempt_count}/{r.attempt_limit}':<7}"
                    f"{r.attempt_number or '':<3}{(r.worker_id or '')[:25]:<26}"
                    f"{r.outcome or '':<15}{r.duration_ms if r.duration_ms is not None else '':>7}"
                    f"  {r.error_type or ''}"
                )
    finally:
        await engine.dispose()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="flowforge", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    api = sub.add_parser("api", help="run the HTTP API")
    api.add_argument("--host", default="127.0.0.1")
    api.add_argument("--port", type=int, default=8000)
    api.add_argument("--workers", type=int, default=1, help="uvicorn worker processes")
    worker = sub.add_parser("worker", help="run a worker process")
    worker.add_argument("--concurrency", type=int, default=4)
    worker.add_argument("--worker-id", default=None)
    sub.add_parser("scheduler", help="run the scheduler / recovery loop")
    mig = sub.add_parser("migrate", help="apply database migrations")
    mig.add_argument("--revision", default="head")
    insp = sub.add_parser("inspect", help="print a run's tasks and attempts")
    insp.add_argument("run_id", type=uuid.UUID)
    args = parser.parse_args(argv)

    settings = get_settings()
    service = f"{settings.service_name}-{args.command}"
    configure_logging(service, settings.log_level, settings.log_format)
    configure_tracing(service)

    if args.command == "api":
        import uvicorn

        from flowforge.api.app import create_app

        if args.workers > 1:
            uvicorn.run(
                "flowforge.api.asgi:app",
                host=args.host,
                port=args.port,
                workers=args.workers,
                log_config=None,
                access_log=False,
            )
        else:
            uvicorn.run(
                create_app(settings),
                host=args.host,
                port=args.port,
                log_config=None,
                access_log=False,
            )
        return 0
    if args.command == "worker":
        _start_metrics_server(settings)
        asyncio.run(_run_worker(settings, args.concurrency, args.worker_id))
        return 0
    if args.command == "scheduler":
        _start_metrics_server(settings)
        asyncio.run(_run_scheduler(settings))
        return 0
    if args.command == "migrate":
        _migrate(settings, args.revision)
        return 0
    if args.command == "inspect":
        return asyncio.run(_inspect(settings, args.run_id))
    return 2


if __name__ == "__main__":
    sys.exit(main())
