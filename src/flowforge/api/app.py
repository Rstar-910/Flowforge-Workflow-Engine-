"""FastAPI application factory."""

from __future__ import annotations

import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine

from flowforge import __version__
from flowforge.api.errors import install_error_handlers
from flowforge.api.middleware import RequestContextMiddleware
from flowforge.api.routes import ops, v1
from flowforge.api.ui import mount_dashboard
from flowforge.config import Settings
from flowforge.db.session import create_engine
from flowforge.worker.registry import load_handler_modules


class TokenBucket:
    """Per-tenant token bucket for run submission (per API process)."""

    def __init__(self, rate_per_second: float, burst: int) -> None:
        self.rate = rate_per_second
        self.burst = burst
        self._state: dict[str, tuple[float, float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        with self._lock:
            tokens, last = self._state.get(key, (float(self.burst), now))
            tokens = min(self.burst, tokens + (now - last) * self.rate)
            if tokens < 1:
                self._state[key] = (tokens, now)
                return False
            self._state[key] = (tokens - 1, now)
            return True


def create_app(settings: Settings, *, engine: AsyncEngine | None = None) -> FastAPI:
    owns_engine = engine is None

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.engine = engine or create_engine(settings)
        try:
            yield
        finally:
            if owns_engine:
                await app.state.engine.dispose()

    app = FastAPI(
        title="FlowForge",
        version=__version__,
        description=(
            "Durable, PostgreSQL-backed workflow engine. Authenticate with the "
            "`X-API-Key` header. Execution is at-least-once; see docs/failure-model.md."
        ),
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.api_keys = settings.parsed_api_keys()
    app.state.known_handlers = frozenset(
        load_handler_modules(settings.handler_module_list()).names()
    )
    app.state.rate_limiter = (
        TokenBucket(settings.run_submit_rate_per_second, settings.run_submit_burst)
        if settings.run_submit_rate_per_second
        else None
    )
    if engine is not None:
        app.state.engine = engine
    install_error_handlers(app)
    app.include_router(v1)
    app.include_router(ops)
    mount_dashboard(app)
    app.add_middleware(
        RequestContextMiddleware,
        max_body_bytes=settings.max_request_bytes,
        timeout_seconds=settings.request_timeout_seconds,
    )
    return app
