"""Structured JSON logging with correlation fields carried in context variables."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

CORRELATION_FIELDS = (
    "request_id",
    "workflow_run_id",
    "task_id",
    "task_key",
    "attempt_id",
    "worker_id",
)
_context: ContextVar[dict[str, Any]] = ContextVar("flowforge_log_context", default={})  # noqa: B039

_service_name = "flowforge"


@contextmanager
def log_context(**fields: Any) -> Iterator[None]:
    """Bind correlation fields for the duration of the block (async-safe)."""
    token = _context.set({**_context.get(), **{k: v for k, v in fields.items() if v is not None}})
    try:
        yield
    finally:
        _context.reset(token)


def bind_log_context(**fields: Any) -> None:
    """Bind fields for the rest of the current context (e.g. worker_id for a process)."""
    _context.set({**_context.get(), **fields})


def current_context() -> dict[str, Any]:
    return dict(_context.get())


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "service": _service_name,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update({k: str(v) for k, v in _context.get().items()})
        extra = getattr(record, "fields", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        ctx = " ".join(f"{k}={v}" for k, v in _context.get().items())
        extra = getattr(record, "fields", None)
        if isinstance(extra, dict):
            ctx += " " + " ".join(f"{k}={v}" for k, v in extra.items())
        base = (
            f"{self.formatTime(record)} {record.levelname:<7} {record.name}: {record.getMessage()}"
        )
        out = f"{base} {ctx}".rstrip()
        if record.exc_info:
            out += "\n" + self.formatException(record.exc_info)
        return out


def configure_logging(service: str, level: str = "INFO", fmt: str = "json") -> None:
    global _service_name  # noqa: PLW0603
    _service_name = service
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    for noisy in ("uvicorn.access", "sqlalchemy.engine", "asyncio"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def log_event(logger: logging.Logger, level: int, message: str, /, **fields: Any) -> None:
    """Log ``message`` with structured ``fields`` (event_type, duration_ms, error_type, ...)."""
    logger.log(level, message, extra={"fields": fields})
