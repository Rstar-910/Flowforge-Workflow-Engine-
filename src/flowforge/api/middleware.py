"""Pure ASGI middleware: request IDs, body-size limit, request timeout, metrics, access log."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from typing import Any

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from flowforge.observability import metrics
from flowforge.observability.logging import log_context, log_event

logger = logging.getLogger("flowforge.api.access")
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._\-]{1,64}$")


class BodyTooLargeError(Exception):
    pass


def _error_body(status: int, code: str, message: str, request_id: str) -> bytes:
    return json.dumps(
        {"error": {"code": code, "message": message, "request_id": request_id}}
    ).encode()


class RequestContextMiddleware:
    def __init__(self, app: ASGIApp, *, max_body_bytes: int, timeout_seconds: float) -> None:
        self.app = app
        self.max_body_bytes = max_body_bytes
        self.timeout_seconds = timeout_seconds

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        incoming = headers.get("x-request-id", "")
        request_id = incoming if _REQUEST_ID_RE.match(incoming) else uuid.uuid4().hex
        scope.setdefault("state", {})["request_id"] = request_id
        started = time.perf_counter()
        status_holder: dict[str, int] = {"status": 500}
        response_started = False

        async def send_wrapper(message: Message) -> None:
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
                status_holder["status"] = message["status"]
                message.setdefault("headers", [])
                message["headers"] = [
                    *message["headers"],
                    (b"x-request-id", request_id.encode()),
                ]
            await send(message)

        async def simple(status: int, code: str, msg: str) -> None:
            await send_wrapper(
                {
                    "type": "http.response.start",
                    "status": status,
                    "headers": [(b"content-type", b"application/json")],
                }
            )
            await send(
                {"type": "http.response.body", "body": _error_body(status, code, msg, request_id)}
            )

        declared = headers.get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > self.max_body_bytes:
            await simple(
                413, "payload_too_large", f"request body exceeds {self.max_body_bytes} bytes"
            )
            self._observe(scope, status_holder["status"], started)
            return

        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_body_bytes:
                    raise BodyTooLargeError
            return message

        with log_context(request_id=request_id):
            try:
                async with asyncio.timeout(self.timeout_seconds):
                    await self.app(scope, limited_receive, send_wrapper)
            except BodyTooLargeError:
                if not response_started:
                    await simple(
                        413,
                        "payload_too_large",
                        f"request body exceeds {self.max_body_bytes} bytes",
                    )
            except TimeoutError:
                if not response_started:
                    await simple(503, "timeout", "request timed out")
            finally:
                self._observe(scope, status_holder["status"], started)

    @staticmethod
    def _observe(scope: Scope, status: int, started: float) -> None:
        route: Any = scope.get("route")
        template = getattr(route, "path", None) or "unmatched"
        method = scope.get("method", "?")
        elapsed = time.perf_counter() - started
        metrics.HTTP_REQUESTS.labels(method, template, str(status)).inc()
        metrics.HTTP_DURATION.labels(method, template).observe(elapsed)
        if template not in ("/metrics", "/health/live", "/health/ready"):
            log_event(
                logger,
                logging.INFO,
                "request",
                event_type="http_request",
                method=method,
                path=template,
                status=status,
                duration_ms=round(elapsed * 1000, 2),
            )
