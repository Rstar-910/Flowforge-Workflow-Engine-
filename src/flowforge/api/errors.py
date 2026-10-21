"""Consistent error responses. Internal details never reach the client."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError, OperationalError

from flowforge.domain.definition import DefinitionValidationError
from flowforge.engine.queries import InvalidCursorError
from flowforge.engine.submission import (
    ConflictError,
    IdempotencyConflictError,
    NotFoundError,
    PayloadTooLargeError,
)

logger = logging.getLogger(__name__)

_HTTP_CODES = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    409: "conflict",
    413: "payload_too_large",
    422: "validation_error",
    429: "rate_limited",
    503: "unavailable",
}


def error_response(
    request: Request,
    status: int,
    code: str,
    message: str,
    details: list[dict[str, Any]] | None = None,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    body: dict[str, Any] = {
        "code": code,
        "message": message,
        "request_id": request.scope.get("state", {}).get("request_id"),
    }
    if details:
        body["details"] = details
    return JSONResponse({"error": body}, status_code=status, headers=headers)


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(HTTPException)
    async def http_error(request: Request, exc: HTTPException) -> JSONResponse:
        return error_response(
            request,
            exc.status_code,
            _HTTP_CODES.get(exc.status_code, "error"),
            str(exc.detail),
            headers=dict(exc.headers or {}),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        # Do not echo submitted values back (they may contain secrets).
        details = [
            {"loc": list(e.get("loc", ())), "msg": e.get("msg"), "type": e.get("type")}
            for e in exc.errors()
        ]
        return error_response(request, 422, "validation_error", "invalid request", details)

    @app.exception_handler(DefinitionValidationError)
    async def definition_error(request: Request, exc: DefinitionValidationError) -> JSONResponse:
        details = [{"loc": i.path, "msg": i.message} for i in exc.issues]
        return error_response(
            request, 422, "invalid_definition", "workflow definition is invalid", details
        )

    @app.exception_handler(NotFoundError)
    async def not_found(request: Request, exc: NotFoundError) -> JSONResponse:
        return error_response(request, 404, "not_found", str(exc))

    @app.exception_handler(IdempotencyConflictError)
    async def idem_conflict(request: Request, exc: IdempotencyConflictError) -> JSONResponse:
        return error_response(request, 409, "idempotency_conflict", str(exc))

    @app.exception_handler(ConflictError)
    async def conflict(request: Request, exc: ConflictError) -> JSONResponse:
        return error_response(request, 409, "conflict", str(exc))

    @app.exception_handler(PayloadTooLargeError)
    async def too_large(request: Request, exc: PayloadTooLargeError) -> JSONResponse:
        return error_response(request, 413, "payload_too_large", str(exc))

    @app.exception_handler(InvalidCursorError)
    async def bad_cursor(request: Request, exc: InvalidCursorError) -> JSONResponse:
        return error_response(request, 400, "bad_request", str(exc))

    async def db_unavailable(request: Request, exc: Exception) -> JSONResponse:
        logger.warning("database unavailable: %s", type(exc).__name__)
        return error_response(
            request,
            503,
            "unavailable",
            "database temporarily unavailable",
            headers={"Retry-After": "2"},
        )

    for exc_type in (OperationalError, InterfaceError, DBAPIError, ConnectionError, OSError):
        app.add_exception_handler(exc_type, db_unavailable)

    @app.exception_handler(IntegrityError)
    async def integrity(request: Request, exc: IntegrityError) -> JSONResponse:
        logger.warning("constraint violation: %s", type(exc.orig).__name__)
        return error_response(request, 409, "conflict", "request violates a data constraint")

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error")
        return error_response(request, 500, "internal_error", "internal server error")
