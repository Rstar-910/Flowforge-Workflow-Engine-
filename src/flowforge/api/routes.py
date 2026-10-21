"""HTTP routes. Handlers only translate between HTTP and the engine; no business logic."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import text

from flowforge.api.auth import AdminPrincipal, CurrentPrincipal
from flowforge.api.schemas import (
    IDEMPOTENCY_KEY_PATTERN,
    AttemptList,
    AttemptOut,
    DefinitionList,
    DefinitionOut,
    ErrorResponse,
    EventList,
    EventOut,
    HealthOut,
    RunList,
    RunOut,
    StartRunRequest,
    StatsOut,
    StatusOut,
    TaskList,
    TaskOut,
    WorkerList,
    WorkerOut,
)
from flowforge.domain.definition import WorkflowDefinitionSpec
from flowforge.domain.states import RunStatus
from flowforge.engine import control, queries, submission
from flowforge.observability.tracing import current_traceparent

ERRORS: dict[int | str, dict[str, Any]] = {
    code: {"model": ErrorResponse} for code in (400, 401, 403, 404, 409, 413, 422, 429, 503)
}

v1 = APIRouter(prefix="/v1", responses=ERRORS)
ops = APIRouter()


def _engine(request: Request) -> Any:
    return request.app.state.engine


def _not_found(what: str) -> HTTPException:
    return HTTPException(status.HTTP_404_NOT_FOUND, f"{what} not found")


# ------------------------------------------------------------- definitions


@v1.post(
    "/workflow-definitions",
    response_model=DefinitionOut,
    status_code=status.HTTP_201_CREATED,
    summary="Create a versioned workflow definition",
)
async def create_definition(
    request: Request,
    response: Response,
    principal: CurrentPrincipal,
    spec: WorkflowDefinitionSpec,
) -> DefinitionOut:
    """Validates schema, handler names, dependency references and acyclicity.

    Re-posting identical content for an existing (name, version) returns 200;
    different content for an existing (name, version) returns 409.
    """
    def_id, created = await submission.create_definition(
        _engine(request),
        tenant_id=principal.tenant,
        created_by=principal.key_id,
        spec=spec,
        known_handlers=request.app.state.known_handlers,
    )
    if not created:
        response.status_code = status.HTTP_200_OK
    row = await queries.get_definition(_engine(request), principal.tenant, def_id)
    assert row is not None
    return DefinitionOut.from_row(row)


@v1.get("/workflow-definitions", response_model=DefinitionList)
async def list_definitions(
    request: Request,
    principal: CurrentPrincipal,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: str | None = None,
    name: str | None = None,
) -> DefinitionList:
    rows, nxt = await queries.list_definitions(
        _engine(request), principal.tenant, limit=limit, cursor=cursor, name=name
    )
    return DefinitionList(items=[DefinitionOut.from_row(r) for r in rows], next_cursor=nxt)


@v1.get("/workflow-definitions/{definition_id}", response_model=DefinitionOut)
async def get_definition(
    request: Request, principal: CurrentPrincipal, definition_id: uuid.UUID
) -> DefinitionOut:
    row = await queries.get_definition(_engine(request), principal.tenant, definition_id)
    if row is None:
        raise _not_found("workflow definition")
    return DefinitionOut.from_row(row)


# -------------------------------------------------------------------- runs


@v1.post(
    "/workflow-definitions/{definition_id}/runs",
    response_model=RunOut,
    status_code=status.HTTP_201_CREATED,
    summary="Start a workflow run (idempotent with Idempotency-Key)",
    description=(
        "Returns 201 for a new run. Repeating the request with the same "
        "Idempotency-Key and the same body returns 200 with the original run; "
        "the same key with a different body returns 409."
    ),
)
async def start_run(
    request: Request,
    response: Response,
    principal: CurrentPrincipal,
    definition_id: uuid.UUID,
    body: StartRunRequest,
    idempotency_key: Annotated[
        str | None, Header(alias="Idempotency-Key", pattern=IDEMPOTENCY_KEY_PATTERN)
    ] = None,
) -> RunOut:
    if idempotency_key and body.idempotency_key and idempotency_key != body.idempotency_key:
        raise HTTPException(400, "Idempotency-Key header and body field differ")
    key = idempotency_key or body.idempotency_key
    limiter = request.app.state.rate_limiter
    if limiter is not None and not limiter.allow(principal.tenant):
        raise HTTPException(429, "run submission rate limit exceeded", headers={"Retry-After": "1"})
    settings = request.app.state.settings
    result = await submission.submit_run(
        _engine(request),
        tenant_id=principal.tenant,
        created_by=principal.key_id,
        definition_id=definition_id,
        run_input=body.input,
        idempotency_key=key,
        max_input_bytes=settings.max_json_value_bytes,
        traceparent=current_traceparent(),
    )
    if not result.created:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replayed"] = "true"
    row = await queries.get_run(_engine(request), principal.tenant, result.run_id)
    assert row is not None
    response.headers["Location"] = f"/v1/runs/{result.run_id}"
    return RunOut.from_row(row)


@v1.get("/runs", response_model=RunList)
async def list_runs(
    request: Request,
    principal: CurrentPrincipal,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: str | None = None,
    status_filter: Annotated[RunStatus | None, Query(alias="status")] = None,
    definition_id: uuid.UUID | None = None,
) -> RunList:
    rows, nxt = await queries.list_runs(
        _engine(request),
        principal.tenant,
        limit=limit,
        cursor=cursor,
        status=status_filter.value if status_filter else None,
        definition_id=definition_id,
    )
    return RunList(items=[RunOut.from_row(r) for r in rows], next_cursor=nxt)


@v1.get("/runs/{run_id}", response_model=RunOut)
async def get_run(request: Request, principal: CurrentPrincipal, run_id: uuid.UUID) -> RunOut:
    row = await queries.get_run(_engine(request), principal.tenant, run_id)
    if row is None:
        raise _not_found("run")
    return RunOut.from_row(row)


@v1.get("/runs/{run_id}/tasks", response_model=TaskList)
async def list_run_tasks(
    request: Request, principal: CurrentPrincipal, run_id: uuid.UUID
) -> TaskList:
    rows = await queries.list_tasks(_engine(request), principal.tenant, run_id)
    if rows is None:
        raise _not_found("run")
    return TaskList(items=[TaskOut.from_row(r) for r in rows])


@v1.get("/runs/{run_id}/events", response_model=EventList)
async def list_run_events(
    request: Request,
    principal: CurrentPrincipal,
    run_id: uuid.UUID,
    after: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    task_id: uuid.UUID | None = None,
) -> EventList:
    rows = await queries.list_events(
        _engine(request), principal.tenant, run_id, after=after, limit=limit, task_id=task_id
    )
    if rows is None:
        raise _not_found("run")
    items = [EventOut.model_validate(r) for r in rows]
    return EventList(items=items, next_after=items[-1].id if len(items) == limit else None)


@v1.post(
    "/runs/{run_id}/cancel",
    response_model=StatusOut,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Request cancellation of a run",
)
async def cancel_run(request: Request, principal: CurrentPrincipal, run_id: uuid.UUID) -> StatusOut:
    new_status = await control.cancel_run(
        _engine(request), tenant_id=principal.tenant, run_id=run_id
    )
    return StatusOut(id=run_id, status=new_status)


# ------------------------------------------------------------------- tasks


@v1.get("/tasks/{task_id}", response_model=TaskOut)
async def get_task(request: Request, principal: CurrentPrincipal, task_id: uuid.UUID) -> TaskOut:
    row = await queries.get_task(_engine(request), principal.tenant, task_id)
    if row is None:
        raise _not_found("task")
    return TaskOut.from_row(row)


@v1.get("/tasks/{task_id}/attempts", response_model=AttemptList)
async def list_attempts(
    request: Request, principal: CurrentPrincipal, task_id: uuid.UUID
) -> AttemptList:
    rows = await queries.list_attempts(_engine(request), principal.tenant, task_id)
    if rows is None:
        raise _not_found("task")
    return AttemptList(items=[AttemptOut.model_validate(r) for r in rows])


@v1.post(
    "/tasks/{task_id}/retry",
    response_model=StatusOut,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Manually retry a terminally FAILED task (admin only)",
    description=(
        "Grants the task a fresh attempt budget. If the handler has external side "
        "effects, a manual retry may repeat them unless the handler uses the "
        "operation key for downstream idempotency."
    ),
)
async def retry_task(request: Request, principal: AdminPrincipal, task_id: uuid.UUID) -> StatusOut:
    new_status = await control.retry_failed_task(
        _engine(request),
        tenant_id=principal.tenant,
        task_id=task_id,
        requested_by=principal.key_id,
    )
    return StatusOut(id=task_id, status=new_status)


@v1.get("/stats", response_model=StatsOut, summary="Run and active-task counts for your tenant")
async def stats(request: Request, principal: CurrentPrincipal) -> StatsOut:
    return StatsOut(**await queries.tenant_stats(_engine(request), principal.tenant))


@v1.get(
    "/workers",
    response_model=WorkerList,
    summary="Worker registry and heartbeat health (admin only)",
    description="Workers are shared infrastructure, not tenant data, so this requires admin.",
)
async def list_workers(
    request: Request,
    principal: AdminPrincipal,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> WorkerList:
    rows = await queries.list_workers(_engine(request), limit=limit)
    lease = request.app.state.settings.lease_seconds

    def health(r: dict[str, Any]) -> str:
        if r["stopped_at"] is not None:
            return "stopped"
        return "live" if r["heartbeat_age_seconds"] < lease else "silent"

    return WorkerList(items=[WorkerOut(**r, health=health(r)) for r in rows])


@v1.get("/handlers", summary="List handler names this API accepts in definitions")
async def list_handlers(request: Request, principal: CurrentPrincipal) -> dict[str, list[str]]:
    return {"handlers": sorted(request.app.state.known_handlers)}


# --------------------------------------------------------------- operations


@ops.get("/health/live", response_model=HealthOut, tags=["operations"])
async def live() -> HealthOut:
    return HealthOut(status="ok")


@ops.get(
    "/health/ready",
    response_model=HealthOut,
    tags=["operations"],
    responses={503: {"model": HealthOut}},
)
async def ready(request: Request, response: Response) -> HealthOut:
    import asyncio

    try:
        async with asyncio.timeout(2):
            async with request.app.state.engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
                head = (
                    await conn.execute(text("SELECT version_num FROM alembic_version"))
                ).scalar_one_or_none()
    except Exception as exc:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return HealthOut(status="unavailable", checks={"database": type(exc).__name__})
    return HealthOut(status="ok", checks={"database": "ok", "schema_version": str(head)})


@ops.get("/metrics", tags=["operations"], response_class=PlainTextResponse)
async def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
