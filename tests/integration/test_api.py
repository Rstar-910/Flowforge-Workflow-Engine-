"""HTTP API tests against a real database (httpx ASGI transport, no network)."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

from flowforge.api.app import create_app
from flowforge.db.session import create_engine
from tests.conftest import make_settings
from tests.support.helpers import Cluster, task, wait_for_run

pytestmark = pytest.mark.integration

ADMIN = {"X-API-Key": "admin-key"}
USER = {"X-API-Key": "user-key"}
OTHER = {"X-API-Key": "other-key"}  # tenant-b


@pytest.fixture
async def client(db, settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings, engine=db)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def definition(name: str = "report", version: int = 1, **overrides):
    body = {
        "name": name,
        "version": version,
        "tasks": [
            task("fetch", "fetch_data"),
            task("sum", "aggregate", ["fetch"], input={"source": "fetch", "op": "sum"}),
            task("max", "aggregate", ["fetch"], input={"source": "fetch", "op": "max"}),
            task("report", "combine", ["sum", "max"]),
        ],
    }
    body.update(overrides)
    return body


async def create_def(client, body=None, headers=USER) -> dict:
    r = await client.post("/v1/workflow-definitions", json=body or definition(), headers=headers)
    assert r.status_code in (200, 201), r.text
    return r.json()


async def test_authentication_and_roles(client):
    assert (await client.get("/v1/workflow-definitions")).status_code == 401
    r = await client.get("/v1/workflow-definitions", headers={"X-API-Key": "nope"})
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "unauthorized"
    r = await client.get("/v1/workflow-definitions", headers={"Authorization": "Bearer user-key"})
    assert r.status_code == 200
    r = await client.post(f"/v1/tasks/{uuid.uuid4()}/retry", headers=USER)
    assert r.status_code == 403


async def test_definition_create_is_validated_and_versioned(client):
    created = await client.post("/v1/workflow-definitions", json=definition(), headers=USER)
    assert created.status_code == 201
    again = await client.post("/v1/workflow-definitions", json=definition(), headers=USER)
    assert again.status_code == 200 and again.json()["id"] == created.json()["id"]
    changed = definition(description="different")
    r = await client.post("/v1/workflow-definitions", json=changed, headers=USER)
    assert r.status_code == 409
    v2 = await client.post(
        "/v1/workflow-definitions",
        json=definition(version=2, description="different"),
        headers=USER,
    )
    assert v2.status_code == 201

    got = await client.get(f"/v1/workflow-definitions/{created.json()['id']}", headers=USER)
    assert got.json()["definition"]["tasks"][0]["id"] == "fetch"
    listed = await client.get("/v1/workflow-definitions?limit=1&name=report", headers=USER)
    page1 = listed.json()
    assert len(page1["items"]) == 1 and page1["next_cursor"]
    page2 = (
        await client.get(
            f"/v1/workflow-definitions?limit=1&name=report&cursor={page1['next_cursor']}",
            headers=USER,
        )
    ).json()
    assert {page1["items"][0]["version"], page2["items"][0]["version"]} == {1, 2}
    bad_cursor = await client.get("/v1/workflow-definitions?cursor=garbage", headers=USER)
    assert bad_cursor.status_code == 400


@pytest.mark.parametrize(
    ("tasks", "fragment"),
    [
        ([task("a", "noop", ["b"]), task("b", "noop", ["a"])], "cycle"),
        ([task("a", "noop", ["a"])], "itself"),
        ([task("a", "noop", ["ghost"])], "unknown task reference"),
        ([task("a", "rm_rf_slash")], "unknown handler"),
        ([task("a"), task("a")], "duplicate task id"),
    ],
)
async def test_invalid_graphs_are_rejected_before_storage(client, tasks, fragment):
    r = await client.post(
        "/v1/workflow-definitions",
        json={"name": "bad", "version": 1, "tasks": tasks},
        headers=USER,
    )
    assert r.status_code == 422
    body = r.json()["error"]
    assert body["code"] == "invalid_definition"
    assert any(fragment in d["msg"] for d in body["details"])
    listed = await client.get("/v1/workflow-definitions?name=bad", headers=USER)
    assert listed.json()["items"] == []


@pytest.mark.parametrize(
    "mutate",
    [
        lambda d: d.update(unexpected=True),
        lambda d: d["tasks"][0].update(timeout_seconds=-1),
        lambda d: d["tasks"][0]["retry"].update(max_attempts=0),
        lambda d: d["tasks"][0]["retry"].update(initial_delay_seconds=10, max_delay_seconds=1),
        lambda d: d.update(tasks=[]),
        lambda d: d["tasks"][0].update(handler="Bad Handler!"),
        lambda d: d.update(name="UPPER case"),
    ],
)
async def test_malformed_definitions_are_422(client, mutate):
    body = definition(name="malformed")
    mutate(body)
    r = await client.post("/v1/workflow-definitions", json=body, headers=USER)
    assert r.status_code == 422, r.text
    assert r.json()["error"]["code"] == "validation_error"
    # Submitted values are not echoed back.
    assert all(set(d) <= {"loc", "msg", "type"} for d in r.json()["error"]["details"])


async def test_request_size_limit(db):
    settings = make_settings(max_request_bytes=2048)
    app = create_app(settings, engine=db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        big = definition(description="x" * 1900, name="big")
        big["tasks"][0]["input"] = {"blob": "y" * 3000}
        r = await c.post("/v1/workflow-definitions", json=big, headers=USER)
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "payload_too_large"


async def test_full_run_lifecycle_via_api(client, db, settings, registry):
    d = await create_def(client)
    r = await client.post(
        f"/v1/workflow-definitions/{d['id']}/runs", json={"input": {"rows": 7}}, headers=USER
    )
    assert r.status_code == 201
    run = r.json()
    assert r.headers["Location"] == f"/v1/runs/{run['id']}"
    assert run["status"] == "PENDING"
    assert r.headers["x-request-id"]

    async with Cluster(db, settings, registry, workers=2):
        await wait_for_run(db, uuid.UUID(run["id"]), {"SUCCEEDED", "FAILED"})

    run = (await client.get(f"/v1/runs/{run['id']}", headers=USER)).json()
    assert run["status"] == "SUCCEEDED"
    assert run["task_counts"] == {"SUCCEEDED": 4}
    values = [(i * 37) % 101 for i in range(7)]
    assert run["output"] == {"report": {"report": {"max": max(values), "sum": sum(values)}}}

    tasks = (await client.get(f"/v1/runs/{run['id']}/tasks", headers=USER)).json()["items"]
    by_key = {t["task_key"]: t for t in tasks}
    assert by_key["report"]["depends_on"] == ["max", "sum"]
    attempts = (await client.get(f"/v1/tasks/{by_key['sum']['id']}/attempts", headers=USER)).json()[
        "items"
    ]
    assert [a["outcome"] for a in attempts] == ["SUCCEEDED"]
    assert attempts[0]["worker_id"] in {"w0", "w1"}

    page = (await client.get(f"/v1/runs/{run['id']}/events?limit=3", headers=USER)).json()
    assert len(page["items"]) == 3 and page["next_after"] == page["items"][-1]["id"]
    rest = (
        await client.get(
            f"/v1/runs/{run['id']}/events?after={page['next_after']}&limit=500", headers=USER
        )
    ).json()
    assert rest["items"][0]["id"] > page["next_after"]
    assert rest["items"][-1]["event_type"] == "run_status_changed"

    runs = (await client.get("/v1/runs?status=SUCCEEDED", headers=USER)).json()
    assert [x["id"] for x in runs["items"]] == [run["id"]]

    # Cancelling a finished run is a conflict.
    r = await client.post(f"/v1/runs/{run['id']}/cancel", headers=USER)
    assert r.status_code == 409


async def test_idempotent_submission_via_api(client):
    d = await create_def(client)
    url = f"/v1/workflow-definitions/{d['id']}/runs"
    first = await client.post(
        url, json={"input": {"a": 1}}, headers={**USER, "Idempotency-Key": "abc-1"}
    )
    replay = await client.post(
        url, json={"input": {"a": 1}}, headers={**USER, "Idempotency-Key": "abc-1"}
    )
    assert first.status_code == 201 and replay.status_code == 200
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert first.json()["id"] == replay.json()["id"]
    body_key = await client.post(
        url, json={"input": {"a": 1}, "idempotency_key": "abc-1"}, headers=USER
    )
    assert body_key.json()["id"] == first.json()["id"]
    conflict = await client.post(
        url, json={"input": {"a": 2}}, headers={**USER, "Idempotency-Key": "abc-1"}
    )
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "idempotency_conflict"
    mismatch = await client.post(
        url, json={"input": {}, "idempotency_key": "x"}, headers={**USER, "Idempotency-Key": "y"}
    )
    assert mismatch.status_code == 400
    bad_key = await client.post(url, json={}, headers={**USER, "Idempotency-Key": "has space"})
    assert bad_key.status_code == 422


async def test_tenant_isolation(client):
    d = await create_def(client)
    run = (
        await client.post(f"/v1/workflow-definitions/{d['id']}/runs", json={}, headers=USER)
    ).json()
    tasks = (await client.get(f"/v1/runs/{run['id']}/tasks", headers=USER)).json()["items"]
    tid = tasks[0]["id"]
    for method, path in [
        ("GET", f"/v1/workflow-definitions/{d['id']}"),
        ("POST", f"/v1/workflow-definitions/{d['id']}/runs"),
        ("GET", f"/v1/runs/{run['id']}"),
        ("GET", f"/v1/runs/{run['id']}/tasks"),
        ("GET", f"/v1/runs/{run['id']}/events"),
        ("POST", f"/v1/runs/{run['id']}/cancel"),
        ("GET", f"/v1/tasks/{tid}"),
        ("GET", f"/v1/tasks/{tid}/attempts"),
    ]:
        kwargs = {"json": {}} if method == "POST" else {}
        r = await client.request(method, path, headers=OTHER, **kwargs)
        assert r.status_code == 404, (path, r.status_code)
    assert (await client.get("/v1/runs", headers=OTHER)).json()["items"] == []
    assert (await client.get("/v1/workflow-definitions", headers=OTHER)).json()["items"] == []


async def test_cancel_and_admin_retry_via_api(client, db, settings, registry):
    body = {
        "name": "retryable",
        "version": 1,
        "tasks": [
            task("a", "fail_until", input={"fail_times": 1}, max_attempts=1),
            task("b", "noop", ["a"]),
        ],
    }
    d = await create_def(client, body)
    run = (
        await client.post(f"/v1/workflow-definitions/{d['id']}/runs", json={}, headers=USER)
    ).json()
    async with Cluster(db, settings, registry):
        assert await wait_for_run(db, uuid.UUID(run["id"]), {"FAILED", "SUCCEEDED"}) == "FAILED"
        tasks = (await client.get(f"/v1/runs/{run['id']}/tasks", headers=USER)).json()["items"]
        failed = next(t for t in tasks if t["task_key"] == "a")
        assert failed["status"] == "FAILED"
        r = await client.post(f"/v1/tasks/{failed['id']}/retry", headers=USER)
        assert r.status_code == 403
        r = await client.post(f"/v1/tasks/{failed['id']}/retry", headers=ADMIN)
        assert r.status_code == 202, r.text
        assert await wait_for_run(db, uuid.UUID(run["id"]), {"SUCCEEDED"}) == "SUCCEEDED"
        r = await client.post(f"/v1/tasks/{failed['id']}/retry", headers=ADMIN)
        assert r.status_code == 409  # no longer FAILED

    pending = (
        await client.post(f"/v1/workflow-definitions/{d['id']}/runs", json={}, headers=USER)
    ).json()
    r = await client.post(f"/v1/runs/{pending['id']}/cancel", headers=USER)
    assert r.status_code == 202 and r.json()["status"] == "CANCELLED"


async def test_health_metrics_and_request_ids(client):
    assert (await client.get("/health/live")).json()["status"] == "ok"
    ready = await client.get("/health/ready")
    assert ready.status_code == 200 and ready.json()["checks"]["schema_version"] == "0001"
    r = await client.get("/v1/runs", headers={**USER, "X-Request-ID": "trace-me-123"})
    assert r.headers["x-request-id"] == "trace-me-123"
    metrics = (await client.get("/metrics")).text
    for name in (
        "flowforge_http_requests_total",
        "flowforge_tasks_claimed_total",
        "flowforge_task_queue_wait_seconds_bucket",
        "flowforge_expired_leases_total",
        "flowforge_database_operation_duration_seconds_bucket",
    ):
        assert name in metrics
    # Route templates, not concrete IDs, are used as labels.
    assert 'route="/v1/runs"' in metrics
    assert "trace-me-123" not in metrics


async def test_rate_limit(db):
    app = create_app(make_settings(run_submit_rate_per_second=0.001, run_submit_burst=2), engine=db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        d = await create_def(c)
        codes = [
            (
                await c.post(f"/v1/workflow-definitions/{d['id']}/runs", json={}, headers=USER)
            ).status_code
            for _ in range(3)
        ]
    assert codes == [201, 201, 429]


async def test_database_outage_returns_503_without_internals():
    settings = make_settings(
        database_url="postgresql+asyncpg://flowforge:secret-password@127.0.0.1:1/nowhere",
        db_connect_timeout_seconds=1,
    )
    engine = create_engine(settings)
    app = create_app(settings, engine=engine)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as c:
            ready = await c.get("/health/ready")
            assert ready.status_code == 503
            r = await c.get(f"/v1/runs/{uuid.uuid4()}", headers=USER)
    finally:
        await engine.dispose()
    assert r.status_code == 503
    assert r.json()["error"]["code"] == "unavailable"
    assert "secret-password" not in r.text and "Traceback" not in r.text
