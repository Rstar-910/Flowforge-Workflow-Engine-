"""Dashboard serving, its security headers, and the stats/workers endpoints it uses."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest

from flowforge.api.app import create_app
from tests.support.helpers import Cluster, start_run, task, wait_for_run

pytestmark = pytest.mark.integration

ADMIN = {"X-API-Key": "admin-key"}
USER = {"X-API-Key": "user-key"}
OTHER = {"X-API-Key": "other-key"}  # tenant-b


@pytest.fixture
async def client(db, settings) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(settings, engine=db)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c


async def test_dashboard_is_served_with_strict_security_headers(client):
    root = await client.get("/")
    assert root.status_code == 307 and root.headers["location"] == "/ui/"
    page = await client.get("/ui/")
    assert page.status_code == 200
    assert "<title>FlowForge Dashboard</title>" in page.text
    csp = page.headers["content-security-policy"]
    assert "script-src 'self'" in csp and "'unsafe-inline'" not in csp
    assert "frame-ancestors 'none'" in csp
    assert page.headers["x-content-type-options"] == "nosniff"
    assert page.headers["x-frame-options"] == "DENY"
    for asset, ctype in (("app.js", "javascript"), ("app.css", "text/css")):
        r = await client.get(f"/ui/{asset}")
        assert r.status_code == 200 and ctype in r.headers["content-type"]
        assert "content-security-policy" in r.headers
    assert (await client.get("/ui/../pyproject.toml")).status_code == 404


async def test_dashboard_never_uses_inner_html(client):
    """The page renders API data via textContent only (defence against stored XSS)."""
    import re

    js = (await client.get("/ui/app.js")).text
    for sink in (
        r"\.innerHTML\b",
        r"\.outerHTML\b",
        r"insertAdjacentHTML",
        r"\beval\(",
        r"document\.write",
        r"new Function\(",
    ):
        assert not re.search(sink, js), sink


async def test_stats_are_tenant_scoped(client, db, settings, registry):
    run_ok = await start_run(db, registry, [task("a"), task("b", "noop", ["a"])])
    await start_run(db, registry, [task("x", "sleep", input={"seconds": 30})])
    await start_run(db, registry, [task("t")], tenant="tenant-b")
    async with Cluster(db, settings, registry):
        await wait_for_run(db, run_ok, {"SUCCEEDED"})

        async def sleeping_task_running() -> bool:
            r = (await client.get("/v1/stats", headers=USER)).json()
            return r["active_tasks"].get("RUNNING") == 1

        from tests.support.helpers import wait_for

        await wait_for(sleeping_task_running)
        stats = (await client.get("/v1/stats", headers=USER)).json()
        other = (await client.get("/v1/stats", headers=OTHER)).json()
    assert stats["runs"]["SUCCEEDED"] == 1
    assert sum(stats["runs"].values()) == 2  # tenant-b's run is not counted
    assert sum(other["runs"].values()) == 1


async def test_workers_endpoint_is_admin_only_and_reports_health(client, db, settings, registry):
    assert (await client.get("/v1/workers", headers=USER)).status_code == 403
    run_id = await start_run(db, registry, [task("a")])
    async with Cluster(db, settings, registry, workers=2):
        await wait_for_run(db, run_id, {"SUCCEEDED"})
        live = (await client.get("/v1/workers", headers=ADMIN)).json()["items"]
    assert {w["worker_id"] for w in live} == {"w0", "w1"}
    assert all(w["health"] == "live" and w["heartbeat_age_seconds"] < 10 for w in live)
    assert all(w["concurrency"] == 4 and "noop" in w["handlers"] for w in live)
    stopped = (await client.get("/v1/workers", headers=ADMIN)).json()["items"]
    assert all(w["health"] == "stopped" and w["running_tasks"] == 0 for w in stopped)


async def test_crashed_worker_is_reported_silent(client, db, settings):
    """A worker that stops heartbeating (no clean shutdown) is 'silent' after one lease."""
    from flowforge.engine.execution import register_worker

    await register_worker(db, "ghost", pid=1, concurrency=2, handlers=["noop"])
    async with db.begin() as conn:
        from sqlalchemy import text

        await conn.execute(
            text(
                "UPDATE workers SET last_heartbeat_at = now() - make_interval(secs => :s) "
                "WHERE worker_id = 'ghost'"
            ),
            {"s": settings.lease_seconds + 1},
        )
    items = (await client.get("/v1/workers", headers=ADMIN)).json()["items"]
    assert [w["health"] for w in items if w["worker_id"] == "ghost"] == ["silent"]


async def test_task_listing_used_by_graph_includes_dependencies(client, db, registry):
    run_id = await start_run(
        db, registry, [task("a"), task("b", "noop", ["a"]), task("c", "noop", ["a", "b"])]
    )
    tasks = (await client.get(f"/v1/runs/{run_id}/tasks", headers=USER)).json()["items"]
    deps = {t["task_key"]: t["depends_on"] for t in tasks}
    assert deps == {"a": [], "b": ["a"], "c": ["a", "b"]}
    assert (await client.get(f"/v1/runs/{uuid.uuid4()}/tasks", headers=USER)).status_code == 404
