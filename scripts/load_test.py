"""Sustained multi-process load test with failures injected under load.

Topology (all on this host, one PostgreSQL):
  3 API processes, 2 schedulers (running concurrently), 6 workers x 4 slots.

Load: open-loop HTTP submissions at --rate runs/s, round-robin over the API
processes, each with an idempotency key. On a transport error or 5xx the client
retries the *same* key on the next API (so a retry can never duplicate a run).
Workflow: 8 tasks (fan-out 3, join, fan-out 2, final), 5 of them sleep 20 ms.

Timeline (seconds, --duration scales it):
  0-30 warm-up | 30-90 steady | 90 SIGKILL worker 1 | 120 start a replacement
  150 SIGKILL scheduler 1 | 170 SIGKILL API 1 | 210 stop submitting | drain

Sampled each second: READY depth, queue age (oldest READY), RUNNING, completions,
DB connections per component, deadlocks. Lock waits are sampled 10x per second
and attributed to the waiting statement.

    uv run python scripts/load_test.py                 # ~4-5 min
    uv run python scripts/load_test.py --rate 15 --duration 120

Writes benchmarks/results/load-<timestamp>.json and an SVG timeline, and prints
a Markdown summary. Multi-host partitions and PostgreSQL failover are NOT covered.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import itertools
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "scripts"), str(ROOT)]

from benchmark import environment, pct, reset_database, summary  # noqa: E402

from flowforge.config import Settings  # noqa: E402
from flowforge.db.session import create_engine  # noqa: E402

DEFAULT_DB_URL = "postgresql+asyncpg://flowforge:flowforge@localhost:5433/flowforge_bench_load"
DB_URL = DEFAULT_DB_URL
API_PORTS = (8801, 8802, 8803)
KEY = "load-test-key"
ENV = {
    "FLOWFORGE_API_KEYS": f"{KEY}:load:admin",
    "FLOWFORGE_HANDLER_MODULES": "flowforge.handlers.builtin",
    "FLOWFORGE_LEASE_SECONDS": "10",
    "FLOWFORGE_HEARTBEAT_INTERVAL_SECONDS": "3",
    "FLOWFORGE_WORKER_POLL_INTERVAL_SECONDS": "0.05",
    "FLOWFORGE_WORKER_MAX_POLL_INTERVAL_SECONDS": "0.25",
    "FLOWFORGE_SCHEDULER_INTERVAL_SECONDS": "0.5",
    "FLOWFORGE_WORKER_SHUTDOWN_GRACE_SECONDS": "2",
    "FLOWFORGE_LOG_LEVEL": "WARNING",
}
WORKFLOW = {
    "name": "load-mixed",
    "version": 1,
    "tasks": [
        {"id": "fetch", "handler": "fetch_data"},
        *[
            {
                "id": f"work{i}",
                "handler": "sleep",
                "depends_on": ["fetch"],
                "input": {"seconds": 0.02},
            }
            for i in range(3)
        ],
        {"id": "join", "handler": "noop", "depends_on": ["work0", "work1", "work2"]},
        *[
            {
                "id": f"post{i}",
                "handler": "sleep",
                "depends_on": ["join"],
                "input": {"seconds": 0.02},
            }
            for i in range(2)
        ],
        {"id": "final", "handler": "noop", "depends_on": ["post0", "post1"]},
    ],
}
TASKS_PER_RUN = len(WORKFLOW["tasks"])


class Procs:
    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir
        self.procs: dict[str, subprocess.Popen[bytes]] = {}

    def spawn(self, name: str, component: str, *args: str) -> None:
        env = {k: v for k, v in os.environ.items() if not k.startswith("FLOWFORGE_")}
        env.update(ENV, FLOWFORGE_DATABASE_URL=DB_URL, FLOWFORGE_SERVICE_NAME=f"ff-{component}")
        log = (self.log_dir / f"{name}.log").open("wb")
        self.procs[name] = subprocess.Popen(
            [sys.executable, "-m", "flowforge", *args], env=env, cwd=ROOT, stdout=log, stderr=log
        )

    def kill(self, name: str) -> None:
        p = self.procs[name]
        p.send_signal(signal.SIGKILL)
        p.wait()

    def stop_all(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in self.procs.values():
            try:
                p.wait(15)
            except subprocess.TimeoutExpired:
                p.kill()


class Submitter:
    """Open-loop load: request i is due at t0 + i / rate, whatever earlier ones did."""

    def __init__(self, def_id: str, rate: float) -> None:
        self.def_id, self.rate = def_id, rate
        self.rr = itertools.count()
        self.records: list[dict[str, Any]] = []
        self.sem = asyncio.Semaphore(256)

    async def one(self, client: httpx.AsyncClient, i: int, due: float) -> None:
        key = f"load-{i}"
        rec: dict[str, Any] = {"i": i, "due": due, "failovers": 0, "status": None}
        start = time.monotonic()
        async with self.sem:
            for _ in range(len(API_PORTS) + 1):
                rec["attempts"] = rec.get("attempts", 0) + 1
                port = API_PORTS[next(self.rr) % len(API_PORTS)]
                try:
                    r = await client.post(
                        f"http://127.0.0.1:{port}/v1/workflow-definitions/{self.def_id}/runs",
                        headers={"X-API-Key": KEY, "Idempotency-Key": key},
                        json={"input": {"i": i}},
                        timeout=10,
                    )
                except httpx.TransportError:
                    rec["failovers"] += 1
                    continue
                if r.status_code >= 500:
                    rec["failovers"] += 1
                    continue
                rec["status"] = r.status_code
                if r.status_code in (200, 201):
                    rec["run_id"] = r.json()["id"]
                break
        rec["latency_ms"] = (time.monotonic() - start) * 1000
        self.records.append(rec)

    async def run(self, until: float, t0: float) -> None:
        async with httpx.AsyncClient(limits=httpx.Limits(max_connections=128)) as client:
            tasks = []
            i = 0
            while (due := t0 + i / self.rate) < until:
                await asyncio.sleep(max(0.0, due - time.monotonic()))
                tasks.append(asyncio.create_task(self.one(client, i, due - t0)))
                i += 1
            await asyncio.gather(*tasks)


SAMPLE_SQL = text("""
    SELECT
      (SELECT count(*) FROM tasks WHERE status = 'READY') AS ready,
      (SELECT coalesce(extract(epoch FROM statement_timestamp() - min(ready_at)), 0)
         FROM tasks WHERE status = 'READY')::float8 AS queue_age_s,
      (SELECT count(*) FROM tasks WHERE status = 'RUNNING') AS running,
      (SELECT count(*) FROM tasks WHERE status = 'RETRY_WAIT') AS retry_wait,
      (SELECT count(*) FROM tasks WHERE status = 'SUCCEEDED') AS succeeded,
      (SELECT count(*) FROM workflow_runs WHERE status IN ('PENDING', 'RUNNING')) AS active_runs,
      (SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()) AS deadlocks
""")
CONN_SQL = text("""
    SELECT coalesce(nullif(application_name, ''), 'other') AS app, count(*)
      FROM pg_stat_activity WHERE datname = current_database() GROUP BY 1
""")
LOCK_SQL = text("""
    SELECT left(regexp_replace(query, '\\s+', ' ', 'g'), 40) AS q
      FROM pg_stat_activity
     WHERE datname = current_database() AND wait_event_type = 'Lock'
""")


def classify(q: str) -> str:
    q = q.strip()
    if "FROM workflow_runs" in q or q.startswith("SELECT workflow_runs"):
        return "run lock (progression)"
    if q.startswith("WITH t AS"):
        return "fenced completion"
    if q.startswith("WITH picked"):
        return "claim"
    if "promoted" in q or "due AS" in q:
        return "retry promotion"
    return q


async def sampler(
    db: AsyncEngine,
    t0: float,
    stop: asyncio.Event,
    out: list[dict[str, Any]],
    locks: list[dict[str, Any]],
) -> None:
    tick = 0
    async with db.connect() as conn:
        while not stop.is_set():
            now = time.monotonic() - t0
            waits = [classify(r.q) for r in (await conn.execute(LOCK_SQL)).all()]
            locks.append({"t": round(now, 2), "waiting": waits})
            if tick % 10 == 0:
                row = (await conn.execute(SAMPLE_SQL)).mappings().one()
                conns = {r.app: r[1] for r in (await conn.execute(CONN_SQL)).all()}
                out.append(
                    {
                        "t": round(now, 1),
                        **{k: float(v) for k, v in row.items()},
                        "connections": conns,
                    }
                )
            await conn.commit()
            tick += 1
            await asyncio.sleep(0.1)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rate", type=float, default=25.0, help="run submissions per second")
    ap.add_argument("--duration", type=float, default=210.0, help="seconds of submission")
    ap.add_argument("--database-url", default=DEFAULT_DB_URL)
    ap.add_argument(
        "--pg-stat-statements",
        action="store_true",
        help="record top statements (server needs pg_stat_statements preloaded)",
    )
    args = ap.parse_args()
    global DB_URL  # noqa: PLW0603
    DB_URL = args.database_url
    k = args.duration / 210.0  # scale the timeline
    t_kill_w, t_new_w, t_kill_s, t_kill_a = 90 * k, 120 * k, 150 * k, 170 * k

    await reset_database(DB_URL)
    if args.pg_stat_statements:
        from sqlalchemy.ext.asyncio import create_async_engine

        ext = create_async_engine(DB_URL, isolation_level="AUTOCOMMIT")
        async with ext.connect() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"))
            await conn.execute(text("SELECT pg_stat_statements_reset()"))
        await ext.dispose()
    out_dir = ROOT / "benchmarks" / "results"
    log_dir = out_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    db = create_engine(
        Settings(_env_file=None, database_url=DB_URL, service_name="ff-observer"), pool_size=4
    )  # type: ignore[call-arg]
    procs = Procs(log_dir)
    for i, port in enumerate(API_PORTS, 1):
        procs.spawn(f"api-{i}", "api", "api", "--port", str(port))
    for i in (1, 2):
        procs.spawn(f"scheduler-{i}", "scheduler", "scheduler")
    for i in range(1, 7):
        procs.spawn(
            f"worker-{i}", "worker", "worker", "--concurrency", "4", "--worker-id", f"load-w{i}"
        )
    events: list[dict[str, Any]] = []
    try:
        async with httpx.AsyncClient() as c:
            for _ in range(200):
                with contextlib.suppress(httpx.HTTPError):
                    if all(
                        [
                            (await c.get(f"http://127.0.0.1:{p}/health/ready")).status_code == 200
                            for p in API_PORTS
                        ]
                    ):
                        break
                await asyncio.sleep(0.1)
            d = await c.post(
                f"http://127.0.0.1:{API_PORTS[0]}/v1/workflow-definitions",
                headers={"X-API-Key": KEY},
                json=WORKFLOW,
            )
            def_id = d.json()["id"]
        async with db.connect() as conn:
            db_t0 = (await conn.execute(text("SELECT statement_timestamp()"))).scalar_one()
        t0 = time.monotonic()
        stop = asyncio.Event()
        samples: list[dict[str, Any]] = []
        locks: list[dict[str, Any]] = []
        sampling = asyncio.create_task(sampler(db, t0, stop, samples, locks))
        sub = Submitter(def_id, args.rate)
        submitting = asyncio.create_task(sub.run(t0 + args.duration, t0))

        async def at(t: float) -> None:
            await asyncio.sleep(max(0.0, t0 + t - time.monotonic()))

        await at(t_kill_w)
        # Kill while the worker holds work, so recovery under load is measured. Which
        # tasks were in flight is read from the database afterwards (attempts of this
        # worker that ended LEASE_EXPIRED), not from a snapshot that races the kill.
        busy = 0
        for _ in range(100):
            async with db.connect() as conn:
                busy = (
                    await conn.execute(
                        text(
                            "SELECT count(*) FROM tasks WHERE status = 'RUNNING'"
                            " AND lease_owner = 'load-w1'"
                        )
                    )
                ).scalar_one()
            if busy:
                break
            await asyncio.sleep(0.05)
        procs.kill("worker-1")
        async with db.connect() as conn:
            kill_db_t = (await conn.execute(text("SELECT statement_timestamp()"))).scalar_one()
        inflight: list[Any] = []
        events.append(
            {
                "t": round(time.monotonic() - t0, 1),
                "event": "SIGKILL worker-1",
            }
        )
        await at(t_new_w)
        procs.spawn("worker-7", "worker", "worker", "--concurrency", "4", "--worker-id", "load-w7")
        events.append({"t": round(time.monotonic() - t0, 1), "event": "start worker-7"})
        await at(t_kill_s)
        procs.kill("scheduler-1")
        events.append({"t": round(time.monotonic() - t0, 1), "event": "SIGKILL scheduler-1"})
        await at(t_kill_a)
        procs.kill("api-1")
        events.append({"t": round(time.monotonic() - t0, 1), "event": "SIGKILL api-1"})
        await submitting
        events.append({"t": round(time.monotonic() - t0, 1), "event": "stop submitting"})
        drain_deadline = time.monotonic() + 180
        while time.monotonic() < drain_deadline:
            if samples and samples[-1]["active_runs"] == 0 and samples[-1]["t"] > args.duration:
                break
            await asyncio.sleep(1)
        events.append({"t": round(time.monotonic() - t0, 1), "event": "drained"})
        stop.set()
        await sampling
        result = await analyse(db, db_t0, sub, samples, locks, events, inflight, kill_db_t, args)
    finally:
        procs.stop_all()
        await db.dispose()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    (out_dir / f"load-{stamp}.json").write_text(json.dumps(result, indent=2, default=str))
    svg = render_svg(result)
    (out_dir / f"load-{stamp}.svg").write_text(svg)
    print(f"raw: benchmarks/results/load-{stamp}.json  chart: benchmarks/results/load-{stamp}.svg")
    print(render_markdown(result))


PHASES = [
    ("warm-up", 0, 30),
    ("steady", 30, 90),
    ("worker crash + replacement", 90, 150),
    ("scheduler + API crash", 150, 210),
]


async def analyse(db, db_t0, sub, samples, locks, events, inflight, kill_db_t, args):
    k = args.duration / 210.0
    phases = [(n, a * k, b * k) for n, a, b in PHASES]
    ids = [r["run_id"] for r in sub.records if "run_id" in r]
    async with db.connect() as conn:
        runs = (
            (
                await conn.execute(
                    text(
                        "SELECT id, status, extract(epoch FROM created_at - :t0)::float8 AS t,"
                        " extract(epoch FROM completed_at - created_at)::float8 * 1000 AS e2e_ms"
                        " FROM workflow_runs"
                    ),
                    {"t0": db_t0},
                )
            )
            .mappings()
            .all()
        )
        attempts = (
            (
                await conn.execute(
                    text(
                        "SELECT extract(epoch FROM started_at - :t0)::float8 AS t,"
                        " queue_wait_ms, outcome"
                        " FROM task_attempts"
                    ),
                    {"t0": db_t0},
                )
            )
            .mappings()
            .all()
        )
        # In flight at the kill = the killed worker's attempts that ended LEASE_EXPIRED.
        inflight = [
            r.task_id
            for r in await conn.execute(
                text(
                    "SELECT task_id FROM task_attempts"
                    " WHERE worker_id = 'load-w1' AND outcome = 'LEASE_EXPIRED'"
                )
            )
        ]
        recovery = (
            await conn.execute(
                text(
                    "SELECT extract(epoch FROM min(a.started_at) - :k)::float8 AS s"
                    " FROM task_attempts a"
                    " WHERE a.task_id = ANY(:ids) AND a.worker_id <> 'load-w1'"
                    " GROUP BY a.task_id"
                ),
                {"k": kill_db_t, "ids": inflight},
            )
        ).all()
        dup_success = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM (SELECT task_id FROM task_attempts"
                    " WHERE outcome = 'SUCCEEDED'"
                    " GROUP BY task_id HAVING count(*) > 1) d"
                )
            )
        ).scalar_one()
        top_statements = []
        if args.pg_stat_statements:
            top_statements = [
                dict(r)
                for r in (
                    await conn.execute(
                        text(
                            "SELECT left(regexp_replace(query, '\\s+', ' ', 'g'), 90) AS query,"
                            " calls, round(total_exec_time::numeric) AS total_ms,"
                            " round(mean_exec_time::numeric, 3) AS mean_ms,"
                            " shared_blks_hit + shared_blks_read AS blocks"
                            " FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database"
                            " WHERE datname = current_database())"
                            " ORDER BY total_exec_time DESC LIMIT 12"
                        )
                    )
                ).mappings()
            ]
        from tests.support.invariants import INVARIANTS

        violations = {}
        for name, sql in INVARIANTS.items():
            n = len((await conn.execute(text(sql))).all())
            if n:
                violations[name] = n
    by_status: dict[str, int] = {}
    for r in runs:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    per_phase = []
    for name, a, b in phases:
        e2e = [r["e2e_ms"] for r in runs if a <= r["t"] < b and r["e2e_ms"] is not None]
        qw = [x["queue_wait_ms"] for x in attempts if a <= x["t"] < b]
        sub_lat = [r["latency_ms"] for r in sub.records if a <= r["due"] < b]
        ss = [s for s in samples if a <= s["t"] < b]
        done = [s["succeeded"] for s in ss]
        thr = (done[-1] - done[0]) / (ss[-1]["t"] - ss[0]["t"]) if len(ss) > 1 else None
        lk = [len(x["waiting"]) for x in locks if a <= x["t"] < b]
        per_phase.append(
            {
                "phase": name,
                "runs": len(e2e),
                "tasks_per_s": thr,
                "submit_ms": summary(sub_lat),
                "queue_wait_ms": summary(qw),
                "run_e2e_ms": summary(e2e),
                "queue_age_s_max": max((s["queue_age_s"] for s in ss), default=None),
                "queue_age_s_p95": pct([s["queue_age_s"] for s in ss], 95),
                "ready_depth_max": max((s["ready"] for s in ss), default=None),
                "lock_waiting_sessions_mean": sum(lk) / len(lk) if lk else None,
                "lock_waiting_sessions_max": max(lk, default=None),
                "lock_wait_sample_share": sum(1 for n in lk if n) / len(lk) if lk else None,
            }
        )
    lock_kinds: dict[str, int] = {}
    for x in locks:
        for w in x["waiting"]:
            lock_kinds[w] = lock_kinds.get(w, 0) + 1
    conns_max: dict[str, int] = {}
    for s in samples:
        for app, n in s["connections"].items():
            conns_max[app] = max(conns_max.get(app, 0), int(n))
    statuses = {}
    for r in sub.records:
        statuses[str(r["status"])] = statuses.get(str(r["status"]), 0) + 1
    return {
        "environment": environment(DB_URL),
        "config": {
            "rate_runs_per_s": args.rate,
            "duration_s": args.duration,
            "tasks_per_run": TASKS_PER_RUN,
            "apis": len(API_PORTS),
            "schedulers": 2,
            "workers": "6 x 4 slots (+1 replacement)",
            "env": ENV,
        },
        "events": events,
        "submissions": {
            "sent": len(sub.records),
            "http_status": statuses,
            "accepted": len(ids),
            "distinct_runs": len(set(ids)),
            "with_failover": sum(1 for r in sub.records if r["failovers"]),
            "lost": len(sub.records) - len(ids),
        },
        "runs_in_db": len(runs),
        "run_status": by_status,
        "duplicate_accepted_successes": dup_success,
        "invariant_violations": violations,
        "attempt_outcomes": {
            o: sum(1 for x in attempts if x["outcome"] == o)
            for o in {x["outcome"] for x in attempts}
        },
        "crash_recovery": {
            "inflight_on_killed_worker": len(inflight),
            "recovered": len(recovery),
            "seconds": summary([r.s for r in recovery]),
        },
        "phases": per_phase,
        "lock_waits_by_statement": lock_kinds,
        "lock_samples": len(locks),
        "db_connections_max": conns_max,
        "deadlocks": (samples[-1]["deadlocks"] - samples[0]["deadlocks"]) if samples else None,
        "top_statements": top_statements,
        "timeseries": samples,
    }


def _f(v: Any, nd: int = 0) -> str:
    return "-" if v is None else f"{v:.{nd}f}"


def render_markdown(r: dict[str, Any]) -> str:
    s = r["submissions"]
    lines = [
        f"Submitted {s['sent']} runs at {r['config']['rate_runs_per_s']}/s: accepted "
        f"{s['accepted']} ({s['distinct_runs']} distinct), {s['with_failover']} needed "
        f"failover, lost {s['lost']}. Runs in DB: {r['runs_in_db']} {r['run_status']}. "
        f"Duplicate successes: {r['duplicate_accepted_successes']}. Invariant violations: "
        f"{r['invariant_violations'] or 'none'}. Deadlocks: {_f(r['deadlocks'])}.",
        "",
        "| phase | tasks/s | submit p50/p95/p99 ms | queue wait p50/p95/p99 ms | "
        "run e2e p50/p95/p99 ms | queue age p95/max s | READY max | lock-wait samples | "
        "waiting sessions mean/max |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for p in r["phases"]:
        sm, q, e = p["submit_ms"], p["queue_wait_ms"], p["run_e2e_ms"]
        lines.append(
            f"| {p['phase']} | {_f(p['tasks_per_s'])} | {_f(sm['p50'])}/{_f(sm['p95'])}/"
            f"{_f(sm['p99'])} | {_f(q['p50'])}/{_f(q['p95'])}/{_f(q['p99'])} | {_f(e['p50'])}/"
            f"{_f(e['p95'])}/{_f(e['p99'])} | {_f(p['queue_age_s_p95'], 2)}/"
            f"{_f(p['queue_age_s_max'], 2)} | {_f(p['ready_depth_max'])} | "
            f"{_f((p['lock_wait_sample_share'] or 0) * 100, 1)}% | "
            f"{_f(p['lock_waiting_sessions_mean'], 2)}/{_f(p['lock_waiting_sessions_max'])} |"
        )
    cr = r["crash_recovery"]
    lines += [
        "",
        f"Crash recovery under load: {cr['inflight_on_killed_worker']} tasks in flight "
        f"on the killed worker, {cr['recovered']} re-executed; kill -> re-execution "
        f"p50 {_f(cr['seconds']['p50'], 2)} s, max {_f(cr['seconds']['max'], 2)} s "
        "(lease 10 s, scheduler interval 0.5 s).",
        f"Lock waits by statement (10 Hz samples): {r['lock_waits_by_statement']}",
        f"Max DB connections by component: {r['db_connections_max']}",
        f"Attempt outcomes: {r['attempt_outcomes']}",
        f"Events: {r['events']}",
    ]
    for st in r.get("top_statements") or []:
        lines.append(
            f"  {st['total_ms']:>8} ms {st['calls']:>8} calls mean {st['mean_ms']:>8} ms"
            f" blks {st['blocks']:>10}  {st['query']}"
        )
    return "\n".join(lines)


def render_svg(r: dict[str, Any]) -> str:
    """Minimal dependency-free SVG timeline: throughput, queue age, READY depth."""
    ts = r["timeseries"]
    if len(ts) < 3:
        return "<svg xmlns='http://www.w3.org/2000/svg'/>"
    w, h, pad = 900, 420, 50
    tmax = ts[-1]["t"]
    thr = [
        (
            ts[i]["t"],
            (ts[i]["succeeded"] - ts[i - 1]["succeeded"]) / max(ts[i]["t"] - ts[i - 1]["t"], 1e-6),
        )
        for i in range(1, len(ts))
    ]
    series = [
        ("completed tasks/s", thr, "#2563eb", 0),
        ("queue age (s)", [(s["t"], s["queue_age_s"]) for s in ts], "#c2272d", 1),
        ("READY depth", [(s["t"], s["ready"]) for s in ts], "#b26b00", 2),
    ]
    band = (h - pad * 2) / 3
    parts = [
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {w} {h}' "
        "font-family='system-ui, sans-serif' font-size='11'>",
        f"<rect width='{w}' height='{h}' fill='white'/>",
    ]
    for label, pts, color, row in series:
        top = pad + row * band
        vmax = max((v for _, v in pts), default=1) or 1
        path = " ".join(
            f"{'M' if i == 0 else 'L'}{pad + (t / tmax) * (w - 2 * pad):.1f},"
            f"{top + band - 8 - (v / vmax) * (band - 20):.1f}"
            for i, (t, v) in enumerate(pts)
        )
        parts += [
            f"<text x='{pad}' y='{top + 10}' fill='{color}'>{label} (max {vmax:.1f})</text>",
            f"<path d='{path}' fill='none' stroke='{color}' stroke-width='1.5'/>",
            f"<line x1='{pad}' x2='{w - pad}' y1='{top + band - 8}' y2='{top + band - 8}' "
            "stroke='#ccc'/>",
        ]
    for e in r["events"]:
        x = pad + (e["t"] / tmax) * (w - 2 * pad)
        parts += [
            f"<line x1='{x:.1f}' x2='{x:.1f}' y1='{pad}' y2='{h - pad}' stroke='#888' "
            "stroke-dasharray='3 3'/>",
            f"<text x='{x + 3:.1f}' y='{h - pad + 14}' fill='#555' "
            f"transform='rotate(20 {x + 3:.1f} {h - pad + 14})'>{e['event']}</text>",
        ]
    parts.append(
        f"<text x='{w - pad}' y='{h - 8}' text-anchor='end' fill='#555'>"
        f"seconds (0-{tmax:.0f})</text></svg>"
    )
    return "".join(parts)


if __name__ == "__main__":
    asyncio.run(main())
