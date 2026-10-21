"""Reproducible FlowForge benchmarks.

Runs against a dedicated database (default ``flowforge_bench`` on the
docker-compose PostgreSQL), which is dropped and recreated. Spawns real
scheduler / worker / API processes. Writes raw results as JSON to
``benchmarks/results/`` and prints a Markdown summary.

    uv run python scripts/benchmark.py                 # full suite (~10 min)
    uv run python scripts/benchmark.py --quick         # smaller workloads, 1 trial
    uv run python scripts/benchmark.py --only linear,wide

All numbers are measured from timestamps persisted in PostgreSQL
(task_attempts.queue_wait_ms / duration_ms, tasks.completed_at, ...).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import platform
import random
import signal
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from flowforge.config import Settings  # noqa: E402
from flowforge.db.session import create_engine  # noqa: E402
from flowforge.domain.definition import WorkflowDefinitionSpec  # noqa: E402
from flowforge.engine.submission import create_definition, submit_run  # noqa: E402

DEFAULT_URL = "postgresql+asyncpg://flowforge:flowforge@localhost:5433/flowforge_bench"
TENANT = "bench"
TABLES = (
    "task_events, task_attempts, task_dependencies, tasks, idempotency_records, "
    "workflow_runs, workflow_definitions, workers"
)

# Process configuration used for every benchmark (documented in benchmark-results.md).
PROC_ENV = {
    "FLOWFORGE_LEASE_SECONDS": "10",
    "FLOWFORGE_HEARTBEAT_INTERVAL_SECONDS": "3",
    "FLOWFORGE_WORKER_POLL_INTERVAL_SECONDS": "0.05",
    "FLOWFORGE_WORKER_MAX_POLL_INTERVAL_SECONDS": "0.25",
    "FLOWFORGE_SCHEDULER_INTERVAL_SECONDS": "0.25",
    "FLOWFORGE_WORKER_SHUTDOWN_GRACE_SECONDS": "1",
    "FLOWFORGE_LOG_LEVEL": "WARNING",
    "FLOWFORGE_LOG_FORMAT": "json",
    "FLOWFORGE_API_KEYS": "bench-key:bench:admin",
    "FLOWFORGE_HANDLER_MODULES": "flowforge.handlers.builtin",
}


def pct(values: list[float], p: float) -> float | None:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100
    lo, hi = int(k), min(int(k) + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def summary(values: list[float]) -> dict[str, float | None]:
    return {
        "n": len(values),
        "p50": pct(values, 50),
        "p95": pct(values, 95),
        "p99": pct(values, 99),
        "max": max(values) if values else None,
    }


# ------------------------------------------------------------------ workloads


def t(task_id: str, handler: str = "noop", deps: list[str] | None = None, **kw: Any) -> dict:
    return {
        "id": task_id,
        "handler": handler,
        "depends_on": deps or [],
        "retry": {
            "max_attempts": 10,
            "initial_delay_seconds": 0.05,
            "max_delay_seconds": 1,
            "jitter_ratio": 0.5,
        },
        **kw,
    }


def linear(n: int = 10) -> list[dict]:
    return [t("s0")] + [t(f"s{i}", deps=[f"s{i - 1}"]) for i in range(1, n)]


def wide(n: int = 50) -> list[dict]:
    return (
        [t("root")]
        + [t(f"leaf{i}", deps=["root"]) for i in range(n)]
        + [t("join", deps=[f"leaf{i}" for i in range(n)])]
    )


def mixed(seed: int = 7, n: int = 20) -> list[dict]:
    rng = random.Random(seed)
    tasks = [t("n0")]
    for i in range(1, n):
        deps = rng.sample(range(i), k=min(i, rng.randint(1, 3)))
        tasks.append(t(f"n{i}", deps=[f"n{d}" for d in deps]))
    return tasks


def flaky(n: int = 10, p: float = 0.3) -> list[dict]:
    return [t(f"f{i}", "flaky", input={"p": p}) for i in range(n)]


def sleepy(n: int = 1, seconds: float = 0.05) -> list[dict]:
    return [t(f"z{i}", "sleep", input={"seconds": seconds}) for i in range(n)]


# ------------------------------------------------------------------ processes


@dataclass
class Cluster:
    db_url: str
    log_dir: Path
    procs: list[subprocess.Popen[bytes]] = field(default_factory=list)

    def env(self, **extra: str) -> dict[str, str]:
        env = {k: v for k, v in os.environ.items() if not k.startswith("FLOWFORGE_")}
        env.update(PROC_ENV)
        env["FLOWFORGE_DATABASE_URL"] = self.db_url
        env.update(extra)
        return env

    def spawn(self, name: str, *args: str, **env: str) -> subprocess.Popen[bytes]:
        log = (self.log_dir / f"{name}.log").open("wb")
        p = subprocess.Popen(
            [sys.executable, "-m", "flowforge", *args],
            env=self.env(**env),
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self.procs.append(p)
        return p

    def workers(self, count: int, concurrency: int) -> list[subprocess.Popen[bytes]]:
        return [
            self.spawn(
                f"worker-{i}",
                "worker",
                "--concurrency",
                str(concurrency),
                "--worker-id",
                f"bench-w{i}",
            )
            for i in range(count)
        ]

    def stop(self) -> None:
        for p in self.procs:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in self.procs:
            try:
                p.wait(20)
            except subprocess.TimeoutExpired:
                p.kill()
        self.procs.clear()


async def reset_database(url: str) -> None:
    u = make_url(url)
    if not (u.database or "").startswith("flowforge_bench"):
        raise SystemExit("benchmark database name must start with flowforge_bench")
    admin = create_async_engine(
        u.set(database="postgres").render_as_string(False), isolation_level="AUTOCOMMIT"
    )
    async with admin.connect() as conn:
        await conn.execute(text(f'DROP DATABASE IF EXISTS "{u.database}" WITH (FORCE)'))
        await conn.execute(text(f'CREATE DATABASE "{u.database}"'))
    await admin.dispose()
    env = {**os.environ, "FLOWFORGE_DATABASE_URL": url}
    # One-off setup step before any measurement; blocking is fine here.
    subprocess.run(  # noqa: ASYNC221
        [sys.executable, "-m", "flowforge", "migrate"], env=env, cwd=ROOT, check=True
    )


async def truncate(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(f"TRUNCATE {TABLES} RESTART IDENTITY CASCADE"))


async def define(engine: AsyncEngine, name: str, tasks: list[dict]) -> uuid.UUID:
    spec = WorkflowDefinitionSpec.model_validate({"name": name, "version": 1, "tasks": tasks})
    handlers = {x["handler"] for x in tasks}
    def_id, _ = await create_definition(
        engine, tenant_id=TENANT, created_by="bench", spec=spec, known_handlers=handlers
    )
    return def_id


async def submit_many(
    engine: AsyncEngine, def_id: uuid.UUID, runs: int, parallel: int = 8
) -> list[uuid.UUID]:
    ids: list[uuid.UUID] = []
    sem = asyncio.Semaphore(parallel)

    async def one(i: int) -> None:
        async with sem:
            r = await submit_run(
                engine,
                tenant_id=TENANT,
                created_by="bench",
                definition_id=def_id,
                run_input={"i": i},
                idempotency_key=None,
                max_input_bytes=65536,
            )
            ids.append(r.run_id)

    await asyncio.gather(*(one(i) for i in range(runs)))
    return ids


async def wait_runs_done(engine: AsyncEngine, run_ids: list[uuid.UUID], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        async with engine.connect() as conn:
            left = (
                await conn.execute(
                    text(
                        "SELECT count(*) FROM workflow_runs WHERE id = ANY(:ids) "
                        "AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED')"
                    ),
                    {"ids": run_ids},
                )
            ).scalar_one()
        if left == 0:
            return
        await asyncio.sleep(0.25)
    raise TimeoutError(f"{left} runs not finished within {timeout}s")


async def collect(engine: AsyncEngine, run_ids: list[uuid.UUID]) -> dict[str, Any]:
    async with engine.connect() as conn:
        attempts = (
            await conn.execute(
                text(
                    "SELECT queue_wait_ms, duration_ms, outcome FROM task_attempts "
                    "WHERE run_id = ANY(:ids)"
                ),
                {"ids": run_ids},
            )
        ).all()
        window = (
            await conn.execute(
                text(
                    "SELECT extract(epoch FROM max(t.completed_at) - min(r.created_at)) AS span, "
                    "count(*) FILTER (WHERE t.status='SUCCEEDED') AS ok, count(*) AS total "
                    "FROM tasks t JOIN workflow_runs r ON r.id = t.run_id WHERE r.id = ANY(:ids)"
                ),
                {"ids": run_ids},
            )
        ).one()
        runs = (
            await conn.execute(
                text(
                    "SELECT status, extract(epoch FROM completed_at - created_at) * 1000 AS e2e "
                    "FROM workflow_runs WHERE id = ANY(:ids)"
                ),
                {"ids": run_ids},
            )
        ).all()
        dup = (
            await conn.execute(
                text(
                    "SELECT count(*) FROM (SELECT task_id FROM task_attempts"
                    " WHERE run_id = ANY(:ids) AND outcome='SUCCEEDED'"
                    " GROUP BY task_id HAVING count(*) > 1) d"
                ),
                {"ids": run_ids},
            )
        ).scalar_one()
    span = float(window.span)
    outcomes: dict[str, int] = {}
    for a in attempts:
        outcomes[a.outcome] = outcomes.get(a.outcome, 0) + 1
    return {
        "runs": len(run_ids),
        "tasks": int(window.total),
        "tasks_succeeded": int(window.ok),
        "attempts": len(attempts),
        "attempt_outcomes": outcomes,
        "duplicate_accepted_results": int(dup),
        "runs_failed": sum(1 for r in runs if r.status != "SUCCEEDED"),
        "wall_seconds": span,
        "tasks_per_second": int(window.ok) / span if span > 0 else None,
        "queue_wait_ms": summary([float(a.queue_wait_ms) for a in attempts]),
        "execution_ms": summary(
            [float(a.duration_ms) for a in attempts if a.duration_ms is not None]
        ),
        "run_e2e_ms": summary([float(r.e2e) for r in runs if r.e2e is not None]),
    }


# ------------------------------------------------------------------ scenarios


HOT_TABLES = ("tasks", "task_events", "task_attempts", "workflow_runs", "task_dependencies")


async def table_stats(engine: AsyncEngine) -> dict[str, dict[str, int]]:
    """Live/dead tuples, on-disk size and autovacuum activity of the hot tables."""
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT relname, n_live_tup, n_dead_tup, autovacuum_count, autoanalyze_count,"
                    " pg_table_size(relid) AS table_bytes,"
                    " pg_indexes_size(relid) AS index_bytes"
                    " FROM pg_stat_user_tables WHERE relname = ANY(:t)"
                ),
                {"t": list(HOT_TABLES)},
            )
        ).mappings()
        return {r["relname"]: {k: int(v) for k, v in r.items() if k != "relname"} for r in rows}


PGSS = {"enabled": False}  # set by --pg-stat-statements


async def pgss_reset(engine: AsyncEngine) -> None:
    if PGSS["enabled"]:
        async with engine.begin() as conn:
            await conn.execute(text("SELECT pg_stat_statements_reset()"))


async def pgss_top(engine: AsyncEngine, limit: int = 8) -> list[dict[str, Any]]:
    """Most expensive statements since the last reset (server-side execution time)."""
    if not PGSS["enabled"]:
        return []
    async with engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT left(regexp_replace(query, '\\s+', ' ', 'g'), 120) AS query,"
                    " calls, round(total_exec_time::numeric, 1) AS total_ms,"
                    " round(mean_exec_time::numeric, 3) AS mean_ms,"
                    " shared_blks_hit + shared_blks_read AS blocks"
                    " FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database"
                    " WHERE datname = current_database()) ORDER BY total_exec_time DESC LIMIT :n"
                ),
                {"n": limit},
            )
        ).mappings()
        return [
            {k: (float(v) if k in ("total_ms", "mean_ms") else v) for k, v in r.items()}
            for r in rows
        ]


async def between_trials(engine: AsyncEngine, mode: str) -> None:
    if mode == "vacuum":
        async with engine.connect() as raw:
            conn = await raw.execution_options(isolation_level="AUTOCOMMIT")
            await conn.execute(text("VACUUM (ANALYZE) " + ", ".join(HOT_TABLES)))
    elif mode == "analyze":
        async with engine.connect() as raw:
            conn = await raw.execution_options(isolation_level="AUTOCOMMIT")
            await conn.execute(text("ANALYZE " + ", ".join(HOT_TABLES)))
    elif mode == "truncate":
        await truncate(engine)


async def scenario_execution(
    engine: AsyncEngine,
    cluster: Cluster,
    *,
    name: str,
    tasks: list[dict],
    runs: int,
    workers: int,
    concurrency: int,
    trials: int,
    between: str = "none",
) -> list[dict[str, Any]]:
    results = []
    cluster.workers(workers, concurrency)
    cluster.spawn("scheduler", "scheduler")
    try:
        def_id = await define(engine, f"{name}-{uuid.uuid4().hex[:6]}", tasks)
        warm = await submit_many(engine, def_id, max(2, runs // 10))  # warm-up, not measured
        await wait_runs_done(engine, warm, 300)
        for trial in range(trials):
            if trial:
                await between_trials(engine, between)
                if between == "truncate":  # the definition was truncated too
                    def_id = await define(engine, f"{name}-{uuid.uuid4().hex[:6]}", tasks)
            stats_before = await table_stats(engine)
            await pgss_reset(engine)
            ids = await submit_many(engine, def_id, runs)
            await wait_runs_done(engine, ids, 600)
            r = await collect(engine, ids)
            r["top_statements"] = await pgss_top(engine)
            r.update(
                {
                    "scenario": name,
                    "workers": workers,
                    "concurrency": concurrency,
                    "trial": trial,
                    "between_trials": between,
                    "db_before": stats_before,
                }
            )
            results.append(r)
            print(
                f"  {name} w={workers}x{concurrency} trial {trial}: "
                f"{r['tasks_per_second']:.0f} tasks/s, queue p95 "
                f"{r['queue_wait_ms']['p95']:.0f} ms | before trial: tasks "
                f"live={stats_before.get('tasks', {}).get('n_live_tup', 0)} "
                f"dead={stats_before.get('tasks', {}).get('n_dead_tup', 0)} "
                f"idx={stats_before.get('tasks', {}).get('index_bytes', 0) // 1024}KiB",
                flush=True,
            )
    finally:
        cluster.stop()
        await truncate(engine)
    return results


async def scenario_submission(
    engine: AsyncEngine,
    cluster: Cluster,
    *,
    requests: int,
    parallel: int,
    trials: int,
    port: int = 8765,
) -> list[dict]:
    """HTTP submission throughput through the API (1 uvicorn process), no workers running."""
    results = []
    cluster.spawn("api", "api", "--port", str(port))
    base = f"http://127.0.0.1:{port}"
    headers = {"X-API-Key": "bench-key"}
    try:
        async with httpx.AsyncClient(base_url=base, timeout=30) as client:
            for _ in range(100):
                with contextlib.suppress(httpx.HTTPError):
                    if (await client.get("/health/ready")).status_code == 200:
                        break
                await asyncio.sleep(0.1)
            d = (
                await client.post(
                    "/v1/workflow-definitions",
                    headers=headers,
                    json={"name": "submit-bench", "version": 1, "tasks": mixed()},
                )
            ).json()
            url = f"/v1/workflow-definitions/{d['id']}/runs"
            for trial in range(trials + 1):  # trial 0 = warm-up
                latencies: list[float] = []
                sem = asyncio.Semaphore(parallel)
                errors = 0

                async def one(
                    i: int,
                    trial: int = trial,
                    sem: asyncio.Semaphore = sem,
                    latencies: list[float] = latencies,
                ) -> None:
                    nonlocal errors
                    async with sem:
                        t0 = time.perf_counter()
                        r = await client.post(
                            url,
                            headers={**headers, "Idempotency-Key": f"t{trial}-{i}"},
                            json={"input": {"i": i}},
                        )
                        latencies.append((time.perf_counter() - t0) * 1000)
                        if r.status_code != 201:
                            errors += 1

                n = requests if trial else max(20, requests // 10)
                t0 = time.perf_counter()
                await asyncio.gather(*(one(i) for i in range(n)))
                elapsed = time.perf_counter() - t0
                if trial:
                    results.append(
                        {
                            "scenario": "submission_http",
                            "trial": trial - 1,
                            "requests": n,
                            "parallel": parallel,
                            "errors": errors,
                            "runs_per_second": n / elapsed,
                            "tasks_per_run": len(mixed()),
                            "latency_ms": summary(latencies),
                        }
                    )
                    print(
                        f"  submission trial {trial - 1}: {n / elapsed:.0f} runs/s, "
                        f"p95 {pct(latencies, 95):.1f} ms, errors {errors}",
                        flush=True,
                    )
    finally:
        cluster.stop()
        await truncate(engine)
    return results


async def scenario_crash_recovery(
    engine: AsyncEngine, cluster: Cluster, *, trials: int, lease: float = 5.0
) -> list[dict]:
    """SIGKILL a worker with in-flight tasks; time until another worker restarts them."""
    results = []
    env = {
        "FLOWFORGE_LEASE_SECONDS": str(lease),
        "FLOWFORGE_HEARTBEAT_INTERVAL_SECONDS": "1",
        "FLOWFORGE_SCHEDULER_INTERVAL_SECONDS": "1.0",
    }
    for trial in range(trials):
        # Defined per trial: the tables are truncated at the end of every trial.
        def_id = await define(engine, "crash-bench", sleepy(4, seconds=30))
        victim = cluster.spawn(
            f"victim-{trial}",
            "worker",
            "--concurrency",
            "4",
            "--worker-id",
            f"victim-{trial}",
            **env,
        )
        cluster.spawn(f"sched-{trial}", "scheduler", **env)
        [run_id] = await submit_many(engine, def_id, 1)
        while True:
            async with engine.connect() as conn:
                n = (
                    await conn.execute(
                        text("SELECT count(*) FROM tasks WHERE run_id=:r AND status='RUNNING'"),
                        {"r": run_id},
                    )
                ).scalar_one()
            if n == 4:
                break
            await asyncio.sleep(0.05)
        killed_at = datetime.now(UTC)
        victim.kill()
        victim.wait()
        cluster.spawn(
            f"rescuer-{trial}",
            "worker",
            "--concurrency",
            "4",
            "--worker-id",
            f"rescuer-{trial}",
            **env,
        )
        while True:
            async with engine.connect() as conn:
                rows = (
                    await conn.execute(
                        text(
                            "SELECT started_at FROM task_attempts"
                            " WHERE run_id=:r AND attempt_number=2"
                        ),
                        {"r": run_id},
                    )
                ).all()
            if len(rows) == 4:
                break
            await asyncio.sleep(0.05)
        recov = [(r.started_at - killed_at).total_seconds() for r in rows]
        results.append(
            {
                "scenario": "crash_recovery",
                "trial": trial,
                "lease_seconds": lease,
                "scheduler_interval_seconds": 1.0,
                "recovery_seconds": summary(recov),
            }
        )
        print(f"  crash recovery trial {trial}: max {max(recov):.2f}s (lease {lease}s)", flush=True)
        cluster.stop()
        await truncate(engine)
    return results


async def scenario_overload(
    engine: AsyncEngine, cluster: Cluster, *, seconds: float, rate: float
) -> list[dict]:
    """Submit 50 ms tasks faster than 1 worker x 4 slots can run them; sample queue depth."""
    cluster.workers(1, 4)
    cluster.spawn("scheduler", "scheduler")
    def_id = await define(engine, "overload", sleepy(1, seconds=0.05))
    samples: list[tuple[float, int]] = []
    stop = asyncio.Event()

    async def sampler() -> None:
        t0 = time.monotonic()
        while not stop.is_set():
            async with engine.connect() as conn:
                depth = (
                    await conn.execute(text("SELECT count(*) FROM tasks WHERE status='READY'"))
                ).scalar_one()
            samples.append((round(time.monotonic() - t0, 2), int(depth)))
            await asyncio.sleep(0.5)

    s = asyncio.create_task(sampler())
    ids: list[uuid.UUID] = []
    t0 = time.monotonic()
    i = 0
    while time.monotonic() - t0 < seconds:
        ids += await submit_many(engine, def_id, 1)
        i += 1
        await asyncio.sleep(max(0.0, t0 + i / rate - time.monotonic()))
    submitted_at = time.monotonic() - t0
    await wait_runs_done(engine, ids, 600)
    drained_at = time.monotonic() - t0
    stop.set()
    await s
    cluster.stop()
    r = await collect(engine, ids)
    await truncate(engine)
    peak = max(d for _, d in samples)
    print(
        f"  overload: offered {len(ids) / submitted_at:.0f}/s, served "
        f"{r['tasks_per_second']:.0f}/s, peak READY {peak}",
        flush=True,
    )
    return [
        {
            "scenario": "overload",
            "offered_rate": len(ids) / submitted_at,
            "submit_seconds": submitted_at,
            "drained_after_seconds": drained_at,
            "peak_ready_depth": peak,
            "depth_samples": samples,
            **r,
        }
    ]


# ------------------------------------------------------------------ main


def environment(url: str) -> dict[str, Any]:
    def sh(cmd: list[str]) -> str:
        try:
            return subprocess.run(cmd, capture_output=True, text=True, check=False).stdout.strip()
        except OSError:
            return "unknown"

    pg = sh(
        [
            "docker",
            "compose",
            "-f",
            str(ROOT / "deployments/docker-compose.yml"),
            "exec",
            "-T",
            "postgres",
            "postgres",
            "--version",
        ]
    )
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu": sh(["sysctl", "-n", "machdep.cpu.brand_string"]) or platform.processor(),
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
        "postgres": pg,
        "database_url_host": make_url(url).host,
        "process_env": PROC_ENV,
        "git_commit": sh(["git", "rev-parse", "--short", "HEAD"]) or None,
    }


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--database-url", default=os.environ.get("FLOWFORGE_BENCH_DATABASE_URL", DEFAULT_URL)
    )
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--trials", type=int, default=None)
    ap.add_argument("--only", default="")
    ap.add_argument(
        "--between-trials",
        choices=("none", "analyze", "vacuum", "truncate"),
        default="none",
        help="maintenance between trials of execution scenarios (default: none)",
    )
    ap.add_argument(
        "--pg-stat-statements",
        action="store_true",
        help="record per-trial top statements (server needs pg_stat_statements preloaded)",
    )
    args = ap.parse_args()
    quick = args.quick
    trials = args.trials or (1 if quick else 3)
    only = {s for s in args.only.split(",") if s}

    def want(name: str) -> bool:
        return not only or name in only

    await reset_database(args.database_url)
    if args.pg_stat_statements:
        PGSS["enabled"] = True
        ext = create_async_engine(args.database_url, isolation_level="AUTOCOMMIT")
        async with ext.connect() as conn:
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"))
        await ext.dispose()
    out_dir = ROOT / "benchmarks" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    log_dir = out_dir / "logs"
    log_dir.mkdir(exist_ok=True)
    settings = Settings(_env_file=None, database_url=args.database_url)  # type: ignore[call-arg]
    engine = create_engine(settings, pool_size=10)
    cluster = Cluster(args.database_url, log_dir)
    env = environment(args.database_url)
    print(json.dumps({k: v for k, v in env.items() if k != "process_env"}, indent=2))
    results: list[dict] = []
    scale = 0.25 if quick else 1.0
    try:
        if want("submission"):
            results += await scenario_submission(
                engine, cluster, requests=int(1000 * scale), parallel=16, trials=trials
            )
        if want("linear"):
            for w in (1, 2, 4):
                results += await scenario_execution(
                    engine,
                    cluster,
                    name="linear10",
                    tasks=linear(10),
                    runs=int(100 * scale),
                    workers=w,
                    concurrency=4,
                    trials=trials,
                    between=args.between_trials,
                )
        if want("wide"):
            for w in (1, 2, 4):
                results += await scenario_execution(
                    engine,
                    cluster,
                    name="wide50",
                    tasks=wide(50),
                    runs=int(20 * scale),
                    workers=w,
                    concurrency=8,
                    trials=trials,
                    between=args.between_trials,
                )
        if want("mixed"):
            results += await scenario_execution(
                engine,
                cluster,
                name="mixed20",
                tasks=mixed(),
                runs=int(100 * scale),
                workers=4,
                concurrency=4,
                trials=trials,
                between=args.between_trials,
            )
        if want("flaky"):
            results += await scenario_execution(
                engine,
                cluster,
                name="flaky_p0.3",
                tasks=flaky(10, 0.3),
                runs=int(100 * scale),
                workers=4,
                concurrency=4,
                trials=trials,
                between=args.between_trials,
            )
        if want("crash"):
            results += await scenario_crash_recovery(engine, cluster, trials=trials)
        if want("overload"):
            results += await scenario_overload(
                engine, cluster, seconds=10 * max(scale, 0.5), rate=120
            )
    finally:
        cluster.stop()
        await engine.dispose()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = out_dir / f"{stamp}{'-quick' if quick else ''}.json"
    path.write_text(json.dumps({"environment": env, "results": results}, indent=2, default=str))
    print(f"\nraw results: {path.relative_to(ROOT)}")
    print(render_markdown(results))


def _f(v: Any, nd: int = 0) -> str:
    return "-" if v is None else f"{v:.{nd}f}"


def render_markdown(results: list[dict]) -> str:
    lines = []
    sub = [r for r in results if r["scenario"] == "submission_http"]
    if sub:
        lines += [
            "| HTTP submission | runs/s | p50 ms | p95 ms | p99 ms | errors |",
            "|---|---|---|---|---|---|",
        ]
        for r in sub:
            lat = r["latency_ms"]
            lines.append(
                f"| trial {r['trial']} ({r['requests']} req, {r['parallel']} parallel)"
                f" | {_f(r['runs_per_second'])} | {_f(lat['p50'], 1)} | "
                f"{_f(lat['p95'], 1)} | {_f(lat['p99'], 1)} | {r['errors']} |"
            )
        lines.append("")
    exe = [r for r in results if "workers" in r]
    if exe:
        lines += [
            "| workload | workers x slots | trial | tasks | attempts | tasks/s | "
            "queue wait p50/p95/p99 ms | exec p50/p95/p99 ms | run e2e p50/p95 ms | "
            "dup results |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]
        for r in exe:
            q, e, rr = r["queue_wait_ms"], r["execution_ms"], r["run_e2e_ms"]
            lines.append(
                f"| {r['scenario']} | {r['workers']}x{r['concurrency']} | {r['trial']} | "
                f"{r['tasks']} | {r['attempts']} | {_f(r['tasks_per_second'])} | "
                f"{_f(q['p50'])}/{_f(q['p95'])}/{_f(q['p99'])} | "
                f"{_f(e['p50'])}/{_f(e['p95'])}/{_f(e['p99'])} | "
                f"{_f(rr['p50'])}/{_f(rr['p95'])} | {r['duplicate_accepted_results']} |"
            )
        lines.append("")
    crash = [r for r in results if r["scenario"] == "crash_recovery"]
    if crash:
        lines += ["| crash recovery | lease s | recovery p50 s | max s |", "|---|---|---|---|"]
        for r in crash:
            rs = r["recovery_seconds"]
            lines.append(
                f"| trial {r['trial']} | {r['lease_seconds']} | {_f(rs['p50'], 2)} | "
                f"{_f(rs['max'], 2)} |"
            )
        lines.append("")
    for r in (r for r in results if r["scenario"] == "overload"):
        lines.append(
            f"Overload: offered {_f(r['offered_rate'])} tasks/s for "
            f"{_f(r['submit_seconds'], 1)} s; served {_f(r['tasks_per_second'])} "
            f"tasks/s; peak READY depth {r['peak_ready_depth']}; drained after "
            f"{_f(r['drained_after_seconds'], 1)} s."
        )
    return "\n".join(lines)


if __name__ == "__main__":
    asyncio.run(main())
