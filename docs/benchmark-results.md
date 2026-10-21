# Benchmark results

All numbers on this page were measured with `scripts/benchmark.py` on the environment
below. They describe this machine and configuration only. They are **not** a claim
about production capacity.

## Environment

| item | value |
|---|---|
| machine | Apple A18 Pro, 6 CPU cores, 8 GB RAM (MacBook) |
| OS | macOS 26.6.2 (arm64) |
| PostgreSQL | 17.11 (`postgres:17-alpine`) in Docker Desktop 29.8 (Linux VM: 6 vCPU, 4 GB), default config except `max_connections=300`, `synchronous_commit=on`, `fsync=on` |
| Python | 3.12.13, asyncpg 0.32, SQLAlchemy 2.1, FastAPI 0.156, uvicorn 0.54 |
| topology | everything on one host: PostgreSQL VM, scheduler, workers, the benchmark driver, and, for the submission benchmark, one uvicorn API process |
| measured round trip | `SELECT 1` through asyncpg: p50 0.17 ms; commit with WAL flush: p50 0.47 ms |

Process configuration for every scenario (`PROC_ENV` in the script):

- lease 10 s, heartbeat 3 s;
- worker poll interval 50 ms, backing off to 250 ms when idle;
- scheduler interval 250 ms;
- log level WARNING.

The crash scenario overrides the lease to 5 s, the heartbeat to 1 s, and the scheduler
interval to 1 s.

## Methodology

- **Isolation.** A dedicated database, `flowforge_bench`, is dropped, recreated, and
  migrated before the suite. Tables are truncated between scenarios. Workers, the
  scheduler, and the API are spawned as real processes (`python -m flowforge ...`).
- **Warm-up.** Each execution scenario first runs a warm-up batch (10% of the workload)
  that is not measured. The submission benchmark runs an unmeasured warm-up round.
- **Trials.** There are 3 measured trials per configuration, all reported. No trial is
  dropped.
- **Workload shape.** All runs of a trial are submitted as a burst, 8 concurrent
  submitters calling the engine directly. Workers start claiming immediately, so
  *queue wait* includes the backlog that the burst creates. This is a saturation test,
  not a latency-at-low-load test.
- **What the metrics mean.** All measurements come from timestamps stored in
  PostgreSQL:
  - *tasks/s* = succeeded tasks ÷ (last task `completed_at` − first run `created_at`).
  - *queue wait* = `task_attempts.queue_wait_ms`, from READY to claimed.
  - *exec* = `duration_ms`, the handler's wall time measured by the worker.
  - *run e2e* = run `completed_at − created_at`.
  - *dup results* = the number of tasks with more than one SUCCEEDED attempt. It must be
    0.
- **Handlers.** All handlers are `noop` unless stated otherwise, so the numbers measure
  engine overhead (claim, commit, progression), not useful work.
- **Workloads:**
  - `linear10`: a chain of 10 tasks × 100 runs.
  - `wide50`: root → 50 parallel leaves → join, × 20 runs (52 tasks each).
  - `mixed20`: a seeded random DAG of 20 tasks with 1 to 3 dependencies each, × 100 runs.
  - `flaky_p0.3`: 10 independent tasks that fail transiently with probability 0.3, ×
    100 runs. Each task allows up to 10 attempts with 50 ms initial backoff and jitter.
- **Submission:** HTTP `POST /v1/workflow-definitions/{id}/runs` for a 20-task
  definition, with an Idempotency-Key, 16 concurrent clients, 1000 requests, one
  uvicorn process, and no workers running.
- **Crash recovery:** one worker with 4 slots runs 4 tasks of 30 s each. The harness
  sends SIGKILL, starts a fresh worker, and measures *kill → attempt 2 started* for each
  task.
- **Overload:** 50 ms `sleep` tasks are offered at about 120 tasks/s to one worker with
  4 slots. Its capacity is about 80 tasks/s at most. READY depth is sampled every 0.5 s.

Reproduce with `uv run python scripts/benchmark.py`; `--quick` gives a smaller run.

## Results (current implementation)

Raw data: [`benchmarks/results/20261021T125203Z.json`](../benchmarks/results/20261021T125203Z.json),
which includes per-trial percentiles and the READY-depth samples for the overload run.
The table below is exactly what the script printed.

| HTTP submission | runs/s | p50 ms | p95 ms | p99 ms | errors |
|---|---|---|---|---|---|
| trial 0 (1000 req, 16 parallel) | 257 | 53.0 | 100.9 | 181.7 | 0 |
| trial 1 (1000 req, 16 parallel) | 245 | 56.7 | 108.5 | 205.7 | 0 |
| trial 2 (1000 req, 16 parallel) | 222 | 60.6 | 142.5 | 216.1 | 0 |

| workload | workers x slots | trial | tasks | attempts | tasks/s | queue wait p50/p95/p99 ms | exec p50/p95/p99 ms | run e2e p50/p95 ms | dup results |
|---|---|---|---|---|---|---|---|---|---|
| linear10 | 1x4 | 0 | 1000 | 1000 | 476 | 182/246/263 | 0/0/0 | 1898/1953 | 0 |
| linear10 | 1x4 | 1 | 1000 | 1000 | 434 | 192/332/334 | 0/0/0 | 2122/2136 | 0 |
| linear10 | 1x4 | 2 | 1000 | 1000 | 534 | 156/218/246 | 0/0/0 | 1653/1676 | 0 |
| linear10 | 2x4 | 0 | 1000 | 1000 | 735 | 106/155/164 | 0/0/0 | 1216/1226 | 0 |
| linear10 | 2x4 | 1 | 1000 | 1000 | 701 | 111/174/194 | 0/0/1 | 1277/1296 | 0 |
| linear10 | 2x4 | 2 | 1000 | 1000 | 738 | 107/162/187 | 0/0/0 | 1208/1217 | 0 |
| linear10 | 4x4 | 0 | 1000 | 1000 | 867 | 87/106/115 | 0/0/1 | 969/1005 | 0 |
| linear10 | 4x4 | 1 | 1000 | 1000 | 889 | 81/99/103 | 0/0/1 | 916/937 | 0 |
| linear10 | 4x4 | 2 | 1000 | 1000 | 846 | 87/108/114 | 0/0/1 | 971/995 | 0 |
| wide50 | 1x8 | 0 | 1040 | 1040 | 552 | 772/1375/1425 | 0/0/1 | 1810/1864 | 0 |
| wide50 | 1x8 | 1 | 1040 | 1040 | 545 | 923/1533/1586 | 0/0/1 | 1870/1891 | 0 |
| wide50 | 1x8 | 2 | 1040 | 1040 | 596 | 690/1301/1357 | 0/0/0 | 1701/1727 | 0 |
| wide50 | 2x8 | 0 | 1040 | 1040 | 670 | 631/1034/1101 | 0/0/0 | 1464/1487 | 0 |
| wide50 | 2x8 | 1 | 1040 | 1040 | 583 | 764/1513/1572 | 0/0/0 | 1740/1759 | 0 |
| wide50 | 2x8 | 2 | 1040 | 1040 | 718 | 632/1058/1088 | 0/0/0 | 1378/1400 | 0 |
| wide50 | 4x8 | 0 | 1040 | 1040 | 713 | 592/1136/1190 | 0/0/1 | 1331/1418 | 0 |
| wide50 | 4x8 | 1 | 1040 | 1040 | 696 | 596/1237/1287 | 0/0/1 | 1420/1459 | 0 |
| wide50 | 4x8 | 2 | 1040 | 1040 | 680 | 610/1011/1064 | 0/0/1 | 1433/1459 | 0 |
| mixed20 | 4x4 | 0 | 2000 | 2000 | 867 | 264/374/379 | 0/0/0 | 2051/2109 | 0 |
| mixed20 | 4x4 | 1 | 2000 | 2000 | 504 | 386/874/1020 | 0/1/3 | 3720/3744 | 0 |
| mixed20 | 4x4 | 2 | 2000 | 2000 | 413 | 551/964/1125 | 0/1/3 | 4363/4535 | 0 |
| flaky_p0.3 | 4x4 | 0 | 1000 | 1419 | 201 | 598/1100/1156 | 0/0/1 | 1683/2888 | 0 |
| flaky_p0.3 | 4x4 | 1 | 1000 | 1444 | 257 | 468/874/919 | 0/0/1 | 1385/2469 | 0 |
| flaky_p0.3 | 4x4 | 2 | 1000 | 1408 | 220 | 1182/1474/1497 | 0/0/1 | 2188/3080 | 0 |

| crash recovery | lease s | recovery p50 s | max s |
|---|---|---|---|
| trial 0 | 5.0 | 6.13 | 6.13 |
| trial 1 | 5.0 | 6.36 | 6.36 |
| trial 2 | 5.0 | 6.33 | 6.33 |

Overload: offered 120 tasks/s for 10.0 s; served 56 tasks/s; peak READY depth 616; drained after 21.8 s.

## Before and after the round-trip optimisation (ADR 0011)

Same harness, same machine, same configuration. The baseline console output is in
[`benchmarks/results/20261021-baseline-console.txt`](../benchmarks/results/20261021-baseline-console.txt).
Each cell lists the 3 trials.

| workload | workers x slots | baseline tasks/s | optimised tasks/s |
|---|---|---|---|
| linear10 | 1x4 | 194 / 225 / 169 | 476 / 434 / 534 |
| linear10 | 2x4 | 102 / 209 / 260 | 735 / 701 / 738 |
| linear10 | 4x4 | 306 / 353 / 303 | 867 / 889 / 846 |
| wide50 | 1x8 | 158 / 72 / 173 | 552 / 545 / 596 |
| wide50 | 2x8 | 199 / 198 / 206 | 670 / 583 / 718 |
| wide50 | 4x8 | 177 / 152 / 100 | 713 / 696 / 680 |
| mixed20 | 4x4 | 167 / 171 / 230 | 867 / 504 / 413 |
| flaky_p0.3 | 4x4 | 122 / 129 / 99 | 201 / 257 / 220 |
| HTTP submission (runs/s) | 1 API process | 291 / 170 / 243 | 257 / 245 / 222 |

Profiling showed per-task database round trips were the bottleneck (ADR 0011). The
optimisation cut statements per no-op task from 18 to 8. The sequential claim cost went
from 4.8 ms to 1.9 ms. HTTP submission did not change, which is expected because that
path was not modified.

## Interpretation

- **Engine overhead dominates.** These are no-op tasks, so tasks/s measures claim,
  commit, and progression cost. On this single-host setup, the optimised engine runs
  about 850 to 890 linear tasks/s with 4 workers × 4 slots. Scaling from 1 to 4 workers
  improves throughput by less than 2x. The workers, PostgreSQL, and the benchmark driver
  all share 6 cores, so this run cannot separate CPU contention from database
  contention.
- **Wide fan-out is limited by per-run serialisation.** The run lock serialises
  completions within one run (ADR 0005). Throughput is about 700 tasks/s whether there
  are 2 or 4 workers. Queue-wait p95 above 1 s mostly reflects the 20 × 50-leaf burst,
  not a slow claim.
- **mixed20 degraded across consecutive trials (867, then 504, then 413 tasks/s).** The
  cause has **not** been diagnosed. Tables are truncated only between scenarios, not
  between trials, so one hypothesis is dead-tuple growth on the hot `tasks` and
  `task_events` rows before autovacuum catches up. That is unverified. A follow-up
  would truncate or `VACUUM` between trials and compare.
- **flaky_p0.3** made about 1.42 attempts per task (expected 1/(1 − 0.3) ≈ 1.43). Every
  run succeeded. Throughput is lower because of the retry backoff delays (50 ms
  initial, with jitter) and the scheduler hop that promotes each RETRY_WAIT task
  (250 ms interval).
- **Crash recovery:** SIGKILL → re-execution took 6.1 to 6.4 s with a 5 s lease and a
  1 s scheduler interval. That fits the model: lease + at most one scheduler interval +
  claim poll + process start of the rescuer. Recovery time scales with the lease. With
  the default 15 s lease, expect roughly 16 to 17 s (not measured).
- **Overload:** 120 tasks/s were offered to one worker with 4 slots, each task being a
  50 ms sleep. The worker served 56 tasks/s, below the theoretical 80. The READY queue
  grew to 616 and drained 11.8 s after submission stopped. Nothing was lost or
  duplicated. The gap below 80 tasks/s is per-task engine overhead (claim, commit, and
  the poll gap when a slot frees). It has not been broken down further.
- **Correctness under load:** 0 duplicate accepted results in every trial. Every run in
  every execution scenario succeeded.

## Not measured

- Database CPU and per-query latency.
- Memory per worker.
- A comparison of polling against LISTEN/NOTIFY.
- Claim batch-size sweeps.
- Behaviour on separate hosts or with a tuned PostgreSQL.

These are listed in the README roadmap and should be measured before making decisions
about them.

