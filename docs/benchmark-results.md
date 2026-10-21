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

## Results (current engine, after ADRs 0015–0016)

Raw data: [`benchmarks/results/20261021T183820Z.json`](../benchmarks/results/20261021T183820Z.json).
Across all 25 measured workloads, **28,560 of 28,560 tasks succeeded, with 0 failed runs
and 0 duplicate accepted results.**

| HTTP submission | runs/s | p50 ms | p95 ms | p99 ms | errors |
|---|---|---|---|---|---|
| trial 0 (1000 req, 16 parallel) | 302 | 49.5 | 74.0 | 88.2 | 0 |
| trial 1 (1000 req, 16 parallel) | 272 | 54.0 | 81.8 | 97.7 | 0 |
| trial 2 (1000 req, 16 parallel) | 281 | 54.4 | 69.2 | 102.8 | 0 |

| workload | workers x slots | trial | tasks | attempts | tasks/s | queue wait p50/p95/p99 ms | exec p50/p95/p99 ms | run e2e p50/p95 ms | dup results |
|---|---|---|---|---|---|---|---|---|---|
| linear10 | 1x4 | 0 | 1000 | 1000 | 435 | 187/315/343 | 0/0/0 | 2114/2160 | 0 |
| linear10 | 1x4 | 1 | 1000 | 1000 | 481 | 178/215/220 | 0/0/0 | 1864/1905 | 0 |
| linear10 | 1x4 | 2 | 1000 | 1000 | 427 | 203/266/275 | 0/0/0 | 2147/2190 | 0 |
| linear10 | 2x4 | 0 | 1000 | 1000 | 606 | 128/161/194 | 0/0/1 | 1357/1438 | 0 |
| linear10 | 2x4 | 1 | 1000 | 1000 | 571 | 140/201/217 | 0/0/0 | 1584/1607 | 0 |
| linear10 | 2x4 | 2 | 1000 | 1000 | 635 | 129/149/153 | 0/0/0 | 1377/1396 | 0 |
| linear10 | 4x4 | 0 | 1000 | 1000 | 590 | 124/248/255 | 0/1/1 | 1483/1507 | 0 |
| linear10 | 4x4 | 1 | 1000 | 1000 | 688 | 114/148/152 | 0/0/1 | 1294/1321 | 0 |
| linear10 | 4x4 | 2 | 1000 | 1000 | 691 | 113/134/138 | 0/1/1 | 1271/1290 | 0 |
| wide50 | 1x8 | 0 | 1040 | 1040 | 495 | 919/1770/1844 | 0/0/1 | 2019/2078 | 0 |
| wide50 | 1x8 | 1 | 1040 | 1040 | 511 | 829/1683/1747 | 0/0/0 | 2002/2019 | 0 |
| wide50 | 1x8 | 2 | 1040 | 1040 | 516 | 886/1661/1725 | 0/0/1 | 1964/1990 | 0 |
| wide50 | 2x8 | 0 | 1040 | 1040 | 506 | 976/1714/1777 | 0/0/1 | 2007/2030 | 0 |
| wide50 | 2x8 | 1 | 1040 | 1040 | 466 | 921/1867/1936 | 0/0/0 | 2169/2219 | 0 |
| wide50 | 2x8 | 2 | 1040 | 1040 | 576 | 806/1475/1540 | 0/0/1 | 1754/1784 | 0 |
| wide50 | 4x8 | 0 | 1040 | 1040 | 478 | 854/1855/1923 | 0/0/2 | 2087/2119 | 0 |
| wide50 | 4x8 | 1 | 1040 | 1040 | 552 | 732/1190/1253 | 0/0/1 | 1780/1805 | 0 |
| wide50 | 4x8 | 2 | 1040 | 1040 | 421 | 1158/2090/2182 | 0/0/1 | 2360/2398 | 0 |
| mixed20 | 4x4 | 0 | 2000 | 2000 | 573 | 428/624/640 | 0/1/2 | 3191/3236 | 0 |
| mixed20 | 4x4 | 1 | 2000 | 2000 | 662 | 381/438/442 | 0/1/1 | 2663/2718 | 0 |
| mixed20 | 4x4 | 2 | 2000 | 2000 | 615 | 419/607/613 | 0/0/1 | 3020/3051 | 0 |
| flaky_p0.3 | 4x4 | 0 | 1000 | 1380 | 204 | 706/1584/1648 | 0/0/1 | 2132/2930 | 0 |
| flaky_p0.3 | 4x4 | 1 | 1000 | 1426 | 181 | 722/1548/1609 | 0/0/1 | 2215/2793 | 0 |
| flaky_p0.3 | 4x4 | 2 | 1000 | 1414 | 204 | 614/1197/1255 | 0/0/1 | 1916/2942 | 0 |

| crash recovery | lease s | recovery p50 s | max s |
|---|---|---|---|
| trial 0 | 5.0 | 6.11 | 6.11 |
| trial 1 | 5.0 | 6.36 | 6.36 |
| trial 2 | 5.0 | 6.09 | 6.09 |

Overload: offered 120 tasks/s for 10.0 s; served 57 tasks/s; peak READY depth 604; drained after 21.2 s.

**Why these are lower than the previous results.** Two things changed:

- **ADR 0016** plans the hot statements on every execution. A same-session A/B test put
  its cost at **5–7% of peak no-op throughput**, in exchange for throughput that holds
  steady under overload. See `docs/load-test-results.md`.
- **Session-to-session variance on this laptop.** In the same session, even the variant
  without custom plans measured 700–738 tasks/s for linear 4 × 4, versus 847–928 in the
  earlier session.

The earlier results are kept below as history and are not repeated as current claims.

For multi-process, sustained and overload behaviour, with crashes injected during the
load, see [load-test-results.md](load-test-results.md).

## Earlier results (after ADR 0012, before ADRs 0015–0016)

Raw data: [`benchmarks/results/20261021T144223Z.json`](../benchmarks/results/20261021T144223Z.json).
This run includes ADR 0011 and ADR 0012, but not ADRs 0015–0016. It was measured in an
earlier session.

| HTTP submission | runs/s | p50 ms | p95 ms | p99 ms | errors |
|---|---|---|---|---|---|
| trial 0 (1000 req, 16 parallel) | 288 | 50.1 | 82.3 | 127.2 | 0 |
| trial 1 (1000 req, 16 parallel) | 252 | 56.5 | 95.7 | 150.0 | 0 |
| trial 2 (1000 req, 16 parallel) | 270 | 53.9 | 89.1 | 98.3 | 0 |

| workload | workers x slots | trial | tasks | attempts | tasks/s | queue wait p50/p95/p99 ms | exec p50/p95/p99 ms | run e2e p50/p95 ms | dup results |
|---|---|---|---|---|---|---|---|---|---|
| linear10 | 1x4 | 0 | 1000 | 1000 | 535 | 164/208/214 | 0/0/0 | 1707/1725 | 0 |
| linear10 | 1x4 | 1 | 1000 | 1000 | 565 | 150/174/176 | 0/0/0 | 1558/1587 | 0 |
| linear10 | 1x4 | 2 | 1000 | 1000 | 511 | 152/283/310 | 0/0/0 | 1751/1786 | 0 |
| linear10 | 2x4 | 0 | 1000 | 1000 | 741 | 106/131/141 | 0/0/0 | 1132/1142 | 0 |
| linear10 | 2x4 | 1 | 1000 | 1000 | 687 | 109/196/218 | 0/0/0 | 1294/1307 | 0 |
| linear10 | 2x4 | 2 | 1000 | 1000 | 675 | 114/212/219 | 0/0/0 | 1324/1341 | 0 |
| linear10 | 4x4 | 0 | 1000 | 1000 | 928 | 83/95/98 | 0/0/1 | 907/928 | 0 |
| linear10 | 4x4 | 1 | 1000 | 1000 | 886 | 87/97/100 | 0/0/1 | 898/934 | 0 |
| linear10 | 4x4 | 2 | 1000 | 1000 | 847 | 88/109/117 | 0/0/1 | 948/990 | 0 |
| wide50 | 1x8 | 0 | 1040 | 1040 | 645 | 654/1282/1336 | 0/0/0 | 1561/1595 | 0 |
| wide50 | 1x8 | 1 | 1040 | 1040 | 569 | 782/1434/1492 | 0/0/0 | 1788/1810 | 0 |
| wide50 | 1x8 | 2 | 1040 | 1040 | 576 | 734/1438/1498 | 0/0/0 | 1771/1787 | 0 |
| wide50 | 2x8 | 0 | 1040 | 1040 | 690 | 604/1002/1054 | 0/0/1 | 1420/1449 | 0 |
| wide50 | 2x8 | 1 | 1040 | 1040 | 674 | 652/1298/1352 | 0/0/0 | 1473/1521 | 0 |
| wide50 | 2x8 | 2 | 1040 | 1040 | 626 | 702/1354/1407 | 0/0/0 | 1620/1640 | 0 |
| wide50 | 4x8 | 0 | 1040 | 1040 | 666 | 774/1192/1223 | 0/0/1 | 1479/1517 | 0 |
| wide50 | 4x8 | 1 | 1040 | 1040 | 728 | 612/1167/1213 | 0/0/1 | 1345/1380 | 0 |
| wide50 | 4x8 | 2 | 1040 | 1040 | 757 | 562/1100/1150 | 0/0/1 | 1306/1328 | 0 |
| mixed20 | 4x4 | 0 | 2000 | 2000 | 836 | 284/397/405 | 0/0/1 | 2166/2205 | 0 |
| mixed20 | 4x4 | 1 | 2000 | 2000 | 772 | 309/455/459 | 0/0/1 | 2354/2389 | 0 |
| mixed20 | 4x4 | 2 | 2000 | 2000 | 729 | 322/499/503 | 0/0/1 | 2545/2566 | 0 |
| flaky_p0.3 | 4x4 | 0 | 1000 | 1461 | 266 | 457/855/884 | 0/0/1 | 1373/2105 | 0 |
| flaky_p0.3 | 4x4 | 1 | 1000 | 1415 | 345 | 451/873/918 | 0/0/1 | 1440/2298 | 0 |
| flaky_p0.3 | 4x4 | 2 | 1000 | 1507 | 178 | 433/832/877 | 0/0/1 | 1504/2941 | 0 |

| crash recovery | lease s | recovery p50 s | max s |
|---|---|---|---|
| trial 0 | 5.0 | 6.34 | 6.34 |
| trial 1 | 5.0 | 6.27 | 6.27 |
| trial 2 | 5.0 | 6.28 | 6.28 |

Overload: offered 120 tasks/s for 10.0 s; served 56 tasks/s; peak READY depth 623; drained after 21.8 s.

Across all 25 measured workloads: **28,560 of 28,560 tasks succeeded, 0 failed runs,
0 duplicate accepted results.**

## History of measured optimisations

Same harness, same machine, same configuration. Each cell lists the 3 trials.

| workload | workers x slots | baseline | after ADR 0011 (round trips) | after ADR 0012 (plan stability) |
|---|---|---|---|---|
| linear10 | 4x4 | 306 / 353 / 303 | 867 / 889 / 846 | 928 / 886 / 847 |
| wide50 | 4x8 | 177 / 152 / 100 | 713 / 696 / 680 | 666 / 728 / 757 |
| mixed20 | 4x4 | 167 / 171 / 230 | **867 / 504 / 413** | **836 / 772 / 729** |
| flaky_p0.3 | 4x4 | 122 / 129 / 99 | 201 / 257 / 220 | 266 / 345 / 178 |
| HTTP submission (runs/s) | 1 API process | 291 / 170 / 243 | 257 / 245 / 222 | 288 / 252 / 270 |

After ADRs 0015–0016, in a later session: linear10 4x4 590 / 688 / 691; wide50 4x8
478 / 552 / 421; mixed20 573 / 662 / 615; flaky 204 / 181 / 204; HTTP 302 / 272 / 281
runs/s. Custom planning costs about 5–7% (A/B), and the rest is session variance.

Raw data and console logs:

- baseline: `benchmarks/results/20261021-baseline-console.txt`
- after ADR 0011: `benchmarks/results/20261021T125203Z.json`
- ADR 0012 diagnostics: `benchmarks/results/2026102114*.json`

**ADR 0011 (round trips).** Profiling showed that each no-op task cost about 33 network
round trips. Statements per task went from 18 to 8.

**ADR 0012 (plan stability).** `mixed20` declined 52% across trials after ADR 0011.
The investigation used per-trial table statistics, maintenance modes,
`pg_stat_statements` and `EXPLAIN`:

- With missing or stale planner statistics, the claim query used a bitmap scan. Its
  cost grows with dead index entries: buffer reads per claim went 218 → 658 over six
  trials.
- Autovacuum (1-minute naptime) rarely ran within a benchmark, so nothing cleaned up
  in time.
- Workers and the scheduler now disable bitmap scans for their sessions. Two hot queries
  were rewritten to be statistics-independent.
- Claim cost is now flat: about 105 buffers per call, versus a growing number before.

## Interpretation

- **Engine overhead dominates.** These are no-op tasks, so tasks/s measures claim,
  commit and progression cost. With the current engine, linear chains run at 590–691
  tasks/s with 4 workers × 4 slots. An earlier session measured 847–928 for the
  pre-ADR-0016 engine; see the history above for why these differ. Going from 1 to 4
  workers improves throughput by less than 2×, because workers, PostgreSQL and the
  benchmark driver all share 6 cores.
- **Per-run serialisation is now the main cost.** In `mixed20` and `wide50`, the
  largest server-side time is waiting for the run lock: about 2 ms per completion,
  flat across trials (measured with `pg_stat_statements`). This is the designed
  trade-off of ADR 0005. More workers do not help a single wide run.
- **Trial-to-trial variation of roughly ±10%** remains. `mixed20` still drifted
  down mildly in this run (836 → 729). Repeated 6-trial runs after the fix ranged
  700–821 with no consistent trend, so this is reported as variation, not as a
  solved or unsolved trend.
- **flaky_p0.3** made 1.42–1.51 attempts per task (expected about 1.43). Every run
  succeeded. Throughput depends on randomised backoff (50 ms initial, with jitter)
  and the 250 ms scheduler interval that promotes RETRY_WAIT tasks, so it varies most
  (178–345).
- **Crash recovery:** SIGKILL → re-execution took 6.3 s with a 5 s lease and a 1 s
  scheduler interval: lease + at most one scheduler interval + claim poll. With the
  default 15 s lease, expect about 16–17 s (not measured).
- **Overload:** 120 tasks/s were offered to one worker with 4 slots, each running a
  50 ms sleep. The worker served 56 tasks/s, below the theoretical 80. The READY queue
  peaked at 623 and drained 11.8 s after submission stopped. Nothing was lost or
  duplicated. The gap below 80 is per-task engine overhead, not broken down further.

## Not measured

- Database CPU.
- Memory per worker.
- Polling versus LISTEN/NOTIFY.
- Claim batch-size sweeps.
- Separate hosts.
- Tuned PostgreSQL, such as autovacuum settings for the queue tables.
- Behaviour after hours of continuous load.

These should be measured before making decisions about them.
