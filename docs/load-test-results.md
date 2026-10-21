# Sustained-load and multi-process results

Measured with `scripts/load_test.py`. Every number below comes from a raw file in
`benchmarks/results/load-*.json`, named in each section, which also holds the
per-second time series.

**Scope.** Everything ran on **one host**: an Apple A18 Pro (6 cores) and PostgreSQL 17
in Docker Desktop. **Multi-host network partitions and PostgreSQL failover are not
tested**, so nothing here says how the system behaves under them.

## Setup

| | |
|---|---|
| Processes | **3 API processes**, **2 schedulers running concurrently**, **6 workers × 4 slots**, plus 1 replacement worker started mid-run. All share one PostgreSQL database. |
| Load | open-loop HTTP submissions at a fixed rate, round-robin across the 3 APIs. Each submission carries an idempotency key; on a transport error or 5xx, the client retries the **same key** on the next API (at most 4 tries, 10 s timeout). |
| Workflow | 8 tasks: `fetch → 3 parallel → join → 2 parallel → final`. Five of the tasks sleep 20 ms. |
| Engine config | lease 10 s; heartbeat 3 s; worker poll 50–250 ms; scheduler interval 0.5 s. |

Failures are injected **during** the load, on a timeline scaled to the run length (the
times below are for 210 s):

| t (s) | event |
|---|---|
| 90 | **SIGKILL a worker**. The harness waits until that worker is holding tasks. |
| 120 | start a replacement worker |
| 150 | **SIGKILL one of the two schedulers** |
| 170 | **SIGKILL one of the three API processes**. The client keeps round-robining to it, so failover keeps being exercised. |
| 210 | stop submitting, then drain |

**Sampling:**

- **Every second:** READY depth, **queue age** (the oldest READY task), RUNNING count,
  completions, and DB connections per component (`application_name`).
- **10 times per second:** sessions waiting on locks, labelled by the statement that is
  waiting.

**After the run:**

- p50/p95/p99 per phase for submission latency, queue wait (per attempt) and end-to-end
  run latency (`completed_at − created_at`);
- crash recovery: when each task the killed worker held started again elsewhere;
- every submitted run accounted for;
- the global invariants (`tests/support/invariants.py`) checked against the final
  database.

## Results: sustained load at 200 tasks/s (25 runs/s, 210 s)

Two runs: `load-20261021T182346Z.json` and the repeat `load-20261021T183144Z.json`.

| | |
|---|---|
| Work | **5,250 workflow runs (42,000 tasks)** per test run, all SUCCEEDED |
| Submissions | 5,250 accepted, all distinct; **500 failed over** after the API kill; **0 lost** |
| Correctness | **0 duplicate successes, 0 invariant violations, 0 deadlocks** |
| Steady state | queue wait p50/p95/p99 **2 / 18 / 29 ms**; end-to-end run p50/p95/p99 **92 / 125–126 / 136 ms**; queue age at most 0.02–0.03 s |
| Crash recovery under load | the killed worker's in-flight task restarted on another worker **11.3 s** after the kill, in both runs (10 s lease + scheduler interval + claim) |
| Lock waits | sessions waiting on a lock in 7–10% of samples, mean 0.1 waiting sessions; almost all on the **per-run progression lock** (ADR 0005) |
| DB connections (max) | workers 30 (≈4 each), API up to 25, schedulers 2, observer 2 |

Per-phase detail for the repeat run (ms, p50/p95/p99):

| phase | tasks/s | submit | queue wait | run e2e | queue age p95/max (s) |
|---|---|---|---|---|---|
| warm-up | 199 | 14/24/41 | 2/19/28 | 93/131/180 | 0.01/0.03 |
| steady | 200 | 14/21/28 | 2/18/29 | 92/125/136 | 0.02/0.02 |
| worker crash and replacement | 200 | 14/38/279 | 2/26/63 | 96/175/651 | 0.02/0.23 |
| scheduler and API crash | 200 | 15/21/36 | 2/18/29 | 93/127/187 | 0.02/0.02 |

**Variance, reported honestly.** In the first of the two runs, the worker-crash phase had
periodic queue-age spikes from t = 122 s to 170 s, up to 0.38 s, with end-to-end p95 of
1.7 s. The repeat run did not show them (one sample above 0.1 s), so I treat them as
environmental noise on a shared 6-core laptop. Both runs are kept in the results
directory.

## Results: near capacity and overload

`load-20261021T182522Z.json` (50 runs/s) and `load-20261021T182742Z.json` (90 runs/s),
90 s each.

| offered | tasks/s by phase | queue age max | run e2e p95 (steady) | lost / dup / invariants |
|---|---|---|---|---|
| 400 tasks/s | 395 / 400 / 401 / 401 | 0.08 s | 133 ms | 0 / 0 / none |
| 720 tasks/s (overload) | **487 / 465 / 465 / 489** | 5.2 s | 22 s | 0 / 0 / none |

At 720 tasks/s the offered load exceeds what this host can do. Throughput **holds at
about 465–490 tasks/s**, and the excess queues up: the READY backlog peaked at 2,467 and
queue age reached 5.2 s. Nothing was lost or duplicated. Under overload, crash recovery
took 16.4 s rather than 11.3 s, because the recovered task joins the back of a queue
several seconds deep.

## What this testing found and fixed (ADR 0016)

The **first** overload run (`load-20261021T180850Z.json`, before the fix) showed
throughput **falling** under overload instead of holding: 527, then 439, **212** and
309 tasks/s. Queue age reached 13 s, and 32 clients gave up.

`pg_stat_statements` and `auto_explain` on a temporary diagnostic PostgreSQL
(`load-20261021T1811*/1814*/1817*.json`) traced it to **cached generic query plans** built
while the tables were nearly empty:

- **Promotion**, which runs while holding the run lock, scanned every BLOCKED task in the
  table: 724 buffer reads per call where about 50 are needed.
- **Claims** scanned `task_dependencies` and `tasks` sequentially.
- The claim's run-activation step scanned every PENDING run, a cost that grew with the
  backlog.

Fixes:

1. Worker and scheduler sessions use `plan_cache_mode = force_custom_plan`.
2. Run activation does primary-key lookups.

**A/B test in the same session**, with the activation fix kept in both:

| | default plan cache | force_custom_plan (shipped) |
|---|---|---|
| overload (720 tasks/s offered): tasks/s by phase | 471 / 370 / **285** / 461 (`load-20261021T183537Z.json`) | 487 / 465 / 465 / 489 |
| overload: queue age max / READY max | **17 s / 8,923** | 5.2 s / 2,467 |
| burst benchmark, linear 4 × 4 | 700–738 tasks/s | 675–683 tasks/s |

Custom plans cost about **5–7% of peak no-op burst throughput**, and are what keeps
overload stable. That trade-off is accepted (ADR 0016).

**A note on the earlier "lost 32".** In the pre-fix overload run, 32 clients gave up
after 4 attempts while the APIs were saturated. The database held exactly as many runs
as accepted responses. So no run was created without the client being told, and any of
the 32 could have been resubmitted safely with the same idempotency key.

## Where the time goes under overload

At 720 tasks/s offered, about 7,600 of the roughly 11,000 statements slower than 10 ms
were **waits for the per-run lock**. Sibling tasks of a fan-out finish together and queue
for their run's lock, each holding a worker slot while it waits. This is the designed
serialisation of ADR 0005, the price of race-free progression. It is the main limit on
throughput for fan-out workflows on this host. Reducing it (for example batched or
deferred progression) would be a design change, deferred under the current feature
freeze.

## Not covered

- Multi-host deployments, network partitions between hosts, PostgreSQL failover or
  replicas.
- Runs longer than about 4 minutes. Hours-long soak tests, with autovacuum and retention
  running continuously, have not been done.
- Separate machines for load generator, application processes and database. Everything
  here competes for the same 6 cores, which both lowers the absolute numbers and adds
  variance.
