# ADR 0016: Plan hot-path statements per execution (no cached generic plans)

- Status: accepted

## Context

The multi-process overload test (`scripts/load_test.py`, 90 runs/s, 720 tasks/s
offered) showed throughput **falling** under overload instead of levelling off at
capacity: 527, then 439, 212 and 309 tasks/s. `pg_stat_statements` and `auto_explain`
on a diagnostic PostgreSQL located the cause:

- **Promote children** averaged 3.8 ms and **724 buffer reads per call**. It runs while
  holding the run lock. Its slowest logged plan scanned every BLOCKED task in the table
  through `(run_id, status)`, filtering out 4,651 rows, instead of three primary-key
  lookups.
- **Claim**: the dependency-outputs subquery did sequential scans of
  `task_dependencies` (15,550 rows) and `tasks` (12,440 rows) to read 1–3 parent
  outputs.
- The estimates in those plans, such as `Seq Scan on tasks (cost=0.00..11.12 rows=9)`,
  showed the cause. These were **cached generic plans**:
  - asyncpg keeps prepared statements per connection;
  - after five executions PostgreSQL may switch to a generic plan;
  - the plan built while the tables were nearly empty was reused as they grew by
    hundreds of rows per second, until autoanalyze invalidated it, up to a minute
    later.

ADR 0012 had disabled bitmap scans for these sessions. That does not stop a stale
generic plan from choosing a sequential scan or the wrong index.

Separately, the claim's run-activation step (`PENDING → RUNNING`) scanned **every
PENDING run**, a cost that grows with the backlog, instead of looking up the claimed
runs.

## Decision

1. Worker and scheduler sessions (`create_engine(..., narrow_lookups_only=True)`) set
   **`plan_cache_mode = force_custom_plan`**. Every execution is planned with its actual
   parameters against the tables' current size. Statements stay prepared, so parsing is
   still saved. The API keeps PostgreSQL's defaults.
2. The run-activation subquery is driven by
   `id = ANY(ARRAY(SELECT run_id FROM claimed))`, i.e. primary-key lookups for the
   claimed runs only.

## Result

Measured on the diagnostic server, with `auto_explain` adding overhead to both sides:

| statement under overload | before | after |
|---|---|---|
| promote children: mean / buffer reads per call | 3.8 ms / 724 | 0.4 ms / 53 |
| promote children: total server time (60 s run) | 65 s | 19 s |
| claim: mean / buffer reads per call | 1.75 ms / 258 | 1.2 ms / 145 |

**A/B test in the same session** on the standard database, with the activation fix kept in
both:

| | default plan cache | force_custom_plan |
|---|---|---|
| overload (720 tasks/s offered): tasks/s by phase | 471 / 370 / **285** / 461 | 487 / 465 / 465 / 489 |
| overload: queue age max / READY backlog max | 17 s / 8,923 | 5.2 s / 2,467 |
| burst benchmark, linear 4 × 4 (no-op tasks) | 700–738 tasks/s | 675–683 tasks/s |

The activation fix alone does not prevent the collapse; custom plans do. Details are in
`docs/load-test-results.md`.

## Consequences

- Each hot statement is planned on every execution. In the A/B test that cost about
  **5–7% of peak burst throughput** for no-op tasks. This is accepted in exchange for
  throughput holding at capacity under overload.
- Under overload, the dominant remaining cost is waiting for the per-run lock
  (ADR 0005): 7,622 of the roughly 11,000 statements slower than 10 ms were run-lock
  waits. Sibling completions of a fan-out queue for their run while holding a worker
  slot. Reducing that, for example with batched or deferred progression, would be a
  design change and is out of scope during the feature freeze.
