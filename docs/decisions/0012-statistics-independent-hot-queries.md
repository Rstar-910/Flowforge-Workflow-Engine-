# ADR 0012: Make hot-path query plans independent of planner statistics

- Status: accepted

## Observation

In the first full benchmark, `mixed20` throughput fell across consecutive trials
(867, then 504, then 413 tasks/s). The cause was not known at the time.

## Investigation

The harness was extended so the behaviour could be measured rather than guessed:

- `--between-trials none|analyze|vacuum|truncate` controls what maintenance runs
  between trials;
- every trial records live and dead tuples, table and index sizes, and autovacuum
  counts;
- `--pg-stat-statements` records the top statements per trial, including server time,
  calls, and buffer reads.

The raw files are in `benchmarks/results/2026102114*.json`.

1. **Maintenance modes, 6 trials each.** With fresh statistics only (`analyze`, dead
   rows left in place, up to 17.5k of them), throughput was 907–957 tasks/s. With no
   maintenance it was noisy (600–840) and, in one run, declined 897 → 586 as dead rows
   grew. Dead rows alone were therefore not the cost. Missing statistics were.
2. **Query plans on a never-analyzed table** (10,000 tasks, `EXPLAIN ANALYZE`):

   | query | no statistics | after ANALYZE |
   |---|---|---|
   | claim pick (`ORDER BY ready_at LIMIT n FOR UPDATE SKIP LOCKED`) | bitmap scan + sort, 1.47 ms, 1,691 buffers | index scan, 0.018 ms, 10 buffers |
   | promote children (runs under the run lock) | 7–11 ms, scans all BLOCKED tasks | 0.09 ms |
   | dependency outputs (part of every claim) | 1–7 ms, sequential scan + hash join | 0.02 ms |

3. **Why the cost grew across trials:**
   - A bitmap scan visits every entry of the partial index `ix_tasks_ready`, including
     the dead entries left behind each time a task leaves READY.
   - Unlike a plain index scan, it never marks those entries dead (`LP_DEAD`), so its
     cost rises with churn until VACUUM runs.
   - The default `autovacuum_naptime` is 1 minute, longer than a whole 6-trial run, so
     autovacuum rarely ran during the benchmark.
   - `pg_stat_statements` confirmed it: buffer reads per claim grew 218 → 658 and mean
     claim time rose 0.82 → 2.2 ms over six trials.

The same situation arises in production after a migration, after a restore, on any new
or truncated database, or when the data distribution shifts faster than autoanalyze
reacts.

## Decision

1. **Worker and scheduler sessions set `enable_bitmapscan = off`**
   (`create_engine(..., narrow_lookups_only=True)`). Their queries only ever touch a
   few rows: a primary key, a small index range, or `ORDER BY ... LIMIT` on a partial
   index. For those, a plain index scan is always the right plan. The setting penalises
   bitmap plans rather than forbidding them, so a query with no alternative still
   runs. The API keeps default planner settings because its list and inspection
   queries can be broad.
2. **Two queries were rewritten to drive from dependency edges** using
   `id = ANY(ARRAY(subquery))`: promote children, and the claim's dependency outputs.
   That shape pins primary-key lookups whatever the statistics say. On a
   never-analyzed table, promote fell from 7.2 ms to 0.09 ms and dependency outputs
   from 0.95 ms to 0.03 ms. With statistics present they are no slower than before.

## Result

`mixed20` × 6 trials, with no maintenance and per-statement statistics:

| | claim buffers/call | claim mean | promote mean | tasks/s |
|---|---|---|---|---|
| before, trials 1–5 | 346 → 658 (growing) | 1.23 → 2.20 ms | 0.70–0.98 ms | 838, 837, 540, 725, 746 |
| after, trials 1–5 | 128 → 104 (flat) | 0.80 → 0.61 ms | 0.27–0.33 ms | 820, 700, 821, 789, 806 |

Claim cost no longer grows with dead tuples.

## Consequences

- The largest remaining server-side cost in `mixed20` is waiting for the run lock
  (about 2 ms per completion, flat across trials). That is the documented
  serialisation from ADR 0005, not a degradation. Reducing it would need batched
  completions or finer-grained progression locking. Neither is attempted without a
  measured need.
- Trial-to-trial variation of roughly ±10% remains on this single-host setup, where
  the workers, PostgreSQL, and the driver share 6 cores. The benchmark report gives
  ranges rather than single numbers for that reason.
- Queue tables still need regular autovacuum in long-running deployments. Dead tuples
  cost disk space and slow broad scans. A retention job for finished runs is a
  separate roadmap item.
