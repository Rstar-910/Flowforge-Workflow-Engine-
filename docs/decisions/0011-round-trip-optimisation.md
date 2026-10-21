# ADR 0011: Reduce database round trips per task (measured bottleneck)

- Status: accepted

## Measurement

The first full benchmark is recorded in
`benchmarks/results/20261021-baseline-console.txt`. It reached 100 to 350 tasks/s for
no-op tasks, with high variance between trials. Adding workers did not help wide
fan-outs. Profiling a single in-process claim and completion loop showed the following
(`docs/benchmark-results.md` has the method):

- One task cost about **18 SQL statements** and **about 33 network round trips**. That
  counts BEGIN/COMMIT for three transactions, a `pre_ping` per pool checkout, and
  prepare/execute pairs.
- Most wall time was spent waiting on the socket (`kqueue`). A raw round trip to
  PostgreSQL in Docker Desktop costs about 0.2 ms, and a commit with WAL flush about
  0.5 ms. The database server was not the limit.
- Sequential cost without contention: claim about 4.8 ms, completion 5.5 to 6.0 ms. The
  completion held the run lock for about 10 round trips, which serialises every
  completion within a wide run.

## Decision

1. **Claim = one autocommit statement.** It is a data-modifying CTE that does all of
   the following atomically:
   - picks the rows (SKIP LOCKED),
   - marks them RUNNING with new attempt ids,
   - inserts the attempt rows and `attempt_started` events,
   - activates PENDING runs (`FOR NO KEY UPDATE SKIP LOCKED`, so it never waits),
   - returns the run input and dependency outputs (`jsonb_object_agg`).
2. **Fenced completion = one statement before the run lock.** The fence (status and
   attempt id) is the `UPDATE`'s own WHERE clause, which PostgreSQL re-checks after
   locking the row. No preceding `SELECT ... FOR UPDATE` is needed. The attempt update
   and the event insert are CTEs in the same statement. Lease recovery uses the same
   statement, adding `lease_expires_at < statement_timestamp()` to the fence.
3. **After a success, progression = one statement.** It promotes only the *direct
   children* of the task that succeeded, plus an "any active tasks left?" probe. Full
   finalisation queries run only when nothing is active. Failure, cancellation, and
   reconcile paths keep the general progression.
4. Workers and the scheduler use **no pool pre-ping**. Their loops already retry on
   connection errors (`test_short_database_outage_is_survived` and
   `test_long_database_outage_leads_to_safe_takeover` still pass). The API keeps
   pre-ping.

## Result

- Statements per task went from 18 to 8.
- Sequential claim went from 4.8 ms to 1.9 ms.
- Completion in a wide run went from 5.5 ms to 2.7 ms.
- End-to-end before/after numbers under the same benchmark harness are in
  `docs/benchmark-results.md`.

## Consequences

The SQL is denser. In exchange, each critical transition is a single atomic statement,
which makes the fencing argument easier to audit. All concurrency, failure-injection,
and mutation checks were re-run after the change.
