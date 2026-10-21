# ADR 0005: The run row lock serialises run progression; global lock order

- Status: accepted. Revised after measurement (ADR 0011) and after a deadlock found by
  the test suite.

## Context

When two sibling tasks finish concurrently, each READ COMMITTED completion transaction
can see the other sibling as still RUNNING. Then neither promotes their shared child.
This is a write-skew anomaly, and it would leave the run stuck forever. The same
applies to run finalisation.

## Decision

1. Every transaction that applies *progression* first takes the **run row lock**.
   Progression means promoting BLOCKED tasks, propagating failures, and finalising the
   run. The transactions that take this lock are completion, lease recovery,
   cancellation, manual retry, and reconciliation. Progression statements are issued
   only *after* the lock is granted. Each READ COMMITTED statement then sees every
   sibling transaction that committed before it.
2. The run row is locked with **`FOR NO KEY UPDATE`**, not `FOR UPDATE`.
   - Completion inserts `task_events` rows. Their foreign-key checks take
     `FOR KEY SHARE` on the run row.
   - With `FOR UPDATE`, two sibling completions that each hold `KEY SHARE` would both
     request the exclusive lock and deadlock.
   - The test suite found exactly that: `test_fan_in_promotion_has_no_write_skew` hit
     `deadlock detected`. `NO KEY UPDATE` is still mutually exclusive between
     progressors, but it is compatible with foreign-key locks.
3. **Global lock order:**
   1. At most the transaction's **own RUNNING task row**. The fenced
      `UPDATE ... WHERE status = 'RUNNING' AND current_attempt_id = :token` takes it.
   2. Then the **run row**.
   3. Then **non-running task rows** of that run (BLOCKED, READY, RETRY_WAIT, FAILED,
      UPSTREAM_FAILED).

   Transactions that start from the run row never write RUNNING rows. Those are
   cancellation, manual retry, and reconciliation. That is why cancellation does not
   flag running tasks: workers read `run.status = 'CANCELLING'` during lease renewal.
   Claims and retry promotion use only `SKIP LOCKED`, including the claim's activation
   of a PENDING run, so they never wait. These rules leave no lock cycle possible.

## Consequences

- Promotion and finalisation decisions for one run are serialised.
  `test_fan_in_promotion_has_no_write_skew` (20 concurrent parent completions) fails
  when the run lock is removed. That mutation check was repeated after the revision.
- Completions *within one run* are serialised for the duration of the run lock: the
  lock request, one progression statement, and the commit. Different runs do not
  contend. The impact is measured in `docs/benchmark-results.md` (the `wide50`
  workload).
- A pending retry decided just before a concurrent cancellation is corrected under the
  run lock: RETRY_WAIT or READY becomes CANCELLED.
- The scheduler's reconciler re-applies progression to any run whose state is
  inconsistent. In normal operation it is a no-op.
