# ADR 0014: Workflow deadlines and task queue timeouts

- Status: accepted

## Context

Spec section 11 defines four timeout types:

- execution timeout;
- lease timeout;
- queue timeout (the maximum wait before a task starts);
- workflow deadline (the maximum total time for a run).

The first two already existed. Without the other two, a run could wait forever for a
worker that never comes, or overrun any business deadline.

## Decision

1. **Workflow deadline.** `deadline_seconds` is an optional definition field, at most
   30 days.
   - Submission stores `deadline_at = created_at + deadline_seconds`.
   - Each scheduler cycle finds overdue PENDING/RUNNING runs through a partial index,
     locks each run, and re-checks it.
   - It then calls the same `begin_stopping()` routine as a user cancellation. Queued
     tasks are cancelled atomically. Running handlers learn about it on their next
     heartbeat, because cancellation never writes RUNNING rows (ADR 0005).
   - The run also gets `deadline_exceeded_at` and finalises as **FAILED** with
     `failure_reason = "workflow deadline exceeded"`. This adds one state-machine edge,
     `CANCELLING → FAILED`, used only for deadline stops. A user's cancellation still
     ends CANCELLED.
   - Manual retry is refused for such runs, because the deadline has already passed.
2. **Queue timeout.** `queue_timeout_seconds` is an optional task field, at most 7
   days.
   - Each scheduler cycle finds READY tasks with
     `ready_at < now - queue_timeout_seconds` through a partial index.
   - For each one it takes the run lock, then runs a conditional
     `UPDATE ... WHERE status = 'READY' AND <still expired>` to FAILED with
     `QueueTimeout`. No attempt is created. Progression then propagates the failure.
   - The clock is `ready_at`, which every path sets when a task becomes READY again.
     Time in RETRY_WAIT therefore does not count.
3. **Compatibility.** Adding optional definition fields changes the canonical JSON of
   a definition. Re-posting a definition stored before this change is therefore
   compared after re-validating the stored JSON through the current schema, so an
   identical re-post is not a false 409 Conflict.

## Correctness notes

- **A claim racing a queue timeout.** The claim locks READY rows with SKIP LOCKED and
  never waits. The timeout's UPDATE waits for an in-flight claim, then re-evaluates
  `status = 'READY'` and does nothing. If the timeout locks the row first, the claim
  skips it. `test_queue_timeout_races_with_claims_safely` runs 6 claimers against the
  sweep over 40 expired tasks; every task ends exactly one way. Removing the `status`
  guard (a mutation check) makes it fail.
- **Deadline precision.** The deadline fires within about one scheduler interval
  (default 1 s). Running handlers stop after up to one heartbeat interval if they
  cooperate, and otherwise when their execution timeout expires.

## Consequences

- A run that failed its deadline is stopped through the cancellation machinery. It
  has the same at-least-once caveats: a handler that ignores cancellation finishes,
  and its true outcome is recorded.
- Deadlines are fixed at submission. Overriding them per run, or extending them, is
  not supported.
