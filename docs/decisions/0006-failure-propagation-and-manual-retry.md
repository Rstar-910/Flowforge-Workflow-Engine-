# ADR 0006: Failure propagation, UPSTREAM_FAILED, and manual retry

- Status: accepted

## Context

The specification lists the task states BLOCKED, READY, RUNNING, RETRY_WAIT, SUCCEEDED,
FAILED, and CANCELLED. It asks the implementer to define how skipped tasks affect
completion. Reusing CANCELLED for "a dependency failed" would conflate a user's
cancellation with failure propagation.

## Decision

- An additional terminal state, `UPSTREAM_FAILED`, marks BLOCKED tasks that can never
  run because a dependency (transitively) ended FAILED, UPSTREAM_FAILED, or CANCELLED.
- Independent branches keep running. A run becomes `FAILED` only when no task is active
  (BLOCKED, READY, RUNNING, or RETRY_WAIT) and at least one task did not succeed. The
  run's `failure_reason` lists the FAILED tasks.
- **Manual retry** is admin-only and applies to a `FAILED` task in a `RUNNING` or
  `FAILED` run. It does the following:
  - Sets the task to READY with a fresh attempt budget
    (`attempt_limit = attempt_count + max_attempts`).
  - Sets UPSTREAM_FAILED tasks back to BLOCKED. Progression re-runs, so any task still
    blocked by *another* failed task is marked UPSTREAM_FAILED again.
  - Moves a FAILED run back to RUNNING.
  - Keeps the attempt history. Attempt numbers keep increasing.
- Compensation (sagas) is not implemented in this version.

## Consequences

- A manual retry may repeat external side effects. The API documentation says so.
