# ADR 0007: An expired lease consumes an attempt

- Status: accepted

## Decision

A claim increments `attempt_count`. When a lease expires, the attempt is marked
`LEASE_EXPIRED` and the task follows the normal retry policy:

- `RETRY_WAIT` with backoff, or `READY` if the computed delay is 0.
- `FAILED` once `attempt_count = attempt_limit`.
- `CANCELLED` if cancellation was requested.

## Rationale

A "poison" task that crashes or hangs every worker that runs it would otherwise loop
forever. Bounded attempts are a stated requirement. `test_lease_expiry_consumes_attempts_and_terminates`
covers this.

## Consequences

A worker crash unrelated to the task (for example, an OOM-kill caused by another task)
also uses up one of the task's attempts. Operators should size `max_attempts` with that
in mind.
