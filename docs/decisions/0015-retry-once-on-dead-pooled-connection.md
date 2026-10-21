# ADR 0015: Retry once, immediately, when a pooled connection was already dead

- Status: accepted

## Context

Since ADR 0011, workers and the scheduler run without SQLAlchemy's pool pre-ping, which
saves one round trip per operation. A connection the server closes while it sits idle
in the pool therefore fails on its next use. Causes include a PostgreSQL restart, an
idle-session timeout, a TCP drop, or `pg_terminate_backend`.

Each such failure counted as a database error, so the loops backed off exponentially
(up to 10 s). In the correctness audit, a workflow whose worker sessions were killed
every ~20 ms made **no progress in 45 s**: every claim ran on a connection that had
been killed while idle.

## Decision

`flowforge.db.session.retry_if_connection_was_dead(op, component)` runs an operation
and, if it failed with a `DBAPIError` whose `connection_invalidated` flag is set, runs
it once more immediately on a fresh connection. Any other error, or a second failure,
goes to the caller's normal backoff. It wraps:

- worker registration, claims, lease renewal and result commits;
- each scheduler cycle.

All of these are safe to repeat:

- a claim is one atomic statement;
- renewal is idempotent;
- a commit is fenced and recognizes its own already-recorded result;
- every scheduler step re-checks state under lock.

Each retry increments `flowforge_db_reconnects_total{component}`.

## Consequences

- Under the same kill storm the workflow completed in about 3 s, with 11–16 interrupted
  operations recovered. `test_server_side_session_kills_during_a_workflow` requires
  evidence that operations were interrupted. Disabling the retry makes it time out
  again.
- The API keeps pre-ping, so its first request after a database restart doesn't fail.
- A lost acknowledgement on a claim (committed, but the reply never arrived) is not
  made worse by this. The tasks belong to this worker but it doesn't know about them,
  so they are recovered after one lease period, as before.
