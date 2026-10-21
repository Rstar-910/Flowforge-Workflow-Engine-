# ADR 0003: Lease-based claiming with attempt-id fencing and database time

- Status: accepted

## Decision

- **Claiming.** A claim locks READY rows with `FOR UPDATE SKIP LOCKED` and sets them to
  `RUNNING`. In the same transaction it increments `attempt_count`, writes a fresh
  `current_attempt_id` (UUID), and sets `lease_owner` and
  `lease_expires_at = statement_timestamp() + lease`. It also inserts the
  `task_attempts` row and commits. The handler runs after the commit, outside any
  transaction.
- **Fencing token.** The token is the pair (`current_attempt_id`, `attempt_count`).
  `attempt_count` is monotonic per task and is exposed to handlers as
  `ctx.fencing_token`. Lease renewal (`UPDATE ... WHERE current_attempt_id = ANY(:ids)
  AND lease_owner = :me AND status = 'RUNNING'`) is conditional on that token. So is
  result persistence. The completion's `UPDATE tasks ... WHERE status = 'RUNNING' AND
  current_attempt_id = :token` is the fence. PostgreSQL re-evaluates that condition
  after locking the row, so a stale writer matches nothing and writes nothing.
- **Clock.** Lease expiry is computed and compared with the PostgreSQL server clock only.
  Worker clocks never decide expiry. A worker keeps a conservative local deadline
  (`send_time + lease`) only to decide when to stop wasting effort.
- **Expiry.** The scheduler recovers expired leases with the same fenced statement,
  adding `lease_expires_at < statement_timestamp()` to the fence. It marks the attempt
  `LEASE_EXPIRED`. A worker's completion and the recovery for the same attempt
  serialise on the task row, and exactly one of them matches.

## Consequences

- Two workers can never both hold the lease for the same attempt. Results from a lost
  lease are rejected and recorded as a `stale_result_rejected` event. This is tested
  with SIGSTOP, absent heartbeats, and a network partition between the worker and
  PostgreSQL.
- A lease does **not** stop the old process. A paused worker can resume and keep
  executing its handler. The fencing token protects only the engine's own state, plus
  any downstream system that checks the token (see ADR 0004).
- If PostgreSQL is unreachable, the worker cannot renew. Once its local deadline passes,
  it signals the handler to stop. Its eventual commit is still fenced, so the
  database's view always wins.
