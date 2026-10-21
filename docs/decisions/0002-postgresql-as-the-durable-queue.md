# ADR 0002: PostgreSQL is the queue and the only source of truth

- Status: accepted

## Context

A workflow engine needs a durable record of runs, tasks and attempts, plus a queue of
work that is ready to run. A separate broker (Redis, Kafka, RabbitMQ) would require
keeping two systems consistent. For example, the engine might enqueue a message but fail
to commit the task row, or the reverse.

## Decision

- Task eligibility *is* the `tasks.status = 'READY'` row state. Workers claim with
  `SELECT ... FOR UPDATE SKIP LOCKED` inside a short transaction (ADR 0003).
- No Redis, Kafka, or other broker. No component keeps pending work only in memory.
- Workers poll with adaptive backoff (50 ms to 250 ms in benchmarks, 0.5 s to 2 s by
  default). LISTEN/NOTIFY is a possible later optimisation. It is not adopted until
  measurements show polling is the bottleneck: notifications are lossy across
  reconnects, so polling would still be required as the fallback.

## Consequences

- One transactional system means no dual-write inconsistency.
- Throughput is bounded by PostgreSQL write capacity. Every task needs about four
  writes: a claim, an attempt insert, a completion, and events. The numbers are in
  `docs/benchmark-results.md`.
- `ix_tasks_ready` is a partial index on `ready_at WHERE status = 'READY'`. It keeps the
  claim query proportional to the ready set, not the whole table.
