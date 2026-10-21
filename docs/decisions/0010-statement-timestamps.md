# ADR 0010: Use statement_timestamp() for lifecycle timestamps

- Status: accepted

## Context

PostgreSQL's `now()` returns the *transaction start* time. The first test run showed a
problem: a claim transaction could begin before a dependency's completion transaction,
yet only observe that dependency's commit later. The dependent task then recorded a
`started_at` *earlier* than its dependency's `completed_at`. The execution order was
correct, but the timestamps were misleading.

## Decision

Engine SQL uses `statement_timestamp()` for all lifecycle timestamps and lease
arithmetic. Column defaults keep `now()`, which is used only for insert-time values.

## Consequences

Timestamps reflect the order in which statements observed state, so they are
consistent with dependency order. Integration tests assert
`started_at >= dependency.completed_at`.
