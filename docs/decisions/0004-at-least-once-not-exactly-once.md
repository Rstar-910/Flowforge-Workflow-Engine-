# ADR 0004: At-least-once execution; no exactly-once claims

- Status: accepted

## Context

A worker can perform an external side effect (charge a card, send an email) and then
crash before the success is committed. The engine cannot see the external system, so it
cannot tell "crashed before the effect" from "crashed after the effect".

## Decision

- FlowForge provides **durable, at-least-once task execution**. An accepted run is never
  silently lost. A task can be executed more than once after a crash, lease expiry,
  timeout, or retry.
- Results are **accepted at most once per task attempt** and **at most one success per
  task**. The fenced commit (ADR 0003) and the state machine both enforce this.
- The attempt is recorded as `LEASE_EXPIRED` with `outcome_known: false` in the event.
  The engine never claims to know whether the side effect happened.
- Every handler receives `ctx.operation_key = "<run_id>/<task_key>"`, which is stable
  across attempts, and `ctx.fencing_token`, the attempt number. Handlers should pass
  the operation key as the idempotency key to downstream systems.
- Handlers can register with `idempotent=False` to document that a retry may duplicate
  an effect.

## Consequences

- The failure test `test_crash_after_side_effect_before_commit` demonstrates both cases.
  Without downstream deduplication, the effect is applied twice. With the operation
  key, it is applied once.
- Exactly-once *effects* need cooperation from the downstream system. The engine
  cannot provide them on its own.
