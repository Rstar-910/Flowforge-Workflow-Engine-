# ADR 0008: Handler isolation modes, timeouts and cancellation

- Status: accepted

## Context

An asyncio timeout cannot terminate arbitrary synchronous Python code, and Python cannot
kill a thread.

## Decision

Handlers register under a name, with one of three isolation modes:

| mode | runs on | timeout | cancellation |
|---|---|---|---|
| `async` (default for `async def`) | worker event loop | `asyncio` cancellation at the next `await` | `ctx.sleep()` / `ctx.check_cancelled()` plus task cancellation |
| `thread` (default for `def`) | bounded thread pool | worker stops waiting, records `TIMED_OUT`; the thread keeps running and counts as a **zombie** against worker capacity until it returns | cooperative only (`ctx.check_cancelled()`); the worker waits and records what the thread actually did |
| `process` | fresh `spawn` subprocess | SIGTERM, then SIGKILL; hard stop | subprocess terminated |

When cancellation is requested, the worker learns about it on its next heartbeat.

- If the handler stops cooperatively, the task becomes CANCELLED.
- If the handler finishes anyway, the true outcome (for example SUCCEEDED) is recorded,
  and the run still ends CANCELLED.
- If it neither finishes nor stops, the timeout applies, and lease expiry applies if the
  worker itself is stuck.

## Consequences

- Recorded outcomes never claim a stop that did not happen.
- A blocking call inside an `async` handler stalls the event loop and its heartbeats.
  The lease then expires and the task is recovered elsewhere, which is safe but
  wasteful. Blocking code belongs in `thread` or `process` handlers.
- Workers execute registered, trusted handlers only. Definitions reference handlers by
  name and never carry code.
