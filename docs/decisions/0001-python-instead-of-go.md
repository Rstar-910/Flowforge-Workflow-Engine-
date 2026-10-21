# ADR 0001: Implement in Python (FastAPI, SQLAlchemy async) instead of Go

- Status: accepted
- Date: 2026-10-21

## Context

The project specification (section 5) recommends Go for the main portfolio version. It
explicitly allows "Python with FastAPI and PostgreSQL ... if Python is the language in
which you can implement and test the most thoroughly". The engineering brief for this
implementation requires Python 3.12+, FastAPI, Pydantic, SQLAlchemy 2.x async, and Alembic.

## Decision

Implement FlowForge in Python 3.12. Use asyncio with SQLAlchemy's async engine over asyncpg
for all database I/O in the API, scheduler, and workers.

## Consequences

- Correctness does not depend on the language: all coordination goes through PostgreSQL
  transactions and row locks, never through in-process locks.
- Python cannot forcibly stop a thread. Handler isolation modes and their timeout
  semantics are designed around that limit (ADR 0008).
- Single-process CPU throughput is lower than Go. Worker processes scale horizontally,
  and CPU-bound handlers can use `process` isolation.
- The spec's Go-specific repository layout (`cmd/`, `internal/`) maps to `src/flowforge/`
  with one CLI entry point (`flowforge api|worker|scheduler|migrate|inspect`).
