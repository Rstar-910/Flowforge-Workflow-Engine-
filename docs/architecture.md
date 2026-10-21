# Architecture

## Components

```
            ┌──────────────┐  HTTP/JSON (X-API-Key)
 Client ───▶│  API (N×)    │──────────────┐
            │ FastAPI      │              │ short transactions
            └──────────────┘              ▼
                                   ┌──────────────┐
            ┌──────────────┐       │  PostgreSQL  │◀── single source of truth:
            │ Scheduler    │──────▶│              │    definitions, runs, tasks,
            │ (1×, safe N×)│       │              │    dependencies, attempts,
            └──────────────┘       │              │    events, workers,
            ┌──────────────┐       │              │    idempotency records
            │ Worker (N×)  │──────▶│              │
            │ claim loop   │       └──────────────┘
            │ heartbeat    │
            │ handlers ────┼──▶ async / thread pool / subprocess
            └──────────────┘
 Each process: structured JSON logs, Prometheus metrics, OpenTelemetry spans.
```

| component | entry point | responsibilities |
|---|---|---|
| API | `flowforge api` → `flowforge.api.app:create_app` | Authentication, validation, definition storage, idempotent submission, inspection, cancellation, manual retry, health, metrics. It holds no workflow state. |
| Scheduler | `flowforge scheduler` → `flowforge.scheduler.service.Scheduler` | Promotes due RETRY_WAIT tasks, recovers expired leases, reconciles runs (safety net), samples queue gauges. |
| Worker | `flowforge worker` → `flowforge.worker.runtime.Worker` | Claims READY tasks for its registered handlers, renews leases, executes handlers under a timeout, and persists fenced results. Shuts down gracefully. |
| PostgreSQL | `deployments/docker-compose.yml` | Durable state and coordination: row locks, SKIP LOCKED, constraints, and triggers. |

## Package layout

```
src/flowforge/
  config.py            environment-driven settings (validated)
  domain/              pure logic, no I/O: states, definition schema + DAG validation,
                       retry policy, error classification
  db/                  SQLAlchemy table models, async engine factory
  engine/              all transactional operations (Core SQL):
    submission.py      definitions, idempotent run submission
    execution.py       claim, renew, fenced completion  (worker-facing)
    recovery.py        retry promotion, lease recovery, reconciliation (scheduler-facing)
    progression.py     promotion / failure propagation / run finalisation (under run lock)
    control.py         cancellation, manual retry
    queries.py         tenant-scoped read models for the API
  worker/              registry, TaskContext, executor (isolation), runtime (loops)
  scheduler/           scheduler loop
  api/                 FastAPI app, routes, auth, schemas, errors, middleware
  observability/       JSON logging with correlation context, Prometheus metrics, tracing
  handlers/builtin.py  example/demo/benchmark handlers
  faults.py            opt-in failure-injection points
  cli.py               `flowforge` command
migrations/            Alembic (initial schema + immutability triggers)
```

The HTTP layer contains no SQL and no state-transition logic. The engine has no HTTP
dependencies. The domain package has no I/O.

## Request and task lifecycle

1. **Define.** `POST /v1/workflow-definitions` validates the schema in Pydantic: types,
   ranges, sizes, slugs, and no unknown fields. Cross-field checks follow: duplicate
   IDs, unknown handlers, unknown or self dependencies, and cycles (iterative DFS). The
   definition is then stored, immutable, keyed by `(tenant, name, version)`. Re-posting
   identical content is idempotent. Different content for the same version gets 409.
2. **Submit.** `POST /v1/workflow-definitions/{id}/runs` runs one transaction:
   1. Reserve the idempotency key (`INSERT ... ON CONFLICT DO NOTHING`).
   2. Insert the run (PENDING), all tasks (root tasks READY, others BLOCKED), the
      dependency edges, and a `run_created` event.
   3. Commit. Concurrent duplicates block on the unique index, then return the winner's
      run, or 409 if the request body differs.
3. **Claim** (worker): **one autocommit statement**, a data-modifying CTE. It locks up
   to *k* READY rows for the worker's handlers (`ORDER BY ready_at ... FOR UPDATE SKIP
   LOCKED`), sets them RUNNING with a new attempt id and lease, and inserts the attempt
   rows and `attempt_started` events. It also moves PENDING runs to RUNNING
   (`FOR NO KEY UPDATE SKIP LOCKED`, so it never waits) and returns run input and
   dependency outputs (ADR 0011).
4. **Execute** (no transaction): the handler runs with `TaskContext` (input, run input,
   dependency outputs, `operation_key`, `fencing_token`, cancel flag) under its timeout.
   A heartbeat loop renews all of this worker's leases every `heartbeat_interval`.
5. **Complete** (worker), one short transaction:
   1. **Fenced statement:**
      `UPDATE tasks ... WHERE status = 'RUNNING' AND current_attempt_id = :token`.
      PostgreSQL re-checks this condition after it locks the row. The same statement
      writes the attempt outcome and an event. The new task state is SUCCEEDED,
      RETRY_WAIT or READY with backoff, FAILED, or CANCELLED.
   2. **Run lock:** `FOR NO KEY UPDATE`. A pending retry in a run that is being
      cancelled becomes CANCELLED here.
   3. **Progression:** after a success, one statement promotes the task's ready
      children. Otherwise the general path propagates failures, promotes ready tasks,
      and finalises the run.

   If the fence matches no row, nothing is written, and a separate transaction records
   `stale_result_rejected`.
6. **Scheduler cycle** (default every 1 s):
   - `RETRY_WAIT`→`READY` for due tasks (set-based, SKIP LOCKED).
   - Expired leases: the same fenced statement, with `lease_expires_at < now` added
     to the fence. It marks the attempt `LEASE_EXPIRED`, applies the retry policy,
     then locks the run and runs progression.
   - Workflow deadlines and queue timeouts (`engine/timeouts.py`, ADR 0014): an
     overdue run is stopped like a cancellation and ends FAILED; a task READY for
     longer than its queue timeout fails with `QueueTimeout`.
   - Reconcile stuck runs, every 5 cycles.
   - Retention, every `retention_interval_seconds` (default 60 s). It prunes events of
     old finished runs, deletes old finished runs, and removes stale worker rows, in
     batches of `retention_batch_size` runs per short transaction (ADR 0013).
   - Gauges: tasks by status and worker heartbeat age.

## Transaction boundaries and locking

| operation | locks (in order) | duration |
|---|---|---|
| submit | idempotency key (unique index); inserts | one short transaction |
| claim + run activation | READY task rows and PENDING run rows, both SKIP LOCKED (never waits) | one autocommit statement |
| renew | own RUNNING task rows (conditional UPDATE) | one statement plus worker heartbeat |
| complete / lease recovery | **own RUNNING task row → run row (NO KEY UPDATE) → non-running task rows** | 3 statements plus commit |
| cancel / manual retry / reconcile | **run row → non-running task rows** (never RUNNING rows) | short |
| retry promotion | due RETRY_WAIT rows, SKIP LOCKED | one statement |

The global lock order is: own RUNNING task row, then run row, then other task rows.
ADR 0005 explains why this order cannot deadlock.

Worker and scheduler sessions run with `enable_bitmapscan = off`, and the hot queries
are written so their efficient plan doesn't depend on planner statistics (ADR 0012).
Their cost therefore stays flat on new, truncated, or not-yet-analyzed tables.

There are no Python-level locks between processes. No transaction is open while a
handler runs. `idle_in_transaction_session_timeout` (60 s) and `statement_timeout`
(15 s) are set on every connection as a backstop.

## Data model

`migrations/versions/0001_initial_schema.py` creates:

- `workflow_definitions`: immutable (trigger); unique `(tenant_id, name, version)`.
- `workflow_runs`: status CHECK; `completed_at` set exactly when terminal; FK to the
  definition with `ON DELETE RESTRICT`, so definitions with runs cannot be deleted.
- `tasks`: unique `(run_id, task_key)`; CHECKs for status, lease ⇔ RUNNING,
  RETRY_WAIT ⇒ time, terminal ⇔ `completed_at`, and `attempt_count <= attempt_limit`.
  Partial indexes cover READY (`ready_at`), RETRY_WAIT (`next_attempt_at`), and
  RUNNING (`lease_expires_at`).
- `task_dependencies`: composite PK; no self-edges; indexed by `depends_on_task_id`.
- `task_attempts`: unique `(task_id, attempt_number)`; outcome CHECK;
  `ended_at` ⇔ finished.
- `workflow_runs.events_pruned_at` (migration 0002): set when retention removed the
  run's events. Partial indexes on `completed_at` for finished runs keep retention
  scans proportional to the work.
- `task_events`: bigint identity; append-only (an UPDATE trigger raises; only
  retention deletes, never edits); indexed by
  `(run_id, id)` for cursor pagination.
- `workers`: registry with `last_heartbeat_at` and `stopped_at`.
- `idempotency_records`: PK `(tenant_id, idempotency_key)`, `request_hash` (SHA-256 of
  canonical JSON), and a **deferred** FK to the run.

All timestamps are `timestamptz`, set by PostgreSQL (`statement_timestamp()`, ADR 0010).
JSON values (inputs and outputs) are JSONB with size limits:

- Static task input: 64 KiB.
- Run input and task output: 256 KiB (`FLOWFORGE_MAX_JSON_VALUE_BYTES`).
- HTTP body: 1 MiB (`FLOWFORGE_MAX_REQUEST_BYTES`).

## Security

- API keys map to `(tenant, role)` and are compared in constant time. Every query is
  tenant-scoped. Cross-tenant access returns 404, which is tested.
- Manual retry requires the `admin` role.
- Handlers are resolved by name from a registry populated by operator-configured modules
  (`FLOWFORGE_HANDLER_MODULES`). Definitions cannot carry code, shell commands, or URLs.
  The demo `record_effect` handler's file path comes from operator configuration, never
  from task input.
- Validation errors never echo submitted values. 5xx responses carry only a request ID,
  with no stack traces or credentials. Settings hold secrets as `SecretStr`.
- All SQL is parameterised: SQLAlchemy Core and `text()` with bind parameters.
- Containers run as a non-root user (`deployments/Dockerfile`).
- The optional rate limiter on run submission is per tenant and per process.

## Observability

- **Logs:** JSON lines carrying `timestamp`, `level`, `service`, `request_id`,
  `workflow_run_id`, `task_id`, `task_key`, `attempt_id`, `worker_id`, `event_type`,
  `duration_ms`, and `error_type` where relevant. Correlation fields come from
  `contextvars`, so concurrent tasks in one worker log with the right IDs.
- **Metrics** (`/metrics` on the API, `FLOWFORGE_METRICS_PORT` for the other processes):
  - Runs: `flowforge_workflow_runs_submitted_total`,
    `flowforge_workflow_runs_completed_total{outcome}`.
  - Tasks: `flowforge_tasks_claimed_total`, `flowforge_tasks_completed_total{outcome}`,
    `flowforge_task_retries_total`.
  - Latency: `flowforge_task_execution_duration_seconds{handler}`,
    `flowforge_task_queue_wait_seconds`.
  - Queue: `flowforge_ready_queue_depth`, `flowforge_tasks_by_status{status}`,
    `flowforge_running_task_count`.
  - Workers and leases: `flowforge_active_workers`,
    `flowforge_worker_heartbeat_age_seconds`, `flowforge_expired_leases_total`,
    `flowforge_lease_renewal_failures_total{reason}`,
    `flowforge_stale_results_rejected_total`.
  - Database: `flowforge_database_operation_duration_seconds{operation}`,
    `flowforge_db_query_duration_seconds`, `flowforge_database_errors_total{component}`.
  - Scheduler and HTTP: `flowforge_scheduler_cycle_duration_seconds`,
    `flowforge_http_requests_total{method,route,status}`,
    `flowforge_http_request_duration_seconds`.
  - Labels are bounded sets only. IDs never appear as labels; a test enforces this.
- **Tracing:** spans use the OpenTelemetry API. The submitting request's `traceparent`
  is stored on the run, and worker spans continue that trace.
- **Inspection:** the API exposes runs, tasks (with dependencies), attempts, and
  paginated events, plus `GET /v1/stats` (tenant-scoped counts) and `GET /v1/workers`
  (admin only; health is `live`, `silent` after one lease without a heartbeat, or
  `stopped`). The dashboard at `/ui` is built only on these endpoints: static files in
  `src/flowforge/api/static/`, served with a strict CSP by `flowforge/api/ui.py`. `flowforge inspect <run_id>` prints a run's attempt table straight
  from the database.
- **Health:** `/health/live` checks the process. `/health/ready` checks database
  connectivity and reports the schema revision.
