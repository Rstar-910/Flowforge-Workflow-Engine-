"""Prometheus metrics.

Labels are restricted to bounded sets (outcome, status, handler name, route
template, operation name). Run, task, attempt and worker IDs belong in logs
and traces, never in labels.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from prometheus_client import Counter, Gauge, Histogram

LATENCY_BUCKETS = (
    0.001,
    0.0025,
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1,
    2.5,
    5,
    10,
    30,
    60,
    120,
    300,
)

RUNS_SUBMITTED = Counter(
    "flowforge_workflow_runs_submitted_total",
    "Workflow runs accepted (new runs only; idempotent replays are not counted).",
)
RUNS_COMPLETED = Counter(
    "flowforge_workflow_runs_completed_total",
    "Workflow runs that reached a terminal state.",
    ["outcome"],
)
TASKS_CLAIMED = Counter("flowforge_tasks_claimed_total", "Tasks claimed by workers.")
TASKS_COMPLETED = Counter(
    "flowforge_tasks_completed_total",
    "Task attempts finished, by attempt outcome.",
    ["outcome"],
)
TASK_RETRIES = Counter("flowforge_task_retries_total", "Retries scheduled after a failed attempt.")
TASK_EXECUTION = Histogram(
    "flowforge_task_execution_duration_seconds",
    "Handler execution time per attempt.",
    ["handler"],
    buckets=LATENCY_BUCKETS,
)
TASK_QUEUE_WAIT = Histogram(
    "flowforge_task_queue_wait_seconds",
    "Time between a task becoming READY and being claimed.",
    buckets=LATENCY_BUCKETS,
)
TASKS_BY_STATUS = Gauge(
    "flowforge_tasks_by_status",
    "Number of tasks in each non-terminal status (sampled by the scheduler).",
    ["status"],
)
READY_QUEUE_DEPTH = Gauge("flowforge_ready_queue_depth", "Tasks in READY state.")
RUNNING_TASKS = Gauge(
    "flowforge_running_task_count", "Tasks currently executing in this worker process."
)
ACTIVE_WORKERS = Gauge(
    "flowforge_active_workers", "Workers whose heartbeat is within the lease period."
)
WORKER_HEARTBEAT_AGE = Gauge(
    "flowforge_worker_heartbeat_age_seconds",
    "Age of the oldest heartbeat among workers that have not stopped.",
)
EXPIRED_LEASES = Counter("flowforge_expired_leases_total", "Task leases recovered after expiry.")
LEASE_RENEWAL_FAILURES = Counter(
    "flowforge_lease_renewal_failures_total",
    "Lease renewals that failed, by reason.",
    ["reason"],
)
STALE_RESULTS_REJECTED = Counter(
    "flowforge_stale_results_rejected_total",
    "Results reported by a worker that no longer held the lease.",
)
DB_OPERATION_DURATION = Histogram(
    "flowforge_database_operation_duration_seconds",
    "Duration of logical database operations (transactions).",
    ["operation"],
    buckets=LATENCY_BUCKETS,
)
DB_QUERY_DURATION = Histogram(
    "flowforge_db_query_duration_seconds",
    "Duration of individual SQL statements.",
    buckets=LATENCY_BUCKETS,
)
DB_ERRORS = Counter(
    "flowforge_database_errors_total", "Database errors by component.", ["component"]
)
RETENTION_DELETED = Counter(
    "flowforge_retention_deleted_total",
    "Rows removed by retention, by kind (runs, events, workers).",
    ["kind"],
)
SCHEDULER_CYCLE = Histogram(
    "flowforge_scheduler_cycle_duration_seconds",
    "Duration of one scheduler reconciliation cycle.",
    buckets=LATENCY_BUCKETS,
)
HTTP_REQUESTS = Counter(
    "flowforge_http_requests_total", "HTTP requests.", ["method", "route", "status"]
)
HTTP_DURATION = Histogram(
    "flowforge_http_request_duration_seconds",
    "HTTP request latency.",
    ["method", "route"],
    buckets=LATENCY_BUCKETS,
)


@contextmanager
def time_db_operation(operation: str) -> Iterator[None]:
    start = time.perf_counter()
    try:
        yield
    finally:
        DB_OPERATION_DURATION.labels(operation).observe(time.perf_counter() - start)
