"""Task, attempt and run state machines.

The transition tables here are the single definition of which state changes are
legal. Repository code asserts against them and the database reinforces the set
of possible values with CHECK constraints.
"""

from __future__ import annotations

from enum import StrEnum


class TaskStatus(StrEnum):
    BLOCKED = "BLOCKED"
    READY = "READY"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UPSTREAM_FAILED = "UPSTREAM_FAILED"
    CANCELLED = "CANCELLED"


class RunStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


class AttemptOutcome(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    # The worker stopped renewing its lease (crash, pause, partition). Whether the
    # handler's side effects happened is unknown.
    LEASE_EXPIRED = "LEASE_EXPIRED"
    CANCELLED = "CANCELLED"


TERMINAL_TASK_STATUSES = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.UPSTREAM_FAILED,
        TaskStatus.CANCELLED,
    }
)
ACTIVE_TASK_STATUSES = frozenset(TaskStatus) - TERMINAL_TASK_STATUSES
TERMINAL_RUN_STATUSES = frozenset({RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED})

TASK_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    TaskStatus.BLOCKED: frozenset(
        {TaskStatus.READY, TaskStatus.CANCELLED, TaskStatus.UPSTREAM_FAILED}
    ),
    TaskStatus.READY: frozenset({TaskStatus.RUNNING, TaskStatus.CANCELLED}),
    TaskStatus.RUNNING: frozenset(
        {
            TaskStatus.SUCCEEDED,
            TaskStatus.RETRY_WAIT,
            TaskStatus.READY,  # lease expired with zero backoff
            TaskStatus.FAILED,
            TaskStatus.CANCELLED,
        }
    ),
    TaskStatus.RETRY_WAIT: frozenset({TaskStatus.READY, TaskStatus.CANCELLED}),
    TaskStatus.SUCCEEDED: frozenset(),
    # Manual (authorised) retry re-opens a failed task.
    TaskStatus.FAILED: frozenset({TaskStatus.READY}),
    # When the failed upstream task is manually retried, dependents re-block.
    TaskStatus.UPSTREAM_FAILED: frozenset({TaskStatus.BLOCKED}),
    TaskStatus.CANCELLED: frozenset(),
}

RUN_TRANSITIONS: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.PENDING: frozenset(
        {RunStatus.RUNNING, RunStatus.CANCELLING, RunStatus.CANCELLED, RunStatus.FAILED}
    ),
    RunStatus.RUNNING: frozenset(
        {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLING, RunStatus.CANCELLED}
    ),
    RunStatus.CANCELLING: frozenset({RunStatus.CANCELLED}),
    RunStatus.SUCCEEDED: frozenset(),
    RunStatus.FAILED: frozenset({RunStatus.RUNNING}),  # manual retry
    RunStatus.CANCELLED: frozenset(),
}


class InvalidTransitionError(Exception):
    pass


def can_transition_task(src: TaskStatus, dst: TaskStatus) -> bool:
    return dst in TASK_TRANSITIONS[src]


def can_transition_run(src: RunStatus, dst: RunStatus) -> bool:
    return dst in RUN_TRANSITIONS[src]


def check_task_transition(src: TaskStatus, dst: TaskStatus) -> None:
    if not can_transition_task(src, dst):
        raise InvalidTransitionError(f"task transition {src} -> {dst} is not allowed")


def check_run_transition(src: RunStatus, dst: RunStatus) -> None:
    if not can_transition_run(src, dst):
        raise InvalidTransitionError(f"run transition {src} -> {dst} is not allowed")
