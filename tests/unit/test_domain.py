from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from flowforge.config import Settings
from flowforge.domain.errors import (
    PermanentTaskError,
    RetryableTaskError,
    TaskTimeoutError,
    classify_exception,
)
from flowforge.domain.retry import RetryPolicy, base_delay, compute_retry_delay
from flowforge.domain.states import (
    RUN_TRANSITIONS,
    TASK_TRANSITIONS,
    TERMINAL_RUN_STATUSES,
    InvalidTransitionError,
    RunStatus,
    TaskStatus,
    check_run_transition,
    check_task_transition,
)
from flowforge.engine.submission import canonical_hash

# ------------------------------------------------------------------ retry


def test_exponential_backoff_is_capped():
    p = RetryPolicy(initial_delay_seconds=1, max_delay_seconds=30, multiplier=2, jitter_ratio=0)
    assert [compute_retry_delay(p, n) for n in range(1, 8)] == [1, 2, 4, 8, 16, 30, 30]


def test_huge_attempt_numbers_do_not_overflow():
    p = RetryPolicy(initial_delay_seconds=1, max_delay_seconds=60, multiplier=10)
    assert base_delay(p, 10_000) == 60


def test_attempt_numbers_are_one_based():
    with pytest.raises(ValueError):
        base_delay(RetryPolicy(), 0)


def test_retry_after_is_a_floor_but_capped():
    p = RetryPolicy(initial_delay_seconds=1, max_delay_seconds=10, jitter_ratio=0)
    assert compute_retry_delay(p, 1, retry_after_seconds=5) == 5
    assert compute_retry_delay(p, 1, retry_after_seconds=3600) == 10


@settings(max_examples=500)
@given(
    initial=st.floats(0, 100),
    extra=st.floats(0, 1000),
    multiplier=st.floats(1, 10),
    jitter=st.floats(0, 1),
    attempt=st.integers(1, 200),
    r=st.floats(0, 1, exclude_max=True),
    retry_after=st.none() | st.floats(0, 10_000),
)
def test_delay_is_always_within_bounds(initial, extra, multiplier, jitter, attempt, r, retry_after):
    p = RetryPolicy(
        initial_delay_seconds=initial,
        max_delay_seconds=initial + extra,
        multiplier=multiplier,
        jitter_ratio=jitter,
    )
    d = compute_retry_delay(p, attempt, retry_after_seconds=retry_after, rand=lambda: r)
    assert 0 <= d <= p.max_delay_seconds
    undelayed = base_delay(p, attempt)
    if retry_after is None:
        # Jitter only shortens, by at most jitter_ratio of the base delay.
        assert undelayed * (1 - jitter) - 1e-9 <= d <= undelayed + 1e-9


@settings(max_examples=200)
@given(attempt=st.integers(1, 30))
def test_backoff_is_monotonic_without_jitter(attempt):
    p = RetryPolicy(initial_delay_seconds=0.5, max_delay_seconds=300, jitter_ratio=0)
    assert base_delay(p, attempt) <= base_delay(p, attempt + 1)


def test_jitter_spreads_retries():
    p = RetryPolicy(initial_delay_seconds=10, max_delay_seconds=10, jitter_ratio=1)
    delays = {round(compute_retry_delay(p, 1), 3) for _ in range(200)}
    assert len(delays) > 50 and all(0 <= d <= 10 for d in delays)


def test_retry_policy_rejects_invalid():
    with pytest.raises(ValidationError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValidationError):
        RetryPolicy(initial_delay_seconds=10, max_delay_seconds=5)
    with pytest.raises(ValidationError):
        RetryPolicy(multiplier=0.5)


# -------------------------------------------------------- classification


def test_error_classification():
    p = RetryPolicy(non_retryable_errors=("KeyError",))
    assert classify_exception(RetryableTaskError("x", retry_after_seconds=3), p).retryable
    assert (
        classify_exception(RetryableTaskError("x", retry_after_seconds=3), p).retry_after_seconds
        == 3
    )
    assert not classify_exception(PermanentTaskError("x"), p).retryable
    assert classify_exception(ConnectionError("x"), p).retryable  # unknown -> transient
    assert not classify_exception(KeyError("x"), p).retryable  # listed as non-retryable
    timeout = classify_exception(TaskTimeoutError("t"), p)
    assert timeout.timed_out and timeout.retryable
    assert not classify_exception(
        TaskTimeoutError("t"), RetryPolicy(retry_on_timeout=False)
    ).retryable
    assert len(classify_exception(ValueError("x" * 10_000), p).message) == 2000


# --------------------------------------------------------- state machines


def test_terminal_task_states_have_no_exits_except_documented_retry_paths():
    assert TASK_TRANSITIONS[TaskStatus.SUCCEEDED] == frozenset()
    assert TASK_TRANSITIONS[TaskStatus.CANCELLED] == frozenset()
    assert TASK_TRANSITIONS[TaskStatus.FAILED] == {TaskStatus.READY}
    assert TASK_TRANSITIONS[TaskStatus.UPSTREAM_FAILED] == {TaskStatus.BLOCKED}


def test_every_status_has_a_transition_entry():
    assert set(TASK_TRANSITIONS) == set(TaskStatus)
    assert set(RUN_TRANSITIONS) == set(RunStatus)


@pytest.mark.parametrize(
    ("src", "dst"),
    [
        (TaskStatus.BLOCKED, TaskStatus.RUNNING),
        (TaskStatus.SUCCEEDED, TaskStatus.READY),
        (TaskStatus.RETRY_WAIT, TaskStatus.RUNNING),
        (TaskStatus.READY, TaskStatus.SUCCEEDED),
    ],
)
def test_illegal_task_transitions(src, dst):
    with pytest.raises(InvalidTransitionError):
        check_task_transition(src, dst)


def test_run_transitions():
    check_run_transition(RunStatus.RUNNING, RunStatus.CANCELLING)
    check_run_transition(RunStatus.FAILED, RunStatus.RUNNING)  # manual retry
    for terminal in (RunStatus.SUCCEEDED, RunStatus.CANCELLED):
        assert RUN_TRANSITIONS[terminal] == frozenset()
    check_run_transition(RunStatus.CANCELLING, RunStatus.FAILED)  # workflow deadline
    with pytest.raises(InvalidTransitionError):
        check_run_transition(RunStatus.CANCELLING, RunStatus.RUNNING)
    assert {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED} == TERMINAL_RUN_STATUSES


# ------------------------------------------------------------ idempotency


def test_canonical_hash_ignores_key_order_but_not_values():
    assert canonical_hash({"a": 1, "b": [1, 2]}) == canonical_hash({"b": [1, 2], "a": 1})
    assert canonical_hash({"a": 1}) != canonical_hash({"a": 2})
    assert canonical_hash({"a": [1, 2]}) != canonical_hash({"a": [2, 1]})


# ----------------------------------------------------------------- config


def test_api_key_parsing_and_validation():
    s = Settings(_env_file=None, api_keys="k1:t1:admin, k2:t2:user")  # type: ignore[call-arg]
    keys = s.parsed_api_keys()
    assert keys["k1"].tenant == "t1" and keys["k2"].role == "user"
    with pytest.raises(ValueError):
        Settings(_env_file=None, api_keys="k1:t1:root").parsed_api_keys()  # type: ignore[call-arg]
    with pytest.raises(ValueError):
        Settings(_env_file=None, api_keys="justakey").parsed_api_keys()  # type: ignore[call-arg]


def test_heartbeat_must_be_shorter_than_lease():
    with pytest.raises(ValidationError):
        Settings(_env_file=None, lease_seconds=5, heartbeat_interval_seconds=5)  # type: ignore[call-arg]


def test_fault_points_require_explicit_enable():
    s = Settings(_env_file=None, fault_points="crash_after_claim")  # type: ignore[call-arg]
    assert s.fault_point_set() == frozenset()
    s = Settings(
        _env_file=None,
        fault_points="crash_after_claim",  # type: ignore[call-arg]
        enable_fault_injection=True,
    )
    assert s.fault_point_set() == {"crash_after_claim"}


def test_secrets_are_not_in_repr():
    s = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://u:hunter2@h/db",  # type: ignore[call-arg]
        api_keys="topsecret:t:admin",
    )
    assert "hunter2" not in repr(s) and "topsecret" not in repr(s)
