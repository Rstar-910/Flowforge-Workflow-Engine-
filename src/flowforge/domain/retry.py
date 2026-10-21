"""Retry policy and bounded exponential backoff with jitter."""

from __future__ import annotations

import random
from collections.abc import Callable

from pydantic import BaseModel, ConfigDict, Field, model_validator

MAX_ATTEMPTS_LIMIT = 50
MAX_DELAY_LIMIT_SECONDS = 24 * 3600.0


class RetryPolicy(BaseModel):
    """Per-task retry configuration.

    ``max_attempts`` counts *total* attempts, including the first one.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_attempts: int = Field(default=3, ge=1, le=MAX_ATTEMPTS_LIMIT)
    initial_delay_seconds: float = Field(default=1.0, ge=0, le=MAX_DELAY_LIMIT_SECONDS)
    max_delay_seconds: float = Field(default=60.0, ge=0, le=MAX_DELAY_LIMIT_SECONDS)
    multiplier: float = Field(default=2.0, ge=1.0, le=10.0)
    # Fraction of the computed delay that is randomised away (0 = no jitter,
    # 1 = "full jitter" in [0, delay]). Jitter only ever shortens the delay, so
    # max_delay_seconds remains a hard upper bound.
    jitter_ratio: float = Field(default=0.2, ge=0.0, le=1.0)
    retry_on_timeout: bool = True
    # Exception class names (as reported in attempt.error_type) that must never retry.
    non_retryable_errors: tuple[str, ...] = Field(default=(), max_length=50)

    @model_validator(mode="after")
    def _delays_ordered(self) -> RetryPolicy:
        if self.max_delay_seconds < self.initial_delay_seconds:
            raise ValueError("max_delay_seconds must be >= initial_delay_seconds")
        return self


def base_delay(policy: RetryPolicy, failed_attempt_number: int) -> float:
    """Delay before the attempt following ``failed_attempt_number`` (1-based), no jitter."""
    if failed_attempt_number < 1:
        raise ValueError("attempt numbers are 1-based")
    exponent = failed_attempt_number - 1
    # Compute in a way that cannot overflow for large exponents.
    delay = policy.initial_delay_seconds
    for _ in range(exponent):
        delay *= policy.multiplier
        if delay >= policy.max_delay_seconds:
            return policy.max_delay_seconds
    return min(policy.max_delay_seconds, delay)


def compute_retry_delay(
    policy: RetryPolicy,
    failed_attempt_number: int,
    *,
    retry_after_seconds: float | None = None,
    rand: Callable[[], float] = random.random,
) -> float:
    """Jittered delay in seconds, always within ``[0, max_delay_seconds]``.

    ``retry_after_seconds`` (e.g. from an HTTP 429 Retry-After header) acts as a
    lower bound but is itself capped by ``max_delay_seconds`` so that a
    misbehaving dependency cannot postpone a retry indefinitely.
    """
    delay = base_delay(policy, failed_attempt_number)
    delay -= delay * policy.jitter_ratio * rand()
    if retry_after_seconds is not None and retry_after_seconds > 0:
        delay = max(delay, min(retry_after_seconds, policy.max_delay_seconds))
    return max(0.0, min(delay, policy.max_delay_seconds))
