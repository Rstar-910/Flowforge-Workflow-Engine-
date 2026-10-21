"""Failure-injection points used by the crash tests and demo scripts.

Disabled unless ``FLOWFORGE_ENABLE_FAULT_INJECTION=true``; then
``FLOWFORGE_FAULT_POINTS`` lists the points that are active. A crash point
terminates the process with ``os._exit`` - no cleanup, no lease release, no
final log flush - which is the closest in-process approximation of SIGKILL.

Points:
  crash_after_claim    - right after the claim transaction commits
  crash_after_handler  - after the handler returned (side effects done), before
                         the result is persisted
  skip_heartbeats      - never renew leases (simulates a paused/partitioned worker
                         that keeps executing)
"""

from __future__ import annotations

import logging
import os
import sys

logger = logging.getLogger(__name__)

CRASH_EXIT_CODE = 86


class FaultInjector:
    def __init__(self, points: frozenset[str]) -> None:
        self.points = points
        if points:
            logger.warning("failure injection enabled: %s", ",".join(sorted(points)))

    def active(self, point: str) -> bool:
        return point in self.points

    def crash_if(self, point: str) -> None:
        if point in self.points:
            sys.stderr.write(f"FAULT INJECTION: crashing at {point}\n")
            sys.stderr.flush()
            os._exit(CRASH_EXIT_CODE)
