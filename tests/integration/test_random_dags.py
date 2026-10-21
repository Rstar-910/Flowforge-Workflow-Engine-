"""Randomised DAG execution: no task ever starts before all its dependencies succeeded.

Graphs come from a seeded generator (reproducible). Hypothesis drives the
pure DAG-validation properties in tests/unit; here each example needs a real
cluster run, so a fixed set of seeds keeps the test fast and deterministic.
"""

from __future__ import annotations

import random

import pytest

from tests.support.helpers import Cluster, attempts_for, start_run, task, tasks_by_key, wait_for_run

pytestmark = pytest.mark.integration


def random_dag(seed: int) -> list[dict]:
    rng = random.Random(seed)
    n = rng.randint(3, 25)
    tasks = []
    for i in range(n):
        deps = rng.sample(range(i), k=min(i, rng.randint(0, 3))) if i else []
        handler = rng.choice(["noop", "noop", "flaky", "test.record_order"])
        tasks.append(
            task(
                f"n{i:02d}",
                handler,
                [f"n{d:02d}" for d in deps],
                input={"p": 0.3} if handler == "flaky" else {},
                max_attempts=8,
            )
        )
    rng.shuffle(tasks)
    return tasks


@pytest.mark.parametrize("seed", range(8))
async def test_random_dag_respects_dependencies(db, settings, registry, seed):
    tasks = random_dag(seed)
    run_id = await start_run(db, registry, tasks)
    async with Cluster(db, settings, registry, workers=3, concurrency=3):
        status = await wait_for_run(db, run_id, {"SUCCEEDED", "FAILED"}, timeout=60)
    persisted = await tasks_by_key(db, run_id)
    attempts = await attempts_for(db, run_id)
    first_start = {}
    for a in attempts:
        first_start.setdefault(a["task_key"], a["started_at"])
    deps = {t["id"]: t["depends_on"] for t in tasks}
    for key, t in persisted.items():
        for d in deps[key]:
            if key in first_start:
                dep = persisted[d]
                assert dep["status"] == "SUCCEEDED"
                assert first_start[key] >= dep["completed_at"], (key, d)
        # Bounded retries.
        assert t["attempt_count"] <= 8
    # flaky with p=0.3 and 8 attempts virtually always succeeds (0.3^8 ~ 7e-5 per task)
    assert status == "SUCCEEDED"
