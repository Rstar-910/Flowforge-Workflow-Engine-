from __future__ import annotations

import itertools

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from flowforge.domain.definition import (
    MAX_TASKS,
    DefinitionValidationError,
    WorkflowDefinitionSpec,
    find_cycle,
    topological_order,
    validate_definition,
)

HANDLERS = {"noop", "echo"}


def spec(tasks, **kw):
    return WorkflowDefinitionSpec.model_validate({"name": "wf", "version": 1, "tasks": tasks, **kw})


def t(task_id, deps=(), handler="noop", **kw):
    return {"id": task_id, "handler": handler, "depends_on": list(deps), **kw}


def test_valid_definition_yields_topological_order():
    v = validate_definition(spec([t("c", ["b"]), t("b", ["a"]), t("a")]), HANDLERS)
    assert v.topological_order == ("a", "b", "c")
    assert v.task("b").depends_on == ("a",)


def test_defaults_are_applied():
    task = spec([t("a")]).tasks[0]
    assert task.timeout_seconds == 60
    assert task.retry.max_attempts == 3
    assert task.input == {}


@pytest.mark.parametrize(
    ("tasks", "message"),
    [
        ([t("a"), t("a")], "duplicate task id"),
        ([t("a", ["missing"])], "unknown task reference"),
        ([t("a", handler="nope")], "unknown handler"),
        ([t("a", ["a"])], "depends on itself"),
        ([t("a"), t("b", ["a", "a"])], "duplicate dependency"),
        ([t("a", ["c"]), t("b", ["a"]), t("c", ["b"])], "dependency cycle"),
    ],
)
def test_invalid_graphs(tasks, message):
    with pytest.raises(DefinitionValidationError) as exc:
        validate_definition(spec(tasks), HANDLERS)
    assert any(message in i.message for i in exc.value.issues)


def test_cycle_error_names_the_cycle():
    with pytest.raises(DefinitionValidationError) as exc:
        validate_definition(spec([t("x", ["z"]), t("y", ["x"]), t("z", ["y"])]), HANDLERS)
    msg = exc.value.issues[0].message
    assert msg.startswith("dependency cycle: ")
    nodes = msg.removeprefix("dependency cycle: ").split(" -> ")
    assert nodes[0] == nodes[-1] and set(nodes) == {"x", "y", "z"}


@pytest.mark.parametrize(
    "bad",
    [
        {"tasks": []},
        {"name": "Has Spaces"},
        {"version": 0},
        {"extra": 1},
        {"tasks": [t("a", timeout_seconds=0)]},
        {"tasks": [t("a", timeout_seconds=10**9)]},
        {"tasks": [t("a", retry={"max_attempts": 0})]},
        {"tasks": [t("a", retry={"max_attempts": 1000})]},
        {"tasks": [t("a", retry={"initial_delay_seconds": 5, "max_delay_seconds": 1})]},
        {"tasks": [t("a", retry={"jitter_ratio": 2})]},
        {"tasks": [t("a", retry={"bogus": 1})]},
        {"tasks": [t("a", handler="../../etc/passwd")]},
        {"tasks": [t("bad id!")]},
        {"tasks": [t("a", input={"blob": "x" * 70_000})]},
        {"tasks": [t(f"t{i}") for i in range(MAX_TASKS + 1)]},
    ],
)
def test_schema_rejects_malformed_fields(bad):
    body = {"name": "wf", "version": 1, "tasks": [t("a")]}
    body.update(bad)
    with pytest.raises(ValidationError):
        WorkflowDefinitionSpec.model_validate(body)


def test_deep_chain_does_not_hit_recursion_limit():
    n = MAX_TASKS
    tasks = [t("n0")] + [t(f"n{i}", [f"n{i - 1}"]) for i in range(1, n)]
    v = validate_definition(spec(tasks), HANDLERS)
    assert v.topological_order[0] == "n0" and v.topological_order[-1] == f"n{n - 1}"


# --------------------------------------------------------------- properties


@st.composite
def dags(draw, max_nodes=25):
    """Random DAG: node i may only depend on nodes < i (guaranteed acyclic)."""
    n = draw(st.integers(1, max_nodes))
    graph = {}
    for i in range(n):
        deps = draw(st.sets(st.integers(0, i - 1), max_size=min(i, 5))) if i else set()
        graph[f"n{i}"] = [f"n{d}" for d in sorted(deps)]
    order = draw(st.permutations(list(graph)))
    return {k: graph[k] for k in order}  # random declaration order


@settings(max_examples=300, deadline=None)
@given(dags())
def test_random_dags_are_accepted_and_ordered(graph):
    assert find_cycle(graph) is None
    order = topological_order(graph)
    pos = {n: i for i, n in enumerate(order)}
    assert sorted(order) == sorted(graph)
    for node, deps in graph.items():
        assert all(pos[d] < pos[node] for d in deps)
    v = validate_definition(spec([t(k, d) for k, d in graph.items()]), HANDLERS)
    assert set(v.topological_order) == set(graph)


@settings(max_examples=300, deadline=None)
@given(dags(), st.data())
def test_adding_a_back_edge_creates_a_detected_cycle(graph, data):
    order = topological_order(graph)
    if len(order) < 2:
        return
    # Pick u before v in topological order, then make u depend on v while v
    # (transitively or directly) depends on u -> guaranteed cycle.
    i = data.draw(st.integers(0, len(order) - 2))
    u, v = order[i], order[i + 1]
    cyclic = {k: list(d) for k, d in graph.items()}
    cyclic[v] = sorted({*cyclic[v], u})
    cyclic[u] = sorted({*cyclic[u], v})
    cycle = find_cycle(cyclic)
    assert cycle is not None and cycle[0] == cycle[-1]
    for a, b in itertools.pairwise(cycle):
        assert b in cyclic[a]  # every hop is a real dependency edge
    with pytest.raises(DefinitionValidationError):
        validate_definition(spec([t(k, d) for k, d in cyclic.items()]), HANDLERS)


def test_deadline_and_queue_timeout_fields():
    s = spec([t("a", queue_timeout_seconds=30)], deadline_seconds=600)
    assert s.deadline_seconds == 600 and s.tasks[0].queue_timeout_seconds == 30
    defaults = spec([t("a")])
    assert defaults.deadline_seconds is None and defaults.tasks[0].queue_timeout_seconds is None
    for bad in ({"deadline_seconds": 0}, {"deadline_seconds": 31 * 24 * 3600}):
        with pytest.raises(ValidationError):
            spec([t("a")], **bad)
    for bad_q in (0, -1, 8 * 24 * 3600):
        with pytest.raises(ValidationError):
            spec([t("a", queue_timeout_seconds=bad_q)])
