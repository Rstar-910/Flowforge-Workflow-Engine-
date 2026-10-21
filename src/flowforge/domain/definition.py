"""Workflow definition schema and DAG validation.

A definition is validated in two layers:

1. ``WorkflowDefinitionSpec`` (Pydantic) checks field types, ranges and sizes.
2. ``validate_definition`` checks cross-field rules: unique task IDs, known
   handlers, dependency references, and absence of cycles.

Nothing is persisted unless both layers pass.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from flowforge.domain.retry import RetryPolicy

SLUG_PATTERN = r"^[a-z0-9][a-z0-9_.-]{0,99}$"
TASK_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$"
HANDLER_PATTERN = r"^[a-z][a-z0-9_.]{0,99}$"
MAX_TASKS = 500
MAX_DEPENDENCIES = 100
MAX_TIMEOUT_SECONDS = 24 * 3600
MAX_STATIC_INPUT_BYTES = 64 * 1024


class TaskSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(pattern=TASK_ID_PATTERN)
    handler: str = Field(pattern=HANDLER_PATTERN)
    depends_on: tuple[str, ...] = Field(default=(), max_length=MAX_DEPENDENCIES)
    input: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: float = Field(default=60.0, gt=0, le=MAX_TIMEOUT_SECONDS)
    retry: RetryPolicy = Field(default_factory=RetryPolicy)

    @field_validator("input")
    @classmethod
    def _input_size(cls, v: dict[str, Any]) -> dict[str, Any]:
        if json_size(v) > MAX_STATIC_INPUT_BYTES:
            raise ValueError(f"task input exceeds {MAX_STATIC_INPUT_BYTES} bytes")
        return v


class WorkflowDefinitionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(pattern=SLUG_PATTERN)
    version: int = Field(ge=1, le=1_000_000)
    description: str | None = Field(default=None, max_length=2000)
    tasks: tuple[TaskSpec, ...] = Field(min_length=1, max_length=MAX_TASKS)


@dataclass(frozen=True, slots=True)
class ValidationIssue:
    path: str
    message: str


class DefinitionValidationError(Exception):
    def __init__(self, issues: list[ValidationIssue]) -> None:
        super().__init__("; ".join(f"{i.path}: {i.message}" for i in issues))
        self.issues = issues


@dataclass(frozen=True, slots=True)
class ValidatedDefinition:
    spec: WorkflowDefinitionSpec
    topological_order: tuple[str, ...]

    def task(self, task_id: str) -> TaskSpec:
        for t in self.spec.tasks:
            if t.id == task_id:
                return t
        raise KeyError(task_id)


def json_size(value: Any) -> int:
    return len(json.dumps(value, separators=(",", ":"), default=str).encode())


def find_cycle(graph: Mapping[str, Iterable[str]]) -> list[str] | None:
    """Return one dependency cycle as a list of node IDs (first == last), or None.

    ``graph`` maps each node to the nodes it depends on. Iterative DFS so that
    deep chains cannot hit Python's recursion limit.
    """
    white, grey, black = 0, 1, 2
    color = dict.fromkeys(graph, white)
    for root in graph:
        if color[root] != white:
            continue
        stack: list[tuple[str, Iterable[str]]] = [(root, iter(graph[root]))]
        path = [root]
        color[root] = grey
        while stack:
            node, it = stack[-1]
            advanced = False
            for nxt in it:
                if nxt not in color:
                    continue  # unknown reference; reported separately
                if color[nxt] == grey:
                    start = path.index(nxt)
                    return [*path[start:], nxt]
                if color[nxt] == white:
                    color[nxt] = grey
                    path.append(nxt)
                    stack.append((nxt, iter(graph[nxt])))
                    advanced = True
                    break
            if not advanced:
                color[node] = black
                path.pop()
                stack.pop()
    return None


def topological_order(graph: Mapping[str, Iterable[str]]) -> list[str]:
    """Kahn's algorithm with deterministic (insertion) ordering. Assumes acyclic."""
    deps = {n: set(d) for n, d in graph.items()}
    dependents: dict[str, list[str]] = {n: [] for n in graph}
    for n, ds in deps.items():
        for d in ds:
            dependents[d].append(n)
    ready = [n for n, ds in deps.items() if not ds]
    order: list[str] = []
    while ready:
        n = ready.pop(0)
        order.append(n)
        for m in dependents[n]:
            deps[m].discard(n)
            if not deps[m]:
                ready.append(m)
    if len(order) != len(graph):
        raise ValueError("graph contains a cycle")
    return order


def validate_definition(
    spec: WorkflowDefinitionSpec, known_handlers: Iterable[str]
) -> ValidatedDefinition:
    handlers = set(known_handlers)
    issues: list[ValidationIssue] = []
    seen: dict[str, int] = {}
    for idx, task in enumerate(spec.tasks):
        if task.id in seen:
            issues.append(ValidationIssue(f"tasks[{idx}].id", f"duplicate task id {task.id!r}"))
        else:
            seen[task.id] = idx
        if task.handler not in handlers:
            issues.append(
                ValidationIssue(f"tasks[{idx}].handler", f"unknown handler {task.handler!r}")
            )
        if len(set(task.depends_on)) != len(task.depends_on):
            issues.append(ValidationIssue(f"tasks[{idx}].depends_on", "duplicate dependency"))
        for dep in task.depends_on:
            if dep == task.id:
                issues.append(ValidationIssue(f"tasks[{idx}].depends_on", "task depends on itself"))
    for idx, task in enumerate(spec.tasks):
        for dep in task.depends_on:
            if dep not in seen:
                issues.append(
                    ValidationIssue(f"tasks[{idx}].depends_on", f"unknown task reference {dep!r}")
                )
    if issues:
        raise DefinitionValidationError(issues)

    graph = {t.id: list(t.depends_on) for t in spec.tasks}
    cycle = find_cycle(graph)
    if cycle is not None:
        raise DefinitionValidationError(
            [ValidationIssue("tasks", "dependency cycle: " + " -> ".join(cycle))]
        )
    return ValidatedDefinition(spec=spec, topological_order=tuple(topological_order(graph)))
