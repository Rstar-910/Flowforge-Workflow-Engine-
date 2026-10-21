"""Registry of trusted task handlers.

Workers execute only functions registered here, by name. Workflow definitions
reference handler names; they can never supply code.
"""

from __future__ import annotations

import importlib
import inspect
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel

Isolation = Literal["async", "thread", "process"]
HANDLER_NAME_RE = re.compile(r"^[a-z][a-z0-9_.]{0,99}$")


class HandlerRegistrationError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class HandlerSpec:
    name: str
    func: Callable[..., Any]
    isolation: Isolation
    input_model: type[BaseModel] | None
    # Documentation flag surfaced in the API/logs: False means a retry may repeat
    # an external side effect that the downstream system cannot de-duplicate.
    idempotent: bool
    description: str


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, HandlerSpec] = {}

    def register(
        self,
        name: str,
        func: Callable[..., Any],
        *,
        isolation: Isolation | None = None,
        input_model: type[BaseModel] | None = None,
        idempotent: bool = True,
        description: str = "",
    ) -> HandlerSpec:
        if not HANDLER_NAME_RE.match(name):
            raise HandlerRegistrationError(f"invalid handler name {name!r}")
        if name in self._handlers:
            raise HandlerRegistrationError(f"handler {name!r} is already registered")
        if not callable(func):
            raise HandlerRegistrationError(f"handler {name!r} is not callable")
        is_async = inspect.iscoroutinefunction(func)
        if isolation is None:
            isolation = "async" if is_async else "thread"
        if isolation == "async" and not is_async:
            raise HandlerRegistrationError(f"handler {name!r}: async isolation needs async def")
        if isolation in ("thread", "process") and is_async:
            raise HandlerRegistrationError(
                f"handler {name!r}: {isolation} isolation needs a synchronous function"
            )
        if isolation == "process" and (
            "<locals>" in func.__qualname__ or func.__module__ == "__main__"
        ):
            raise HandlerRegistrationError(
                f"handler {name!r}: process isolation needs an importable module-level function"
            )
        if input_model is not None and not (
            isinstance(input_model, type) and issubclass(input_model, BaseModel)
        ):
            raise HandlerRegistrationError(f"handler {name!r}: input_model must be a BaseModel")
        spec = HandlerSpec(
            name=name,
            func=func,
            isolation=isolation,
            input_model=input_model,
            idempotent=idempotent,
            description=description or (inspect.getdoc(func) or "").split("\n")[0],
        )
        self._handlers[name] = spec
        return spec

    def handler(
        self,
        name: str,
        *,
        isolation: Isolation | None = None,
        input_model: type[BaseModel] | None = None,
        idempotent: bool = True,
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            self.register(
                name, func, isolation=isolation, input_model=input_model, idempotent=idempotent
            )
            return func

        return decorator

    def get(self, name: str) -> HandlerSpec | None:
        return self._handlers.get(name)

    def names(self) -> list[str]:
        return sorted(self._handlers)

    def __contains__(self, name: object) -> bool:
        return name in self._handlers


# Process-wide default registry used by handler modules (`@task_handler(...)`).
default_registry = HandlerRegistry()
task_handler = default_registry.handler


def load_handler_modules(modules: Iterable[str]) -> HandlerRegistry:
    """Import operator-configured modules so their @task_handler decorators run."""
    for module in modules:
        importlib.import_module(module)
    return default_registry
