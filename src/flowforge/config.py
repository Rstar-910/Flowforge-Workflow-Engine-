"""Process configuration, loaded from environment variables (prefix ``FLOWFORGE_``)."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

Role = Literal["user", "admin"]


@dataclass(frozen=True, slots=True)
class ApiKeyEntry:
    key: str
    tenant: str
    role: Role


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="FLOWFORGE_", env_file=".env", extra="ignore", frozen=True
    )

    database_url: SecretStr = SecretStr(
        "postgresql+asyncpg://flowforge:flowforge@localhost:5433/flowforge"
    )
    db_pool_size: int = Field(default=10, ge=1, le=200)
    db_max_overflow: int = Field(default=5, ge=0, le=200)
    db_connect_timeout_seconds: float = Field(default=5.0, gt=0)
    db_statement_timeout_ms: int = Field(default=15_000, ge=100)

    service_name: str = "flowforge"
    log_level: str = "INFO"
    log_format: Literal["json", "text"] = "json"

    # "key:tenant:role" entries separated by commas.
    api_keys: SecretStr = SecretStr("")
    max_request_bytes: int = Field(default=1_048_576, ge=1024)
    max_json_value_bytes: int = Field(default=256 * 1024, ge=1024)
    request_timeout_seconds: float = Field(default=30.0, gt=0)
    # Optional per-tenant limit on run submissions (per API process). None = off.
    run_submit_rate_per_second: float | None = Field(default=None, gt=0)
    run_submit_burst: int = Field(default=20, ge=1)

    handler_modules: str = "flowforge.handlers.builtin"

    lease_seconds: float = Field(default=15.0, gt=0)
    heartbeat_interval_seconds: float = Field(default=5.0, gt=0)
    worker_poll_interval_seconds: float = Field(default=0.5, gt=0)
    worker_max_poll_interval_seconds: float = Field(default=2.0, gt=0)
    worker_shutdown_grace_seconds: float = Field(default=30.0, ge=0)
    scheduler_interval_seconds: float = Field(default=1.0, gt=0)
    scheduler_batch_size: int = Field(default=500, ge=1)

    metrics_port: int | None = None

    # Failure injection is only honoured when explicitly enabled; see flowforge.faults.
    enable_fault_injection: bool = False
    fault_points: str = ""

    @field_validator("log_level")
    @classmethod
    def _upper(cls, v: str) -> str:
        v = v.upper()
        if v not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("invalid log level")
        return v

    @model_validator(mode="after")
    def _check_lease(self) -> Settings:
        if self.heartbeat_interval_seconds >= self.lease_seconds:
            raise ValueError("heartbeat_interval_seconds must be smaller than lease_seconds")
        return self

    def parsed_api_keys(self) -> dict[str, ApiKeyEntry]:
        entries: dict[str, ApiKeyEntry] = {}
        raw = self.api_keys.get_secret_value().strip()
        if not raw:
            return entries
        for item in raw.split(","):
            parts = item.strip().split(":")
            if len(parts) != 3 or not all(parts):
                raise ValueError("FLOWFORGE_API_KEYS entries must look like key:tenant:role")
            key, tenant, role = parts
            if role not in ("user", "admin"):
                raise ValueError(f"unknown role {role!r} in FLOWFORGE_API_KEYS")
            entries[key] = ApiKeyEntry(key=key, tenant=tenant, role=role)  # type: ignore[arg-type]
        return entries

    def handler_module_list(self) -> list[str]:
        return [m.strip() for m in self.handler_modules.split(",") if m.strip()]

    def fault_point_set(self) -> frozenset[str]:
        if not self.enable_fault_injection:
            return frozenset()
        return frozenset(p.strip() for p in self.fault_points.split(",") if p.strip())


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
