"""ASGI entry point for multi-process deployments (``uvicorn flowforge.api.asgi:app``)."""

from flowforge.api.app import create_app
from flowforge.config import get_settings
from flowforge.observability.logging import configure_logging

_settings = get_settings()
configure_logging(f"{_settings.service_name}-api", _settings.log_level, _settings.log_format)
app = create_app(_settings)
