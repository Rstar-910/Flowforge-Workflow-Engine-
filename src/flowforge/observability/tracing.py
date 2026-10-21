"""OpenTelemetry tracing.

Code uses only the OpenTelemetry *API*, which is a no-op until an SDK is
configured. ``configure_tracing`` installs the SDK with an OTLP/HTTP exporter
when the optional ``otel`` extra is installed and ``OTEL_EXPORTER_OTLP_ENDPOINT``
is set. The W3C ``traceparent`` of the submitting request is stored on the run
so that worker spans join the same trace.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from opentelemetry import propagate, trace

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("flowforge")


def configure_tracing(service_name: str) -> bool:
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return False
    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:
        logger.warning("OTEL endpoint configured but the 'otel' extra is not installed")
        return False
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    return True


def current_traceparent() -> str | None:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier.get("traceparent")


def context_from_traceparent(traceparent: str | None) -> Any:
    if not traceparent:
        return None
    return propagate.extract({"traceparent": traceparent})
