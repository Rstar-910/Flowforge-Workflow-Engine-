# ADR 0009: No additional infrastructure in version one

- Status: accepted

## Decision

The deployment is PostgreSQL plus three process types: the API, the scheduler, and
workers. All run from one Python package. There is no Redis, Kafka, Kubernetes, or
service mesh.

- Metrics: each process exposes a `prometheus_client` endpoint. The API serves
  `/metrics`; the scheduler and workers use `FLOWFORGE_METRICS_PORT`. No Prometheus
  server is bundled.
- Tracing: the code depends only on the OpenTelemetry *API*. The SDK and OTLP exporter
  are an optional extra, activated by `OTEL_EXPORTER_OTLP_ENDPOINT`.
- The submission rate limiter is in-process, per API process and per tenant. That is
  enough for a single-host deployment. A shared limiter would need shared state, for
  example in PostgreSQL or Redis, and is deferred until it is needed.

## Consequences

Any added component must be justified by a measured need recorded in a new ADR.
