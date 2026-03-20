# Features

saga-conductor ships with several opt-in features. All are disabled by default and activated through environment variables.

| Feature | How to enable |
|---------|--------------|
| [Authentication](auth.md) | Set `AUTH_TYPE` to any value other than `none` |
| [Prometheus Metrics](prometheus-metrics.md) | Scraped automatically at `GET /metrics` |
| [Distributed Tracing](distributed-tracing.md) | Set `OTLP_ENDPOINT` |
| [Real-Time Dashboard](dashboard.md) | Built-in; open `/dashboard` in a browser |
| [YAML Saga Definitions](yaml-definitions.md) | Set `SAGA_DEFINITIONS_PATH` |
