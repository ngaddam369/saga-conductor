# Distributed Tracing

saga-conductor emits OpenTelemetry traces for every saga execution. Traces are exported via OTLP (OpenTelemetry Protocol) over gRPC.

## Configuration

Set `OTLP_ENDPOINT` to the address of your OTLP collector:

```bash
OTLP_ENDPOINT=localhost:4317
```

When unset, traces are recorded in a no-op provider and discarded.

## Span structure

Each `StartSaga` call produces a root span named `saga.<saga-name>`. Child spans are created for each step:

```
saga.order-saga
├── step.reserve-inventory
└── step.charge-payment
```

All spans carry these attributes:

| Attribute | Value |
|-----------|-------|
| `saga.id` | Saga UUID |
| `saga.name` | Saga name |
| `step.name` | Step name (step spans only) |
| `step.forward_url` | Forward URL (step spans only) |

Spans that fail set the OpenTelemetry span status to `ERROR` with the error message.

## W3C Trace Context propagation

Outbound HTTP calls to step services carry a `traceparent` header following the [W3C Trace Context](https://www.w3.org/TR/trace-context/) specification. Downstream services that are also instrumented with OpenTelemetry will automatically attach their spans as children of the step span, producing a full distributed trace across service boundaries.

## Inbound gRPC instrumentation

gRPC calls to saga-conductor are automatically instrumented via `otelgrpc.NewServerHandler()`. Traces initiated by clients that propagate `grpc-trace-bin` or `traceparent` metadata will be continued inside the server.

## Local development with Jaeger

Start a local Jaeger instance with OTLP support:

```bash
docker run --rm -it \
  -p 16686:16686 \
  -p 4317:4317 \
  jaegertracing/all-in-one:latest \
  --collector.otlp.enabled=true
```

Run saga-conductor with the Jaeger endpoint:

```bash
OTLP_ENDPOINT=localhost:4317 ./bin/saga-conductor
```

Open `http://localhost:16686` to browse traces. Execute a saga and search for the service `saga-conductor` to see the full trace.
