# Configuration

All configuration is via environment variables. Every variable has a default; none are required unless you enable a specific feature.

## Core

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_ADDR` | `:8080` | TCP address for the gRPC listener |
| `HEALTH_ADDR` | `:8081` | TCP address for health, metrics, and dashboard HTTP server |
| `DB_PATH` | `saga-conductor.db` | Path to the bbolt database file |

## gRPC tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_MAX_RECV_MB` | `4` | Maximum inbound message size in MiB |
| `GRPC_MAX_SEND_MB` | `16` | Maximum outbound message size in MiB |
| `GRPC_HANDLER_TIMEOUT_SECONDS` | `60` | Per-RPC handler deadline in seconds. `0` disables the timeout. Does not apply to `StartSaga` (which blocks until terminal). |
| `GRPC_STOP_TIMEOUT_SECONDS` | `30` | Graceful stop fallback: if `GracefulStop` does not complete within this duration, `Stop()` force-closes remaining connections |
| `GRPC_MAX_CONN_IDLE_MINUTES` | `5` | Keepalive: close idle connections after this many minutes |
| `GRPC_KEEPALIVE_TIME_MINUTES` | `2` | Keepalive: server sends pings every N minutes |
| `GRPC_KEEPALIVE_TIMEOUT_SECONDS` | `20` | Keepalive: seconds to wait for a ping acknowledgement |
| `GRPC_KEEPALIVE_MIN_TIME_SECONDS` | `30` | Keepalive enforcement: minimum interval clients may send pings |

## Health HTTP server

| Variable | Default | Description |
|----------|---------|-------------|
| `HEALTH_READ_TIMEOUT_SECONDS` | `5` | HTTP read timeout |
| `HEALTH_WRITE_TIMEOUT_SECONDS` | `5` | HTTP write timeout |
| `HEALTH_IDLE_TIMEOUT_SECONDS` | `60` | HTTP idle connection timeout |

## Saga execution

| Variable | Default | Description |
|----------|---------|-------------|
| `SAGA_TIMEOUT_SECONDS` | none | Server-wide default execution deadline per saga. `0` or unset means no deadline. Individual sagas override this via `saga_timeout_seconds` in `CreateSaga`. |

## Graceful shutdown

| Variable | Default | Description |
|----------|---------|-------------|
| `SHUTDOWN_DRAIN_SECONDS` | `5` | Seconds to wait after flipping readiness to false before closing connections. Set `0` to skip the drain pause. |
| `SHUTDOWN_SAGA_TIMEOUT_SECONDS` | `30` | Seconds to wait for in-flight sagas to finish before force-stopping the gRPC server. Interrupted sagas are resumed by the scheduler on the next startup. |

## Idempotency

| Variable | Default | Description |
|----------|---------|-------------|
| `IDEMPOTENCY_KEY_TTL_HOURS` | `24` | How long `CreateSaga` idempotency keys are retained. Duplicate `CreateSaga` calls with the same key within this window return the original saga. |

## Data retention

| Variable | Default | Description |
|----------|---------|-------------|
| `SAGA_RETENTION_DAYS` | `90` | Delete terminal sagas older than N days. `0` keeps sagas forever. |
| `PURGE_INTERVAL_HOURS` | `24` | How often the background purger runs. |

## Authentication

| Variable | Default | Description |
|----------|---------|-------------|
| `AUTH_TYPE` | `none` | Authentication mode. One of: `none`, `static`, `jwt`, `oidc`, `svid-exchange`. See [Authentication](features/auth.md). |
| `AUTH_STATIC_TOKEN` | — | Required when `AUTH_TYPE=static`. Shared secret token. |
| `AUTH_JWKS_URL` | — | Required when `AUTH_TYPE=jwt`. URL of the JWKS endpoint for token verification. |
| `AUTH_OIDC_TOKEN_URL` | — | Required when `AUTH_TYPE=oidc`. Token endpoint for OAuth2 client credentials flow. |
| `AUTH_OIDC_CLIENT_ID` | — | Required when `AUTH_TYPE=oidc`. OAuth2 client ID. |
| `AUTH_OIDC_CLIENT_SECRET` | — | Required when `AUTH_TYPE=oidc`. OAuth2 client secret. |
| `AUTH_OIDC_SCOPES` | — | Optional. Space-separated list of OAuth2 scopes to request. |
| `AUTH_SVID_EXCHANGE_ADDR` | — | Required when `AUTH_TYPE=svid-exchange`. Address of the svid-exchange service. |

## mTLS

| Variable | Default | Description |
|----------|---------|-------------|
| `SPIFFE_ENDPOINT_SOCKET` | — | Path to the SPIFFE workload API socket. When set, the gRPC listener is upgraded to mTLS using the server's X.509 SVID. |

## Observability

| Variable | Default | Description |
|----------|---------|-------------|
| `OTLP_ENDPOINT` | — | OTLP gRPC endpoint for OpenTelemetry traces (e.g. `localhost:4317`). When unset, traces are dropped. See [Distributed Tracing](features/distributed-tracing.md). |

## YAML saga definitions

| Variable | Default | Description |
|----------|---------|-------------|
| `SAGA_DEFINITIONS_PATH` | — | Path to a YAML file containing saga templates. When set, clients can create sagas by name without supplying step definitions. See [YAML Saga Definitions](features/yaml-definitions.md). |
