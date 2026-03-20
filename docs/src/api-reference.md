# API Reference

## gRPC service: `saga.v1.SagaOrchestrator`

Default address: `localhost:8080`

---

### `CreateSaga`

Registers a saga definition. Returns it in `PENDING` state. The saga is not executed until `StartSaga` is called.

**Request: `CreateSagaRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Human-readable saga name. Not unique. |
| `steps` | `[]StepDefinition` | Ordered list of steps. May be empty when using a server-side template. |
| `payload` | `bytes` | Opaque payload forwarded to every forward and compensating URL. |
| `saga_timeout_seconds` | `int32` | Per-saga execution deadline. Overrides `SAGA_TIMEOUT_SECONDS`. Valid range: 1–86400. `0` uses the server default. |
| `idempotency_key` | `string` | Optional deduplication token. Max 256 characters. If a matching key exists within the TTL window, the original saga is returned. |

**Response: `CreateSagaResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `saga` | `SagaExecution` | The created saga in `PENDING` state. |

**Error codes**

| Code | Condition |
|------|-----------|
| `INVALID_ARGUMENT` | Empty name, duplicate step names, invalid step fields, unknown template name. |
| `ALREADY_EXISTS` | (Never returned for idempotency key — duplicate key returns `OK` with the original saga.) |

---

### `StartSaga`

Begins executing a `PENDING` saga. Executes steps synchronously in order; triggers compensation automatically on step failure. **Blocks until the saga reaches a terminal state.**

**Request: `StartSagaRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `saga_id` | `string` | ID of the saga to start. |

**Response: `StartSagaResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `saga` | `SagaExecution` | Final execution snapshot. Status is one of `COMPLETED`, `FAILED`, `COMPENSATION_FAILED`, or `ABORTED`. |

**Error codes**

| Code | Condition |
|------|-----------|
| `NOT_FOUND` | Saga ID does not exist. |
| `FAILED_PRECONDITION` | Saga is not in `PENDING` state. |

---

### `GetSaga`

Retrieves the current state of any saga.

**Request: `GetSagaRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `saga_id` | `string` | ID of the saga to retrieve. |

**Response: `GetSagaResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `saga` | `SagaExecution` | Current execution snapshot. |

**Error codes**

| Code | Condition |
|------|-----------|
| `NOT_FOUND` | Saga ID does not exist. |

---

### `ListSagas`

Returns a page of saga executions, optionally filtered by status.

**Request: `ListSagasRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `status` | `SagaStatus` | Optional filter. `SAGA_STATUS_UNSPECIFIED` (0) returns all sagas. |
| `page_size` | `int32` | Maximum results to return. Clamped to 1–1000. Default 100. |
| `page_token` | `string` | Opaque cursor from the previous response. Empty requests the first page. |

**Response: `ListSagasResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `sagas` | `[]SagaExecution` | Saga executions for this page. |
| `next_page_token` | `string` | Cursor for the next page. Empty when this is the last page. |

---

### `AbortSaga`

Forcibly moves a non-terminal saga to `ABORTED`. No compensation is triggered.

**Request: `AbortSagaRequest`**

| Field | Type | Description |
|-------|------|-------------|
| `saga_id` | `string` | ID of the saga to abort. |

**Response: `AbortSagaResponse`**

| Field | Type | Description |
|-------|------|-------------|
| `saga` | `SagaExecution` | Final snapshot with status `ABORTED`. |

**Error codes**

| Code | Condition |
|------|-----------|
| `NOT_FOUND` | Saga ID does not exist. |
| `FAILED_PRECONDITION` | Saga is already in a terminal state. |

---

## Domain types

### `SagaExecution`

| Field | Type | Description |
|-------|------|-------------|
| `id` | `string` | Unique saga ID (UUID). |
| `name` | `string` | Name supplied at creation. |
| `status` | `SagaStatus` | Current status. |
| `steps` | `[]StepExecution` | Per-step execution state. |
| `payload` | `bytes` | Payload supplied at creation. |
| `failed_step` | `string` | Name of the step that caused failure. Empty unless `FAILED` or `COMPENSATION_FAILED`. |
| `created_at` | `Timestamp` | When the saga was created. |
| `started_at` | `Timestamp` | When execution began. |
| `completed_at` | `Timestamp` | When the saga reached a terminal state. |
| `saga_timeout_seconds` | `int32` | Execution deadline in seconds. Mirrors the value from `CreateSagaRequest`. |

### `StepExecution`

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Step name. |
| `status` | `StepStatus` | Current step status. |
| `error` | `string` | Human-readable error message for failed steps. |
| `started_at` | `Timestamp` | When this step began executing. |
| `completed_at` | `Timestamp` | When this step finished. |
| `error_detail` | `bytes` | JSON-encoded structured error context (HTTP status, response body, duration). Present only when status is `FAILED` or `COMPENSATION_FAILED`. |

### `StepDefinition`

| Field | Type | Description |
|-------|------|-------------|
| `name` | `string` | Unique step name within the saga. |
| `forward_url` | `string` | URL the orchestrator POSTs to when executing the step. |
| `compensate_url` | `string` | URL the orchestrator POSTs to when compensating the step. **(required)** |
| `timeout_seconds` | `int32` | Per-step HTTP timeout. `0` uses the engine default. |
| `max_retries` | `int32` | Maximum HTTP retries. Valid range: 0–100. `0` uses the engine default. |
| `retry_backoff_ms` | `int32` | Base backoff in milliseconds. Valid range: 0–60000. `0` uses the engine default. |
| `auth_type` | `string` | Per-step auth override. One of: `none`, `static`, `jwt`, `oidc`, `svid-exchange`. Empty uses the global default. |
| `auth_config` | `map<string,string>` | Additional auth parameters for the selected `auth_type`. |
| `target_spiffe_id` | `string` | SPIFFE ID of the target service. Used by `svid-exchange` auth. |

### `SagaStatus` values

| Value | Description |
|-------|-------------|
| `SAGA_STATUS_PENDING` | Created, not yet started. |
| `SAGA_STATUS_RUNNING` | Executing forward steps. |
| `SAGA_STATUS_COMPENSATING` | A step failed; running compensation in reverse. |
| `SAGA_STATUS_COMPLETED` | All forward steps succeeded. Terminal. |
| `SAGA_STATUS_FAILED` | All compensations succeeded after a step failure. Terminal. |
| `SAGA_STATUS_COMPENSATION_FAILED` | A compensation step exhausted its retries. Terminal. Requires manual intervention. |
| `SAGA_STATUS_ABORTED` | Aborted via `AbortSaga` or admin endpoint. Terminal. |

### `StepStatus` values

| Value | Description |
|-------|-------------|
| `STEP_STATUS_PENDING` | Not yet started. |
| `STEP_STATUS_RUNNING` | HTTP call in progress. |
| `STEP_STATUS_SUCCEEDED` | Forward call returned 2xx. |
| `STEP_STATUS_FAILED` | Forward call exhausted retries. |
| `STEP_STATUS_COMPENSATING` | Compensation call in progress. |
| `STEP_STATUS_COMPENSATED` | Compensation call returned 2xx. |
| `STEP_STATUS_COMPENSATION_FAILED` | Compensation call exhausted retries. |

---

## HTTP endpoints

All HTTP endpoints are on `HEALTH_ADDR` (default `:8081`).

### Health

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health/live` | Liveness probe. Returns `200` when the store is reachable. |
| `GET` | `/health/ready` | Readiness probe. Returns `200` when the server is ready; `503` during graceful shutdown drain. |

### Metrics

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/metrics` | Prometheus metrics in text exposition format. |

### Admin

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/admin/sagas/{id}/abort` | Forcibly abort a saga. Returns `{"id":"...","status":"ABORTED"}`. Errors: `404` (not found), `409` (already terminal). |

### Dashboard

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/dashboard` | Single-page HTML dashboard. |
| `GET` | `/dashboard/events` | Server-Sent Events stream of saga state transitions. |
