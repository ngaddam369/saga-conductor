# Getting Started

## Prerequisites

- Go 1.22+ (to build from source)
- `grpcurl` for the CLI walkthrough
- A running saga-conductor instance

## Build and run

```bash
git clone https://github.com/ngaddam369/saga-conductor
cd saga-conductor
make build
./bin/saga-conductor
```

The server starts two listeners:

| Port | Purpose |
|------|---------|
| `:8080` | gRPC API |
| `:8081` | Health, metrics, dashboard, admin |

## Health check

```bash
curl http://localhost:8081/health/live
curl http://localhost:8081/health/ready
```

Both return `200 OK` when the server is healthy.

## Your first saga with grpcurl

### 1. Create a saga

```bash
grpcurl -plaintext \
  -d '{
    "name": "my-first-saga",
    "steps": [
      {
        "name": "step-1",
        "forward_url": "https://httpbin.org/status/200",
        "compensate_url": "https://httpbin.org/status/200"
      },
      {
        "name": "step-2",
        "forward_url": "https://httpbin.org/status/200",
        "compensate_url": "https://httpbin.org/status/200"
      }
    ],
    "payload": ""
  }' \
  localhost:8080 saga.v1.SagaOrchestrator/CreateSaga
```

The response contains the saga in `PENDING` state with an `id` field. Copy it for the next step.

### 2. Start the saga

```bash
grpcurl -plaintext \
  -d '{"saga_id": "<id from step 1>"}' \
  localhost:8080 saga.v1.SagaOrchestrator/StartSaga
```

`StartSaga` blocks until the saga reaches a terminal state (`COMPLETED`, `FAILED`, `COMPENSATION_FAILED`, or `ABORTED`) and returns the final execution snapshot.

### 3. Inspect the result

```bash
grpcurl -plaintext \
  -d '{"saga_id": "<id>"}' \
  localhost:8080 saga.v1.SagaOrchestrator/GetSaga
```

### 4. List all sagas

```bash
grpcurl -plaintext \
  -d '{"page_size": 10}' \
  localhost:8080 saga.v1.SagaOrchestrator/ListSagas
```

Filter by status:

```bash
grpcurl -plaintext \
  -d '{"status": "SAGA_STATUS_COMPLETED", "page_size": 10}' \
  localhost:8080 saga.v1.SagaOrchestrator/ListSagas
```

### 5. Abort a saga

```bash
grpcurl -plaintext \
  -d '{"saga_id": "<id>"}' \
  localhost:8080 saga.v1.SagaOrchestrator/AbortSaga
```

`AbortSaga` immediately marks a non-terminal saga as `ABORTED` without triggering compensation.

## Dashboard

Open `http://localhost:8081/dashboard` in your browser. The page streams live saga state transitions via Server-Sent Events — no page refresh required.

## Payload propagation

The `payload` field of `CreateSaga` is forwarded verbatim as the request body of every forward and compensating HTTP call. Use it to pass context (order ID, user ID, etc.) to downstream services.

## Simulating a failure

Use a `forward_url` that returns a non-2xx status to see compensation in action:

```bash
grpcurl -plaintext \
  -d '{
    "name": "failing-saga",
    "steps": [
      {
        "name": "step-1",
        "forward_url": "https://httpbin.org/status/200",
        "compensate_url": "https://httpbin.org/status/200"
      },
      {
        "name": "step-2",
        "forward_url": "https://httpbin.org/status/500",
        "compensate_url": "https://httpbin.org/status/200",
        "max_retries": 0
      }
    ]
  }' \
  localhost:8080 saga.v1.SagaOrchestrator/CreateSaga
```

After `StartSaga`, the saga transitions: `RUNNING → COMPENSATING → FAILED`. Step-1's `compensate_url` is called automatically.
