# Client Library

`pkg/client` provides a Go client for the saga-conductor gRPC API. It wraps connection management and exposes a clean API over the five `SagaOrchestrator` RPCs so callers do not need to import or manage gRPC plumbing directly.

## Installation

```bash
go get github.com/ngaddam369/saga-conductor
```

## Creating a client

```go
import (
    "github.com/ngaddam369/saga-conductor/pkg/client"
)

c, err := client.New("localhost:8080", client.WithInsecure())
if err != nil {
    log.Fatal(err)
}
defer c.Close()
```

## Options

### `WithInsecure()`

Disables transport security. Use for local development and testing only.

```go
c, err := client.New("localhost:8080", client.WithInsecure())
```

### `WithTransportCredentials(creds)`

Sets custom TLS or mTLS credentials for production deployments.

```go
tlsCreds, err := credentials.NewClientTLSFromFile("ca.crt", "")
c, err := client.New("conductor.internal:8080",
    client.WithTransportCredentials(tlsCreds),
)
```

### `WithToken(token)`

Injects a static `Authorization: Bearer <token>` header on every outbound RPC. Matches `AUTH_TYPE=static` on the server.

```go
c, err := client.New("localhost:8080",
    client.WithInsecure(),
    client.WithToken("my-secret-token"),
)
```

### `WithDialOptions(opts...)`

Escape hatch for raw gRPC dial options not covered by the other helpers (keepalive parameters, custom interceptors, etc.).

```go
c, err := client.New("localhost:8080",
    client.WithInsecure(),
    client.WithDialOptions(
        grpc.WithKeepaliveParams(keepalive.ClientParameters{
            Time:    30 * time.Second,
            Timeout: 10 * time.Second,
        }),
    ),
)
```

## Methods

### `CreateSaga`

Registers a new saga and returns it in `PENDING` state. The saga is not executed until `StartSaga` is called.

```go
exec, err := c.CreateSaga(ctx, &pb.CreateSagaRequest{
    Name: "order-saga",
    Steps: []*pb.StepDefinition{
        {
            Name:         "reserve-inventory",
            ForwardUrl:   "http://inventory-service/api/v1/reservations",
            CompensateUrl: "http://inventory-service/api/v1/reservations/release",
            TimeoutSeconds: 10,
            MaxRetries:   3,
        },
        {
            Name:         "charge-payment",
            ForwardUrl:   "http://payment-service/api/v1/charges",
            CompensateUrl: "http://payment-service/api/v1/charges/refund",
            TimeoutSeconds: 15,
        },
    },
    Payload:          []byte(`{"order_id":"ord-123"}`),
    IdempotencyKey:   "ord-123-create",  // optional deduplication key
})
```

When `SAGA_DEFINITIONS_PATH` is configured on the server, you can omit `Steps` and create a saga by name alone:

```go
exec, err := c.CreateSaga(ctx, &pb.CreateSagaRequest{
    Name: "order-saga",
    Payload: []byte(`{"order_id":"ord-123"}`),
})
```

### `StartSaga`

Begins executing the saga. Blocks until the saga reaches a terminal state and returns the final snapshot.

```go
exec, err := c.StartSaga(ctx, exec.Id)
if err != nil {
    return err
}
switch exec.Status {
case pb.SagaStatus_SAGA_STATUS_COMPLETED:
    fmt.Println("success")
case pb.SagaStatus_SAGA_STATUS_FAILED:
    fmt.Printf("failed at step %s\n", exec.FailedStep)
case pb.SagaStatus_SAGA_STATUS_COMPENSATION_FAILED:
    fmt.Println("compensation failed — manual intervention required")
}
```

### `GetSaga`

Retrieves the current state of a saga by ID.

```go
exec, err := c.GetSaga(ctx, sagaID)
```

### `ListSagas`

Returns a page of saga executions with optional status filter.

```go
// First page
sagas, nextToken, err := c.ListSagas(ctx, &pb.ListSagasRequest{
    PageSize: 50,
})

// Next page
sagas, nextToken, err = c.ListSagas(ctx, &pb.ListSagasRequest{
    PageSize:  50,
    PageToken: nextToken,
})

// Filter by status
sagas, _, err = c.ListSagas(ctx, &pb.ListSagasRequest{
    Status:   pb.SagaStatus_SAGA_STATUS_FAILED,
    PageSize: 100,
})
```

### `AbortSaga`

Forcibly moves a non-terminal saga to `ABORTED`. No compensation is triggered.

```go
exec, err := c.AbortSaga(ctx, sagaID)
```

Returns an error if the saga does not exist or is already in a terminal state.

## Full example

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ngaddam369/saga-conductor/pkg/client"
    pb "github.com/ngaddam369/saga-conductor/proto/saga/v1"
)

func main() {
    c, err := client.New("localhost:8080", client.WithInsecure())
    if err != nil {
        log.Fatal(err)
    }
    defer c.Close()

    ctx := context.Background()

    exec, err := c.CreateSaga(ctx, &pb.CreateSagaRequest{
        Name: "order-saga",
        Steps: []*pb.StepDefinition{
            {
                Name:          "reserve-inventory",
                ForwardUrl:    "http://inventory/reservations",
                CompensateUrl: "http://inventory/reservations/release",
            },
        },
        Payload: []byte(`{"order_id":"ord-42"}`),
    })
    if err != nil {
        log.Fatal(err)
    }

    result, err := c.StartSaga(ctx, exec.Id)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("saga %s finished with status %s\n", result.Id, result.Status)
}
```

## Thread safety

`Client` is safe for concurrent use across goroutines. A single client instance can be shared across the lifetime of an application.
