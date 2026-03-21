# YAML Saga Definitions

YAML saga definitions let you declare saga templates in a config file and create sagas by name at runtime — without clients needing to supply step definitions on every `CreateSaga` call.

## Enabling

Set `SAGA_DEFINITIONS_PATH` to the path of a YAML file:

```bash
SAGA_DEFINITIONS_PATH=/etc/saga-conductor/sagas.yaml ./bin/saga-conductor
```

## YAML format

```yaml
sagas:
  - name: order-saga
    saga_timeout_seconds: 300
    steps:
      - name: reserve-inventory
        forward_url: http://inventory-service/api/v1/reservations
        compensate_url: http://inventory-service/api/v1/reservations/release
        timeout_seconds: 10
        max_retries: 3
        retry_backoff_ms: 500

      - name: charge-payment
        forward_url: http://payment-service/api/v1/charges
        compensate_url: http://payment-service/api/v1/charges/refund
        timeout_seconds: 15
        max_retries: 2
        auth_type: oidc

      - name: send-confirmation
        forward_url: http://notification-service/api/v1/emails
        compensate_url: http://notification-service/api/v1/emails/cancel
        timeout_seconds: 5

  - name: payment-saga
    saga_timeout_seconds: 120
    steps:
      - name: validate-payment-method
        forward_url: http://payment-service/api/v1/validate
        compensate_url: http://payment-service/api/v1/validate/undo
        timeout_seconds: 10

      - name: transfer-funds
        forward_url: http://payment-service/api/v1/transfers
        compensate_url: http://payment-service/api/v1/transfers/reverse
        timeout_seconds: 30
        max_retries: 1
        auth_type: svid-exchange
        target_spiffe_id: spiffe://example.org/payment-service
```

An example file is included in the repository at `config/saga.example.yaml`.

## Step fields

All `StepDefinition` fields are supported in YAML using snake_case:

| YAML field | Description |
|------------|-------------|
| `name` | Step name (required, unique within the saga) |
| `forward_url` | URL to POST to when executing the step (required) |
| `compensate_url` | URL to POST to when compensating the step (required) |
| `timeout_seconds` | Per-step HTTP timeout |
| `max_retries` | Maximum HTTP retries |
| `retry_backoff_ms` | Base backoff in milliseconds |
| `auth_type` | Per-step auth override |
| `target_spiffe_id` | SPIFFE ID of the target service (for `svid-exchange` auth) |

## Creating sagas from templates

When a saga is registered in `SAGA_DEFINITIONS_PATH`, clients can create it by name without supplying steps:

```bash
grpcurl -plaintext \
  -d '{"name": "order-saga", "payload": "{\"order_id\":\"ord-42\"}"}' \
  localhost:8080 saga.v1.SagaOrchestrator/CreateSaga
```

With the Go client library:

```go
exec, err := c.CreateSaga(ctx, &pb.CreateSagaRequest{
    Name:    "order-saga",
    Payload: []byte(`{"order_id":"ord-42"}`),
})
```

If the name matches a loaded template and `steps` is empty in the request, the template steps are used. If `steps` is non-empty in the request, the inline steps always take precedence over the template.

If `steps` is empty and the name does not match any loaded template, `CreateSaga` returns `INVALID_ARGUMENT`.

## Validation

The loader validates definitions at startup:

- Each saga must have a non-empty `name`.
- Each saga must have at least one step.
- `saga_timeout_seconds`, if set, must be in [1, 86400].
- Each step must have a non-empty `name`.
- Each step's `name` must match `^[a-zA-Z0-9_-]{1,64}$`.
- Each step must have a non-empty `forward_url`.
- Each step must have a non-empty `compensate_url`.
- Step names must be unique within each saga.
- Each step's `timeout_seconds`, if set, must be in [1, 3600].
- Each step's `max_retries` must be in [0, 100].
- Each step's `retry_backoff_ms` must be in [0, 60000].
- Each step's `auth_type`, if set, must be one of: `none`, `static`, `jwt`, `oidc`, `svid-exchange`.
- Each step's `target_spiffe_id`, if set, must start with `spiffe://`.

Invalid definitions cause the server to exit on startup with a descriptive error message.

## Reloading

Templates are loaded once at startup. To reload a changed definition file, restart the server. In-flight sagas are not affected — they continue with the step definitions captured at creation time.
