# Authentication

saga-conductor supports two independent authentication concerns:

1. **Inbound auth** — validating tokens on incoming gRPC calls from clients.
2. **Outbound auth** — attaching tokens to outgoing HTTP calls to downstream step services.

Both are controlled by `AUTH_TYPE`.

## AUTH_TYPE modes

### `none` (default)

No authentication. All inbound gRPC calls are accepted without validation. No `Authorization` header is added to outbound step calls.

```bash
# Default — nothing to configure.
```

---

### `static`

Inbound: validates that every gRPC call carries `Authorization: Bearer <token>`. Outbound: attaches the same token to every step HTTP call.

```bash
AUTH_TYPE=static
AUTH_STATIC_TOKEN=my-secret-token
```

Clients must set the `Authorization` metadata header:

```bash
grpcurl -plaintext \
  -H "Authorization: Bearer my-secret-token" \
  -d '{"saga_id":"..."}' \
  localhost:8080 saga.v1.SagaOrchestrator/GetSaga
```

With the Go client library:

```go
c, err := client.New("localhost:8080",
    client.WithInsecure(),
    client.WithToken("my-secret-token"),
)
```

---

### `jwt`

Inbound: validates JWT bearer tokens using a JWKS endpoint. Outbound: no token added (JWT mode is inbound-only).

```bash
AUTH_TYPE=jwt
AUTH_JWKS_URL=https://my-idp.example.com/.well-known/jwks.json
```

The validator fetches keys from the JWKS URL and caches them. Token expiry and signature are verified on every call.

---

### `oidc`

Outbound: fetches a short-lived access token from an OAuth2 token endpoint using the client credentials flow, and attaches it as `Authorization: Bearer` on every outgoing step HTTP call. Tokens are cached until 30 seconds before expiry.

Inbound: no validation (use `jwt` for inbound validation alongside `oidc` for outbound via per-step overrides).

```bash
AUTH_TYPE=oidc
AUTH_OIDC_TOKEN_URL=https://auth.example.com/oauth/token
AUTH_OIDC_CLIENT_ID=saga-conductor
AUTH_OIDC_CLIENT_SECRET=secret
AUTH_OIDC_SCOPES=read:orders write:payments   # optional, space-separated
```

---

### `svid-exchange`

Outbound: obtains a scoped JWT from the [svid-exchange](https://github.com/ngaddam369/svid-exchange) service for each downstream step. The JWT audience is the step's `target_spiffe_id`. Uses the SPIFFE workload API to authenticate to the svid-exchange service with the server's own X.509 SVID.

```bash
AUTH_TYPE=svid-exchange
AUTH_SVID_EXCHANGE_ADDR=svid-exchange.internal:8080
SPIFFE_ENDPOINT_SOCKET=/run/spiffe/workload.sock
```

Each step that should receive an audience-scoped JWT must set `target_spiffe_id`:

```proto
StepDefinition {
  name: "transfer-funds"
  forward_url: "http://payment-service/api/v1/transfers"
  compensate_url: "http://payment-service/api/v1/transfers/reverse"
  target_spiffe_id: "spiffe://example.org/payment-service"
}
```

---

## Per-step auth overrides

Each step can override the global `AUTH_TYPE` using the `auth_type` field. This lets different steps use different auth mechanisms within the same saga.

```proto
StepDefinition {
  name: "charge-payment"
  forward_url: "http://payment-service/api/v1/charges"
  compensate_url: "http://payment-service/api/v1/charges/refund"
  auth_type: "oidc"      // override: use OIDC for this step only
}
```

Valid values for `auth_type`: `none`, `static`, `jwt`, `oidc`, `svid-exchange`. Empty string uses the global `AUTH_TYPE`.

Per-step `auth_config` can supply additional parameters keyed by the auth type implementation (e.g., a custom token URL for a specific step's OIDC flow).

---

## mTLS on the gRPC listener

Setting `SPIFFE_ENDPOINT_SOCKET` upgrades the gRPC listener to mutual TLS. The server obtains its X.509 SVID from the SPIFFE workload API and presents it to clients. Clients must also present a valid SVID to connect.

```bash
SPIFFE_ENDPOINT_SOCKET=/run/spiffe/workload.sock
```

mTLS and `AUTH_TYPE` are independent: you can use mTLS transport security with any `AUTH_TYPE` for application-level auth.
