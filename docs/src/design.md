# Design & Motivation

## Why orchestration, not choreography

Sagas can be implemented two ways: choreography (each service reacts to events) or orchestration (a central coordinator drives the sequence). Both are valid. Orchestration wins for saga-conductor's target use case because:

- **Visibility**: the coordinator knows the full state of every saga at all times. Debugging a choreographed saga means correlating events across multiple services and queues.
- **Predictable compensation**: when a step fails the coordinator knows exactly which prior steps succeeded and calls their compensating endpoints in reverse order. With choreography each service must emit a failure event and rely on all other services to react correctly.
- **Simpler services**: downstream services expose plain HTTP endpoints. They do not need to publish or consume events, implement state machines, or know about other services in the saga.

## Why a single binary with bbolt

The standard advice for stateful services is to push state into a dedicated database and keep the application layer stateless. That advice is correct at scale. At saga-conductor's target scale — hundreds to low thousands of concurrent sagas on a single machine — the operational cost of an external database is rarely worth it.

bbolt (bolt) is an embedded key-value store backed by a memory-mapped file. It provides:

- **Crash safety**: every write is fsync'd before returning. A crashed process restarts with a fully consistent store.
- **Zero operational overhead**: no separate process, no network hop, no credentials to rotate, no backup agent to run.
- **Sufficient throughput**: bbolt can sustain tens of thousands of write transactions per second on commodity hardware — well above what a saga orchestrator typically needs.

The tradeoff is horizontal scalability: a single bbolt file cannot be shared across processes. If you outgrow a single machine, you need a different tool. saga-conductor is designed to be that tool _before_ you need to make that jump.

## Why not Temporal

[Temporal](https://temporal.io) is a mature, battle-tested workflow engine. If you need deterministic workflow replays, versioning, child workflows, signals, or multi-tenant namespaces — use Temporal.

saga-conductor exists for teams that need reliable distributed transactions but not the operational complexity of a Temporal cluster. The deployment model is intentionally simple: one binary, one file, no external dependencies.

## Idempotency at the boundary

`CreateSaga` accepts an optional `idempotency_key`. When a client retries a `CreateSaga` call after a network timeout it cannot know whether the original call succeeded. Without an idempotency key, a retry creates a duplicate saga. With the key, the server returns the original saga if the key was seen within the TTL window (default 24 h).

Downstream services should also be idempotent: saga-conductor may retry a step's `forward_url` on transient failures, and on restart after a crash it re-drives any saga that was in-flight. The engine makes a best-effort attempt to avoid duplicate calls on crash recovery but cannot guarantee exactly-once delivery.

## Retry design

Each step retries independently up to `max_retries` times (default 3) with exponential backoff and full jitter:

```
wait = random(0, base_backoff_ms * 2^attempt)
```

Full jitter prevents the thundering-herd problem when multiple sagas are retrying simultaneously against the same downstream service.

Once a step exhausts its retries, compensation begins immediately — earlier steps are compensated in reverse order. There is no overall retry budget shared across steps.

## Compensation design

Compensation runs in reverse order: if steps 1, 2, 3 execute and step 3 fails, then `compensate_url` is called for step 2 then step 1. Step 3's compensation is not called because step 3 never succeeded.

Compensating calls are also retried with the same backoff policy as forward calls. If a compensating call exhausts its retries the saga transitions to `COMPENSATION_FAILED` — a dead-letter state indicating the system may be partially compensated and requires manual intervention.

## Crash recovery

The scheduler runs once at startup. It scans the store for sagas in `RUNNING` or `COMPENSATING` state and re-submits them to the engine. This handles the common crash scenario cleanly: the engine picks up where it left off.

The one subtlety is that a step that was mid-execution when the crash occurred will be retried from the beginning. This means a forward step's `forward_url` may receive a duplicate call. Downstream services should treat forward requests as idempotent.

## Graceful shutdown

On `SIGTERM`, the server:

1. Flips `/health/ready` to 503 so the load balancer stops routing new requests.
2. Waits `SHUTDOWN_DRAIN_SECONDS` (default 5) for in-flight connections to complete naturally.
3. Waits up to `SHUTDOWN_SAGA_TIMEOUT_SECONDS` (default 30) for any executing sagas to finish.
4. Calls `GracefulStop` on the gRPC server, with a hard `GRPC_STOP_TIMEOUT_SECONDS` fallback.

Any sagas still in-flight after the timeout are logged and will be resumed by the scheduler on the next startup.
