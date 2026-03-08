# saga-conductor

![CI](https://github.com/ngaddam369/saga-conductor/actions/workflows/ci.yml/badge.svg)

A lightweight saga orchestrator for distributed transactions in Go. Define a sequence of steps — each with a forward action and a compensating action — and saga-conductor executes them in order, automatically rolling back completed steps in reverse if any step fails.

```
CreateSaga → StartSaga → step-1 → step-2 → step-3
                                       ↓ (fails)
                         comp-1 ← comp-2
```

Single binary. No external database.

## Quick start

```bash
make build
./bin/saga-conductor
```

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `GRPC_ADDR` | `:8080` | gRPC listen address |
| `HEALTH_ADDR` | `:8081` | Health HTTP listen address |
| `DB_PATH` | `saga-conductor.db` | Database file path |

## Development

```bash
make verify   # build → lint → test
make test     # race-detector tests + coverage
```
