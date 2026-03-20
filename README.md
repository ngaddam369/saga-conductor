# saga-conductor

![CI](https://github.com/ngaddam369/saga-conductor/actions/workflows/ci.yml/badge.svg)

A lightweight saga orchestrator for distributed transactions in Go. Define steps with forward and compensating actions — saga-conductor executes them in order and rolls back completed steps automatically on failure.

```
CreateSaga → StartSaga → step-1 → step-2 → step-3
                                       ↓ (fails)
                         comp-1 ← comp-2
```

Single binary. No external database. gRPC API.

## Quick start

```bash
make build
./bin/saga-conductor
```

## Documentation

**[Read the documentation](https://ngaddam369.github.io/saga-conductor/)**
