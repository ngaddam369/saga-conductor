# Prometheus Metrics

Metrics are exposed at `GET /metrics` on the health HTTP server (default `:8081`) in Prometheus text exposition format. They are always available — no additional configuration is required.

## Metric families

### `saga_conductor_saga_total`

**Type**: Counter
**Labels**: `status`

Total number of sagas that reached a terminal state. The `status` label holds the saga status string.

| `status` label value | Condition |
|---------------------|-----------|
| `COMPLETED` | All forward steps succeeded |
| `FAILED` | Compensation succeeded after a step failure |
| `COMPENSATION_FAILED` | A compensating step exhausted its retries |
| `ABORTED` | Saga was forcibly aborted |

**Example**:
```
saga_conductor_saga_total{status="COMPLETED"} 142
saga_conductor_saga_total{status="FAILED"} 7
saga_conductor_saga_total{status="COMPENSATION_FAILED"} 1
saga_conductor_saga_total{status="ABORTED"} 3
```

---

### `saga_conductor_saga_duration_seconds`

**Type**: Histogram
**Buckets**: Prometheus default (`.005`, `.01`, `.025`, `.05`, `.1`, `.25`, `.5`, `1`, `2.5`, `5`, `10`)

Wall-clock duration of sagas from entering `RUNNING` state to reaching a terminal state.

---

### `saga_conductor_step_total`

**Type**: Counter
**Labels**: `status`

Total number of steps that reached a terminal state.

| `status` label value | Condition |
|---------------------|-----------|
| `SUCCEEDED` | Forward call returned 2xx |
| `FAILED` | Forward call exhausted retries |
| `COMPENSATED` | Compensation call returned 2xx |
| `COMPENSATION_FAILED` | Compensation call exhausted retries |

---

### `saga_conductor_step_duration_seconds`

**Type**: Histogram
**Buckets**: Prometheus default

Wall-clock duration of individual steps from entering `RUNNING` state to terminal.

---

## Example Prometheus queries

**Saga success rate (5-minute window)**:
```promql
rate(saga_conductor_saga_total{status="COMPLETED"}[5m])
/
rate(saga_conductor_saga_total[5m])
```

**Step failure rate**:
```promql
rate(saga_conductor_step_total{status="FAILED"}[5m])
```

**P99 saga duration**:
```promql
histogram_quantile(0.99, rate(saga_conductor_saga_duration_seconds_bucket[5m]))
```

**P95 step duration**:
```promql
histogram_quantile(0.95, rate(saga_conductor_step_duration_seconds_bucket[5m]))
```

**Compensation failures (any in the last hour)**:
```promql
increase(saga_conductor_saga_total{status="COMPENSATION_FAILED"}[1h])
```

## Scrape configuration

```yaml
scrape_configs:
  - job_name: saga-conductor
    static_configs:
      - targets: ["localhost:8081"]
```
