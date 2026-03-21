# Real-Time Dashboard

saga-conductor includes a built-in single-page dashboard that shows live saga state transitions without requiring any page refresh.

## Accessing the dashboard

The dashboard is served by the health HTTP server (default `:8081`):

```
http://localhost:8081/dashboard
```

No authentication is required to access the dashboard. It works on any modern browser.

## What the dashboard shows

The dashboard displays a live table of all sagas. Each row shows:

- Saga ID
- Saga name
- Current status (colour-coded)
- Per-step name and status

The table updates in real time as sagas transition through states. Status colours follow the standard traffic-light convention: green for `COMPLETED`, red for `FAILED` and `COMPENSATION_FAILED`, yellow for `RUNNING` and `COMPENSATING`, grey for `PENDING` and `ABORTED`.

## How it works

The dashboard page opens a [Server-Sent Events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events) connection to `/dashboard/events`. The engine broadcasts every saga state transition as a JSON event over this stream. The browser-side JavaScript receives the event and updates the table immediately.

The SSE endpoint is available directly at:

```
GET /dashboard/events
```

Each event has type `update` and a JSON body containing the full `SagaExecution` snapshot:

```
event: update
data: {"id":"abc123","name":"order-saga","status":"COMPLETED",...}
```

## Production considerations

The SSE connection is a long-lived HTTP connection. Each connected browser holds one connection open. For high-traffic deployments, consider placing an HTTP proxy (nginx, Envoy) in front of the health server and limiting SSE connections from untrusted networks.

The dashboard is read-only. No saga management operations can be performed through it.
