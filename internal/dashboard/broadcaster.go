// Package dashboard provides a real-time web dashboard for saga-conductor.
// It implements engine.Observer so it can receive every saga state transition
// from the engine and fan them out to all active browser SSE connections.
package dashboard

import (
	"encoding/json"
	"sync"

	"github.com/ngaddam369/saga-conductor/internal/saga"
)

// sagaEvent is the JSON payload pushed to the browser on every update.
// It carries only what the dashboard needs — no step definitions or raw payload.
type sagaEvent struct {
	ID     string          `json:"id"`
	Name   string          `json:"name"`
	Status saga.SagaStatus `json:"status"`
	Steps  []stepEvent     `json:"steps"`
}

type stepEvent struct {
	Name   string          `json:"name"`
	Status saga.StepStatus `json:"status"`
	Error  string          `json:"error,omitempty"`
}

func toEvent(exec *saga.Execution) sagaEvent {
	steps := make([]stepEvent, len(exec.Steps))
	for i, s := range exec.Steps {
		steps[i] = stepEvent{Name: s.Name, Status: s.Status, Error: s.Error}
	}
	return sagaEvent{ID: exec.ID, Name: exec.Name, Status: exec.Status, Steps: steps}
}

// Broadcaster fans out saga execution updates to all active SSE connections.
// It implements engine.Observer and is safe for concurrent use.
type Broadcaster struct {
	mu   sync.Mutex
	subs map[chan []byte]struct{}
}

// NewBroadcaster returns an initialised Broadcaster ready to use.
func NewBroadcaster() *Broadcaster {
	return &Broadcaster{subs: make(map[chan []byte]struct{})}
}

// OnUpdate implements engine.Observer. It serialises exec to a compact
// sagaEvent JSON payload and publishes it to all active subscribers.
func (b *Broadcaster) OnUpdate(exec *saga.Execution) {
	data, err := json.Marshal(toEvent(exec))
	if err != nil {
		return
	}
	b.publish(data)
}

func (b *Broadcaster) publish(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subs {
		select {
		case ch <- data:
		default:
			// Slow subscriber: drop this event rather than blocking the engine.
		}
	}
}

// subscribe adds a new SSE subscriber and returns a read-only channel of JSON
// payloads together with an unsubscribe function the caller must invoke when
// the connection closes.
func (b *Broadcaster) subscribe() (<-chan []byte, func()) {
	ch := make(chan []byte, 64)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch, func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}
}
