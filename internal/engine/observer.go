package engine

import "github.com/ngaddam369/saga-conductor/internal/saga"

// Observer is an optional hook the Engine calls after every saga state
// transition, including intermediate steps. Implementations must be safe for
// concurrent use. A nil Observer is a no-op — the engine checks before calling.
//
// Unlike Recorder (which fires only on terminal transitions), Observer fires on
// every transition so streaming UIs and dashboards receive real-time updates.
type Observer interface {
	OnUpdate(exec *saga.Execution)
}
