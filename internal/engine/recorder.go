package engine

import (
	"github.com/ngaddam369/saga-conductor/internal/saga"
)

// Recorder is an optional hook the Engine calls after each saga or step
// reaches a terminal state. Implementations must be safe for concurrent use.
// A nil Recorder is a no-op — the engine checks before calling.
type Recorder interface {
	// RecordSaga is called when a saga reaches a terminal state.
	// durationSecs is the wall-clock seconds from RUNNING→terminal;
	// it is 0 when StartedAt is unknown (e.g. Abort on a PENDING saga).
	RecordSaga(status saga.SagaStatus, durationSecs float64)

	// RecordStep is called when a step reaches a terminal state.
	// durationSecs is the wall-clock seconds from RUNNING→terminal;
	// it is 0 when the step's StartedAt is unknown.
	RecordStep(status saga.StepStatus, durationSecs float64)
}
