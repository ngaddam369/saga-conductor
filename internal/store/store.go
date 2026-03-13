// Package store defines the persistence interface for saga-conductor.
package store

import (
	"context"
	"time"

	"github.com/ngaddam369/saga-conductor/internal/saga"
)

// Store persists saga executions.
// All implementations must be safe for concurrent use.
type Store interface {
	// Create persists a new saga execution. Returns an error if the ID already
	// exists.
	Create(ctx context.Context, exec *saga.Execution) error

	// Get retrieves a saga execution by ID.
	Get(ctx context.Context, id string) (*saga.Execution, error)

	// Update overwrites an existing saga execution. The caller is responsible
	// for applying state-machine rules before calling Update.
	Update(ctx context.Context, exec *saga.Execution) error

	// List returns all stored saga executions. When status is non-empty only
	// sagas whose status matches are returned.
	List(ctx context.Context, status saga.SagaStatus) ([]*saga.Execution, error)

	// TransitionToRunning atomically checks that the saga is PENDING and, if so,
	// writes it to RUNNING and returns the updated execution. It returns
	// ErrAlreadyRunning if the saga is not PENDING when the write fires.
	TransitionToRunning(ctx context.Context, id string, startedAt time.Time) (*saga.Execution, error)

	// Ping verifies the store is reachable and its bucket is accessible.
	// Used by the liveness probe to detect storage failures.
	Ping(ctx context.Context) error
}
