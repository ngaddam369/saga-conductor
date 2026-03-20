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

	// List returns stored saga executions, optionally filtered by status and
	// paginated. pageSize=0 means no limit (returns all matching items).
	// pageToken is the ID of the last item returned by the previous call; empty
	// means start from the beginning. Returns the next page token (empty when
	// this is the last page).
	List(ctx context.Context, status saga.SagaStatus, pageSize int, pageToken string) ([]*saga.Execution, string, error)

	// TransitionToRunning atomically checks that the saga is PENDING and, if so,
	// writes it to RUNNING and returns the updated execution. It returns
	// ErrAlreadyRunning if the saga is not PENDING when the write fires.
	TransitionToRunning(ctx context.Context, id string, startedAt time.Time) (*saga.Execution, error)

	// GetOrCreateWithKey atomically creates exec and records the idempotency
	// key, or — if key was seen within its TTL — returns the existing saga
	// without creating a new one. Expired keys are transparently overwritten.
	GetOrCreateWithKey(ctx context.Context, key string, expiresAt time.Time, exec *saga.Execution) (*saga.Execution, error)

	// Delete removes a saga execution by ID. Returns ErrNotFound if the ID
	// does not exist.
	Delete(ctx context.Context, id string) error

	// Ping verifies the store is reachable and its bucket is accessible.
	// Used by the liveness probe to detect storage failures.
	Ping(ctx context.Context) error
}
