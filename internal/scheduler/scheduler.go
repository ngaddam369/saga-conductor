// Package scheduler recovers non-terminal sagas after a process restart.
// It is intended to be called once at startup, before the gRPC server begins
// accepting traffic, so that sagas interrupted by a crash are driven to a
// terminal state before any new requests arrive.
package scheduler

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/store"
)

// Enginer is the subset of the engine required by the Scheduler.
type Enginer interface {
	Resume(ctx context.Context, id string) (*saga.Execution, error)
}

// Scheduler finds RUNNING and COMPENSATING sagas in the store and re-drives
// each one to a terminal state via the engine's Resume method.
type Scheduler struct {
	store  store.Store
	engine Enginer
}

// New returns a Scheduler backed by the given store and engine.
func New(s store.Store, eng Enginer) *Scheduler {
	return &Scheduler{store: s, engine: eng}
}

// Run lists all RUNNING and COMPENSATING sagas and resumes each one in a
// separate goroutine. It blocks until every resumed saga has reached a
// terminal state or ctx is cancelled. Individual saga errors are logged to
// stderr but do not abort other resumes; a non-nil error is only returned if
// the store itself cannot be queried.
func (s *Scheduler) Run(ctx context.Context) error {
	running, err := s.store.List(ctx, saga.SagaStatusRunning)
	if err != nil {
		return fmt.Errorf("list running sagas: %w", err)
	}
	compensating, err := s.store.List(ctx, saga.SagaStatusCompensating)
	if err != nil {
		return fmt.Errorf("list compensating sagas: %w", err)
	}

	toResume := append(running, compensating...)
	if len(toResume) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	for _, exec := range toResume {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if _, resumeErr := s.engine.Resume(ctx, id); resumeErr != nil {
				fmt.Fprintf(os.Stderr, "scheduler: resume saga %s: %v\n", id, resumeErr)
			}
		}(exec.ID)
	}
	wg.Wait()
	return nil
}
