// Package scheduler recovers non-terminal sagas after a process restart.
// It is intended to be called once at startup, before the gRPC server begins
// accepting traffic, so that sagas interrupted by a crash are driven to a
// terminal state before any new requests arrive.
package scheduler

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"

	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/store"
)

// Engine is the subset of the engine required by the Scheduler.
type Engine interface {
	Resume(ctx context.Context, id string) (*saga.Execution, error)
}

// Scheduler finds RUNNING and COMPENSATING sagas in the store and re-drives
// each one to a terminal state via the engine's Resume method.
type Scheduler struct {
	store  store.Store
	engine Engine
	log    zerolog.Logger
}

// New returns a Scheduler backed by the given store and engine.
func New(s store.Store, eng Engine, log zerolog.Logger) *Scheduler {
	return &Scheduler{store: s, engine: eng, log: log}
}

// Run lists all RUNNING and COMPENSATING sagas and resumes each one in a
// separate goroutine. It blocks until every resumed saga has reached a
// terminal state or ctx is cancelled. Individual saga errors are logged via
// the structured logger but do not abort other resumes; a non-nil error is
// only returned if the store itself cannot be queried.
func (s *Scheduler) Run(ctx context.Context) error {
	running, _, err := s.store.List(ctx, saga.SagaStatusRunning, 0, "")
	if err != nil {
		return fmt.Errorf("list running sagas: %w", err)
	}
	compensating, _, err := s.store.List(ctx, saga.SagaStatusCompensating, 0, "")
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
				s.log.Error().Err(resumeErr).Str("saga_id", id).Msg("scheduler: resume saga failed")
			}
		}(exec.ID)
	}
	wg.Wait()
	return nil
}
