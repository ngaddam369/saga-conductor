// Package purger implements background saga data retention for saga-conductor.
// It periodically scans for terminal sagas (COMPLETED or FAILED) older than a
// configurable retention window and deletes them from the store.
package purger

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ngaddam369/saga-conductor/internal/saga"
)

const (
	defaultRetentionDays = 90
	defaultIntervalHours = 24
)

// Store is the subset of store.Store that the purger requires.
type Store interface {
	List(ctx context.Context, status saga.SagaStatus) ([]*saga.Execution, error)
	Delete(ctx context.Context, id string) error
}

// Purger deletes expired terminal sagas on a periodic schedule.
type Purger struct {
	store         Store
	retentionDays int
	intervalHours int
}

// New reads SAGA_RETENTION_DAYS (default 90; 0 = keep forever) and
// PURGE_INTERVAL_HOURS (default 24) from the environment and returns a ready
// Purger. If retentionDays is 0 the Purger is a no-op.
func New(s Store) *Purger {
	retentionDays := defaultRetentionDays
	if v := os.Getenv("SAGA_RETENTION_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			retentionDays = n
		}
	}

	intervalHours := defaultIntervalHours
	if v := os.Getenv("PURGE_INTERVAL_HOURS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			intervalHours = n
		}
	}

	return &Purger{
		store:         s,
		retentionDays: retentionDays,
		intervalHours: intervalHours,
	}
}

// NewWithConfig constructs a Purger with explicit configuration, bypassing
// environment variable parsing. Used by tests.
func NewWithConfig(s Store, retentionDays, intervalHours int) *Purger {
	return &Purger{
		store:         s,
		retentionDays: retentionDays,
		intervalHours: intervalHours,
	}
}

// Run starts the background purge loop. It runs an initial purge immediately,
// then repeats every intervalHours. Run blocks until ctx is cancelled.
// If retentionDays is 0 (keep forever), Run returns immediately.
// Errors are logged to stderr; Run never stops due to individual purge failures.
func (p *Purger) Run(ctx context.Context) {
	if p.retentionDays == 0 {
		return
	}

	p.runOnce(ctx)

	ticker := time.NewTicker(time.Duration(p.intervalHours) * time.Hour)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.runOnce(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// runOnce executes a single purge cycle, logging any errors to stderr.
func (p *Purger) runOnce(ctx context.Context) {
	if _, err := p.Purge(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "purger: purge cycle error: %v\n", err)
	}
}

// Purge deletes all terminal sagas older than retentionDays. It returns the
// number of sagas deleted and the first error encountered (if any). Partial
// success is possible: some sagas may be deleted even when an error is returned.
// If retentionDays is 0 (keep forever) Purge is a no-op.
func (p *Purger) Purge(ctx context.Context) (int, error) {
	if p.retentionDays == 0 {
		return 0, nil
	}

	cutoff := time.Now().UTC().AddDate(0, 0, -p.retentionDays)

	var deleted int
	var firstErr error

	for _, status := range []saga.SagaStatus{saga.SagaStatusCompleted, saga.SagaStatusFailed} {
		execs, err := p.store.List(ctx, status)
		if err != nil {
			fmt.Fprintf(os.Stderr, "purger: list %s sagas: %v\n", status, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		for _, exec := range execs {
			if exec.CreatedAt.Before(cutoff) {
				if err := p.store.Delete(ctx, exec.ID); err != nil {
					fmt.Fprintf(os.Stderr, "purger: delete saga %s: %v\n", exec.ID, err)
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				deleted++
			}
		}
	}

	return deleted, firstErr
}
