package purger_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ngaddam369/saga-conductor/internal/purger"
	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/store"
)

func newTestStore(t *testing.T) *store.BoltStore {
	t.Helper()
	s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func createExec(t *testing.T, s *store.BoltStore, id string, status saga.SagaStatus, age time.Duration) {
	t.Helper()
	exec := &saga.Execution{
		ID:        id,
		Name:      id,
		Status:    status,
		CreatedAt: time.Now().UTC().Add(-age),
		Steps:     []saga.StepExecution{},
		StepDefs:  []saga.StepDefinition{},
	}
	if err := s.Create(context.Background(), exec); err != nil {
		t.Fatalf("Create %s: %v", id, err)
	}
}

func TestPurger(t *testing.T) {
	t.Run("ZeroRetention_KeepsAll", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		createExec(t, s, "old-completed", saga.SagaStatusCompleted, 200*24*time.Hour)
		createExec(t, s, "old-failed", saga.SagaStatusFailed, 200*24*time.Hour)

		p := purger.NewWithConfig(s, 0, 24, zerolog.Nop())
		n, err := p.Purge(context.Background())
		if err != nil {
			t.Fatalf("Purge: %v", err)
		}
		if n != 0 {
			t.Errorf("deleted %d sagas, want 0 (retention disabled)", n)
		}

		// Verify both still exist.
		for _, id := range []string{"old-completed", "old-failed"} {
			if _, err := s.Get(context.Background(), id); err != nil {
				t.Errorf("saga %s should still exist: %v", id, err)
			}
		}
	})

	t.Run("DeletesExpiredTerminalSagas", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		createExec(t, s, "old-completed", saga.SagaStatusCompleted, 100*24*time.Hour)
		createExec(t, s, "old-failed", saga.SagaStatusFailed, 100*24*time.Hour)

		p := purger.NewWithConfig(s, 90, 24, zerolog.Nop()) // 90-day retention
		n, err := p.Purge(context.Background())
		if err != nil {
			t.Fatalf("Purge: %v", err)
		}
		if n != 2 {
			t.Errorf("deleted %d sagas, want 2", n)
		}

		for _, id := range []string{"old-completed", "old-failed"} {
			if _, err := s.Get(context.Background(), id); err == nil {
				t.Errorf("saga %s should have been deleted", id)
			}
		}
	})

	t.Run("KeepsRecentSagas", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		createExec(t, s, "recent-completed", saga.SagaStatusCompleted, 10*24*time.Hour)
		createExec(t, s, "recent-failed", saga.SagaStatusFailed, 10*24*time.Hour)

		p := purger.NewWithConfig(s, 90, 24, zerolog.Nop())
		n, err := p.Purge(context.Background())
		if err != nil {
			t.Fatalf("Purge: %v", err)
		}
		if n != 0 {
			t.Errorf("deleted %d sagas, want 0 (recent sagas should be kept)", n)
		}

		for _, id := range []string{"recent-completed", "recent-failed"} {
			if _, err := s.Get(context.Background(), id); err != nil {
				t.Errorf("saga %s should still exist: %v", id, err)
			}
		}
	})

	t.Run("KeepsNonTerminalSagas", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		createExec(t, s, "old-pending", saga.SagaStatusPending, 200*24*time.Hour)
		createExec(t, s, "old-running", saga.SagaStatusRunning, 200*24*time.Hour)
		createExec(t, s, "old-compensating", saga.SagaStatusCompensating, 200*24*time.Hour)

		p := purger.NewWithConfig(s, 90, 24, zerolog.Nop())
		n, err := p.Purge(context.Background())
		if err != nil {
			t.Fatalf("Purge: %v", err)
		}
		if n != 0 {
			t.Errorf("deleted %d sagas, want 0 (non-terminal sagas must not be purged)", n)
		}

		for _, id := range []string{"old-pending", "old-running", "old-compensating"} {
			if _, err := s.Get(context.Background(), id); err != nil {
				t.Errorf("non-terminal saga %s should still exist: %v", id, err)
			}
		}
	})

	t.Run("MixedExpiredAndRecent", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)
		createExec(t, s, "expired", saga.SagaStatusCompleted, 100*24*time.Hour)
		createExec(t, s, "recent", saga.SagaStatusCompleted, 5*24*time.Hour)
		createExec(t, s, "non-terminal", saga.SagaStatusRunning, 200*24*time.Hour)

		p := purger.NewWithConfig(s, 90, 24, zerolog.Nop())
		n, err := p.Purge(context.Background())
		if err != nil {
			t.Fatalf("Purge: %v", err)
		}
		if n != 1 {
			t.Errorf("deleted %d sagas, want 1", n)
		}

		if _, err := s.Get(context.Background(), "expired"); err == nil {
			t.Error("expired saga should have been deleted")
		}
		for _, id := range []string{"recent", "non-terminal"} {
			if _, err := s.Get(context.Background(), id); err != nil {
				t.Errorf("saga %s should still exist: %v", id, err)
			}
		}
	})

	t.Run("EnvVar_RetentionDays", func(t *testing.T) {
		t.Setenv("SAGA_RETENTION_DAYS", "30")
		t.Setenv("PURGE_INTERVAL_HOURS", "1")

		s := newTestStore(t)
		// Old enough to be purged under 30-day retention.
		createExec(t, s, "old", saga.SagaStatusFailed, 40*24*time.Hour)
		// Too recent for 30-day retention.
		createExec(t, s, "recent", saga.SagaStatusFailed, 20*24*time.Hour)

		p := purger.New(s, zerolog.Nop())
		n, err := p.Purge(context.Background())
		if err != nil {
			t.Fatalf("Purge: %v", err)
		}
		if n != 1 {
			t.Errorf("deleted %d, want 1", n)
		}
		if _, err := s.Get(context.Background(), "old"); err == nil {
			t.Error("old saga should have been purged")
		}
		if _, err := s.Get(context.Background(), "recent"); err != nil {
			t.Errorf("recent saga should still exist: %v", err)
		}
	})

	t.Run("Run_StopsOnContextCancel", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		ctx, cancel := context.WithCancel(context.Background())
		p := purger.NewWithConfig(s, 90, 1000, zerolog.Nop()) // 1000-hour interval — won't tick in test

		done := make(chan struct{})
		go func() {
			p.Run(ctx)
			close(done)
		}()

		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Run did not stop after context cancellation")
		}
	})
}

func TestPurgeEmptyStore(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	p := purger.NewWithConfig(s, 90, 24, zerolog.Nop())
	n, err := p.Purge(context.Background())
	if err != nil {
		t.Fatalf("Purge on empty store: %v", err)
	}
	if n != 0 {
		t.Errorf("deleted %d sagas from empty store, want 0", n)
	}
}

func TestPurgeContextCancelled(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	createExec(t, s, "old-completed", saga.SagaStatusCompleted, 200*24*time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling Purge

	p := purger.NewWithConfig(s, 90, 24, zerolog.Nop())
	_, err := p.Purge(ctx)
	if err == nil {
		t.Fatal("Purge with cancelled context: expected non-nil error, got nil")
	}
}
