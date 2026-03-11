package store_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func newExec(id, name string, status saga.SagaStatus) *saga.Execution {
	return &saga.Execution{
		ID:        id,
		Name:      name,
		Status:    status,
		CreatedAt: time.Now().UTC(),
		Steps:     []saga.StepExecution{},
		StepDefs:  []saga.StepDefinition{},
	}
}

func TestNewBoltStore(t *testing.T) {
	t.Run("LockTimeout", func(t *testing.T) {
		tests := []struct {
			name        string
			envVal      string
			wantTimeout time.Duration
		}{
			{"default when unset", "", 5 * time.Second},
			{"custom value from env", "10", 10 * time.Second},
			{"zero falls back to default", "0", 5 * time.Second},
			{"negative falls back to default", "-1", 5 * time.Second},
			{"non-numeric falls back to default", "bad", 5 * time.Second},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				if tc.envVal != "" {
					t.Setenv("DB_LOCK_TIMEOUT_SECONDS", tc.envVal)
				}
				// Open and immediately close — we just want it to succeed
				// with the configured timeout (positive case).
				path := filepath.Join(t.TempDir(), "lock-test.db")
				s, err := store.NewBoltStore(path)
				if err != nil {
					t.Fatalf("NewBoltStore: %v", err)
				}
				if err := s.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
			})
		}
	})

	t.Run("LockContention", func(t *testing.T) {
		// Hold the lock with a first store, then verify a second open
		// with a short timeout returns an error rather than blocking.
		t.Setenv("DB_LOCK_TIMEOUT_SECONDS", "1")
		path := filepath.Join(t.TempDir(), "contention.db")

		holder, err := store.NewBoltStore(path)
		if err != nil {
			t.Fatalf("open holder: %v", err)
		}
		defer func() {
			if err := holder.Close(); err != nil {
				t.Errorf("close holder: %v", err)
			}
		}()

		start := time.Now()
		_, err = store.NewBoltStore(path)
		elapsed := time.Since(start)

		if err == nil {
			t.Fatal("expected error due to lock contention, got nil")
		}
		if elapsed > 3*time.Second {
			t.Errorf("open took %v, expected to fail within ~1s timeout", elapsed)
		}
	})
}

func TestBoltStore(t *testing.T) {
	t.Run("CreateAndGet", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		exec := newExec("id-1", "order-saga", saga.SagaStatusPending)
		if err := s.Create(ctx, exec); err != nil {
			t.Fatalf("Create: %v", err)
		}

		got, err := s.Get(ctx, "id-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Name != exec.Name || got.Status != saga.SagaStatusPending {
			t.Errorf("got Name=%q Status=%q, want %q %q", got.Name, got.Status, exec.Name, saga.SagaStatusPending)
		}
	})

	t.Run("CreateDuplicate", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()
		exec := newExec("dup-1", "saga", saga.SagaStatusPending)
		if err := s.Create(ctx, exec); err != nil {
			t.Fatalf("first Create: %v", err)
		}
		if err := s.Create(ctx, exec); err != store.ErrAlreadyExists {
			t.Errorf("want ErrAlreadyExists, got %v", err)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		s := newTestStore(t)
		if _, err := s.Get(context.Background(), "nonexistent"); err != store.ErrNotFound {
			t.Errorf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("Update", func(t *testing.T) {
		tests := []struct {
			name       string
			fromStatus saga.SagaStatus
			toStatus   saga.SagaStatus
		}{
			{"pending to running", saga.SagaStatusPending, saga.SagaStatusRunning},
			{"running to completed", saga.SagaStatusRunning, saga.SagaStatusCompleted},
			{"running to compensating", saga.SagaStatusRunning, saga.SagaStatusCompensating},
			{"compensating to failed", saga.SagaStatusCompensating, saga.SagaStatusFailed},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				s := newTestStore(t)
				ctx := context.Background()

				exec := newExec("upd-1", "saga", tc.fromStatus)
				if err := s.Create(ctx, exec); err != nil {
					t.Fatalf("Create: %v", err)
				}
				exec.Status = tc.toStatus
				if err := s.Update(ctx, exec); err != nil {
					t.Fatalf("Update: %v", err)
				}
				got, err := s.Get(ctx, exec.ID)
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if got.Status != tc.toStatus {
					t.Errorf("Status: got %q, want %q", got.Status, tc.toStatus)
				}
			})
		}
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		s := newTestStore(t)
		exec := newExec("missing", "saga", saga.SagaStatusPending)
		if err := s.Update(context.Background(), exec); err != store.ErrNotFound {
			t.Errorf("want ErrNotFound, got %v", err)
		}
	})

	t.Run("List", func(t *testing.T) {
		tests := []struct {
			name         string
			filterStatus saga.SagaStatus
			wantCount    int
		}{
			{"all", "", 3},
			{"pending only", saga.SagaStatusPending, 1},
			{"running only", saga.SagaStatusRunning, 1},
			{"completed only", saga.SagaStatusCompleted, 1},
			{"failed only", saga.SagaStatusFailed, 0},
		}
		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				s := newTestStore(t)
				ctx := context.Background()

				seed := []*saga.Execution{
					newExec("a", "saga-a", saga.SagaStatusPending),
					newExec("b", "saga-b", saga.SagaStatusRunning),
					newExec("c", "saga-c", saga.SagaStatusCompleted),
				}
				for _, e := range seed {
					if err := s.Create(ctx, e); err != nil {
						t.Fatalf("Create %s: %v", e.ID, err)
					}
				}

				got, err := s.List(ctx, tc.filterStatus)
				if err != nil {
					t.Fatalf("List: %v", err)
				}
				if len(got) != tc.wantCount {
					t.Errorf("List(%q): got %d, want %d", tc.filterStatus, len(got), tc.wantCount)
				}
			})
		}
	})

	t.Run("ListCanceledContext", func(t *testing.T) {
		s := newTestStore(t)
		ctx := context.Background()

		for i := range 3 {
			e := newExec(fmt.Sprintf("ctx-%d", i), "saga", saga.SagaStatusPending)
			if err := s.Create(ctx, e); err != nil {
				t.Fatalf("Create: %v", err)
			}
		}

		canceled, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := s.List(canceled, "")
		if err == nil {
			t.Fatal("expected error from canceled context, got nil")
		}
	})

	t.Run("PersistsAcrossReopen", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "persist.db")
		ctx := context.Background()

		s1, err := store.NewBoltStore(path)
		if err != nil {
			t.Fatalf("open s1: %v", err)
		}
		exec := newExec("persist-1", "saga", saga.SagaStatusCompleted)
		if err := s1.Create(ctx, exec); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := s1.Close(); err != nil {
			t.Fatalf("Close s1: %v", err)
		}

		s2, err := store.NewBoltStore(path)
		if err != nil {
			t.Fatalf("open s2: %v", err)
		}
		defer func() { _ = s2.Close() }()

		got, err := s2.Get(ctx, "persist-1")
		if err != nil {
			t.Fatalf("Get after reopen: %v", err)
		}
		if got.Status != saga.SagaStatusCompleted {
			t.Errorf("Status: got %q, want COMPLETED", got.Status)
		}
		if _, err := os.Stat(path); err != nil {
			t.Errorf("db file missing: %v", err)
		}
	})
}
