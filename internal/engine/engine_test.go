package engine_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/ngaddam369/saga-conductor/internal/engine"
	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/store"
)

// stepServer returns a test HTTP server that responds with the given status code
// and records how many times it was called.
func stepServer(t *testing.T, statusCode int) (*httptest.Server, *int) {
	t.Helper()
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		count++
		w.WriteHeader(statusCode)
	}))
	t.Cleanup(srv.Close)
	return srv, &count
}

func newEngine(t *testing.T) (*engine.Engine, *store.BoltStore) {
	t.Helper()
	s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return engine.New(s), s
}

func seedSaga(t *testing.T, s *store.BoltStore, steps []saga.StepDefinition) *saga.Execution {
	t.Helper()
	stepExecs := make([]saga.StepExecution, len(steps))
	for i, d := range steps {
		stepExecs[i] = saga.StepExecution{Name: d.Name, Status: saga.StepStatusPending}
	}
	exec := &saga.Execution{
		ID:        "saga-1",
		Name:      "test-saga",
		Status:    saga.SagaStatusPending,
		StepDefs:  steps,
		Steps:     stepExecs,
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.Create(context.Background(), exec); err != nil {
		t.Fatalf("seed Create: %v", err)
	}
	return exec
}

func TestEngine(t *testing.T) {
	t.Run("AllStepsSucceed", func(t *testing.T) {
		eng, s := newEngine(t)

		fwd1, calls1 := stepServer(t, http.StatusOK)
		fwd2, calls2 := stepServer(t, http.StatusOK)

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: fwd1.URL, CompensateURL: fwd1.URL},
			{Name: "step-2", ForwardURL: fwd2.URL, CompensateURL: fwd2.URL},
		})

		exec, err := eng.Start(context.Background(), "saga-1")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusCompleted {
			t.Errorf("Status: got %q, want COMPLETED", exec.Status)
		}
		if *calls1 != 1 || *calls2 != 1 {
			t.Errorf("forward call counts: step-1=%d step-2=%d, want 1 each", *calls1, *calls2)
		}
	})

	t.Run("FirstStepFails_NoCompensation", func(t *testing.T) {
		eng, s := newEngine(t)

		fwd, fwdCalls := stepServer(t, http.StatusInternalServerError)
		comp, compCalls := stepServer(t, http.StatusOK)
		_ = comp

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: fwd.URL, CompensateURL: comp.URL},
		})

		exec, err := eng.Start(context.Background(), "saga-1")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusFailed {
			t.Errorf("Status: got %q, want FAILED", exec.Status)
		}
		if exec.FailedStep != "step-1" {
			t.Errorf("FailedStep: got %q, want step-1", exec.FailedStep)
		}
		// step-1 failed on forward, no prior succeeded steps to compensate.
		if *compCalls != 0 {
			t.Errorf("compensation should not be called, got %d calls", *compCalls)
		}
		if *fwdCalls != 1 {
			t.Errorf("forward call count: got %d, want 1", *fwdCalls)
		}
	})

	t.Run("SecondStepFails_FirstCompensated", func(t *testing.T) {
		eng, s := newEngine(t)

		fwd1, _ := stepServer(t, http.StatusOK)
		comp1, compCalls1 := stepServer(t, http.StatusOK)
		fwd2, _ := stepServer(t, http.StatusInternalServerError)
		comp2, compCalls2 := stepServer(t, http.StatusOK)
		_ = comp2

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: fwd1.URL, CompensateURL: comp1.URL},
			{Name: "step-2", ForwardURL: fwd2.URL, CompensateURL: comp2.URL},
		})

		exec, err := eng.Start(context.Background(), "saga-1")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusFailed {
			t.Errorf("Status: got %q, want FAILED", exec.Status)
		}
		// step-1 succeeded then must be compensated; step-2 failed so no compensation.
		if *compCalls1 != 1 {
			t.Errorf("step-1 compensation calls: got %d, want 1", *compCalls1)
		}
		if *compCalls2 != 0 {
			t.Errorf("step-2 compensation should not be called, got %d", *compCalls2)
		}
		if exec.Steps[0].Status != saga.StepStatusCompensated {
			t.Errorf("step-1 Status: got %q, want COMPENSATED", exec.Steps[0].Status)
		}
		if exec.Steps[1].Status != saga.StepStatusFailed {
			t.Errorf("step-2 Status: got %q, want FAILED", exec.Steps[1].Status)
		}
	})

	t.Run("CompensationFails", func(t *testing.T) {
		eng, s := newEngine(t)

		fwd1, _ := stepServer(t, http.StatusOK)
		comp1, _ := stepServer(t, http.StatusInternalServerError) // compensation fails
		fwd2, _ := stepServer(t, http.StatusInternalServerError)  // forward fails

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: fwd1.URL, CompensateURL: comp1.URL},
			{Name: "step-2", ForwardURL: fwd2.URL, CompensateURL: fwd2.URL},
		})

		exec, err := eng.Start(context.Background(), "saga-1")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusFailed {
			t.Errorf("Status: got %q, want FAILED", exec.Status)
		}
		if exec.Steps[0].Status != saga.StepStatusCompensationFailed {
			t.Errorf("step-1 Status: got %q, want COMPENSATION_FAILED", exec.Steps[0].Status)
		}
	})

	t.Run("StartNonPendingSaga", func(t *testing.T) {
		tests := []struct {
			status saga.SagaStatus
		}{
			{saga.SagaStatusRunning},
			{saga.SagaStatusCompleted},
			{saga.SagaStatusFailed},
			{saga.SagaStatusCompensating},
		}
		for _, tc := range tests {
			t.Run(string(tc.status), func(t *testing.T) {
				eng, s := newEngine(t)
				exec := &saga.Execution{
					ID:        "saga-1",
					Name:      "test",
					Status:    tc.status,
					CreatedAt: time.Now().UTC(),
					Steps:     []saga.StepExecution{},
					StepDefs:  []saga.StepDefinition{},
				}
				if err := s.Create(context.Background(), exec); err != nil {
					t.Fatalf("Create: %v", err)
				}
				if _, err := eng.Start(context.Background(), "saga-1"); err == nil {
					t.Errorf("expected error starting saga in status %q, got nil", tc.status)
				}
			})
		}
	})

	t.Run("ContextAlreadyCanceled", func(t *testing.T) {
		eng, s := newEngine(t)

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: "http://x.com", CompensateURL: "http://x.com"},
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel before calling Start

		_, err := eng.Start(ctx, "saga-1")
		if err == nil {
			t.Fatal("expected error for cancelled context, got nil")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected errors.Is(err, context.Canceled), got: %v", err)
		}

		// Saga must still be PENDING — engine must not have written RUNNING.
		got, _ := s.Get(context.Background(), "saga-1")
		if got.Status != saga.SagaStatusPending {
			t.Errorf("saga status: got %q, want PENDING (engine must not have written RUNNING)", got.Status)
		}
	})

	t.Run("StepDefsMismatch", func(t *testing.T) {
		eng, s := newEngine(t)

		// Manually write a corrupted saga where Steps and StepDefs lengths differ.
		exec := &saga.Execution{
			ID:        "saga-1",
			Name:      "test-saga",
			Status:    saga.SagaStatusPending,
			StepDefs:  []saga.StepDefinition{{Name: "step-1", ForwardURL: "http://x.com", CompensateURL: "http://x.com"}},
			Steps:     []saga.StepExecution{}, // mismatched: 0 steps, 1 def
			Payload:   []byte(`{}`),
			CreatedAt: time.Now().UTC(),
		}
		if err := s.Create(context.Background(), exec); err != nil {
			t.Fatalf("Create: %v", err)
		}

		_, err := eng.Start(context.Background(), "saga-1")
		if err == nil {
			t.Fatal("expected error for Steps/StepDefs length mismatch, got nil")
		}
	})

	t.Run("StepsPersistedAfterCrashResume", func(t *testing.T) {
		// Simulate crash-safety: create a saga in RUNNING state mid-way (as if
		// the orchestrator crashed after persisting step-1 SUCCEEDED), then
		// verify the store reflects it correctly on re-open.
		dbPath := filepath.Join(t.TempDir(), "crash.db")
		ctx := context.Background()

		s1, err := store.NewBoltStore(dbPath)
		if err != nil {
			t.Fatalf("open s1: %v", err)
		}
		exec := &saga.Execution{
			ID:        "saga-crash",
			Name:      "crash-test",
			Status:    saga.SagaStatusRunning,
			CreatedAt: time.Now().UTC(),
			StepDefs: []saga.StepDefinition{
				{Name: "step-1", ForwardURL: "http://example.com", CompensateURL: "http://example.com"},
			},
			Steps: []saga.StepExecution{
				{Name: "step-1", Status: saga.StepStatusSucceeded},
			},
		}
		if err = s1.Create(ctx, exec); err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err = s1.Close(); err != nil {
			t.Fatalf("Close s1: %v", err)
		}

		// Re-open — simulates orchestrator restart.
		s2, err := store.NewBoltStore(dbPath)
		if err != nil {
			t.Fatalf("open s2: %v", err)
		}
		defer func() { _ = s2.Close() }()

		got, err := s2.Get(ctx, "saga-crash")
		if err != nil {
			t.Fatalf("Get after reopen: %v", err)
		}
		if got.Status != saga.SagaStatusRunning {
			t.Errorf("Status: got %q, want RUNNING", got.Status)
		}
		if got.Steps[0].Status != saga.StepStatusSucceeded {
			t.Errorf("step-1 Status: got %q, want SUCCEEDED", got.Steps[0].Status)
		}
	})
}
