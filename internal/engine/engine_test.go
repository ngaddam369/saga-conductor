package engine_test

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
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
	return engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0)), s
}

// faultyStore wraps a real Store and injects Update failures for the first
// failUpdates calls, then delegates normally.
type faultyStore struct {
	store.Store
	failUpdates int
	calls       int
}

func (f *faultyStore) Update(ctx context.Context, exec *saga.Execution) error {
	f.calls++
	if f.calls <= f.failUpdates {
		return errors.New("simulated store failure")
	}
	return f.Store.Update(ctx, exec)
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
	t.Run("ZeroSteps", func(t *testing.T) {
		eng, s := newEngine(t)
		seedSaga(t, s, []saga.StepDefinition{})

		exec, err := eng.Start(context.Background(), "saga-1")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusCompleted {
			t.Errorf("status: got %q, want COMPLETED", exec.Status)
		}
	})

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
			status  saga.SagaStatus
			wantErr error
		}{
			{saga.SagaStatusRunning, store.ErrAlreadyRunning},
			{saga.SagaStatusCompensating, store.ErrAlreadyCompensating},
			{saga.SagaStatusCompleted, store.ErrAlreadyCompleted},
			{saga.SagaStatusFailed, store.ErrAlreadyFailed},
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
				_, err := eng.Start(context.Background(), "saga-1")
				if !errors.Is(err, tc.wantErr) {
					t.Errorf("err: got %v, want %v", err, tc.wantErr)
				}
			})
		}
	})

	t.Run("StepTimeoutEnvVar", func(t *testing.T) {
		// A step server that hangs must be aborted within the configured timeout.
		// t.Setenv must be called before newEngine so New() reads the env var.
		t.Setenv("STEP_TIMEOUT_SECONDS", "1")
		eng, s := newEngine(t)

		done := make(chan struct{})
		hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-done
		}))
		t.Cleanup(func() { close(done); hang.Close() })

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: hang.URL, CompensateURL: hang.URL},
		})

		start := time.Now()
		exec, err := eng.Start(context.Background(), "saga-1")
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusFailed {
			t.Errorf("Status: got %q, want FAILED", exec.Status)
		}
		if elapsed > 3*time.Second {
			t.Errorf("took %v, expected step to be aborted within ~1s", elapsed)
		}
	})

	t.Run("HTTPRedirectRejected", func(t *testing.T) {
		// A step endpoint that redirects must be treated as a failure, not
		// silently followed — redirects are a potential SSRF vector.
		eng, s := newEngine(t)

		target, _ := stepServer(t, http.StatusOK)
		redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, target.URL, http.StatusFound)
		}))
		t.Cleanup(redirect.Close)

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: redirect.URL, CompensateURL: redirect.URL},
		})

		exec, err := eng.Start(context.Background(), "saga-1")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusFailed {
			t.Errorf("Status: got %q, want FAILED — redirect must not be followed", exec.Status)
		}
	})

	t.Run("FailedStepErrorIsHTTPStatus", func(t *testing.T) {
		// Verifies that when a step returns a non-2xx response the error message
		// reflects the HTTP status code, not a body-close side-effect.
		eng, s := newEngine(t)
		fwd, _ := stepServer(t, http.StatusUnprocessableEntity)

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: fwd.URL, CompensateURL: fwd.URL},
		})

		exec, err := eng.Start(context.Background(), "saga-1")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusFailed {
			t.Errorf("Status: got %q, want FAILED", exec.Status)
		}
		if exec.Steps[0].Error == "" {
			t.Error("expected step error to be set")
		}
		wantSubstr := "422"
		if !strings.Contains(exec.Steps[0].Error, wantSubstr) {
			t.Errorf("step error %q does not contain %q", exec.Steps[0].Error, wantSubstr)
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

	t.Run("StepHTTPRetry", func(t *testing.T) {
		tests := []struct {
			name       string
			failFirst  int // number of requests that return 500 before succeeding
			maxRetries int // StepDefinition.MaxRetries (must be > 0 to override engine default)
			wantStatus saga.SagaStatus
		}{
			{"succeeds on retry", 1, 3, saga.SagaStatusCompleted},
			{"exhausts retries → FAILED", 5, 2, saga.SagaStatusFailed},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				calls := 0
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					calls++
					if calls <= tc.failFirst {
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					w.WriteHeader(http.StatusOK)
				}))
				t.Cleanup(srv.Close)

				eng, s := newEngine(t)
				if err := s.Create(context.Background(), &saga.Execution{
					ID:        "retry-http-saga",
					Name:      "retry-http",
					Status:    saga.SagaStatusPending,
					CreatedAt: time.Now().UTC(),
					StepDefs: []saga.StepDefinition{{
						Name:           "s1",
						ForwardURL:     srv.URL,
						CompensateURL:  srv.URL,
						MaxRetries:     tc.maxRetries,
						RetryBackoffMs: 1, // 1ms base so tests run fast
					}},
					Steps: []saga.StepExecution{{Name: "s1", Status: saga.StepStatusPending}},
				}); err != nil {
					t.Fatalf("Create: %v", err)
				}

				exec, err := eng.Start(context.Background(), "retry-http-saga")
				if err != nil {
					t.Fatalf("Start: %v", err)
				}
				if exec.Status != tc.wantStatus {
					t.Errorf("status: got %q, want %q", exec.Status, tc.wantStatus)
				}
			})
		}
	})

	t.Run("StepHTTPRetryEnvDefaults", func(t *testing.T) {
		// Verify STEP_MAX_RETRIES and STEP_RETRY_BACKOFF_MS env vars are
		// picked up by New(). Set max retries to 2; a step that fails once
		// should still complete.
		t.Setenv("STEP_MAX_RETRIES", "2")
		t.Setenv("STEP_RETRY_BACKOFF_MS", "1")

		calls := 0
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls++
			if calls == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)

		// Create engine and store directly (not via newEngine) so env vars
		// set above are read by engine.New without being overridden.
		s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "env-retry.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond))

		if err := s.Create(context.Background(), &saga.Execution{
			ID:        "env-retry-saga",
			Name:      "env-retry",
			Status:    saga.SagaStatusPending,
			CreatedAt: time.Now().UTC(),
			StepDefs:  []saga.StepDefinition{{Name: "s1", ForwardURL: srv.URL, CompensateURL: srv.URL}},
			Steps:     []saga.StepExecution{{Name: "s1", Status: saga.StepStatusPending}},
		}); err != nil {
			t.Fatalf("Create: %v", err)
		}

		exec, err := eng.Start(context.Background(), "env-retry-saga")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusCompleted {
			t.Errorf("status: got %q, want COMPLETED", exec.Status)
		}
	})

	t.Run("StoreUpdateRetrySucceeds", func(t *testing.T) {
		// First two Update calls fail; the third succeeds.
		// The saga should still complete normally.
		srv, _ := stepServer(t, http.StatusOK)
		boltStore, err := store.NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = boltStore.Close() })

		fs := &faultyStore{Store: boltStore, failUpdates: 2}
		eng := engine.New(fs, engine.WithRetryBackoff(time.Millisecond))

		if err := boltStore.Create(context.Background(), &saga.Execution{
			ID:        "retry-saga",
			Name:      "retry",
			Status:    saga.SagaStatusPending,
			CreatedAt: time.Now().UTC(),
			StepDefs:  []saga.StepDefinition{{Name: "s1", ForwardURL: srv.URL, CompensateURL: srv.URL}},
			Steps:     []saga.StepExecution{{Name: "s1", Status: saga.StepStatusPending}},
		}); err != nil {
			t.Fatalf("Create: %v", err)
		}

		exec, err := eng.Start(context.Background(), "retry-saga")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusCompleted {
			t.Errorf("status: got %q, want COMPLETED", exec.Status)
		}
	})

	t.Run("StoreUpdateExhausted", func(t *testing.T) {
		// All Update calls fail — simulates disk full / store unavailable.
		// Start() must return an error; the best-effort FAILED mark also fails
		// (same faulty store) and logs to stderr.
		srv, _ := stepServer(t, http.StatusOK)
		boltStore, err := store.NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = boltStore.Close() })

		fs := &faultyStore{Store: boltStore, failUpdates: math.MaxInt}
		eng := engine.New(fs, engine.WithRetryBackoff(time.Millisecond))

		if err := boltStore.Create(context.Background(), &saga.Execution{
			ID:        "exhaust-saga",
			Name:      "exhaust",
			Status:    saga.SagaStatusPending,
			CreatedAt: time.Now().UTC(),
			StepDefs:  []saga.StepDefinition{{Name: "s1", ForwardURL: srv.URL, CompensateURL: srv.URL}},
			Steps:     []saga.StepExecution{{Name: "s1", Status: saga.StepStatusPending}},
		}); err != nil {
			t.Fatalf("Create: %v", err)
		}

		_, err = eng.Start(context.Background(), "exhaust-saga")
		if err == nil {
			t.Fatal("expected error when store is unavailable, got nil")
		}
	})

	t.Run("ConcurrentStart", func(t *testing.T) {
		// Two goroutines race to start the same PENDING saga.
		// Exactly one must succeed; the other must get ErrAlreadyRunning.
		srv, _ := stepServer(t, http.StatusOK)
		eng, s := newEngine(t)
		ctx := context.Background()

		exec := &saga.Execution{
			ID:        "concurrent-saga",
			Name:      "concurrent",
			Status:    saga.SagaStatusPending,
			CreatedAt: time.Now().UTC(),
			StepDefs:  []saga.StepDefinition{{Name: "s1", ForwardURL: srv.URL, CompensateURL: srv.URL}},
			Steps:     []saga.StepExecution{{Name: "s1", Status: saga.StepStatusPending}},
		}
		if err := s.Create(ctx, exec); err != nil {
			t.Fatalf("Create: %v", err)
		}

		var (
			wg             sync.WaitGroup
			mu             sync.Mutex
			successes      int
			alreadyRunning int
		)
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := eng.Start(ctx, "concurrent-saga")
				mu.Lock()
				defer mu.Unlock()
				if err == nil {
					successes++
				} else if errors.Is(err, store.ErrAlreadyRunning) {
					alreadyRunning++
				} else {
					t.Errorf("unexpected error: %v", err)
				}
			}()
		}
		wg.Wait()

		if successes != 1 {
			t.Errorf("successes: got %d, want 1", successes)
		}
		if alreadyRunning != 1 {
			t.Errorf("alreadyRunning errors: got %d, want 1", alreadyRunning)
		}
	})

	t.Run("PerStepTimeout", func(t *testing.T) {
		// A step with TimeoutSeconds:1 must be aborted in ~1s regardless of
		// the engine's default timeout (which is much larger).
		done := make(chan struct{})
		hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-done
		}))
		t.Cleanup(func() { close(done); hang.Close() })

		// Build engine with a large default timeout so the per-step override
		// is the only reason it aborts quickly.
		s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "per-step-timeout.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		t.Setenv("STEP_TIMEOUT_SECONDS", "30")
		eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0))

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: hang.URL, CompensateURL: hang.URL, TimeoutSeconds: 1},
		})

		start := time.Now()
		exec, startErr := eng.Start(context.Background(), "saga-1")
		elapsed := time.Since(start)

		if startErr != nil {
			t.Fatalf("Start: %v", startErr)
		}
		if exec.Status != saga.SagaStatusFailed {
			t.Errorf("Status: got %q, want FAILED", exec.Status)
		}
		if elapsed > 3*time.Second {
			t.Errorf("took %v; expected per-step timeout of 1s to abort quickly", elapsed)
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
