package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

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
		if err == nil {
			t.Fatal("Start: expected error when compensation exhausts retries, got nil")
		}
		if exec == nil {
			t.Fatal("Start: expected non-nil exec even on compensation failure")
		}
		if exec.Status != saga.SagaStatusCompensationFailed {
			t.Errorf("Status: got %q, want COMPENSATION_FAILED", exec.Status)
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
			{saga.SagaStatusAborted, store.ErrAlreadyAborted},
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

	t.Run("StepErrorDetailHTTP", func(t *testing.T) {
		// A non-2xx step response must populate ErrorDetail with the HTTP status
		// code, the first 512 bytes of the response body, DurationMs, and
		// IsNetworkError=false.
		eng, s := newEngine(t)

		body := "invalid payment method"
		fwd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(body))
		}))
		t.Cleanup(fwd.Close)

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

		d := exec.Steps[0].ErrorDetail
		if d == nil {
			t.Fatal("ErrorDetail is nil, want structured error")
		}
		if d.HTTPStatusCode != http.StatusUnprocessableEntity {
			t.Errorf("HTTPStatusCode: got %d, want 422", d.HTTPStatusCode)
		}
		if d.ResponseBody != body {
			t.Errorf("ResponseBody: got %q, want %q", d.ResponseBody, body)
		}
		if d.IsNetworkError {
			t.Error("IsNetworkError: got true, want false for HTTP error")
		}
		if d.DurationMs < 0 {
			t.Errorf("DurationMs: got %d, want >= 0", d.DurationMs)
		}
		if !strings.Contains(d.Message, "422") {
			t.Errorf("Message %q does not contain 422", d.Message)
		}
	})

	t.Run("StepErrorDetailNetwork", func(t *testing.T) {
		// A transport-level failure (connection refused) must populate ErrorDetail
		// with IsNetworkError=true and HTTPStatusCode=0.
		eng, s := newEngine(t)

		// Start a server, capture its URL, then close it so connections are refused.
		closed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
		closedURL := closed.URL
		closed.Close()

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: closedURL, CompensateURL: closedURL},
		})

		exec, err := eng.Start(context.Background(), "saga-1")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusFailed {
			t.Errorf("Status: got %q, want FAILED", exec.Status)
		}

		d := exec.Steps[0].ErrorDetail
		if d == nil {
			t.Fatal("ErrorDetail is nil, want structured error")
		}
		if !d.IsNetworkError {
			t.Error("IsNetworkError: got false, want true for transport error")
		}
		if d.HTTPStatusCode != 0 {
			t.Errorf("HTTPStatusCode: got %d, want 0 for transport error", d.HTTPStatusCode)
		}
		if d.ResponseBody != "" {
			t.Errorf("ResponseBody: got %q, want empty for transport error", d.ResponseBody)
		}
	})

	t.Run("StepErrorBodyTruncatedAt512", func(t *testing.T) {
		// Response bodies larger than 512 bytes must be truncated in ErrorDetail.
		eng, s := newEngine(t)

		longBody := strings.Repeat("x", 1024)
		fwd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(longBody))
		}))
		t.Cleanup(fwd.Close)

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: fwd.URL, CompensateURL: fwd.URL},
		})

		exec, err := eng.Start(context.Background(), "saga-1")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}

		d := exec.Steps[0].ErrorDetail
		if d == nil {
			t.Fatal("ErrorDetail is nil")
		}
		if len(d.ResponseBody) > 512 {
			t.Errorf("ResponseBody length %d exceeds 512-byte cap", len(d.ResponseBody))
		}
		if len(d.ResponseBody) == 0 {
			t.Error("ResponseBody is empty, expected truncated content")
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

	t.Run("SagaTimeout", func(t *testing.T) {
		// SAGA_TIMEOUT_SECONDS=1 must abort a hanging step within ~1s and mark
		// the saga FAILED. The error returned must wrap context.DeadlineExceeded.
		done := make(chan struct{})
		hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-done
		}))
		t.Cleanup(func() { close(done); hang.Close() })

		t.Setenv("SAGA_TIMEOUT_SECONDS", "1")
		t.Setenv("STEP_TIMEOUT_SECONDS", "30") // per-step timeout larger than saga timeout
		t.Setenv("STEP_MAX_RETRIES", "0")
		s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "saga-timeout.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond))

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: hang.URL, CompensateURL: hang.URL},
		})

		start := time.Now()
		exec, startErr := eng.Start(context.Background(), "saga-1")
		elapsed := time.Since(start)

		if !errors.Is(startErr, context.DeadlineExceeded) {
			t.Errorf("Start error: got %v, want wrapping context.DeadlineExceeded", startErr)
		}
		if exec == nil || exec.Status != saga.SagaStatusFailed {
			t.Errorf("Status: got %v, want FAILED", exec)
		}
		if elapsed > 3*time.Second {
			t.Errorf("took %v; expected saga timeout of 1s to abort quickly", elapsed)
		}
	})

	t.Run("SagaTimeoutCompensationRuns", func(t *testing.T) {
		// When the saga deadline fires during step-2, compensation for the
		// already-succeeded step-1 must still be attempted using a fresh context.
		var compCalls int
		compSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			compCalls++
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(compSrv.Close)

		fwdOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(fwdOK.Close)

		done := make(chan struct{})
		hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-done
		}))
		t.Cleanup(func() { close(done); hang.Close() })

		t.Setenv("SAGA_TIMEOUT_SECONDS", "1")
		t.Setenv("STEP_TIMEOUT_SECONDS", "30")
		t.Setenv("STEP_MAX_RETRIES", "0")
		s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "saga-timeout-comp.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond))

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: fwdOK.URL, CompensateURL: compSrv.URL},
			{Name: "step-2", ForwardURL: hang.URL, CompensateURL: compSrv.URL},
		})

		start := time.Now()
		exec, startErr := eng.Start(context.Background(), "saga-1")
		elapsed := time.Since(start)

		if !errors.Is(startErr, context.DeadlineExceeded) {
			t.Errorf("Start error: got %v, want wrapping context.DeadlineExceeded", startErr)
		}
		if exec == nil || exec.Status != saga.SagaStatusFailed {
			t.Errorf("Status: got %v, want FAILED", exec)
		}
		if compCalls != 1 {
			t.Errorf("compensation calls: got %d, want 1 (step-1 should be compensated)", compCalls)
		}
		if elapsed > 3*time.Second {
			t.Errorf("took %v; expected saga timeout of 1s to abort quickly", elapsed)
		}
	})

	t.Run("DrainWaitsForInflight", func(t *testing.T) {
		eng, s := newEngine(t)

		hit := make(chan struct{})
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(hit)
			<-release
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: srv.URL, CompensateURL: srv.URL},
		})

		startDone := make(chan error, 1)
		go func() {
			_, err := eng.Start(context.Background(), "saga-1")
			startDone <- err
		}()

		// Wait until Start is inside callHTTP — past inflight registration.
		<-hit

		drainDone := make(chan []string, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			drainDone <- eng.Drain(ctx)
		}()

		// Give Drain a moment to block on the WaitGroup.
		time.Sleep(20 * time.Millisecond)
		select {
		case <-drainDone:
			t.Fatal("Drain returned before in-flight saga completed")
		default:
		}

		close(release) // let the step succeed

		interrupted := <-drainDone
		if len(interrupted) != 0 {
			t.Errorf("Drain: unexpected interrupted IDs: %v", interrupted)
		}
		if err := <-startDone; err != nil {
			t.Errorf("Start: %v", err)
		}
	})

	t.Run("DrainTimeoutReturnsIDs", func(t *testing.T) {
		eng, s := newEngine(t)

		hit := make(chan struct{})
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(hit)
			<-release
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: srv.URL, CompensateURL: srv.URL},
		})

		startDone := make(chan error, 1)
		go func() {
			_, err := eng.Start(context.Background(), "saga-1")
			startDone <- err
		}()

		<-hit // Start is past inflight registration

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		interrupted := eng.Drain(ctx)

		if len(interrupted) != 1 || interrupted[0] != "saga-1" {
			t.Errorf("Drain: want [saga-1], got %v", interrupted)
		}

		close(release) // let Start finish cleanly
		<-startDone
	})

	t.Run("DrainRejectsNewStart", func(t *testing.T) {
		eng, s := newEngine(t)
		seedSaga(t, s, []saga.StepDefinition{})

		// Drain with no in-flight sagas completes immediately.
		interrupted := eng.Drain(context.Background())
		if len(interrupted) != 0 {
			t.Errorf("Drain: unexpected interrupted IDs: %v", interrupted)
		}

		_, err := eng.Start(context.Background(), "saga-1")
		if !errors.Is(err, engine.ErrDraining) {
			t.Errorf("Start after Drain: got %v, want ErrDraining", err)
		}
	})

	t.Run("CompensationRetrySucceeds", func(t *testing.T) {
		// Compensation step fails once then succeeds — saga reaches FAILED
		// (all compensations done), not COMPENSATION_FAILED.
		eng, s := newEngine(t)

		fwd, _ := stepServer(t, http.StatusOK)

		compCalls := 0
		comp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			compCalls++
			if compCalls == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(comp.Close)

		fwd2, _ := stepServer(t, http.StatusInternalServerError) // triggers compensation

		// Give compensation one retry so the second attempt succeeds.
		eng2 := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(1))

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: fwd.URL, CompensateURL: comp.URL},
			{Name: "step-2", ForwardURL: fwd2.URL, CompensateURL: comp.URL},
		})

		exec, err := eng2.Start(context.Background(), "saga-1")
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
		if exec.Status != saga.SagaStatusFailed {
			t.Errorf("status: got %q, want FAILED", exec.Status)
		}
		if compCalls != 2 {
			t.Errorf("compensation calls: got %d, want 2 (1 fail + 1 succeed)", compCalls)
		}
		_ = eng // silence unused variable
	})

	t.Run("CompensationExhaustsRetries", func(t *testing.T) {
		// Compensation step always fails — saga dead-letters to COMPENSATION_FAILED
		// and compensation halts on the first irrecoverable failure.
		_, s := newEngine(t)

		fwd, _ := stepServer(t, http.StatusOK)
		comp, compCalls := stepServer(t, http.StatusInternalServerError)
		fwd2, _ := stepServer(t, http.StatusInternalServerError) // triggers compensation

		eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(1))

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step-1", ForwardURL: fwd.URL, CompensateURL: comp.URL},
			{Name: "step-2", ForwardURL: fwd2.URL, CompensateURL: comp.URL},
		})

		exec, err := eng.Start(context.Background(), "saga-1")
		if err == nil {
			t.Fatal("Start: expected error, got nil")
		}
		if exec == nil {
			t.Fatal("Start: expected execution to be returned alongside error")
		}
		if exec.Status != saga.SagaStatusCompensationFailed {
			t.Errorf("status: got %q, want COMPENSATION_FAILED", exec.Status)
		}
		// step-1 compensation should have been attempted (1 initial + 1 retry = 2),
		// then halted — step-2 compensation never attempted.
		if *compCalls != 2 {
			t.Errorf("compensation calls: got %d, want 2 (retries on step-1 only)", *compCalls)
		}
		// The failed step's ErrorDetail must be populated.
		step1 := exec.Steps[0]
		if step1.Status != saga.StepStatusCompensationFailed {
			t.Errorf("step-1 status: got %q, want COMPENSATION_FAILED", step1.Status)
		}
		if step1.ErrorDetail == nil {
			t.Error("step-1 ErrorDetail: got nil, want populated StepError")
		}
	})
}

func TestEngineResume(t *testing.T) {
	t.Parallel()

	// seedRunning writes a saga directly into the store in RUNNING state with
	// some steps already in the given statuses, simulating a mid-run crash.
	seedRunning := func(t *testing.T, s *store.BoltStore, defs []saga.StepDefinition, stepStatuses []saga.StepStatus) {
		t.Helper()
		steps := make([]saga.StepExecution, len(defs))
		for i, d := range defs {
			steps[i] = saga.StepExecution{Name: d.Name, Status: stepStatuses[i]}
		}
		exec := &saga.Execution{
			ID:        "saga-1",
			Name:      "resume-saga",
			Status:    saga.SagaStatusRunning,
			StepDefs:  defs,
			Steps:     steps,
			Payload:   []byte(`{}`),
			CreatedAt: time.Now().UTC(),
		}
		if err := s.Create(context.Background(), exec); err != nil {
			t.Fatalf("seedRunning Create: %v", err)
		}
	}

	t.Run("TerminalIsNoop", func(t *testing.T) {
		t.Parallel()

		t.Run("COMPLETED", func(t *testing.T) {
			t.Parallel()
			eng, s := newEngine(t)
			ok, _ := stepServer(t, http.StatusOK)
			seedSaga(t, s, []saga.StepDefinition{
				{Name: "s1", ForwardURL: ok.URL, CompensateURL: ok.URL},
			})
			if _, err := eng.Start(context.Background(), "saga-1"); err != nil {
				t.Fatalf("Start: %v", err)
			}
			exec, err := eng.Resume(context.Background(), "saga-1")
			if err != nil {
				t.Fatalf("Resume on COMPLETED saga: %v", err)
			}
			if exec.Status != saga.SagaStatusCompleted {
				t.Errorf("Status: got %s, want COMPLETED", exec.Status)
			}
		})

		// ABORTED and COMPENSATION_FAILED are also terminal; Resume must
		// return the execution unchanged rather than an error.
		for _, terminalStatus := range []saga.SagaStatus{
			saga.SagaStatusAborted,
			saga.SagaStatusCompensationFailed,
		} {
			t.Run(string(terminalStatus), func(t *testing.T) {
				t.Parallel()
				_, s := newEngine(t)
				eng := engine.New(s, engine.WithDefaultMaxRetries(0))

				exec := &saga.Execution{
					ID:        "saga-1",
					Name:      "terminal-saga",
					Status:    terminalStatus,
					StepDefs:  []saga.StepDefinition{{Name: "s1"}},
					Steps:     []saga.StepExecution{{Name: "s1", Status: saga.StepStatusFailed}},
					CreatedAt: time.Now().UTC(),
				}
				if err := s.Create(context.Background(), exec); err != nil {
					t.Fatalf("seed: %v", err)
				}

				got, err := eng.Resume(context.Background(), "saga-1")
				if err != nil {
					t.Fatalf("Resume on %s saga: unexpected error: %v", terminalStatus, err)
				}
				if got.Status != terminalStatus {
					t.Errorf("Status: got %s, want %s", got.Status, terminalStatus)
				}
			})
		}
	})

	t.Run("ResumeRunning", func(t *testing.T) {
		t.Parallel()

		t.Run("SkipsSucceeded_CompletesRemaining", func(t *testing.T) {
			t.Parallel()
			_, s := newEngine(t)
			ok, callCount := stepServer(t, http.StatusOK)
			defs := []saga.StepDefinition{
				{Name: "s1", ForwardURL: ok.URL, CompensateURL: ok.URL},
				{Name: "s2", ForwardURL: ok.URL, CompensateURL: ok.URL},
			}
			// s1 already SUCCEEDED, s2 still PENDING — simulate crash after s1.
			seedRunning(t, s, defs, []saga.StepStatus{
				saga.StepStatusSucceeded,
				saga.StepStatusPending,
			})

			eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0))
			exec, err := eng.Resume(context.Background(), "saga-1")
			if err != nil {
				t.Fatalf("Resume: %v", err)
			}
			if exec.Status != saga.SagaStatusCompleted {
				t.Errorf("Status: got %s, want COMPLETED", exec.Status)
			}
			// s1 must not be re-executed; only s2's forward URL is called.
			if *callCount != 1 {
				t.Errorf("forward calls: got %d, want 1 (s1 must not be re-executed)", *callCount)
			}
		})

		t.Run("CrashedMidHTTP_Retries", func(t *testing.T) {
			t.Parallel()
			_, s := newEngine(t)
			ok, callCount := stepServer(t, http.StatusOK)
			defs := []saga.StepDefinition{
				{Name: "s1", ForwardURL: ok.URL, CompensateURL: ok.URL},
			}
			// s1 marked RUNNING but HTTP response never arrived before crash.
			seedRunning(t, s, defs, []saga.StepStatus{saga.StepStatusRunning})

			eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0))
			exec, err := eng.Resume(context.Background(), "saga-1")
			if err != nil {
				t.Fatalf("Resume: %v", err)
			}
			if exec.Status != saga.SagaStatusCompleted {
				t.Errorf("Status: got %s, want COMPLETED", exec.Status)
			}
			if *callCount != 1 {
				t.Errorf("forward calls: got %d, want 1", *callCount)
			}
		})
	})

	t.Run("ResumeCompensating", func(t *testing.T) {
		t.Parallel()

		t.Run("SkipsCompensated_RetriesToRemaining", func(t *testing.T) {
			t.Parallel()
			_, s := newEngine(t)
			comp, compCalls := stepServer(t, http.StatusOK)
			defs := []saga.StepDefinition{
				{Name: "s1", ForwardURL: comp.URL, CompensateURL: comp.URL},
				{Name: "s2", ForwardURL: comp.URL, CompensateURL: comp.URL},
			}
			// s2 failed, s1 already COMPENSATED — simulate crash mid-compensation.
			steps := []saga.StepExecution{
				{Name: "s1", Status: saga.StepStatusCompensated},
				{Name: "s2", Status: saga.StepStatusFailed},
			}
			exec := &saga.Execution{
				ID: "saga-1", Name: "resume-comp",
				Status:     saga.SagaStatusCompensating,
				StepDefs:   defs,
				Steps:      steps,
				FailedStep: "s2",
				Payload:    []byte(`{}`),
				CreatedAt:  time.Now().UTC(),
			}
			if err := s.Create(context.Background(), exec); err != nil {
				t.Fatalf("Create: %v", err)
			}

			eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0))
			got, err := eng.Resume(context.Background(), "saga-1")
			if err != nil {
				t.Fatalf("Resume: %v", err)
			}
			if got.Status != saga.SagaStatusFailed {
				t.Errorf("Status: got %s, want FAILED", got.Status)
			}
			// s1 was already COMPENSATED — its compensate URL must not be called again.
			if *compCalls != 0 {
				t.Errorf("comp calls: got %d, want 0 (s1 already compensated)", *compCalls)
			}
		})

		t.Run("CrashedMidCompensation_Retries", func(t *testing.T) {
			t.Parallel()
			_, s := newEngine(t)
			comp, compCalls := stepServer(t, http.StatusOK)
			defs := []saga.StepDefinition{
				{Name: "s1", ForwardURL: comp.URL, CompensateURL: comp.URL},
				{Name: "s2", ForwardURL: comp.URL, CompensateURL: comp.URL},
			}
			// s2 failed, s1 was marked COMPENSATING but HTTP call didn't finish.
			steps := []saga.StepExecution{
				{Name: "s1", Status: saga.StepStatusCompensating},
				{Name: "s2", Status: saga.StepStatusFailed},
			}
			exec := &saga.Execution{
				ID: "saga-1", Name: "resume-comp-mid",
				Status:     saga.SagaStatusCompensating,
				StepDefs:   defs,
				Steps:      steps,
				FailedStep: "s2",
				Payload:    []byte(`{}`),
				CreatedAt:  time.Now().UTC(),
			}
			if err := s.Create(context.Background(), exec); err != nil {
				t.Fatalf("Create: %v", err)
			}

			eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0))
			got, err := eng.Resume(context.Background(), "saga-1")
			if err != nil {
				t.Fatalf("Resume: %v", err)
			}
			if got.Status != saga.SagaStatusFailed {
				t.Errorf("Status: got %s, want FAILED", got.Status)
			}
			// s1 was COMPENSATING — its compensate URL must be retried once.
			if *compCalls != 1 {
				t.Errorf("comp calls: got %d, want 1 (s1 compensation must be retried)", *compCalls)
			}
		})
	})
}

// seedSagaWithStatus creates a saga in the given status directly in the store.
// Useful for abort tests that need a saga already past PENDING.
func seedSagaWithStatus(t *testing.T, s *store.BoltStore, id string, status saga.SagaStatus) *saga.Execution {
	t.Helper()
	exec := &saga.Execution{
		ID:        id,
		Name:      "abort-test-saga",
		Status:    saga.SagaStatusPending, // must Create as PENDING
		StepDefs:  []saga.StepDefinition{{Name: "s1", ForwardURL: "http://localhost", CompensateURL: "http://localhost"}},
		Steps:     []saga.StepExecution{{Name: "s1", Status: saga.StepStatusPending}},
		Payload:   []byte(`{}`),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.Create(context.Background(), exec); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if status != saga.SagaStatusPending {
		exec.Status = status
		if err := s.Update(context.Background(), exec); err != nil {
			t.Fatalf("Update to %s: %v", status, err)
		}
	}
	return exec
}

func TestEngineAbort(t *testing.T) {
	t.Parallel()

	t.Run("Pending", func(t *testing.T) {
		t.Parallel()
		eng, s := newEngine(t)
		seedSagaWithStatus(t, s, "abort-1", saga.SagaStatusPending)

		got, err := eng.Abort(context.Background(), "abort-1")
		if err != nil {
			t.Fatalf("Abort: %v", err)
		}
		if got.Status != saga.SagaStatusAborted {
			t.Errorf("Status: got %s, want ABORTED", got.Status)
		}
		if got.CompletedAt == nil {
			t.Error("CompletedAt: want non-nil")
		}
		// Verify persisted.
		persisted, err := s.Get(context.Background(), "abort-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if persisted.Status != saga.SagaStatusAborted {
			t.Errorf("persisted Status: got %s, want ABORTED", persisted.Status)
		}
	})

	t.Run("Running", func(t *testing.T) {
		t.Parallel()
		eng, s := newEngine(t)
		seedSagaWithStatus(t, s, "abort-2", saga.SagaStatusRunning)

		got, err := eng.Abort(context.Background(), "abort-2")
		if err != nil {
			t.Fatalf("Abort: %v", err)
		}
		if got.Status != saga.SagaStatusAborted {
			t.Errorf("Status: got %s, want ABORTED", got.Status)
		}
	})

	t.Run("Compensating", func(t *testing.T) {
		t.Parallel()
		eng, s := newEngine(t)
		seedSagaWithStatus(t, s, "abort-3", saga.SagaStatusCompensating)

		got, err := eng.Abort(context.Background(), "abort-3")
		if err != nil {
			t.Fatalf("Abort: %v", err)
		}
		if got.Status != saga.SagaStatusAborted {
			t.Errorf("Status: got %s, want ABORTED", got.Status)
		}
	})

	t.Run("AlreadyAborted", func(t *testing.T) {
		t.Parallel()
		eng, s := newEngine(t)
		seedSagaWithStatus(t, s, "abort-4", saga.SagaStatusAborted)

		_, err := eng.Abort(context.Background(), "abort-4")
		if !errors.Is(err, store.ErrAlreadyAborted) {
			t.Errorf("Abort: got %v, want ErrAlreadyAborted", err)
		}
	})

	t.Run("AlreadyCompleted", func(t *testing.T) {
		t.Parallel()
		eng, s := newEngine(t)
		seedSagaWithStatus(t, s, "abort-5", saga.SagaStatusCompleted)

		_, err := eng.Abort(context.Background(), "abort-5")
		if !errors.Is(err, store.ErrAlreadyCompleted) {
			t.Errorf("Abort: got %v, want ErrAlreadyCompleted", err)
		}
	})

	t.Run("AlreadyFailed", func(t *testing.T) {
		t.Parallel()
		eng, s := newEngine(t)
		seedSagaWithStatus(t, s, "abort-6", saga.SagaStatusFailed)

		_, err := eng.Abort(context.Background(), "abort-6")
		if !errors.Is(err, store.ErrAlreadyFailed) {
			t.Errorf("Abort: got %v, want ErrAlreadyFailed", err)
		}
	})

	t.Run("AlreadyCompensationFailed", func(t *testing.T) {
		t.Parallel()
		eng, s := newEngine(t)
		seedSagaWithStatus(t, s, "abort-7", saga.SagaStatusCompensationFailed)

		_, err := eng.Abort(context.Background(), "abort-7")
		if !errors.Is(err, store.ErrAlreadyFailed) {
			t.Errorf("Abort: got %v, want ErrAlreadyFailed", err)
		}
	})

	t.Run("NotFound", func(t *testing.T) {
		t.Parallel()
		eng, s := newEngine(t)
		_ = s

		_, err := eng.Abort(context.Background(), "does-not-exist")
		if err == nil {
			t.Fatal("Abort: want error for non-existent saga, got nil")
		}
	})

	t.Run("CancelledContext", func(t *testing.T) {
		t.Parallel()
		eng, s := newEngine(t)
		_ = s

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := eng.Abort(ctx, "any-id")
		if err == nil {
			t.Fatal("Abort: want error for cancelled context, got nil")
		}
	})
}

func TestEngineLogging(t *testing.T) {
	t.Parallel()

	t.Run("saga completed emits structured log with saga_id and status", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		log := zerolog.New(&buf)

		s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "log.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0), engine.WithLogger(log))

		fwd, _ := stepServer(t, http.StatusOK)
		seedSaga(t, s, []saga.StepDefinition{
			{Name: "pay", ForwardURL: fwd.URL, CompensateURL: fwd.URL},
		})

		ctx := log.WithContext(context.Background())
		if _, err = eng.Start(ctx, "saga-1"); err != nil {
			t.Fatalf("Start: %v", err)
		}

		var foundCompleted bool
		for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
			var entry map[string]any
			if err := json.Unmarshal(line, &entry); err != nil {
				continue
			}
			if entry["saga_id"] == "saga-1" && entry["status"] == string(saga.SagaStatusCompleted) {
				foundCompleted = true
			}
		}
		if !foundCompleted {
			t.Errorf("expected log entry with saga_id=saga-1 status=COMPLETED; got:\n%s", buf.String())
		}
	})

	t.Run("step failed emits error log with step name", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		log := zerolog.New(&buf)

		s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "log2.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0), engine.WithLogger(log))

		fwd, _ := stepServer(t, http.StatusInternalServerError)
		seedSaga(t, s, []saga.StepDefinition{
			{Name: "charge", ForwardURL: fwd.URL, CompensateURL: fwd.URL},
		})

		ctx := log.WithContext(context.Background())
		_, _ = eng.Start(ctx, "saga-1")

		var foundStepFailed bool
		for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n")) {
			var entry map[string]any
			if err := json.Unmarshal(line, &entry); err != nil {
				continue
			}
			if entry["step"] == "charge" && entry["status"] == string(saga.StepStatusFailed) {
				foundStepFailed = true
			}
		}
		if !foundStepFailed {
			t.Errorf("expected log entry with step=charge status=FAILED; got:\n%s", buf.String())
		}
	})
}

// fakeRecorder is a thread-safe Recorder that captures calls for assertion.
type fakeRecorder struct {
	mu    sync.Mutex
	sagas []sagaRecord
	steps []stepRecord
}

type sagaRecord struct {
	status      saga.SagaStatus
	durationSec float64
}

type stepRecord struct {
	status      saga.StepStatus
	durationSec float64
}

func (r *fakeRecorder) RecordSaga(status saga.SagaStatus, durationSecs float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sagas = append(r.sagas, sagaRecord{status, durationSecs})
}

func (r *fakeRecorder) RecordStep(status saga.StepStatus, durationSecs float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.steps = append(r.steps, stepRecord{status, durationSecs})
}

func TestEngineRecorder(t *testing.T) {
	t.Parallel()

	newEngineWithRecorder := func(t *testing.T, rec engine.Recorder) (*engine.Engine, *store.BoltStore) {
		t.Helper()
		s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "rec.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })
		eng := engine.New(s,
			engine.WithRetryBackoff(time.Millisecond),
			engine.WithDefaultMaxRetries(0),
			engine.WithRecorder(rec),
		)
		return eng, s
	}

	t.Run("saga completed records saga COMPLETED and all steps SUCCEEDED", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRecorder{}
		eng, s := newEngineWithRecorder(t, rec)

		fwd, _ := stepServer(t, http.StatusOK)
		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step1", ForwardURL: fwd.URL, CompensateURL: fwd.URL},
			{Name: "step2", ForwardURL: fwd.URL, CompensateURL: fwd.URL},
		})

		if _, err := eng.Start(context.Background(), "saga-1"); err != nil {
			t.Fatalf("Start: %v", err)
		}

		rec.mu.Lock()
		defer rec.mu.Unlock()

		if len(rec.sagas) != 1 {
			t.Fatalf("want 1 saga record, got %d", len(rec.sagas))
		}
		if rec.sagas[0].status != saga.SagaStatusCompleted {
			t.Errorf("saga status: want COMPLETED, got %s", rec.sagas[0].status)
		}
		if rec.sagas[0].durationSec <= 0 {
			t.Errorf("saga duration should be positive, got %f", rec.sagas[0].durationSec)
		}

		if len(rec.steps) != 2 {
			t.Fatalf("want 2 step records, got %d", len(rec.steps))
		}
		for _, sr := range rec.steps {
			if sr.status != saga.StepStatusSucceeded {
				t.Errorf("step status: want SUCCEEDED, got %s", sr.status)
			}
		}
	})

	t.Run("step fails records step FAILED then saga FAILED", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRecorder{}
		eng, s := newEngineWithRecorder(t, rec)

		ok, _ := stepServer(t, http.StatusOK)
		fail, _ := stepServer(t, http.StatusInternalServerError)
		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step1", ForwardURL: ok.URL, CompensateURL: ok.URL},
			{Name: "step2", ForwardURL: fail.URL, CompensateURL: ok.URL},
		})

		if _, err := eng.Start(context.Background(), "saga-1"); err != nil {
			t.Fatalf("Start: %v", err)
		}

		rec.mu.Lock()
		defer rec.mu.Unlock()

		if len(rec.sagas) != 1 {
			t.Fatalf("want 1 saga record, got %d", len(rec.sagas))
		}
		if rec.sagas[0].status != saga.SagaStatusFailed {
			t.Errorf("saga status: want FAILED, got %s", rec.sagas[0].status)
		}

		var stepFailed, stepSucceeded, stepCompensated int
		for _, sr := range rec.steps {
			switch sr.status {
			case saga.StepStatusFailed:
				stepFailed++
			case saga.StepStatusSucceeded:
				stepSucceeded++
			case saga.StepStatusCompensated:
				stepCompensated++
			}
		}
		if stepFailed != 1 {
			t.Errorf("want 1 FAILED step record, got %d", stepFailed)
		}
		if stepSucceeded != 1 {
			t.Errorf("want 1 SUCCEEDED step record, got %d", stepSucceeded)
		}
		if stepCompensated != 1 {
			t.Errorf("want 1 COMPENSATED step record, got %d", stepCompensated)
		}
	})

	t.Run("saga aborted records saga ABORTED", func(t *testing.T) {
		t.Parallel()
		rec := &fakeRecorder{}
		eng, s := newEngineWithRecorder(t, rec)

		fwd, _ := stepServer(t, http.StatusOK)
		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step1", ForwardURL: fwd.URL, CompensateURL: fwd.URL},
		})

		if _, err := eng.Abort(context.Background(), "saga-1"); err != nil {
			t.Fatalf("Abort: %v", err)
		}

		rec.mu.Lock()
		defer rec.mu.Unlock()

		if len(rec.sagas) != 1 {
			t.Fatalf("want 1 saga record, got %d", len(rec.sagas))
		}
		if rec.sagas[0].status != saga.SagaStatusAborted {
			t.Errorf("saga status: want ABORTED, got %s", rec.sagas[0].status)
		}
	})
}

func TestEngineTokenSource(t *testing.T) {
	t.Parallel()

	t.Run("token is sent as Authorization header", func(t *testing.T) {
		t.Parallel()

		var gotHeader string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotHeader = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)

		s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "tok.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })

		ts := &staticTokenSource{token: "test-bearer-token"}
		eng := engine.New(s,
			engine.WithRetryBackoff(time.Millisecond),
			engine.WithDefaultMaxRetries(0),
			engine.WithTokenSource(ts),
		)

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step1", ForwardURL: srv.URL, CompensateURL: srv.URL},
		})

		if _, err := eng.Start(context.Background(), "saga-1"); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if gotHeader != "Bearer test-bearer-token" {
			t.Errorf("Authorization header: want %q, got %q", "Bearer test-bearer-token", gotHeader)
		}
	})

	t.Run("nil token source sends no Authorization header", func(t *testing.T) {
		t.Parallel()

		var gotHeader string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotHeader = r.Header.Get("Authorization")
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)

		eng, s := newEngine(t)
		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step1", ForwardURL: srv.URL, CompensateURL: srv.URL},
		})

		if _, err := eng.Start(context.Background(), "saga-1"); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if gotHeader != "" {
			t.Errorf("expected no Authorization header, got %q", gotHeader)
		}
	})

	t.Run("token source error fails the step", func(t *testing.T) {
		t.Parallel()

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(srv.Close)

		s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "tokerr.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = s.Close() })

		ts := &errorTokenSource{err: errors.New("token unavailable")}
		eng := engine.New(s,
			engine.WithRetryBackoff(time.Millisecond),
			engine.WithDefaultMaxRetries(0),
			engine.WithTokenSource(ts),
		)

		seedSaga(t, s, []saga.StepDefinition{
			{Name: "step1", ForwardURL: srv.URL, CompensateURL: srv.URL},
		})

		exec, _ := eng.Start(context.Background(), "saga-1")
		if exec == nil {
			t.Fatal("expected non-nil exec")
		}
		if exec.Status != saga.SagaStatusFailed {
			t.Errorf("saga status: want FAILED, got %s", exec.Status)
		}
	})
}

// staticTokenSource always returns the same token.
type staticTokenSource struct{ token string }

func (s *staticTokenSource) Token(_ context.Context, _ string, _ engine.StepAuthContext) (string, error) {
	return s.token, nil
}

// errorTokenSource always returns an error.
type errorTokenSource struct{ err error }

func (e *errorTokenSource) Token(_ context.Context, _ string, _ engine.StepAuthContext) (string, error) {
	return "", e.err
}
