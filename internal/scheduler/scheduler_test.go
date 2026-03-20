package scheduler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/ngaddam369/saga-conductor/internal/engine"
	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/scheduler"
	"github.com/ngaddam369/saga-conductor/internal/store"
)

func newStore(t *testing.T) *store.BoltStore {
	t.Helper()
	s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func okServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestScheduler(t *testing.T) {
	t.Parallel()

	t.Run("NoInFlightSagas_RunSucceeds", func(t *testing.T) {
		t.Parallel()
		s := newStore(t)
		eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0))
		sched := scheduler.New(s, eng, zerolog.Nop())
		if err := sched.Run(context.Background()); err != nil {
			t.Errorf("Run: %v", err)
		}
	})

	t.Run("ResumeRunning", func(t *testing.T) {
		t.Parallel()

		t.Run("DrivesToCompleted", func(t *testing.T) {
			t.Parallel()
			s := newStore(t)
			ok := okServer(t)

			defs := []saga.StepDefinition{
				{Name: "s1", ForwardURL: ok.URL, CompensateURL: ok.URL},
				{Name: "s2", ForwardURL: ok.URL, CompensateURL: ok.URL},
			}
			exec := &saga.Execution{
				ID: "saga-run", Name: "run",
				Status:   saga.SagaStatusRunning,
				StepDefs: defs,
				Steps: []saga.StepExecution{
					{Name: "s1", Status: saga.StepStatusSucceeded},
					{Name: "s2", Status: saga.StepStatusPending},
				},
				Payload:   []byte(`{}`),
				CreatedAt: time.Now().UTC(),
			}
			if err := s.Create(context.Background(), exec); err != nil {
				t.Fatalf("Create: %v", err)
			}

			eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0))
			sched := scheduler.New(s, eng, zerolog.Nop())
			if err := sched.Run(context.Background()); err != nil {
				t.Fatalf("Run: %v", err)
			}

			got, err := s.Get(context.Background(), "saga-run")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Status != saga.SagaStatusCompleted {
				t.Errorf("Status: got %s, want COMPLETED", got.Status)
			}
		})

		t.Run("DrivesToFailed_WhenStepFails", func(t *testing.T) {
			t.Parallel()
			s := newStore(t)
			fail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			}))
			t.Cleanup(fail.Close)
			ok := okServer(t)

			defs := []saga.StepDefinition{
				{Name: "s1", ForwardURL: fail.URL, CompensateURL: ok.URL},
			}
			exec := &saga.Execution{
				ID: "saga-fail", Name: "fail",
				Status:   saga.SagaStatusRunning,
				StepDefs: defs,
				Steps:    []saga.StepExecution{{Name: "s1", Status: saga.StepStatusPending}},
				Payload:  []byte(`{}`), CreatedAt: time.Now().UTC(),
			}
			if err := s.Create(context.Background(), exec); err != nil {
				t.Fatalf("Create: %v", err)
			}

			eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0))
			sched := scheduler.New(s, eng, zerolog.Nop())
			if err := sched.Run(context.Background()); err != nil {
				t.Fatalf("Run: %v", err)
			}

			got, err := s.Get(context.Background(), "saga-fail")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Status != saga.SagaStatusFailed {
				t.Errorf("Status: got %s, want FAILED", got.Status)
			}
		})
	})

	t.Run("ResumeCompensating", func(t *testing.T) {
		t.Parallel()

		t.Run("DrivesToFailed", func(t *testing.T) {
			t.Parallel()
			s := newStore(t)
			ok := okServer(t)

			defs := []saga.StepDefinition{
				{Name: "s1", ForwardURL: ok.URL, CompensateURL: ok.URL},
				{Name: "s2", ForwardURL: ok.URL, CompensateURL: ok.URL},
			}
			exec := &saga.Execution{
				ID: "saga-comp", Name: "comp",
				Status:   saga.SagaStatusCompensating,
				StepDefs: defs,
				Steps: []saga.StepExecution{
					{Name: "s1", Status: saga.StepStatusSucceeded},
					{Name: "s2", Status: saga.StepStatusFailed},
				},
				FailedStep: "s2",
				Payload:    []byte(`{}`), CreatedAt: time.Now().UTC(),
			}
			if err := s.Create(context.Background(), exec); err != nil {
				t.Fatalf("Create: %v", err)
			}

			eng := engine.New(s, engine.WithRetryBackoff(time.Millisecond), engine.WithDefaultMaxRetries(0))
			sched := scheduler.New(s, eng, zerolog.Nop())
			if err := sched.Run(context.Background()); err != nil {
				t.Fatalf("Run: %v", err)
			}

			got, err := s.Get(context.Background(), "saga-comp")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if got.Status != saga.SagaStatusFailed {
				t.Errorf("Status: got %s, want FAILED", got.Status)
			}
			if got.Steps[0].Status != saga.StepStatusCompensated {
				t.Errorf("s1 status: got %s, want COMPENSATED", got.Steps[0].Status)
			}
		})
	})
}
