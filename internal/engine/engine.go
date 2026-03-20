// Package engine implements the saga state machine and synchronous HTTP step
// executor. It is the core of saga-conductor: given a saga execution persisted
// in a Store it advances the saga step-by-step, compensates on failure, and
// keeps the store in sync after every state transition.
package engine

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/store"
)

// ErrDraining is returned by Start when the engine is draining for shutdown.
var ErrDraining = errors.New("engine draining for shutdown")

const (
	storeMaxRetries     = 3
	defaultRetryBackoff = 500 * time.Millisecond
)

// Option configures an Engine.
type Option func(*Engine)

// WithRetryBackoff sets the backoff between store.Update retry attempts.
// The default is 500ms. Tests may pass a smaller value to keep runs fast.
func WithRetryBackoff(d time.Duration) Option {
	return func(e *Engine) {
		e.retryBackoff = d
	}
}

// WithDefaultMaxRetries overrides the engine-wide default number of HTTP step
// retries. Tests set this to 0 to disable retries and keep existing test
// behaviour unchanged.
func WithDefaultMaxRetries(n int) Option {
	return func(e *Engine) {
		e.defaultMaxRetries = n
	}
}

// WithTokenSource attaches a TokenSource used to inject Authorization headers
// into outbound step HTTP calls. Defaults to nil (no auth header added).
// Implementations must be safe for concurrent use.
func WithTokenSource(ts TokenSource) Option {
	return func(e *Engine) {
		e.tokenSource = ts
	}
}

// WithRecorder attaches a Recorder that is called after each saga and step
// terminal transition. Defaults to nil (no-op). Implementations must be safe
// for concurrent use.
func WithRecorder(r Recorder) Option {
	return func(e *Engine) {
		e.recorder = r
	}
}

// WithLogger sets the logger used for engine-level events (e.g. best-effort
// failure recording). Request-scoped logs use the logger embedded in the
// context via zerolog.Ctx. Defaults to zerolog.Nop() so tests are silent.
func WithLogger(l zerolog.Logger) Option {
	return func(e *Engine) {
		e.log = l
	}
}

// Engine executes sagas stored in a Store.
type Engine struct {
	store              store.Store
	httpClient         *http.Client
	defaultTimeout     time.Duration
	retryBackoff       time.Duration
	defaultMaxRetries  int
	defaultRetryBaseMs int
	sagaTimeoutSecs    int
	log                zerolog.Logger
	recorder           Recorder
	tokenSource        TokenSource

	// Graceful-shutdown fields.
	// draining is set by Drain() to prevent new Start() calls.
	// inflightWg counts active Start() goroutines.
	// inflight maps saga ID → struct{} for the in-flight set (used for logging on timeout).
	draining   atomic.Bool
	inflightWg sync.WaitGroup
	inflight   sync.Map
}

// New returns an Engine backed by the given store.
func New(s store.Store, opts ...Option) *Engine {
	timeout := 30 * time.Second
	if v := os.Getenv("STEP_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			timeout = time.Duration(n) * time.Second
		}
	}

	transport := &http.Transport{
		DialContext:     (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		MaxIdleConns:    100,
		MaxConnsPerHost: 10,
		IdleConnTimeout: 90 * time.Second,
	}
	maxRetries := 3
	if v := os.Getenv("STEP_MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxRetries = n
		}
	}
	retryBaseMs := 100
	if v := os.Getenv("STEP_RETRY_BACKOFF_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			retryBaseMs = n
		}
	}

	sagaTimeoutSecs := 3600
	if v := os.Getenv("SAGA_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			sagaTimeoutSecs = n
		}
	}

	eng := &Engine{
		store:              s,
		defaultTimeout:     timeout,
		retryBackoff:       defaultRetryBackoff,
		defaultMaxRetries:  maxRetries,
		defaultRetryBaseMs: retryBaseMs,
		sagaTimeoutSecs:    sagaTimeoutSecs,
		log:                zerolog.Nop(),
		httpClient: &http.Client{
			Transport: transport,
			// Never follow redirects — a 3xx to file:// or an internal service
			// is a silent SSRF vector.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
	for _, o := range opts {
		o(eng)
	}
	return eng
}

// recordSaga calls e.recorder.RecordSaga if a recorder is set.
func (e *Engine) recordSaga(exec *saga.Execution) {
	if e.recorder == nil {
		return
	}
	var dur float64
	if exec.StartedAt != nil && exec.CompletedAt != nil {
		dur = exec.CompletedAt.Sub(*exec.StartedAt).Seconds()
	}
	e.recorder.RecordSaga(exec.Status, dur)
}

// recordStep calls e.recorder.RecordStep if a recorder is set.
func (e *Engine) recordStep(step *saga.StepExecution) {
	if e.recorder == nil {
		return
	}
	var dur float64
	if step.StartedAt != nil && step.CompletedAt != nil {
		dur = step.CompletedAt.Sub(*step.StartedAt).Seconds()
	}
	e.recorder.RecordStep(step.Status, dur)
}

// sagaDurationMs returns the elapsed milliseconds between exec.StartedAt and
// completedAt, or -1 if StartedAt is nil (e.g. resumed from COMPENSATING without
// a prior RUNNING record in the same process).
func sagaDurationMs(exec *saga.Execution, completedAt time.Time) int64 {
	if exec.StartedAt == nil {
		return -1
	}
	return completedAt.Sub(*exec.StartedAt).Milliseconds()
}

// stepDurationMs returns the step's wall-clock duration in milliseconds.
// Returns -1 when StartedAt is nil (e.g. a step that was already in RUNNING
// state when Resume loaded it, meaning it crashed before StartedAt was set).
func stepDurationMs(step *saga.StepExecution, completedAt time.Time) int64 {
	if step.StartedAt == nil {
		return -1
	}
	return completedAt.Sub(*step.StartedAt).Milliseconds()
}

// updateWithRetry retries store.Update up to storeMaxRetries times with fixed
// backoff. If the caller's context is done between retries the last store error
// is returned immediately without waiting for the next attempt.
func (e *Engine) updateWithRetry(ctx context.Context, exec *saga.Execution) error {
	var err error
	for i := range storeMaxRetries {
		if err = e.store.Update(ctx, exec); err == nil {
			return nil
		}
		if i < storeMaxRetries-1 {
			select {
			case <-ctx.Done():
				return err
			case <-time.After(e.retryBackoff):
			}
		}
	}
	return err
}

// markFailedBestEffort makes one final attempt to write a FAILED terminal state
// using a fresh background context (the caller's context may already be done).
// If the write also fails it logs to stderr so operators can manually recover.
func (e *Engine) markFailedBestEffort(exec *saga.Execution, step, cause string) {
	now := time.Now().UTC()
	exec.Status = saga.SagaStatusFailed
	exec.CompletedAt = &now
	if step != "" {
		exec.FailedStep = step
	}
	if err := e.store.Update(context.Background(), exec); err != nil {
		e.log.Error().
			Str("saga_id", exec.ID).
			Str("step", step).
			Str("cause", cause).
			Msg("store unavailable during best-effort FAILED write; manual recovery required")
		return
	}
	e.recordSaga(exec)
}

// Abort forcibly transitions a non-terminal saga to ABORTED and persists the
// terminal state. It is an operator escape hatch for sagas stuck in PENDING,
// RUNNING, or COMPENSATING with no active caller.
//
// Abort deliberately does NOT check the draining flag. During shutdown an
// operator may still want to abort a stuck saga; Abort does no forward work
// and does not block shutdown.
//
// If the saga is RUNNING or COMPENSATING an active engine goroutine (Start or
// Resume) may still be executing against it. Abort cannot cancel that goroutine,
// so its subsequent store.Update calls may race with the ABORTED write. This is
// accepted: Abort is best-effort for in-flight sagas and is designed primarily
// for truly stuck sagas that have no active caller.
//
// No compensation pass is triggered by Abort. If the saga was RUNNING and some
// steps had succeeded, those steps' compensations will not be called. The
// operator is taking explicit responsibility for any cleanup.
func (e *Engine) Abort(ctx context.Context, id string) (*saga.Execution, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context already done: %w", err)
	}

	exec, err := e.store.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get saga: %w", err)
	}

	switch exec.Status {
	case saga.SagaStatusCompleted:
		return nil, store.ErrAlreadyCompleted
	case saga.SagaStatusFailed, saga.SagaStatusCompensationFailed:
		return nil, store.ErrAlreadyFailed
	case saga.SagaStatusAborted:
		return nil, store.ErrAlreadyAborted
	}

	if err = saga.ValidateTransition(exec.Status, saga.SagaStatusAborted); err != nil {
		return nil, fmt.Errorf("state machine: %w", err)
	}

	now := time.Now().UTC()
	exec.Status = saga.SagaStatusAborted
	exec.CompletedAt = &now
	if err = e.store.Update(ctx, exec); err != nil {
		return nil, fmt.Errorf("persist ABORTED: %w", err)
	}
	zerolog.Ctx(ctx).Info().Str("saga_id", id).Str("status", string(saga.SagaStatusAborted)).Msg("saga aborted")
	e.recordSaga(exec)
	return exec, nil
}

// Resume re-drives a saga that was left in RUNNING or COMPENSATING state after
// a process crash. Unlike Start(), it does not require the saga to be PENDING
// and skips steps that already reached a terminal state in the previous run.
//
// Steps in RUNNING state (crashed mid-HTTP call) are retried from scratch;
// steps in SUCCEEDED state are skipped; steps in COMPENSATING state
// (crashed mid-compensation) have their compensation call retried.
//
// If the saga is already in a terminal state (COMPLETED or FAILED), Resume is
// a no-op and returns the current execution without touching the store.
//
// Resume deliberately does NOT participate in the inflight WaitGroup. The
// scheduler calls Resume synchronously at startup, before the gRPC server
// starts accepting connections and before Drain() can ever be invoked, so
// there is no shutdown race to guard against.
func (e *Engine) Resume(ctx context.Context, id string) (*saga.Execution, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context already done: %w", err)
	}

	exec, err := e.store.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get saga: %w", err)
	}

	switch exec.Status {
	case saga.SagaStatusCompleted, saga.SagaStatusFailed,
		saga.SagaStatusAborted, saga.SagaStatusCompensationFailed:
		return exec, nil // already terminal — nothing to do
	case saga.SagaStatusRunning, saga.SagaStatusCompensating:
		// handled below
	default:
		return nil, fmt.Errorf("saga %s: cannot resume from status %s", id, exec.Status)
	}

	log := zerolog.Ctx(ctx).With().Str("saga_id", id).Logger()
	log.Info().Str("status", string(exec.Status)).Msg("saga resumed after crash")

	if len(exec.Steps) != len(exec.StepDefs) {
		return nil, fmt.Errorf("saga %s: corrupted — Steps length %d != StepDefs length %d",
			id, len(exec.Steps), len(exec.StepDefs))
	}

	sagaCtx := ctx
	var sagaCancel context.CancelFunc
	timeoutSecs := e.sagaTimeoutSecs
	if exec.SagaTimeoutSeconds > 0 {
		timeoutSecs = exec.SagaTimeoutSeconds
	}
	if timeoutSecs > 0 {
		sagaCtx, sagaCancel = context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
		defer sagaCancel()
	}

	failedIdx := -1

	if exec.Status == saga.SagaStatusRunning {
		for i := range exec.Steps {
			step := &exec.Steps[i]
			def := exec.StepDefs[i]

			switch step.Status {
			case saga.StepStatusSucceeded:
				continue // already committed — skip
			case saga.StepStatusFailed:
				// Crashed after persisting step FAILED but before transitioning
				// saga to COMPENSATING. Proceed directly to compensation.
				failedIdx = i
				goto compensate
			}

			// Step is PENDING or RUNNING (crashed mid-HTTP).
			if step.Status == saga.StepStatusPending {
				t := time.Now().UTC()
				if err = saga.ValidateStepTransition(step.Status, saga.StepStatusRunning); err != nil {
					return nil, fmt.Errorf("state machine: %w", err)
				}
				step.Status = saga.StepStatusRunning
				step.StartedAt = &t
				if err = e.updateWithRetry(sagaCtx, exec); err != nil {
					e.markFailedBestEffort(exec, step.Name, err.Error())
					return nil, fmt.Errorf("persist step RUNNING: %w", err)
				}
			}
			// For RUNNING: the step is already marked running; just retry HTTP.

			var stepDetail *saga.StepError
			stepDetail, err = e.callHTTPWithRetry(sagaCtx, def.ForwardURL, exec.Payload, def)

			t := time.Now().UTC()
			step.CompletedAt = &t

			if err != nil {
				if verr := saga.ValidateStepTransition(step.Status, saga.StepStatusFailed); verr != nil {
					return nil, fmt.Errorf("state machine: %w", verr)
				}
				step.Status = saga.StepStatusFailed
				step.Error = err.Error()
				step.ErrorDetail = stepDetail
				exec.FailedStep = step.Name
				if uerr := e.updateWithRetry(sagaCtx, exec); uerr != nil {
					e.markFailedBestEffort(exec, step.Name, uerr.Error())
					return nil, fmt.Errorf("persist step FAILED: %w", uerr)
				}
				log.Error().Str("step", step.Name).Str("status", string(saga.StepStatusFailed)).
					Int64("duration_ms", stepDurationMs(step, *step.CompletedAt)).
					Str("error", err.Error()).Msg("step failed")
				e.recordStep(step)
				failedIdx = i
				break
			}

			if verr := saga.ValidateStepTransition(step.Status, saga.StepStatusSucceeded); verr != nil {
				return nil, fmt.Errorf("state machine: %w", verr)
			}
			step.Status = saga.StepStatusSucceeded
			if err = e.updateWithRetry(sagaCtx, exec); err != nil {
				e.markFailedBestEffort(exec, step.Name, err.Error())
				return nil, fmt.Errorf("persist step SUCCEEDED: %w", err)
			}
			log.Info().Str("step", step.Name).Str("status", string(saga.StepStatusSucceeded)).
				Int64("duration_ms", stepDurationMs(step, *step.CompletedAt)).Msg("step succeeded")
			e.recordStep(step)
		}

		if failedIdx == -1 {
			// All steps succeeded.
			completed := time.Now().UTC()
			if err = saga.ValidateTransition(exec.Status, saga.SagaStatusCompleted); err != nil {
				return nil, fmt.Errorf("state machine: %w", err)
			}
			exec.Status = saga.SagaStatusCompleted
			exec.CompletedAt = &completed
			if err = e.updateWithRetry(sagaCtx, exec); err != nil {
				e.markFailedBestEffort(exec, "", err.Error())
				return nil, fmt.Errorf("persist COMPLETED: %w", err)
			}
			log.Info().Str("status", string(saga.SagaStatusCompleted)).
				Int64("duration_ms", sagaDurationMs(exec, *exec.CompletedAt)).Msg("saga completed")
			e.recordSaga(exec)
			return exec, nil
		}
	}

compensate:
	compCtx := sagaCtx
	if sagaCtx.Err() != nil && ctx.Err() == nil {
		compCtx = context.Background()
	}

	if exec.Status == saga.SagaStatusRunning {
		// Transition to COMPENSATING (was RUNNING when we found the failed step).
		if err = saga.ValidateTransition(exec.Status, saga.SagaStatusCompensating); err != nil {
			return nil, fmt.Errorf("state machine: %w", err)
		}
		exec.Status = saga.SagaStatusCompensating
		if err = e.updateWithRetry(compCtx, exec); err != nil {
			e.markFailedBestEffort(exec, exec.FailedStep, err.Error())
			return nil, fmt.Errorf("persist COMPENSATING: %w", err)
		}
	}
	// else exec.Status is already COMPENSATING (loaded from store in that state).

	// Determine where to begin the compensation sweep. Prefer failedIdx when we
	// just ran the forward pass; otherwise locate the FAILED step by name.
	startFrom := failedIdx
	if startFrom == -1 {
		for i, step := range exec.Steps {
			if step.Status == saga.StepStatusFailed {
				startFrom = i
				break
			}
		}
	}
	if startFrom == -1 {
		startFrom = len(exec.Steps)
	}

	for i := startFrom - 1; i >= 0; i-- {
		step := &exec.Steps[i]
		def := exec.StepDefs[i]

		// Skip steps that already reached a terminal compensation state.
		if step.Status == saga.StepStatusCompensated ||
			step.Status == saga.StepStatusCompensationFailed {
			continue
		}
		// Skip steps that were never executed (should not normally occur, but
		// guards against unexpected store state).
		if step.Status != saga.StepStatusSucceeded &&
			step.Status != saga.StepStatusCompensating {
			continue
		}

		if step.Status == saga.StepStatusSucceeded {
			if verr := saga.ValidateStepTransition(step.Status, saga.StepStatusCompensating); verr != nil {
				return nil, fmt.Errorf("state machine: %w", verr)
			}
			step.Status = saga.StepStatusCompensating
			if err = e.updateWithRetry(compCtx, exec); err != nil {
				e.markFailedBestEffort(exec, step.Name, err.Error())
				return nil, fmt.Errorf("persist step COMPENSATING: %w", err)
			}
		}
		// For COMPENSATING: already marked; just retry the HTTP call.

		compStartedAt := time.Now().UTC()
		compDetail, compErr := e.callHTTPWithRetry(compCtx, def.CompensateURL, exec.Payload, def)

		t := time.Now().UTC()
		step.CompletedAt = &t

		if compErr != nil {
			if verr := saga.ValidateStepTransition(step.Status, saga.StepStatusCompensationFailed); verr != nil {
				return nil, fmt.Errorf("state machine: %w", verr)
			}
			step.Status = saga.StepStatusCompensationFailed
			step.Error = fmt.Sprintf("compensation failed: %s", compErr)
			step.ErrorDetail = compDetail

			// Dead-letter the saga immediately. Continuing to compensate later
			// steps after one compensation has failed would leave the system in
			// an even more inconsistent state. Operators must intervene.
			if err = saga.ValidateTransition(exec.Status, saga.SagaStatusCompensationFailed); err != nil {
				return nil, fmt.Errorf("state machine: %w", err)
			}
			now := time.Now().UTC()
			exec.Status = saga.SagaStatusCompensationFailed
			exec.CompletedAt = &now
			if err = e.updateWithRetry(compCtx, exec); err != nil {
				e.markFailedBestEffort(exec, step.Name, err.Error())
				return nil, fmt.Errorf("persist COMPENSATION_FAILED: %w", err)
			}
			log.Error().Str("step", step.Name).Str("status", string(saga.StepStatusCompensationFailed)).
				Int64("duration_ms", t.Sub(compStartedAt).Milliseconds()).
				Str("error", compErr.Error()).Msg("step compensation failed; saga dead-lettered")
			e.recordStep(step)
			e.recordSaga(exec)
			return exec, fmt.Errorf("compensation halted: step %q exhausted retries: %s", step.Name, compErr)
		}

		if verr := saga.ValidateStepTransition(step.Status, saga.StepStatusCompensated); verr != nil {
			return nil, fmt.Errorf("state machine: %w", verr)
		}
		step.Status = saga.StepStatusCompensated

		if err = e.updateWithRetry(compCtx, exec); err != nil {
			e.markFailedBestEffort(exec, step.Name, err.Error())
			return nil, fmt.Errorf("persist compensation result: %w", err)
		}
		log.Info().Str("step", step.Name).Str("status", string(saga.StepStatusCompensated)).
			Int64("duration_ms", t.Sub(compStartedAt).Milliseconds()).Msg("step compensated")
		e.recordStep(step)
	}

	failed := time.Now().UTC()
	if err = saga.ValidateTransition(exec.Status, saga.SagaStatusFailed); err != nil {
		return nil, fmt.Errorf("state machine: %w", err)
	}
	exec.Status = saga.SagaStatusFailed
	exec.CompletedAt = &failed
	if err = e.updateWithRetry(compCtx, exec); err != nil {
		e.markFailedBestEffort(exec, exec.FailedStep, err.Error())
		return nil, fmt.Errorf("persist FAILED: %w", err)
	}
	log.Info().Str("status", string(saga.SagaStatusFailed)).Str("failed_step", exec.FailedStep).
		Int64("duration_ms", sagaDurationMs(exec, *exec.CompletedAt)).Msg("saga failed")
	e.recordSaga(exec)

	if sagaCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
		return exec, fmt.Errorf("saga timed out after %ds: %w", timeoutSecs, context.DeadlineExceeded)
	}
	return exec, nil
}

// Start transitions a PENDING saga to RUNNING and executes all steps in order.
// If any step fails, compensation runs automatically in reverse for all
// previously succeeded steps. Start blocks until the saga reaches a terminal
// state (COMPLETED or FAILED) and then returns the final execution.
//
// If SAGA_TIMEOUT_SECONDS is configured, a saga-level deadline wraps the
// entire forward execution. On timeout the current step fails immediately
// via context cancellation, then compensation runs on a fresh context so
// previously succeeded steps can still be rolled back.

// Drain signals the engine to stop accepting new Start calls and waits for all
// in-flight sagas to finish. If ctx expires before they all finish, Drain
// returns the IDs of any sagas still running at that point; the background
// scheduler will resume them on next startup.
func (e *Engine) Drain(ctx context.Context) []string {
	e.draining.Store(true)

	done := make(chan struct{})
	go func() {
		e.inflightWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		var interrupted []string
		e.inflight.Range(func(k, _ any) bool {
			if id, ok := k.(string); ok {
				interrupted = append(interrupted, id)
			}
			return true
		})
		return interrupted
	}
}

func (e *Engine) Start(ctx context.Context, id string) (*saga.Execution, error) {
	// Add to the WaitGroup BEFORE checking the draining flag. This closes the
	// race where Drain() sets draining=true and calls Wait() while the counter
	// is still 0: if a concurrent Start() checked the flag first (false), it
	// will increment the counter before Drain's Wait() goroutine is scheduled,
	// so Drain blocks until this Start() exits. If Drain's Wait() returned
	// already (counter was 0), this Start() still checks the flag below and
	// returns ErrDraining without running any saga — no harm done.
	e.inflightWg.Add(1)
	defer e.inflightWg.Done()

	if e.draining.Load() {
		return nil, ErrDraining
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context already done: %w", err)
	}

	// Register the saga ID for interrupted-saga reporting on drain timeout.
	// Done only after passing the draining check to avoid false positives in
	// the list returned by Drain() when it times out.
	e.inflight.Store(id, struct{}{})
	defer e.inflight.Delete(id)

	exec, err := e.store.TransitionToRunning(ctx, id, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("transition to RUNNING: %w", err)
	}
	if len(exec.Steps) != len(exec.StepDefs) {
		return nil, fmt.Errorf("saga %s: corrupted — Steps length %d != StepDefs length %d", id, len(exec.Steps), len(exec.StepDefs))
	}

	log := zerolog.Ctx(ctx).With().Str("saga_id", id).Logger()
	log.Info().Str("status", string(saga.SagaStatusRunning)).Msg("saga started")

	// Derive a saga-scoped deadline for forward execution. This caps total saga
	// duration independently of per-step timeouts (STEP_TIMEOUT_SECONDS).
	// Per-saga timeout (SagaTimeoutSeconds) takes precedence over the global env var.
	sagaCtx := ctx
	var sagaCancel context.CancelFunc
	timeoutSecs := e.sagaTimeoutSecs
	if exec.SagaTimeoutSeconds > 0 {
		timeoutSecs = exec.SagaTimeoutSeconds
	}
	if timeoutSecs > 0 {
		sagaCtx, sagaCancel = context.WithTimeout(ctx, time.Duration(timeoutSecs)*time.Second)
		defer sagaCancel()
	}

	failedIdx := -1
	for i := range exec.Steps {
		step := &exec.Steps[i]
		def := exec.StepDefs[i]

		t := time.Now().UTC()
		if err = saga.ValidateStepTransition(step.Status, saga.StepStatusRunning); err != nil {
			return nil, fmt.Errorf("state machine: %w", err)
		}
		step.Status = saga.StepStatusRunning
		step.StartedAt = &t
		if err = e.updateWithRetry(sagaCtx, exec); err != nil {
			e.markFailedBestEffort(exec, step.Name, err.Error())
			return nil, fmt.Errorf("persist step RUNNING: %w", err)
		}
		log.Info().Str("step", step.Name).Str("status", string(saga.StepStatusRunning)).Msg("step started")

		var stepDetail *saga.StepError
		stepDetail, err = e.callHTTPWithRetry(sagaCtx, def.ForwardURL, exec.Payload, def)

		t = time.Now().UTC()
		step.CompletedAt = &t

		if err != nil {
			if verr := saga.ValidateStepTransition(step.Status, saga.StepStatusFailed); verr != nil {
				return nil, fmt.Errorf("state machine: %w", verr)
			}
			step.Status = saga.StepStatusFailed
			step.Error = err.Error()
			step.ErrorDetail = stepDetail
			exec.FailedStep = step.Name
			if uerr := e.updateWithRetry(sagaCtx, exec); uerr != nil {
				e.markFailedBestEffort(exec, step.Name, uerr.Error())
				return nil, fmt.Errorf("persist step FAILED: %w", uerr)
			}
			log.Error().Str("step", step.Name).Str("status", string(saga.StepStatusFailed)).
				Int64("duration_ms", step.CompletedAt.Sub(*step.StartedAt).Milliseconds()).
				Str("error", err.Error()).Msg("step failed")
			e.recordStep(step)
			failedIdx = i
			break
		}

		if err = saga.ValidateStepTransition(step.Status, saga.StepStatusSucceeded); err != nil {
			return nil, fmt.Errorf("state machine: %w", err)
		}
		step.Status = saga.StepStatusSucceeded
		if err = e.updateWithRetry(sagaCtx, exec); err != nil {
			e.markFailedBestEffort(exec, step.Name, err.Error())
			return nil, fmt.Errorf("persist step SUCCEEDED: %w", err)
		}
		log.Info().Str("step", step.Name).Str("status", string(saga.StepStatusSucceeded)).
			Int64("duration_ms", step.CompletedAt.Sub(*step.StartedAt).Milliseconds()).Msg("step succeeded")
		e.recordStep(step)
	}

	if failedIdx == -1 {
		completed := time.Now().UTC()
		if err = saga.ValidateTransition(exec.Status, saga.SagaStatusCompleted); err != nil {
			return nil, fmt.Errorf("state machine: %w", err)
		}
		exec.Status = saga.SagaStatusCompleted
		exec.CompletedAt = &completed
		if err = e.updateWithRetry(sagaCtx, exec); err != nil {
			e.markFailedBestEffort(exec, "", err.Error())
			return nil, fmt.Errorf("persist COMPLETED: %w", err)
		}
		log.Info().Str("status", string(saga.SagaStatusCompleted)).
			Int64("duration_ms", sagaDurationMs(exec, *exec.CompletedAt)).Msg("saga completed")
		e.recordSaga(exec)
		return exec, nil
	}

	// If the saga deadline fired but the caller's context is still alive,
	// switch to a fresh context for compensation so previously succeeded steps
	// can still be rolled back even though the saga timeout has expired.
	compCtx := sagaCtx
	if sagaCtx.Err() != nil && ctx.Err() == nil {
		compCtx = context.Background()
	}

	if err = saga.ValidateTransition(exec.Status, saga.SagaStatusCompensating); err != nil {
		return nil, fmt.Errorf("state machine: %w", err)
	}
	exec.Status = saga.SagaStatusCompensating
	if err = e.updateWithRetry(compCtx, exec); err != nil {
		e.markFailedBestEffort(exec, exec.FailedStep, err.Error())
		return nil, fmt.Errorf("persist COMPENSATING: %w", err)
	}
	log.Info().Str("status", string(saga.SagaStatusCompensating)).Str("failed_step", exec.FailedStep).Msg("saga compensating")

	for i := failedIdx - 1; i >= 0; i-- {
		step := &exec.Steps[i]
		def := exec.StepDefs[i]

		if step.Status != saga.StepStatusSucceeded {
			continue
		}

		if err = saga.ValidateStepTransition(step.Status, saga.StepStatusCompensating); err != nil {
			return nil, fmt.Errorf("state machine: %w", err)
		}
		step.Status = saga.StepStatusCompensating
		compStartedAt := time.Now().UTC()
		if err = e.updateWithRetry(compCtx, exec); err != nil {
			e.markFailedBestEffort(exec, step.Name, err.Error())
			return nil, fmt.Errorf("persist step COMPENSATING: %w", err)
		}
		log.Info().Str("step", step.Name).Str("status", string(saga.StepStatusCompensating)).Msg("step compensating")

		compDetail, compErr := e.callHTTPWithRetry(compCtx, def.CompensateURL, exec.Payload, def)

		t := time.Now().UTC()
		step.CompletedAt = &t

		if compErr != nil {
			if verr := saga.ValidateStepTransition(step.Status, saga.StepStatusCompensationFailed); verr != nil {
				return nil, fmt.Errorf("state machine: %w", verr)
			}
			step.Status = saga.StepStatusCompensationFailed
			step.Error = fmt.Sprintf("compensation failed: %s", compErr)
			step.ErrorDetail = compDetail

			// Dead-letter the saga immediately. Continuing to compensate later
			// steps after one compensation has failed would leave the system in
			// an even more inconsistent state. Operators must intervene.
			if err = saga.ValidateTransition(exec.Status, saga.SagaStatusCompensationFailed); err != nil {
				return nil, fmt.Errorf("state machine: %w", err)
			}
			now := time.Now().UTC()
			exec.Status = saga.SagaStatusCompensationFailed
			exec.CompletedAt = &now
			if err = e.updateWithRetry(compCtx, exec); err != nil {
				e.markFailedBestEffort(exec, step.Name, err.Error())
				return nil, fmt.Errorf("persist COMPENSATION_FAILED: %w", err)
			}
			log.Error().Str("step", step.Name).Str("status", string(saga.StepStatusCompensationFailed)).
				Int64("duration_ms", t.Sub(compStartedAt).Milliseconds()).
				Str("error", compErr.Error()).Msg("step compensation failed; saga dead-lettered")
			e.recordStep(step)
			e.recordSaga(exec)
			return exec, fmt.Errorf("compensation halted: step %q exhausted retries: %s", step.Name, compErr)
		}

		if verr := saga.ValidateStepTransition(step.Status, saga.StepStatusCompensated); verr != nil {
			return nil, fmt.Errorf("state machine: %w", verr)
		}
		step.Status = saga.StepStatusCompensated

		if err = e.updateWithRetry(compCtx, exec); err != nil {
			e.markFailedBestEffort(exec, step.Name, err.Error())
			return nil, fmt.Errorf("persist compensation result: %w", err)
		}
		log.Info().Str("step", step.Name).Str("status", string(saga.StepStatusCompensated)).
			Int64("duration_ms", t.Sub(compStartedAt).Milliseconds()).Msg("step compensated")
		e.recordStep(step)
	}

	failed := time.Now().UTC()
	if err = saga.ValidateTransition(exec.Status, saga.SagaStatusFailed); err != nil {
		return nil, fmt.Errorf("state machine: %w", err)
	}
	exec.Status = saga.SagaStatusFailed
	exec.CompletedAt = &failed
	if err = e.updateWithRetry(compCtx, exec); err != nil {
		e.markFailedBestEffort(exec, exec.FailedStep, err.Error())
		return nil, fmt.Errorf("persist FAILED: %w", err)
	}
	log.Info().Str("status", string(saga.SagaStatusFailed)).Str("failed_step", exec.FailedStep).
		Int64("duration_ms", sagaDurationMs(exec, *exec.CompletedAt)).Msg("saga failed")
	e.recordSaga(exec)

	// If the saga's own deadline fired (not the caller's context), return a
	// typed error so the gRPC server maps it to codes.DeadlineExceeded.
	if sagaCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil {
		return exec, fmt.Errorf("saga timed out after %ds: %w", timeoutSecs, context.DeadlineExceeded)
	}
	return exec, nil
}

// callHTTPWithRetry calls callHTTP and retries on failure using exponential
// backoff with full jitter: sleep = rand(0, base * 2^attempt).
// The number of retries and base backoff come from the StepDefinition if set,
// otherwise from the engine defaults. Context cancellation stops retries early.
// Returns the structured StepError from the last attempt alongside the error.
func (e *Engine) callHTTPWithRetry(ctx context.Context, url string, payload []byte, def saga.StepDefinition) (*saga.StepError, error) {
	maxRetries := e.defaultMaxRetries
	if def.MaxRetries > 0 {
		maxRetries = def.MaxRetries
	}
	baseMs := e.defaultRetryBaseMs
	if def.RetryBackoffMs > 0 {
		baseMs = def.RetryBackoffMs
	}

	stepAuth := StepAuthContext{
		SpiffeID:   def.TargetSPIFFEID,
		AuthType:   def.AuthType,
		AuthConfig: def.AuthConfig,
	}

	var (
		err        error
		lastDetail *saga.StepError
	)
	for attempt := range maxRetries + 1 {
		var detail *saga.StepError
		detail, err = e.callHTTP(ctx, url, payload, def.TimeoutSeconds, stepAuth)
		lastDetail = detail
		if err == nil {
			return nil, nil
		}
		if attempt == maxRetries {
			break
		}
		// Exponential backoff with full jitter: rand(0, base * 2^attempt).
		cap := time.Duration(baseMs) * time.Millisecond * (1 << attempt)
		sleep := time.Duration(rand.Int64N(int64(cap) + 1))
		select {
		case <-ctx.Done():
			return lastDetail, err
		case <-time.After(sleep):
		}
	}
	return lastDetail, err
}

// callHTTP POSTs payload to url, returning a structured StepError and a plain
// error for non-2xx responses or transport failures. On success both are nil.
func (e *Engine) callHTTP(ctx context.Context, url string, payload []byte, timeoutSeconds int, auth StepAuthContext) (*saga.StepError, error) {
	timeout := e.defaultTimeout
	if timeoutSeconds > 0 {
		timeout = time.Duration(timeoutSeconds) * time.Second
	}

	// Start a client span covering the full step HTTP call. Using ctx (not
	// reqCtx) as the parent so the span lifetime is not bounded by the
	// per-request timeout — the span ends via defer after the function returns.
	spanCtx, span := otel.Tracer("saga-conductor/engine").Start(ctx, "saga.step.http",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("http.request.method", http.MethodPost),
			attribute.String("url.full", url),
		),
	)
	defer span.End()

	reqCtx, cancel := context.WithTimeout(spanCtx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	// Inject W3C trace context into outbound headers so downstream services
	// appear as child spans in the same trace.
	otel.GetTextMapPropagator().Inject(spanCtx, propagation.HeaderCarrier(req.Header))

	if e.tokenSource != nil {
		tok, tokErr := e.tokenSource.Token(ctx, url, auth)
		if tokErr != nil {
			span.RecordError(tokErr)
			span.SetStatus(otelcodes.Error, tokErr.Error())
			return nil, fmt.Errorf("token source: %w", tokErr)
		}
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
	}

	start := time.Now()
	resp, err := e.httpClient.Do(req)
	durationMs := time.Since(start).Milliseconds()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(otelcodes.Error, err.Error())
		msg := fmt.Sprintf("http post %s: %v", url, err)
		return &saga.StepError{
			Message:        msg,
			IsNetworkError: true,
			DurationMs:     durationMs,
		}, fmt.Errorf("http post %s: %w", url, err)
	}

	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Read up to 512 bytes of the body for operator diagnostics; drain
		// the rest so the connection can be reused.
		var preview []byte
		if p, readErr := io.ReadAll(io.LimitReader(resp.Body, 512)); readErr == nil {
			preview = p
		}
		_, drainErr := io.Copy(io.Discard, resp.Body)
		closeErr := resp.Body.Close()
		_ = drainErr
		_ = closeErr
		msg := fmt.Sprintf("step returned HTTP %d", resp.StatusCode)
		span.SetStatus(otelcodes.Error, msg)
		return &saga.StepError{
			Message:        msg,
			HTTPStatusCode: resp.StatusCode,
			ResponseBody:   string(preview),
			DurationMs:     durationMs,
		}, fmt.Errorf("%s", msg)
	}

	// Step succeeded — drain body to allow connection reuse, then close.
	// Drain/close errors only surface when the step itself succeeded.
	_, drainErr := io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()

	if drainErr != nil {
		return nil, fmt.Errorf("drain response body: %w", drainErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close response body: %w", closeErr)
	}
	return nil, nil
}
