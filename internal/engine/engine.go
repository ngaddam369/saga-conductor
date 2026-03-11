// Package engine implements the saga state machine and synchronous HTTP step
// executor. It is the core of saga-conductor: given a saga execution persisted
// in a Store it advances the saga step-by-step, compensates on failure, and
// keeps the store in sync after every state transition.
package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/store"
)

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

// Engine executes sagas stored in a Store.
type Engine struct {
	store              store.Store
	httpClient         *http.Client
	defaultTimeout     time.Duration
	retryBackoff       time.Duration
	defaultMaxRetries  int
	defaultRetryBaseMs int
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

	eng := &Engine{
		store:              s,
		defaultTimeout:     timeout,
		retryBackoff:       defaultRetryBackoff,
		defaultMaxRetries:  maxRetries,
		defaultRetryBaseMs: retryBaseMs,
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
		fmt.Fprintf(os.Stderr,
			"ERROR saga-conductor: saga %s step %q store unavailable (%s); manual recovery required\n",
			exec.ID, step, cause)
	}
}

// Start transitions a PENDING saga to RUNNING and executes all steps in order.
// If any step fails, compensation runs automatically in reverse for all
// previously succeeded steps. Start blocks until the saga reaches a terminal
// state (COMPLETED or FAILED) and then returns the final execution.
func (e *Engine) Start(ctx context.Context, id string) (*saga.Execution, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context already done: %w", err)
	}

	exec, err := e.store.TransitionToRunning(ctx, id, time.Now().UTC())
	if err != nil {
		return nil, fmt.Errorf("transition to RUNNING: %w", err)
	}
	if len(exec.Steps) != len(exec.StepDefs) {
		return nil, fmt.Errorf("saga %s: corrupted — Steps length %d != StepDefs length %d", id, len(exec.Steps), len(exec.StepDefs))
	}

	failedIdx := -1
	for i := range exec.Steps {
		step := &exec.Steps[i]
		def := exec.StepDefs[i]

		t := time.Now().UTC()
		step.Status = saga.StepStatusRunning
		step.StartedAt = &t
		if err = e.updateWithRetry(ctx, exec); err != nil {
			e.markFailedBestEffort(exec, step.Name, err.Error())
			return nil, fmt.Errorf("persist step RUNNING: %w", err)
		}

		err = e.callHTTPWithRetry(ctx, def.ForwardURL, exec.Payload, def)

		t = time.Now().UTC()
		step.CompletedAt = &t

		if err != nil {
			step.Status = saga.StepStatusFailed
			step.Error = err.Error()
			exec.FailedStep = step.Name
			if uerr := e.updateWithRetry(ctx, exec); uerr != nil {
				e.markFailedBestEffort(exec, step.Name, uerr.Error())
				return nil, fmt.Errorf("persist step FAILED: %w", uerr)
			}
			failedIdx = i
			break
		}

		step.Status = saga.StepStatusSucceeded
		if err = e.updateWithRetry(ctx, exec); err != nil {
			e.markFailedBestEffort(exec, step.Name, err.Error())
			return nil, fmt.Errorf("persist step SUCCEEDED: %w", err)
		}
	}

	if failedIdx == -1 {
		completed := time.Now().UTC()
		exec.Status = saga.SagaStatusCompleted
		exec.CompletedAt = &completed
		if err = e.updateWithRetry(ctx, exec); err != nil {
			e.markFailedBestEffort(exec, "", err.Error())
			return nil, fmt.Errorf("persist COMPLETED: %w", err)
		}
		return exec, nil
	}

	exec.Status = saga.SagaStatusCompensating
	if err = e.updateWithRetry(ctx, exec); err != nil {
		e.markFailedBestEffort(exec, exec.FailedStep, err.Error())
		return nil, fmt.Errorf("persist COMPENSATING: %w", err)
	}

	for i := failedIdx - 1; i >= 0; i-- {
		step := &exec.Steps[i]
		def := exec.StepDefs[i]

		if step.Status != saga.StepStatusSucceeded {
			continue
		}

		step.Status = saga.StepStatusCompensating
		if err = e.updateWithRetry(ctx, exec); err != nil {
			e.markFailedBestEffort(exec, step.Name, err.Error())
			return nil, fmt.Errorf("persist step COMPENSATING: %w", err)
		}

		err = e.callHTTPWithRetry(ctx, def.CompensateURL, exec.Payload, def)

		t := time.Now().UTC()
		step.CompletedAt = &t

		if err != nil {
			step.Status = saga.StepStatusCompensationFailed
			step.Error = fmt.Sprintf("compensation failed: %s", err)
		} else {
			step.Status = saga.StepStatusCompensated
		}

		if err = e.updateWithRetry(ctx, exec); err != nil {
			e.markFailedBestEffort(exec, step.Name, err.Error())
			return nil, fmt.Errorf("persist compensation result: %w", err)
		}
	}

	failed := time.Now().UTC()
	exec.Status = saga.SagaStatusFailed
	exec.CompletedAt = &failed
	if err = e.updateWithRetry(ctx, exec); err != nil {
		e.markFailedBestEffort(exec, exec.FailedStep, err.Error())
		return nil, fmt.Errorf("persist FAILED: %w", err)
	}

	return exec, nil
}

// callHTTPWithRetry calls callHTTP and retries on failure using exponential
// backoff with full jitter: sleep = rand(0, base * 2^attempt).
// The number of retries and base backoff come from the StepDefinition if set,
// otherwise from the engine defaults. Context cancellation stops retries early.
func (e *Engine) callHTTPWithRetry(ctx context.Context, url string, payload []byte, def saga.StepDefinition) error {
	maxRetries := e.defaultMaxRetries
	if def.MaxRetries > 0 {
		maxRetries = def.MaxRetries
	}
	baseMs := e.defaultRetryBaseMs
	if def.RetryBackoffMs > 0 {
		baseMs = def.RetryBackoffMs
	}

	var err error
	for attempt := range maxRetries + 1 {
		err = e.callHTTP(ctx, url, payload, def.TimeoutSeconds)
		if err == nil {
			return nil
		}
		if attempt == maxRetries {
			break
		}
		// Exponential backoff with full jitter: rand(0, base * 2^attempt).
		cap := time.Duration(baseMs) * time.Millisecond * (1 << attempt)
		sleep := time.Duration(rand.Int64N(int64(cap) + 1))
		select {
		case <-ctx.Done():
			return err
		case <-time.After(sleep):
		}
	}
	return err
}

// callHTTP POSTs payload to url, returning an error for non-2xx responses or
// transport failures.
func (e *Engine) callHTTP(ctx context.Context, url string, payload []byte, timeoutSeconds int) error {
	timeout := e.defaultTimeout
	if timeoutSeconds > 0 {
		timeout = time.Duration(timeoutSeconds) * time.Second
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http post %s: %w", url, err)
	}

	// Determine step outcome before touching the body.
	var stepErr error
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		stepErr = fmt.Errorf("step returned HTTP %d", resp.StatusCode)
	}

	// Drain body to allow connection reuse, then close.
	// Step failure takes precedence — drain/close errors only surface when
	// the step itself succeeded.
	_, drainErr := io.Copy(io.Discard, resp.Body)
	closeErr := resp.Body.Close()

	if stepErr != nil {
		return stepErr
	}
	if drainErr != nil {
		return fmt.Errorf("drain response body: %w", drainErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close response body: %w", closeErr)
	}
	return nil
}
