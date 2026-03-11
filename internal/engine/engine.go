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
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/store"
)

// Engine executes sagas stored in a Store.
type Engine struct {
	store          store.Store
	httpClient     *http.Client
	defaultTimeout time.Duration
}

// New returns an Engine backed by the given store.
func New(s store.Store) *Engine {
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
	return &Engine{
		store:          s,
		defaultTimeout: timeout,
		httpClient: &http.Client{
			Transport: transport,
			// Never follow redirects — a 3xx to file:// or an internal service
			// is a silent SSRF vector.
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
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
		if err = e.store.Update(ctx, exec); err != nil {
			return nil, fmt.Errorf("persist step RUNNING: %w", err)
		}

		err = e.callHTTP(ctx, def.ForwardURL, exec.Payload, def.TimeoutSeconds)

		t = time.Now().UTC()
		step.CompletedAt = &t

		if err != nil {
			step.Status = saga.StepStatusFailed
			step.Error = err.Error()
			exec.FailedStep = step.Name
			if err = e.store.Update(ctx, exec); err != nil {
				return nil, fmt.Errorf("persist step FAILED: %w", err)
			}
			failedIdx = i
			break
		}

		step.Status = saga.StepStatusSucceeded
		if err = e.store.Update(ctx, exec); err != nil {
			return nil, fmt.Errorf("persist step SUCCEEDED: %w", err)
		}
	}

	if failedIdx == -1 {
		completed := time.Now().UTC()
		exec.Status = saga.SagaStatusCompleted
		exec.CompletedAt = &completed
		if err = e.store.Update(ctx, exec); err != nil {
			return nil, fmt.Errorf("persist COMPLETED: %w", err)
		}
		return exec, nil
	}

	exec.Status = saga.SagaStatusCompensating
	if err = e.store.Update(ctx, exec); err != nil {
		return nil, fmt.Errorf("persist COMPENSATING: %w", err)
	}

	for i := failedIdx - 1; i >= 0; i-- {
		step := &exec.Steps[i]
		def := exec.StepDefs[i]

		if step.Status != saga.StepStatusSucceeded {
			continue
		}

		step.Status = saga.StepStatusCompensating
		if err = e.store.Update(ctx, exec); err != nil {
			return nil, fmt.Errorf("persist step COMPENSATING: %w", err)
		}

		err = e.callHTTP(ctx, def.CompensateURL, exec.Payload, def.TimeoutSeconds)

		t := time.Now().UTC()
		step.CompletedAt = &t

		if err != nil {
			step.Status = saga.StepStatusCompensationFailed
			step.Error = fmt.Sprintf("compensation failed: %s", err)
		} else {
			step.Status = saga.StepStatusCompensated
		}

		if err = e.store.Update(ctx, exec); err != nil {
			return nil, fmt.Errorf("persist compensation result: %w", err)
		}
	}

	failed := time.Now().UTC()
	exec.Status = saga.SagaStatusFailed
	exec.CompletedAt = &failed
	if err = e.store.Update(ctx, exec); err != nil {
		return nil, fmt.Errorf("persist FAILED: %w", err)
	}

	return exec, nil
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
