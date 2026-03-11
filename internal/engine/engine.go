// Package engine implements the saga state machine and synchronous HTTP step
// executor. It is the core of saga-conductor: given a saga execution persisted
// in a Store it advances the saga step-by-step, compensates on failure, and
// keeps the store in sync after every state transition.
package engine

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/store"
)

const defaultStepTimeout = 30 * time.Second

// Engine executes sagas stored in a Store.
type Engine struct {
	store      store.Store
	httpClient *http.Client
}

// New returns an Engine backed by the given store.
func New(s store.Store) *Engine {
	return &Engine{
		store:      s,
		httpClient: &http.Client{},
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

	exec, err := e.store.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("get saga: %w", err)
	}
	if exec.Status != saga.SagaStatusPending {
		return nil, fmt.Errorf("saga %s is %s, want PENDING", id, exec.Status)
	}
	if len(exec.Steps) != len(exec.StepDefs) {
		return nil, fmt.Errorf("saga %s: corrupted — Steps length %d != StepDefs length %d", id, len(exec.Steps), len(exec.StepDefs))
	}

	now := time.Now().UTC()
	exec.Status = saga.SagaStatusRunning
	exec.StartedAt = &now
	if err = e.store.Update(ctx, exec); err != nil {
		return nil, fmt.Errorf("persist RUNNING: %w", err)
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
		now = time.Now().UTC()
		exec.Status = saga.SagaStatusCompleted
		exec.CompletedAt = &now
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

	now = time.Now().UTC()
	exec.Status = saga.SagaStatusFailed
	exec.CompletedAt = &now
	if err = e.store.Update(ctx, exec); err != nil {
		return nil, fmt.Errorf("persist FAILED: %w", err)
	}

	return exec, nil
}

// callHTTP POSTs payload to url, returning an error for non-2xx responses or
// transport failures.
func (e *Engine) callHTTP(ctx context.Context, url string, payload []byte, timeoutSeconds int) error {
	timeout := defaultStepTimeout
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
	defer func() {
		if err = resp.Body.Close(); err != nil {
			err = fmt.Errorf("close response body: %w", err)
		}
	}()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("step returned HTTP %d", resp.StatusCode)
	}
	return nil
}
