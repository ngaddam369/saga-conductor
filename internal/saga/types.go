// Package saga defines the core domain types for saga-conductor.
package saga

import "time"

// SagaStatus is the lifecycle state of a saga as a whole.
type SagaStatus string

const (
	SagaStatusPending      SagaStatus = "PENDING"
	SagaStatusRunning      SagaStatus = "RUNNING"
	SagaStatusCompensating SagaStatus = "COMPENSATING"
	SagaStatusCompleted    SagaStatus = "COMPLETED"
	SagaStatusFailed       SagaStatus = "FAILED"
	// SagaStatusAborted is a terminal state set by the operator via AbortSaga.
	// It is used to forcibly terminate a stuck saga (PENDING, RUNNING, or
	// COMPENSATING) when the natural failure path is unavailable or too slow.
	SagaStatusAborted SagaStatus = "ABORTED"
)

// StepStatus is the state of an individual step within a saga.
type StepStatus string

const (
	StepStatusPending            StepStatus = "PENDING"
	StepStatusRunning            StepStatus = "RUNNING"
	StepStatusSucceeded          StepStatus = "SUCCEEDED"
	StepStatusFailed             StepStatus = "FAILED"
	StepStatusCompensating       StepStatus = "COMPENSATING"
	StepStatusCompensated        StepStatus = "COMPENSATED"
	StepStatusCompensationFailed StepStatus = "COMPENSATION_FAILED"
)

// StepDefinition describes a single step in a saga: what to call and how.
type StepDefinition struct {
	// Name is unique within the saga and used as the step identifier.
	Name string `json:"name"`
	// ForwardURL is POSTed to when executing the step.
	ForwardURL string `json:"forward_url"`
	// CompensateURL is POSTed to when rolling back the step.
	CompensateURL string `json:"compensate_url"`
	// TimeoutSeconds caps how long the orchestrator waits for the HTTP response.
	// Zero means the server-wide default is used.
	TimeoutSeconds int `json:"timeout_seconds"`
	// MaxRetries is the number of times to retry a failed step HTTP call before
	// triggering compensation. Zero means the engine default is used.
	MaxRetries int `json:"max_retries,omitempty"`
	// RetryBackoffMs is the base backoff in milliseconds for the first retry.
	// Subsequent retries use base * 2^attempt + jitter. Zero means the engine
	// default is used.
	RetryBackoffMs int `json:"retry_backoff_ms,omitempty"`
}

// StepError holds structured context for a failed step HTTP call.
// It is stored as JSON in the execution blob alongside the plain Error string.
type StepError struct {
	// Message is the human-readable summary (matches step.Error).
	Message string `json:"message"`
	// HTTPStatusCode is the HTTP response status, or 0 for transport errors.
	HTTPStatusCode int `json:"http_status_code,omitempty"`
	// ResponseBody holds the first 512 bytes of the response body, empty on
	// transport errors. Gives operators the service-returned error message.
	ResponseBody string `json:"response_body,omitempty"`
	// IsNetworkError is true when no HTTP response was received (connection
	// refused, timeout, context cancellation).
	IsNetworkError bool `json:"is_network_error,omitempty"`
	// DurationMs is the round-trip time of the HTTP call in milliseconds.
	DurationMs int64 `json:"duration_ms"`
}

// StepExecution is the runtime state of a step within a saga execution.
type StepExecution struct {
	Name   string     `json:"name"`
	Status StepStatus `json:"status"`
	// Error is the plain-string error message, kept for backward compatibility.
	Error string `json:"error,omitempty"`
	// ErrorDetail holds structured context for failed steps. Stored as a nested
	// JSON object in the execution blob; nil on success.
	ErrorDetail *StepError `json:"error_detail,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// Execution is the runtime state of a saga.
type Execution struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	Status      SagaStatus       `json:"status"`
	Steps       []StepExecution  `json:"steps"`
	StepDefs    []StepDefinition `json:"step_defs"`
	Payload     []byte           `json:"payload,omitempty"`
	FailedStep  string           `json:"failed_step,omitempty"`
	CreatedAt   time.Time        `json:"created_at"`
	StartedAt   *time.Time       `json:"started_at,omitempty"`
	CompletedAt *time.Time       `json:"completed_at,omitempty"`
}
