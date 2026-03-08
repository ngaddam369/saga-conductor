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
}

// StepExecution is the runtime state of a step within a saga execution.
type StepExecution struct {
	Name        string     `json:"name"`
	Status      StepStatus `json:"status"`
	Error       string     `json:"error,omitempty"`
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
