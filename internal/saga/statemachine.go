package saga

import (
	"fmt"
	"slices"
)

// validSagaTransitions defines every permitted saga status change.
// Terminal states (COMPLETED, FAILED) have empty target sets — no transition
// out of a terminal state is ever valid.
var validSagaTransitions = map[SagaStatus][]SagaStatus{
	SagaStatusPending:      {SagaStatusRunning},
	SagaStatusRunning:      {SagaStatusCompleted, SagaStatusCompensating, SagaStatusFailed},
	SagaStatusCompensating: {SagaStatusFailed},
	SagaStatusCompleted:    {},
	SagaStatusFailed:       {},
}

// validStepTransitions defines every permitted step status change.
// SUCCEEDED can transition to COMPENSATING because a later step failing
// requires rolling back all previously committed (succeeded) steps.
var validStepTransitions = map[StepStatus][]StepStatus{
	StepStatusPending:            {StepStatusRunning},
	StepStatusRunning:            {StepStatusSucceeded, StepStatusFailed},
	StepStatusSucceeded:          {StepStatusCompensating},
	StepStatusFailed:             {},
	StepStatusCompensating:       {StepStatusCompensated, StepStatusCompensationFailed},
	StepStatusCompensated:        {},
	StepStatusCompensationFailed: {},
}

// ValidateTransition returns an error if transitioning a saga from 'from' to
// 'to' is not permitted by the state machine. A non-nil error always indicates
// a programming bug — the caller should never attempt an invalid transition.
func ValidateTransition(from, to SagaStatus) error {
	allowed, ok := validSagaTransitions[from]
	if !ok {
		return fmt.Errorf("saga: unknown source status %q", from)
	}
	if !slices.Contains(allowed, to) {
		return fmt.Errorf("saga: invalid transition %s → %s", from, to)
	}
	return nil
}

// ValidateStepTransition returns an error if transitioning a step from 'from'
// to 'to' is not permitted by the state machine. A non-nil error always
// indicates a programming bug — the caller should never attempt an invalid
// transition.
func ValidateStepTransition(from, to StepStatus) error {
	allowed, ok := validStepTransitions[from]
	if !ok {
		return fmt.Errorf("step: unknown source status %q", from)
	}
	if !slices.Contains(allowed, to) {
		return fmt.Errorf("step: invalid transition %s → %s", from, to)
	}
	return nil
}
