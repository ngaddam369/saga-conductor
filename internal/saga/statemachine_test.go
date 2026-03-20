package saga_test

import (
	"testing"

	"github.com/ngaddam369/saga-conductor/internal/saga"
)

func TestValidateTransition(t *testing.T) {
	t.Parallel()

	valid := []struct {
		from saga.SagaStatus
		to   saga.SagaStatus
	}{
		{saga.SagaStatusPending, saga.SagaStatusRunning},
		{saga.SagaStatusRunning, saga.SagaStatusCompleted},
		{saga.SagaStatusRunning, saga.SagaStatusCompensating},
		{saga.SagaStatusRunning, saga.SagaStatusFailed},
		{saga.SagaStatusCompensating, saga.SagaStatusFailed},
		// abort transitions
		{saga.SagaStatusPending, saga.SagaStatusAborted},
		{saga.SagaStatusRunning, saga.SagaStatusAborted},
		{saga.SagaStatusCompensating, saga.SagaStatusAborted},
		// compensation dead-letter
		{saga.SagaStatusCompensating, saga.SagaStatusCompensationFailed},
	}
	for _, tc := range valid {
		if err := saga.ValidateTransition(tc.from, tc.to); err != nil {
			t.Errorf("ValidateTransition(%s, %s) = %v; want nil", tc.from, tc.to, err)
		}
	}

	invalid := []struct {
		from saga.SagaStatus
		to   saga.SagaStatus
	}{
		// terminal → anything
		{saga.SagaStatusCompleted, saga.SagaStatusRunning},
		{saga.SagaStatusFailed, saga.SagaStatusRunning},
		{saga.SagaStatusCompleted, saga.SagaStatusFailed},
		{saga.SagaStatusAborted, saga.SagaStatusRunning},
		{saga.SagaStatusAborted, saga.SagaStatusPending},
		{saga.SagaStatusCompensationFailed, saga.SagaStatusRunning},
		{saga.SagaStatusCompensationFailed, saga.SagaStatusCompensating},
		// skipping states
		{saga.SagaStatusPending, saga.SagaStatusCompleted},
		{saga.SagaStatusPending, saga.SagaStatusFailed},
		{saga.SagaStatusCompensating, saga.SagaStatusCompleted},
		{saga.SagaStatusCompensating, saga.SagaStatusRunning},
		// unknown source
		{"BOGUS", saga.SagaStatusRunning},
	}
	for _, tc := range invalid {
		if err := saga.ValidateTransition(tc.from, tc.to); err == nil {
			t.Errorf("ValidateTransition(%s, %s) = nil; want error", tc.from, tc.to)
		}
	}
}

func TestValidateStepTransition(t *testing.T) {
	t.Parallel()

	valid := []struct {
		from saga.StepStatus
		to   saga.StepStatus
	}{
		{saga.StepStatusPending, saga.StepStatusRunning},
		{saga.StepStatusRunning, saga.StepStatusSucceeded},
		{saga.StepStatusRunning, saga.StepStatusFailed},
		{saga.StepStatusSucceeded, saga.StepStatusCompensating},
		{saga.StepStatusCompensating, saga.StepStatusCompensated},
		{saga.StepStatusCompensating, saga.StepStatusCompensationFailed},
	}
	for _, tc := range valid {
		if err := saga.ValidateStepTransition(tc.from, tc.to); err != nil {
			t.Errorf("ValidateStepTransition(%s, %s) = %v; want nil", tc.from, tc.to, err)
		}
	}

	invalid := []struct {
		from saga.StepStatus
		to   saga.StepStatus
	}{
		// terminal → anything
		{saga.StepStatusFailed, saga.StepStatusRunning},
		{saga.StepStatusCompensated, saga.StepStatusRunning},
		{saga.StepStatusCompensationFailed, saga.StepStatusCompensating},
		// skipping states
		{saga.StepStatusPending, saga.StepStatusSucceeded},
		{saga.StepStatusPending, saga.StepStatusCompensating},
		{saga.StepStatusRunning, saga.StepStatusCompensating},
		{saga.StepStatusSucceeded, saga.StepStatusFailed},
		// unknown source
		{"BOGUS", saga.StepStatusRunning},
	}
	for _, tc := range invalid {
		if err := saga.ValidateStepTransition(tc.from, tc.to); err == nil {
			t.Errorf("ValidateStepTransition(%s, %s) = nil; want error", tc.from, tc.to)
		}
	}
}
