// Package server wires the gRPC SagaOrchestrator service.
package server

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/store"
	pb "github.com/ngaddam369/saga-conductor/proto/saga/v1"
)

var stepNameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// validateStepURL returns an error if raw is not a valid http/https URL within 2048 chars.
func validateStepURL(raw string) error {
	if len(raw) > 2048 {
		return errors.New("URL exceeds 2048 characters")
	}
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("scheme %q not allowed; use http or https", u.Scheme)
	}
	return nil
}

// Engine is the interface the server uses to start saga executions.
type Engine interface {
	Start(ctx context.Context, id string) (*saga.Execution, error)
}

// Server implements the gRPC SagaOrchestrator service.
type Server struct {
	pb.UnimplementedSagaOrchestratorServer
	store  store.Store
	engine Engine
}

// New returns a Server wired with the given store and engine.
func New(s store.Store, eng Engine) *Server {
	return &Server{store: s, engine: eng}
}

// CreateSaga registers a new saga in PENDING state.
func (s *Server) CreateSaga(ctx context.Context, req *pb.CreateSagaRequest) (*pb.CreateSagaResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}
	if len(req.Steps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one step is required")
	}
	if len(req.Payload) > 10*1024*1024 {
		return nil, status.Error(codes.InvalidArgument, "payload exceeds 10 MB")
	}

	seen := make(map[string]struct{}, len(req.Steps))
	stepDefs := make([]saga.StepDefinition, len(req.Steps))
	stepExecs := make([]saga.StepExecution, len(req.Steps))
	for i, sd := range req.Steps {
		if sd.Name == "" {
			return nil, status.Errorf(codes.InvalidArgument, "step %d: name is required", i)
		}
		if !stepNameRE.MatchString(sd.Name) {
			return nil, status.Errorf(codes.InvalidArgument, "step %d: name must match ^[a-zA-Z0-9_-]{1,64}$", i)
		}
		if _, dup := seen[sd.Name]; dup {
			return nil, status.Errorf(codes.InvalidArgument, "duplicate step name %q", sd.Name)
		}
		seen[sd.Name] = struct{}{}

		if sd.ForwardUrl == "" {
			return nil, status.Errorf(codes.InvalidArgument, "step %d: forward_url is required", i)
		}
		if err := validateStepURL(sd.ForwardUrl); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "step %d: forward_url: %v", i, err)
		}
		if sd.CompensateUrl == "" {
			return nil, status.Errorf(codes.InvalidArgument, "step %d: compensate_url is required", i)
		}
		if err := validateStepURL(sd.CompensateUrl); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "step %d: compensate_url: %v", i, err)
		}
		if sd.TimeoutSeconds != 0 && (sd.TimeoutSeconds < 1 || sd.TimeoutSeconds > 3600) {
			return nil, status.Errorf(codes.InvalidArgument, "step %d: timeout_seconds must be in [1, 3600]", i)
		}

		stepDefs[i] = saga.StepDefinition{
			Name:           sd.Name,
			ForwardURL:     sd.ForwardUrl,
			CompensateURL:  sd.CompensateUrl,
			TimeoutSeconds: int(sd.TimeoutSeconds),
		}
		stepExecs[i] = saga.StepExecution{
			Name:   sd.Name,
			Status: saga.StepStatusPending,
		}
	}

	exec := &saga.Execution{
		ID:        uuid.New().String(),
		Name:      req.Name,
		Status:    saga.SagaStatusPending,
		StepDefs:  stepDefs,
		Steps:     stepExecs,
		Payload:   req.Payload,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.store.Create(ctx, exec); err != nil {
		return nil, status.Errorf(codes.Internal, "create saga: %v", err)
	}

	return &pb.CreateSagaResponse{Saga: toProto(exec)}, nil
}

// StartSaga begins executing a PENDING saga. It blocks until the saga reaches
// a terminal state (COMPLETED or FAILED).
func (s *Server) StartSaga(ctx context.Context, req *pb.StartSagaRequest) (*pb.StartSagaResponse, error) {
	if req.SagaId == "" {
		return nil, status.Error(codes.InvalidArgument, "saga_id is required")
	}

	exec, err := s.engine.Start(ctx, req.SagaId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "start saga: %v", err)
	}

	return &pb.StartSagaResponse{Saga: toProto(exec)}, nil
}

// GetSaga retrieves the current state of a saga by ID.
func (s *Server) GetSaga(ctx context.Context, req *pb.GetSagaRequest) (*pb.GetSagaResponse, error) {
	if req.SagaId == "" {
		return nil, status.Error(codes.InvalidArgument, "saga_id is required")
	}

	exec, err := s.store.Get(ctx, req.SagaId)
	if err == store.ErrNotFound {
		return nil, status.Errorf(codes.NotFound, "saga %s not found", req.SagaId)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get saga: %v", err)
	}

	return &pb.GetSagaResponse{Saga: toProto(exec)}, nil
}

// ListSagas returns all sagas, optionally filtered by status.
func (s *Server) ListSagas(ctx context.Context, req *pb.ListSagasRequest) (*pb.ListSagasResponse, error) {
	filter := fromProtoStatus(req.Status)

	executions, err := s.store.List(ctx, filter)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list sagas: %v", err)
	}

	pbSagas := make([]*pb.SagaExecution, len(executions))
	for i, e := range executions {
		pbSagas[i] = toProto(e)
	}

	return &pb.ListSagasResponse{Sagas: pbSagas}, nil
}

// ── Conversion helpers ────────────────────────────────────────────────────────

func toProto(exec *saga.Execution) *pb.SagaExecution {
	pbSteps := make([]*pb.StepExecution, len(exec.Steps))
	for i, st := range exec.Steps {
		pbSteps[i] = &pb.StepExecution{
			Name:        st.Name,
			Status:      toProtoStepStatus(st.Status),
			Error:       st.Error,
			StartedAt:   timeToProto(st.StartedAt),
			CompletedAt: timeToProto(st.CompletedAt),
		}
	}

	return &pb.SagaExecution{
		Id:          exec.ID,
		Name:        exec.Name,
		Status:      toProtoSagaStatus(exec.Status),
		Steps:       pbSteps,
		Payload:     exec.Payload,
		FailedStep:  exec.FailedStep,
		CreatedAt:   timestamppb.New(exec.CreatedAt),
		StartedAt:   timeToProto(exec.StartedAt),
		CompletedAt: timeToProto(exec.CompletedAt),
	}
}

func timeToProto(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}

func toProtoSagaStatus(s saga.SagaStatus) pb.SagaStatus {
	switch s {
	case saga.SagaStatusPending:
		return pb.SagaStatus_SAGA_STATUS_PENDING
	case saga.SagaStatusRunning:
		return pb.SagaStatus_SAGA_STATUS_RUNNING
	case saga.SagaStatusCompensating:
		return pb.SagaStatus_SAGA_STATUS_COMPENSATING
	case saga.SagaStatusCompleted:
		return pb.SagaStatus_SAGA_STATUS_COMPLETED
	case saga.SagaStatusFailed:
		return pb.SagaStatus_SAGA_STATUS_FAILED
	default:
		return pb.SagaStatus_SAGA_STATUS_UNSPECIFIED
	}
}

func toProtoStepStatus(s saga.StepStatus) pb.StepStatus {
	switch s {
	case saga.StepStatusPending:
		return pb.StepStatus_STEP_STATUS_PENDING
	case saga.StepStatusRunning:
		return pb.StepStatus_STEP_STATUS_RUNNING
	case saga.StepStatusSucceeded:
		return pb.StepStatus_STEP_STATUS_SUCCEEDED
	case saga.StepStatusFailed:
		return pb.StepStatus_STEP_STATUS_FAILED
	case saga.StepStatusCompensating:
		return pb.StepStatus_STEP_STATUS_COMPENSATING
	case saga.StepStatusCompensated:
		return pb.StepStatus_STEP_STATUS_COMPENSATED
	case saga.StepStatusCompensationFailed:
		return pb.StepStatus_STEP_STATUS_COMPENSATION_FAILED
	default:
		return pb.StepStatus_STEP_STATUS_UNSPECIFIED
	}
}

func fromProtoStatus(s pb.SagaStatus) saga.SagaStatus {
	switch s {
	case pb.SagaStatus_SAGA_STATUS_PENDING:
		return saga.SagaStatusPending
	case pb.SagaStatus_SAGA_STATUS_RUNNING:
		return saga.SagaStatusRunning
	case pb.SagaStatus_SAGA_STATUS_COMPENSATING:
		return saga.SagaStatusCompensating
	case pb.SagaStatus_SAGA_STATUS_COMPLETED:
		return saga.SagaStatusCompleted
	case pb.SagaStatus_SAGA_STATUS_FAILED:
		return saga.SagaStatusFailed
	default:
		return ""
	}
}
