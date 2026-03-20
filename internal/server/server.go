// Package server wires the gRPC SagaOrchestrator service.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ngaddam369/saga-conductor/internal/engine"
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

// Engine is the interface the server uses to start and abort saga executions.
type Engine interface {
	Start(ctx context.Context, id string) (*saga.Execution, error)
	Abort(ctx context.Context, id string) (*saga.Execution, error)
}

// Server implements the gRPC SagaOrchestrator service.
type Server struct {
	pb.UnimplementedSagaOrchestratorServer
	store             store.Store
	engine            Engine
	idempotencyKeyTTL time.Duration
}

// New returns a Server wired with the given store, engine, and idempotency key TTL.
func New(s store.Store, eng Engine, idempotencyKeyTTL time.Duration) *Server {
	return &Server{store: s, engine: eng, idempotencyKeyTTL: idempotencyKeyTTL}
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
	if req.SagaTimeoutSeconds != 0 && (req.SagaTimeoutSeconds < 1 || req.SagaTimeoutSeconds > 86400) {
		return nil, status.Error(codes.InvalidArgument, "saga_timeout_seconds must be in [1, 86400]")
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
		ID:                 uuid.New().String(),
		Name:               req.Name,
		Status:             saga.SagaStatusPending,
		StepDefs:           stepDefs,
		Steps:              stepExecs,
		Payload:            req.Payload,
		SagaTimeoutSeconds: int(req.SagaTimeoutSeconds),
		CreatedAt:          time.Now().UTC(),
	}

	if req.IdempotencyKey != "" {
		if len(req.IdempotencyKey) > 256 {
			return nil, status.Error(codes.InvalidArgument, "idempotency_key exceeds 256 characters")
		}
		expiresAt := time.Now().UTC().Add(s.idempotencyKeyTTL)
		result, err := s.store.GetOrCreateWithKey(ctx, req.IdempotencyKey, expiresAt, exec)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "create saga: %v", err)
		}
		return &pb.CreateSagaResponse{Saga: toProto(result)}, nil
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

	log := zerolog.Ctx(ctx).With().Str("saga_id", req.SagaId).Logger()
	exec, err := s.engine.Start(ctx, req.SagaId)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, "saga start canceled")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, "saga start timed out")
		}
		if errors.Is(err, store.ErrAlreadyRunning) {
			return nil, status.Error(codes.FailedPrecondition, "saga is already running")
		}
		if errors.Is(err, store.ErrAlreadyCompensating) {
			return nil, status.Error(codes.FailedPrecondition, "saga is already compensating")
		}
		if errors.Is(err, store.ErrAlreadyCompleted) {
			return nil, status.Error(codes.FailedPrecondition, "saga is already completed")
		}
		if errors.Is(err, store.ErrAlreadyFailed) {
			return nil, status.Error(codes.FailedPrecondition, "saga has already failed")
		}
		if errors.Is(err, store.ErrAlreadyAborted) {
			return nil, status.Error(codes.FailedPrecondition, "saga has already been aborted")
		}
		if errors.Is(err, engine.ErrDraining) {
			return nil, status.Error(codes.Unavailable, "server is shutting down")
		}
		log.Error().Err(err).Msg("StartSaga failed")
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

// ListSagas returns sagas, optionally filtered by status, with cursor-based
// pagination. page_size defaults to 100 and is capped at 1000.
func (s *Server) ListSagas(ctx context.Context, req *pb.ListSagasRequest) (*pb.ListSagasResponse, error) {
	filter := fromProtoStatus(req.Status)

	pageSize := int(req.PageSize)
	if pageSize <= 0 {
		pageSize = 100
	}
	if pageSize > 1000 {
		pageSize = 1000
	}

	executions, nextToken, err := s.store.List(ctx, filter, pageSize, req.PageToken)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list sagas: %v", err)
	}

	pbSagas := make([]*pb.SagaExecution, len(executions))
	for i, e := range executions {
		pbSagas[i] = toProto(e)
	}

	return &pb.ListSagasResponse{Sagas: pbSagas, NextPageToken: nextToken}, nil
}

// AbortSaga forcibly marks a non-terminal saga ABORTED. No compensation is
// triggered. Returns NotFound if the saga does not exist, AlreadyExists (409)
// mapped to FailedPrecondition if already in a terminal state.
func (s *Server) AbortSaga(ctx context.Context, req *pb.AbortSagaRequest) (*pb.AbortSagaResponse, error) {
	if req.SagaId == "" {
		return nil, status.Error(codes.InvalidArgument, "saga_id is required")
	}

	exec, err := s.engine.Abort(ctx, req.SagaId)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "saga %s not found", req.SagaId)
		}
		if errors.Is(err, store.ErrAlreadyCompleted) ||
			errors.Is(err, store.ErrAlreadyFailed) ||
			errors.Is(err, store.ErrAlreadyAborted) {
			return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
		}
		return nil, status.Errorf(codes.Internal, "abort saga: %v", err)
	}

	return &pb.AbortSagaResponse{Saga: toProto(exec)}, nil
}

// ── Conversion helpers ────────────────────────────────────────────────────────

func toProto(exec *saga.Execution) *pb.SagaExecution {
	pbSteps := make([]*pb.StepExecution, len(exec.Steps))
	for i, st := range exec.Steps {
		var errorDetail []byte
		if st.ErrorDetail != nil {
			if b, err := json.Marshal(st.ErrorDetail); err == nil {
				errorDetail = b
			}
		}
		pbSteps[i] = &pb.StepExecution{
			Name:        st.Name,
			Status:      toProtoStepStatus(st.Status),
			Error:       st.Error,
			StartedAt:   timeToProto(st.StartedAt),
			CompletedAt: timeToProto(st.CompletedAt),
			ErrorDetail: errorDetail,
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
	case saga.SagaStatusAborted:
		return pb.SagaStatus_SAGA_STATUS_ABORTED
	case saga.SagaStatusCompensationFailed:
		return pb.SagaStatus_SAGA_STATUS_COMPENSATION_FAILED
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
	case pb.SagaStatus_SAGA_STATUS_ABORTED:
		return saga.SagaStatusAborted
	case pb.SagaStatus_SAGA_STATUS_COMPENSATION_FAILED:
		return saga.SagaStatusCompensationFailed
	default:
		return ""
	}
}
