package server_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/ngaddam369/saga-conductor/internal/engine"
	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/server"
	"github.com/ngaddam369/saga-conductor/internal/store"
	pb "github.com/ngaddam369/saga-conductor/proto/saga/v1"
)

// mockEngine lets tests control what Start returns without running real HTTP.
type mockEngine struct {
	result *saga.Execution
	err    error
}

func (m *mockEngine) Start(_ context.Context, id string) (*saga.Execution, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.result != nil {
		return m.result, nil
	}
	// Default: return a COMPLETED execution.
	return &saga.Execution{
		ID:        id,
		Name:      "test-saga",
		Status:    saga.SagaStatusCompleted,
		Steps:     []saga.StepExecution{},
		StepDefs:  []saga.StepDefinition{},
		CreatedAt: time.Now().UTC(),
	}, nil
}

func newTestServer(t *testing.T, eng server.Engine) (pb.SagaOrchestratorClient, *store.BoltStore) {
	t.Helper()

	s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if eng == nil {
		eng = engine.New(s)
	}

	grpcSrv := grpc.NewServer(grpc.MaxRecvMsgSize(20 * 1024 * 1024))
	pb.RegisterSagaOrchestratorServer(grpcSrv, server.New(s, eng))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewSagaOrchestratorClient(conn), s
}

func validSteps() []*pb.StepDefinition {
	return []*pb.StepDefinition{
		{Name: "step-1", ForwardUrl: "http://example.com/fwd", CompensateUrl: "http://example.com/comp"},
	}
}

func TestServer(t *testing.T) {
	t.Run("CreateSaga", func(t *testing.T) {
		tests := []struct {
			name     string
			req      *pb.CreateSagaRequest
			wantCode codes.Code
		}{
			{
				name:     "valid request",
				req:      &pb.CreateSagaRequest{Name: "order-saga", Steps: validSteps()},
				wantCode: codes.OK,
			},
			{
				name:     "missing name",
				req:      &pb.CreateSagaRequest{Steps: validSteps()},
				wantCode: codes.InvalidArgument,
			},
			{
				name:     "no steps",
				req:      &pb.CreateSagaRequest{Name: "order-saga"},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "step missing name",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{ForwardUrl: "http://x.com", CompensateUrl: "http://x.com"},
				}},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "step missing forward_url",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "s1", CompensateUrl: "http://x.com"},
				}},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "step missing compensate_url",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "s1", ForwardUrl: "http://x.com"},
				}},
				wantCode: codes.InvalidArgument,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				client, _ := newTestServer(t, nil)
				resp, err := client.CreateSaga(context.Background(), tc.req)
				code := status.Code(err)
				if code != tc.wantCode {
					t.Errorf("code: got %v, want %v (err=%v)", code, tc.wantCode, err)
				}
				if tc.wantCode == codes.OK {
					if resp.Saga.Id == "" {
						t.Error("expected non-empty saga ID")
					}
					if resp.Saga.Status != pb.SagaStatus_SAGA_STATUS_PENDING {
						t.Errorf("Status: got %v, want PENDING", resp.Saga.Status)
					}
				}
			})
		}
	})

	t.Run("CreateSaga/GRPCRecvSizeLimit", func(t *testing.T) {
		// When the gRPC server is configured with a small MaxRecvMsgSize the
		// transport itself rejects messages above that limit with ResourceExhausted
		// before they reach the handler.
		tests := []struct {
			name        string
			recvLimitMB int
			payloadMB   int
			wantCode    codes.Code
		}{
			{
				name:        "payload within limit passes to handler",
				recvLimitMB: 2,
				payloadMB:   1,
				wantCode:    codes.OK,
			},
			{
				name:        "payload exceeds transport limit — ResourceExhausted",
				recvLimitMB: 1,
				payloadMB:   2,
				wantCode:    codes.ResourceExhausted,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
				if err != nil {
					t.Fatalf("NewBoltStore: %v", err)
				}
				t.Cleanup(func() { _ = s.Close() })

				grpcSrv := grpc.NewServer(grpc.MaxRecvMsgSize(tc.recvLimitMB * 1024 * 1024))
				pb.RegisterSagaOrchestratorServer(grpcSrv, server.New(s, engine.New(s)))

				lis, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatalf("listen: %v", err)
				}
				go func() { _ = grpcSrv.Serve(lis) }()
				t.Cleanup(grpcSrv.GracefulStop)

				conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					t.Fatalf("dial: %v", err)
				}
				t.Cleanup(func() { _ = conn.Close() })

				client := pb.NewSagaOrchestratorClient(conn)
				_, err = client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
					Name:    "saga",
					Steps:   validSteps(),
					Payload: make([]byte, tc.payloadMB*1024*1024),
				})
				if code := status.Code(err); code != tc.wantCode {
					t.Errorf("code: got %v, want %v (err=%v)", code, tc.wantCode, err)
				}
			})
		}
	})

	t.Run("CreateSaga/Validation", func(t *testing.T) {
		bigPayload := make([]byte, 10*1024*1024+1)

		tests := []struct {
			name     string
			req      *pb.CreateSagaRequest
			wantCode codes.Code
		}{
			{
				name:     "payload too large",
				req:      &pb.CreateSagaRequest{Name: "saga", Steps: validSteps(), Payload: bigPayload},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "invalid step name format — spaces",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "bad name", ForwardUrl: "http://x.com/f", CompensateUrl: "http://x.com/c"},
				}},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "invalid step name format — special chars",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "bad!name", ForwardUrl: "http://x.com/f", CompensateUrl: "http://x.com/c"},
				}},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "duplicate step names",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: "http://x.com/f", CompensateUrl: "http://x.com/c"},
					{Name: "step-1", ForwardUrl: "http://x.com/f2", CompensateUrl: "http://x.com/c2"},
				}},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "file:// forward_url",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: "file:///etc/passwd", CompensateUrl: "http://x.com/c"},
				}},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "ftp:// compensate_url",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: "http://x.com/f", CompensateUrl: "ftp://x.com/c"},
				}},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "malformed forward_url",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: "not a url", CompensateUrl: "http://x.com/c"},
				}},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "timeout_seconds too large",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: "http://x.com/f", CompensateUrl: "http://x.com/c", TimeoutSeconds: 3601},
				}},
				wantCode: codes.InvalidArgument,
			},
			{
				name: "timeout_seconds=0 allowed (use default)",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: "http://x.com/f", CompensateUrl: "http://x.com/c", TimeoutSeconds: 0},
				}},
				wantCode: codes.OK,
			},
			{
				name: "timeout_seconds=3600 at boundary",
				req: &pb.CreateSagaRequest{Name: "saga", Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: "http://x.com/f", CompensateUrl: "http://x.com/c", TimeoutSeconds: 3600},
				}},
				wantCode: codes.OK,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				client, _ := newTestServer(t, nil)
				_, err := client.CreateSaga(context.Background(), tc.req)
				if code := status.Code(err); code != tc.wantCode {
					t.Errorf("code: got %v, want %v (err=%v)", code, tc.wantCode, err)
				}
			})
		}
	})

	t.Run("GetSaga", func(t *testing.T) {
		tests := []struct {
			name     string
			sagaID   string
			seed     bool
			wantCode codes.Code
		}{
			{"found", "saga-x", true, codes.OK},
			{"not found", "missing", false, codes.NotFound},
			{"empty id", "", false, codes.InvalidArgument},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				client, s := newTestServer(t, nil)

				if tc.seed {
					exec := &saga.Execution{
						ID:        tc.sagaID,
						Name:      "test",
						Status:    saga.SagaStatusPending,
						CreatedAt: time.Now().UTC(),
						Steps:     []saga.StepExecution{},
						StepDefs:  []saga.StepDefinition{},
					}
					if err := s.Create(context.Background(), exec); err != nil {
						t.Fatalf("seed: %v", err)
					}
				}

				_, err := client.GetSaga(context.Background(), &pb.GetSagaRequest{SagaId: tc.sagaID})
				if code := status.Code(err); code != tc.wantCode {
					t.Errorf("code: got %v, want %v", code, tc.wantCode)
				}
			})
		}
	})

	t.Run("ListSagas", func(t *testing.T) {
		tests := []struct {
			name         string
			filterStatus pb.SagaStatus
			wantCount    int
		}{
			{"all", pb.SagaStatus_SAGA_STATUS_UNSPECIFIED, 3},
			{"pending", pb.SagaStatus_SAGA_STATUS_PENDING, 1},
			{"completed", pb.SagaStatus_SAGA_STATUS_COMPLETED, 1},
			{"failed", pb.SagaStatus_SAGA_STATUS_FAILED, 1},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				client, s := newTestServer(t, nil)
				ctx := context.Background()

				seed := []struct {
					id     string
					status saga.SagaStatus
				}{
					{"a", saga.SagaStatusPending},
					{"b", saga.SagaStatusCompleted},
					{"c", saga.SagaStatusFailed},
				}
				for _, se := range seed {
					exec := &saga.Execution{
						ID:        se.id,
						Name:      "saga-" + se.id,
						Status:    se.status,
						CreatedAt: time.Now().UTC(),
						Steps:     []saga.StepExecution{},
						StepDefs:  []saga.StepDefinition{},
					}
					if err := s.Create(ctx, exec); err != nil {
						t.Fatalf("seed %s: %v", se.id, err)
					}
				}

				resp, err := client.ListSagas(ctx, &pb.ListSagasRequest{Status: tc.filterStatus})
				if err != nil {
					t.Fatalf("ListSagas: %v", err)
				}
				if len(resp.Sagas) != tc.wantCount {
					t.Errorf("got %d sagas, want %d", len(resp.Sagas), tc.wantCount)
				}
			})
		}
	})

	t.Run("StartSaga", func(t *testing.T) {
		tests := []struct {
			name     string
			sagaID   string
			engine   server.Engine
			wantCode codes.Code
		}{
			{
				name:     "empty saga_id",
				sagaID:   "",
				wantCode: codes.InvalidArgument,
			},
			{
				name:     "engine returns completed",
				sagaID:   "saga-1",
				engine:   &mockEngine{},
				wantCode: codes.OK,
			},
			{
				name:     "engine returns context.Canceled → codes.Canceled",
				sagaID:   "saga-1",
				engine:   &mockEngine{err: fmt.Errorf("engine: %w", context.Canceled)},
				wantCode: codes.Canceled,
			},
			{
				name:     "engine returns context.DeadlineExceeded → codes.DeadlineExceeded",
				sagaID:   "saga-1",
				engine:   &mockEngine{err: fmt.Errorf("engine: %w", context.DeadlineExceeded)},
				wantCode: codes.DeadlineExceeded,
			},
			{
				name:     "engine returns generic error → codes.Internal",
				sagaID:   "saga-1",
				engine:   &mockEngine{err: fmt.Errorf("some internal failure")},
				wantCode: codes.Internal,
			},
			{
				name:     "ErrAlreadyRunning → codes.FailedPrecondition",
				sagaID:   "saga-1",
				engine:   &mockEngine{err: fmt.Errorf("transition: %w", store.ErrAlreadyRunning)},
				wantCode: codes.FailedPrecondition,
			},
			{
				name:     "ErrAlreadyCompensating → codes.FailedPrecondition",
				sagaID:   "saga-1",
				engine:   &mockEngine{err: fmt.Errorf("transition: %w", store.ErrAlreadyCompensating)},
				wantCode: codes.FailedPrecondition,
			},
			{
				name:     "ErrAlreadyCompleted → codes.FailedPrecondition",
				sagaID:   "saga-1",
				engine:   &mockEngine{err: fmt.Errorf("transition: %w", store.ErrAlreadyCompleted)},
				wantCode: codes.FailedPrecondition,
			},
			{
				name:     "ErrAlreadyFailed → codes.FailedPrecondition",
				sagaID:   "saga-1",
				engine:   &mockEngine{err: fmt.Errorf("transition: %w", store.ErrAlreadyFailed)},
				wantCode: codes.FailedPrecondition,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				client, _ := newTestServer(t, tc.engine)
				_, err := client.StartSaga(context.Background(), &pb.StartSagaRequest{SagaId: tc.sagaID})
				if code := status.Code(err); code != tc.wantCode {
					t.Errorf("code: got %v, want %v (err=%v)", code, tc.wantCode, err)
				}
			})
		}
	})
}
