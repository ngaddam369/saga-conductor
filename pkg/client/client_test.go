package client_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/ngaddam369/saga-conductor/pkg/client"
	pb "github.com/ngaddam369/saga-conductor/proto/saga/v1"
)

// fakeServer is a minimal SagaOrchestratorServer that returns canned responses.
type fakeServer struct {
	pb.UnimplementedSagaOrchestratorServer
	createResp *pb.SagaExecution
	startResp  *pb.SagaExecution
	getResp    *pb.SagaExecution
	listResp   []*pb.SagaExecution
	listToken  string
	abortResp  *pb.SagaExecution
}

func (f *fakeServer) CreateSaga(_ context.Context, _ *pb.CreateSagaRequest) (*pb.CreateSagaResponse, error) {
	return &pb.CreateSagaResponse{Saga: f.createResp}, nil
}
func (f *fakeServer) StartSaga(_ context.Context, _ *pb.StartSagaRequest) (*pb.StartSagaResponse, error) {
	return &pb.StartSagaResponse{Saga: f.startResp}, nil
}
func (f *fakeServer) GetSaga(_ context.Context, _ *pb.GetSagaRequest) (*pb.GetSagaResponse, error) {
	return &pb.GetSagaResponse{Saga: f.getResp}, nil
}
func (f *fakeServer) ListSagas(_ context.Context, _ *pb.ListSagasRequest) (*pb.ListSagasResponse, error) {
	return &pb.ListSagasResponse{Sagas: f.listResp, NextPageToken: f.listToken}, nil
}
func (f *fakeServer) AbortSaga(_ context.Context, _ *pb.AbortSagaRequest) (*pb.AbortSagaResponse, error) {
	return &pb.AbortSagaResponse{Saga: f.abortResp}, nil
}

// metadataCapturingServer records the incoming metadata from the first RPC it
// receives so WithToken tests can assert the Authorization header.
type metadataCapturingServer struct {
	pb.UnimplementedSagaOrchestratorServer
	md chan metadata.MD
}

func (s *metadataCapturingServer) GetSaga(ctx context.Context, _ *pb.GetSagaRequest) (*pb.GetSagaResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.md <- md
	return &pb.GetSagaResponse{Saga: &pb.SagaExecution{Id: "x"}}, nil
}

// newFakeServer starts an in-process gRPC server backed by srv, registers it,
// and returns a connected *client.Client plus a cleanup function.
func newFakeServer(t *testing.T, srv pb.SagaOrchestratorServer) (*client.Client, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterSagaOrchestratorServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()

	c, err := client.New(lis.Addr().String(), client.WithInsecure())
	if err != nil {
		grpcSrv.Stop()
		t.Fatalf("client.New: %v", err)
	}
	return c, func() {
		_ = c.Close()
		grpcSrv.GracefulStop()
	}
}

func TestCreateSaga(t *testing.T) {
	t.Parallel()
	want := &pb.SagaExecution{Id: "saga-1", Name: "order", Status: pb.SagaStatus_SAGA_STATUS_PENDING}
	c, cleanup := newFakeServer(t, &fakeServer{createResp: want})
	defer cleanup()

	got, err := c.CreateSaga(context.Background(), &pb.CreateSagaRequest{Name: "order"})
	if err != nil {
		t.Fatalf("CreateSaga: %v", err)
	}
	if got.Id != want.Id {
		t.Errorf("ID: want %q, got %q", want.Id, got.Id)
	}
}

func TestStartSaga(t *testing.T) {
	t.Parallel()
	want := &pb.SagaExecution{Id: "saga-1", Status: pb.SagaStatus_SAGA_STATUS_COMPLETED}
	c, cleanup := newFakeServer(t, &fakeServer{startResp: want})
	defer cleanup()

	got, err := c.StartSaga(context.Background(), "saga-1")
	if err != nil {
		t.Fatalf("StartSaga: %v", err)
	}
	if got.Status != want.Status {
		t.Errorf("Status: want %v, got %v", want.Status, got.Status)
	}
}

func TestGetSaga(t *testing.T) {
	t.Parallel()
	want := &pb.SagaExecution{Id: "saga-2", Status: pb.SagaStatus_SAGA_STATUS_RUNNING}
	c, cleanup := newFakeServer(t, &fakeServer{getResp: want})
	defer cleanup()

	got, err := c.GetSaga(context.Background(), "saga-2")
	if err != nil {
		t.Fatalf("GetSaga: %v", err)
	}
	if got.Id != want.Id {
		t.Errorf("ID: want %q, got %q", want.Id, got.Id)
	}
}

func TestListSagas(t *testing.T) {
	t.Parallel()
	sagas := []*pb.SagaExecution{
		{Id: "s1", Status: pb.SagaStatus_SAGA_STATUS_COMPLETED},
		{Id: "s2", Status: pb.SagaStatus_SAGA_STATUS_FAILED},
	}
	c, cleanup := newFakeServer(t, &fakeServer{listResp: sagas, listToken: "tok-2"})
	defer cleanup()

	got, nextToken, err := c.ListSagas(context.Background(), &pb.ListSagasRequest{PageSize: 2})
	if err != nil {
		t.Fatalf("ListSagas: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("len(sagas): want 2, got %d", len(got))
	}
	if nextToken != "tok-2" {
		t.Errorf("nextPageToken: want %q, got %q", "tok-2", nextToken)
	}
}

func TestAbortSaga(t *testing.T) {
	t.Parallel()
	want := &pb.SagaExecution{Id: "saga-3", Status: pb.SagaStatus_SAGA_STATUS_ABORTED}
	c, cleanup := newFakeServer(t, &fakeServer{abortResp: want})
	defer cleanup()

	got, err := c.AbortSaga(context.Background(), "saga-3")
	if err != nil {
		t.Fatalf("AbortSaga: %v", err)
	}
	if got.Status != pb.SagaStatus_SAGA_STATUS_ABORTED {
		t.Errorf("Status: want ABORTED, got %v", got.Status)
	}
}

func TestWithToken(t *testing.T) {
	t.Parallel()
	mdCh := make(chan metadata.MD, 1)
	srv := &metadataCapturingServer{md: mdCh}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterSagaOrchestratorServer(grpcSrv, srv)
	go func() { _ = grpcSrv.Serve(lis) }()
	defer grpcSrv.GracefulStop()

	c, err := client.New(lis.Addr().String(),
		client.WithInsecure(),
		client.WithToken("secret-token"),
	)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	defer func() { _ = c.Close() }()

	if _, err := c.GetSaga(context.Background(), "any"); err != nil {
		t.Fatalf("GetSaga: %v", err)
	}

	md := <-mdCh
	auth := md.Get("authorization")
	if len(auth) == 0 || auth[0] != "Bearer secret-token" {
		t.Errorf("authorization header: want %q, got %v", "Bearer secret-token", auth)
	}
}

func TestClose(t *testing.T) {
	t.Parallel()
	c, cleanup := newFakeServer(t, &fakeServer{getResp: &pb.SagaExecution{Id: "x"}})
	defer cleanup()

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// RPC on a closed connection must fail.
	_, err := c.GetSaga(context.Background(), "x")
	if err == nil {
		t.Fatal("expected error after Close, got nil")
	}
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.OK {
			t.Errorf("expected non-OK status after Close, got OK")
		}
	}
}

func TestNewClientInsecure(t *testing.T) {
	t.Parallel()
	// grpc.NewClient is lazy — connecting to a bad address succeeds at dial time.
	c, err := client.New("localhost:1", client.WithInsecure())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = c.Close() }()

	// But the first RPC should fail quickly with a connection-refused error.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err = c.GetSaga(ctx, "any")
	if err == nil {
		t.Fatal("expected error dialling unreachable server, got nil")
	}
}
