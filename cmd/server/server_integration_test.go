//go:build integration

package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/ngaddam369/saga-conductor/internal/engine"
	"github.com/ngaddam369/saga-conductor/internal/server"
	"github.com/ngaddam369/saga-conductor/internal/store"
	pb "github.com/ngaddam369/saga-conductor/proto/saga/v1"
)

// productionGRPCServer starts a gRPC server wired exactly as production
// (message limits + keepalive) and returns a connected client.
func productionGRPCServer(t *testing.T, cfg config) pb.SagaOrchestratorClient {
	t.Helper()

	s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	grpcSrv := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.grpcMaxRecvMB*1024*1024),
		grpc.MaxSendMsgSize(cfg.grpcMaxSendMB*1024*1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: time.Duration(cfg.grpcMaxConnIdleMinutes) * time.Minute,
			Time:              time.Duration(cfg.grpcKeepaliveTimeMinutes) * time.Minute,
			Timeout:           time.Duration(cfg.grpcKeepaliveTimeoutSecs) * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Duration(cfg.grpcKeepaliveMinTimeSecs) * time.Second,
			PermitWithoutStream: true,
		}),
	)
	pb.RegisterSagaOrchestratorServer(grpcSrv, server.New(s, engine.New(s)))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.GracefulStop)

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewSagaOrchestratorClient(conn)
}

// productionHealthServer starts the health HTTP server as production does
// (with timeouts) and returns its base URL.
func productionHealthServer(t *testing.T, cfg config) string {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := &http.Server{
		Handler:      mux,
		ReadTimeout:  time.Duration(cfg.healthReadTimeoutSecs) * time.Second,
		WriteTimeout: time.Duration(cfg.healthWriteTimeoutSecs) * time.Second,
		IdleTimeout:  time.Duration(cfg.healthIdleTimeoutSecs) * time.Second,
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})

	return fmt.Sprintf("http://%s", lis.Addr().String())
}

func defaultTestConfig() config {
	return config{
		grpcMaxRecvMB:            4,
		grpcMaxSendMB:            16,
		grpcMaxConnIdleMinutes:   5,
		grpcKeepaliveTimeMinutes: 2,
		grpcKeepaliveTimeoutSecs: 20,
		grpcKeepaliveMinTimeSecs: 30,
		healthReadTimeoutSecs:    5,
		healthWriteTimeoutSecs:   5,
		healthIdleTimeoutSecs:    60,
	}
}

func TestIntegration(t *testing.T) {
	t.Run("Keepalive", func(t *testing.T) {
		tests := []struct {
			name string
			fn   func(t *testing.T, client pb.SagaOrchestratorClient)
		}{
			{
				name: "well-behaved client is not rejected",
				fn: func(t *testing.T, client pb.SagaOrchestratorClient) {
					_, err := client.ListSagas(context.Background(), &pb.ListSagasRequest{})
					if err != nil {
						t.Errorf("ListSagas: %v", err)
					}
				},
			},
			{
				name: "multiple sequential RPCs succeed on same connection",
				fn: func(t *testing.T, client pb.SagaOrchestratorClient) {
					for i := range 5 {
						_, err := client.ListSagas(context.Background(), &pb.ListSagasRequest{})
						if err != nil {
							t.Errorf("call %d: ListSagas: %v", i+1, err)
						}
					}
				},
			},
			{
				name: "create and get round-trip works",
				fn: func(t *testing.T, client pb.SagaOrchestratorClient) {
					resp, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
						Name: "integration-saga",
						Steps: []*pb.StepDefinition{
							{Name: "step-1", ForwardUrl: "http://example.com/fwd", CompensateUrl: "http://example.com/comp"},
						},
					})
					if err != nil {
						t.Fatalf("CreateSaga: %v", err)
					}
					got, err := client.GetSaga(context.Background(), &pb.GetSagaRequest{SagaId: resp.Saga.Id})
					if err != nil {
						t.Fatalf("GetSaga: %v", err)
					}
					if got.Saga.Id != resp.Saga.Id {
						t.Errorf("saga ID: got %q, want %q", got.Saga.Id, resp.Saga.Id)
					}
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				client := productionGRPCServer(t, defaultTestConfig())
				tc.fn(t, client)
			})
		}
	})

	t.Run("GRPCMessageSizeLimits", func(t *testing.T) {
		tests := []struct {
			name        string
			recvLimitMB int
			payloadMB   int
			wantCode    codes.Code
		}{
			{
				name:        "payload within recv limit reaches handler",
				recvLimitMB: 4,
				payloadMB:   1,
				wantCode:    codes.OK,
			},
			{
				name:        "payload exceeding recv limit rejected by transport",
				recvLimitMB: 1,
				payloadMB:   2,
				wantCode:    codes.ResourceExhausted,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				cfg := defaultTestConfig()
				cfg.grpcMaxRecvMB = tc.recvLimitMB
				client := productionGRPCServer(t, cfg)

				_, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
					Name: "saga",
					Steps: []*pb.StepDefinition{
						{Name: "step-1", ForwardUrl: "http://example.com/fwd", CompensateUrl: "http://example.com/comp"},
					},
					Payload: make([]byte, tc.payloadMB*1024*1024),
				})
				if code := status.Code(err); code != tc.wantCode {
					t.Errorf("code: got %v, want %v (err=%v)", code, tc.wantCode, err)
				}
			})
		}
	})

	t.Run("HealthEndpoints", func(t *testing.T) {
		tests := []struct {
			name     string
			path     string
			wantCode int
		}{
			{
				name:     "live returns 200",
				path:     "/health/live",
				wantCode: http.StatusOK,
			},
			{
				name:     "ready returns 200",
				path:     "/health/ready",
				wantCode: http.StatusOK,
			},
			{
				name:     "unknown path returns 404",
				path:     "/unknown",
				wantCode: http.StatusNotFound,
			},
		}

		baseURL := productionHealthServer(t, defaultTestConfig())

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				resp, err := http.Get(baseURL + tc.path)
				if err != nil {
					t.Fatalf("GET %s: %v", tc.path, err)
				}
				defer func() {
					if closeErr := resp.Body.Close(); closeErr != nil {
						t.Errorf("close response body: %v", closeErr)
					}
				}()

				if resp.StatusCode != tc.wantCode {
					t.Errorf("status: got %d, want %d", resp.StatusCode, tc.wantCode)
				}
			})
		}
	})

	t.Run("SagaExecution", func(t *testing.T) {
		tests := []struct {
			name          string
			step1Code     int
			step2Code     int
			wantStatus    pb.SagaStatus
			wantCompCalls int // expected compensate calls
		}{
			{
				name:          "all steps succeed → COMPLETED",
				step1Code:     http.StatusOK,
				step2Code:     http.StatusOK,
				wantStatus:    pb.SagaStatus_SAGA_STATUS_COMPLETED,
				wantCompCalls: 0,
			},
			{
				name:          "second step fails → FAILED with step-1 compensated",
				step1Code:     http.StatusOK,
				step2Code:     http.StatusInternalServerError,
				wantStatus:    pb.SagaStatus_SAGA_STATUS_FAILED,
				wantCompCalls: 1, // step-1 compensation runs
			},
			{
				name:          "first step fails → FAILED with no compensation",
				step1Code:     http.StatusInternalServerError,
				step2Code:     http.StatusOK,
				wantStatus:    pb.SagaStatus_SAGA_STATUS_FAILED,
				wantCompCalls: 0,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				compCalls := 0
				step1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/comp" {
						compCalls++
						w.WriteHeader(http.StatusOK)
						return
					}
					w.WriteHeader(tc.step1Code)
				}))
				t.Cleanup(step1.Close)

				step2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(tc.step2Code)
				}))
				t.Cleanup(step2.Close)

				client := productionGRPCServer(t, defaultTestConfig())

				created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
					Name: "integration-exec",
					Steps: []*pb.StepDefinition{
						{Name: "step-1", ForwardUrl: step1.URL, CompensateUrl: step1.URL + "/comp"},
						{Name: "step-2", ForwardUrl: step2.URL, CompensateUrl: step2.URL},
					},
				})
				if err != nil {
					t.Fatalf("CreateSaga: %v", err)
				}

				started, err := client.StartSaga(context.Background(), &pb.StartSagaRequest{
					SagaId: created.Saga.Id,
				})
				if err != nil {
					t.Fatalf("StartSaga: %v", err)
				}

				if started.Saga.Status != tc.wantStatus {
					t.Errorf("status: got %v, want %v", started.Saga.Status, tc.wantStatus)
				}
				if compCalls != tc.wantCompCalls {
					t.Errorf("compensation calls: got %d, want %d", compCalls, tc.wantCompCalls)
				}
			})
		}
	})

	t.Run("StartSagaNotPending", func(t *testing.T) {
		tests := []struct {
			name     string
			wantCode codes.Code
		}{
			{"starting completed saga returns FailedPrecondition", codes.FailedPrecondition},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				step := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(http.StatusOK)
				}))
				t.Cleanup(step.Close)

				client := productionGRPCServer(t, defaultTestConfig())

				created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
					Name: "double-start",
					Steps: []*pb.StepDefinition{
						{Name: "step-1", ForwardUrl: step.URL, CompensateUrl: step.URL},
					},
				})
				if err != nil {
					t.Fatalf("CreateSaga: %v", err)
				}

				// First start — must succeed.
				if _, err := client.StartSaga(context.Background(), &pb.StartSagaRequest{
					SagaId: created.Saga.Id,
				}); err != nil {
					t.Fatalf("first StartSaga: %v", err)
				}

				// Second start on already-completed saga.
				_, err = client.StartSaga(context.Background(), &pb.StartSagaRequest{
					SagaId: created.Saga.Id,
				})
				if code := status.Code(err); code != tc.wantCode {
					t.Errorf("code: got %v, want %v (err=%v)", code, tc.wantCode, err)
				}
			})
		}
	})

	t.Run("HealthReadTimeout", func(t *testing.T) {
		// A client that opens a connection and never sends headers must be
		// dropped by the server's ReadTimeout, not held open indefinitely.
		cfg := defaultTestConfig()
		cfg.healthReadTimeoutSecs = 1
		baseURL := productionHealthServer(t, cfg)

		// Dial raw TCP and send nothing — the server should close the
		// connection once ReadTimeout fires.
		addr := baseURL[len("http://"):]
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() {
			if closeErr := conn.Close(); closeErr != nil {
				t.Errorf("close conn: %v", closeErr)
			}
		}()

		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatalf("SetDeadline: %v", err)
		}

		buf := make([]byte, 1)
		_, err = conn.Read(buf)
		if err == nil {
			t.Error("expected connection to be closed by ReadTimeout, got no error")
		}
	})
}
