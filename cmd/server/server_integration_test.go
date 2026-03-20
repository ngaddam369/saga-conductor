//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	"github.com/ngaddam369/saga-conductor/internal/engine"
	"github.com/ngaddam369/saga-conductor/internal/saga"
	"github.com/ngaddam369/saga-conductor/internal/server"
	"github.com/ngaddam369/saga-conductor/internal/store"
	pb "github.com/ngaddam369/saga-conductor/proto/saga/v1"
)

// productionGRPCServer starts a gRPC server wired exactly as production
// (message limits + keepalive) and returns a connected client.
func productionGRPCServer(t *testing.T, cfg config) pb.SagaOrchestratorClient {
	t.Helper()
	client, _ := productionGRPCServerWithEngine(t, cfg)
	return client
}

// productionGRPCServerWithEngine is like productionGRPCServer but also returns
// the underlying Engine so tests can call Drain() directly.
func productionGRPCServerWithEngine(t *testing.T, cfg config) (pb.SagaOrchestratorClient, *engine.Engine) {
	t.Helper()

	s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	eng := engine.New(s)

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
	pb.RegisterSagaOrchestratorServer(grpcSrv, server.New(s, eng, 24*time.Hour))

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

	return pb.NewSagaOrchestratorClient(conn), eng
}

// productionHealthServer starts the health HTTP server as production does
// (with timeouts, a real store-backed liveness check, and a readiness flag)
// and returns its base URL. Pass a ready flag initialised to true for normal
// operation; set it to false to simulate the shutdown drain phase.
func productionHealthServer(t *testing.T, cfg config, ready *atomic.Bool, s store.Store) string {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if err := s.Ping(ctx); err != nil {
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, _ *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
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

// contains reports whether b contains the UTF-8 substring sub.
func contains(b []byte, sub string) bool {
	return bytes.Contains(b, []byte(sub))
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

		hs, err := store.NewBoltStore(filepath.Join(t.TempDir(), "health.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = hs.Close() })
		var ready atomic.Bool
		ready.Store(true)
		baseURL := productionHealthServer(t, defaultTestConfig(), &ready, hs)

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

	t.Run("StepRetry", func(t *testing.T) {
		tests := []struct {
			name       string
			maxRetries string // STEP_MAX_RETRIES env value
			failFirst  int    // how many requests return 500 before 200
			wantStatus pb.SagaStatus
		}{
			{
				name:       "transient failure retried to completion",
				maxRetries: "3",
				failFirst:  2,
				wantStatus: pb.SagaStatus_SAGA_STATUS_COMPLETED,
			},
			{
				name:       "retries exhausted triggers compensation",
				maxRetries: "2",
				failFirst:  10, // always fails within retry budget
				wantStatus: pb.SagaStatus_SAGA_STATUS_FAILED,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				t.Setenv("STEP_MAX_RETRIES", tc.maxRetries)
				t.Setenv("STEP_RETRY_BACKOFF_MS", "1")

				calls := 0
				stepSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					calls++
					if calls <= tc.failFirst {
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					w.WriteHeader(http.StatusOK)
				}))
				t.Cleanup(stepSrv.Close)

				// productionGRPCServer calls engine.New internally, which reads
				// STEP_MAX_RETRIES / STEP_RETRY_BACKOFF_MS at construction time.
				client := productionGRPCServer(t, defaultTestConfig())

				created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
					Name: "retry-integration",
					Steps: []*pb.StepDefinition{
						{Name: "step-1", ForwardUrl: stepSrv.URL, CompensateUrl: stepSrv.URL},
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

	t.Run("PerStepTimeout", func(t *testing.T) {
		// A saga step with timeout_seconds=1 must abort within ~1s even though
		// the engine's default STEP_TIMEOUT_SECONDS is much larger.
		// Disable retries so the test only measures the per-step timeout.
		t.Setenv("STEP_TIMEOUT_SECONDS", "30")
		t.Setenv("STEP_MAX_RETRIES", "0")

		done := make(chan struct{})
		hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-done
		}))
		t.Cleanup(func() { close(done); hang.Close() })

		client := productionGRPCServer(t, defaultTestConfig())

		created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
			Name: "per-step-timeout",
			Steps: []*pb.StepDefinition{
				{
					Name:           "step-1",
					ForwardUrl:     hang.URL,
					CompensateUrl:  hang.URL,
					TimeoutSeconds: 1,
				},
			},
		})
		if err != nil {
			t.Fatalf("CreateSaga: %v", err)
		}

		start := time.Now()
		started, err := client.StartSaga(context.Background(), &pb.StartSagaRequest{
			SagaId: created.Saga.Id,
		})
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("StartSaga: %v", err)
		}
		if started.Saga.Status != pb.SagaStatus_SAGA_STATUS_FAILED {
			t.Errorf("status: got %v, want FAILED", started.Saga.Status)
		}
		if elapsed > 3*time.Second {
			t.Errorf("took %v; per-step timeout of 1s should have aborted quickly", elapsed)
		}
	})

	t.Run("SagaTimeout", func(t *testing.T) {
		// SAGA_TIMEOUT_SECONDS=1 must abort a hanging step and surface as
		// codes.DeadlineExceeded on the gRPC caller. The saga must be FAILED.
		t.Setenv("SAGA_TIMEOUT_SECONDS", "1")
		t.Setenv("STEP_TIMEOUT_SECONDS", "30")
		t.Setenv("STEP_MAX_RETRIES", "0")

		done := make(chan struct{})
		hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-done
		}))
		t.Cleanup(func() { close(done); hang.Close() })

		cfg := defaultTestConfig()
		client := productionGRPCServer(t, cfg)
		ctx := context.Background()

		created, err := client.CreateSaga(ctx, &pb.CreateSagaRequest{
			Name: "timeout-saga",
			Steps: []*pb.StepDefinition{
				{Name: "step-1", ForwardUrl: hang.URL, CompensateUrl: hang.URL},
			},
		})
		if err != nil {
			t.Fatalf("CreateSaga: %v", err)
		}

		start := time.Now()
		_, startErr := client.StartSaga(ctx, &pb.StartSagaRequest{SagaId: created.Saga.Id})
		elapsed := time.Since(start)

		if st, ok := status.FromError(startErr); !ok || st.Code() != codes.DeadlineExceeded {
			t.Errorf("StartSaga error code: got %v, want DeadlineExceeded", startErr)
		}
		if elapsed > 5*time.Second {
			t.Errorf("took %v; expected saga timeout of 1s to abort quickly", elapsed)
		}

		got, err := client.GetSaga(ctx, &pb.GetSagaRequest{SagaId: created.Saga.Id})
		if err != nil {
			t.Fatalf("GetSaga: %v", err)
		}
		if got.Saga.Status != pb.SagaStatus_SAGA_STATUS_FAILED {
			t.Errorf("saga status: got %v, want FAILED", got.Saga.Status)
		}
	})

	t.Run("LivenessCheck", func(t *testing.T) {
		t.Run("returns 200 when store is healthy", func(t *testing.T) {
			hs, err := store.NewBoltStore(filepath.Join(t.TempDir(), "live-ok.db"))
			if err != nil {
				t.Fatalf("NewBoltStore: %v", err)
			}
			t.Cleanup(func() { _ = hs.Close() })
			var ready atomic.Bool
			ready.Store(true)
			baseURL := productionHealthServer(t, defaultTestConfig(), &ready, hs)

			resp, err := http.Get(baseURL + "/health/live")
			if err != nil {
				t.Fatalf("GET /health/live: %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Errorf("status: got %d, want 200", resp.StatusCode)
			}
		})

		t.Run("returns 503 when store is closed", func(t *testing.T) {
			hs, err := store.NewBoltStore(filepath.Join(t.TempDir(), "live-fail.db"))
			if err != nil {
				t.Fatalf("NewBoltStore: %v", err)
			}
			// Close the store before starting the server to simulate a storage failure.
			if err = hs.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			var ready atomic.Bool
			ready.Store(true)
			baseURL := productionHealthServer(t, defaultTestConfig(), &ready, hs)

			resp, err := http.Get(baseURL + "/health/live")
			if err != nil {
				t.Fatalf("GET /health/live: %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Errorf("status: got %d, want 503", resp.StatusCode)
			}
		})
	})

	t.Run("ReadinessDrain", func(t *testing.T) {
		// /health/ready must return 200 while ready=true and 503 after it is
		// flipped to false, simulating the SIGTERM drain phase.
		hs, err := store.NewBoltStore(filepath.Join(t.TempDir(), "drain.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = hs.Close() })
		var ready atomic.Bool
		ready.Store(true)
		baseURL := productionHealthServer(t, defaultTestConfig(), &ready, hs)

		get := func(t *testing.T, path string) int {
			t.Helper()
			resp, err := http.Get(baseURL + path)
			if err != nil {
				t.Fatalf("GET %s: %v", path, err)
			}
			_ = resp.Body.Close()
			return resp.StatusCode
		}

		if got := get(t, "/health/ready"); got != http.StatusOK {
			t.Errorf("before drain: got %d, want 200", got)
		}

		ready.Store(false) // simulate SIGTERM

		if got := get(t, "/health/ready"); got != http.StatusServiceUnavailable {
			t.Errorf("after drain: got %d, want 503", got)
		}
		// /health/live must remain 200 throughout — only readiness is affected.
		if got := get(t, "/health/live"); got != http.StatusOK {
			t.Errorf("live after drain: got %d, want 200", got)
		}
	})

	t.Run("AbortSaga", func(t *testing.T) {
		t.Run("aborts pending saga via gRPC", func(t *testing.T) {
			client := productionGRPCServer(t, defaultTestConfig())

			created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
				Name: "abort-pending",
				Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: "http://127.0.0.1:19999/fwd", CompensateUrl: "http://127.0.0.1:19999/comp"},
				},
			})
			if err != nil {
				t.Fatalf("CreateSaga: %v", err)
			}

			resp, err := client.AbortSaga(context.Background(), &pb.AbortSagaRequest{SagaId: created.Saga.Id})
			if err != nil {
				t.Fatalf("AbortSaga: %v", err)
			}
			if resp.Saga.Status != pb.SagaStatus_SAGA_STATUS_ABORTED {
				t.Errorf("status: got %v, want SAGA_STATUS_ABORTED", resp.Saga.Status)
			}

			// GetSaga must also reflect ABORTED.
			got, err := client.GetSaga(context.Background(), &pb.GetSagaRequest{SagaId: created.Saga.Id})
			if err != nil {
				t.Fatalf("GetSaga after abort: %v", err)
			}
			if got.Saga.Status != pb.SagaStatus_SAGA_STATUS_ABORTED {
				t.Errorf("GetSaga status: got %v, want SAGA_STATUS_ABORTED", got.Saga.Status)
			}
		})

		t.Run("aborting terminal saga returns FailedPrecondition", func(t *testing.T) {
			step := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(step.Close)

			client := productionGRPCServer(t, defaultTestConfig())

			created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
				Name: "abort-completed",
				Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: step.URL, CompensateUrl: step.URL},
				},
			})
			if err != nil {
				t.Fatalf("CreateSaga: %v", err)
			}
			if _, err = client.StartSaga(context.Background(), &pb.StartSagaRequest{SagaId: created.Saga.Id}); err != nil {
				t.Fatalf("StartSaga: %v", err)
			}

			_, err = client.AbortSaga(context.Background(), &pb.AbortSagaRequest{SagaId: created.Saga.Id})
			if code := status.Code(err); code != codes.FailedPrecondition {
				t.Errorf("code: got %v, want FailedPrecondition", code)
			}
		})
	})

	t.Run("PerSagaTimeout", func(t *testing.T) {
		// saga_timeout_seconds in CreateSagaRequest must override the global
		// SAGA_TIMEOUT_SECONDS env var for this specific saga.
		t.Setenv("SAGA_TIMEOUT_SECONDS", "30") // global is 30s — per-saga is 1s
		t.Setenv("STEP_TIMEOUT_SECONDS", "60")
		t.Setenv("STEP_MAX_RETRIES", "0")

		done := make(chan struct{})
		hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			<-done
		}))
		t.Cleanup(func() { close(done); hang.Close() })

		client := productionGRPCServer(t, defaultTestConfig())

		created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
			Name:               "per-saga-timeout",
			SagaTimeoutSeconds: 1, // override global 30s → abort in 1s
			Steps: []*pb.StepDefinition{
				{Name: "step-1", ForwardUrl: hang.URL, CompensateUrl: hang.URL},
			},
		})
		if err != nil {
			t.Fatalf("CreateSaga: %v", err)
		}

		start := time.Now()
		_, startErr := client.StartSaga(context.Background(), &pb.StartSagaRequest{SagaId: created.Saga.Id})
		elapsed := time.Since(start)

		if st, ok := status.FromError(startErr); !ok || st.Code() != codes.DeadlineExceeded {
			t.Errorf("StartSaga error code: got %v, want DeadlineExceeded", startErr)
		}
		if elapsed > 5*time.Second {
			t.Errorf("took %v; per-saga timeout of 1s should have aborted quickly", elapsed)
		}
	})

	t.Run("ErrorDetailInResponse", func(t *testing.T) {
		// A failed step must populate error_detail bytes in the gRPC response.
		t.Setenv("STEP_MAX_RETRIES", "0")

		stepSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("business logic rejected"))
		}))
		t.Cleanup(stepSrv.Close)

		client := productionGRPCServer(t, defaultTestConfig())

		created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
			Name: "error-detail-saga",
			Steps: []*pb.StepDefinition{
				{Name: "step-1", ForwardUrl: stepSrv.URL, CompensateUrl: stepSrv.URL},
			},
		})
		if err != nil {
			t.Fatalf("CreateSaga: %v", err)
		}

		started, err := client.StartSaga(context.Background(), &pb.StartSagaRequest{SagaId: created.Saga.Id})
		if err != nil {
			t.Fatalf("StartSaga: %v", err)
		}
		if started.Saga.Status != pb.SagaStatus_SAGA_STATUS_FAILED {
			t.Fatalf("status: got %v, want FAILED", started.Saga.Status)
		}

		step := started.Saga.Steps[0]
		if len(step.ErrorDetail) == 0 {
			t.Fatal("ErrorDetail is empty; want JSON-encoded StepError")
		}
		// Sanity-check that it is valid JSON with expected fields.
		if !contains(step.ErrorDetail, "http_status_code") {
			t.Errorf("ErrorDetail JSON missing http_status_code field: %s", step.ErrorDetail)
		}
	})

	t.Run("ListSagasPagination", func(t *testing.T) {
		// Seed 5 sagas, then page through them 2 at a time and verify all are
		// returned without duplicates.
		client := productionGRPCServer(t, defaultTestConfig())
		ctx := context.Background()

		for i := range 5 {
			_, err := client.CreateSaga(ctx, &pb.CreateSagaRequest{
				Name: fmt.Sprintf("page-saga-%d", i),
				Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: "http://127.0.0.1:19999/fwd", CompensateUrl: "http://127.0.0.1:19999/comp"},
				},
			})
			if err != nil {
				t.Fatalf("CreateSaga %d: %v", i, err)
			}
		}

		var all []*pb.SagaExecution
		var token string
		for {
			resp, err := client.ListSagas(ctx, &pb.ListSagasRequest{PageSize: 2, PageToken: token})
			if err != nil {
				t.Fatalf("ListSagas: %v", err)
			}
			all = append(all, resp.Sagas...)
			token = resp.NextPageToken
			if token == "" {
				break
			}
		}
		if len(all) != 5 {
			t.Errorf("paginated total: got %d, want 5", len(all))
		}
		seen := map[string]bool{}
		for _, sg := range all {
			if seen[sg.Id] {
				t.Errorf("duplicate saga %s across pages", sg.Id)
			}
			seen[sg.Id] = true
		}
	})

	t.Run("IdempotencyKey", func(t *testing.T) {
		// Two CreateSaga calls with the same idempotency_key must return the
		// same saga ID; a third call without a key must create a fresh saga.
		client, _ := productionGRPCServerWithEngine(t, defaultTestConfig())

		req := &pb.CreateSagaRequest{
			Name:           "idempotent-order",
			Steps:          []*pb.StepDefinition{{Name: "s1", ForwardUrl: "http://localhost:9", CompensateUrl: "http://localhost:9"}},
			IdempotencyKey: "order-integration-42",
		}
		resp1, err := client.CreateSaga(context.Background(), req)
		if err != nil {
			t.Fatalf("first CreateSaga: %v", err)
		}
		resp2, err := client.CreateSaga(context.Background(), req)
		if err != nil {
			t.Fatalf("second CreateSaga: %v", err)
		}
		if resp1.Saga.Id != resp2.Saga.Id {
			t.Errorf("IDs differ: %q vs %q; expected same saga on duplicate key", resp1.Saga.Id, resp2.Saga.Id)
		}

		// No idempotency key → always creates a fresh saga.
		reqNoKey := &pb.CreateSagaRequest{
			Name:  "fresh-order",
			Steps: []*pb.StepDefinition{{Name: "s1", ForwardUrl: "http://localhost:9", CompensateUrl: "http://localhost:9"}},
		}
		respA, err := client.CreateSaga(context.Background(), reqNoKey)
		if err != nil {
			t.Fatalf("CreateSaga no-key A: %v", err)
		}
		respB, err := client.CreateSaga(context.Background(), reqNoKey)
		if err != nil {
			t.Fatalf("CreateSaga no-key B: %v", err)
		}
		if respA.Saga.Id == respB.Saga.Id {
			t.Error("no idempotency key: each call must create a distinct saga")
		}
	})

	t.Run("CompensationDeadLetter", func(t *testing.T) {
		// A saga whose compensation step always returns HTTP 500 must dead-letter
		// to SAGA_STATUS_COMPENSATION_FAILED over gRPC.
		t.Setenv("STEP_MAX_RETRIES", "0")

		fwd1Srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(fwd1Srv.Close)

		fwd2Srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		t.Cleanup(fwd2Srv.Close)

		compSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		t.Cleanup(compSrv.Close)

		client, _ := productionGRPCServerWithEngine(t, defaultTestConfig())

		created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
			Name: "dead-letter-saga",
			Steps: []*pb.StepDefinition{
				{Name: "step-1", ForwardUrl: fwd1Srv.URL, CompensateUrl: compSrv.URL},
				{Name: "step-2", ForwardUrl: fwd2Srv.URL, CompensateUrl: compSrv.URL},
			},
		})
		if err != nil {
			t.Fatalf("CreateSaga: %v", err)
		}

		// StartSaga returns an error on compensation exhaustion but also
		// the final saga state via GetSaga.
		_, _ = client.StartSaga(context.Background(), &pb.StartSagaRequest{SagaId: created.Saga.Id})

		got, err := client.GetSaga(context.Background(), &pb.GetSagaRequest{SagaId: created.Saga.Id})
		if err != nil {
			t.Fatalf("GetSaga: %v", err)
		}
		if got.Saga.Status != pb.SagaStatus_SAGA_STATUS_COMPENSATION_FAILED {
			t.Errorf("saga status: got %v, want COMPENSATION_FAILED", got.Saga.Status)
		}
		// step-1 compensation failed; step-2 was never compensated (halt-on-failure)
		if got.Saga.Steps[0].Status != pb.StepStatus_STEP_STATUS_COMPENSATION_FAILED {
			t.Errorf("step-1 status: got %v, want COMPENSATION_FAILED", got.Saga.Steps[0].Status)
		}
		if got.Saga.Steps[1].Status != pb.StepStatus_STEP_STATUS_FAILED {
			t.Errorf("step-2 status: got %v, want FAILED", got.Saga.Steps[1].Status)
		}
	})

	t.Run("GracefulDrain", func(t *testing.T) {
		// While a StartSaga call is in-flight, simulate the SIGTERM shutdown
		// sequence by calling eng.Drain. Verify:
		//   (a) Drain waits for the in-flight StartSaga to complete normally.
		//   (b) A new StartSaga issued after Drain returns codes.Unavailable.
		t.Setenv("STEP_MAX_RETRIES", "0")

		hit := make(chan struct{})
		release := make(chan struct{})
		stepSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(hit)
			<-release
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(stepSrv.Close)

		client, eng := productionGRPCServerWithEngine(t, defaultTestConfig())

		created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
			Name: "drain-saga",
			Steps: []*pb.StepDefinition{
				{Name: "step-1", ForwardUrl: stepSrv.URL, CompensateUrl: stepSrv.URL},
			},
		})
		if err != nil {
			t.Fatalf("CreateSaga: %v", err)
		}

		startDone := make(chan error, 1)
		go func() {
			_, err := client.StartSaga(context.Background(), &pb.StartSagaRequest{SagaId: created.Saga.Id})
			startDone <- err
		}()

		// Wait until the step server receives the request — Start is past the
		// inflight registration at this point.
		<-hit

		drainDone := make(chan []string, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			drainDone <- eng.Drain(ctx)
		}()

		// Give Drain a moment to block on the WaitGroup.
		time.Sleep(20 * time.Millisecond)
		select {
		case result := <-drainDone:
			t.Fatalf("Drain returned early with interrupted IDs: %v", result)
		default:
		}

		close(release) // let the step succeed and StartSaga return

		interrupted := <-drainDone
		if len(interrupted) != 0 {
			t.Errorf("Drain: unexpected interrupted IDs: %v", interrupted)
		}
		if err := <-startDone; err != nil {
			t.Errorf("StartSaga: %v", err)
		}

		// After Drain, a new StartSaga must be rejected with Unavailable.
		created2, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
			Name: "drain-saga-2",
			Steps: []*pb.StepDefinition{
				{Name: "step-1", ForwardUrl: stepSrv.URL, CompensateUrl: stepSrv.URL},
			},
		})
		if err != nil {
			t.Fatalf("CreateSaga 2: %v", err)
		}
		_, err = client.StartSaga(context.Background(), &pb.StartSagaRequest{SagaId: created2.Saga.Id})
		if code := status.Code(err); code != codes.Unavailable {
			t.Errorf("StartSaga after drain: got %v, want Unavailable", code)
		}
	})

	t.Run("GRPCStopWithTimeoutIntegration", func(t *testing.T) {
		// Verify the real gRPC server shuts down correctly via grpcStopWithTimeout.
		// Two sub-cases:
		//   (a) No active connections — GracefulStop completes immediately, well
		//       within the 5s timeout.
		//   (b) Force-stop path — a client holds an open connection; the 50ms
		//       timeout fires and Stop() force-closes it.

		t.Run("graceful stop completes when server is idle", func(t *testing.T) {
			s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "stop-idle.db"))
			if err != nil {
				t.Fatalf("NewBoltStore: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })

			grpcSrv := grpc.NewServer()
			pb.RegisterSagaOrchestratorServer(grpcSrv, server.New(s, engine.New(s), 24*time.Hour))

			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			go func() { _ = grpcSrv.Serve(lis) }()

			start := time.Now()
			grpcStopWithTimeout(grpcSrv, 5*time.Second, zerolog.Nop())
			if elapsed := time.Since(start); elapsed > 3*time.Second {
				t.Errorf("took %v; expected idle server to stop quickly", elapsed)
			}
		})

		t.Run("force stop fires when client holds connection past timeout", func(t *testing.T) {
			s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "stop-force.db"))
			if err != nil {
				t.Fatalf("NewBoltStore: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })

			// Use a slow step so StartSaga stays in-flight, keeping a gRPC stream
			// active and preventing GracefulStop from completing.
			release := make(chan struct{})
			stepSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				<-release
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(func() { close(release); stepSrv.Close() })

			eng := engine.New(s, engine.WithDefaultMaxRetries(0))
			grpcSrv := grpc.NewServer()
			pb.RegisterSagaOrchestratorServer(grpcSrv, server.New(s, eng, 24*time.Hour))

			lis, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			go func() { _ = grpcSrv.Serve(lis) }()

			conn, err := grpc.NewClient(lis.Addr().String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			client := pb.NewSagaOrchestratorClient(conn)

			created, err := client.CreateSaga(context.Background(), &pb.CreateSagaRequest{
				Name: "stop-saga",
				Steps: []*pb.StepDefinition{
					{Name: "step-1", ForwardUrl: stepSrv.URL, CompensateUrl: stepSrv.URL},
				},
			})
			if err != nil {
				t.Fatalf("CreateSaga: %v", err)
			}
			// StartSaga blocks while step-1 is held; this keeps the gRPC stream open.
			go func() {
				_, _ = client.StartSaga(context.Background(), &pb.StartSagaRequest{SagaId: created.Saga.Id})
			}()

			// Give StartSaga a moment to reach the step server and open the stream.
			time.Sleep(20 * time.Millisecond)

			const stopTimeout = 100 * time.Millisecond
			start := time.Now()
			grpcStopWithTimeout(grpcSrv, stopTimeout, zerolog.Nop())
			elapsed := time.Since(start)

			if elapsed < stopTimeout {
				t.Errorf("returned after %v; expected to wait at least %s before forcing", elapsed, stopTimeout)
			}
			if elapsed > 3*time.Second {
				t.Errorf("took %v; expected force stop within ~%s", elapsed, stopTimeout)
			}
		})
	})

	t.Run("HealthReadTimeout", func(t *testing.T) {
		// A client that opens a connection and never sends headers must be
		// dropped by the server's ReadTimeout, not held open indefinitely.
		hs, err := store.NewBoltStore(filepath.Join(t.TempDir(), "timeout.db"))
		if err != nil {
			t.Fatalf("NewBoltStore: %v", err)
		}
		t.Cleanup(func() { _ = hs.Close() })
		cfg := defaultTestConfig()
		cfg.healthReadTimeoutSecs = 1
		var ready atomic.Bool
		ready.Store(true)
		baseURL := productionHealthServer(t, cfg, &ready, hs)

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

func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()

	// Use a fresh registry per test to avoid duplicate-registration panics.
	reg := prometheus.NewRegistry()
	rec := newPrometheusRecorder(reg)

	s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "metrics.db"))
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	eng := engine.New(s, engine.WithRecorder(rec))

	// Spin up an HTTP server with /metrics backed by our custom registry.
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	httpSrv := httptest.NewServer(mux)
	t.Cleanup(httpSrv.Close)

	// Run a two-step saga so we get both saga and step metrics.
	fwd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(fwd.Close)

	createErr := s.Create(context.Background(), &saga.Execution{
		ID:      "m-saga-1",
		Status:  saga.SagaStatusPending,
		Payload: []byte(`{}`),
		StepDefs: []saga.StepDefinition{
			{Name: "step1", ForwardURL: fwd.URL, CompensateURL: fwd.URL},
		},
		Steps: []saga.StepExecution{
			{Name: "step1", Status: saga.StepStatusPending},
		},
	})
	if createErr != nil {
		t.Fatalf("Create: %v", createErr)
	}
	if _, err := eng.Start(context.Background(), "m-saga-1"); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resp, err := http.Get(httpSrv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /metrics: want 200, got %d", resp.StatusCode)
	}

	var body bytes.Buffer
	if _, err := body.ReadFrom(resp.Body); err != nil {
		t.Fatalf("read body: %v", err)
	}
	text := body.String()

	for _, want := range []string{
		"saga_conductor_saga_total",
		"saga_conductor_saga_duration_seconds",
		"saga_conductor_step_total",
		"saga_conductor_step_duration_seconds",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("/metrics body missing %q", want)
		}
	}

	// Verify the saga completed counter is present with label status=COMPLETED.
	if !strings.Contains(text, `saga_conductor_saga_total{status="COMPLETED"}`) {
		t.Errorf("/metrics missing saga_conductor_saga_total{status=COMPLETED}; got:\n%s", text)
	}
}
