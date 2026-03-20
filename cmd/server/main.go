package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/ngaddam369/saga-conductor/internal/engine"
	"github.com/ngaddam369/saga-conductor/internal/purger"
	"github.com/ngaddam369/saga-conductor/internal/scheduler"
	"github.com/ngaddam369/saga-conductor/internal/server"
	"github.com/ngaddam369/saga-conductor/internal/store"
	pb "github.com/ngaddam369/saga-conductor/proto/saga/v1"
)

// grpcStopper is the subset of *grpc.Server used by grpcStopWithTimeout.
// Extracted as an interface so the function can be tested without a real server.
type grpcStopper interface {
	GracefulStop()
	Stop()
}

// grpcStopWithTimeout attempts a graceful gRPC server stop, waiting up to
// timeout for all open connections to close. If the deadline is exceeded,
// Stop() is called to force-close any remaining connections.
func grpcStopWithTimeout(srv grpcStopper, timeout time.Duration, log zerolog.Logger) {
	graceDone := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(graceDone)
	}()
	select {
	case <-graceDone:
	case <-time.After(timeout):
		log.Error().Dur("timeout", timeout).Msg("gRPC graceful stop timed out; forcing stop")
		srv.Stop()
	}
}

func main() {
	log := zerolog.New(os.Stderr).With().Timestamp().Str("service", "saga-conductor").Logger()
	if err := run(log); err != nil {
		log.Fatal().Err(err).Msg("startup failed")
	}
}

func run(log zerolog.Logger) error {
	cfg := loadConfig()

	s, err := store.NewBoltStore(cfg.dbPath)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() {
		if err = s.Close(); err != nil {
			log.Error().Err(err).Msg("close store failed")
		}
	}()

	rec := newPrometheusRecorder(prometheus.DefaultRegisterer)
	eng := engine.New(s, engine.WithLogger(log), engine.WithRecorder(rec))

	// Resume any sagas left in RUNNING or COMPENSATING state by a previous crash.
	resumeCtx := log.WithContext(context.Background())
	if err = scheduler.New(s, eng).Run(resumeCtx); err != nil {
		return fmt.Errorf("scheduler: %w", err)
	}

	// Start background data-retention purger.
	purgeCtx, purgeCancel := context.WithCancel(context.Background())
	defer purgeCancel()
	go purger.New(s).Run(purgeCtx)

	srv := server.New(s, eng, time.Duration(cfg.idempotencyKeyTTLHours)*time.Hour)

	handlerTimeout := time.Duration(cfg.grpcHandlerTimeoutSecs) * time.Second
	// AUTH_TYPE controls inbound gRPC authentication. Currently only "none"
	// (default) is implemented. Future tasks add cases without restructuring main.
	var validator server.TokenValidator // nil = open server (no-op)
	_ = validator                       // AUTH_TYPE wiring added in a later task

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			server.AuthInterceptor(validator),
			server.TimeoutInterceptor(handlerTimeout),
		),
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
	pb.RegisterSagaOrchestratorServer(grpcServer, srv)

	lis, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.grpcAddr, err)
	}

	// Health HTTP server.
	// ready is flipped to false on SIGTERM so the load balancer stops routing
	// traffic before GracefulStop() closes connections.
	var ready atomic.Bool
	ready.Store(true)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health/live", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
		defer cancel()
		if err := s.Ping(ctx); err != nil {
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	// Admin: forcibly abort a stuck saga.
	// POST /admin/sagas/{id}/abort
	// Returns 200 with {"id":"...","status":"ABORTED"} on success.
	// Returns 404 if not found, 409 if already terminal, 500 otherwise.
	mux.HandleFunc("POST /admin/sagas/{id}/abort", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "missing saga id", http.StatusBadRequest)
			return
		}
		exec, err := eng.Abort(r.Context(), id)
		if err != nil {
			switch {
			case errors.Is(err, store.ErrNotFound):
				http.Error(w, "saga not found", http.StatusNotFound)
			case errors.Is(err, store.ErrAlreadyCompleted),
				errors.Is(err, store.ErrAlreadyFailed),
				errors.Is(err, store.ErrAlreadyAborted):
				http.Error(w, err.Error(), http.StatusConflict)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err = json.NewEncoder(w).Encode(map[string]string{
			"id":     exec.ID,
			"status": string(exec.Status),
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/health/ready", func(w http.ResponseWriter, _ *http.Request) {
		if ready.Load() {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	healthSrv := &http.Server{
		Addr:         cfg.healthAddr,
		Handler:      mux,
		ReadTimeout:  time.Duration(cfg.healthReadTimeoutSecs) * time.Second,
		WriteTimeout: time.Duration(cfg.healthWriteTimeoutSecs) * time.Second,
		IdleTimeout:  time.Duration(cfg.healthIdleTimeoutSecs) * time.Second,
	}

	errCh := make(chan error, 2)

	go func() {
		log.Info().Str("addr", cfg.grpcAddr).Msg("gRPC server listening")
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- fmt.Errorf("grpc serve: %w", err)
		}
	}()

	go func() {
		log.Info().Str("addr", cfg.healthAddr).Msg("health HTTP server listening")
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- fmt.Errorf("health serve: %w", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-quit:
		log.Info().Str("signal", sig.String()).Msg("shutdown signal received")
	case err = <-errCh:
		return err
	}

	// Signal unreadiness so the load balancer stops routing new traffic, then
	// wait for the drain period before closing connections.
	ready.Store(false)
	time.Sleep(time.Duration(cfg.shutdownDrainSecs) * time.Second)

	// Wait for in-flight sagas to complete before tearing down the gRPC server.
	// If they don't finish within the saga shutdown timeout, log the interrupted
	// IDs — the scheduler will resume them on the next startup.
	sagaShutdownTimeout := time.Duration(cfg.shutdownSagaTimeoutSecs) * time.Second
	drainCtx, drainCancel := context.WithTimeout(context.Background(), sagaShutdownTimeout)
	interrupted := eng.Drain(drainCtx)
	drainCancel()
	if len(interrupted) > 0 {
		log.Warn().
			Int("count", len(interrupted)).
			Strs("saga_ids", interrupted).
			Msg("shutdown timeout reached; interrupted sagas will be resumed on next startup")
	}

	// Attempt a graceful stop, but enforce a hard deadline. GracefulStop blocks
	// until every open connection is closed — including idle keep-alive
	// connections held by well-behaved but slow clients and any connection held
	// open by a network partition. Without a fallback, a single misbehaving
	// client can prevent the process from ever exiting, blocking rolling deploys
	// and pod recycling.
	grpcStopWithTimeout(grpcServer, time.Duration(cfg.grpcStopTimeoutSecs)*time.Second, log)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = healthSrv.Shutdown(ctx); err != nil {
		return fmt.Errorf("health shutdown: %w", err)
	}

	return nil
}
