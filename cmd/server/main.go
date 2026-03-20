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
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/spiffe/go-spiffe/v2/spiffetls/tlsconfig"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/ngaddam369/saga-conductor/internal/engine"
	"github.com/ngaddam369/saga-conductor/internal/purger"
	"github.com/ngaddam369/saga-conductor/internal/scheduler"
	"github.com/ngaddam369/saga-conductor/internal/server"
	"github.com/ngaddam369/saga-conductor/internal/store"
	"github.com/ngaddam369/saga-conductor/pkg/auth"
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
	ctx := context.Background()
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

	socketPath := os.Getenv("SPIFFE_ENDPOINT_SOCKET")
	grpcCreds, closeGRPCCreds, err := buildGRPCCredentials(ctx, socketPath)
	if err != nil {
		return fmt.Errorf("grpc credentials: %w", err)
	}
	defer closeGRPCCreds()

	shutdownTracing, err := buildTracerProvider(ctx, os.Getenv("OTLP_ENDPOINT"))
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownCtx); err != nil {
			log.Error().Err(err).Msg("flush traces failed")
		}
	}()

	tokenSrc, validator, err := buildAuthProviders(getEnv("AUTH_TYPE", "none"))
	if err != nil {
		return err
	}

	routingSrc := buildRoutingSource(tokenSrc)
	eng := engine.New(s, engine.WithLogger(log), engine.WithRecorder(rec), engine.WithTokenSource(routingSrc))

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
	grpcOpts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			server.AuthInterceptor(validator),
			server.TimeoutInterceptor(handlerTimeout),
		),
		grpc.MaxRecvMsgSize(cfg.grpcMaxRecvMB * 1024 * 1024),
		grpc.MaxSendMsgSize(cfg.grpcMaxSendMB * 1024 * 1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: time.Duration(cfg.grpcMaxConnIdleMinutes) * time.Minute,
			Time:              time.Duration(cfg.grpcKeepaliveTimeMinutes) * time.Minute,
			Timeout:           time.Duration(cfg.grpcKeepaliveTimeoutSecs) * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             time.Duration(cfg.grpcKeepaliveMinTimeSecs) * time.Second,
			PermitWithoutStream: true,
		}),
	}
	if grpcCreds != nil {
		grpcOpts = append([]grpc.ServerOption{grpc.Creds(grpcCreds)}, grpcOpts...)
	}
	grpcServer := grpc.NewServer(grpcOpts...)
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

// buildGRPCCredentials returns mTLS transport credentials when socketPath is
// non-empty, using the SPIFFE workload API at that address to obtain the
// server's X.509 SVID and trusted CA bundles. Returns nil credentials (plain
// TCP) when socketPath is empty. The caller must invoke the cleanup function
// to close the workload API connection.
func buildGRPCCredentials(ctx context.Context, socketPath string) (credentials.TransportCredentials, func(), error) {
	if socketPath == "" {
		return nil, func() {}, nil
	}
	source, err := workloadapi.NewX509Source(ctx,
		workloadapi.WithClientOptions(workloadapi.WithAddr(socketPath)),
	)
	if err != nil {
		return nil, func() {}, fmt.Errorf("create X.509 source: %w", err)
	}
	tlsCfg := tlsconfig.MTLSServerConfig(source, source, tlsconfig.AuthorizeAny())
	return credentials.NewTLS(tlsCfg), func() {
		if err := source.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close X.509 source: %v\n", err)
		}
	}, nil
}

// buildAuthProviders selects the TokenSource and TokenValidator implementations
// based on authType. It is a pure function — no I/O — so it is easy to test.
func buildAuthProviders(authType string) (engine.TokenSource, server.TokenValidator, error) {
	switch authType {
	case "none":
		return auth.NoopTokenSource{}, auth.NoopTokenValidator{}, nil
	case "static":
		token := os.Getenv("AUTH_STATIC_TOKEN")
		if token == "" {
			return nil, nil, fmt.Errorf("AUTH_STATIC_TOKEN must be set when AUTH_TYPE=static")
		}
		return auth.NewStaticTokenSource(token), auth.NewStaticTokenValidator(token), nil
	case "jwt":
		jwksURL := os.Getenv("AUTH_JWKS_URL")
		if jwksURL == "" {
			return nil, nil, fmt.Errorf("AUTH_JWKS_URL must be set when AUTH_TYPE=jwt")
		}
		return auth.NoopTokenSource{}, auth.NewJWTValidator(jwksURL, 0), nil
	case "oidc":
		tokenURL := os.Getenv("AUTH_OIDC_TOKEN_URL")
		clientID := os.Getenv("AUTH_OIDC_CLIENT_ID")
		clientSecret := os.Getenv("AUTH_OIDC_CLIENT_SECRET")
		if tokenURL == "" || clientID == "" || clientSecret == "" {
			return nil, nil, fmt.Errorf("AUTH_OIDC_TOKEN_URL, AUTH_OIDC_CLIENT_ID, and AUTH_OIDC_CLIENT_SECRET must all be set when AUTH_TYPE=oidc")
		}
		var scopes []string
		if s := os.Getenv("AUTH_OIDC_SCOPES"); s != "" {
			scopes = strings.Fields(s)
		}
		return auth.NewOIDCTokenSource(tokenURL, clientID, clientSecret, scopes), auth.NoopTokenValidator{}, nil
	case "svid-exchange":
		addr := os.Getenv("AUTH_SVID_EXCHANGE_ADDR")
		if addr == "" {
			return nil, nil, fmt.Errorf("AUTH_SVID_EXCHANGE_ADDR must be set when AUTH_TYPE=svid-exchange")
		}
		// Socket path is optional: falls back to SPIFFE_ENDPOINT_SOCKET env var
		// in the svid-exchange client when empty.
		socketPath := os.Getenv("SPIFFE_ENDPOINT_SOCKET")
		return auth.NewSVIDExchangeTokenSource(addr, socketPath), auth.NoopTokenValidator{}, nil
	default:
		return nil, nil, fmt.Errorf("unknown AUTH_TYPE %q", authType)
	}
}

// buildRoutingSource constructs a RoutingTokenSource that wraps globalSrc as
// the default and registers a per-type source for every auth type whose
// required environment variables are set. Steps that set auth_type use the
// matching registered source; steps with no auth_type use the global default.
func buildRoutingSource(globalSrc engine.TokenSource) *auth.RoutingTokenSource {
	sources := map[string]engine.TokenSource{
		"none": auth.NoopTokenSource{},
	}

	if token := os.Getenv("AUTH_STATIC_TOKEN"); token != "" {
		sources["static"] = auth.NewStaticTokenSource(token)
	}

	if jwksURL := os.Getenv("AUTH_JWKS_URL"); jwksURL != "" {
		// jwt type only validates inbound tokens; outbound calls use Noop.
		sources["jwt"] = auth.NoopTokenSource{}
	}

	if tokenURL := os.Getenv("AUTH_OIDC_TOKEN_URL"); tokenURL != "" {
		clientID := os.Getenv("AUTH_OIDC_CLIENT_ID")
		clientSecret := os.Getenv("AUTH_OIDC_CLIENT_SECRET")
		if clientID != "" && clientSecret != "" {
			var scopes []string
			if s := os.Getenv("AUTH_OIDC_SCOPES"); s != "" {
				scopes = strings.Fields(s)
			}
			sources["oidc"] = auth.NewOIDCTokenSource(tokenURL, clientID, clientSecret, scopes)
		}
	}

	if addr := os.Getenv("AUTH_SVID_EXCHANGE_ADDR"); addr != "" {
		socketPath := os.Getenv("SPIFFE_ENDPOINT_SOCKET")
		sources["svid-exchange"] = auth.NewSVIDExchangeTokenSource(addr, socketPath)
	}

	return auth.NewRoutingTokenSource(globalSrc, sources)
}
