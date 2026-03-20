package auth

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/ngaddam369/svid-exchange/pkg/client"

	"github.com/ngaddam369/saga-conductor/internal/engine"
)

// SVIDTokenClient abstracts the svid-exchange per-service client.
// The production implementation is *client.Client; tests inject a mock.
// Each instance is bound to a single target SPIFFE ID (TargetService).
type SVIDTokenClient interface {
	Token(ctx context.Context) (string, error)
	Close() error
}

// SVIDTokenClientFactory creates an SVIDTokenClient for the given server
// address, SPIFFE workload API socket path, and target SPIFFE ID.
type SVIDTokenClientFactory func(ctx context.Context, addr, socketPath, spiffeID string) (SVIDTokenClient, error)

// SVIDExchangeOption configures a SVIDExchangeTokenSource.
type SVIDExchangeOption func(*SVIDExchangeTokenSource)

// WithNewTokenClient injects a custom SVIDTokenClientFactory, bypassing the
// real svid-exchange gRPC client. Intended for tests; required to be exported
// because test packages in other directories (e.g. cmd/server) also inject
// mocks via this option.
func WithNewTokenClient(fn SVIDTokenClientFactory) SVIDExchangeOption {
	return func(s *SVIDExchangeTokenSource) { s.newClient = fn }
}

// SVIDExchangeTokenSource is a TokenSource that exchanges saga-conductor's
// SPIFFE identity for a scoped ES256 JWT via the svid-exchange service.
// One SVIDTokenClient is pooled per target SPIFFE ID (step's TargetSPIFFEID).
// Steps with an empty TargetSPIFFEID get no Authorization header injected.
//
// Falls back to returning an empty token (no Authorization header) when
// addr is empty or client creation fails, so local development without a
// svid-exchange sidecar is unaffected.
//
// Configured via AUTH_SVID_EXCHANGE_ADDR (gRPC server address) and optionally
// SPIFFE_ENDPOINT_SOCKET (SPIFFE workload API socket path).
type SVIDExchangeTokenSource struct {
	addr       string
	socketPath string
	newClient  SVIDTokenClientFactory

	mu      sync.RWMutex
	clients map[string]SVIDTokenClient
}

// NewSVIDExchangeTokenSource returns an SVIDExchangeTokenSource that connects
// to the svid-exchange service at addr. socketPath is the SPIFFE workload API
// socket (empty falls back to the SPIFFE_ENDPOINT_SOCKET env var). Pass opts
// to inject a mock client factory for tests.
func NewSVIDExchangeTokenSource(addr, socketPath string, opts ...SVIDExchangeOption) *SVIDExchangeTokenSource {
	s := &SVIDExchangeTokenSource{
		addr:       addr,
		socketPath: socketPath,
		newClient:  defaultNewTokenClient,
		clients:    make(map[string]SVIDTokenClient),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Token implements engine.TokenSource. It returns a scoped JWT for the step's
// target SPIFFE ID, fetching a fresh one from the svid-exchange service the
// first time a given SPIFFE ID is seen (subsequent calls use the pooled client
// which caches its own token internally).
//
// Returns an empty string (no Authorization header) when spiffeID is empty
// (step does not use SPIFFE auth) or addr is empty (graceful no-op for local
// dev). Also returns an empty string — rather than an error — when client
// creation fails (socket unreachable, server unavailable), so a missing SPIRE
// agent does not abort in-flight sagas.
func (s *SVIDExchangeTokenSource) Token(ctx context.Context, _ string, auth engine.StepAuthContext) (string, error) {
	if auth.SpiffeID == "" || s.addr == "" {
		return "", nil
	}

	c, err := s.getOrCreate(ctx, auth.SpiffeID)
	if err != nil {
		// Client creation failure is treated as unavailable: return empty
		// token rather than aborting the saga.
		return "", nil
	}

	tok, err := c.Token(ctx)
	if err != nil {
		return "", err
	}
	return tok, nil
}

// Close releases all pooled svid-exchange clients.
func (s *SVIDExchangeTokenSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var errs []error
	for _, c := range s.clients {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// getOrCreate returns the cached client for spiffeID, creating one if needed.
// It uses a read lock for the common (cached) path so concurrent calls for
// different SPIFFE IDs do not serialise behind a single gRPC dial.
func (s *SVIDExchangeTokenSource) getOrCreate(ctx context.Context, spiffeID string) (SVIDTokenClient, error) {
	// Fast path: client already cached.
	s.mu.RLock()
	c, ok := s.clients[spiffeID]
	s.mu.RUnlock()
	if ok {
		return c, nil
	}

	// Slow path: create the client outside any lock.
	newC, err := s.newClient(ctx, s.addr, s.socketPath, spiffeID)
	if err != nil {
		return nil, err
	}

	// Re-check under the write lock in case another goroutine raced us.
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.clients[spiffeID]; ok {
		if err := newC.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "svid-exchange: close duplicate client for %q: %v\n", spiffeID, err)
		}
		return existing, nil
	}
	s.clients[spiffeID] = newC
	return newC, nil
}

// defaultNewTokenClient is the production factory: wraps client.New from the
// svid-exchange library.
func defaultNewTokenClient(ctx context.Context, addr, socketPath, spiffeID string) (SVIDTokenClient, error) {
	return client.New(ctx, client.Options{
		Addr:          addr,
		SpiffeSocket:  socketPath,
		TargetService: spiffeID,
	})
}
