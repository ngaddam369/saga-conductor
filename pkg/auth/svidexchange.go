package auth

import (
	"context"
	"errors"
	"sync"

	"github.com/ngaddam369/svid-exchange/pkg/client"
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
// real svid-exchange gRPC client. Intended for tests.
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

	mu      sync.Mutex
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
func (s *SVIDExchangeTokenSource) Token(ctx context.Context, _ string, spiffeID string) (string, error) {
	if spiffeID == "" || s.addr == "" {
		return "", nil
	}

	c, err := s.getOrCreate(ctx, spiffeID)
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

// getOrCreate returns the cached client for spiffeID or creates a new one.
func (s *SVIDExchangeTokenSource) getOrCreate(ctx context.Context, spiffeID string) (SVIDTokenClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.clients[spiffeID]; ok {
		return c, nil
	}
	c, err := s.newClient(ctx, s.addr, s.socketPath, spiffeID)
	if err != nil {
		return nil, err
	}
	s.clients[spiffeID] = c
	return c, nil
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
