package auth_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/ngaddam369/saga-conductor/internal/engine"
	"github.com/ngaddam369/saga-conductor/pkg/auth"
)

// mockTokenClient is a test double for auth.SVIDTokenClient.
type mockTokenClient struct {
	calls atomic.Int32
	token string
	err   error
}

func (m *mockTokenClient) Token(_ context.Context) (string, error) {
	m.calls.Add(1)
	if m.err != nil {
		return "", m.err
	}
	return m.token, nil
}

func (m *mockTokenClient) Close() error { return nil }

// closingTokenClient records that Close was called.
type closingTokenClient struct {
	mockTokenClient
	closed atomic.Bool
}

func (c *closingTokenClient) Close() error {
	c.closed.Store(true)
	return nil
}

// newMockFactory returns an SVIDExchangeOption that injects a factory always
// returning the same client.
func newMockFactory(c auth.SVIDTokenClient) auth.SVIDExchangeOption {
	return auth.WithNewTokenClient(func(_ context.Context, _, _, _ string) (auth.SVIDTokenClient, error) {
		return c, nil
	})
}

func TestSVIDExchangeTokenSource(t *testing.T) {
	t.Parallel()

	t.Run("empty spiffeID returns empty token without calling factory", func(t *testing.T) {
		t.Parallel()
		var factoryCalled atomic.Bool
		src := auth.NewSVIDExchangeTokenSource("svid.internal:443", "",
			auth.WithNewTokenClient(func(_ context.Context, _, _, _ string) (auth.SVIDTokenClient, error) {
				factoryCalled.Store(true)
				return &mockTokenClient{token: "should-not-be-returned"}, nil
			}),
		)

		tok, err := src.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{})
		if err != nil {
			t.Fatalf("Token: unexpected error: %v", err)
		}
		if tok != "" {
			t.Errorf("Token: got %q; want empty string for empty spiffeID", tok)
		}
		if factoryCalled.Load() {
			t.Error("factory must not be called when spiffeID is empty")
		}
	})

	t.Run("empty addr returns empty token without calling factory", func(t *testing.T) {
		t.Parallel()
		var factoryCalled atomic.Bool
		src := auth.NewSVIDExchangeTokenSource("" /* empty addr */, "",
			auth.WithNewTokenClient(func(_ context.Context, _, _, _ string) (auth.SVIDTokenClient, error) {
				factoryCalled.Store(true)
				return &mockTokenClient{token: "should-not-be-returned"}, nil
			}),
		)

		tok, err := src.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{SpiffeID: "spiffe://cluster/ns/svc-a"})
		if err != nil {
			t.Fatalf("Token: unexpected error: %v", err)
		}
		if tok != "" {
			t.Errorf("Token: got %q; want empty string when addr is empty", tok)
		}
		if factoryCalled.Load() {
			t.Error("factory must not be called when addr is empty")
		}
	})

	t.Run("returns token from client", func(t *testing.T) {
		t.Parallel()
		mock := &mockTokenClient{token: "svid-jwt"}
		src := auth.NewSVIDExchangeTokenSource("svid.internal:443", "", newMockFactory(mock))

		tok, err := src.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{SpiffeID: "spiffe://cluster/ns/svc-a"})
		if err != nil {
			t.Fatalf("Token: unexpected error: %v", err)
		}
		if tok != "svid-jwt" {
			t.Errorf("Token: got %q, want %q", tok, "svid-jwt")
		}
	})

	t.Run("client is created once per spiffeID — factory called once", func(t *testing.T) {
		t.Parallel()
		mock := &mockTokenClient{token: "cached-jwt"}
		var factoryCalls atomic.Int32
		src := auth.NewSVIDExchangeTokenSource("svid.internal:443", "",
			auth.WithNewTokenClient(func(_ context.Context, _, _, _ string) (auth.SVIDTokenClient, error) {
				factoryCalls.Add(1)
				return mock, nil
			}),
		)

		const spiffeID = "spiffe://cluster/ns/svc-a"
		for i := range 5 {
			tok, err := src.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{SpiffeID: spiffeID})
			if err != nil {
				t.Fatalf("call %d: Token: %v", i+1, err)
			}
			if tok != "cached-jwt" {
				t.Errorf("call %d: got %q, want %q", i+1, tok, "cached-jwt")
			}
		}
		if factoryCalls.Load() != 1 {
			t.Errorf("factory called %d times; want 1", factoryCalls.Load())
		}
		if mock.calls.Load() != 5 {
			t.Errorf("client.Token called %d times; want 5", mock.calls.Load())
		}
	})

	t.Run("different spiffeIDs get separate clients", func(t *testing.T) {
		t.Parallel()
		var factoryCalls atomic.Int32
		src := auth.NewSVIDExchangeTokenSource("svid.internal:443", "",
			auth.WithNewTokenClient(func(_ context.Context, _, _, spiffeID string) (auth.SVIDTokenClient, error) {
				factoryCalls.Add(1)
				return &mockTokenClient{token: "token-for-" + spiffeID}, nil
			}),
		)

		tok1, err := src.Token(context.Background(), "http://svc-a/fwd", engine.StepAuthContext{SpiffeID: "spiffe://cluster/ns/svc-a"})
		if err != nil {
			t.Fatalf("Token svc-a: %v", err)
		}
		tok2, err := src.Token(context.Background(), "http://svc-b/fwd", engine.StepAuthContext{SpiffeID: "spiffe://cluster/ns/svc-b"})
		if err != nil {
			t.Fatalf("Token svc-b: %v", err)
		}

		if tok1 == tok2 {
			t.Errorf("expected distinct tokens for distinct SPIFFE IDs; got %q for both", tok1)
		}
		if factoryCalls.Load() != 2 {
			t.Errorf("factory called %d times; want 2 (one per distinct SPIFFE ID)", factoryCalls.Load())
		}
	})

	t.Run("client creation failure returns empty token gracefully", func(t *testing.T) {
		t.Parallel()
		src := auth.NewSVIDExchangeTokenSource("svid.internal:443", "",
			auth.WithNewTokenClient(func(_ context.Context, _, _, _ string) (auth.SVIDTokenClient, error) {
				return nil, errors.New("socket unreachable")
			}),
		)

		tok, err := src.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{SpiffeID: "spiffe://cluster/ns/svc-a"})
		if err != nil {
			t.Fatalf("Token: unexpected error (should be graceful no-op): %v", err)
		}
		if tok != "" {
			t.Errorf("Token: got %q; want empty string on client creation failure", tok)
		}
	})

	t.Run("token fetch error is propagated", func(t *testing.T) {
		t.Parallel()
		mock := &mockTokenClient{err: errors.New("token exchange failed")}
		src := auth.NewSVIDExchangeTokenSource("svid.internal:443", "", newMockFactory(mock))

		if _, err := src.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{SpiffeID: "spiffe://cluster/ns/svc-a"}); err == nil {
			t.Error("Token: expected error from token client, got nil")
		}
	})

	t.Run("Close shuts down all pooled clients", func(t *testing.T) {
		t.Parallel()
		clientA := &closingTokenClient{}
		clientA.token = "a"
		clientB := &closingTokenClient{}
		clientB.token = "b"

		idToClient := map[string]*closingTokenClient{
			"spiffe://cluster/ns/svc-a": clientA,
			"spiffe://cluster/ns/svc-b": clientB,
		}
		src := auth.NewSVIDExchangeTokenSource("svid.internal:443", "",
			auth.WithNewTokenClient(func(_ context.Context, _, _, spiffeID string) (auth.SVIDTokenClient, error) {
				c, ok := idToClient[spiffeID]
				if !ok {
					return nil, errors.New("unknown id")
				}
				return c, nil
			}),
		)

		// Warm up the pool.
		for id := range idToClient {
			if _, err := src.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{SpiffeID: id}); err != nil {
				t.Fatalf("Token(%q): %v", id, err)
			}
		}

		if err := src.Close(); err != nil {
			t.Fatalf("Close: unexpected error: %v", err)
		}
		if !clientA.closed.Load() {
			t.Error("clientA was not closed")
		}
		if !clientB.closed.Load() {
			t.Error("clientB was not closed")
		}
	})

	t.Run("addr and socket are forwarded to factory", func(t *testing.T) {
		t.Parallel()
		var gotAddr, gotSocket, gotSPIFFEID string
		src := auth.NewSVIDExchangeTokenSource("svid.prod:443", "unix:///run/spire.sock",
			auth.WithNewTokenClient(func(_ context.Context, addr, socket, spiffeID string) (auth.SVIDTokenClient, error) {
				gotAddr = addr
				gotSocket = socket
				gotSPIFFEID = spiffeID
				return &mockTokenClient{token: "tok"}, nil
			}),
		)

		if _, err := src.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{SpiffeID: "spiffe://cluster/ns/svc-a"}); err != nil {
			t.Fatalf("Token: %v", err)
		}
		if gotAddr != "svid.prod:443" {
			t.Errorf("addr: got %q, want %q", gotAddr, "svid.prod:443")
		}
		if gotSocket != "unix:///run/spire.sock" {
			t.Errorf("socket: got %q, want %q", gotSocket, "unix:///run/spire.sock")
		}
		if gotSPIFFEID != "spiffe://cluster/ns/svc-a" {
			t.Errorf("spiffeID: got %q, want %q", gotSPIFFEID, "spiffe://cluster/ns/svc-a")
		}
	})
}
