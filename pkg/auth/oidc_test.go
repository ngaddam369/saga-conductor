package auth_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaddam369/saga-conductor/pkg/auth"
)

// tokenServer starts an httptest.Server that serves OAuth2 token responses.
// Each call to the handler increments calls. accessToken is returned in the
// access_token field; expiresIn is the expires_in value in seconds (0 = omit).
func tokenServer(t *testing.T, calls *atomic.Int32, accessToken string, expiresIn int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		resp := map[string]any{
			"access_token": accessToken,
			"token_type":   "Bearer",
		}
		if expiresIn > 0 {
			resp["expires_in"] = expiresIn
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			t.Errorf("encode token response: %v", err)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestOIDCTokenSource(t *testing.T) {
	t.Parallel()

	t.Run("returns access token from token endpoint", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		srv := tokenServer(t, &calls, "my-access-token", 3600)
		src := auth.NewOIDCTokenSource(srv.URL, "client-id", "client-secret", nil)

		tok, err := src.Token(context.Background(), "http://step-service/forward")
		if err != nil {
			t.Fatalf("Token: unexpected error: %v", err)
		}
		if tok != "my-access-token" {
			t.Errorf("Token: got %q, want %q", tok, "my-access-token")
		}
		if calls.Load() != 1 {
			t.Errorf("token endpoint called %d times; want 1", calls.Load())
		}
	})

	t.Run("cached token is reused — endpoint called only once", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		srv := tokenServer(t, &calls, "cached-token", 3600)
		src := auth.NewOIDCTokenSource(srv.URL, "client-id", "client-secret", nil)

		for i := range 5 {
			tok, err := src.Token(context.Background(), "http://step/fwd")
			if err != nil {
				t.Fatalf("call %d: Token: %v", i+1, err)
			}
			if tok != "cached-token" {
				t.Errorf("call %d: got %q, want %q", i+1, tok, "cached-token")
			}
		}
		if calls.Load() != 1 {
			t.Errorf("token endpoint called %d times; want 1 (subsequent calls must hit cache)", calls.Load())
		}
	})

	t.Run("expired token triggers re-fetch", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		// expires_in=1 so the token expires after 1 second.
		// WithOIDCExpiryBuffer(0) disables the proactive buffer so the token
		// is considered fresh until it actually expires.
		srv := tokenServer(t, &calls, "short-lived-token", 1)
		src := auth.NewOIDCTokenSource(srv.URL, "client-id", "client-secret", nil,
			auth.WithOIDCExpiryBuffer(0))

		if _, err := src.Token(context.Background(), "http://step/fwd"); err != nil {
			t.Fatalf("first Token: %v", err)
		}
		if calls.Load() != 1 {
			t.Errorf("after first call: endpoint called %d times; want 1", calls.Load())
		}

		// Wait for the 1-second token to expire.
		time.Sleep(2 * time.Second)

		if _, err := src.Token(context.Background(), "http://step/fwd"); err != nil {
			t.Fatalf("second Token: %v", err)
		}
		if calls.Load() != 2 {
			t.Errorf("after expiry+second call: endpoint called %d times; want 2", calls.Load())
		}
	})

	t.Run("target URL is ignored — same token for any step", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		srv := tokenServer(t, &calls, "global-token", 3600)
		src := auth.NewOIDCTokenSource(srv.URL, "client-id", "client-secret", nil)

		for _, url := range []string{
			"http://service-a/forward",
			"http://service-b/compensate",
		} {
			tok, err := src.Token(context.Background(), url)
			if err != nil {
				t.Fatalf("Token(%q): %v", url, err)
			}
			if tok != "global-token" {
				t.Errorf("Token(%q): got %q, want %q", url, tok, "global-token")
			}
		}
		// Both calls must return from cache after the first fetch.
		if calls.Load() != 1 {
			t.Errorf("endpoint called %d times; want 1", calls.Load())
		}
	})

	t.Run("token endpoint error is propagated", func(t *testing.T) {
		t.Parallel()
		errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "server error", http.StatusInternalServerError)
		}))
		t.Cleanup(errSrv.Close)
		src := auth.NewOIDCTokenSource(errSrv.URL, "client-id", "client-secret", nil)

		if _, err := src.Token(context.Background(), "http://step/fwd"); err == nil {
			t.Error("Token: expected error when endpoint returns 500, got nil")
		}
	})

	t.Run("token with no expiry is treated as perpetually fresh", func(t *testing.T) {
		t.Parallel()
		var calls atomic.Int32
		srv := tokenServer(t, &calls, "no-expiry-token", 0 /* no expires_in */)
		src := auth.NewOIDCTokenSource(srv.URL, "client-id", "client-secret", nil)

		for i := range 3 {
			tok, err := src.Token(context.Background(), "http://step/fwd")
			if err != nil {
				t.Fatalf("call %d: Token: %v", i+1, err)
			}
			if tok != "no-expiry-token" {
				t.Errorf("call %d: got %q, want %q", i+1, tok, "no-expiry-token")
			}
		}
		if calls.Load() != 1 {
			t.Errorf("endpoint called %d times; want 1", calls.Load())
		}
	})
}
