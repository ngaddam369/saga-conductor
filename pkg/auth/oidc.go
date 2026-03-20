package auth

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// expiryBuffer is the time before token expiry at which OIDCTokenSource
// proactively fetches a fresh token. This prevents injecting an access token
// that expires mid-flight during a step HTTP call.
const expiryBuffer = 30 * time.Second

// OIDCTokenSource is a TokenSource that obtains bearer tokens via the OAuth2
// client credentials flow (machine-to-machine). Tokens are cached and reused
// until they are within expiryBuffer of their expiry time.
//
// Configured via AUTH_OIDC_TOKEN_URL, AUTH_OIDC_CLIENT_ID,
// AUTH_OIDC_CLIENT_SECRET, and optionally AUTH_OIDC_SCOPES (space-separated).
type OIDCTokenSource struct {
	cfg          *clientcredentials.Config
	expiryBuffer time.Duration

	mu     sync.Mutex
	cached *oauth2.Token
}

// OIDCOption configures an OIDCTokenSource.
type OIDCOption func(*OIDCTokenSource)

// WithOIDCExpiryBuffer overrides the default 30-second pre-expiry refresh
// buffer. Primarily useful in tests where token lifetimes are short.
func WithOIDCExpiryBuffer(d time.Duration) OIDCOption {
	return func(s *OIDCTokenSource) { s.expiryBuffer = d }
}

// NewOIDCTokenSource returns an OIDCTokenSource that fetches tokens from
// tokenURL using the given client credentials.
func NewOIDCTokenSource(tokenURL, clientID, clientSecret string, scopes []string, opts ...OIDCOption) *OIDCTokenSource {
	s := &OIDCTokenSource{
		cfg: &clientcredentials.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			TokenURL:     tokenURL,
			Scopes:       scopes,
		},
		expiryBuffer: expiryBuffer,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Token implements engine.TokenSource. It returns the cached access token if
// it is still valid with at least expiryBuffer remaining; otherwise it fetches
// a fresh token from the configured token endpoint using the provided context.
func (s *OIDCTokenSource) Token(ctx context.Context, _ string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isTokenFresh() {
		return s.cached.AccessToken, nil
	}

	tok, err := s.cfg.Token(ctx)
	if err != nil {
		return "", fmt.Errorf("obtain OIDC token: %w", err)
	}
	s.cached = tok
	return tok.AccessToken, nil
}

// isTokenFresh reports whether the cached token can be used without fetching.
// Must be called with s.mu held.
func (s *OIDCTokenSource) isTokenFresh() bool {
	if s.cached == nil || s.cached.AccessToken == "" {
		return false
	}
	// Tokens with no expiry field are treated as perpetually valid (some
	// providers issue non-expiring tokens).
	if s.cached.Expiry.IsZero() {
		return true
	}
	return time.Until(s.cached.Expiry) > s.expiryBuffer
}
