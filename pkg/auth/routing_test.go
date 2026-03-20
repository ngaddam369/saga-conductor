package auth_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ngaddam369/saga-conductor/internal/engine"
	"github.com/ngaddam369/saga-conductor/pkg/auth"
)

// recordingTokenSource records calls to Token and returns a preset token.
type recordingTokenSource struct {
	token    string
	lastAuth engine.StepAuthContext
	called   int
}

func (r *recordingTokenSource) Token(_ context.Context, _ string, a engine.StepAuthContext) (string, error) {
	r.called++
	r.lastAuth = a
	return r.token, nil
}

// erroringTokenSource always returns an error.
type erroringTokenSource struct{ err error }

func (e *erroringTokenSource) Token(_ context.Context, _ string, _ engine.StepAuthContext) (string, error) {
	return "", e.err
}

func TestRoutingTokenSource(t *testing.T) {
	t.Parallel()

	t.Run("empty AuthType routes to default", func(t *testing.T) {
		t.Parallel()
		dflt := &recordingTokenSource{token: "default-tok"}
		other := &recordingTokenSource{token: "other-tok"}
		r := auth.NewRoutingTokenSource(dflt, map[string]engine.TokenSource{"static": other})

		tok, err := r.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{})
		if err != nil {
			t.Fatalf("Token: unexpected error: %v", err)
		}
		if tok != "default-tok" {
			t.Errorf("Token: got %q, want %q", tok, "default-tok")
		}
		if dflt.called != 1 {
			t.Errorf("default called %d times; want 1", dflt.called)
		}
		if other.called != 0 {
			t.Errorf("other source must not be called; got %d calls", other.called)
		}
	})

	t.Run("known AuthType routes to registered source", func(t *testing.T) {
		t.Parallel()
		dflt := &recordingTokenSource{token: "default-tok"}
		staticSrc := &recordingTokenSource{token: "static-tok"}
		r := auth.NewRoutingTokenSource(dflt, map[string]engine.TokenSource{"static": staticSrc})

		tok, err := r.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{AuthType: "static"})
		if err != nil {
			t.Fatalf("Token: unexpected error: %v", err)
		}
		if tok != "static-tok" {
			t.Errorf("Token: got %q, want %q", tok, "static-tok")
		}
		if staticSrc.called != 1 {
			t.Errorf("static source called %d times; want 1", staticSrc.called)
		}
		if dflt.called != 0 {
			t.Errorf("default source must not be called; got %d calls", dflt.called)
		}
	})

	t.Run("unknown AuthType returns error", func(t *testing.T) {
		t.Parallel()
		dflt := &recordingTokenSource{token: "default-tok"}
		r := auth.NewRoutingTokenSource(dflt, map[string]engine.TokenSource{})

		_, err := r.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{AuthType: "unknown-type"})
		if err == nil {
			t.Fatal("Token: expected error for unknown auth_type, got nil")
		}
		if dflt.called != 0 {
			t.Errorf("default source must not be called on unknown type; got %d calls", dflt.called)
		}
	})

	t.Run("StepAuthContext is forwarded to selected source", func(t *testing.T) {
		t.Parallel()
		oidcSrc := &recordingTokenSource{token: "oidc-tok"}
		r := auth.NewRoutingTokenSource(
			&recordingTokenSource{},
			map[string]engine.TokenSource{"oidc": oidcSrc},
		)
		authCtx := engine.StepAuthContext{
			AuthType:   "oidc",
			SpiffeID:   "spiffe://cluster/ns/svc",
			AuthConfig: map[string]string{"scope": "read"},
		}

		_, err := r.Token(context.Background(), "http://svc/fwd", authCtx)
		if err != nil {
			t.Fatalf("Token: unexpected error: %v", err)
		}
		if oidcSrc.lastAuth.AuthType != "oidc" {
			t.Errorf("AuthType forwarded as %q; want %q", oidcSrc.lastAuth.AuthType, "oidc")
		}
		if oidcSrc.lastAuth.SpiffeID != "spiffe://cluster/ns/svc" {
			t.Errorf("SpiffeID forwarded as %q; want %q", oidcSrc.lastAuth.SpiffeID, "spiffe://cluster/ns/svc")
		}
		if oidcSrc.lastAuth.AuthConfig["scope"] != "read" {
			t.Errorf("AuthConfig[scope] forwarded as %q; want %q", oidcSrc.lastAuth.AuthConfig["scope"], "read")
		}
	})

	t.Run("default source error is propagated", func(t *testing.T) {
		t.Parallel()
		dflt := &erroringTokenSource{err: errors.New("default error")}
		r := auth.NewRoutingTokenSource(dflt, map[string]engine.TokenSource{})

		_, err := r.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{})
		if err == nil {
			t.Fatal("Token: expected error from default source, got nil")
		}
	})

	t.Run("registered source error is propagated", func(t *testing.T) {
		t.Parallel()
		oidcSrc := &erroringTokenSource{err: errors.New("oidc error")}
		r := auth.NewRoutingTokenSource(
			&recordingTokenSource{},
			map[string]engine.TokenSource{"oidc": oidcSrc},
		)

		_, err := r.Token(context.Background(), "http://svc/fwd", engine.StepAuthContext{AuthType: "oidc"})
		if err == nil {
			t.Fatal("Token: expected error from registered source, got nil")
		}
	})
}
