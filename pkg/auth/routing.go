package auth

import (
	"context"
	"fmt"

	"github.com/ngaddam369/saga-conductor/internal/engine"
)

// RoutingTokenSource dispatches Token calls to a named TokenSource selected by
// the step's AuthType. When AuthType is empty the default source is used. An
// explicitly set but unregistered AuthType is returned as an error so
// misconfiguration fails fast rather than silently falling back.
type RoutingTokenSource struct {
	dflt    engine.TokenSource
	sources map[string]engine.TokenSource
}

// NewRoutingTokenSource returns a RoutingTokenSource that routes by AuthType.
// dflt is invoked when a step's AuthType is empty. sources maps AuthType values
// to their TokenSource implementations.
func NewRoutingTokenSource(dflt engine.TokenSource, sources map[string]engine.TokenSource) *RoutingTokenSource {
	return &RoutingTokenSource{dflt: dflt, sources: sources}
}

// Token implements engine.TokenSource. It selects the TokenSource registered
// for auth.AuthType and delegates to it. An empty AuthType uses the default
// source. An unregistered AuthType returns an error.
func (r *RoutingTokenSource) Token(ctx context.Context, targetURL string, auth engine.StepAuthContext) (string, error) {
	if auth.AuthType == "" {
		return r.dflt.Token(ctx, targetURL, auth)
	}
	src, ok := r.sources[auth.AuthType]
	if !ok {
		return "", fmt.Errorf("no TokenSource registered for auth_type %q", auth.AuthType)
	}
	return src.Token(ctx, targetURL, auth)
}
