package engine

import "context"

// StepAuthContext carries per-step authentication configuration forwarded from
// the step definition to the TokenSource on every outbound HTTP call.
type StepAuthContext struct {
	// SpiffeID is the SPIFFE ID of the target service (from target_spiffe_id).
	SpiffeID string
	// AuthType is the per-step auth override (from auth_type). Empty means use
	// the global default.
	AuthType string
	// AuthConfig holds additional auth parameters for the selected AuthType.
	AuthConfig map[string]string
}

// TokenSource provides bearer tokens for outbound step HTTP calls.
// The engine calls Token before every callHTTP invocation. If the returned
// string is empty, no Authorization header is added. Implementations must be
// safe for concurrent use. A nil TokenSource is a no-op.
type TokenSource interface {
	Token(ctx context.Context, targetURL string, auth StepAuthContext) (string, error)
}
