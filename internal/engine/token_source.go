package engine

import "context"

// TokenSource provides bearer tokens for outbound step HTTP calls.
// The engine calls Token before every callHTTP invocation. If the returned
// string is empty, no Authorization header is added. Implementations must be
// safe for concurrent use. A nil TokenSource is a no-op.
//
// spiffeID is the SPIFFE ID of the target service from the step definition;
// it is empty when the step does not use SPIFFE-based auth.
type TokenSource interface {
	Token(ctx context.Context, targetURL string, spiffeID string) (string, error)
}
