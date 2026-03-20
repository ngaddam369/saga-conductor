package engine

import "context"

// TokenSource provides bearer tokens for outbound step HTTP calls.
// The engine calls Token before every callHTTP invocation. If the returned
// string is empty, no Authorization header is added. Implementations must be
// safe for concurrent use. A nil TokenSource is a no-op.
type TokenSource interface {
	Token(ctx context.Context, targetURL string) (string, error)
}
