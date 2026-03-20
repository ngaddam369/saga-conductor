package server

import "context"

// TokenValidator validates inbound bearer tokens on gRPC calls.
// AuthInterceptor calls Validate with the raw token string extracted from the
// Authorization header. Returning a non-nil error rejects the call with
// codes.Unauthenticated. Implementations must be safe for concurrent use.
// A nil TokenValidator disables inbound auth (open server).
type TokenValidator interface {
	Validate(ctx context.Context, token string) error
}
