// Package auth provides pluggable authentication implementations for
// saga-conductor's TokenSource and TokenValidator interfaces.
//
// Concrete implementations are wired in cmd/server/main.go based on the
// AUTH_TYPE environment variable. Adding a new provider only requires a new
// case in that switch — no changes to core packages are needed.
package auth

import "context"

// NoopTokenSource is a TokenSource that always returns an empty token.
// No Authorization header is added to outbound step HTTP calls.
// This is the default when AUTH_TYPE is unset or "none".
type NoopTokenSource struct{}

func (NoopTokenSource) Token(_ context.Context, _ string, _ string) (string, error) {
	return "", nil
}

// NoopTokenValidator is a TokenValidator that accepts every inbound call
// unconditionally. The gRPC server remains open — any caller is allowed.
// This is the default when AUTH_TYPE is unset or "none".
type NoopTokenValidator struct{}

func (NoopTokenValidator) Validate(_ context.Context, _ string) error {
	return nil
}
