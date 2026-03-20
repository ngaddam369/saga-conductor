package auth

import (
	"context"
	"crypto/subtle"
	"errors"

	"github.com/ngaddam369/saga-conductor/internal/engine"
)

// StaticTokenSource is a TokenSource that returns the same pre-configured
// bearer token for every outbound step call, regardless of target URL.
// Suitable for internal services protected by a shared API key.
type StaticTokenSource struct{ token string }

// NewStaticTokenSource returns a StaticTokenSource that injects token on every
// outbound step HTTP call.
func NewStaticTokenSource(token string) StaticTokenSource {
	return StaticTokenSource{token: token}
}

func (s StaticTokenSource) Token(_ context.Context, _ string, _ engine.StepAuthContext) (string, error) {
	return s.token, nil
}

// StaticTokenValidator is a TokenValidator that accepts inbound gRPC calls only
// when the bearer token matches the configured expected value. Comparison is
// constant-time to prevent timing-based token enumeration.
type StaticTokenValidator struct{ expected string }

// NewStaticTokenValidator returns a StaticTokenValidator that requires token to
// equal expected on every inbound gRPC call.
func NewStaticTokenValidator(expected string) StaticTokenValidator {
	return StaticTokenValidator{expected: expected}
}

func (v StaticTokenValidator) Validate(_ context.Context, token string) error {
	// Reject immediately when expected is empty — misconfigured validator must
	// not accidentally grant access to callers that omit the Authorization header.
	if v.expected == "" {
		return errors.New("invalid token")
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(v.expected)) != 1 {
		return errors.New("invalid token")
	}
	return nil
}
