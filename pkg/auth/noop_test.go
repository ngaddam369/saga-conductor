package auth_test

import (
	"context"
	"testing"

	"github.com/ngaddam369/saga-conductor/pkg/auth"
)

func TestNoopTokenSource(t *testing.T) {
	t.Parallel()
	var ts auth.NoopTokenSource
	tok, err := ts.Token(context.Background(), "http://example.com/step", "")
	if err != nil {
		t.Fatalf("Token: unexpected error: %v", err)
	}
	if tok != "" {
		t.Errorf("Token: want empty string, got %q", tok)
	}
}

func TestNoopTokenValidator(t *testing.T) {
	t.Parallel()
	var v auth.NoopTokenValidator

	t.Run("accepts empty token", func(t *testing.T) {
		t.Parallel()
		if err := v.Validate(context.Background(), ""); err != nil {
			t.Errorf("Validate: unexpected error: %v", err)
		}
	})
	t.Run("accepts non-empty token", func(t *testing.T) {
		t.Parallel()
		if err := v.Validate(context.Background(), "some-token"); err != nil {
			t.Errorf("Validate: unexpected error: %v", err)
		}
	})
}
