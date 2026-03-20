package auth_test

import (
	"context"
	"testing"

	"github.com/ngaddam369/saga-conductor/internal/engine"
	"github.com/ngaddam369/saga-conductor/pkg/auth"
)

func TestStaticTokenSource(t *testing.T) {
	t.Parallel()

	t.Run("returns configured token", func(t *testing.T) {
		t.Parallel()
		src := auth.NewStaticTokenSource("my-secret")
		got, err := src.Token(context.Background(), "http://example.com/step", engine.StepAuthContext{})
		if err != nil {
			t.Fatalf("Token: unexpected error: %v", err)
		}
		if got != "my-secret" {
			t.Errorf("Token: got %q, want %q", got, "my-secret")
		}
	})

	t.Run("returns same token for any URL", func(t *testing.T) {
		t.Parallel()
		src := auth.NewStaticTokenSource("tok")
		for _, url := range []string{
			"http://service-a/forward",
			"http://service-b/compensate",
			"https://other.internal/step",
		} {
			got, err := src.Token(context.Background(), url, engine.StepAuthContext{})
			if err != nil {
				t.Fatalf("Token(%q): unexpected error: %v", url, err)
			}
			if got != "tok" {
				t.Errorf("Token(%q): got %q, want %q", url, got, "tok")
			}
		}
	})
}

func TestStaticTokenValidator(t *testing.T) {
	t.Parallel()

	t.Run("accepts correct token", func(t *testing.T) {
		t.Parallel()
		v := auth.NewStaticTokenValidator("correct-secret")
		if err := v.Validate(context.Background(), "correct-secret"); err != nil {
			t.Errorf("Validate: unexpected error: %v", err)
		}
	})

	t.Run("rejects wrong token", func(t *testing.T) {
		t.Parallel()
		v := auth.NewStaticTokenValidator("correct-secret")
		if err := v.Validate(context.Background(), "wrong-secret"); err == nil {
			t.Error("Validate: expected error for wrong token, got nil")
		}
	})

	t.Run("rejects empty token", func(t *testing.T) {
		t.Parallel()
		v := auth.NewStaticTokenValidator("correct-secret")
		if err := v.Validate(context.Background(), ""); err == nil {
			t.Error("Validate: expected error for empty token, got nil")
		}
	})

	t.Run("rejects when expected is empty", func(t *testing.T) {
		t.Parallel()
		// A validator with no configured secret must reject everything,
		// including an empty token — misconfigured validator must not grant access.
		v := auth.NewStaticTokenValidator("")
		if err := v.Validate(context.Background(), ""); err == nil {
			t.Error("Validate: expected error when expected is empty, got nil")
		}
	})

	t.Run("rejects prefix match", func(t *testing.T) {
		t.Parallel()
		// "correct-secretX" must not be accepted for expected "correct-secret".
		v := auth.NewStaticTokenValidator("correct-secret")
		if err := v.Validate(context.Background(), "correct-secretX"); err == nil {
			t.Error("Validate: expected error for token that is a prefix extension, got nil")
		}
	})
}
