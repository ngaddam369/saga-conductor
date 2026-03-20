package main

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
)

func TestBuildTracerProviderEmptyEndpoint(t *testing.T) {
	// Capture the global provider before the call so we can verify it is
	// unchanged afterwards — an empty endpoint must be a no-op.
	before := otel.GetTracerProvider()

	shutdown, err := buildTracerProvider(context.Background(), "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if otel.GetTracerProvider() != before {
		t.Error("empty endpoint must not change the global TracerProvider")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown returned error: %v", err)
	}
}

func TestBuildTracerProviderUnavailableEndpoint(t *testing.T) {
	// A non-empty endpoint that is unreachable. otlptracegrpc connects lazily,
	// so buildTracerProvider itself must succeed; the error only surfaces when
	// spans are flushed. Verify the call completes without error and that a
	// non-nil shutdown is returned.
	shutdown, err := buildTracerProvider(context.Background(), "localhost:14317")
	if err != nil {
		t.Fatalf("unexpected error for unreachable endpoint: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil shutdown function")
	}
	// Flush with a short deadline — expect either success or a timeout error,
	// not a panic or nil dereference.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_ = shutdown(ctx) // flush error is acceptable; we just verify no panic
}
