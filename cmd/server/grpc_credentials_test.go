package main

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestBuildGRPCCredentialsEmptySocket(t *testing.T) {
	creds, cleanup, err := buildGRPCCredentials(context.Background(), "", zerolog.Nop())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cleanup()
	if creds != nil {
		t.Fatal("expected nil credentials for empty socket path")
	}
}

func TestBuildGRPCCredentialsUnavailableSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	creds, cleanup, err := buildGRPCCredentials(ctx, "unix:///nonexistent.sock", zerolog.Nop())
	defer cleanup()
	if err == nil {
		t.Fatal("expected error when workload API is unavailable")
	}
	if creds != nil {
		t.Fatal("expected nil credentials on error")
	}
}
