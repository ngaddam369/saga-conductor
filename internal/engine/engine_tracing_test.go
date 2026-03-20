// Internal test file (package engine, not engine_test) so callHTTP can be
// called directly without exporting it.
package engine

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/ngaddam369/saga-conductor/internal/store"
)

// setupInMemoryTracer installs an in-memory span exporter as the global
// TracerProvider and W3C propagator for the duration of the test, then
// restores the previous provider in t.Cleanup.
func setupInMemoryTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	oldTP := otel.GetTracerProvider()
	oldProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))
	t.Cleanup(func() {
		otel.SetTracerProvider(oldTP)
		otel.SetTextMapPropagator(oldProp)
	})
	return exp
}

func newEngineInternal(t *testing.T) *Engine {
	t.Helper()
	s, err := store.NewBoltStore(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return New(s)
}

func TestCallHTTPCreatesSpan(t *testing.T) {
	exp := setupInMemoryTracer(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	eng := newEngineInternal(t)
	stepErr, err := eng.callHTTP(context.Background(), srv.URL, []byte(`{}`), 0, StepAuthContext{})
	if err != nil || stepErr != nil {
		t.Fatalf("callHTTP: stepErr=%v err=%v", stepErr, err)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	span := spans[0]
	if span.Name != "saga.step.http" {
		t.Errorf("span name: got %q, want %q", span.Name, "saga.step.http")
	}
}

func TestCallHTTPInjectsTraceparentHeader(t *testing.T) {
	setupInMemoryTracer(t)

	var receivedTraceparent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedTraceparent = r.Header.Get("Traceparent")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	eng := newEngineInternal(t)
	if _, err := eng.callHTTP(context.Background(), srv.URL, []byte(`{}`), 0, StepAuthContext{}); err != nil {
		t.Fatalf("callHTTP: %v", err)
	}

	if receivedTraceparent == "" {
		t.Error("expected Traceparent header to be injected into outbound request, got empty")
	}
}

func TestCallHTTPSpanAttributesOnHTTPError(t *testing.T) {
	exp := setupInMemoryTracer(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	eng := newEngineInternal(t)
	stepErr, _ := eng.callHTTP(context.Background(), srv.URL, []byte(`{}`), 0, StepAuthContext{})
	if stepErr == nil {
		t.Fatal("expected stepErr for 500 response")
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	span := spans[0]

	// Verify http.response.status_code attribute is present and correct.
	var gotStatus int
	for _, attr := range span.Attributes {
		if string(attr.Key) == "http.response.status_code" {
			gotStatus = int(attr.Value.AsInt64())
		}
	}
	if gotStatus != http.StatusInternalServerError {
		t.Errorf("http.response.status_code: got %d, want %d", gotStatus, http.StatusInternalServerError)
	}
}

func TestCallHTTPSpanAttributesOnNetworkError(t *testing.T) {
	exp := setupInMemoryTracer(t)

	eng := newEngineInternal(t)
	// Use a closed server address to force a network error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	addr := srv.URL
	srv.Close() // close immediately so the connection is refused

	// Short timeout to fail fast.
	stepErr, _ := eng.callHTTP(
		context.Background(), addr, []byte(`{}`),
		1, // 1 second timeout — plenty for a refused connection
		StepAuthContext{},
	)
	if stepErr == nil {
		t.Fatal("expected stepErr for network error")
	}
	if !stepErr.IsNetworkError {
		t.Error("expected IsNetworkError=true")
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	// Span must have recorded the error.
	span := spans[0]
	if len(span.Events) == 0 {
		t.Error("expected span to record the network error as an event")
	}
	_ = time.Second // keep time import used
}
