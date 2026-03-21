package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ngaddam369/saga-conductor/internal/dashboard"
)

// noFlushWriter is a minimal http.ResponseWriter that does NOT implement
// http.Flusher, allowing us to test the SSEHandler's streaming-unsupported path.
// (httptest.ResponseRecorder has a Flush method and therefore satisfies
// http.Flusher, so it cannot be used for this test.)
type noFlushWriter struct {
	header http.Header
	body   strings.Builder
	code   int
}

func (w *noFlushWriter) Header() http.Header         { return w.header }
func (w *noFlushWriter) WriteHeader(code int)        { w.code = code }
func (w *noFlushWriter) Write(b []byte) (int, error) { return w.body.Write(b) }

func TestSSEHandlerNoFlusherReturns500(t *testing.T) {
	t.Parallel()

	b := dashboard.NewBroadcaster()
	handler := b.SSEHandler()

	req := httptest.NewRequest(http.MethodGet, "/dashboard/events", nil)
	w := &noFlushWriter{header: make(http.Header)}

	handler(w, req)

	if w.code != http.StatusInternalServerError {
		t.Errorf("status: got %d, want %d", w.code, http.StatusInternalServerError)
	}
	if body := w.body.String(); !strings.Contains(body, "streaming unsupported") {
		t.Errorf("body: want %q substring, got %q", "streaming unsupported", body)
	}
}

func TestPageHandlerServesHTML(t *testing.T) {
	t.Parallel()

	handler := dashboard.PageHandler()
	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
		t.Errorf("Content-Type: got %q, want %q", ct, "text/html; charset=utf-8")
	}
	if body := rec.Body.String(); !strings.Contains(body, "<!DOCTYPE html>") {
		t.Errorf("body does not contain <!DOCTYPE html>; got: %.100s", body)
	}
}
