package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ngaddam369/saga-conductor/internal/dashboard"
)

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
