package saga_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ngaddam369/saga-conductor/internal/saga"
)

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "sagas.yaml")
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write YAML: %v", err)
	}
	return p
}

func TestLoadDefinitionsValid(t *testing.T) {
	t.Parallel()
	path := writeYAML(t, `
sagas:
  - name: order-saga
    saga_timeout_seconds: 300
    steps:
      - name: reserve
        forward_url: http://inventory/reserve
        compensate_url: http://inventory/release
        timeout_seconds: 10
        max_retries: 3
        retry_backoff_ms: 500
        auth_type: oidc
      - name: charge
        forward_url: http://payment/charge
        compensate_url: http://payment/refund
  - name: simple-saga
    steps:
      - name: do-it
        forward_url: http://service/do
        compensate_url: http://service/undo
`)

	defs, err := saga.LoadDefinitions(path)
	if err != nil {
		t.Fatalf("LoadDefinitions: %v", err)
	}
	if len(defs) != 2 {
		t.Fatalf("want 2 definitions, got %d", len(defs))
	}

	order := defs[0]
	if order.Name != "order-saga" {
		t.Errorf("name: want %q, got %q", "order-saga", order.Name)
	}
	if order.SagaTimeoutSeconds != 300 {
		t.Errorf("saga_timeout_seconds: want 300, got %d", order.SagaTimeoutSeconds)
	}
	if len(order.Steps) != 2 {
		t.Fatalf("want 2 steps, got %d", len(order.Steps))
	}

	step := order.Steps[0]
	if step.Name != "reserve" {
		t.Errorf("step name: want %q, got %q", "reserve", step.Name)
	}
	if step.ForwardURL != "http://inventory/reserve" {
		t.Errorf("forward_url: want %q, got %q", "http://inventory/reserve", step.ForwardURL)
	}
	if step.MaxRetries != 3 {
		t.Errorf("max_retries: want 3, got %d", step.MaxRetries)
	}
	if step.AuthType != "oidc" {
		t.Errorf("auth_type: want %q, got %q", "oidc", step.AuthType)
	}
}

func TestLoadDefinitionsValidationEmptyName(t *testing.T) {
	t.Parallel()
	path := writeYAML(t, `
sagas:
  - name: ""
    steps:
      - name: step1
        forward_url: http://svc/do
        compensate_url: http://svc/undo
`)
	_, err := saga.LoadDefinitions(path)
	if err == nil {
		t.Fatal("expected error for empty definition name, got nil")
	}
}

func TestLoadDefinitionsValidationNoSteps(t *testing.T) {
	t.Parallel()
	path := writeYAML(t, `
sagas:
  - name: empty-saga
    steps: []
`)
	_, err := saga.LoadDefinitions(path)
	if err == nil {
		t.Fatal("expected error for empty steps, got nil")
	}
}

func TestLoadDefinitionsValidationStepMissingForwardURL(t *testing.T) {
	t.Parallel()
	path := writeYAML(t, `
sagas:
  - name: bad-saga
    steps:
      - name: step1
        compensate_url: http://svc/undo
`)
	_, err := saga.LoadDefinitions(path)
	if err == nil {
		t.Fatal("expected error for missing forward_url, got nil")
	}
}

func TestLoadDefinitionsValidationStepMissingName(t *testing.T) {
	t.Parallel()
	path := writeYAML(t, `
sagas:
  - name: bad-saga
    steps:
      - forward_url: http://svc/do
        compensate_url: http://svc/undo
`)
	_, err := saga.LoadDefinitions(path)
	if err == nil {
		t.Fatal("expected error for missing step name, got nil")
	}
}

func TestLoadDefinitionsFileNotFound(t *testing.T) {
	t.Parallel()
	_, err := saga.LoadDefinitions("/nonexistent/path/sagas.yaml")
	if err == nil {
		t.Fatal("expected error for nonexistent file, got nil")
	}
}

func TestLoadDefinitionsMalformedYAML(t *testing.T) {
	t.Parallel()
	path := writeYAML(t, `this: [is: not: valid yaml`)
	_, err := saga.LoadDefinitions(path)
	if err == nil {
		t.Fatal("expected error for malformed YAML, got nil")
	}
}
