package saga

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// stepNameRE mirrors the validation in internal/server so that YAML-loaded
// templates are rejected at startup rather than at saga-creation time.
var stepNameRE = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// SagaDefinition is the in-memory representation of one saga template loaded
// from a YAML definitions file. Templates are referenced by name when calling
// CreateSaga without inline step definitions.
type SagaDefinition struct {
	Name               string           `yaml:"name"`
	SagaTimeoutSeconds int              `yaml:"saga_timeout_seconds"`
	Steps              []StepDefinition `yaml:"steps"`
}

// definitionFile is the root structure of a YAML definitions file.
type definitionFile struct {
	Sagas []SagaDefinition `yaml:"sagas"`
}

// LoadDefinitions parses the YAML file at path and returns the validated list
// of saga templates it contains. Every definition must have a non-empty name
// and at least one step; every step must have a non-empty name and forward_url.
func LoadDefinitions(path string) ([]SagaDefinition, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read definitions file: %w", err)
	}

	var f definitionFile
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse definitions file: %w", err)
	}

	for i, def := range f.Sagas {
		if def.Name == "" {
			return nil, fmt.Errorf("definition %d: name is required", i)
		}
		if len(def.Steps) == 0 {
			return nil, fmt.Errorf("definition %q: at least one step is required", def.Name)
		}
		if def.SagaTimeoutSeconds != 0 && (def.SagaTimeoutSeconds < 1 || def.SagaTimeoutSeconds > 86400) {
			return nil, fmt.Errorf("definition %q: saga_timeout_seconds must be in [1, 86400]", def.Name)
		}
		seen := make(map[string]struct{})
		for j, step := range def.Steps {
			if step.Name == "" {
				return nil, fmt.Errorf("definition %q step %d: name is required", def.Name, j)
			}
			if !stepNameRE.MatchString(step.Name) {
				return nil, fmt.Errorf("definition %q step %d: name must match ^[a-zA-Z0-9_-]{1,64}$", def.Name, j)
			}
			if _, dup := seen[step.Name]; dup {
				return nil, fmt.Errorf("definition %q: duplicate step name %q", def.Name, step.Name)
			}
			seen[step.Name] = struct{}{}
			if step.ForwardURL == "" {
				return nil, fmt.Errorf("definition %q step %q: forward_url is required", def.Name, step.Name)
			}
			if step.CompensateURL == "" {
				return nil, fmt.Errorf("definition %q step %q: compensate_url is required", def.Name, step.Name)
			}
			if step.TimeoutSeconds != 0 && (step.TimeoutSeconds < 1 || step.TimeoutSeconds > 3600) {
				return nil, fmt.Errorf("definition %q step %q: timeout_seconds must be in [1, 3600]", def.Name, step.Name)
			}
			if step.MaxRetries < 0 || step.MaxRetries > 100 {
				return nil, fmt.Errorf("definition %q step %q: max_retries must be in [0, 100]", def.Name, step.Name)
			}
			if step.RetryBackoffMs < 0 || step.RetryBackoffMs > 60000 {
				return nil, fmt.Errorf("definition %q step %q: retry_backoff_ms must be in [0, 60000]", def.Name, step.Name)
			}
			validAuthTypes := map[string]bool{
				"none": true, "static": true, "jwt": true,
				"oidc": true, "svid-exchange": true,
			}
			if step.AuthType != "" && !validAuthTypes[step.AuthType] {
				return nil, fmt.Errorf("definition %q step %q: unknown auth_type %q", def.Name, step.Name, step.AuthType)
			}
			if step.TargetSPIFFEID != "" && !strings.HasPrefix(step.TargetSPIFFEID, "spiffe://") {
				return nil, fmt.Errorf("definition %q step %q: target_spiffe_id must start with spiffe://", def.Name, step.Name)
			}
		}
	}

	return f.Sagas, nil
}
