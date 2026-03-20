package saga

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

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
		for j, step := range def.Steps {
			if step.Name == "" {
				return nil, fmt.Errorf("definition %q step %d: name is required", def.Name, j)
			}
			if step.ForwardURL == "" {
				return nil, fmt.Errorf("definition %q step %q: forward_url is required", def.Name, step.Name)
			}
		}
	}

	return f.Sagas, nil
}
