package runtimebridge

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"jinx/internal/execution"
)

type BackendFile struct {
	SchemaVersion string              `json:"schemaVersion"`
	Backends      []BackendDefinition `json:"backends"`
}

type BackendDefinition struct {
	ID         string            `json:"id"`
	Kind       execution.Kind    `json:"kind"`
	Models     []execution.Model `json:"models"`
	AlmaURL    string            `json:"almaURL,omitempty"`
	ProviderID string            `json:"providerID,omitempty"`
	Hermes     *HermesDefinition `json:"hermes,omitempty"`
}

type HermesDefinition struct {
	Command  string   `json:"command"`
	Dir      string   `json:"dir,omitempty"`
	Profile  string   `json:"profile"`
	Toolsets []string `json:"toolsets"`
	MaxTurns int      `json:"maxTurns"`
}

var modelIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_./:@+-]{0,255}$`)

func LoadBackendOptions(path string) (execution.Options, error) {
	file, err := os.Open(path)
	if err != nil {
		return execution.Options{}, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxRequestBytes+1))
	if err != nil || len(data) > maxRequestBytes {
		return execution.Options{}, &execution.Error{Code: "too_large", Field: "backends_file"}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config BackendFile
	if validateJSON(data) != nil || decoder.Decode(&config) != nil || decoder.Decode(new(any)) != io.EOF || config.SchemaVersion != execution.SchemaVersion || len(config.Backends) == 0 {
		return execution.Options{}, &execution.Error{Code: "validation", Field: "backends_file"}
	}
	var fields struct {
		Backends []map[string]json.RawMessage `json:"backends"`
	}
	if json.Unmarshal(data, &fields) != nil {
		return execution.Options{}, &execution.Error{Code: "validation", Field: "backends_file"}
	}
	options := execution.Options{}
	seen := map[string]bool{}
	for index, backend := range config.Backends {
		if !providerIDPattern.MatchString(backend.ID) || seen[backend.ID] || len(backend.Models) == 0 {
			return execution.Options{}, &execution.Error{Code: "validation", Field: "backend"}
		}
		seen[backend.ID] = true
		models := map[string]bool{}
		for _, model := range backend.Models {
			if !modelIDPattern.MatchString(model.ID) || models[model.ID] || strings.TrimSpace(model.Label) == "" {
				return execution.Options{}, &execution.Error{Code: "validation", Field: "models"}
			}
			models[model.ID] = true
		}
		for field := range fields.Backends[index] {
			field = strings.ToLower(field)
			if (backend.Kind == execution.LegacyHermes && (field == "almaurl" || field == "providerid")) ||
				((backend.Kind == execution.AlmaPersonal || backend.Kind == execution.TextOnly) && field == "hermes") {
				return execution.Options{}, &execution.Error{Code: "validation", Field: "backend_fields"}
			}
		}
		option := execution.BackendOptions{ID: backend.ID, Kind: backend.Kind, Models: backend.Models}
		switch backend.Kind {
		case execution.AlmaPersonal, execution.TextOnly:
			origin, err := operatorOrigin(backend.AlmaURL)
			if err != nil {
				return execution.Options{}, err
			}
			option.AlmaBaseURL = origin
			if backend.Kind == execution.AlmaPersonal && backend.ProviderID != "" {
				return execution.Options{}, &execution.Error{Code: "validation", Field: "personal_providerID"}
			}
			if backend.Kind == execution.TextOnly {
				option.Provider, err = NewAnthropicProvider(origin, backend.ProviderID)
				if err != nil {
					return execution.Options{}, err
				}
			}
		case execution.LegacyHermes:
			hermes := backend.Hermes
			if hermes == nil || !filepath.IsAbs(hermes.Command) || (hermes.Dir != "" && !filepath.IsAbs(hermes.Dir)) ||
				!providerIDPattern.MatchString(hermes.Profile) || hermes.MaxTurns < 1 || hermes.MaxTurns > 100 {
				return execution.Options{}, &execution.Error{Code: "validation", Field: "hermes_options"}
			}
			tools := map[string]bool{}
			for _, tool := range hermes.Toolsets {
				if (tool != "web" && tool != "memory" && tool != "skills") || tools[tool] {
					return execution.Options{}, &execution.Error{Code: "validation", Field: "hermes_toolsets"}
				}
				tools[tool] = true
			}
			option.Hermes = execution.HermesOptions{Command: hermes.Command, Dir: hermes.Dir, Profile: hermes.Profile,
				Toolsets: hermes.Toolsets, MaxTurns: hermes.MaxTurns}
		default:
			return execution.Options{}, &execution.Error{Code: "unsupported", Field: "backend_kind"}
		}
		options.Backends = append(options.Backends, option)
	}
	return options, nil
}

func validateJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func(int) error
	walk = func(depth int) error {
		if depth > 64 {
			return &execution.Error{Code: "validation", Field: "json_depth"}
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delimiter, compound := token.(json.Delim)
		if !compound {
			return nil
		}
		keys := map[string]bool{}
		for decoder.More() {
			if delimiter == '{' {
				key, err := decoder.Token()
				if err != nil {
					return err
				}
				name, valid := key.(string)
				name = strings.ToLower(name)
				if !valid || keys[name] {
					return &execution.Error{Code: "validation", Field: "duplicate_json_key"}
				}
				keys[name] = true
			}
			if err := walk(depth + 1); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	}
	if err := walk(0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return &execution.Error{Code: "validation", Field: "trailing_json"}
	}
	return nil
}
