package runtimebridge

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"jinx/internal/execution"
)

func loadBackendFile(t *testing.T, data string) (execution.Options, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "operator.json")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return LoadBackendOptions(path)
}

func TestOperatorBackendFileSupportsAllKinds(t *testing.T) {
	data := `{"schemaVersion":"1","backends":[
		{"id":"personal","kind":"alma_personal","models":[{"id":"mock-model","label":"Mock model"}],"almaURL":"http://127.0.0.1:1234/"},
		{"id":"text","kind":"text_only","models":[{"id":"mock-model","label":"Mock model"}],"almaURL":"http://127.0.0.1:1234","providerID":"mock-provider"},
		{"id":"legacy","kind":"legacy_hermes","models":[{"id":"mock-model","label":"Mock model"}],"hermes":{"command":"/operator/hermes","dir":"/operator/work","profile":"fixed-profile","toolsets":["web","memory","skills"],"maxTurns":3}},
		{"id":"agy","kind":"agy_cli","models":[{"id":"gemini-4-hydrogen-low","label":"Hydrogen Low"}],"agy":{"command":"/operator/agy","dir":"/operator/agy-work","agent":"helper","effort":"low","sandbox":true,"disableSlashCommands":true,"settingsFile":"/operator/settings.json","addDirs":["/operator/extra"]}}
	]}`
	options, err := loadBackendFile(t, data)
	if err != nil || len(options.Backends) != 4 {
		t.Fatal(options, err)
	}
	if options.Backends[0].Kind != execution.AlmaPersonal || options.Backends[0].AlmaBaseURL != "http://127.0.0.1:1234" || options.Backends[0].Provider != nil ||
		options.Backends[1].Kind != execution.TextOnly || options.Backends[1].Provider == nil {
		t.Fatal(options)
	}
	legacy := options.Backends[2]
	expected := execution.HermesOptions{Command: "/operator/hermes", Dir: "/operator/work", Profile: "fixed-profile", Toolsets: []string{"web", "memory", "skills"}, MaxTurns: 3}
	if legacy.Kind != execution.LegacyHermes || legacy.AlmaBaseURL != "" || legacy.Provider != nil || !reflect.DeepEqual(legacy.Hermes, expected) {
		t.Fatal(legacy)
	}
	agy := options.Backends[3]
	expectedAgy := execution.AgyOptions{
		Command: "/operator/agy", Dir: "/operator/agy-work", Agent: "helper", Effort: "low",
		Sandbox: true, DisableSlashCommands: true, SettingsFile: "/operator/settings.json", AddDirs: []string{"/operator/extra"},
	}
	if agy.Kind != execution.AgyCLI || agy.AlmaBaseURL != "" || agy.Provider != nil || !reflect.DeepEqual(agy.Agy, expectedAgy) {
		t.Fatal(agy)
	}
	if _, err := execution.Open(filepath.Join(t.TempDir(), "execution.json"), options); err != nil {
		t.Fatal("loader returned invalid engine options", err)
	}
}

func TestOperatorBackendFileAllowlist(t *testing.T) {
	valid := `{"schemaVersion":"1","backends":[{"id":"text","kind":"text_only","models":[{"id":"mock-model","label":"Mock model"}],"almaURL":"http://127.0.0.1:1234","providerID":"mock-provider"}]}`
	for _, scenario := range []string{"valid", "unknown_field", "command", "proxy", "credentials", "duplicate_key", "duplicate_id", "trailing", "oversized", "legacy", "remote_url", "provider_path", "schema"} {
		t.Run(scenario, func(t *testing.T) {
			data := valid
			switch scenario {
			case "unknown_field", "command", "proxy", "credentials":
				data = strings.Replace(valid, `"id":"text"`, `"`+scenario+`":"rejected","id":"text"`, 1)
			case "duplicate_key":
				data = strings.Replace(valid, `"id":"text"`, `"id":"hidden","id":"text"`, 1)
			case "duplicate_id":
				var config BackendFile
				_ = json.Unmarshal([]byte(valid), &config)
				config.Backends = append(config.Backends, config.Backends[0])
				encoded, _ := json.Marshal(config)
				data = string(encoded)
			case "trailing":
				data += `{}`
			case "oversized":
				data = strings.Repeat(" ", maxRequestBytes+1)
			case "legacy":
				data = strings.Replace(valid, "text_only", "legacy_hermes", 1)
			case "remote_url":
				data = strings.Replace(valid, "127.0.0.1", "192.0.2.1", 1)
			case "provider_path":
				data = strings.Replace(valid, "mock-provider", "../path", 1)
			case "schema":
				data = strings.Replace(valid, `"schemaVersion":"1"`, `"schemaVersion":"2"`, 1)
			}
			options, err := loadBackendFile(t, data)
			if (err == nil) != (scenario == "valid") {
				t.Fatal(options, err)
			}
			if scenario == "valid" && (len(options.Backends) != 1 || options.Backends[0].Kind != execution.TextOnly || options.Backends[0].Provider == nil) {
				t.Fatal(options)
			}
		})
	}
}

func TestOperatorBackendFileRejectsMixedFields(t *testing.T) {
	for _, kind := range []execution.Kind{execution.AlmaPersonal, execution.TextOnly, execution.LegacyHermes, execution.AgyCLI} {
		for _, field := range []string{`"hermes":null`, `"hermes":{}`, `"hermes":{"command":"/operator/hermes"}`, `"HERmes":null`,
			`"agy":null`, `"agy":{}`, `"agy":{"command":"/operator/agy"}`, `"AGY":null`,
			`"almaURL":"http://127.0.0.1:1234"`, `"almaURL":""`, `"ALMAURL":null`, `"providerID":"mock-provider"`, `"providerID":""`, `"PROVIDERID":null`} {
			lowerField := strings.ToLower(field)
			if ((kind == execution.AlmaPersonal || kind == execution.TextOnly) && (strings.Contains(lowerField, "almaurl") || strings.Contains(lowerField, "providerid"))) ||
				(kind == execution.LegacyHermes && strings.Contains(lowerField, "hermes")) ||
				(kind == execution.AgyCLI && strings.Contains(lowerField, "agy")) {
				continue
			}
			t.Run(string(kind)+"/"+field, func(t *testing.T) {
				base := `"almaURL":"http://127.0.0.1:1234"`
				if kind == execution.TextOnly {
					base += `,"providerID":"mock-provider"`
				}
				if kind == execution.LegacyHermes {
					base = `"hermes":{"command":"/operator/hermes","profile":"fixed-profile","toolsets":[],"maxTurns":3}`
				}
				if kind == execution.AgyCLI {
					base = `"agy":{"command":"/operator/agy"}`
				}
				data := `{"schemaVersion":"1","backends":[{"id":"backend","kind":"` + string(kind) + `","models":[{"id":"mock-model","label":"Mock model"}],` + base + `,` + field + `}]}`
				if _, err := loadBackendFile(t, data); execution.Code(err) != "validation" {
					t.Fatal("mixed backend fields accepted", err)
				}
			})
		}
	}
}

func TestOperatorAgyValidation(t *testing.T) {
	valid := `{"schemaVersion":"1","backends":[{"id":"agy","kind":"agy_cli","models":[{"id":"gemini-4-hydrogen-low","label":"Hydrogen Low"}],"agy":{"command":"/operator/agy","dir":"/operator/work","agent":"helper","effort":"low","sandbox":true,"settingsFile":"/operator/settings.json","addDirs":["/operator/extra"]}}]}`
	for _, scenario := range []struct {
		name string
		from string
		to   string
	}{
		{"missing_agy", `,"agy":{"command":"/operator/agy","dir":"/operator/work","agent":"helper","effort":"low","sandbox":true,"settingsFile":"/operator/settings.json","addDirs":["/operator/extra"]}`, ""},
		{"null_agy", `"agy":{"command":"/operator/agy","dir":"/operator/work","agent":"helper","effort":"low","sandbox":true,"settingsFile":"/operator/settings.json","addDirs":["/operator/extra"]}`, `"agy":null`},
		{"relative_command", `"/operator/agy"`, `"agy"`},
		{"relative_dir", `"/operator/work"`, `"work"`},
		{"relative_settings", `"/operator/settings.json"`, `"settings.json"`},
		{"relative_add_dir", `["/operator/extra"]`, `["extra"]`},
		{"too_many_add_dirs", `["/operator/extra"]`, `["/1","/2","/3","/4","/5","/6","/7","/8","/9"]`},
		{"bad_effort", `"low"`, `"ultra"`},
		{"bad_agent", `"helper"`, `"../bad"`},
		{"env_rejected", `"sandbox":true`, `"sandbox":true,"env":["SECRET=1"]`},
		{"skip_perms_rejected", `"sandbox":true`, `"sandbox":true,"dangerouslySkipPermissions":true`},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			if _, err := loadBackendFile(t, strings.Replace(valid, scenario.from, scenario.to, 1)); execution.Code(err) != "validation" {
				t.Fatalf("expected validation failure for %s", scenario.name)
			}
		})
	}
}

func TestOperatorHermesValidation(t *testing.T) {
	valid := `{"schemaVersion":"1","backends":[{"id":"legacy","kind":"legacy_hermes","models":[{"id":"mock-model","label":"Mock model"}],"hermes":{"command":"/operator/hermes","dir":"/operator/work","profile":"fixed-profile","toolsets":["web"],"maxTurns":3}}]}`
	for _, scenario := range []struct {
		name string
		from string
		to   string
	}{
		{"missing_hermes", `,"hermes":{"command":"/operator/hermes","dir":"/operator/work","profile":"fixed-profile","toolsets":["web"],"maxTurns":3}`, ""},
		{"null_hermes", `"hermes":{"command":"/operator/hermes","dir":"/operator/work","profile":"fixed-profile","toolsets":["web"],"maxTurns":3}`, `"hermes":null`},
		{"relative_command", `"/operator/hermes"`, `"hermes"`},
		{"missing_command", `"command":"/operator/hermes",`, ""},
		{"relative_dir", `"/operator/work"`, `"work"`},
		{"profile_path", `"fixed-profile"`, `"../profile"`},
		{"profile_flag", `"fixed-profile"`, `"--profile"`},
		{"empty_profile", `"fixed-profile"`, `""`},
		{"zero_turns", `"maxTurns":3`, `"maxTurns":0`},
		{"too_many_turns", `"maxTurns":3`, `"maxTurns":101`},
		{"fractional_turns", `"maxTurns":3`, `"maxTurns":1.5`},
		{"missing_turns", `,"maxTurns":3`, ""},
		{"unknown_toolset", `["web"]`, `["shell"]`},
		{"duplicate_toolset", `["web"]`, `["web","web"]`},
		{"env", `"maxTurns":3`, `"maxTurns":3,"env":["PRIVATE=value"]`},
		{"extra_args", `"maxTurns":3`, `"maxTurns":3,"args":["--resume"]`},
		{"duplicate_key", `"maxTurns":3`, `"maxTurns":3,"MAXTURNS":1`},
		{"empty_models", `[{"id":"mock-model","label":"Mock model"}]`, `[]`},
		{"duplicate_models", `[{"id":"mock-model","label":"Mock model"}]`, `[{"id":"mock-model","label":"Mock model"},{"id":"mock-model","label":"Other model"}]`},
		{"model_flag", `"mock-model"`, `"--model"`},
		{"model_whitespace", `"mock-model"`, `"model --flag"`},
		{"empty_model_label", `"Mock model"`, `" "`},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			if _, err := loadBackendFile(t, strings.Replace(valid, scenario.from, scenario.to, 1)); execution.Code(err) != "validation" {
				t.Fatal("invalid operator Hermes configuration accepted", err)
			}
		})
	}
	for _, kind := range []execution.Kind{execution.AlmaPersonal, execution.TextOnly} {
		t.Run(string(kind)+"/invalid_model", func(t *testing.T) {
			data := `{"schemaVersion":"1","backends":[{"id":"backend","kind":"` + string(kind) + `","almaURL":"http://127.0.0.1:1234","models":[{"id":"--model","label":"Mock model"}]}]}`
			if _, err := loadBackendFile(t, data); execution.Code(err) != "validation" {
				t.Fatal(err)
			}
		})
	}
}

func TestLoadedHermesKeepsSoftPermissionsAndFallbackBoundaries(t *testing.T) {
	for _, scenario := range []string{"hard_no_tools", "hard_to_soft_fallback", "remote_unknown"} {
		t.Run(scenario, func(t *testing.T) {
			directory := t.TempDir()
			command := filepath.Join(directory, "fixed-hermes-stub")
			if err := os.WriteFile(command, []byte("#!/bin/sh\nprintf 'PRIVATE STDERR' >&2\nexit 1\n"), 0700); err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(BackendFile{SchemaVersion: execution.SchemaVersion, Backends: []BackendDefinition{{ID: "legacy", Kind: execution.LegacyHermes,
				Models: []execution.Model{{ID: testModel, Label: "Mock model"}}, Hermes: &HermesDefinition{Command: command, Profile: "fixed-profile", Toolsets: []string{"web"}, MaxTurns: 3}}}})
			if err != nil {
				t.Fatal(err)
			}
			options, err := loadBackendFile(t, string(data))
			if err != nil {
				t.Fatal(err)
			}
			fallbackCalls := 0
			options.Backends = append(options.Backends, textBackend("text", func(context.Context, execution.ModelRequest) (string, error) {
				fallbackCalls++
				return "unexpected fallback", nil
			}))
			engine, err := execution.Open(filepath.Join(directory, "execution.json"), options)
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := engine.Catalog(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			for _, evidence := range catalog[0].PermissionEvidence {
				if evidence.Capability != execution.TextGeneration && evidence.Capability != execution.AutoTextDelivery &&
					(evidence.Enforcement != execution.Soft || evidence.Provenance != "hermes_cli_toolsets" || evidence.Effective != nil) {
					t.Fatal("CLI toolset selection claimed isolation", evidence)
				}
			}
			config := testConfig("legacy", scenario != "hard_no_tools")
			if scenario == "hard_to_soft_fallback" {
				config = testConfig("text", true)
				group := config.Groups[testGroup]
				group.Fallbacks = []execution.RouteAttempt{testAttempt("legacy")}
				config.Groups[testGroup] = group
			}
			if scenario != "remote_unknown" {
				expected := "policy_mismatch"
				if scenario == "hard_to_soft_fallback" {
					expected = "fallback_policy_mismatch"
				}
				if err := engine.Validate(context.Background(), config); execution.Code(err) != expected {
					t.Fatal(err)
				}
				return
			}
			group := config.Groups[testGroup]
			group.Fallbacks = []execution.RouteAttempt{testAttempt("text")}
			config.Groups[testGroup] = group
			publish(t, engine, config, "initial")
			result, err := engine.Execute(context.Background(), execution.Request{TraceID: "uncertain", GroupID: testGroup, Prompt: "synthetic prompt"})
			if execution.Code(err) != "execution_uncertain" || result.Fallback || result.Text != "" || fallbackCalls != 0 || len(result.Attempts) != 1 ||
				result.Attempts[0].RemoteExecution != "unknown" || result.Attempts[0].Reason != "backend_failed" || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatal(result, err, fallbackCalls)
			}
		})
	}
}
