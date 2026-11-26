package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"jinx/internal/execution"
	"jinx/internal/runtimebridge"
	"jinx/internal/supervisor"
)

func configurationArguments(directory string) []string {
	return []string{"-dispatcher", "http://127.0.0.1:8091", "-state-dir", directory, "-execution-config", filepath.Join(directory, "execution.json"), "-backend-definitions", filepath.Join(directory, "backends.json")}
}

func TestSettingsRequireIsolatedStateAndExplicitControl(testing *testing.T) {
	directory := testing.TempDir()
	arguments := configurationArguments(directory)
	config, err := parseSettings(arguments, io.Discard)
	if err != nil || config.enableControl || config.serviceUID != -1 {
		testing.Fatalf("%+v %v", config, err)
	}
	for _, extra := range [][]string{
		{"-enable-service-control"},
		{"-enable-service-control", "-service-uid", "501"},
		{"-execution-config", "/tmp/outside-control-plane.json"},
		{"-execution-config", filepath.Join(directory, "console", "console-changes.json")},
		{"-execution-config", filepath.Join(directory, "supervisor.json")},
		{"-listen", "0.0.0.0:8090"},
		{"-listen", "127.0.0.1:0"},
		{"-service-uid", "-2"},
		{"-service-plist-dir", "relative"},
	} {
		if _, err := parseSettings(append(append([]string{}, arguments...), extra...), io.Discard); err == nil {
			testing.Fatalf("accepted invalid options: %v", extra)
		}
	}
	if _, err := parseSettings(append(arguments, "-enable-service-control", "-service-uid", "501", "-service-plist-dir", directory), io.Discard); err != nil {
		testing.Fatal(err)
	}
	withConnectors, err := parseSettings(append(append([]string{}, arguments...), "-bilibili-connector", "http://127.0.0.1:18092", "-web-connector", "http://127.0.0.1:18093"), io.Discard)
	if err != nil || withConnectors.bilibiliConnector != "http://127.0.0.1:18092" || withConnectors.webConnector != "http://127.0.0.1:18093" {
		testing.Fatalf("failed parsing connector flags: %+v %v", withConnectors, err)
	}
	forums, err := parseSettings(append(append([]string{}, arguments...), "-connector", "zhihu=http://127.0.0.1:18095", "-connector", "v2ex=http://127.0.0.1:18096"), io.Discard)
	if err != nil || forums.connectors["zhihu"] != "http://127.0.0.1:18095" || forums.connectors["v2ex"] != "http://127.0.0.1:18096" {
		testing.Fatalf("repeatable -connector not parsed: %+v %v", forums.connectors, err)
	}
	for _, bad := range [][]string{{"-connector", "zhihu"}, {"-connector", "=http://127.0.0.1:1"}, {"-connector", "zhihu=http://127.0.0.1:1", "-connector", "zhihu=http://127.0.0.1:2"}} {
		if _, err := parseSettings(append(append([]string{}, arguments...), bad...), io.Discard); err == nil {
			testing.Fatalf("invalid -connector accepted: %v", bad)
		}
	}
}

func TestBackendDefinitionsUseSharedSchema(test *testing.T) {
	directory := test.TempDir()
	path := filepath.Join(directory, "backends.json")

	valid := `{"schemaVersion":"1","backends":[{"id":"text","kind":"text_only","models":[{"id":"model","label":"Model"}],"almaURL":"http://127.0.0.1:1234","providerID":"provider"}]}`
	for name, payload := range map[string]string{
		"valid":             valid,
		"large_valid":       valid + strings.Repeat(" ", 256<<10),
		"unknown_field":     strings.Replace(valid, `"id":"text"`, `"secret":"token","id":"text"`, 1),
		"duplicate_key":     strings.Replace(valid, `"id":"text"`, `"id":"hidden","ID":"text"`, 1),
		"duplicate_backend": strings.Replace(valid, `}]}`, `},{"id":"text","kind":"text_only","models":[{"id":"model","label":"Model"}],"almaURL":"http://127.0.0.1:1234","providerID":"provider"}]}`, 1),
		"version":           strings.Replace(valid, `"schemaVersion":"1"`, `"schemaVersion":"2"`, 1),
		"empty":             `{"schemaVersion":"1","backends":[]}`,
		"null":              `{"schemaVersion":"1","backends":null}`,
		"missing_provider":  strings.Replace(valid, `,"providerID":"provider"`, "", 1),
		"remote_origin":     strings.Replace(valid, "127.0.0.1", "192.0.2.1", 1),
		"chat_path":         strings.Replace(valid, ":1234", ":1234/api/chat/completions", 1),
		"legacy_fields":     strings.Replace(valid, "almaURL", "textBaseURL", 1),
		"trailing":          valid + `{}`,
		"oversized":         strings.Repeat(" ", (1<<20)+1),
		"legacy_versioned":  `{"schemaVersion":"1","backends":[{"id":"personal","kind":"alma_personal","models":[{"id":"provider:model","label":"Model"}],"almaBaseURL":"http://127.0.0.1:1234"}]}`,
	} {
		test.Run(name, func(testing *testing.T) {
			if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
				testing.Fatal(err)
			}
			options, sharedErr := runtimebridge.LoadBackendOptions(path)
			backends, err := loadBackends(path)
			wantValid := name == "valid" || name == "large_valid"
			if (err == nil) != wantValid || (sharedErr == nil) != wantValid {
				testing.Fatalf("control-plane=%v dispatcher=%v", err, sharedErr)
			}
			if wantValid {
				if len(backends) != 1 || !reflect.DeepEqual(backends[0].Models, options.Backends[0].Models) || backends[0].AlmaBaseURL != options.Backends[0].AlmaBaseURL {
					testing.Fatalf("backend definitions differ: %+v", backends)
				}
				if _, ok := backends[0].Provider.(*runtimebridge.AnthropicProvider); !ok {
					testing.Fatal("text backend does not use the shared Anthropic provider")
				}
			}
		})
	}
}

func TestDeprecatedUnversionedBackendDefinitions(testing *testing.T) {
	var warning bytes.Buffer
	output := log.Writer()
	log.SetOutput(&warning)
	testing.Cleanup(func() { log.SetOutput(output) })
	path := filepath.Join(testing.TempDir(), "backends.json")
	payload := `{"backends":[{"id":"text","kind":"text_only","models":[{"id":"model","label":"Model"}]},{"id":"personal","kind":"alma_personal","models":[{"id":"provider:model","label":"Model"}],"almaBaseURL":"http://127.0.0.1:1"},{"id":"legacy","kind":"legacy_hermes","models":[{"id":"model","label":"Model"}],"hermes":{"command":"/never/run/hermes","dir":"/never/run","profile":"test","toolsets":[],"maxTurns":1}}]}`
	if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
		testing.Fatal(err)
	}
	backends, err := loadBackends(path)
	if err != nil || len(backends) != 3 || backends[0].Provider != nil || backends[1].Models[0].ID != "provider:model" || backends[2].Hermes.Command != "/never/run/hermes" {
		testing.Fatalf("legacy compatibility: %+v %v", backends, err)
	}
	if !strings.Contains(warning.String(), "deprecated") || !strings.Contains(warning.String(), "schemaVersion 1") {
		testing.Fatal("legacy definitions did not emit a migration warning")
	}
	for _, payload := range []string{
		`{"backends":[],"secret":"token"}`,
		`{"backends":[{"id":"text","kind":"text_only","models":[],"env":["KEY=secret"]}]}`,
		`{"backends":null}`,
		`{"backends":[]} {}`,
		strings.Replace(payload, `{"backends":`, `{"schemaVersion":null,"backends":`, 1),
		strings.Replace(payload, `{"backends":`, `{"schemaVersion":"","backends":`, 1),
		strings.Replace(payload, `"id":"text"`, `"id":"text","textBaseURL":"http://127.0.0.1:1234"`, 1),
	} {
		if err := os.WriteFile(path, []byte(payload), 0600); err != nil {
			testing.Fatal(err)
		}
		if _, err := loadBackends(path); err == nil {
			testing.Fatalf("invalid or unsafe legacy definition accepted: %s", payload)
		}
	}
}

func TestSharedBackendFileCatalogRoutingAndNoEffectsOnAssembly(testing *testing.T) {
	directory := testing.TempDir()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { calls.Add(1) }))
	defer server.Close()
	const personalModel = "provider:family/model@revision+v1"
	const textModel = "family/model@revision+v1"
	payload, err := json.Marshal(runtimebridge.BackendFile{SchemaVersion: "1", Backends: []runtimebridge.BackendDefinition{
		{ID: "personal", Kind: execution.AlmaPersonal, Models: []execution.Model{{ID: personalModel, Label: "Personal model"}}, AlmaURL: server.URL},
		{ID: "text", Kind: execution.TextOnly, Models: []execution.Model{{ID: textModel, Label: "Text model"}}, AlmaURL: server.URL, ProviderID: "provider"},
	}})
	if err != nil {
		testing.Fatal(err)
	}
	path := filepath.Join(directory, "backends.json")
	if err := os.WriteFile(path, payload, 0600); err != nil {
		testing.Fatal(err)
	}
	config, err := parseSettings(append(configurationArguments(directory), "-dispatcher", server.URL), io.Discard)
	if err != nil {
		testing.Fatal(err)
	}
	config.enableControl, config.serviceUID, config.plistDir = true, 501, directory
	handler, cleanup, err := assemble(config)
	if err != nil {
		testing.Fatal(err)
	}
	defer cleanup()
	if calls.Load() != 0 {
		testing.Fatal("assembly contacted upstream")
	}
	options, err := runtimebridge.LoadBackendOptions(path)
	if err != nil {
		testing.Fatal(err)
	}
	engine, err := execution.Open(config.executionConfig, options)
	if err != nil {
		testing.Fatal(err)
	}
	expected, err := engine.Catalog(context.Background())
	if err != nil {
		testing.Fatal(err)
	}
	request := httptest.NewRequest("GET", "http://127.0.0.1:8090/console/v1/backends", nil)
	writer := httptest.NewRecorder()
	handler.ServeHTTP(writer, request)
	var catalog struct {
		OK    bool                `json:"ok"`
		Value []execution.Backend `json:"value"`
	}
	if writer.Code != 200 || json.Unmarshal(writer.Body.Bytes(), &catalog) != nil || !catalog.OK || !reflect.DeepEqual(catalog.Value, expected) || !catalog.Value[1].Available || catalog.Value[0].Models[0].ID != personalModel || catalog.Value[1].Models[0].ID != textModel || strings.Contains(writer.Body.String(), server.URL) || strings.Contains(writer.Body.String(), "almaBaseURL") || strings.Contains(writer.Body.String(), directory) || calls.Load() != 0 {
		testing.Fatalf("unsafe catalog: %d %s", writer.Code, writer.Body)
	}
	capabilities := map[execution.Capability]bool{}
	for _, capability := range execution.Capabilities() {
		capabilities[capability] = capability == execution.TextGeneration
	}
	routing := execution.Config{SchemaVersion: "1", Groups: map[string]execution.Group{}, Profiles: map[string]string{}, Policies: map[string]execution.Policy{"policy": {Capabilities: capabilities, AllowSoft: true}}}
	for _, backend := range expected {
		routing.Groups[backend.ID] = execution.Group{RouteID: backend.ID, PolicyID: "policy", Fallbacks: []execution.RouteAttempt{}, Primary: execution.RouteAttempt{BackendID: backend.ID, ModelID: backend.Models[0].ID, TimeoutSec: 10, Session: execution.SessionPolicy{Mode: "independent_run"}}}
	}
	snapshot, err := engine.Snapshot(context.Background())
	if err != nil {
		testing.Fatal(err)
	}
	if _, err := engine.ApplyConfig(context.Background(), snapshot.Revision, "shared-routing", routing); err != nil {
		testing.Fatal(err)
	}
	for _, backendID := range []string{"personal", "text"} {
		request = httptest.NewRequest("GET", "http://127.0.0.1:8090/console/v1/groups/"+backendID+"/routing", nil)
		writer = httptest.NewRecorder()
		handler.ServeHTTP(writer, request)
		var group struct {
			Value struct {
				Routing execution.Group `json:"routing"`
			} `json:"value"`
		}
		if writer.Code != 200 || json.Unmarshal(writer.Body.Bytes(), &group) != nil || !reflect.DeepEqual(group.Value.Routing, routing.Groups[backendID]) {
			testing.Fatalf("routing differs for %s: %s", backendID, writer.Body)
		}
		original := routing.Groups[backendID]
		confused := original
		confused.Primary.ModelID = personalModel
		if backendID == "personal" {
			confused.Primary.ModelID = textModel
		}
		routing.Groups[backendID] = confused
		if err := engine.Validate(context.Background(), routing); err == nil {
			testing.Fatalf("accepted the other backend's model ID for %s", backendID)
		}
		routing.Groups[backendID] = original
	}
	if calls.Load() != 0 {
		testing.Fatal("catalog or routing contacted a provider")
	}
	if _, err := os.Stat(filepath.Join(directory, "console", "console-changes.lock")); err != nil {
		testing.Fatal(err)
	}
	if _, err := os.Stat(config.executionConfig); err != nil {
		testing.Fatal(err)
	}
}

func TestObserverRejectsWritesBeforeStartingProcess(testing *testing.T) {
	observer := launchdObserver{uid: 501}
	for _, arguments := range [][]string{{"kickstart", "gui/501/io.jinx.relay"}, {"print", "gui/502/io.jinx.relay"}, {"print", "gui/501/arbitrary"}} {
		if _, _, _, err := observer.Run(context.Background(), "/bin/launchctl", arguments...); err == nil {
			testing.Fatal(arguments)
		}
	}
}

func TestParseSettingsServiceLabelsAndObserver(testing *testing.T) {
	directory := testing.TempDir()
	args := append(configurationArguments(directory), "-service-labels", "relay=org.example.relay,dispatcher=org.example.dispatcher")
	parsed, err := parseSettings(args, io.Discard)
	if err != nil {
		testing.Fatalf("parseSettings valid -service-labels failed: %v", err)
	}
	if parsed.serviceLabels[supervisor.Relay] != "org.example.relay" || parsed.serviceLabels[supervisor.Dispatcher] != "org.example.dispatcher" {
		testing.Fatalf("unexpected resolved labels: %+v", parsed.serviceLabels)
	}
	if parsed.serviceLabels[supervisor.Bridge] != "io.jinx.bridge" {
		testing.Fatalf("expected default bridge label fallback, got %q", parsed.serviceLabels[supervisor.Bridge])
	}

	observer := launchdObserver{uid: 501, labels: []string{parsed.serviceLabels[supervisor.Relay]}}
	if _, _, _, err := observer.Run(context.Background(), "/bin/launchctl", "print", "gui/501/io.jinx.relay"); err == nil {
		testing.Fatal("custom observer allowed unconfigured default label")
	}

	for _, bad := range []string{
		"relay=",
		"unknown=org.example.other",
		"relay=bad/label",
		"relay=org.example.dup,bridge=org.example.dup",
		"relay=org.example.a,relay=org.example.b",
	} {
		badArgs := append(configurationArguments(directory), "-service-labels", bad)
		if _, err := parseSettings(badArgs, io.Discard); err == nil {
			testing.Fatalf("accepted invalid -service-labels %q", bad)
		}
	}
}
