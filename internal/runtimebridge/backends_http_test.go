package runtimebridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"jinx/internal/execution"
	"jinx/internal/gateway"
)

func TestOperatorHermesIsPrivateAndCannotBeConfiguredOverHTTP(t *testing.T) {
	directory := t.TempDir()
	command := filepath.Join(directory, "private-fixed-command")
	script := "#!/bin/sh\nprintf 'executed' > started\n"
	if err := os.WriteFile(command, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(BackendFile{SchemaVersion: execution.SchemaVersion, Backends: []BackendDefinition{{ID: "legacy", Kind: execution.LegacyHermes,
		Models: []execution.Model{{ID: testModel, Label: "Mock model"}}, Hermes: &HermesDefinition{Command: command, Dir: directory, Profile: "private-fixed-profile", Toolsets: []string{"web"}, MaxTurns: 3}}}})
	if err != nil {
		t.Fatal(err)
	}
	options, err := loadBackendFile(t, string(data))
	if err != nil {
		t.Fatal(err)
	}
	engine, err := execution.Open(filepath.Join(directory, "execution.json"), options)
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig("legacy", true)
	snapshot := publish(t, engine, config, "initial")
	control, err := gateway.New(gateway.Options{Execution: engine})
	if err != nil {
		t.Fatal(err)
	}
	handler := control.Handler()
	for _, path := range []string{"/backends", "/execution/snapshot", "/groups/" + testGroup + "/routing", "/groups/" + testGroup + "/permissions"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8090/console/v1"+path, nil))
		if response.Code != http.StatusOK {
			t.Fatal(path, response.Code, response.Body.String())
		}
		for _, private := range []string{command, directory, "private-fixed-profile", `"command"`, `"dir"`, `"hermes"`, `"toolsets"`, `"maxTurns"`} {
			if strings.Contains(response.Body.String(), private) {
				t.Fatal("operator Hermes configuration exposed", path, private)
			}
		}
	}
	input, err := json.Marshal(gateway.ExecutionApplyInput{ExpectedRevision: snapshot.Revision, RequestID: "injected", Config: config})
	if err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{
		strings.Replace(string(input), `"config":`, `"hermes":{"command":"/bin/sh","env":["PRIVATE=value"]},"config":`, 1),
		strings.Replace(string(input), `"config":{`, `"config":{"hermes":{"command":"/bin/sh"},`, 1),
		strings.Replace(string(input), `"backendId":"legacy"`, `"backendId":"legacy","hermes":{"command":"/bin/sh"}`, 1),
	} {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8090/console/v1/execution/apply", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatal("HTTP accepted operator-only configuration", response.Code, response.Body.String())
		}
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8090/console/v1/backends", strings.NewReader(string(data)))
	request.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusMethodNotAllowed {
		t.Fatal("backend catalog is writable", response.Code)
	}
	current, err := engine.Snapshot(context.Background())
	if err != nil || current.Revision != snapshot.Revision {
		t.Fatal("HTTP injection changed execution configuration", current, err)
	}
	if _, err := os.Stat(filepath.Join(directory, "started")); !os.IsNotExist(err) {
		t.Fatal("load or HTTP request executed operator command", err)
	}
}
