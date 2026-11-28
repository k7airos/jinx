package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"jinx/internal/connector"
	"jinx/internal/console"
	"jinx/internal/contracts"
	"jinx/internal/execution"
	"jinx/internal/runtimebridge"
)

type mainConnectorStub struct{}

func (m *mainConnectorStub) Platform() contracts.Platform { return contracts.PlatformBilibili }
func (m *mainConnectorStub) AccountID() string            { return "bilibili-ego" }
func (m *mainConnectorStub) ConnectorName() string        { return "bilibili-ego-connector" }
func (m *mainConnectorStub) Capabilities() []contracts.ActionKind {
	return []contracts.ActionKind{contracts.ActionReplyText, contracts.ActionThreadReply}
}
func (m *mainConnectorStub) Health(context.Context) (bool, string) { return true, "" }
func (m *mainConnectorStub) Status() connector.StatusSnapshot {
	return connector.StatusSnapshot{
		Targets: []connector.HealthTarget{
			{ContainerID: "bilibili:BV1example0001", Label: "Demo", Enabled: true},
		},
	}
}
func (m *mainConnectorStub) Execute(_ context.Context, req connector.ActionRequest) (connector.ActionResponse, error) {
	return connector.ActionResponse{ActionID: req.ActionID, Status: "confirmed", ExternalID: "rpid-1"}, nil
}

func TestAssembleInjectsSharedArchitectureCatalog(t *testing.T) {
	var providerCalls, healthReads atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		providerCalls.Add(1)
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer provider.Close()
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			t.Errorf("unexpected write: %s %s", request.Method, request.URL.Path)
		}
		if request.URL.Path == "/health" {
			healthReads.Add(1)
			_, _ = io.WriteString(writer, `{"status":"ready","protocol_version":1,"group_routing":true,"serial_ui":true,"accessibility_trusted":true,"screen_capture_authorized":true}`)
			return
		}
		if request.URL.Path != "/api/trace/snapshot" && request.URL.Path != "/api/v1/connectors" {
			t.Errorf("unexpected upstream read: %s", request.URL.Path)
		}
		writer.WriteHeader(http.StatusNotFound)
	}))
	defer upstream.Close()
	directory := t.TempDir()
	config, err := parseSettings(append(configurationArguments(directory), "-dispatcher", upstream.URL, "-relay", upstream.URL, "-bridge", upstream.URL), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	definitions := runtimebridge.BackendFile{SchemaVersion: "1", Backends: []runtimebridge.BackendDefinition{
		{ID: "personal", Kind: execution.AlmaPersonal, Models: []execution.Model{{ID: "provider:family/model@revision+v1", Label: "/private/SECRET"}}, AlmaURL: provider.URL},
		{ID: "text", Kind: execution.TextOnly, Models: []execution.Model{{ID: "family/model@revision+v1", Label: "SECRET"}}, AlmaURL: provider.URL, ProviderID: "provider"},
	}}
	data, err := json.Marshal(definitions)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config.backendDefinitions, data, 0600); err != nil {
		t.Fatal(err)
	}
	handler, cleanup, err := assemble(config)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if healthReads.Load() != 0 || providerCalls.Load() != 0 {
		t.Fatal("assembly contacted an upstream")
	}
	read := func(path string, target any) string {
		t.Helper()
		writer := httptest.NewRecorder()
		handler.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8090/console/v1/"+path, nil))
		if writer.Code != http.StatusOK || json.Unmarshal(writer.Body.Bytes(), target) != nil {
			t.Fatalf("%s: %d %s", path, writer.Code, writer.Body)
		}
		return writer.Body.String()
	}
	var catalog struct {
		OK    bool                `json:"ok"`
		Value []execution.Backend `json:"value"`
	}
	read("backends", &catalog)
	var snapshot struct {
		OK    bool                         `json:"ok"`
		Value console.ArchitectureSnapshot `json:"value"`
	}
	body := read("architecture", &snapshot)
	if !catalog.OK || !snapshot.OK || len(snapshot.Value.Processing.Backends) != len(catalog.Value) {
		t.Fatalf("missing shared catalog: %s", body)
	}
	found := false
	for _, capability := range snapshot.Value.Processing.Capabilities {
		if capability.ID == "backend_catalog" {
			found = capability.Supported && capability.Source == contracts.CapabilityExecution
		}
	}
	if !found || len(snapshot.Value.Processing.Agents) != 0 {
		t.Fatalf("missing catalog capability or invented agents: %s", body)
	}
	for index, backend := range catalog.Value {
		projected := snapshot.Value.Processing.Backends[index]
		if projected.ID != backend.ID || projected.Available != backend.Available || projected.AgentBinding != backend.AgentBinding || !reflect.DeepEqual(projected.ModelIDs, []string{backend.Models[0].ID}) || len(projected.Capabilities) != len(backend.PermissionEvidence) {
			t.Fatalf("catalog projection mismatch: %+v %+v", projected, backend)
		}
		for permissionIndex, permission := range backend.PermissionEvidence {
			capability := projected.Capabilities[permissionIndex]
			supported := backend.Available && permission.Effective != nil && *permission.Effective && permission.Enforcement == execution.Hard
			if capability.ID != string(permission.Capability) || capability.Enforcement != contracts.Enforcement(permission.Enforcement) || capability.Supported != supported || capability.Source != contracts.CapabilityExecution || contracts.ValidateCapabilityEvidence(capability) != nil {
				t.Fatalf("permission mismatch: %+v %+v", capability, permission)
			}
		}
	}
	for _, private := range []string{"SECRET", "/private/", provider.URL, directory, "providerID"} {
		if strings.Contains(body, private) {
			t.Fatalf("private catalog data leaked: %s", body)
		}
	}
	if providerCalls.Load() != 0 || healthReads.Load() != 3 {
		t.Fatalf("unexpected I/O: provider=%d health=%d", providerCalls.Load(), healthReads.Load())
	}
	cleanup()
	handler, reopened, err := assemble(config)
	if err != nil || handler == nil {
		t.Fatalf("cleanup retained journal ownership: %v", err)
	}
	reopened()
}

func TestAssembleFailureReleasesConsoleJournal(t *testing.T) {
	directory := t.TempDir()
	config, err := parseSettings(configurationArguments(directory), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config.backendDefinitions, []byte(`{"schemaVersion":"1","backends":[{"id":"personal","kind":"alma_personal","models":[{"id":"model","label":"Model"}],"almaURL":"http://127.0.0.1:1"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	config.serviceUID = 501
	journal := filepath.Join(directory, "supervisor.json")
	if err := os.WriteFile(journal, []byte(`invalid`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := assemble(config); err == nil || !strings.Contains(err.Error(), "supervisor journal") {
		t.Fatalf("invalid supervisor journal was not the failure: %v", err)
	}
	config.serviceUID = -1
	_, cleanup, err := assemble(config)
	if err != nil {
		t.Fatalf("failed assembly retained console lock: %v", err)
	}
	cleanup()
}

func TestAssembleInvalidExecutionDoesNotOpenConsole(t *testing.T) {
	directory := t.TempDir()
	config, err := parseSettings(configurationArguments(directory), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config.backendDefinitions, []byte(`{"schemaVersion":"1","backends":[{"id":"personal","kind":"alma_personal","models":[{"id":"model","label":"Model"}],"almaURL":"http://127.0.0.1:1"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config.executionConfig, []byte(`invalid`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := assemble(config); err == nil || !strings.Contains(err.Error(), "execution state") {
		t.Fatalf("invalid execution state was not the failure: %v", err)
	}
	if _, err := os.Stat(filepath.Join(directory, "console")); !os.IsNotExist(err) {
		t.Fatalf("Console opened before Engine: %v", err)
	}
}

func TestAssembleWiresConnectorsAndConfirmedDryRunControl(t *testing.T) {
	store, err := connector.OpenIdempotencyStore(filepath.Join(t.TempDir(), "idem.json"))
	if err != nil {
		t.Fatal(err)
	}
	exec := &mainConnectorStub{}
	connSrv := connector.NewServer(connector.ServerOptions{
		Executor: exec,
		Store:    store,
		DryRun:   true,
	})
	connHTTP := httptest.NewServer(connSrv.Handler())
	defer connHTTP.Close()

	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/health":
			_, _ = io.WriteString(writer, `{"status":"ready","protocol_version":1}`)
		case "/api/trace/snapshot":
			writer.WriteHeader(http.StatusNotFound)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer upstream.Close()

	directory := t.TempDir()
	args := append(configurationArguments(directory), "-dispatcher", upstream.URL, "-bilibili-connector", connHTTP.URL)
	config, err := parseSettings(args, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config.backendDefinitions, []byte(`{"schemaVersion":"1","backends":[{"id":"text","kind":"text_only","models":[{"id":"model-1","label":"Model"}],"almaURL":"http://127.0.0.1:1","providerID":"provider"}]}`), 0600); err != nil {
		t.Fatal(err)
	}
	handler, cleanup, err := assemble(config)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8090/console/v1/"+path, nil))
		return rec
	}
	post := func(path string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8090/console/v1/"+path, strings.NewReader(string(raw)))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	bootRec := get("bootstrap")
	if bootRec.Code != http.StatusOK || !strings.Contains(bootRec.Body.String(), `"connectors":{"supported":true,"enabled":true`) {
		t.Fatalf("expected connectors capability in bootstrap: %d %s", bootRec.Code, bootRec.Body.String())
	}
	connRec := get("connectors")
	if connRec.Code != http.StatusOK || !strings.Contains(connRec.Body.String(), `"bilibili:BV1example0001"`) {
		t.Fatalf("expected bilibili connector view: %d %s", connRec.Code, connRec.Body.String())
	}

	planRec := post("connectors/bilibili/dry-run/plan", console.ConnectorDryRunPlanInput{
		RequestID:      "gw-plan-1",
		ExpectedDryRun: true,
		TargetDryRun:   false,
	})
	if planRec.Code != http.StatusOK {
		t.Fatalf("gateway dry-run plan failed: %d %s", planRec.Code, planRec.Body.String())
	}
	var planOut struct {
		Value console.ConnectorDryRunPlan `json:"value"`
	}
	if err := json.Unmarshal(planRec.Body.Bytes(), &planOut); err != nil {
		t.Fatal(err)
	}
	applyRec := post("connectors/bilibili/dry-run/apply", console.ConnectorDryRunApplyInput{
		RequestID: "gw-apply-1",
		PlanID:    planOut.Value.ID,
		Token:     planOut.Value.Token,
		Confirm:   true,
	})
	if applyRec.Code != http.StatusOK || !strings.Contains(applyRec.Body.String(), `"dryRun":false`) {
		t.Fatalf("gateway dry-run apply failed: %d %s", applyRec.Code, applyRec.Body.String())
	}

	archRec := get("architecture")
	var archOut struct {
		OK    bool                         `json:"ok"`
		Value console.ArchitectureSnapshot `json:"value"`
	}
	if err := json.Unmarshal(archRec.Body.Bytes(), &archOut); err != nil || !archOut.OK {
		t.Fatalf("architecture read failed: %d %s", archRec.Code, archRec.Body.String())
	}
	if archOut.Value.Sources[1].State != "ready" || !archOut.Value.Sources[1].Availability.Enabled || archOut.Value.Actions[4].State != "ready" || !archOut.Value.Actions[4].Availability.Enabled {
		t.Fatalf("expected live bilibili source & action ready after dry_run=false: source=%+v action=%+v", archOut.Value.Sources[1], archOut.Value.Actions[4])
	}
}
