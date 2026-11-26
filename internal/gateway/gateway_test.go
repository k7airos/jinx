package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	gotesting "testing"
	"time"

	"jinx/internal/console"
	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	"jinx/internal/execution"
	"jinx/internal/observability"
	"jinx/internal/supervisor"
)

func invoke(handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "http://127.0.0.1:8090"+path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	writer := httptest.NewRecorder()
	handler.ServeHTTP(writer, request)
	return writer
}

func valueOf[T any](testing *gotesting.T, response *httptest.ResponseRecorder) T {
	testing.Helper()
	var envelope struct {
		OK    bool `json:"ok"`
		Value T    `json:"value"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &envelope) != nil || !envelope.OK {
		testing.Fatalf("unexpected response %d: %s", response.Code, response.Body)
	}
	return envelope.Value
}

func encoded(value any) string {
	data, _ := json.Marshal(value)
	return string(data)
}

type mockProvider struct {
	calls int
}

func (provider *mockProvider) Generate(context.Context, execution.ModelRequest) (string, error) {
	provider.calls++
	return "unused", nil
}

func executionFixture(testing *gotesting.T) (*execution.Engine, *mockProvider, execution.Config) {
	testing.Helper()
	provider := &mockProvider{}
	engine, err := execution.Open(filepath.Join(testing.TempDir(), "execution.json"), execution.Options{Backends: []execution.BackendOptions{{ID: "text", Kind: execution.TextOnly, Models: []execution.Model{{ID: "model", Label: "Model"}}, Provider: provider}}})
	if err != nil {
		testing.Fatal(err)
	}
	capabilities := map[execution.Capability]bool{}
	for _, capability := range execution.Capabilities() {
		capabilities[capability] = capability == execution.TextGeneration
	}
	config := execution.Config{SchemaVersion: execution.SchemaVersion, Groups: map[string]execution.Group{"1@chatroom": {RouteID: "route", PolicyID: "policy", Primary: execution.RouteAttempt{BackendID: "text", ModelID: "model", TimeoutSec: 10, Session: execution.SessionPolicy{Mode: "independent_run"}}, Fallbacks: []execution.RouteAttempt{}}}, Profiles: map[string]string{}, Policies: map[string]execution.Policy{"policy": {Capabilities: capabilities, NoTools: true}}}
	return engine, provider, config
}

func TestExecutionRoutesCASAndNoGeneration(testing *gotesting.T) {
	engine, provider, config := executionFixture(testing)
	control, err := New(Options{Execution: engine})
	if err != nil {
		testing.Fatal(err)
	}
	handler := control.Handler()
	snapshot := valueOf[execution.Snapshot](testing, invoke(handler, "GET", "/console/v1/execution/snapshot", ""))
	plan := valueOf[execution.ChangePlan](testing, invoke(handler, "POST", "/console/v1/execution/plan", encoded(ExecutionPlanInput{ExpectedRevision: snapshot.Revision, Config: config})))
	if len(plan.AffectedGroups) != 1 {
		testing.Fatal(plan)
	}
	input := ExecutionApplyInput{ExpectedRevision: snapshot.Revision, RequestID: "apply-1", Config: config}
	result := valueOf[execution.ApplyResult](testing, invoke(handler, "POST", "/console/v1/execution/apply", encoded(input)))
	if result.Snapshot.Revision == snapshot.Revision {
		testing.Fatal("revision did not change")
	}
	replayed := valueOf[execution.ApplyResult](testing, invoke(handler, "POST", "/console/v1/execution/apply", encoded(input)))
	if !replayed.Replayed {
		testing.Fatal("apply did not replay")
	}
	input.RequestID = "apply-2"
	if response := invoke(handler, "POST", "/console/v1/execution/apply", encoded(input)); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), result.Snapshot.Revision) {
		testing.Fatalf("missing conflict: %d %s", response.Code, response.Body)
	}
	routing := valueOf[struct {
		Routing execution.Group `json:"routing"`
	}](testing, invoke(handler, "GET", "/console/v1/groups/1@chatroom/routing", ""))
	if routing.Routing.Primary.BackendID != "text" {
		testing.Fatal(routing)
	}
	permissions := valueOf[struct {
		Permissions []execution.PermissionView `json:"permissions"`
	}](testing, invoke(handler, "GET", "/console/v1/groups/1@chatroom/permissions", ""))
	if len(permissions.Permissions) != len(execution.Capabilities()) || permissions.Permissions[2].Enforcement != execution.Hard {
		testing.Fatal(permissions)
	}
	catalog := valueOf[[]execution.Backend](testing, invoke(handler, "GET", "/console/v1/backends", ""))
	if len(catalog) != 1 || !catalog[0].Available || provider.calls != 0 {
		testing.Fatalf("unexpected generation or catalog: %v %d", catalog, provider.calls)
	}
	if response := invoke(handler, "GET", "/console/v1/groups/missing/routing", ""); response.Code != http.StatusNotFound {
		testing.Fatal(response.Code)
	}
}

func TestSecurityAndStrictJSON(testing *gotesting.T) {
	engine, _, _ := executionFixture(testing)
	control, _ := New(Options{Execution: engine})
	handler := control.Handler()
	for _, test := range []struct {
		name   string
		host   string
		origin string
		site   string
	}{
		{name: "remote host", host: "attacker.example:8090"},
		{name: "missing port", host: "127.0.0.1"},
		{name: "foreign origin", origin: "http://evil.example"},
		{name: "different loopback origin", origin: "http://localhost:8090"},
		{name: "same site is not same origin", site: "same-site"},
		{name: "cross site", site: "cross-site"},
	} {
		testing.Run(test.name, func(testing *gotesting.T) {
			request := httptest.NewRequest("GET", "http://127.0.0.1:8090/console/v1/backends", nil)
			if test.host != "" {
				request.Host = test.host
			}
			request.Header.Set("Origin", test.origin)
			request.Header.Set("Sec-Fetch-Site", test.site)
			writer := httptest.NewRecorder()
			handler.ServeHTTP(writer, request)
			if writer.Code != http.StatusForbidden {
				testing.Fatal(writer.Code)
			}
		})
	}
	for _, body := range []string{`null`, `[]`, `{} {}`, `{"env":[]}`, `{"url":"http://evil.example"}`, `{"cmd":"launchctl"}`, `{"config":{"schemaVersion":"1","groups":{},"profiles":{},"policies":{},"env":[]}}`, `{"requestId":"a","requestId":"b"}`, `{"config":{"schemaVersion":"1","SchemaVersion":"2"}}`} {
		if response := invoke(handler, "POST", "/console/v1/execution/apply", body); response.Code != http.StatusBadRequest {
			testing.Fatalf("accepted invalid body %s: %d", body, response.Code)
		}
	}
	if response := invoke(handler, "POST", "/console/v1/execution/apply", `{"unknown":"`+strings.Repeat("x", maxBody)+`"}`); response.Code != http.StatusRequestEntityTooLarge {
		testing.Fatal(response.Code)
	}
	request := httptest.NewRequest("POST", "http://127.0.0.1:8090/console/v1/execution/plan", strings.NewReader(`{}`))
	writer := httptest.NewRecorder()
	handler.ServeHTTP(writer, request)
	if writer.Code != http.StatusUnsupportedMediaType {
		testing.Fatal(writer.Code)
	}
	if response := invoke(handler, "POST", "/console/v1/backends", `{}`); response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET" {
		testing.Fatal(response.Code)
	}
	if response := invoke(handler, "GET", "/console/v1/backends?url=http://evil.example", ""); response.Code != http.StatusBadRequest {
		testing.Fatal(response.Code)
	}
	if response := invoke(handler, "GET", "/console/v1/backends", ""); strings.Contains(response.Body.String(), "/private/") {
		testing.Fatal("path in catalog")
	}
}

func readyControl() dispatcher.ControlSnapshot {
	now := time.Now().UTC()
	return dispatcher.ControlSnapshot{Revision: "5", Global: dispatcher.ScopeControl{Revision: "5", Boundary: 1, ChangedAt: now}, Groups: map[string]dispatcher.ScopeControl{}, Maintenance: &dispatcher.MaintenanceView{Maintenance: dispatcher.Maintenance{ID: "maintenance-1", Revision: "5", Scope: dispatcher.ControlScope{Kind: "global"}, Boundary: 1, StartedAt: now, Active: true, Deadline: now.Add(time.Minute)}, Stage: "ready", Counts: dispatcher.ControlCounts{Phases: map[string]int{}}, Blockers: []string{}}}
}

func TestDrainRequiresFullSettledBoundary(testing *gotesting.T) {
	for _, test := range []struct {
		name   string
		change func(*dispatcher.ControlSnapshot)
		state  string
	}{
		{name: "settled", state: "ready", change: func(*dispatcher.ControlSnapshot) {}},
		{name: "pending stats alone do not authorize", state: "draining", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Stage = "draining" }},
		{name: "unknown delivery", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Counts.UnknownDelivery = 1 }},
		{name: "remote unsettled", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Counts.RemoteUnsettled = 1 }},
		{name: "worker", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Counts.ActiveWorkers = 1 }},
		{name: "unresolved run", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Counts.UnresolvedRuns = 1 }},
		{name: "contradictory phases", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Counts.Phases["executing"] = 1 }},
		{name: "terminal history allowed", state: "ready", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Counts.Phases["terminal"] = 10 }},
		{name: "pending", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Counts.Pending = 1 }},
		{name: "blocker", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) {
			snapshot.Maintenance.Blockers = []string{"restart_recovery"}
		}},
		{name: "null blockers", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Blockers = nil }},
		{name: "remote blocker", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) {
			snapshot.Maintenance.Blockers = []string{"remote_unsettled"}
		}},
		{name: "not global", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) {
			snapshot.Maintenance.Scope = dispatcher.ControlScope{Kind: "group", GroupID: "group"}
		}},
		{name: "paused group", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) {
			group := snapshot.Global
			group.Paused = true
			snapshot.Groups["group"] = group
		}},
		{name: "recovery", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.RecoveryRequired = true }},
		{name: "negative count", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Counts.Pending = -1 }},
		{name: "expired", state: "blocked", change: func(snapshot *dispatcher.ControlSnapshot) {
			snapshot.Maintenance.Deadline = time.Now().Add(-time.Second)
		}},
		{name: "canceled", state: "canceled", change: func(snapshot *dispatcher.ControlSnapshot) { snapshot.Maintenance.Active = false }},
	} {
		testing.Run(test.name, func(testing *gotesting.T) {
			snapshot := readyControl()
			test.change(&snapshot)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/api/control/snapshot" {
					testing.Errorf("unexpected endpoint: %s", request.URL)
				}
				_ = json.NewEncoder(writer).Encode(snapshot)
			}))
			defer server.Close()
			coordinator, _ := NewDispatcherDrainCoordinator(server.URL, nil)
			status, err := coordinator.Poll(context.Background(), "maintenance-1")
			if test.name == "null blockers" || test.name == "negative count" {
				requireUpstreamInvalid(testing, err)
				return
			}
			if err != nil || status.State != test.state {
				testing.Fatalf("%+v %v", status, err)
			}
			if test.name == "unknown delivery" && !status.UnknownDelivery || (test.name == "remote unsettled" || test.name == "remote blocker") && status.RemoteSettled {
				testing.Fatal(status)
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(writer, `{"revision":"5","groups":{},"maintenance":{"maintenanceId":"maintenance-1","revision":"5","scope":{"kind":"global"},"active":true,"stage":"ready","counts":{"phases":{}},"blockers":[]}}`)
	}))
	defer server.Close()
	coordinator, _ := NewDispatcherDrainCoordinator(server.URL, nil)
	if _, err := coordinator.Poll(context.Background(), "maintenance-1"); err == nil {
		testing.Fatal("missing counts accepted")
	}
}

func TestDispatcherMappingStatusesAndNoRawProxy(testing *gotesting.T) {
	var requests []dispatcher.ControlRequest
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		switch request.URL.Path {
		case "/api/control/snapshot":
			_ = json.NewEncoder(writer).Encode(readyControl())
		case "/api/config":
			_, _ = io.WriteString(writer, `{"rules":{"laya_filter":false},"secret":"must-not-leak"}`)
		case "/api/trace/snapshot":
			_ = json.NewEncoder(writer).Encode(observability.Snapshot{StreamID: "stream", Runs: []contracts.Run{}})
		case "/api/control/action":
			var input dispatcher.ControlRequest
			_ = json.NewDecoder(request.Body).Decode(&input)
			requests = append(requests, input)
			_ = json.NewEncoder(writer).Encode(dispatcher.ControlOperation{OperationID: "operation-1", RequestID: input.RequestID, Revision: "6", Status: "succeeded", MaintenanceID: "maintenance-1", Action: input.Action})
		case "/api/control/operations/operation-1":
			_ = json.NewEncoder(writer).Encode(dispatcher.ControlOperation{OperationID: "operation-1"})
		default:
			writer.WriteHeader(http.StatusTeapot)
			_, _ = io.WriteString(writer, `{"detail":"/private/secret","url":"token"}`)
		}
	}))
	defer server.Close()
	coordinator, _ := NewDispatcherDrainCoordinator(server.URL, nil)
	snapshot, err := coordinator.Snapshot(context.Background())
	if err != nil || snapshot.Version != "5" || snapshot.DependencyVersion == "" || snapshot.DecisionRequired || snapshot.UnknownDelivery {
		testing.Fatalf("%+v %v", snapshot, err)
	}
	id, err := coordinator.Drain(context.Background(), supervisor.Global)
	if err != nil || id != "maintenance-1" {
		testing.Fatal(id, err)
	}
	if err := coordinator.Cancel(context.Background(), id); err != nil {
		testing.Fatal(err)
	}
	if err := coordinator.Continue(context.Background(), id); err != nil {
		testing.Fatal(err)
	}
	for index, kind := range []string{"drain", "cancel_maintenance", "continue_maintenance"} {
		input := requests[index]
		if input.ExpectedRevision != "5" || input.RequestID == "" || input.Action.Kind != kind || input.Action.Scope.Kind != "global" || input.Action.Scope.GroupID != "" {
			testing.Fatal(input)
		}
	}
	control, _ := New(Options{DispatcherURL: server.URL})
	handler := control.Handler()
	valueOf[dispatcher.ControlSnapshot](testing, invoke(handler, "GET", "/console/v1/controls", ""))
	valueOf[dispatcher.ControlOperation](testing, invoke(handler, "POST", "/console/v1/controls/actions", encoded(dispatcher.ControlRequest{RequestID: "user-1", ExpectedRevision: "5", Action: dispatcher.ControlAction{Kind: "pause", Scope: dispatcher.ControlScope{Kind: "global"}}})))
	valueOf[dispatcher.ControlOperation](testing, invoke(handler, "GET", "/console/v1/controls/operations/operation-1", ""))
	response := invoke(handler, "GET", "/console/v1/controls/operations/missing", "")
	if response.Code != http.StatusTeapot || strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "token") {
		testing.Fatalf("%d %s", response.Code, response.Body)
	}
	before := len(paths)
	for _, path := range []string{"/console/v1/controls?url=http://evil.example", "/console/v1/controls/operations/../config", "/console/v1/proxy?url=http://evil.example"} {
		invoke(handler, "GET", path, "")
	}
	if len(paths) != before {
		testing.Fatal("raw proxy was reachable")
	}
	server.Close()
	if coordinator.Continue(context.Background(), id) == nil || coordinator.Cancel(context.Background(), id) == nil {
		testing.Fatal("unreachable Dispatcher claimed maintenance resumed")
	}
}

func TestRedirectsAndInvalidUpstreams(testing *gotesting.T) {
	for _, base := range []string{"https://127.0.0.1:1234", "http://example.com:1234", "http://127.0.0.1:1234/path", "http://user:secret@127.0.0.1:1234", "http://127.0.0.1:1234?", "http://127.0.0.1:0"} {
		if _, err := NewDispatcherDrainCoordinator(base, nil); err == nil {
			testing.Fatal(base)
		}
	}
	calls := 0
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { calls++ }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	control, _ := New(Options{DispatcherURL: redirect.URL, HTTPClient: &http.Client{}})
	response := invoke(control.Handler(), "GET", "/console/v1/controls", "")
	if response.Code != http.StatusTemporaryRedirect || calls != 0 || response.Header().Get("Location") != "" {
		testing.Fatalf("redirect followed: %d %d", response.Code, calls)
	}
}

type fakeCommander struct {
	mu      sync.Mutex
	running bool
	writes  int
}

func (commander *fakeCommander) Run(_ context.Context, executable string, arguments ...string) (string, string, int, error) {
	commander.mu.Lock()
	defer commander.mu.Unlock()
	if executable != "/bin/launchctl" {
		return "", "", -1, fmt.Errorf("unexpected command")
	}
	if arguments[0] == "print" {
		if strings.HasSuffix(arguments[1], "io.jinx.relay") && !commander.running {
			return "", `Could not find service "io.jinx.relay"`, 113, nil
		}
		return arguments[1] + " = {\n state = running\n pid = 123\n runs = 1\n}\n", "", 0, nil
	}
	commander.writes++
	commander.running = false
	return "", "", 0, nil
}

func invokeIdleMaintenance(testing *gotesting.T, handler http.Handler, operation supervisor.Operation, action string) *httptest.ResponseRecorder {
	testing.Helper()
	eligible := action == "cancel" && (operation.State == "succeeded" || operation.State == "failed")
	if operation.State == "blocked" && (action == "continue" || action == "cancel") {
		for _, step := range operation.Steps {
			eligible = eligible || step.Name == "command" && step.State == "pending"
		}
	}
	if !eligible {
		testing.Fatalf("operation is not eligible for maintenance %s: %+v", action, operation)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		response := invoke(handler, "POST", "/console/v1/services/maintenance/"+operation.ID+"/"+action, `{}`)
		var envelope struct {
			Problem Problem `json:"problem"`
		}
		if response.Code != http.StatusConflict || json.Unmarshal(response.Body.Bytes(), &envelope) != nil || envelope.Problem.Code != "operation_active" {
			return response
		}
		current := valueOf[supervisor.Operation](testing, invoke(handler, "GET", "/console/v1/services/operations/"+operation.ID, ""))
		if current.State != operation.State || current.Revision != operation.Revision {
			testing.Fatalf("active worker conflict changed operation: before=%+v after=%+v", operation, current)
		}
		time.Sleep(time.Millisecond)
	}
	testing.Fatalf("worker did not release maintenance %s for operation: %+v", action, operation)
	return nil
}

func TestServiceRoutesUseManagerAndRealHTTPCoordinator(testing *gotesting.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/control/snapshot":
			_ = json.NewEncoder(writer).Encode(readyControl())
		case "/api/config":
			_, _ = io.WriteString(writer, `{"rules":{"laya_filter":false}}`)
		case "/api/trace/snapshot":
			_ = json.NewEncoder(writer).Encode(observability.Snapshot{StreamID: "stream", Runs: []contracts.Run{}})
		case "/api/control/action":
			var input dispatcher.ControlRequest
			_ = json.NewDecoder(request.Body).Decode(&input)
			_ = json.NewEncoder(writer).Encode(dispatcher.ControlOperation{OperationID: "operation-1", RequestID: input.RequestID, Revision: "6", Status: "succeeded", MaintenanceID: "maintenance-1", Action: input.Action})
		default:
			testing.Errorf("unexpected endpoint %s", request.URL.Path)
		}
	}))
	defer server.Close()
	coordinator, _ := NewDispatcherDrainCoordinator(server.URL, nil)
	commander := &fakeCommander{running: true}
	uid := uint32(501)
	manager, err := supervisor.New(supervisor.Options{UID: &uid, PlistDir: testing.TempDir(), DataPath: filepath.Join(testing.TempDir(), "supervisor.json"), EnableControl: true, Commander: commander, DrainCoordinator: coordinator, PollInterval: time.Millisecond})
	if err != nil {
		testing.Fatal(err)
	}
	defer manager.Close()
	control, _ := New(Options{Supervisor: manager})
	handler := control.Handler()
	list := valueOf[supervisor.ServiceList](testing, invoke(handler, "GET", "/console/v1/services/runtime", ""))
	plan := valueOf[supervisor.ActionPlan](testing, invoke(handler, "POST", "/console/v1/services/plan", encoded(ServicePlanInput{ExpectedRevision: list.Revision, Kind: supervisor.Stop, ServiceID: supervisor.Relay})))
	if !plan.Eligible || !plan.DrainRequired || commander.writes != 0 {
		testing.Fatalf("plan has effects or is blocked: %+v", plan)
	}
	operation := valueOf[supervisor.Operation](testing, invoke(handler, "POST", "/console/v1/services/execute", encoded(supervisor.ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "stop-1"})))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		operation = valueOf[supervisor.Operation](testing, invoke(handler, "GET", "/console/v1/services/operations/"+operation.ID, ""))
		if operation.State == "succeeded" || operation.State == "blocked" || operation.State == "unknown" || operation.State == "failed" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	commander.mu.Lock()
	writes := commander.writes
	commander.mu.Unlock()
	if operation.State != "succeeded" || writes != 1 || operation.Drain == nil || operation.Drain.State != "ready" {
		testing.Fatalf("%+v writes=%d", operation, writes)
	}
	for _, action := range []string{"continue", "cancel"} {
		var response *httptest.ResponseRecorder
		if action == "cancel" {
			response = invokeIdleMaintenance(testing, handler, operation, action)
		} else {
			response = invoke(handler, "POST", "/console/v1/services/maintenance/"+operation.ID+"/"+action, `{}`)
		}
		if action == "continue" && response.Code != http.StatusConflict || action == "cancel" && response.Code != http.StatusOK {
			testing.Fatalf("unexpected completed operation maintenance: %d %s", response.Code, response.Body)
		}
		response = invoke(handler, "POST", "/console/v1/services/maintenance/missing/"+action, `{}`)
		if response.Code != http.StatusNotFound {
			testing.Fatal(response.Code)
		}
	}
	server.Close()
	uncertain := valueOf[supervisor.Operation](testing, invoke(handler, "POST", "/console/v1/services/maintenance/"+operation.ID+"/cancel", `{}`))
	if uncertain.State != "unknown" || uncertain.Problem == nil || uncertain.Problem.Code != "drain_unknown" {
		testing.Fatalf("unreachable Dispatcher claimed cleanup succeeded: %+v", uncertain)
	}
}

func TestServiceMaintenanceContinueAndCancelReachDispatcher(testing *gotesting.T) {
	var mutex sync.Mutex
	stage := "blocked"
	var actions []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		switch request.URL.Path {
		case "/api/control/snapshot":
			snapshot := readyControl()
			snapshot.Maintenance.Stage = stage
			if stage == "blocked" {
				snapshot.Maintenance.Blockers = []string{"restart_recovery"}
				snapshot.Maintenance.RecoveryRequired = true
			}
			_ = json.NewEncoder(writer).Encode(snapshot)
		case "/api/config":
			_, _ = io.WriteString(writer, `{"rules":{"laya_filter":false}}`)
		case "/api/trace/snapshot":
			_ = json.NewEncoder(writer).Encode(observability.Snapshot{StreamID: "stream", Runs: []contracts.Run{}})
		case "/api/control/action":
			var input dispatcher.ControlRequest
			_ = json.NewDecoder(request.Body).Decode(&input)
			actions = append(actions, input.Action.Kind)
			if input.Action.Kind == "continue_maintenance" {
				stage = "ready"
			}
			_ = json.NewEncoder(writer).Encode(dispatcher.ControlOperation{OperationID: "operation-1", RequestID: input.RequestID, Revision: "6", Status: "succeeded", MaintenanceID: "maintenance-1", Action: input.Action})
		default:
			testing.Errorf("unexpected endpoint: %s", request.URL.Path)
		}
	}))
	defer server.Close()
	coordinator, _ := NewDispatcherDrainCoordinator(server.URL, nil)
	commander := &fakeCommander{running: true}
	uid := uint32(501)
	manager, err := supervisor.New(supervisor.Options{UID: &uid, PlistDir: testing.TempDir(), DataPath: filepath.Join(testing.TempDir(), "supervisor.json"), EnableControl: true, Commander: commander, DrainCoordinator: coordinator, PollInterval: time.Millisecond})
	if err != nil {
		testing.Fatal(err)
	}
	defer manager.Close()
	control, _ := New(Options{Supervisor: manager})
	handler := control.Handler()
	list := valueOf[supervisor.ServiceList](testing, invoke(handler, "GET", "/console/v1/services/runtime", ""))
	plan := valueOf[supervisor.ActionPlan](testing, invoke(handler, "POST", "/console/v1/services/plan", encoded(ServicePlanInput{ExpectedRevision: list.Revision, Kind: supervisor.Stop, ServiceID: supervisor.Relay})))
	input := supervisor.ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "blocked-stop"}
	operation := valueOf[supervisor.Operation](testing, invoke(handler, "POST", "/console/v1/services/execute", encoded(input)))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		operation = valueOf[supervisor.Operation](testing, invoke(handler, "GET", "/console/v1/services/operations/"+operation.ID, ""))
		if operation.State == "blocked" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if operation.State != "blocked" || operation.MaintenanceID == "" {
		testing.Fatalf("did not block at maintenance: %+v", operation)
	}
	operation = valueOf[supervisor.Operation](testing, invokeIdleMaintenance(testing, handler, operation, "continue"))
	if operation.Drain == nil || operation.Drain.State != "ready" {
		testing.Fatalf("continue did not poll ready boundary: %+v", operation)
	}
	commander.mu.Lock()
	writes := commander.writes
	commander.mu.Unlock()
	if writes != 0 {
		testing.Fatal("continue executed a command without explicit execute")
	}
	operation = valueOf[supervisor.Operation](testing, invoke(handler, "POST", "/console/v1/services/maintenance/"+operation.ID+"/cancel", `{}`))
	if operation.State != "canceled" {
		testing.Fatal(operation)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if strings.Join(actions, ",") != "drain,continue_maintenance,cancel_maintenance" {
		testing.Fatal(actions)
	}
}

func TestFacadeDelegationAndBootstrapCapabilityAugmentation(testing *gotesting.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/control/snapshot":
			_ = json.NewEncoder(writer).Encode(readyControl())
		case "/health":
			_, _ = io.WriteString(writer, `{"status":"ready","protocol_version":1}`)
		case "/api/stats":
			_, _ = io.WriteString(writer, `{"pending_messages":0}`)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	facade, err := console.New(console.Options{DispatcherURL: server.URL, StateDir: testing.TempDir()})
	if err != nil {
		testing.Fatal(err)
	}
	defer facade.Close()
	engine, _, _ := executionFixture(testing)
	control, _ := New(Options{Console: facade, Execution: engine, DispatcherURL: server.URL})
	for _, path := range []string{"/console/v1/services", "/console/v1/rules/global", "/console/v1/changes", "/console/v1/runs"} {
		expected := invoke(facade.Handler(), "GET", path, "")
		actual := invoke(control.Handler(), "GET", path, "")
		if actual.Code != expected.Code {
			testing.Fatalf("delegation changed %s: %d != %d", path, actual.Code, expected.Code)
		}
	}
	bootstrap := valueOf[console.Bootstrap](testing, invoke(control.Handler(), "GET", "/console/v1/bootstrap", ""))
	for _, name := range []string{"pause", "resume", "maintenance", "cancel", "routing.edit", "permissions.edit"} {
		if !bootstrap.Capabilities[name].Supported {
			testing.Fatal(name)
		}
	}
	for _, name := range []string{"traceRuns", "resumableRunEvents", "relay.rules", "service.actions", "execution.dispatch"} {
		if bootstrap.Capabilities[name].Supported {
			testing.Fatalf("unsupported capability advertised: %s", name)
		}
	}
}
