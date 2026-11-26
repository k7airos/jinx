package console

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type dispatcherStub struct {
	mu       sync.Mutex
	root     map[string]any
	revision int
	writes   int
	paths    []string
	mode     string
	patch    map[string]any
}

func fixture() map[string]any {
	var root map[string]any
	_ = json.Unmarshal([]byte(`{
		"server":{"listen_addr":"127.0.0.1:8088"},
		"relay":{"stream_url":"http://127.0.0.1:8080/events"},
		"hermes":{"profile":"isolated","bin_path":"/private/SECRET"},
		"bridge":{"dry_run":true,"url":"http://127.0.0.1:5033"},
		"rules":{"batch_size":8,"idle_timeout_sec":5,"max_batch_wait_sec":15,
			"max_message_age_sec":180,"session_max_turns":0,"session_max_age_sec":0,
			"laya_filter":false,"bypass_decision_on_mention":true,"ignore_xml":false,
			"bot_names":["bot"],"self_sender_ids":["global-self"],"keyword_blocklist":[],
			"group_whitelist":["1@chatroom","2@chatroom"],
			"group_profiles":{},"group_other_bots":{},"default_profile":"default",
			"group_rules":{
				"1@chatroom":{"enabled":true,"target":"one","target_group_id":"","hermes_profile":"","self_sender_ids":null,"allowed_types":null},
				"2@chatroom":{"enabled":true,"target":"two","target_group_id":"","hermes_profile":"","self_sender_ids":[],"allowed_types":[],"future_field":"preserve"}
			}
		}
	}`), &root)
	return root
}

func (stub *dispatcherStub) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.paths = append(stub.paths, request.Method+" "+request.URL.Path)
	writer.Header().Set("Content-Type", "application/json")
	switch request.URL.Path {
	case "/health":
		_ = json.NewEncoder(writer).Encode(map[string]any{"status": "ready", "protocol_version": "1", "secret": "/private/SECRET", "relay_connected": true})
	case "/api/stats":
		_ = json.NewEncoder(writer).Encode(map[string]any{"total_received": 10, "pending_messages": 2, "relay_connected": true, "message": "SECRET"})
	case "/api/config":
		if request.Method == http.MethodPost {
			stub.writes++
			if request.Header.Get("If-Match") != fmt.Sprintf(`"v%d"`, stub.revision) || request.Header.Get("Content-Type") != "application/json" {
				writer.WriteHeader(409)
				return
			}
			if stub.mode == "reject" {
				writer.WriteHeader(400)
				return
			}
			if stub.mode == "race" {
				stub.revision++
				writer.WriteHeader(409)
				return
			}
			stub.patch = map[string]any{}
			_ = json.NewDecoder(request.Body).Decode(&stub.patch)
			for key, value := range stub.patch {
				if key == "dry_run" {
					stub.root["bridge"].(map[string]any)[key] = value
				} else {
					stub.root["rules"].(map[string]any)[key] = value
				}
			}
			stub.revision++
			if stub.mode == "applied500" {
				writer.WriteHeader(500)
				return
			}
			if stub.mode == "lostreadback" {
				writer.WriteHeader(200)
				return
			}
		}
		if stub.mode == "lostreadback" && stub.writes > 0 {
			writer.WriteHeader(503)
			return
		}
		writer.Header().Set("ETag", fmt.Sprintf(`"v%d"`, stub.revision))
		_ = json.NewEncoder(writer).Encode(stub.root)
	default:
		writer.WriteHeader(404)
	}
}

func setup(t *testing.T) (*Facade, *dispatcherStub, Options) {
	t.Helper()
	stub := &dispatcherStub{root: fixture(), revision: 1}
	server := httptest.NewServer(stub)
	t.Cleanup(server.Close)
	options := Options{DispatcherURL: server.URL, RelayURL: server.URL, DecisionURL: server.URL, BridgeURL: server.URL, StateDir: t.TempDir(), Timeout: time.Second}
	facade, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = facade.Close() })
	return facade, stub, options
}

func planEdit(t *testing.T, facade *Facade, edits ...Edit) ChangePlan {
	t.Helper()
	config, err := facade.backend.config(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := facade.PlanChange(context.Background(), PlanInput{RequestID: newID(), Base: versions(config), SchemaVersion: SchemaVersion, Edits: edits})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func fieldEdit(group, field, value string) Edit {
	return Edit{Kind: "field", Scope: Scope{Kind: "group", GroupID: group}, Field: field, Value: json.RawMessage(value)}
}

func TestPublishPreservesMapAndEmptyOverride(t *testing.T) {
	facade, stub, options := setup(t)
	plan := planEdit(t, facade, fieldEdit("1@chatroom", "self_sender_ids", `[]`), fieldEdit("1@chatroom", "allowed_types", `[]`))
	if len(plan.Changes) != 2 || len(plan.AffectedGroups) != 2 {
		t.Fatalf("incomplete preview: %+v", plan)
	}
	input := PublishInput{RequestID: "publish-1", PlanID: plan.ID, Token: plan.Token}
	operation, err := facade.PublishChange(context.Background(), input)
	if err != nil || operation.Status != "succeeded" {
		t.Fatalf("%+v %v", operation, err)
	}
	groupMap := stub.patch["group_rules"].(map[string]any)
	if groupMap["2@chatroom"].(map[string]any)["future_field"] != "preserve" {
		t.Fatal("lost sibling or unknown field")
	}
	for _, field := range []string{"self_sender_ids", "allowed_types"} {
		value := groupMap["1@chatroom"].(map[string]any)[field]
		if list, ok := value.([]any); !ok || len(list) != 0 {
			t.Fatalf("empty override was lost: %#v", value)
		}
	}
	if len(stub.patch) != 1 {
		t.Fatalf("unexpected flat patch: %+v", stub.patch)
	}
	if err := facade.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	repeated, err := reopened.PublishChange(context.Background(), input)
	if err != nil || repeated.ID != operation.ID || stub.writes != 1 {
		t.Fatalf("publication duplicated: %+v %v", repeated, err)
	}
	journal, err := os.Stat(filepath.Join(options.StateDir, "console-changes.json"))
	if err != nil || journal.Mode().Perm() != 0600 {
		t.Fatalf("journal permissions: %v %v", journal, err)
	}
}

func TestPublishReadbackOutcomes(t *testing.T) {
	for _, scenario := range []struct{ mode, status, state string }{
		{"applied500", "partial_apply", "applied_persistence_unverified"},
		{"reject", "failed", "failed_not_applied"},
		{"lostreadback", "unknown", "unknown"},
		{"race", "unknown", "unknown"},
	} {
		t.Run(scenario.mode, func(t *testing.T) {
			facade, stub, _ := setup(t)
			plan := planEdit(t, facade, fieldEdit("1@chatroom", "target", `"changed"`))
			stub.mode = scenario.mode
			input := PublishInput{RequestID: "pub", PlanID: plan.ID, Token: plan.Token}
			operation, err := facade.PublishChange(context.Background(), input)
			if err != nil || operation.Status != scenario.status || operation.Components[0].State != scenario.state {
				t.Fatalf("%+v %v", operation, err)
			}
			_, err = facade.PublishChange(context.Background(), input)
			if err != nil || stub.writes != 1 {
				t.Fatalf("retried write: %d %v", stub.writes, err)
			}
			if scenario.mode == "lostreadback" {
				previous := operation
				stub.mode = ""
				operation, err = facade.GetOperation(context.Background(), "pub")
				if err != nil || operation.Status != "partial_apply" || stub.writes != 1 {
					t.Fatalf("recovery: %+v %v", operation, err)
				}
				if previous.Components[0].State != "unknown" {
					t.Fatal("readback mutated a previously returned operation")
				}
			}
		})
	}
}

func TestPlanRejectsUnsafeOrInvalidEdits(t *testing.T) {
	for _, edit := range []Edit{
		fieldEdit("1@chatroom", "target", `"bad\nname"`),
		fieldEdit("1@chatroom", "batch_size", `51`),
		fieldEdit("1@chatroom", "decision_threshold", `0.3`),
		fieldEdit("1@chatroom", "regex_blocklist", `["["]`),
		fieldEdit("1@chatroom", "hermes_profile", `"other"`),
		fieldEdit("1@chatroom", "support_mode", `true`),
		{Kind: "permission"}, {Kind: "routing"},
	} {
		t.Run(edit.Kind+edit.Field, func(t *testing.T) {
			facade, stub, _ := setup(t)
			config, _ := facade.backend.config(context.Background())
			_, err := facade.PlanChange(context.Background(), PlanInput{RequestID: "bad", Base: versions(config), SchemaVersion: SchemaVersion, Edits: []Edit{edit}})
			if err == nil || stub.writes != 0 {
				t.Fatalf("unsafe edit accepted: %+v %v", edit, err)
			}
		})
	}
}

func TestConflictExpiryAndToken(t *testing.T) {
	facade, stub, _ := setup(t)
	plan := planEdit(t, facade, fieldEdit("1@chatroom", "target", `"changed"`))
	input := PublishInput{RequestID: "pub", PlanID: plan.ID, Token: "wrong"}
	if _, err := facade.PublishChange(context.Background(), input); err == nil {
		t.Fatal("accepted invalid token")
	}
	input.Token = plan.Token
	stub.revision++
	if _, err := facade.PublishChange(context.Background(), input); err == nil {
		t.Fatal("accepted stale base")
	}
	stub.revision--
	facade.plans[plan.ID].Plan.ExpiresAt = time.Now().Add(-time.Second)
	if _, err := facade.PublishChange(context.Background(), input); err == nil {
		t.Fatal("accepted expired plan")
	}
	if stub.writes != 0 {
		t.Fatal("wrote rejected plan")
	}
}

func TestMissingPresenceFailsClosed(t *testing.T) {
	facade, stub, _ := setup(t)
	delete(stub.root["rules"].(map[string]any)["group_rules"].(map[string]any)["2@chatroom"].(map[string]any), "self_sender_ids")
	config, _ := facade.backend.config(context.Background())
	_, err := facade.PlanChange(context.Background(), PlanInput{RequestID: "bad", Base: versions(config), SchemaVersion: SchemaVersion, Edits: []Edit{fieldEdit("1@chatroom", "target", `"changed"`)}})
	if err == nil || err.(*Problem).Code != "unsupported" || stub.writes != 0 {
		t.Fatalf("lossy map allowed: %v", err)
	}
}

func TestAdmissionAndGlobalPatch(t *testing.T) {
	facade, stub, _ := setup(t)
	plan := planEdit(t, facade, Edit{Kind: "group_admission", GroupID: "1@chatroom", Enabled: false})
	_, err := facade.PublishChange(context.Background(), PublishInput{RequestID: "disable", PlanID: plan.ID, Token: plan.Token})
	if err != nil {
		t.Fatal(err)
	}
	if includes(stub.patch["group_whitelist"], "1@chatroom") || !includes(stub.patch["group_whitelist"], "2@chatroom") {
		t.Fatal("bad whitelist merge")
	}
	plan = planEdit(t, facade, Edit{Kind: "field", Scope: Scope{Kind: "global"}, Field: "dry_run", Value: json.RawMessage(`false`)})
	_, err = facade.PublishChange(context.Background(), PublishInput{RequestID: "dryrun", PlanID: plan.ID, Token: plan.Token})
	if err != nil || stub.patch["dry_run"] != false || len(stub.patch) != 1 {
		t.Fatalf("dry_run not flattened: %+v %v", stub.patch, err)
	}
}

func TestHTTPBoundaryAndDataMinimization(t *testing.T) {
	facade, stub, _ := setup(t)
	for _, scenario := range []struct {
		host, origin, method, path, content, body string
		status                                    int
	}{
		{"evil.example:8090", "", "GET", "/console/v1/bootstrap", "", "", 403},
		{"127.0.0.1:8090", "http://127.0.0.1:8088", "GET", "/console/v1/bootstrap", "", "", 403},
		{"127.0.0.1:8090", "https://127.0.0.1:8090", "GET", "/console/v1/bootstrap", "", "", 403},
		{"127.0.0.1:8090", "", "POST", "/console/v1/changes/plan", "text/plain", `{}`, 415},
		{"127.0.0.1:8090", "", "POST", "/console/v1/changes/plan", "application/json", `{"unknown":1}`, 400},
		{"127.0.0.1:8090", "", "POST", "/console/v1/changes/plan", "application/json", `{} {}`, 400},
		{"127.0.0.1:8090", "", "POST", "/console/v1/changes/plan", "application/json", strings.Repeat(" ", maxBody+1), 413},
		{"127.0.0.1:8090", "", "GET", "/console/v1/runs", "", "", 501},
		{"127.0.0.1:8090", "http://127.0.0.1:8090", "GET", "/console/v1/bootstrap", "", "", 200},
		{"127.0.0.1:8090", "", "GET", "/console/v1/groups", "", "", 200},
	} {
		request := httptest.NewRequest(scenario.method, "http://"+scenario.host+scenario.path, bytes.NewBufferString(scenario.body))
		request.Header.Set("Origin", scenario.origin)
		request.Header.Set("Content-Type", scenario.content)
		writer := httptest.NewRecorder()
		facade.Handler().ServeHTTP(writer, request)
		if writer.Code != scenario.status {
			t.Fatalf("%+v: %d %s", scenario, writer.Code, writer.Body.String())
		}
		if strings.Contains(writer.Body.String(), "SECRET") || strings.Contains(writer.Body.String(), "/private/") {
			t.Fatal("private data leaked")
		}
		if writer.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatal("CORS unexpectedly enabled")
		}
	}
	for _, path := range stub.paths {
		if path != "GET /health" && path != "GET /api/stats" && path != "GET /api/config" && path != "GET /api/trace/snapshot" && path != "GET /api/v1/console-config" && path != "GET /api/v1/config" {
			t.Fatalf("unsafe probe: %s", path)
		}
	}
}

func TestUpstreamOriginValidationAndRedirect(t *testing.T) {
	for _, origin := range []string{"http://example.com:8088", "http://127.0.0.1:8088/send", "http://user@127.0.0.1:8088", "http://127.0.0.1:8088?url=x", "https://127.0.0.1:8088", "http://127.0.0.1"} {
		if _, err := New(Options{DispatcherURL: origin, StateDir: t.TempDir()}); err == nil {
			t.Fatalf("accepted %s", origin)
		}
	}
	hits := 0
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, 302)
	}))
	defer redirect.Close()
	facade, err := New(Options{DispatcherURL: redirect.URL, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := facade.GetGlobalRules(context.Background()); err == nil || hits != 0 {
		t.Fatalf("redirect followed: %d %v", hits, err)
	}
}

func TestConcurrentPublishWritesOnce(t *testing.T) {
	facade, stub, _ := setup(t)
	plan := planEdit(t, facade, fieldEdit("1@chatroom", "target", `"changed"`))
	var workers sync.WaitGroup
	for index := 0; index < 8; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			_, err := facade.PublishChange(context.Background(), PublishInput{RequestID: "same", PlanID: plan.ID, Token: plan.Token})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	if stub.writes != 1 {
		t.Fatalf("writes: %d", stub.writes)
	}
}

func TestJournalLockAndInterruptedPublication(t *testing.T) {
	facade, stub, options := setup(t)
	if other, err := New(options); err == nil {
		other.Close()
		t.Fatal("journal accepted a second writer")
	}
	plan := planEdit(t, facade, fieldEdit("1@chatroom", "target", `"changed"`))
	saved := facade.plans[plan.ID]
	saved.PublishID = "interrupted"
	saved.Record = &ChangeRecord{ID: plan.ID, Operation: Operation{ID: "operation", RequestID: "interrupted", Status: "pending", Components: []ComponentResult{{ID: "dispatcher", State: "applying"}}}}
	if err := facade.persist(); err != nil {
		t.Fatal(err)
	}
	if err := facade.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	operation, err := reopened.GetOperation(context.Background(), "interrupted")
	if err != nil || operation.Status != "unknown" {
		t.Fatalf("interrupted intent incorrectly resolved: %+v %v", operation, err)
	}
	operation, err = reopened.PublishChange(context.Background(), PublishInput{RequestID: "interrupted", PlanID: plan.ID, Token: plan.Token})
	if err != nil || operation.Status != "unknown" || stub.writes != 0 {
		t.Fatalf("interrupted write replayed: %+v %v", operation, err)
	}
}

func TestPlanIdempotencyAndChangedPayload(t *testing.T) {
	facade, _, _ := setup(t)
	config, _ := facade.backend.config(context.Background())
	input := PlanInput{RequestID: "same-plan", Base: versions(config), SchemaVersion: SchemaVersion, Edits: []Edit{fieldEdit("1@chatroom", "target", `"changed"`)}}
	first, err := facade.PlanChange(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	second, err := facade.PlanChange(context.Background(), input)
	if err != nil || first.ID != second.ID || first.Token != second.Token {
		t.Fatalf("plan replay changed: %+v %v", second, err)
	}
	input.Edits[0].Value = json.RawMessage(`"another"`)
	if _, err := facade.PlanChange(context.Background(), input); err == nil {
		t.Fatal("request ID accepted changed payload")
	}
}

func TestHealthStatesAndMissingFacts(t *testing.T) {
	for _, scenario := range []struct {
		status         int
		body, expected string
	}{
		{404, `{}`, "unsupported"}, {503, `{"status":"not_ready"}`, "not_ready"},
		{200, `{"status":"ready","protocol_version":99}`, "unknown"},
		{200, `{"status":"ready","protocol_version":1}`, "ready"},
		{200, `not json`, "unknown"},
	} {
		t.Run(scenario.expected+scenario.body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != "GET" || request.URL.Path != "/health" {
					t.Errorf("unsafe health call: %s %s", request.Method, request.URL.Path)
				}
				writer.WriteHeader(scenario.status)
				_, _ = writer.Write([]byte(scenario.body))
			}))
			defer server.Close()
			facade, err := New(Options{DispatcherURL: server.URL, StateDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer facade.Close()
			items := facade.Services(context.Background()).Data.([]ServiceView)
			if items[1].Health != scenario.expected || items[1].Process != "unknown" {
				t.Fatalf("bad health interpretation: %+v", items[1])
			}
			if items[1].Facts["accessibility_trusted"] != nil || items[0].Health != "unsupported" {
				t.Fatal("missing fact or source was invented")
			}
		})
	}
}

func TestPlanAndPublishMultiPlatformContainer(t *testing.T) {
	facade, stub, _ := setup(t)
	plan := planEdit(t, facade,
		Edit{Kind: "create_group", GroupID: "bilibili:BV1xx411c7mD", TargetLabel: "Bilibili Demo"},
		Edit{Kind: "group_admission", GroupID: "bilibili:BV1xx411c7mD", Enabled: true},
	)
	op, err := facade.PublishChange(context.Background(), PublishInput{RequestID: "bili-create", PlanID: plan.ID, Token: plan.Token})
	if err != nil || op.Status != "succeeded" {
		t.Fatalf("multi-platform create failed: %+v %v", op, err)
	}
	editPlan := planEdit(t, facade, fieldEdit("bilibili:BV1xx411c7mD", "target", `"Bilibili Video"`))
	if len(editPlan.Changes) != 1 || stub.root["rules"].(map[string]any)["group_rules"].(map[string]any)["bilibili:BV1xx411c7mD"] == nil {
		t.Fatalf("multi-platform route missing after create: %+v", editPlan)
	}
	config, err := facade.backend.config(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := facade.PlanChange(context.Background(), PlanInput{
		RequestID:     "bili-bad-target",
		Base:          versions(config),
		SchemaVersion: SchemaVersion,
		Edits:         []Edit{fieldEdit("bilibili:BV1xx411c7mD", "target_group_id", `"1@chatroom"`)},
	}); err == nil {
		t.Fatal("accepted cross-platform target_group_id in console rules")
	}
}

func TestPlanAndPublishScenarioAndMountFields(t *testing.T) {
	facade, stub, _ := setup(t)
	plan := planEdit(t, facade,
		fieldEdit("1@chatroom", "scenario", `"tech_community"`),
		fieldEdit("1@chatroom", "response_goal", `"Answer directly and verify claims"`),
		fieldEdit("1@chatroom", "tone", `"concise"`),
		fieldEdit("1@chatroom", "skills", `["skill.debug"]`),
		fieldEdit("1@chatroom", "tools", `["web_search","web_fetch"]`),
		fieldEdit("1@chatroom", "knowledge_bases", `["kb-core"]`),
	)
	if len(plan.Changes) != 6 {
		t.Fatalf("expected 6 diff changes for scenario+mount fields, got %d: %+v", len(plan.Changes), plan.Changes)
	}
	op, err := facade.PublishChange(context.Background(), PublishInput{RequestID: "scenario-pub", PlanID: plan.ID, Token: plan.Token})
	if err != nil || op.Status != "succeeded" {
		t.Fatalf("scenario publish failed: %+v %v", op, err)
	}
	groupViewRaw, err := facade.GetGroup(context.Background(), "1@chatroom")
	if err != nil {
		t.Fatal(err)
	}
	gv := groupViewRaw.Data.(GroupView)
	if gv.Fields["scenario"].Effective != "tech_community" || gv.Fields["response_goal"].Effective != "Answer directly and verify claims" || gv.Fields["tone"].Effective != "concise" {
		t.Fatalf("unexpected scenario fields on GroupView: %+v", gv.Fields)
	}
	// Verify empty slice vs null on omitempty mount fields reads back losslessly when Dispatcher omits empty mounts.
	clearPlan := planEdit(t, facade,
		fieldEdit("1@chatroom", "skills", `[]`),
		fieldEdit("1@chatroom", "tools", `[]`),
		fieldEdit("1@chatroom", "knowledge_bases", `[]`),
	)
	delete(stub.root["rules"].(map[string]any)["group_rules"].(map[string]any)["1@chatroom"].(map[string]any), "skills")
	op2, err := facade.PublishChange(context.Background(), PublishInput{RequestID: "scenario-clear", PlanID: clearPlan.ID, Token: clearPlan.Token})
	if err != nil || op2.Status != "succeeded" {
		t.Fatalf("clearing mount lists failed: %+v %v", op2, err)
	}

	// Reject invalid scenario/tone/mount inputs.
	config, err := facade.backend.config(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []Edit{
		fieldEdit("1@chatroom", "scenario", `"../bad"`),
		fieldEdit("1@chatroom", "response_goal", strings.Repeat("a", 201)),
		fieldEdit("1@chatroom", "tone", "bad\ntone"),
		fieldEdit("1@chatroom", "skills", `["dup","dup"]`),
		fieldEdit("1@chatroom", "tools", `["bad id"]`),
		fieldEdit("1@chatroom", "knowledge_bases", `["a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q"]`),
	} {
		if _, err := facade.PlanChange(context.Background(), PlanInput{
			RequestID:     "invalid-" + invalid.Field,
			Base:          versions(config),
			SchemaVersion: SchemaVersion,
			Edits:         []Edit{invalid},
		}); err == nil {
			t.Fatalf("accepted invalid edit %+v", invalid)
		}
	}
}

func TestConsoleCatalogUnsupportedAndValid(t *testing.T) {
	// Upstream without /api/catalog returns 501 unsupported.
	facade, _, _ := setup(t)
	req := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8090/console/v1/catalog", nil)
	rec := httptest.NewRecorder()
	facade.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 when upstream omits /api/catalog, got %d %s", rec.Code, rec.Body.String())
	}

	// Upstream returning valid sanitized catalog projects cleanly.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/catalog" {
			_, _ = w.Write([]byte(`{"scenarios":[{"id":"tech_community","builtin":true,"hasLocalPolicy":true},{"id":"chat","builtin":true,"hasLocalPolicy":false}],"knowledgeBases":[{"id":"kb-1"}],"tools":[{"id":"web_search","supportAllowed":true}],"skills":[{"id":"debug-skill"}]}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	facade2, err := New(Options{DispatcherURL: srv.URL, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer facade2.Close()

	rec2 := httptest.NewRecorder()
	facade2.Handler().ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8090/console/v1/catalog", nil))
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 on /console/v1/catalog, got %d %s", rec2.Code, rec2.Body.String())
	}
	var out struct {
		OK    bool `json:"ok"`
		Value struct {
			Data CatalogView `json:"data"`
		} `json:"value"`
	}
	if err := json.Unmarshal(rec2.Body.Bytes(), &out); err != nil || !out.OK || len(out.Value.Data.Scenarios) != 2 || out.Value.Data.Scenarios[0].ID != "chat" {
		t.Fatalf("unexpected catalog envelope: %s (%v)", rec2.Body.String(), err)
	}

	// Invalid IDs from upstream fail closed with 503.
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"scenarios":[{"id":"../secret/path","builtin":false,"hasLocalPolicy":false}],"knowledgeBases":[],"tools":[],"skills":[]}`))
	}))
	defer badSrv.Close()
	facade3, err := New(Options{DispatcherURL: badSrv.URL, StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer facade3.Close()
	rec3 := httptest.NewRecorder()
	facade3.Handler().ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "http://127.0.0.1:8090/console/v1/catalog", nil))
	if rec3.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for invalid upstream catalog ID, got %d", rec3.Code)
	}
}
