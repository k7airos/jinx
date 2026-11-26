package console_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/console"
	"jinx/internal/dispatcher"
)

const publicationGroupA = "930001@chatroom"
const publicationGroupB = "930002@chatroom"

type publicationHandler struct {
	facade *console.Facade
	server *httptest.Server
	path   string
	writes atomic.Int32
}

func newPublicationHandler(t *testing.T, mode string) *publicationHandler {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "dispatcher.json")
	data := []byte(`{
		"server":{"listen_addr":"127.0.0.1:0"},
		"relay":{"stream_url":"http://127.0.0.1:1/events"},
		"bridge":{"url":"http://127.0.0.1:1","dry_run":true,"timeout_sec":1},
		"hermes":{"bin_path":"/synthetic/not-executed","profile":"publication","profiles_dir":"` + filepath.ToSlash(root) + `","timeout_sec":1},
		"rules":{"batch_size":8,"idle_timeout_sec":5,"max_batch_wait_sec":15,
			"max_message_age_sec":180,"self_sender_ids":["global-self"],
			"group_whitelist":["930001@chatroom","930002@chatroom"],
			"group_rules":{
				"930001@chatroom":{"enabled":true,"target":"publication one","target_group_id":"930003@chatroom",
					"self_sender_ids":["local-self"],"allowed_types":[1],"dry_run":true,"mention_only":true,
					"blocked_sender_ids":["blocked"],"keyword_blocklist":["blocked"],"regex_blocklist":["blocked"],
					"batch_size":10,"idle_timeout_sec":6,"max_batch_wait_sec":16,
					"max_reply_runes":600,"reply_window_sec":60,"max_replies_in_window":3,"decision_threshold":0.8,
					"decision_context":"before","sensitive_criteria":"before","noise_criteria":"before","safe_criteria":"before",
					"promo_shadow":true,"promo_keywords":["before"],"promo_daily_cap":2},
				"930002@chatroom":{"enabled":true,"target":"publication two","self_sender_ids":[],"allowed_types":[]}
			}
		}
	}`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	dispatch, err := dispatcher.NewDispatcher(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dispatch.Close)
	if mode == "saved500" {
		if err := os.Rename(filepath.Join(root, "logs", "state.json"), filepath.Join(root, "initial-state.json")); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(root, "logs", "state.json"), 0700); err != nil {
			t.Fatal(err)
		}
	}
	fixture := &publicationHandler{path: path}
	handler := dispatch.Handler()
	fixture.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost && request.URL.Path == "/api/config" {
			fixture.writes.Add(1)
			if mode == "saved500" {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusInternalServerError {
					t.Errorf("real Handler should fail state persistence after saving config: %d %s", response.Code, response.Body.String())
				}
				for name, values := range response.Header() {
					writer.Header()[name] = values
				}
				writer.WriteHeader(response.Code)
				_, _ = writer.Write(response.Body.Bytes())
				return
			}
		}
		if mode == "mismatch" && request.Method == http.MethodGet && request.URL.Path == "/api/config" && fixture.writes.Load() > 0 {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			var root map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &root); err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			root["rules"].(map[string]any)["group_rules"].(map[string]any)[publicationGroupA].(map[string]any)["promo_shadow"] = true
			writer.Header().Set("ETag", response.Header().Get("ETag"))
			_ = json.NewEncoder(writer).Encode(root)
			return
		}
		handler.ServeHTTP(writer, request)
	}))
	t.Cleanup(fixture.server.Close)
	fixture.facade, err = console.New(console.Options{DispatcherURL: fixture.server.URL, StateDir: filepath.Join(root, "console"), Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.facade.Close() })
	return fixture
}

func (fixture *publicationHandler) publish(t *testing.T, edits ...console.Edit) console.Operation {
	t.Helper()
	ctx := context.Background()
	view, err := fixture.facade.GetGlobalRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	base := view.Data.(map[string]any)["versions"].(console.Versions)
	requestID := t.Name() + time.Now().Format("150405.000000000")
	plan, err := fixture.facade.PlanChange(ctx, console.PlanInput{RequestID: requestID, Base: base, SchemaVersion: console.SchemaVersion, Edits: edits})
	if err != nil {
		t.Fatal(err)
	}
	input := console.PublishInput{RequestID: requestID, PlanID: plan.ID, Token: plan.Token}
	operation, err := fixture.facade.PublishChange(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	writes := fixture.writes.Load()
	repeated, err := fixture.facade.PublishChange(ctx, input)
	if err != nil || !reflect.DeepEqual(repeated, operation) || fixture.writes.Load() != writes {
		t.Fatalf("replay changed operation or repeated write: %+v %v", repeated, err)
	}
	return operation
}

func publicationField(field, value string) console.Edit {
	return console.Edit{Kind: "field", Scope: console.Scope{Kind: "group", GroupID: publicationGroupA}, Field: field, Value: json.RawMessage(value)}
}

func (fixture *publicationHandler) groups(t *testing.T) map[string]any {
	t.Helper()
	response, err := fixture.server.Client().Get(fixture.server.URL + "/api/config")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var config map[string]any
	if response.StatusCode != http.StatusOK || json.NewDecoder(response.Body).Decode(&config) != nil {
		t.Fatal("cannot read real Handler config")
	}
	data, err := os.ReadFile(fixture.path)
	if err != nil {
		t.Fatal(err)
	}
	var persisted map[string]any
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if fixture.writes.Load() > 0 && !reflect.DeepEqual(config, persisted) {
		t.Fatal("Handler memory and persisted configuration differ")
	}
	return config["rules"].(map[string]any)["group_rules"].(map[string]any)
}

func assertPublicationVerified(t *testing.T, operation console.Operation) {
	t.Helper()
	if operation.Status != "succeeded" || len(operation.Components) != 1 || operation.Components[0].State != "verified" || operation.Components[0].PersistenceVerified == nil || !*operation.Components[0].PersistenceVerified {
		t.Fatalf("publication not verified: %+v", operation)
	}
}

func TestPublicationRealHandlerDeleteAndRecreate(t *testing.T) {
	fixture := newPublicationHandler(t, "")
	before := fixture.groups(t)
	operation := fixture.publish(t, console.Edit{Kind: "delete_group", GroupID: publicationGroupA})
	after := fixture.groups(t)
	if _, exists := after[publicationGroupA]; exists {
		t.Fatal("delete_group retained the deleted group key")
	}
	assertPublicationVerified(t, operation)
	if len(after) != 1 || !reflect.DeepEqual(after[publicationGroupB], before[publicationGroupB]) {
		t.Fatal("deleting A changed sibling B")
	}
	operation = fixture.publish(t, console.Edit{Kind: "create_group", GroupID: publicationGroupA, TargetLabel: "recreated publication one"})
	assertPublicationVerified(t, operation)
	after = fixture.groups(t)
	created := after[publicationGroupA].(map[string]any)
	if len(after) != 2 || !reflect.DeepEqual(after[publicationGroupB], before[publicationGroupB]) || created["enabled"] != false || created["self_sender_ids"] != nil || created["allowed_types"] != nil || fixture.writes.Load() != 2 {
		t.Fatalf("recreate lost sparse group defaults or sibling: %+v", after)
	}
	for _, field := range []string{"batch_size", "max_reply_runes", "reply_window_sec", "decision_threshold"} {
		if created[field] != float64(0) {
			t.Fatalf("stored %s should remain zero, not its runtime effective default: %+v", field, created[field])
		}
	}
}

func TestPublicationRealHandlerDefaultAndClear(t *testing.T) {
	for _, scenario := range []struct{ field, value string }{
		{"dry_run", `false`}, {"mention_only", `false`}, {"promo_shadow", `false`},
		{"target_group_id", `""`}, {"decision_context", `""`}, {"sensitive_criteria", `""`}, {"noise_criteria", `""`}, {"safe_criteria", `""`},
		{"batch_size", `0`}, {"idle_timeout_sec", `0`}, {"max_batch_wait_sec", `0`},
		{"max_reply_runes", `0`}, {"reply_window_sec", `0`}, {"max_replies_in_window", `0`}, {"decision_threshold", `0`}, {"promo_daily_cap", `0`},
		{"self_sender_ids", `null`}, {"self_sender_ids", `[]`}, {"allowed_types", `null`}, {"allowed_types", `[]`},
		{"blocked_sender_ids", `null`}, {"blocked_sender_ids", `[]`}, {"keyword_blocklist", `null`}, {"keyword_blocklist", `[]`},
		{"regex_blocklist", `null`}, {"regex_blocklist", `[]`}, {"promo_keywords", `null`}, {"promo_keywords", `[]`},
	} {
		t.Run(scenario.field+"="+scenario.value, func(t *testing.T) {
			fixture := newPublicationHandler(t, "")
			before := fixture.groups(t)
			operation := fixture.publish(t, publicationField(scenario.field, scenario.value))
			assertPublicationVerified(t, operation)
			after := fixture.groups(t)
			if !reflect.DeepEqual(before[publicationGroupB], after[publicationGroupB]) || fixture.writes.Load() != 1 {
				t.Fatal("field publication changed sibling or write count")
			}
			group := after[publicationGroupA].(map[string]any)
			if scenario.field == "self_sender_ids" || scenario.field == "allowed_types" {
				value, exists := group[scenario.field]
				var expected any
				_ = json.Unmarshal([]byte(scenario.value), &expected)
				if !exists || !reflect.DeepEqual(value, expected) {
					t.Fatalf("null/[] semantics lost: %+v", group)
				}
			}
		})
	}
}

func TestPublicationRealHandlerReadbackEvidence(t *testing.T) {
	for _, scenario := range []struct{ mode, status, state string }{
		{"saved500", "partial_apply", "applied_persistence_unverified"},
		{"mismatch", "unknown", "unknown"},
	} {
		t.Run(scenario.mode, func(t *testing.T) {
			fixture := newPublicationHandler(t, scenario.mode)
			operation := fixture.publish(t, publicationField("promo_shadow", `false`))
			if operation.Status != scenario.status || operation.Components[0].State != scenario.state || operation.Components[0].PersistenceVerified != nil {
				t.Fatalf("readback falsely verified: %+v", operation)
			}
			data, err := os.ReadFile(fixture.path)
			if err != nil || bytes.Contains(data, []byte(`"promo_shadow": true`)) {
				t.Fatalf("real Handler did not save cleared value: %v", err)
			}
			recovered, err := fixture.facade.GetOperation(context.Background(), operation.ID)
			if err != nil || recovered.Status != scenario.status || fixture.writes.Load() != 1 {
				t.Fatalf("operation read changed evidence or retried: %+v %v", recovered, err)
			}
		})
	}
}
