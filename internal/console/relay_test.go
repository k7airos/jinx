package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type relayStub struct {
	mu                   sync.Mutex
	root                 map[string]any
	dispatcher           *dispatcherStub
	driftAfterDispatcher bool
	writes               int
}

func (stub *relayStub) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if request.URL.Path != "/api/v1/config" {
		writer.WriteHeader(404)
		return
	}
	if stub.driftAfterDispatcher {
		stub.dispatcher.mu.Lock()
		count := stub.dispatcher.writes
		stub.dispatcher.mu.Unlock()
		if count > 0 {
			stub.root["private"] = "external-drift"
		}
	}
	if request.Method == "POST" {
		stub.writes++
		_ = json.NewDecoder(request.Body).Decode(&stub.root)
	}
	_ = json.NewEncoder(writer).Encode(stub.root)
}

func setupRelayStub(t *testing.T, facade *Facade, dispatcher *dispatcherStub) *relayStub {
	t.Helper()
	stub := &relayStub{dispatcher: dispatcher, root: map[string]any{}}
	_ = json.Unmarshal([]byte(`{"group_control":{"monitor_all":false,"monitored_groups":[],"exposed_groups":[],"group_aliases":{},"drop_unexposed_completely":false},"filters":{"keyword_blocklist":[],"regex_blocklist":[],"sender_blocklist":[],"allowed_types":[1],"clean_content":true,"ignore_echo":false},"private":"not-journaled"}`), &stub.root)
	server := httptest.NewServer(stub)
	t.Cleanup(server.Close)
	facade.backend.bases["relay"] = server.URL
	return stub
}

func TestBetweenComponentDriftRejectsLaterWrite(t *testing.T) {
	facade, dispatcher, _ := setup(t)
	relay := setupRelayStub(t, facade, dispatcher)
	view, _ := facade.GetRelayRules(context.Background())
	plan, err := facade.PlanChange(context.Background(), PlanInput{RequestID: "between", Base: view.Data.(RelayRulesView).Versions, SchemaVersion: SchemaVersion, Edits: []Edit{fieldEdit("1@chatroom", "target", `"changed"`), {Kind: "relay_field", Scope: Scope{Kind: "relay_filters"}, Field: "clean_content", Value: json.RawMessage(`false`)}}})
	if err != nil {
		t.Fatal(err)
	}
	relay.driftAfterDispatcher = true
	operation, err := facade.PublishChange(context.Background(), PublishInput{RequestID: "between-pub", PlanID: plan.ID, Token: plan.Token})
	if err != nil || operation.Status != "partial_apply" || operation.Components[1].State != "failed_not_applied" || relay.writes != 0 || dispatcher.writes != 1 {
		t.Fatalf("%+v %v", operation, err)
	}
}

func TestInterruptedCrossComponentReadbackNeverResumesWrites(t *testing.T) {
	facade, dispatcher, options := setup(t)
	relay := setupRelayStub(t, facade, dispatcher)
	options.RelayURL = facade.backend.bases["relay"]
	view, _ := facade.GetRelayRules(context.Background())
	plan, err := facade.PlanChange(context.Background(), PlanInput{RequestID: "interrupted-cross", Base: view.Data.(RelayRulesView).Versions, SchemaVersion: SchemaVersion, Edits: []Edit{fieldEdit("1@chatroom", "target", `"changed"`), {Kind: "relay_field", Scope: Scope{Kind: "relay_filters"}, Field: "clean_content", Value: json.RawMessage(`false`)}}})
	if err != nil {
		t.Fatal(err)
	}
	saved := facade.plans[plan.ID]
	saved.PublishID = "interrupted-cross-pub"
	saved.Record = &ChangeRecord{ID: plan.ID, Operation: Operation{ID: "cross-operation", RequestID: saved.PublishID, Status: "pending", Components: []ComponentResult{{ID: "dispatcher", State: "applying"}, {ID: "relay", State: "not_started"}}}}
	dispatcher.mu.Lock()
	dispatcher.root["rules"].(map[string]any)["group_rules"].(map[string]any)["1@chatroom"].(map[string]any)["target"] = "changed"
	dispatcher.revision++
	dispatcher.mu.Unlock()
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
	operation, err := reopened.GetOperation(context.Background(), "cross-operation")
	if err != nil || operation.Status != "partial_apply" || operation.Components[0].State != "applied_persistence_unverified" || operation.Components[1].State != "not_started" || relay.writes != 0 || dispatcher.writes != 0 {
		t.Fatalf("%+v %v", operation, err)
	}
}
