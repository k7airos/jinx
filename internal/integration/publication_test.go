package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"jinx/internal/console"
	"jinx/internal/dispatcher"
	relayconfig "jinx/internal/relay/config"
)

func TestRelayDispatcherPublicationRollback(t *testing.T) {
	root := t.TempDir()
	relay := newRelay(t, root)
	var posts, writes atomic.Int32
	bridge := newBridge(t, relay.Client(), relay.URL, &posts, make(chan bridgeDelivery, 4))
	path, profile := prepareDispatcher(t, root, relay.URL, bridge.URL)
	backendResult(t, profile, syntheticReply, "synthetic-publication")
	services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes)
	ctx := context.Background()
	beforeEnvelope, err := services.facade.GetRelayRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	before := beforeEnvelope.Data.(console.RelayRulesView)
	var dispatcherBefore dispatcher.Config
	requestJSON(t, services.upstream.Client(), http.MethodGet, services.upstream.URL+"/api/config", nil, &dispatcherBefore)
	plan, err := services.facade.PlanChange(ctx, console.PlanInput{
		RequestID: "mixed-plan", Base: before.Versions, SchemaVersion: console.SchemaVersion,
		Edits: []console.Edit{
			{Kind: "field", Scope: console.Scope{Kind: "group", GroupID: sourceGroup}, Field: "dry_run", Value: json.RawMessage(`true`)},
			{Kind: "relay_field", Scope: console.Scope{Kind: "relay_filters"}, Field: "keyword_blocklist", Value: json.RawMessage(`["SYNTHETIC_BLOCKED_WORD"]`)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.ComponentOrder, []string{"dispatcher", "relay"}) || len(plan.Changes) != 2 || writes.Load() != 0 {
		t.Fatalf("mixed preview lost ordering or changed config: %+v", plan)
	}
	operation, err := services.facade.PublishChange(ctx, console.PublishInput{RequestID: "mixed-publish", PlanID: plan.ID, Token: plan.Token})
	if err != nil {
		t.Fatal(err)
	}
	assertComponents(t, operation, 2)
	afterEnvelope, err := services.facade.GetRelayRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	after := afterEnvelope.Data.(console.RelayRulesView)
	if after.Versions.Dispatcher == before.Versions.Dispatcher || after.Versions.Relay == before.Versions.Relay || !reflect.DeepEqual(after.Rules.Filters.KeywordBlocklist, []string{"SYNTHETIC_BLOCKED_WORD"}) {
		t.Fatalf("mixed publication failed versioned readback: %+v", after)
	}
	_, err = services.facade.PlanRollback(ctx, console.RollbackInput{RequestID: "stale-rollback-plan", ChangeID: plan.ID, Base: before.Versions})
	if problem, ok := err.(*console.Problem); !ok || problem.Code != "conflict" || writes.Load() != 1 {
		t.Fatalf("stale rollback did not reject the old component versions: %v writes=%d", err, writes.Load())
	}
	var denied struct{ Allowed bool }
	requestJSON(t, relay.Client(), http.MethodPost, relay.URL+"/api/v1/messages", message("relay-blocked-input", "@Jinx SYNTHETIC_BLOCKED_WORD"), &denied)
	if denied.Allowed {
		t.Fatal("published Relay blocklist did not block input")
	}
	var saved relayconfig.Config
	data := readFixtureFile(t, filepath.Join(root, "relay.json"))
	if json.Unmarshal(data, &saved) != nil || !reflect.DeepEqual(saved.Filters.KeywordBlocklist, []string{"SYNTHETIC_BLOCKED_WORD"}) {
		t.Fatal("Relay did not persist the published blocklist")
	}
	rollback, err := services.facade.PlanRollback(ctx, console.RollbackInput{RequestID: "mixed-rollback-plan", ChangeID: plan.ID, Base: after.Versions})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rollback.ComponentOrder, []string{"dispatcher", "relay"}) {
		t.Fatalf("rollback missed a component: %+v", rollback)
	}
	rolledBack, err := services.facade.PublishChange(ctx, console.PublishInput{RequestID: "mixed-rollback-publish", PlanID: rollback.ID, Token: rollback.Token})
	if err != nil {
		t.Fatal(err)
	}
	assertComponents(t, rolledBack, 2)
	finalEnvelope, err := services.facade.GetRelayRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	final := finalEnvelope.Data.(console.RelayRulesView)
	var dispatcherFinal dispatcher.Config
	requestJSON(t, services.upstream.Client(), http.MethodGet, services.upstream.URL+"/api/config", nil, &dispatcherFinal)
	if !reflect.DeepEqual(before.Rules, final.Rules) || !reflect.DeepEqual(dispatcherBefore, dispatcherFinal) || writes.Load() != 2 {
		t.Fatal("rollback failed to restore both public configurations exactly")
	}
	record, err := services.facade.GetChange(rollback.ID)
	if err != nil {
		t.Fatal(err)
	}
	history := record.Data.(console.ChangeRecord)
	if history.RollbackOf == nil || *history.RollbackOf != plan.ID || history.BeforeSnapshot == nil || history.TargetSnapshot == nil || history.ReadbackSnapshot == nil {
		t.Fatalf("rollback journal lacks snapshot evidence: %+v", history)
	}
	postMessage(t, relay.Client(), relay.URL, "after-rollback", "@Jinx SYNTHETIC_BLOCKED_WORD")
	if run := waitRun(t, services, "after-rollback"); run.Delivery != "confirmed" || posts.Load() != 1 {
		t.Fatalf("restored rules did not restore synthetic delivery: %+v", run)
	}
}

func assertComponents(t *testing.T, operation console.Operation, count int) {
	t.Helper()
	if operation.Status != "succeeded" || len(operation.Components) != count {
		t.Fatalf("publication failed: %+v", operation)
	}
	for _, component := range operation.Components {
		if component.State != "verified" || component.PersistenceVerified == nil || !*component.PersistenceVerified || component.ReadbackVersion == nil {
			t.Fatalf("component lacks persistence/readback evidence: %+v", component)
		}
	}
}
