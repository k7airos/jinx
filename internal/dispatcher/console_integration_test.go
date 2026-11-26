package dispatcher

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"jinx/internal/console"
)

func TestFacadePublishesToRealDispatcherHandler(t *testing.T) {
	cfg := testConfig()
	cfg.Rules.SelfSenderIDs = []string{"global-self"}
	dispatcher := newTestDispatcher(t, cfg)
	server := httptest.NewServer(dispatcher.Handler())
	defer server.Close()
	facade, err := console.New(console.Options{DispatcherURL: server.URL, StateDir: t.TempDir(), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer facade.Close()
	ctx := context.Background()
	view, err := facade.GetGroup(ctx, groupA)
	if err != nil {
		t.Fatal(err)
	}
	group := view.Data.(console.GroupView)
	plan, err := facade.PlanChange(ctx, console.PlanInput{
		RequestID: "integration-plan", Base: group.Versions, SchemaVersion: console.SchemaVersion,
		Edits: []console.Edit{
			{Kind: "field", Scope: console.Scope{Kind: "group", GroupID: groupA}, Field: "self_sender_ids", Value: json.RawMessage(`[]`)},
			{Kind: "field", Scope: console.Scope{Kind: "group", GroupID: groupA}, Field: "allowed_types", Value: json.RawMessage(`[]`)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := facade.PublishChange(ctx, console.PublishInput{RequestID: "integration-publish", PlanID: plan.ID, Token: plan.Token})
	if err != nil || operation.Status != "succeeded" {
		t.Fatal(operation, err)
	}
	saved := dispatcher.configSnapshot()
	rule := saved.Rules.GroupRules[groupA]
	if rule.SelfSenderIDs == nil || rule.AllowedTypes == nil || len(rule.SelfSenderIDs) != 0 || len(rule.AllowedTypes) != 0 {
		t.Fatal("facade publication lost explicit empty overrides", rule)
	}
	if saved.Rules.GroupRules[groupB].Target != cfg.Rules.GroupRules[groupB].Target || saved.Hermes.BinPath != cfg.Hermes.BinPath {
		t.Fatal("facade publication changed unrelated configuration")
	}
	view, err = facade.GetGroup(ctx, groupA)
	if err != nil || view.Data.(console.GroupView).Versions.Dispatcher == group.Versions.Dispatcher {
		t.Fatal("facade readback did not observe a new version", err)
	}
}
