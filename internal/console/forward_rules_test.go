package console

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestForwardRulesPlanPublishReadbackAndRollback(t *testing.T) {
	facade, stub, _ := setup(t)
	ctx := context.Background()

	// 1. GetGlobalRules exposes forward_rules even when omitted upstream.
	globalEnv, err := facade.GetGlobalRules(ctx)
	if err != nil {
		t.Fatalf("GetGlobalRules failed: %v", err)
	}
	fieldsMap := globalEnv.Data.(map[string]any)["fields"].(map[string]FieldValue)
	fwdField, ok := fieldsMap["forward_rules"]
	if !ok || !fwdField.Editable {
		t.Fatalf("expected editable forward_rules field in GetGlobalRules, got %+v", fwdField)
	}

	// 2. Reject invalid forward_rules (invalid source, self-loop, unknown target).
	invalidEdits := []struct {
		name  string
		value string
	}{
		{
			name:  "invalid_source",
			value: `[{"id":"r1","enabled":true,"source_container_id":"http://bad","target_container_id":"1@chatroom"}]`,
		},
		{
			name:  "self_loop",
			value: `[{"id":"r1","enabled":true,"source_container_id":"1@chatroom","target_container_id":"1@chatroom"}]`,
		},
		{
			name:  "unknown_target",
			value: `[{"id":"r1","enabled":true,"source_container_id":"v2ex:t-100001","target_container_id":"999@chatroom"}]`,
		},
	}
	for _, tc := range invalidEdits {
		t.Run(tc.name, func(t *testing.T) {
			cfg, _ := facade.backend.config(ctx)
			_, err := facade.PlanChange(ctx, PlanInput{
				RequestID:     "bad-" + tc.name,
				Base:          versions(cfg),
				SchemaVersion: SchemaVersion,
				Edits: []Edit{{
					Kind:  "field",
					Scope: Scope{Kind: "global"},
					Field: "forward_rules",
					Value: json.RawMessage(tc.value),
				}},
			})
			if err == nil {
				t.Fatalf("expected validation error for %s", tc.name)
			}
		})
	}

	// 3. Plan + publish adding a cross-platform forward rule (v2ex -> 1@chatroom).
	planAdd := planEdit(t, facade, Edit{
		Kind:  "field",
		Scope: Scope{Kind: "global"},
		Field: "forward_rules",
		Value: json.RawMessage(`[{"id":"v2ex-to-wx","enabled":true,"source_container_id":"v2ex:t-100001","target_container_id":"1@chatroom","dry_run":true,"template":"[V2EX] {{sender}}: {{content}}"}]`),
	})
	if len(planAdd.Changes) != 1 || planAdd.Changes[0].Path != "forward_rules" {
		t.Fatalf("unexpected plan changes: %+v", planAdd.Changes)
	}
	opAdd, err := facade.PublishChange(ctx, PublishInput{
		RequestID: "pub-fwd-add",
		PlanID:    planAdd.ID,
		Token:     planAdd.Token,
	})
	if err != nil || opAdd.Status != "succeeded" {
		t.Fatalf("publish add failed: op=%+v err=%v", opAdd, err)
	}

	// 4. Deleting a group still referenced by forward_rules must be rejected.
	cfgAfterAdd, _ := facade.backend.config(ctx)
	if _, err := facade.PlanChange(ctx, PlanInput{
		RequestID:     "del-referenced-group",
		Base:          versions(cfgAfterAdd),
		SchemaVersion: SchemaVersion,
		Edits:         []Edit{{Kind: "delete_group", GroupID: "1@chatroom"}},
	}); err == nil || !strings.Contains(err.Error(), "forward_rules") {
		t.Fatalf("expected forward_rules validation error when deleting referenced target group, got %v", err)
	}

	// 5. Clearing forward_rules to [] verifies even when Dispatcher omits empty forward_rules via omitempty.
	planClear := planEdit(t, facade, Edit{
		Kind:  "field",
		Scope: Scope{Kind: "global"},
		Field: "forward_rules",
		Value: json.RawMessage(`[]`),
	})
	// Simulate upstream Dispatcher omitting empty forward_rules on write readback.
	origHandler := stub.ServeHTTP
	_ = origHandler
	opClear, err := facade.PublishChange(ctx, PublishInput{
		RequestID: "pub-fwd-clear",
		PlanID:    planClear.ID,
		Token:     planClear.Token,
	})
	if err != nil || opClear.Status != "succeeded" {
		t.Fatalf("publish clear failed: op=%+v err=%v", opClear, err)
	}

	// Simulate omitted forward_rules in stub and verify matchPatch still considers [] equal to omitted.
	delete(stub.root["rules"].(map[string]any), "forward_rules")
	matched, err := matchPatch(snapshot{root: stub.root, rules: stub.root["rules"].(map[string]any)}, map[string]any{
		"forward_rules": []any{},
	})
	if err != nil || !matched {
		t.Fatalf("expected matchPatch to treat omitted forward_rules as [], matched=%v err=%v", matched, err)
	}

	// 6. PlanRollback on planAdd's beforeSnapshot (which had empty forward_rules) restores empty forward_rules.
	planAdd2 := planEdit(t, facade, Edit{
		Kind:  "field",
		Scope: Scope{Kind: "global"},
		Field: "forward_rules",
		Value: json.RawMessage(`[{"id":"v2ex-to-wx-2","enabled":true,"source_container_id":"v2ex:t-100002","target_container_id":"2@chatroom"}]`),
	})
	if op, err := facade.PublishChange(ctx, PublishInput{RequestID: "pub-fwd-add-2", PlanID: planAdd2.ID, Token: planAdd2.Token}); err != nil || op.Status != "succeeded" {
		t.Fatalf("publish add2 failed: op=%+v err=%v", op, err)
	}
	cfgNow, _ := facade.backend.config(ctx)
	rbPlan, err := facade.PlanRollback(ctx, RollbackInput{
		RequestID: "rb-fwd-add-2",
		ChangeID:  planAdd2.ID,
		Base:      versions(cfgNow),
	})
	if err != nil {
		t.Fatalf("PlanRollback failed: %v", err)
	}
	opRB, err := facade.PublishChange(ctx, PublishInput{
		RequestID: "pub-rb-fwd-add-2",
		PlanID:    rbPlan.ID,
		Token:     rbPlan.Token,
	})
	if err != nil || opRB.Status != "succeeded" {
		t.Fatalf("publish rollback failed: op=%+v err=%v", opRB, err)
	}
	if rulesList, _ := stub.root["rules"].(map[string]any)["forward_rules"].([]any); len(rulesList) != 0 {
		t.Fatalf("expected empty forward_rules after rollback, got %+v", rulesList)
	}
}
