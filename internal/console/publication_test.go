package console

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func publicationRules(t *testing.T, value string) map[string]any {
	t.Helper()
	var rules map[string]any
	if err := json.Unmarshal([]byte(value), &rules); err != nil {
		t.Fatal(err)
	}
	return rules
}

func TestMatchPatchStoredJSONDefaults(t *testing.T) {
	sparse := publicationRules(t, `{"1@chatroom":{"target":"one","self_sender_ids":null,"allowed_types":[]}}`)
	expanded := publicationRules(t, `{"1@chatroom":{
		"target":"one","self_sender_ids":null,"allowed_types":[],
		"enabled":false,"dry_run":false,"mention_only":false,"support_mode":false,"promo_shadow":false,
		"target_group_id":"","hermes_profile":"","decision_context":"","sensitive_criteria":"","noise_criteria":"","safe_criteria":"",
		"support_since_ms":0,"batch_size":0,"idle_timeout_sec":0,"max_batch_wait_sec":0,"max_reply_runes":0,
		"reply_window_sec":0,"max_replies_in_window":0,"decision_threshold":0,"promo_daily_cap":0,
		"blocked_sender_ids":null,"keyword_blocklist":null,"regex_blocklist":null,"promo_keywords":[]
	}}`)
	for _, pair := range [][2]map[string]any{{sparse, expanded}, {expanded, sparse}} {
		matches, err := matchPatch(snapshot{rules: map[string]any{"group_rules": pair[1]}}, map[string]any{"group_rules": pair[0]})
		if err != nil || !matches {
			t.Fatalf("stored JSON defaults did not normalize symmetrically: %v %v", matches, err)
		}
	}
	if len(sparse["1@chatroom"].(map[string]any)) != 3 || len(expanded["1@chatroom"].(map[string]any)["promo_keywords"].([]any)) != 0 {
		t.Fatal("normalization mutated source JSON")
	}
}

func TestMatchPatchKeepsActualDifferences(t *testing.T) {
	for _, scenario := range []struct{ name, expected, actual string }{
		{"boolean", `"promo_shadow":false`, `"promo_shadow":true`},
		{"string", `"decision_context":""`, `"decision_context":"actual"`},
		{"number", `"batch_size":0`, `"batch_size":8`},
		{"effective_default", `"max_reply_runes":0`, `"max_reply_runes":500`},
		{"decimal", `"decision_threshold":0.5`, `"decision_threshold":0.8`},
		{"nonzero_missing", `"promo_daily_cap":2`, ``},
		{"nonempty_missing", `"safe_criteria":"keep"`, ``},
		{"list", `"blocked_sender_ids":null`, `"blocked_sender_ids":[]`},
		{"missing_list", `"keyword_blocklist":[]`, ``},
		{"promo_list", `"promo_keywords":["before"]`, ``},
		{"skills_list", `"skills":["skill-a"]`, ``},
		{"tools_list", `"tools":["web_search"]`, ``},
		{"kb_list", `"knowledge_bases":["kb-a"]`, ``},
		{"scenario_change", `"scenario":"chat"`, `"scenario":"tech_community"`},
		{"unknown_removed", `"future_field":null`, ``},
		{"unknown_added", ``, `"future_field":null`},
		{"unknown_changed", `"future_field":{"enabled":false}`, `"future_field":{"enabled":true}`},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			wrap := func(extra string) map[string]any {
				if extra != "" {
					extra = "," + extra
				}
				return publicationRules(t, `{"1@chatroom":{"target":"one","self_sender_ids":null,"allowed_types":null`+extra+`}}`)
			}
			matches, err := matchPatch(snapshot{rules: map[string]any{"group_rules": wrap(scenario.actual)}}, map[string]any{"group_rules": wrap(scenario.expected)})
			if err != nil || matches {
				t.Fatalf("actual difference was ignored: %v %v", matches, err)
			}
		})
	}
	for _, field := range []string{"self_sender_ids", "allowed_types"} {
		t.Run(field, func(t *testing.T) {
			expected := publicationRules(t, `{"1@chatroom":{"self_sender_ids":null,"allowed_types":null}}`)
			actual := publicationRules(t, `{"1@chatroom":{"self_sender_ids":null,"allowed_types":null}}`)
			actual["1@chatroom"].(map[string]any)[field] = []any{}
			matches, err := matchPatch(snapshot{rules: map[string]any{"group_rules": actual}}, map[string]any{"group_rules": expected})
			if err != nil || matches {
				t.Fatalf("null and [] collapsed for %s: %v %v", field, matches, err)
			}
		})
	}
	for _, rules := range []string{`{}`, `{"2@chatroom":{"self_sender_ids":null,"allowed_types":null}}`, `{"1@chatroom":{"self_sender_ids":null,"allowed_types":null},"2@chatroom":{"self_sender_ids":null,"allowed_types":null}}`} {
		expected := publicationRules(t, `{"1@chatroom":{"self_sender_ids":null,"allowed_types":null}}`)
		matches, err := matchPatch(snapshot{rules: map[string]any{"group_rules": publicationRules(t, rules)}}, map[string]any{"group_rules": expected})
		if err != nil || matches {
			t.Fatalf("group key difference was ignored: %s %v %v", rules, matches, err)
		}
	}
	for _, actual := range []map[string]any{{}, {"self_sender_ids": []any{}}, {"self_sender_ids": nil}} {
		matches, err := matchPatch(snapshot{rules: actual}, map[string]any{"self_sender_ids": nil})
		if err != nil || matches != reflect.DeepEqual(actual, map[string]any{"self_sender_ids": nil}) {
			t.Fatalf("global fields must retain exact JSON presence and list values: %v %v", matches, err)
		}
	}
}

func TestNormalizationFailsClosed(t *testing.T) {
	valid := publicationRules(t, `{"1@chatroom":{"self_sender_ids":null,"allowed_types":null}}`)
	for _, value := range []string{
		`null`, `[]`, `{"1@chatroom":null}`, `{"1@chatroom":[]}`,
		`{"1@chatroom":{"allowed_types":null}}`, `{"1@chatroom":{"self_sender_ids":null}}`,
		`{"1@chatroom":{"self_sender_ids":null,"allowed_types":null,"promo_shadow":null}}`,
		`{"1@chatroom":{"self_sender_ids":null,"allowed_types":null,"decision_context":null}}`,
		`{"1@chatroom":{"self_sender_ids":null,"allowed_types":null,"batch_size":"0"}}`,
		`{"1@chatroom":{"self_sender_ids":null,"allowed_types":["1"]}}`,
	} {
		t.Run(value, func(t *testing.T) {
			var invalid any
			if err := json.Unmarshal([]byte(value), &invalid); err != nil {
				t.Fatal(err)
			}
			for _, pair := range [][2]any{{valid, invalid}, {invalid, valid}} {
				matches, err := matchPatch(snapshot{rules: map[string]any{"group_rules": pair[1]}}, map[string]any{"group_rules": pair[0]})
				if err == nil || err.(*Problem).Code != "unsupported" || matches {
					t.Fatalf("unprovable normalization accepted: %v %v", matches, err)
				}
			}
		})
	}
}

func TestPlanRejectsAmbiguousGroupLists(t *testing.T) {
	for _, field := range []string{"self_sender_ids", "allowed_types"} {
		t.Run(field, func(t *testing.T) {
			facade, stub, _ := setup(t)
			delete(stub.root["rules"].(map[string]any)["group_rules"].(map[string]any)["2@chatroom"].(map[string]any), field)
			view, err := facade.GetGroup(context.Background(), "2@chatroom")
			if err != nil {
				t.Fatal(err)
			}
			group := view.Data.(GroupView)
			if group.Fields[field].Editable || group.Fields[field].Reason == "" {
				t.Fatal("ambiguous field is presented as safely editable")
			}
			_, err = facade.PlanChange(context.Background(), PlanInput{RequestID: "ambiguous", Base: group.Versions, SchemaVersion: SchemaVersion, Edits: []Edit{fieldEdit("1@chatroom", "target", `"changed"`)}})
			if err == nil || err.(*Problem).Code != "unsupported" || stub.writes != 0 {
				t.Fatalf("ambiguous group replacement accepted: %v", err)
			}
		})
	}
}

func TestPublicationUnprovableReadbackStaysUnknown(t *testing.T) {
	facade, stub, _ := setup(t)
	plan := planEdit(t, facade, fieldEdit("1@chatroom", "target", `"changed"`))
	stub.mode = "lostreadback"
	operation, err := facade.PublishChange(context.Background(), PublishInput{RequestID: "unprovable", PlanID: plan.ID, Token: plan.Token})
	if err != nil || operation.Status != "unknown" {
		t.Fatalf("missing readback: %+v %v", operation, err)
	}
	stub.mu.Lock()
	stub.mode = ""
	delete(stub.root["rules"].(map[string]any)["group_rules"].(map[string]any)["1@chatroom"].(map[string]any), "allowed_types")
	stub.mu.Unlock()
	operation, err = facade.GetOperation(context.Background(), operation.ID)
	if err != nil || operation.Status != "unknown" || operation.Components[0].PersistenceVerified != nil || !strings.Contains(operation.Summary, "cannot be normalized losslessly") || stub.writes != 1 {
		t.Fatalf("unprovable readback was not explicit or repeated the write: %+v %v", operation, err)
	}
}
