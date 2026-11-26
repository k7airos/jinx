package dispatcher

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestFlatPatchReplacesMapsWithoutRetainingDeletedEntries(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	before := d.configSnapshot()
	remaining := before.Rules.GroupRules[groupB]
	body, err := json.Marshal(map[string]any{
		"group_rules":     map[string]GroupRule{groupB: remaining},
		"group_whitelist": []string{groupB},
		"group_profiles":  map[string]string{groupB: "replacement-background"},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := configRequest(d, http.MethodPost, string(body), policyID(before))
	if response.Code != http.StatusOK {
		t.Fatalf("delete patch: %d %s", response.Code, response.Body.String())
	}
	after := d.configSnapshot()
	if _, found := after.Rules.GroupRules[groupA]; found {
		t.Fatal("deleted group rule retained")
	}
	if _, found := after.Rules.GroupProfiles[groupA]; found {
		t.Fatal("deleted group profile retained")
	}
	if len(after.Rules.GroupRules) != 1 || after.Rules.GroupProfiles[groupB] != "replacement-background" {
		t.Fatal("replacement map did not match the requested contents")
	}
	if after.Rules.BatchSize != before.Rules.BatchSize || after.Rules.BotNames[0] != before.Rules.BotNames[0] {
		t.Fatal("omitted unrelated fields changed")
	}
	response = configRequest(d, http.MethodPost, `{"group_rules":{},"group_whitelist":[],"group_profiles":{}}`, policyID(after))
	if response.Code != http.StatusOK {
		t.Fatalf("empty maps: %d %s", response.Code, response.Body.String())
	}
	after = d.configSnapshot()
	if len(after.Rules.GroupRules) != 0 || len(after.Rules.GroupProfiles) != 0 {
		t.Fatal("explicit empty maps retained old keys")
	}
}
