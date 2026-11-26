package dispatcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func demoReminderPolicy() reminderPolicy {
	return reminderPolicy{Enabled: true, AppPath: "/synthetic/DemoDesk Reminders.app", Application: "DemoDesk Reminders", BundleID: "org.example.demodesk-reminders", CalendarTitle: "DemoDesk Feedback", SourceTitle: "Demo Cloud", SourceType: 2, RequireCloudVerified: true, TitlePrefix: "DemoDesk", MarkerPrefix: "demo:feedback:"}
}

func demoReminderResponse() reminderResponse {
	policy := demoReminderPolicy()
	return reminderResponse{Application: policy.Application, BundleID: policy.BundleID, Calendar: reminderCalendar{ID: "cloud-list", Title: policy.CalendarTitle, SourceID: "cloud", SourceType: policy.SourceType, SourceTitle: policy.SourceTitle, ICloudVerified: true}}
}

func writeTestPolicy(t *testing.T, policy *localPolicy) string {
	t.Helper()
	if policy == nil {
		return ""
	}
	copy := *policy
	copy.Support = policy.Support
	for _, item := range []struct {
		text string
		path *string
	}{{policy.communication, &copy.CommunicationFile}, {policy.Support.instructions, &copy.Support.InstructionsFile}} {
		if item.text == "" {
			continue
		}
		*item.path = filepath.Join(t.TempDir(), "trusted-rules.md")
		if err := os.WriteFile(*item.path, []byte(item.text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	data, err := json.Marshal(copy)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "local-policy.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLocalPolicyStrictPrivateAndSideEffectFree(t *testing.T) {
	policy := supportTestConfig().localPolicy
	path := writeTestPolicy(t, policy)
	loaded, err := loadLocalPolicy(path)
	if err != nil || loaded.Support.GroupID != supportGroup {
		t.Fatal("valid local policy rejected", err)
	}
	for _, raw := range []string{`{"version":1,"unexpected":"secret"}`, `{"version":1} {"version":1}`, `{"version":2}`} {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := loadLocalPolicy(path); err == nil {
			t.Fatal("invalid schema accepted")
		}
	}
	path = writeTestPolicy(t, policy)
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLocalPolicy(path); err == nil {
		t.Fatal("insecure policy accepted")
	}
	link := filepath.Join(t.TempDir(), "policy-link")
	if err := os.Symlink(writeTestPolicy(t, policy), link); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLocalPolicy(link); err == nil {
		t.Fatal("policy symlink accepted")
	}
	if _, err := loadLocalPolicy(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("missing policy accepted")
	}
}

func TestDefaultSupportAndRemindersFailClosed(t *testing.T) {
	cfg := supportTestConfig()
	cfg.localPolicy = nil
	if err := cfg.normalizeAndValidate(); err == nil {
		t.Fatal("support enabled without local policy")
	}
	d := &Dispatcher{}
	called := false
	d.reminderRunner = func(reminderRequest) (reminderResponse, error) {
		called = true
		return demoReminderResponse(), nil
	}
	if _, err := d.callReminders(reminderRequest{Operation: "read"}); err == nil || called {
		t.Fatal("disabled reminders reached transport")
	}
	if _, err := d.syncSupport(nil, true); err == nil {
		t.Fatal("disabled sync accepted")
	}
}

func TestLocalPolicyCannotBeReadOrRemovedByConfigAPI(t *testing.T) {
	cfg := supportTestConfig()
	cfg.localPolicy.communication = "PRIVATE_COMMUNICATION_SENTINEL"
	cfg.localPolicy.Support.instructions = "PRIVATE_SUPPORT_SENTINEL"
	d := newTestDispatcher(t, cfg)
	if !strings.Contains(trustedCommunication(d.cfg), "PRIVATE_COMMUNICATION_SENTINEL") {
		t.Fatal("private communication was not loaded")
	}
	b := batch{GroupID: supportGroup, Config: d.cfg, Rule: d.cfg.resolveRule(supportGroup, d.cfg.Rules.GroupRules[supportGroup]), Policy: policyID(d.cfg)}
	d.mu.Lock()
	snapshotID, err := d.retainConfigSnapshotLocked(b, time.Now())
	d.mu.Unlock()
	if err != nil {
		t.Fatal("config snapshot failed")
	}
	snapshot := traceRequest(d, "/api/trace/config-snapshots/"+snapshotID)
	if snapshot.Code != http.StatusOK {
		t.Fatal("snapshot read failed")
	}
	for _, secret := range []string{"local_policy", "DemoDesk Feedback", "demo-support-memory", "PRIVATE_", "app_path", "knowledge_files"} {
		if strings.Contains(snapshot.Body.String(), secret) {
			t.Fatal("local policy leaked in config snapshot")
		}
	}
	for _, method := range []string{http.MethodPut, http.MethodPost} {
		read := httptest.NewRecorder()
		d.Handler().ServeHTTP(read, httptest.NewRequest(http.MethodGet, "http://127.0.0.1/api/config", nil))
		if read.Code != http.StatusOK {
			t.Fatal("configuration read failed")
		}
		for _, secret := range []string{"localPolicy", "local_policy", "DemoDesk Feedback", "demo-support-memory", "PRIVATE_", "app_path", "knowledge_files"} {
			if strings.Contains(read.Body.String(), secret) {
				t.Fatal("local policy leaked in readback")
			}
		}
		var body []byte
		if method == http.MethodPut {
			data, _ := json.Marshal(d.configSnapshot())
			var next Config
			if err := json.Unmarshal(data, &next); err != nil {
				t.Fatal(err)
			}
			rule := next.Rules.GroupRules[supportGroup]
			rule.SupportMode = false
			next.Rules.GroupRules[supportGroup] = rule
			body, _ = json.Marshal(next)
		} else {
			body = []byte(`{"group_rules":{}}`)
		}
		request := httptest.NewRequest(method, "http://127.0.0.1/api/config", strings.NewReader(string(body)))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("If-Match", read.Header().Get("ETag"))
		response := httptest.NewRecorder()
		d.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("policy downgrade accepted: %d", response.Code)
		}
	}
}

func TestLocalPolicyValidConfigUpdatesRetainAuthorization(t *testing.T) {
	d := newTestDispatcher(t, supportTestConfig())
	for _, method := range []string{http.MethodPut, http.MethodPost} {
		before := d.configSnapshot()
		body := `{"batch_size":2}`
		if method == http.MethodPut {
			data, _ := json.Marshal(before)
			body = string(data)
		}
		response := configRequest(d, method, body, policyID(before))
		if response.Code != http.StatusOK {
			t.Fatal("valid configuration update rejected", response.Code)
		}
		if d.configSnapshot().localPolicy != before.localPolicy {
			t.Fatal("local authorization changed after API update")
		}
	}
}

func TestLocalPolicySupportAuthorizationUsesConfiguredValues(t *testing.T) {
	cfg := supportTestConfig()
	group := "400@chatroom"
	rule := cfg.Rules.GroupRules[supportGroup]
	delete(cfg.Rules.GroupRules, supportGroup)
	rule.SelfSenderIDs = []string{"demo-reconfigured-self"}
	cfg.Rules.GroupRules[group] = rule
	cfg.Rules.GroupWhitelist = []string{groupA, groupB, group}
	identity := cfg.localPolicy.Identities[supportGroup]
	delete(cfg.localPolicy.Identities, supportGroup)
	cfg.localPolicy.Identities[group] = identity
	cfg.localPolicy.Support.GroupID = group
	cfg.localPolicy.Support.SelfSenderIDs = rule.SelfSenderIDs
	if err := cfg.normalizeAndValidate(); err != nil {
		t.Fatal("configured support route rejected", err)
	}
	for _, mutate := range []func(*GroupRule){
		func(rule *GroupRule) { rule.SupportMode = false },
		func(rule *GroupRule) { rule.SelfSenderIDs = []string{"demo-wrong-self"} },
		func(rule *GroupRule) { rule.TargetGroupID = groupB },
		func(rule *GroupRule) { rule.MentionOnly = true },
		func(rule *GroupRule) { rule.SupportSinceMS = 0 },
		func(rule *GroupRule) { rule.PromoKeywords = []string{"demo"} },
	} {
		copy := rule
		mutate(&copy)
		if err := cfg.validateSupportRule(group, copy); err == nil {
			t.Fatal("unsafe support override accepted")
		}
	}
}

func TestLocalPolicySourcesAreExplicitAndLoadedFromFixture(t *testing.T) {
	cfg := supportTestConfig()
	root := t.TempDir()
	knowledge := filepath.Join(root, "knowledge.md")
	document := filepath.Join(root, "document.md")
	code := filepath.Join(root, "example_limits.txt")
	for path, text := range map[string]string{
		knowledge: "SYNTHETIC_PRIMARY DemoDesk Windows 0.9.0 2026-01-01",
		document:  "SYNTHETIC_SECONDARY 2025-12-31",
		code:      "public enum PrivateSource { exampleDefaultCount = 12_345; exampleRetentionDays = 42; exampleMaximumDays = 365 }",
	} {
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cfg.localPolicy.Support.KnowledgeFiles = []string{knowledge}
	cfg.localPolicy.Support.DocumentFiles = []string{document}
	cfg.localPolicy.Support.ReleaseFiles = []string{knowledge}
	cfg.localPolicy.Support.CodeFactsFile = code
	cfg.localPolicy.Support.CodeFactRules = []codeFactRule{{Label: "示例默认记录条数", Pattern: `exampleDefaultCount = ([0-9_]+)`}}
	d := newTestDispatcher(t, cfg)
	b := batch{GroupID: supportGroup, Config: d.cfg, Rule: d.cfg.Rules.GroupRules[supportGroup]}
	prompt := supportContext(b)
	if !strings.Contains(prompt, "SYNTHETIC_PRIMARY") || !strings.Contains(prompt, "SYNTHETIC_SECONDARY") || !strings.Contains(prompt, "12345") || strings.Contains(prompt, "PrivateSource") || strings.Contains(prompt, root) {
		t.Fatal("explicit source fixture was not loaded safely")
	}
	cfg.localPolicy.Support.CodeFactRules = nil
	if closed := supportCodeFacts(batch{GroupID: supportGroup, Config: cfg}); strings.Contains(closed, "12345") || !strings.Contains(closed, "受控代码核验未配置提取规则") {
		t.Fatal("missing code fact rules did not fail closed")
	}
	if !supportDatedAnswer(b, "截至2026-01-01的虚构发布记录，DemoDesk Windows 0.9.0") {
		t.Fatal("allowed release source was not used")
	}
	b.Config.localPolicy = nil
	if strings.Contains(supportContext(b), "SYNTHETIC_PRIMARY") || supportDatedAnswer(b, "截至2026-01-01的虚构发布记录") {
		t.Fatal("sources loaded without local authorization")
	}
	called := false
	if _, err := launchReminderAppWith(context.Background(), reminderPolicy{}, reminderRequest{Operation: "upsert"}, func(context.Context, string) error {
		called = true
		return nil
	}); err == nil || called {
		t.Fatal("disabled app transport did not fail closed")
	}
}

func TestLocalPolicyCommunicationModeAndCodeFactValidation(t *testing.T) {
	cfg := supportTestConfig()
	cfg.localPolicy.CommunicationMode = "replace"
	cfg.localPolicy.communication = "PRIVATE_REPLACEMENT_STYLE"
	if got := trustedCommunication(cfg); !strings.Contains(got, "PRIVATE_REPLACEMENT_STYLE") || !strings.Contains(got, "明确受话人优先") || strings.Contains(got, "面对非技术用户") || !strings.HasPrefix(got, "【助手沟通规则】") || !strings.HasSuffix(got, "【助手沟通规则结束】") {
		t.Fatalf("unexpected replace communication output: %s", got)
	}
	cfg.localPolicy.CommunicationMode = "append"
	if got := trustedCommunication(cfg); !strings.Contains(got, "PRIVATE_REPLACEMENT_STYLE") || !strings.Contains(got, "面对非技术用户") {
		t.Fatalf("unexpected append communication output: %s", got)
	}
	if err := cfg.localPolicy.validate(); err != nil {
		t.Fatal("unexpected policy validate error with empty CommunicationFile in append mode", err)
	}
	cfg.localPolicy.CommunicationMode = "replace"
	cfg.localPolicy.CommunicationFile = ""
	if err := cfg.localPolicy.validate(); err == nil {
		t.Fatal("accepted replace communication_mode without communication_file")
	}
	cfg.localPolicy.CommunicationFile = "/tmp/fake-comm.md"
	cfg.localPolicy.CommunicationMode = "overwrite"
	if err := cfg.localPolicy.validate(); err == nil {
		t.Fatal("accepted unsupported communication_mode")
	}
	cfg.localPolicy.CommunicationMode = ""
	for _, bad := range [][]codeFactRule{
		{{Label: "bad", Pattern: `[`}},
		{{Label: "no-group", Pattern: `value = [0-9]+`}},
		{{Label: "two-groups", Pattern: `(a)(b)`}},
		{{Label: "   ", Pattern: `(a)`}},
	} {
		cfg.localPolicy.Support.CodeFactRules = bad
		if err := cfg.localPolicy.validate(); err == nil {
			t.Fatalf("accepted invalid code fact rules: %+v", bad)
		}
	}
	cfg.localPolicy.Support.CodeFactRules = nil
	cfg.localPolicy.Support.CreatorSenderIDs = []string{"synthetic-creator"}
	if err := cfg.localPolicy.validate(); err != nil {
		t.Fatalf("rejected valid creator_sender_ids: %v", err)
	}
	cfg.localPolicy.Support.CreatorSenderIDs = []string{"   "}
	if err := cfg.localPolicy.validate(); err == nil {
		t.Fatal("accepted blank creator_sender_ids entry")
	}
	tooMany := make([]string, 17)
	for i := range tooMany {
		tooMany[i] = "creator-id"
	}
	cfg.localPolicy.Support.CreatorSenderIDs = tooMany
	if err := cfg.localPolicy.validate(); err == nil {
		t.Fatal("accepted excessive creator_sender_ids entries")
	}
}

func TestOperatorPolicyValidationOnly(t *testing.T) {
	path := os.Getenv("DISPATCHER_POLICY_CHECK")
	configPath := os.Getenv("DISPATCHER_CONFIG_CHECK")
	if path == "" || configPath == "" {
		t.Skip("set DISPATCHER_POLICY_CHECK and DISPATCHER_CONFIG_CHECK for read-only policy/config validation")
	}
	policy, err := loadLocalPolicy(path)
	if err != nil {
		t.Fatal("operator policy validation failed")
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal("operator config unavailable")
	}
	if _, err := decodeConfigWithPolicy(data, policy); err != nil {
		t.Fatal("operator config does not match local policy")
	}
}
