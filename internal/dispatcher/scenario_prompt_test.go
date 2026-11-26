package dispatcher

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuiltinScenarioPresetsAndGoalToneRendering(t *testing.T) {
	cfg := supportTestConfig()
	presets := []struct {
		name string
		want string
	}{
		{"chat", "【场景策略：社群日常交流】"},
		{"tech_community", "【场景策略：技术交流】"},
		{"product_support", "【场景策略：产品客服与反馈】"},
		{"soft_promo", "【场景策略：自然推荐】"},
		{"public_comment", "【场景策略：公开评论区回复】"},
		{"feedback_intake", "【场景策略：需求与缺陷收集】"},
		{"test_echo", "【场景策略：测试验证】"},
	}
	for _, tc := range presets {
		t.Run(tc.name, func(t *testing.T) {
			rule := cfg.Rules.GroupRules[groupA]
			rule.Scenario = tc.name
			rule.ResponseGoal = "先解答核心问题，再给下一步"
			rule.Tone = "温和真诚、克制短句"
			b := batch{GroupID: groupA, Messages: []RelayMessage{testMessage(groupA, "s-"+tc.name)}, Rule: rule, Config: cfg}
			prompt := buildPrompt(b)
			if !strings.Contains(prompt, tc.want) ||
				!strings.Contains(prompt, "本轮目标：先解答核心问题，再给下一步") ||
				!strings.Contains(prompt, "语气基调：温和真诚、克制短句") {
				t.Fatalf("scenario %s missing guide/goal/tone in prompt", tc.name)
			}
		})
	}

	legacy := batch{GroupID: groupA, Messages: []RelayMessage{testMessage(groupA, "legacy")}, Rule: cfg.Rules.GroupRules[groupA], Config: cfg}
	if sec := scenarioPromptSection(legacy, resolveMounts(cfg, legacy.Rule)); sec != "" {
		t.Fatalf("expected empty scenarioPromptSection for legacy config, got %q", sec)
	}
}

func TestPlatformAwareFramingForBilibiliAndWeb(t *testing.T) {
	cases := []struct {
		id       string
		platform string
	}{
		{"bilibili:BV1example0001", "平台：bilibili"},
		{"web:demo-topic", "平台：web"},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			cfg := biliTestConfig()
			cfg.ActionEndpoints["web"] = ActionEndpointConfig{URL: "http://127.0.0.1:18093", TimeoutSec: 15}
			rule := GroupRule{
				Enabled:       true,
				TargetGroupID: tc.id,
				Target:        "Demo Channel",
				HermesProfile: "demo-channel",
				Scenario:      "public_comment",
				MaxReplyRunes: 220,
			}
			cfg.Rules.GroupWhitelist = []string{tc.id}
			cfg.Rules.GroupRules = map[string]GroupRule{tc.id: rule}
			cfg.Rules.GroupProfiles = map[string]string{tc.id: "示例频道背景"}
			if err := cfg.normalizeAndValidate(); err != nil {
				t.Fatal(err)
			}
			resolved, _ := cfg.route(tc.id)
			b := batch{GroupID: tc.id, Messages: []RelayMessage{testMessage(tc.id, "p1")}, Rule: resolved, Config: cfg}
			prompt := buildPrompt(b)
			if !strings.Contains(prompt, tc.platform) ||
				!strings.Contains(prompt, "目标回复位置由调度器决定") ||
				!strings.Contains(prompt, "保持自然、完整的中文标点") {
				t.Fatalf("missing non-WeChat framing in prompt: %s", prompt)
			}
			for _, forbidden := range []string{"回复结尾绝不要带中文句号", "目标群由调度器决定", "短句(20字内)回复不超过30字", "【本轮搜索边界】本产品群"} {
				if strings.Contains(prompt, forbidden) {
					t.Fatalf("non-WeChat prompt unexpectedly contained WeChat wording %q", forbidden)
				}
			}
			if mc := modelCurrencyContext(b); strings.Contains(mc, "本群") {
				t.Fatalf("non-WeChat modelCurrencyContext contained group phrasing: %s", mc)
			}
		})
	}
}

func TestSupportInstructionsModeReplaceAndValidation(t *testing.T) {
	cfg := supportTestConfig()
	cfg.localPolicy.Support.InstructionsMode = "replace"
	cfg.localPolicy.Support.instructions = "PRIVATE_SUPPORT_REPLACE_BODY"
	rule, _ := cfg.route(supportGroup)
	prompt := supportContext(batch{GroupID: supportGroup, Rule: rule, Config: cfg})
	if !strings.Contains(prompt, "【客服安全与证据边界】") ||
		!strings.Contains(prompt, "不需要@") ||
		!strings.Contains(prompt, "负责人和排期均未确认") ||
		!strings.Contains(prompt, "PRIVATE_SUPPORT_REPLACE_BODY") {
		t.Fatalf("replace mode missing core safety or custom body: %s", prompt)
	}
	if strings.Contains(prompt, "【客服规则，优先于闲聊静默与预热规则】") {
		t.Fatalf("replace mode failed to omit duplicated default support boilerplate")
	}

	// Missing instructions_file with replace mode must fail validation.
	bad := supportTestConfig().localPolicy
	bad.Support.InstructionsMode = "replace"
	bad.Support.InstructionsFile = ""
	if err := bad.validate(); err == nil {
		t.Fatal("accepted support.instructions_mode=replace without instructions_file")
	}
	bad.Support.InstructionsFile = filepath.Join(t.TempDir(), "inst.md")
	bad.Support.InstructionsMode = "invalid"
	if err := bad.validate(); err == nil {
		t.Fatal("accepted unsupported support.instructions_mode")
	}
}

func TestLocalPolicyScenariosModesPermissionsAndNoLeakage(t *testing.T) {
	dir := t.TempDir()
	promptPath := filepath.Join(dir, "scenario-custom.md")
	if err := os.WriteFile(promptPath, []byte("PRIVATE_SCENARIO_PLAYBOOK_BODY"), 0600); err != nil {
		t.Fatal(err)
	}
	policy := &localPolicy{
		Version: 1,
		Identities: map[string]localIdentity{
			groupA: {Profile: "demo-group-a", MemoryUser: "demo-mem-a", VectorCollection: "demo_vec_a"},
		},
		Scenarios: map[string]scenarioPolicy{
			"tech_community": {
				PromptFile:    promptPath,
				PromptMode:    "append",
				Skills:        []string{"demo-skill"},
				Tools:         []string{"web_search"},
				MaxReplyRunes: 120,
			},
			"public_comment": {
				PromptFile:    promptPath,
				PromptMode:    "replace",
				Skills:        []string{"demo-comment-skill"},
				MaxReplyRunes: 180,
			},
		},
	}
	policyPath := filepath.Join(dir, "policy.json")
	data, _ := json.Marshal(policy)
	if err := os.WriteFile(policyPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadLocalPolicy(policyPath)
	if err != nil {
		t.Fatalf("loadLocalPolicy failed: %v", err)
	}

	// Verify 0600 requirement on scenario prompt_file.
	if err := os.Chmod(promptPath, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadLocalPolicy(policyPath); err == nil {
		t.Fatal("accepted scenario prompt_file with permissive mode 0644")
	}
	if err := os.Chmod(promptPath, 0600); err != nil {
		t.Fatal(err)
	}

	// Verify append vs replace prompt rendering and budget clamping.
	cfg := testConfig()
	cfg.localPolicy = loaded
	cfg.Hermes.ProfilesDir = dir
	cfg.Rules.GroupRules[groupA] = GroupRule{
		Enabled:       true,
		TargetGroupID: groupA,
		Target:        "target-a",
		HermesProfile: "demo-group-a",
		MaxReplyRunes: 400,
		Scenario:      "tech_community",
	}
	appendPrompt := buildPrompt(batch{GroupID: groupA, Rule: cfg.Rules.GroupRules[groupA], Config: cfg, Messages: []RelayMessage{testMessage(groupA, "a1")}})
	if !strings.Contains(appendPrompt, "【场景策略：技术交流】") ||
		!strings.Contains(appendPrompt, "PRIVATE_SCENARIO_PLAYBOOK_BODY") ||
		!strings.Contains(appendPrompt, "允许技能：demo-skill") ||
		!strings.Contains(appendPrompt, "其余最多120字") {
		t.Fatalf("append scenario rendering failed: %s", appendPrompt)
	}

	cfgReplace := cfg
	rReplace := cfg.Rules.GroupRules[groupA]
	rReplace.Scenario = "public_comment"
	cfgReplace.Rules.GroupRules[groupA] = rReplace
	replacePrompt := buildPrompt(batch{GroupID: groupA, Rule: rReplace, Config: cfgReplace, Messages: []RelayMessage{testMessage(groupA, "a2")}})
	if strings.Contains(replacePrompt, "【场景策略：公开评论区回复】") || !strings.Contains(replacePrompt, "PRIVATE_SCENARIO_PLAYBOOK_BODY") {
		t.Fatalf("replace scenario mode did not suppress built-in preset: %s", replacePrompt)
	}

	// Verify private scenario prompt content and file path do NOT leak via /api/config or snapshots.
	d := newTestDispatcher(t, cfg)
	b := batch{GroupID: groupA, Config: d.cfg, Rule: cfg.Rules.GroupRules[groupA], Policy: policyID(d.cfg)}
	d.mu.Lock()
	snapshotID, err := d.retainConfigSnapshotLocked(b, time.Now())
	d.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	snap := traceRequest(d, "/api/trace/config-snapshots/"+snapshotID)
	read := httptest.NewRecorder()
	d.Handler().ServeHTTP(read, httptest.NewRequest(http.MethodGet, "http://127.0.0.1/api/config", nil))
	if snap.Code != http.StatusOK || read.Code != http.StatusOK {
		t.Fatal("snapshot or config API failed")
	}
	for _, body := range []string{snap.Body.String(), read.Body.String()} {
		for _, secret := range []string{"PRIVATE_SCENARIO_PLAYBOOK_BODY", promptPath, "prompt_file"} {
			if strings.Contains(body, secret) {
				t.Fatalf("leaked private scenario data %q in API response", secret)
			}
		}
	}
}

func TestKnowledgeBasesFailClosedAndSupportRestrictions(t *testing.T) {
	dir := t.TempDir()
	kbFile := filepath.Join(dir, "kb-doc.md")
	missingFile := filepath.Join(dir, "secret-missing-kb.md")
	if err := os.WriteFile(kbFile, []byte("SYNTHETIC_KB_FACT"), 0600); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	rule := cfg.Rules.GroupRules[groupA]
	rule.KnowledgeBases = []string{"demo-kb"}
	cfg.Rules.GroupRules[groupA] = rule
	if err := cfg.normalizeAndValidate(); err == nil {
		t.Fatal("accepted GroupRule.KnowledgeBases without localPolicy")
	}

	cfg.localPolicy = &localPolicy{
		Version: 1,
		KnowledgeBases: map[string]knowledgeBasePolicy{
			"demo-kb":    {Files: []string{kbFile}, MaxBytes: 4096},
			"missing-kb": {Files: []string{missingFile}, MaxBytes: 4096},
		},
	}
	if err := cfg.normalizeAndValidate(); err != nil {
		t.Fatalf("expected valid KB reference to pass: %v", err)
	}

	rule.KnowledgeBases = []string{"unknown-kb"}
	cfg.Rules.GroupRules[groupA] = rule
	if err := cfg.normalizeAndValidate(); err == nil {
		t.Fatal("accepted unknown knowledge base ID")
	}

	// Verify KB rendering and no path leak on missing file.
	rule.KnowledgeBases = []string{"demo-kb", "missing-kb"}
	kbText := mountedKnowledgeContext(batch{GroupID: groupA, Config: cfg, Rule: rule}, resolveMounts(cfg, rule))
	if !strings.Contains(kbText, "SYNTHETIC_KB_FACT") ||
		!strings.Contains(kbText, "受控知识资料不可用") ||
		strings.Contains(kbText, missingFile) {
		t.Fatalf("unexpected KB context output: %s", kbText)
	}

	// SupportMode must reject skills & disallowed tools in both rule and mapped scenario.
	sCfg := supportTestConfig()
	sRule := sCfg.Rules.GroupRules[supportGroup]
	sRule.Skills = []string{"demo-skill"}
	if err := sCfg.validateSupportRule(supportGroup, sRule); err == nil {
		t.Fatal("support mode accepted Skills on GroupRule")
	}
	sRule.Skills = nil
	sRule.Tools = []string{"web_search", "bash"}
	if err := sCfg.validateSupportRule(supportGroup, sRule); err == nil {
		t.Fatal("support mode accepted disallowed tool on GroupRule")
	}
	sRule.Tools = []string{"web_search", "web_fetch"}
	sCfg.localPolicy.Scenarios = map[string]scenarioPolicy{
		"bad-support-scenario": {Skills: []string{"demo-skill"}},
	}
	sRule.Scenario = "bad-support-scenario"
	if err := sCfg.validateSupportRule(supportGroup, sRule); err == nil {
		t.Fatal("support mode accepted scenario with Skills")
	}
}

func TestBackendRequestReceivesResolvedMounts(t *testing.T) {
	cfg := testConfig()
	cfg.localPolicy = &localPolicy{
		Version: 1,
		Scenarios: map[string]scenarioPolicy{
			"tech_community": {
				Skills: []string{"scenario-skill", "shared-skill"},
				Tools:  []string{"web_search"},
			},
		},
		KnowledgeBases: map[string]knowledgeBasePolicy{
			"demo-kb": {Files: []string{filepath.Join(t.TempDir(), "k.md")}},
		},
	}
	rule := cfg.Rules.GroupRules[groupA]
	rule.Scenario = "tech_community"
	rule.Skills = []string{"rule-skill", "shared-skill"}
	rule.Tools = []string{"web_fetch"}
	rule.KnowledgeBases = []string{"demo-kb"}
	cfg.Rules.GroupRules[groupA] = rule

	d := newTestDispatcher(t, cfg)
	b := batch{
		GroupID:  groupA,
		TraceID:  "trace-mounts",
		Rule:     rule,
		Config:   d.cfg,
		Messages: []RelayMessage{testMessage(groupA, "m-mounts")},
	}
	req := d.backendRequest(b)
	if req.Scenario != "tech_community" ||
		strings.Join(req.Skills, ",") != "rule-skill,shared-skill,scenario-skill" ||
		strings.Join(req.Tools, ",") != "web_fetch,web_search" ||
		strings.Join(req.KnowledgeBases, ",") != "demo-kb" {
		t.Fatalf("unexpected BackendRequest mounts: %+v", req)
	}
}

func TestCatalogEndpointReturnsSanitizedLogicalIDsOnly(t *testing.T) {
	dir := t.TempDir()
	promptFile := filepath.Join(dir, "secret-playbook.md")
	kbFile := filepath.Join(dir, "secret-kb.md")
	for _, p := range []string{promptFile, kbFile} {
		if err := os.WriteFile(p, []byte("PRIVATE_SECRET_BODY"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	cfg := testConfig()
	cfg.localPolicy = &localPolicy{
		Version: 1,
		Identities: map[string]localIdentity{
			groupA: {Profile: "isolated-group-a", MemoryUser: "demo-mem", VectorCollection: "demo_vec"},
		},
		Scenarios: map[string]scenarioPolicy{
			"tech_community": {
				PromptFile:     promptFile,
				KnowledgeFiles: []string{kbFile},
				Skills:         []string{"debug-skill"},
				Tools:          []string{"web_search"},
			},
			"custom_playbook": {
				PromptFile: promptFile,
				Skills:     []string{"custom-skill"},
			},
		},
		KnowledgeBases: map[string]knowledgeBasePolicy{
			"demo-kb": {Files: []string{kbFile}, MaxBytes: 4096},
		},
	}
	cfg.Rules.GroupRules[groupA] = GroupRule{
		Enabled:        true,
		TargetGroupID:  groupA,
		Target:         "target-a",
		HermesProfile:  "isolated-group-a",
		Scenario:       "custom_playbook",
		Skills:         []string{"rule-skill"},
		Tools:          []string{"web_fetch"},
		KnowledgeBases: []string{"demo-kb"},
	}
	d := newTestDispatcher(t, cfg)

	rec := httptest.NewRecorder()
	d.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://127.0.0.1/api/catalog", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on GET /api/catalog, got %d %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, secret := range []string{dir, promptFile, kbFile, "PRIVATE_SECRET_BODY", "isolated-group-a", "demo_vec"} {
		if strings.Contains(body, secret) {
			t.Fatalf("catalog leaked private detail %q in %s", secret, body)
		}
	}
	var cat CatalogSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &cat); err != nil {
		t.Fatal(err)
	}
	if len(cat.Scenarios) != 8 || len(cat.KnowledgeBases) != 1 || cat.KnowledgeBases[0].ID != "demo-kb" ||
		len(cat.Skills) != 3 || len(cat.Tools) < 2 {
		t.Fatalf("unexpected catalog contents: %+v", cat)
	}

	postRec := httptest.NewRecorder()
	d.Handler().ServeHTTP(postRec, httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/catalog", nil))
	if postRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on POST /api/catalog, got %d", postRec.Code)
	}
}
