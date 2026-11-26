package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Uses synthetic release references, an isolated profile and a local HTTP sink.
// An optional saved backend result exercises the final output path without reruns.
func TestSupportReleaseKnowledgeFinalPayload(t *testing.T) {
	const question = "截至合成日期2026-01-01，虚构 DemoDesk Windows x64 0.9.0发布了吗，改了什么？macOS对应哪版？这些记录能证明某条反馈已修复或有排期吗？"
	path := os.Getenv("SUPPORT_RELEASE_RESULT")
	if path == "" {
		path = "testdata/demodesk-release-answer-synthetic-20260101.json"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result struct{ Text string }
	if err := json.Unmarshal(data, &result); err != nil || result.Text == "" {
		t.Fatal("saved backend result missing text", err)
	}
	answer := strings.TrimSpace(result.Text)
	var d *Dispatcher
	var final string
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Text    string `json:"text"`
			GroupID string `json:"group_id"`
			Target  string `json:"target"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.GroupID != supportGroup || body.Target != "demo-support" {
			t.Error("wrong release answer route")
		}
		final = body.Text
		echo := testMessage(supportGroup, "offline-release-echo")
		echo.IsEcho, echo.Content = true, final
		if err := d.handleIncomingMessage(echo, ""); err != nil {
			t.Error(err)
		}
		fmt.Fprint(w, "OK")
	})
	cfg := supportTestConfig()
	base := t.TempDir()
	cfg.Hermes.ProfilesDir = filepath.Join(base, ".hermes", "profiles")
	cfg.Bridge.URL = bridge.URL
	rule := cfg.Rules.GroupRules[supportGroup]
	rule.DryRun, rule.HermesProfile, rule.MaxReplyRunes = false, "demo-support", 1200
	cfg.Rules.GroupRules[supportGroup] = rule
	root := filepath.Join(base, "knowledge")
	for _, name := range []string{"SKILL.md", "references/demodesk.md", "references/demodesk-release-notes.md", "references/store-live.md"} {
		data, err := os.ReadFile(filepath.Join("testdata", "knowledge", filepath.Base(name)))
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, name)
		cfg.localPolicy.Support.KnowledgeFiles = append(cfg.localPolicy.Support.KnowledgeFiles, path)
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	profile := filepath.Join(cfg.Hermes.ProfilesDir, rule.HermesProfile)
	cfg.localPolicy.Support.DocumentFiles = []string{filepath.Join(profile, "support-knowledge.md")}
	cfg.localPolicy.Support.ReleaseFiles = []string{filepath.Join(root, "references/demodesk-release-notes.md")}
	if err := os.MkdirAll(profile, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(profile, "support-knowledge.md"), []byte("合成旧交接记录日期2025-12-31：虚构 DemoDesk Windows 0.9.0 尚未发布，不代表今天状态"), 0600); err != nil {
		t.Fatal(err)
	}
	d = newTestDispatcher(t, cfg)
	d.reminderRunner = (&mockReminders{items: map[string]reminderItem{}}).run
	msg := testMessage(supportGroup, "offline-release-question")
	msg.Content = question
	d.hermesRunner = func(_ context.Context, b batch) (HermesResult, error) {
		prompt := buildPrompt(b)
		for _, fact := range []string{"Windows 0.9.0", "macOS 0.8.0", "演示列表新增纸飞机标签", "演示卡片新增月亮徽记", "2026-01-01", "开发进度仍须真实任务依据"} {
			if !strings.Contains(prompt, fact) {
				t.Fatalf("backend did not receive release fact: %s", fact)
			}
		}
		if strings.Index(prompt, "### Windows 0.9.0") > strings.Index(prompt, "合成旧交接记录日期2025-12-31") {
			t.Fatal("new release facts must precede old knowledge")
		}
		if dir := os.Getenv("SUPPORT_RELEASE_EVIDENCE_DIR"); dir != "" {
			if err := os.WriteFile(filepath.Join(dir, "backend-prompt.txt"), []byte(prompt), 0600); err != nil {
				t.Fatal(err)
			}
		}
		return HermesResult{Text: answer, Backend: "Alma", RunID: "offline-release", SessionID: "alma:offline-release"}, nil
	}
	rule, _ = d.cfg.route(supportGroup)
	b := batch{GroupID: supportGroup, Messages: []RelayMessage{msg}, Rule: rule, Config: d.cfg, Policy: policyID(d.cfg)}
	if !d.processBatch(b) || final != answer || d.state.Stats.TotalReplied != 1 {
		t.Fatalf("release answer overwritten: got %q want %q", final, answer)
	}
	for _, fact := range []string{"2026-01-01", "0.9.0", "0.8.0", "纸飞机标签", "月亮徽记", "不能证明某条反馈已修复", "开发进度仍须真实任务依据"} {
		if !strings.Contains(final, fact) {
			t.Fatalf("final output lacks %s", fact)
		}
	}
	if dir := os.Getenv("SUPPORT_RELEASE_EVIDENCE_DIR"); dir != "" {
		if err := os.WriteFile(filepath.Join(dir, "final-http-text.txt"), []byte(final), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("isolated final HTTP text: %s", final)
}
