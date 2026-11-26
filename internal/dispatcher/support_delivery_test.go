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

// Local HTTP fixture only: exercises processBatch -> deliver -> actual JSON body.
// Never contacts the configured bridge/Alma/relay or real Reminders.
func TestSupportWindowsSyntheticFinalBridgePayload(t *testing.T) {
	var fixtures []struct {
		Synthetic   bool   `json:"synthetic"`
		FixtureDate string `json:"fixture_date"`
		RunID       string `json:"run_id"`
		Message     struct {
			ID, Content string
			SenderName  string `json:"sender_name"`
		} `json:"message"`
		Summary string `json:"summary"`
	}
	captures := map[string]string{}
	data, err := os.ReadFile("testdata/windows-reply-synthetic-20260101.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures) != 2 {
		t.Fatal("missing synthetic Windows scenarios")
	}
	type scenario struct {
		name, input, raw, want string
		failWrite              bool
	}
	cases := []scenario{
		{"function", "怎么打开历史面板？", "按快捷键打开历史面板，复制的内容已保存为历史记录", "按快捷键打开历史面板，复制的内容已保存为历史记录", false},
		{"unknown-progress", "这个问题处理进度怎么样？", "已经修复", "暂时没找到对应的问题记录，还不能确认处理进度", false},
		{"dated-progress", "Windows开发进度怎么样？", "截至 2025-12-31 的虚构交接记录，Windows 演示组件已经完成，但不代表已发布", "截至 2025-12-31 的虚构交接记录，Windows 演示组件已经完成，但不代表已发布", false},
		{"reminder-success", "建议添加快捷键功能", `{"tool_result":"PRIVATE"}`, "我记下来了，后续处理进度有确认信息再告诉你", false},
		{"reminder-failure", "建议添加快捷键功能", "我记下来了", "这条反馈还没记成功，我这边需要稍后再同步", true},
		{"mixed-log", "怎么打开历史面板？", "[INFO] internal trace\n打开历史面板即可", "这部分我还不能确认，可以再说一下具体情况吗", false},
	}
	for _, record := range fixtures {
		if !record.Synthetic || record.FixtureDate != "2026-01-01" || record.Message.SenderName != "ExamplePeer" || !strings.Contains(record.Message.Content, "@DemoBot Windows") {
			t.Fatal("Windows fixture must use an explicitly synthetic scenario")
		}
		cases = append(cases, scenario{record.RunID, record.Message.Content, record.Summary, record.Summary, false})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
					t.Error("wrong route")
				}
				final = body.Text
				echo := testMessage(supportGroup, "offline-echo-"+tc.name)
				echo.IsEcho = true
				echo.Content = body.Text
				if err := d.handleIncomingMessage(echo, ""); err != nil {
					t.Error(err)
				}
				fmt.Fprint(w, "OK")
			})
			cfg := supportTestConfig()
			cfg.Hermes.ProfilesDir = t.TempDir()
			cfg.Bridge.URL = bridge.URL
			rule := cfg.Rules.GroupRules[supportGroup]
			rule.DryRun = false
			rule.HermesProfile = "demo-support"
			rule.MaxReplyRunes = 1200
			cfg.Rules.GroupRules[supportGroup] = rule
			profile := filepath.Join(cfg.Hermes.ProfilesDir, rule.HermesProfile)
			cfg.localPolicy.Support.DocumentFiles = []string{filepath.Join(profile, "support-knowledge.md")}
			os.MkdirAll(profile, 0700)
			os.WriteFile(filepath.Join(profile, "support-knowledge.md"), []byte("Windows 虚构交接状态日期：2025-12-31；仅为合成测试数据，不代表实时状态"), 0600)
			d = newTestDispatcher(t, cfg)
			m := &mockReminders{items: map[string]reminderItem{}, failWrite: tc.failWrite}
			d.reminderRunner = m.run
			msg := testMessage(supportGroup, "offline-replay-"+tc.name)
			msg.Content = tc.input
			id, err := d.collectSupport(msg)
			if err != nil {
				t.Fatal(err)
			}
			msg.IntakeReceipt = id
			d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
				return HermesResult{Text: tc.raw, Backend: "Alma", SessionID: "alma:offline-run", RunID: "offline-run"}, nil
			}
			rule, _ = d.cfg.route(supportGroup)
			b := batch{GroupID: supportGroup, Messages: []RelayMessage{msg}, Rule: rule, Config: d.cfg, Policy: policyID(d.cfg)}
			if !d.processBatch(b) || final != tc.want || d.state.Stats.TotalReplied != 1 {
				t.Fatalf("final bridge text mismatch:\ngot %s\nwant %s", final, tc.want)
			}
			if strings.Contains(final, "PRIVATE") {
				t.Fatal("internal log escaped")
			}
			if strings.HasPrefix(tc.name, "synthetic-windows-") {
				for _, fact := range []string{"ExamplePeer", "Windows", "无法确认"} {
					if !strings.Contains(final, fact) {
						t.Fatalf("Windows final payload lacks %s", fact)
					}
				}
				for _, step := range []string{"macOS", "Mac", "Command", "⌘", "辅助功能", "系统设置"} {
					if strings.Contains(final, step) {
						t.Fatalf("Windows final payload borrowed platform-specific advice: %s", step)
					}
				}
			}
			captures[tc.name] = final
			t.Logf("OFFLINE bridge text: %s", final)
		})
	}
	if dir := os.Getenv("SUPPORT_REPLAY_EVIDENCE_DIR"); dir != "" {
		data, _ := json.MarshalIndent(captures, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "final-bridge-texts.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
