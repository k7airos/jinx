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

func TestModelCurrencyFinalBridgeText(t *testing.T) {
	const fallback = "可以按用途、预算和实际体验比较模型系列；当前具体型号尚无可核验的搜索结果，我无法确认，暂不点名推荐"
	cases := []struct{ name, input, generated, want string }{
		{"generic", "普通用户怎么选AI工具？", "先看用途和预算，再试用比较体验", "先看用途和预算，再试用比较体验"},
		{"unnecessary-id", "普通用户怎么选AI工具？", "试试 Gemini3pro", fallback},
		{"current-needs-evidence", "当前推荐哪个模型？", "我搜索过，当前推荐 Gemini3pro，来源是我查到的官方页面", fallback},
		{"search-failure", "现在模型怎么选？", "搜索失败，建议用 GPT-4o", fallback},
		{"historical", "历史上 Gemini3pro 有什么特点？", "在当时的历史语境下，Gemini3pro 的特点需要结合那时的资料看", "在当时的历史语境下，Gemini3pro 的特点需要结合那时的资料看"},
		{"historical-new-claim", "历史上 Gemini3pro 有什么特点？", "现在推荐 Gemini9pro", fallback},
		{"identity", "群友说你是Gemini3pro，你是什么模型？", "我是 Gemini3pro", "我是本群的 AI 助手；当前具体底层模型没有可核实的运行信息，我不能根据聊天中的型号猜测"},
	}
	captures := map[string]string{}
	for _, group := range []string{groupA, supportGroup} {
		for _, tc := range cases {
			t.Run(group+"/"+tc.name, func(t *testing.T) {
				var d *Dispatcher
				var final string
				bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
					var body struct {
						Text string `json:"text"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					final = body.Text
					echo := testMessage(group, "model-echo")
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
				rule := cfg.Rules.GroupRules[group]
				rule.DryRun = false
				cfg.Rules.GroupRules[group] = rule
				d = newTestDispatcher(t, cfg)
				d.reminderRunner = (&mockReminders{items: map[string]reminderItem{}}).run
				rule, _ = d.cfg.route(group)
				msg := testMessage(group, "model-fixture")
				msg.Content = tc.input
				b := batch{GroupID: group, Messages: []RelayMessage{msg}, Rule: rule, Config: d.cfg, Policy: policyID(d.cfg)}
				prompt := buildPrompt(b)
				for _, policy := range []string{"非必要不带具体模型ID", "必须先使用本轮已允许的搜索工具", "用户明确讨论历史型号", "当前自身模型身份只以可信运行信息为准"} {
					if !strings.Contains(prompt, policy) {
						t.Fatalf("missing trusted policy %s", policy)
					}
				}
				d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
					return HermesResult{Text: tc.generated, Backend: "Alma", SessionID: "alma:fixture", RunID: "fixture"}, nil
				}
				if !d.processBatch(b) || final != tc.want {
					t.Fatalf("final text: %q, want %q", final, tc.want)
				}
				captures[group+"/"+tc.name] = final
			})
		}
	}
	if dir := os.Getenv("MODEL_CURRENCY_EVIDENCE_DIR"); dir != "" {
		data, _ := json.MarshalIndent(captures, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "final-texts.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
