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
	"time"
)

// Replays already captured real backend results. Never invokes models or public web.
func TestSupportWebRealResultFinalHTTP(t *testing.T) {
	dir := os.Getenv("SUPPORT_WEB_EVIDENCE_DIR")
	if dir == "" {
		t.Skip("set SUPPORT_WEB_EVIDENCE_DIR for saved live-result replay")
	}
	for _, name := range []string{"adapter-live.stdout.jsonl", "hermes-live.stdout.jsonl", "hermes-resume.stdout.jsonl", "adapter-fallback-live.stdout.jsonl", "current-version-live.json"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			var result HermesResult
			if name == "current-version-live.json" {
				var value struct {
					Text     string        `json:"text"`
					Evidence []WebEvidence `json:"evidence"`
				}
				if err := json.Unmarshal(data, &value); err != nil {
					t.Fatal(err)
				}
				result.Text = value.Text
				result.WebEvidence = value.Evidence
			}
			for _, line := range strings.Split(string(data), "\n") {
				var event struct {
					Type        string        `json:"type"`
					Text        string        `json:"text"`
					WebEvidence []WebEvidence `json:"web_evidence"`
				}
				if json.Unmarshal([]byte(line), &event) == nil && event.Type == "result" {
					result.Text = event.Text
					result.WebEvidence = event.WebEvidence
				}
			}
			if !strings.Contains(result.Text, "DemoDesk") {
				t.Fatal("missing synthetic product fact")
			}
			if name == "adapter-live.stdout.jsonl" || name == "adapter-fallback-live.stdout.jsonl" {
				seen := map[string]bool{}
				for _, e := range result.WebEvidence {
					if e.OK {
						seen[e.Tool] = true
					}
				}
				if !seen["WebSearch"] || !seen["WebFetch"] {
					t.Fatal("missing executed tool evidence")
				}
			}
			var d *Dispatcher
			final := ""
			bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Text string `json:"text"`
				}
				json.NewDecoder(r.Body).Decode(&body)
				final = body.Text
				echo := testMessage(supportGroup, "saved-web-echo")
				echo.IsEcho = true
				echo.Content = body.Text
				d.handleIncomingMessage(echo, "")
				fmt.Fprint(w, "OK")
			})
			cfg := supportTestConfig()
			cfg.Bridge.URL = bridge.URL
			cfg.Hermes.ProfilesDir = t.TempDir()
			rule := cfg.Rules.GroupRules[supportGroup]
			rule.DryRun = false
			rule.MaxReplyRunes = 1200
			cfg.Rules.GroupRules[supportGroup] = rule
			d = newTestDispatcher(t, cfg)
			d.reminderRunner = (&mockReminders{items: map[string]reminderItem{}}).run
			result.Backend = "Alma"
			result.RunID = "saved-web"
			result.SessionID = "alma:saved-web"
			d.hermesRunner = func(context.Context, batch) (HermesResult, error) { return result, nil }
			msg := testMessage(supportGroup, "saved-web")
			msg.Content = "DemoDesk叫什么，有什么功能？"
			rule, _ = d.cfg.route(supportGroup)
			b := batch{GroupID: supportGroup, Messages: []RelayMessage{msg}, Rule: rule, Config: d.cfg, Policy: policyID(d.cfg)}
			if !d.processBatch(b) || final != strings.TrimRight(strings.TrimSpace(result.Text), "。\n\r\t ") {
				t.Fatalf("result rewritten before HTTP: %q", final)
			}
			if err := os.WriteFile(filepath.Join(dir, name+".final-http.txt"), []byte(final), 0600); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestModelNameRequiresExecutorSearchEvidence(t *testing.T) {
	b := batch{Messages: []RelayMessage{{Content: "当前确需点名比较型号"}}}
	answer := "官方搜索结果中的 Gemini99pro 仅为离线虚构测试型号"
	b.WebEvidence = []WebEvidence{{Tool: "WebSearch", URLs: []string{"https://ai.google.dev/models"}, CheckedAt: time.Now().Format(time.RFC3339Nano), OK: true, Text: "Gemini99pro"}}
	if got := modelCurrencyReply(b, answer); got != answer {
		t.Fatal(got)
	}
	for _, mutate := range []func(*WebEvidence){func(e *WebEvidence) { e.OK = false }, func(e *WebEvidence) { e.CheckedAt = time.Now().Add(-time.Hour).Format(time.RFC3339Nano) }, func(e *WebEvidence) { e.URLs = []string{"https://untrusted.invalid/"} }, func(e *WebEvidence) { e.Text = "different model" }} {
		copy := b
		copy.WebEvidence = append([]WebEvidence(nil), b.WebEvidence...)
		mutate(&copy.WebEvidence[0])
		if modelCurrencyReply(copy, answer) == answer {
			t.Fatal("invalid evidence unlocked model")
		}
	}
}
