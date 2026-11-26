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

func TestAddresseeSyntheticInputsNeverReachBridge(t *testing.T) {
	var rows []struct {
		ID, Content string
		Synthetic   bool          `json:"synthetic"`
		FixtureDate string        `json:"fixture_date"`
		Details     *QuoteDetails `json:"details"`
	}
	data, err := os.ReadFile("testdata/addressee-synthetic-20260101.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &rows); err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatal("missing synthetic addressee scenarios")
	}
	for _, row := range rows {
		if !row.Synthetic || row.FixtureDate != "2026-01-01" {
			t.Fatal("addressee fixture must be explicitly synthetic")
		}
		t.Run(row.ID, func(t *testing.T) {
			cfg := supportTestConfig()
			calls := 0
			bridge := bridgeFixture(t, func(http.ResponseWriter, *http.Request) { calls++; t.Error("NONE must never be sent") })
			cfg.Bridge.URL = bridge.URL
			d := newTestDispatcher(t, cfg)
			d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
				t.Error("other addressee must not invoke backend")
				return HermesResult{Text: "抢答"}, nil
			}
			msg := testMessage(groupA, row.ID)
			msg.Content = row.Content
			msg.Details = row.Details
			rule, _ := d.cfg.route(groupA)
			b := batch{GroupID: groupA, Messages: []RelayMessage{msg}, Rule: rule, Config: d.cfg, Policy: policyID(d.cfg), Warm: true, Promo: true}
			if !d.processBatch(b) || calls != 0 || d.state.Stats.TotalSubmitted != 0 {
				t.Fatal("other-directed batch submitted")
			}
			audit, _ := filepath.Glob(filepath.Join(filepath.Dir(d.stateFile), "audit_*.jsonl"))
			raw, _ := os.ReadFile(audit[0])
			if !strings.Contains(string(raw), `"outcome":"none"`) {
				t.Fatal(string(raw))
			}
		})
	}
}

func TestAddresseeOwnProductQuoteAndMixedBatch(t *testing.T) {
	cases := []struct {
		name, content string
		quote         *QuoteDetails
		mixed         bool
	}{
		{"assistant", "@DEMOBOT 帮我解释下", nil, false},
		{"both", "@ExamplePeer @demobot 请分别说自己的部分", nil, false},
		{"product-no-mention", "建议添加快捷键功能", nil, false},
		{"quoted-old-mention", "[引用消息] 这个怎么操作？", &QuoteDetails{}, false},
		{"mixed", "@DEMOBOT 只回答我的这个问题", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var d *Dispatcher
			final := ""
			backendCalls := 0
			bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
				var body struct {
					Text string `json:"text"`
				}
				json.NewDecoder(r.Body).Decode(&body)
				final = body.Text
				echo := testMessage(supportGroup, "offline-echo")
				echo.IsEcho = true
				echo.Content = body.Text
				d.handleIncomingMessage(echo, "")
				fmt.Fprint(w, "OK")
			})
			cfg := supportTestConfig()
			cfg.Hermes.ProfilesDir = t.TempDir()
			cfg.Bridge.URL = bridge.URL
			rule := cfg.Rules.GroupRules[supportGroup]
			rule.DryRun = false
			cfg.Rules.GroupRules[supportGroup] = rule
			d = newTestDispatcher(t, cfg)
			d.reminderRunner = (&mockReminders{items: map[string]reminderItem{}}).run
			msg := testMessage(supportGroup, "own")
			msg.Content = tc.content
			if tc.quote != nil {
				json.Unmarshal([]byte(`{"quote":{"content":"@ExamplePeer 旧的虚构纸船问题 @DEMOBOT","type":1}}`), tc.quote)
				msg.Details = tc.quote
			}
			id, err := d.collectSupport(msg)
			if err != nil {
				t.Fatal(err)
			}
			msg.IntakeReceipt = id
			messages := []RelayMessage{msg}
			if tc.mixed {
				other := testMessage(supportGroup, "other")
				other.Content = "@ExamplePeer 核对演示棋盘的编号"
				messages = append(messages, other)
			}
			d.hermesRunner = func(_ context.Context, b batch) (HermesResult, error) {
				backendCalls++
				if len(b.Messages) != 1 || b.Messages[0].ID != "own" {
					t.Fatal("mixed batch leaked other addressee")
				}
				prompt := buildPrompt(b)
				if !strings.Contains(prompt, "明确受话人优先") {
					t.Fatal("missing shared rule")
				}
				return HermesResult{Text: "只回答当前对我的问题", Backend: "Alma", SessionID: "alma:offline", RunID: "offline"}, nil
			}
			rule, _ = d.cfg.route(supportGroup)
			b := batch{GroupID: supportGroup, Messages: messages, Rule: rule, Config: d.cfg, Policy: policyID(d.cfg)}
			if !d.processBatch(b) || backendCalls != 1 || final == "" || final == "NONE" {
				t.Fatal("eligible answer lost", final)
			}
			if tc.name == "product-no-mention" && !strings.Contains(final, "我记下来了") {
				t.Fatal("product feedback lost", final)
			}
		})
	}
}

func TestAddresseeCurrentSpeechBeatsQuotedSummon(t *testing.T) {
	cfg := testConfig()
	msg := testMessage(groupA, "quote")
	json.Unmarshal([]byte(`{"quote":{"content":"@DEMOBOT 上一轮虚构灯塔的编号是多少？","type":1}}`), &msg.Details)
	msg.Content = "[引用消息] @ExamplePeer 核对这轮演示的纸船编号"
	if !addressedElsewhere(msg, cfg) {
		t.Fatal("quoted old assistant mention bypassed current addressee")
	}
	msg.Content = "[引用消息] 现在问这个"
	if addressedElsewhere(msg, cfg) {
		t.Fatal("quote old mention became current addressee")
	}
	msg.Content = "建议添加快捷键功能 @ExamplePeer"
	if !addressedElsewhere(msg, supportTestConfig()) {
		t.Fatal("product no-mention policy bypassed explicit addressee")
	}
	other := msg
	other.Content = "@ExamplePeer 核对演示棋盘的编号"
	if len(pendingForAssistant(batch{Config: cfg, Messages: []RelayMessage{msg, other}})) != 0 {
		t.Fatal("all-other batch acquired pending content")
	}
}
