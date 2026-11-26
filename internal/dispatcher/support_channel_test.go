package dispatcher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSupportPublicReplyNeverContainsDiagnosticsOrIDs(t *testing.T) {
	for _, raw := range []string{
		`{"type":"tool_result","text":"secret"}`, "stdout: sync done\nhello", "[INFO] read ok\n我记下来了",
		`prefix {"calendar_id":"internal"}`, "提醒事项尚未核实同步成功，当前待同步，不能确认已记录",
		"不过现在好了，已记录", "我记下来了", "Bearer synthetic-token",
	} {
		b := batch{Messages: []RelayMessage{{Content: "不过现在好了，能回复了..."}}, SupportProgress: remindersHeading + "[]"}
		if got := supportSafeReply(b, raw); got != "NONE" {
			t.Fatalf("diagnostic/false receipt escaped: %q", got)
		}
	}
	b := batch{Messages: []RelayMessage{{Content: "怎么调整快捷键？"}}, SupportProgress: remindersHeading + "[]"}
	if got := supportSafeReply(b, "按设置里的快捷键选项调整"); got != "按设置里的快捷键选项调整" {
		t.Fatal(got)
	}
}

func TestSupportSourcedFeatureAnswerSurvivesProgressGuard(t *testing.T) {
	b := batch{Messages: []RelayMessage{{Content: "win版本也有适配计划吗"}}, SupportProgress: remindersHeading + "[]"}
	b.Config.Hermes.ProfilesDir = t.TempDir()
	b.Rule.HermesProfile = "fixture"
	b.Config.localPolicy = supportTestConfig().localPolicy
	b.Config.localPolicy.Support.DocumentFiles = []string{filepath.Join(b.Config.Hermes.ProfilesDir, "fixture", "support-knowledge.md")}
	os.MkdirAll(filepath.Join(b.Config.Hermes.ProfilesDir, "fixture"), 0700)
	os.WriteFile(filepath.Join(b.Config.Hermes.ProfilesDir, "fixture", "support-knowledge.md"), []byte("Windows交接状态日期2026-11-10"), 0600)
	answer := "截至 2026-11-10 的交接记录，DemoDesk Windows 2.0.0 代码已经完成，并在 ARM64 虚拟机通过 Debug 和 Native AOT 验证；x64 真机和公开发布仍未确认"
	if got := supportSafeReply(b, answer); got != answer {
		t.Fatalf("sourced feature answer was overwritten: %q", got)
	}
	for _, answer := range []string{"目前还没有确认的处理进度或排期，我不能据此判断是否修复或发布", "已完成"} {
		b.Messages[0].Content = "现在开发进度怎么样，什么时候发布？"
		got := supportSafeReply(b, answer)
		if answer == "已完成" && got == answer {
			t.Fatalf("bare unsupported status escaped: %q", got)
		}
	}
}

func TestSupportSuccessfulAndFailedReceiptUseOnlyVerifiedFacts(t *testing.T) {
	msg := RelayMessage{ID: "feedback", IntakeReceipt: "intake-test", Content: "建议添加快捷键功能"}
	task := supportTask{IntakeID: msg.IntakeReceipt, MessageID: msg.ID, Status: "synced", ReminderID: "real-shaped-mock", CalendarID: "mock-cloud"}
	b := batch{Messages: []RelayMessage{msg}}
	for _, status := range []string{"synced", "pending_sync", "unavailable"} {
		task.Status = status
		data, _ := json.Marshal([]supportTask{task})
		b.SupportProgress = remindersHeading + string(data)
		got := supportSafeReply(b, `{"tool_result":"synthetic private output"}`)
		if status == "synced" {
			if !strings.Contains(got, "我记下来了") {
				t.Fatal(got)
			}
		} else if strings.Contains(got, "记下来了") || !strings.Contains(got, "还没记成功") {
			t.Fatal(got)
		}
		for _, internal := range []string{"intake-", "mock-cloud", "JSON", "回读", "pending_sync", "tool_result"} {
			if strings.Contains(got, internal) {
				t.Fatal(got)
			}
		}
	}
}

func TestSupportMalformedBackendMetadataRejectedWithoutChangingLegacyRoute(t *testing.T) {
	for _, r := range []HermesResult{
		{Backend: "Alma", SessionID: "alma:r", RunID: "r"},
		{Backend: "Hermes", Fallback: true, FallbackReason: "alma_timeout", SessionID: "local-s"},
	} {
		if !validSupportBackend(r) {
			t.Fatal(r)
		}
	}
	for _, r := range []HermesResult{
		{Backend: "Hermes", SessionID: "local-s"}, {Backend: "Alma", SessionID: "alma:r"},
		{Backend: "Hermes", Fallback: true, FallbackReason: "raw secret"},
	} {
		if validSupportBackend(r) {
			t.Fatal("invalid metadata accepted")
		}
	}
}

func TestOrdinaryLauncherRetainsConfiguredAdapterAndResume(t *testing.T) {
	base := t.TempDir()
	cfg := testConfig()
	cfg.Hermes.ProfilesDir = filepath.Join(base, "profiles")
	rule := cfg.Rules.GroupRules[groupA]
	rule.HermesProfile = "demo-ordinary"
	rule.SupportMode = false
	profile := filepath.Join(cfg.Hermes.ProfilesDir, rule.HermesProfile)
	os.MkdirAll(filepath.Join(profile, "workspace"), 0700)
	os.WriteFile(filepath.Join(profile, "mem0.json"), []byte(`{"user_id":"group:100@chatroom","mode":"oss","oss":{"vector_store":{"provider":"qdrant","config":{"collection_name":"group_100"}}}}`), 0600)
	launcher := filepath.Join(base, "existing-adapter")
	cfg.Hermes.BinPath = launcher
	script := `#!/bin/sh
for arg in "$@"; do [ "$arg" != "--support-mode" ] || exit 9; done
case "$*" in *"--resume legacy-session"*) ;; *) exit 8;; esac
cat >/dev/null
printf '%s\n' '{"type":"result","text":"legacy reply","session_id":"alma:legacy-run","exit_code":0,"backend":"Alma"}'
`
	os.WriteFile(launcher, []byte(script), 0700)
	result, err := runHermesOnce(context.Background(), batch{GroupID: groupA, Rule: rule, Config: cfg}, profile, "legacy-session")
	if err != nil || result.Text != "legacy reply" {
		t.Fatal(result, err)
	}
}

func TestSupportRuntimeIgnoresLogToolAndDeltaChannels(t *testing.T) {
	base := t.TempDir()
	cfg := supportTestConfig()
	cfg.Hermes.ProfilesDir = filepath.Join(base, "profiles")
	rule := cfg.Rules.GroupRules[supportGroup]
	rule.HermesProfile = "demo-support"
	profile := filepath.Join(cfg.Hermes.ProfilesDir, rule.HermesProfile)
	os.MkdirAll(filepath.Join(profile, "workspace"), 0700)
	os.WriteFile(filepath.Join(profile, "mem0.json"), []byte(`{"user_id":"demo-support-memory","mode":"oss","oss":{"vector_store":{"provider":"qdrant","config":{"collection_name":"demo-support-vectors"}}}}`), 0600)
	launcher := filepath.Join(base, "adapter")
	cfg.Hermes.BinPath = launcher
	for _, metadata := range []string{
		`"backend":"Alma","fallback":false,"session_id":"alma:mock-run","run_id":"mock-run"`,
		`"backend":"Hermes","fallback":true,"fallback_reason":"alma_timeout","session_id":"fallback-session","run_id":"mock-run"`,
	} {
		script := `#!/bin/sh
cat >/dev/null
printf '%s\n' 'raw internal stdout' '{"type":"tool_result","text":"PRIVATE TOOL OUTPUT"}' '{"type":"delta","text":"PRIVATE DELTA"}' '{"type":"result","text":"正常的用户答复","exit_code":0,` + metadata + `}'
`
		os.WriteFile(launcher, []byte(script), 0700)
		result, err := runHermesOnce(context.Background(), batch{GroupID: supportGroup, Rule: rule, Config: cfg}, profile, "")
		if err != nil || result.Text != "正常的用户答复" || !validSupportBackend(result) {
			t.Fatal(result, err)
		}
	}
}

func TestSupportBatchSeparatesAuditFromOutgoingDryRun(t *testing.T) {
	for _, tc := range []struct {
		name, content, raw, want string
		fail                     bool
	}{
		{"success", "建议添加快捷键功能", `{"tool_result":"PRIVATE"}`, "我记下来了", false},
		{"failure", "建议添加快捷键功能", "我记下来了", "还没记成功", true},
		{"unrelated", "不过现在好了，能回复了...", "提醒事项尚未核实同步成功，当前待同步，不能确认已记录", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := supportTestConfig()
			d := newTestDispatcher(t, cfg)
			cfg = d.cfg
			m := &mockReminders{items: map[string]reminderItem{}, failWrite: tc.fail}
			d.reminderRunner = m.run
			msg := testMessage(supportGroup, "fixture-"+tc.name)
			msg.Content = tc.content
			id, err := d.collectSupport(msg)
			if err != nil {
				t.Fatal(err)
			}
			msg.IntakeReceipt = id
			rule, _ := cfg.route(supportGroup)
			rule.DryRun = tc.name != "success" && tc.name != "failure"
			// Keep delivery dry while exercising mocked task writes; no network/server exists.
			cfg.Bridge.DryRun = true
			d.cfg = cfg
			d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
				return HermesResult{Text: tc.raw, Backend: "Alma", SessionID: "alma:mock-run", RunID: "mock-run"}, nil
			}
			b := batch{GroupID: supportGroup, Messages: []RelayMessage{msg}, Rule: rule, Config: cfg, Policy: policyID(cfg)}
			if !d.processBatch(b) {
				t.Fatal("batch failed")
			}
			files, _ := filepath.Glob(filepath.Join(filepath.Dir(d.stateFile), "audit_*.jsonl"))
			if len(files) != 1 {
				t.Fatal(files)
			}
			data, _ := os.ReadFile(files[0])
			var outgoing string
			for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
				var record struct {
					Outcome string `json:"outcome"`
					Detail  string `json:"detail"`
				}
				json.Unmarshal([]byte(line), &record)
				if record.Outcome == "dry_run" {
					outgoing = record.Detail
				}
			}
			if tc.want != "" && !strings.Contains(outgoing, tc.want) {
				t.Fatal(outgoing)
			}
			if tc.want == "" && outgoing != "" {
				t.Fatal("diagnostic escaped", outgoing)
			}
			if strings.Contains(outgoing, "PRIVATE") || strings.Contains(outgoing, "intake-") {
				t.Fatal("private channel escaped")
			}
		})
	}
}
