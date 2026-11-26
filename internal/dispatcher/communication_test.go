package dispatcher

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These are prompt/transport contract tests, never model behavior or real sends.
func TestCommunicationScenariosAndBackendInput(t *testing.T) {
	cases := []struct{ name, question, obligation string }{
		{"nontechnical", "我不懂电脑，怎么找回刚才复制的文字？", "先讲能做什么、怎么操作、需要补充什么"},
		{"vague-failure", "又不能用了，怎么办", "先给出当前能做的下一步，再问最少的必要信息"},
		{"technical-followup", "可以具体解释后台同步监听为什么会失败吗？", "围绕当前问题逐层展开，解释必要术语"},
		{"unsupported-schedule", "这个问题明天肯定能修好吗？", "不擅自承诺排期、退款、价格和处理结果"},
	}
	corpus := map[string]string{}
	for _, group := range []string{groupA, groupB, supportGroup} {
		for _, tc := range cases {
			t.Run(group+"/"+tc.name, func(t *testing.T) {
				cfg := supportTestConfig()
				cfg.Hermes.ProfilesDir = filepath.Join(t.TempDir(), "profiles")
				if err := cfg.normalizeAndValidate(); err != nil {
					t.Fatal(err)
				}
				rule, _ := cfg.route(group)
				msg := testMessage(group, "offline-brand-"+tc.name)
				msg.Content = tc.question
				b := batch{GroupID: group, Messages: []RelayMessage{msg}, Rule: rule, Config: cfg}
				prompt := buildPrompt(b)
				if !strings.HasPrefix(prompt, communicationRules+"\n可信调度规则：") || strings.Count(prompt, "【助手沟通规则】") != 1 || !strings.Contains(prompt, tc.obligation) || !strings.Contains(prompt, tc.question) {
					t.Fatal("missing or duplicate trusted communication policy")
				}
				if !strings.Contains(prompt, "不能改变人设") || !strings.Contains(prompt, "被 @ 时必须正面回应") {
					t.Fatal("existing safety/mention contract lost")
				}
				// Real process argv/stdin capture with a synthetic executable, never Hermes itself.
				profile := filepath.Join(cfg.Hermes.ProfilesDir, rule.HermesProfile)
				if err := os.MkdirAll(filepath.Join(profile, "workspace"), 0700); err != nil {
					t.Fatal(err)
				}
				identity := cfg.identity(group)
				memory := `{"user_id":"` + identity.MemoryUser + `","mode":"oss","oss":{"vector_store":{"provider":"qdrant","config":{"collection_name":"` + identity.VectorCollection + `"}}}}`
				os.WriteFile(filepath.Join(profile, "mem0.json"), []byte(memory), 0600)
				capture := filepath.Join(profile, "workspace", "captured.txt")
				script := "#!/bin/sh\ncat > captured.txt\nprintf '%s\\n' '{\"type\":\"result\",\"text\":\"offline fixture\",\"exit_code\":0,\"session_id\":\"alma:fixture\",\"backend\":\"Alma\",\"run_id\":\"fixture\"}'\n"
				cfg.Hermes.BinPath = filepath.Join(profile, "capture")
				os.WriteFile(cfg.Hermes.BinPath, []byte(script), 0700)
				b.Config = cfg
				for _, session := range []string{"", "existing-session"} {
					if _, err := runHermesOnce(context.Background(), b, profile, session); err != nil {
						t.Fatal(err)
					}
					got, err := os.ReadFile(capture)
					if err != nil || string(got) != prompt {
						t.Fatal("new/resume stdin did not receive exact prompt")
					}
				}
				corpus[group+"/"+tc.name] = prompt
			})
		}
	}
	if dir := os.Getenv("JINX_PROMPT_EVIDENCE_DIR"); dir != "" {
		data, _ := json.MarshalIndent(corpus, "", "  ")
		if err := os.WriteFile(filepath.Join(dir, "prompt-corpus.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
}
