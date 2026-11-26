package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"jinx/internal/contracts"
)

func TestSplitAndResolveReplyTarget(t *testing.T) {
	for _, tc := range []struct {
		in      string
		ref     int
		body    string
		present bool
	}{
		{"REPLY_TO: 2\n谢谢反馈", 2, "谢谢反馈", true},
		{"reply_to：#3\n\n第一行\n第二行", 3, "第一行\n第二行", true},
		{"REPLY_TO: 1 同一行正文", 1, "同一行正文", true},
		{"REPLY_TO: 2\nNONE", 2, "NONE", true},
		{"REPLY_TO: rpid-123\n正文", 0, "正文", true},
		{"REPLY_TO:\n正文", 0, "正文", true},
		{"普通回复，没有指令", 0, "普通回复，没有指令", false},
		{"NONE", 0, "NONE", false},
		{"正文\nREPLY_TO: 2", 0, "正文\nREPLY_TO: 2", false},
	} {
		ref, body, present := splitReplyTarget(tc.in)
		if ref != tc.ref || body != tc.body || present != tc.present {
			t.Fatalf("splitReplyTarget(%q) = %d %q %t", tc.in, ref, body, present)
		}
	}
	for _, tc := range []struct {
		ref     int
		present bool
		pending int
		index   int
		reason  string
	}{
		{0, false, 1, 1, ""},
		{1, true, 1, 1, ""},
		{2, true, 3, 2, ""},
		{0, false, 3, 0, reasonReplyTargetMissing},
		{4, true, 3, 0, reasonReplyTargetInvalid},
		{0, true, 3, 0, reasonReplyTargetInvalid},
		{2, true, 1, 0, reasonReplyTargetInvalid},
	} {
		index, reason := resolveReplyTarget(tc.ref, tc.present, tc.pending)
		if index != tc.index || reason != tc.reason {
			t.Fatalf("resolveReplyTarget(%d,%t,%d) = %d %q", tc.ref, tc.present, tc.pending, index, reason)
		}
	}
}

func threadedComments(now time.Time) []RelayMessage {
	out := []RelayMessage{}
	for i, content := range []string{"这个插件支持导出吗？", "前排", "求推荐类似工具"} {
		received := now
		out = append(out, RelayMessage{
			ID: fmt.Sprintf("rpid-%d", 101+i), Source: "bilibili:ego", Platform: "bilibili", AccountID: "bilibili-ego",
			GroupID: "bilibili:BV1example0001", GroupName: "Demo Video", SenderID: fmt.Sprintf("%d", 2001+i), SenderName: fmt.Sprintf("user%d", i+1),
			ParentID: fmt.Sprintf("root-%d", i+1), RootID: fmt.Sprintf("root-%d", i+1),
			ReplyContext: json.RawMessage(fmt.Sprintf(`{"oid":"88001","comment_type":1,"marker":%d}`, i+1)),
			Type:         1, Action: "forward", Content: content, Timestamp: now.UnixMilli(), ReceivedAt: &received,
		})
	}
	return out
}

func TestPromptShowsRefsOnlyForThreadedPlatforms(t *testing.T) {
	cfg := biliTestConfig()
	rule := cfg.Rules.GroupRules["bilibili:BV1example0001"]
	rule.MaxReplyRunes = 200
	prompt := buildPrompt(batch{GroupID: "bilibili:BV1example0001", Rule: rule, Config: cfg, Messages: threadedComments(time.Now())})
	if !strings.Contains(prompt, `"ref":1`) || !strings.Contains(prompt, `"ref":3`) || !strings.Contains(prompt, "REPLY_TO: <ref>") || !strings.Contains(prompt, "共 3 条") {
		t.Fatalf("threaded prompt lacks refs or REPLY_TO instruction:\n%s", prompt)
	}
	single := buildPrompt(batch{GroupID: "bilibili:BV1example0001", Rule: rule, Config: cfg, Messages: threadedComments(time.Now())[:1]})
	if !strings.Contains(single, `"ref":1`) || !strings.Contains(single, "可省略 REPLY_TO") {
		t.Fatalf("single-message threaded prompt lacks optional instruction:\n%s", single)
	}
	wx := testConfig()
	wxRule := wx.Rules.GroupRules[groupA]
	wxPrompt := buildPrompt(batch{GroupID: groupA, Rule: wxRule, Config: wx, Messages: []RelayMessage{testMessage(groupA, "m1"), testMessage(groupA, "m2")}})
	if strings.Contains(wxPrompt, `"ref":`) || strings.Contains(wxPrompt, "REPLY_TO") {
		t.Fatalf("WeChat prompt must not change:\n%s", wxPrompt)
	}
}

func TestActionTargetForBatchUsesSelectedMessage(t *testing.T) {
	comments := threadedComments(time.Now())
	work := batch{GroupID: "bilibili:BV1example0001", Rule: GroupRule{TargetGroupID: "bilibili:BV1example0001"}, Messages: comments, ReplyTargetIndex: 1}
	target, kind, ctx, err := ActionTargetForBatch(work)
	if err != nil || kind != contracts.ActionThreadReply || target.ContentID != "rpid-101" || target.RootID != "root-1" || !strings.Contains(string(ctx), `"marker":1`) {
		t.Fatalf("selected target = %+v %s %s %v", target, kind, ctx, err)
	}
	for _, index := range []int{0, 4, -1} {
		work.ReplyTargetIndex = index
		if _, _, _, err := ActionTargetForBatch(work); err == nil {
			t.Fatalf("multi-message batch with index %d produced a target", index)
		}
	}
	work.Messages, work.ReplyTargetIndex = comments[:1], 0
	if target, _, _, err := ActionTargetForBatch(work); err != nil || target.ContentID != "rpid-101" {
		t.Fatalf("single-message batch target = %+v %v", target, err)
	}
}

type fakeThreadConnector struct {
	platform string
	mu       sync.Mutex
	actions  []map[string]any
}

func (f *fakeThreadConnector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health":
		platform := f.platform
		if platform == "" {
			platform = "bilibili"
		}
		fmt.Fprintf(w, `{"status":"ready","protocol_version":1,"platform":%q,"ready":true,"dry_run":false,"capabilities":["reply_text","thread_reply"]}`, platform)
	case "/v1/actions":
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		f.mu.Lock()
		f.actions = append(f.actions, body)
		f.mu.Unlock()
		fmt.Fprintf(w, `{"action_id":%q,"status":"confirmed","submitted":true,"external_id":"rpid-reply-1"}`, body["action_id"])
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeThreadConnector) posted() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[string]any(nil), f.actions...)
}

func runThreadedBatch(t *testing.T, modelOutput string) (contracts.Run, []map[string]any) {
	t.Helper()
	conn := &fakeThreadConnector{}
	server := httptest.NewServer(conn)
	t.Cleanup(server.Close)
	cfg := biliTestConfig()
	cfg.ActionEndpoints["bilibili"] = ActionEndpointConfig{URL: server.URL, TimeoutSec: 5}
	cfg.Rules.BatchSize = 3
	cfg.Rules.IdleTimeoutSec = 30
	cfg.Rules.MaxBatchWaitSec = 60
	d := newTestDispatcher(t, cfg)
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		return HermesResult{Text: modelOutput}, nil
	}
	for i, msg := range threadedComments(time.Now()) {
		if err := d.handleIncomingMessage(msg, fmt.Sprintf("relay:%d", i+1)); err != nil {
			t.Fatal(err)
		}
	}
	awaitCondition(t, func() bool {
		snapshot, err := d.trace.Snapshot()
		return err == nil && len(snapshot.Runs) == 1 && snapshot.Runs[0].Phase == contracts.PhaseTerminal
	})
	return onlyTraceRun(t, d), conn.posted()
}

func TestThreadedBatchRepliesToSelectedComment(t *testing.T) {
	run, actions := runThreadedBatch(t, "REPLY_TO: 1\n支持导出，在设置里打开即可。")
	if run.Delivery != contracts.DeliveryConfirmed || len(actions) != 1 {
		t.Fatalf("selected reply not delivered: run=%+v actions=%v", run, actions)
	}
	action := actions[0]
	target, _ := action["target"].(map[string]any)
	replyCtx, _ := action["reply_context"].(map[string]any)
	if target["content_id"] != "rpid-101" || target["root_id"] != "root-1" || replyCtx["marker"] != float64(1) {
		t.Fatalf("reply went to the wrong comment: %v", action)
	}
	if action["text"] != "支持导出，在设置里打开即可。" || strings.Contains(fmt.Sprint(action["text"]), "REPLY_TO") {
		t.Fatalf("directive leaked into reply text: %q", action["text"])
	}
}

func TestThreadedBatchFailsClosedWithoutValidTarget(t *testing.T) {
	for output, reason := range map[string]string{
		"直接回复，没有指定目标":                reasonReplyTargetMissing,
		"REPLY_TO: 7\n超出范围":          reasonReplyTargetInvalid,
		"REPLY_TO: rpid-999\n伪造的 ID": reasonReplyTargetInvalid,
		"REPLY_TO: abc":              reasonReplyTargetInvalid,
	} {
		t.Run(reason+"/"+output, func(t *testing.T) {
			run, actions := runThreadedBatch(t, output)
			if len(actions) != 0 || run.Outcome != contracts.OutcomeBlocked || run.ReasonCode != reason || run.Delivery != contracts.DeliveryBlocked || !run.DeliveryEvidence.NoSubmission {
				t.Fatalf("expected fail-closed %s with no action, got run=%+v actions=%v", reason, run, actions)
			}
		})
	}
	run, actions := runThreadedBatch(t, "REPLY_TO: 2\nNONE")
	if len(actions) != 0 || run.Outcome != contracts.OutcomeSilent {
		t.Fatalf("REPLY_TO + NONE must stay silent: %+v %v", run, actions)
	}
}

func TestOperatorAddedForumPlatformEndToEnd(t *testing.T) {
	conn := &fakeThreadConnector{platform: "zhihu"}
	server := httptest.NewServer(conn)
	t.Cleanup(server.Close)
	const container = "zhihu:question-1"
	cfg := biliTestConfig()
	cfg.ActionEndpoints = map[string]ActionEndpointConfig{"zhihu": {URL: server.URL, TimeoutSec: 5}}
	cfg.Rules.BatchSize = 2
	cfg.Rules.IdleTimeoutSec = 30
	cfg.Rules.MaxBatchWaitSec = 60
	cfg.Rules.GroupWhitelist = []string{container}
	cfg.Rules.GroupProfiles = map[string]string{}
	cfg.Rules.GroupRules = map[string]GroupRule{container: {Enabled: true, TargetGroupID: container, Target: "Demo Question", HermesProfile: "demo-zhihu"}}
	d := newTestDispatcher(t, cfg)
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		return HermesResult{Text: "REPLY_TO: 2\n可以试试导出功能。"}, nil
	}
	now := time.Now()
	for i, content := range []string{"路过", "有没有能批量导出的工具？"} {
		received := now
		msg := RelayMessage{
			ID: fmt.Sprintf("answer-%d", i+1), Source: "zhihu:poller", Platform: "zhihu", GroupID: container, GroupName: "Demo Question",
			SenderID: fmt.Sprintf("u%d", i+1), SenderName: fmt.Sprintf("user%d", i+1), RootID: "answer-root",
			ReplyContext: json.RawMessage(fmt.Sprintf(`{"answer_token":"tok-%d"}`, i+1)),
			Type:         1, Action: "forward", Content: content, Timestamp: now.UnixMilli(), ReceivedAt: &received,
		}
		if err := d.handleIncomingMessage(msg, fmt.Sprintf("relay:%d", i+1)); err != nil {
			t.Fatal(err)
		}
	}
	awaitCondition(t, func() bool {
		snapshot, err := d.trace.Snapshot()
		return err == nil && len(snapshot.Runs) == 1 && snapshot.Runs[0].Phase == contracts.PhaseTerminal
	})
	run, actions := onlyTraceRun(t, d), conn.posted()
	if run.Delivery != contracts.DeliveryConfirmed || len(actions) != 1 || len(run.Inputs) != 2 || run.Inputs[0].Source.Platform != "zhihu" {
		t.Fatalf("zhihu run not delivered: %+v %v", run, actions)
	}
	target, _ := actions[0]["target"].(map[string]any)
	replyCtx, _ := actions[0]["reply_context"].(map[string]any)
	if actions[0]["platform"] != "zhihu" || target["content_id"] != "answer-2" || target["account_id"] != "zhihu-connector" || replyCtx["answer_token"] != "tok-2" {
		t.Fatalf("zhihu action used the wrong target: %v", actions[0])
	}
}
