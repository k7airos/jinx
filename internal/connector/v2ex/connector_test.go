package v2ex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

type fakeDriver struct {
	mu       sync.Mutex
	ready    bool
	reason   string
	viewer   string
	comments []Comment
	fetchErr error
	replies  []ReplyRequest
	postFunc func(ctx context.Context, req ReplyRequest) (ReplyResult, error)
}

func (f *fakeDriver) Health(context.Context) (bool, string) {
	return f.ready, f.reason
}

func (f *fakeDriver) FetchComments(context.Context, TargetConfig) ([]Comment, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fetchErr != nil {
		return nil, f.viewer, f.fetchErr
	}
	out := make([]Comment, len(f.comments))
	copy(out, f.comments)
	return out, f.viewer, nil
}

func (f *fakeDriver) PostReply(ctx context.Context, req ReplyRequest) (ReplyResult, error) {
	f.mu.Lock()
	f.replies = append(f.replies, req)
	fn := f.postFunc
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return ReplyResult{
		Status:     "confirmed",
		Submitted:  connector.BoolPtr(true),
		ExternalID: "18330099",
	}, nil
}

func TestConfigDecodeDefaultsAndValidation(t *testing.T) {
	raw := []byte(`{
		"listen_addr": "127.0.0.1:18094",
		"relay_url": "http://127.0.0.1:18080",
		"state_dir": "/tmp/jinx-v2ex-test",
		"self_usernames": [" DemoBot "],
		"targets": [
			{"container_id": "v2ex:t-100001", "enabled": true}
		]
	}`)
	cfg, err := DecodeConfig(raw)
	if err != nil {
		t.Fatalf("expected valid minimal config, got %v", err)
	}
	if !cfg.IsDryRun() {
		t.Fatal("expected dry_run to default to true")
	}
	if !cfg.IsAutoMention() {
		t.Fatal("expected auto_mention to default to true")
	}
	if cfg.AccountID != "v2ex-ego" || cfg.PollIntervalSec != 60 || cfg.MaxInitialAgeSec != 300 ||
		cfg.Ego.TaskSpace != "jinx-v2ex-comments" || cfg.Ego.TimeoutSec != 45 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if len(cfg.SelfUsernames) != 1 || cfg.SelfUsernames[0] != "DemoBot" {
		t.Fatalf("unexpected self_usernames: %+v", cfg.SelfUsernames)
	}
	if len(cfg.Targets) != 1 || cfg.Targets[0].TopicID != "100001" ||
		cfg.Targets[0].URL != "https://www.v2ex.com/t/100001" || cfg.Targets[0].PollLimit != 20 {
		t.Fatalf("unexpected target defaults: %+v", cfg.Targets)
	}

	invalidCases := []struct {
		name string
		json string
	}{
		{
			name: "unknown_field",
			json: `{"listen_addr":"127.0.0.1:18094","relay_url":"http://127.0.0.1:18080","state_dir":"/tmp/s","extra":true,"targets":[]}`,
		},
		{
			name: "trailing_json",
			json: `{"listen_addr":"127.0.0.1:18094","relay_url":"http://127.0.0.1:18080","state_dir":"/tmp/s","targets":[]} {}`,
		},
		{
			name: "non_loopback_listen",
			json: `{"listen_addr":"0.0.0.0:18094","relay_url":"http://127.0.0.1:18080","state_dir":"/tmp/s","targets":[]}`,
		},
		{
			name: "non_loopback_relay",
			json: `{"listen_addr":"127.0.0.1:18094","relay_url":"https://example.com:18080","state_dir":"/tmp/s","targets":[]}`,
		},
		{
			name: "non_v2ex_container",
			json: `{"listen_addr":"127.0.0.1:18094","relay_url":"http://127.0.0.1:18080","state_dir":"/tmp/s","targets":[{"container_id":"bilibili:BV1example0001","enabled":true}]}`,
		},
		{
			name: "topic_id_mismatch",
			json: `{"listen_addr":"127.0.0.1:18094","relay_url":"http://127.0.0.1:18080","state_dir":"/tmp/s","targets":[{"container_id":"v2ex:t-100001","topic_id":"100002","enabled":true}]}`,
		},
		{
			name: "non_v2ex_host",
			json: `{"listen_addr":"127.0.0.1:18094","relay_url":"http://127.0.0.1:18080","state_dir":"/tmp/s","targets":[{"container_id":"v2ex:t-100001","url":"https://evil.example/t/100001","enabled":true}]}`,
		},
		{
			name: "wrong_path_topic",
			json: `{"listen_addr":"127.0.0.1:18094","relay_url":"http://127.0.0.1:18080","state_dir":"/tmp/s","targets":[{"container_id":"v2ex:t-100001","url":"https://www.v2ex.com/t/999999","enabled":true}]}`,
		},
		{
			name: "invalid_self_username",
			json: `{"listen_addr":"127.0.0.1:18094","relay_url":"http://127.0.0.1:18080","state_dir":"/tmp/s","self_usernames":["bad user!"],"targets":[]}`,
		},
		{
			name: "duplicate_target",
			json: `{"listen_addr":"127.0.0.1:18094","relay_url":"http://127.0.0.1:18080","state_dir":"/tmp/s","targets":[{"container_id":"v2ex:t-100001","enabled":true},{"container_id":"v2ex:t-100001","enabled":false}]}`,
		},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := DecodeConfig([]byte(tc.json)); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestNormalizeReplyIDAndFormatReplyText(t *testing.T) {
	idCases := map[string]string{
		"":              "",
		"0":             "",
		"r_0":           "",
		" r_18329962 \n": "18329962",
		"18330006":      "18330006",
	}
	for in, want := range idCases {
		if got := NormalizeReplyID(in); got != want {
			t.Fatalf("NormalizeReplyID(%q) = %q, want %q", in, got, want)
		}
	}

	if got := FormatReplyText("同意这个观点", "Alice", true); got != "@Alice 同意这个观点" {
		t.Fatalf("expected @Alice prefix, got %q", got)
	}
	if got := FormatReplyText("@alice 同意这个观点", "Alice", true); got != "@alice 同意这个观点" {
		t.Fatalf("expected case-insensitive existing @mention preserved, got %q", got)
	}
	if got := FormatReplyText("@Alice: 收到", "Alice", true); got != "@Alice: 收到" {
		t.Fatalf("expected punctuation-delimited @mention preserved, got %q", got)
	}
	if got := FormatReplyText("@AliceX 不是同一个人", "Alice", true); got != "@Alice @AliceX 不是同一个人" {
		t.Fatalf("expected @Alice prepended when prefix only matched substring, got %q", got)
	}
	if got := FormatReplyText("同意这个观点", "Alice", false); got != "同意这个观点" {
		t.Fatalf("expected text unchanged when autoMention=false, got %q", got)
	}
}

func TestEgoDriverFetchCommentsAndPostReply(t *testing.T) {
	var capturedScripts []string
	driver := NewEgoDriver(EgoConfig{
		TaskSpace:  "jinx-v2ex-test",
		TimeoutSec: 15,
	})
	target := TargetConfig{
		ContainerID: "v2ex:t-100001",
		TopicID:     "100001",
		URL:         "https://www.v2ex.com/t/100001",
		Enabled:     true,
		PollLimit:   20,
	}

	driver.runScript = func(_ context.Context, script string) ([]byte, error) {
		capturedScripts = append(capturedScripts, script)
		return []byte("debug\n" + egoResultMarker + `{"ok":true,"viewer_username":"botuser","comments":[{"reply_id":"r_101","floor":1,"sender_id":"alice","sender_name":"alice","content":"first reply","timestamp_ms":1700000001000,"is_self":false},{"reply_id":"102","floor":2,"sender_id":"BotUser","sender_name":"BotUser","content":"self reply","timestamp_ms":1700000002000,"is_self":false}]}` + "\n"), nil
	}
	comments, viewer, err := driver.FetchComments(context.Background(), target)
	if err != nil {
		t.Fatalf("FetchComments error: %v", err)
	}
	if viewer != "botuser" || len(comments) != 2 {
		t.Fatalf("unexpected fetch output viewer=%q comments=%+v", viewer, comments)
	}
	if comments[0].ReplyID != "101" || comments[0].TopicID != "100001" || comments[0].Floor != 1 || comments[0].IsSelf {
		t.Fatalf("unexpected first comment: %+v", comments[0])
	}
	if comments[1].ReplyID != "102" || !comments[1].IsSelf {
		t.Fatalf("expected case-insensitive self match, got %+v", comments[1])
	}
	for _, snippet := range []string{
		"keep: CONFIG.keepPage && page ? [page.label] : []",
		"user_control_active",
		"agentDelegatedToUser",
		`div[id^="r_"]`,
		`a.page_normal[href*="?p="]`,
		"captcha_required",
	} {
		if !strings.Contains(capturedScripts[0], snippet) {
			t.Fatalf("fetch script missing %q", snippet)
		}
	}

	// Challenge during fetch fails closed.
	driver.runScript = func(_ context.Context, _ string) ([]byte, error) {
		return []byte(egoResultMarker + `{"ok":false,"error_code":"captcha_required","detail":"challenge page detected"}`), nil
	}
	if _, _, err := driver.FetchComments(context.Background(), target); err == nil || !strings.Contains(err.Error(), "captcha_required") {
		t.Fatalf("expected captcha_required error, got %v", err)
	}

	replyReq := ReplyRequest{
		Target:        target,
		TopicID:       "100001",
		ReplyToID:     "101",
		ReplyToMember: "alice",
		ReplyToFloor:  1,
		Text:          "@alice hello forum",
	}

	// Confirmed reply.
	driver.runScript = func(_ context.Context, script string) ([]byte, error) {
		capturedScripts = append(capturedScripts, script)
		return []byte(egoResultMarker + `{"status":"confirmed","submitted":true,"external_id":"r_18330099"}`), nil
	}
	res, err := driver.PostReply(context.Background(), replyReq)
	if err != nil || res.Status != "confirmed" || res.ExternalID != "18330099" || res.Submitted == nil || !*res.Submitted {
		t.Fatalf("expected confirmed result, got res=%+v err=%v", res, err)
	}
	for _, snippet := range []string{
		`form[action^="/t/"]`,
		`input[name="once"]`,
		`.problem`,
		`redirect: "follow"`,
	} {
		if !strings.Contains(capturedScripts[1], snippet) {
			t.Fatalf("reply script missing %q", snippet)
		}
	}

	// Submitted without external_id.
	driver.runScript = func(context.Context, string) ([]byte, error) {
		return []byte(egoResultMarker + `{"status":"submitted","submitted":true,"reason_code":"missing_external_id"}`), nil
	}
	res, err = driver.PostReply(context.Background(), replyReq)
	if err != nil || res.Status != "submitted" || res.Submitted == nil || !*res.Submitted {
		t.Fatalf("expected submitted result, got res=%+v err=%v", res, err)
	}

	// Problem text classification -> failed + submitted: false.
	problemCases := []struct {
		detail string
		want   string
	}{
		{detail: "请不要在短期内重复发表相同内容，或者过于频繁回复", want: "rate_limited"},
		{detail: "请不要重复发送相同回复", want: "duplicate_reply"},
		{detail: "本主题已关闭，禁止回复", want: "replies_closed"},
		{detail: "请先登录后再试", want: "not_logged_in"},
		{detail: "请完成 Cloudflare 验证码校验", want: "captcha_required"},
		{detail: "未知错误提示", want: "platform_rejected"},
	}
	for _, tc := range problemCases {
		driver.runScript = func(context.Context, string) ([]byte, error) {
			raw, _ := json.Marshal(egoReplyOutput{
				Status:     "failed",
				Submitted:  connector.BoolPtr(false),
				ReasonCode: "problem",
				Detail:     tc.detail,
			})
			return append([]byte(egoResultMarker), raw...), nil
		}
		res, err = driver.PostReply(context.Background(), replyReq)
		if err != nil || res.Status != "failed" || res.Submitted == nil || *res.Submitted || res.ReasonCode != tc.want {
			t.Fatalf("detail=%q got res=%+v err=%v, want reason=%q", tc.detail, res, err, tc.want)
		}
	}

	// Transport error maps to unknown (never failed/retryable).
	driver.runScript = func(context.Context, string) ([]byte, error) {
		return nil, errors.New("process killed")
	}
	res, err = driver.PostReply(context.Background(), replyReq)
	if err != nil || res.Status != "unknown" || res.ReasonCode != "transport_unknown" {
		t.Fatalf("expected unknown/transport_unknown, got res=%+v err=%v", res, err)
	}
}

func TestConnectorPollOnceCursorAndEcho(t *testing.T) {
	var pushed []connector.InboundPayload
	var pushMu sync.Mutex
	relaySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/messages" {
			http.NotFound(w, r)
			return
		}
		var payload connector.InboundPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		pushMu.Lock()
		pushed = append(pushed, payload)
		pushMu.Unlock()
		_ = json.NewEncoder(w).Encode(connector.RelayPushResponse{Status: "ok", ID: payload.ID, Allowed: true, Action: "forward"})
	}))
	defer relaySrv.Close()

	now := time.Unix(1700001000, 0).UTC()
	drv := &fakeDriver{
		ready:  true,
		viewer: "DemoBot",
		comments: []Comment{
			{ReplyID: "100", TopicID: "100001", Floor: 1, SenderID: "olduser", SenderName: "olduser", Content: "historical", TimestampMS: now.Add(-10 * time.Minute).UnixMilli()},
			{ReplyID: "101", TopicID: "100001", Floor: 2, SenderID: "alice", SenderName: "alice", Content: "fresh question", TimestampMS: now.Add(-30 * time.Second).UnixMilli()},
			{ReplyID: "102", TopicID: "100001", Floor: 3, SenderID: "demobot", SenderName: "demobot", Content: "bot echo", TimestampMS: now.Add(-10 * time.Second).UnixMilli()},
		},
	}

	cfg := Config{
		ListenAddr:       "127.0.0.1:0",
		RelayURL:         relaySrv.URL,
		StateDir:         t.TempDir(),
		SelfUsernames:    []string{"DemoBot"},
		PollIntervalSec:  60,
		MaxInitialAgeSec: 300,
		Targets: []TargetConfig{
			{ContainerID: "v2ex:t-100001", Label: "Synthetic Topic", Enabled: true, PollLimit: 20},
		},
	}
	conn, err := NewConnector(cfg, drv)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	conn.now = func() time.Time { return now }

	report1, err := conn.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("PollOnce #1 failed: %v", err)
	}
	if report1.TargetsPolled != 1 || report1.CommentsFetched != 3 || report1.Forwarded != 2 || report1.SkippedHistorical != 1 {
		t.Fatalf("unexpected report1: %+v", report1)
	}
	if len(pushed) != 2 {
		t.Fatalf("expected 2 pushed items, got %d", len(pushed))
	}
	if pushed[0].Platform != "v2ex" || pushed[0].Source != "v2ex:ego" || pushed[0].GroupID != "v2ex:t-100001" || pushed[0].IsEcho {
		t.Fatalf("unexpected first pushed payload: %+v", pushed[0])
	}
	var rc replyContextPayload
	if err := json.Unmarshal(pushed[0].ReplyContext, &rc); err != nil {
		t.Fatalf("unmarshal reply_context: %v", err)
	}
	if rc.TopicID != "100001" || rc.ReplyID != "101" || rc.ReplyNo != 2 || rc.ReplyToMember != "alice" {
		t.Fatalf("unexpected reply_context: %+v", rc)
	}
	if !pushed[1].IsEcho {
		t.Fatalf("expected case-insensitive self reply to be marked is_echo=true: %+v", pushed[1])
	}

	// Second poll with same comments should skip all as seen.
	report2, err := conn.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("PollOnce #2 failed: %v", err)
	}
	if report2.Forwarded != 0 || report2.SkippedSeen != 3 {
		t.Fatalf("unexpected report2: %+v", report2)
	}
}

func TestConnectorExecuteGuardsAndServer(t *testing.T) {
	drv := &fakeDriver{ready: true, viewer: "DemoBot"}
	cfg := Config{
		ListenAddr:    "127.0.0.1:0",
		RelayURL:      "http://127.0.0.1:18080",
		StateDir:      t.TempDir(),
		SelfUsernames: []string{"DemoBot"},
		DryRun:        connector.BoolPtr(true),
		Targets: []TargetConfig{
			{ContainerID: "v2ex:t-100001", Label: "Synthetic Topic", Enabled: true, PollLimit: 20},
		},
	}
	conn, err := NewConnector(cfg, drv)
	if err != nil {
		t.Fatalf("NewConnector failed: %v", err)
	}
	now := time.Now().UTC()
	conn.now = func() time.Time { return now }

	srv := httptest.NewServer(conn.Handler())
	defer srv.Close()

	// 1. /health reports v2ex + dry_run=true.
	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	var health connector.HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode /health: %v", err)
	}
	resp.Body.Close()
	if health.Platform != "v2ex" || !health.Ready || !health.DryRun || len(health.Targets) != 1 {
		t.Fatalf("unexpected /health: %+v", health)
	}

	postAction := func(req connector.ActionRequest) (int, connector.ActionResponse) {
		raw, _ := json.Marshal(req)
		httpResp, err := http.Post(srv.URL+"/v1/actions", "application/json", bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("POST /v1/actions failed: %v", err)
		}
		defer httpResp.Body.Close()
		var out connector.ActionResponse
		if err := json.NewDecoder(httpResp.Body).Decode(&out); err != nil {
			t.Fatalf("decode action response: %v", err)
		}
		return httpResp.StatusCode, out
	}

	baseReq := connector.ActionRequest{
		ProtocolVersion: 1,
		ActionID:        "act-v2ex-1",
		RunID:           "run-v2ex-1",
		IdempotencyKey:  "idem-v2ex-1",
		Platform:        "v2ex",
		Kind:            string(contracts.ActionThreadReply),
		Text:            "这里是回复正文",
		ExpiresAt:       now.Add(5 * time.Minute).Format(time.RFC3339Nano),
		Target: connector.ActionTarget{
			Platform:    "v2ex",
			ContainerID: "v2ex:t-100001",
			ContentID:   "101",
		},
		ReplyContext: json.RawMessage(`{"topic_id":"100001","reply_id":"101","reply_no":2,"reply_to_member":"alice"}`),
	}

	// 2. Dry-run blocks execution.
	code, out := postAction(baseReq)
	if code != http.StatusConflict || out.ReasonCode != "dry_run" || out.Submitted == nil || *out.Submitted {
		t.Fatalf("expected dry_run block, got %d %+v", code, out)
	}

	// 3. Toggle dry_run=false via /v1/control/dry-run.
	ctrlBody, _ := json.Marshal(connector.DryRunControlRequest{DryRun: false, RequestID: "req-toggle-1"})
	ctrlResp, err := http.Post(srv.URL+"/v1/control/dry-run", "application/json", bytes.NewReader(ctrlBody))
	if err != nil || ctrlResp.StatusCode != http.StatusOK {
		t.Fatalf("dry-run toggle failed: resp=%v err=%v", ctrlResp, err)
	}
	ctrlResp.Body.Close()

	// 4. Thread reply auto-prepends @alice and confirms.
	var calls atomic.Int32
	drv.postFunc = func(_ context.Context, req ReplyRequest) (ReplyResult, error) {
		calls.Add(1)
		return ReplyResult{Status: "confirmed", Submitted: connector.BoolPtr(true), ExternalID: "18330099"}, nil
	}
	code, out = postAction(baseReq)
	if code != http.StatusOK || out.Status != "confirmed" || out.ExternalID != "18330099" {
		t.Fatalf("expected confirmed, got %d %+v", code, out)
	}
	if len(drv.replies) != 1 || drv.replies[0].Text != "@alice 这里是回复正文" {
		t.Fatalf("expected @alice prepended, got %+v", drv.replies)
	}

	// 5. Idempotency replay does not call driver again.
	code, out = postAction(baseReq)
	if code != http.StatusOK || out.ExternalID != "18330099" || calls.Load() != 1 {
		t.Fatalf("expected idempotent replay without re-execution, got %d %+v calls=%d", code, out, calls.Load())
	}

	// 6. Topic mismatch fails closed.
	mismatchReq := baseReq
	mismatchReq.ActionID = "act-v2ex-2"
	mismatchReq.IdempotencyKey = "idem-v2ex-2"
	mismatchReq.ReplyContext = json.RawMessage(`{"topic_id":"999999","reply_to_member":"alice"}`)
	code, out = postAction(mismatchReq)
	if code != http.StatusUnprocessableEntity || out.ReasonCode != "target_mismatch" || calls.Load() != 1 {
		t.Fatalf("expected target_mismatch, got %d %+v", code, out)
	}

	// 7. Invalid reply_to_member fails closed.
	badMemberReq := baseReq
	badMemberReq.ActionID = "act-v2ex-3"
	badMemberReq.IdempotencyKey = "idem-v2ex-3"
	badMemberReq.ReplyContext = json.RawMessage(`{"topic_id":"100001","reply_to_member":"bad member!"}`)
	code, out = postAction(badMemberReq)
	if code != http.StatusUnprocessableEntity || out.ReasonCode != "invalid_reply_context" || calls.Load() != 1 {
		t.Fatalf("expected invalid_reply_context, got %d %+v", code, out)
	}

	// 8. ActionReplyText does not prepend @alice.
	plainReq := baseReq
	plainReq.ActionID = "act-v2ex-4"
	plainReq.IdempotencyKey = "idem-v2ex-4"
	plainReq.Kind = string(contracts.ActionReplyText)
	plainReq.Target.ContentID = ""
	code, out = postAction(plainReq)
	if code != http.StatusOK || out.Status != "confirmed" || calls.Load() != 2 {
		t.Fatalf("expected plain reply confirmed, got %d %+v calls=%d", code, out, calls.Load())
	}
	if drv.replies[1].Text != "这里是回复正文" {
		t.Fatalf("expected no @mention for reply_text, got %q", drv.replies[1].Text)
	}
}
