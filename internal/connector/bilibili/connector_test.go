package bilibili

import (
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
	mu          sync.Mutex
	ready       bool
	reason      string
	viewer      string
	comments    []Comment
	commentsBy  map[string][]Comment
	searchRes   []VideoCandidate
	searchCalls []SearchQuery
	fetchErr    error
	searchErr   error
	replies     []ReplyRequest
	postFunc    func(ctx context.Context, req ReplyRequest) (ReplyResult, error)
}

func (f *fakeDriver) Health(context.Context) (bool, string) {
	return f.ready, f.reason
}

func (f *fakeDriver) FetchComments(_ context.Context, target TargetConfig) ([]Comment, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fetchErr != nil {
		return nil, f.viewer, f.fetchErr
	}
	src := f.comments
	if f.commentsBy != nil {
		if specific, ok := f.commentsBy[target.ContainerID]; ok {
			src = specific
		}
	}
	out := make([]Comment, len(src))
	copy(out, src)
	return out, f.viewer, nil
}

func (f *fakeDriver) SearchVideos(_ context.Context, query SearchQuery) ([]VideoCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.searchCalls = append(f.searchCalls, query)
	if f.searchErr != nil {
		return nil, f.searchErr
	}
	out := make([]VideoCandidate, len(f.searchRes))
	copy(out, f.searchRes)
	return out, nil
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
		ExternalID: "9001001",
	}, nil
}

func TestConfigDecodeDefaultsAndValidation(t *testing.T) {
	raw := []byte(`{
		"listen_addr": "127.0.0.1:18092",
		"relay_url": "http://127.0.0.1:8080",
		"state_dir": "/tmp/jinx-bili-test",
		"targets": [
			{"container_id": "bilibili:BV1example0001", "enabled": true}
		]
	}`)
	cfg, err := DecodeConfig(raw)
	if err != nil {
		t.Fatalf("expected valid minimal config, got %v", err)
	}
	if !cfg.IsDryRun() {
		t.Fatal("expected dry_run to default to true when omitted")
	}
	if cfg.AccountID != "bilibili-ego" || cfg.PollIntervalSec != 30 || cfg.MaxInitialAgeSec != 300 ||
		cfg.Ego.TaskSpace != "jinx-bilibili-comments" || cfg.Ego.TimeoutSec != 45 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if len(cfg.Targets) != 1 || cfg.Targets[0].BVID != "BV1example0001" ||
		cfg.Targets[0].URL != "https://www.bilibili.com/video/BV1example0001" ||
		cfg.Targets[0].CommentType != 1 || cfg.Targets[0].PollLimit != 20 {
		t.Fatalf("unexpected target defaults: %+v", cfg.Targets)
	}

	invalidCases := []struct {
		name string
		json string
	}{
		{
			name: "unknown_field",
			json: `{"listen_addr":"127.0.0.1:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","extra":true,"targets":[]}`,
		},
		{
			name: "non_loopback_listen",
			json: `{"listen_addr":"0.0.0.0:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","targets":[]}`,
		},
		{
			name: "non_loopback_relay",
			json: `{"listen_addr":"127.0.0.1:18092","relay_url":"https://example.com","state_dir":"/tmp/s","targets":[]}`,
		},
		{
			name: "non_bilibili_container",
			json: `{"listen_addr":"127.0.0.1:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","targets":[{"container_id":"123@chatroom","enabled":true}]}`,
		},
		{
			name: "non_bilibili_host",
			json: `{"listen_addr":"127.0.0.1:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","targets":[{"container_id":"bilibili:BV1example0001","url":"https://evil.example/video/BV1example0001","enabled":true}]}`,
		},
		{
			name: "duplicate_target",
			json: `{"listen_addr":"127.0.0.1:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","targets":[{"container_id":"bilibili:BV1example0001","enabled":true},{"container_id":"bilibili:BV1example0001","enabled":false}]}`,
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

func TestNormalizeReplyID(t *testing.T) {
	cases := map[string]string{
		"":                        "",
		"0":                       "",
		"  0  ":                   "",
		" 1234567890123456789 \n": "1234567890123456789",
	}
	for in, want := range cases {
		if got := NormalizeReplyID(in); got != want {
			t.Fatalf("NormalizeReplyID(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEgoDriverFetchCommentsAndPostReply(t *testing.T) {
	var capturedScripts []string
	driver := NewEgoDriver(EgoConfig{
		TaskSpace:  "jinx-test-space",
		TimeoutSec: 15,
	})
	target := TargetConfig{
		ContainerID: "bilibili:BV1example0001",
		BVID:        "BV1example0001",
		OID:         "777001",
		CommentType: 1,
		URL:         "https://www.bilibili.com/video/BV1example0001",
		Enabled:     true,
		PollLimit:   20,
	}

	driver.runScript = func(_ context.Context, script string) ([]byte, error) {
		capturedScripts = append(capturedScripts, script)
		return []byte("debug log line\n" + egoResultMarker + `{"ok":true,"oid":"","viewer_mid":"5001","comments":[{"rpid":"101","root":"0","parent":"0","sender_id":"2001","sender_name":"alice","content":"first top comment","timestamp_ms":1700000001000,"is_self":false},{"rpid":"102","root":"101","parent":"101","sender_id":"5001","sender_name":"bot","content":"bot reply","timestamp_ms":1700000002000,"is_self":false}]}` + "\n"), nil
	}
	comments, viewerMID, err := driver.FetchComments(context.Background(), target)
	if err != nil {
		t.Fatalf("FetchComments error: %v", err)
	}
	if viewerMID != "5001" || len(comments) != 2 {
		t.Fatalf("unexpected fetch output viewer=%q comments=%+v", viewerMID, comments)
	}
	if comments[0].OID != "777001" || comments[0].RootID != "" || comments[0].ParentID != "" || comments[0].IsSelf {
		t.Fatalf("unexpected normalized top comment: %+v", comments[0])
	}
	if comments[1].RootID != "101" || comments[1].ParentID != "101" || !comments[1].IsSelf {
		t.Fatalf("unexpected sub/self comment: %+v", comments[1])
	}
	for _, snippet := range []string{
		"keep: CONFIG.keepPage && page ? [page.label] : []",
		"user_control_active",
		"agentDelegatedToUser",
		"https://api.bilibili.com/x/v2/reply",
	} {
		if !strings.Contains(capturedScripts[0], snippet) {
			t.Fatalf("fetch script missing %q", snippet)
		}
	}

	replyReq := ReplyRequest{
		Target:      target,
		OID:         "777001",
		CommentType: 1,
		RootID:      "101",
		ParentID:    "101",
		Text:        "hello thread",
	}

	// Confirmed reply.
	driver.runScript = func(_ context.Context, script string) ([]byte, error) {
		capturedScripts = append(capturedScripts, script)
		return []byte(egoResultMarker + `{"status":"confirmed","submitted":true,"external_id":"987654321098765432"}`), nil
	}
	res, err := driver.PostReply(context.Background(), replyReq)
	if err != nil || res.Status != "confirmed" || res.ExternalID != "987654321098765432" || res.Submitted == nil || !*res.Submitted {
		t.Fatalf("expected confirmed result, got res=%+v err=%v", res, err)
	}
	for _, snippet := range []string{
		"keep: CONFIG.keepPage && page ? [page.label] : []",
		"user_control_active",
		"https://api.bilibili.com/x/v2/reply/add",
	} {
		if !strings.Contains(capturedScripts[1], snippet) {
			t.Fatalf("reply script missing %q", snippet)
		}
	}

	// Confirmed without valid external_id downgrades to submitted.
	driver.runScript = func(context.Context, string) ([]byte, error) {
		return []byte(egoResultMarker + `{"status":"confirmed","submitted":true,"external_id":"0"}`), nil
	}
	res, err = driver.PostReply(context.Background(), replyReq)
	if err != nil || res.Status != "submitted" || res.Submitted == nil || !*res.Submitted || res.ReasonCode != "missing_external_id" {
		t.Fatalf("expected submitted downgrade for empty external_id, got %+v %v", res, err)
	}

	// Async submitted status.
	driver.runScript = func(context.Context, string) ([]byte, error) {
		return []byte(egoResultMarker + `{"status":"submitted","submitted":true,"reason_code":"accepted_without_id"}`), nil
	}
	res, err = driver.PostReply(context.Background(), replyReq)
	if err != nil || res.Status != "submitted" || res.Submitted == nil || !*res.Submitted {
		t.Fatalf("expected submitted result, got %+v %v", res, err)
	}

	// Known Bilibili failure code mapping with submitted=false.
	errorCases := []struct {
		code int
		want string
	}{
		{-101, "not_logged_in"},
		{12002, "comments_closed"},
		{12015, "captcha_required"},
		{12022, "comment_deleted"},
		{12025, "rate_limited"},
	}
	for _, tc := range errorCases {
		driver.runScript = func(context.Context, string) ([]byte, error) {
			payload, _ := json.Marshal(egoReplyOutput{
				Status:    "failed",
				Submitted: connector.BoolPtr(false),
				Code:      tc.code,
				Detail:    "rejected",
			})
			return append([]byte(egoResultMarker), payload...), nil
		}
		res, err = driver.PostReply(context.Background(), replyReq)
		if err != nil || res.Status != "failed" || res.Submitted == nil || *res.Submitted || res.ReasonCode != tc.want {
			t.Fatalf("code %d: got %+v err=%v, want failed/%s", tc.code, res, err, tc.want)
		}
	}

	// Transport/script error maps to unknown without falsely claiming submitted=false.
	driver.runScript = func(context.Context, string) ([]byte, error) {
		return nil, errors.New("ego timeout")
	}
	res, err = driver.PostReply(context.Background(), replyReq)
	if err != nil || res.Status != "unknown" || res.Submitted != nil || res.ReasonCode != "transport_unknown" {
		t.Fatalf("expected transport_unknown, got %+v %v", res, err)
	}

	// Missing marker maps to unknown.
	driver.runScript = func(context.Context, string) ([]byte, error) {
		return []byte("no marker here"), nil
	}
	res, err = driver.PostReply(context.Background(), replyReq)
	if err != nil || res.Status != "unknown" || res.Submitted != nil || res.ReasonCode != "output_parse_unknown" {
		t.Fatalf("expected output_parse_unknown, got %+v %v", res, err)
	}
}

func TestConnectorPollOnceCursorAndSelfDetection(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	var mu sync.Mutex
	var pushed []connector.InboundPayload
	var failFirst atomic.Bool

	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/messages" {
			http.NotFound(w, r)
			return
		}
		if failFirst.Load() {
			http.Error(w, "temporary relay error", http.StatusInternalServerError)
			return
		}
		var payload connector.InboundPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		pushed = append(pushed, payload)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(connector.RelayPushResponse{
			Status:  "ok",
			ID:      payload.ID,
			Allowed: true,
			Action:  "forward",
		})
	}))
	defer relay.Close()

	drv := &fakeDriver{
		ready:  true,
		viewer: "8801",
		comments: []Comment{
			{
				RPID:        "old-1",
				OID:         "321",
				BVID:        "BV1example0001",
				CommentType: 1,
				SenderID:    "1001",
				SenderName:  "old-user",
				Content:     "historical comment",
				TimestampMS: now.Add(-10 * time.Minute).UnixMilli(),
			},
			{
				RPID:        "fresh-user",
				OID:         "321",
				BVID:        "BV1example0001",
				CommentType: 1,
				RootID:      "root-10",
				ParentID:    "parent-10",
				SenderID:    "1002",
				SenderName:  "alice",
				Content:     "fresh question",
				TimestampMS: now.Add(-10 * time.Second).UnixMilli(),
			},
			{
				RPID:        "fresh-viewer-self",
				OID:         "321",
				BVID:        "BV1example0001",
				CommentType: 1,
				SenderID:    "8801",
				SenderName:  "viewer-bot",
				Content:     "viewer self",
				TimestampMS: now.Add(-5 * time.Second).UnixMilli(),
			},
			{
				RPID:        "fresh-cfg-self",
				OID:         "321",
				BVID:        "BV1example0001",
				CommentType: 1,
				SenderID:    "8802",
				SenderName:  "cfg-bot",
				Content:     "cfg self",
				TimestampMS: now.Add(-3 * time.Second).UnixMilli(),
			},
			{
				RPID:        "fresh-driver-self",
				OID:         "321",
				BVID:        "BV1example0001",
				CommentType: 1,
				SenderID:    "8803",
				SenderName:  "drv-bot",
				Content:     "drv self",
				TimestampMS: now.Add(-1 * time.Second).UnixMilli(),
				IsSelf:      true,
			},
		},
	}

	cfg := Config{
		ListenAddr:       "127.0.0.1:0",
		RelayURL:         relay.URL,
		StateDir:         t.TempDir(),
		AccountID:        "bilibili-ego",
		SelfMIDs:         []string{"8802"},
		PollIntervalSec:  30,
		MaxInitialAgeSec: 300,
		Targets: []TargetConfig{
			{ContainerID: "bilibili:BV1example0001", Label: "Demo Video", Enabled: true},
		},
	}
	conn, err := NewConnector(cfg, drv)
	if err != nil {
		t.Fatal(err)
	}
	conn.now = func() time.Time { return now }

	// First poll seeds target, skips historical comment, pushes 4 fresh comments.
	rep1, err := conn.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("PollOnce #1 error: %v", err)
	}
	if rep1.TargetsPolled != 1 || rep1.CommentsFetched != 5 || rep1.SkippedHistorical != 1 || rep1.Forwarded != 4 || rep1.PushErrors != 0 {
		t.Fatalf("unexpected PollOnce #1 report: %+v", rep1)
	}

	mu.Lock()
	if len(pushed) != 4 {
		mu.Unlock()
		t.Fatalf("expected 4 pushed payloads, got %d", len(pushed))
	}
	byID := make(map[string]connector.InboundPayload, len(pushed))
	for _, p := range pushed {
		byID[p.ID] = p
	}
	mu.Unlock()

	userMsg := byID["fresh-user"]
	if userMsg.Source != "bilibili:ego" || userMsg.Platform != "bilibili" || userMsg.GroupID != "bilibili:BV1example0001" ||
		userMsg.ParentID != "parent-10" || userMsg.RootID != "root-10" || userMsg.IsEcho {
		t.Fatalf("unexpected fresh user payload: %+v", userMsg)
	}
	var rc replyContextPayload
	if err := json.Unmarshal(userMsg.ReplyContext, &rc); err != nil || rc.OID != "321" || rc.BVID != "BV1example0001" || rc.RPID != "fresh-user" {
		t.Fatalf("unexpected reply_context: %s %v", userMsg.ReplyContext, err)
	}
	for _, selfID := range []string{"fresh-viewer-self", "fresh-cfg-self", "fresh-driver-self"} {
		if !byID[selfID].IsEcho {
			t.Fatalf("expected IsEcho=true for %s: %+v", selfID, byID[selfID])
		}
	}

	// Second poll skips all 5 previously seen / skipped historical comments.
	rep2, err := conn.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("PollOnce #2 error: %v", err)
	}
	if rep2.Forwarded != 0 || rep2.SkippedSeen != 5 {
		t.Fatalf("expected 5 skipped_seen on second poll, got %+v", rep2)
	}

	// Push failure leaves failed item un-seen and container un-seeded so next poll retries.
	mu.Lock()
	pushed = nil
	mu.Unlock()
	drv2 := &fakeDriver{
		ready: true,
		comments: []Comment{
			{RPID: "retry-1", OID: "321", Content: "retry me", TimestampMS: now.UnixMilli()},
		},
	}
	cfg2 := Config{
		ListenAddr:       "127.0.0.1:0",
		RelayURL:         relay.URL,
		StateDir:         t.TempDir(),
		PollIntervalSec:  30,
		MaxInitialAgeSec: 300,
		Targets: []TargetConfig{
			{ContainerID: "bilibili:BV1example0001", Enabled: true},
		},
	}
	conn2, err := NewConnector(cfg2, drv2)
	if err != nil {
		t.Fatal(err)
	}
	conn2.now = func() time.Time { return now }

	failFirst.Store(true)
	repFail, err := conn2.PollOnce(context.Background())
	if err == nil || repFail.PushErrors != 1 || repFail.Forwarded != 0 {
		t.Fatalf("expected push error on failed poll, got rep=%+v err=%v", repFail, err)
	}
	if conn2.cursors.IsSeeded("bilibili:BV1example0001") || conn2.cursors.HasSeen("bilibili:BV1example0001", "retry-1") {
		t.Fatal("push error should prevent marking seeded or seen")
	}

	failFirst.Store(false)
	repRetry, err := conn2.PollOnce(context.Background())
	if err != nil || repRetry.Forwarded != 1 || repRetry.PushErrors != 0 {
		t.Fatalf("expected successful retry on next poll, got rep=%+v err=%v", repRetry, err)
	}
	if !conn2.cursors.IsSeeded("bilibili:BV1example0001") || !conn2.cursors.HasSeen("bilibili:BV1example0001", "retry-1") {
		t.Fatal("successful retry should mark seeded and seen")
	}
}

func TestConnectorExecuteThreadingAndValidation(t *testing.T) {
	relay := httptest.NewServer(http.NotFoundHandler())
	defer relay.Close()

	drv := &fakeDriver{ready: true}
	cfg := Config{
		ListenAddr: "127.0.0.1:0",
		RelayURL:   relay.URL,
		StateDir:   t.TempDir(),
		DryRun:     connector.BoolPtr(false),
		Targets: []TargetConfig{
			{ContainerID: "bilibili:BV1example0001", Enabled: true, OID: "555", CommentType: 1},
			{ContainerID: "bilibili:BV1bvidonly01", Enabled: true, BVID: "BV1bvidonly01", CommentType: 1},
			{ContainerID: "bilibili:BV1disabled001", Enabled: false},
		},
	}
	conn, err := NewConnector(cfg, drv)
	if err != nil {
		t.Fatal(err)
	}

	// Top-level comment reply uses trusted TargetConfig OID/CommentType and sets root=parent=content_id.
	respTop, err := conn.Execute(context.Background(), connector.ActionRequest{
		ActionID:     "act-top",
		Text:         "reply to top",
		ReplyContext: json.RawMessage(`{"oid":"555","comment_type":1}`),
		Target: connector.ActionTarget{
			Platform:    string(contracts.PlatformBilibili),
			ContainerID: "bilibili:BV1example0001",
			ContentID:   "rpid-100",
		},
	})
	if err != nil || respTop.Status != "confirmed" || respTop.ExternalID != "9001001" {
		t.Fatalf("top reply execute failed: %+v %v", respTop, err)
	}

	// Sub-comment reply preserves root and sets parent=content_id.
	respSub, err := conn.Execute(context.Background(), connector.ActionRequest{
		ActionID:     "act-sub",
		Text:         "reply to sub",
		ReplyContext: json.RawMessage(`{"oid":"555"}`),
		Target: connector.ActionTarget{
			Platform:    string(contracts.PlatformBilibili),
			ContainerID: "bilibili:BV1example0001",
			ContentID:   "rpid-102",
			RootID:      "rpid-100",
		},
	})
	if err != nil || respSub.Status != "confirmed" {
		t.Fatalf("sub reply execute failed: %+v %v", respSub, err)
	}

	// Untrusted reply_context cannot redirect target OID or CommentType.
	for _, badCtx := range []string{
		`{"oid":"999999","comment_type":1}`,
		`{"oid":"555","comment_type":11}`,
		`{"bvid":"BV1other00001","oid":"555"}`,
	} {
		respBadCtx, err := conn.Execute(context.Background(), connector.ActionRequest{
			ActionID:     "act-mismatch",
			Text:         "should block",
			ReplyContext: json.RawMessage(badCtx),
			Target: connector.ActionTarget{
				Platform:    string(contracts.PlatformBilibili),
				ContainerID: "bilibili:BV1example0001",
				ContentID:   "rpid-100",
			},
		})
		if err != nil || respBadCtx.Status != "failed" || respBadCtx.Submitted == nil || *respBadCtx.Submitted || respBadCtx.ReasonCode != "target_mismatch" {
			t.Fatalf("expected target_mismatch for %s, got %+v %v", badCtx, respBadCtx, err)
		}
	}

	// When config omits OID and FetchComments has not resolved it yet, Execute never trusts rc.OID directly.
	respBvidOnly, err := conn.Execute(context.Background(), connector.ActionRequest{
		ActionID:     "act-bvid-only",
		Text:         "reply resolving on target page",
		ReplyContext: json.RawMessage(`{"oid":"777777"}`),
		Target: connector.ActionTarget{
			Platform:    string(contracts.PlatformBilibili),
			ContainerID: "bilibili:BV1bvidonly01",
			ContentID:   "rpid-200",
		},
	})
	if err != nil || respBvidOnly.Status != "confirmed" {
		t.Fatalf("bvid-only execute failed: %+v %v", respBvidOnly, err)
	}

	drv.mu.Lock()
	if len(drv.replies) != 3 {
		drv.mu.Unlock()
		t.Fatalf("expected 3 driver PostReply calls, got %d", len(drv.replies))
	}
	topReq, subReq, bvidOnlyReq := drv.replies[0], drv.replies[1], drv.replies[2]
	drv.mu.Unlock()

	if topReq.OID != "555" || topReq.CommentType != 1 || topReq.RootID != "rpid-100" || topReq.ParentID != "rpid-100" || topReq.Text != "reply to top" {
		t.Fatalf("unexpected top PostReply args: %+v", topReq)
	}
	if subReq.OID != "555" || subReq.CommentType != 1 || subReq.RootID != "rpid-100" || subReq.ParentID != "rpid-102" {
		t.Fatalf("unexpected sub PostReply args: %+v", subReq)
	}
	if bvidOnlyReq.OID != "" || bvidOnlyReq.CommentType != 1 {
		t.Fatalf("expected untrusted rc.OID not to bypass empty OID resolution, got %+v", bvidOnlyReq)
	}

	// Unconfigured and disabled targets return pre-submission failed without calling Driver.
	for _, container := range []string{"bilibili:BV1missing000", "bilibili:BV1disabled001"} {
		resp, err := conn.Execute(context.Background(), connector.ActionRequest{
			ActionID: "act-bad",
			Text:     "should fail",
			Target: connector.ActionTarget{
				Platform:    string(contracts.PlatformBilibili),
				ContainerID: container,
				ContentID:   "rpid-1",
			},
		})
		if err != nil || resp.Status != "failed" || resp.Submitted == nil || *resp.Submitted || resp.ReasonCode != "target_not_configured" {
			t.Fatalf("expected target_not_configured for %s, got %+v %v", container, resp, err)
		}
	}
	drv.mu.Lock()
	totalCalls := len(drv.replies)
	drv.mu.Unlock()
	if totalCalls != 3 {
		t.Fatalf("expected no additional PostReply calls for unconfigured targets, got %d", totalCalls)
	}
}

func TestConnectorHealthStatusAndDryRunControl(t *testing.T) {
	now := time.Unix(1700000500, 0).UTC()
	var failRelay atomic.Bool
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failRelay.Load() {
			http.Error(w, "relay down", http.StatusBadGateway)
			return
		}
		var payload connector.InboundPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(connector.RelayPushResponse{
			Status:  "ok",
			ID:      payload.ID,
			Allowed: true,
			Action:  "forward",
		})
	}))
	defer relay.Close()

	drv := &fakeDriver{
		ready:    true,
		fetchErr: errors.New("ego timeout"),
	}
	cfg := Config{
		ListenAddr:       "127.0.0.1:0",
		RelayURL:         relay.URL,
		StateDir:         t.TempDir(),
		AccountID:        "bilibili-ego",
		PollIntervalSec:  30,
		MaxInitialAgeSec: 300,
		Targets: []TargetConfig{
			{ContainerID: "bilibili:BV1zzzzz00002", Label: "Second", Enabled: false},
			{ContainerID: "bilibili:BV1aaaaa00001", Label: "First", Enabled: true},
		},
	}
	conn, err := NewConnector(cfg, drv)
	if err != nil {
		t.Fatal(err)
	}
	conn.now = func() time.Time { return now }

	readHealth := func() connector.HealthResponse {
		t.Helper()
		rec := httptest.NewRecorder()
		conn.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "http://127.0.0.1/health", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET /health failed: %d %s", rec.Code, rec.Body.String())
		}
		var h connector.HealthResponse
		if err := json.NewDecoder(rec.Body).Decode(&h); err != nil {
			t.Fatal(err)
		}
		return h
	}

	// Initial health before polling: sorted targets, dry_run=true, controls=["dry_run"], no last_poll_at.
	h0 := readHealth()
	if !h0.Ready || !h0.DryRun || len(h0.Controls) != 1 || h0.Controls[0] != "dry_run" ||
		len(h0.Targets) != 2 || h0.Targets[0].ContainerID != "bilibili:BV1aaaaa00001" ||
		h0.Targets[1].ContainerID != "bilibili:BV1zzzzz00002" || h0.LastPollAt != nil || h0.LastPollErrorCode != "" {
		t.Fatalf("unexpected initial health: %+v", h0)
	}

	// Fetch failure records fetch_failed and poll timestamp.
	if _, err := conn.PollOnce(context.Background()); err == nil {
		t.Fatal("expected fetch error")
	}
	hFetchErr := readHealth()
	if hFetchErr.LastPollAt == nil || !hFetchErr.LastPollAt.Equal(now) || hFetchErr.LastPollErrorCode != "fetch_failed" {
		t.Fatalf("unexpected health after fetch failure: %+v", hFetchErr)
	}

	// Relay push failure records relay_push_failed.
	now = now.Add(10 * time.Second)
	drv.mu.Lock()
	drv.fetchErr = nil
	drv.comments = []Comment{{RPID: "c-1", OID: "111", SenderID: "900", Content: "hi", TimestampMS: now.UnixMilli()}}
	drv.mu.Unlock()
	failRelay.Store(true)
	if _, err := conn.PollOnce(context.Background()); err == nil {
		t.Fatal("expected relay push error")
	}
	hPushErr := readHealth()
	if hPushErr.LastPollAt == nil || !hPushErr.LastPollAt.Equal(now) || hPushErr.LastPollErrorCode != "relay_push_failed" {
		t.Fatalf("unexpected health after relay push failure: %+v", hPushErr)
	}

	// Clean poll clears last_poll_error_code.
	now = now.Add(10 * time.Second)
	failRelay.Store(false)
	if _, err := conn.PollOnce(context.Background()); err != nil {
		t.Fatalf("expected clean poll, got %v", err)
	}
	hClean := readHealth()
	if hClean.LastPollAt == nil || !hClean.LastPollAt.Equal(now) || hClean.LastPollErrorCode != "" {
		t.Fatalf("unexpected health after clean poll: %+v", hClean)
	}

	// Toggle dry_run via HTTP control endpoint and verify /v1/actions transitions from 409 dry_run to 200 confirmed.
	recCtrl := httptest.NewRecorder()
	reqCtrl := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/control/dry-run", strings.NewReader(`{"dry_run":false,"request_id":"bili-live-1"}`))
	reqCtrl.Header.Set("Content-Type", "application/json")
	conn.Handler().ServeHTTP(recCtrl, reqCtrl)
	if recCtrl.Code != http.StatusOK {
		t.Fatalf("dry-run toggle failed: %d %s", recCtrl.Code, recCtrl.Body.String())
	}
	if hLive := readHealth(); hLive.DryRun {
		t.Fatalf("expected health.DryRun=false after control toggle, got %+v", hLive)
	}

	actionBody, _ := json.Marshal(connector.ActionRequest{
		ProtocolVersion: 1,
		ActionID:        "act-live-1",
		RunID:           "run-live-1",
		IdempotencyKey:  "idem-bili-live-1",
		Platform:        "bilibili",
		Kind:            "thread_reply",
		Text:            "live comment reply",
		ExpiresAt:       now.Add(time.Minute).Format(time.RFC3339Nano),
		Target: connector.ActionTarget{
			Platform:    "bilibili",
			ContainerID: "bilibili:BV1aaaaa00001",
			ContentID:   "c-1",
		},
	})
	recAct := httptest.NewRecorder()
	reqAct := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/actions", strings.NewReader(string(actionBody)))
	reqAct.Header.Set("Content-Type", "application/json")
	conn.Handler().ServeHTTP(recAct, reqAct)
	if recAct.Code != http.StatusOK {
		t.Fatalf("expected 200 live action after dry_run=false, got %d %s", recAct.Code, recAct.Body.String())
	}
}

func TestSearchTargetsConfigAndEgoSearchAndFetch(t *testing.T) {
	raw := []byte(`{
		"listen_addr": "127.0.0.1:18092",
		"relay_url": "http://127.0.0.1:8080",
		"state_dir": "/tmp/jinx-bili-search-test",
		"targets": [],
		"search_targets": [
			{"keyword": "  AI Agent  ", "enabled": true}
		]
	}`)
	cfg, err := DecodeConfig(raw)
	if err != nil {
		t.Fatalf("expected valid search config, got %v", err)
	}
	if cfg.SearchRefreshSec != 300 || len(cfg.SearchTargets) != 1 {
		t.Fatalf("unexpected search defaults: %+v", cfg)
	}
	st := cfg.SearchTargets[0]
	if st.Keyword != "AI Agent" || st.Order != "totalrank" || st.MaxVideos != 1 || st.CommentType != 1 || st.PollLimit != 20 || st.SortMode != "time" {
		t.Fatalf("unexpected search_target defaults: %+v", st)
	}

	for _, bad := range []string{
		`{"listen_addr":"127.0.0.1:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","targets":[{"container_id":"bilibili:BV1example0001","sort_mode":"bad","enabled":true}]}`,
		`{"listen_addr":"127.0.0.1:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","targets":[{"container_id":"bilibili:BV1example0001","sub_reply_limit":25,"enabled":true}]}`,
		`{"listen_addr":"127.0.0.1:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","search_refresh_sec":10,"targets":[]}`,
		`{"listen_addr":"127.0.0.1:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","targets":[],"search_targets":[{"keyword":"   ","enabled":true}]}`,
		`{"listen_addr":"127.0.0.1:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","targets":[],"search_targets":[{"keyword":"go","order":"invalid","enabled":true}]}`,
		`{"listen_addr":"127.0.0.1:18092","relay_url":"http://127.0.0.1:8080","state_dir":"/tmp/s","targets":[],"search_targets":[{"keyword":"go","max_videos":9,"enabled":true}]}`,
	} {
		if _, err := DecodeConfig([]byte(bad)); err == nil {
			t.Fatalf("expected DecodeConfig to fail for %s", bad)
		}
	}

	// Verify EgoDriver.SearchVideos parsing, deduplication, and error classification.
	drv := NewEgoDriver(EgoConfig{TaskSpace: "jinx-test", TimeoutSec: 10})
	var capturedSearchScript string
	drv.runScript = func(_ context.Context, script string) ([]byte, error) {
		capturedSearchScript = script
		return []byte(egoResultMarker + `{"ok":true,"videos":[` +
			`{"bvid":"BV1example0001","oid":"111","title":"Agent Demo","author":"alice","author_mid":"12","comment_count":8,"play_count":1000},` +
			`{"bvid":"BV1example0001","oid":"111","title":"Duplicate","author":"alice"},` +
			`{"bvid":"not-a-bvid","oid":"222","title":"Invalid"},` +
			`{"bvid":"BV1example0002","oid":"333","title":"Second Video","author":"bob","comment_count":3}` +
			`]}`), nil
	}
	videos, err := drv.SearchVideos(context.Background(), SearchQuery{Keyword: "AI Agent", Order: "pubdate", Limit: 5})
	if err != nil {
		t.Fatalf("SearchVideos error: %v", err)
	}
	if len(videos) != 2 || videos[0].BVID != "BV1example0001" || videos[0].URL != "https://www.bilibili.com/video/BV1example0001" || videos[1].BVID != "BV1example0002" {
		t.Fatalf("unexpected search results: %+v", videos)
	}
	if !strings.Contains(capturedSearchScript, "search/type") || !strings.Contains(capturedSearchScript, "domcontentloaded") {
		t.Fatalf("unexpected search script content")
	}

	// Rate limit (-352) maps to rate_limited.
	drv.runScript = func(context.Context, string) ([]byte, error) {
		return []byte(egoResultMarker + `{"ok":false,"code":-352,"error_code":"bilibili_api_error","detail":"risk control"}`), nil
	}
	if _, err := drv.SearchVideos(context.Background(), SearchQuery{Keyword: "AI Agent"}); err == nil || !strings.Contains(err.Error(), "rate_limited") {
		t.Fatalf("expected rate_limited error, got %v", err)
	}

	// FetchComments preserves video_title and handles sub_reply_limit script generation.
	var capturedFetchScript string
	drv.runScript = func(_ context.Context, script string) ([]byte, error) {
		capturedFetchScript = script
		return []byte(egoResultMarker + `{"ok":true,"oid":"88001","video_title":"Demo Title","viewer_mid":"5001","comments":[{"rpid":"101","sender_id":"2001","sender_name":"alice","content":"hello","timestamp_ms":1700000001000}]}`), nil
	}
	comments, _, err := drv.FetchComments(context.Background(), TargetConfig{
		ContainerID:   "bilibili:BV1example0001",
		BVID:          "BV1example0001",
		CommentType:   1,
		URL:           "https://www.bilibili.com/video/BV1example0001",
		PollLimit:     20,
		SortMode:      "hot",
		SubReplyLimit: 5,
	})
	if err != nil || len(comments) != 1 || comments[0].VideoTitle != "Demo Title" {
		t.Fatalf("unexpected FetchComments result: %+v err=%v", comments, err)
	}
	if !strings.Contains(capturedFetchScript, "/x/v2/reply/reply") || !strings.Contains(capturedFetchScript, "/x/web-interface/view") {
		t.Fatalf("fetch script missing sub-reply or view fallback endpoints")
	}
}

func TestConnectorSearchDiscoverySkipRelayAndRelayBlocked(t *testing.T) {
	now := time.Unix(1700001000, 0).UTC()
	var blockMode atomic.Int32 // 0=allow, 1=blocked_unmonitored, 2=blocked_keyword
	var pushed []connector.InboundPayload
	var mu sync.Mutex

	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload connector.InboundPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		pushed = append(pushed, payload)
		mu.Unlock()
		resp := connector.RelayPushResponse{Status: "ok", ID: payload.ID, Allowed: true, Action: "forward"}
		switch blockMode.Load() {
		case 1:
			resp.Allowed = false
			resp.Action = "blocked_unmonitored"
		case 2:
			resp.Allowed = false
			resp.Action = "blocked_keyword"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer relay.Close()

	drv := &fakeDriver{
		ready:  true,
		viewer: "5001",
		searchRes: []VideoCandidate{
			{BVID: "BV1zero000001", OID: "100", Title: "Zero Comments", CommentCount: 0, URL: "https://www.bilibili.com/video/BV1zero000001"},
			{BVID: "BV1found00001", OID: "200", Title: "Found Video", CommentCount: 5, URL: "https://www.bilibili.com/video/BV1found00001"},
		},
		commentsBy: map[string][]Comment{
			"bilibili:BV1found00001": {
				{
					RPID:        "c-found-1",
					OID:         "200",
					BVID:        "BV1found00001",
					VideoTitle:  "Found Video",
					CommentType: 1,
					SenderID:    "3001",
					SenderName:  "viewer",
					Content:     "real question",
					TimestampMS: now.Add(-5 * time.Second).UnixMilli(),
				},
			},
		},
	}

	cfg := Config{
		ListenAddr:       "127.0.0.1:0",
		RelayURL:         relay.URL,
		StateDir:         t.TempDir(),
		DryRun:           connector.BoolPtr(false),
		SearchRefreshSec: 300,
		SearchTargets: []SearchTargetConfig{
			{Keyword: "AI Agent", Order: "totalrank", MaxVideos: 1, MinComments: 1, Enabled: true},
		},
	}
	conn, err := NewConnector(cfg, drv)
	if err != nil {
		t.Fatal(err)
	}
	conn.now = func() time.Time { return now }

	// 1. SkipRelay inspects without pushing to relay or seeding cursor.
	repSkip, err := conn.PollOnceWithOptions(context.Background(), PollOptions{SkipRelay: true})
	if err != nil {
		t.Fatalf("PollOnceWithOptions(SkipRelay) failed: %v", err)
	}
	if repSkip.VideosDiscovered != 1 || repSkip.TargetsPolled != 1 || repSkip.CommentsFetched != 1 || repSkip.Forwarded != 0 {
		t.Fatalf("unexpected SkipRelay report: %+v", repSkip)
	}
	mu.Lock()
	if len(pushed) != 0 {
		mu.Unlock()
		t.Fatalf("SkipRelay should not push to relay, got %d", len(pushed))
	}
	mu.Unlock()
	if conn.cursors.HasSeen("bilibili:BV1found00001", "c-found-1") {
		t.Fatal("SkipRelay should not mutate cursor")
	}

	// 2. blocked_unmonitored leaves item un-seen so fixing relay config allows retry.
	blockMode.Store(1)
	repBlocked, err := conn.PollOnce(context.Background())
	if err != nil || repBlocked.RelayBlocked != 1 || repBlocked.Forwarded != 0 {
		t.Fatalf("unexpected blocked_unmonitored report: %+v err=%v", repBlocked, err)
	}
	if conn.cursors.HasSeen("bilibili:BV1found00001", "c-found-1") {
		t.Fatal("blocked_unmonitored should not mark cursor seen")
	}

	// 3. Allowed push succeeds and marks cursor seen.
	blockMode.Store(0)
	repAllow, err := conn.PollOnce(context.Background())
	if err != nil || repAllow.Forwarded != 1 || repAllow.RelayBlocked != 0 {
		t.Fatalf("expected successful forward after unblocking, got %+v err=%v", repAllow, err)
	}
	if !conn.cursors.HasSeen("bilibili:BV1found00001", "c-found-1") {
		t.Fatal("expected cursor seen after forward")
	}

	// 4. Health Status() includes dynamically discovered video, and Execute() can reply to it.
	snap := conn.Status()
	if len(snap.Targets) != 1 || snap.Targets[0].ContainerID != "bilibili:BV1found00001" {
		t.Fatalf("unexpected status targets: %+v", snap.Targets)
	}
	execResp, err := conn.Execute(context.Background(), connector.ActionRequest{
		ActionID:     "act-search-1",
		Text:         "reply to discovered video",
		ReplyContext: json.RawMessage(`{"oid":"200","bvid":"BV1found00001","comment_type":1}`),
		Target: connector.ActionTarget{
			Platform:    "bilibili",
			ContainerID: "bilibili:BV1found00001",
			ContentID:   "c-found-1",
		},
	})
	if err != nil || execResp.Status != "confirmed" {
		t.Fatalf("Execute on discovered video failed: %+v err=%v", execResp, err)
	}
}
