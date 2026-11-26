package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/execution"
)

func TestValidateForwardRules(t *testing.T) {
	groups := map[string]GroupRule{
		groupA: {Enabled: true, Target: "target-a"},
	}
	valid := []ForwardRule{
		{
			ID:                "v2ex-to-wx",
			Enabled:           true,
			SourceContainerID: "v2ex:t-100001",
			TargetContainerID: groupA,
			DryRun:            true,
			Template:          "[V2EX] {{sender}}: {{content}}",
		},
	}
	got, err := validateForwardRules(valid, groups)
	if err != nil {
		t.Fatalf("expected valid forward rules, got %v", err)
	}
	if len(got) != 1 || got[0].ID != "v2ex-to-wx" {
		t.Fatalf("unexpected normalized rules: %+v", got)
	}

	if gotNil, err := validateForwardRules([]ForwardRule{}, groups); err != nil || gotNil != nil {
		t.Fatalf("expected nil normalized slice for empty rules, got %+v err=%v", gotNil, err)
	}

	invalidCases := []struct {
		name  string
		rules []ForwardRule
	}{
		{
			name:  "invalid_id",
			rules: []ForwardRule{{ID: "bad id!", SourceContainerID: "v2ex:t-100001", TargetContainerID: groupA}},
		},
		{
			name: "duplicate_id",
			rules: []ForwardRule{
				{ID: "dup", SourceContainerID: "v2ex:t-100001", TargetContainerID: groupA},
				{ID: "dup", SourceContainerID: "bilibili:BV1example0001", TargetContainerID: groupA},
			},
		},
		{
			name:  "invalid_source",
			rules: []ForwardRule{{ID: "r1", SourceContainerID: "http://bad", TargetContainerID: groupA}},
		},
		{
			name:  "invalid_target",
			rules: []ForwardRule{{ID: "r1", SourceContainerID: "v2ex:t-100001", TargetContainerID: "bad"}},
		},
		{
			name:  "self_loop",
			rules: []ForwardRule{{ID: "r1", SourceContainerID: groupA, TargetContainerID: groupA}},
		},
		{
			name:  "unknown_target_route",
			rules: []ForwardRule{{ID: "r1", SourceContainerID: "v2ex:t-100001", TargetContainerID: groupB}},
		},
		{
			name:  "nul_in_template",
			rules: []ForwardRule{{ID: "r1", SourceContainerID: "v2ex:t-100001", TargetContainerID: groupA, Template: "hi\x00there"}},
		},
		{
			name:  "template_too_long",
			rules: []ForwardRule{{ID: "r1", SourceContainerID: "v2ex:t-100001", TargetContainerID: groupA, Template: strings.Repeat("a", maxForwardTemplateRunes+1)}},
		},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := validateForwardRules(tc.rules, groups); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}

	tooMany := make([]ForwardRule, maxForwardRules+1)
	for i := range tooMany {
		tooMany[i] = ForwardRule{
			ID:                fmt.Sprintf("rule-%d", i),
			SourceContainerID: "v2ex:t-100001",
			TargetContainerID: groupA,
		}
	}
	if _, err := validateForwardRules(tooMany, groups); err == nil {
		t.Fatal("expected error for > maxForwardRules")
	}
}

func TestRenderForwardTemplate(t *testing.T) {
	msg := RelayMessage{
		ID:         "18330001",
		Platform:   "",
		Source:     "v2ex:ego",
		GroupID:    "v2ex:t-100001",
		GroupName:  "",
		SenderID:   "alice",
		SenderName: "",
		Content:    "hello forum",
	}
	got := renderForwardTemplate("", msg)
	want := "[v2ex] v2ex:t-100001 · alice: hello forum"
	if got != want {
		t.Fatalf("renderForwardTemplate default = %q, want %q", got, want)
	}

	custom := renderForwardTemplate("{{platform}}/{{id}} {{sender_id}} -> {{content}}", msg)
	if custom != "v2ex/18330001 alice -> hello forum" {
		t.Fatalf("renderForwardTemplate custom = %q", custom)
	}

	longMsg := msg
	longMsg.Content = strings.Repeat("字", maxDirectDeliverRunes+200)
	truncated := renderForwardTemplate("{{content}}", longMsg)
	if len([]rune(truncated)) != maxDirectDeliverRunes || !strings.HasSuffix(truncated, "…") {
		t.Fatalf("expected rune truncation to %d with ellipsis, got runes=%d", maxDirectDeliverRunes, len([]rune(truncated)))
	}
}

func TestPassthroughForwardBypassesFilterAndAIRunner(t *testing.T) {
	var d *Dispatcher
	var bridgeMu sync.Mutex
	var delivered []string
	var bridgePosts atomic.Int32

	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		bridgePosts.Add(1)
		var body struct {
			GroupID string `json:"group_id"`
			Text    string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode bridge body: %v", err)
		}
		bridgeMu.Lock()
		delivered = append(delivered, body.GroupID+"|"+body.Text)
		bridgeMu.Unlock()

		// Simulate correlated echo arriving on the SSE stream.
		var targetRun contracts.Run
		snap, err := d.trace.Snapshot()
		if err == nil {
			for i := len(snap.Runs) - 1; i >= 0; i-- {
				if snap.Runs[i].Phase == contracts.PhaseSubmitting {
					targetRun = snap.Runs[i]
					break
				}
			}
		}
		if targetRun.DeliveryEvidence.SubmissionStartedAt != nil {
			echo := testMessage(groupA, fmt.Sprintf("wx-echo-%d", bridgePosts.Load()))
			echo.IsEcho = true
			echo.Content = body.Text
			echo.Timestamp = targetRun.DeliveryEvidence.SubmissionStartedAt.UnixMilli() + 1
			if err := d.handleIncomingMessage(echo, fmt.Sprintf("echo:%d", bridgePosts.Load())); err != nil {
				t.Errorf("echo failed: %v", err)
			}
		}
		_, _ = fmt.Fprint(w, "OK")
	})

	cfg := testConfig()
	cfg.Bridge.URL, cfg.Bridge.TimeoutSec = bridge.URL, 5
	cfg.Rules.LayaFilter = true
	cfg.Rules.LayaURL = "http://127.0.0.1:19999"
	cfg.Rules.KeywordBlocklist = []string{"blockedword"}
	cfg.Rules.ForwardRules = []ForwardRule{
		{
			ID:                "v2ex-to-wx-live",
			Enabled:           true,
			SourceContainerID: "v2ex:t-100001",
			TargetContainerID: groupA,
			DryRun:            false,
			IncludeEcho:       false,
			Template:          "[V2EX] {{sender}}: {{content}}",
		},
		{
			ID:                "v2ex-to-wx-dry",
			Enabled:           true,
			SourceContainerID: "v2ex:t-100001",
			TargetContainerID: groupB,
			DryRun:            true,
			IncludeEcho:       false,
		},
		{
			ID:                "v2ex-echo-only",
			Enabled:           true,
			SourceContainerID: "v2ex:t-100002",
			TargetContainerID: groupA,
			DryRun:            false,
			IncludeEcho:       true,
			Template:          "[ECHO] {{content}}",
		},
	}

	runner := &fakeRunnerWithGate{
		run: func(context.Context, BackendRequest) (Result, error) {
			t.Fatal("AI runner must NOT be called for passthrough forward rules")
			return Result{}, nil
		},
		gate: func(context.Context, string, string, func(context.Context) error) error {
			t.Fatal("DeliveryGate must NOT be called for passthrough forward rules")
			return nil
		},
		stop: func(context.Context, string) (execution.CancelResult, bool, error) {
			return execution.CancelResult{}, false, nil
		},
	}
	d = newRunnerDispatcher(t, cfg, runner)

	// 1. Non-whitelisted V2EX comment containing blockedword + bare URL should still forward without filtering or AI.
	msg := RelayMessage{
		ID:         "r-101",
		Platform:   "v2ex",
		Source:     "v2ex:ego",
		GroupID:    "v2ex:t-100001",
		GroupName:  "V2EX Topic",
		SenderID:   "alice",
		SenderName: "alice",
		Type:       99,
		Action:     "forward",
		Content:    "blockedword https://example.com/post",
		Timestamp:  time.Now().UnixMilli(),
	}
	if err := d.handleIncomingMessage(msg, "evt:1"); err != nil {
		t.Fatalf("handleIncomingMessage failed: %v", err)
	}

	awaitCondition(t, func() bool {
		snap, err := d.trace.Snapshot()
		if err != nil || len(snap.Runs) < 2 {
			return false
		}
		terminal := 0
		for _, r := range snap.Runs {
			if r.Phase == contracts.PhaseTerminal {
				terminal++
			}
		}
		return terminal >= 2
	})

	if bridgePosts.Load() != 1 || runner.calls.Load() != 0 {
		t.Fatalf("expected 1 live bridge post and 0 runner calls, got posts=%d runner=%d", bridgePosts.Load(), runner.calls.Load())
	}
	bridgeMu.Lock()
	gotDelivered := append([]string(nil), delivered...)
	bridgeMu.Unlock()
	if len(gotDelivered) != 1 || gotDelivered[0] != groupA+"|[V2EX] alice: blockedword https://example.com/post" {
		t.Fatalf("unexpected delivered payloads: %+v", gotDelivered)
	}

	// Verify both runs (live confirmed + dry-run suppressed).
	snap, err := d.trace.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var liveRun, dryRun *contracts.Run
	for i := range snap.Runs {
		r := &snap.Runs[i]
		if r.TargetGroupID == groupA {
			liveRun = r
		} else if r.TargetGroupID == groupB {
			dryRun = r
		}
	}
	if liveRun == nil || liveRun.Outcome != contracts.OutcomeReplied || liveRun.Delivery != contracts.DeliveryConfirmed || liveRun.Trigger != "direct:forward" {
		t.Fatalf("unexpected live forward run: %+v", liveRun)
	}
	if dryRun == nil || dryRun.Outcome != contracts.OutcomeSuppressed || dryRun.Delivery != contracts.DeliveryBlocked || dryRun.ReasonCode != "dry_run" {
		t.Fatalf("unexpected dry_run forward run: %+v", dryRun)
	}

	// 2. Echo from v2ex:t-100001 must be skipped (IncludeEcho=false), while echo from v2ex:t-100002 forwards (IncludeEcho=true).
	echo1 := msg
	echo1.ID = "r-102"
	echo1.IsEcho = true
	echo1.Content = "self echo skipped"
	if err := d.handleIncomingMessage(echo1, "evt:2"); err != nil {
		t.Fatalf("echo1 failed: %v", err)
	}
	echo2 := msg
	echo2.ID = "r-201"
	echo2.GroupID = "v2ex:t-100002"
	echo2.IsEcho = true
	echo2.Content = "self echo forwarded"
	if err := d.handleIncomingMessage(echo2, "evt:3"); err != nil {
		t.Fatalf("echo2 failed: %v", err)
	}
	awaitCondition(t, func() bool {
		return bridgePosts.Load() == 2
	})
	bridgeMu.Lock()
	gotDelivered = append([]string(nil), delivered...)
	bridgeMu.Unlock()
	if len(gotDelivered) != 2 || gotDelivered[1] != groupA+"|[ECHO] self echo forwarded" {
		t.Fatalf("unexpected delivered after echo checks: %+v", gotDelivered)
	}

	// 3. Hot-reload via POST /api/config can add and clear forward_rules even when initially omitted.
	patchAdd := []byte(`{"forward_rules":[{"id":"hot-fwd","enabled":true,"source_container_id":"web:demo-1","target_container_id":"100@chatroom"}]}`)
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8088/api/config", bytes.NewReader(patchAdd))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", policyID(d.configSnapshot()))
	rec := httptest.NewRecorder()
	d.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/config add forward_rules status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := d.configSnapshot().Rules.ForwardRules; len(got) != 1 || got[0].ID != "hot-fwd" {
		t.Fatalf("expected hot-reloaded forward_rules, got %+v", got)
	}

	patchClear := []byte(`{"forward_rules":[]}`)
	reqClear := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8088/api/config", bytes.NewReader(patchClear))
	reqClear.Header.Set("Content-Type", "application/json")
	reqClear.Header.Set("If-Match", policyID(d.configSnapshot()))
	recClear := httptest.NewRecorder()
	d.Handler().ServeHTTP(recClear, reqClear)
	if recClear.Code != http.StatusOK {
		t.Fatalf("POST /api/config clear forward_rules status=%d body=%s", recClear.Code, recClear.Body.String())
	}
	if got := d.configSnapshot().Rules.ForwardRules; len(got) != 0 {
		t.Fatalf("expected cleared forward_rules, got %+v", got)
	}
}
