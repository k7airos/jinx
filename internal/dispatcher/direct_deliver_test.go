package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/execution"
)

func TestDirectDeliverWeChatSuccessAndIdempotentReplay(t *testing.T) {
	var d *Dispatcher
	var bridgePosts atomic.Int32
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		bridgePosts.Add(1)
		var body struct {
			GroupID        string `json:"group_id"`
			Target         string `json:"target"`
			Text           string `json:"text"`
			RequestID      string `json:"request_id"`
			IdempotencyKey string `json:"idempotency_key"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode bridge body: %v", err)
		}
		if body.GroupID != groupA || body.Text != "整理后的推文快讯" {
			t.Errorf("unexpected bridge payload: %+v", body)
		}
		run := onlyTraceRun(t, d)
		boundary := run.DeliveryEvidence.SubmissionStartedAt
		echo := testMessage(groupA, "direct-echo-1")
		echo.IsEcho, echo.Content, echo.Timestamp = true, body.Text, boundary.UnixMilli()+1
		if err := d.handleIncomingMessage(echo, "echo:direct"); err != nil {
			t.Errorf("handleIncomingMessage echo failed: %v", err)
		}
		_, _ = fmt.Fprint(w, "OK")
	})

	cfg := testConfig()
	cfg.Bridge.URL, cfg.Bridge.TimeoutSec = bridge.URL, 5
	runner := &fakeRunnerWithGate{
		run: func(context.Context, BackendRequest) (Result, error) {
			t.Fatal("agent Run must NOT be called during direct delivery")
			return Result{}, nil
		},
		gate: func(context.Context, string, string, func(context.Context) error) error {
			t.Fatal("execution DeliveryGate must NOT be called during direct delivery")
			return nil
		},
		stop: func(context.Context, string) (execution.CancelResult, bool, error) {
			return execution.CancelResult{}, false, nil
		},
	}
	d = newRunnerDispatcher(t, cfg, runner)

	payload := DirectDeliverRequest{
		TargetContainerID: groupA,
		Text:              "整理后的推文快讯",
		IdempotencyKey:    "tw-001",
		Source:            "twitter",
	}
	raw, _ := json.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8088/api/v1/direct-deliver", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	d.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/v1/direct-deliver status=%d body=%s", rec.Code, rec.Body.String())
	}

	var resp DirectDeliverResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Replayed || resp.DryRun || resp.Outcome != contracts.OutcomeReplied || resp.Delivery != contracts.DeliveryConfirmed || resp.Trigger != "direct:twitter" {
		t.Fatalf("unexpected direct deliver response: %+v", resp)
	}
	if bridgePosts.Load() != 1 || runner.calls.Load() != 0 || runner.gateCalls.Load() != 0 {
		t.Fatalf("unexpected invocation counts: bridge=%d run=%d gate=%d", bridgePosts.Load(), runner.calls.Load(), runner.gateCalls.Load())
	}

	persisted := onlyTraceRun(t, d)
	if persisted.PlannedBackendID != "direct_passthrough" || len(persisted.Attempts) != 0 || persisted.Trigger != "direct:twitter" {
		t.Fatalf("unexpected persisted run metadata: %+v", persisted)
	}
	if len(persisted.Inputs) != 1 || persisted.Inputs[0].Source.ContainerID != groupA {
		t.Fatalf("expected persisted inbound metadata without raw text, got inputs=%+v missing=%q", persisted.Inputs, persisted.InputsMissingReason)
	}

	// Identical retry replays idempotently without hitting bridge a second time.
	reqReplay := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8088/api/v1/direct-deliver", bytes.NewReader(raw))
	reqReplay.Header.Set("Content-Type", "application/json")
	recReplay := httptest.NewRecorder()
	d.Handler().ServeHTTP(recReplay, reqReplay)
	if recReplay.Code != http.StatusOK {
		t.Fatalf("replay status=%d body=%s", recReplay.Code, recReplay.Body.String())
	}
	var replayResp DirectDeliverResponse
	_ = json.Unmarshal(recReplay.Body.Bytes(), &replayResp)
	if !replayResp.Replayed || replayResp.RunID != resp.RunID || bridgePosts.Load() != 1 {
		t.Fatalf("expected idempotent replay without second bridge post: resp=%+v bridge=%d", replayResp, bridgePosts.Load())
	}

	// Reusing idempotency_key with different text returns 409 Conflict.
	conflictPayload := payload
	conflictPayload.Text = "恶意篡改后的内容"
	conflictRaw, _ := json.Marshal(conflictPayload)
	reqConflict := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8088/api/v1/direct-deliver", bytes.NewReader(conflictRaw))
	reqConflict.Header.Set("Content-Type", "application/json")
	recConflict := httptest.NewRecorder()
	d.Handler().ServeHTTP(recConflict, reqConflict)
	if recConflict.Code != http.StatusConflict || bridgePosts.Load() != 1 {
		t.Fatalf("expected 409 Conflict for mismatched replay payload, got %d %s", recConflict.Code, recConflict.Body.String())
	}
}

func TestDirectDeliverDryRunAndGuards(t *testing.T) {
	var bridgePosts atomic.Int32
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		bridgePosts.Add(1)
		_, _ = fmt.Fprint(w, "OK")
	})

	postDirect := func(d *Dispatcher, reqPayload DirectDeliverRequest) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(reqPayload)
		req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8088/api/v1/direct-deliver", bytes.NewReader(raw))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		d.Handler().ServeHTTP(rec, req)
		return rec
	}

	// 1. Caller dry_run=true suppresses bridge POST and records TotalDryRun.
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	d := newTestDispatcher(t, cfg)
	rec := postDirect(d, DirectDeliverRequest{
		TargetContainerID: groupA,
		Text:              "演练播报内容",
		IdempotencyKey:    "dry-1",
		Source:            "rss",
		DryRun:            true,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("dry-run status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp DirectDeliverResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if !resp.DryRun || resp.Outcome != contracts.OutcomeSuppressed || resp.Delivery != contracts.DeliveryBlocked || resp.ReasonCode != "dry_run" || bridgePosts.Load() != 0 {
		t.Fatalf("unexpected caller dry-run response: %+v bridgePosts=%d", resp, bridgePosts.Load())
	}
	d.mu.Lock()
	dryCount := d.state.Stats.TotalDryRun
	d.mu.Unlock()
	if dryCount != 1 {
		t.Fatalf("expected TotalDryRun=1, got %d", dryCount)
	}

	// 2. Caller dry_run=false cannot bypass group/bridge dry_run=true.
	rule := cfg.Rules.GroupRules[groupA]
	rule.DryRun = true
	cfg.Rules.GroupRules[groupA] = rule
	dGroupDry := newTestDispatcher(t, cfg)
	recGroupDry := postDirect(dGroupDry, DirectDeliverRequest{
		TargetContainerID: groupA,
		Text:              "尝试实盘",
		IdempotencyKey:    "dry-bypass-attempt",
		DryRun:            false,
	})
	var groupDryResp DirectDeliverResponse
	_ = json.Unmarshal(recGroupDry.Body.Bytes(), &groupDryResp)
	if recGroupDry.Code != http.StatusOK || !groupDryResp.DryRun || groupDryResp.ReasonCode != "dry_run" || bridgePosts.Load() != 0 {
		t.Fatalf("caller dry_run=false bypassed group dry_run: %+v", groupDryResp)
	}

	// 3. Unlisted container returns 403 Forbidden.
	recForbidden := postDirect(d, DirectDeliverRequest{
		TargetContainerID: "999999@chatroom",
		Text:              "未授权群",
		IdempotencyKey:    "forbid-1",
	})
	if recForbidden.Code != http.StatusForbidden || bridgePosts.Load() != 0 {
		t.Fatalf("expected 403 for unallowlisted container, got %d %s", recForbidden.Code, recForbidden.Body.String())
	}

	// 4. Globally paused dispatcher suppresses direct delivery at boundary.
	dPaused := newTestDispatcher(t, testConfig())
	dPaused.mu.Lock()
	dPaused.state.Control.Global = ScopeControl{Paused: true, Revision: "2", Boundary: dPaused.intakeSequence.Load() + 100, ChangedAt: time.Now()}
	dPaused.mu.Unlock()
	recPaused := postDirect(dPaused, DirectDeliverRequest{
		TargetContainerID: groupA,
		Text:              "暂停期间直发",
		IdempotencyKey:    "pause-1",
	})
	var pausedResp DirectDeliverResponse
	_ = json.Unmarshal(recPaused.Body.Bytes(), &pausedResp)
	if recPaused.Code != http.StatusOK || pausedResp.Outcome != contracts.OutcomeSuppressed || pausedResp.ReasonCode != "paused_boundary" || bridgePosts.Load() != 0 {
		t.Fatalf("expected paused_boundary suppression, got code=%d resp=%+v", recPaused.Code, pausedResp)
	}

	// 5. Validation guards: oversized text, bad idempotency_key, parent without content_id.
	for _, bad := range []DirectDeliverRequest{
		{TargetContainerID: groupA, Text: strings.Repeat("字", 4001), IdempotencyKey: "too-long"},
		{TargetContainerID: groupA, Text: "ok", IdempotencyKey: "../bad"},
		{TargetContainerID: groupA, Text: "ok", IdempotencyKey: "bad-parent", ParentID: "p1"},
		{TargetContainerID: groupA, Text: "ok", IdempotencyKey: "bad-source", Source: "bad source!"},
	} {
		if r := postDirect(d, bad); r.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for invalid input %+v, got %d %s", bad, r.Code, r.Body.String())
		}
	}
}

func TestDirectDeliverNonWeChatConnector(t *testing.T) {
	var actionCalls atomic.Int32
	connSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_, _ = fmt.Fprint(w, `{"status":"ready","protocol_version":1,"platform":"bilibili","ready":true,"dry_run":false,"capabilities":["reply_text","thread_reply"]}`)
		case "/v1/actions":
			actionCalls.Add(1)
			var req struct {
				ActionID string `json:"action_id"`
				Kind     string `json:"kind"`
				Text     string `json:"text"`
				Target   struct {
					ContainerID string `json:"container_id"`
					ContentID   string `json:"content_id"`
				} `json:"target"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode connector request: %v", err)
			}
			if req.Kind != "reply_text" || req.Target.ContainerID != "bilibili:BV1example0001" || req.Target.ContentID != "" || req.Text != "新版本发布动态" {
				t.Errorf("unexpected connector action payload: %+v", req)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"action_id":   req.ActionID,
				"status":      "confirmed",
				"external_id": "rpid-direct-1",
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer connSrv.Close()

	cfg := biliTestConfig()
	cfg.ActionEndpoints["bilibili"] = ActionEndpointConfig{URL: connSrv.URL, TimeoutSec: 5}
	d := newTestDispatcher(t, cfg)

	payload, _ := json.Marshal(DirectDeliverRequest{
		TargetContainerID: "bilibili:BV1example0001",
		Text:              "新版本发布动态",
		IdempotencyKey:    "bili-direct-1",
		Source:            "github",
	})
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8088/api/v1/direct-deliver", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	d.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("connector direct deliver status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp DirectDeliverResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Outcome != contracts.OutcomeReplied || resp.Delivery != contracts.DeliveryConfirmed || resp.DeliveryEvidence.EchoMessageID != "rpid-direct-1" || actionCalls.Load() != 1 {
		t.Fatalf("unexpected connector response: %+v actionCalls=%d", resp, actionCalls.Load())
	}
}
