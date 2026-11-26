package dispatcher

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"jinx/internal/contracts"
)

func biliTestConfig() Config {
	var cfg Config
	cfg.Server.ListenAddr = "127.0.0.1:8088"
	cfg.Relay.StreamURL = "http://127.0.0.1:8080/api/v1/stream"
	cfg.ActionEndpoints = map[string]ActionEndpointConfig{
		"bilibili": {URL: "http://127.0.0.1:18092", TimeoutSec: 15},
	}
	cfg.Hermes.BinPath = "hermes"
	cfg.Hermes.Profile = "demo-profile"
	cfg.Hermes.TimeoutSec = 10
	cfg.Rules = Rules{
		BatchSize:       1,
		IdleTimeoutSec:  1,
		MaxBatchWaitSec: 3,
		BotNames:        []string{"DemoBot"},
		SelfSenderIDs:   []string{"50001"},
		GroupWhitelist:  []string{"bilibili:BV1example0001"},
		GroupProfiles:   map[string]string{"bilibili:BV1example0001": "bili-bg"},
		GroupRules: map[string]GroupRule{
			"bilibili:BV1example0001": {
				Enabled:       true,
				TargetGroupID: "bilibili:BV1example0001",
				Target:        "Demo Video",
				HermesProfile: "demo-bili",
			},
		},
	}
	return cfg
}

func TestActionEndpointsConfigValidationAndHotReloadGuard(t *testing.T) {
	// Allows empty Bridge.URL when only non-WeChat routes exist, and allows both bilibili + web loopback endpoints.
	cfg := biliTestConfig()
	cfg.ActionEndpoints["web"] = ActionEndpointConfig{URL: "http://localhost:18093"}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeConfig(raw)
	if err != nil {
		t.Fatalf("expected valid non-WeChat config without Bridge.URL, got %v", err)
	}
	if decoded.ActionEndpoints["web"].TimeoutSec != 30 {
		t.Fatalf("expected default endpoint timeout 30, got %d", decoded.ActionEndpoints["web"].TimeoutSec)
	}

	// Rejects missing Bridge.URL when a WeChat route is present alongside ActionEndpoints.
	mixed := biliTestConfig()
	mixed.Rules.GroupWhitelist = append(mixed.Rules.GroupWhitelist, groupA)
	mixed.Rules.GroupRules[groupA] = GroupRule{Enabled: true, TargetGroupID: groupA, Target: "target-a", HermesProfile: "demo-wx"}
	mixedRaw, _ := json.Marshal(mixed)
	if _, err := decodeConfig(mixedRaw); err == nil {
		t.Fatal("expected error when WeChat route exists without Bridge.URL")
	}

	// Rejects non-loopback endpoint URLs, unknown platform keys, and invalid timeouts.
	invalidCases := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "non_loopback_endpoint",
			mutate: func(c *Config) {
				c.ActionEndpoints["bilibili"] = ActionEndpointConfig{URL: "https://example.com/action", TimeoutSec: 10}
			},
		},
		{
			name: "unsupported_platform_key_wechat",
			mutate: func(c *Config) {
				c.ActionEndpoints["wechat"] = ActionEndpointConfig{URL: "http://127.0.0.1:18094", TimeoutSec: 10}
			},
		},
		{
			name: "unsupported_platform_key_discord",
			mutate: func(c *Config) {
				c.ActionEndpoints["discord"] = ActionEndpointConfig{URL: "http://127.0.0.1:18094", TimeoutSec: 10}
			},
		},
		{
			name: "negative_timeout",
			mutate: func(c *Config) {
				c.ActionEndpoints["bilibili"] = ActionEndpointConfig{URL: "http://127.0.0.1:18092", TimeoutSec: -1}
			},
		},
		{
			name: "excessive_timeout",
			mutate: func(c *Config) {
				c.ActionEndpoints["bilibili"] = ActionEndpointConfig{URL: "http://127.0.0.1:18092", TimeoutSec: 121}
			},
		},
	}
	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			bad := biliTestConfig()
			tc.mutate(&bad)
			data, _ := json.Marshal(bad)
			if _, err := decodeConfig(data); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}

	// handleConfig blocks hot-reload changes to action_endpoints.
	d := newTestDispatcher(t, biliTestConfig())
	before := d.configSnapshot()
	etag := policyID(before)
	updated := before
	updated.ActionEndpoints = map[string]ActionEndpointConfig{
		"bilibili": {URL: "http://127.0.0.1:19092", TimeoutSec: 15},
	}
	body, _ := json.Marshal(updated)
	req := httptest.NewRequest(http.MethodPut, "http://127.0.0.1/api/config", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("If-Match", etag)
	rec := httptest.NewRecorder()
	d.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest || !bytes.Contains(rec.Body.Bytes(), []byte("require restart")) {
		t.Fatalf("expected 400 require restart when changing action_endpoints, got %d %s", rec.Code, rec.Body.String())
	}
}

func TestActionTargetForBatchAndPlatformActionRequest(t *testing.T) {
	// WeChat batch preserves ActionReplyText without thread IDs.
	wxTarget, wxKind, wxCtx, err := ActionTargetForBatch(batch{
		GroupID:  groupA,
		Rule:     GroupRule{TargetGroupID: groupA, Target: "target-a"},
		Messages: []RelayMessage{{ID: "m1", GroupID: groupA, Content: "hi"}},
	})
	if err != nil || wxKind != contracts.ActionReplyText || wxTarget.Platform != contracts.PlatformWeChat ||
		wxTarget.ContentID != "" || len(wxCtx) != 0 {
		t.Fatalf("unexpected WeChat batch action target: target=%+v kind=%s ctx=%s err=%v", wxTarget, wxKind, wxCtx, err)
	}

	// Bilibili batch preserves AccountID, ContentID, ParentID, RootID, and ReplyContext.
	replyCtx := json.RawMessage(`{"oid":"123","comment_type":1}`)
	biliMsg := RelayMessage{
		ID:           "rpid-200",
		Source:       "bilibili:ego",
		Platform:     "bilibili",
		AccountID:    "bilibili-ego",
		GroupID:      "bilibili:BV1example0001",
		ParentID:     "rpid-100",
		RootID:       "rpid-100",
		ReplyContext: replyCtx,
		Type:         1,
		Action:       "forward",
		Content:      "question",
		Timestamp:    time.Now().UnixMilli(),
	}
	target, kind, rawCtx, err := ActionTargetForBatch(batch{
		GroupID:  "bilibili:BV1example0001",
		Rule:     GroupRule{TargetGroupID: "bilibili:BV1example0001", Target: "Demo Video"},
		Messages: []RelayMessage{biliMsg},
	})
	if err != nil {
		t.Fatalf("ActionTargetForBatch bilibili failed: %v", err)
	}
	if kind != contracts.ActionThreadReply || target.Platform != contracts.PlatformBilibili ||
		target.AccountID != "bilibili-ego" || target.ContainerID != "bilibili:BV1example0001" ||
		target.ContentID != "rpid-200" || target.ParentID != "rpid-100" || target.RootID != "rpid-100" ||
		string(rawCtx) != string(replyCtx) {
		t.Fatalf("unexpected Bilibili action target: target=%+v kind=%s ctx=%s", target, kind, rawCtx)
	}

	// PlatformActionRequest + ValidatePlatformActionRequest round-trip against persisted trace run.
	d := newTestDispatcher(t, biliTestConfig())
	cfg := d.configSnapshot()
	rule, _ := cfg.route("bilibili:BV1example0001")
	now := time.Now().UTC()
	biliMsg.ReceivedAt = &now
	work := batch{
		GroupID:  "bilibili:BV1example0001",
		Messages: []RelayMessage{biliMsg},
		Rule:     rule,
		Config:   cfg,
		Policy:   policyID(cfg),
		Reason:   "batch_size",
	}
	if err := d.ensureTrace(&work); err != nil {
		t.Fatal(err)
	}
	if err := d.tracePhase(work, contracts.PhaseChecking, "test_checking"); err != nil {
		t.Fatal(err)
	}
	run, err := d.trace.GetRun(work.TraceID)
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Minute).UTC()
	actionReq, err := PlatformActionRequest(run, target, kind, "你好，可以的。", "act-bili-1", expires)
	if err != nil {
		t.Fatalf("PlatformActionRequest failed: %v", err)
	}
	if err := ValidatePlatformActionRequest(run, target, kind, actionReq, expires); err != nil {
		t.Fatalf("ValidatePlatformActionRequest failed: %v", err)
	}
	tampered := target
	tampered.ContentID = "rpid-999"
	if err := ValidatePlatformActionRequest(run, tampered, kind, actionReq, expires); err == nil {
		t.Fatal("expected ValidatePlatformActionRequest to reject mutated target ContentID")
	}
}

func prepareBilibiliSubmittingRun(t *testing.T, d *Dispatcher, id string) (batch, contracts.ActionRequest, *awaitingConfirmation) {
	t.Helper()
	cfg := d.configSnapshot()
	rule, _ := cfg.route("bilibili:BV1example0001")
	now := time.Now().UTC()
	msg := RelayMessage{
		ID:         id,
		Source:     "bilibili:ego",
		Platform:   "bilibili",
		AccountID:  "bilibili-ego",
		GroupID:    "bilibili:BV1example0001",
		SenderID:   "2001",
		SenderName: "alice",
		Type:       1,
		Action:     "forward",
		Content:    "请问支持吗？",
		Timestamp:  now.UnixMilli(),
		ReceivedAt: &now,
	}
	work := batch{
		GroupID:  "bilibili:BV1example0001",
		Messages: []RelayMessage{msg},
		Rule:     rule,
		Config:   cfg,
		Policy:   policyID(cfg),
		Reason:   "batch_size",
	}
	if err := d.ensureTrace(&work); err != nil {
		t.Fatal(err)
	}
	if err := d.tracePhase(work, contracts.PhaseChecking, "test_checking"); err != nil {
		t.Fatal(err)
	}
	run, err := d.trace.GetRun(work.TraceID)
	if err != nil {
		t.Fatal(err)
	}
	target, kind, _, err := ActionTargetForBatch(work)
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Minute).UTC()
	actionReq, err := PlatformActionRequest(run, target, kind, "支持导出。", messageDigest(work.TraceID+":action:bilibili"), expires)
	if err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	confirmation := d.newConfirmationLocked(work, actionReq.Text, actionReq.ActionID)
	if err := d.persistConfirmationsLocked(); err != nil {
		t.Fatal(err)
	}
	if err := d.updateTrace(work, "submission_started", func(r *contracts.Run) {
		r.Phase, r.Delivery, r.ReasonCode = contracts.PhaseSubmitting, contracts.DeliverySubmitting, ""
		r.DeliveryEvidence.SubmissionStartedAt = &confirmation.SubmissionStartedAt
		r.DeliveryEvidence.BridgeRequestID = actionReq.ActionID
		r.Action = &contracts.ActionRef{
			ActionID:       actionReq.ActionID,
			Kind:           actionReq.Kind,
			Platform:       actionReq.Target.Source.Platform,
			IdempotencyKey: actionReq.IdempotencyKey,
			AdmissionID:    "admission-" + actionReq.ActionID,
			AdmittedAt:     &confirmation.SubmissionStartedAt,
		}
	}); err != nil {
		t.Fatal(err)
	}
	return work, actionReq, confirmation
}

func TestConfirmDirectLockedAndFailSubmissionLockedValidation(t *testing.T) {
	t.Run("confirmDirectLocked", func(t *testing.T) {
		d := newTestDispatcher(t, biliTestConfig())
		work, actionReq, confirmation := prepareBilibiliSubmittingRun(t, d, "rpid-input-1")

		d.mu.Lock()
		err := d.confirmDirectLocked(confirmation, "rpid-9001", time.Now().UTC(), actionReq.Text, "bilibili-ego")
		_, seen := d.state.Seen["bilibili:BV1example0001/rpid-9001"]
		d.mu.Unlock()
		if err != nil {
			t.Fatalf("confirmDirectLocked failed: %v", err)
		}
		if !seen {
			t.Fatal("expected confirmDirectLocked to record external ID in state.Seen")
		}
		run, err := d.trace.GetRun(work.TraceID)
		if err != nil {
			t.Fatal(err)
		}
		if err := contracts.ValidateRun(run); err != nil {
			t.Fatalf("ValidateRun failed: %v", err)
		}
		if run.Phase != contracts.PhaseTerminal || run.Outcome != contracts.OutcomeReplied || run.Delivery != contracts.DeliveryConfirmed {
			t.Fatalf("unexpected terminal state: %+v", run)
		}
		if run.DeliveryEvidence.SubmittedAtSource != "bridge_ack" || run.DeliveryEvidence.EchoMessageID != "rpid-9001" {
			t.Fatalf("unexpected delivery evidence: %+v", run.DeliveryEvidence)
		}
		receiptView, err := ProcessingActionReceipt(run)
		if err != nil || receiptView.Receipt == nil || receiptView.Status != contracts.ActionConfirmed || receiptView.Receipt.ExternalID != "rpid-9001" {
			t.Fatalf("unexpected action receipt: %+v err=%v", receiptView, err)
		}
		if err := contracts.ValidateActionReceipt(*receiptView.Receipt); err != nil {
			t.Fatalf("ValidateActionReceipt failed: %v", err)
		}
	})

	t.Run("failSubmissionLocked", func(t *testing.T) {
		d := newTestDispatcher(t, biliTestConfig())
		work, _, confirmation := prepareBilibiliSubmittingRun(t, d, "rpid-input-2")

		d.mu.Lock()
		err := d.failSubmissionLocked(confirmation, "comment_deleted")
		d.mu.Unlock()
		if err != nil {
			t.Fatalf("failSubmissionLocked failed: %v", err)
		}
		run, err := d.trace.GetRun(work.TraceID)
		if err != nil {
			t.Fatal(err)
		}
		if err := contracts.ValidateRun(run); err != nil {
			t.Fatalf("ValidateRun failed: %v", err)
		}
		if run.Phase != contracts.PhaseTerminal || run.Outcome != contracts.OutcomeFailed || run.Delivery != contracts.DeliveryFailed {
			t.Fatalf("unexpected failed state: %+v", run)
		}
		if run.DeliveryEvidence.FailureEvidence != "comment_deleted" || run.DeliveryEvidence.SubmittedAt != nil || run.DeliveryEvidence.ConfirmedAt != nil {
			t.Fatalf("unexpected failure evidence: %+v", run.DeliveryEvidence)
		}
		receiptView, err := ProcessingActionReceipt(run)
		if err != nil || receiptView.Receipt == nil || receiptView.Status != contracts.ActionFailed {
			t.Fatalf("unexpected failed action receipt: %+v err=%v", receiptView, err)
		}
		if err := contracts.ValidateActionReceipt(*receiptView.Receipt); err != nil {
			t.Fatalf("ValidateActionReceipt failed: %v", err)
		}
	})
}
