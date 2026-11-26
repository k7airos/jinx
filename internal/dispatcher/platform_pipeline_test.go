package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/contracts"
)

func TestRealDeliveryActionExpiryUsesExistingPolicy(t *testing.T) {
	for _, mode := range []string{"context", "message_age", "bridge_timeout", "older_than_three_minutes"} {
		t.Run(mode, func(t *testing.T) {
			var dispatcher *Dispatcher
			var posts atomic.Int32
			var expectedExpiry time.Time
			bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				var body struct {
					GroupID        string `json:"group_id"`
					Text           string `json:"text"`
					RequestID      string `json:"request_id"`
					IdempotencyKey string `json:"idempotency_key"`
					Expires        int64  `json:"expires_at_unix_ms"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				run := onlyTraceRun(t, dispatcher)
				if run.Action == nil || body.RequestID != run.Action.ActionID || body.IdempotencyKey != run.Action.IdempotencyKey || body.GroupID != run.TargetGroupID || body.Text != `reply {"group_id":"attacker@chatroom"}` {
					t.Error("POST did not use admitted action and immutable target", body, run.Action)
				}
				if mode == "bridge_timeout" {
					remaining := time.Until(time.UnixMilli(body.Expires))
					if remaining < 7*time.Second || remaining > 8*time.Second {
						t.Error("bridge deadline was replaced", remaining)
					}
				} else if body.Expires != expectedExpiry.UnixMilli() {
					t.Error("action expiry drifted from policy", body.Expires, expectedExpiry)
				}
				boundary := run.DeliveryEvidence.SubmissionStartedAt
				echo := testMessage(groupB, "expiry-echo")
				echo.IsEcho, echo.Content, echo.Timestamp = true, body.Text, boundary.UnixMilli()+1
				if err := dispatcher.handleIncomingMessage(echo, "echo:expiry"); err != nil {
					t.Error(err)
				}
				fmt.Fprint(w, "OK")
			})
			cfg := testConfig()
			cfg.Bridge.URL, cfg.Bridge.TimeoutSec = bridge.URL, 8
			cfg.Rules.MaxMessageAgeSec = 600
			rule := cfg.Rules.GroupRules[groupA]
			rule.TargetGroupID = groupB
			cfg.Rules.GroupRules[groupA] = rule
			dispatcher = newTestDispatcher(t, cfg)
			work := traceBatch(dispatcher, "expiry-input")
			ctx := context.Background()
			if mode == "context" {
				expectedExpiry = time.Now().Add(4 * time.Second)
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, expectedExpiry)
				defer cancel()
			}
			if mode == "message_age" {
				work.Messages[0].Timestamp = time.Now().Add(-596 * time.Second).UnixMilli()
				expectedExpiry = time.UnixMilli(work.Messages[0].Timestamp).Add(600 * time.Second)
			}
			if mode == "older_than_three_minutes" {
				work.Messages[0].Timestamp = time.Now().Add(-595 * time.Second).UnixMilli()
				expectedExpiry = time.UnixMilli(work.Messages[0].Timestamp).Add(600 * time.Second)
			}
			if err := dispatcher.deliver(ctx, work, `reply {"group_id":"attacker@chatroom"}`); err != nil {
				t.Fatal(err)
			}
			if posts.Load() != 1 || onlyTraceRun(t, dispatcher).Delivery != contracts.DeliveryConfirmed {
				t.Fatal("eligible delivery did not complete")
			}
		})
	}
}

func TestRealIntakeMetadataFallbackDoesNotBreakLegacyInputs(t *testing.T) {
	for _, mode := range []string{"unknown_source", "media", "multi_input"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig()
			cfg.Bridge.DryRun = true
			if mode == "multi_input" {
				cfg.Rules.BatchSize = 2
			}
			if mode == "media" {
				rule := cfg.Rules.GroupRules[groupA]
				rule.AllowedTypes = []int{1, 3}
				cfg.Rules.GroupRules[groupA] = rule
			}
			dispatcher := newTestDispatcher(t, cfg)
			var generated atomic.Int32
			dispatcher.hermesRunner = func(context.Context, batch) (HermesResult, error) {
				generated.Add(1)
				return HermesResult{Text: "reply"}, nil
			}
			msg := testMessage(groupA, "fallback-input")
			if mode == "unknown_source" {
				msg.Source = "unknown"
			}
			if mode == "media" {
				msg.Type = 3
			}
			if err := dispatcher.handleIncomingMessage(msg, "relay:1"); err != nil {
				t.Fatal(err)
			}
			if mode == "multi_input" {
				if err := dispatcher.handleIncomingMessage(testMessage(groupA, "second-input"), "relay:2"); err != nil {
					t.Fatal(err)
				}
			}
			awaitCondition(t, func() bool { return idle(dispatcher) })
			run := onlyTraceRun(t, dispatcher)
			events, err := dispatcher.trace.RunEvents(run.ID)
			if err != nil || generated.Load() != 1 || run.Action != nil || len(events) == 0 {
				t.Fatal("legacy pipeline changed", run, err)
			}
			if mode == "multi_input" {
				if len(run.Inputs) != 2 || run.InputsMissingReason != "" {
					t.Fatal("batch lost input references", run)
				}
			} else if len(run.Inputs) != 0 || run.InputsMissingReason != "relay_input_metadata_unavailable" {
				t.Fatal("unsupported mapping did not report missing evidence", run)
			}
			for _, event := range events {
				value, err := ProcessingArchitectureEvent(run, event)
				if err != nil || value.Source != nil || value.EnvelopeID != "" || value.Action != nil {
					t.Fatal("projection invented a platform or a single batch input", value, err)
				}
			}
		})
	}
}

func TestUnknownActionCannotSubmitAgain(t *testing.T) {
	var posts atomic.Int32
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		http.Error(w, "unknown outcome", http.StatusInternalServerError)
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	dispatcher := newTestDispatcher(t, cfg)
	work := traceBatch(dispatcher, "unknown-input")
	if err := dispatcher.ensureTrace(&work); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.tracePhase(work, contracts.PhaseChecking, "direct_delivery"); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.deliver(context.Background(), work, "reply"); err == nil {
		t.Fatal("HTTP 500 claimed success")
	}
	before := onlyTraceRun(t, dispatcher)
	if before.Action == nil || before.Delivery != contracts.DeliveryUnknown {
		t.Fatal("unknown action metadata missing", before)
	}
	receipt, err := ProcessingActionReceipt(before)
	if err != nil || receipt.Receipt == nil || contracts.ValidateActionReceipt(*receipt.Receipt) != nil || receipt.Receipt.Status != contracts.ActionUnknown || receipt.Receipt.SubmittedAt != nil {
		t.Fatal("invalid unknown public receipt", receipt, err)
	}
	if err := dispatcher.deliver(context.Background(), work, "reply"); err == nil {
		t.Fatal("unknown action admitted a resend")
	}
	after := onlyTraceRun(t, dispatcher)
	if posts.Load() != 1 || after.Revision != before.Revision || after.Delivery != contracts.DeliveryUnknown || dispatcher.traceFailed.Load() {
		t.Fatal("resend changed uncertain state", after)
	}
}
