package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/contracts"
)

func prepareConfirmation(t *testing.T, d *Dispatcher, id, text string, startedAt time.Time) *awaitingConfirmation {
	t.Helper()
	work := traceBatch(d, id)
	if err := d.ensureTrace(&work); err != nil {
		t.Fatal(err)
	}
	if err := d.tracePhase(work, contracts.PhaseChecking, "direct_delivery"); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	confirmation := d.newConfirmationLocked(work, text, messageDigest(work.TraceID+":bridge"))
	if startedAt.After(confirmation.SubmissionStartedAt) {
		t.Fatal("fixture submission boundary is in the future")
	}
	run, err := d.trace.GetRun(work.TraceID)
	if err != nil {
		t.Fatal(err)
	}
	if startedAt.Before(run.AcceptedAt) {
		startedAt = run.AcceptedAt
	}
	confirmation.SubmissionStartedAt = startedAt.UTC()
	if err := d.persistConfirmationsLocked(); err != nil {
		t.Fatal(err)
	}
	if err := d.updateTrace(work, "submission_started", func(run *contracts.Run) {
		run.Phase, run.Delivery = contracts.PhaseSubmitting, contracts.DeliverySubmitting
		run.DeliveryEvidence.SubmissionStartedAt = &confirmation.SubmissionStartedAt
		run.DeliveryEvidence.BridgeRequestID = confirmation.RequestID
	}); err != nil {
		t.Fatal(err)
	}
	return confirmation
}

func localEcho(id int64, timestamp int64) RelayMessage {
	msg := testMessage(groupA, fmt.Sprintf("account:message_0.db:%s:%d", groupA, id))
	msg.Source, msg.LocalID = "message/message_0.db", id
	msg.Timestamp, msg.Content, msg.IsEcho = timestamp, "reply", true
	return msg
}

func TestConfirmationTimestampPrecisionAndBoundaries(t *testing.T) {
	for _, mode := range []string{"seconds_same", "seconds_seen", "seconds_old_sequence", "seconds_old_timestamp", "pre_submission_receipt", "milliseconds_old", "milliseconds_exact_second", "wrong_identity", "wrong_text", "wrong_group"} {
		t.Run(mode, func(t *testing.T) {
			d := newTestDispatcher(t, testConfig())
			prior := localEcho(40, time.Now().Unix()*1000)
			if err := d.handleIncomingMessage(prior, "prior:1"); err != nil {
				t.Fatal(err)
			}
			confirmation := prepareConfirmation(t, d, "precision-input", "reply", time.Now())
			boundary := confirmation.SubmissionStartedAt
			echo := localEcho(41, boundary.Unix()*1000)
			switch mode {
			case "seconds_seen":
				echo = prior
			case "seconds_old_sequence":
				echo = localEcho(39, boundary.Unix()*1000)
			case "seconds_old_timestamp":
				echo.Timestamp -= 1000
			case "pre_submission_receipt":
				d.mu.Lock()
				match := d.matchConfirmationLocked(echo, boundary.Add(-time.Millisecond), confirmation.IncomingSequence)
				d.mu.Unlock()
				if match != nil {
					t.Fatal("pre-submission receipt matched")
				}
				return
			case "milliseconds_old":
				echo.Source, echo.LocalID, echo.Timestamp = "", 0, boundary.UnixMilli()-1
			case "milliseconds_exact_second":
				echo.Source, echo.LocalID = "", 0
				if boundary.UnixMilli()%1000 == 0 {
					echo.Timestamp -= 1000
				}
			case "wrong_identity":
				echo.IsEcho = false
			case "wrong_text":
				echo.Content = "other reply"
			case "wrong_group":
				echo.GroupID = groupB
			}
			if err := d.handleIncomingMessage(echo, "echo:1"); err != nil {
				t.Fatal(err)
			}
			run := onlyTraceRun(t, d)
			if run.Delivery == contracts.DeliveryConfirmed {
				t.Fatal("old or unrelated echo confirmed", run)
			}
			if mode == "seconds_same" {
				d.mu.Lock()
				err := d.confirmOrUnknownLocked(confirmation, "echo_timeout")
				d.mu.Unlock()
				if err == nil || onlyTraceRun(t, d).Delivery != contracts.DeliveryUnknown {
					t.Fatal("ambiguous same-second evidence did not remain unknown")
				}
			}
		})
	}
}

func TestSecondPrecisionEchoAtHalfSecondSubmission(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	confirmation := prepareConfirmation(t, d, "half-second-input", "reply", time.Now())
	boundary := time.Now().Truncate(time.Second).Add(500 * time.Millisecond)
	d.mu.Lock()
	defer d.mu.Unlock()
	confirmation.SubmissionStartedAt = boundary
	echo := localEcho(41, boundary.Unix()*1000)
	confirmation.SourceWatermarks[messageSource(echo)] = sourceWatermark{LocalID: 40}
	if got := d.matchConfirmationLocked(echo, boundary.Add(200*time.Millisecond), confirmation.IncomingSequence+1); got != nil {
		t.Fatal("unseen old 41 produced at T+.1 accepted after T+.5 boundary with watermark 40")
	}
	if got := d.matchConfirmationLocked(echo, boundary.Add(-time.Millisecond), confirmation.IncomingSequence+1); got != nil {
		t.Fatal("pre-submission echo receipt accepted")
	}
	if got := d.matchConfirmationLocked(echo, boundary.Add(200*time.Millisecond), confirmation.IncomingSequence); got != nil {
		t.Fatal("pre-submission receive watermark accepted")
	}
	echo.Timestamp += 1000
	if got := d.matchConfirmationLocked(echo, boundary.Add(time.Second), confirmation.IncomingSequence+1); got != confirmation {
		t.Fatal("next-second echo rejected")
	}
	echo.Source, echo.LocalID = "", 0
	echo.Timestamp = boundary.UnixMilli() + 1
	if got := d.matchConfirmationLocked(echo, boundary.Add(200*time.Millisecond), confirmation.IncomingSequence+1); got != confirmation {
		t.Fatal("precise after-boundary echo rejected")
	}
	echo.Timestamp = boundary.Unix() * 1000
	if got := d.matchConfirmationLocked(echo, boundary.Add(200*time.Millisecond), confirmation.IncomingSequence+1); got != nil {
		t.Fatal("precise source received a one-second allowance")
	}
}

func TestReliableEchoBeforeHTTPErrorConfirms(t *testing.T) {
	for _, transportError := range []bool{false, true} {
		t.Run(fmt.Sprint(transportError), func(t *testing.T) {
			var dispatcher *Dispatcher
			var posts atomic.Int32
			bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				echo := testMessage(groupA, "echo-before-error")
				echo.IsEcho, echo.Content = true, "reply"
				if err := dispatcher.handleIncomingMessage(echo, "echo:error"); err != nil {
					t.Error(err)
				}
				if transportError {
					connection, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						connection.Close()
					}
					return
				}
				http.Error(w, "failed after echo", http.StatusInternalServerError)
			})
			cfg := testConfig()
			cfg.Bridge.URL = bridge.URL
			dispatcher = newTestDispatcher(t, cfg)
			if err := dispatcher.deliver(context.Background(), traceBatch(dispatcher, "echo-error-input"), "reply"); err != nil {
				t.Fatal(err)
			}
			run := onlyTraceRun(t, dispatcher)
			if run.Delivery != contracts.DeliveryConfirmed || run.ReasonCode != "submission_time_echo_inferred" || run.DeliveryEvidence.SubmittedAt != nil || run.DeliveryEvidence.SubmittedAtSource != "echo_inferred" || posts.Load() != 1 {
				t.Fatal(run)
			}
			receipt, err := ProcessingActionReceipt(run)
			if err != nil || receipt.Receipt == nil || contracts.ValidateActionReceipt(*receipt.Receipt) != nil || receipt.Receipt.SubmittedAt != nil || receipt.Receipt.SubmittedAtSource != "echo_inferred" || receipt.Receipt.Confirmation == nil || receipt.Receipt.Confirmation.EchoMessageID != "echo-before-error" {
				t.Fatal("HTTP error with echo lost correlated public receipt", receipt, err)
			}
			if dispatcher.state.Stats.TotalReplied != 1 || dispatcher.state.Stats.TotalSubmitted != 1 || dispatcher.state.Stats.TotalUnknown != 0 || len(dispatcher.state.AwaitingConfirmations) != 0 {
				t.Fatal(dispatcher.state)
			}
		})
	}
}

func TestLateEchoConfirmationAndRestart(t *testing.T) {
	for _, mode := range []string{"late", "reopen", "error_reopen", "persisted_echo", "error_persisted_echo", "confirmed_before_state_commit", "ack_persisted_only", "ack_before_state_commit"} {
		t.Run(mode, func(t *testing.T) {
			var posts atomic.Int32
			bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				if mode == "error_reopen" || mode == "error_persisted_echo" {
					http.Error(w, "unknown", 500)
					return
				}
				fmt.Fprint(w, "OK")
			})
			cfg := testConfig()
			cfg.Bridge.URL = bridge.URL
			d := newTestDispatcher(t, cfg)
			d.echoTimeout = time.Millisecond
			if mode == "ack_persisted_only" {
				d.trace = failingTrace{traceStore: d.trace, kind: "submission_accepted"}
			} else if mode == "ack_before_state_commit" {
				d.trace = confirmationCommitFailure{traceStore: d.trace, kind: "submission_accepted"}
			}
			if err := d.deliver(context.Background(), traceBatch(d, "late-input"), "reply"); err == nil {
				t.Fatal("missing echo reported success")
			}
			unknown := onlyTraceRun(t, d)
			if (unknown.Delivery != contracts.DeliveryUnknown && mode != "ack_persisted_only" && mode != "ack_before_state_commit") || len(d.state.AwaitingConfirmations) != 1 {
				t.Fatal(unknown)
			}
			echo := testMessage(groupA, "late-echo")
			echo.IsEcho, echo.Content = true, "reply"
			if mode == "persisted_echo" || mode == "error_persisted_echo" || mode == "confirmed_before_state_commit" {
				if mode == "persisted_echo" || mode == "error_persisted_echo" {
					d.trace = failingTrace{traceStore: d.trace, kind: "delivery_confirmed"}
				} else {
					d.trace = confirmationCommitFailure{traceStore: d.trace, kind: "delivery_confirmed"}
				}
				if err := d.handleIncomingMessage(echo, "late:1"); err == nil || !d.traceFailed.Load() {
					t.Fatal("trace write failure did not stop dispatcher")
				}
			}
			if mode != "late" {
				d.Close()
				reopened, err := NewDispatcher(d.cfgFile)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(reopened.Close)
				d = reopened
			}
			if err := d.handleIncomingMessage(echo, "late:1"); err != nil {
				t.Fatal(err)
			}
			run := onlyTraceRun(t, d)
			if err := contracts.ValidateRun(run); err != nil || run.Delivery != contracts.DeliveryConfirmed || posts.Load() != 1 || d.state.Stats.TotalReplied != 1 || d.state.Stats.TotalSubmitted != 1 || d.state.Stats.TotalUnknown != 1 || len(d.state.AwaitingConfirmations) != 0 {
				t.Fatal(run, d.state.Stats, err)
			}
			receipt, err := ProcessingActionReceipt(run)
			if err != nil || receipt.Receipt == nil || contracts.ValidateActionReceipt(*receipt.Receipt) != nil || receipt.Receipt.Confirmation == nil || receipt.Receipt.Confirmation.RequestID != run.Action.ActionID {
				t.Fatal("late/restart confirmation lost durable action correlation", receipt, err)
			}
			if mode == "error_reopen" || mode == "error_persisted_echo" {
				if run.DeliveryEvidence.SubmittedAt != nil || run.DeliveryEvidence.SubmittedAtSource != "echo_inferred" {
					t.Fatal("late echo fabricated a historical ACK", run)
				}
			} else if run.DeliveryEvidence.SubmittedAt == nil || run.DeliveryEvidence.SubmittedAtSource != "bridge_ack" {
				t.Fatal("observed ACK evidence lost", run)
			}
			events, err := d.trace.RunEvents(run.ID)
			if err != nil {
				t.Fatal(err)
			}
			unknownEvent := false
			for _, event := range events {
				unknownEvent = unknownEvent || event.Kind == "run_unknown" || event.Kind == "run_recovered"
			}
			if !unknownEvent {
				t.Fatal("unknown history erased")
			}
			revision := run.Revision
			d.Close()
			reopened, err := NewDispatcher(d.cfgFile)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if err := reopened.handleIncomingMessage(echo, "replay:1"); err != nil {
				t.Fatal(err)
			}
			if onlyTraceRun(t, reopened).Revision != revision || reopened.state.Stats.TotalReplied != 1 || posts.Load() != 1 {
				t.Fatal("confirmed echo counted again")
			}
		})
	}
}

type confirmationCommitFailure struct {
	traceStore
	kind string
}

func (store confirmationCommitFailure) UpdateRun(run contracts.Run, revision, kind string) (contracts.Run, error) {
	stored, err := store.traceStore.UpdateRun(run, revision, kind)
	if err == nil && kind == store.kind {
		return contracts.Run{}, fmt.Errorf("commit result unknown")
	}
	return stored, err
}

func TestAmbiguousLateEchoDoesNotConfirm(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	first := prepareConfirmation(t, d, "first-unknown", "reply", time.Now())
	second := prepareConfirmation(t, d, "second-unknown", "reply", time.Now())
	d.mu.Lock()
	for _, confirmation := range []*awaitingConfirmation{first, second} {
		if err := d.confirmOrUnknownLocked(confirmation, "echo_timeout"); err == nil {
			t.Fatal("missing echo reported success")
		}
	}
	d.mu.Unlock()
	echo := testMessage(groupA, "ambiguous-echo")
	echo.IsEcho, echo.Content = true, "reply"
	if err := d.handleIncomingMessage(echo, "ambiguous:1"); err != nil {
		t.Fatal(err)
	}
	if first.Echo != nil || second.Echo != nil || d.state.Stats.TotalReplied != 0 || len(d.state.AwaitingConfirmations) != 2 {
		t.Fatal("ambiguous echo matched")
	}
	data, err := os.ReadFile(d.stateFile)
	if err != nil {
		t.Fatal(err)
	}
	var disk struct {
		Index map[string]json.RawMessage `json:"awaiting_confirmations"`
	}
	if err := json.Unmarshal(data, &disk); err != nil {
		t.Fatal(err)
	}
	for _, entry := range disk.Index {
		if strings.Contains(string(entry), `"reply"`) || !strings.Contains(string(entry), first.TextDigest) {
			t.Fatal("index leaked text or lost digest", string(entry))
		}
	}
}

func TestLateEchoDoesNotWaitForConfigurationOrSubmissionLock(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	prepareConfirmation(t, d, "locked-config", "reply", time.Now())
	d.cfgMu.Lock()
	d.submissionMu.Lock()
	done := make(chan error, 1)
	go func() {
		echo := testMessage(groupA, "independent-echo")
		echo.IsEcho, echo.Content = true, "reply"
		done <- d.handleIncomingMessage(echo, "echo:independent")
	}()
	select {
	case err := <-done:
		d.submissionMu.Unlock()
		d.cfgMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		d.submissionMu.Unlock()
		d.cfgMu.Unlock()
		t.Fatal("independent confirmation blocked on config/submission lock")
	}
}

func TestSubmissionBoundaryPersistsReceiveSequenceBeforeRestart(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	d.intakeSequence.Add(7)
	confirmation := prepareConfirmation(t, d, "pending-receipt", "reply", time.Now())
	if confirmation.IncomingSequence != 7 || d.state.IncomingSequence != 7 {
		t.Fatal("submission did not persist the receive boundary")
	}
	d.Close()
	reopened, err := NewDispatcher(d.cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	echo := testMessage(groupA, "first-after-restart")
	echo.Content, echo.IsEcho = "reply", true
	if err := reopened.handleIncomingMessage(echo, "echo:restart"); err != nil {
		t.Fatal(err)
	}
	if onlyTraceRun(t, reopened).Delivery != contracts.DeliveryConfirmed || reopened.intakeSequence.Load() != 8 {
		t.Fatal("first post-restart receipt rejected")
	}
}

func TestConfirmedEchoRemainsDeduplicatedAfterSeenRetention(t *testing.T) {
	for _, reopen := range []bool{false, true} {
		t.Run(fmt.Sprint(reopen), func(t *testing.T) {
			d := newTestDispatcher(t, testConfig())
			prepareConfirmation(t, d, "first-confirmation", "reply", time.Now())
			echo := testMessage(groupA, "retained-confirmed-id")
			echo.Content, echo.IsEcho = "reply", true
			if err := d.handleIncomingMessage(echo, "confirmed:1"); err != nil {
				t.Fatal(err)
			}
			d.mu.Lock()
			d.state.Seen = make(map[string]time.Time)
			d.groupLocked(groupA).History = nil
			d.mu.Unlock()
			second := prepareConfirmation(t, d, "other-awaiting-confirmation", "reply", time.Now())
			if reopen {
				d.Close()
				reopened, err := NewDispatcher(d.cfgFile)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(reopened.Close)
				d = reopened
			}
			echo.Timestamp = time.Now().UnixMilli()
			if err := d.handleIncomingMessage(echo, "replayed:1"); err != nil {
				t.Fatal(err)
			}
			run, err := d.trace.GetRun(second.RunID)
			if err != nil || run.Delivery == contracts.DeliveryConfirmed || d.state.Stats.TotalReplied != 1 {
				t.Fatal("confirmed echo reused by another run", run, err)
			}
		})
	}
}
