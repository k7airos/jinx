package dispatcher

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/observability"
)

func traceBatch(d *Dispatcher, id string) batch {
	cfg := d.configSnapshot()
	rule, _ := cfg.route(groupA)
	msg := testMessage(groupA, id)
	receivedAt := time.Now().UTC()
	msg.ReceivedAt = &receivedAt
	return batch{GroupID: groupA, Messages: []RelayMessage{msg}, Rule: rule, Config: cfg, Policy: policyID(cfg), Reason: "batch_size"}
}

func onlyTraceRun(t *testing.T, d *Dispatcher) contracts.Run {
	t.Helper()
	snapshot, err := d.trace.Snapshot()
	if err != nil || len(snapshot.Runs) != 1 {
		t.Fatalf("trace snapshot: %+v %v", snapshot, err)
	}
	return snapshot.Runs[0]
}

func traceRequest(d *Dispatcher, path string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "http://localhost"+path, nil)
	w := httptest.NewRecorder()
	d.Handler().ServeHTTP(w, r)
	return w
}

func TestTraceTerminalOutcomes(t *testing.T) {
	for _, mode := range []string{"none", "gate", "error", "dryrun", "configchanged", "invalid", "expired", "limited", "other", "shadow", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			var bridgeCalls atomic.Int32
			bridge := bridgeFixture(t, func(http.ResponseWriter, *http.Request) { bridgeCalls.Add(1) })
			gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"should_reply":false,"intent":"noise","decision":"block","probability":0.9}`)
			}))
			defer gate.Close()
			cfg := testConfig()
			cfg.Bridge.URL = bridge.URL
			cfg.Bridge.DryRun = mode == "dryrun"
			cfg.Rules.LayaFilter, cfg.Rules.LayaURL = mode == "gate", gate.URL
			if mode == "shadow" {
				rule := cfg.Rules.GroupRules[groupA]
				rule.PromoKeywords, rule.PromoShadow = []string{"changed"}, true
				cfg.Rules.GroupRules[groupA] = rule
			}
			d := newTestDispatcher(t, cfg)
			var calls int
			d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
				calls++
				if mode == "error" {
					return HermesResult{}, errors.New("PRIVATE_BACKEND_ERROR /private/profile")
				}
				if mode == "timeout" {
					return HermesResult{}, context.DeadlineExceeded
				}
				if mode == "configchanged" {
					response := configRequest(d, http.MethodPost, `{"dry_run":true}`, b.Policy)
					if response.Code != http.StatusOK {
						t.Fatal(response.Code, response.Body.String())
					}
				}
				text := "PRIVATE_REPLY"
				if mode == "none" {
					text = "NONE"
				}
				if mode == "invalid" {
					text = "bad\x00reply"
				}
				return HermesResult{Text: text, SessionID: "fake-session"}, nil
			}
			work := traceBatch(d, "message-"+mode)
			if mode == "expired" {
				work.Messages[0].Timestamp = time.Now().Add(-time.Hour).UnixMilli()
			}
			if mode == "limited" {
				d.groupLocked(groupA).ReplyTimes = []time.Time{time.Now(), time.Now()}
			}
			if mode == "other" {
				work.Messages[0].Content = "@SomeoneElse hello"
			}
			if !d.processBatch(work) {
				t.Fatal("batch did not complete")
			}
			run := onlyTraceRun(t, d)
			wantOutcome := map[string]contracts.Outcome{"none": contracts.OutcomeSilent, "gate": contracts.OutcomeBlocked, "error": contracts.OutcomeFailed, "dryrun": contracts.OutcomeSuppressed, "configchanged": contracts.OutcomeSuppressed, "invalid": contracts.OutcomeBlocked, "expired": contracts.OutcomeExpired, "limited": contracts.OutcomeSuppressed, "other": contracts.OutcomeSilent, "shadow": contracts.OutcomeSuppressed, "timeout": contracts.OutcomeFailed}[mode]
			if run.Phase != contracts.PhaseTerminal || run.Outcome != wantOutcome || !run.DeliveryEvidence.NoSubmission || run.ReasonCode == "" || bridgeCalls.Load() != 0 || run.Action != nil {
				t.Fatalf("bad terminal run: %+v", run)
			}
			if mode == "configchanged" && run.ReasonCode != "config_changed" {
				t.Fatal(run.ReasonCode)
			}
			if mode == "timeout" && (run.Attempts[0].Status != contracts.AttemptTimedOut || run.Attempts[0].RemoteExecution != contracts.RemoteMayBeRunning) {
				t.Fatal(run.Attempts)
			}
			if (mode == "gate" || mode == "expired" || mode == "limited" || mode == "other") && calls != 0 {
				t.Fatal("pre-generation decision called backend")
			}
			data, err := os.ReadFile(filepath.Join(filepath.Dir(d.stateFile), "trace.json"))
			if err != nil {
				t.Fatal(err)
			}
			for _, private := range []string{"PRIVATE_REPLY", "PRIVATE_BACKEND_ERROR", "/private/profile", "What changed?", d.cfg.Hermes.ProfilesDir, "prompt", "bin_path"} {
				if strings.Contains(string(data), private) {
					t.Fatalf("trace leaked %q", private)
				}
			}
		})
	}
}

func TestTraceAcceptedThroughConfirmedAndBackendEvidence(t *testing.T) {
	var d *Dispatcher
	var posts atomic.Int32
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		var body struct {
			RequestID string `json:"request_id"`
			Text      string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		run := onlyTraceRun(t, d)
		if run.Phase != contracts.PhaseSubmitting || run.DeliveryEvidence.SubmissionStartedAt == nil || run.DeliveryEvidence.BridgeRequestID != body.RequestID || run.Action == nil || run.Action.ActionID != body.RequestID {
			t.Error("POST preceded durable submission boundary", run)
		}
		if !d.submissionMu.TryLock() {
			t.Error("submission lock held across bridge HTTP")
		} else {
			d.submissionMu.Unlock()
		}
		echo := testMessage(groupB, "verified-echo")
		echo.IsEcho, echo.Content = true, body.Text
		if err := d.handleIncomingMessage(echo, "echo:9"); err != nil {
			t.Error(err)
		}
		fmt.Fprint(w, "OK")
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	rule := cfg.Rules.GroupRules[groupA]
	rule.TargetGroupID = groupB
	cfg.Rules.GroupRules[groupA] = rule
	d = newTestDispatcher(t, cfg)
	bootstrap, err := d.trace.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
		run := onlyTraceRun(t, d)
		if b.TraceID != run.ID || run.Phase != contracts.PhaseExecuting || len(run.Attempts) != 1 || run.Attempts[0].Status != contracts.AttemptRunning {
			t.Error("backend preceded attempt start", run)
		}
		return HermesResult{Text: "private generated reply", Backend: "Hermes", Fallback: true, FallbackReason: "alma_timeout", RunID: "actual-run", SessionID: "local"}, nil
	}
	input := testMessage(groupA, "input")
	untrustedReceipt := time.Now().Add(-time.Hour)
	input.ReceivedAt = &untrustedReceipt
	beforeReceipt := time.Now().UTC()
	if err := d.handleIncomingMessage(input, "relay:1"); err != nil {
		t.Fatal(err)
	}
	awaitCondition(t, func() bool { return idle(d) })
	run := onlyTraceRun(t, d)
	if len(run.Inputs) != 1 || run.InputsMissingReason != "" || run.Inputs[0].ReceivedAt == nil || run.Inputs[0].ReceivedAt.Before(beforeReceipt) || run.Inputs[0].ReceivedAt.After(run.AcceptedAt) || run.Inputs[0].Source.AccountID != wechatMappingAccount || run.Inputs[0].Source.ContentID != input.ID || run.Inputs[0].Source.ParentID != "" || run.Inputs[0].Source.RootID != "" {
		t.Fatal("real intake did not persist normalized observed metadata", run.Inputs)
	}
	normalizedInput := input
	normalizedInput.ReceivedAt = run.Inputs[0].ReceivedAt
	envelope, err := NormalizeRelayMessage(normalizedInput)
	if err != nil || run.Inputs[0].EnvelopeID != envelope.EnvelopeID || run.Inputs[0].DedupKey != envelope.DedupKey {
		t.Fatal("real intake did not use the shared normalization", envelope, err)
	}
	receipt, err := ProcessingActionReceipt(run)
	if err != nil || receipt.Receipt == nil || contracts.ValidateActionReceipt(*receipt.Receipt) != nil || receipt.Receipt.SubmittedAt != nil || receipt.Receipt.Confirmation == nil || receipt.Receipt.Confirmation.RequestID != run.Action.ActionID {
		t.Fatal("real echo-inferred delivery has no valid action receipt", receipt, err)
	}
	if run.SourceGroupID != groupA || run.TargetGroupID != groupB || run.AdmissionWatermark.Token != "relay:1" || run.AdmissionWatermark.SourceSequence != "1" || run.Versions.Policy != policyID(d.cfg) || run.MissingSnapshotReason != "" || run.ConfigSnapshotID == "" {
		t.Fatal("admission facts missing", run)
	}
	if run.Delivery != contracts.DeliveryConfirmed || run.Outcome != contracts.OutcomeReplied || run.DeliveryEvidence.EchoMessageID != "verified-echo" || posts.Load() != 1 {
		t.Fatal("confirmation evidence missing", run)
	}
	if run.DeliveryEvidence.SubmittedAt != nil || run.DeliveryEvidence.SubmittedAtSource != "echo_inferred" {
		t.Fatal("echo before ACK fabricated a precise submission time", run)
	}
	if len(run.Attempts) != 2 || run.Attempts[0].BackendID != "reply_adapter" || run.Attempts[0].Status != contracts.AttemptSucceeded || run.Attempts[1].BackendID != "Hermes" || run.Attempts[1].ExternalID != "actual-run" || run.Attempts[1].FallbackReason != "alma_timeout" || run.Attempts[1].StartedAt != nil || run.Attempts[1].EndedAt != nil || run.Attempts[1].Status != contracts.AttemptUnknown {
		t.Fatal("adapter internal attempt details invented or result missing", run.Attempts)
	}
	events, err := d.trace.RunEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	kinds := []string{"run_accepted", "run_aggregating", "run_gating", "run_queued", "run_executing", "attempt_started", "attempt_finished", "run_checking", "submission_started", "delivery_confirmed"}
	if len(events) != len(kinds) {
		t.Fatal(events)
	}
	for index, kind := range kinds {
		if events[index].Kind != kind {
			t.Fatal(events)
		}
		projected, err := ProcessingArchitectureEvent(run, events[index])
		if err != nil || projected.EnvelopeID != run.Inputs[0].EnvelopeID || projected.Source == nil || projected.Source.ContentID != input.ID || (projected.Action != nil) != (index >= 8) {
			t.Fatal("historical projection attached future action or lost inputs", projected, err)
		}
	}
	if events[0].Phase != contracts.PhaseAccepted || events[0].Delivery != contracts.DeliveryNotStarted || events[0].Outcome != contracts.OutcomePending {
		t.Fatal(events[0])
	}
	page, err := d.trace.EventsAfter(bootstrap.Cursor, 100)
	if err != nil {
		t.Fatal(err)
	}
	response := traceRequest(d, "/api/trace/events?projection=processing&cursor="+bootstrap.Cursor)
	var projected processingEventPage
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &projected) != nil || projected.Cursor != page.Cursor || projected.HasMore != page.HasMore || len(projected.Events) != len(page.Events) {
		t.Fatal("processing endpoint changed stream cursor", response.Body.String(), page)
	}
	for index, item := range projected.Events {
		if item.Seq != page.Events[index].Seq || item.Cursor != page.Events[index].Cursor || item.StreamID != page.Events[index].StreamID || (item.Event.Action != nil) != (index >= 8) || (item.Receipt != nil) != (index == len(projected.Events)-1) {
			t.Fatal("processing endpoint attached future action or receipt", item)
		}
	}
	finalReceipt := projected.Events[len(projected.Events)-1].Receipt
	if finalReceipt.Receipt == nil || contracts.ValidateActionReceipt(*finalReceipt.Receipt) != nil || finalReceipt.Receipt.SubmittedAt != nil {
		t.Fatal("processing endpoint lost public echo-inferred receipt", finalReceipt)
	}
	d.Close()
	reopened, err := NewDispatcher(d.cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := onlyTraceRun(t, reopened); got.Revision != run.Revision || got.Delivery != contracts.DeliveryConfirmed || len(got.Inputs) != 1 || got.Inputs[0].EnvelopeID != run.Inputs[0].EnvelopeID || got.Action == nil || got.Action.ActionID != run.Action.ActionID {
		t.Fatal("terminal history changed across restart", got)
	}
}

func TestTraceUnknownAndRestartNeverRegenerates(t *testing.T) {
	for _, mode := range []string{"accepted_no_echo", "http_error", "transport", "false_echo"} {
		t.Run(mode, func(t *testing.T) {
			var d *Dispatcher
			var posts atomic.Int32
			bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				if mode == "transport" {
					connection, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						connection.Close()
					}
					return
				}
				if mode == "http_error" {
					http.Error(w, "PRIVATE_BRIDGE_ERROR", 500)
					return
				}
				if mode == "false_echo" {
					d.mu.Lock()
					close(d.awaitingEcho.done)
					d.mu.Unlock()
				}
				fmt.Fprint(w, "OK")
			})
			cfg := testConfig()
			cfg.Bridge.URL = bridge.URL
			d = newTestDispatcher(t, cfg)
			d.echoTimeout = 10 * time.Millisecond
			d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
				return HermesResult{Text: "reply", SessionID: "fake"}, nil
			}
			work := traceBatch(d, "unknown-input")
			if !d.processBatch(work) {
				t.Fatal("uncertain delivery processing failed")
			}
			run := onlyTraceRun(t, d)
			if run.Phase != contracts.PhaseUnknown || run.Delivery != contracts.DeliveryUnknown || run.DeliveryEvidence.EchoMessageID != "" || run.DeliveryEvidence.NoSubmission || posts.Load() != 1 {
				t.Fatal(run)
			}
			if (run.DeliveryEvidence.SubmittedAt != nil) != (mode != "http_error" && mode != "transport") {
				t.Fatal("acceptance evidence fabricated or missing", run)
			}
			d.mu.Lock()
			work.Messages[0].TraceID = ""
			d.groupLocked(groupA).Pending = work.Messages
			d.state.Seen = map[string]time.Time{}
			if err := d.saveStateLocked(); err != nil {
				t.Fatal(err)
			}
			d.mu.Unlock()
			d.Close()
			reopened, err := NewDispatcher(d.cfgFile)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			reopened.hermesRunner = func(context.Context, batch) (HermesResult, error) {
				t.Error("historical run regenerated")
				return HermesResult{}, nil
			}
			reopened.mu.Lock()
			generation := reopened.groupLocked(groupA).TimerGeneration
			reopened.mu.Unlock()
			reopened.startBatch(groupA, generation, "batch_size")
			if err := reopened.handleIncomingMessage(work.Messages[0], "replay:1"); err != nil {
				t.Fatal(err)
			}
			if !idle(reopened) || len(reopened.state.Groups[groupA].Pending) != 0 || onlyTraceRun(t, reopened).ReasonCode != "restart_recovery" || posts.Load() != 1 {
				t.Fatal("replayed traced input was admitted")
			}
		})
	}
}

type failingTrace struct {
	traceStore
	kind string
}

func (store failingTrace) CreateRun(run contracts.Run) (contracts.Run, error) {
	if store.kind == "create" {
		return contracts.Run{}, errors.New("PRIVATE_STORE_ERROR /secret/path")
	}
	return store.traceStore.CreateRun(run)
}

func (store failingTrace) UpdateRun(run contracts.Run, revision, kind string) (contracts.Run, error) {
	if store.kind == kind {
		return contracts.Run{}, errors.New("PRIVATE_STORE_ERROR /secret/path")
	}
	return store.traceStore.UpdateRun(run, revision, kind)
}

func TestTracePersistenceFailureStopsExternalWork(t *testing.T) {
	for _, kind := range []string{"create", "run_gating", "run_queued", "run_executing", "attempt_started", "attempt_finished", "run_checking", "submission_started", "submission_accepted", "delivery_confirmed", "run_terminal"} {
		t.Run(kind, func(t *testing.T) {
			var d *Dispatcher
			var posts, generations atomic.Int32
			bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				echo := testMessage(groupA, "echo")
				echo.IsEcho, echo.Content = true, "reply"
				if kind != "submission_accepted" {
					if err := d.handleIncomingMessage(echo, ""); err != nil {
						t.Error(err)
					}
				}
				fmt.Fprint(w, "OK")
			})
			cfg := testConfig()
			cfg.Bridge.URL = bridge.URL
			d = newTestDispatcher(t, cfg)
			d.trace = failingTrace{traceStore: d.trace, kind: kind}
			d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
				generations.Add(1)
				text := "reply"
				if kind == "run_terminal" {
					text = "NONE"
				}
				return HermesResult{Text: text, SessionID: "fake"}, nil
			}
			if d.processBatch(traceBatch(d, "failure")) || !d.traceFailed.Load() || d.ctx.Err() == nil {
				t.Fatal("persistence failure reported success")
			}
			beforeGeneration := kind == "create" || kind == "run_gating" || kind == "run_queued" || kind == "run_executing" || kind == "attempt_started"
			if beforeGeneration && generations.Load() != 0 {
				t.Fatal("generation bypassed persistence failure")
			}
			if kind != "submission_accepted" && kind != "delivery_confirmed" && posts.Load() != 0 {
				t.Fatal("send bypassed persistence failure")
			}
			if d.state.Stats.TotalReplied != 0 || d.HealthSnapshot().Status != "not_ready" {
				t.Fatal("failure counted as confirmed reply")
			}
			if response := traceRequest(d, "/api/trace/snapshot"); response.Code != 503 || strings.Contains(response.Body.String(), "PRIVATE") || strings.Contains(response.Body.String(), "/secret") {
				t.Fatal(response.Code, response.Body.String())
			}
		})
	}
}

func TestTraceHTTPAtomicBootstrapPagingAndGuard(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	response := traceRequest(d, "/api/trace/snapshot")
	var snapshot observability.Snapshot
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil || response.Code != 200 || snapshot.StreamID == "" || snapshot.Cursor == "" || snapshot.LastSeq != 0 || len(snapshot.Runs) != 0 {
		t.Fatal(response.Code, response.Body.String(), err)
	}
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		return HermesResult{Text: "NONE", SessionID: "fake"}, nil
	}
	if !d.processBatch(traceBatch(d, "http")) {
		t.Fatal("fixture failed")
	}
	run := onlyTraceRun(t, d)
	cursor, count := snapshot.Cursor, 0
	for {
		response = traceRequest(d, "/api/trace/events?cursor="+url.QueryEscape(cursor)+"&limit=2")
		var page observability.EventPage
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil || response.Code != 200 || len(page.Events) == 0 {
			t.Fatal(response.Code, response.Body.String(), err)
		}
		for _, event := range page.Events {
			count++
			if event.Seq != uint64(count) || event.StreamID != snapshot.StreamID {
				t.Fatal("non-contiguous event page", event)
			}
		}
		cursor = page.Cursor
		if !page.HasMore {
			break
		}
	}
	response = traceRequest(d, "/api/trace/snapshot")
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil || snapshot.Cursor != cursor || snapshot.LastSeq != uint64(count) || snapshot.Runs[0].EntitySeq != snapshot.LastSeq {
		t.Fatal("snapshot and journal disagree", snapshot, err)
	}
	response = traceRequest(d, "/api/trace/runs/"+run.ID)
	var detail struct {
		Run    contracts.Run     `json:"run"`
		Events []contracts.Event `json:"events"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &detail); err != nil || response.Code != 200 || detail.Run.ID != run.ID || len(detail.Events) != count || detail.Events[0].Phase != contracts.PhaseAccepted {
		t.Fatal(response.Code, response.Body.String(), err)
	}
	for _, path := range []string{"/api/trace/events?cursor=bad", "/api/trace/events", "/api/trace/events?cursor=" + base64.RawURLEncoding.EncodeToString([]byte(`{"StreamID":"foreign","Seq":0}`))} {
		if response := traceRequest(d, path); response.Code != 409 || strings.TrimSpace(response.Body.String()) != `{"code":"reset_required"}` {
			t.Fatal(response.Code, response.Body.String())
		}
	}
	if response := traceRequest(d, "/api/trace/runs/missing"); response.Code != 404 {
		t.Fatal(response.Code)
	}
	for _, path := range []string{"/api/trace/snapshot", "/api/trace/events", "/api/trace/runs/" + run.ID} {
		for _, mode := range []string{"host", "origin", "cross-site", "method"} {
			r := httptest.NewRequest(http.MethodGet, "http://localhost"+path, nil)
			switch mode {
			case "host":
				r.Host = "external.invalid"
			case "origin":
				r.Header.Set("Origin", "http://external.invalid")
			case "cross-site":
				r.Header.Set("Sec-Fetch-Site", "cross-site")
			case "method":
				r.Method = http.MethodPost
			}
			w := httptest.NewRecorder()
			d.Handler().ServeHTTP(w, r)
			if w.Code != 403 && w.Code != 405 {
				t.Fatal(mode, path, w.Code)
			}
		}
	}
	store := d.trace.(*observability.Store)
	if err := store.PruneEvents(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if response := traceRequest(d, "/api/trace/events?cursor="+url.QueryEscape(detail.Events[0].Cursor)); response.Code != 409 {
		t.Fatal("expired cursor accepted", response.Code)
	}
}

func TestTraceLongPollCancellationAndWakeup(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	snapshot, err := d.trace.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	path := "http://localhost/api/trace/events?wait=1&cursor=" + url.QueryEscape(snapshot.Cursor)
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, path, nil).WithContext(ctx)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		w := httptest.NewRecorder()
		d.Handler().ServeHTTP(w, r)
		done <- w
	}()
	cancel()
	select {
	case response := <-done:
		if response.Code != 503 {
			t.Fatal(response.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("longpoll ignored cancellation")
	}
	r = httptest.NewRequest(http.MethodGet, path, nil)
	go func() {
		w := httptest.NewRecorder()
		d.Handler().ServeHTTP(w, r)
		done <- w
	}()
	work := traceBatch(d, "wakeup")
	if err := d.ensureTrace(&work); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-done:
		var page observability.EventPage
		if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil || response.Code != 200 || len(page.Events) != 1 {
			t.Fatal(response.Code, response.Body.String(), err)
		}
	case <-time.After(time.Second):
		t.Fatal("longpoll lost wakeup")
	}
}

func TestTraceOpenFailureLegacyQueueAndHealth(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	if _, err := NewDispatcher(d.cfgFile); !errors.Is(err, observability.ErrLocked) {
		t.Fatal("second writer allowed", err)
	}
	d.Close()
	tracePath := filepath.Join(filepath.Dir(d.stateFile), "trace.json")
	if err := os.WriteFile(tracePath, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDispatcher(d.cfgFile); err == nil {
		t.Fatal("corrupt trace opened")
	}
	if data, _ := os.ReadFile(tracePath); string(data) != "corrupt" {
		t.Fatal("corrupt trace replaced")
	}
	legacy := newTestDispatcher(t, testConfig())
	legacy.mu.Lock()
	legacy.groupLocked(groupA).Pending = []RelayMessage{testMessage(groupA, "legacy-input")}
	legacy.mu.Unlock()
	legacy.Close()
	reopened, err := NewDispatcher(legacy.cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if snapshot, _ := reopened.trace.Snapshot(); len(snapshot.Runs) != 0 {
		t.Fatal("old state synthesized runs")
	}
	reopened.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		return HermesResult{Text: "NONE", SessionID: "fake"}, nil
	}
	reopened.startBatch(groupA, reopened.state.Groups[groupA].TimerGeneration, "batch_size")
	awaitCondition(t, func() bool { return idle(reopened) })
	if run := onlyTraceRun(t, reopened); run.Completeness != contracts.Partial || run.MissingSnapshotReason != "legacy_queue_admission_history_unavailable" {
		t.Fatal(run)
	}
	if err := reopened.trace.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(filepath.Dir(reopened.stateFile), "trace.json")); err != nil {
		t.Fatal(err)
	}
	reopened.client = &http.Client{Transport: rejectingTransport{t}}
	health := traceRequest(reopened, "/health")
	var value Health
	if err := json.Unmarshal(health.Body.Bytes(), &value); err != nil || health.Code != 200 || !value.Capabilities["trace"].Supported || !value.Capabilities["trace"].ReadOnly {
		t.Fatal("health depends on trace database", health.Code, health.Body.String(), err)
	}
}

func TestTraceShutdownKeepsExecutingAndQueuedRunsUnknown(t *testing.T) {
	cfg := testConfig()
	cfg.Hermes.MaxConcurrent = 1
	d := newTestDispatcher(t, cfg)
	exited := make(chan struct{})
	d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
		<-ctx.Done()
		close(exited)
		return HermesResult{}, ctx.Err()
	}
	for _, group := range []string{groupA, groupB} {
		if err := d.handleIncomingMessage(testMessage(group, "shutdown-input"), ""); err != nil {
			t.Fatal(err)
		}
	}
	awaitCondition(t, func() bool {
		snapshot, err := d.trace.Snapshot()
		if err != nil || len(snapshot.Runs) != 2 {
			return false
		}
		queued, executing := false, false
		for _, run := range snapshot.Runs {
			queued = queued || run.Phase == contracts.PhaseQueued
			executing = executing || len(run.Attempts) == 1
		}
		return queued && executing
	})
	d.Close()
	select {
	case <-exited:
	default:
		t.Fatal("trace closed before backend exited")
	}
	if _, err := d.trace.Snapshot(); !errors.Is(err, observability.ErrClosed) {
		t.Fatal("trace not closed", err)
	}
	if d.traceFailed.Load() {
		t.Fatal("shutdown closed trace before final worker evidence")
	}
	reopened, err := NewDispatcher(d.cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	snapshot, err := reopened.trace.Snapshot()
	if err != nil || len(snapshot.Runs) != 2 {
		t.Fatal(snapshot, err)
	}
	for _, run := range snapshot.Runs {
		if run.Phase != contracts.PhaseUnknown || run.Delivery != contracts.DeliveryNotStarted || run.ReasonCode != "restart_recovery" || run.Completeness != contracts.Partial {
			t.Fatal(run)
		}
	}
	reopened.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		t.Error("shutdown run regenerated")
		return HermesResult{}, nil
	}
	for _, group := range []string{groupA, groupB} {
		reopened.startBatch(group, reopened.state.Groups[group].TimerGeneration, "batch_size")
	}
	if !idle(reopened) {
		t.Fatal("historical queue resumed")
	}
}

func TestTraceHTTPBoundedLongPoll(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	snapshot, err := d.trace.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	response := traceRequest(d, "/api/trace/events?wait=1&cursor="+url.QueryEscape(snapshot.Cursor))
	var page observability.EventPage
	if err := json.Unmarshal(response.Body.Bytes(), &page); err != nil || response.Code != 200 || len(page.Events) != 0 || page.Cursor != snapshot.Cursor || page.HasMore {
		t.Fatal(response.Code, response.Body.String(), err)
	}
	if elapsed := time.Since(started); elapsed < 19*time.Second || elapsed > 22*time.Second {
		t.Fatal("unexpected longpoll bound", elapsed)
	}
}

func TestTraceQueueTimeoutDoesNotGenerate(t *testing.T) {
	cfg := testConfig()
	cfg.Hermes.TimeoutSec, cfg.Hermes.MaxConcurrent = 1, 1
	d := newTestDispatcher(t, cfg)
	d.hermesSlots <- struct{}{}
	defer func() { <-d.hermesSlots }()
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		t.Error("expired queued batch generated")
		return HermesResult{}, nil
	}
	done := make(chan bool, 1)
	go func() { done <- d.processBatch(traceBatch(d, "queue-timeout")) }()
	select {
	case completed := <-done:
		if !completed {
			t.Fatal("queue timeout failed")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queue ignored batch deadline")
	}
	run := onlyTraceRun(t, d)
	if run.Outcome != contracts.OutcomeExpired || run.ReasonCode != "queue_timeout" || !run.DeliveryEvidence.NoSubmission || len(run.Attempts) != 0 {
		t.Fatal(run)
	}
}
