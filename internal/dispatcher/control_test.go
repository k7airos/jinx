package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/contracts"
)

func readControls(t *testing.T, d *Dispatcher) ControlSnapshot {
	t.Helper()
	response := traceRequest(d, "/api/control/snapshot")
	var snapshot ControlSnapshot
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil || response.Code != http.StatusOK {
		t.Fatal(response.Code, response.Body.String(), err)
	}
	return snapshot
}

func controlRequest(d *Dispatcher, request ControlRequest) *httptest.ResponseRecorder {
	body, _ := json.Marshal(request)
	r := httptest.NewRequest(http.MethodPost, "http://localhost/api/control/action", strings.NewReader(string(body)))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	d.Handler().ServeHTTP(w, r)
	return w
}

func controlAction(t *testing.T, d *Dispatcher, id, revision string, action ControlAction) ControlOperation {
	t.Helper()
	if revision == "" {
		revision = readControls(t, d).Revision
	}
	response := controlRequest(d, ControlRequest{RequestID: id, ExpectedRevision: revision, Action: action})
	var operation ControlOperation
	if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil || (response.Code != http.StatusOK && response.Code != http.StatusNotImplemented) || (operation.Status != "succeeded" && operation.Status != "delivery_blocked") {
		t.Fatal(response.Code, response.Body.String(), err)
	}
	return operation
}

func TestControlPauseResumeCompositionAndRestart(t *testing.T) {
	cfg := testConfig()
	cfg.Rules.BatchSize = 50
	d := newTestDispatcher(t, cfg)
	if err := d.handleIncomingMessage(testMessage(groupA, "pending-before-pause"), "incoming:1"); err != nil {
		t.Fatal(err)
	}
	global := ControlScope{Kind: "global"}
	group := ControlScope{Kind: "group", GroupID: groupA}
	controlAction(t, d, "pause-global", "", ControlAction{Kind: "pause", Scope: global})
	run := onlyTraceRun(t, d)
	if run.Outcome != contracts.OutcomeSuppressed || run.ReasonCode != "paused_boundary" || !run.DeliveryEvidence.NoSubmission {
		t.Fatal(run)
	}
	controlAction(t, d, "pause-group", "", ControlAction{Kind: "pause", Scope: group})
	controlAction(t, d, "resume-global", "", ControlAction{Kind: "resume", Scope: global})
	for index := 0; index < 205; index++ {
		if err := d.handleIncomingMessage(testMessage(groupA, fmt.Sprint("paused-", index)), ""); err != nil {
			t.Fatal(err)
		}
	}
	d.mu.Lock()
	if len(d.state.Groups[groupA].Pending) != 0 || d.state.Stats.TotalReceived != 206 {
		t.Fatal(d.state.Stats)
	}
	d.mu.Unlock()
	d.Close()
	reopened, err := NewDispatcher(d.cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.hermesRunner = func(context.Context, batch) (HermesResult, error) { return HermesResult{Text: "NONE"}, nil }
	if snapshot := readControls(t, reopened); snapshot.Global.Paused || !snapshot.Groups[groupA].Paused || len(snapshot.Groups) != 2 {
		t.Fatal(snapshot)
	}
	controlAction(t, reopened, "resume-group", "", ControlAction{Kind: "resume", Scope: group})
	if !idle(reopened) {
		t.Fatal("resume replayed paused messages")
	}
	if err := reopened.handleIncomingMessage(testMessage(groupA, "after-resume"), ""); err != nil {
		t.Fatal(err)
	}
	reopened.mu.Lock()
	if pending := reopened.state.Groups[groupA].Pending; len(pending) != 1 || pending[0].ID != "after-resume" {
		t.Fatal(pending)
	}
	reopened.mu.Unlock()
}

func TestControlPauseAndCancelBeforeSubmission(t *testing.T) {
	for _, kind := range []string{"pause", "cancel_run"} {
		t.Run(kind, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var posts atomic.Int32
			bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					close(entered)
					<-release
					fmt.Fprint(w, `{"protocol_version":2,"group_routing":true,"serial_ui":true,"accessibility_trusted":true,"screen_capture_authorized":true}`)
					return
				}
				posts.Add(1)
				fmt.Fprint(w, "OK")
			}))
			defer bridge.Close()
			cfg := testConfig()
			cfg.Bridge.URL = bridge.URL
			d := newTestDispatcher(t, cfg)
			work := traceBatch(d, "before-submission")
			done := make(chan error, 1)
			go func() { done <- d.deliver(context.Background(), work, "reply") }()
			<-entered
			action := ControlAction{Kind: kind, Scope: ControlScope{Kind: "global"}}
			revision := ""
			if kind == "cancel_run" {
				run := onlyTraceRun(t, d)
				action.RunID, action.CancelMode, revision = run.ID, "block_delivery", run.Revision
			}
			controlAction(t, d, "before-submit-"+kind, revision, action)
			close(release)
			if err := <-done; err == nil || posts.Load() != 0 {
				t.Fatal("control failed to block bridge POST", err, posts.Load())
			}
			run := onlyTraceRun(t, d)
			if !run.DeliveryEvidence.NoSubmission || run.DeliveryEvidence.SubmissionStartedAt != nil || d.traceFailed.Load() {
				t.Fatal(run)
			}
		})
	}
}

func TestControlPauseQueuedWorkNeverStarts(t *testing.T) {
	cfg := testConfig()
	cfg.Hermes.MaxConcurrent = 1
	d := newTestDispatcher(t, cfg)
	d.hermesSlots <- struct{}{}
	var calls atomic.Int32
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		calls.Add(1)
		return HermesResult{Text: "NONE"}, nil
	}
	if err := d.handleIncomingMessage(testMessage(groupA, "pause-queued-input"), ""); err != nil {
		t.Fatal(err)
	}
	awaitCondition(t, func() bool {
		snapshot, _ := d.trace.Snapshot()
		return len(snapshot.Runs) == 1 && snapshot.Runs[0].Phase == contracts.PhaseQueued
	})
	controlAction(t, d, "pause-queued", "", ControlAction{Kind: "pause", Scope: ControlScope{Kind: "group", GroupID: groupA}})
	controlAction(t, d, "resume-queued", "", ControlAction{Kind: "resume", Scope: ControlScope{Kind: "group", GroupID: groupA}})
	<-d.hermesSlots
	awaitCondition(t, func() bool { return idle(d) })
	if calls.Load() != 0 || onlyTraceRun(t, d).ReasonCode != "paused_boundary" {
		t.Fatal("resume resurrected suppressed queued execution")
	}
}

func TestControlSubmittedCannotCancelPauseStillConfirms(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var d *Dispatcher
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		echo := testMessage(groupA, "paused-confirmation")
		echo.IsEcho, echo.Content = true, "reply"
		echo.Timestamp++
		if err := d.handleIncomingMessage(echo, "echo:1"); err != nil {
			t.Error(err)
		}
		fmt.Fprint(w, "OK")
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	d = newTestDispatcher(t, cfg)
	done := make(chan error, 1)
	go func() { done <- d.deliver(context.Background(), traceBatch(d, "already-submitting"), "reply") }()
	<-entered
	run := onlyTraceRun(t, d)
	request := ControlRequest{RequestID: "cannot-cancel", ExpectedRevision: run.Revision, Action: ControlAction{Kind: "cancel_run", Scope: ControlScope{Kind: "global"}, RunID: run.ID, CancelMode: "stop_execution"}}
	if response := controlRequest(d, request); response.Code != http.StatusConflict {
		t.Fatal(response.Code, response.Body.String())
	}
	controlAction(t, d, "pause-submitted", "", ControlAction{Kind: "pause", Scope: ControlScope{Kind: "global"}})
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if run := onlyTraceRun(t, d); run.Delivery != contracts.DeliveryConfirmed {
		t.Fatal(run)
	}
}

func TestControlDrainPendingIsolationReadyAndRecovery(t *testing.T) {
	cfg := testConfig()
	cfg.Rules.BatchSize = 50
	d := newTestDispatcher(t, cfg)
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) { return HermesResult{Text: "NONE"}, nil }
	if err := d.handleIncomingMessage(testMessage(groupA, "accepted-pending"), "before-drain"); err != nil {
		t.Fatal(err)
	}
	scope := ControlScope{Kind: "global"}
	operation := controlAction(t, d, "drain", "", ControlAction{Kind: "drain", Scope: scope})
	if snapshot := readControls(t, d); snapshot.Maintenance.Stage != "draining" || snapshot.Maintenance.Counts.Pending != 1 {
		t.Fatal(snapshot)
	}
	for index := 0; index < 3; index++ {
		if err := d.handleIncomingMessage(testMessage(groupB, fmt.Sprint("draining-", index)), "during-drain"); err != nil {
			t.Fatal(err)
		}
	}
	d.mu.Lock()
	generation := d.state.Groups[groupA].TimerGeneration
	if len(d.state.Groups[groupB].Pending) != 0 {
		t.Fatal("new group entered drain tasks")
	}
	d.mu.Unlock()
	d.startBatch(groupA, generation, "batch_size")
	awaitCondition(t, func() bool { return idle(d) })
	if snapshot := readControls(t, d); snapshot.Maintenance.Stage != "ready" || snapshot.Maintenance.Counts.Phases["terminal"] != 1 {
		t.Fatal(snapshot)
	}
	d.Close()
	reopened, err := NewDispatcher(d.cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	snapshot := readControls(t, reopened)
	if snapshot.Maintenance.Stage != "blocked" || !snapshot.Maintenance.RecoveryRequired || snapshot.Maintenance.ID != operation.MaintenanceID {
		t.Fatal(snapshot)
	}
	controlAction(t, reopened, "continue", "", ControlAction{Kind: "continue_maintenance", Scope: scope, MaintenanceID: operation.MaintenanceID})
	if snapshot := readControls(t, reopened); snapshot.Maintenance.Stage != "ready" {
		t.Fatal(snapshot)
	}
	controlAction(t, reopened, "pause-after-drain", "", ControlAction{Kind: "pause", Scope: scope})
	if snapshot := readControls(t, reopened); snapshot.Maintenance.Stage != "blocked_by_pause" {
		t.Fatal(snapshot)
	}
	controlAction(t, reopened, "cancel-maintenance", "", ControlAction{Kind: "cancel_maintenance", Scope: scope, MaintenanceID: operation.MaintenanceID})
	if snapshot := readControls(t, reopened); snapshot.Maintenance.Stage != "canceled" || !snapshot.Global.Paused {
		t.Fatal(snapshot)
	}
}

func TestControlDrainUnknownDeliveryBlocks(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	confirmation := prepareConfirmation(t, d, "unknown-blocker", "reply", time.Now())
	d.mu.Lock()
	_ = d.confirmOrUnknownLocked(confirmation, "echo_timeout")
	d.mu.Unlock()
	controlAction(t, d, "drain-unknown", "", ControlAction{Kind: "drain", Scope: ControlScope{Kind: "global"}})
	snapshot := readControls(t, d)
	if snapshot.Maintenance.Stage != "blocked" || snapshot.Maintenance.Counts.UnknownDelivery != 1 || snapshot.Maintenance.Counts.UnresolvedRuns != 1 {
		t.Fatal(snapshot)
	}
}

func TestControlCancelMaintenanceDoesNotCancelRun(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	entered, release := make(chan struct{}), make(chan struct{})
	var canceled atomic.Bool
	d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
		close(entered)
		select {
		case <-ctx.Done():
			canceled.Store(true)
			return HermesResult{}, ctx.Err()
		case <-release:
			return HermesResult{Text: "NONE"}, nil
		}
	}
	if err := d.handleIncomingMessage(testMessage(groupA, "running-maintenance"), ""); err != nil {
		t.Fatal(err)
	}
	<-entered
	scope := ControlScope{Kind: "global"}
	operation := controlAction(t, d, "drain-active", "", ControlAction{Kind: "drain", Scope: scope})
	controlAction(t, d, "cancel-active-maintenance", "", ControlAction{Kind: "cancel_maintenance", Scope: scope, MaintenanceID: operation.MaintenanceID})
	close(release)
	awaitCondition(t, func() bool { return idle(d) })
	if canceled.Load() || onlyTraceRun(t, d).Outcome != contracts.OutcomeSilent {
		t.Fatal("cancel maintenance canceled existing execution")
	}
}

func TestControlRestartHoldsUnstartedDrainPendingUntilContinue(t *testing.T) {
	cfg := testConfig()
	cfg.Rules.BatchSize = 50
	d := newTestDispatcher(t, cfg)
	if err := d.handleIncomingMessage(testMessage(groupA, "durable-pending"), ""); err != nil {
		t.Fatal(err)
	}
	scope := ControlScope{Kind: "global"}
	operation := controlAction(t, d, "drain-pending-restart", "", ControlAction{Kind: "drain", Scope: scope})
	d.Close()
	reopened, err := NewDispatcher(d.cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.hermesRunner = func(context.Context, batch) (HermesResult, error) { return HermesResult{Text: "NONE"}, nil }
	reopened.mu.Lock()
	generation := reopened.state.Groups[groupA].TimerGeneration
	reopened.mu.Unlock()
	reopened.startBatch(groupA, generation, "batch_size")
	if snapshot := readControls(t, reopened); snapshot.Maintenance.Counts.Pending != 1 || snapshot.Maintenance.Stage != "blocked" {
		t.Fatal(snapshot)
	}
	controlAction(t, reopened, "continue-pending", "", ControlAction{Kind: "continue_maintenance", Scope: scope, MaintenanceID: operation.MaintenanceID})
	reopened.mu.Lock()
	generation = reopened.state.Groups[groupA].TimerGeneration
	reopened.mu.Unlock()
	reopened.startBatch(groupA, generation, "batch_size")
	awaitCondition(t, func() bool { return idle(reopened) })
	if snapshot := readControls(t, reopened); snapshot.Maintenance.Stage != "ready" {
		t.Fatal(snapshot)
	}
}

type revisionChangingTrace struct {
	traceStore
	reads int
}

func (store *revisionChangingTrace) GetRun(id string) (contracts.Run, error) {
	run, err := store.traceStore.GetRun(id)
	store.reads++
	if err == nil && store.reads == 2 {
		run.UpdatedAt = time.Now().UTC()
		run.LastProgressAt = run.UpdatedAt
		return store.traceStore.UpdateRun(run, run.Revision, "concurrent_progress")
	}
	return run, err
}

func TestControlCancelRechecksRunRevision(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	work := traceBatch(d, "revision-race")
	if err := d.ensureTrace(&work); err != nil {
		t.Fatal(err)
	}
	run := onlyTraceRun(t, d)
	d.trace = &revisionChangingTrace{traceStore: d.trace}
	request := ControlRequest{RequestID: "cancel-revision-race", ExpectedRevision: run.Revision, Action: ControlAction{Kind: "cancel_run", Scope: ControlScope{Kind: "global"}, RunID: run.ID, CancelMode: "block_delivery"}}
	if response := controlRequest(d, request); response.Code != http.StatusConflict {
		t.Fatal(response.Code, response.Body.String())
	}
	d.mu.Lock()
	if len(d.state.Control.BlockedRuns) != 0 || len(d.state.Control.Operations) != 0 || d.traceFailed.Load() {
		t.Fatal("revision conflict mutated controls")
	}
	d.mu.Unlock()
}

func TestControlRecoversDurablePauseBeforeTerminalWrite(t *testing.T) {
	cfg := testConfig()
	cfg.Rules.BatchSize = 50
	d := newTestDispatcher(t, cfg)
	if err := d.handleIncomingMessage(testMessage(groupA, "pause-crash-pending"), ""); err != nil {
		t.Fatal(err)
	}
	d.mu.Lock()
	d.state.Control.Global = ScopeControl{Paused: true, Revision: "2", Boundary: d.intakeSequence.Load(), ChangedAt: time.Now()}
	d.state.Control.Revision = "2"
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
	if run := onlyTraceRun(t, reopened); run.Outcome != contracts.OutcomeSuppressed || run.ReasonCode != "paused_boundary" || !run.DeliveryEvidence.NoSubmission {
		t.Fatal(run)
	}
	if !idle(reopened) {
		t.Fatal("paused pending survived restart")
	}
}

func TestControlCancelQueuedAndRunningModes(t *testing.T) {
	for _, mode := range []string{"queued", "block_delivery", "stop_execution"} {
		t.Run(mode, func(t *testing.T) {
			d := newTestDispatcher(t, testConfig())
			entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
			d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
				close(entered)
				if mode == "stop_execution" {
					<-ctx.Done()
					close(stopped)
					return HermesResult{}, ctx.Err()
				}
				<-release
				return HermesResult{Text: "reply"}, nil
			}
			if mode == "queued" {
				for index := 0; index < cap(d.hermesSlots); index++ {
					d.hermesSlots <- struct{}{}
				}
			}
			if err := d.handleIncomingMessage(testMessage(groupA, "cancel-input"), ""); err != nil {
				t.Fatal(err)
			}
			if mode == "queued" {
				awaitCondition(t, func() bool {
					snapshot, _ := d.trace.Snapshot()
					return len(snapshot.Runs) == 1 && snapshot.Runs[0].Phase == contracts.PhaseQueued
				})
			} else {
				<-entered
			}
			run := onlyTraceRun(t, d)
			cancelMode := mode
			if mode == "queued" {
				cancelMode = "stop_execution"
			}
			operation := controlAction(t, d, "cancel-"+mode, run.Revision, ControlAction{Kind: "cancel_run", Scope: ControlScope{Kind: "global"}, RunID: run.ID, CancelMode: cancelMode})
			if mode == "stop_execution" && (operation.HTTPStatus != http.StatusNotImplemented || operation.Status != "delivery_blocked" || operation.Code != "remote_cancel_unsupported") {
				t.Fatal(operation)
			}
			if mode == "stop_execution" {
				repeated := controlRequest(d, ControlRequest{RequestID: "cancel-" + mode, ExpectedRevision: run.Revision, Action: ControlAction{Kind: "cancel_run", Scope: ControlScope{Kind: "global"}, RunID: run.ID, CancelMode: cancelMode}})
				if repeated.Code != http.StatusNotImplemented {
					t.Fatal("unsupported remote cancel retry lost durable response", repeated.Code)
				}
			}
			if mode == "queued" {
				if operation.RemoteExecution != "stopped" {
					t.Fatal(operation)
				}
				for index := 0; index < cap(d.hermesSlots); index++ {
					<-d.hermesSlots
				}
			} else if operation.RemoteExecution != "may_be_running" {
				t.Fatal(operation)
			}
			if mode == "block_delivery" {
				select {
				case <-stopped:
					t.Fatal("block_delivery canceled execution")
				default:
				}
				close(release)
			}
			awaitCondition(t, func() bool { return idle(d) })
			if run := onlyTraceRun(t, d); run.Outcome != contracts.OutcomeCanceled || !run.DeliveryEvidence.NoSubmission || d.traceFailed.Load() {
				t.Fatal(run)
			}
			if mode == "queued" {
				select {
				case <-entered:
					t.Fatal("queued canceled task executed")
				default:
				}
			}
			controlAction(t, d, "drain-"+mode, "", ControlAction{Kind: "drain", Scope: ControlScope{Kind: "global"}})
			snapshot := readControls(t, d)
			if mode == "stop_execution" && (snapshot.Maintenance.Stage != "blocked" || snapshot.Maintenance.Counts.RemoteUnsettled == 0) {
				t.Fatal("unsupported remote cancellation claimed stopped", snapshot)
			}
		})
	}
}

func TestControlPersistentIdempotencyValidationAndReadPurity(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	snapshot := readControls(t, d)
	request := ControlRequest{RequestID: "idempotent", ExpectedRevision: snapshot.Revision, Action: ControlAction{Kind: "pause", Scope: ControlScope{Kind: "global"}}}
	first := controlRequest(d, request)
	if first.Code != http.StatusOK {
		t.Fatal(first.Code, first.Body.String())
	}
	d.Close()
	reopened, err := NewDispatcher(d.cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	second := controlRequest(reopened, request)
	if second.Code != http.StatusOK || first.Body.String() != second.Body.String() {
		t.Fatal(second.Code, second.Body.String())
	}
	var operation ControlOperation
	_ = json.Unmarshal(first.Body.Bytes(), &operation)
	before, _ := os.ReadFile(reopened.stateFile)
	if response := traceRequest(reopened, "/api/control/operations/"+operation.OperationID); response.Code != http.StatusOK {
		t.Fatal(response.Code)
	}
	readControls(t, reopened)
	after, _ := os.ReadFile(reopened.stateFile)
	if string(before) != string(after) {
		t.Fatal("GET mutated persistent state")
	}
	request.Action.Kind = "resume"
	if response := controlRequest(reopened, request); response.Code != http.StatusConflict {
		t.Fatal(response.Code)
	}
	request.RequestID = "stale"
	if response := controlRequest(reopened, request); response.Code != http.StatusConflict {
		t.Fatal(response.Code)
	}
	request.Action.CancelMode = "unsupported"
	if response := controlRequest(reopened, request); response.Code != http.StatusBadRequest {
		t.Fatal(response.Code)
	}
	for _, origin := range []string{"https://localhost", "http://evil.example"} {
		r := httptest.NewRequest(http.MethodPost, "http://localhost/api/control/action", strings.NewReader(`{}`))
		r.Header.Set("Origin", origin)
		w := httptest.NewRecorder()
		reopened.Handler().ServeHTTP(w, r)
		if w.Code != http.StatusForbidden {
			t.Fatal(w.Code)
		}
	}
}

func TestImmutableConfigSnapshotAndPublicRunner(t *testing.T) {
	cfg := testConfig()
	rule := cfg.Rules.GroupRules[groupA]
	rule.DecisionContext, rule.SafeCriteria = "user gate text", "user safety text"
	cfg.Rules.GroupRules[groupA] = rule
	cfg.Hermes.ProfilesDir = t.TempDir()
	data, _ := json.Marshal(cfg)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	requests := make(chan BackendRequest, 1)
	d, err := NewDispatcherWithOptions(path, Options{Runner: RunFunc(func(ctx context.Context, request BackendRequest) (Result, error) {
		requests <- request
		return Result{Text: "NONE", Backend: "Alma", RunID: "synthetic-run"}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	if err := d.handleIncomingMessage(testMessage(groupA, "runner-input"), ""); err != nil {
		t.Fatal(err)
	}
	awaitCondition(t, func() bool { return idle(d) })
	request := <-requests
	run := onlyTraceRun(t, d)
	if request.TraceID != run.ID || request.ConfigSnapshotID != run.ConfigSnapshotID || request.ConfigSnapshot == nil || request.Prompt == "" || request.Profile == "" || request.GroupID != groupA || len(request.Messages) != 1 {
		t.Fatal("public runner request incomplete")
	}
	response := traceRequest(d, "/api/trace/config-snapshots/"+run.ConfigSnapshotID)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "user gate text") || !strings.Contains(response.Body.String(), "user safety text") {
		t.Fatal(response.Code, response.Body.String())
	}
	for _, secret := range []string{cfg.Hermes.ProfilesDir, cfg.Hermes.BinPath, "background-a", "What changed?", "profiles_dir", "bin_path", "prompt", "secret"} {
		if strings.Contains(response.Body.String(), secret) {
			t.Fatal("snapshot leaked", secret)
		}
	}
	old := response.Body.String()
	current := d.configSnapshot()
	encoded, _ := json.Marshal(current)
	var changed Config
	_ = json.Unmarshal(encoded, &changed)
	changedRule := changed.Rules.GroupRules[groupA]
	changedRule.DecisionContext = "changed gate"
	changed.Rules.GroupRules[groupA] = changedRule
	encoded, _ = json.Marshal(changed)
	if result := configRequest(d, http.MethodPut, string(encoded), policyID(current)); result.Code != http.StatusOK {
		t.Fatal(result.Code, result.Body.String())
	}
	if response := traceRequest(d, "/api/trace/config-snapshots/"+run.ConfigSnapshotID); response.Body.String() != old {
		t.Fatal("snapshot changed with current config")
	}
	if response := traceRequest(d, "/api/trace/config-snapshots/missing"); response.Code != http.StatusNotFound {
		t.Fatal(response.Code)
	}
}
