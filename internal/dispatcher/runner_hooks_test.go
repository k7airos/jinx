package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/execution"
)

type fakeRunnerWithGate struct {
	run       RunFunc
	gate      func(context.Context, string, string, func(context.Context) error) error
	stop      func(context.Context, string) (execution.CancelResult, bool, error)
	mu        sync.Mutex
	metadata  map[string]execution.Result
	forgotten map[string]int
	calls     atomic.Int32
	gateCalls atomic.Int32
	stopCalls atomic.Int32
}

func (runner *fakeRunnerWithGate) Run(ctx context.Context, request BackendRequest) (Result, error) {
	runner.calls.Add(1)
	return runner.run(ctx, request)
}

func (runner *fakeRunnerWithGate) Meta(traceID string) (execution.Result, bool) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	result, exists := runner.metadata[traceID]
	result.Attempts = append([]execution.Attempt(nil), result.Attempts...)
	return result, exists
}

func (runner *fakeRunnerWithGate) retain(traceID string, result execution.Result) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.metadata == nil {
		runner.metadata = make(map[string]execution.Result)
	}
	runner.metadata[traceID] = result
}

func (runner *fakeRunnerWithGate) Forget(traceID string) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	delete(runner.metadata, traceID)
	if runner.forgotten == nil {
		runner.forgotten = make(map[string]int)
	}
	runner.forgotten[traceID]++
}

func (runner *fakeRunnerWithGate) DeliveryGate(ctx context.Context, traceID, groupID string, admit func(context.Context) error) error {
	runner.gateCalls.Add(1)
	return runner.gate(ctx, traceID, groupID, admit)
}

func (runner *fakeRunnerWithGate) StopExecution(ctx context.Context, traceID string) (execution.CancelResult, bool, error) {
	runner.stopCalls.Add(1)
	return runner.stop(ctx, traceID)
}

func newRunnerDispatcher(t *testing.T, cfg Config, runner Runner) *Dispatcher {
	t.Helper()
	cfg.Hermes.ProfilesDir = t.TempDir()
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	d, err := NewDispatcherWithOptions(path, Options{Runner: runner})
	if err != nil {
		t.Fatal(err)
	}
	d.sendCooldown, d.echoTimeout = time.Millisecond, time.Millisecond
	t.Cleanup(d.Close)
	return d
}

func TestRunnerDeliveryGateRejectsBeforeAdmission(t *testing.T) {
	for _, mode := range []string{"deny", "changed", "expanded", "no_callback", "validation"} {
		t.Run(mode, func(t *testing.T) {
			var posts atomic.Int32
			bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
				posts.Add(1)
				fmt.Fprint(w, "OK")
			})
			cfg := testConfig()
			cfg.Bridge.URL = bridge.URL
			entered, release := make(chan struct{}), make(chan struct{})
			var policyRevision atomic.Int32
			policyRevision.Store(1)
			var d *Dispatcher
			runner := &fakeRunnerWithGate{run: func(context.Context, BackendRequest) (Result, error) {
				return Result{Text: "reply"}, nil
			}}
			runner.gate = func(ctx context.Context, traceID, groupID string, admit func(context.Context) error) error {
				if groupID != groupA {
					t.Error("delivery gate must receive the source group")
				}
				if d.submissionMu.TryLock() {
					d.submissionMu.Unlock()
					t.Error("delivery gate must run under submission boundary for the source group")
				}
				if mode == "changed" || mode == "expanded" {
					close(entered)
					<-release
				}
				run := onlyTraceRun(t, d)
				if run.DeliveryEvidence.SubmissionStartedAt != nil || run.Phase != contracts.PhaseChecking || run.Action != nil {
					t.Error("admission preceded gate", run)
				}
				if mode == "no_callback" {
					return nil
				}
				if mode == "validation" {
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					return admit(canceled)
				}
				if mode == "deny" || policyRevision.Load() != 1 {
					return &execution.Error{Code: "policy_blocked", Field: "delivery_revision_or_permission"}
				}
				return admit(ctx)
			}
			d = newRunnerDispatcher(t, cfg, runner)
			done := make(chan bool, 1)
			go func() { done <- d.processBatch(traceBatch(d, "gate-"+mode)) }()
			if mode == "changed" || mode == "expanded" {
				<-entered
				policyRevision.Store(2)
				close(release)
			}
			if !<-done {
				t.Fatal("batch failed")
			}
			run := onlyTraceRun(t, d)
			if posts.Load() != 0 || run.DeliveryEvidence.SubmissionStartedAt != nil || !run.DeliveryEvidence.NoSubmission || run.Action != nil {
				t.Fatal("rejected gate crossed submission boundary", run, posts.Load())
			}
			if mode != "validation" && (run.Outcome != contracts.OutcomeSuppressed || run.ReasonCode != "permission_changed") {
				t.Fatal(run)
			}
			if runner.calls.Load() != 1 || runner.gateCalls.Load() != 1 || runner.forgotten[run.ID] != 1 {
				t.Fatal("runner invoked repeatedly or retained", runner.calls.Load(), runner.gateCalls.Load(), runner.forgotten)
			}
		})
	}
}

func TestRunnerDeliveryGateReleasesBeforeHTTPAndEcho(t *testing.T) {
	var d *Dispatcher
	var executionLock sync.Mutex
	var posts atomic.Int32
	gateReturned := make(chan struct{})
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		posts.Add(1)
		select {
		case <-gateReturned:
		default:
			t.Error("POST preceded gate return")
		}
		if !executionLock.TryLock() {
			t.Error("execution gate held across HTTP")
		} else {
			executionLock.Unlock()
		}
		if !d.submissionMu.TryLock() {
			t.Error("submission boundary held across HTTP")
		} else {
			d.submissionMu.Unlock()
		}
		var body struct{ Text string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		confirmation := onlyTraceRun(t, d).DeliveryEvidence.SubmissionStartedAt
		for time.Now().UnixMilli() <= confirmation.UnixMilli() {
			time.Sleep(time.Millisecond)
		}
		echo := testMessage(groupA, "gate-echo")
		echo.IsEcho, echo.Content = true, body.Text
		if err := d.handleIncomingMessage(echo, ""); err != nil {
			t.Error(err)
		}
		fmt.Fprint(w, "OK")
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	runner := &fakeRunnerWithGate{run: func(context.Context, BackendRequest) (Result, error) { return Result{Text: "reply"}, nil }}
	runner.gate = func(ctx context.Context, traceID, groupID string, admit func(context.Context) error) error {
		executionLock.Lock()
		defer executionLock.Unlock()
		defer close(gateReturned)
		return admit(ctx)
	}
	d = newRunnerDispatcher(t, cfg, runner)
	if !d.processBatch(traceBatch(d, "gate-accepted")) {
		t.Fatal("batch failed")
	}
	if run := onlyTraceRun(t, d); run.Delivery != contracts.DeliveryConfirmed || posts.Load() != 1 {
		t.Fatal(run, posts.Load())
	}
}

func TestRunnerForgetPreservesLateConfirmation(t *testing.T) {
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "OK") })
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	runner := &fakeRunnerWithGate{run: func(context.Context, BackendRequest) (Result, error) { return Result{Text: "reply"}, nil }}
	runner.gate = func(ctx context.Context, traceID, groupID string, admit func(context.Context) error) error {
		return admit(ctx)
	}
	d := newRunnerDispatcher(t, cfg, runner)
	if !d.processBatch(traceBatch(d, "late-runner-echo")) {
		t.Fatal("batch failed")
	}
	run := onlyTraceRun(t, d)
	if run.Delivery != contracts.DeliveryUnknown || runner.forgotten[run.ID] != 1 {
		t.Fatal("unknown delivery retained Runner state", run, runner.forgotten)
	}
	for time.Now().UnixMilli() <= run.DeliveryEvidence.SubmissionStartedAt.UnixMilli() {
		time.Sleep(time.Millisecond)
	}
	echo := testMessage(groupA, "late-runner-confirmation")
	echo.IsEcho, echo.Content = true, "reply"
	if err := d.handleIncomingMessage(echo, "late:echo"); err != nil {
		t.Fatal(err)
	}
	if run := onlyTraceRun(t, d); run.Delivery != contracts.DeliveryConfirmed {
		t.Fatal("late confirmation depended on Runner metadata", run)
	}
}

func TestRunnerMetadataAndForgetTerminalPaths(t *testing.T) {
	for _, mode := range []string{"none", "error", "permission", "dryrun", "invalid", "blocked", "other", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			cfg := testConfig()
			cfg.Bridge.DryRun = mode == "dryrun"
			runner := &fakeRunnerWithGate{}
			runner.run = func(ctx context.Context, request BackendRequest) (Result, error) {
				started := time.Now().UTC()
				ended := time.Now().UTC()
				runner.retain(request.TraceID, execution.Result{Text: "PRIVATE_META_TEXT", Backend: "text-final", Model: "model-final", RoutingRevision: "route-v7", PolicyRevision: "policy-v9", Attempts: []execution.Attempt{
					{BackendID: "personal-primary", ModelID: "model-primary", ExternalID: "external-primary", Status: "timed_out", RemoteExecution: "may_be_running", StartedAt: started, EndedAt: ended, PolicyRevision: "policy-v8"},
					{BackendID: "text-final", ModelID: "model-final", Status: "succeeded", RemoteExecution: "not_applicable", StartedAt: ended, EndedAt: ended, PolicyRevision: "policy-v9", FallbackReason: "timeout"},
				}})
				if mode == "error" {
					return Result{}, errors.New("PRIVATE_ERROR")
				}
				if mode == "permission" {
					return Result{}, &execution.Error{Code: "policy_blocked", Field: "result_boundary"}
				}
				if mode == "timeout" {
					return Result{}, context.DeadlineExceeded
				}
				text := "NONE"
				if mode == "dryrun" {
					text = "reply"
				} else if mode == "invalid" {
					text = "bad\x00reply"
				}
				return Result{Text: text, Backend: "Alma", RunID: "execution-id", Fallback: true}, nil
			}
			d := newRunnerDispatcher(t, cfg, runner)
			work := traceBatch(d, "metadata-"+mode)
			if err := d.ensureTrace(&work); err != nil {
				t.Fatal(err)
			}
			original := onlyTraceRun(t, d).Versions
			if mode == "blocked" {
				controlAction(t, d, "pause", "", ControlAction{Kind: "pause", Scope: ControlScope{Kind: "global"}})
			}
			if mode == "other" {
				work.Messages[0].Content = "@SomeoneElse hello"
			}
			if !d.processBatch(work) {
				t.Fatal("batch failed")
			}
			run := onlyTraceRun(t, d)
			if run.Phase != contracts.PhaseTerminal || run.Versions != original || runner.forgotten[run.ID] != 1 || len(runner.metadata) != 0 {
				t.Fatal("run changed source version or retained result", run, runner.forgotten)
			}
			if mode != "blocked" && mode != "other" {
				if runner.calls.Load() != 1 || len(run.Attempts) != 3 {
					t.Fatal("missing actual attempts", run.Attempts)
				}
				primary, fallback := run.Attempts[1], run.Attempts[2]
				if primary.BackendID != "personal-primary" || primary.ModelLabel != "model-primary" || primary.ExternalID != "external-primary" || primary.Versions.Routing != "route-v7" || primary.Versions.Policy != "policy-v8" || primary.Status != contracts.AttemptTimedOut || primary.RemoteExecution != contracts.RemoteMayBeRunning || fallback.ModelLabel != "model-final" || fallback.Versions.Policy != "policy-v9" || fallback.FallbackReason != "timeout" || fallback.Status != contracts.AttemptSucceeded {
					t.Fatal("metadata lost", run.Attempts)
				}
			}
			if mode == "permission" && (run.Outcome != contracts.OutcomeSuppressed || run.ReasonCode != "permission_changed") {
				t.Fatal(run)
			}
			response := traceRequest(d, "/api/trace/runs/"+run.ID)
			if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "PRIVATE_") {
				t.Fatal("metadata content leaked", response.Body.String())
			}
			stored, err := os.ReadFile(filepath.Join(filepath.Dir(d.stateFile), "trace.json"))
			if err != nil || strings.Contains(string(stored), "PRIVATE_") {
				t.Fatal("metadata persisted content", err)
			}
		})
	}
}

func TestRunnerStopExecutionOutsideLocksAndDurableBarrier(t *testing.T) {
	for _, remote := range []string{"stopped", "stopped_late", "may_be_running", "unknown", "error", "unsupported"} {
		t.Run(remote, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var d *Dispatcher
			runner := &fakeRunnerWithGate{}
			runner.run = func(ctx context.Context, request BackendRequest) (Result, error) {
				started := time.Now().UTC()
				close(entered)
				<-release
				actualRemote := remote
				if actualRemote == "error" || actualRemote == "unsupported" || actualRemote == "stopped_late" {
					actualRemote = "unknown"
				}
				runner.retain(request.TraceID, execution.Result{RoutingRevision: "route-v1", PolicyRevision: "policy-v1", Attempts: []execution.Attempt{{BackendID: "personal", ModelID: "model", ExternalID: "external", Status: "canceled", RemoteExecution: actualRemote, StartedAt: started, EndedAt: time.Now().UTC(), PolicyRevision: "policy-v1"}}})
				return Result{}, ctx.Err()
			}
			runner.stop = func(ctx context.Context, traceID string) (execution.CancelResult, bool, error) {
				if !d.mu.TryLock() {
					t.Error("state lock held across StopExecution")
				} else {
					d.mu.Unlock()
				}
				if !d.submissionMu.TryLock() {
					t.Error("submission lock held across StopExecution")
				} else {
					d.submissionMu.Unlock()
				}
				if !d.traceMu.TryLock() {
					t.Error("trace lock held across StopExecution")
				} else {
					d.traceMu.Unlock()
				}
				var state persistentState
				data, err := os.ReadFile(d.stateFile)
				if err != nil || json.Unmarshal(data, &state) != nil || state.Control.BlockedRuns[traceID].Mode != "stop_execution" {
					t.Error("stop preceded durable delivery barrier", err)
				}
				result := execution.CancelResult{RemoteExecution: remote, LocalStopRequested: true, LocalStopped: true, RemoteCancelSupported: remote == "stopped"}
				if remote == "stopped_late" {
					result.RemoteExecution, result.RemoteCancelSupported = "stopped", true
				}
				if remote == "error" {
					result.RemoteExecution = "stopped"
					return result, true, errors.New("remote stop unproven")
				}
				return result, remote != "unsupported", nil
			}
			d = newRunnerDispatcher(t, testConfig(), runner)
			done := make(chan bool, 1)
			go func() { done <- d.processBatch(traceBatch(d, "stop-"+remote)) }()
			<-entered
			run := onlyTraceRun(t, d)
			request := ControlRequest{RequestID: "cancel", ExpectedRevision: run.Revision, Action: ControlAction{Kind: "cancel_run", Scope: ControlScope{Kind: "global"}, RunID: run.ID, CancelMode: "stop_execution"}}
			response := controlRequest(d, request)
			var operation ControlOperation
			if err := json.Unmarshal(response.Body.Bytes(), &operation); err != nil {
				t.Fatal(err)
			}
			if remote == "stopped" || remote == "stopped_late" {
				if response.Code != http.StatusOK || operation.RemoteExecution != "stopped" {
					t.Fatal(response.Code, operation)
				}
			} else if response.Code != http.StatusNotImplemented || operation.Status != "delivery_blocked" || operation.RemoteExecution == "stopped" {
				t.Fatal("unknown remote execution claimed stopped", response.Code, operation)
			}
			close(release)
			if !<-done {
				t.Fatal("batch failed")
			}
			run = onlyTraceRun(t, d)
			if run.Outcome != contracts.OutcomeCanceled || !run.DeliveryEvidence.NoSubmission || len(run.Attempts) != 2 || d.traceFailed.Load() {
				t.Fatal(run)
			}
			actual := run.Attempts[1]
			if remote == "stopped" || remote == "stopped_late" {
				if actual.RemoteExecution != contracts.RemoteStopped || actual.Status != contracts.AttemptCanceled {
					t.Fatal(actual)
				}
			} else if actual.RemoteExecution == contracts.RemoteStopped || actual.Status == contracts.AttemptCanceled {
				t.Fatal("local cancellation claimed remote stop", actual)
			}
			repeated := controlRequest(d, request)
			if repeated.Code != response.Code || repeated.Body.String() != response.Body.String() || runner.stopCalls.Load() != 1 {
				t.Fatal("cancel retry reran remote stop", repeated.Code, repeated.Body.String())
			}
		})
	}
}

func TestRunnerPauseDuringExecutionRetainsActualAttempts(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	runner := &fakeRunnerWithGate{}
	runner.run = func(ctx context.Context, request BackendRequest) (Result, error) {
		started := time.Now().UTC()
		close(entered)
		<-release
		runner.retain(request.TraceID, execution.Result{RoutingRevision: "routing", PolicyRevision: "policy", Attempts: []execution.Attempt{{BackendID: "text", ModelID: "actual-model", Status: "succeeded", RemoteExecution: "not_applicable", StartedAt: started, EndedAt: time.Now().UTC(), PolicyRevision: "policy"}}})
		return Result{Text: "reply"}, nil
	}
	d := newRunnerDispatcher(t, testConfig(), runner)
	done := make(chan bool, 1)
	go func() { done <- d.processBatch(traceBatch(d, "paused-execution")) }()
	<-entered
	controlAction(t, d, "pause", "", ControlAction{Kind: "pause", Scope: ControlScope{Kind: "global"}})
	close(release)
	if !<-done {
		t.Fatal("batch failed")
	}
	run := onlyTraceRun(t, d)
	if run.Outcome != contracts.OutcomeSuppressed || run.ReasonCode != "paused_boundary" || len(run.Attempts) != 2 || run.Attempts[1].ModelLabel != "actual-model" || runner.forgotten[run.ID] != 1 {
		t.Fatal(run)
	}
}

func TestRunnerRemoteStopKeepsEarlierFallbackUncertain(t *testing.T) {
	runner := &fakeRunnerWithGate{}
	runner.run = func(ctx context.Context, request BackendRequest) (Result, error) {
		started, ended := time.Now().UTC(), time.Now().UTC()
		runner.retain(request.TraceID, execution.Result{RoutingRevision: "routing", PolicyRevision: "policy", Attempts: []execution.Attempt{
			{BackendID: "primary", ModelID: "model-a", Status: "timed_out", RemoteExecution: "may_be_running", StartedAt: started, EndedAt: ended, PolicyRevision: "policy"},
			{BackendID: "fallback", ModelID: "model-b", Status: "timed_out", RemoteExecution: "may_be_running", StartedAt: ended, EndedAt: ended, PolicyRevision: "policy"},
		}})
		return Result{}, context.DeadlineExceeded
	}
	d := newRunnerDispatcher(t, testConfig(), runner)
	work := traceBatch(d, "stop-fallback")
	if err := d.ensureTrace(&work); err != nil {
		t.Fatal(err)
	}
	if err := d.tracePhase(work, contracts.PhaseQueued, "test_execution_queue"); err != nil {
		t.Fatal(err)
	}
	if err := d.beginControlledExecution(work); err != nil {
		t.Fatal(err)
	}
	result, backendErr := d.hermesRunner(context.Background(), work)
	if err := d.traceAttemptEnd(context.Background(), work, result, backendErr); err != nil {
		t.Fatal(err)
	}
	if err := d.recordExecutionStop(work.TraceID, execution.CancelResult{RemoteExecution: "stopped"}, true, nil); err != nil {
		t.Fatal(err)
	}
	run := onlyTraceRun(t, d)
	if run.Attempts[1].RemoteExecution != contracts.RemoteMayBeRunning || run.Attempts[2].RemoteExecution != contracts.RemoteStopped {
		t.Fatal("stopping current attempt rewrote earlier fallback evidence", run.Attempts)
	}
	runner.Forget(work.TraceID)
}

func TestRunnerConcurrentCancelRetryWaitsForReceipt(t *testing.T) {
	entered, release, stopEntered, stopRelease := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	runner := &fakeRunnerWithGate{run: func(ctx context.Context, request BackendRequest) (Result, error) {
		close(entered)
		<-release
		return Result{}, ctx.Err()
	}}
	runner.stop = func(context.Context, string) (execution.CancelResult, bool, error) {
		close(stopEntered)
		<-stopRelease
		return execution.CancelResult{RemoteExecution: "may_be_running"}, true, nil
	}
	d := newRunnerDispatcher(t, testConfig(), runner)
	done := make(chan bool, 1)
	go func() { done <- d.processBatch(traceBatch(d, "concurrent-cancel")) }()
	<-entered
	run := onlyTraceRun(t, d)
	request := ControlRequest{RequestID: "concurrent", ExpectedRevision: run.Revision, Action: ControlAction{Kind: "cancel_run", Scope: ControlScope{Kind: "global"}, RunID: run.ID, CancelMode: "stop_execution"}}
	responses := make(chan *httptest.ResponseRecorder, 2)
	go func() { responses <- controlRequest(d, request) }()
	<-stopEntered
	retryStarted := make(chan struct{})
	go func() {
		close(retryStarted)
		responses <- controlRequest(d, request)
	}()
	<-retryStarted
	select {
	case response := <-responses:
		t.Error("retry returned an unfinished operation", response.Body.String())
		responses <- response
	case <-time.After(10 * time.Millisecond):
	}
	close(stopRelease)
	first, second := <-responses, <-responses
	if first.Code != http.StatusNotImplemented || second.Code != first.Code || first.Body.String() != second.Body.String() || runner.stopCalls.Load() != 1 {
		t.Fatal("retry lost final receipt", first.Body.String(), second.Body.String())
	}
	close(release)
	if !<-done {
		t.Fatal("batch failed")
	}
}

func TestRunnerCapabilitiesExposeLegacySoft(t *testing.T) {
	legacy := newTestDispatcher(t, testConfig())
	capabilities := readControls(t, legacy).Capabilities
	if capabilities["deliveryGate"] || !capabilities["legacySoft"] || capabilities["remoteCancel"] {
		t.Fatal(capabilities)
	}
	structured := newRunnerDispatcher(t, testConfig(), &fakeRunnerWithGate{})
	capabilities = readControls(t, structured).Capabilities
	if !capabilities["deliveryGate"] || capabilities["legacySoft"] || !capabilities["stopExecution"] || capabilities["remoteCancel"] {
		t.Fatal(capabilities)
	}
}
