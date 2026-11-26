package supervisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeCall struct {
	executable string
	args       []string
}

type fakeRunner struct {
	mu          sync.Mutex
	services    map[ServiceID]Service
	calls       []fakeCall
	control     func(context.Context, []string) (int, error)
	printError  bool
	nextPID     int
	commandSeen chan struct{}
}

func newFakeRunner() *fakeRunner {
	runner := &fakeRunner{services: make(map[ServiceID]Service), nextPID: 500}
	for index, id := range serviceOrder {
		runner.services[id] = Service{ID: id, Registration: "registered", State: "running", PID: 100 + index}
	}
	return runner
}

func (runner *fakeRunner) Run(ctx context.Context, executable string, args ...string) (string, string, int, error) {
	runner.mu.Lock()
	runner.calls = append(runner.calls, fakeCall{executable, append([]string(nil), args...)})
	if args[0] == "print" {
		defer runner.mu.Unlock()
		if runner.printError {
			return "environment secret", "secret failure", 1, errors.New("private path")
		}
		for id, label := range labels {
			if strings.HasSuffix(args[1], "/"+label) {
				service := runner.services[id]
				if service.Registration == "unregistered" {
					return "", `Could not find service "` + label + `" in domain for user gui: 501`, 113, nil
				}
				pid := ""
				if service.PID > 0 {
					pid = fmt.Sprintf("\tpid = %d\n", service.PID)
				}
				return args[1] + " = {\n\tstate = " + service.State + "\n" + pid + "\truns = 2\n\tlast exit code = 0\n\tenvironment = {\n\t\tTOKEN = secret\n\t\tpid = 99999\n\t}\n\targuments = {\n\t\tsecret argument\n\t}\n}\n", "", 0, nil
			}
		}
		return "", "not allowlisted", 1, nil
	}
	control := runner.control
	seen := runner.commandSeen
	runner.mu.Unlock()
	if seen != nil {
		select {
		case seen <- struct{}{}:
		default:
		}
	}
	if control != nil {
		exit, err := control(ctx, args)
		return "raw secret stdout", "raw secret stderr", exit, err
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	for id, label := range labels {
		if strings.HasSuffix(args[len(args)-1], "/"+label) || strings.HasSuffix(args[len(args)-1], "/"+label+".plist") {
			service := runner.services[id]
			if args[0] == "bootout" {
				service.Registration, service.State, service.PID = "unregistered", "stopped", 0
			} else {
				runner.nextPID++
				service.Registration, service.State, service.PID = "registered", "running", runner.nextPID
			}
			runner.services[id] = service
			return "", "", 0, nil
		}
	}
	return "", "", 1, nil
}

func (runner *fakeRunner) set(id ServiceID, registration, state string, pid int) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	runner.services[id] = Service{ID: id, Registration: registration, State: state, PID: pid}
}

func (runner *fakeRunner) commands() []fakeCall {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	var result []fakeCall
	for _, call := range runner.calls {
		if call.args[0] != "print" {
			result = append(result, call)
		}
	}
	return result
}

type fakeDrain struct {
	mu                  sync.Mutex
	snapshot            ControlSnapshot
	snapshotUnavailable bool
	status              DrainStatus
	pollError           bool
	drainError          bool
	calls               []string
	polls               int
	pollHook            func()
}

func newFakeDrain() *fakeDrain {
	return &fakeDrain{snapshot: ControlSnapshot{Version: "control-1", DependencyVersion: "dependencies-1"}, status: DrainStatus{MaintenanceID: "maintenance-1", State: "ready", Global: true, Persisted: true, ConfirmedSettled: true, RemoteSettled: true}}
}

func (drain *fakeDrain) Snapshot(context.Context) (ControlSnapshot, error) {
	drain.mu.Lock()
	defer drain.mu.Unlock()
	if drain.snapshotUnavailable {
		return ControlSnapshot{}, errors.New("dispatcher offline")
	}
	return drain.snapshot, nil
}

func (drain *fakeDrain) Drain(_ context.Context, scope Scope) (string, error) {
	drain.mu.Lock()
	defer drain.mu.Unlock()
	drain.calls = append(drain.calls, "drain:"+string(scope))
	if drain.drainError {
		return "", errors.New("secret drain error")
	}
	drain.snapshot.Version = "maintenance-version"
	return drain.status.MaintenanceID, nil
}

func (drain *fakeDrain) Poll(context.Context, string) (DrainStatus, error) {
	drain.mu.Lock()
	defer drain.mu.Unlock()
	drain.polls++
	if drain.pollHook != nil {
		drain.pollHook()
	}
	if drain.pollError {
		return DrainStatus{}, errors.New("secret poll error")
	}
	return drain.status, nil
}

func (drain *fakeDrain) Cancel(context.Context, string) error {
	drain.mu.Lock()
	defer drain.mu.Unlock()
	drain.calls = append(drain.calls, "cancel")
	return nil
}

func (drain *fakeDrain) Continue(context.Context, string) error {
	drain.mu.Lock()
	defer drain.mu.Unlock()
	drain.calls = append(drain.calls, "continue")
	drain.status.State = "ready"
	return nil
}

func testOptions(t *testing.T, runner *fakeRunner, drain *fakeDrain) Options {
	t.Helper()
	uid := uint32(501)
	return Options{UID: &uid, PlistDir: "/trusted/LaunchAgents", DataPath: filepath.Join(t.TempDir(), "control", "operations.json"), EnableControl: true, Commander: runner, DrainCoordinator: drain, PlanTTL: time.Second * 3, OperationTimeout: time.Second * 2, CommandTimeout: time.Second, VerifyTimeout: time.Millisecond * 20, PollInterval: time.Millisecond}
}

func testManager(t *testing.T, options Options) *Manager {
	t.Helper()
	manager, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { manager.Close() })
	return manager
}

func makePlan(t *testing.T, manager *Manager, kind Kind, id ServiceID) ActionPlan {
	t.Helper()
	list, err := manager.ListServices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := manager.Plan(context.Background(), kind, id, list.Revision)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func execute(t *testing.T, manager *Manager, plan ActionPlan) Operation {
	t.Helper()
	if !plan.Eligible {
		t.Fatalf("ineligible plan: %+v", plan.Blockers)
	}
	operation, err := manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "request-1"})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func waitOperation(t *testing.T, manager *Manager, id string) Operation {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		operation, err := manager.GetOperation(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		manager.mu.Lock()
		working := manager.workers[id]
		manager.mu.Unlock()
		if !working {
			operation, err = manager.GetOperation(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			return operation
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("worker did not finish")
	return Operation{}
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var problem *Problem
	if !errors.As(err, &problem) || problem.Code != code {
		t.Fatalf("got %v; want %s", err, code)
	}
}

func TestControlCommandsAndReadback(t *testing.T) {
	for _, id := range serviceOrder {
		for _, scenario := range []struct {
			name       string
			kind       Kind
			registered bool
			args       []string
		}{
			{"bootstrap", Start, false, []string{"bootstrap", "gui/501", "/trusted/LaunchAgents/" + labels[id] + ".plist"}},
			{"kickstart", Start, true, []string{"kickstart", "gui/501/" + labels[id]}},
			{"bootout", Stop, true, []string{"bootout", "gui/501/" + labels[id]}},
			{"restart", Restart, true, []string{"kickstart", "-k", "gui/501/" + labels[id]}},
		} {
			t.Run(string(id)+"/"+scenario.name, func(t *testing.T) {
				runner, drain := newFakeRunner(), newFakeDrain()
				if scenario.kind == Start {
					if scenario.registered {
						runner.set(id, "registered", "waiting", 0)
					} else {
						runner.set(id, "unregistered", "stopped", 0)
					}
				}
				options := testOptions(t, runner, drain)
				manager := testManager(t, options)
				runner.mu.Lock()
				if len(runner.calls) != 0 {
					t.Fatal("constructor called launchctl")
				}
				runner.mu.Unlock()
				plan := makePlan(t, manager, scenario.kind, id)
				if len(runner.commands()) != 0 || len(drain.calls) != 0 {
					t.Fatal("plan mutated runtime")
				}
				operation := waitOperation(t, manager, execute(t, manager, plan).ID)
				if operation.State != "succeeded" || operation.Readback == nil || !achieved(plan, *operation.Readback) {
					t.Fatalf("unverified operation: %+v", operation)
				}
				calls := runner.commands()
				if len(calls) != 1 || calls[0].executable != launchctl || !reflect.DeepEqual(calls[0].args, scenario.args) {
					t.Fatalf("commands: %+v", calls)
				}
				drain.mu.Lock()
				if scenario.kind == Start && len(drain.calls) != 0 || scenario.kind != Start && (len(drain.calls) != 1 || drain.calls[0] != "drain:global" || drain.polls < 2) {
					t.Fatalf("drain: %+v, polls=%d", drain.calls, drain.polls)
				}
				drain.mu.Unlock()
				if scenario.kind != Start && (operation.Drain == nil || !operation.Drain.Persisted) {
					t.Fatal("missing persistent drain proof")
				}
				info, err := os.Stat(options.DataPath)
				if err != nil || info.Mode().Perm() != 0600 {
					t.Fatalf("journal mode: %v %v", info, err)
				}
			})
		}
	}
}

func TestReadbackMustProveStopAndRestart(t *testing.T) {
	for _, kind := range []Kind{Stop, Restart, Start} {
		t.Run(string(kind), func(t *testing.T) {
			runner, drain := newFakeRunner(), newFakeDrain()
			if kind == Start {
				runner.set(Relay, "registered", "waiting", 0)
			}
			runner.control = func(context.Context, []string) (int, error) {
				if kind == Stop {
					runner.set(Relay, "registered", "waiting", 0)
				}
				return 0, nil
			}
			manager := testManager(t, testOptions(t, runner, drain))
			operation := waitOperation(t, manager, execute(t, manager, makePlan(t, manager, kind, Relay)).ID)
			if operation.State != "unknown" || operation.Problem.Code != "readback_unknown" || len(runner.commands()) != 1 {
				t.Fatalf("accepted unverified action: %+v", operation)
			}
			if kind == Stop && operation.Readback.Registration != "registered" {
				t.Fatal("KeepAlive exit falsely became an unregister")
			}
		})
	}
}

func TestStalePlanAndWrongInstance(t *testing.T) {
	for _, scenario := range []string{"revision", "instance", "dependencies", "expiry"} {
		t.Run(scenario, func(t *testing.T) {
			runner, drain := newFakeRunner(), newFakeDrain()
			manager := testManager(t, testOptions(t, runner, drain))
			plan := makePlan(t, manager, Restart, Dispatcher)
			code := "stale_plan"
			switch scenario {
			case "revision":
				drain.snapshot.Version = "new-version"
			case "instance":
				runner.set(Dispatcher, "registered", "running", 999)
				code = "wrong_instance"
			case "dependencies":
				runner.set(Bridge, "registered", "waiting", 0)
				code = "dependency_not_ready"
			case "expiry":
				manager.mu.Lock()
				stored := manager.plans[plan.ID]
				stored.ExpiresAt = time.Now().Add(-time.Second)
				manager.plans[plan.ID] = stored
				manager.mu.Unlock()
				code = "plan_expired"
			}
			_, err := manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "request-1"})
			requireCode(t, err, code)
			if len(runner.commands()) != 0 || len(drain.calls) != 0 {
				t.Fatal("stale plan mutated runtime")
			}
		})
	}
}

func TestDrainSafetyGates(t *testing.T) {
	for _, scenario := range []string{"blocked", "unknown", "unsettled", "remote", "persisted", "scope", "pause", "delivery", "timedout", "draining", "mismatch", "drain_error"} {
		t.Run(scenario, func(t *testing.T) {
			runner, drain := newFakeRunner(), newFakeDrain()
			options := testOptions(t, runner, drain)
			if scenario == "draining" {
				options.OperationTimeout = 150 * time.Millisecond
			}
			manager := testManager(t, options)
			plan := makePlan(t, manager, Stop, Dispatcher)
			code := "drain_unknown"
			switch scenario {
			case "blocked":
				drain.status.State, code = "blocked", "drain_blocked"
			case "unknown":
				drain.pollError = true
			case "unsettled":
				drain.status.ConfirmedSettled = false
			case "remote":
				drain.status.RemoteSettled = false
			case "persisted":
				drain.status.Persisted = false
			case "scope":
				drain.status.Global = false
			case "pause":
				drain.status.Paused, code = true, "blocked_by_pause"
			case "delivery":
				drain.status.UnknownDelivery, code = true, "unknown_delivery"
			case "timedout":
				drain.status.State, code = "timedout", "drain_timedout"
			case "draining":
				drain.status.State, code = "draining", "deadline"
			case "mismatch":
				drain.pollHook = func() { drain.status.MaintenanceID = "different-maintenance" }
			case "drain_error":
				drain.drainError = true
			}
			operation := waitOperation(t, manager, execute(t, manager, plan).ID)
			if operation.State != "blocked" && operation.State != "unknown" || operation.Problem == nil || operation.Problem.Code != code {
				t.Fatalf("unexpected drain result: %+v", operation)
			}
			if len(runner.commands()) != 0 {
				t.Fatal("unsafe drain caused service command")
			}
			drain.mu.Lock()
			if len(drain.calls) != 1 || drain.calls[0] != "drain:global" {
				t.Fatalf("automatic resume or continue: %v", drain.calls)
			}
			drain.mu.Unlock()
		})
	}
}

func TestIdempotencySurvivesReopen(t *testing.T) {
	runner, drain := newFakeRunner(), newFakeDrain()
	options := testOptions(t, runner, drain)
	manager := testManager(t, options)
	plan := makePlan(t, manager, Restart, Bridge)
	input := ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "request-1"}
	first := waitOperation(t, manager, execute(t, manager, plan).ID)
	manager.Close()
	reopened := testManager(t, options)
	duplicate, err := reopened.Execute(context.Background(), input)
	if err != nil || duplicate.ID != first.ID || duplicate.State != "succeeded" || len(runner.commands()) != 1 {
		t.Fatalf("duplicate reexecuted: %+v %v", duplicate, err)
	}
	input.Token = "different-token"
	_, err = reopened.Execute(context.Background(), input)
	requireCode(t, err, "request_conflict")
}

func TestCrashRecoveryOnlyReadsBack(t *testing.T) {
	runner, drain := newFakeRunner(), newFakeDrain()
	options := testOptions(t, runner, drain)
	manager := testManager(t, options)
	plan := makePlan(t, manager, Restart, Bridge)
	input := ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "request-1"}
	first := waitOperation(t, manager, execute(t, manager, plan).ID)
	manager.mu.Lock()
	record := manager.journal.Operations[first.ID]
	record.Operation.State = "executing"
	setStep(&record.Operation, "command", "running")
	manager.journal.Operations[first.ID] = record
	if err := manager.persistLocked(); err != nil {
		t.Fatal(err)
	}
	manager.mu.Unlock()
	manager.Close()
	before := len(runner.calls)
	reopened := testManager(t, options)
	if len(runner.calls) != before {
		t.Fatal("reopen touched launchd")
	}
	operation, err := reopened.GetOperation(context.Background(), first.ID)
	if err != nil || operation.State != "unknown" || operation.Readback == nil || operation.Problem.Code != "recovered_unknown" {
		t.Fatalf("recovery: %+v %v", operation, err)
	}
	_, err = reopened.Execute(context.Background(), input)
	if err != nil || len(runner.commands()) != 1 {
		t.Fatalf("recovery retried command: %v", err)
	}
	if makePlan(t, reopened, Stop, Relay).Eligible {
		t.Fatal("unknown operation did not block subsequent controls")
	}
	runner.set(Relay, "unregistered", "stopped", 0)
	drain.snapshotUnavailable = true
	plan = makePlan(t, reopened, Start, Relay)
	if plan.Eligible {
		t.Fatal("unknown operation did not block offline start")
	}
	_, err = reopened.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "offline-start"})
	requireCode(t, err, "operation_active")
	if len(runner.commands()) != 1 {
		t.Fatal("unknown operation permitted another command")
	}
}

func TestConcurrentRequestsUseOneCommand(t *testing.T) {
	runner, drain := newFakeRunner(), newFakeDrain()
	manager := testManager(t, testOptions(t, runner, drain))
	plan := makePlan(t, manager, Restart, Relay)
	input := ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "concurrent"}
	var wait sync.WaitGroup
	results := make(chan Operation, 12)
	for index := 0; index < 12; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			operation, err := manager.Execute(context.Background(), input)
			if err != nil {
				t.Errorf("duplicate failed: %v", err)
				return
			}
			results <- operation
		}()
	}
	wait.Wait()
	close(results)
	var id string
	for operation := range results {
		if id != "" && operation.ID != id {
			t.Fatal("multiple operations for one request")
		}
		id = operation.ID
	}
	waitOperation(t, manager, id)
	if len(runner.commands()) != 1 {
		t.Fatalf("duplicate commands: %v", runner.commands())
	}
}

func TestCloseCancelsAndWaitsWithoutReexecution(t *testing.T) {
	runner, drain := newFakeRunner(), newFakeDrain()
	runner.commandSeen = make(chan struct{}, 1)
	runner.control = func(ctx context.Context, _ []string) (int, error) {
		<-ctx.Done()
		return -1, ctx.Err()
	}
	options := testOptions(t, runner, drain)
	manager := testManager(t, options)
	operation := execute(t, manager, makePlan(t, manager, Restart, Relay))
	select {
	case <-runner.commandSeen:
	case <-time.After(time.Second):
		t.Fatal("command did not start")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := testManager(t, options)
	recovered, err := reopened.GetOperation(context.Background(), operation.ID)
	if err != nil || recovered.State != "unknown" || len(runner.commands()) != 1 {
		t.Fatalf("close/reopen retried: %+v %v", recovered, err)
	}
}

func TestReadonlyAllowlistAndRedaction(t *testing.T) {
	zero := testManager(t, Options{})
	if zero.ControlCapabilities(context.Background()).Actions[Start].Supported {
		t.Fatal("zero value advertised control support")
	}
	if _, _, _, err := (ExecCommander{}).Run(context.Background(), launchctl, "print", "gui/501/anything"); err == nil {
		t.Fatal("real exec was permitted outside Execute")
	}
	runner, drain := newFakeRunner(), newFakeDrain()
	options := testOptions(t, runner, drain)
	options.EnableControl = false
	manager := testManager(t, options)
	for _, id := range []ServiceID{"wechat", "WeChat", "com.tencent.xinWeChat", "io.jinx.relay", "relay;touch /tmp/injected", "../relay", "$(launchctl bootout gui/501)"} {
		_, err := manager.Plan(context.Background(), Stop, id, "unused")
		requireCode(t, err, "invalid_service")
	}
	plan := makePlan(t, manager, Restart, Relay)
	if plan.Eligible {
		t.Fatal("read-only plan eligible")
	}
	_, err := manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "readonly"})
	requireCode(t, err, "readonly")
	data, _ := json.Marshal(plan)
	if strings.Contains(string(data), "secret") || strings.Contains(string(data), "99999") || strings.Contains(string(data), "environment") {
		t.Fatalf("raw launchd output escaped: %s", data)
	}
	for _, call := range runner.calls {
		if call.executable != launchctl || len(call.args) != 2 || call.args[0] != "print" {
			t.Fatalf("unsafe command: %+v", call)
		}
	}
	if len(runner.commands()) != 0 {
		t.Fatal("read-only control invoked")
	}
}

func TestJournalLockScopeAndIntegrity(t *testing.T) {
	runner, drain := newFakeRunner(), newFakeDrain()
	options := testOptions(t, runner, drain)
	manager := testManager(t, options)
	_, err := New(options)
	requireCode(t, err, "journal_locked")
	manager.Close()
	other := uint32(502)
	wrong := options
	wrong.UID = &other
	_, err = New(wrong)
	requireCode(t, err, "journal_invalid")
	if err := os.WriteFile(options.DataPath, []byte(`{"schema":1`), 0600); err != nil {
		t.Fatal(err)
	}
	_, err = New(options)
	requireCode(t, err, "journal_invalid")
	if len(runner.calls) != 0 {
		t.Fatal("constructor performed launchctl")
	}
}

func TestMaintenanceContinueCancelAndDeadline(t *testing.T) {
	for _, action := range []string{"continue", "cancel", "deadline"} {
		t.Run(action, func(t *testing.T) {
			runner, drain := newFakeRunner(), newFakeDrain()
			drain.status.State = "blocked"
			manager := testManager(t, testOptions(t, runner, drain))
			plan := makePlan(t, manager, Stop, Bridge)
			operation := waitOperation(t, manager, execute(t, manager, plan).ID)
			if operation.State != "blocked" {
				t.Fatalf("expected blocked, got %+v", operation)
			}
			if action == "continue" {
				if _, err := manager.ContinueMaintenance(context.Background(), operation.ID); err != nil {
					t.Fatal(err)
				}
				ready, err := manager.GetOperation(context.Background(), operation.ID)
				if err != nil || ready.State != "blocked" || ready.Problem.Code != "execute_required" || len(runner.commands()) != 0 {
					t.Fatalf("Continue executed service command: %+v %v", ready, err)
				}
				if _, err := manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "request-1"}); err != nil {
					t.Fatal(err)
				}
				if operation = waitOperation(t, manager, operation.ID); operation.State != "succeeded" || len(runner.commands()) != 1 {
					t.Fatalf("explicit continue failed: %+v", operation)
				}
			} else {
				if action == "deadline" {
					manager.mu.Lock()
					record := manager.journal.Operations[operation.ID]
					record.Operation.Deadline = time.Now().Add(-time.Second)
					manager.journal.Operations[operation.ID] = record
					manager.mu.Unlock()
					_, err := manager.ContinueMaintenance(context.Background(), operation.ID)
					requireCode(t, err, "deadline")
				}
				canceled, err := manager.CancelMaintenance(context.Background(), operation.ID)
				if err != nil || canceled.State != "canceled" || len(runner.commands()) != 0 {
					t.Fatalf("cancel/deadline executed command: %+v %v", canceled, err)
				}
				_, err = manager.ContinueMaintenance(context.Background(), operation.ID)
				requireCode(t, err, "operation_active")
			}
		})
	}
}

func TestDependencyAndPauseAreReportedWithoutImplicitStart(t *testing.T) {
	for _, scenario := range []string{"dependency", "pause", "delivery", "observer"} {
		t.Run(scenario, func(t *testing.T) {
			runner, drain := newFakeRunner(), newFakeDrain()
			kind := Restart
			code := "dependency_not_ready"
			switch scenario {
			case "dependency":
				kind = Start
				runner.set(Dispatcher, "unregistered", "stopped", 0)
				runner.set(Bridge, "unregistered", "stopped", 0)
			case "pause":
				drain.snapshot.Paused, code = true, "blocked_by_pause"
			case "delivery":
				drain.snapshot.UnknownDelivery, code = true, "unknown_delivery"
			case "observer":
				runner.printError, code = true, "observation_unknown"
			}
			manager := testManager(t, testOptions(t, runner, drain))
			plan := makePlan(t, manager, kind, Dispatcher)
			if plan.Eligible {
				t.Fatal("unsafe control allowed")
			}
			found := false
			for _, problem := range plan.Blockers {
				found = found || problem.Code == code
			}
			if !found || len(runner.commands()) != 0 {
				t.Fatalf("missing blocker %s: %+v", code, plan.Blockers)
			}
		})
	}
}

func TestDriftDuringDrainNeverControlsService(t *testing.T) {
	for _, scenario := range []string{"instance", "dependency", "dependency_version", "pause"} {
		t.Run(scenario, func(t *testing.T) {
			runner, drain := newFakeRunner(), newFakeDrain()
			drain.pollHook = func() {
				if drain.polls != 1 {
					return
				}
				switch scenario {
				case "instance":
					runner.set(Dispatcher, "registered", "running", 999)
				case "dependency":
					runner.set(Bridge, "registered", "waiting", 0)
				case "dependency_version":
					drain.snapshot.DependencyVersion = "changed"
				case "pause":
					drain.snapshot.Paused = true
				}
			}
			manager := testManager(t, testOptions(t, runner, drain))
			operation := waitOperation(t, manager, execute(t, manager, makePlan(t, manager, Restart, Dispatcher)).ID)
			if operation.State != "blocked" || len(runner.commands()) != 0 {
				t.Fatalf("drift during drain ignored: %+v", operation)
			}
		})
	}
}

func TestCommandFailureAndUncertaintyNeverRetry(t *testing.T) {
	for _, scenario := range []string{"nonzero", "transport_error", "observed_success"} {
		t.Run(scenario, func(t *testing.T) {
			runner, drain := newFakeRunner(), newFakeDrain()
			runner.control = func(context.Context, []string) (int, error) {
				if scenario == "observed_success" {
					runner.set(Relay, "unregistered", "stopped", 0)
				}
				if scenario != "nonzero" {
					return -1, errors.New("secret ambiguous connection")
				}
				return 5, nil
			}
			manager := testManager(t, testOptions(t, runner, drain))
			plan := makePlan(t, manager, Stop, Relay)
			operation := waitOperation(t, manager, execute(t, manager, plan).ID)
			want := "unknown"
			if scenario == "nonzero" {
				want = "failed"
			} else if scenario == "observed_success" {
				want = "succeeded"
			}
			if operation.State != want {
				t.Fatalf("got %+v; want %s", operation, want)
			}
			duplicate, err := manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "request-1"})
			if err != nil || duplicate.ID != operation.ID || len(runner.commands()) != 1 {
				t.Fatalf("command retried: %+v %v", duplicate, err)
			}
			data, _ := json.Marshal(operation)
			stored, _ := os.ReadFile(manager.options.DataPath)
			if strings.Contains(string(data)+string(stored), "secret") {
				t.Fatal("command output or error leaked")
			}
		})
	}
}

func TestJournalFailurePreventsCommand(t *testing.T) {
	runner, drain := newFakeRunner(), newFakeDrain()
	manager := testManager(t, testOptions(t, runner, drain))
	plan := makePlan(t, manager, Restart, Relay)
	if err := os.Rename(manager.options.DataPath, manager.options.DataPath+".saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(manager.options.DataPath, 0700); err != nil {
		t.Fatal(err)
	}
	_, err := manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "request-1"})
	requireCode(t, err, "journal_unavailable")
	if len(runner.commands()) != 0 || len(drain.calls) != 0 {
		t.Fatal("journal failure still performed control")
	}
	capabilities := manager.ControlCapabilities(context.Background())
	if capabilities.Actions[Restart].Ready || capabilities.Actions[Restart].Problem.Code != "journal_unavailable" {
		t.Fatal("poisoned journal advertised readiness")
	}
}

func TestParsePrintOnlyUsesTopLevelFacts(t *testing.T) {
	target := "gui/501/" + labels[Relay]
	valid := target + " = {\n state = running\n pid = 123\n runs = 4\n last exit code = -9\n environment = {\n pid = 999\n state = bad\n }\n}\n"
	service := parsePrint(Relay, target, valid)
	if service.Registration != "registered" || service.PID != 123 || service.LastExit == nil || *service.LastExit != -9 || service.InstanceID == "" {
		t.Fatalf("parse facts: %+v", service)
	}
	for _, output := range []string{
		strings.Replace(valid, target, "gui/501/com.tencent.xinWeChat", 1),
		strings.Replace(valid, "state = running", "state = unsupported", 1),
		strings.Replace(valid, "pid = 123", "pid = invalid", 1),
		strings.Replace(valid, "pid = 123", "pid = 123\n pid = 456", 1),
		strings.TrimSuffix(valid, "}\n"),
		"permission denied",
	} {
		if got := parsePrint(Relay, target, output); got.Registration != "unknown" {
			t.Fatalf("malformed print accepted: %+v", got)
		}
	}
}

func TestPlanTokensAndCancellation(t *testing.T) {
	runner, drain := newFakeRunner(), newFakeDrain()
	manager := testManager(t, testOptions(t, runner, drain))
	plan := makePlan(t, manager, Restart, Relay)
	_, err := manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: "wrong-token", RequestID: "request-1"})
	requireCode(t, err, "invalid_token")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.Execute(ctx, ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "request-1"}); err == nil {
		t.Fatal("canceled request authorized operation")
	}
	if len(runner.commands()) != 0 {
		t.Fatal("canceled request launched command")
	}
	operation := waitOperation(t, manager, execute(t, manager, plan).ID)
	_, err = manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "new-request"})
	requireCode(t, err, "plan_used")
	cleaned, err := manager.CancelMaintenance(context.Background(), operation.ID)
	if err != nil || cleaned.State != "succeeded" || len(runner.commands()) != 1 {
		t.Fatalf("explicit cleanup altered result or repeated command: %+v %v", cleaned, err)
	}
}

func TestTrustedOptionsAndOptionalDecision(t *testing.T) {
	for _, options := range []Options{
		{EnableControl: true},
		{EnableControl: true, UID: new(uint32), PlistDir: "relative", DataPath: "/tmp/unused"},
		{DataPath: "relative"},
	} {
		_, err := New(options)
		requireCode(t, err, "invalid_options")
	}
	runner, drain := newFakeRunner(), newFakeDrain()
	runner.set(Dispatcher, "unregistered", "stopped", 0)
	runner.set(Decision, "unregistered", "stopped", 0)
	options := testOptions(t, runner, drain)
	manager := testManager(t, options)
	*options.UID = 999
	plan := makePlan(t, manager, Start, Dispatcher)
	if !plan.Eligible {
		t.Fatalf("optional Decision blocked start: %+v", plan.Blockers)
	}
	operation := waitOperation(t, manager, execute(t, manager, plan).ID)
	if operation.State != "succeeded" || runner.commands()[0].args[1] != "gui/501" {
		t.Fatalf("trusted UID changed through caller pointer: %+v", operation)
	}
	drain.mu.Lock()
	drain.snapshot.DecisionRequired = true
	drain.mu.Unlock()
	if makePlan(t, manager, Restart, Dispatcher).Eligible {
		t.Fatal("required Decision did not block restart")
	}
}

func TestPlistPathIsOneArgumentWithoutShell(t *testing.T) {
	runner, drain := newFakeRunner(), newFakeDrain()
	runner.set(Relay, "unregistered", "stopped", 0)
	options := testOptions(t, runner, drain)
	options.PlistDir = "/trusted/space directory;$(never-execute)"
	manager := testManager(t, options)
	operation := waitOperation(t, manager, execute(t, manager, makePlan(t, manager, Start, Relay)).ID)
	want := []string{"bootstrap", "gui/501", options.PlistDir + "/" + labels[Relay] + ".plist"}
	if operation.State != "succeeded" || !reflect.DeepEqual(runner.commands()[0].args, want) {
		t.Fatalf("plist path was interpreted as shell text: %+v", runner.commands())
	}
}

func TestCustomServiceLabelsValidationAndObservation(t *testing.T) {
	for _, bad := range []map[ServiceID]string{
		{"unknown": "org.example.svc"},
		{Relay: "not-reverse-dns"},
		{Relay: "../bad.label"},
		{Relay: "org.example.same", Bridge: "org.example.same"},
	} {
		options := testOptions(t, newFakeRunner(), newFakeDrain())
		options.ServiceLabels = bad
		if _, err := New(options); err == nil {
			t.Fatalf("expected invalid ServiceLabels rejected: %+v", bad)
		}
	}

	options := testOptions(t, newFakeRunner(), newFakeDrain())
	options.ServiceLabels = map[ServiceID]string{Relay: "org.example.custom-relay"}
	options.Observer = commanderFunc(func(_ context.Context, _ string, args ...string) (string, string, int, error) {
		if len(args) == 2 && args[0] == "print" && args[1] == "gui/501/org.example.custom-relay" {
			return args[1] + " = {\n\tstate = running\n\tpid = 4242\n\truns = 1\n}\n", "", 0, nil
		}
		return "", `Could not find service "` + strings.TrimPrefix(args[1], "gui/501/") + `"`, 113, nil
	})
	manager := testManager(t, options)
	list, err := manager.ListServices(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if list.Services[0].Label != "org.example.custom-relay" || list.Services[0].State != "running" || list.Services[0].PID != 4242 {
		t.Fatalf("unexpected custom-labeled service observation: %+v", list.Services[0])
	}
}

type commanderFunc func(context.Context, string, ...string) (string, string, int, error)

func (f commanderFunc) Run(ctx context.Context, exe string, args ...string) (string, string, int, error) {
	return f(ctx, exe, args...)
}
