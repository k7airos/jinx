package supervisor

import (
	"context"
	"reflect"
	"testing"
)

func TestDispatcherStartAfterOfflineStop(t *testing.T) {
	runner, drain := newFakeRunner(), newFakeDrain()
	manager := testManager(t, testOptions(t, runner, drain))
	stopped := waitOperation(t, manager, execute(t, manager, makePlan(t, manager, Stop, Dispatcher)).ID)
	if stopped.State != "succeeded" || stopped.MaintenanceID == "" || stopped.Readback.Registration != "unregistered" {
		t.Fatalf("stop failed: %+v", stopped)
	}
	drain.mu.Lock()
	drain.snapshotUnavailable, drain.snapshot.Paused = true, true
	snapshot, status, polls := drain.snapshot, drain.status, drain.polls
	drain.mu.Unlock()
	plan := makePlan(t, manager, Start, Dispatcher)
	if !plan.Eligible || plan.DrainRequired || plan.Base.Control != nil {
		t.Fatalf("offline start blocked: %+v", plan)
	}
	input := ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "start-dispatcher"}
	started, err := manager.Execute(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	started = waitOperation(t, manager, started.ID)
	if started.State != "succeeded" || started.Readback == nil || !achieved(plan, *started.Readback) || started.MaintenanceID != "" || started.Drain != nil {
		t.Fatalf("start failed: %+v", started)
	}
	duplicate, err := manager.Execute(context.Background(), input)
	if err != nil || duplicate.ID != started.ID {
		t.Fatalf("duplicate start: %+v %v", duplicate, err)
	}
	commands := runner.commands()
	if len(commands) != 2 || !reflect.DeepEqual(commands[1].args, []string{"bootstrap", "gui/501", "/trusted/LaunchAgents/" + labels[Dispatcher] + ".plist"}) {
		t.Fatalf("commands: %+v", commands)
	}
	if makePlan(t, manager, Start, Dispatcher).Eligible {
		t.Fatal("running Dispatcher was eligible for another start")
	}
	drain.mu.Lock()
	defer drain.mu.Unlock()
	if !reflect.DeepEqual(drain.calls, []string{"drain:global"}) || drain.polls != polls || drain.snapshot != snapshot || drain.status != status {
		t.Fatalf("start changed pause/maintenance: %+v", drain)
	}
}

func TestStartDoesNotRequireCurrentControl(t *testing.T) {
	for _, mode := range []string{"offline", "unconfigured", "paused_delivery"} {
		for _, id := range serviceOrder {
			t.Run(mode+"/"+string(id), func(t *testing.T) {
				runner, drain := newFakeRunner(), newFakeDrain()
				runner.set(Dispatcher, "unregistered", "stopped", 0)
				runner.set(id, "unregistered", "stopped", 0)
				drain.snapshotUnavailable = mode == "offline"
				drain.snapshot.Paused, drain.snapshot.UnknownDelivery = true, true
				snapshot, status := drain.snapshot, drain.status
				options := testOptions(t, runner, drain)
				controlCode := "blocked_by_pause"
				if mode == "offline" {
					controlCode = "control_unknown"
				} else if mode == "unconfigured" {
					options.DrainCoordinator = nil
					controlCode = "unsupported"
				}
				manager := testManager(t, options)
				capabilities := manager.ControlCapabilities(context.Background())
				if !capabilities.Actions[Start].Ready || capabilities.Actions[Start].Problem != nil {
					t.Fatalf("start unavailable: %+v", capabilities.Actions[Start])
				}
				for _, kind := range []Kind{Stop, Restart} {
					availability := capabilities.Actions[kind]
					if availability.Ready {
						t.Fatalf("%s advertised readiness without control", kind)
					}
					requireCode(t, availability.Problem, controlCode)
					blocked := makePlan(t, manager, kind, id)
					if blocked.Eligible {
						t.Fatalf("%s eligible without current control", kind)
					}
				}
				plan := makePlan(t, manager, Start, id)
				operation := waitOperation(t, manager, execute(t, manager, plan).ID)
				if operation.State != "succeeded" || operation.Drain != nil || operation.MaintenanceID != "" || len(runner.commands()) != 1 {
					t.Fatalf("start failed: %+v", operation)
				}
				for _, step := range operation.Steps {
					if step.Name == "drain" && step.State != "skipped" {
						t.Fatalf("start did not skip drain: %+v", step)
					}
				}
				drain.mu.Lock()
				defer drain.mu.Unlock()
				if len(drain.calls) != 0 || drain.polls != 0 || drain.snapshot != snapshot || drain.status != status {
					t.Fatalf("start changed pause/maintenance: %+v", drain)
				}
			})
		}
	}
}

func TestDispatcherOfflineStartRequiresDependencies(t *testing.T) {
	for _, id := range []ServiceID{Relay, Bridge, Decision} {
		t.Run(string(id), func(t *testing.T) {
			runner, drain := newFakeRunner(), newFakeDrain()
			runner.set(Dispatcher, "unregistered", "stopped", 0)
			runner.set(id, "registered", "waiting", 0)
			drain.snapshotUnavailable = true
			manager := testManager(t, testOptions(t, runner, drain))
			plan := makePlan(t, manager, Start, Dispatcher)
			if plan.Eligible || plan.Base.Control != nil {
				t.Fatalf("missing dependency allowed start: %+v", plan)
			}
			for _, dependency := range plan.Dependencies {
				if !dependency.Required || dependency.Ready != (dependency.ServiceID != id) {
					t.Fatalf("unknown Decision requirement was not conservative: %+v", dependency)
				}
			}
			_, err := manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "start-dispatcher"})
			requireCode(t, err, "dependency_not_ready")
			if len(runner.commands()) != 0 || len(drain.calls) != 0 {
				t.Fatal("blocked start mutated runtime")
			}
		})
	}
}

func TestStopRestartRejectUnavailableSnapshotBeforeExecute(t *testing.T) {
	for _, kind := range []Kind{Stop, Restart} {
		t.Run(string(kind), func(t *testing.T) {
			runner, drain := newFakeRunner(), newFakeDrain()
			manager := testManager(t, testOptions(t, runner, drain))
			plan := makePlan(t, manager, kind, Dispatcher)
			if !plan.Eligible {
				t.Fatalf("initial plan blocked: %+v", plan.Blockers)
			}
			drain.snapshotUnavailable = true
			_, err := manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "offline-control"})
			requireCode(t, err, "control_unknown")
			if len(runner.commands()) != 0 || len(drain.calls) != 0 {
				t.Fatal("missing snapshot authorized control")
			}
		})
	}
}

func TestOfflineStartRevalidatesPlan(t *testing.T) {
	for _, scenario := range []string{"target", "running", "instance", "dependency", "observation", "control"} {
		t.Run(scenario, func(t *testing.T) {
			runner, drain := newFakeRunner(), newFakeDrain()
			runner.set(Dispatcher, "registered", "waiting", 0)
			drain.snapshotUnavailable = true
			manager := testManager(t, testOptions(t, runner, drain))
			plan := makePlan(t, manager, Start, Dispatcher)
			if !plan.Eligible {
				t.Fatalf("offline start blocked: %+v", plan.Blockers)
			}
			code := "stale_plan"
			switch scenario {
			case "target":
				runner.set(Dispatcher, "unregistered", "stopped", 0)
			case "running":
				runner.set(Dispatcher, "registered", "running", 999)
				code = "already_running"
			case "instance":
				runner.set(Relay, "registered", "running", 999)
			case "dependency":
				runner.set(Bridge, "registered", "waiting", 0)
				code = "dependency_not_ready"
			case "observation":
				runner.printError = true
				code = "observation_unknown"
			case "control":
				drain.snapshotUnavailable = false
			}
			_, err := manager.Execute(context.Background(), ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: "start-dispatcher"})
			requireCode(t, err, code)
			if len(runner.commands()) != 0 || len(drain.calls) != 0 {
				t.Fatal("stale start mutated runtime")
			}
		})
	}
}
