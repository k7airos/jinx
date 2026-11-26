package integration_test

import (
	"context"
	"net/http"
	"net/url"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"jinx/internal/console"
	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
)

func controlSnapshot(t *testing.T, services *stack) dispatcher.ControlSnapshot {
	t.Helper()
	var snapshot dispatcher.ControlSnapshot
	requestJSON(t, services.upstream.Client(), http.MethodGet, services.upstream.URL+"/api/control/snapshot", nil, &snapshot)
	return snapshot
}

func controlAction(t *testing.T, services *stack, id, revision string, action dispatcher.ControlAction) dispatcher.ControlOperation {
	t.Helper()
	input := dispatcher.ControlRequest{RequestID: id, ExpectedRevision: revision, Action: action}
	var operation dispatcher.ControlOperation
	status := http.StatusOK
	if action.Kind == "cancel_run" && action.CancelMode == "stop_execution" {
		status = http.StatusNotImplemented
	}
	requestStatusJSON(t, services.upstream.Client(), http.MethodPost, services.upstream.URL+"/api/control/action", input, &operation, status)
	var readback dispatcher.ControlOperation
	requestJSON(t, services.upstream.Client(), http.MethodGet, services.upstream.URL+"/api/control/operations/"+operation.OperationID, nil, &readback)
	if (operation.Status != "succeeded" && operation.Status != "delivery_blocked") || !reflect.DeepEqual(operation, readback) {
		t.Fatalf("control action lacks stable operation readback: %+v %+v", operation, readback)
	}
	var replay dispatcher.ControlOperation
	requestStatusJSON(t, services.upstream.Client(), http.MethodPost, services.upstream.URL+"/api/control/action", input, &replay, status)
	if !reflect.DeepEqual(operation, replay) {
		t.Fatal("control request replay changed the operation")
	}
	return operation
}

func waitInputAcknowledged(t *testing.T, services *stack, previous string) {
	t.Helper()
	await(t, "HTTP relay acknowledgement", func() bool {
		return stackHealth(t, services).LastEventID != previous
	})
}

func stackHealth(t *testing.T, services *stack) dispatcher.Health {
	t.Helper()
	var health dispatcher.Health
	requestJSON(t, services.upstream.Client(), http.MethodGet, services.upstream.URL+"/health", nil, &health)
	return health
}

func stackRuns(t *testing.T, services *stack) console.RunPage {
	t.Helper()
	view, err := services.facade.ListRuns(context.Background(), url.Values{"groupId": {sourceGroup}})
	if err != nil {
		t.Fatal(err)
	}
	return view.Data.(console.RunPage)
}

func TestControlPauseResumeDrainThroughHTTP(t *testing.T) {
	root := t.TempDir()
	relay := newRelay(t, root)
	var posts, writes, calls atomic.Int32
	bridge := newBridge(t, relay.Client(), relay.URL, &posts, make(chan bridgeDelivery, 8))
	path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
	runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
		calls.Add(1)
		return dispatcher.Result{Text: syntheticReply, Backend: "synthetic-http", RunID: "synthetic-control-run", SessionID: "synthetic-control-session"}, nil
	})
	services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes, runner)
	scope := dispatcher.ControlScope{Kind: "group", GroupID: sourceGroup}
	initial := controlSnapshot(t, services)
	pause := controlAction(t, services, "pause-synthetic", initial.Revision, dispatcher.ControlAction{Kind: "pause", Scope: scope})
	if pause.Control == nil || !pause.Control.Paused || !controlSnapshot(t, services).Groups[sourceGroup].Paused {
		t.Fatal("pause was acknowledged without effective HTTP readback")
	}
	previous := stackHealth(t, services).LastEventID
	postMessage(t, relay.Client(), relay.URL, "paused-input", "@Jinx synthetic paused input")
	waitInputAcknowledged(t, services, previous)
	if calls.Load() != 0 || posts.Load() != 0 || len(stackRuns(t, services).Items) != 0 {
		t.Fatal("paused intake crossed the execution boundary")
	}
	resume := controlAction(t, services, "resume-synthetic", pause.Revision, dispatcher.ControlAction{Kind: "resume", Scope: scope})
	if resume.Control == nil || resume.Control.Paused {
		t.Fatal("resume did not change the effective control")
	}
	postMessage(t, relay.Client(), relay.URL, "resumed-input", "@Jinx synthetic resumed input")
	run := waitRun(t, services, "resumed-input")
	if run.Delivery != contracts.DeliveryConfirmed || posts.Load() != 1 || calls.Load() != 1 {
		t.Fatalf("resumed input did not finish once: %+v calls=%d posts=%d", run, calls.Load(), posts.Load())
	}
	drain := controlAction(t, services, "drain-synthetic", resume.Revision, dispatcher.ControlAction{Kind: "drain", Scope: dispatcher.ControlScope{Kind: "global"}})
	snapshot := controlSnapshot(t, services)
	if snapshot.Maintenance == nil || snapshot.Maintenance.ID != drain.MaintenanceID || snapshot.Maintenance.Stage != "ready" || snapshot.Maintenance.Counts.UnresolvedRuns != 0 {
		t.Fatalf("idle maintenance did not settle: %+v", snapshot)
	}
	previous = stackHealth(t, services).LastEventID
	postMessage(t, relay.Client(), relay.URL, "drained-input", "@Jinx synthetic maintenance input")
	waitInputAcknowledged(t, services, previous)
	if calls.Load() != 1 || posts.Load() != 1 || len(stackRuns(t, services).Items) != 1 {
		t.Fatal("maintenance admitted new work")
	}
	controlAction(t, services, "cancel-maintenance-synthetic", snapshot.Revision, dispatcher.ControlAction{Kind: "cancel_maintenance", Scope: dispatcher.ControlScope{Kind: "global"}, MaintenanceID: drain.MaintenanceID})
	if snapshot = controlSnapshot(t, services); snapshot.Maintenance == nil || snapshot.Maintenance.Active || snapshot.Maintenance.Stage != "canceled" {
		t.Fatalf("maintenance cancel readback did not release intake: %+v", snapshot)
	}
	postMessage(t, relay.Client(), relay.URL, "after-maintenance", "@Jinx synthetic post maintenance input")
	if run = waitRun(t, services, "after-maintenance"); run.Delivery != contracts.DeliveryConfirmed || calls.Load() != 2 || posts.Load() != 2 {
		t.Fatalf("maintenance release did not restore delivery: %+v", run)
	}
}

func TestControlCancelRunningBackendThroughHTTP(t *testing.T) {
	for _, mode := range []string{"block_delivery", "stop_execution"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			relay := newRelay(t, root)
			var posts, writes atomic.Int32
			bridge := newBridge(t, relay.Client(), relay.URL, &posts, make(chan bridgeDelivery, 2))
			path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
			started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			defer close(release)
			runner := dispatcher.RunFunc(func(ctx context.Context, request dispatcher.BackendRequest) (dispatcher.Result, error) {
				defer close(finished)
				close(started)
				select {
				case <-ctx.Done():
					return dispatcher.Result{}, ctx.Err()
				case <-release:
					return dispatcher.Result{Text: syntheticReply, Backend: "synthetic-http", RunID: "synthetic-cancel-run", SessionID: "synthetic-cancel-session"}, nil
				}
			})
			services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes, runner)
			postMessage(t, relay.Client(), relay.URL, "cancel-input", "@Jinx synthetic cancel input")
			await(t, "runner started", func() bool {
				select {
				case <-started:
					return true
				default:
					return false
				}
			})
			var active console.RunView
			await(t, "executing trace through Console", func() bool {
				page := stackRuns(t, services)
				if len(page.Items) != 1 {
					return false
				}
				active = page.Items[0]
				return active.Phase == contracts.PhaseExecuting
			})
			operation := controlAction(t, services, "cancel-run-synthetic", active.Revision, dispatcher.ControlAction{Kind: "cancel_run", Scope: dispatcher.ControlScope{Kind: "group", GroupID: sourceGroup}, RunID: active.ID, CancelMode: mode})
			if operation.RunID != active.ID || operation.RunRevision == "" {
				t.Fatalf("cancel lost run correlation: %+v", operation)
			}
			if mode == "stop_execution" {
				if operation.Code != "remote_cancel_unsupported" || operation.RemoteExecution != "may_be_running" {
					t.Fatalf("unsupported remote cancel fabricated success: %+v", operation)
				}
				await(t, "backend context canceled", func() bool {
					select {
					case <-finished:
						return true
					default:
						return false
					}
				})
			} else {
				release <- struct{}{}
			}
			run := waitRun(t, services, "cancel-input")
			if run.Outcome != contracts.OutcomeCanceled || run.Delivery != contracts.DeliveryBlocked || !run.DeliveryEvidence.NoSubmission || posts.Load() != 0 {
				t.Fatalf("canceled backend crossed sink boundary: %+v posts=%d", run, posts.Load())
			}
		})
	}
}
