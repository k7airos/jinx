package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/gateway"
	"jinx/internal/supervisor"
)

type fakeLaunchd struct {
	mu       sync.Mutex
	running  bool
	pid      int
	commands [][]string
}

func (launchd *fakeLaunchd) Run(ctx context.Context, executable string, args ...string) (string, string, int, error) {
	launchd.mu.Lock()
	defer launchd.mu.Unlock()
	if executable != "/bin/launchctl" || ctx.Err() != nil {
		return "", "", -1, fmt.Errorf("fake launchd rejected executable/context")
	}
	target := "gui/501/io.jinx.dispatcher"
	if len(args) == 2 && args[0] == "print" {
		if !strings.HasPrefix(args[1], "gui/501/io.jinx.") {
			return "", "", -1, fmt.Errorf("fake launchd rejected target")
		}
		if args[1] == target && !launchd.running {
			return "", `Could not find service "io.jinx.dispatcher"`, 113, nil
		}
		pid := 401
		if args[1] == target {
			pid = launchd.pid
		}
		return fmt.Sprintf("%s = {\n state = running\n pid = %d\n runs = 1\n}\n", args[1], pid), "", 0, nil
	}
	launchd.commands = append(launchd.commands, append([]string{executable}, args...))
	switch {
	case len(args) == 3 && args[0] == "bootstrap" && args[1] == "gui/501" && filepath.Base(args[2]) == "io.jinx.dispatcher.plist":
		launchd.running = true
		launchd.pid++
	case reflect.DeepEqual(args, []string{"kickstart", "-k", target}):
		launchd.running = true
		launchd.pid++
	case reflect.DeepEqual(args, []string{"bootout", target}):
		launchd.running = false
	default:
		return "", "", -1, fmt.Errorf("fake launchd rejected mutation")
	}
	return "", "", 0, nil
}

func TestFakeLaunchdLifecycleWithRealDispatcherDrain(t *testing.T) {
	root := t.TempDir()
	relay := newRelay(t, root)
	var posts, writes atomic.Int32
	bridge := newBridge(t, relay.Client(), relay.URL, &posts, make(chan bridgeDelivery, 1))
	path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
	services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes)
	drain, err := gateway.NewDispatcherDrainCoordinator(services.upstream.URL, services.upstream.Client())
	if err != nil {
		t.Fatal(err)
	}
	launchd := &fakeLaunchd{pid: 500}
	uid := uint32(501)
	options := supervisor.Options{UID: &uid, PlistDir: filepath.Join(root, "plists"), DataPath: filepath.Join(root, "supervisor.json"), EnableControl: true,
		Commander: launchd, Observer: launchd, DrainCoordinator: drain, PollInterval: time.Millisecond, OperationTimeout: 4 * time.Second, VerifyTimeout: time.Second}
	manager, err := supervisor.New(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Close() })
	api, err := gateway.New(gateway.Options{Console: services.facade, Supervisor: manager, DispatcherURL: services.upstream.URL, HTTPClient: services.upstream.Client()})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	ctx := context.Background()
	var last supervisor.Operation
	for index, kind := range []supervisor.Kind{supervisor.Start, supervisor.Restart, supervisor.Stop} {
		var list supervisor.ServiceList
		gatewayGet(t, server, "/console/v1/services/runtime", &list)
		var plan supervisor.ActionPlan
		gatewayJSON(t, server, http.MethodPost, "/console/v1/services/plan", gateway.ServicePlanInput{Kind: kind, ServiceID: supervisor.Dispatcher, ExpectedRevision: list.Revision}, &plan)
		if !plan.Eligible {
			t.Fatalf("lifecycle plan: %+v", plan)
		}
		input := supervisor.ExecuteInput{PlanID: plan.ID, Token: plan.Token, RequestID: fmt.Sprintf("fake-launchd-%d", index)}
		var operation supervisor.Operation
		gatewayJSON(t, server, http.MethodPost, "/console/v1/services/execute", input, &operation)
		await(t, "fake launchd verified operation", func() bool {
			last = supervisor.Operation{}
			gatewayGet(t, server, "/console/v1/services/operations/"+operation.ID, &last)
			switch last.State {
			case "succeeded", "failed", "blocked", "unknown":
				return true
			}
			return false
		})
		if last.State != "succeeded" || last.Readback == nil || last.Problem != nil {
			t.Fatalf("lifecycle operation lacks verified fake readback: %+v", last)
		}
		if kind == supervisor.Stop && (last.Readback.PID != 0 || last.Readback.InstanceID != "") {
			t.Fatalf("stopped service retained running process identity: %+v", last.Readback)
		}
		if kind != supervisor.Start {
			if last.MaintenanceID == "" || last.Drain == nil || !last.Drain.Persisted || !last.Drain.ConfirmedSettled || !last.Drain.RemoteSettled {
				t.Fatalf("lifecycle mutated before real Dispatcher drained: %+v", last)
			}
			if err := drain.Cancel(ctx, last.MaintenanceID); err != nil {
				t.Fatal(err)
			}
		}
		var replay supervisor.Operation
		gatewayJSON(t, server, http.MethodPost, "/console/v1/services/execute", input, &replay)
		if replay.ID != last.ID || replay.State != "succeeded" {
			t.Fatalf("lifecycle replay changed operation: %+v", replay)
		}
	}
	launchd.mu.Lock()
	commands := append([][]string{}, launchd.commands...)
	launchd.mu.Unlock()
	if len(commands) != 3 || commands[0][1] != "bootstrap" || commands[1][1] != "kickstart" || commands[2][1] != "bootout" || posts.Load() != 0 {
		t.Fatalf("fake command allowlist/replay failed: %+v sink=%d", commands, posts.Load())
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := supervisor.New(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	readback, err := reopened.GetOperation(ctx, last.ID)
	beforeJSON, _ := json.Marshal(last)
	afterJSON, _ := json.Marshal(readback)
	if err != nil || !bytes.Equal(beforeJSON, afterJSON) {
		t.Fatalf("reopened lifecycle journal changed: before=%s after=%s err=%v", beforeJSON, afterJSON, err)
	}
	launchd.mu.Lock()
	defer launchd.mu.Unlock()
	if len(launchd.commands) != 3 {
		t.Fatal("journal reopening reran a service command")
	}
}
