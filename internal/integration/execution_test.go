package integration_test

import (
	"context"
	"encoding/json"
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
	"jinx/internal/dispatcher"
	"jinx/internal/execution"
	"jinx/internal/gateway"
	"jinx/internal/runtimebridge"
)

const syntheticModel = "synthetic-provider/synthetic-model"

func readFixtureFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

type executionRunner struct {
	*runtimebridge.Runner
	engine      *execution.Engine
	inboundOnly bool
	mu          sync.Mutex
	results     map[string]execution.Result
	errors      map[string]error
}

func (runner *executionRunner) Run(ctx context.Context, request dispatcher.BackendRequest) (dispatcher.Result, error) {
	if runner.inboundOnly {
		if len(request.Messages) != 1 {
			return dispatcher.Result{}, fmt.Errorf("live test requires one fictional inbound message")
		}
		request.Prompt = strings.TrimSpace(strings.TrimPrefix(request.Messages[0].Content, "@Jinx"))
		request.Messages = nil
	}
	return runner.Runner.Run(ctx, request)
}

func (runner *executionRunner) Execute(ctx context.Context, request execution.Request) (execution.Result, error) {
	result, err := runner.engine.Execute(ctx, request)
	runner.mu.Lock()
	runner.results[request.TraceID], runner.errors[request.TraceID] = result, err
	runner.mu.Unlock()
	return result, err
}

func (runner *executionRunner) WithDeliveryGate(ctx context.Context, result execution.Result, group string, submit func(context.Context) error) error {
	return runner.engine.WithDeliveryGate(ctx, result, group, submit)
}

func (runner *executionRunner) observed(id string) (execution.Result, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.results[id], runner.errors[id]
}

func newExecution(t *testing.T, root string, client *http.Client, backends []execution.BackendOptions, config execution.Config) (*execution.Engine, *executionRunner) {
	t.Helper()
	engine, err := execution.Open(filepath.Join(root, "execution.json"), execution.Options{Backends: backends, Client: client})
	if err != nil {
		t.Fatal(err)
	}
	api, err := gateway.New(gateway.Options{Execution: engine})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	var snapshot execution.Snapshot
	gatewayJSON(t, server, http.MethodGet, "/console/v1/execution/snapshot", nil, &snapshot)
	var plan execution.ChangePlan
	gatewayJSON(t, server, http.MethodPost, "/console/v1/execution/plan", gateway.ExecutionPlanInput{ExpectedRevision: snapshot.Revision, Config: config}, &plan)
	if len(plan.AffectedGroups) != 1 {
		t.Fatalf("execution plan: %+v", plan)
	}
	var current execution.Snapshot
	gatewayJSON(t, server, http.MethodGet, "/console/v1/execution/snapshot", nil, &current)
	if current.Revision != snapshot.Revision {
		t.Fatal("execution preview mutated routing")
	}
	var applied execution.ApplyResult
	gatewayJSON(t, server, http.MethodPost, "/console/v1/execution/apply", gateway.ExecutionApplyInput{ExpectedRevision: snapshot.Revision, RequestID: "synthetic-routing-publish", Config: config}, &applied)
	if applied.AppliedRevision == snapshot.Revision {
		t.Fatalf("execution apply: %+v", applied)
	}
	runner := &executionRunner{engine: engine, results: map[string]execution.Result{}, errors: map[string]error{}}
	runner.Runner, err = runtimebridge.NewRunner(runner)
	if err != nil {
		t.Fatal(err)
	}
	return engine, runner
}

func executionConfig(kind execution.Kind, model string, timeout int) execution.Config {
	capabilities := map[execution.Capability]bool{}
	for _, capability := range execution.Capabilities() {
		capabilities[capability] = false
	}
	capabilities[execution.TextGeneration], capabilities[execution.AutoTextDelivery] = true, true
	return execution.Config{SchemaVersion: execution.SchemaVersion,
		Profiles: map[string]string{profileName: sourceGroup},
		Policies: map[string]execution.Policy{"synthetic-policy": {Capabilities: capabilities, NoTools: kind == execution.TextOnly, AllowSoft: kind == execution.AlmaPersonal}},
		Groups: map[string]execution.Group{sourceGroup: {RouteID: "synthetic-route", PolicyID: "synthetic-policy",
			Primary: execution.RouteAttempt{BackendID: "synthetic-primary", ModelID: model, TimeoutSec: timeout, Session: execution.SessionPolicy{Mode: "independent_run"}}, Fallbacks: []execution.RouteAttempt{}}},
	}
}

func fakePersonal(t *testing.T, status string, calls, polls *atomic.Int32) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/api/personal/dispatch":
			calls.Add(1)
			var body map[string]any
			if json.NewDecoder(request.Body).Decode(&body) != nil || body["model"] != syntheticModel || !strings.Contains(fmt.Sprint(body["task"]), "synthetic") || len(body) != 3 {
				t.Error("personal dispatch lost explicit catalog model or sent unexpected fields")
				http.Error(writer, "bad synthetic dispatch", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]string{"id": "synthetic-personal-run", "status": "running", "threadId": "synthetic-personal-thread"})
		case request.Method == http.MethodGet && request.URL.Path == "/api/personal/runs/synthetic-personal-run":
			polls.Add(1)
			current := status
			if status == "waiting_then_done" {
				current = "done"
				if polls.Load() == 1 {
					current = "waiting"
				}
			}
			_ = json.NewEncoder(writer).Encode(map[string]string{"id": "synthetic-personal-run", "status": current, "threadId": "synthetic-personal-thread", "summary": syntheticReply})
		default:
			t.Errorf("unexpected Alma endpoint: %s %s", request.Method, request.URL.Path)
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func TestPersonalHTTPRealModuleChain(t *testing.T) {
	personalHTTPModuleChain(t, "done")
}

func TestPersonalHTTPWaitingThenDone(t *testing.T) {
	personalHTTPModuleChain(t, "waiting_then_done")
}

func personalHTTPModuleChain(t *testing.T, status string) {
	t.Helper()
	root := t.TempDir()
	var calls, polls, posts, writes atomic.Int32
	alma := fakePersonal(t, status, &calls, &polls)
	config := executionConfig(execution.AlmaPersonal, syntheticModel, 4)
	_, runner := newExecution(t, root, alma.Client(), []execution.BackendOptions{{ID: "synthetic-primary", Kind: execution.AlmaPersonal, Models: []execution.Model{{ID: syntheticModel, Label: "Synthetic model"}}, AlmaBaseURL: alma.URL, PollInterval: time.Millisecond}}, config)
	relay := newRelay(t, root)
	deliveries := make(chan bridgeDelivery, 2)
	bridge := newBridge(t, relay.Client(), relay.URL, &posts, deliveries)
	path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
	services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes, runner)
	postMessage(t, relay.Client(), relay.URL, "personal-http-input", "@Jinx synthetic personal HTTP question")
	run := waitRun(t, services, "personal-http-input")
	result, err := runner.observed(run.ID)
	if err != nil || run.Delivery != contracts.DeliveryConfirmed || run.Outcome != contracts.OutcomeReplied || calls.Load() != 1 || polls.Load() < 1 || posts.Load() != 1 {
		t.Fatalf("HTTP execution chain failed: run=%+v result=%+v err=%v calls=%d polls=%d sink=%d", run, result, err, calls.Load(), polls.Load(), posts.Load())
	}
	if len(result.Attempts) != 1 || result.Attempts[0].ExternalID != "synthetic-personal-run" || result.Attempts[0].Status != "succeeded" || result.Fallback || result.RoutingRevision == "" || result.PolicyRevision == "" {
		t.Fatalf("execution result lacks actual attempt evidence: %+v", result)
	}
	if delivery := <-deliveries; delivery.Text != syntheticReply || delivery.GroupID != targetGroup || run.DeliveryEvidence.EchoMessageID == nil {
		t.Fatalf("echo-confirmed sink content/routing lost: %+v", delivery)
	}
	found := false
	for _, attempt := range run.Attempts {
		found = found || attempt.ExternalID != nil && *attempt.ExternalID == result.Attempts[0].ExternalID
	}
	if !found {
		t.Fatalf("Console trace lost Alma run correlation: %+v", run.Attempts)
	}
	t.Logf("mock passed: trace=%s Alma run=synthetic-personal-run thread=synthetic-personal-thread sink=%d", run.ID, posts.Load())
}

func TestUnreachablePersonalDoesNotFallbackOrEcho(t *testing.T) {
	root := t.TempDir()
	unreachable := httptest.NewServer(http.NotFoundHandler())
	endpoint := unreachable.URL
	unreachable.Close()
	var posts, writes atomic.Int32
	_, runner := newExecution(t, root, unreachable.Client(), []execution.BackendOptions{{ID: "synthetic-primary", Kind: execution.AlmaPersonal, Models: []execution.Model{{ID: syntheticModel, Label: "Synthetic"}}, AlmaBaseURL: endpoint}}, executionConfig(execution.AlmaPersonal, syntheticModel, 1))
	relay := newRelay(t, root)
	bridge := newBridge(t, relay.Client(), relay.URL, &posts, make(chan bridgeDelivery, 1))
	path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
	services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes, runner)
	postMessage(t, relay.Client(), relay.URL, "unreachable-input", "@Jinx synthetic unreachable question")
	run := waitRun(t, services, "unreachable-input")
	result, err := runner.observed(run.ID)
	if err == nil || result.Text != "" || result.Fallback || len(result.Attempts) != 1 || result.Attempts[0].Status == "succeeded" || run.Outcome != contracts.OutcomeFailed || posts.Load() != 0 {
		t.Fatalf("unreachable backend fabricated success: result=%+v err=%v run=%+v sink=%d", result, err, run, posts.Load())
	}
}
