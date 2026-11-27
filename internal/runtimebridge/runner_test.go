package runtimebridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/dispatcher"
	"jinx/internal/execution"
)

const testGroup = "100@chatroom"
const testModel = "mock-model"

type providerFunc func(context.Context, execution.ModelRequest) (string, error)

func (generate providerFunc) Generate(ctx context.Context, request execution.ModelRequest) (string, error) {
	return generate(ctx, request)
}

func textBackend(id string, generate providerFunc) execution.BackendOptions {
	return execution.BackendOptions{ID: id, Kind: execution.TextOnly, Models: []execution.Model{{ID: testModel, Label: "Mock model"}}, Provider: generate}
}

func testAttempt(backend string) execution.RouteAttempt {
	return execution.RouteAttempt{BackendID: backend, ModelID: testModel, TimeoutSec: 2, Session: execution.SessionPolicy{Mode: "independent_run"}}
}

func testConfig(primary string, soft bool) execution.Config {
	policy := execution.Policy{Capabilities: map[execution.Capability]bool{}, NoTools: !soft, AllowSoft: soft}
	for _, capability := range execution.Capabilities() {
		policy.Capabilities[capability] = capability == execution.TextGeneration || capability == execution.AutoTextDelivery
	}
	return execution.Config{SchemaVersion: execution.SchemaVersion, Profiles: map[string]string{"demo": testGroup},
		Policies: map[string]execution.Policy{"policy": policy},
		Groups:   map[string]execution.Group{testGroup: {RouteID: "route", PolicyID: "policy", Primary: testAttempt(primary), Fallbacks: []execution.RouteAttempt{}}}}
}

func publish(t *testing.T, engine *execution.Engine, config execution.Config, requestID string) execution.Snapshot {
	t.Helper()
	snapshot, err := engine.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	applied, err := engine.ApplyConfig(context.Background(), snapshot.Revision, requestID, config)
	if err != nil {
		t.Fatal(err)
	}
	return applied.Snapshot
}

func testEngine(t *testing.T, config execution.Config, backends ...execution.BackendOptions) (*execution.Engine, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "execution.json")
	engine, err := execution.Open(path, execution.Options{Backends: backends})
	if err != nil {
		t.Fatal(err)
	}
	publish(t, engine, config, "initial")
	return engine, path
}

func testRunner(t *testing.T, engine Executor) *Runner {
	t.Helper()
	runner, err := NewRunner(engine)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func testRequest(trace string) dispatcher.BackendRequest {
	return dispatcher.BackendRequest{TraceID: trace, GroupID: testGroup, Profile: "demo", SessionID: "do-not-resume",
		Prompt:   "rules; history: private-history-once; pending: private-pending-once",
		Messages: []dispatcher.BackendMessage{{ID: "message", SenderID: "sender", Content: "private-pending-once"}}}
}

func TestRequestDoesNotDuplicatePromptOrReuseSession(t *testing.T) {
	engine, path := testEngine(t, testConfig("text", false), textBackend("text", func(_ context.Context, request execution.ModelRequest) (string, error) {
		if request.ModelID != testModel || len(request.Messages) != 1 || request.Messages[0].Role != "user" {
			t.Errorf("unexpected request: %+v", request)
		}
		for _, secret := range []string{"private-history-once", "private-pending-once"} {
			if strings.Count(request.Messages[0].Content, secret) != 1 {
				t.Error("duplicated history or pending batch")
			}
		}
		if strings.Contains(request.Messages[0].Content, "do-not-resume") {
			t.Error("session leaked into model input")
		}
		return "private-reply", nil
	}))
	runner := testRunner(t, engine)
	var configured dispatcher.Runner = runner
	result, err := configured.Run(context.Background(), testRequest("trace"))
	if err != nil || result.Text != "private-reply" || result.Backend != "Alma" || result.RunID != "trace" || result.SessionID != "execution:trace" {
		t.Fatal(result, err)
	}
	meta, exists := runner.Meta("trace")
	if !exists || meta.Text != "" || meta.Model != testModel || meta.Backend != "text" || meta.RoutingRevision == "" || meta.PolicyRevision == "" || len(meta.Attempts) != 1 {
		t.Fatal(meta, exists)
	}
	meta.Attempts[0].ModelID = "changed-copy"
	meta, _ = runner.Meta("trace")
	if meta.Attempts[0].ModelID != testModel {
		t.Fatal("metadata aliases retained attempts")
	}
	disk, err := os.ReadFile(path)
	if err != nil || strings.Contains(string(disk), "private-") {
		t.Fatal("prompt or body persisted", err)
	}
	runner.Forget("trace")
	if _, exists := runner.Meta("trace"); exists {
		t.Fatal("terminal result retained")
	}
	request := testRequest("messages-only")
	request.Prompt = ""
	converted := ConvertRequest(request)
	if converted.Prompt != "" || len(converted.Messages) != 1 || converted.Messages[0].Content != "private-pending-once" {
		t.Fatal(converted)
	}
}

func TestResultCompatibilityPreservesExecutionIdentity(t *testing.T) {
	for _, scenario := range []struct {
		kind    execution.Kind
		backend string
	}{{execution.AlmaPersonal, "Alma"}, {execution.TextOnly, "Alma"}, {execution.LegacyHermes, "Hermes"}, {execution.AgyCLI, "Agy"}} {
		result := ConvertResult(execution.Result{Kind: scenario.kind, RunID: "trace", Text: "reply", Fallback: true, FallbackReason: "timeout"})
		if result.Backend != scenario.backend || result.RunID != "trace" || result.SessionID != "execution:trace" || !result.Fallback || result.FallbackReason != "timeout" || result.Text != "reply" {
			t.Fatal(result)
		}
	}
	if result := ConvertResult(execution.Result{}); result.Backend != "" || result.SessionID != "" || result.RunID != "" {
		t.Fatal("empty execution falsely identified as a backend run", result)
	}
}

func TestPersonalDispatchPollAndFallbackMetadata(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprint(fallback), func(t *testing.T) {
			var polls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.Method + " " + request.URL.Path {
				case "POST /api/personal/dispatch":
					var body map[string]any
					if json.NewDecoder(request.Body).Decode(&body) != nil || len(body) != 3 || body["model"] != testModel || strings.Count(fmt.Sprint(body["task"]), "private-pending-once") != 1 {
						t.Errorf("unexpected personal payload: %+v", body)
					}
					_, _ = io.WriteString(writer, `{"id":"remote-run","status":"running","sourceThreadId":"shared-thread"}`)
				case "GET /api/personal/runs/remote-run":
					polls.Add(1)
					if fallback {
						_, _ = io.WriteString(writer, `{"id":"remote-run","status":"failed"}`)
					} else {
						_, _ = io.WriteString(writer, `{"id":"remote-run","status":"done","summary":"personal reply","threadId":"shared-thread"}`)
					}
				default:
					t.Errorf("unexpected personal endpoint: %s", request.URL)
					writer.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			personal := execution.BackendOptions{ID: "personal", Kind: execution.AlmaPersonal, AlmaBaseURL: server.URL,
				PollInterval: time.Millisecond, Models: []execution.Model{{ID: testModel, Label: "Mock model"}}}
			config := testConfig("personal", true)
			if fallback {
				group := config.Groups[testGroup]
				group.Fallbacks = []execution.RouteAttempt{testAttempt("text")}
				config.Groups[testGroup] = group
			}
			engine, _ := testEngine(t, config, personal, textBackend("text", func(context.Context, execution.ModelRequest) (string, error) { return "fallback reply", nil }))
			runner := testRunner(t, engine)
			result, err := runner.Run(context.Background(), testRequest("trace"))
			meta, exists := runner.Meta("trace")
			if err != nil || !exists || result.Fallback != fallback || result.SessionID != "execution:trace" || polls.Load() != 1 || meta.Attempts[0].ExternalID != "remote-run" {
				t.Fatal(result, meta, err)
			}
			if fallback && (meta.Backend != "text" || len(meta.Attempts) != 2 || meta.Attempts[0].Status != "failed" || meta.Attempts[1].FallbackReason != "backend_failed") {
				t.Fatal(meta)
			}
			if !fallback && (meta.Backend != "personal" || len(meta.Attempts) != 1 || result.Text != "personal reply") {
				t.Fatal(meta, result)
			}
		})
	}
}

func TestDeliveryGateCleansEveryTerminalOutcome(t *testing.T) {
	for _, scenario := range []string{"success", "tightened", "wrong_group", "submit_error", "canceled", "nil_submit", "silent", "failed_execution"} {
		t.Run(scenario, func(t *testing.T) {
			engine, _ := testEngine(t, testConfig("text", false), textBackend("text", func(context.Context, execution.ModelRequest) (string, error) {
				if scenario == "silent" {
					return "NONE", nil
				}
				if scenario == "failed_execution" {
					return "", errors.New("synthetic failure")
				}
				return "reply", nil
			}))
			runner := testRunner(t, engine)
			_, err := runner.Run(context.Background(), testRequest("trace"))
			if (err != nil) != (scenario == "failed_execution") {
				t.Fatal(err)
			}
			if scenario == "tightened" {
				config := testConfig("text", false)
				config.Policies["policy"].Capabilities[execution.AutoTextDelivery] = false
				publish(t, engine, config, "tighten")
			}
			group := testGroup
			if scenario == "wrong_group" {
				group = "200@chatroom"
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario == "canceled" {
				cancel()
			}
			called := false
			submissionError := errors.New("synthetic submission failure")
			var submit func(context.Context) error = func(context.Context) error {
				called = true
				if scenario == "submit_error" {
					return submissionError
				}
				return nil
			}
			if scenario == "nil_submit" {
				submit = nil
			}
			err = runner.DeliveryGate(ctx, "trace", group, submit)
			if (err == nil) != (scenario == "success") || called != (scenario == "success" || scenario == "submit_error") {
				t.Fatal("gate outcome", err, called)
			}
			if scenario == "submit_error" && !errors.Is(err, submissionError) {
				t.Fatal(err)
			}
			if _, exists := runner.Meta("trace"); exists {
				t.Fatal("gate retained a terminal result")
			}
			if err := runner.DeliveryGate(context.Background(), "trace", testGroup, func(context.Context) error { t.Fatal("double submission"); return nil }); err == nil {
				t.Fatal("consumed result admitted")
			}
		})
	}
}

func TestDeliveryGateConcurrentSubmissionIsRejected(t *testing.T) {
	engine, _ := testEngine(t, testConfig("text", false), textBackend("text", func(context.Context, execution.ModelRequest) (string, error) { return "reply", nil }))
	runner := testRunner(t, engine)
	if _, err := runner.Run(context.Background(), testRequest("trace")); err != nil {
		t.Fatal(err)
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- runner.DeliveryGate(context.Background(), "trace", testGroup, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	err := runner.DeliveryGate(context.Background(), "trace", testGroup, func(context.Context) error { t.Error("concurrent submit admitted"); return nil })
	close(release)
	if firstErr := <-done; firstErr != nil || err == nil {
		t.Fatal(firstErr, err)
	}
}

func TestStopExecutionCancelsLocalPollingOnly(t *testing.T) {
	polled := make(chan struct{}, 1)
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method + " " + request.URL.Path {
		case "POST /api/personal/dispatch":
			_, _ = io.WriteString(writer, `{"id":"remote-run","status":"running"}`)
		case "GET /api/personal/runs/remote-run":
			polls.Add(1)
			select {
			case polled <- struct{}{}:
			default:
			}
			_, _ = io.WriteString(writer, `{"id":"remote-run","status":"running"}`)
		default:
			t.Error("unexpected remote cancel request", request.URL)
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	engine, _ := testEngine(t, testConfig("personal", true), execution.BackendOptions{ID: "personal", Kind: execution.AlmaPersonal,
		AlmaBaseURL: server.URL, PollInterval: 10 * time.Millisecond, Models: []execution.Model{{ID: testModel, Label: "Mock model"}}})
	runner := testRunner(t, engine)
	done := make(chan error, 1)
	go func() {
		_, err := runner.Run(context.Background(), testRequest("trace"))
		done <- err
	}()
	select {
	case <-polled:
	case <-time.After(3 * time.Second):
		t.Fatal("mock not polled")
	}
	result, supported, err := runner.StopExecution(context.Background(), "trace")
	if err != nil || !supported || !result.LocalStopRequested || result.LocalStopped || result.RemoteCancelSupported || result.RemoteExecution != "may_be_running" {
		t.Fatal(result, supported, err)
	}
	select {
	case err := <-done:
		// Local polling is canceled, but the remote task can still be running.
		// Execution reports that uncertainty instead of claiming remote stop.
		if !errors.Is(err, context.Canceled) && execution.Code(err) != "execution_uncertain" {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("local execution did not stop")
	}
	count := polls.Load()
	time.Sleep(30 * time.Millisecond)
	if polls.Load() != count {
		t.Fatal("polling continued after cancellation")
	}
	if _, exists := runner.Meta("trace"); exists {
		t.Fatal("canceled result retained")
	}
	if _, supported, err := runner.StopExecution(context.Background(), "trace"); !supported || execution.Code(err) != "not_found" {
		t.Fatal("nonexistent execution falsely stopped", supported, err)
	}
}

type executorWithoutCancel struct{}

func (executorWithoutCancel) Execute(context.Context, execution.Request) (execution.Result, error) {
	return execution.Result{}, nil
}

func (executorWithoutCancel) WithDeliveryGate(context.Context, execution.Result, string, func(context.Context) error) error {
	return nil
}

func TestUnsupportedStopIsHonest(t *testing.T) {
	runner := testRunner(t, executorWithoutCancel{})
	result, supported, err := runner.StopExecution(context.Background(), "trace")
	if err != nil || supported || result.LocalStopped || result.RemoteCancelSupported {
		t.Fatal(result, supported, err)
	}
}
