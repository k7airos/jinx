package execution

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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type providerFunc func(context.Context, ModelRequest) (string, error)

func (provider providerFunc) Generate(ctx context.Context, request ModelRequest) (string, error) {
	return provider(ctx, request)
}

func testPolicy(soft bool) Policy {
	policy := Policy{Capabilities: map[Capability]bool{}, NoTools: !soft, AllowSoft: soft}
	for _, capability := range Capabilities() {
		policy.Capabilities[capability] = capability == TextGeneration || capability == AutoTextDelivery
	}
	return policy
}

func testConfig(backend string, soft bool) Config {
	return Config{SchemaVersion: SchemaVersion, Profiles: map[string]string{"demo-profile": "demo-group"}, Policies: map[string]Policy{"demo-policy": testPolicy(soft)},
		Groups: map[string]Group{"demo-group": {RouteID: "demo-route", PolicyID: "demo-policy", Primary: testAttempt(backend), Fallbacks: []RouteAttempt{}}}}
}

func testAttempt(backend string) RouteAttempt {
	return RouteAttempt{BackendID: backend, ModelID: "demo-provider:demo-model", TimeoutSec: 1, Session: SessionPolicy{Mode: "independent_run"}}
}

func textOption(id string, provider Provider) BackendOptions {
	return BackendOptions{ID: id, Kind: TextOnly, Models: []Model{{ID: "demo-provider:demo-model", Label: "Fictional model"}}, Provider: provider}
}

func personalOption(id, baseURL string) BackendOptions {
	return BackendOptions{ID: id, Kind: AlmaPersonal, Models: []Model{{ID: "demo-provider:demo-model", Label: "Fictional model"}}, AlmaBaseURL: baseURL, PollInterval: time.Millisecond}
}

func openTest(t *testing.T, options ...BackendOptions) *Engine {
	t.Helper()
	engine, err := Open(filepath.Join(t.TempDir(), "execution.json"), Options{Backends: options})
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

func publish(t *testing.T, engine *Engine, config Config, requestID string) Snapshot {
	t.Helper()
	ctx := context.Background()
	before, err := engine.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	after, err := engine.ApplyConfig(ctx, before.Revision, requestID, config)
	if err != nil {
		t.Fatal(err)
	}
	return after.Snapshot
}

func executeTest(engine *Engine, trace string) (Result, error) {
	return engine.Execute(context.Background(), Request{TraceID: trace, GroupID: "demo-group", Prompt: "fictional prompt"})
}

func TestPersonalDispatchPollShapeAndNone(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		for _, initialStatus := range []string{"queued", "running", "pending", "waiting"} {
			t.Run(fmt.Sprintf("%s/wrapped=%t", initialStatus, wrapped), func(t *testing.T) {
				var polls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					var run any
					switch request.Method + " " + request.URL.Path {
					case "POST /api/personal/dispatch":
						var body map[string]any
						if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
							t.Error(err)
						}
						if len(body) != 3 || body["model"] != "demo-provider:demo-model" || !strings.Contains(fmt.Sprint(body["task"]), "fictional prompt") || body["agent_id"] != nil {
							t.Errorf("unexpected dispatch payload: %#v", body)
						}
						run = map[string]any{"id": "demo-remote", "status": initialStatus, "threadId": "demo-thread"}
					case "GET /api/personal/runs/demo-remote":
						run = map[string]any{"id": "demo-remote", "status": "done", "summary": " NONE \n"}
						if polls.Add(1) == 1 {
							run = map[string]any{"id": "demo-remote", "status": "waiting"}
						}
					default:
						t.Errorf("unexpected endpoint %s", request.URL)
						writer.WriteHeader(404)
						return
					}
					if wrapped {
						run = map[string]any{"run": run}
					}
					_ = json.NewEncoder(writer).Encode(run)
				}))
				defer server.Close()
				engine := openTest(t, personalOption("personal", server.URL))
				publish(t, engine, testConfig("personal", true), "publish")
				result, err := executeTest(engine, "trace")
				if err != nil || !result.Silent || result.Text != "NONE" || result.Fallback || len(result.Attempts) != 1 || polls.Load() != 2 || result.Attempts[0].ExternalID != "demo-remote" || result.Attempts[0].RemoteExecution != "stopped" {
					t.Fatalf("result=%+v error=%v polls=%d", result, err, polls.Load())
				}
			})
		}
	}
}

func TestPersonalFailureTimeoutFallback(t *testing.T) {
	for _, scenario := range []struct {
		name       string
		reason     string
		externalID string
		safe       bool
	}{
		{"failed", "backend_failed", "demo-remote", true},
		{"skipped", "backend_failed", "demo-remote", true},
		{"empty", "invalid_output", "demo-remote", true},
		{"timeout", "timeout", "", false},
		{"poll_timeout", "timeout", "demo-remote", false},
		{"invalid_id", "invalid_output", "", false},
		{"transport", "backend_http_error", "", false},
		{"poll_http", "backend_http_error", "demo-remote", false},
		{"dispatch_lost", "transport_error", "", false},
		{"dispatch_partial_lost", "invalid_output", "", false},
		{"poll_lost", "transport_error", "demo-remote", false},
		{"poll_partial_lost", "invalid_output", "demo-remote", false},
		{"malformed_dispatch", "invalid_output", "", false},
		{"unknown_status", "invalid_output", "demo-remote", false},
		{"poll_wrong_id_done", "invalid_output", "demo-remote", false},
		{"poll_wrong_id_failed", "invalid_output", "demo-remote", false},
		{"poll_missing_id_done", "invalid_output", "demo-remote", false},
		{"poll_missing_id_failed", "invalid_output", "demo-remote", false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			release := make(chan struct{})
			var dispatches, polls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				dispatch := request.Method == http.MethodPost && request.URL.Path == "/api/personal/dispatch"
				poll := request.Method == http.MethodGet && request.URL.Path == "/api/personal/runs/demo-remote"
				if dispatch {
					dispatches.Add(1)
				} else if poll {
					polls.Add(1)
				} else {
					t.Error("unexpected endpoint", request.Method, request.URL.Path)
					writer.WriteHeader(http.StatusNotFound)
					return
				}
				if scenario.name == "timeout" || scenario.name == "poll_timeout" && poll {
					select {
					case <-request.Context().Done():
					case <-release:
					}
					return
				}
				if scenario.name == "transport" || scenario.name == "poll_http" && poll {
					writer.WriteHeader(503)
					_, _ = io.WriteString(writer, "PRIVATE ERROR MUST NOT ESCAPE")
					return
				}
				if strings.HasPrefix(scenario.name, "dispatch_") || strings.HasPrefix(scenario.name, "poll_") && strings.HasSuffix(scenario.name, "lost") && poll {
					connection, buffered, err := writer.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					defer connection.Close()
					if strings.Contains(scenario.name, "partial") {
						_, _ = buffered.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n{\"id\":\"PRIVATE")
						_ = buffered.Flush()
					}
					return
				}
				if scenario.name == "malformed_dispatch" {
					_, _ = io.WriteString(writer, "PRIVATE INVALID JSON")
					return
				}
				run := personalRun{ID: "demo-remote", Status: "waiting"}
				if scenario.name == "empty" {
					run.Status, run.Summary = "done", " \n\t "
				} else if scenario.name == "invalid_id" {
					run.ID = "../../PRIVATE"
				} else if scenario.name == "unknown_status" {
					run.Status = "PRIVATE UNKNOWN STATUS"
				} else if poll {
					if scenario.safe {
						run.Status = scenario.name
					} else {
						run.Status = "done"
						if strings.HasSuffix(scenario.name, "failed") {
							run.Status = "failed"
						}
						run.Summary = "PRIVATE OTHER RUN SUMMARY"
						if strings.Contains(scenario.name, "wrong_id") {
							run.ID = "PRIVATE-other-run"
						} else {
							run.ID = ""
						}
					}
				}
				_ = json.NewEncoder(writer).Encode(run)
			}))
			defer server.Close()
			defer close(release)
			var fallbackCalls atomic.Int32
			engine := openTest(t, personalOption("personal", server.URL), textOption("text", providerFunc(func(context.Context, ModelRequest) (string, error) {
				fallbackCalls.Add(1)
				return "safe fallback", nil
			})))
			config := testConfig("personal", true)
			group := config.Groups["demo-group"]
			group.Fallbacks = []RouteAttempt{testAttempt("text")}
			config.Groups["demo-group"] = group
			publish(t, engine, config, "publish")
			result, err := executeTest(engine, "trace")
			if scenario.safe {
				if err != nil || !result.Fallback || result.Backend != "text" || result.Text != "safe fallback" || len(result.Attempts) != 2 || fallbackCalls.Load() != 1 || result.FallbackReason != scenario.reason || result.Attempts[1].FallbackReason != scenario.reason {
					t.Fatalf("%+v %v", result, err)
				}
			} else {
				var problem *Error
				if !errors.As(err, &problem) || *problem != (Error{Code: "execution_uncertain", Field: "fallback_blocked"}) || result.Fallback || result.Text != "" || result.DeliveryAllowed || len(result.Attempts) != 1 || fallbackCalls.Load() != 0 {
					t.Fatalf("%+v %v", result, err)
				}
			}
			attempt := result.Attempts[0]
			expectedRemote, expectedStatus := "may_be_running", "failed"
			if scenario.safe {
				expectedRemote = "stopped"
			}
			if scenario.reason == "timeout" {
				expectedStatus = "timed_out"
			}
			if attempt.Status != expectedStatus || attempt.Reason != scenario.reason || attempt.ExternalID != scenario.externalID || attempt.RemoteExecution != expectedRemote {
				t.Fatal(result.Attempts)
			}
			expectedPolls := int32(0)
			if scenario.safe && scenario.name != "empty" || strings.HasPrefix(scenario.name, "poll_") {
				expectedPolls = 1
			}
			if dispatches.Load() != 1 || (scenario.name == "poll_lost" && polls.Load() < 1) || (scenario.name != "poll_lost" && polls.Load() != expectedPolls) {
				t.Fatal("unexpected request counts", dispatches.Load(), polls.Load())
			}
			if strings.Contains(fmt.Sprint(result, err), "PRIVATE") {
				t.Fatal("backend error leaked")
			}
		})
	}
}

func TestTextOnlyTimeoutFallback(t *testing.T) {
	var fallbackCalls atomic.Int32
	engine := openTest(t,
		textOption("primary", providerFunc(func(ctx context.Context, request ModelRequest) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		})),
		textOption("fallback", providerFunc(func(context.Context, ModelRequest) (string, error) {
			fallbackCalls.Add(1)
			return "pure generation retry", nil
		})))
	config := testConfig("primary", false)
	group := config.Groups["demo-group"]
	group.Fallbacks = []RouteAttempt{testAttempt("fallback")}
	config.Groups["demo-group"] = group
	publish(t, engine, config, "initial")
	result, err := executeTest(engine, "trace")
	if err != nil || !result.Fallback || result.Text != "pure generation retry" || result.FallbackReason != "timeout" || fallbackCalls.Load() != 1 || len(result.Attempts) != 2 {
		t.Fatal(result, err)
	}
	if result.Attempts[0].Status != "timed_out" || result.Attempts[0].RemoteExecution != "not_applicable" {
		t.Fatal(result.Attempts)
	}
}

func TestPolicyModelSessionValidationAndCatalog(t *testing.T) {
	engine := openTest(t, textOption("text", providerFunc(func(context.Context, ModelRequest) (string, error) { return "ok", nil })),
		personalOption("personal", "http://127.0.0.1:1"), textOption("unconfigured", nil))
	ctx := context.Background()
	for _, scenario := range []struct {
		name string
		edit func(*Config)
		code string
	}{
		{"personal_no_tools", func(config *Config) {
			group := config.Groups["demo-group"]
			group.Primary.BackendID = "personal"
			config.Groups["demo-group"] = group
		}, "policy_mismatch"},
		{"arbitrary_model", func(config *Config) {
			group := config.Groups["demo-group"]
			group.Primary.ModelID = "attacker:model"
			config.Groups["demo-group"] = group
		}, "validation"},
		{"host_write", func(config *Config) { config.Policies["demo-policy"].Capabilities[HostWrite] = true }, "unsupported"},
		{"reminders", func(config *Config) { config.Policies["demo-policy"].Capabilities[ReminderSync] = true }, "unsupported"},
		{"web", func(config *Config) { config.Policies["demo-policy"].Capabilities[WebSearch] = true }, "unsupported"},
		{"missing_capability", func(config *Config) { delete(config.Policies["demo-policy"].Capabilities, HostWrite) }, "validation"},
		{"unconfigured", func(config *Config) {
			group := config.Groups["demo-group"]
			group.Primary.BackendID = "unconfigured"
			config.Groups["demo-group"] = group
		}, "unavailable"},
		{"session", func(config *Config) {
			group := config.Groups["demo-group"]
			group.Primary.Session.Mode = "isolated_session"
			config.Groups["demo-group"] = group
		}, "unsupported"},
		{"soft_fallback_from_hard", func(config *Config) {
			config.Policies["demo-policy"] = testPolicy(true)
			group := config.Groups["demo-group"]
			group.Fallbacks = []RouteAttempt{testAttempt("personal")}
			config.Groups["demo-group"] = group
		}, "fallback_policy_mismatch"},
		{"multi_step_does_not_widen", func(config *Config) {
			config.Policies["demo-policy"] = testPolicy(true)
			group := config.Groups["demo-group"]
			group.Primary = testAttempt("personal")
			group.Fallbacks = []RouteAttempt{testAttempt("text"), testAttempt("personal")}
			config.Groups["demo-group"] = group
		}, "fallback_policy_mismatch"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			config := testConfig("text", false)
			scenario.edit(&config)
			if err := engine.Validate(ctx, config); Code(err) != scenario.code {
				t.Fatalf("got %v expected %s", err, scenario.code)
			}
		})
	}
	catalog, err := engine.Catalog(ctx)
	if err != nil || len(catalog) != 3 || catalog[2].Available {
		t.Fatal(catalog, err)
	}
	for _, backend := range catalog {
		for _, permission := range backend.PermissionEvidence {
			if permission.Capability == AutoTextDelivery && (permission.Enforcement != Host || len(permission.EnforcedAt) != 0) {
				t.Fatal("claimed external delivery enforcement", permission)
			}
			if backend.Kind == AlmaPersonal && permission.Capability == HostTools && (permission.Enforcement != Soft || permission.Effective != nil) {
				t.Fatal("claimed personal isolation", permission)
			}
			if backend.ID == "text" && permission.Capability == HostTools && (permission.Enforcement != Hard || permission.Effective == nil || *permission.Effective || len(permission.EnforcedAt) == 0) {
				t.Fatal("text-only evidence missing", permission)
			}
		}
	}
}

func TestCASReopenIdempotencyAndStrictSchema(t *testing.T) {
	option := textOption("text", providerFunc(func(context.Context, ModelRequest) (string, error) { return "ok", nil }))
	engine := openTest(t, option)
	ctx := context.Background()
	before, _ := engine.Snapshot(ctx)
	config := testConfig("text", false)
	plan, err := engine.Plan(ctx, before.Revision, config)
	if err != nil || len(plan.AffectedGroups) != 1 {
		t.Fatal(plan, err)
	}
	after, err := engine.Update(ctx, before.Revision, "demo-request", config)
	if err != nil || after.Snapshot.Revision == before.Revision {
		t.Fatal(after, err)
	}
	reopened, err := Open(engine.path, Options{Backends: []BackendOptions{option}})
	if err != nil {
		t.Fatal(err)
	}
	replay, err := reopened.ApplyConfig(ctx, before.Revision, "demo-request", config)
	if err != nil || !replay.Replayed || replay.AppliedRevision != after.AppliedRevision {
		t.Fatal(replay, err)
	}
	if _, err := reopened.ApplyConfig(ctx, before.Revision, "other-request", config); Code(err) != "conflict" {
		t.Fatal(err)
	}
	if _, err := reopened.ApplyConfig(ctx, after.AppliedRevision, "demo-request", config); Code(err) != "idempotency_conflict" {
		t.Fatal(err)
	}
	group, err := reopened.GetGroup("demo-group")
	if err != nil || group.Routing.Primary.ModelID != "demo-provider:demo-model" || group.Revision != after.AppliedRevision {
		t.Fatal(group, err)
	}
	info, _ := os.Stat(engine.path)
	if info.Mode().Perm() != 0600 {
		t.Fatal(info.Mode())
	}
	encoded, _ := json.Marshal(config)
	for _, malformed := range []string{
		strings.Replace(string(encoded), `"schemaVersion":"1"`, `"schemaVersion":"1","schemaVersion":"2"`, 1),
		strings.Replace(string(encoded), `"noTools":true,`, ``, 1),
		strings.Replace(string(encoded), `"allowSoft":false`, `"allowSoft":null`, 1),
		strings.Replace(string(encoded), `"schemaVersion":"1"`, `"schemaVersion":"1","apiKey":"secret"`, 1),
		string(encoded) + `{}`,
	} {
		if _, err := DecodeConfig(strings.NewReader(malformed)); err == nil {
			t.Fatal("malformed config accepted", malformed)
		}
	}
}

func TestConcurrentCASAcrossEngines(t *testing.T) {
	option := textOption("text", providerFunc(func(context.Context, ModelRequest) (string, error) { return "ok", nil }))
	first := openTest(t, option)
	second, err := Open(first.path, Options{Backends: []BackendOptions{option}})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := first.Snapshot(context.Background())
	var succeeded, conflicts atomic.Int32
	var workers sync.WaitGroup
	for index, engine := range []*Engine{first, second} {
		workers.Add(1)
		go func(index int, engine *Engine) {
			defer workers.Done()
			_, err := engine.ApplyConfig(context.Background(), before.Revision, fmt.Sprintf("request-%d", index), testConfig("text", false))
			if err == nil {
				succeeded.Add(1)
			} else if Code(err) == "conflict" {
				conflicts.Add(1)
			} else {
				t.Error(err)
			}
		}(index, engine)
	}
	workers.Wait()
	if succeeded.Load() != 1 || conflicts.Load() != 1 {
		t.Fatal(succeeded.Load(), conflicts.Load())
	}
}

func TestRuntimeTighteningBlocksFallbackAndKeepsRoutingRevision(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	primary := textOption("primary", providerFunc(func(context.Context, ModelRequest) (string, error) {
		close(started)
		<-release
		return "", errors.New("model unavailable")
	}))
	fallback := textOption("fallback", providerFunc(func(context.Context, ModelRequest) (string, error) { calls.Add(1); return "forbidden", nil }))
	engine := openTest(t, primary, fallback)
	config := testConfig("primary", false)
	group := config.Groups["demo-group"]
	group.Fallbacks = []RouteAttempt{testAttempt("fallback")}
	config.Groups["demo-group"] = group
	initial := publish(t, engine, config, "initial")
	done := make(chan struct {
		result Result
		err    error
	}, 1)
	go func() {
		result, err := executeTest(engine, "trace")
		done <- struct {
			result Result
			err    error
		}{result, err}
	}()
	<-started
	config.Policies["demo-policy"].Capabilities[TextGeneration] = false
	publish(t, engine, config, "tighten")
	close(release)
	outcome := <-done
	if Code(outcome.err) != "fallback_policy_mismatch" || calls.Load() != 0 || outcome.result.RoutingRevision != initial.Revision || len(outcome.result.Attempts) != 1 {
		t.Fatalf("%+v %v calls=%d", outcome.result, outcome.err, calls.Load())
	}
}

func TestDeliveryTighteningAndNoRetroactiveExpansion(t *testing.T) {
	engine := openTest(t, textOption("text", providerFunc(func(context.Context, ModelRequest) (string, error) { return "reply", nil })))
	config := testConfig("text", false)
	config.Policies["demo-policy"].Capabilities[AutoTextDelivery] = false
	publish(t, engine, config, "initial")
	result, err := executeTest(engine, "first")
	if err != nil || result.DeliveryAllowed {
		t.Fatal(result, err)
	}
	config.Policies["demo-policy"].Capabilities[AutoTextDelivery] = true
	publish(t, engine, config, "expand")
	if err := engine.CheckDelivery(context.Background(), result, "demo-group"); Code(err) != "policy_blocked" {
		t.Fatal(err)
	}
	result, err = executeTest(engine, "second")
	if err != nil {
		t.Fatal(err)
	}
	submitted := false
	if err := engine.WithDeliveryGate(context.Background(), result, "demo-group", func(context.Context) error { submitted = true; return nil }); err != nil || !submitted {
		t.Fatal(err)
	}
	config.Policies["demo-policy"].Capabilities[AutoTextDelivery] = false
	publish(t, engine, config, "tighten")
	if err := engine.WithDeliveryGate(context.Background(), result, "demo-group", func(context.Context) error { t.Fatal("submitted after tightening"); return nil }); Code(err) != "policy_blocked" {
		t.Fatal(err)
	}
}

func TestResultBoundaryRejectsTighteningAndExecuteRevisionConflict(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	engine := openTest(t, textOption("text", providerFunc(func(context.Context, ModelRequest) (string, error) {
		close(started)
		<-release
		return "must be blocked", nil
	})))
	config := testConfig("text", false)
	initial := publish(t, engine, config, "initial")
	if _, err := engine.Execute(context.Background(), Request{TraceID: "stale", GroupID: "demo-group", Prompt: "prompt", ExpectedRevision: "obsolete"}); Code(err) != "conflict" {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		result, err := executeTest(engine, "trace")
		if result.RoutingRevision != initial.Revision || result.Text != "" {
			t.Error(result)
		}
		done <- err
	}()
	<-started
	config.Policies["demo-policy"].Capabilities[TextGeneration] = false
	publish(t, engine, config, "tighten")
	close(release)
	if err := <-done; Code(err) != "policy_blocked" {
		t.Fatal(err)
	}
}

func TestRoutingChangeKeepsOriginalFallbackChain(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	engine := openTest(t,
		textOption("primary", providerFunc(func(context.Context, ModelRequest) (string, error) {
			close(started)
			<-release
			return "", errors.New("unavailable")
		})),
		textOption("original", providerFunc(func(context.Context, ModelRequest) (string, error) { return "original fallback", nil })),
		textOption("replacement", providerFunc(func(context.Context, ModelRequest) (string, error) { return "new execution only", nil })))
	config := testConfig("primary", false)
	group := config.Groups["demo-group"]
	group.Fallbacks = []RouteAttempt{testAttempt("original")}
	config.Groups["demo-group"] = group
	initial := publish(t, engine, config, "initial")
	done := make(chan Result, 1)
	go func() {
		result, err := executeTest(engine, "trace")
		if err != nil {
			t.Error(err)
		}
		done <- result
	}()
	<-started
	group.Primary, group.Fallbacks = testAttempt("replacement"), []RouteAttempt{}
	config.Groups["demo-group"] = group
	updated := publish(t, engine, config, "reroute")
	close(release)
	result := <-done
	if result.Backend != "original" || result.Text != "original fallback" || result.RoutingRevision != initial.Revision || result.PolicyRevision != updated.Revision {
		t.Fatal(result)
	}
}

func TestCancelStopsPollingWithoutRemoteCancel(t *testing.T) {
	polled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method+" "+request.URL.Path != "POST /api/personal/dispatch" && request.Method+" "+request.URL.Path != "GET /api/personal/runs/demo-remote" {
			t.Error("unexpected endpoint", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if request.Method == http.MethodGet {
			select {
			case polled <- struct{}{}:
			default:
			}
		}
		_ = json.NewEncoder(writer).Encode(personalRun{ID: "demo-remote", Status: "running"})
	}))
	defer server.Close()
	engine := openTest(t, personalOption("personal", server.URL))
	publish(t, engine, testConfig("personal", true), "initial")
	done := make(chan error, 1)
	go func() {
		result, err := executeTest(engine, "trace")
		if len(result.Attempts) != 1 || result.Attempts[0].Status != "canceled" || result.Attempts[0].Reason != "canceled" || result.Attempts[0].RemoteExecution != "may_be_running" || result.Attempts[0].ExternalID != "demo-remote" || result.Text != "" {
			t.Error(result)
		}
		done <- err
	}()
	<-polled
	canceled, err := engine.Cancel("trace")
	if err != nil || !canceled.LocalStopRequested || canceled.LocalStopped || canceled.RemoteCancelSupported || canceled.RemoteExecution != "may_be_running" {
		t.Fatal(canceled, err)
	}
	if err := <-done; Code(err) != "execution_uncertain" {
		t.Fatal(err)
	}
	if _, err := engine.Cancel("missing"); Code(err) != "not_found" {
		t.Fatal(err)
	}
}
