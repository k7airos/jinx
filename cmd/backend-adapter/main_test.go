package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"jinx/internal/execution"
)

func TestStrictCompatibilityArguments(t *testing.T) {
	t.Setenv("JINX_ROUTING_PATH", "/fictional/execution.json")
	t.Setenv("JINX_OPERATOR_PATH", "/fictional/operator.json")
	base := []string{"-p", "demo-profile", "chat", "--query-file", "-", "-Q", "--format", "stream-json"}
	for _, extra := range [][]string{nil, {"--resume", "alma:old-run"}, {"--resume=old-run", "--max-turns", "8", "-t", "memory,skills,web"}} {
		if _, err := parseArgs(append(append([]string{}, base...), extra...)); err != nil {
			t.Fatal(err)
		}
	}
	for _, extra := range [][]string{{"--command", "sh"}, {"--provider", "attacker"}, {"--support-mode"}, {"-p", "duplicate"}, {"-t", "shell"}, {"--resume"}, {"--format", "json"}} {
		if _, err := parseArgs(append(append([]string{}, base...), extra...)); err == nil {
			t.Fatal("accepted arguments", extra)
		}
	}
}

func TestOfflineCLIAlmaDispatchPollResult(t *testing.T) {
	var dispatches, polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == "POST" && request.URL.Path == "/api/personal/dispatch" {
			dispatches.Add(1)
			var body map[string]any
			_ = json.NewDecoder(request.Body).Decode(&body)
			if body["model"] != "demo-provider:demo-model" || body["agent_id"] != nil || !strings.Contains(body["task"].(string), "offline prompt") {
				t.Error(body)
			}
			_, _ = writer.Write([]byte(`{"id":"demo-remote","status":"waiting","threadId":"demo-thread"}`))
			return
		}
		if request.Method == "GET" && request.URL.Path == "/api/personal/runs/demo-remote" {
			polls.Add(1)
			_, _ = writer.Write([]byte(`{"run":{"id":"demo-remote","status":"done","summary":"offline answer"}}`))
			return
		}
		t.Error("unexpected endpoint", request.Method, request.URL)
		writer.WriteHeader(404)
	}))
	defer server.Close()
	directory := t.TempDir()
	routingPath, operatorPath := filepath.Join(directory, "execution.json"), filepath.Join(directory, "operator.json")
	operator := operatorFile{SchemaVersion: "1", Backends: []operatorBackend{{ID: "personal", Kind: execution.AlmaPersonal, BaseURL: server.URL,
		Models: []execution.Model{{ID: "demo-provider:demo-model", Label: "Fictional model"}}}}}
	data, _ := json.Marshal(operator)
	if err := os.WriteFile(operatorPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	options, err := readOptions(operatorPath)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := execution.Open(routingPath, options)
	if err != nil {
		t.Fatal(err)
	}
	capabilities := map[execution.Capability]bool{}
	for _, capability := range execution.Capabilities() {
		capabilities[capability] = capability == execution.TextGeneration || capability == execution.AutoTextDelivery
	}
	config := execution.Config{SchemaVersion: "1", Profiles: map[string]string{"demo-profile": "demo-group"},
		Policies: map[string]execution.Policy{"demo-policy": {Capabilities: capabilities, AllowSoft: true}},
		Groups: map[string]execution.Group{"demo-group": {RouteID: "demo-route", PolicyID: "demo-policy", Primary: execution.RouteAttempt{
			BackendID: "personal", ModelID: "demo-provider:demo-model", TimeoutSec: 3, Session: execution.SessionPolicy{Mode: "independent_run"}}, Fallbacks: []execution.RouteAttempt{}}}}
	before, _ := engine.Snapshot(context.Background())
	if _, err := engine.ApplyConfig(context.Background(), before.Revision, "publish", config); err != nil {
		t.Fatal(err)
	}
	argv := []string{"--operator", operatorPath, "--routing", routingPath, "-p", "demo-profile", "chat", "--query-file", "-", "-Q", "--format", "stream-json", "--resume", "alma:unrelated-old-run", "-t", "memory,skills,web", "--max-turns", "8"}
	var stdout bytes.Buffer
	if status := run(context.Background(), argv, strings.NewReader("offline prompt"), &stdout); status != 0 {
		t.Fatal(status, stdout.String())
	}
	var event resultEvent
	if err := json.Unmarshal(stdout.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if event.Type != "result" || event.Text != "offline answer" || event.ExitCode != 0 || event.SessionID != "alma:demo-remote" || event.Backend != "Alma" || event.BackendID != "personal" || event.RunID != "demo-remote" || event.Fallback || event.Execution == nil || dispatches.Load() != 1 || polls.Load() != 1 {
		t.Fatalf("unexpected result %+v", event)
	}
	noToolsArgs := append([]string{}, argv...)
	for index, value := range noToolsArgs {
		if value == "-t" {
			noToolsArgs[index+1] = "none"
		}
	}
	stdout.Reset()
	if status := run(context.Background(), noToolsArgs, strings.NewReader("offline prompt"), &stdout); status != 1 || dispatches.Load() != 1 || !strings.Contains(stdout.String(), "no_tools_requires_hard_routing_policy") {
		t.Fatal(status, stdout.String(), dispatches.Load())
	}
	stdout.Reset()
	if status := run(context.Background(), append(argv, "-m", "attacker:model"), strings.NewReader("offline prompt"), &stdout); status != 1 || dispatches.Load() != 1 || !strings.Contains(stdout.String(), "model_override_rejected") {
		t.Fatal(status, stdout.String(), dispatches.Load())
	}
}

func TestOfflineCLIUncertainExecutionRetainsAttempts(t *testing.T) {
	for _, scenario := range []string{"dispatch_partial_lost", "poll_wrong_id", "poll_http"} {
		t.Run(scenario, func(t *testing.T) {
			var dispatches, polls, fallbackCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.Method + " " + request.URL.Path {
				case "POST /api/personal/dispatch":
					dispatches.Add(1)
					if scenario == "dispatch_partial_lost" {
						connection, buffered, err := writer.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						defer connection.Close()
						_, _ = buffered.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n{\"id\":\"PRIVATE")
						_ = buffered.Flush()
						return
					}
					_, _ = writer.Write([]byte(`{"id":"demo-remote","status":"waiting","threadId":"demo-thread"}`))
				case "GET /api/personal/runs/demo-remote":
					polls.Add(1)
					if scenario == "poll_wrong_id" {
						_, _ = writer.Write([]byte(`{"id":"PRIVATE-other-run","status":"done","summary":"PRIVATE other run answer"}`))
					} else {
						writer.WriteHeader(http.StatusBadGateway)
						_, _ = writer.Write([]byte("PRIVATE backend diagnostic"))
					}
				case "POST /v1/chat/completions":
					fallbackCalls.Add(1)
					_, _ = writer.Write([]byte(`{"choices":[{"finish_reason":"stop","message":{"content":"unsafe retry"}}]}`))
				default:
					t.Error("unexpected endpoint", request.Method, request.URL.Path)
					writer.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			directory := t.TempDir()
			routingPath, operatorPath := filepath.Join(directory, "execution.json"), filepath.Join(directory, "operator.json")
			models := []execution.Model{{ID: "demo-provider:demo-model", Label: "Fictional model"}}
			operator := operatorFile{SchemaVersion: "1", Backends: []operatorBackend{
				{ID: "personal", Kind: execution.AlmaPersonal, BaseURL: server.URL, Models: models},
				{ID: "text", Kind: execution.TextOnly, BaseURL: server.URL, TextAPI: "/v1/chat/completions", Models: models},
			}}
			data, err := json.Marshal(operator)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(operatorPath, data, 0600); err != nil {
				t.Fatal(err)
			}
			options, err := readOptions(operatorPath)
			if err != nil {
				t.Fatal(err)
			}
			engine, err := execution.Open(routingPath, options)
			if err != nil {
				t.Fatal(err)
			}
			capabilities := map[execution.Capability]bool{}
			for _, capability := range execution.Capabilities() {
				capabilities[capability] = capability == execution.TextGeneration || capability == execution.AutoTextDelivery
			}
			primary := execution.RouteAttempt{BackendID: "personal", ModelID: models[0].ID, TimeoutSec: 3, Session: execution.SessionPolicy{Mode: "independent_run"}}
			fallback := primary
			fallback.BackendID = "text"
			config := execution.Config{SchemaVersion: "1", Profiles: map[string]string{"demo-profile": "demo-group"},
				Policies: map[string]execution.Policy{"demo-policy": {Capabilities: capabilities, AllowSoft: true}},
				Groups:   map[string]execution.Group{"demo-group": {RouteID: "demo-route", PolicyID: "demo-policy", Primary: primary, Fallbacks: []execution.RouteAttempt{fallback}}}}
			before, err := engine.Snapshot(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := engine.ApplyConfig(context.Background(), before.Revision, "initial", config); err != nil {
				t.Fatal(err)
			}
			argv := []string{"--operator", operatorPath, "--routing", routingPath, "-p", "demo-profile", "chat", "--query-file", "-", "-Q", "--format", "stream-json"}
			var stdout bytes.Buffer
			if status := run(context.Background(), argv, strings.NewReader("PRIVATE prompt"), &stdout); status != 1 {
				t.Fatal(status, stdout.String())
			}
			var event resultEvent
			if err := json.Unmarshal(stdout.Bytes(), &event); err != nil {
				t.Fatal(err)
			}
			if event.ExitCode != 1 || event.Error != "execution_uncertain" || event.Text != "" || event.Fallback || event.Execution == nil || event.RunID != event.Execution.RunID || event.Execution.RunID == "" || event.Execution.DeliveryAllowed || len(event.Execution.Attempts) != 1 || dispatches.Load() != 1 || fallbackCalls.Load() != 0 || strings.Contains(stdout.String(), "PRIVATE") {
				t.Fatalf("unexpected result %+v", event)
			}
			attempt := event.Execution.Attempts[0]
			expectedID, expectedReason, expectedPolls := "demo-remote", "backend_http_error", int32(1)
			if scenario == "dispatch_partial_lost" {
				expectedID, expectedReason, expectedPolls = "", "invalid_output", 0
			} else if scenario == "poll_wrong_id" {
				expectedReason = "invalid_output"
			}
			if attempt.BackendID != "personal" || attempt.Status != "failed" || attempt.RemoteExecution != "may_be_running" || attempt.ExternalID != expectedID || attempt.Reason != expectedReason || polls.Load() != expectedPolls {
				t.Fatal(attempt, polls.Load())
			}
		})
	}
}
