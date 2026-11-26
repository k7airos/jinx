package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	"jinx/internal/execution"
	"jinx/internal/observability"
)

func TestLocalPolicyCLIFlagAndOperatorEnvironment(t *testing.T) {
	base := []string{"-config", "config.json", "-execution-config", "execution.json", "-backends-json", "operator.json"}
	t.Setenv("JINX_LOCAL_POLICY", "")
	args, err := parseArgs(base, io.Discard)
	if err != nil || args.localPolicy != "" {
		t.Fatal("optional policy was required", err)
	}
	t.Setenv("JINX_LOCAL_POLICY", "operator-policy.json")
	expected, err := filepath.Abs("operator-policy.json")
	if err != nil {
		t.Fatal(err)
	}
	args, err = parseArgs(base, io.Discard)
	if err != nil || args.localPolicy != expected {
		t.Fatal("operator policy environment was not used", err)
	}
	expected, err = filepath.Abs("explicit-policy.json")
	if err != nil {
		t.Fatal(err)
	}
	args, err = parseArgs(append(base, "-local-policy", "explicit-policy.json"), io.Discard)
	if err != nil || args.localPolicy != expected {
		t.Fatal("explicit policy did not override operator environment", err)
	}
	var output strings.Builder
	_, _ = parseArgs([]string{"-h"}, &output)
	if strings.Contains(output.String(), "operator-policy.json") {
		t.Fatal("flag help exposed the private environment path")
	}
}

func localPolicyFixture(t *testing.T, args arguments, endpoint string) (arguments, []string) {
	t.Helper()
	root := filepath.Dir(args.config)
	files := map[string]string{
		"communication.private.md": "DEMO_PRIVATE_BRAND_TEXT",
		"instructions.private.md":  "DEMO_PRIVATE_SUPPORT_INSTRUCTIONS",
		"knowledge.private.md":     "DEMO_PRIVATE_KNOWLEDGE_FACT",
		"document.private.md":      "DEMO_PRIVATE_DOCUMENT_FACT",
	}
	private := []string{"demo-private-memory", "demo-private-collection"}
	for name, text := range files {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
		private = append(private, path, text)
	}
	args.localPolicy = filepath.Join(root, "policy.private.json")
	private = append(private, args.localPolicy)
	writeJSON(t, args.localPolicy, map[string]any{
		"version": 1, "communication_file": filepath.Join(root, "communication.private.md"),
		"identities": map[string]any{"100@chatroom": map[string]string{
			"profile": "demo-100", "memory_user": "demo-private-memory", "vector_collection": "demo-private-collection",
		}},
		"support": map[string]any{
			"enabled": true, "group_id": "100@chatroom", "self_sender_ids": []string{"synthetic-self"},
			"instructions_file": filepath.Join(root, "instructions.private.md"),
			"knowledge_files":   []string{filepath.Join(root, "knowledge.private.md")},
			"document_files":    []string{filepath.Join(root, "document.private.md")},
			"reminders":         map[string]bool{"enabled": false},
		},
	})
	data, err := os.ReadFile(args.config)
	if err != nil {
		t.Fatal(err)
	}
	var config dispatcher.Config
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	config.Bridge.DryRun = false
	config.Bridge.URL = endpoint + "/send"
	config.Relay.StreamURL = endpoint + "/stream"
	config.Rules.SelfSenderIDs = []string{"synthetic-self"}
	config.Rules.GroupWhitelist = []string{"100@chatroom"}
	config.Rules.GroupRules = map[string]dispatcher.GroupRule{"100@chatroom": {
		Enabled: true, Target: "Demo Room", TargetGroupID: "100@chatroom", HermesProfile: "demo-100",
		SupportMode: true, SupportSinceMS: time.Now().Add(-time.Hour).UnixMilli(), SelfSenderIDs: []string{"synthetic-self"},
	}}
	writeJSON(t, args.config, config)
	return args, private
}

func localResponse(t *testing.T, service dispatcherService, method, path, body, etag string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, "http://127.0.0.1"+path, strings.NewReader(body))
	request.RemoteAddr = "127.0.0.1:1234"
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("If-Match", etag)
	}
	response := httptest.NewRecorder()
	service.Handler().ServeHTTP(response, request)
	return response
}

func rejectPrivateOutput(t *testing.T, data string, private []string) {
	t.Helper()
	for _, value := range private {
		if value != "" && strings.Contains(data, value) {
			t.Fatal("private policy content exposed")
		}
	}
}

func TestLocalPolicyPathFactoryValidation(t *testing.T) {
	t.Setenv("JINX_LOCAL_POLICY", "")
	for _, scenario := range []string{"valid", "missing", "invalid_schema", "wrong_identity", "insecure", "symlink", "absent"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("construction contacted a service")
			}))
			defer server.Close()
			args, private := localPolicyFixture(t, configuredArguments(t, server.URL, execution.TextOnly), server.URL)
			switch scenario {
			case "missing":
				args.localPolicy = filepath.Join(filepath.Dir(args.config), "missing-policy.json")
			case "invalid_schema":
				writeJSON(t, args.localPolicy, map[string]any{"version": 1, "unexpected": "DEMO_PRIVATE_UNKNOWN_VALUE"})
			case "wrong_identity":
				writeJSON(t, args.localPolicy, map[string]any{"version": 1, "identities": map[string]any{"100@chatroom": map[string]string{
					"profile": "wrong-demo-profile", "memory_user": "demo-private-memory", "vector_collection": "demo-private-collection",
				}}, "support": map[string]any{"enabled": true, "group_id": "100@chatroom", "self_sender_ids": []string{"synthetic-self"}}})
			case "insecure":
				if err := os.Chmod(args.localPolicy, 0644); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				link := filepath.Join(filepath.Dir(args.config), "policy-link.json")
				if err := os.Symlink(args.localPolicy, link); err != nil {
					t.Fatal(err)
				}
				args.localPolicy = link
			case "absent":
				args.localPolicy = ""
			}
			calls := 0
			service, _, err := loadDispatcher(args, func(path string, options dispatcher.Options) (dispatcherService, error) {
				calls++
				if path != args.config || options.LocalPolicyPath != args.localPolicy || options.Runner == nil {
					t.Fatal("CLI did not pass policy path and runner into factory")
				}
				return dispatcher.NewDispatcherWithOptions(path, options)
			})
			if calls != 1 {
				t.Fatal("factory was not invoked")
			}
			if scenario == "valid" {
				if err != nil {
					t.Fatal("valid local policy rejected", err)
				}
				defer service.Close()
				response := localResponse(t, service, http.MethodGet, "/api/config", "", "")
				if response.Code != http.StatusOK {
					t.Fatal("config read failed", response.Code)
				}
				rejectPrivateOutput(t, response.Body.String(), private)
			} else {
				if err == nil {
					service.Close()
					t.Fatal("invalid local policy accepted")
				}
				rejectPrivateOutput(t, err.Error(), append(private, args.localPolicy, "DEMO_PRIVATE_UNKNOWN_VALUE"))
			}
		})
	}
}

func TestLocalPolicyStartupPromptAndBridgeRemainPrivate(t *testing.T) {
	t.Setenv("JINX_LOCAL_POLICY", "")
	events := make(chan dispatcher.RelayMessage, 8)
	prompts := make(chan dispatcher.BackendRequest, 1)
	deliveries := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method + " " + request.URL.Path {
		case "GET /stream":
			writer.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(writer, "event: connected\nid: fixture:0\ndata: {}\n\n")
			writer.(http.Flusher).Flush()
			for {
				select {
				case message := <-events:
					data, err := json.Marshal(message)
					if err != nil {
						t.Error("encode relay fixture", err)
						return
					}
					_, _ = fmt.Fprintf(writer, "event: message\nid: fixture:%s\ndata: %s\n\n", message.ID, data)
					writer.(http.Flusher).Flush()
				case <-request.Context().Done():
					return
				}
			}
		case "GET /send/health":
			_, _ = io.WriteString(writer, `{"status":"ready","protocol_version":2,"group_routing":true,"serial_ui":true,"accessibility_trusted":true,"screen_capture_authorized":true,"dry_run":false}`)
		case "POST /send":
			var delivery struct {
				GroupID string `json:"group_id"`
				Text    string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&delivery); err != nil {
				t.Error("invalid bridge fixture payload", err)
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			if delivery.GroupID != "100@chatroom" || delivery.Text != "demo fixture reply" {
				t.Error("bridge did not receive the synthetic route and reply")
			}
			deliveries <- delivery.Text
			events <- dispatcher.RelayMessage{ID: "fixture-echo", GroupID: delivery.GroupID, SenderID: "synthetic-self", Type: 1, Content: delivery.Text, Action: "forward", Timestamp: time.Now().Add(time.Millisecond).UnixMilli(), IsEcho: true}
			_, _ = io.WriteString(writer, "OK")
		default:
			t.Error("unexpected fixture endpoint")
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	args, private := localPolicyFixture(t, configuredArguments(t, server.URL, execution.TextOnly), server.URL)
	service, _, err := loadDispatcher(args, func(path string, options dispatcher.Options) (dispatcherService, error) {
		if options.LocalPolicyPath != args.localPolicy || options.Runner == nil {
			t.Fatal("CLI factory lost local policy or runtime runner")
		}
		options.Runner = dispatcher.RunFunc(func(ctx context.Context, request dispatcher.BackendRequest) (dispatcher.Result, error) {
			select {
			case prompts <- request:
			case <-ctx.Done():
				return dispatcher.Result{}, ctx.Err()
			}
			return dispatcher.Result{Text: "demo fixture reply", Backend: "Alma", RunID: request.TraceID}, nil
		})
		return dispatcher.NewDispatcherWithOptions(path, options)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	initial := localResponse(t, service, http.MethodGet, "/api/trace/snapshot", "", "")
	var before observability.Snapshot
	if initial.Code != http.StatusOK || json.Unmarshal(initial.Body.Bytes(), &before) != nil {
		t.Fatal("initial trace snapshot failed")
	}
	service.Start()
	events <- dispatcher.RelayMessage{ID: "fixture-question", GroupID: "100@chatroom", GroupName: "Demo Room", SenderID: "synthetic-member", Type: 1, Content: "这个功能怎么使用？", Action: "forward", Timestamp: time.Now().UnixMilli()}
	var generated dispatcher.BackendRequest
	select {
	case generated = <-prompts:
	case <-time.After(10 * time.Second):
		t.Fatal("local startup did not produce a prompt")
	}
	for _, expected := range []string{"DEMO_PRIVATE_BRAND_TEXT", "DEMO_PRIVATE_SUPPORT_INSTRUCTIONS", "DEMO_PRIVATE_KNOWLEDGE_FACT", "DEMO_PRIVATE_DOCUMENT_FACT"} {
		if !strings.Contains(generated.Prompt, expected) {
			t.Fatal("local policy did not affect the actual runner prompt")
		}
	}
	snapshot, err := json.Marshal(generated.ConfigSnapshot)
	if err != nil || generated.ConfigSnapshot == nil || generated.Profile != "demo-100" {
		t.Fatal("runner did not receive the route snapshot", err)
	}
	rejectPrivateOutput(t, string(snapshot), private)
	select {
	case text := <-deliveries:
		rejectPrivateOutput(t, text, private)
	case <-time.After(10 * time.Second):
		t.Fatal("isolated bridge did not receive a submission")
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		response := localResponse(t, service, http.MethodGet, "/api/trace/snapshot", "", "")
		rejectPrivateOutput(t, response.Body.String(), private)
		var trace observability.Snapshot
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &trace) != nil {
			t.Fatal("trace snapshot failed")
		}
		if len(trace.Runs) == 1 && trace.Runs[0].Delivery == contracts.DeliveryConfirmed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("synthetic bridge echo was not confirmed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, path := range []string{"/api/config", "/health", "/api/control/snapshot", "/api/trace/snapshot", "/api/trace/events?cursor=" + before.Cursor, "/api/trace/runs/" + generated.TraceID, "/api/trace/config-snapshots/" + generated.ConfigSnapshotID} {
		response := localResponse(t, service, http.MethodGet, path, "", "")
		if response.Code != http.StatusOK {
			t.Fatal("public read failed", path, response.Code)
		}
		rejectPrivateOutput(t, response.Body.String(), private)
	}
	read := localResponse(t, service, http.MethodGet, "/api/config", "", "")
	for _, body := range []string{`{"local_policy":"untrusted.json"}`, `{"group_rules":{}}`} {
		response := localResponse(t, service, http.MethodPost, "/api/config", body, read.Header().Get("ETag"))
		if response.Code != http.StatusBadRequest {
			t.Fatal("HTTP changed operator-owned policy constraints", response.Code)
		}
		rejectPrivateOutput(t, response.Body.String(), private)
	}
}
