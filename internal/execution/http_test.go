package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestControlledHTTPTextOnlyAndRejectToolCalls(t *testing.T) {
	for _, scenario := range []string{"text", "tools", "function", "truncated"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.Method != "POST" || request.URL.Path != "/v1/chat/completions" {
					t.Error("unexpected endpoint")
				}
				var body map[string]any
				_ = json.NewDecoder(request.Body).Decode(&body)
				if body["tools"] != nil || body["functions"] != nil || body["stream"] != false || body["tool_choice"] != "none" || body["model"] != "demo-provider:demo-model" {
					t.Errorf("unexpected generation request %#v", body)
				}
				message := map[string]any{"content": "safe reply"}
				finish := "stop"
				if scenario == "tools" {
					message["tool_calls"] = []any{map[string]any{"function": map[string]any{"name": "shell", "arguments": "never executed"}}}
				} else if scenario == "function" {
					message["function_call"] = map[string]any{"name": "shell"}
				} else if scenario == "truncated" {
					finish = "length"
				}
				_ = json.NewEncoder(writer).Encode(map[string]any{"choices": []any{map[string]any{"finish_reason": finish, "message": message}}})
			}))
			defer server.Close()
			provider, err := NewHTTPTextProvider(HTTPTextOptions{BaseURL: server.URL, Path: "/v1/chat/completions"})
			if err != nil {
				t.Fatal(err)
			}
			engine := openTest(t, textOption("text", provider))
			publish(t, engine, testConfig("text", false), "initial")
			result, err := engine.Execute(context.Background(), Request{TraceID: "trace", GroupID: "demo-group", Prompt: "prompt", Messages: []Message{{Role: "assistant", Content: "history"}}})
			if scenario == "text" {
				if err != nil || result.Text != "safe reply" || len(result.Attempts) != 1 {
					t.Fatal(result, err)
				}
			} else if Code(err) != "invalid_output" || result.Text != "" {
				t.Fatal(result, err)
			}
		})
	}
}

func TestLoopbackAndRedirectBoundaries(t *testing.T) {
	for _, endpoint := range []string{"https://127.0.0.1:1234", "http://example.invalid:1234", "http://localhost:1234", "http://user:pass@127.0.0.1:1234", "http://127.0.0.1:1234/path", "http://127.0.0.1:1234?secret=value"} {
		if _, err := NewHTTPTextProvider(HTTPTextOptions{BaseURL: endpoint, Path: "/v1/chat/completions"}); err == nil {
			t.Fatal(endpoint)
		}
	}
	reached := false
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { reached = true }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	provider, err := NewHTTPTextProvider(HTTPTextOptions{BaseURL: redirect.URL, Path: "/v1/chat/completions"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Generate(context.Background(), ModelRequest{ModelID: "demo-provider:demo-model", Messages: []Message{{Role: "user", Content: "prompt"}}})
	if err == nil || reached {
		t.Fatal("redirect followed", err)
	}
}

func TestLegacyHermesUsesFixedOperatorCommandAndFreshSession(t *testing.T) {
	directory := t.TempDir()
	command := filepath.Join(directory, "demo-hermes")
	argsPath := filepath.Join(directory, "args")
	inputPath := filepath.Join(directory, "stdin")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$DEMO_ARGS\"\ncat > \"$DEMO_INPUT\"\nprintf '%s\\n' '{\"type\":\"tool_result\",\"text\":\"not a reply\"}' '{\"type\":\"result\",\"text\":\"legacy reply\",\"session_id\":\"demo-session\",\"exit_code\":0}'\n"
	if err := os.WriteFile(command, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	option := BackendOptions{ID: "legacy", Kind: LegacyHermes, Models: []Model{{ID: "demo-provider:demo-model", Label: "Fictional model"}}, Hermes: HermesOptions{
		Command: command, Dir: directory, Env: []string{"DEMO_ARGS=" + argsPath, "DEMO_INPUT=" + inputPath}, Profile: "fixed-profile", Toolsets: []string{"web"}, MaxTurns: 3}}
	engine := openTest(t, option)
	publish(t, engine, testConfig("legacy", true), "initial")
	result, err := executeTest(engine, "trace")
	if err != nil || result.Text != "legacy reply" || result.Kind != LegacyHermes {
		t.Fatal(result, err)
	}
	data, _ := os.ReadFile(argsPath)
	expected := "-p\nfixed-profile\nchat\n--query-file\n-\n-Q\n--format\nstream-json\n--max-turns\n3\n-m\ndemo-provider:demo-model\n-t\nweb\n"
	if string(data) != expected {
		t.Fatalf("unexpected args %q", data)
	}
	input, _ := os.ReadFile(inputPath)
	if string(input) != "fictional prompt" {
		t.Fatal(string(input))
	}
	config := testConfig("legacy", false)
	if err := engine.Validate(context.Background(), config); Code(err) != "policy_mismatch" {
		t.Fatal(err)
	}
}

func TestLegacyHermesUncertainExecutionBlocksFallback(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		script string
		reason string
		remote string
		safe   bool
	}{
		{"failed", "#!/bin/sh\nprintf 'PRIVATE STDERR' >&2\nexit 1\n", "backend_failed", "unknown", false},
		{"timeout", "#!/bin/sh\nexec sleep 30\n", "timeout", "unknown", false},
		{"invalid_result", "#!/bin/sh\nprintf '%s\\n' '{\"type\":\"result\",\"text\":\"PRIVATE RESULT\",\"session_id\":\"secret\",\"exit_code\":1}'\n", "invalid_output", "unknown", false},
		{"no_result", "#!/bin/sh\nprintf 'PRIVATE OUTPUT'\n", "invalid_output", "unknown", false},
		{"not_started", "", "backend_failed", "not_applicable", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			directory := t.TempDir()
			command := filepath.Join(directory, "demo-hermes")
			if scenario.script != "" {
				if err := os.WriteFile(command, []byte(scenario.script), 0700); err != nil {
					t.Fatal(err)
				}
			}
			option := BackendOptions{ID: "legacy", Kind: LegacyHermes, Models: []Model{{ID: "demo-provider:demo-model", Label: "Fictional model"}}, Hermes: HermesOptions{
				Command: command, Dir: directory, Profile: "fixed-profile", Toolsets: []string{"web"}, MaxTurns: 3}}
			var fallbackCalls atomic.Int32
			engine := openTest(t, option, textOption("text", providerFunc(func(context.Context, ModelRequest) (string, error) {
				fallbackCalls.Add(1)
				return "safe fallback", nil
			})))
			config := testConfig("legacy", true)
			group := config.Groups["demo-group"]
			group.Fallbacks = []RouteAttempt{testAttempt("text")}
			config.Groups["demo-group"] = group
			publish(t, engine, config, "initial")
			result, err := executeTest(engine, "trace")
			if scenario.safe {
				if err != nil || !result.Fallback || result.Text != "safe fallback" || len(result.Attempts) != 2 || fallbackCalls.Load() != 1 {
					t.Fatal(result, err)
				}
			} else if Code(err) != "execution_uncertain" || result.Fallback || result.Text != "" || len(result.Attempts) != 1 || fallbackCalls.Load() != 0 {
				t.Fatal(result, err)
			}
			if result.Attempts[0].RemoteExecution != scenario.remote || result.Attempts[0].Reason != scenario.reason || strings.Contains(fmt.Sprint(result, err), "PRIVATE") {
				t.Fatal(result, err)
			}
		})
	}
}

func TestStrictConfigRejectsOversize(t *testing.T) {
	_, err := DecodeConfig(io.LimitReader(strings.NewReader(strings.Repeat(" ", maxConfigBytes+1)), maxConfigBytes+1))
	if Code(err) != "too_large" {
		t.Fatal(err)
	}
}

func TestAgyCLIRunsStreamJSONAndCapturesConversationID(t *testing.T) {
	directory := t.TempDir()
	command := filepath.Join(directory, "demo-agy")
	argsPath := filepath.Join(directory, "args")
	inputPath := filepath.Join(directory, "stdin")
	settingsPath := filepath.Join(directory, "settings.json")
	if err := os.WriteFile(settingsPath, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$DEMO_ARGS\"\ncat > \"$DEMO_INPUT\"\nprintf '%s\\n' " +
		"'{\"event\":\"init\",\"conversation_id\":\"conv-agy-1\"}' " +
		"'{\"event\":\"step_update\",\"step_update\":{\"state\":\"DONE\",\"step_type\":\"agent_response\",\"text_delta\":\"agy reply\"}}' " +
		"'{\"event\":\"result\",\"result\":{\"conversation_id\":\"conv-agy-1\",\"status\":\"SUCCESS\",\"response\":\"agy reply\\n\"}}'\n"
	if err := os.WriteFile(command, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	option := BackendOptions{
		ID:     "agy",
		Kind:   AgyCLI,
		Models: []Model{{ID: "gemini-4-hydrogen-low", Label: "Hydrogen Low"}},
		Agy: AgyOptions{
			Command:              command,
			Dir:                  directory,
			Env:                  []string{"DEMO_ARGS=" + argsPath, "DEMO_INPUT=" + inputPath},
			Agent:                "comment-helper",
			Effort:               "low",
			Sandbox:              true,
			DisableSlashCommands: true,
			SettingsFile:         settingsPath,
			AddDirs:              []string{directory},
		},
	}
	engine := openTest(t, option)
	config := testConfig("agy", true)
	group := config.Groups["demo-group"]
	group.Primary.ModelID = "gemini-4-hydrogen-low"
	group.Primary.TimeoutSec = 15
	config.Groups["demo-group"] = group
	publish(t, engine, config, "initial")

	result, err := executeTest(engine, "trace")
	if err != nil || result.Text != "agy reply" || result.Kind != AgyCLI || len(result.Attempts) != 1 || result.Attempts[0].ExternalID != "conv-agy-1" {
		t.Fatal(result, err)
	}
	data, _ := os.ReadFile(argsPath)
	expected := "--input-format\nstream-json\n--output-format\nstream-json\n--model\ngemini-4-hydrogen-low\n--print-timeout\n15s\n--effort\nlow\n--agent\ncomment-helper\n--sandbox\n--disable-slash-commands\n--settings\n" + settingsPath + "\n--add-dir\n" + directory + "\n"
	if string(data) != expected {
		t.Fatalf("unexpected agy args %q", data)
	}
	input, _ := os.ReadFile(inputPath)
	var envelope struct {
		Event   string `json:"event"`
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(input), &envelope); err != nil || envelope.Event != "user" || envelope.Message.Content != "fictional prompt" {
		t.Fatalf("unexpected stdin payload %q err=%v", input, err)
	}
	noTools := testConfig("agy", false)
	noToolsGroup := noTools.Groups["demo-group"]
	noToolsGroup.Primary.ModelID = "gemini-4-hydrogen-low"
	noTools.Groups["demo-group"] = noToolsGroup
	if err := engine.Validate(context.Background(), noTools); Code(err) != "policy_mismatch" {
		t.Fatal(err)
	}
}

func TestAgyCLIUncertainExecutionBlocksFallback(t *testing.T) {
	for _, scenario := range []struct {
		name   string
		script string
		reason string
		remote string
		safe   bool
	}{
		{"failed", "#!/bin/sh\nprintf 'PRIVATE STDERR' >&2\nexit 1\n", "backend_failed", "unknown", false},
		{"timeout", "#!/bin/sh\nexec sleep 30\n", "timeout", "unknown", false},
		{"error_result", "#!/bin/sh\nprintf '%s\\n' '{\"event\":\"result\",\"result\":{\"conversation_id\":\"conv-agy-err\",\"status\":\"ERROR\",\"response\":\"\",\"error\":\"PRIVATE ERROR\"}}'\n", "invalid_output", "unknown", false},
		{"no_result", "#!/bin/sh\nprintf 'PRIVATE OUTPUT'\n", "invalid_output", "unknown", false},
		{"not_started", "", "backend_failed", "not_applicable", true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			directory := t.TempDir()
			command := filepath.Join(directory, "demo-agy")
			if scenario.script != "" {
				if err := os.WriteFile(command, []byte(scenario.script), 0700); err != nil {
					t.Fatal(err)
				}
			}
			option := BackendOptions{
				ID:     "agy",
				Kind:   AgyCLI,
				Models: []Model{{ID: "demo-provider:demo-model", Label: "Fictional model"}},
				Agy:    AgyOptions{Command: command, Dir: directory, Sandbox: true},
			}
			var fallbackCalls atomic.Int32
			engine := openTest(t, option, textOption("text", providerFunc(func(context.Context, ModelRequest) (string, error) {
				fallbackCalls.Add(1)
				return "safe fallback", nil
			})))
			config := testConfig("agy", true)
			group := config.Groups["demo-group"]
			group.Fallbacks = []RouteAttempt{testAttempt("text")}
			config.Groups["demo-group"] = group
			publish(t, engine, config, "initial")
			result, err := executeTest(engine, "trace")
			if scenario.safe {
				if err != nil || !result.Fallback || result.Text != "safe fallback" || len(result.Attempts) != 2 || fallbackCalls.Load() != 1 {
					t.Fatal(result, err)
				}
			} else if Code(err) != "execution_uncertain" || result.Fallback || result.Text != "" || len(result.Attempts) != 1 || fallbackCalls.Load() != 0 {
				t.Fatal(result, err)
			}
			if result.Attempts[0].RemoteExecution != scenario.remote || result.Attempts[0].Reason != scenario.reason || strings.Contains(fmt.Sprint(result, err), "PRIVATE") {
				t.Fatal(result, err)
			}
		})
	}
}
