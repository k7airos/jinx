package runtimebridge

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"jinx/internal/execution"
)

func TestAnthropicMessagesTextOnly(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Method != "POST" || request.URL.Path != "/anthropic-proxy/mock-provider/v1/messages" || request.Header.Get("anthropic-version") != "2023-06-01" {
			t.Error("unexpected Anthropic endpoint", request.URL)
		}
		var body map[string]json.RawMessage
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if len(body) != 5 || body["tools"] != nil || body["tool_choice"] != nil || string(body["stream"]) != "false" || string(body["model"]) != `"mock-model"` || string(body["system"]) != `"trusted rules"` {
			t.Error("unexpected Anthropic request", body)
		}
		var messages []execution.Message
		if json.Unmarshal(body["messages"], &messages) != nil || len(messages) != 1 || messages[0].Content != "synthetic prompt" {
			t.Error(messages)
		}
		_, _ = io.WriteString(writer, `{"type":"message","role":"assistant","model":"mock-model","stop_reason":"end_turn","content":[{"type":"text","text":"first "},{"type":"text","text":"second"}]}`)
	}))
	defer server.Close()
	provider, err := NewAnthropicProvider(server.URL, "mock-provider")
	if err != nil || calls.Load() != 0 {
		t.Fatal("constructor called upstream", err)
	}
	text, err := provider.Generate(context.Background(), execution.ModelRequest{ModelID: testModel,
		Messages: []execution.Message{{Role: "system", Content: "trusted rules"}, {Role: "user", Content: "synthetic prompt"}}})
	if err != nil || text != "first second" || calls.Load() != 1 {
		t.Fatal(text, err)
	}
}

func TestAnthropicRejectsNonTextAndMalformedResponses(t *testing.T) {
	for _, scenario := range []string{"tool_use", "server_tool_use", "thinking", "mixed_tool", "tool_stop", "max_tokens", "empty", "wrong_model", "invalid_json", "duplicate_key", "case_duplicate", "oversized", "http_error"} {
		t.Run(scenario, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				response := map[string]any{"type": "message", "role": "assistant", "model": testModel, "stop_reason": "end_turn", "content": []any{map[string]any{"type": "text", "text": "safe reply"}}}
				switch scenario {
				case "tool_use", "server_tool_use", "thinking":
					response["content"] = []any{map[string]any{"type": scenario, "name": "shell", "input": "never executed"}}
				case "mixed_tool":
					response["content"] = []any{map[string]any{"type": "text", "text": "partial text"}, map[string]any{"type": "tool_use", "name": "shell"}}
				case "tool_stop":
					response["stop_reason"] = "tool_use"
				case "max_tokens":
					response["stop_reason"] = "max_tokens"
				case "wrong_model":
					response["model"] = "unexpected-model"
				case "empty":
					response["content"] = []any{map[string]any{"type": "text", "text": " "}}
				case "invalid_json":
					_, _ = io.WriteString(writer, `{"not":"complete"} trailing`)
					return
				case "duplicate_key":
					_, _ = io.WriteString(writer, `{"type":"message","role":"assistant","model":"mock-model","stop_reason":"end_turn","content":[{"type":"tool_use","type":"text","text":"hidden tool"}]}`)
					return
				case "case_duplicate":
					_, _ = io.WriteString(writer, `{"type":"message","role":"assistant","model":"mock-model","stop_reason":"end_turn","content":[{"type":"tool_use","Type":"text","text":"hidden tool"}]}`)
					return
				case "oversized":
					_, _ = io.WriteString(writer, strings.Repeat(" ", maxResponseBytes+1))
					return
				case "http_error":
					writer.WriteHeader(http.StatusBadGateway)
					_, _ = io.WriteString(writer, "PRIVATE ERROR BODY")
					return
				}
				_ = json.NewEncoder(writer).Encode(response)
			}))
			defer server.Close()
			provider, err := NewAnthropicProvider(server.URL, "mock-provider")
			if err != nil {
				t.Fatal(err)
			}
			text, err := provider.Generate(context.Background(), execution.ModelRequest{ModelID: testModel, Messages: []execution.Message{{Role: "user", Content: "prompt"}}})
			if err == nil || text != "" || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatal("invalid output admitted or leaked", text, err)
			}
		})
	}
}

func TestAnthropicRejectsInvalidInputsBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { calls.Add(1) }))
	defer server.Close()
	provider, err := NewAnthropicProvider(server.URL, "mock-provider")
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []execution.ModelRequest{
		{ModelID: testModel, Messages: []execution.Message{{Role: "tool", Content: "never executed"}}},
		{ModelID: testModel, Messages: []execution.Message{{Role: "system", Content: "rules only"}}},
		{ModelID: testModel, Messages: []execution.Message{{Role: "user", Content: "input"}, {Role: "system", Content: "out of order"}}},
		{ModelID: testModel, Messages: []execution.Message{{Role: "user", Content: strings.Repeat("x", maxRequestBytes+1)}}},
		{Messages: []execution.Message{{Role: "user", Content: "missing model"}}},
	} {
		if text, err := provider.Generate(context.Background(), request); err == nil || text != "" {
			t.Fatal("invalid input admitted", err)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input reached network")
	}
}

func TestAnthropicOriginRedirectAndProxyBoundaries(t *testing.T) {
	for _, origin := range []string{"https://127.0.0.1:1234", "http://localhost:1234", "http://example.invalid:1234", "http://192.0.2.1:1234", "http://127.0.0.1", "http://127.0.0.1:0", "http://127.0.0.1:65536", "http://user:pass@127.0.0.1:1234", "http://127.0.0.1:1234/path", "http://127.0.0.1:1234/%2f", "http://127.0.0.1:1234?", "http://127.0.0.1:1234#"} {
		if _, err := NewAnthropicProvider(origin, "mock-provider"); err == nil {
			t.Fatal("unsafe origin accepted", origin)
		}
	}
	for _, id := range []string{"", "../shell", "provider?proxy=bad", "provider/name", "provider#fragment"} {
		if _, err := NewAnthropicProvider("http://127.0.0.1:1234", id); err == nil {
			t.Fatal("unsafe provider accepted", id)
		}
	}
	var reached atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { reached.Add(1) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	t.Setenv("HTTP_PROXY", target.URL)
	t.Setenv("HTTPS_PROXY", target.URL)
	t.Setenv("ALL_PROXY", target.URL)
	provider, err := NewAnthropicProvider(redirect.URL, "mock-provider")
	if err != nil {
		t.Fatal(err)
	}
	if provider.client.Transport.(*http.Transport).Proxy != nil {
		t.Fatal("environment proxy enabled")
	}
	if text, err := provider.Generate(context.Background(), execution.ModelRequest{ModelID: testModel, Messages: []execution.Message{{Role: "user", Content: "prompt"}}}); err == nil || text != "" || reached.Load() != 0 {
		t.Fatal("redirect or proxy followed", err, reached.Load())
	}
}
