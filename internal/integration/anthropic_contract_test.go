package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"jinx/internal/execution"
	"jinx/internal/runtimebridge"
)

func TestAnthropicProxyTextOnlyContract(t *testing.T) {
	for _, scenario := range []string{"text", "tool_use"} {
		t.Run(scenario, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				if request.Method != http.MethodPost || request.URL.Path != "/anthropic-proxy/synthetic-provider/v1/messages" {
					t.Error("controlled provider used the wrong Alma proxy API")
					http.NotFound(writer, request)
					return
				}
				var body map[string]any
				if json.NewDecoder(request.Body).Decode(&body) != nil || body["model"] != syntheticModel || body["tools"] != nil || body["stream"] == true {
					t.Error("pure model request registered tools or lost explicit model")
				}
				content := []any{map[string]any{"type": "text", "text": syntheticReply}}
				stop := "end_turn"
				if scenario == "tool_use" {
					content = append(content, map[string]any{"type": "tool_use", "id": "synthetic-tool", "name": "never_execute", "input": map[string]any{}})
					stop = "tool_use"
				}
				writer.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(writer).Encode(map[string]any{"id": "synthetic-response", "type": "message", "role": "assistant", "content": content, "model": syntheticModel, "stop_reason": stop})
			}))
			t.Cleanup(server.Close)
			provider, err := runtimebridge.NewAnthropicProvider(server.URL, "synthetic-provider")
			if err != nil {
				t.Fatalf("required Alma pure-model proxy contract is not wired: %v", err)
			}
			text, err := provider.Generate(context.Background(), execution.ModelRequest{ModelID: syntheticModel, Messages: []execution.Message{{Role: "user", Content: "synthetic fictional prompt"}}})
			if scenario == "text" {
				if err != nil || text != syntheticReply || calls.Load() != 1 {
					t.Fatalf("pure-model result missing: text=%q err=%v calls=%d", text, err, calls.Load())
				}
			} else if err == nil || text != "" || calls.Load() != 1 {
				t.Fatalf("tool_use response escaped pure-model boundary: text=%q err=%v calls=%d", text, err, calls.Load())
			}
		})
	}
}
