package runtimebridge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"jinx/internal/execution"
)

const maxRequestBytes = 1 << 20
const maxResponseBytes = 4 << 20

var providerIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

type AnthropicProvider struct {
	endpoint string
	client   *http.Client
}

var _ execution.Provider = (*AnthropicProvider)(nil)

func operatorOrigin(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || strings.Contains(raw, "#") || parsed.RawPath != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", &execution.Error{Code: "validation", Field: "almaURL"}
	}
	address := net.ParseIP(parsed.Hostname())
	port, portErr := strconv.Atoi(parsed.Port())
	if address == nil || !address.IsLoopback() || portErr != nil || port < 1 || port > 65535 {
		return "", &execution.Error{Code: "validation", Field: "almaURL"}
	}
	return "http://" + parsed.Host, nil
}

func NewAnthropicProvider(almaURL, providerID string) (*AnthropicProvider, error) {
	origin, err := operatorOrigin(almaURL)
	if err != nil {
		return nil, err
	}
	if !providerIDPattern.MatchString(providerID) {
		return nil, &execution.Error{Code: "validation", Field: "providerID"}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	client := &http.Client{Transport: transport, Timeout: 120 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return &execution.Error{Code: "forbidden", Field: "redirect"}
		}}
	return &AnthropicProvider{endpoint: origin + "/anthropic-proxy/" + providerID + "/v1/messages", client: client}, nil
}

func (provider *AnthropicProvider) Generate(ctx context.Context, request execution.ModelRequest) (string, error) {
	body := struct {
		Model     string              `json:"model"`
		Messages  []execution.Message `json:"messages"`
		System    string              `json:"system,omitempty"`
		MaxTokens int                 `json:"max_tokens"`
		Stream    bool                `json:"stream"`
	}{Model: request.ModelID, MaxTokens: 4096}
	if strings.TrimSpace(request.ModelID) == "" || len(request.Messages) > 256 {
		return "", &execution.Error{Code: "validation", Field: "model_request"}
	}
	for _, message := range request.Messages {
		switch message.Role {
		case "system":
			if len(body.Messages) > 0 {
				return "", &execution.Error{Code: "validation", Field: "system_message_order"}
			}
			if body.System != "" {
				body.System += "\n\n"
			}
			body.System += message.Content
		case "user", "assistant":
			body.Messages = append(body.Messages, message)
		default:
			return "", &execution.Error{Code: "validation", Field: "message_role"}
		}
	}
	if len(body.Messages) == 0 {
		return "", &execution.Error{Code: "validation", Field: "messages"}
	}
	data, err := json.Marshal(body)
	if err != nil || len(data) > maxRequestBytes {
		return "", &execution.Error{Code: "too_large", Field: "request"}
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, provider.endpoint, bytes.NewReader(data))
	if err != nil {
		return "", &execution.Error{Code: "validation", Field: "http_request"}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("anthropic-version", "2023-06-01")
	response, err := provider.client.Do(httpRequest)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", &execution.Error{Code: "transport_error", Field: "anthropic_proxy"}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", &execution.Error{Code: "backend_http_error", Field: "status"}
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(payload) > maxResponseBytes {
		return "", &execution.Error{Code: "invalid_output", Field: "response_size"}
	}
	var output struct {
		Type       string `json:"type"`
		Role       string `json:"role"`
		Model      string `json:"model"`
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if validateJSON(payload) != nil || json.Unmarshal(payload, &output) != nil || output.Type != "message" || output.Role != "assistant" || output.Model != request.ModelID || output.StopReason != "end_turn" || len(output.Content) == 0 {
		return "", &execution.Error{Code: "invalid_output", Field: "anthropic_message"}
	}
	var text strings.Builder
	for _, block := range output.Content {
		if block.Type != "text" {
			return "", &execution.Error{Code: "invalid_output", Field: "text_only_response"}
		}
		text.WriteString(block.Text)
	}
	if strings.TrimSpace(text.String()) == "" {
		return "", &execution.Error{Code: "invalid_output", Field: "empty_text"}
	}
	return text.String(), nil
}
