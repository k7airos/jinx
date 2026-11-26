package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

func jsonRequest(ctx context.Context, client *http.Client, method, endpoint string, body any, target any) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return failure("validation", "http_body")
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if err != nil {
		return failure("validation", "http_request")
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return failure("transport_error", "backend_http")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return failure("backend_http_error", "status")
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
	if err != nil || len(payload) > 4<<20 || json.Unmarshal(payload, target) != nil {
		return failure("invalid_output", "backend_json")
	}
	return nil
}

type personalRun struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Summary string `json:"summary"`
}

type personalEnvelope struct {
	personalRun
	Run *personalRun `json:"run"`
}

func (envelope personalEnvelope) value() personalRun {
	if envelope.Run != nil {
		return *envelope.Run
	}
	return envelope.personalRun
}

func (engine *Engine) personal(ctx context.Context, option BackendOptions, attempt RouteAttempt, request Request, boundary func(func() error) error) (executionOutput, error) {
	baseURL := strings.TrimSuffix(option.AlmaBaseURL, "/")
	body := struct {
		Title string `json:"title"`
		Task  string `json:"task"`
		Model string `json:"model"`
	}{Title: "group execution " + request.TraceID, Task: "Respond with final text or NONE. Do not use tools, write files, sync reminders or delegate. This is a prompt convention, not tool isolation.\n\n" + requestText(request), Model: attempt.ModelID}
	var envelope personalEnvelope
	output := executionOutput{remote: "not_applicable"}
	err := boundary(func() error {
		output.remote = "may_be_running"
		return jsonRequest(ctx, engine.client, http.MethodPost, baseURL+"/api/personal/dispatch", body, &envelope)
	})
	if err != nil {
		return output, err
	}
	run := envelope.value()
	if !tokenPattern.MatchString(run.ID) {
		return output, failure("invalid_output", "run_id")
	}
	output.external = run.ID
	interval := option.PollInterval
	if interval <= 0 {
		interval = 1500 * time.Millisecond
	}
	for {
		switch run.Status {
		case "done":
			output.remote = "stopped"
			if strings.TrimSpace(run.Summary) == "" {
				return output, failure("invalid_output", "empty_summary")
			}
			output.text = run.Summary
			return output, nil
		case "failed", "skipped":
			output.remote = "stopped"
			return output, failure("backend_failed", "personal_run")
		case "queued", "running", "pending", "waiting", "":
		default:
			return output, failure("invalid_output", "run_status")
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return output, ctx.Err()
		case <-timer.C:
		}
		if err := boundary(func() error { return nil }); err != nil {
			return output, err
		}
		envelope = personalEnvelope{}
		if err := jsonRequest(ctx, engine.client, http.MethodGet, baseURL+"/api/personal/runs/"+output.external, nil, &envelope); err != nil {
			return output, err
		}
		run = envelope.value()
		if run.ID != output.external {
			return output, failure("invalid_output", "poll_run_id")
		}
	}
}

type HTTPTextOptions struct {
	BaseURL string
	Path    string
	Client  *http.Client
}

type HTTPTextProvider struct {
	endpoint string
	client   *http.Client
}

func NewHTTPTextProvider(options HTTPTextOptions) (*HTTPTextProvider, error) {
	base, err := loopbackURL(options.BaseURL)
	if err != nil {
		return nil, err
	}
	if options.Path != "/v1/chat/completions" {
		return nil, failure("unsupported", "text_proxy_protocol_path")
	}
	client := http.Client{}
	if options.Client != nil {
		client = *options.Client
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return failure("forbidden", "redirect") }
	if client.Transport == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		client.Transport = transport
	}
	return &HTTPTextProvider{endpoint: base + options.Path, client: &client}, nil
}

func (provider *HTTPTextProvider) Generate(ctx context.Context, request ModelRequest) (string, error) {
	body := struct {
		Model      string    `json:"model"`
		Messages   []Message `json:"messages"`
		Stream     bool      `json:"stream"`
		ToolChoice string    `json:"tool_choice"`
	}{Model: request.ModelID, Messages: request.Messages, ToolChoice: "none"}
	var response struct {
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				Content      string            `json:"content"`
				ToolCalls    []json.RawMessage `json:"tool_calls"`
				FunctionCall json.RawMessage   `json:"function_call"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := jsonRequest(ctx, provider.client, http.MethodPost, provider.endpoint, body, &response); err != nil {
		return "", err
	}
	if len(response.Choices) != 1 {
		return "", failure("invalid_output", "choices")
	}
	choice := response.Choices[0]
	if choice.FinishReason != "stop" || len(choice.Message.ToolCalls) != 0 || (len(choice.Message.FunctionCall) != 0 && string(choice.Message.FunctionCall) != "null") || strings.TrimSpace(choice.Message.Content) == "" {
		return "", failure("invalid_output", "text_only_response")
	}
	return choice.Message.Content, nil
}
