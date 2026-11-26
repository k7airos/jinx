package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/console"
	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	relayconfig "jinx/internal/relay/config"
	"jinx/internal/relay/filter"
	"jinx/internal/relay/hub"
	"jinx/internal/relay/web"
)

const sourceGroup = "910001@chatroom"
const targetGroup = "910002@chatroom"
const profileName = "integration-source"
const syntheticReply = "SYNTHETIC_INTEGRATION_REPLY"

type stack struct {
	dispatcher *dispatcher.Dispatcher
	upstream   *httptest.Server
	facade     *console.Facade
	console    *httptest.Server
}

func startStack(t *testing.T, configPath, journal, relayURL, bridgeURL string, writes *atomic.Int32, runners ...dispatcher.Runner) *stack {
	t.Helper()
	var options dispatcher.Options
	if len(runners) > 0 {
		options.Runner = runners[0]
	}
	dispatch, err := dispatcher.NewDispatcherWithOptions(configPath, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dispatch.Close)
	handler := dispatch.Handler()
	upstream := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPost && request.URL.Path == "/api/config" {
			writes.Add(1)
		}
		handler.ServeHTTP(writer, request)
	}))
	t.Cleanup(upstream.Close)
	facade, err := console.New(console.Options{
		DispatcherURL: upstream.URL, RelayURL: relayURL, BridgeURL: bridgeURL,
		StateDir: journal, Timeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := facade.Close(); err != nil {
			t.Error(err)
		}
	})
	server := httptest.NewServer(facade.Handler())
	t.Cleanup(server.Close)
	dispatch.Start()
	await(t, "dispatcher relay subscription", func() bool {
		health := dispatch.HealthSnapshot()
		return health.RelayConnected && health.LastEventID != ""
	})
	return &stack{dispatcher: dispatch, upstream: upstream, facade: facade, console: server}
}

func (services *stack) close() {
	services.console.Close()
	_ = services.facade.Close()
	services.dispatcher.Close()
	services.upstream.Close()
}

func newRelay(t *testing.T, root string) *httptest.Server {
	t.Helper()
	cfg := relayconfig.Config{
		Server: relayconfig.ServerConfig{ListenAddr: "127.0.0.1:0"},
		GroupControl: relayconfig.GroupControl{
			MonitoredGroups: []string{sourceGroup, targetGroup},
			ExposedGroups:   []string{sourceGroup, targetGroup},
			GroupAliases:    map[string]string{},
		},
		Filters: relayconfig.FilterConfig{AllowedTypes: []int{1}, KeywordBlocklist: []string{}, RegexBlocklist: []string{}, SenderBlocklist: []string{}},
	}
	path := filepath.Join(root, "relay.json")
	writeJSON(t, path, cfg)
	manager, err := relayconfig.NewManager(path)
	if err != nil {
		t.Fatal(err)
	}
	relay := web.NewServer(manager, hub.NewHub(100), filter.NewEngine())
	handler, err := relay.Handler()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

type bridgeDelivery struct {
	GroupID   string `json:"group_id"`
	Target    string `json:"target"`
	Text      string `json:"text"`
	RequestID string `json:"request_id"`
	ExpiresAt int64  `json:"expires_at_unix_ms"`
	Delivery  struct {
		Type string `json:"type"`
	} `json:"delivery"`
}

func newBridge(t *testing.T, client *http.Client, relayURL string, posts *atomic.Int32, deliveries chan<- bridgeDelivery) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/health":
			writer.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(writer, `{"status":"ready","protocol_version":2,"group_routing":true,"serial_ui":true,"accessibility_trusted":true,"screen_capture_authorized":true,"dry_run":false}`)
		case request.Method == http.MethodPost && request.URL.Path == "/":
			posts.Add(1)
			var delivery bridgeDelivery
			if err := json.NewDecoder(request.Body).Decode(&delivery); err != nil {
				t.Error(err)
				http.Error(writer, "invalid synthetic delivery", http.StatusBadRequest)
				return
			}
			select {
			case deliveries <- delivery:
			default:
				t.Error("unexpected extra bridge submission")
			}
			echo := message("echo-"+delivery.RequestID, delivery.Text)
			echo["group_id"], echo["sender_id"], echo["is_echo"] = delivery.GroupID, "integration-self", true
			body, err := json.Marshal(echo)
			if err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			req, err := http.NewRequestWithContext(request.Context(), http.MethodPost, relayURL+"/api/v1/messages", bytes.NewReader(body))
			if err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			response, err := client.Do(req)
			if err != nil {
				t.Error(err)
				writer.WriteHeader(http.StatusBadGateway)
				return
			}
			defer response.Body.Close()
			var ack struct{ Allowed bool }
			if err := json.NewDecoder(response.Body).Decode(&ack); err != nil || response.StatusCode != http.StatusOK || !ack.Allowed {
				t.Errorf("synthetic echo webhook: status=%d allowed=%t err=%v", response.StatusCode, ack.Allowed, err)
				writer.WriteHeader(http.StatusBadGateway)
				return
			}
			_, _ = io.WriteString(writer, "OK")
		default:
			t.Errorf("unexpected bridge endpoint: %s %s", request.Method, request.URL.Path)
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func prepareDispatcher(t *testing.T, root, relayURL, bridgeURL string) (string, string) {
	t.Helper()
	profile := filepath.Join(root, "profiles", profileName)
	if err := os.MkdirAll(filepath.Join(profile, "workspace"), 0700); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(profile, "mem0.json"), map[string]any{
		"user_id": "group:" + sourceGroup, "mode": "oss",
		"oss": map[string]any{"vector_store": map[string]any{
			"provider": "qdrant", "config": map[string]string{"collection_name": "group_910001"},
		}},
	})
	script := `#!/bin/sh
set -eu
printf '%s\n' invocation >> "$HERMES_HOME/calls.txt"
printf '%s\n' "$PWD" "$HERMES_HOME" "$@" > "$HERMES_HOME/argv.txt"
while IFS= read -r line || [ -n "$line" ]; do
    printf '%s\n' "$line"
done > "$HERMES_HOME/prompt.txt"
while IFS= read -r line || [ -n "$line" ]; do
    printf '%s\n' "$line"
done < "$HERMES_HOME/result.json"
`
	bin := filepath.Join(root, "synthetic-backend.sh")
	if err := os.WriteFile(bin, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	var cfg dispatcher.Config
	cfg.Server.ListenAddr = "127.0.0.1:0"
	cfg.Relay.StreamURL = relayURL + "/api/v1/stream"
	cfg.Bridge.URL, cfg.Bridge.TimeoutSec = bridgeURL, 5
	cfg.Hermes.BinPath, cfg.Hermes.Profile = bin, "integration"
	cfg.Hermes.ProfilesDir, cfg.Hermes.TimeoutSec, cfg.Hermes.MaxConcurrent = filepath.Dir(profile), 10, 1
	cfg.Rules = dispatcher.Rules{
		BatchSize: 1, IdleTimeoutSec: 1, MaxBatchWaitSec: 2,
		BotNames: []string{"Jinx"}, SelfSenderIDs: []string{"integration-self"},
		GroupWhitelist: []string{sourceGroup}, GroupRules: map[string]dispatcher.GroupRule{
			sourceGroup: {Enabled: true, TargetGroupID: targetGroup, Target: "integration target", HermesProfile: profileName},
		},
	}
	path := filepath.Join(root, "dispatcher.json")
	writeJSON(t, path, cfg)
	return path, profile
}

func backendResult(t *testing.T, profile, text, runID string) {
	t.Helper()
	writeJSON(t, filepath.Join(profile, "result.json"), map[string]any{
		"type": "result", "text": text, "session_id": "synthetic-session", "exit_code": 0,
		"backend": "Hermes", "run_id": runID,
	})
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

func requestJSON(t *testing.T, client *http.Client, method, target string, body, result any) http.Header {
	t.Helper()
	return requestStatusJSON(t, client, method, target, body, result, http.StatusOK)
}

func requestStatusJSON(t *testing.T, client *http.Client, method, target string, body, result any, status int) http.Header {
	t.Helper()
	var input bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&input).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	request, err := http.NewRequest(method, target, &input)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != status {
		data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		t.Fatalf("%s %s: HTTP %d: %s", method, target, response.StatusCode, data)
	}
	if err := json.NewDecoder(response.Body).Decode(result); err != nil {
		t.Fatal(err)
	}
	return response.Header
}

func message(id, content string) map[string]any {
	return map[string]any{
		"id": id, "group_id": sourceGroup, "group_name": "synthetic source",
		"sender_id": "integration-human", "sender_name": "synthetic human",
		"type": 1, "content": content, "timestamp": time.Now().UnixMilli(),
	}
}

func postMessage(t *testing.T, client *http.Client, relayURL, id, content string) {
	t.Helper()
	var ack struct{ Allowed bool }
	requestJSON(t, client, http.MethodPost, relayURL+"/api/v1/messages", message(id, content), &ack)
	if !ack.Allowed {
		t.Fatal("synthetic input was not forwarded by the real relay")
	}
}

func await(t *testing.T, description string, ready func() bool) {
	t.Helper()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if ready() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		}
	}
}

func waitRun(t *testing.T, services *stack, id string) console.RunView {
	t.Helper()
	var found console.RunView
	await(t, "terminal run for "+id, func() bool {
		view, err := services.facade.ListRuns(context.Background(), url.Values{"groupId": {sourceGroup}})
		if err != nil {
			t.Fatal(err)
		}
		for _, run := range view.Data.(console.RunPage).Items {
			if len(run.MessageIDs) == 1 && run.MessageIDs[0] == id {
				found = run
				return run.Phase == contracts.PhaseTerminal
			}
		}
		return false
	})
	await(t, "dispatcher idle", func() bool {
		health := services.dispatcher.HealthSnapshot()
		return health.PendingMessages == 0 && health.ExecutingGroups == 0 && !health.AwaitingEcho
	})
	return found
}

type runStream struct {
	body    io.ReadCloser
	cancel  context.CancelFunc
	scanner *bufio.Scanner
}

func openStream(t *testing.T, client *http.Client, origin, cursor, lastID string) *runStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/console/v1/events?"+url.Values{"cursor": {cursor}}.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if lastID != "" {
		request.Header.Set("Last-Event-ID", lastID)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	stream := &runStream{body: response.Body, cancel: cancel, scanner: bufio.NewScanner(response.Body)}
	t.Cleanup(stream.close)
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("console event stream: HTTP %d %s", response.StatusCode, response.Header.Get("Content-Type"))
	}
	return stream
}

func (stream *runStream) close() {
	stream.cancel()
	_ = stream.body.Close()
}

func (stream *runStream) read(t *testing.T, count int) []contracts.Event {
	t.Helper()
	var events []contracts.Event
	var kind, id, data string
	for len(events) < count && stream.scanner.Scan() {
		line := stream.scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			kind = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "id: "):
			id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		case line == "" && data != "":
			var event contracts.Event
			if err := json.Unmarshal([]byte(data), &event); err != nil || kind != "run" || id != event.Cursor {
				t.Fatalf("invalid run frame: kind=%s id=%s data=%s err=%v", kind, id, data, err)
			}
			events = append(events, event)
			kind, id, data = "", "", ""
		}
	}
	if len(events) != count {
		t.Fatalf("SSE ended after %d/%d run events: %v", len(events), count, stream.scanner.Err())
	}
	return events
}
