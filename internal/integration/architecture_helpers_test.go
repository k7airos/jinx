package integration_test

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/console"
	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	"jinx/internal/execution"
	"jinx/internal/gateway"
	"jinx/internal/observability"
)

type architectureRequests struct {
	mutex sync.Mutex
	paths map[string]int
}

func architectureCountRequests(t *testing.T, handler http.Handler) (*httptest.Server, *architectureRequests) {
	t.Helper()
	requests := &architectureRequests{paths: map[string]int{}}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.mutex.Lock()
		requests.paths[request.Method+" "+request.URL.Path]++
		requests.mutex.Unlock()
		handler.ServeHTTP(writer, request)
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func (requests *architectureRequests) snapshot() map[string]int {
	requests.mutex.Lock()
	defer requests.mutex.Unlock()
	result := make(map[string]int, len(requests.paths))
	for path, count := range requests.paths {
		result[path] = count
	}
	return result
}

type architectureRun struct {
	Inputs  []contracts.InboundReference `json:"inputs"`
	Action  *contracts.ActionRef         `json:"action"`
	Receipt *console.ProcessingReceipt   `json:"receipt"`
}

func newArchitectureEchoBridge(t *testing.T, relay *httptest.Server, posts *atomic.Int32) *httptest.Server {
	t.Helper()
	bridge := newBridge(t, relay.Client(), relay.URL, posts, make(chan bridgeDelivery, 2))
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			bridge.Config.Handler.ServeHTTP(writer, request)
			return
		}
		response := httptest.NewRecorder()
		bridge.Config.Handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Errorf("echo fixture failed: HTTP %d %s", response.Code, response.Body.String())
		}
		http.Error(writer, "synthetic ACK lost after self echo", http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)
	return server
}

type architectureDetail struct {
	Run      contracts.Run
	Metadata architectureRun
	Events   []contracts.Event
}

func architectureRunDetail(t *testing.T, services *stack, id string) architectureDetail {
	t.Helper()
	var detail struct {
		Run    json.RawMessage   `json:"run"`
		Events []contracts.Event `json:"events"`
	}
	requestJSON(t, services.upstream.Client(), http.MethodGet, services.upstream.URL+"/api/trace/runs/"+id, nil, &detail)
	result := architectureDetail{Events: detail.Events}
	if err := json.Unmarshal(detail.Run, &result.Run); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(detail.Run, &result.Metadata); err != nil {
		t.Fatal(err)
	}
	return result
}

func architectureConsoleRun(t *testing.T, server *httptest.Server, id string) architectureRun {
	t.Helper()
	var detail struct {
		Data struct {
			Run json.RawMessage `json:"run"`
		} `json:"data"`
	}
	gatewayGet(t, server, "/console/v1/runs/"+id, &detail)
	if strings.Contains(string(detail.Data.Run), "PRIVATE_ARCHITECTURE_INPUT") || strings.Contains(string(detail.Data.Run), "PRIVATE_MODEL_LABEL") || strings.Contains(string(detail.Data.Run), "integration-human") {
		t.Error("Console run detail exposed private inbound/provider content")
	}
	var run architectureRun
	if err := json.Unmarshal(detail.Data.Run, &run); err != nil {
		t.Fatal(err)
	}
	return run
}

func architectureTraceEvents(t *testing.T, services *stack, cursor string) observability.EventPage {
	t.Helper()
	var page observability.EventPage
	requestJSON(t, services.upstream.Client(), http.MethodGet, services.upstream.URL+"/api/trace/events?"+url.Values{"cursor": {cursor}, "limit": {"1000"}}.Encode(), nil, &page)
	if page.HasMore {
		t.Fatal("architecture fixture exceeded bounded event page")
	}
	return page
}

func assertArchitectureInputs(t *testing.T, public, stored architectureRun, messageIDs []string) {
	t.Helper()
	if len(stored.Inputs) != len(messageIDs) || len(public.Inputs) != len(messageIDs) {
		t.Errorf("safe inputs/envelope IDs missing from persisted or Console run: stored=%+v public=%+v", stored, public)
		return
	}
	seen := map[string]bool{}
	for index, input := range stored.Inputs {
		if input.EnvelopeID == "" || seen[input.EnvelopeID] || input.DedupKey == "" || input.ReceivedAt == nil || input.ReceivedAt.IsZero() || input.Source.Platform != contracts.PlatformWeChat || input.Source.AccountID == "" || input.Source.ContainerID != sourceGroup || input.Source.ContentID != messageIDs[index] {
			t.Errorf("input lost actual WeChat envelope/source identity: %+v", input)
		}
		input.Source.ContainerLabel = ""
		if !reflect.DeepEqual(public.Inputs[index], input) {
			t.Errorf("Console altered persisted input identity/time: %+v / %+v", public.Inputs[index], input)
		}
		seen[input.EnvelopeID] = true
	}
	data, err := json.Marshal(public.Inputs)
	if err != nil || strings.Contains(string(data), "PRIVATE_ARCHITECTURE_INPUT") || strings.Contains(string(data), "synthetic human") || strings.Contains(string(data), "integration-human") || strings.Contains(string(data), "PRIVATE_MODEL_LABEL") {
		t.Errorf("input references exposed message/author/model content: %s %v", data, err)
	}
}

func assertArchitectureEchoReceipt(t *testing.T, run contracts.Run, public architectureRun) {
	t.Helper()
	evidence := run.DeliveryEvidence
	if evidence.SubmittedAtSource != "echo_inferred" || evidence.SubmittedAt != nil || evidence.ConfirmedAt == nil || evidence.SubmissionStartedAt == nil || evidence.EchoMessageID != "echo-"+evidence.BridgeRequestID {
		t.Errorf("echo-only evidence invented ACK or lost correlation: %+v", evidence)
	}
	if public.Receipt == nil || public.Receipt.Status != contracts.ActionConfirmed || public.Receipt.Evidence.SubmittedAt != nil || public.Receipt.Evidence.SubmittedAtSource == nil || *public.Receipt.Evidence.SubmittedAtSource != "echo_inferred" || public.Receipt.Evidence.ConfirmedAt == nil {
		t.Error("Console run detail lost echo-inferred receipt or invented ACK")
		return
	}
	projected, err := dispatcher.ProcessingActionReceipt(run)
	if err != nil || projected.Receipt == nil {
		t.Errorf("persisted run cannot produce public ActionReceipt: %+v %v", projected, err)
		return
	}
	receipt := *projected.Receipt
	if err := contracts.ValidateActionReceipt(receipt); err != nil {
		t.Errorf("actual persisted-run receipt is not a valid public ActionReceipt: %+v: %v", receipt, err)
	}
	if receipt.Status != contracts.ActionConfirmed || receipt.ActionID != evidence.BridgeRequestID || receipt.Platform != contracts.PlatformWeChat || receipt.ExternalID != evidence.EchoMessageID || receipt.SubmittedAt != nil || receipt.ConfirmedAt == nil || public.Action == nil || public.Action.ActionID != receipt.ActionID {
		t.Errorf("public receipt lost echo identity or invented ACK: %+v", receipt)
	}
	if receipt.ConfirmedAt != nil && !receipt.ConfirmedAt.Equal(*evidence.ConfirmedAt) {
		t.Error("public receipt changed real echo confirmation time")
	}
}

func assertArchitecturePosition(t *testing.T, snapshot console.ArchitectureSnapshot, streamID, cursor string, seq uint64) {
	t.Helper()
	if len(snapshot.EventStreams) != 3 {
		t.Fatalf("missing three stream domains: %+v", snapshot.EventStreams)
	}
	for _, stream := range snapshot.EventStreams {
		if stream.Domain == contracts.DomainProcessing {
			if !stream.Supported || stream.StreamID == nil || *stream.StreamID != streamID || stream.Cursor == nil || *stream.Cursor != cursor || stream.LastSeq == nil || *stream.LastSeq != seq {
				t.Fatalf("architecture changed real stream position: %+v", stream)
			}
		} else if stream.Supported || stream.Cursor != nil || stream.StreamID != nil || stream.LastSeq != nil || stream.Reason != contracts.ReasonUnsupported {
			t.Fatalf("unsupported domain fabricated cursor: %+v", stream)
		}
	}
}

type architectureFrame struct {
	Cursor string                      `json:"cursor"`
	Event  contracts.ArchitectureEvent `json:"event"`
}

func readArchitectureFrames(t *testing.T, server *httptest.Server, cursor, lastID string, count int) []architectureFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/console/v1/architecture/events?"+url.Values{"domain": {"processing"}, "cursor": {cursor}}.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if lastID != "" {
		request.Header.Set("Last-Event-ID", lastID)
	}
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("architecture SSE rejected actual trace: HTTP %d %s", response.StatusCode, body)
	}
	frames := make([]architectureFrame, 0, count)
	scanner := bufio.NewScanner(response.Body)
	var id, kind, data string
	for len(frames) < count && scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			kind = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		case line == "" && data != "":
			var event contracts.ArchitectureEvent
			if kind != "architecture" || id == "" || json.Unmarshal([]byte(data), &event) != nil || contracts.ValidateArchitectureEvent(event) != nil {
				t.Fatalf("architecture client cannot read SSE: kind=%s id=%s data=%s", kind, id, data)
			}
			if strings.Contains(data, "PRIVATE_ARCHITECTURE_INPUT") || strings.Contains(data, "PRIVATE_MODEL_LABEL") {
				t.Fatalf("architecture SSE leaked private fixture: %s", data)
			}
			frames = append(frames, architectureFrame{Cursor: id, Event: event})
			id, kind, data = "", "", ""
		}
	}
	if len(frames) != count {
		t.Fatalf("architecture SSE ended at %d/%d: %v", len(frames), count, scanner.Err())
	}
	return frames
}

func assertArchitectureHistory(t *testing.T, events []contracts.Event, frames []architectureFrame, run architectureRun, batch bool) {
	t.Helper()
	seenAction := false
	for index, raw := range events {
		frame := frames[index]
		var position struct {
			StreamID string
			Seq      uint64
		}
		decoded, err := base64.RawURLEncoding.DecodeString(frame.Cursor)
		if err != nil || json.Unmarshal(decoded, &position) != nil || position.StreamID != raw.StreamID || position.Seq != raw.Seq || raw.Seq != uint64(index+1) || frame.Cursor != raw.Cursor || frame.Event.RunID != raw.EntityID || frame.Event.Phase != raw.Phase || frame.Event.Outcome != raw.Outcome || !frame.Event.OccurredAt.Equal(raw.OccurredAt) {
			t.Fatalf("architecture SSE changed persisted order/identity/time: %+v / %+v", frame, raw)
		}
		encoded, err := json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		var persisted struct {
			EnvelopeID string               `json:"envelopeId"`
			Source     *contracts.SourceRef `json:"source"`
			Action     *contracts.ActionRef `json:"action"`
		}
		if err := json.Unmarshal(encoded, &persisted); err != nil {
			t.Fatal(err)
		}
		if persisted.Source != nil {
			persisted.Source.ContainerLabel = ""
		}
		if !reflect.DeepEqual(frame.Event.Source, persisted.Source) || !reflect.DeepEqual(frame.Event.Action, persisted.Action) || frame.Event.EnvelopeID != persisted.EnvelopeID {
			t.Errorf("SSE backfilled current run instead of persisted Event: event=%s SSE=%+v", encoded, frame.Event)
		}
		if len(run.Inputs) == 1 && (persisted.Source == nil || persisted.Source.ContentID != run.Inputs[0].Source.ContentID || persisted.Source.ContainerID != sourceGroup || persisted.Source.Platform != contracts.PlatformWeChat || persisted.EnvelopeID != run.Inputs[0].EnvelopeID) {
			t.Errorf("persisted Event lost single actual input: %s", encoded)
		}
		if batch && (persisted.EnvelopeID != "" || persisted.Source != nil && (persisted.Source.ContentID != "" || persisted.Source.Platform != contracts.PlatformWeChat || persisted.Source.ContainerID != sourceGroup)) {
			t.Errorf("batch fabricated one-input source/envelope: %s", encoded)
		}
		if raw.Kind == "submission_started" {
			seenAction = true
		}
		if !seenAction && (persisted.Action != nil || frame.Event.Action != nil) {
			t.Errorf("historical pre-submission event acquired future Action: %s", encoded)
		}
		if seenAction && run.Action != nil && !reflect.DeepEqual(persisted.Action, run.Action) {
			t.Errorf("submitted event lacks persisted ActionRef: %s", encoded)
		}
	}
	if run.Action != nil && !seenAction {
		t.Error("ActionRef exists without persisted submission_started event")
	}
}

func assertArchitectureClient(t *testing.T, server *httptest.Server, frames []architectureFrame) {
	t.Helper()
	if _, err := exec.LookPath("bun"); err != nil {
		t.Fatal("bun required to execute actual TypeScript architecture client")
	}
	clientPath, err := filepath.Abs("../console/architecture-client.ts")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	fixture := filepath.Join(root, "frames.json")
	writeJSON(t, fixture, frames)
	script := `import { createArchitectureClient } from ` + strconv.Quote(clientPath) + `;
const frames = await Bun.file(process.argv[2]).json();
const nativeFetch = globalThis.fetch;
globalThis.fetch = (path, options) => nativeFetch(new URL(String(path), process.argv[3]), options);
class FixtureEventSource extends EventTarget {
  static instance;
  closed = false;
  constructor(url) { super(); this.url = url; FixtureEventSource.instance = this; }
  close() { this.closed = true; }
}
globalThis.EventSource = FixtureEventSource;
const client = createArchitectureClient();
const snapshot = await client.getArchitecture();
if (!snapshot.ok || snapshot.value.layers.join(',') !== 'source,processing,action') throw Error('snapshot unreadable');
const received = [];
const errors = [];
const close = client.subscribeArchitecture({ cursor: frames[0].cursor, domain: 'processing',
  onEvent: (event, cursor) => received.push({event, cursor}),
  onResetRequired: problem => errors.push(problem),
  onUnavailable: problem => errors.push(problem), onConnection: () => {} });
const stream = FixtureEventSource.instance;
for (const frame of frames) {
  const event = new Event('architecture');
  Object.defineProperties(event, {data: {value: JSON.stringify(frame.event)}, lastEventId: {value: frame.cursor}});
  stream.dispatchEvent(event);
}
if (errors.length || received.length !== frames.length || JSON.stringify(received) !== JSON.stringify(frames.map(frame => ({event: frame.event, cursor: frame.cursor})))) throw Error(JSON.stringify({errors, received}));
close();
if (!stream.closed) throw Error('client failed to disconnect');
console.log('architecture client accepted ' + received.length + ' actual SSE events');
`
	path := filepath.Join(root, "client-check.ts")
	if err := os.WriteFile(path, []byte(script), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "bun", path, fixture, server.URL)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Errorf("actual architecture-client.ts rejected real snapshot/SSE: %v\n%s", err, output)
	} else {
		t.Log(strings.TrimSpace(string(output)))
	}
}

func newArchitectureGateway(t *testing.T, root, dispatcherURL, relayURL, bridgeURL string, engine *execution.Engine, catalog console.ExecutionCatalogReader) *httptest.Server {
	t.Helper()
	facade, err := console.New(console.Options{
		DispatcherURL: dispatcherURL, RelayURL: relayURL, BridgeURL: bridgeURL,
		StateDir: filepath.Join(root, "architecture-console"), Timeout: time.Second, ExecutionCatalog: catalog,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := facade.Close(); err != nil {
			t.Error(err)
		}
	})
	api, err := gateway.New(gateway.Options{Console: facade, Execution: engine, DispatcherURL: dispatcherURL})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	return server
}

func assertArchitectureUnsupportedDomains(t *testing.T, server *httptest.Server, cursor *string) {
	t.Helper()
	if cursor == nil {
		t.Fatal("real processing cursor missing")
	}
	for _, domain := range []string{"source", "action"} {
		response, err := server.Client().Get(server.URL + "/console/v1/architecture/events?" + url.Values{"domain": {domain}, "cursor": {*cursor}}.Encode())
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		var result struct {
			OK      bool            `json:"ok"`
			Problem console.Problem `json:"problem"`
		}
		if err != nil || json.Unmarshal(body, &result) != nil || response.StatusCode != http.StatusNotImplemented || result.OK || result.Problem.Code != "unsupported" || strings.Contains(response.Header.Get("Content-Type"), "event-stream") || strings.Contains(string(body), "event:") {
			t.Fatalf("%s domain fabricated events: HTTP %d %s err=%v", domain, response.StatusCode, body, err)
		}
	}
}
