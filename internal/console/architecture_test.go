package console

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/execution"
)

func TestArchitectureSnapshotUsesOnlyHealthTraceAndCatalog(t *testing.T) {
	facade, stub := traceSetup(t, testRun("run-1", 1))
	var mutex sync.Mutex
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mutex.Lock()
		paths = append(paths, request.URL.Path)
		mutex.Unlock()
		if request.URL.Path != "/health" {
			t.Errorf("unexpected platform read: %s", request.URL.Path)
			writer.WriteHeader(404)
			return
		}
		_, _ = io.WriteString(writer, `{"status":"ready","protocol_version":1,"secret":"SECRET","path":"/private/SECRET"}`)
	}))
	defer server.Close()
	bridge := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mutex.Lock()
		paths = append(paths, request.URL.Path)
		mutex.Unlock()
		if request.URL.Path != "/health" {
			t.Errorf("unexpected bridge read: %s", request.URL.Path)
			writer.WriteHeader(404)
			return
		}
		_, _ = io.WriteString(writer, `{"status":"ready","protocol_version":2,"dry_run":false,"group_routing":true,"serial_ui":true,"accessibility_trusted":true,"screen_capture_authorized":true}`)
	}))
	defer bridge.Close()
	facade.backend.bases["relay"], facade.backend.bases["bridge"] = server.URL, bridge.URL
	engine, err := execution.Open(filepath.Join(t.TempDir(), "execution.json"), execution.Options{Backends: []execution.BackendOptions{{ID: "text", Kind: execution.TextOnly, Models: []execution.Model{{ID: "model-1", Label: "/private/SECRET"}}}, {ID: "personal", Kind: execution.AlmaPersonal, AlmaBaseURL: "http://127.0.0.1:1", Models: []execution.Model{{ID: "model-2", Label: "SECRET"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	facade.executionCatalog = engine
	response := consoleRequest(facade, "architecture")
	var result struct {
		OK    bool                 `json:"ok"`
		Value ArchitectureSnapshot `json:"value"`
	}
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &result) != nil || !result.OK {
		t.Fatalf("bad snapshot: %d %s", response.Code, response.Body.String())
	}
	value := result.Value
	if len(value.Layers) != 3 || len(value.Sources) != 3 || len(value.Actions) != 12 || len(value.Processing.Backends) != 2 || value.Revision == "" || value.ObservedAt.IsZero() || len(value.Processing.Agents) != 0 {
		t.Fatalf("bad snapshot: %+v", value)
	}
	if value.Sources[0].State != "ready" || !value.Sources[0].Capabilities[0].Supported || !value.Actions[0].Capabilities[0].Supported {
		t.Fatal("health capability missing")
	}
	if !value.Sources[0].Availability.Enabled || value.Sources[0].AccountID != nil || value.Sources[0].ContainerIDs != nil || value.Actions[0].Capabilities[3].Supported {
		t.Fatalf("invented identity or adapter contract: source=%+v action=%+v", value.Sources[0], value.Actions[0])
	}
	if !value.Actions[0].Availability.Enabled || value.Actions[0].State != "ready" || value.Actions[0].Availability.Reason != "" {
		t.Fatal("verified protocol v2 non-dry-run Bridge sending not enabled")
	}
	for _, source := range value.Sources[1:] {
		if source.State != "unsupported" || source.Capabilities[0].Supported || source.Capabilities[0].Reason == "" {
			t.Fatal("fabricated platform support")
		}
	}
	for _, stream := range value.EventStreams {
		if stream.Domain == contracts.DomainProcessing {
			if !stream.Supported || stream.Cursor == nil || *stream.Cursor != stub.snapshot.Cursor {
				t.Fatal("missing trace position")
			}
		} else if stream.Supported || stream.Cursor != nil {
			t.Fatal("fabricated adapter stream")
		}
	}
	foundSoft, foundHard := false, false
	for _, backend := range value.Processing.Backends {
		for _, capability := range backend.Capabilities {
			if err := contracts.ValidateCapabilityEvidence(capability); err != nil {
				t.Fatal(err)
			}
			foundSoft = foundSoft || capability.Enforcement == contracts.EnforcementSoft && !capability.Supported
			foundHard = foundHard || capability.Enforcement == contracts.EnforcementHard
		}
	}
	if !foundSoft || !foundHard || strings.Contains(response.Body.String(), "SECRET") || strings.Contains(response.Body.String(), "/private/") {
		t.Fatalf("capability or privacy failure: %s", response.Body.String())
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	for _, path := range stub.paths {
		if path != "/health" && path != "/api/trace/snapshot" && path != "/api/trace/events" {
			t.Fatalf("unexpected dispatcher read: %s", path)
		}
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(paths) != 2 {
		t.Fatalf("unexpected platform probes: %v", paths)
	}
}

func TestArchitectureMissingCapabilitiesAndGuard(t *testing.T) {
	facade, _, _ := setup(t)
	value := facade.Architecture(context.Background())
	if len(value.Processing.Backends) != 0 || value.EventStreams[1].Supported || value.EventStreams[1].Cursor != nil {
		t.Fatal("fabricated processing support")
	}
	for _, capability := range value.Processing.Capabilities {
		if capability.ID == "backend_catalog" && (capability.Supported || capability.Reason != contracts.ReasonUnsupported) {
			t.Fatal("missing Engine was not explicit")
		}
	}
	for _, path := range []string{"architecture?refresh=1", "architecture/events?cursor=x&domain=bogus", "architecture/events?cursor=x&domain=processing&domain=processing", "architecture/events?cursor=%xx", "architecture/events"} {
		if response := consoleRequest(facade, path); response.Code != 400 {
			t.Fatalf("accepted invalid query: %s %d", path, response.Code)
		}
	}
	for _, domain := range []string{"source", "action"} {
		response := consoleRequest(facade, "architecture/events?domain="+domain+"&cursor=x")
		if response.Code != 501 || strings.Contains(response.Body.String(), "event:") {
			t.Fatal("unsupported emitted events")
		}
	}
	request := httptest.NewRequest("GET", "http://127.0.0.1:8090/console/v1/architecture", nil)
	request.Header.Set("Origin", "http://localhost:8090")
	writer := httptest.NewRecorder()
	facade.Handler().ServeHTTP(writer, request)
	if writer.Code != 403 {
		t.Fatal("same origin guard bypassed")
	}
}

func TestArchitectureHostSupportSurvivesHealthFailure(t *testing.T) {
	facade, _ := traceSetup(t)
	server := httptest.NewServer(http.NotFoundHandler())
	address := server.URL
	server.Close()
	facade.backend.bases["relay"], facade.backend.bases["bridge"] = address, address
	value := facade.Architecture(context.Background())
	for _, availability := range []Availability{value.Sources[0].Availability, value.Actions[0].Availability} {
		if !availability.Supported || availability.Enabled || availability.Reason != "unavailable" {
			t.Fatalf("unreachable host mistaken for unsupported implementation: %+v", availability)
		}
	}
	for _, capability := range []ArchitectureCapability{value.Sources[0].Capabilities[0], value.Actions[0].Capabilities[0]} {
		if !capability.Supported || capability.Enforcement != contracts.EnforcementHost || capability.Reason != contracts.ReasonUnavailable {
			t.Fatalf("host support lost: %+v", capability)
		}
	}
	if value.Sources[0].Capabilities[1].Supported || value.Actions[0].Capabilities[3].Supported || value.Sources[0].State != "unreachable" || value.Actions[0].State != "unreachable" {
		t.Fatal("health claimed adapter support")
	}
	for _, capability := range value.Processing.Capabilities {
		switch capability.ID {
		case "agent_catalog", "allowlist_refs", "skills_catalog", "tools_catalog", "knowledge_catalog":
			if capability.Supported || capability.Reason != contracts.ReasonUnsupported {
				t.Fatalf("invented registry: %+v", capability)
			}
		}
	}
}

func TestArchitectureSSEReplayResetAndOpaqueCursor(t *testing.T) {
	for _, mode := range []string{"reset", "gap"} {
		t.Run(mode, func(t *testing.T) {
			facade, stub := traceSetup(t, testRun("a", 1), testRun("b", 2))
			stub.mode = mode
			stub.events[0].ReasonCode = "/private/SECRET prompt"
			response := consoleRequest(facade, "architecture/events?domain=processing&cursor="+testCursor("stream-1", 0))
			body := response.Body.String()
			if response.Code != 200 || strings.Count(body, "event: architecture\n") != 2 || !strings.Contains(body, "event: reset_required\n") || strings.Contains(body, "SECRET") || strings.Contains(body, `"domain":"action"`) {
				t.Fatalf("bad SSE: %d %s", response.Code, body)
			}
			if strings.Index(body, `"runId":"a"`) > strings.Index(body, `"runId":"b"`) {
				t.Fatal("reordered events")
			}
			for _, line := range strings.Split(body, "\n") {
				if !strings.HasPrefix(line, "data: ") || !strings.Contains(line, `"eventId"`) {
					continue
				}
				var event contracts.ArchitectureEvent
				if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event) != nil || contracts.ValidateArchitectureEvent(event) != nil || event.Source != nil || event.Action != nil || event.EnvelopeID != "" {
					t.Fatalf("bad event: %s", line)
				}
			}
		})
	}
	facade, stub := traceSetup(t, testRun("a", 1))
	stub.mode = "reset"
	request := httptest.NewRequest("GET", "http://127.0.0.1:8090/console/v1/architecture/events?cursor="+testCursor("stream-1", 0), nil)
	request.Header.Set("Last-Event-ID", stub.snapshot.Cursor)
	writer := httptest.NewRecorder()
	facade.Handler().ServeHTTP(writer, request)
	if strings.Contains(writer.Body.String(), "event: architecture\n") || !strings.Contains(writer.Body.String(), "reset_required") {
		t.Fatal("ignored resume position")
	}
	for _, cursor := range []string{"invalid", testCursor("other", 0), testCursor("stream-1", 9)} {
		response := consoleRequest(facade, "architecture/events?cursor="+cursor)
		if response.Code != 409 || strings.Contains(response.Header().Get("Content-Type"), "event-stream") {
			t.Fatalf("invalid cursor: %d %s", response.Code, response.Body.String())
		}
	}
}

func TestArchitectureSSEDisconnectAndKeepalive(t *testing.T) {
	facade, stub := traceSetup(t)
	stub.mode, stub.waiting, stub.canceled = "cancel", make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(facade.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/console/v1/architecture/events?cursor="+testCursor("stream-1", 0), nil)
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	select {
	case <-stub.waiting:
	case <-time.After(time.Second):
		t.Fatal("no upstream longpoll")
	}
	if !testing.Short() {
		line, err := bufio.NewReader(response.Body).ReadString('\n')
		if err != nil || line != ": keepalive\n" {
			t.Fatalf("bad keepalive: %q %v", line, err)
		}
	}
	cancel()
	select {
	case <-stub.canceled:
	case <-time.After(time.Second):
		t.Fatal("disconnect did not cancel upstream")
	}
}

func TestArchitectureRejectsUnknownEnumsWithoutSkippingSequence(t *testing.T) {
	facade, stub := traceSetup(t, testRun("a", 1))
	stub.events[0].Phase = "made-up"
	response := consoleRequest(facade, "architecture/events?cursor="+testCursor("stream-1", 0))
	if response.Code != 409 || strings.Contains(response.Body.String(), "event: architecture") {
		t.Fatalf("invalid event escaped: %s", response.Body.String())
	}
}
