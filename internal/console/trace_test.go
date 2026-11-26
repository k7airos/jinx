package console

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/observability"
)

func testCursor(stream string, seq uint64) string {
	data, _ := json.Marshal(tracePosition{StreamID: stream, Seq: seq})
	return base64.RawURLEncoding.EncodeToString(data)
}

func testRun(id string, seq uint64) contracts.Run {
	accepted := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	return contracts.Run{
		ID: id, Revision: "revision-" + id, EntitySeq: seq, SourceGroupID: "1@chatroom", TargetGroupID: "2@chatroom",
		AcceptedAt: accepted, UpdatedAt: accepted.Add(time.Minute), LastProgressAt: accepted.Add(time.Minute),
		AdmissionWatermark: contracts.Watermark{Token: "admission-1", RecordedAt: accepted, SourceSequence: "1"},
		Trigger:            "batch", MessageIDs: []string{"message-" + id}, Phase: contracts.PhaseExecuting,
		Outcome: contracts.OutcomePending, Delivery: contracts.DeliveryNotStarted, Completeness: contracts.Partial,
		Versions: contracts.Versions{Dispatcher: "cfg-1", Fingerprint: "hash-1"}, ConfigSnapshotID: "snapshot-1", PlannedBackendID: "hermes",
		Attempts: []contracts.Attempt{{ID: "attempt-1", BackendID: "hermes", ModelLabel: "model-1", Status: contracts.AttemptRunning, RemoteExecution: contracts.RemoteMayBeRunning, StartedAt: &accepted, MissingExternalIDReason: "not_retained", Versions: contracts.Versions{Dispatcher: "cfg-1", Fingerprint: "hash-1"}}},
	}
}

func testEvent(run contracts.Run) contracts.Event {
	return contracts.Event{StreamID: "stream-1", Seq: run.EntitySeq, Cursor: testCursor("stream-1", run.EntitySeq), Entity: "run", EntityID: run.ID, Revision: run.Revision, Kind: "run_updated", OccurredAt: run.UpdatedAt, Phase: run.Phase, Outcome: run.Outcome, Delivery: run.Delivery}
}

type traceStub struct {
	mu       sync.Mutex
	snapshot observability.Snapshot
	events   []contracts.Event
	mode     string
	paths    []string
	queries  []url.Values
	waiting  chan struct{}
	canceled chan struct{}
}

func (stub *traceStub) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	stub.mu.Lock()
	defer stub.mu.Unlock()
	stub.paths = append(stub.paths, request.URL.Path)
	writer.Header().Set("Content-Type", "application/json")
	switch {
	case request.URL.Path == "/api/trace/snapshot":
		_ = json.NewEncoder(writer).Encode(stub.snapshot)
	case request.URL.Path == "/api/trace/events":
		stub.queries = append(stub.queries, request.URL.Query())
		position, err := parseTraceCursor(request.URL.Query().Get("cursor"))
		if err != nil || position.StreamID != stub.snapshot.StreamID || position.Seq > stub.snapshot.LastSeq || stub.mode == "expired" {
			writer.WriteHeader(409)
			_, _ = io.WriteString(writer, `{"code":"reset_required","detail":"/private/SECRET reply text"}`)
			return
		}
		if request.URL.Query().Get("limit") != "100" {
			writer.WriteHeader(400)
			return
		}
		if stub.mode == "cancel" && request.URL.Query().Get("wait") == "1" {
			waiting, canceled := stub.waiting, stub.canceled
			close(waiting)
			stub.mu.Unlock()
			<-request.Context().Done()
			close(canceled)
			stub.mu.Lock()
			return
		}
		page := observability.EventPage{Events: []contracts.Event{}, Cursor: request.URL.Query().Get("cursor")}
		for _, event := range stub.events {
			if event.Seq > position.Seq {
				if len(page.Events) == 1 {
					page.HasMore = true
					break
				}
				page.Events = append(page.Events, event)
				page.Cursor = event.Cursor
			}
		}
		if stub.mode == "gap" && request.URL.Query().Get("wait") == "1" {
			event := testEvent(testRun("gap", position.Seq+2))
			page.Events, page.Cursor = []contracts.Event{event}, event.Cursor
		}
		if stub.mode == "reset" && request.URL.Query().Get("wait") == "1" {
			writer.WriteHeader(409)
			_, _ = io.WriteString(writer, `{"code":"reset_required"}`)
			return
		}
		_ = json.NewEncoder(writer).Encode(page)
	case strings.HasPrefix(request.URL.Path, "/api/trace/runs/"):
		id := strings.TrimPrefix(request.URL.Path, "/api/trace/runs/")
		for _, run := range stub.snapshot.Runs {
			if run.ID == id {
				_ = json.NewEncoder(writer).Encode(map[string]any{"run": run, "events": []contracts.Event{testEvent(run)}, "reply": "/private/SECRET reply text"})
				return
			}
		}
		writer.WriteHeader(404)
	case request.URL.Path == "/health":
		_, _ = io.WriteString(writer, `{"status":"ready","protocol_version":1}`)
	case request.URL.Path == "/api/stats":
		_, _ = io.WriteString(writer, `{"total_received":4}`)
	default:
		writer.WriteHeader(404)
	}
}

func traceSetup(t *testing.T, runs ...contracts.Run) (*Facade, *traceStub) {
	t.Helper()
	stub := &traceStub{snapshot: observability.Snapshot{StreamID: "stream-1", Cursor: testCursor("stream-1", 0), CoverageSince: time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC), Runs: []contracts.Run{}}}
	for _, run := range runs {
		stub.snapshot.Runs = append(stub.snapshot.Runs, run)
		stub.events = append(stub.events, testEvent(run))
		if run.EntitySeq > stub.snapshot.LastSeq {
			stub.snapshot.LastSeq = run.EntitySeq
		}
	}
	stub.snapshot.Cursor = testCursor(stub.snapshot.StreamID, stub.snapshot.LastSeq)
	server := httptest.NewServer(stub)
	t.Cleanup(server.Close)
	facade, err := New(Options{DispatcherURL: server.URL, StateDir: t.TempDir(), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = facade.Close() })
	return facade, stub
}

func consoleRequest(facade *Facade, path string) *httptest.ResponseRecorder {
	writer := httptest.NewRecorder()
	facade.Handler().ServeHTTP(writer, httptest.NewRequest("GET", "http://127.0.0.1:8090/console/v1/"+path, nil))
	return writer
}

func TestTraceBootstrapMappingAndDetail(t *testing.T) {
	run := testRun("run-1", 1)
	started, submitted, confirmed := run.AcceptedAt.Add(10*time.Second), run.AcceptedAt.Add(20*time.Second), run.UpdatedAt
	run.Phase, run.Outcome, run.Delivery = contracts.PhaseTerminal, contracts.OutcomeReplied, contracts.DeliveryConfirmed
	run.Attempts[0].Status, run.Attempts[0].EndedAt = contracts.AttemptSucceeded, &submitted
	run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &started, SubmittedAt: &submitted, ConfirmedAt: &confirmed, BridgeRequestID: "bridge-1", EchoMessageID: "echo-1"}
	facade, stub := traceSetup(t, run)
	bootstrap := facade.Bootstrap(context.Background())
	if bootstrap.Cursor == nil || *bootstrap.Cursor != stub.snapshot.Cursor || bootstrap.LastSeq == nil || *bootstrap.LastSeq != 1 || !bootstrap.Capabilities["traceRuns"].Enabled || !bootstrap.Capabilities["resumableRunEvents"].Supported {
		t.Fatalf("bad bootstrap: %+v", bootstrap)
	}
	if bootstrap.Overview.StreamID != "" || bootstrap.Overview.EntitySeq != nil {
		t.Fatal("mixed overview claimed a global trace position")
	}
	recent := bootstrap.Overview.Data.(map[string]any)["recentRuns"].([]RunView)
	if len(recent) != 1 || recent[0].Delivery != contracts.DeliveryConfirmed || recent[0].DeliveryEvidence.ConfirmedAt == nil || recent[0].Excerpt != nil || len(recent[0].Attempts) != 1 || recent[0].MissingSnapshotReason == nil {
		t.Fatalf("bad run mapping: %+v", recent)
	}
	detail, err := facade.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	view := detail.Data.(RunDetail)
	if len(view.Timeline) != 1 || view.Timeline[0].Certainty != "observed" || !view.Timeline[0].OccurredAt.Equal(run.UpdatedAt) || detail.Revision != run.Revision || detail.EntitySeq == nil || *detail.EntitySeq != 1 {
		t.Fatalf("bad timeline: %+v", detail)
	}
	for _, path := range []string{"bootstrap", "runs", "runs/run-1"} {
		response := consoleRequest(facade, path)
		if response.Code != 200 || strings.Contains(response.Body.String(), "SECRET") || strings.Contains(response.Body.String(), "reply text") {
			t.Fatalf("bad response: %d %s", response.Code, response.Body.String())
		}
	}
	if response := consoleRequest(facade, "runs/missing"); response.Code != 404 {
		t.Fatalf("missing run: %d %s", response.Code, response.Body.String())
	}
	for _, flag := range []string{"cancel", "pause", "service.actions", "configSnapshots"} {
		if bootstrap.Capabilities[flag].Supported {
			t.Fatalf("unsupported capability enabled: %s", flag)
		}
	}
}

func TestRunPaginationFreezesRowsAndFilters(t *testing.T) {
	facade, stub := traceSetup(t, testRun("a", 1), testRun("b", 2), testRun("c", 3))
	query := url.Values{"limit": {"1"}, "phases": {"executing"}, "groupId": {"1@chatroom"}}
	first, err := facade.ListRuns(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	page := first.Data.(RunPage)
	if len(page.Items) != 1 || page.Items[0].ID != "a" || page.NextCursor == nil {
		t.Fatalf("first page: %+v", page)
	}
	stub.mu.Lock()
	newer := testRun("newer", 4)
	newer.AcceptedAt = newer.AcceptedAt.Add(10 * time.Second)
	stub.snapshot.Runs = append(stub.snapshot.Runs, newer)
	stub.snapshot.Runs[1].Phase = contracts.PhaseChecking
	stub.snapshot.LastSeq, stub.snapshot.Cursor = 4, testCursor("stream-1", 4)
	stub.mu.Unlock()
	query.Set("cursor", *page.NextCursor)
	second, err := facade.ListRuns(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	page = second.Data.(RunPage)
	if len(page.Items) != 1 || page.Items[0].ID != "b" || page.Items[0].Phase != contracts.PhaseExecuting || second.ObservedAt != first.ObservedAt || page.Watermark != first.Revision {
		t.Fatalf("pagination drifted: %+v", second)
	}
	query.Set("cursor", *page.NextCursor)
	last, err := facade.ListRuns(context.Background(), query)
	if err != nil || last.Data.(RunPage).Items[0].ID != "c" || last.Data.(RunPage).NextCursor != nil {
		t.Fatalf("last page: %+v %v", last, err)
	}
	query.Set("groupId", "2@chatroom")
	if _, err := facade.ListRuns(context.Background(), query); err == nil || err.(*Problem).Code != "reset_required" {
		t.Fatalf("cursor accepted changed filters: %v", err)
	}
	query.Set("groupId", "1@chatroom")
	facade.traceMu.Lock()
	for _, listing := range facade.listings {
		listing.createdAt = time.Now().Add(-2 * listingTTL)
	}
	facade.traceMu.Unlock()
	if _, err := facade.ListRuns(context.Background(), query); err == nil || err.(*Problem).Code != "reset_required" {
		t.Fatalf("expired page: %v", err)
	}
}

func TestRunFilters(t *testing.T) {
	first, second := testRun("a", 1), testRun("b", 2)
	second.Phase, second.PlannedBackendID = contracts.PhaseChecking, "alma"
	second.Attempts = append(second.Attempts, contracts.Attempt{ID: "attempt-2", BackendID: "alma", Status: contracts.AttemptRunning, RemoteExecution: contracts.RemoteMayBeRunning, StartedAt: &second.AcceptedAt, MissingExternalIDReason: "not_retained", FallbackReason: "primary_failed", Versions: second.Versions})
	facade, _ := traceSetup(t, first, second)
	for _, scenario := range []struct {
		query string
		count int
	}{
		{"groupId=2%40chatroom", 2}, {"phases=checking", 1}, {"phases=executing,checking&phases=checking", 2},
		{"outcomes=failed", 0}, {"deliveries=not_started", 2}, {"backendId=alma", 1}, {"backendId=hermes", 2},
		{"fallback=true", 1}, {"fallback=false", 1}, {"version=cfg-1", 2}, {"version=missing", 0},
		{"completeness=complete", 0}, {"query=message-a", 1}, {"query=reply", 0},
		{"from=2026-11-19T12:00:00Z&to=2026-11-19T12:01:00Z", 2}, {"to=2026-11-19T12:00:00Z", 0},
	} {
		query, _ := url.ParseQuery(scenario.query)
		result, err := facade.ListRuns(context.Background(), query)
		if err != nil || len(result.Data.(RunPage).Items) != scenario.count {
			t.Fatalf("%s: %+v %v", scenario.query, result, err)
		}
	}
	for _, query := range []string{"limit=0", "limit=101", "limit=x", "from=bad", "from=2026-11-20T00:00:00Z&to=2026-11-19T00:00:00Z", "phases=imaginary", "fallback=1", "completeness=invalid", "wat=1", "groupId=a&groupId=b"} {
		response := consoleRequest(facade, "runs?"+query)
		if response.Code != 400 {
			t.Fatalf("accepted invalid query %s: %d", query, response.Code)
		}
	}
}

func TestFallbackRequiresEvidenceNotAttemptCount(t *testing.T) {
	run := testRun("a", 1)
	result := run.Attempts[0]
	result.ID, result.BackendID = "result", "hermes"
	run.Attempts = append(run.Attempts, result)
	view := runView(run)
	yes, no := true, false
	if matchingRun(view, RunFilter{Fallback: &yes}) || !matchingRun(view, RunFilter{Fallback: &no}) {
		t.Fatal("adapter invocation plus result was misrepresented as a fallback")
	}
}

func TestRunSanitization(t *testing.T) {
	run := testRun("safe", 1)
	run.ReasonCode, run.Trigger, run.ConfigSnapshotID = "/private/SECRET reply text", "/private/SECRET", "/private/SECRET"
	run.MissingSnapshotReason, run.PlannedBackendID = "reply text", "/private/SECRET"
	run.Attempts[0].ModelLabel, run.Attempts[0].ExternalID, run.Attempts[0].FallbackReason = "reply text", "/private/SECRET", "reply text"
	run.DeliveryEvidence.FailureEvidence = "/private/SECRET reply text"
	data, _ := json.Marshal(runView(run))
	if strings.Contains(string(data), "SECRET") || strings.Contains(string(data), "reply text") || strings.Contains(string(data), "/private/") {
		t.Fatalf("trace leaked private text: %s", data)
	}
	if !strings.Contains(string(data), `"excerpt":null`) || !strings.Contains(string(data), `"sideEffects":[]`) {
		t.Fatalf("unavailable body or nil list semantics: %s", data)
	}
}

func TestRunSSEReplayAndGap(t *testing.T) {
	for _, mode := range []string{"reset", "gap"} {
		t.Run(mode, func(t *testing.T) {
			facade, stub := traceSetup(t, testRun("a", 1), testRun("b", 2))
			stub.mode = mode
			response := consoleRequest(facade, "events?cursor="+testCursor("stream-1", 0))
			body := response.Body.String()
			if response.Code != 200 || response.Header().Get("Content-Type") != "text/event-stream" || strings.Count(body, "event: run\n") != 2 || !strings.Contains(body, "event: reset_required\n") || strings.Contains(body, "SECRET") {
				t.Fatalf("bad SSE: %d %s", response.Code, body)
			}
			if strings.Index(body, `"entityId":"a"`) > strings.Index(body, `"entityId":"b"`) {
				t.Fatal("replay out of order")
			}
			stub.mu.Lock()
			defer stub.mu.Unlock()
			if len(stub.queries) != 3 || stub.queries[1].Get("wait") != "" || stub.queries[2].Get("wait") != "1" {
				t.Fatalf("bad replay/longpoll: %+v", stub.queries)
			}
		})
	}
}

func TestRunSSECloseCancelsLongpoll(t *testing.T) {
	facade, stub := traceSetup(t)
	stub.mode, stub.waiting, stub.canceled = "cancel", make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(facade.Handler())
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/console/v1/events?cursor="+testCursor("stream-1", 0), nil)
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
	cancel()
	select {
	case <-stub.canceled:
	case <-time.After(time.Second):
		t.Fatal("client disconnect did not cancel upstream longpoll")
	}
}

func TestRunSSEKeepaliveHasNoBusinessTime(t *testing.T) {
	if testing.Short() {
		t.Skip("15 second keepalive integration check")
	}
	facade, stub := traceSetup(t)
	stub.mode, stub.waiting, stub.canceled = "cancel", make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(facade.Handler())
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/console/v1/events?cursor="+testCursor("stream-1", 0), nil)
	started := time.Now()
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil || line != ": keepalive\n" || time.Since(started) < 14*time.Second || time.Since(started) > 17*time.Second {
		t.Fatalf("bad heartbeat: %q %v after %v", line, err, time.Since(started))
	}
}

func TestRunSSEExpiredAndLastEventID(t *testing.T) {
	facade, stub := traceSetup(t, testRun("a", 1))
	stub.mode = "expired"
	response := consoleRequest(facade, "events?cursor="+stub.snapshot.Cursor)
	if response.Code != 409 || !strings.Contains(response.Body.String(), `"code":"reset_required"`) {
		t.Fatalf("expired cursor: %d %s", response.Code, response.Body.String())
	}
	stub.mode = "reset"
	request := httptest.NewRequest("GET", "http://127.0.0.1:8090/console/v1/events?cursor="+testCursor("stream-1", 0), nil)
	request.Header.Set("Last-Event-ID", stub.snapshot.Cursor)
	writer := httptest.NewRecorder()
	facade.Handler().ServeHTTP(writer, request)
	if strings.Contains(writer.Body.String(), "event: run\n") || !strings.Contains(writer.Body.String(), "event: reset_required\n") {
		t.Fatalf("Last-Event-ID ignored: %s", writer.Body.String())
	}
}

func TestNoTraceDegradesWithoutLegacyReads(t *testing.T) {
	facade, stub, _ := setup(t)
	bootstrap := facade.Bootstrap(context.Background())
	if bootstrap.Cursor != nil || bootstrap.LastSeq != nil || bootstrap.Capabilities["traceRuns"].Supported || bootstrap.Capabilities["resumableRunEvents"].Enabled || len(bootstrap.Overview.Data.(map[string]any)["recentRuns"].([]RunView)) != 0 {
		t.Fatalf("fabricated trace: %+v", bootstrap)
	}
	for _, path := range []string{"runs", "runs/run-1", "events?cursor=" + testCursor("stream-1", 0)} {
		response := consoleRequest(facade, path)
		if response.Code != 501 {
			t.Fatalf("no-trace %s: %d %s", path, response.Code, response.Body.String())
		}
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	for _, path := range stub.paths {
		if strings.Contains(path, "log") || strings.Contains(path, "history") {
			t.Fatalf("legacy source used: %s", path)
		}
	}
}

func TestTraceCapabilityRequiresIndependentReplaySupport(t *testing.T) {
	for _, mode := range []string{"missing_events", "unavailable_snapshot", "malformed_snapshot"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/api/trace/snapshot" {
					switch mode {
					case "unavailable_snapshot":
						writer.WriteHeader(503)
						return
					case "malformed_snapshot":
						_, _ = io.WriteString(writer, `{"streamId":"stream-1","lastSeq":8,"cursor":"invalid","runs":[]}`)
						return
					}
					_ = json.NewEncoder(writer).Encode(observability.Snapshot{StreamID: "stream-1", Cursor: testCursor("stream-1", 0), CoverageSince: time.Now().UTC(), Runs: []contracts.Run{}})
					return
				}
				writer.WriteHeader(404)
			}))
			defer server.Close()
			facade, err := New(Options{DispatcherURL: server.URL, StateDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer facade.Close()
			bootstrap := facade.Bootstrap(context.Background())
			if bootstrap.Capabilities["resumableRunEvents"].Supported || bootstrap.EventStream.Cursor != nil {
				t.Fatalf("replay claimed without support: %+v", bootstrap)
			}
			if mode == "missing_events" {
				if !bootstrap.Capabilities["traceRuns"].Enabled || bootstrap.LastSeq == nil || *bootstrap.LastSeq != 0 || bootstrap.Cursor == nil {
					t.Fatalf("snapshot position lost when replay unsupported: %+v", bootstrap)
				}
			} else if bootstrap.Cursor != nil || bootstrap.Capabilities["traceRuns"].Supported {
				t.Fatalf("invalid trace claimed as supported: %+v", bootstrap)
			}
		})
	}
}

func TestTraceRejectsInconsistentReplay(t *testing.T) {
	for _, mode := range []string{"stream_change", "duplicate", "empty_cursor_change", "page_cursor_change", "other_domain", "bad_json"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				event := testEvent(testRun("a", 1))
				page := observability.EventPage{Events: []contracts.Event{event}, Cursor: event.Cursor}
				switch mode {
				case "stream_change":
					page.Events[0].StreamID = "other"
				case "duplicate":
					page.Events = append(page.Events, event)
				case "empty_cursor_change":
					page.Events = []contracts.Event{}
				case "page_cursor_change":
					page.Cursor = testCursor("stream-1", 0)
				case "other_domain":
					page.Events[0].Entity = "operation"
				case "bad_json":
					_, _ = io.WriteString(writer, `{"events":null}`)
					return
				}
				_ = json.NewEncoder(writer).Encode(page)
			}))
			defer server.Close()
			backend, err := newUpstream(Options{DispatcherURL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			_, err = backend.traceEvents(context.Background(), testCursor("stream-1", 0), false)
			if err == nil {
				t.Fatal("accepted inconsistent event page")
			}
			expected := "reset_required"
			if mode == "bad_json" {
				expected = "unavailable"
			}
			if err.(*Problem).Code != expected {
				t.Fatalf("unexpected failure: %v", err)
			}
		})
	}
}

func TestTraceCursorRejectsExtraContentAndFrameInjection(t *testing.T) {
	for _, cursor := range []string{
		testCursor("stream-1", 0) + "\n",
		base64.RawURLEncoding.EncodeToString([]byte(`{"StreamID":"stream-1","Seq":0,"reply":"SECRET"}`)),
		base64.RawURLEncoding.EncodeToString([]byte(`{"StreamID":"stream-1","Seq":0} {}`)),
		strings.Repeat("a", 2049),
	} {
		if _, err := parseTraceCursor(cursor); err == nil {
			t.Fatal("accepted noncanonical or extra cursor content")
		}
	}
}

func TestRunPaginationRejectsTamperingAndEvicts(t *testing.T) {
	facade, _ := traceSetup(t, testRun("a", 1), testRun("b", 2))
	query := url.Values{"limit": {"1"}}
	first, err := facade.ListRuns(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	cursor := *first.Data.(RunPage).NextCursor
	data, _ := base64.RawURLEncoding.DecodeString(cursor)
	var position runCursor
	_ = json.Unmarshal(data, &position)
	position.Offset++
	data, _ = json.Marshal(position)
	query.Set("cursor", base64.RawURLEncoding.EncodeToString(data))
	if _, err := facade.ListRuns(context.Background(), query); err == nil || err.(*Problem).Code != "reset_required" {
		t.Fatalf("edited cursor accepted: %v", err)
	}
	query.Del("cursor")
	for index := 0; index < 16; index++ {
		if _, err := facade.ListRuns(context.Background(), query); err != nil {
			t.Fatal(err)
		}
	}
	query.Set("cursor", cursor)
	if _, err := facade.ListRuns(context.Background(), query); err == nil || err.(*Problem).Code != "reset_required" {
		t.Fatalf("evicted cursor accepted: %v", err)
	}
}

func TestOperationIDsAndBootstrapRecovery(t *testing.T) {
	facade, stub, options := setup(t)
	plan := planEdit(t, facade, fieldEdit("1@chatroom", "target", `"changed"`))
	stub.mode = "lostreadback"
	operation, err := facade.PublishChange(context.Background(), PublishInput{RequestID: "publish-id", PlanID: plan.ID, Token: plan.Token})
	if err != nil {
		t.Fatal(err)
	}
	if err := facade.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	active := reopened.Bootstrap(context.Background()).ActiveOperations
	if len(active) != 1 || active[0].ID != operation.ID || active[0].Status != "unknown" {
		t.Fatalf("operation not recoverable: %+v", active)
	}
	for _, path := range []string{"operations/" + operation.ID, "operations/publish-id", "operations/by-request/publish-id"} {
		response := consoleRequest(reopened, path)
		if response.Code != 200 || !strings.Contains(response.Body.String(), fmt.Sprintf(`"id":%q`, operation.ID)) {
			t.Fatalf("operation route %s: %d %s", path, response.Code, response.Body.String())
		}
	}
	if response := consoleRequest(reopened, "operations/by-request/missing"); response.Code != 200 || !strings.Contains(response.Body.String(), `"value":null`) {
		t.Fatalf("missing request result: %d %s", response.Code, response.Body.String())
	}
	if response := consoleRequest(reopened, "operations/missing"); response.Code != 404 {
		t.Fatalf("missing operation result: %d %s", response.Code, response.Body.String())
	}
}
