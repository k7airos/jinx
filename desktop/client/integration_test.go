package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPClientEndpointsAndResumableSSE(t *testing.T) {
	var seenLastEventID atomic.Value
	var sseCount atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/console/v1/bootstrap":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{
				"ok": true,
				"value": {
					"streamId": "stream-1",
					"cursor": "cur-1",
					"lastSeq": 1,
					"overview": {
						"data": {
							"stats": {"total_received": 12},
							"statsSource": "persistent_cumulative",
							"statsAvailable": true,
							"pendingMeaning": "pending_and_in_flight_messages",
							"services": [],
							"controls": {"supported": false, "enabled": false, "reason": "read only"},
							"recentRuns": [],
							"trace": {"supported": true, "enabled": true, "domain": "run", "streamId": "stream-1", "cursor": "cur-1", "lastSeq": 1},
							"currentWork": {"executing": 1},
							"workSource": "retained_trace_snapshot",
							"attention": []
						},
						"observedAt": "2026-11-24T08:00:00Z",
						"source": "dispatcher",
						"freshness": "fresh",
						"completeness": "partial",
						"revision": "rev-1"
					},
					"activeOperations": [],
					"capabilities": {"catalog": {"supported": true, "enabled": true}},
					"schemas": {"schemaVersion": "console-rules-v1", "group": {}, "global": {}},
					"eventStream": {"supported": true, "enabled": true, "domain": "run"}
				}
			}`))
		case "/console/v1/connectors/bilibili/dry-run/apply":
			if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
				w.WriteHeader(http.StatusUnsupportedMediaType)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ok":false,"problem":{"code":"unknown","message":"Connector write outcome ambiguous","requestId":"req-1","retry":"read"}}`))
		case "/console/v1/events":
			if id := r.Header.Get("Last-Event-ID"); id != "" {
				seenLastEventID.Store(id)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			flusher := w.(http.Flusher)
			attempt := sseCount.Add(1)
			if attempt == 1 {
				_, _ = fmt.Fprint(w, ": keepalive\n\n")
				_, _ = fmt.Fprint(w, "id: cur-2\nevent: run\ndata: {\"streamId\":\"stream-1\",\"seq\":2,\"cursor\":\"cur-2\",\"occurredAt\":\"2026-11-24T08:01:00Z\",\"entity\":\"run\",\"entityId\":\"run-1\",\"revision\":\"rev-2\",\"kind\":\"run_changed\",\"reasonCode\":\"\",\"phase\":\"terminal\",\"outcome\":\"replied\",\"delivery\":\"confirmed\"}\n\n")
				flusher.Flush()
				return
			}
			_, _ = fmt.Fprint(w, "event: reset_required\ndata: {\"code\":\"reset_required\",\"message\":\"Cursor expired\",\"retry\":\"read\"}\n\n")
			flusher.Flush()
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	if !strings.HasPrefix(srv.URL, "http://127.0.0.1:") {
		t.Fatalf("unexpected httptest URL: %s", srv.URL)
	}
	hc, err := NewHTTPClient(srv.URL)
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	boot, err := hc.Bootstrap(ctx)
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if boot.Overview.Data.Stats.TotalReceived == nil || *boot.Overview.Data.Stats.TotalReceived != 12 {
		t.Fatalf("unexpected bootstrap overview: %+v", boot.Overview)
	}

	_, err = hc.ApplyConnectorDryRun(ctx, "bilibili", ConnectorDryRunApplyInput{
		RequestID: "req-1", PlanID: "p-1", Token: "t-1", Confirm: true,
	})
	var apiErr *APIError
	if !okAPI(err, &apiErr) || apiErr.Status != 202 || apiErr.Problem.Code != "unknown" {
		t.Fatalf("expected 202 unknown APIError, got %v", err)
	}

	events := make(chan RunEvent, 4)
	resets := make(chan Problem, 2)
	stop := hc.SubscribeRuns(ctx, "cur-1", RunCallbacks{
		OnEvent:         func(ev RunEvent) { events <- ev },
		OnResetRequired: func(p Problem) { resets <- p },
	})
	defer stop()

	select {
	case ev := <-events:
		if ev.EntityID != "run-1" || ev.Cursor != "cur-2" {
			t.Fatalf("unexpected SSE event: %+v", ev)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for SSE run event")
	}

	select {
	case p := <-resets:
		if p.Code != "reset_required" {
			t.Fatalf("unexpected reset problem: %+v", p)
		}
	case <-ctx.Done():
		t.Fatalf("timed out waiting for SSE reset_required after reconnect")
	}

	gotLast, _ := seenLastEventID.Load().(string)
	if gotLast != "cur-2" {
		t.Fatalf("expected reconnect to send Last-Event-ID=cur-2, got %q", gotLast)
	}

	// Ensure TriState encoding works inside Edit.Value too.
	raw, _ := json.Marshal(TriStringsEmpty())
	if string(raw) != "[]" {
		t.Fatalf("expected [] for empty tri-state")
	}
}

func okAPI(err error, target **APIError) bool {
	if err == nil {
		return false
	}
	typed, ok := err.(*APIError)
	if ok {
		*target = typed
	}
	return ok
}
