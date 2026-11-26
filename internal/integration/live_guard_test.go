package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestAlmaTransportGuardUsesOnlyOwnedRun(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		hits.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/models":
			_, _ = io.WriteString(writer, `[{"id":"synthetic-catalog-id"}]`)
		case "/api/personal/dispatch":
			_, _ = io.WriteString(writer, `{"id":"owned-run","threadId":"owned-thread","status":"running"}`)
		case "/api/personal/runs/owned-run":
			_, _ = io.WriteString(writer, `{"id":"owned-run","threadId":"owned-thread","status":"done","summary":"SYNTHETIC_NONCE"}`)
		default:
			t.Errorf("guard leaked unexpected request: %s %s", request.Method, request.URL.Path)
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(server.Close)
	guard, client := newAlmaGuard(server.URL, "synthetic-catalog-id", "SYNTHETIC_NONCE")
	t.Cleanup(guard.transport.CloseIdleConnections)
	requireCatalogModel(t, client, server.URL, "synthetic-catalog-id")
	deny := func(method, target, body string) {
		t.Helper()
		before := hits.Load()
		request, err := http.NewRequestWithContext(context.Background(), method, target, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		if err == nil || hits.Load() != before {
			t.Fatalf("guard contacted a denied path: %s %s", method, target)
		}
	}
	deny(http.MethodGet, "http://127.0.0.1:5033/health", "")
	deny(http.MethodGet, server.URL+"/api/settings/providers", "")
	deny(http.MethodPost, server.URL+"/api/personal/stop", `{"all":true}`)
	deny(http.MethodPost, server.URL+"/api/personal/dispatch", `{"title":"group execution synthetic","task":"read private context","model":"synthetic-catalog-id"}`)
	var dispatched almaObservation
	requestJSON(t, client, http.MethodPost, server.URL+"/api/personal/dispatch", map[string]string{"title": "group execution synthetic-trace", "task": "只输出SYNTHETIC_NONCE，不用工具、不读文件、不发送消息。", "model": "synthetic-catalog-id"}, &dispatched)
	if dispatched.ID != "owned-run" || dispatched.ThreadID != "owned-thread" {
		t.Fatalf("top-level Alma identity lost: %+v", dispatched)
	}
	deny(http.MethodGet, server.URL+"/api/personal/runs/another-run", "")
	deny(http.MethodPost, server.URL+"/api/personal/stop", `{"runId":"another-run"}`)
	deny(http.MethodPost, server.URL+"/api/personal/stop", `{"runId":"owned-run","all":true}`)
	deny(http.MethodPost, server.URL+"/api/personal/dispatch", `{"title":"group execution synthetic","task":"只输出SYNTHETIC_NONCE，不用工具、不读文件、不发送消息。","model":"synthetic-catalog-id"}`)
	var completed almaObservation
	requestJSON(t, client, http.MethodGet, server.URL+"/api/personal/runs/owned-run", nil, &completed)
	deny(http.MethodPost, server.URL+"/api/personal/stop", `{"runId":"owned-run"}`)
	observed, dispatches, polls, stops := guard.observation()
	if hits.Load() != 3 || dispatches != 1 || polls != 1 || stops != 0 || observed.ID != completed.ID || completed.Status != "done" || completed.Summary != "SYNTHETIC_NONCE" {
		t.Fatal("guard counters or same-run evidence disagree")
	}
}

func TestAlmaTransportRejectsSpoofedRunIdentity(t *testing.T) {
	for _, body := range []string{
		`{"run":{"id":"owned-run","threadId":"owned-thread","status":"running"}}`,
		`{"id":"owned-run","status":"running"}`,
		`{"id":"../another-run","threadId":"owned-thread","status":"running"}`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) { _, _ = io.WriteString(writer, body) }))
			t.Cleanup(server.Close)
			guard, client := newAlmaGuard(server.URL, "synthetic-model", "SYNTHETIC_NONCE")
			t.Cleanup(guard.transport.CloseIdleConnections)
			payload, _ := json.Marshal(map[string]string{"title": "group execution synthetic-trace", "task": "只输出SYNTHETIC_NONCE，不用工具、不读文件、不发送消息。", "model": "synthetic-model"})
			response, err := client.Post(server.URL+"/api/personal/dispatch", "application/json", strings.NewReader(string(payload)))
			if response != nil {
				_ = response.Body.Close()
			}
			if err == nil {
				t.Fatal("guard accepted a wrapped, missing-thread or unsafe run identity")
			}
		})
	}
}
