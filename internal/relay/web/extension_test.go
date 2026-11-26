package web

import (
	"context"
	"net/http"
	"testing"

	"jinx/internal/relay/model"
)

type testExtension struct{ patterns []string }

func (testExtension) Groups(context.Context) []model.GroupInfo { panic("health must only use memory") }
func (testExtension) Start(func(*model.Message)) error         { return nil }
func (testExtension) Stop()                                    {}
func (extension testExtension) RegisterRoutes(register RouteRegistrar) error {
	for _, pattern := range extension.patterns {
		if err := register(pattern, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }); err != nil {
			return err
		}
	}
	return nil
}

func TestExtensionRoutesRejectConflictsAndHealthUsesMemory(t *testing.T) {
	for _, patterns := range [][]string{{"/api/v1/groups"}, {"/api/plugin", "/api/plugin"}, {"/api/"}, {"GET /api/plugin"}} {
		server := newTestServer(t)
		server.extension = testExtension{patterns}
		if _, err := server.Handler(); err == nil {
			t.Fatal("route conflict accepted", patterns)
		}
	}
	server := newTestServer(t)
	server.extension = testExtension{[]string{"/api/plugin"}}
	handler, err := server.Handler()
	if err != nil {
		t.Fatal(err)
	}
	if response := localRequest(t, handler, "GET", "/health", ""); response.Code != 200 {
		t.Fatal(response.Code)
	}
}

func TestPublicModeSupportsGroupsStatsAndWebhook(t *testing.T) {
	server := newTestServer(t)
	handler, err := server.Handler()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/v1/groups", "/api/v1/stats", "/health", "/api/v1/capabilities"} {
		if response := localRequest(t, handler, "GET", path, ""); response.Code != 200 {
			t.Fatal(path, response.Code)
		}
	}
	for _, path := range []string{"/api/webhook", "/api/v1/messages"} {
		if response := localRequest(t, handler, "POST", path, `{"group_id":"test@chatroom","sender_id":"synthetic","content":"webhook regression","type":1}`); response.Code != 200 {
			t.Fatal(path, response.Code, response.Body.String())
		}
	}
	for _, path := range []string{"/api/v1/history", "/api/v1/history/export", "/api/v1/sources", "/api/v1/attachments"} {
		if response := localRequest(t, handler, "GET", path, ""); response.Code != 404 {
			t.Fatal("private route remains in public server", path, response.Code)
		}
	}
}
