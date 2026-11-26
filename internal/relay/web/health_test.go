package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"jinx/internal/relay/hub"
	"jinx/internal/relay/model"
)

func TestHealthDoesNotUseLocalDatabase(t *testing.T) {
	server := newTestServer(t)
	handler, err := server.Handler()
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://localhost/health", nil)
	request.RemoteAddr = "127.0.0.1:12345"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	var health hub.Health
	if err := json.Unmarshal(response.Body.Bytes(), &health); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || health.Service != "relay" || health.LastProgressAt != nil || health.LastEventID == "" {
		t.Fatal(response.Code, response.Body.String())
	}
	server.ProcessIncomingMessage(&model.Message{ID: "synthetic", GroupID: "test@chatroom", Type: 1, Content: "private health test"})
	health = server.hub.HealthSnapshot()
	if health.LastProgressAt == nil || health.Sequence != 1 {
		t.Fatal("memory watermark did not advance", health)
	}
	for _, check := range []struct {
		method, origin string
		status         int
	}{
		{http.MethodPost, "", http.StatusMethodNotAllowed},
		{http.MethodGet, "https://example.com", http.StatusForbidden},
	} {
		request := httptest.NewRequest(check.method, "http://localhost/health", nil)
		request.RemoteAddr = "127.0.0.1:12345"
		request.Header.Set("Origin", check.origin)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != check.status {
			t.Fatal(response.Code, response.Body.String())
		}
	}
	encoded, _ := json.Marshal(health)
	if strings.Contains(string(encoded), "private health test") || strings.Contains(string(encoded), "keys_file") {
		t.Fatal("health leaked private data")
	}
}
