package web

import (
	"encoding/json"
	"jinx/internal/relay/model"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func localRequest(t *testing.T, handler http.Handler, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, "http://localhost"+path, strings.NewReader(body))
	r.RemoteAddr = "127.0.0.1:34567"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestAdminRejectsRemoteAndCrossSiteAccess(t *testing.T) {
	s := newTestServer(t)
	handler, _ := s.Handler()
	for _, tc := range []struct {
		remote, host, origin, site string
		status                     int
	}{
		{"127.0.0.1:1234", "localhost", "", "", 200},
		{"[::1]:1234", "[::1]", "http://[::1]", "same-origin", 200},
		{"192.0.2.1:1234", "localhost", "", "", 403},
		{"127.0.0.1:1234", "attacker.example", "http://attacker.example", "", 403},
		{"127.0.0.1:1234", "localhost", "https://attacker.example", "cross-site", 403},
	} {
		r := httptest.NewRequest("GET", "http://"+tc.host+"/api/v1/config", nil)
		r.RemoteAddr = tc.remote
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("Sec-Fetch-Site", tc.site)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != tc.status || w.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatalf("access control failed: %+v status %d", tc, w.Code)
		}
	}
}

func TestGroupTogglesReflectNameRulesAndRejectWildcardConflict(t *testing.T) {
	s := newTestServer(t)
	cfg := s.cfgMgr.Get()
	cfg.GroupControl.GroupAliases["regression@chatroom"] = "测试群"
	cfg.GroupControl.ExposedGroups = []string{" 测试群 "}
	cfg.GroupControl.MonitorAll = false
	cfg.GroupControl.MonitoredGroups = []string{"测试群"}
	if err := s.cfgMgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	handler, _ := s.Handler()
	for _, kind := range []string{"expose", "monitor"} {
		w := localRequest(t, handler, "POST", "/api/v1/groups/"+kind, `{"group_id":"regression@chatroom","exposed":false,"monitored":false}`)
		if w.Code != 200 {
			t.Fatalf("group toggle failed: %s", w.Body.String())
		}
	}
	cfg = s.cfgMgr.Get()
	if len(cfg.GroupControl.ExposedGroups) != 0 || len(cfg.GroupControl.MonitoredGroups) != 0 {
		t.Fatal("name rule survived disabling the group")
	}
	cfg.GroupControl.ExposedGroups = []string{"*"}
	if err := s.cfgMgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	if w := localRequest(t, handler, "POST", "/api/v1/groups/expose", `{"group_id":"regression@chatroom","exposed":false}`); w.Code != 409 {
		t.Fatal("wildcard conflict reported false success")
	}
}

func TestLiveMediaPayloadIsRedacted(t *testing.T) {
	s := newTestServer(t)
	s.ProcessIncomingMessage(&model.Message{ID: "image", GroupID: "test@chatroom", Type: 3, Content: `<msg><img aeskey="hidden-key"/></msg>`})
	data, _ := json.Marshal(s.hub.GetRecentMessages())
	if strings.Contains(string(data), "hidden-key") {
		t.Fatal("monitor payload leaked media key")
	}
}
