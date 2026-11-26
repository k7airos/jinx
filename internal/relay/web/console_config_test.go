package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"jinx/internal/relay/config"
	"jinx/internal/relay/model"
)

const consoleConfigPath = "/api/v1/console-config"

func consoleRequest(handler http.Handler, method, body, etag, contentType string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, "http://localhost"+consoleConfigPath, strings.NewReader(body))
	request.RemoteAddr = "127.0.0.1:12345"
	request.Header.Set("If-Match", etag)
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func encodePublic(t *testing.T, snapshot config.PublicSnapshot) string {
	t.Helper()
	data, err := json.Marshal(config.PublicConfig{GroupControl: snapshot.GroupControl, Filters: snapshot.Filters})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func decodePublic(t *testing.T, response *httptest.ResponseRecorder) config.PublicSnapshot {
	t.Helper()
	var object map[string]json.RawMessage
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &object) != nil || len(object) != 3 || object["revision"] == nil || object["group_control"] == nil || object["filters"] == nil {
		t.Fatalf("unexpected response shape/status: %d %s", response.Code, response.Body.String())
	}
	var snapshot config.PublicSnapshot
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Revision == "" || response.Header().Get("ETag") != `"`+snapshot.Revision+`"` || response.Header().Get("Cache-Control") != "no-store" || response.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("unexpected public response headers: %v", response.Header())
	}
	return snapshot
}

func TestConsoleConfigRoundTripWithoutLocalStore(t *testing.T) {
	server := newTestServer(t)
	private := server.cfgMgr.Get()
	private.Upstream.WSURL = "ws://synthetic.invalid/synthetic-private-token"
	if err := server.cfgMgr.Update(private); err != nil {
		t.Fatal(err)
	}
	handler, err := server.Handler()
	if err != nil {
		t.Fatal(err)
	}
	before := decodePublic(t, consoleRequest(handler, http.MethodGet, "", "", ""))
	for _, empty := range []bool{false, true} {
		public := config.PublicSnapshot{GroupControl: config.GroupControl{ExposedGroups: []string{"test@chatroom"}}}
		if empty {
			public.GroupControl.MonitoredGroups = []string{}
			public.GroupControl.GroupAliases = map[string]string{}
			public.Filters = config.FilterConfig{KeywordBlocklist: []string{}, RegexBlocklist: []string{}, SenderBlocklist: []string{}, AllowedTypes: []int{}}
		}
		response := consoleRequest(handler, http.MethodPut, encodePublic(t, public), `"`+before.Revision+`"`, "application/json; charset=utf-8")
		after := decodePublic(t, response)
		if !reflect.DeepEqual(after.GroupControl, public.GroupControl) || !reflect.DeepEqual(after.Filters, public.Filters) || after.Revision == before.Revision {
			t.Fatalf("public fields or revision lost: %s", response.Body.String())
		}
		cfg := server.cfgMgr.Get()
		if cfg.Server != private.Server || cfg.Upstream != private.Upstream {
			t.Fatal("public HTTP update changed private fields")
		}
		cfg.GroupControl.MonitorAll = true
		if !server.filterEng.Evaluate(&model.Message{GroupID: "test@chatroom", Type: 999, Content: "synthetic"}, cfg).Allowed {
			t.Fatal("relay nil/empty allowed_types must allow all types")
		}
		for _, forbidden := range []string{"server", "upstream", "local_db", "keys_file", "enc_key", "synthetic-private-token"} {
			if strings.Contains(response.Body.String(), forbidden) {
				t.Fatalf("public response leaked %s", forbidden)
			}
		}
		before = decodePublic(t, consoleRequest(handler, http.MethodGet, "", "", ""))
	}
	if stats := server.hub.GetStats(server.cfgMgr.Get()); stats.TotalReceived != 0 || stats.TotalForwarded != 0 || len(server.hub.GetRecentMessages()) != 0 {
		t.Fatal("console config caused history or relay side effects")
	}
}

func TestConsoleConfigRejectsInvalidRequests(t *testing.T) {
	server := newTestServer(t)
	handler, _ := server.Handler()
	before := server.cfgMgr.GetPublic()
	body := encodePublic(t, before)
	etag := `"` + before.Revision + `"`
	for _, check := range []struct {
		name, method, body, etag, contentType string
		status                                int
	}{
		{"missing precondition", "PUT", body, "", "application/json", 428},
		{"wildcard precondition", "PUT", body, "*", "application/json", 400},
		{"weak precondition", "PUT", body, "W/" + etag, "application/json", 400},
		{"bare precondition", "PUT", body, before.Revision, "application/json", 400},
		{"stale precondition", "PUT", body, `"` + strings.Repeat("0", 64) + `"`, "application/json", 409},
		{"opaque stale precondition", "PUT", body, `"stale"`, "application/json", 409},
		{"multiple preconditions", "PUT", body, etag + "," + etag, "application/json", 400},
		{"content type", "PUT", body, etag, "text/plain", 415},
		{"missing content type", "PUT", body, etag, "", 415},
		{"oversize", "PUT", strings.Repeat(" ", 256<<10) + body, etag, "application/json", 413},
		{"invalid JSON", "PUT", "{", etag, "application/json", 400},
		{"trailing JSON", "PUT", body + `{}`, etag, "application/json", 400},
		{"null", "PUT", "null", etag, "application/json", 400},
		{"missing objects", "PUT", `{}`, etag, "application/json", 400},
		{"null objects", "PUT", `{"group_control":null,"filters":null}`, etag, "application/json", 400},
		{"partial objects", "PUT", `{"group_control":{},"filters":{}}`, etag, "application/json", 400},
		{"private field", "PUT", strings.TrimSuffix(body, "}") + `,"local_db":{"enabled":true}}`, etag, "application/json", 400},
		{"revision field", "PUT", strings.TrimSuffix(body, "}") + `,"revision":"x"}`, etag, "application/json", 400},
		{"nested unknown", "PUT", strings.Replace(body, `"ignore_echo":false`, `"ignore_echo":false,"secret":true`, 1), etag, "application/json", 400},
		{"wrong type", "PUT", strings.Replace(body, `"ignore_echo":false`, `"ignore_echo":"true"`, 1), etag, "application/json", 400},
		{"null boolean", "PUT", strings.Replace(body, `"ignore_echo":false`, `"ignore_echo":null`, 1), etag, "application/json", 400},
		{"fractional type", "PUT", strings.Replace(body, `"allowed_types":[1,81]`, `"allowed_types":[1.5]`, 1), etag, "application/json", 400},
		{"string type", "PUT", strings.Replace(body, `"allowed_types":[1,81]`, `"allowed_types":["1"]`, 1), etag, "application/json", 400},
		{"null type", "PUT", strings.Replace(body, `"allowed_types":[1,81]`, `"allowed_types":[null]`, 1), etag, "application/json", 400},
		{"overflow type", "PUT", strings.Replace(body, `"allowed_types":[1,81]`, `"allowed_types":[9223372036854775808]`, 1), etag, "application/json", 400},
		{"negative type", "PUT", strings.Replace(body, `"allowed_types":[1,81]`, `"allowed_types":[-1]`, 1), etag, "application/json", 400},
		{"zero type", "PUT", strings.Replace(body, `"allowed_types":[1,81]`, `"allowed_types":[0]`, 1), etag, "application/json", 400},
		{"null keyword", "PUT", strings.Replace(body, `"regex_blocklist":[]`, `"regex_blocklist":[null]`, 1), etag, "application/json", 400},
		{"invalid regex", "PUT", strings.Replace(body, `"regex_blocklist":[]`, `"regex_blocklist":["["]`, 1), etag, "application/json", 400},
		{"invalid ID", "PUT", strings.Replace(body, "test@chatroom", "bad@@chatroom", 1), etag, "application/json", 400},
		{"partial wildcard", "PUT", strings.Replace(body, "test@chatroom", "test*", 1), etag, "application/json", 400},
		{"method", "POST", body, etag, "application/json", 405},
	} {
		t.Run(check.name, func(t *testing.T) {
			response := consoleRequest(handler, check.method, check.body, check.etag, check.contentType)
			if response.Code != check.status {
				t.Fatalf("got %d want %d: %s", response.Code, check.status, response.Body.String())
			}
			if !reflect.DeepEqual(before, server.cfgMgr.GetPublic()) {
				t.Fatal("rejected request changed config")
			}
		})
	}
}

func TestConsoleConfigCASAndLegacyConflict(t *testing.T) {
	server := newTestServer(t)
	handler, _ := server.Handler()
	before := server.cfgMgr.GetPublic()
	body := encodePublic(t, before)
	etag := `"` + before.Revision + `"`
	var workers sync.WaitGroup
	results := make(chan int, 12)
	start := make(chan struct{})
	for writer := 0; writer < cap(results); writer++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			results <- consoleRequest(handler, "PUT", body, etag, "application/json").Code
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	successes := 0
	for status := range results {
		if status == 200 {
			successes++
		} else if status != 409 {
			t.Fatalf("unexpected CAS status: %d", status)
		}
	}
	if successes != 1 {
		t.Fatalf("expected one HTTP CAS winner, got %d", successes)
	}
	before = server.cfgMgr.GetPublic()
	legacy := server.cfgMgr.Get()
	legacy.Upstream.WSURL = "ws://synthetic.invalid/legacy"
	data, _ := json.Marshal(legacy)
	if response := localRequest(t, handler, "POST", "/api/v1/config", string(data)); response.Code != 200 {
		t.Fatalf("legacy POST compatibility: %d", response.Code)
	}
	response := consoleRequest(handler, "PUT", encodePublic(t, before), `"`+before.Revision+`"`, "application/json")
	if response.Code != 409 || server.cfgMgr.Get().Upstream.WSURL != legacy.Upstream.WSURL {
		t.Fatal("legacy update did not invalidate HTTP CAS")
	}
}

func TestConsoleConfigExplicitWildcardPolicy(t *testing.T) {
	server := newTestServer(t)
	handler, _ := server.Handler()
	before := server.cfgMgr.GetPublic()
	before.GroupControl = config.GroupControl{MonitorAll: false, MonitoredGroups: []string{"*"}, ExposedGroups: []string{"*"}}
	before.Filters = config.FilterConfig{AllowedTypes: []int{244813135921}}
	after := decodePublic(t, consoleRequest(handler, "PUT", encodePublic(t, before), `"`+before.Revision+`"`, "application/json"))
	if !reflect.DeepEqual(after.GroupControl, before.GroupControl) || !server.filterEng.Evaluate(&model.Message{GroupID: "another@chatroom", Type: 244813135921}, server.cfgMgr.Get()).Allowed {
		t.Fatal("explicit wildcards or extended message types lost")
	}
	before = after
	before.GroupControl.MonitorAll = true
	before.GroupControl.MonitoredGroups = nil
	decodePublic(t, consoleRequest(handler, "PUT", encodePublic(t, before), `"`+before.Revision+`"`, "application/json"))
}

func TestConsoleConfigEmptyScopeDoesNotWidenMonitoringOrExposure(t *testing.T) {
	server := newTestServer(t)
	handler, _ := server.Handler()
	for _, empty := range []bool{false, true} {
		before := server.cfgMgr.GetPublic()
		public := config.PublicSnapshot{}
		if empty {
			public.GroupControl.MonitoredGroups = []string{}
			public.GroupControl.ExposedGroups = []string{}
		}
		decodePublic(t, consoleRequest(handler, "PUT", encodePublic(t, public), `"`+before.Revision+`"`, "application/json"))
		cfg := server.cfgMgr.Get()
		message := &model.Message{GroupID: "synthetic@chatroom", Type: 1}
		if result := server.filterEng.Evaluate(message, cfg); result.Action != model.ActionBlockedUnmonitored {
			t.Fatal("empty monitored list widened monitoring", result)
		}
		cfg.GroupControl.MonitorAll = true
		if result := server.filterEng.Evaluate(message, cfg); result.Action != model.ActionBlockedUnexposed {
			t.Fatal("empty exposed list widened exposure", result)
		}
	}
}

func TestConsoleConfigAdminGuardAndNoCORSPreflight(t *testing.T) {
	server := newTestServer(t)
	handler, _ := server.Handler()
	for _, check := range []struct {
		method, host, remote, origin, site string
		status                             int
	}{
		{"GET", "localhost", "127.0.0.1:1234", "http://localhost", "same-origin", 200},
		{"GET", "[::1]", "[::1]:1234", "http://[::1]", "same-origin", 200},
		{"GET", "localhost", "192.0.2.1:1234", "", "", 403},
		{"GET", "attacker.invalid", "127.0.0.1:1234", "", "", 403},
		{"GET", "localhost", "127.0.0.1:1234", "https://localhost", "", 403},
		{"GET", "localhost", "127.0.0.1:1234", "http://localhost:9999", "", 403},
		{"GET", "localhost", "127.0.0.1:1234", "", "cross-site", 403},
		{"OPTIONS", "localhost", "127.0.0.1:1234", "https://attacker.invalid", "cross-site", 403},
		{"OPTIONS", "localhost", "127.0.0.1:1234", "http://localhost", "same-origin", 405},
	} {
		request := httptest.NewRequest(check.method, "http://"+check.host+consoleConfigPath, nil)
		request.RemoteAddr = check.remote
		request.Header.Set("Origin", check.origin)
		request.Header.Set("Sec-Fetch-Site", check.site)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != check.status || response.Header().Get("Access-Control-Allow-Origin") != "" || response.Header().Get("Access-Control-Allow-Headers") != "" {
			t.Fatalf("admin guard failed: %+v got %d %v", check, response.Code, response.Header())
		}
	}
}

func TestConsoleConfigSaveFailureDoesNotLeakPrivatePath(t *testing.T) {
	server := newTestServer(t)
	handler, _ := server.Handler()
	privatePath := filepath.Join(t.TempDir(), "synthetic-private-config", "config.json")
	manager, err := config.NewManager(privatePath)
	if err != nil {
		t.Fatal(err)
	}
	server.cfgMgr = manager
	before := manager.GetPublic()
	if err := os.Remove(privatePath); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(privatePath, 0700); err != nil {
		t.Fatal(err)
	}
	response := consoleRequest(handler, "PUT", encodePublic(t, before), `"`+before.Revision+`"`, "application/json")
	if response.Code != 500 || strings.Contains(response.Body.String(), privatePath) || !reflect.DeepEqual(before, manager.GetPublic()) {
		t.Fatal("failed save leaked private data or changed config", response.Code, response.Body.String())
	}
}
