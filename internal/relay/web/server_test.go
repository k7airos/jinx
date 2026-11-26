package web

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"jinx/internal/relay/config"
	"jinx/internal/relay/filter"
	"jinx/internal/relay/hub"
	"jinx/internal/relay/ingest"
	"jinx/internal/relay/model"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	mgr, err := config.NewManager(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.DefaultConfig()
	cfg.GroupControl.MonitorAll = true
	cfg.GroupControl.ExposedGroups = []string{"test@chatroom"}
	cfg.GroupControl.GroupAliases = map[string]string{}
	if err := mgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	return NewServer(mgr, hub.NewHub(200), filter.NewEngine())
}

func TestLoopbackModeProtectsStreamsAndIngestion(t *testing.T) {
	s := newTestServer(t)
	cfg := s.cfgMgr.Get()
	cfg.Server.ListenAddr = "127.0.0.1:8080"
	if err := s.cfgMgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	handler, _ := s.Handler()
	for _, path := range []string{"/api/v1/messages", "/api/webhook", "/api/v1/stream", "/sse", "/api/v1/ws", "/ws"} {
		for _, host := range []string{"localhost", "attacker.example"} {
			r := httptest.NewRequest(http.MethodPost, "http://"+host+path, strings.NewReader(`{}`))
			r.RemoteAddr = "127.0.0.1:40000"
			r.Header.Set("Origin", "https://attacker.example")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != http.StatusForbidden || w.Header().Get("Access-Control-Allow-Origin") != "" {
				t.Fatalf("loopback endpoint allowed cross-site access: %s %d", path, w.Code)
			}
		}
	}
}

func TestReplayHonorsRevokedExposure(t *testing.T) {
	s := newTestServer(t)
	cfg := s.cfgMgr.Get()
	cfg.GroupControl.ExposedGroups = []string{"test@chatroom", "other@chatroom"}
	if err := s.cfgMgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	_, cursor, _ := s.hub.RegisterAIWithReplay(&hub.SSEClient{ID: "baseline", Ch: make(chan *model.Message, 1)}, "")
	s.hub.UnregisterSSE("baseline")
	s.ProcessIncomingMessage(&model.Message{ID: "revoked", GroupID: "test@chatroom", Type: 1, Content: "private"})
	s.ProcessIncomingMessage(&model.Message{ID: "allowed", GroupID: "other@chatroom", Type: 1, Content: "public"})
	cfg.GroupControl.ExposedGroups = []string{"other@chatroom"}
	if err := s.cfgMgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(s.handleSSE))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	req.Header.Set("Last-Event-ID", cursor)
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	scanner := bufio.NewScanner(response.Body)
	readEvent(t, scanner)
	event, data := readEvent(t, scanner)
	if event != "message" || !strings.Contains(data, `"id":"allowed"`) || strings.Contains(data, "private") {
		t.Fatalf("replay ignored updated exposure: %s %s", event, data)
	}
}

func readEvent(t *testing.T, scanner *bufio.Scanner) (string, string) {
	t.Helper()
	var event, data string
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" && event != "" {
			return event, data
		}
		if strings.HasPrefix(line, "event: ") {
			event = strings.TrimPrefix(line, "event: ")
		}
		if strings.HasPrefix(line, "data: ") {
			data = strings.TrimPrefix(line, "data: ")
		}
	}
	t.Fatalf("SSE ended before an event: %v", scanner.Err())
	return "", ""
}

func TestMonitorStreamIncludesBlockedMediaAndAIStreamStaysFiltered(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/monitor" {
			s.handleMonitorSSE(w, r)
		} else {
			s.handleSSE(w, r)
		}
	}))
	defer ts.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connect := func(path string) *bufio.Scanner {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+path, nil)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		// The request context closes streaming responses before server shutdown.
		scanner := bufio.NewScanner(res.Body)
		event, _ := readEvent(t, scanner)
		if event != "connected" {
			t.Fatalf("expected connected, got %s", event)
		}
		return scanner
	}
	s.ProcessIncomingMessage(&model.Message{ID: "earlier", GroupID: "private@chatroom", Type: model.TypeImage, Content: "<msg><img/></msg>"})
	monitor := connect("/monitor")
	event, data := readEvent(t, monitor)
	var snapshot []*model.Message
	if err := json.Unmarshal([]byte(data), &snapshot); err != nil || event != "snapshot" || len(snapshot) != 1 || snapshot[0].ID != "earlier" {
		t.Fatalf("missing reconnect history: event=%s data=%s err=%v", event, data, err)
	}
	ai := connect("/ai?all=1")
	messages := []*model.Message{
		{ID: "image", Type: model.TypeImage, Content: "<msg><img/></msg>"},
		{ID: "voice", Type: model.TypeVoice},
		{ID: "video", Type: model.TypeVideo},
		{ID: "emoji", Type: model.TypeEmoji},
		{ID: "card", Type: 244813135921, Content: "<msg><appmsg><title><![CDATA[带 <标签> 的引用]]></title></appmsg></msg>"},
		{ID: "system", Type: model.TypeSystem, Content: "某人撤回了一条消息"},
		{ID: "unknown", Type: 999},
		{ID: "keyword", Type: model.TypeText, Content: "A 拍了拍 B"},
		{ID: "unexposed", GroupID: "private@chatroom", Type: model.TypeText, Content: "private"},
		{ID: "allowed", Type: model.TypeText, Content: "@助手  保留后台原文"},
	}
	for _, msg := range messages {
		if msg.GroupID == "" {
			msg.GroupID = "test@chatroom"
		}
		msg.TypeName = ingest.GetTypeName(msg.Type)
		s.ProcessIncomingMessage(msg)
	}
	for _, want := range messages {
		event, data = readEvent(t, monitor)
		var got model.Message
		if err := json.Unmarshal([]byte(data), &got); err != nil || event != "message" || got.ID != want.ID || got.Content == "" {
			t.Fatalf("monitor missing %s: event=%s data=%s err=%v", want.ID, event, data, err)
		}
		if want.ID == "allowed" && got.Content != "@助手  保留后台原文" {
			t.Fatalf("monitor content was cleaned: %q", got.Content)
		}
	}
	event, data = readEvent(t, ai)
	var forwarded model.Message
	if err := json.Unmarshal([]byte(data), &forwarded); err != nil || event != "message" || forwarded.ID != "allowed" || forwarded.Content != "保留后台原文" {
		t.Fatalf("AI stream leaked blocked content or missed cleaning: %s (%v)", data, err)
	}
	stats := s.hub.GetStats(s.cfgMgr.Get())
	if stats.ActiveSSEClients != 1 || stats.TotalReceived != 11 || stats.TotalForwarded != 1 {
		t.Fatalf("wrong stream counters: %+v", stats)
	}
}

func TestWebSocketDisconnectUnregistersClient(t *testing.T) {
	s := newTestServer(t)
	done := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		s.handleWS(w, r)
	}))
	defer ts.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	var welcome map[string]string
	if err := conn.ReadJSON(&welcome); err != nil {
		t.Fatal(err)
	}
	defer s.hub.UnregisterWS(welcome["client_id"])
	conn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("disconnected websocket retained its handler and subscription")
	}
	if got := s.hub.GetStats(s.cfgMgr.Get()).ActiveWSClients; got != 0 {
		t.Fatalf("disconnected client still counted: %d", got)
	}
}

func TestRelayHardenedConfigBodyAndWebSocketOrigin(t *testing.T) {
	s := newTestServer(t)
	handler, err := s.Handler()
	if err != nil {
		t.Fatal(err)
	}

	// Runtime /api/v1/config must reject Server mutations.
	cfg := s.cfgMgr.Get()
	cfg.Server.ListenAddr = ":8080"
	cfg.Server.EnableCORS = true
	raw, _ := json.Marshal(cfg)
	rec := localRequest(t, handler, http.MethodPost, "/api/v1/config", string(raw))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected runtime server mutation rejection, got %d %s", rec.Code, rec.Body.String())
	}

	// Webhook enforces MaxBytesReader (1 MiB) and rejects form Content-Type.
	formRec := httptest.NewRecorder()
	formReq := httptest.NewRequest(http.MethodPost, "http://localhost/api/webhook", strings.NewReader(`{}`))
	formReq.RemoteAddr = "127.0.0.1:12345"
	formReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	handler.ServeHTTP(formRec, formReq)
	if formRec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("expected 415 for form Content-Type on webhook, got %d", formRec.Code)
	}

	bigBody := `{"group_id":"test@chatroom","content":"` + strings.Repeat("a", (1<<20)+1024) + `"}`
	bigRec := localRequest(t, handler, http.MethodPost, "/api/webhook", bigBody)
	if bigRec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized webhook body, got %d", bigRec.Code)
	}

	// WebSocket rejects cross-origin browser requests.
	ts := httptest.NewServer(handler)
	defer ts.Close()
	headers := http.Header{}
	headers.Set("Origin", "https://evil.example")
	_, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/api/v1/ws", headers)
	if err == nil || (resp != nil && resp.StatusCode != http.StatusForbidden) {
		t.Fatalf("expected cross-origin WS upgrade rejection, got err=%v resp=%v", err, resp)
	}
}
