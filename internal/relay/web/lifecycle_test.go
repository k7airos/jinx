package web

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"jinx/internal/relay/hub"
)

func TestServerListenFailureDoesNotSignalReadyOrHealthy(t *testing.T) {
	server := newTestServer(t)
	cfg := server.cfgMgr.Get()
	cfg.Server.ListenAddr = "synthetic-invalid-address"
	if err := server.cfgMgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(); err == nil {
		t.Fatal("expected listener failure")
	}
	select {
	case <-server.Ready():
		t.Fatal("failed listener reported ready")
	default:
	}
	response := httptest.NewRecorder()
	server.handleHealth(response, httptest.NewRequest("GET", "/health", nil))
	var health hub.Health
	if json.Unmarshal(response.Body.Bytes(), &health) != nil || response.Code != http.StatusServiceUnavailable || health.Status != "failed" {
		t.Fatal("failed listener reported healthy", response.Code, response.Body.String())
	}
	if err := server.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestServerStopJoinsSSEAndWebSocket(t *testing.T) {
	server := newTestServer(t)
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/sse" {
			server.handleSSE(w, r)
		} else {
			server.handleWS(w, r)
		}
	}))
	defer testServer.Close()
	response, err := http.Get(testServer.URL + "/sse")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	readEvent(t, bufio.NewScanner(response.Body))
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(testServer.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	var welcome map[string]string
	if err := connection.ReadJSON(&welcome); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var callers sync.WaitGroup
	for caller := 0; caller < 8; caller++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			if err := server.Stop(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	callers.Wait()
	stats := server.hub.GetStats(server.cfgMgr.Get())
	if stats.ActiveSSEClients != 0 || stats.ActiveWSClients != 0 {
		t.Fatal("Stop returned before stream handlers exited")
	}
	if err := server.Start(); err == nil {
		t.Fatal("stopped server restarted")
	}
	stopped := httptest.NewRecorder()
	server.handleHealth(stopped, httptest.NewRequest("GET", "/health", nil))
	if stopped.Code != http.StatusServiceUnavailable || !strings.Contains(stopped.Body.String(), `"status":"stopped"`) {
		t.Fatal("stopped server reported healthy")
	}
}

func TestServerShutdownDeadlineClosesConnections(t *testing.T) {
	server := newTestServer(t)
	entered := make(chan struct{})
	left := make(chan struct{})
	testServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(left)
	}))
	server.httpServer = testServer.Config
	testServer.Start()
	defer testServer.Close()
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, err := http.Get(testServer.URL)
		if err == nil {
			response.Body.Close()
		}
	}()
	<-entered
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := server.Stop(ctx); err != context.DeadlineExceeded {
		t.Fatalf("expected shutdown deadline, got %v", err)
	}
	select {
	case <-left:
	case <-time.After(time.Second):
		t.Fatal("shutdown failure left HTTP connection open")
	}
	<-requestDone
}
