package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"jinx/internal/contracts"
)

type stubRegistryExecutor struct {
	platform contracts.Platform
	ready    bool
	targets  []HealthTarget
}

func (s *stubRegistryExecutor) Platform() contracts.Platform { return s.platform }
func (s *stubRegistryExecutor) AccountID() string            { return "acc-test" }
func (s *stubRegistryExecutor) ConnectorName() string        { return "test-connector" }
func (s *stubRegistryExecutor) Capabilities() []contracts.ActionKind {
	return []contracts.ActionKind{contracts.ActionReplyText, contracts.ActionThreadReply}
}
func (s *stubRegistryExecutor) Health(context.Context) (bool, string) { return s.ready, "" }
func (s *stubRegistryExecutor) Status() StatusSnapshot {
	now := time.Now().UTC()
	return StatusSnapshot{Targets: s.targets, LastPollAt: &now}
}
func (s *stubRegistryExecutor) Execute(context.Context, ActionRequest) (ActionResponse, error) {
	return ActionResponse{Status: "confirmed", ExternalID: "ext-1"}, nil
}

func TestNormalizeRegistryClientConfig(t *testing.T) {
	var empty RegistryClientConfig
	if err := NormalizeRegistryClientConfig(&empty); err != nil {
		t.Fatalf("empty config should succeed: %v", err)
	}

	missingURL := RegistryClientConfig{TokenFile: "/tmp/token"}
	if err := NormalizeRegistryClientConfig(&missingURL); err == nil {
		t.Fatal("expected error when token_file set without url")
	}

	nonLoopback := RegistryClientConfig{URL: "http://192.168.1.5:18088"}
	if err := NormalizeRegistryClientConfig(&nonLoopback); err == nil {
		t.Fatal("expected non-loopback url rejection")
	}

	badTTL := RegistryClientConfig{URL: "http://127.0.0.1:18088", HeartbeatIntervalSec: 15, HeartbeatTTLSec: 10}
	if err := NormalizeRegistryClientConfig(&badTTL); err == nil {
		t.Fatal("expected error when ttl <= interval")
	}

	valid := RegistryClientConfig{URL: "http://127.0.0.1:18088/"}
	if err := NormalizeRegistryClientConfig(&valid); err != nil {
		t.Fatalf("expected valid config, got %v", err)
	}
	if valid.URL != "http://127.0.0.1:18088" || valid.HeartbeatIntervalSec != DefaultHeartbeatIntervalSec || valid.HeartbeatTTLSec != DefaultHeartbeatTTLSec || valid.TimeoutSec != 5 {
		t.Fatalf("unexpected normalized defaults: %+v", valid)
	}
}

func TestRegistryClientRegisterHeartbeat404ReRegisterAndUnregister(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "registry.token")
	if err := os.WriteFile(tokenPath, []byte("secret-token-123\n"), 0600); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	registered := false
	registerCount := 0
	heartbeatCount := 0
	unregisterCount := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(ConnectorTokenHeader) != "secret-token-123" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": false, "code": "unauthorized"})
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/v1/connectors/register":
			var req RegisterRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode register: %v", err)
			}
			if req.Platform != "bilibili" || req.URL != "http://127.0.0.1:18092" || req.InstanceID == "" {
				t.Fatalf("unexpected register payload: %+v", req)
			}
			registered = true
			registerCount++
			_ = json.NewEncoder(w).Encode(RegisterResponse{OK: true, Platform: req.Platform, DiscoverySource: DiscoverySourceDynamic})
		case "/api/v1/connectors/heartbeat":
			var req HeartbeatRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode heartbeat: %v", err)
			}
			heartbeatCount++
			if !registered {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(HeartbeatResponse{OK: false, Code: "not_registered"})
				return
			}
			_ = json.NewEncoder(w).Encode(HeartbeatResponse{OK: true, Platform: req.Platform, HeartbeatState: HeartbeatAlive})
		case "/api/v1/connectors/unregister":
			unregisterCount++
			registered = false
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	exec := &stubRegistryExecutor{
		platform: contracts.PlatformBilibili,
		ready:    true,
		targets:  []HealthTarget{{ContainerID: "bilibili:BV1test000001", Enabled: true}},
	}
	client, err := NewRegistryClient(RegistryClientConfig{
		URL:                  srv.URL,
		TokenFile:            tokenPath,
		HeartbeatIntervalSec: 2,
		HeartbeatTTLSec:      10,
	}, "127.0.0.1:18092", exec, func() bool { return true })
	if err != nil {
		t.Fatalf("NewRegistryClient: %v", err)
	}

	ctx := context.Background()
	if err := client.Register(ctx); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := client.Heartbeat(ctx); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// Simulate dispatcher restart wiping memory registry -> 404 triggers auto re-register.
	mu.Lock()
	registered = false
	mu.Unlock()
	if err := client.Heartbeat(ctx); err != nil {
		t.Fatalf("Heartbeat self-heal failed: %v", err)
	}

	if err := client.Unregister(ctx); err != nil {
		t.Fatalf("Unregister: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if registerCount != 2 || heartbeatCount != 2 || unregisterCount != 1 {
		t.Fatalf("unexpected counts register=%d heartbeat=%d unregister=%d", registerCount, heartbeatCount, unregisterCount)
	}
}
