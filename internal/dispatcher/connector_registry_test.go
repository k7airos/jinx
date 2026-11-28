package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

type testConnExecutor struct {
	mu       sync.Mutex
	platform contracts.Platform
	ready    bool
	lastReq  *connector.ActionRequest
}

func (e *testConnExecutor) Platform() contracts.Platform { return e.platform }
func (e *testConnExecutor) AccountID() string            { return "acc-1" }
func (e *testConnExecutor) ConnectorName() string        { return string(e.platform) + "-test" }
func (e *testConnExecutor) Capabilities() []contracts.ActionKind {
	return []contracts.ActionKind{contracts.ActionReplyText, contracts.ActionThreadReply}
}
func (e *testConnExecutor) Health(context.Context) (bool, string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ready, ""
}
func (e *testConnExecutor) Status() connector.StatusSnapshot {
	return connector.StatusSnapshot{
		Targets: []connector.HealthTarget{{ContainerID: string(e.platform) + ":topic-1", Label: "Topic 1", Enabled: true}},
	}
}
func (e *testConnExecutor) Execute(_ context.Context, req connector.ActionRequest) (connector.ActionResponse, error) {
	e.mu.Lock()
	cp := req
	e.lastReq = &cp
	e.mu.Unlock()
	return connector.ActionResponse{ActionID: req.ActionID, Status: "confirmed", ExternalID: "ext-reply-1"}, nil
}

func startTestConnectorServer(t *testing.T, platform contracts.Platform, dryRun bool) (*httptest.Server, *testConnExecutor) {
	t.Helper()
	store, err := connector.OpenIdempotencyStore(filepath.Join(t.TempDir(), "idem.json"))
	if err != nil {
		t.Fatal(err)
	}
	exec := &testConnExecutor{platform: platform, ready: true}
	srv := connector.NewServer(connector.ServerOptions{Executor: exec, Store: store, DryRun: dryRun})
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv, exec
}

func TestConnectorRegistryTokenManagementAndFailClosed(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "registry.token")

	reg, err := newConnectorRegistry("", tokenFile, nil, nil)
	if err != nil {
		t.Fatalf("newConnectorRegistry auto-create token failed: %v", err)
	}
	info, err := os.Stat(tokenFile)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("expected 0600 token file, got mode=%v err=%v", info.Mode(), err)
	}
	if reg.token == "" {
		t.Fatal("expected generated non-empty token")
	}

	// Reject permissive mode.
	badTokenFile := filepath.Join(dir, "bad.token")
	if err := os.WriteFile(badTokenFile, []byte("abc\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := newConnectorRegistry("", badTokenFile, nil, nil); err == nil {
		t.Fatal("expected error for 0644 token file")
	}

	// No token file -> fail-closed 501.
	disabled, err := newConnectorRegistry("", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, status := disabled.Register(context.Background(), "anything", connector.RegisterRequest{
		ProtocolVersion: 1, Platform: "bilibili", URL: "http://127.0.0.1:18092",
	})
	if status != http.StatusNotImplemented {
		t.Fatalf("expected 501 registration_disabled, got %d", status)
	}
}

func TestConnectorRegistryLifecycleWatchdogAndDropIn(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "registry.token")
	dropInDir := filepath.Join(dir, "connectors.d")
	if err := os.MkdirAll(dropInDir, 0700); err != nil {
		t.Fatal(err)
	}

	zhihuSrv, _ := startTestConnectorServer(t, "zhihu", true)
	biliSrv, _ := startTestConnectorServer(t, "bilibili", true)
	otherZhihuSrv, _ := startTestConnectorServer(t, "zhihu", true)

	now := time.Date(2026, 11, 27, 14, 0, 0, 0, time.UTC)
	var clockMu sync.Mutex
	currentNow := func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		clockMu.Lock()
		now = now.Add(d)
		clockMu.Unlock()
	}

	reg, err := newConnectorRegistry(dropInDir, tokenFile, map[string]ActionEndpointConfig{
		"v2ex": {URL: "http://127.0.0.1:18094", DryRun: true, TimeoutSec: 30},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	reg.now = currentNow
	token := reg.token

	// Bad token rejected.
	_, st := reg.Register(context.Background(), "wrong", connector.RegisterRequest{
		ProtocolVersion: 1, Platform: "zhihu", URL: zhihuSrv.URL, HeartbeatTTLSec: 10,
	})
	if st != http.StatusUnauthorized {
		t.Fatalf("expected 401 on wrong token, got %d", st)
	}

	// Reverse /health mismatch rejected (request says bilibili, server reports zhihu).
	_, st = reg.Register(context.Background(), token, connector.RegisterRequest{
		ProtocolVersion: 1, Platform: "bilibili", URL: zhihuSrv.URL, HeartbeatTTLSec: 10,
	})
	if st != http.StatusBadGateway {
		t.Fatalf("expected 502 on platform mismatch, got %d", st)
	}

	// Register zhihu dynamically.
	resp, st := reg.Register(context.Background(), token, connector.RegisterRequest{
		ProtocolVersion: 1, Platform: "zhihu", URL: zhihuSrv.URL, InstanceID: "inst-1", HeartbeatTTLSec: 10,
	})
	if st != http.StatusOK || !resp.OK || resp.DiscoverySource != connector.DiscoverySourceDynamic {
		t.Fatalf("expected zhihu register ok, got %d %+v", st, resp)
	}

	// Active conflict rejected with 409.
	_, st = reg.Register(context.Background(), token, connector.RegisterRequest{
		ProtocolVersion: 1, Platform: "zhihu", URL: otherZhihuSrv.URL, InstanceID: "inst-2", HeartbeatTTLSec: 10,
	})
	if st != http.StatusConflict {
		t.Fatalf("expected 409 conflict while zhihu is alive, got %d", st)
	}

	// Conflict against static endpoint.
	_, st = reg.Register(context.Background(), token, connector.RegisterRequest{
		ProtocolVersion: 1, Platform: "v2ex", URL: "http://127.0.0.1:19999", HeartbeatTTLSec: 10,
	})
	if st != http.StatusConflict {
		t.Fatalf("expected 409 conflict against static v2ex, got %d", st)
	}

	// Heartbeat extends lease.
	advance(6 * time.Second)
	hb, st := reg.Heartbeat(token, connector.HeartbeatRequest{
		Platform: "zhihu", URL: zhihuSrv.URL, InstanceID: "inst-1", Ready: true, DryRun: true, HeartbeatTTLSec: 10,
	})
	if st != http.StatusOK || !hb.OK || hb.HeartbeatState != connector.HeartbeatAlive {
		t.Fatalf("expected alive heartbeat, got %d %+v", st, hb)
	}

	// Advance past TTL -> stale; past 2*TTL -> expired; past 11*TTL -> pruned.
	advance(12 * time.Second)
	reg.Tick(context.Background())
	_, _, entry, ok := reg.Resolve("zhihu:q-1")
	if !ok || entry.HeartbeatState != connector.HeartbeatStale {
		t.Fatalf("expected stale zhihu entry, got ok=%v entry=%+v", ok, entry)
	}

	advance(10 * time.Second)
	reg.Tick(context.Background())
	_, _, entry, ok = reg.Resolve("zhihu:q-1")
	if !ok || entry.HeartbeatState != connector.HeartbeatExpired {
		t.Fatalf("expected expired zhihu entry, got ok=%v entry=%+v", ok, entry)
	}

	advance(100 * time.Second)
	reg.Tick(context.Background())
	if _, _, _, ok := reg.Resolve("zhihu:q-1"); ok {
		t.Fatal("expected long-expired dynamic zhihu entry to be pruned")
	}

	// Drop-in add, reload, and removal.
	dropInFile := filepath.Join(dropInDir, "bilibili.json")
	manifest, _ := json.Marshal(connector.DropInManifest{
		SchemaVersion: 1,
		Platform:      "bilibili",
		URL:           biliSrv.URL,
		DryRun:        true,
		TimeoutSec:    25,
	})
	if err := os.WriteFile(dropInFile, manifest, 0600); err != nil {
		t.Fatal(err)
	}
	reg.Tick(context.Background())
	platform, ep, dropEntry, ok := reg.Resolve("bilibili:BV1test")
	if !ok || platform != "bilibili" || ep.URL != biliSrv.URL || dropEntry.DiscoverySource != connector.DiscoverySourceDropIn {
		t.Fatalf("expected drop-in bilibili resolution, got ok=%v ep=%+v entry=%+v", ok, ep, dropEntry)
	}

	// Unregister dynamic entry test.
	_, st = reg.Register(context.Background(), token, connector.RegisterRequest{
		ProtocolVersion: 1, Platform: "zhihu", URL: zhihuSrv.URL, InstanceID: "inst-3", HeartbeatTTLSec: 10,
	})
	if st != http.StatusOK {
		t.Fatalf("re-register zhihu failed: %d", st)
	}
	if _, st = reg.Unregister(token, connector.UnregisterRequest{Platform: "zhihu", URL: zhihuSrv.URL, InstanceID: "inst-3"}); st != http.StatusOK {
		t.Fatalf("unregister zhihu failed: %d", st)
	}
	if _, _, _, ok := reg.Resolve("zhihu:q-1"); ok {
		t.Fatal("expected zhihu removed after unregister")
	}

	// Remove drop-in file -> disappears on next Tick.
	_ = os.Remove(dropInFile)
	reg.Tick(context.Background())
	if _, _, _, ok := reg.Resolve("bilibili:BV1test"); ok {
		t.Fatal("expected drop-in bilibili removed after file deletion")
	}
}

func TestDispatcherDynamicConnectorRegistrationAndDirectDeliver(t *testing.T) {
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "registry.token")
	dropInDir := filepath.Join(dir, "connectors.d")
	if err := os.MkdirAll(dropInDir, 0700); err != nil {
		t.Fatal(err)
	}

	biliSrv, exec := startTestConnectorServer(t, contracts.PlatformBilibili, false)

	// Dispatcher config has NO bridge.url and NO static action_endpoints!
	cfg := map[string]any{
		"server": map[string]any{"listen_addr": "127.0.0.1:18088"},
		"relay":  map[string]any{"stream_url": "http://127.0.0.1:18080/api/v1/stream"},
		"hermes": map[string]any{
			"bin_path":       "./unused",
			"profile":        "demo",
			"profiles_dir":   "./profiles",
			"timeout_sec":    30,
			"max_concurrent": 1,
		},
		"rules": map[string]any{
			"batch_size":          1,
			"idle_timeout_sec":    1,
			"max_batch_wait_sec":  2,
			"max_message_age_sec": 60,
			"group_whitelist":     []string{"bilibili:BV1dyn0000001"},
			"group_rules": map[string]any{
				"bilibili:BV1dyn0000001": map[string]any{
					"enabled":               true,
					"target_group_id":       "bilibili:BV1dyn0000001",
					"target":                "Dynamic Video",
					"hermes_profile":        "bili-dyn",
					"dry_run":               false,
					"batch_size":            1,
					"idle_timeout_sec":      1,
					"max_batch_wait_sec":    2,
					"max_reply_runes":       200,
					"reply_window_sec":      60,
					"max_replies_in_window": 5,
					"decision_threshold":    0.8,
				},
			},
		},
	}
	cfgPath := filepath.Join(dir, "dispatcher.json")
	cfgBytes, _ := json.MarshalIndent(cfg, "", "  ")
	if err := os.WriteFile(cfgPath, cfgBytes, 0600); err != nil {
		t.Fatal(err)
	}

	d, err := NewDispatcherWithOptions(cfgPath, Options{
		ConnectorsDir:      dropInDir,
		ConnectorTokenFile: tokenFile,
	})
	if err != nil {
		t.Fatalf("NewDispatcherWithOptions failed without static endpoints: %v", err)
	}
	defer d.Close()

	httpSrv := httptest.NewServer(d.Handler())
	defer httpSrv.Close()

	// Register bilibili connector over HTTP using generated token.
	tokBytes, err := os.ReadFile(tokenFile)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.TrimSpace(string(tokBytes))
	regBody, _ := json.Marshal(connector.RegisterRequest{
		ProtocolVersion: 1,
		Platform:        "bilibili",
		URL:             biliSrv.URL,
		InstanceID:      "bili-inst-1",
		HeartbeatTTLSec: 30,
	})
	req, _ := http.NewRequest(http.MethodPost, httpSrv.URL+"/api/v1/connectors/register", bytes.NewReader(regBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(connector.ConnectorTokenHeader, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register HTTP status = %d", resp.StatusCode)
	}

	// Verify GET /api/v1/connectors lists bilibili.
	listResp, err := http.Get(httpSrv.URL + "/api/v1/connectors")
	if err != nil {
		t.Fatal(err)
	}
	var listed struct {
		Connectors []connector.RegistryEntry `json:"connectors"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listed); err != nil {
		t.Fatal(err)
	}
	listResp.Body.Close()
	if len(listed.Connectors) != 1 || listed.Connectors[0].Platform != "bilibili" || listed.Connectors[0].HeartbeatState != connector.HeartbeatAlive {
		t.Fatalf("unexpected GET /api/v1/connectors: %+v", listed.Connectors)
	}

	// DirectDeliver should now route cleanly to the dynamically registered connector!
	res, err := d.DirectDeliver(context.Background(), DirectDeliverRequest{
		TargetContainerID: "bilibili:BV1dyn0000001",
		Text:              "Dynamic discovery works!",
		IdempotencyKey:    "dyn-deliver-1",
		ContentID:         "rpid-1",
	})
	if err != nil {
		t.Fatalf("DirectDeliver failed: %v", err)
	}
	if res.Outcome != contracts.OutcomeReplied || res.Delivery != contracts.DeliveryConfirmed {
		t.Fatalf("expected confirmed direct delivery, got %+v", res)
	}
	exec.mu.Lock()
	defer exec.mu.Unlock()
	if exec.lastReq == nil || exec.lastReq.Text != "Dynamic discovery works!" {
		t.Fatalf("connector did not receive expected action request: %+v", exec.lastReq)
	}
}
