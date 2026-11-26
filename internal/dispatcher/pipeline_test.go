package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// All groups, members, products and profiles in this package's tests are synthetic.
const groupA = "100@chatroom"
const groupB = "200@chatroom"

func testConfig() Config {
	var cfg Config
	cfg.Server.ListenAddr = "127.0.0.1:8088"
	cfg.Relay.StreamURL = "http://127.0.0.1:8080/api/v1/stream"
	cfg.Bridge.URL = "http://127.0.0.1:5033"
	cfg.Hermes.BinPath = "hermes"
	cfg.Hermes.Profile = "demo-profile"
	cfg.Hermes.TimeoutSec = 10
	cfg.Rules = Rules{BatchSize: 1, IdleTimeoutSec: 1, MaxBatchWaitSec: 3, BotNames: []string{"DemoBot"}, GroupWhitelist: []string{groupA, groupB}, GroupProfiles: map[string]string{groupA: "background-a", groupB: "background-b"}, GroupRules: map[string]GroupRule{groupA: {Enabled: true, Target: "target-a"}, groupB: {Enabled: true, Target: "target-b"}}}
	return cfg
}

func newTestDispatcher(t *testing.T, cfg Config) *Dispatcher {
	t.Helper()
	if cfg.Hermes.ProfilesDir == "" {
		cfg.Hermes.ProfilesDir = t.TempDir()
	}
	data, _ := json.Marshal(cfg)
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	d, err := NewDispatcherWithOptions(path, Options{LocalPolicyPath: writeTestPolicy(t, cfg.localPolicy)})
	if err != nil {
		t.Fatal(err)
	}
	d.sendCooldown = time.Millisecond
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		return HermesResult{}, fmt.Errorf("no backend fixture configured")
	}
	t.Cleanup(d.Close)
	return d
}

func testMessage(group, id string) RelayMessage {
	return RelayMessage{ID: id, GroupID: group, GroupName: "untrusted-name", SenderID: "human", SenderName: "human", Type: 1, Action: "forward", Content: "What changed?", Timestamp: time.Now().UnixMilli()}
}

func awaitCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func idle(d *Dispatcher) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, g := range d.state.Groups {
		if g.Executing || len(g.Pending) > 0 {
			return false
		}
	}
	return true
}

func bridgeFixture(t *testing.T, send func(http.ResponseWriter, *http.Request)) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			fmt.Fprint(w, `{"protocol_version":2,"group_routing":true,"serial_ui":true,"accessibility_trusted":true,"screen_capture_authorized":true,"dry_run":false}`)
			return
		}
		send(w, r)
	}))
	t.Cleanup(s.Close)
	return s
}

func TestMultipleGroupsInferConcurrentlyButDeliverSerially(t *testing.T) {
	var d *Dispatcher
	var active, maximum atomic.Int32
	var mu sync.Mutex
	targets := map[string]string{}
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
		defer active.Add(-1)
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		targets[body["group_id"].(string)] = body["target"].(string)
		mu.Unlock()
		echo := testMessage(body["group_id"].(string), "echo-"+body["request_id"].(string))
		echo.IsEcho = true
		echo.Content = body["text"].(string)
		if err := d.handleIncomingMessage(echo, ""); err != nil {
			t.Error(err)
		}
		time.Sleep(20 * time.Millisecond)
		fmt.Fprint(w, "OK")
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	d = newTestDispatcher(t, cfg)
	entered := make(chan string, 2)
	release := make(chan struct{})
	d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
		entered <- b.GroupID
		if strings.Contains(buildPrompt(b), "background-"+map[string]string{groupA: "b", groupB: "a"}[b.GroupID]) {
			t.Error("cross-group prompt leak")
		}
		select {
		case <-release:
		case <-ctx.Done():
			return HermesResult{}, ctx.Err()
		}
		return HermesResult{Text: "reply", SessionID: b.GroupID}, nil
	}
	for _, id := range []string{groupA, groupB} {
		if err := d.handleIncomingMessage(testMessage(id, "1"), "event-"+id); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("groups did not infer concurrently")
		}
	}
	close(release)
	awaitCondition(t, func() bool { return idle(d) })
	if maximum.Load() != 1 {
		t.Fatalf("concurrent GUI calls: %d", maximum.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if targets[groupA] != "target-a" || targets[groupB] != "target-b" {
		t.Fatalf("wrong routes: %v", targets)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state.Stats.TotalReplied != 2 {
		t.Fatalf("success count: %+v", d.state.Stats)
	}
}

func TestSameGroupQueuesArrivalsAndUsesFrozenContext(t *testing.T) {
	cfg := testConfig()
	cfg.Bridge.DryRun = true
	d := newTestDispatcher(t, cfg)
	first := make(chan batch, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
		if calls.Add(1) == 1 {
			first <- b
			select {
			case <-release:
			case <-ctx.Done():
				return HermesResult{}, ctx.Err()
			}
		}
		return HermesResult{Text: "reply", SessionID: "session"}, nil
	}
	if err := d.handleIncomingMessage(testMessage(groupA, "one"), ""); err != nil {
		t.Fatal(err)
	}
	b := <-first
	if err := d.handleIncomingMessage(testMessage(groupA, "two"), ""); err != nil {
		t.Fatal(err)
	}
	if len(b.Messages) != 1 || len(b.History) != 1 {
		t.Fatal("batch context mutated after snapshot")
	}
	if calls.Load() != 1 {
		t.Fatal("same group ran concurrently")
	}
	close(release)
	awaitCondition(t, func() bool { return idle(d) })
	if calls.Load() != 2 {
		t.Fatalf("queued arrival not drained: %d", calls.Load())
	}
}

func TestDedupEchoAndUnknownGroupsCannotTrigger(t *testing.T) {
	cfg := testConfig()
	cfg.Bridge.DryRun = true
	d := newTestDispatcher(t, cfg)
	var calls atomic.Int32
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		calls.Add(1)
		return HermesResult{SessionID: "s"}, nil
	}
	msg := testMessage(groupA, "one")
	for i := 0; i < 3; i++ {
		if err := d.handleIncomingMessage(msg, ""); err != nil {
			t.Fatal(err)
		}
	}
	// Finish the human batch before the echo: an echo that overtakes it is
	// intentionally suppressed and is covered separately below.
	awaitCondition(t, func() bool { return idle(d) })
	msg.ID = "echo"
	msg.IsEcho = true
	_ = d.handleIncomingMessage(msg, "")
	msg = testMessage("300@chatroom", "unknown")
	_ = d.handleIncomingMessage(msg, "")
	msg = testMessage(groupA, "blocked")
	msg.Action = "blocked_type"
	_ = d.handleIncomingMessage(msg, "")
	awaitCondition(t, func() bool { return idle(d) })
	if calls.Load() != 1 {
		t.Fatalf("unexpected calls %d", calls.Load())
	}
}

func TestOwnReplyOvertakesPendingBatch(t *testing.T) {
	cfg := testConfig()
	cfg.Bridge.DryRun = true
	cfg.Rules.BatchSize = 5
	d := newTestDispatcher(t, cfg)
	var calls atomic.Int32
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		calls.Add(1)
		return HermesResult{Text: "reply", SessionID: "s"}, nil
	}
	if err := d.handleIncomingMessage(testMessage(groupA, "human"), ""); err != nil {
		t.Fatal(err)
	}
	echo := testMessage(groupA, "echo")
	echo.IsEcho = true
	if err := d.handleIncomingMessage(echo, ""); err != nil {
		t.Fatal(err)
	}
	awaitCondition(t, func() bool { return idle(d) })
	if calls.Load() != 0 {
		t.Fatal("replied to a batch overtaken by our own reply")
	}
}

func TestOwnReplyDuringInferenceRejectsOlderReply(t *testing.T) {
	var submissions atomic.Int32
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		submissions.Add(1)
		http.Error(w, "unexpected stale submission", 409)
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	d := newTestDispatcher(t, cfg)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
		if calls.Add(1) != 1 {
			return HermesResult{Text: "NONE", SessionID: "s"}, nil
		}
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return HermesResult{}, ctx.Err()
		}
		return HermesResult{Text: "old reply", SessionID: "s"}, nil
	}
	_ = d.handleIncomingMessage(testMessage(groupA, "old-question"), "")
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("inference did not start")
	}
	echo := testMessage(groupA, "manual-reply")
	echo.IsEcho = true
	_ = d.handleIncomingMessage(echo, "")
	// Later human messages must not erase the fact that the old question was answered.
	for i := 0; i < 26; i++ {
		_ = d.handleIncomingMessage(testMessage(groupA, fmt.Sprintf("later-%d", i)), "")
	}
	close(release)
	awaitCondition(t, func() bool { return idle(d) })
	if submissions.Load() != 0 {
		t.Fatal("submitted a stale reply after our own response")
	}
}

func TestShutdownPreservesInferenceAndWorkerQueue(t *testing.T) {
	cfg := testConfig()
	cfg.Hermes.MaxConcurrent = 1
	d := newTestDispatcher(t, cfg)
	entered := make(chan struct{}, 1)
	d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return HermesResult{}, ctx.Err()
	}
	for _, id := range []string{groupA, groupB} {
		if err := d.handleIncomingMessage(testMessage(id, "pending"), ""); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("inference did not start")
	}
	d.Close()
	recovered, err := NewDispatcher(d.cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	for _, id := range []string{groupA, groupB} {
		if len(recovered.state.Groups[id].Pending) != 1 {
			t.Fatalf("shutdown lost unsent work for %s", id)
		}
	}
}

func TestPendingMessagesFollowUpdatedFilters(t *testing.T) {
	cfg := testConfig()
	cfg.Rules.BatchSize = 5
	cfg.Bridge.DryRun = true
	d := newTestDispatcher(t, cfg)
	var calls atomic.Int32
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		calls.Add(1)
		return HermesResult{SessionID: "s"}, nil
	}
	if err := d.handleIncomingMessage(testMessage(groupA, "old-rule"), ""); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(`{"keyword_blocklist":["What changed?"]}`))
	r.Host = "localhost:8088"
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("If-Match", policyID(d.configSnapshot()))
	w := httptest.NewRecorder()
	d.Handler().ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	awaitCondition(t, func() bool { return idle(d) })
	if calls.Load() != 0 {
		t.Fatal("queued message bypassed updated filter")
	}
}

func TestOnlyNewMatchingEchoConfirmsDelivery(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	echo := testMessage(groupA, "already-seen")
	echo.IsEcho = true
	echo.Content = "reply"
	if err := d.handleIncomingMessage(echo, ""); err != nil {
		t.Fatal(err)
	}
	confirmation := prepareConfirmation(t, d, "new-matching-echo", "reply", time.Now())
	invalid := []RelayMessage{echo}
	wrongGroup := echo
	wrongGroup.GroupID, wrongGroup.ID = groupB, "wrong-group"
	invalid = append(invalid, wrongGroup)
	old := echo
	old.ID, old.Timestamp = "old", time.Now().Add(-time.Minute).UnixMilli()
	invalid = append(invalid, old)
	notForwarded := echo
	notForwarded.ID, notForwarded.Action = "blocked", "blocked_type"
	invalid = append(invalid, notForwarded)
	for _, msg := range invalid {
		if err := d.handleIncomingMessage(msg, ""); err != nil {
			t.Fatal(err)
		}
		if confirmation.Echo != nil {
			t.Fatalf("invalid echo confirmed delivery: %s", msg.ID)
		}
	}
	echo.ID = "new-echo"
	echo.Timestamp = time.Now().UnixMilli()
	if err := d.handleIncomingMessage(echo, ""); err != nil {
		t.Fatal(err)
	}
	if confirmation.Echo == nil {
		t.Fatal("new matching echo did not confirm delivery")
	}
}

func TestHermesRejectsSharedMemoryCollection(t *testing.T) {
	cfg := testConfig()
	cfg.Hermes.ProfilesDir = t.TempDir()
	if err := cfg.normalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	rule, _ := cfg.route(groupA)
	dir := filepath.Join(cfg.Hermes.ProfilesDir, rule.HermesProfile)
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	data := []byte(`{"user_id":"group:100@chatroom","mode":"oss","oss":{"vector_store":{"provider":"qdrant","config":{"collection_name":"mem0"}}}}`)
	if err := os.WriteFile(filepath.Join(dir, "mem0.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := runHermes(context.Background(), batch{GroupID: groupA, Rule: rule, Config: cfg})
	if err == nil || !strings.Contains(err.Error(), "memory identity") {
		t.Fatalf("shared memory accepted: %v", err)
	}
}

func TestGateFailuresAndRepeatedMentionsNeverReachHermes(t *testing.T) {
	for _, mode := range []string{"http_error", "invalid_json", "missing_fields", "sensitive", "low_confidence"} {
		t.Run(mode, func(t *testing.T) {
			gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "http_error":
					w.WriteHeader(503)
				case "invalid_json":
					fmt.Fprint(w, "bad")
				case "missing_fields":
					fmt.Fprint(w, `{}`)
				case "sensitive":
					fmt.Fprint(w, `{"should_reply":false,"probability":0.1,"intent":"sensitive","decision":"block"}`)
				case "low_confidence":
					fmt.Fprint(w, `{"should_reply":true,"probability":0.4,"intent":"safe","decision":"allow"}`)
				}
			}))
			defer gate.Close()
			cfg := testConfig()
			cfg.Rules.LayaURL = gate.URL
			cfg.Rules.LayaFilter = true
			d := newTestDispatcher(t, cfg)
			var called atomic.Bool
			d.hermesRunner = func(context.Context, batch) (HermesResult, error) { called.Store(true); return HermesResult{}, nil }
			msg := testMessage(groupA, "one")
			msg.Content = "@DemoBot @DemoBot hello"
			_ = d.handleIncomingMessage(msg, "")
			awaitCondition(t, func() bool { return idle(d) })
			if called.Load() {
				t.Fatal("gate bypassed")
			}
		})
	}
}

func TestSendFailureDoesNotCountAsReply(t *testing.T) {
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		fmt.Fprint(w, "FAIL: target missing")
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	d := newTestDispatcher(t, cfg)
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		return HermesResult{Text: "reply", SessionID: "s"}, nil
	}
	_ = d.handleIncomingMessage(testMessage(groupA, "one"), "")
	awaitCondition(t, func() bool { return idle(d) })
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state.Stats.TotalReplied != 0 || len(d.state.Groups[groupA].ReplyTimes) != 0 || d.state.Stats.TotalSendFailed != 1 {
		t.Fatalf("bad success accounting: %+v", d.state.Stats)
	}
}

func TestConfigChangeWhileThinkingPreventsStaleSend(t *testing.T) {
	var sent atomic.Bool
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) { sent.Store(true); fmt.Fprint(w, "OK") })
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	d := newTestDispatcher(t, cfg)
	entered := make(chan struct{})
	release := make(chan struct{})
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		close(entered)
		<-release
		return HermesResult{Text: "reply", SessionID: "s"}, nil
	}
	_ = d.handleIncomingMessage(testMessage(groupA, "one"), "")
	<-entered
	d.cfgMu.Lock()
	d.cfg.Bridge.DryRun = true
	d.cfgMu.Unlock()
	close(release)
	awaitCondition(t, func() bool { return idle(d) })
	if sent.Load() {
		t.Fatal("stale reply sent after dry run was enabled")
	}
}

func TestStateRecoveryRequeuesUnsentButNotUncertainDelivery(t *testing.T) {
	cfg := testConfig()
	d := newTestDispatcher(t, cfg)
	d.mu.Lock()
	d.groupLocked(groupA).InFlight = []RelayMessage{testMessage(groupA, "pending")}
	d.groupLocked(groupB).InFlight = []RelayMessage{testMessage(groupB, "uncertain")}
	d.groupLocked(groupB).SendStarted = true
	if err := d.saveStateLocked(); err != nil {
		t.Fatal(err)
	}
	d.mu.Unlock()
	d.Close()
	recovered, err := NewDispatcher(d.cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	if len(recovered.state.Groups[groupA].Pending) != 1 || len(recovered.state.Groups[groupB].Pending) != 0 || recovered.state.Stats.TotalUnknown != 1 {
		t.Fatal("incorrect crash recovery")
	}
}

func TestRulesAndParserContracts(t *testing.T) {
	if mentioned("@DemoBotOther hello", []string{"DemoBot"}) || !mentioned("@DemoBot\u2005hello", []string{"DemoBot"}) {
		t.Fatal("mention boundaries")
	}
	// WeChat @ payloads are case-insensitive in practice.
	if !mentioned("@demobot 在干嘛呢", []string{"DemoBot"}) {
		t.Fatal("case-insensitive mention not detected")
	}
	if !mentioned("[引用消息] @demobot", []string{"DemoBot"}) {
		t.Fatal("quoted mention not detected")
	}
	if !mentioned("@OtherBot9000\u2005介绍一下", []string{"OtherBot9000"}) {
		t.Fatal("numeric-suffix nickname mention not detected")
	}
	if mentioned("@OtherBot9000x", []string{"OtherBot9000"}) {
		t.Fatal("matched nickname with trailing letters")
	}
	for _, output := range []string{"diagnostic text", `{"type":"result","text":"oops","session_id":"s"}`, `{"type":"result","exit_code":1,"session_id":"s"}`} {
		if _, err := parseHermesOutput([]byte(output)); err == nil {
			t.Fatal("accepted invalid Hermes output")
		}
	}
	result, err := parseHermesOutput([]byte("{\"type\":\"text\",\"text\":\"ignore delta\"}\n{\"type\":\"result\",\"exit_code\":0,\"text\":\"final\",\"session_id\":\"s\"}\n"))
	if err != nil || result.Text != "final" || result.SessionID != "s" {
		t.Fatalf("bad Hermes result %+v %v", result, err)
	}
	var events []string
	err = readSSE(strings.NewReader("event: connected\ndata: {}\n\nevent: message\nid: a:1\ndata: {\ndata: \"id\":\"one\"}\n\nevent: message\ndata: incomplete"), func(event, id, data string) error { events = append(events, event+"/"+id+"/"+data); return nil })
	if err != io.EOF || len(events) != 2 || !strings.Contains(events[1], "message/a:1/{\n") {
		t.Fatalf("bad SSE frames %v %v", events, err)
	}
	cfg := testConfig()
	cfg.Rules.GroupWhitelist = nil
	if err := cfg.normalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.route(groupA); ok {
		t.Fatal("empty whitelist allowed a group")
	}
}

func TestConfigPatchPreservesOmittedFieldsAndValidatesBeforeSaving(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	request := func(body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(body))
		r.Host = "localhost:8088"
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("If-Match", policyID(d.configSnapshot()))
		w := httptest.NewRecorder()
		d.Handler().ServeHTTP(w, r)
		return w
	}
	if w := request(`{"dry_run":true}`); w.Code != 200 {
		t.Fatalf("patch %d %s", w.Code, w.Body.String())
	}
	if len(d.cfg.Rules.GroupWhitelist) != 2 || !d.cfg.Bridge.DryRun {
		t.Fatal("patch reset unrelated settings")
	}
	if w := request(`{"batch_size":7}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if rule, _ := d.configSnapshot().route(groupA); rule.BatchSize != 7 {
		t.Fatal("group lost inherited global defaults")
	}
	before, _ := os.ReadFile(d.cfgFile)
	if w := request(`{"batch_size":0}`); w.Code != 400 {
		t.Fatalf("invalid batch accepted: %d", w.Code)
	}
	after, _ := os.ReadFile(d.cfgFile)
	if string(before) != string(after) {
		t.Fatal("invalid config persisted")
	}
}

func TestBridgeReadinessPreventsGUISubmission(t *testing.T) {
	for _, permissions := range []struct{ accessibility, capture, dryRun bool }{{false, true, false}, {true, false, false}, {true, true, true}} {
		t.Run(fmt.Sprint(permissions), func(t *testing.T) {
			var submissions atomic.Int32
			bridge := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					fmt.Fprintf(w, `{"protocol_version":2,"group_routing":true,"serial_ui":true,"accessibility_trusted":%t,"screen_capture_authorized":%t,"dry_run":%t}`, permissions.accessibility, permissions.capture, permissions.dryRun)
					return
				}
				submissions.Add(1)
			}))
			defer bridge.Close()
			cfg := testConfig()
			cfg.Bridge.URL = bridge.URL
			d := newTestDispatcher(t, cfg)
			d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
				return HermesResult{Text: "reply", SessionID: "s"}, nil
			}
			_ = d.handleIncomingMessage(testMessage(groupA, "question"), "")
			awaitCondition(t, func() bool { return idle(d) })
			d.mu.Lock()
			defer d.mu.Unlock()
			if submissions.Load() != 0 || d.state.Stats.TotalUnknown != 0 || d.state.Stats.TotalReplied != 0 {
				t.Fatal("unavailable bridge was treated as a submitted reply")
			}
		})
	}
}

func TestGUIAcceptanceWithoutEchoRemainsUnknown(t *testing.T) {
	var requests atomic.Int32
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1); fmt.Fprint(w, "OK") })
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	d := newTestDispatcher(t, cfg)
	d.echoTimeout = 20 * time.Millisecond
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		return HermesResult{Text: "reply", SessionID: "s"}, nil
	}
	_ = d.handleIncomingMessage(testMessage(groupA, "one"), "")
	awaitCondition(t, func() bool { return idle(d) })
	d.mu.Lock()
	defer d.mu.Unlock()
	if requests.Load() != 1 || d.state.Stats.TotalReplied != 0 || d.state.Stats.TotalSubmitted != 1 || d.state.Stats.TotalUnknown != 1 {
		t.Fatalf("unknown delivery misreported/retried: %+v", d.state.Stats)
	}
}

func TestDistinctGroupFiltersAndHardRateLimit(t *testing.T) {
	cfg := testConfig()
	cfg.Bridge.DryRun = true
	a := cfg.Rules.GroupRules[groupA]
	a.KeywordBlocklist = []string{"blocked"}
	cfg.Rules.GroupRules[groupA] = a
	b := cfg.Rules.GroupRules[groupB]
	b.MentionOnly = true
	cfg.Rules.GroupRules[groupB] = b
	d := newTestDispatcher(t, cfg)
	var calls atomic.Int32
	d.hermesRunner = func(context.Context, batch) (HermesResult, error) {
		calls.Add(1)
		return HermesResult{SessionID: "s"}, nil
	}
	msg := testMessage(groupA, "a")
	msg.Content = "blocked"
	_ = d.handleIncomingMessage(msg, "")
	msg = testMessage(groupB, "b")
	_ = d.handleIncomingMessage(msg, "")
	awaitCondition(t, func() bool { return idle(d) })
	if calls.Load() != 0 {
		t.Fatal("per-group filters not applied")
	}
	// A direct @ is an explicit summon and must not be swallowed by the reply-rate cap.
	d.mu.Lock()
	d.groupLocked(groupA).ReplyTimes = []time.Time{time.Now(), time.Now()}
	d.mu.Unlock()
	msg = testMessage(groupA, "c")
	msg.Content = "@DemoBot help"
	_ = d.handleIncomingMessage(msg, "")
	awaitCondition(t, func() bool { return idle(d) })
	if calls.Load() != 1 {
		t.Fatal("mention did not bypass the reply-rate cap")
	}
	msg = testMessage(groupB, "d")
	msg.Content = "@DemoBot help"
	_ = d.handleIncomingMessage(msg, "")
	awaitCondition(t, func() bool { return idle(d) })
	if calls.Load() != 2 {
		t.Fatal("other group incorrectly suppressed")
	}
}

func TestRelayGateHermesBridgeAndEchoEndToEnd(t *testing.T) {
	stream := make(chan RelayMessage, 16)
	relay := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: connected\nid: test:0\ndata: {}\n\n")
		w.(http.Flusher).Flush()
		seq := 0
		for {
			select {
			case msg := <-stream:
				seq++
				data, _ := json.Marshal(msg)
				fmt.Fprintf(w, "event: message\nid: test:%d\ndata: %s\n\n", seq, data)
				w.(http.Flusher).Flush()
			case <-r.Context().Done():
				return
			}
		}
	}))
	defer relay.Close()
	var gates atomic.Int32
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gates.Add(1)
		fmt.Fprint(w, `{"should_reply":true,"probability":0.9,"intent":"safe","decision":"allow"}`)
	}))
	defer gate.Close()
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		echo := testMessage(body["group_id"].(string), "echo-"+body["request_id"].(string))
		echo.Content = body["text"].(string)
		echo.IsEcho = true
		stream <- echo
		fmt.Fprint(w, "OK")
	})
	cfg := testConfig()
	cfg.Relay.StreamURL = relay.URL
	cfg.Bridge.URL = bridge.URL
	cfg.Rules.LayaFilter = true
	cfg.Rules.LayaURL = gate.URL
	d := newTestDispatcher(t, cfg)
	defer d.Close() // Close SSE before httptest servers shut down.
	d.hermesRunner = func(_ context.Context, b batch) (HermesResult, error) {
		return HermesResult{Text: "reply", SessionID: b.GroupID}, nil
	}
	d.Start()
	for _, id := range []string{groupA, groupB} {
		stream <- testMessage(id, "input")
	}
	awaitCondition(t, func() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.state.Stats.TotalReplied == 2 })
	if gates.Load() != 2 {
		t.Fatalf("gate calls %d", gates.Load())
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state.LastEventID != "test:4" {
		t.Fatalf("durable SSE cursor %q", d.state.LastEventID)
	}
}

// Opt in explicitly: this calls the configured model, but never invokes the bridge or
// the production dispatcher state. Each group must return its own structured result.
func TestConfiguredHermesRuntime(t *testing.T) {
	path := os.Getenv("PIPELINE_LIVE_CONFIG")
	if path == "" {
		t.Skip("set PIPELINE_LIVE_CONFIG to verify the configured Hermes runtime")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := loadLocalPolicy(os.Getenv("JINX_LOCAL_POLICY"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := decodeConfigWithPolicy(data, policy)
	if err != nil {
		t.Fatal(err)
	}
	for id := range cfg.Rules.GroupRules {
		id := id
		rule, enabled := cfg.route(id)
		if !enabled {
			continue
		}
		t.Run(id, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.Hermes.TimeoutSec)*time.Second)
			defer cancel()
			msg := testMessage(id, "runtime-smoke-test")
			msg.Content = "谢谢，收到。"
			result, err := runHermes(ctx, batch{GroupID: id, Rule: rule, Config: cfg, Messages: []RelayMessage{msg}})
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(result.Text) != "NONE" || result.SessionID == "" {
				t.Fatalf("closing remark should remain silent: %+v", result)
			}
		})
	}
}

func TestSessionRotationAndModelFingerprint(t *testing.T) {
	cfg := testConfig()
	now := time.Now()
	g := &GroupState{SessionID: "s1"}
	if sessionExpired(g, cfg, now) {
		t.Fatal("fresh session must not expire")
	}
	cfg.Rules.SessionMaxTurns = 2
	g.SessionTurns = 2
	if !sessionExpired(g, cfg, now) {
		t.Fatal("session must expire after max turns")
	}
	cfg.Rules.SessionMaxTurns = 0
	cfg.Rules.SessionMaxAgeSec = 60
	g.SessionTurns = 0
	g.SessionStartedAt = now.Add(-2 * time.Minute)
	if !sessionExpired(g, cfg, now) {
		t.Fatal("session must expire after max age")
	}
	g.SessionStartedAt = time.Time{}
	if sessionExpired(g, cfg, now) {
		t.Fatal("session without a start time must not expire by age")
	}

	// The model fingerprint must read the profile's model block, not the provider list.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("model:\n  default: group/cheap-first\n  provider: magpie\nproviders:\n  a2-local:\n    default: other-model\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := modelFingerprint(dir); got != "group/cheap-first" {
		t.Fatalf("unexpected model fingerprint %q", got)
	}
	if got := modelProvider(dir); got != "magpie" {
		t.Fatalf("unexpected provider %q", got)
	}
}
