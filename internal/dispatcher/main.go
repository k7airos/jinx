package dispatcher

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"jinx/internal/observability"
)

//go:embed web/*.html
var legacyWeb embed.FS

type Stats struct {
	RelayConnected  bool  `json:"relay_connected"`
	TotalReceived   int64 `json:"total_received"`
	TotalTriggered  int64 `json:"total_triggered"`
	TotalReplied    int64 `json:"total_replied"`
	TotalSubmitted  int64 `json:"total_submitted"`
	TotalDryRun     int64 `json:"total_dry_run"`
	TotalSendFailed int64 `json:"total_send_failed"`
	TotalUnknown    int64 `json:"total_unknown"`
	ReplayGaps      int64 `json:"replay_gaps"`
	PendingMessages int   `json:"pending_messages"`
}

type LogItem struct {
	Time    string `json:"time"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

type persistentState struct {
	Version               int                              `json:"version"`
	LastEventID           string                           `json:"last_event_id"`
	Sequence              uint64                           `json:"sequence"`
	IncomingSequence      uint64                           `json:"incoming_sequence,omitempty"`
	Seen                  map[string]time.Time             `json:"seen"`
	Groups                map[string]*GroupState           `json:"groups"`
	Stats                 Stats                            `json:"stats"`
	SourceWatermarks      map[string]sourceWatermark       `json:"source_watermarks,omitempty"`
	AwaitingConfirmations map[string]*awaitingConfirmation `json:"awaiting_confirmations,omitempty"`
	Control               ControlState                     `json:"control"`
	ConfigSnapshots       map[string]ConfigSnapshot        `json:"config_snapshots,omitempty"`
}

type Dispatcher struct {
	trace          traceStore
	traceMu        sync.Mutex
	traceOps       uint64
	traceFailed    atomic.Bool
	tracedMessages map[string]string
	closeOnce      sync.Once
	reminderRunner func(reminderRequest) (reminderResponse, error)
	cfgFile        string
	stateFile      string
	cfgMu          sync.RWMutex
	controlMu      sync.Mutex
	submissionMu   sync.Mutex
	cfg            Config
	connectors     *connectorRegistry
	mu             sync.Mutex
	state          persistentState
	closed         bool
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	client         *http.Client
	hermesSlots    chan struct{}
	runner         Runner
	hermesRunner   func(context.Context, batch) (HermesResult, error)
	runContexts    map[string]activeRun
	sendMu         sync.Mutex
	sendCooldown   time.Duration
	echoTimeout    time.Duration
	awaitingEcho   *echoWaiter
	intakeSequence atomic.Uint64
	logMu          sync.Mutex
	logClients     map[chan LogItem]bool
	logHistory     []LogItem
	logDate        string
	auditMu        sync.Mutex
	auditDate      string
	history        *historyStore
	startedAt      time.Time
	lastProgressAt time.Time
	directMu       sync.Mutex
	directInFlight map[string]bool
	directDigests  map[string]string
}

func NewDispatcher(cfgPath string) (*Dispatcher, error) {
	return NewDispatcherWithOptions(cfgPath, Options{})
}

func NewDispatcherWithOptions(cfgPath string, options Options) (*Dispatcher, error) {
	policyPath := options.LocalPolicyPath
	if policyPath == "" {
		policyPath = os.Getenv("JINX_LOCAL_POLICY")
	}
	policy, err := loadLocalPolicy(policyPath)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, err
	}
	allowDynamic := strings.TrimSpace(options.ConnectorsDir) != "" || strings.TrimSpace(options.ConnectorTokenFile) != ""
	cfg, err := decodeConfigWithOptions(data, policy, allowDynamic)
	if err != nil {
		return nil, err
	}
	reg, err := newConnectorRegistry(options.ConnectorsDir, options.ConnectorTokenFile, cfg.ActionEndpoints, nil)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dispatcher{
		cfgFile: cfgPath, stateFile: filepath.Join(filepath.Dir(cfgPath), "logs", "state.json"),
		cfg: cfg, connectors: reg, ctx: ctx, cancel: cancel, startedAt: time.Now(),
		state:       persistentState{Version: 1, Seen: map[string]time.Time{}, Groups: map[string]*GroupState{}},
		client:      &http.Client{Transport: &http.Transport{ResponseHeaderTimeout: 10 * time.Second}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		hermesSlots: make(chan struct{}, cfg.Hermes.MaxConcurrent), hermesRunner: runHermes, runner: options.Runner,
		sendCooldown: 500 * time.Millisecond, echoTimeout: 10 * time.Second, logClients: map[chan LogItem]bool{},
		runContexts:    make(map[string]activeRun),
		directInFlight: make(map[string]bool),
		directDigests:  make(map[string]string),
	}
	if options.Runner != nil {
		d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
			return d.runner.Run(ctx, d.backendRequest(b))
		}
	}
	d.history = newHistoryStore(filepath.Join(filepath.Dir(d.stateFile), "history"))
	store, err := observability.Open(filepath.Join(filepath.Dir(d.stateFile), "trace.json"))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open dispatcher trace: %w", err)
	}
	d.trace = store
	ready := false
	defer func() {
		if !ready {
			cancel()
			_ = store.Close()
		}
	}()
	snapshot, err := store.Snapshot()
	if err != nil {
		return nil, err
	}
	d.tracedMessages = make(map[string]string)
	for _, run := range snapshot.Runs {
		for _, messageID := range run.MessageIDs {
			d.tracedMessages[run.SourceGroupID+"/"+messageID] = run.ID
		}
		if run.DeliveryEvidence.EchoMessageID != "" {
			d.tracedMessages[run.TargetGroupID+"/"+run.DeliveryEvidence.EchoMessageID] = run.ID
		}
	}
	data, err = os.ReadFile(d.stateFile)
	if err == nil {
		if err = json.Unmarshal(data, &d.state); err != nil || d.state.Version != 1 || d.state.Seen == nil || d.state.Groups == nil {
			cancel()
			return nil, fmt.Errorf("invalid dispatcher state; recover it before restarting")
		}
	} else if !os.IsNotExist(err) {
		cancel()
		return nil, err
	}
	d.state.Stats.RelayConnected = false
	d.intakeSequence.Store(d.state.IncomingSequence)
	for id, g := range d.state.Groups {
		if g == nil {
			cancel()
			return nil, fmt.Errorf("invalid state for group %s", id)
		}
		for _, msg := range g.History {
			if msg.IsEcho && msg.Sequence > g.LastOwnReplySequence {
				g.LastOwnReplySequence = msg.Sequence
			}
		}
		if g.SendStarted && !d.hasConfirmationForGroup(id) {
			d.state.Stats.TotalUnknown++
			d.Log("ERROR", "group=%s interrupted delivery outcome unknown; no automatic resend", id)
		} else if !g.SendStarted {
			g.Pending = append(g.InFlight, g.Pending...)
		}
		g.InFlight = nil
		g.SendStarted = false
		for index := range g.Pending {
			msg := &g.Pending[index]
			if traceID := d.tracedMessages[id+"/"+msg.ID]; traceID != "" {
				msg.TraceID = traceID
			} else if msg.TraceID == "" {
				msg.LegacyQueued = true
			}
		}
	}
	if err = d.recoverConfirmationsLocked(); err != nil {
		return nil, err
	}
	if err = d.recoverControlLocked(); err != nil {
		return nil, err
	}
	if err = d.saveStateLocked(); err != nil {
		cancel()
		return nil, err
	}
	ready = true
	if d.connectors != nil {
		d.connectors.logf = func(format string, args ...any) {
			d.Log("INFO", format, args...)
		}
	}
	return d, nil
}

func (d *Dispatcher) saveStateLocked() error {
	data, err := json.Marshal(d.state)
	if err != nil {
		return err
	}
	return atomicWrite(d.stateFile, data)
}

func (d *Dispatcher) Start() {
	cfg := d.configSnapshot()
	d.mu.Lock()
	if d.closed || d.ctx.Err() != nil {
		d.mu.Unlock()
		return
	}
	for id, g := range d.state.Groups {
		if rule, ok := cfg.route(id); ok {
			d.scheduleLocked(id, g, rule, cfg.Rules.BotNames)
		}
	}
	d.wg.Add(1)
	d.mu.Unlock()
	go func() { defer d.wg.Done(); d.listenRelay() }()
	d.startConnectorWatchdog()
}

func (d *Dispatcher) Close() {
	d.closeOnce.Do(d.close)
}

func (d *Dispatcher) close() {
	d.mu.Lock()
	d.closed = true
	for _, g := range d.state.Groups {
		if g.Timer != nil {
			g.Timer.Stop()
		}
	}
	d.cancel()
	d.mu.Unlock()
	d.wg.Wait()
	d.mu.Lock()
	_ = d.saveStateLocked()
	d.mu.Unlock()
	d.client.CloseIdleConnections()
	if d.trace != nil {
		_ = d.trace.Close()
	}
}

func messageDigest(text string) string {
	hash := sha256.Sum256([]byte(text))
	return hex.EncodeToString(hash[:])
}

func (d *Dispatcher) Log(level, format string, args ...interface{}) {
	item := LogItem{Time: time.Now().Format("15:04:05"), Level: level, Message: fmt.Sprintf(format, args...)}
	log.Printf("[%s] %s", level, item.Message)
	d.logMu.Lock()
	defer d.logMu.Unlock()
	today := time.Now().Format("2006-01-02")
	if d.logDate != today {
		d.logHistory = nil
		d.logDate = today
	}
	d.logHistory = append(d.logHistory, item)
	if len(d.logHistory) > 100 {
		d.logHistory = append([]LogItem(nil), d.logHistory[len(d.logHistory)-100:]...)
	}
	for ch := range d.logClients {
		select {
		case ch <- item:
		default:
		}
	}
}

func (d *Dispatcher) audit(b batch, outcome, detail string) {
	groupName := b.Rule.Target
	if groupName == "" {
		groupName = b.GroupID
	}
	level := "INFO"
	var outcomeCN string
	switch outcome {
	case "gate_blocked":
		level = "BLOCK"
		reason := detail
		if strings.HasPrefix(detail, "content gate rejected: ") {
			tag := strings.TrimPrefix(detail, "content gate rejected: ")
			switch tag {
			case "noise":
				reason = "判定为灌水噪音/无讨论价值"
			case "sensitive":
				reason = "判定为敏感违规话题"
			case "safe":
				reason = "置信度低于放行门槛"
			default:
				reason = tag
			}
		}
		outcomeCN = fmt.Sprintf("已拦截 (%s)", reason)
	case "none":
		level = "SKIP"
		outcomeCN = "无需回复 (模型决定静默)"
	case "sent":
		level = "REPLY"
		outcomeCN = fmt.Sprintf("已成功发送: %s", detail)
	case "dry_run":
		level = "REPLY"
		outcomeCN = fmt.Sprintf("演练生成 (未发群): %s", detail)
	case "send_failed":
		level = "ERROR"
		outcomeCN = fmt.Sprintf("发信失败: %s", detail)
	case "rate_limited":
		level = "SKIP"
		outcomeCN = "发言过频跳过 (防刷屏保护)"
	case "expired":
		level = "SKIP"
		outcomeCN = "消息超时丢弃"
	case "hermes_error":
		level = "ERROR"
		outcomeCN = fmt.Sprintf("大脑模型推理异常: %s", detail)
	case "promo_shadow":
		level = "SHADOW"
		outcomeCN = fmt.Sprintf("影子模式：本会插话 (未发送): %s", detail)
	case "invalid_reply":
		level = "SKIP"
		outcomeCN = fmt.Sprintf("回复不合规丢弃: %s", detail)
	default:
		outcomeCN = fmt.Sprintf("%s (%s)", outcome, detail)
	}
	d.Log(level, "群「%s」处理结果: %s", groupName, outcomeCN)
	ids := []string{}
	for _, msg := range b.Messages {
		ids = append(ids, msg.ID)
	}
	record := map[string]interface{}{"timestamp": time.Now().Format(time.RFC3339Nano), "group_id": b.GroupID, "target_group_id": b.Rule.TargetGroupID, "message_ids": ids, "outcome": outcome, "detail": detail}
	data, _ := json.Marshal(record)
	d.auditMu.Lock()
	defer d.auditMu.Unlock()
	dir := filepath.Dir(d.stateFile)
	today := time.Now().Format("2006-01-02")
	if d.auditDate != today {
		d.auditDate = today
		entries, _ := os.ReadDir(dir)
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasPrefix(entry.Name(), "audit_") || !strings.HasSuffix(entry.Name(), ".jsonl") {
				continue
			}
			date, err := time.ParseInLocation("2006-01-02", strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "audit_"), ".jsonl"), time.Local)
			if err == nil && time.Since(date) > 4*24*time.Hour {
				_ = os.Remove(filepath.Join(dir, entry.Name()))
			}
		}
	}
	path := filepath.Join(dir, "audit_"+time.Now().Format("2006-01-02")+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		d.Log("ERROR", "open audit: %v", err)
		return
	}
	_, err = f.Write(append(data, '\n'))
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		d.Log("ERROR", "write audit failed: %v %v", err, closeErr)
	}
}

func (d *Dispatcher) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", 405)
			return
		}
		http.ServeFileFS(w, r, legacyWeb, "web/index.html")
	})
	mux.HandleFunc("/api/config", d.handleConfig)
	mux.HandleFunc("/api/catalog", d.handleCatalog)
	mux.HandleFunc("/health", d.handleHealth)
	mux.HandleFunc("/api/trace/snapshot", d.handleTraceSnapshot)
	mux.HandleFunc("/api/trace/events", d.handleTraceEvents)
	mux.HandleFunc("/api/trace/runs/", d.handleTraceRun)
	mux.HandleFunc("/api/trace/config-snapshots/", d.handleConfigSnapshot)
	mux.HandleFunc("/api/control/snapshot", d.handleControlSnapshot)
	mux.HandleFunc("/api/control/action", d.handleControlAction)
	mux.HandleFunc("/api/control/operations/", d.handleControlOperation)
	mux.HandleFunc("/api/v1/direct-deliver", d.handleDirectDeliver)
	mux.HandleFunc("/api/v1/connectors", d.handleConnectorsList)
	mux.HandleFunc("/api/v1/connectors/register", d.handleConnectorRegister)
	mux.HandleFunc("/api/v1/connectors/heartbeat", d.handleConnectorHeartbeat)
	mux.HandleFunc("/api/v1/connectors/unregister", d.handleConnectorUnregister)
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", 405)
			return
		}
		d.mu.Lock()
		stats := d.state.Stats
		for _, g := range d.state.Groups {
			stats.PendingMessages += len(g.Pending) + len(g.InFlight)
		}
		d.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	})
	mux.HandleFunc("/api/events", d.handleEvents)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; frame-ancestors 'none'")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			w.Header().Set("Cache-Control", "no-store")
		}
		host := r.Host
		if parsed, _, err := net.SplitHostPort(host); err == nil {
			host = parsed
		}
		if host != "localhost" && !net.ParseIP(strings.Trim(host, "[]")).IsLoopback() {
			http.Error(w, "local host required", 403)
			return
		}
		if fetchSite := r.Header.Get("Sec-Fetch-Site"); fetchSite != "" && fetchSite != "same-origin" && fetchSite != "none" {
			http.Error(w, "cross-site access denied", 403)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			u, err := url.Parse(origin)
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}
			if err != nil || u.Host != r.Host || u.Scheme != scheme || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
				http.Error(w, "origin denied", 403)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}

func (d *Dispatcher) handleConfig(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodGet {
		cfg := d.configSnapshot()
		w.Header().Set("ETag", policyID(cfg))
		_ = json.NewEncoder(w).Encode(cfg)
		return
	}
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		http.Error(w, "method not allowed", 405)
		return
	}
	if mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mediaType != "application/json" {
		http.Error(w, "JSON required", 415)
		return
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(10 * time.Second))
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 256<<10))
	if err != nil {
		http.Error(w, "request too large", 413)
		return
	}
	d.submissionMu.Lock()
	defer d.submissionMu.Unlock()
	d.cfgMu.Lock()
	defer d.cfgMu.Unlock()
	if r.Header.Get("If-Match") != policyID(d.cfg) {
		http.Error(w, "configuration changed; reload before saving", 409)
		return
	}
	var next Config
	if r.Method == http.MethodPut {
		next, err = decodeConfigWithOptions(data, d.cfg.localPolicy, d.cfg.allowDynamicEndpoints)
	} else {
		// The existing console sends a partial flat patch. Missing fields must not reset safety settings.
		var patch map[string]json.RawMessage
		err = json.Unmarshal(data, &patch)
		if err == nil {
			original, _ := json.Marshal(d.cfg)
			_ = json.Unmarshal(original, &next)
			next.localPolicy = d.cfg.localPolicy
			next.allowDynamicEndpoints = d.cfg.allowDynamicEndpoints
			ruleData, _ := json.Marshal(next.Rules)
			var ruleFields map[string]json.RawMessage
			_ = json.Unmarshal(ruleData, &ruleFields)
			for key, value := range patch {
				if key == "dry_run" {
					if err = json.Unmarshal(value, &next.Bridge.DryRun); err != nil {
						break
					}
					continue
				}
				if _, known := ruleFields[key]; !known && key != "forward_rules" {
					err = fmt.Errorf("unknown configuration field %q", key)
					break
				}
				ruleFields[key] = value
			}
			if err == nil {
				ruleData, _ = json.Marshal(ruleFields)
				// Decode the merged field set into a fresh value. Decoding into
				// next.Rules would reuse non-nil maps and retain removed entries.
				var replacement Rules
				if err = json.Unmarshal(ruleData, &replacement); err == nil {
					next.Rules = replacement
				}
			}
			if err == nil {
				err = next.normalizeAndValidate()
			}
		}
	}
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if next.Server != d.cfg.Server || next.Relay != d.cfg.Relay || next.Hermes != d.cfg.Hermes ||
		next.Bridge.URL != d.cfg.Bridge.URL || next.Bridge.TimeoutSec != d.cfg.Bridge.TimeoutSec ||
		next.Rules.LayaURL != d.cfg.Rules.LayaURL || !reflect.DeepEqual(next.ActionEndpoints, d.cfg.ActionEndpoints) {
		http.Error(w, "service/worker changes require restart", 400)
		return
	}
	encoded, _ := json.MarshalIndent(next, "", "  ")
	if err = atomicWrite(d.cfgFile, encoded); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	d.cfg = next
	d.mu.Lock()
	for id, g := range d.state.Groups {
		if g.Timer != nil {
			g.Timer.Stop()
		}
		g.TimerGeneration++
		if rule, ok := next.route(id); ok {
			d.scheduleLocked(id, g, rule, next.Rules.BotNames)
		} else {
			g.Pending = nil
		}
	}
	err = d.saveStateLocked()
	d.mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("ETag", policyID(next))
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func (d *Dispatcher) handleCatalog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cfg := d.configSnapshot()
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(cfg.sanitizedCatalog())
}

func (d *Dispatcher) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", 405)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "stream unavailable", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	ch := make(chan LogItem, 100)
	d.logMu.Lock()
	if len(d.logClients) >= 64 {
		d.logMu.Unlock()
		http.Error(w, "too many log subscribers", http.StatusServiceUnavailable)
		return
	}
	d.logClients[ch] = true
	history := append([]LogItem(nil), d.logHistory...)
	d.logMu.Unlock()
	defer func() { d.logMu.Lock(); delete(d.logClients, ch); d.logMu.Unlock() }()
	write := func(item LogItem) bool {
		_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(5 * time.Second))
		data, _ := json.Marshal(item)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return false
		}
		flusher.Flush()
		_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
		return true
	}
	for _, item := range history {
		if !write(item) {
			return
		}
	}
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-d.ctx.Done():
			return
		case item := <-ch:
			if !write(item) {
				return
			}
		case <-ticker.C:
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(5 * time.Second))
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
			_ = http.NewResponseController(w).SetWriteDeadline(time.Time{})
		}
	}
}

func Main() {
	cfgPath := flag.String("config", "config.json", "dispatcher configuration")
	policyPath := flag.String("local-policy", os.Getenv("JINX_LOCAL_POLICY"), "operator-owned local policy (0600)")
	remindersOperation := flag.String("reminders", "", "explicit Reminders operation: authorize, status or read; no messages")
	syncOnly := flag.Bool("support-sync", false, "migrate explicitly authorized feedback only; no dispatcher or messaging")
	flag.Parse()
	absolute, err := filepath.Abs(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	policy, err := loadLocalPolicy(*policyPath)
	if err != nil {
		log.Fatal(err)
	}
	if *remindersOperation != "" {
		if !contains([]string{"authorize", "status", "read"}, *remindersOperation) {
			log.Fatal("invalid reminders operation")
		}
		if policy == nil || !policy.Support.Enabled {
			log.Fatal("reminders require explicit local policy")
		}
		response, err := launchReminderApp(context.Background(), policy.Support.Reminders, reminderRequest{Operation: *remindersOperation})
		if err != nil {
			log.Fatal(err)
		}
		_ = json.NewEncoder(os.Stdout).Encode(response)
		return
	}
	if *syncOnly {
		if policy == nil || !policy.Support.Enabled || !policy.Support.Reminders.Enabled {
			log.Fatal("support sync requires explicit local policy")
		}
		d := &Dispatcher{cfg: Config{localPolicy: policy}, stateFile: filepath.Join(filepath.Dir(absolute), "logs", "state.json"), ctx: context.Background()}
		files, err := filepath.Glob(filepath.Join(filepath.Dir(d.stateFile), "support", policy.Support.GroupID, "intake-*.json"))
		if err != nil {
			log.Fatal(err)
		}
		ids := []string{}
		for _, path := range files {
			ids = append(ids, strings.TrimSuffix(filepath.Base(path), ".json"))
		}
		tasks, err := d.syncSupport(ids, true)
		if err != nil {
			log.Fatal(err)
		}
		if tasks == nil {
			tasks = []supportTask{}
		}
		_ = json.NewEncoder(os.Stdout).Encode(tasks)
		return
	}
	lock, err := os.OpenFile(filepath.Join(filepath.Dir(absolute), ".dispatcher.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		log.Fatal("another dispatcher is already running")
	}
	disp, err := NewDispatcherWithOptions(absolute, Options{LocalPolicyPath: *policyPath})
	if err != nil {
		log.Fatal(err)
	}
	defer disp.Close()
	disp.Start()
	server := &http.Server{Addr: disp.cfg.Server.ListenAddr, Handler: disp.Handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	stopping, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-stopping.Done()
		disp.cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()
	disp.Log("INFO", "dispatcher listening on %s", server.Addr)
	disp.Log("INFO", "communication prompt sha256=%s", messageDigest(communicationRules))
	if err = server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Print(err)
	}
}
