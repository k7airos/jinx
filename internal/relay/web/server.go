package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	embedded "jinx/internal/relay/assets"
	"jinx/internal/contracts"
	"jinx/internal/relay/config"
	"jinx/internal/relay/filter"
	"jinx/internal/relay/hub"
	"jinx/internal/relay/ingest"
	"jinx/internal/relay/model"
)

// Server handles all HTTP, SSE, WebSocket, and REST endpoints
type Server struct {
	cfgMgr        *config.Manager
	hub           *hub.Hub
	filterEng     *filter.Engine
	httpServer    *http.Server
	upgrader      websocket.Upgrader
	clientSeq     int64
	extension     Extension
	lifecycleMu   sync.Mutex
	ready         chan struct{}
	shutdown      chan struct{}
	serveDone     chan struct{}
	stopped       bool
	failed        bool
	streams       sync.WaitGroup
	listenerBound atomic.Bool
	boundLoopback atomic.Bool
	corsSnapshot  atomic.Bool
}

func isLoopbackListenAddr(listenAddr string) bool {
	host, _, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	return host == "localhost" || net.ParseIP(host).IsLoopback()
}

// NewServer constructs Server
func NewServer(cfgMgr *config.Manager, h *hub.Hub, fe *filter.Engine, extensions ...Extension) *Server {
	s := &Server{
		cfgMgr:    cfgMgr,
		hub:       h,
		filterEng: fe,
		ready:     make(chan struct{}),
		shutdown:  make(chan struct{}),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				host, _, err := net.SplitHostPort(r.RemoteAddr)
				if err != nil {
					return false
				}
				ip := net.ParseIP(host)
				if ip == nil || !ip.IsLoopback() {
					return false
				}
				return sameOriginRequest(r)
			},
		},
	}
	if cfgMgr != nil {
		cfg := cfgMgr.Get()
		s.boundLoopback.Store(isLoopbackListenAddr(cfg.Server.ListenAddr))
		s.corsSnapshot.Store(cfg.Server.EnableCORS)
	}
	if len(extensions) > 0 {
		s.extension = extensions[0]
	}
	return s
}

// ProcessIncomingMessage is the centralized pipeline for any received message
func (s *Server) ProcessIncomingMessage(msg *model.Message) model.FilterResult {
	cfg := s.cfgMgr.Get()

	// Apply filter rules
	res := s.filterEng.Evaluate(msg, cfg)
	msg.Action = res.Action
	msg.FilterReason = res.Reason
	if msg.RawContent == "" {
		msg.RawContent = msg.Content
	}
	forwarded := *msg
	forwarded.Content = ingest.SummarizeContent(msg.Type, res.CleanedText)
	msg.Content = ingest.SummarizeContent(msg.Type, msg.Content)

	if msg.Details == nil {
		msg.Details = ingest.ParseMessageDetails(msg.Type, msg.RawContent)
	}
	if forwarded.Details == nil {
		forwarded.Details = msg.Details
	}
	ingest.SanitizeMessage(msg)
	ingest.SanitizeMessage(&forwarded)

	// Record in hub (updates stats, ring buffer, and group info)
	s.hub.RecordMessage(msg, res.Allowed, cfg)

	// If allowed, broadcast to downstream AI consumers
	if res.Allowed {
		s.hub.BroadcastToAI(&forwarded)
	}

	return res
}

// Start launches the HTTP server
func (s *Server) Handler() (http.Handler, error) {
	mux := http.NewServeMux()
	extensionPaths := map[string]bool{}

	// Embedded static files
	staticSub, err := fs.Sub(embedded.StaticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("failed to load static sub fs: %w", err)
	}
	fileServer := http.FileServer(http.FS(staticSub))
	var pageHandler http.Handler
	if provider, ok := s.extension.(PageProvider); ok {
		pageHandler = provider.PageHandler()
		extensionPaths["/"] = true
		extensionPaths["/index.html"] = true
	}

	// Web UI
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path == "/" || req.URL.Path == "/index.html" {
			if pageHandler != nil {
				pageHandler.ServeHTTP(w, req)
				return
			}
			indexData, err := staticSub.Open("index.html")
			if err != nil {
				http.Error(w, "index.html not found", http.StatusNotFound)
				return
			}
			defer indexData.Close()
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = io.Copy(w, indexData)
			return
		}
		fileServer.ServeHTTP(w, req)
	})

	// SSE stream endpoints for AI
	mux.HandleFunc("/api/v1/stream", s.handleSSE)
	mux.HandleFunc("/sse", s.handleSSE)
	mux.HandleFunc("/api/v1/monitor/stream", s.admin(s.handleMonitorSSE))

	// WebSocket stream endpoints for AI
	mux.HandleFunc("/api/v1/ws", s.handleWS)
	mux.HandleFunc("/ws", s.handleWS)

	// Inbound Webhook (receives external messages)
	mux.HandleFunc("/api/v1/messages", s.handleWebhook)
	mux.HandleFunc("/api/webhook", s.handleWebhook)

	// Mock testing
	mux.HandleFunc("/api/v1/mock", s.admin(s.handleMock))

	// REST Config & Telemetry
	mux.HandleFunc("/health", s.admin(s.handleHealth))
	mux.HandleFunc("/api/v1/stats", s.admin(s.handleStats))
	configHandler := s.handleConfig
	if legacy, ok := s.extension.(LegacyConfigHandler); ok {
		configHandler = legacy.ConfigHandler()
		extensionPaths["/api/v1/config"] = true
	}
	mux.HandleFunc("/api/v1/config", s.admin(configHandler))
	mux.HandleFunc("/api/v1/console-config", s.admin(s.handleConsoleConfig))
	mux.HandleFunc("/api/v1/groups", s.admin(s.handleGroups))
	mux.HandleFunc("/api/v1/groups/expose", s.admin(s.handleGroupExpose))
	mux.HandleFunc("/api/v1/messages/recent", s.admin(s.handleRecentMessages))

	mux.HandleFunc("/api/v1/groups/monitor", s.admin(s.handleGroupMonitor))
	mux.HandleFunc("/api/v1/capabilities", s.admin(func(w http.ResponseWriter, r *http.Request) {
		s.sendJSON(w, http.StatusOK, map[string]bool{"extension": s.extension != nil})
	}))
	if s.extension != nil {
		register := func(pattern string, handler http.HandlerFunc) (err error) {
			defer func() {
				if failure := recover(); failure != nil {
					err = fmt.Errorf("conflicting extension route %q: %v", pattern, failure)
				}
			}()
			if !strings.HasPrefix(pattern, "/api/") || strings.ContainsAny(pattern, " {}\t\r\n") || strings.HasSuffix(pattern, "/") || handler == nil {
				return fmt.Errorf("invalid extension route %q", pattern)
			}
			mux.HandleFunc(pattern, s.admin(handler))
			extensionPaths[pattern] = true
			return nil
		}
		if err := s.extension.RegisterRoutes(register); err != nil {
			return nil, err
		}
	}
	// Snapshot initial loopback/CORS mode when Handler is built before Start(),
	// so runtime config updates cannot silently drop admin protection or toggle CORS.
	if !s.listenerBound.Load() && s.cfgMgr != nil {
		initialCfg := s.cfgMgr.Get()
		s.boundLoopback.Store(isLoopbackListenAddr(initialCfg.Server.ListenAddr))
		s.corsSnapshot.Store(initialCfg.Server.EnableCORS)
	}
	corsHandler := s.withCORS(mux)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if extensionPaths[r.URL.Path] {
			s.admin(mux.ServeHTTP)(w, r)
			return
		}
		corsHandler.ServeHTTP(w, r)
	}), nil
}

func (s *Server) Start() error {
	s.lifecycleMu.Lock()
	if s.stopped || s.httpServer != nil || s.failed {
		s.lifecycleMu.Unlock()
		return errors.New("server already started or stopped")
	}
	cfg := s.cfgMgr.Get()
	listener, err := net.Listen("tcp", cfg.Server.ListenAddr)
	if err != nil {
		s.failed = true
		s.lifecycleMu.Unlock()
		return err
	}
	if tcpAddr, ok := listener.Addr().(*net.TCPAddr); ok {
		s.boundLoopback.Store(tcpAddr.IP != nil && tcpAddr.IP.IsLoopback() && isLoopbackListenAddr(cfg.Server.ListenAddr))
	} else {
		s.boundLoopback.Store(isLoopbackListenAddr(cfg.Server.ListenAddr))
	}
	s.corsSnapshot.Store(cfg.Server.EnableCORS)
	s.listenerBound.Store(true)

	handler, err := s.Handler()
	if err != nil {
		_ = listener.Close()
		s.failed = true
		s.lifecycleMu.Unlock()
		return err
	}

	s.httpServer = &http.Server{
		Addr:              cfg.Server.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      0, // Keep 0 for long-lived SSE / WS; per-frame write deadlines set in handlers
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	server := s.httpServer
	s.serveDone = make(chan struct{})
	close(s.ready)
	s.lifecycleMu.Unlock()

	log.Printf("[Server] Starting WeChat Group Relay server on %s ...", cfg.Server.ListenAddr)
	err = server.Serve(listener)
	s.lifecycleMu.Lock()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.failed = true
	}
	close(s.serveDone)
	s.lifecycleMu.Unlock()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) Ready() <-chan struct{} { return s.ready }

// Stop gracefully terminates the server
func (s *Server) Stop(ctx context.Context) error {
	s.lifecycleMu.Lock()
	if !s.stopped {
		s.stopped = true
		close(s.shutdown)
	}
	server, serveDone := s.httpServer, s.serveDone
	s.lifecycleMu.Unlock()
	var err error
	if server != nil {
		err = server.Shutdown(ctx)
		if err != nil {
			_ = server.Close()
		}
	}
	done := make(chan struct{})
	go func() {
		if serveDone != nil {
			<-serveDone
		}
		s.streams.Wait()
		close(done)
	}()
	select {
	case <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) beginStream(w http.ResponseWriter) bool {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	if s.stopped || s.failed {
		http.Error(w, "server is unavailable", http.StatusServiceUnavailable)
		return false
	}
	s.streams.Add(1)
	return true
}

// handleSSE serves Server-Sent Events to AI clients
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	s.serveSSE(w, r, false)
}

func (s *Server) handleMonitorSSE(w http.ResponseWriter, r *http.Request) {
	s.serveSSE(w, r, true)
}

func (s *Server) serveSSE(w http.ResponseWriter, r *http.Request, monitor bool) {
	if !s.beginStream(w) {
		return
	}
	defer s.streams.Done()
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	// Parse optional filter groups from query param: ?groups=g1@chatroom,g2@chatroom
	groupsParam := strings.TrimSpace(r.URL.Query().Get("groups"))
	var targetGroups []string
	if groupsParam != "" && !monitor {
		for _, g := range strings.Split(groupsParam, ",") {
			g = strings.TrimSpace(g)
			if g != "" {
				targetGroups = append(targetGroups, g)
			}
		}
	}

	clientID := fmt.Sprintf("sse-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&s.clientSeq, 1))
	client := &hub.SSEClient{
		ID:       clientID,
		Ch:       make(chan *model.Message, 100),
		Filter:   hub.ClientFilter{Groups: targetGroups},
		Monitor:  monitor,
		JoinedAt: time.Now(),
	}

	var replay []*model.Message
	var cursor string
	var gap bool
	if monitor {
		s.hub.RegisterSSE(client)
	} else {
		replay, cursor, gap = s.hub.RegisterAIWithReplay(client, r.Header.Get("Last-Event-ID"))
	}
	defer s.hub.UnregisterSSE(clientID)

	controller := http.NewResponseController(w)
	writeDeadline := func() {
		_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	}
	flushReset := func() error {
		flusher.Flush()
		_ = controller.SetWriteDeadline(time.Time{})
		return nil
	}

	// Send initial handshake ping event
	writeDeadline()
	if !monitor {
		if _, err := fmt.Fprintf(w, "id: %s\n", cursor); err != nil {
			return
		}
	}
	if _, err := fmt.Fprintf(w, "event: connected\ndata: {\"client_id\":\"%s\",\"status\":\"connected\",\"replay_supported\":true}\n\n", clientID); err != nil {
		return
	}
	_ = flushReset()
	if gap {
		writeDeadline()
		if _, err := fmt.Fprint(w, "event: gap\ndata: {\"reason\":\"cursor_outside_replay_buffer\"}\n\n"); err != nil {
			return
		}
		_ = flushReset()
	}
	writeMessage := func(msg *model.Message) error {
		if !monitor && !s.filterEng.Evaluate(msg, s.cfgMgr.Get()).Allowed {
			return nil // A cached message cannot bypass a subsequently revoked exposure/rule.
		}
		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}
		writeDeadline()
		if !monitor {
			if _, err := fmt.Fprintf(w, "id: %s\n", msg.StreamID); err != nil {
				return err
			}
		}
		_, err = fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
		if err == nil {
			err = flushReset()
		}
		return err
	}
	for _, msg := range replay {
		if err := writeMessage(msg); err != nil {
			return
		}
	}
	if monitor {
		// Register before taking the snapshot so arrivals during reconnect are queued.
		data, err := json.Marshal(s.hub.GetRecentMessages())
		if err != nil {
			return
		}
		writeDeadline()
		if _, err := fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", data); err != nil {
			return
		}
		_ = flushReset()
	}

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.shutdown:
			return
		case <-r.Context().Done():
			return
		case <-ticker.C:
			// Heartbeat keep-alive comment
			writeDeadline()
			if _, err := fmt.Fprintf(w, ": keepalive %d\n\n", time.Now().Unix()); err != nil {
				return
			}
			_ = flushReset()
		case msg, ok := <-client.Ch:
			if !ok {
				return
			}
			if err := writeMessage(msg); err != nil {
				return
			}
		}
	}
}

// handleWS serves WebSocket stream to AI clients
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if !s.beginStream(w) {
		return
	}
	defer s.streams.Done()
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Upgrade error: %v", err)
		return
	}
	conn.SetReadLimit(64 << 10)

	groupsParam := strings.TrimSpace(r.URL.Query().Get("groups"))
	var targetGroups []string
	if groupsParam != "" {
		for _, g := range strings.Split(groupsParam, ",") {
			g = strings.TrimSpace(g)
			if g != "" {
				targetGroups = append(targetGroups, g)
			}
		}
	}

	clientID := fmt.Sprintf("ws-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&s.clientSeq, 1))
	client := &hub.WSClient{
		ID:       clientID,
		Conn:     conn,
		Filter:   hub.ClientFilter{Groups: targetGroups},
		SendCh:   make(chan *model.Message, 100),
		JoinedAt: time.Now(),
	}

	s.hub.RegisterWS(client)
	defer s.hub.UnregisterWS(clientID)

	// Send welcome frame
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_ = conn.WriteJSON(map[string]interface{}{
		"type":      "welcome",
		"client_id": clientID,
		"message":   "WeChat Group Relay WebSocket connected",
	})
	_ = conn.SetWriteDeadline(time.Time{})

	// Writer goroutine
	done := make(chan struct{})
	go func() {
		select {
		case <-s.shutdown:
			_ = conn.Close()
		case <-done:
		}
	}()
	go func() {
		defer close(done)
		defer conn.Close()
		for {
			var msg *model.Message
			select {
			case <-s.shutdown:
				return
			case message, ok := <-client.SendCh:
				if !ok {
					return
				}
				msg = message
			}
			if !s.filterEng.Evaluate(msg, s.cfgMgr.Get()).Allowed {
				continue
			}
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteJSON(msg); err != nil {
				return
			}
			_ = conn.SetWriteDeadline(time.Time{})
		}
	}()

	// Reader loop (handles pings or client close)
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
	s.hub.UnregisterWS(clientID)
	<-done
}

// handleWebhook processes incoming raw JSON messages
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if rejectFormContentType(w, r) {
		return
	}

	var raw map[string]interface{}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&raw); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	msg, err := ingest.NormalizeRawMessage(raw)
	if err != nil || msg == nil {
		http.Error(w, "Failed to normalize message", http.StatusBadRequest)
		return
	}

	res := s.ProcessIncomingMessage(msg)

	s.sendJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "ok",
		"id":      msg.ID,
		"allowed": res.Allowed,
		"action":  res.Action,
		"reason":  res.Reason,
	})
}

// handleMock enables sending simulated messages from Web UI
func (s *Server) handleMock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if rejectFormContentType(w, r) {
		return
	}

	var input struct {
		GroupID    string `json:"group_id"`
		GroupName  string `json:"group_name"`
		SenderID   string `json:"sender_id"`
		SenderName string `json:"sender_name"`
		Type       int    `json:"type"`
		Content    string `json:"content"`
	}

	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10)).Decode(&input); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if input.GroupID == "" {
		input.GroupID = "test-room@chatroom"
	}
	if input.Type == 0 {
		input.Type = model.TypeText
	}

	msg := &model.Message{
		ID:         fmt.Sprintf("mock-%d", time.Now().UnixNano()),
		GroupID:    input.GroupID,
		GroupName:  input.GroupName,
		SenderID:   input.SenderID,
		SenderName: input.SenderName,
		Type:       input.Type,
		TypeName:   ingest.GetTypeName(input.Type),
		Content:    input.Content,
		RawContent: input.Content,
		Timestamp:  time.Now().UnixMilli(),
		CreatedAt:  time.Now().Format("2006-01-02 15:04:05"),
	}

	res := s.ProcessIncomingMessage(msg)

	s.sendJSON(w, http.StatusOK, res)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	stats := s.hub.GetStats(cfg)
	groups := s.allGroups(r.Context(), cfg)
	stats.MonitoredGroupNum = len(groups)
	stats.ExposedGroupNum = 0
	for _, group := range groups {
		if group.IsExposed {
			stats.ExposedGroupNum++
		}
	}
	s.sendJSON(w, http.StatusOK, stats)
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		s.sendJSON(w, http.StatusOK, s.cfgMgr.Get())
		return
	}

	if r.Method == http.MethodPost {
		if rejectFormContentType(w, r) {
			return
		}
		var newCfg config.Config
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&newCfg); err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		current := s.cfgMgr.Get()
		if newCfg.Server != current.Server {
			http.Error(w, "server settings cannot be changed at runtime", http.StatusBadRequest)
			return
		}
		if newCfg.Upstream.Enabled {
			if err := config.ValidateLoopbackWSURL(newCfg.Upstream.WSURL); err != nil {
				http.Error(w, "upstream.ws_url must target loopback", http.StatusBadRequest)
				return
			}
		}

		if err := s.cfgMgr.Update(newCfg); err != nil {
			http.Error(w, "Failed to save config", http.StatusBadRequest)
			return
		}

		s.sendJSON(w, http.StatusOK, s.cfgMgr.Get())
		return
	}

	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func (s *Server) handleGroups(w http.ResponseWriter, r *http.Request) {
	cfg := s.cfgMgr.Get()
	groups := s.allGroups(r.Context(), cfg)
	s.sendJSON(w, http.StatusOK, groups)
}

func (s *Server) handleGroupExpose(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if rejectFormContentType(w, r) {
		return
	}

	var req struct {
		GroupID string `json:"group_id"`
		Exposed bool   `json:"exposed"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			http.Error(w, "Request too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	req.GroupID = strings.TrimSpace(req.GroupID)
	if !contracts.ValidRelayContainerID(req.GroupID) {
		http.Error(w, "群 ID 无效", http.StatusBadRequest)
		return
	}

	cfg := s.cfgMgr.Get()
	if !req.Exposed && config.MatchesGroup(cfg.GroupControl.ExposedGroups, "", "") {
		http.Error(w, "暴露名单包含 *，请先从配置中移除通配符", http.StatusConflict)
		return
	}
	name := req.GroupID
	for _, group := range s.allGroups(r.Context(), cfg) {
		if group.GroupID == req.GroupID {
			name = group.GroupName
			break
		}
	}
	var newExposed []string
	for _, g := range cfg.GroupControl.ExposedGroups {
		if strings.TrimSpace(g) != req.GroupID && strings.TrimSpace(g) != name {
			newExposed = append(newExposed, g)
		}
	}
	if req.Exposed {
		newExposed = append(newExposed, req.GroupID)
	}
	cfg.GroupControl.ExposedGroups = newExposed

	if err := s.cfgMgr.Update(cfg); err != nil {
		http.Error(w, "failed to save configuration", http.StatusInternalServerError)
		return
	}

	s.sendJSON(w, http.StatusOK, map[string]interface{}{"status": "ok", "exposed_groups": newExposed})
}

func (s *Server) handleRecentMessages(w http.ResponseWriter, r *http.Request) {
	list := s.hub.GetRecentMessages()
	s.sendJSON(w, http.StatusOK, list)
}

func (s *Server) sendJSON(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(data)
}

func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/v1/console-config" {
			s.admin(next.ServeHTTP)(w, r)
			return
		}
		if s.boundLoopback.Load() {
			// A loopback deployment is private, including AI streams and inbound webhooks.
			s.admin(next.ServeHTTP)(w, r)
			return
		}
		if s.corsSnapshot.Load() {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, PUT, DELETE")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept")
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}
