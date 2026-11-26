package hub

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"jinx/internal/relay/config"
	"jinx/internal/relay/model"
)

// ClientFilter specifies per-client subscription filtering
type ClientFilter struct {
	Groups []string // specific groups requested by this consumer (empty = all exposed)
}

// SSEClient represents a downstream Server-Sent Events subscriber
type SSEClient struct {
	ID       string
	Ch       chan *model.Message
	Filter   ClientFilter
	Monitor  bool // administrative feed, including messages blocked from AI
	JoinedAt time.Time
}

// WSClient represents a downstream WebSocket subscriber
type WSClient struct {
	ID       string
	Conn     *websocket.Conn
	Filter   ClientFilter
	SendCh   chan *model.Message
	JoinedAt time.Time
}

// Hub manages active clients, ring buffer, discovered groups, and message dispatch
type Hub struct {
	mu             sync.RWMutex
	sseClients     map[string]*SSEClient
	wsClients      map[string]*WSClient
	recentMessages []*model.Message
	maxRecent      int
	aiRecent       []*model.Message
	streamEpoch    string
	streamSequence uint64
	lastProgressAt time.Time

	groups map[string]*model.GroupInfo // GroupID -> GroupInfo

	// Stats
	startTime      time.Time
	totalReceived  int64
	totalForwarded int64
	totalBlocked   int64

	upstreamConnected atomic.Bool
	upstreamURL       string
}

// NewHub initializes Hub
func NewHub(maxRecent int) *Hub {
	if maxRecent <= 0 {
		maxRecent = 200
	}
	return &Hub{
		sseClients:     make(map[string]*SSEClient),
		wsClients:      make(map[string]*WSClient),
		recentMessages: make([]*model.Message, 0, maxRecent),
		maxRecent:      maxRecent,
		streamEpoch:    fmt.Sprintf("%x", time.Now().UnixNano()),
		groups:         make(map[string]*model.GroupInfo),
		startTime:      time.Now(),
	}
}

// SetUpstreamStatus updates upstream connection indicator
func (h *Hub) SetUpstreamStatus(connected bool, url string) {
	h.upstreamConnected.Store(connected)
	h.mu.Lock()
	h.upstreamURL = url
	h.mu.Unlock()
}

// RecordMessage updates counters, group info, and ring buffer
func (h *Hub) RecordMessage(msg *model.Message, allowed bool, cfg config.Config) {
	atomic.AddInt64(&h.totalReceived, 1)
	if allowed {
		atomic.AddInt64(&h.totalForwarded, 1)
	} else {
		atomic.AddInt64(&h.totalBlocked, 1)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// Update or discover group
	h.lastProgressAt = time.Now()
	if msg.GroupID != "" {
		g, exists := h.groups[msg.GroupID]
		if !exists {
			if len(h.groups) >= 4096 {
				var evictID string
				var oldest time.Time
				for id, existing := range h.groups {
					if evictID == "" || existing.LastActiveAt.Before(oldest) {
						evictID = id
						oldest = existing.LastActiveAt
					}
				}
				if evictID != "" {
					delete(h.groups, evictID)
				}
			}
			displayName := msg.GroupName
			if alias, ok := cfg.GroupControl.GroupAliases[msg.GroupID]; ok && alias != "" {
				displayName = alias
			}
			if displayName == "" {
				displayName = msg.GroupID
			}
			g = &model.GroupInfo{
				GroupID:   msg.GroupID,
				GroupName: displayName,
			}
			h.groups[msg.GroupID] = g
		} else if msg.GroupName != "" && g.GroupName == msg.GroupID {
			g.GroupName = msg.GroupName
		}

		g.MessageCount++
		if allowed {
			g.ForwardCount++
		}
		g.LastActiveAt = time.Now()

		// Refresh monitored/exposed flags
		g.IsMonitored = cfg.GroupControl.MonitorAll || contains(cfg.GroupControl.MonitoredGroups, msg.GroupID)
		g.IsExposed = contains(cfg.GroupControl.ExposedGroups, msg.GroupID) || contains(cfg.GroupControl.ExposedGroups, "*")
	}

	// Retention depends on group exposure, not whether the message passed AI filters.
	isExposed := false
	for _, group := range cfg.GroupControl.ExposedGroups {
		group = strings.TrimSpace(group)
		if group != "" && (group == "*" || group == msg.GroupID || group == msg.GroupName) {
			isExposed = true
			break
		}
	}
	if !cfg.GroupControl.DropUnexposedCompletely || isExposed {
		if len(h.recentMessages) >= h.maxRecent {
			h.recentMessages = h.recentMessages[1:]
		}
		h.recentMessages = append(h.recentMessages, msg)
		for _, client := range h.sseClients {
			if client.Monitor && matchesClientFilter(msg, client.Filter) {
				select {
				case client.Ch <- msg:
				default:
					// Reconnect with a fresh snapshot if the dashboard falls behind.
					close(client.Ch)
					delete(h.sseClients, client.ID)
				}
			}
		}
	}
}

// BroadcastToAI delivers an allowed message to all downstream SSE & WS subscribers
func (h *Hub) BroadcastToAI(msg *model.Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.streamSequence++
	copy := *msg
	copy.StreamID = fmt.Sprintf("%s:%d", h.streamEpoch, h.streamSequence)
	msg = &copy
	h.aiRecent = append(h.aiRecent, msg)
	if len(h.aiRecent) > h.maxRecent {
		h.aiRecent = append([]*model.Message(nil), h.aiRecent[len(h.aiRecent)-h.maxRecent:]...)
	}

	// Send to SSE clients
	for _, client := range h.sseClients {
		if !client.Monitor && matchesClientFilter(msg, client.Filter) {
			select {
			case client.Ch <- msg:
			default:
				// Reconnect and replay from the last durable cursor instead of silently losing messages.
				close(client.Ch)
				delete(h.sseClients, client.ID)
			}
		}
	}

	// Send to WS clients
	for _, client := range h.wsClients {
		if matchesClientFilter(msg, client.Filter) {
			select {
			case client.SendCh <- msg:
			default:
			}
		}
	}
}

func (h *Hub) addSSEClientLocked(client *SSEClient) {
	if _, exists := h.sseClients[client.ID]; !exists && len(h.sseClients) >= 64 {
		var oldestID string
		var oldestAt time.Time
		for id, c := range h.sseClients {
			if oldestID == "" || c.JoinedAt.Before(oldestAt) {
				oldestID = id
				oldestAt = c.JoinedAt
			}
		}
		if oldestID != "" {
			close(h.sseClients[oldestID].Ch)
			delete(h.sseClients, oldestID)
		}
	}
	h.sseClients[client.ID] = client
}

// RegisterAIWithReplay registers and snapshots under the same lock, leaving no reconnect gap.
// A cursor outside the bounded buffer (or from a previous process) produces an explicit gap event.
func (h *Hub) RegisterAIWithReplay(client *SSEClient, lastID string) (replay []*model.Message, cursor string, gap bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.addSSEClientLocked(client)
	cursor = fmt.Sprintf("%s:%d", h.streamEpoch, h.streamSequence)
	if lastID == "" || lastID == cursor {
		return nil, cursor, false
	}
	start := 0
	found := lastID == h.streamEpoch+":0" && h.streamSequence <= uint64(len(h.aiRecent))
	for i, msg := range h.aiRecent {
		if msg.StreamID == lastID {
			start = i + 1
			found = true
			break
		}
	}
	for _, msg := range h.aiRecent[start:] {
		if matchesClientFilter(msg, client.Filter) {
			replay = append(replay, msg)
		}
	}
	return replay, cursor, !found
}

// RegisterSSE adds an SSE subscriber
func (h *Hub) RegisterSSE(client *SSEClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.addSSEClientLocked(client)
}

// UnregisterSSE removes an SSE subscriber
func (h *Hub) UnregisterSSE(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.sseClients[id]; ok {
		close(c.Ch)
		delete(h.sseClients, id)
	}
}

// RegisterWS adds a WebSocket subscriber
func (h *Hub) RegisterWS(client *WSClient) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.wsClients[client.ID]; !exists && len(h.wsClients) >= 64 {
		var oldestID string
		var oldestAt time.Time
		for id, c := range h.wsClients {
			if oldestID == "" || c.JoinedAt.Before(oldestAt) {
				oldestID = id
				oldestAt = c.JoinedAt
			}
		}
		if oldestID != "" {
			old := h.wsClients[oldestID]
			close(old.SendCh)
			_ = old.Conn.Close()
			delete(h.wsClients, oldestID)
		}
	}
	h.wsClients[client.ID] = client
}

// UnregisterWS removes a WebSocket subscriber
func (h *Hub) UnregisterWS(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.wsClients[id]; ok {
		close(c.SendCh)
		_ = c.Conn.Close()
		delete(h.wsClients, id)
	}
}

// GetRecentMessages returns a copy of recently received messages
func (h *Hub) GetRecentMessages() []*model.Message {
	h.mu.RLock()
	defer h.mu.RUnlock()

	res := make([]*model.Message, len(h.recentMessages))
	copy(res, h.recentMessages)
	return res
}

// GetGroups returns list of discovered groups with active flags based on cfg
func (h *Hub) GetGroups(cfg config.Config) []*model.GroupInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()

	seen := make(map[string]bool)
	res := make([]*model.GroupInfo, 0, len(h.groups)+len(cfg.GroupControl.GroupAliases))

	// 1. Groups that have already received messages in this session
	for _, g := range h.groups {
		item := *g
		item.IsMonitored = cfg.GroupControl.MonitorAll || contains(cfg.GroupControl.MonitoredGroups, g.GroupID)
		item.IsExposed = contains(cfg.GroupControl.ExposedGroups, g.GroupID) || contains(cfg.GroupControl.ExposedGroups, "*")
		if alias, ok := cfg.GroupControl.GroupAliases[g.GroupID]; ok && alias != "" {
			item.GroupName = alias
		}
		seen[g.GroupID] = true
		res = append(res, &item)
	}

	// 2. Groups configured in GroupAliases
	for gID, alias := range cfg.GroupControl.GroupAliases {
		if !seen[gID] {
			item := &model.GroupInfo{
				GroupID:     gID,
				GroupName:   alias,
				IsMonitored: cfg.GroupControl.MonitorAll || contains(cfg.GroupControl.MonitoredGroups, gID),
				IsExposed:   contains(cfg.GroupControl.ExposedGroups, gID) || contains(cfg.GroupControl.ExposedGroups, "*"),
			}
			seen[gID] = true
			res = append(res, item)
		}
	}

	// 3. Groups in ExposedGroups
	for _, gID := range cfg.GroupControl.ExposedGroups {
		if gID != "*" && !seen[gID] {
			displayName := gID
			if alias, ok := cfg.GroupControl.GroupAliases[gID]; ok && alias != "" {
				displayName = alias
			}
			item := &model.GroupInfo{
				GroupID:     gID,
				GroupName:   displayName,
				IsMonitored: cfg.GroupControl.MonitorAll || contains(cfg.GroupControl.MonitoredGroups, gID),
				IsExposed:   true,
			}
			seen[gID] = true
			res = append(res, item)
		}
	}

	return res
}

// GetStats returns current server telemetry
func (h *Hub) GetStats(cfg config.Config) model.ServerStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	monitoredNum := len(h.groups)
	if !cfg.GroupControl.MonitorAll {
		monitoredNum = len(cfg.GroupControl.MonitoredGroups)
	}
	aiSSEClients := 0
	for _, client := range h.sseClients {
		if !client.Monitor {
			aiSSEClients++
		}
	}

	return model.ServerStats{
		UptimeSeconds:     int64(time.Since(h.startTime).Seconds()),
		TotalReceived:     atomic.LoadInt64(&h.totalReceived),
		TotalForwarded:    atomic.LoadInt64(&h.totalForwarded),
		TotalBlocked:      atomic.LoadInt64(&h.totalBlocked),
		ActiveSSEClients:  aiSSEClients,
		ActiveWSClients:   len(h.wsClients),
		UpstreamConnected: h.upstreamConnected.Load(),
		UpstreamURL:       h.upstreamURL,
		MonitoredGroupNum: monitoredNum,
		ExposedGroupNum:   len(cfg.GroupControl.ExposedGroups),
	}
}

func matchesClientFilter(msg *model.Message, filter ClientFilter) bool {
	if len(filter.Groups) == 0 {
		return true
	}
	for _, g := range filter.Groups {
		if g == msg.GroupID || (msg.GroupName != "" && g == msg.GroupName) {
			return true
		}
	}
	return false
}

func contains(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}
