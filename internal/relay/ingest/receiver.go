package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"jinx/internal/relay/config"
	"jinx/internal/relay/hub"
	"jinx/internal/contracts"
	"jinx/internal/relay/model"
)

// ProcessMessageFunc processes an ingested normalized message
type ProcessMessageFunc func(msg *model.Message)

// UpstreamListener manages the client connection to an upstream WebSocket service
type UpstreamListener struct {
	cfgMgr    *config.Manager
	h         *hub.Hub
	processFn ProcessMessageFunc
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	conn      *websocket.Conn
	started   bool
	stopped   bool
	wg        sync.WaitGroup
}

// NewUpstreamListener creates an upstream WS subscriber
func NewUpstreamListener(cfgMgr *config.Manager, h *hub.Hub, processFn ProcessMessageFunc) *UpstreamListener {
	ctx, cancel := context.WithCancel(context.Background())
	return &UpstreamListener{
		cfgMgr:    cfgMgr,
		h:         h,
		processFn: processFn,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Start begins the upstream listener loop
func (u *UpstreamListener) Start() {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.started || u.stopped {
		return
	}
	u.started = true
	u.wg.Add(1)
	go func() { defer u.wg.Done(); u.runLoop() }()
}

// Stop shuts down the upstream listener
func (u *UpstreamListener) Stop() {
	u.mu.Lock()
	u.stopped = true
	u.cancel()
	if u.conn != nil {
		_ = u.conn.Close()
	}
	u.mu.Unlock()
	u.wg.Wait()
}

func (u *UpstreamListener) runLoop() {
	for {
		select {
		case <-u.ctx.Done():
			return
		default:
		}

		cfg := u.cfgMgr.Get()
		if !cfg.Upstream.Enabled || cfg.Upstream.WSURL == "" {
			u.h.SetUpstreamStatus(false, "")
			u.sleep(2 * time.Second)
			continue
		}

		wsURL := cfg.Upstream.WSURL
		if err := config.ValidateLoopbackWSURL(wsURL); err != nil {
			u.h.SetUpstreamStatus(false, "")
			log.Printf("[Upstream] Rejected non-loopback upstream URL: %v", err)
			u.sleep(5 * time.Second)
			continue
		}
		u.h.SetUpstreamStatus(false, wsURL)
		log.Printf("[Upstream] Connecting to upstream WebSocket: %s", wsURL)

		dialer := websocket.Dialer{
			HandshakeTimeout: 5 * time.Second,
			Proxy:            nil,
		}

		conn, _, err := dialer.DialContext(u.ctx, wsURL, nil)
		if err != nil {
			log.Printf("[Upstream] Dial error: %v (will retry in %ds)", err, cfg.Upstream.ReconnectIntervalSec)
			u.sleep(time.Duration(cfg.Upstream.ReconnectIntervalSec) * time.Second)
			continue
		}
		conn.SetReadLimit(4 << 20)

		log.Printf("[Upstream] Connected successfully to %s", wsURL)
		u.mu.Lock()
		if u.stopped {
			u.mu.Unlock()
			_ = conn.Close()
			return
		}
		u.conn = conn
		u.mu.Unlock()
		u.h.SetUpstreamStatus(true, wsURL)

		// Send subscribe handshake if upstream expects it (e.g. WeFlow / WeChatBridge subscribe_all)
		_ = conn.WriteJSON(map[string]string{"type": "subscribe_all"})

		// Read loop
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				log.Printf("[Upstream] Read error: %v", err)
				_ = conn.Close()
				u.h.SetUpstreamStatus(false, wsURL)
				break
			}

			var raw map[string]interface{}
			if err := json.Unmarshal(data, &raw); err != nil {
				continue
			}

			normMsg, err := NormalizeRawMessage(raw)
			if err != nil {
				continue
			}

			if normMsg != nil {
				if u.ctx.Err() != nil {
					_ = conn.Close()
					return
				}
				u.processFn(normMsg)
			}
		}

		u.sleep(time.Duration(cfg.Upstream.ReconnectIntervalSec) * time.Second)
	}
}

func (u *UpstreamListener) sleep(d time.Duration) {
	if d <= 0 {
		d = 2 * time.Second
	}
	select {
	case <-u.ctx.Done():
	case <-time.After(d):
	}
}

// NormalizeRawMessage parses various WeChat bridge/hook payloads into a canonical Message
func NormalizeRawMessage(obj map[string]interface{}) (*model.Message, error) {
	if obj == nil {
		return nil, fmt.Errorf("empty payload")
	}

	// Case A: Nested wrapper like {"type": "new_message", "message": {...}}
	if inner, ok := obj["message"].(map[string]interface{}); ok {
		m, err := NormalizeRawMessage(inner)
		if err == nil && m != nil {
			if m.GroupID == "" {
				m.GroupID = extractSessionID(obj)
			}
			if m.Source == "" {
				m.Source = stringVal(obj["source"])
			}
			if m.Platform == "" {
				m.Platform = stringVal(obj["platform"])
			}
			if m.Source == "" && m.Platform != "" {
				m.Source = m.Platform
			}
			if m.AccountID == "" {
				m.AccountID = firstStringVal(obj, "account_id", "accountId")
			}
			return m, nil
		}
	}

	source := stringVal(obj["source"])
	platform := stringVal(obj["platform"])
	if source == "" && platform != "" {
		source = platform
	}
	accountID := firstStringVal(obj, "account_id", "accountId")
	parentID := firstStringVal(obj, "parent_id", "parentId")
	rootID := firstStringVal(obj, "root_id", "rootId")
	localID := int64Val(obj["local_id"])
	if localID == 0 {
		localID = int64Val(obj["localId"])
	}
	var replyContext json.RawMessage
	if rawCtx, hasCtx := obj["reply_context"]; hasCtx && rawCtx != nil {
		var err error
		replyContext, err = extractReplyContext(rawCtx)
		if err != nil {
			return nil, err
		}
	} else if rawCtx, hasCtx := obj["replyContext"]; hasCtx && rawCtx != nil {
		var err error
		replyContext, err = extractReplyContext(rawCtx)
		if err != nil {
			return nil, err
		}
	}

	// Extract Session / Group ID
	groupID := extractSessionID(obj)
	// If message is not from a chatroom, check talker or sender
	if groupID == "" {
		groupID = stringVal(obj["talker"])
	}

	// Filter out non-chatroom messages if strictly monitoring groups, but allow if it is a valid container ID or user passed mock
	isGroup := contracts.ValidRelayContainerID(groupID) || strings.Contains(groupID, "group") || strings.Contains(groupID, "room")

	// Message ID
	id := stringVal(obj["id"])
	if id == "" {
		id = stringVal(obj["msgId"])
	}
	if id == "" {
		id = stringVal(obj["platformMessageId"])
	}
	if id == "" {
		id = fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}

	// Sender ID & Name
	senderID := stringVal(obj["senderId"])
	if senderID == "" {
		senderID = stringVal(obj["sender_id"])
	}
	if senderID == "" {
		senderID = stringVal(obj["fromUser"])
	}
	if senderID == "" {
		senderID = stringVal(obj["from"])
	}
	if senderID == "" && !isGroup {
		senderID = groupID
	}

	senderName := stringVal(obj["senderName"])
	if senderName == "" {
		senderName = stringVal(obj["sender_name"])
	}
	if senderName == "" {
		senderName = stringVal(obj["sender"])
	}
	if senderName == "" {
		senderName = senderID
	}

	// Group Name
	groupName := stringVal(obj["groupName"])
	if groupName == "" {
		groupName = stringVal(obj["group_name"])
	}
	if groupName == "" {
		groupName = stringVal(obj["roomName"])
	}

	// Message Type
	mType := intVal(obj["type"])
	if mType == 0 {
		mType = intVal(obj["msg_type"])
	}
	if mType == 0 {
		mType = intVal(obj["msgType"])
	}
	if mType == 0 {
		mType = model.TypeText // default to text
	}

	// Content
	content := stringVal(obj["content"])
	if content == "" {
		content = stringVal(obj["parsedContent"])
	}
	if content == "" {
		content = stringVal(obj["rawContent"])
	}
	if content == "" {
		content = stringVal(obj["text"])
	}

	// Timestamp
	ts := int64Val(obj["timestamp"])
	if ts == 0 {
		ts = int64Val(obj["createTime"])
	}
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}
	if ts < 10000000000 { // Convert seconds to milliseconds if needed
		ts *= 1000
	}

	isEcho := boolVal(obj["isEcho"]) || boolVal(obj["is_echo"]) || boolVal(obj["isSend"])

	typeName := GetTypeName(mType)

	return &model.Message{
		ID:           id,
		Source:       source,
		LocalID:      localID,
		Platform:     platform,
		AccountID:    accountID,
		GroupID:      groupID,
		GroupName:    groupName,
		SenderID:     senderID,
		SenderName:   senderName,
		ParentID:     parentID,
		RootID:       rootID,
		ReplyContext: replyContext,
		Type:         mType,
		TypeName:     typeName,
		Content:      content,
		RawContent:   content,
		Timestamp:    ts,
		CreatedAt:    time.UnixMilli(ts).Format("2006-01-02 15:04:05"),
		IsEcho:       isEcho,
		Raw:          obj,
	}, nil
}

const maxReplyContextBytes = 8 << 10

func firstStringVal(obj map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if val := stringVal(obj[k]); val != "" {
			return val
		}
	}
	return ""
}

func extractReplyContext(raw interface{}) (json.RawMessage, error) {
	switch v := raw.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return nil, nil
		}
		if !json.Valid([]byte(trimmed)) {
			return nil, fmt.Errorf("invalid reply_context JSON")
		}
		var buf bytes.Buffer
		if err := json.Compact(&buf, []byte(trimmed)); err != nil {
			return nil, fmt.Errorf("invalid reply_context JSON: %w", err)
		}
		if buf.Len() > maxReplyContextBytes {
			return nil, fmt.Errorf("reply_context exceeds %d bytes", maxReplyContextBytes)
		}
		return json.RawMessage(buf.Bytes()), nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal reply_context: %w", err)
		}
		if len(data) > maxReplyContextBytes {
			return nil, fmt.Errorf("reply_context exceeds %d bytes", maxReplyContextBytes)
		}
		if string(data) == "null" {
			return nil, nil
		}
		return json.RawMessage(data), nil
	}
}

func extractSessionID(obj map[string]interface{}) string {
	keys := []string{
		"sessionId", "session_id", "chatroomId", "chatroom",
		"roomId", "room_id", "conversationId", "conversation_id",
		"groupId", "group_id", "containerId", "container_id", "talker",
	}
	for _, k := range keys {
		val := stringVal(obj[k])
		if val != "" {
			return val
		}
	}
	return ""
}

// GetTypeName returns human readable description of message type
func GetTypeName(t int) string {
	base := t & 0xFFFFFFFF
	switch base {
	case model.TypeText:
		return "文本消息"
	case model.TypeImage:
		return "图片"
	case model.TypeVoice:
		return "语音"
	case model.TypeVideo:
		return "视频"
	case model.TypeEmoji:
		return "表情"
	case model.TypeLocation:
		return "位置"
	case model.TypeApp:
		if t>>32 == 57 {
			return "引用消息"
		}
		return "卡片/应用/链接"
	case model.TypeQuote:
		return "引用消息"
	case model.TypeSystem:
		return "系统通知"
	default:
		return fmt.Sprintf("类型(%d)", t)
	}
}

func stringVal(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return strings.TrimSpace(val)
	case fmt.Stringer:
		return strings.TrimSpace(val.String())
	case float64:
		return fmt.Sprintf("%.0f", val)
	case int:
		return fmt.Sprintf("%d", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func intVal(v interface{}) int {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int:
		return val
	case float64:
		return int(val)
	case int64:
		return int(val)
	case string:
		n, _ := strconv.Atoi(strings.TrimSpace(val))
		return n
	case json.Number:
		n, _ := val.Int64()
		return int(n)
	default:
		return 0
	}
}

func int64Val(v interface{}) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return val
	case int:
		return int64(val)
	case float64:
		return int64(val)
	default:
		return 0
	}
}

func boolVal(v interface{}) bool {
	if v == nil {
		return false
	}
	if b, ok := v.(bool); ok {
		return b
	}
	return false
}
