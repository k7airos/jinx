package model

import (
	"encoding/json"
	"time"
)

// MessageType constants representing common WeChat message types
const (
	TypeUnknown  = 0
	TypeText     = 1     // 文本消息
	TypeImage    = 3     // 图片消息
	TypeVoice    = 34    // 语音消息
	TypeVideo    = 43    // 视频消息
	TypeEmoji    = 47    // 表情消息
	TypeLocation = 48    // 位置消息
	TypeApp      = 49    // App/卡片/链接
	TypeQuote    = 81    // 引用文本消息 (部分微信协议)
	TypeSystem   = 10000 // 系统消息（如拍一拍、移出群等）
)

// FilterAction indicates what happened to the message
type FilterAction string

const (
	ActionForward            FilterAction = "forward"             // 放行并对外推送给 AI
	ActionBlockedUnexposed   FilterAction = "blocked_unexposed"   // 群未在对外暴露白名单中
	ActionBlockedUnmonitored FilterAction = "blocked_unmonitored" // 群未在监控列表中
	ActionBlockedKeyword     FilterAction = "blocked_keyword"     // 命中屏蔽关键字
	ActionBlockedRegex       FilterAction = "blocked_regex"       // 命中屏蔽正则表达式
	ActionBlockedSender      FilterAction = "blocked_sender"      // 命中发送者黑名单
	ActionBlockedType        FilterAction = "blocked_type"        // 消息类型不被允许
)

// Message represents a normalized WeChat message
type Message struct {
	StreamID     string                 `json:"stream_id,omitempty"` // AI SSE replay cursor; scoped to this relay process
	Source       string                 `json:"source,omitempty"`
	LocalID      int64                  `json:"local_id,omitempty"`
	ServerID     string                 `json:"server_id,omitempty"`
	Details      *MessageDetails        `json:"details,omitempty"`
	ID           string                 `json:"id"`                      // 消息全局/局部唯一ID
	Platform     string                 `json:"platform,omitempty"`      // 来源平台标识 (wechat/bilibili/web)
	AccountID    string                 `json:"account_id,omitempty"`    // 采集账号标识
	GroupID      string                 `json:"group_id"`                // 群聊/容器唯一标识 (例如 xxxx@chatroom, bilibili:BV...)
	GroupName    string                 `json:"group_name"`              // 群名称 / 备注名 / 容器标签
	SenderID     string                 `json:"sender_id"`               // 发送者标识
	SenderName   string                 `json:"sender_name"`             // 发送者昵称
	ParentID     string                 `json:"parent_id,omitempty"`     // 上级内容 ID
	RootID       string                 `json:"root_id,omitempty"`       // 根内容 ID
	ReplyContext json.RawMessage        `json:"reply_context,omitempty"` // 平台回复所需透传上下文 (<= 8 KiB JSON)
	Type         int                    `json:"type"`                    // 消息类型码 (1: 文本, 3: 图片等)
	TypeName     string                 `json:"type_name"`               // 消息类型描述
	Content      string                 `json:"content"`                 // 消息文本内容 (或摘要)
	RawContent   string                 `json:"raw_content,omitempty"`   // 原始未经处理的内容
	Timestamp    int64                  `json:"timestamp"`               // 毫秒时间戳
	CreatedAt    string                 `json:"created_at"`              // 格式化时间 2006-01-02 15:04:05
	IsEcho       bool                   `json:"is_echo,omitempty"`       // 是否是自己发出的消息
	Action       FilterAction           `json:"action"`                  // 过滤判定动作
	FilterReason string                 `json:"filter_reason,omitempty"` // 阻断或放行原因说明
	Raw          map[string]interface{} `json:"raw,omitempty"`           // 原始报文字段
}

type QuotedMessage struct {
	Type    int    `json:"type"`
	Author  string `json:"author,omitempty"`
	Content string `json:"content,omitempty"`
}

type MessageDetails struct {
	Quote               *QuotedMessage `json:"quote,omitempty"`
	Kind                string         `json:"kind"`
	Title               string         `json:"title,omitempty"`
	Description         string         `json:"description,omitempty"`
	URL                 string         `json:"url,omitempty"`
	DurationSeconds     float64        `json:"duration_seconds,omitempty"`
	FileName            string         `json:"file_name,omitempty"`
	FileSize            int64          `json:"file_size,omitempty"`
	AttachmentAvailable bool           `json:"attachment_available"`
	AttachmentURL       string         `json:"attachment_url,omitempty"`
}

// FilterResult holds evaluation detail
type FilterResult struct {
	Allowed     bool         `json:"allowed"`
	Action      FilterAction `json:"action"`
	Reason      string       `json:"reason"`
	CleanedText string       `json:"cleaned_text"`
}

// GroupInfo describes a monitored/discovered group
type GroupInfo struct {
	GroupID      string    `json:"group_id"`
	GroupName    string    `json:"group_name"`
	IsMonitored  bool      `json:"is_monitored"`
	IsExposed    bool      `json:"is_exposed"`
	MessageCount int64     `json:"message_count"`
	ForwardCount int64     `json:"forward_count"`
	LastActiveAt time.Time `json:"last_active_at"`
}

// ServerStats contains runtime statistics
type ServerStats struct {
	UptimeSeconds     int64  `json:"uptime_seconds"`
	TotalReceived     int64  `json:"total_received"`
	TotalForwarded    int64  `json:"total_forwarded"`
	TotalBlocked      int64  `json:"total_blocked"`
	ActiveSSEClients  int    `json:"active_sse_clients"`
	ActiveWSClients   int    `json:"active_ws_clients"`
	UpstreamConnected bool   `json:"upstream_connected"`
	UpstreamURL       string `json:"upstream_url"`
	MonitoredGroupNum int    `json:"monitored_group_num"`
	ExposedGroupNum   int    `json:"exposed_group_num"`
}
