// Package v2ex implements JINX's reference V2EX topic comment polling and
// reply Action Connector. It decouples forum transport behind the Driver
// interface so operators can swap EgoDriver (local ego-browser) for any other
// browser or API client without changing Relay or Dispatcher contracts.
package v2ex

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"

	"jinx/internal/contracts"
)

const Platform = contracts.Platform("v2ex")

type Comment struct {
	ReplyID     string
	TopicID     string
	Floor       int
	SenderID    string
	SenderName  string
	Content     string
	TimestampMS int64
	IsSelf      bool
}

type ReplyRequest struct {
	Target        TargetConfig
	TopicID       string
	ReplyToID     string
	ReplyToMember string
	ReplyToFloor  int
	Text          string
}

type ReplyResult struct {
	Status     string
	Submitted  *bool
	ExternalID string
	ReasonCode string
	Detail     string
}

// Driver abstracts comment polling and reply submission for a V2EX topic.
// EgoDriver is the built-in local browser reference implementation; callers
// may pass a custom Driver to NewConnector.
type Driver interface {
	Health(ctx context.Context) (ready bool, reason string)
	FetchComments(ctx context.Context, target TargetConfig) (comments []Comment, viewerUsername string, err error)
	PostReply(ctx context.Context, req ReplyRequest) (ReplyResult, error)
}

func NormalizeReplyID(raw string) string {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "r_")
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "" || trimmed == "0" {
		return ""
	}
	return trimmed
}

func FormatReplyText(text, replyToMember string, autoMention bool) string {
	member := strings.TrimSpace(replyToMember)
	if !autoMention || member == "" {
		return text
	}
	trimmed := strings.TrimSpace(text)
	prefix := "@" + member
	if len(trimmed) >= len(prefix) && strings.EqualFold(trimmed[:len(prefix)], prefix) {
		rest := trimmed[len(prefix):]
		if rest == "" {
			return text
		}
		r, _ := utf8.DecodeRuneInString(rest)
		if unicode.IsSpace(r) || r == ':' || r == '：' || r == ',' || r == '，' || r == '#' {
			return text
		}
	}
	return prefix + " " + text
}
