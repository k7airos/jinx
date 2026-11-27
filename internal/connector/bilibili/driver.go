// Package bilibili implements JINX's reference Bilibili comment polling and
// reply Action Connector. It decouples comment transport behind the Driver
// interface so operators can swap EgoDriver (local ego-browser) for any other
// browser or official API client without changing Relay or Dispatcher contracts.
package bilibili

import (
	"context"
	"strings"
)

type Comment struct {
	RPID        string
	OID         string
	BVID        string
	VideoTitle  string
	CommentType int
	RootID      string
	ParentID    string
	SenderID    string
	SenderName  string
	Content     string
	TimestampMS int64
	IsSelf      bool
}

type SearchQuery struct {
	Keyword string `json:"keyword"`
	Order   string `json:"order,omitempty"` // totalrank | pubdate | click | dm | stow
	Limit   int    `json:"limit,omitempty"` // 1..20, default 5
}

type VideoCandidate struct {
	BVID         string `json:"bvid"`
	OID          string `json:"oid,omitempty"`
	Title        string `json:"title"`
	Author       string `json:"author,omitempty"`
	AuthorMID    string `json:"author_mid,omitempty"`
	URL          string `json:"url"`
	CommentCount int    `json:"comment_count,omitempty"`
	PlayCount    int    `json:"play_count,omitempty"`
	PubDate      int64  `json:"pub_date,omitempty"`
}

type ReplyRequest struct {
	Target      TargetConfig
	OID         string
	CommentType int
	RootID      string
	ParentID    string
	Text        string
}

type ReplyResult struct {
	Status     string
	Submitted  *bool
	ExternalID string
	ReasonCode string
	Detail     string
}

// Driver abstracts comment polling and reply submission for a Bilibili target.
// EgoDriver is the built-in local browser reference implementation; callers
// may pass a custom Driver to NewConnector (for example an official API client
// or an alternate browser automation adapter).
type Driver interface {
	Health(ctx context.Context) (ready bool, reason string)
	FetchComments(ctx context.Context, target TargetConfig) (comments []Comment, viewerMID string, err error)
	PostReply(ctx context.Context, req ReplyRequest) (ReplyResult, error)
}

// VideoSearcher is an optional extension interface for drivers that support
// discovering Bilibili videos by keyword search.
type VideoSearcher interface {
	SearchVideos(ctx context.Context, query SearchQuery) ([]VideoCandidate, error)
}

func NormalizeReplyID(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "0" {
		return ""
	}
	return trimmed
}
