package v2ex

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

type PollReport struct {
	TargetsPolled     int
	CommentsFetched   int
	Forwarded         int
	SkippedSeen       int
	SkippedHistorical int
	PushErrors        int
}

type replyContextPayload struct {
	TopicID       string `json:"topic_id,omitempty"`
	ReplyID       string `json:"reply_id,omitempty"`
	ReplyNo       int    `json:"reply_no,omitempty"`
	ReplyToMember string `json:"reply_to_member,omitempty"`
	PageURL       string `json:"page_url,omitempty"`
}

type Connector struct {
	cfg               Config
	driver            Driver
	relay             *connector.RelayClient
	cursors           *connector.CursorStore
	idem              *connector.IdempotencyStore
	server            *connector.Server
	targets           map[string]TargetConfig
	selfSet           map[string]bool
	pollMu            sync.Mutex
	statusMu          sync.RWMutex
	lastPollAt        *time.Time
	lastPollErrorCode string
	now               func() time.Time
}

func NewConnector(cfg Config, driver Driver) (*Connector, error) {
	if err := cfg.NormalizeAndValidate(); err != nil {
		return nil, err
	}
	if driver == nil {
		driver = NewEgoDriver(cfg.Ego)
	}
	relayClient, err := connector.NewRelayClient(cfg.RelayURL, 10*time.Second)
	if err != nil {
		return nil, err
	}
	cursors, err := connector.OpenCursorStore(filepath.Join(cfg.StateDir, "cursor.json"))
	if err != nil {
		return nil, err
	}
	idem, err := connector.OpenIdempotencyStore(filepath.Join(cfg.StateDir, "idempotency.json"))
	if err != nil {
		return nil, err
	}
	targets := make(map[string]TargetConfig, len(cfg.Targets))
	for _, t := range cfg.Targets {
		targets[t.ContainerID] = t
	}
	selfSet := make(map[string]bool, len(cfg.SelfUsernames))
	for _, username := range cfg.SelfUsernames {
		selfSet[strings.ToLower(username)] = true
	}
	c := &Connector{
		cfg:     cfg,
		driver:  driver,
		relay:   relayClient,
		cursors: cursors,
		idem:    idem,
		targets: targets,
		selfSet: selfSet,
		now:     time.Now,
	}
	c.server = connector.NewServer(connector.ServerOptions{
		Executor: c,
		Store:    idem,
		DryRun:   cfg.IsDryRun(),
		Now:      func() time.Time { return c.now() },
	})
	return c, nil
}

func (c *Connector) Handler() http.Handler        { return c.server.Handler() }
func (c *Connector) Platform() contracts.Platform { return Platform }
func (c *Connector) AccountID() string            { return c.cfg.AccountID }
func (c *Connector) ConnectorName() string        { return "v2ex-ego-connector" }
func (c *Connector) Capabilities() []contracts.ActionKind {
	return []contracts.ActionKind{contracts.ActionReplyText, contracts.ActionThreadReply}
}

func (c *Connector) Health(ctx context.Context) (bool, string) {
	return c.driver.Health(ctx)
}

func (c *Connector) Status() connector.StatusSnapshot {
	targets := make([]connector.HealthTarget, 0, len(c.cfg.Targets))
	for _, t := range c.cfg.Targets {
		targets = append(targets, connector.HealthTarget{
			ContainerID: t.ContainerID,
			Label:       t.Label,
			Enabled:     t.Enabled,
		})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].ContainerID < targets[j].ContainerID })
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	var polledAt *time.Time
	if c.lastPollAt != nil {
		copied := *c.lastPollAt
		polledAt = &copied
	}
	return connector.StatusSnapshot{
		Targets:           targets,
		LastPollAt:        polledAt,
		LastPollErrorCode: c.lastPollErrorCode,
	}
}

func (c *Connector) recordPollStatus(errCode string) {
	now := c.now().UTC()
	c.statusMu.Lock()
	c.lastPollAt = &now
	c.lastPollErrorCode = errCode
	c.statusMu.Unlock()
}

func (c *Connector) PollOnce(ctx context.Context) (PollReport, error) {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()

	var report PollReport
	var firstErr error
	var errCode string
	cutoffMS := c.now().Add(-time.Duration(c.cfg.MaxInitialAgeSec) * time.Second).UnixMilli()

	for _, target := range c.cfg.Targets {
		if !target.Enabled {
			continue
		}
		if ctx.Err() != nil {
			if firstErr == nil {
				firstErr = ctx.Err()
				errCode = "context_canceled"
			}
			break
		}
		report.TargetsPolled++
		fetchCtx, cancel := context.WithTimeout(ctx, time.Duration(c.cfg.Ego.TimeoutSec)*time.Second)
		comments, viewerUsername, err := c.driver.FetchComments(fetchCtx, target)
		cancel()
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("fetch comments for %s: %w", target.ContainerID, err)
				errCode = "fetch_failed"
			}
			continue
		}
		if target.PollLimit > 0 && len(comments) > target.PollLimit {
			comments = comments[len(comments)-target.PollLimit:]
		}
		report.CommentsFetched += len(comments)
		seeded := c.cursors.IsSeeded(target.ContainerID)
		targetPushFailed := false

		for _, comment := range comments {
			replyID := NormalizeReplyID(comment.ReplyID)
			if replyID == "" || strings.TrimSpace(comment.Content) == "" {
				continue
			}
			if c.cursors.HasSeen(target.ContainerID, replyID) {
				report.SkippedSeen++
				continue
			}
			if !seeded && comment.TimestampMS > 0 && comment.TimestampMS < cutoffMS {
				if err := c.cursors.MarkSeen(target.ContainerID, replyID, comment.TimestampMS); err != nil && firstErr == nil {
					firstErr = err
					errCode = "cursor_error"
				}
				report.SkippedHistorical++
				continue
			}

			topicID := strings.TrimSpace(comment.TopicID)
			if topicID == "" {
				topicID = target.TopicID
			}
			senderID := strings.TrimSpace(comment.SenderID)
			senderName := strings.TrimSpace(comment.SenderName)
			if senderName == "" {
				senderName = senderID
			}
			replyCtx, _ := json.Marshal(replyContextPayload{
				TopicID:       topicID,
				ReplyID:       replyID,
				ReplyNo:       comment.Floor,
				ReplyToMember: senderID,
				PageURL:       target.URL,
			})
			isEcho := comment.IsSelf ||
				(senderID != "" && c.selfSet[strings.ToLower(senderID)]) ||
				(viewerUsername != "" && senderID != "" && strings.EqualFold(senderID, viewerUsername))
			label := target.Label
			if label == "" {
				label = target.ContainerID
			}
			payload := connector.InboundPayload{
				ID:           replyID,
				Source:       "v2ex:ego",
				Platform:     string(Platform),
				AccountID:    c.cfg.AccountID,
				GroupID:      target.ContainerID,
				GroupName:    label,
				SenderID:     senderID,
				SenderName:   senderName,
				ReplyContext: replyCtx,
				Type:         1,
				Content:      comment.Content,
				Timestamp:    comment.TimestampMS,
				IsEcho:       isEcho,
			}
			if payload.Timestamp <= 0 {
				payload.Timestamp = c.now().UnixMilli()
			}
			if _, err := c.relay.Push(ctx, payload); err != nil {
				targetPushFailed = true
				report.PushErrors++
				if firstErr == nil {
					firstErr = fmt.Errorf("push %s/%s: %w", target.ContainerID, replyID, err)
					errCode = "relay_push_failed"
				}
				continue
			}
			report.Forwarded++
			if err := c.cursors.MarkSeen(target.ContainerID, replyID, payload.Timestamp); err != nil && firstErr == nil {
				firstErr = err
				errCode = "cursor_error"
			}
		}

		if !seeded && !targetPushFailed {
			if err := c.cursors.MarkSeeded(target.ContainerID); err != nil && firstErr == nil {
				firstErr = err
				errCode = "cursor_error"
			}
		}
	}
	c.recordPollStatus(errCode)
	return report, firstErr
}

func (c *Connector) Execute(ctx context.Context, req connector.ActionRequest) (connector.ActionResponse, error) {
	target, ok := c.targets[req.Target.ContainerID]
	if !ok || !target.Enabled {
		return connector.ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  connector.BoolPtr(false),
			ReasonCode: "target_not_configured",
		}, nil
	}
	var rc replyContextPayload
	if len(req.ReplyContext) > 0 {
		if err := json.Unmarshal(req.ReplyContext, &rc); err != nil {
			return connector.ActionResponse{
				ActionID:   req.ActionID,
				Status:     "failed",
				Submitted:  connector.BoolPtr(false),
				ReasonCode: "invalid_reply_context",
			}, nil
		}
	}
	if rcTopic := strings.TrimSpace(rc.TopicID); rcTopic != "" && rcTopic != target.TopicID {
		return connector.ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  connector.BoolPtr(false),
			ReasonCode: "target_mismatch",
		}, nil
	}
	replyToMember := strings.TrimSpace(rc.ReplyToMember)
	if replyToMember != "" && !usernamePattern.MatchString(replyToMember) {
		return connector.ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  connector.BoolPtr(false),
			ReasonCode: "invalid_reply_context",
		}, nil
	}
	replyToID := NormalizeReplyID(req.Target.ContentID)
	text := req.Text
	if req.Kind == string(contracts.ActionThreadReply) &&
		replyToID != "" &&
		replyToMember != "" &&
		!c.selfSet[strings.ToLower(replyToMember)] {
		text = FormatReplyText(req.Text, replyToMember, c.cfg.IsAutoMention())
	}

	execCtx, cancel := context.WithTimeout(ctx, time.Duration(c.cfg.Ego.TimeoutSec)*time.Second)
	defer cancel()
	result, err := c.driver.PostReply(execCtx, ReplyRequest{
		Target:        target,
		TopicID:       target.TopicID,
		ReplyToID:     replyToID,
		ReplyToMember: replyToMember,
		ReplyToFloor:  rc.ReplyNo,
		Text:          text,
	})
	if err != nil {
		if result.Status == "failed" && result.Submitted != nil && !*result.Submitted {
			return connector.ActionResponse{
				ActionID:   req.ActionID,
				Status:     "failed",
				Submitted:  connector.BoolPtr(false),
				ReasonCode: result.ReasonCode,
				Detail:     result.Detail,
			}, err
		}
		return connector.ActionResponse{
			ActionID:   req.ActionID,
			Status:     "unknown",
			ReasonCode: result.ReasonCode,
		}, err
	}
	return connector.ActionResponse{
		ActionID:   req.ActionID,
		Status:     result.Status,
		Submitted:  result.Submitted,
		ExternalID: result.ExternalID,
		ReasonCode: result.ReasonCode,
		Detail:     result.Detail,
	}, nil
}
