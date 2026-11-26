package bilibili

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
	OID         string `json:"oid,omitempty"`
	BVID        string `json:"bvid,omitempty"`
	CommentType int    `json:"comment_type,omitempty"`
	RPID        string `json:"rpid,omitempty"`
	Root        string `json:"root,omitempty"`
	Parent      string `json:"parent,omitempty"`
	PageURL     string `json:"page_url,omitempty"`
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
	resolvedOIDs      map[string]string
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
	resolvedOIDs := make(map[string]string, len(cfg.Targets))
	for _, t := range cfg.Targets {
		targets[t.ContainerID] = t
		if t.OID != "" {
			resolvedOIDs[t.ContainerID] = t.OID
		}
	}
	selfSet := make(map[string]bool, len(cfg.SelfMIDs))
	for _, mid := range cfg.SelfMIDs {
		selfSet[mid] = true
	}
	c := &Connector{
		cfg:          cfg,
		driver:       driver,
		relay:        relayClient,
		cursors:      cursors,
		idem:         idem,
		targets:      targets,
		resolvedOIDs: resolvedOIDs,
		selfSet:      selfSet,
		now:          time.Now,
	}
	c.server = connector.NewServer(connector.ServerOptions{
		Executor: c,
		Store:    idem,
		DryRun:   cfg.IsDryRun(),
		Now:      func() time.Time { return c.now() },
	})
	return c, nil
}

func (c *Connector) Handler() http.Handler              { return c.server.Handler() }
func (c *Connector) Platform() contracts.Platform       { return contracts.PlatformBilibili }
func (c *Connector) AccountID() string                  { return c.cfg.AccountID }
func (c *Connector) ConnectorName() string              { return "bilibili-ego-connector" }
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
		comments, viewerMID, err := c.driver.FetchComments(fetchCtx, target)
		cancel()
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("fetch comments for %s: %w", target.ContainerID, err)
				errCode = "fetch_failed"
			}
			continue
		}
		report.CommentsFetched += len(comments)
		seeded := c.cursors.IsSeeded(target.ContainerID)
		targetPushFailed := false

		for _, comment := range comments {
			if c.cursors.HasSeen(target.ContainerID, comment.RPID) {
				report.SkippedSeen++
				continue
			}
			if !seeded && comment.TimestampMS > 0 && comment.TimestampMS < cutoffMS {
				if err := c.cursors.MarkSeen(target.ContainerID, comment.RPID, comment.TimestampMS); err != nil && firstErr == nil {
					firstErr = err
					errCode = "cursor_error"
				}
				report.SkippedHistorical++
				continue
			}

			oid := comment.OID
			if oid == "" {
				oid = target.OID
			}
			if target.OID == "" && oidPattern.MatchString(oid) {
				c.statusMu.Lock()
				c.resolvedOIDs[target.ContainerID] = oid
				c.statusMu.Unlock()
			}
			commentType := comment.CommentType
			if commentType == 0 {
				commentType = target.CommentType
			}
			bvid := comment.BVID
			if bvid == "" {
				bvid = target.BVID
			}
			replyCtx, _ := json.Marshal(replyContextPayload{
				OID:         oid,
				BVID:        bvid,
				CommentType: commentType,
				RPID:        comment.RPID,
				Root:        comment.RootID,
				Parent:      comment.ParentID,
				PageURL:     target.URL,
			})
			isEcho := comment.IsSelf || c.selfSet[comment.SenderID] || (viewerMID != "" && comment.SenderID == viewerMID)
			label := target.Label
			if label == "" {
				label = target.ContainerID
			}
			senderName := comment.SenderName
			if senderName == "" {
				senderName = comment.SenderID
			}
			payload := connector.InboundPayload{
				ID:           comment.RPID,
				Source:       "bilibili:ego",
				Platform:     string(contracts.PlatformBilibili),
				AccountID:    c.cfg.AccountID,
				GroupID:      target.ContainerID,
				GroupName:    label,
				SenderID:     comment.SenderID,
				SenderName:   senderName,
				ParentID:     comment.ParentID,
				RootID:       comment.RootID,
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
					firstErr = fmt.Errorf("push %s/%s: %w", target.ContainerID, comment.RPID, err)
					errCode = "relay_push_failed"
				}
				continue
			}
			report.Forwarded++
			if err := c.cursors.MarkSeen(target.ContainerID, comment.RPID, payload.Timestamp); err != nil && firstErr == nil {
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
	commentType := target.CommentType
	if commentType == 0 {
		commentType = 1
	}
	if rc.CommentType != 0 && rc.CommentType != commentType {
		return connector.ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  connector.BoolPtr(false),
			ReasonCode: "target_mismatch",
		}, nil
	}
	if rc.BVID != "" && target.BVID != "" && strings.TrimSpace(rc.BVID) != target.BVID {
		return connector.ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  connector.BoolPtr(false),
			ReasonCode: "target_mismatch",
		}, nil
	}
	oid := target.OID
	if oid == "" {
		c.statusMu.RLock()
		oid = c.resolvedOIDs[target.ContainerID]
		c.statusMu.RUnlock()
	}
	rcOID := strings.TrimSpace(rc.OID)
	if rcOID != "" {
		if !oidPattern.MatchString(rcOID) || (oid != "" && rcOID != oid) {
			return connector.ActionResponse{
				ActionID:   req.ActionID,
				Status:     "failed",
				Submitted:  connector.BoolPtr(false),
				ReasonCode: "target_mismatch",
			}, nil
		}
	}
	if oid != "" && !oidPattern.MatchString(oid) {
		return connector.ActionResponse{
			ActionID:   req.ActionID,
			Status:     "failed",
			Submitted:  connector.BoolPtr(false),
			ReasonCode: "invalid_target_oid",
		}, nil
	}
	rootID, parentID := "", ""
	if rpid := NormalizeReplyID(req.Target.ContentID); rpid != "" {
		parentID = rpid
		rootID = NormalizeReplyID(req.Target.RootID)
		if rootID == "" {
			rootID = rpid
		}
	}

	execCtx, cancel := context.WithTimeout(ctx, time.Duration(c.cfg.Ego.TimeoutSec)*time.Second)
	defer cancel()
	result, err := c.driver.PostReply(execCtx, ReplyRequest{
		Target:      target,
		OID:         oid,
		CommentType: commentType,
		RootID:      rootID,
		ParentID:    parentID,
		Text:        req.Text,
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
