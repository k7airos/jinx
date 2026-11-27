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

type PollOptions struct {
	SkipRelay bool
}

type PollReport struct {
	VideosDiscovered  int
	TargetsPolled     int
	CommentsFetched   int
	Forwarded         int
	RelayBlocked      int
	SkippedSeen       int
	SkippedHistorical int
	PushErrors        int
	FetchedComments   []Comment `json:"-"`
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
	staticTargets     map[string]bool
	selfSet           map[string]bool
	pollMu            sync.Mutex
	statusMu          sync.RWMutex
	resolvedOIDs      map[string]string
	lastSearchAt      time.Time
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
	staticTargets := make(map[string]bool, len(cfg.Targets))
	resolvedOIDs := make(map[string]string, len(cfg.Targets))
	for _, t := range cfg.Targets {
		targets[t.ContainerID] = t
		staticTargets[t.ContainerID] = true
		if t.OID != "" {
			resolvedOIDs[t.ContainerID] = t.OID
		}
	}
	selfSet := make(map[string]bool, len(cfg.SelfMIDs))
	for _, mid := range cfg.SelfMIDs {
		selfSet[mid] = true
	}
	c := &Connector{
		cfg:           cfg,
		driver:        driver,
		relay:         relayClient,
		cursors:       cursors,
		idem:          idem,
		targets:       targets,
		staticTargets: staticTargets,
		resolvedOIDs:  resolvedOIDs,
		selfSet:       selfSet,
		now:           time.Now,
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

func (c *Connector) SearchVideos(ctx context.Context, query SearchQuery) ([]VideoCandidate, error) {
	searcher, ok := c.driver.(VideoSearcher)
	if !ok {
		return nil, fmt.Errorf("bilibili driver does not support video search")
	}
	searchCtx, cancel := context.WithTimeout(ctx, time.Duration(c.cfg.Ego.TimeoutSec)*time.Second)
	defer cancel()
	return searcher.SearchVideos(searchCtx, query)
}

func (c *Connector) Status() connector.StatusSnapshot {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	targets := make([]connector.HealthTarget, 0, len(c.targets))
	for _, t := range c.targets {
		targets = append(targets, connector.HealthTarget{
			ContainerID: t.ContainerID,
			Label:       t.Label,
			Enabled:     t.Enabled,
		})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].ContainerID < targets[j].ContainerID })
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

func (c *Connector) snapshotEnabledTargets() []TargetConfig {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	out := make([]TargetConfig, 0, len(c.targets))
	for _, t := range c.targets {
		if t.Enabled {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ContainerID < out[j].ContainerID })
	return out
}

func (c *Connector) hasEnabledSearchTargets() bool {
	for _, st := range c.cfg.SearchTargets {
		if st.Enabled {
			return true
		}
	}
	return false
}

func (c *Connector) resolveSearchTargets(ctx context.Context) (int, error) {
	if !c.hasEnabledSearchTargets() {
		return 0, nil
	}
	now := c.now()
	c.statusMu.RLock()
	lastSearch := c.lastSearchAt
	c.statusMu.RUnlock()
	if !lastSearch.IsZero() && now.Sub(lastSearch) < time.Duration(c.cfg.SearchRefreshSec)*time.Second {
		return 0, nil
	}

	searcher, ok := c.driver.(VideoSearcher)
	if !ok {
		return 0, fmt.Errorf("search_targets configured but driver does not support video search")
	}

	discovered := 0
	var firstErr error
	for _, st := range c.cfg.SearchTargets {
		if !st.Enabled {
			continue
		}
		if ctx.Err() != nil {
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			break
		}
		limit := st.MaxVideos * 3
		if limit < 5 {
			limit = 5
		}
		if limit > 20 {
			limit = 20
		}
		searchCtx, cancel := context.WithTimeout(ctx, time.Duration(c.cfg.Ego.TimeoutSec)*time.Second)
		candidates, err := searcher.SearchVideos(searchCtx, SearchQuery{
			Keyword: st.Keyword,
			Order:   st.Order,
			Limit:   limit,
		})
		cancel()
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("search %q: %w", st.Keyword, err)
			}
			continue
		}

		addedForRule := 0
		c.statusMu.Lock()
		for _, cand := range candidates {
			if addedForRule >= st.MaxVideos {
				break
			}
			if cand.CommentCount < st.MinComments {
				continue
			}
			cid := string(contracts.PlatformBilibili) + ":" + cand.BVID
			if existing, exists := c.targets[cid]; exists {
				if cand.OID != "" && existing.OID == "" {
					existing.OID = cand.OID
					c.targets[cid] = existing
					c.resolvedOIDs[cid] = cand.OID
				}
				if existing.Enabled && !c.staticTargets[cid] {
					addedForRule++
				}
				continue
			}
			c.targets[cid] = TargetConfig{
				ContainerID:   cid,
				Label:         cand.Title,
				BVID:          cand.BVID,
				OID:           cand.OID,
				CommentType:   st.CommentType,
				URL:           cand.URL,
				Enabled:       true,
				PollLimit:     st.PollLimit,
				SortMode:      st.SortMode,
				SubReplyLimit: st.SubReplyLimit,
			}
			if cand.OID != "" {
				c.resolvedOIDs[cid] = cand.OID
			}
			addedForRule++
			discovered++
		}
		c.statusMu.Unlock()
	}

	c.statusMu.Lock()
	c.lastSearchAt = c.now()
	c.statusMu.Unlock()
	return discovered, firstErr
}

func (c *Connector) PollOnce(ctx context.Context) (PollReport, error) {
	return c.PollOnceWithOptions(ctx, PollOptions{})
}

func (c *Connector) PollOnceWithOptions(ctx context.Context, opts PollOptions) (PollReport, error) {
	c.pollMu.Lock()
	defer c.pollMu.Unlock()

	var report PollReport
	var firstErr error
	var errCode string
	cutoffMS := c.now().Add(-time.Duration(c.cfg.MaxInitialAgeSec) * time.Second).UnixMilli()

	discovered, searchErr := c.resolveSearchTargets(ctx)
	report.VideosDiscovered = discovered
	if searchErr != nil {
		firstErr = searchErr
		errCode = "search_failed"
	}

	targets := c.snapshotEnabledTargets()
	for _, target := range targets {
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
			report.FetchedComments = append(report.FetchedComments, comment)

			oid := comment.OID
			if oid == "" {
				oid = target.OID
			}
			if target.OID == "" && oidPattern.MatchString(oid) {
				c.statusMu.Lock()
				c.resolvedOIDs[target.ContainerID] = oid
				c.statusMu.Unlock()
			}
			if target.Label == "" && comment.VideoTitle != "" {
				target.Label = comment.VideoTitle
				c.statusMu.Lock()
				if cur, ok := c.targets[target.ContainerID]; ok && cur.Label == "" {
					cur.Label = comment.VideoTitle
					c.targets[target.ContainerID] = cur
				}
				c.statusMu.Unlock()
			}

			if opts.SkipRelay {
				continue
			}

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
			pushResp, err := c.relay.Push(ctx, payload)
			if err != nil {
				targetPushFailed = true
				report.PushErrors++
				if firstErr == nil {
					firstErr = fmt.Errorf("push %s/%s: %w", target.ContainerID, comment.RPID, err)
					errCode = "relay_push_failed"
				}
				continue
			}
			if !pushResp.Allowed {
				report.RelayBlocked++
				if pushResp.Action == "blocked_unmonitored" || pushResp.Action == "blocked_unexposed" {
					targetPushFailed = true
					continue
				}
				if err := c.cursors.MarkSeen(target.ContainerID, comment.RPID, payload.Timestamp); err != nil && firstErr == nil {
					firstErr = err
					errCode = "cursor_error"
				}
				continue
			}
			report.Forwarded++
			if err := c.cursors.MarkSeen(target.ContainerID, comment.RPID, payload.Timestamp); err != nil && firstErr == nil {
				firstErr = err
				errCode = "cursor_error"
			}
		}

		if !opts.SkipRelay && !seeded && !targetPushFailed {
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
	c.statusMu.RLock()
	target, ok := c.targets[req.Target.ContainerID]
	c.statusMu.RUnlock()
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
