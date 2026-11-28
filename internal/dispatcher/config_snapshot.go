package dispatcher

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

type ConfigSnapshot struct {
	ID            string         `json:"id"`
	Policy        string         `json:"policy"`
	RecordedAt    time.Time      `json:"recordedAt"`
	SourceGroupID string         `json:"sourceGroupId"`
	Rules         map[string]any `json:"rules"`
}

func cloneSnapshot(snapshot ConfigSnapshot) ConfigSnapshot {
	data, _ := json.Marshal(snapshot)
	var cloned ConfigSnapshot
	_ = json.Unmarshal(data, &cloned)
	return cloned
}

const maxRetainedConfigSnapshots = 512

func (d *Dispatcher) retainConfigSnapshotLocked(b batch, now time.Time) (string, error) {
	rules := map[string]any{
		"targetGroupId": b.Rule.TargetGroupID, "enabled": b.Rule.Enabled,
		"supportMode": b.Rule.SupportMode, "supportSinceMS": b.Rule.SupportSinceMS,
		"dryRun": b.Config.Bridge.DryRun || b.Rule.DryRun || d.endpointDryRun(b.Config, b.Rule.TargetGroupID), "mentionOnly": b.Rule.MentionOnly,
		"allowedTypes": b.Rule.AllowedTypes, "blockedSenderIds": b.Rule.BlockedSenderIDs,
		"keywordBlocklist": b.Rule.KeywordBlocklist, "globalKeywordBlocklist": b.Config.Rules.KeywordBlocklist,
		"regexBlocklist": b.Rule.RegexBlocklist, "ignoreXML": b.Config.Rules.IgnoreXML,
		"batchSize": b.Rule.BatchSize, "idleTimeoutSec": b.Rule.IdleTimeoutSec,
		"maxBatchWaitSec": b.Rule.MaxBatchWaitSec, "maxMessageAgeSec": b.Config.Rules.MaxMessageAgeSec,
		"maxReplyRunes": b.Rule.MaxReplyRunes, "replyWindowSec": b.Rule.ReplyWindowSec,
		"maxRepliesInWindow": b.Rule.MaxRepliesInWindow,
		"decisionContext":    b.Rule.DecisionContext, "sensitiveCriteria": b.Rule.SensitiveCriteria,
		"noiseCriteria": b.Rule.NoiseCriteria, "safeCriteria": b.Rule.SafeCriteria,
		"decisionThreshold": b.Rule.DecisionThreshold, "decisionEnabled": b.Config.Rules.LayaFilter,
		"bypassDecisionOnMention": b.Config.Rules.BypassDecisionOnMention,
		"promoKeywords":           b.Rule.PromoKeywords, "promoShadow": b.Rule.PromoShadow,
		"promoDailyCap": b.Rule.PromoDailyCap,
		"scenario":      b.Rule.Scenario, "responseGoal": b.Rule.ResponseGoal, "tone": b.Rule.Tone,
		"skills":        b.Rule.Skills, "tools": b.Rule.Tools, "knowledgeBases": b.Rule.KnowledgeBases,
	}
	data, err := json.Marshal(rules)
	if err != nil {
		return "", err
	}
	id := "config-" + messageDigest(b.Policy+":"+b.GroupID+":"+string(data))
	if d.state.ConfigSnapshots == nil {
		d.state.ConfigSnapshots = make(map[string]ConfigSnapshot)
	}
	if _, exists := d.state.ConfigSnapshots[id]; !exists {
		for len(d.state.ConfigSnapshots) >= maxRetainedConfigSnapshots {
			oldestID := ""
			var oldestAt time.Time
			for key, snap := range d.state.ConfigSnapshots {
				if oldestID == "" || snap.RecordedAt.Before(oldestAt) || (snap.RecordedAt.Equal(oldestAt) && key < oldestID) {
					oldestID, oldestAt = key, snap.RecordedAt
				}
			}
			if oldestID == "" {
				break
			}
			delete(d.state.ConfigSnapshots, oldestID)
		}
		d.state.ConfigSnapshots[id] = cloneSnapshot(ConfigSnapshot{
			ID: id, Policy: b.Policy, RecordedAt: now, SourceGroupID: b.GroupID, Rules: rules,
		})
		if err := d.saveStateLocked(); err != nil {
			return "", d.traceFailure()
		}
	}
	return id, nil
}

func (d *Dispatcher) handleConfigSnapshot(w http.ResponseWriter, r *http.Request) {
	if !d.traceReadAllowed(w, r) {
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/trace/config-snapshots/")
	d.mu.Lock()
	snapshot, exists := d.state.ConfigSnapshots[id]
	d.mu.Unlock()
	if !exists {
		traceJSON(w, http.StatusNotFound, map[string]string{"code": "not_found"})
		return
	}
	traceJSON(w, http.StatusOK, snapshot)
}
