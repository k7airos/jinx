package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"time"
)

type ConfigSnapshot struct {
	ID           string                `json:"id"`
	Versions     Versions              `json:"versions"`
	CapturedAt   time.Time             `json:"capturedAt"`
	Group        GroupRef              `json:"group"`
	Fields       map[string]FieldValue `json:"fields"`
	GlobalFields map[string]FieldValue `json:"globalFields"`
	Routing      any                   `json:"routing"`
	Permissions  []any                 `json:"permissions"`
	Source       string                `json:"source"`
	Scope        []string              `json:"scope"`
}

func (facade *Facade) GetConfigSnapshot(ctx context.Context, id string) (Envelope, error) {
	if safeToken(id) == "" {
		return Envelope{}, failure("validation", "Configuration snapshot ID is invalid")
	}
	data, _, status, err := facade.backend.boundedRequest(ctx, facade.backend.client, maxTraceBody, "dispatcher", http.MethodGet, "/api/trace/config-snapshots/"+url.PathEscape(id), "", nil)
	if err != nil {
		return Envelope{}, err
	}
	if status == 404 || status == 405 || status == 410 || status == 501 {
		result := envelope(nil, id, "dispatcher_trace_snapshot_missing", "partial")
		result.MissingReason = nullable("Historical configuration snapshot was not retained, was removed, or this dispatcher does not implement the content endpoint")
		return result, nil
	}
	if status != 200 {
		return Envelope{}, failure("unavailable", "Historical configuration snapshot could not be read")
	}
	var stored struct {
		ID            string         `json:"id"`
		Policy        string         `json:"policy"`
		RecordedAt    time.Time      `json:"recordedAt"`
		SourceGroupID string         `json:"sourceGroupId"`
		Rules         map[string]any `json:"rules"`
	}
	if json.Unmarshal(data, &stored) != nil || stored.ID != id || stored.RecordedAt.IsZero() || !publicVersion.MatchString(stored.Policy) || stored.Rules == nil || safeToken(stored.SourceGroupID) == "" {
		return Envelope{}, failure("unavailable", "Historical configuration snapshot has an invalid shape")
	}
	raw := ConfigSnapshot{ID: id, CapturedAt: stored.RecordedAt, Versions: Versions{Policy: &stored.Policy}, Group: GroupRef{ID: stored.SourceGroupID, Visibility: "not_observed"}, Fields: map[string]FieldValue{}, GlobalFields: map[string]FieldValue{}, Permissions: []any{}, Source: "dispatcher_trace_config_snapshot", Scope: []string{"dispatcher.resolved_group_policy", "dispatcher.selected_global_policy"}}
	groupMapping := map[string]string{
		"targetGroupId": "target_group_id", "enabled": "enabled", "supportMode": "support_mode", "supportSinceMS": "support_since_ms",
		"dryRun": "dry_run", "mentionOnly": "mention_only", "allowedTypes": "allowed_types", "blockedSenderIds": "blocked_sender_ids",
		"keywordBlocklist": "keyword_blocklist", "regexBlocklist": "regex_blocklist", "batchSize": "batch_size", "idleTimeoutSec": "idle_timeout_sec",
		"maxBatchWaitSec": "max_batch_wait_sec", "maxReplyRunes": "max_reply_runes", "replyWindowSec": "reply_window_sec", "maxRepliesInWindow": "max_replies_in_window",
		"decisionContext": "decision_context", "sensitiveCriteria": "sensitive_criteria", "noiseCriteria": "noise_criteria", "safeCriteria": "safe_criteria",
		"decisionThreshold": "decision_threshold", "promoKeywords": "promo_keywords", "promoShadow": "promo_shadow", "promoDailyCap": "promo_daily_cap",
		"scenario": "scenario", "responseGoal": "response_goal", "tone": "tone",
		"skills": "skills", "tools": "tools", "knowledgeBases": "knowledge_bases",
	}
	globalMapping := map[string]string{"globalKeywordBlocklist": "keyword_blocklist", "ignoreXML": "ignore_xml", "maxMessageAgeSec": "max_message_age_sec", "decisionEnabled": "laya_filter", "bypassDecisionOnMention": "bypass_decision_on_mention"}
	for _, mapping := range []struct {
		names  map[string]string
		target map[string]FieldValue
		schema map[string]SchemaEntry
	}{{groupMapping, raw.Fields, groupSchema()}, {globalMapping, raw.GlobalFields, globalSchema()}} {
		for source, target := range mapping.names {
			value, exists := stored.Rules[source]
			if !exists {
				continue
			}
			if err := validateValue(target, value, mapping.schema[target], true); err != nil {
				return Envelope{}, failure("unavailable", "Historical configuration rule has an invalid value")
			}
			mapping.target[target] = FieldValue{Effective: value, Origin: "historical_resolved", Reason: "Only the effective historical value was retained; configured value and inheritance are unknown"}
		}
	}
	return envelope(raw, stored.Policy, "dispatcher_trace_config_snapshot", "partial"), nil
}
