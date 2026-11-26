package console

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

type RollbackInput struct {
	RequestID string   `json:"requestId"`
	ChangeID  string   `json:"changeId"`
	Base      Versions `json:"base"`
}

type ChangePage struct {
	Items      []ChangeRecord `json:"items"`
	NextCursor *string        `json:"nextCursor"`
}

func protectedDigests(config snapshot) map[string]string {
	result := map[string]string{}
	for id, value := range protectedGroups(groups(config)) {
		result[id] = digest(value)
	}
	return result
}

func (facade *Facade) GetChange(id string) (Envelope, error) {
	facade.mu.Lock()
	defer facade.mu.Unlock()
	if saved := facade.plans[id]; saved != nil && saved.Record != nil {
		return envelope(copyJSON(*saved.Record), saved.Record.Operation.Revision, "console_change_journal", "partial"), nil
	}
	return Envelope{}, failure("not_found", "Change was not found in the console journal")
}

func (facade *Facade) QueryChanges(query url.Values) (Envelope, error) {
	for key, values := range query {
		if len(values) != 1 || !containsString([]string{"from", "to", "origin", "status", "component", "groupId", "query", "cursor", "limit"}, key) {
			return Envelope{}, failure("validation", "Unknown or repeated change query field")
		}
	}
	limit := 50
	if raw := query.Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 100 {
			return Envelope{}, failure("validation", "Change limit must be 1..100")
		}
		limit = value
	}
	var from, to *time.Time
	for key, target := range map[string]**time.Time{"from": &from, "to": &to} {
		if raw := query.Get(key); raw != "" {
			parsed, err := time.Parse(time.RFC3339Nano, raw)
			if err != nil {
				return Envelope{}, failure("validation", "Change time filters require RFC3339")
			}
			*target = &parsed
		}
	}
	if from != nil && to != nil && !from.Before(*to) {
		return Envelope{}, failure("validation", "Change from must precede to")
	}
	for key, allowed := range map[string][]string{"origin": {"console", "external", "unknown"}, "status": {"pending", "unknown", "partial_apply", "succeeded", "failed"}, "component": {"dispatcher", "relay"}} {
		if value := query.Get(key); value != "" && !containsString(allowed, value) {
			return Envelope{}, failure("validation", "Invalid change filter")
		}
	}
	if len(query.Get("query")) > 256 || len(query.Get("groupId")) > 256 {
		return Envelope{}, failure("validation", "Change filter is too long")
	}
	items := []ChangeRecord{}
	for _, record := range facade.ListChanges() {
		if (from != nil && record.CreatedAt.Before(*from)) || (to != nil && !record.CreatedAt.Before(*to)) || (query.Get("origin") != "" && record.Origin != query.Get("origin")) || (query.Get("status") != "" && record.Operation.Status != query.Get("status")) {
			continue
		}
		componentMatch, groupMatch := query.Get("component") == "", query.Get("groupId") == ""
		for _, change := range record.Changes {
			componentMatch = componentMatch || change.Component == query.Get("component")
			groupMatch = groupMatch || strings.Contains(change.Path, "group_rules."+query.Get("groupId"))
		}
		if !groupMatch {
			facade.mu.Lock()
			if saved := facade.plans[record.ID]; saved != nil {
				groupMatch = containsString(saved.Plan.AffectedGroups, query.Get("groupId"))
			}
			facade.mu.Unlock()
		}
		if componentMatch && groupMatch && strings.Contains(strings.ToLower(record.ID+" "+record.Operation.RequestID), strings.ToLower(query.Get("query"))) {
			items = append(items, record)
		}
	}
	sort.Slice(items, func(first, second int) bool {
		if items[first].CreatedAt.Equal(items[second].CreatedAt) {
			return items[first].ID < items[second].ID
		}
		return items[first].CreatedAt.After(items[second].CreatedAt)
	})
	filters := copyJSON(query)
	filters.Del("cursor")
	binding := digest([]any{filters, items})
	offset := 0
	if raw := query.Get("cursor"); raw != "" {
		var cursor runCursor
		data, err := base64.RawURLEncoding.DecodeString(raw)
		if len(raw) > 2048 || err != nil || json.Unmarshal(data, &cursor) != nil || cursor.Binding != binding || cursor.Offset < 0 || cursor.Offset >= len(items) {
			return Envelope{}, failure("reset_required", "Change list changed or cursor filters differ; read from the first page")
		}
		offset = cursor.Offset
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	page := ChangePage{Items: items[offset:end]}
	if end < len(items) {
		data, _ := json.Marshal(runCursor{Offset: end, Binding: binding})
		cursor := base64.RawURLEncoding.EncodeToString(data)
		page.NextCursor = &cursor
	}
	return envelope(page, binding, "console_change_journal", "partial"), nil
}

func (facade *Facade) PlanRollback(ctx context.Context, input RollbackInput) (ChangePlan, error) {
	facade.mu.Lock()
	defer facade.mu.Unlock()
	if facade.lock == nil {
		return ChangePlan{}, failure("unavailable", "Console journal is closed")
	}
	if !validRequestID(input.RequestID) {
		return ChangePlan{}, failure("validation", "Request ID is required")
	}
	for _, saved := range facade.plans {
		if saved.RequestID == input.RequestID {
			if saved.InputHash == digest(input) {
				return saved.Plan, nil
			}
			return ChangePlan{}, failure("conflict", "Request ID was already used")
		}
	}
	previous := facade.plans[input.ChangeID]
	if previous == nil || previous.Record == nil {
		return ChangePlan{}, failure("not_found", "Rollback change was not found")
	}
	if previous.Before == nil || previous.Before.Dispatcher == nil {
		return ChangePlan{}, failure("unsupported", "This legacy change did not retain a complete allowlisted snapshot")
	}
	before, err := facade.backend.config(ctx)
	if err != nil {
		return ChangePlan{}, err
	}
	base := versions(before)
	var relayBefore *relaySnapshot
	if previous.RelayPatch != nil {
		relay, err := facade.backend.relayConfig(ctx)
		if err != nil {
			return ChangePlan{}, err
		}
		relayBefore, base.Relay = &relay, relay.digest
	}
	if !sameVersions(input.Base, base) {
		return ChangePlan{}, failure("conflict", "Configuration changed; read before planning rollback")
	}
	after := cloneConfig(before)
	targetRules, ok := previous.Before.Dispatcher["rules"].(map[string]any)
	if !ok {
		return ChangePlan{}, failure("invalid", "Stored rollback snapshot rules are invalid")
	}
	patch := copyJSON(targetRules)
	bridgeMap, ok := previous.Before.Dispatcher["bridge"].(map[string]any)
	if !ok {
		return ChangePlan{}, failure("invalid", "Stored rollback snapshot bridge configuration is invalid")
	}
	patch["dry_run"] = bridgeMap["dry_run"]
	if targetGroupRules, ok := targetRules["group_rules"].(map[string]any); ok {
		for id := range targetGroupRules {
			if _, exists := groups(before)[id]; !exists && previous.BeforeProtected[id] != "" && previous.BeforeProtected[id] != digest(map[string]any{}) {
				return ChangePlan{}, failure("unsupported", "Rollback cannot reconstruct removed private or unknown group fields from an allowlisted snapshot")
			}
		}
	}
	patch = restoreProtected(patch, before)
	afterBridge, ok := after.root["bridge"].(map[string]any)
	if !ok {
		return ChangePlan{}, failure("invalid", "Current dispatcher bridge configuration is invalid")
	}
	for name, value := range patch {
		if name == "dry_run" {
			afterBridge[name] = value
		} else {
			after.rules[name] = value
		}
	}
	if err := validateConfig(after); err != nil {
		return ChangePlan{}, err
	}
	if _, err := canonicalGroupRules(groups(after)); err != nil {
		return ChangePlan{}, err
	}
	changes := diff(before, after, patch)
	var relayPatch *RelayRules
	if relayBefore != nil && previous.Before.Relay != nil && !reflect.DeepEqual(relayBefore.rules, *previous.Before.Relay) {
		relayPatch = copyJSON(previous.Before.Relay)
		changes = append(changes, relayDiff(relayBefore.rules, *relayPatch)...)
	}
	if len(changes) == 0 {
		return ChangePlan{}, failure("validation", "Rollback snapshot already matches the current allowed configuration")
	}
	order := []string{}
	if len(diff(before, after, patch)) > 0 {
		order = append(order, "dispatcher")
	} else {
		patch = map[string]any{}
	}
	if relayPatch != nil {
		order = append(order, "relay")
	}
	affected := []string{}
	for id := range groups(before) {
		affected = append(affected, id)
	}
	for id := range groups(after) {
		if _, exists := groups(before)[id]; !exists {
			affected = append(affected, id)
		}
	}
	sort.Strings(affected)
	plan := ChangePlan{ID: newID(), Token: newID(), ExpiresAt: time.Now().UTC().Add(5 * time.Minute), Base: base, Changes: changes, AffectedGroups: affected, Effect: []string{"hot_reload"}, ComponentOrder: order, Warnings: []Warning{{Code: "rollback_new_publish", Message: "Rollback publishes the complete previous allowlisted policy against the current base. Delivered messages, cleared queues, sessions and other side effects are not reversed"}, {Code: "config_hash_changed", Message: "Changing policy can invalidate in-flight output"}}}
	if relayPatch != nil && !relayBefore.cas && facade.relayWriter == nil {
		plan.Warnings = append(plan.Warnings, relayWarnings()...)
	}
	if relayPatch != nil {
		plan.Warnings = append(plan.Warnings, Warning{Code: "cross_component_partial", Message: "Components publish and read back in order; failure stops later steps without automatic compensation"})
	}
	target := captureConfiguration(after, relayBefore)
	target.Source = "console_intended_policy"
	if relayPatch != nil {
		target.Relay = copyJSON(relayPatch)
	}
	target.Versions.Dispatcher, target.Versions.Fingerprint, target.Versions.Relay = "", "", ""
	facade.plans[plan.ID] = &savedPlan{Plan: plan, RequestID: input.RequestID, InputHash: digest(input), Patch: safePatch(patch), Before: captureConfiguration(before, relayBefore), Target: target, RelayPatch: relayPatch, ProtectedHash: digest(protectedGroups(groups(after))), BeforeProtected: protectedDigests(before), RollbackOf: &input.ChangeID}
	if relayBefore != nil {
		facade.plans[plan.ID].RelayCAS = relayBefore.cas
	}
	if err := facade.persist(); err != nil {
		delete(facade.plans, plan.ID)
		return ChangePlan{}, failure("internal", "Cannot persist rollback plan")
	}
	return plan, nil
}
