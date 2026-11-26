package console

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"jinx/internal/contracts"
)

type SchemaEntry struct {
	Type      string  `json:"type"`
	Nullable  bool    `json:"nullable"`
	Minimum   float64 `json:"minimum"`
	Maximum   float64 `json:"maximum"`
	MaxRunes  int     `json:"maxRunes"`
	Editable  bool    `json:"editable"`
	Inherited bool    `json:"inherited"`
	Default   any     `json:"default"`
}

func groupSchema() map[string]SchemaEntry {
	schema := map[string]SchemaEntry{}
	for _, field := range []string{"enabled", "dry_run", "mention_only", "promo_shadow", "support_mode"} {
		schema[field] = SchemaEntry{Type: "boolean", Editable: field != "support_mode", Default: false}
	}
	for _, field := range []string{"target_group_id", "target", "hermes_profile", "decision_context", "sensitive_criteria", "noise_criteria", "safe_criteria", "scenario", "response_goal", "tone"} {
		limit := 2000
		if field == "decision_context" {
			limit = 4000
		} else if field == "scenario" {
			limit = 64
		} else if field == "response_goal" || field == "tone" {
			limit = 200
		}
		schema[field] = SchemaEntry{Type: "string", MaxRunes: limit, Editable: field != "hermes_profile", Default: ""}
	}
	for _, field := range []string{"self_sender_ids", "blocked_sender_ids", "keyword_blocklist", "regex_blocklist", "promo_keywords", "skills", "tools", "knowledge_bases"} {
		schema[field] = SchemaEntry{Type: "string_list", Nullable: true, Editable: true, Default: []string{}}
	}
	schema["self_sender_ids"] = SchemaEntry{Type: "string_list", Nullable: true, Editable: true, Inherited: true}
	schema["allowed_types"] = SchemaEntry{Type: "number_list", Nullable: true, Editable: true, Default: []int{1, 49, 81}}
	for field, bounds := range map[string][3]float64{
		"batch_size": {1, 50, 0}, "idle_timeout_sec": {1, 300, 0}, "max_batch_wait_sec": {1, 300, 0},
		"max_reply_runes": {1, 2000, 500}, "reply_window_sec": {1, 3600, 45},
		"max_replies_in_window": {1, 20, 2}, "promo_daily_cap": {1, 20, 1},
		"support_since_ms": {0, 9007199254740991, 0},
	} {
		schema[field] = SchemaEntry{Type: "integer", Minimum: bounds[0], Maximum: bounds[1], Editable: field != "support_since_ms", Default: bounds[2], Inherited: bounds[2] == 0 && field != "support_since_ms"}
	}
	schema["decision_threshold"] = SchemaEntry{Type: "decimal", Minimum: .5, Maximum: 1, Editable: true, Default: .5}
	return schema
}

func globalSchema() map[string]SchemaEntry {
	schema := map[string]SchemaEntry{}
	for _, field := range []string{"laya_filter", "bypass_decision_on_mention", "ignore_xml", "dry_run"} {
		schema[field] = SchemaEntry{Type: "boolean", Editable: true, Default: false}
	}
	for _, field := range []string{"bot_names", "self_sender_ids", "keyword_blocklist"} {
		schema[field] = SchemaEntry{Type: "string_list", Nullable: true, Editable: true}
	}
	schema["forward_rules"] = SchemaEntry{Type: "forward_rule_list", Nullable: true, Editable: true, Default: []any{}}
	for field, bounds := range map[string][2]float64{
		"batch_size": {1, 50}, "idle_timeout_sec": {1, 300}, "max_batch_wait_sec": {1, 300},
		"max_message_age_sec": {1, 3600}, "session_max_turns": {0, 1000}, "session_max_age_sec": {0, 2592000},
	} {
		schema[field] = SchemaEntry{Type: "integer", Minimum: bounds[0], Maximum: bounds[1], Editable: true}
	}
	return schema
}

func validateValue(field string, value any, entry SchemaEntry, group bool) error {
	if value == nil && entry.Nullable {
		return nil
	}
	valid := false
	switch entry.Type {
	case "boolean":
		_, valid = value.(bool)
	case "string":
		text, ok := value.(string)
		valid = ok && utf8.RuneCountInString(text) <= entry.MaxRunes && !strings.ContainsRune(text, '\x00')
		if valid && group {
			switch field {
			case "scenario":
				valid = text == "" || mountIDPattern.MatchString(text)
			case "response_goal", "tone":
				valid = text == "" || !strings.ContainsFunc(text, unicode.IsControl)
			}
		}
	case "integer", "decimal":
		number, ok := value.(float64)
		valid = ok && number >= entry.Minimum && number <= entry.Maximum
		if group && number == 0 {
			valid = ok
		}
		if entry.Type == "integer" && number != float64(int64(number)) {
			valid = false
		}
	case "string_list", "number_list":
		list, ok := value.([]any)
		valid = ok && len(list) <= 1000
		isMountList := group && (field == "skills" || field == "tools" || field == "knowledge_bases")
		if isMountList && len(list) > 16 {
			valid = false
		}
		seen := map[string]bool{}
		for _, item := range list {
			if entry.Type == "string_list" {
				text, ok := item.(string)
				valid = valid && ok && utf8.RuneCountInString(text) <= 4000 && !strings.ContainsRune(text, '\x00')
				if valid && isMountList {
					trimmed := strings.TrimSpace(text)
					valid = trimmed != "" && trimmed == text && mountIDPattern.MatchString(text) && !seen[text]
					seen[text] = true
				}
			} else {
				number, ok := item.(float64)
				valid = valid && ok && number >= 0 && number <= 2147483647 && number == float64(int64(number))
			}
		}
	case "forward_rule_list":
		if _, err := canonicalForwardRules(value); err != nil {
			valid = false
		} else {
			valid = true
		}
	}
	if !valid {
		return invalidField(field)
	}
	return nil
}

func canonicalForwardRules(value any) ([]map[string]any, error) {
	if value == nil {
		return []map[string]any{}, nil
	}
	var rawList []any
	switch typed := value.(type) {
	case []any:
		rawList = typed
	case []map[string]any:
		rawList = make([]any, len(typed))
		for i, item := range typed {
			rawList[i] = item
		}
	default:
		return nil, invalidField("forward_rules")
	}
	if len(rawList) > 64 {
		return nil, invalidField("forward_rules")
	}
	allowedKeys := map[string]bool{
		"id":                  true,
		"enabled":             true,
		"source_container_id": true,
		"target_container_id": true,
		"dry_run":             true,
		"include_echo":        true,
		"template":            true,
	}
	seenIDs := make(map[string]bool, len(rawList))
	out := make([]map[string]any, 0, len(rawList))
	for _, rawItem := range rawList {
		obj, ok := rawItem.(map[string]any)
		if !ok {
			return nil, invalidField("forward_rules")
		}
		for k := range obj {
			if !allowedKeys[k] {
				return nil, invalidField("forward_rules")
			}
		}
		idRaw, idOK := obj["id"].(string)
		id := strings.TrimSpace(idRaw)
		if !idOK || !mountIDPattern.MatchString(id) || seenIDs[id] {
			return nil, invalidField("forward_rules")
		}
		seenIDs[id] = true

		enabled, enOK := obj["enabled"].(bool)
		if !enOK {
			return nil, invalidField("forward_rules")
		}
		srcRaw, srcOK := obj["source_container_id"].(string)
		dstRaw, dstOK := obj["target_container_id"].(string)
		src := strings.TrimSpace(srcRaw)
		dst := strings.TrimSpace(dstRaw)
		if !srcOK || !dstOK || !contracts.ValidContainerID(src) || !contracts.ValidContainerID(dst) || src == dst {
			return nil, invalidField("forward_rules")
		}
		dryRun := false
		if rawDry, exists := obj["dry_run"]; exists {
			b, ok := rawDry.(bool)
			if !ok {
				return nil, invalidField("forward_rules")
			}
			dryRun = b
		}
		includeEcho := false
		if rawEcho, exists := obj["include_echo"]; exists {
			b, ok := rawEcho.(bool)
			if !ok {
				return nil, invalidField("forward_rules")
			}
			includeEcho = b
		}
		tmpl := ""
		if rawTmpl, exists := obj["template"]; exists {
			s, ok := rawTmpl.(string)
			if !ok || !utf8.ValidString(s) || strings.ContainsRune(s, '\x00') || utf8.RuneCountInString(s) > 1000 {
				return nil, invalidField("forward_rules")
			}
			tmpl = s
		}
		out = append(out, map[string]any{
			"id":                  id,
			"enabled":             enabled,
			"source_container_id": src,
			"target_container_id": dst,
			"dry_run":             dryRun,
			"include_echo":        includeEcho,
			"template":            tmpl,
		})
	}
	return out, nil
}

var (
	profilePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)
	mountIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)
)

func groups(config snapshot) map[string]any { return config.rules["group_rules"].(map[string]any) }

func includes(list any, id string) bool {
	items, _ := list.([]any)
	for _, item := range items {
		if item == id {
			return true
		}
	}
	return false
}

func fields(config snapshot, group map[string]any) map[string]FieldValue {
	result := map[string]FieldValue{}
	for name, entry := range groupSchema() {
		configured, exists := group[name]
		effective, origin := configured, "group"
		if !exists {
			effective = entry.Default
			origin = "default"
		}
		if entry.Inherited && (configured == nil || configured == float64(0)) {
			effective, origin = config.rules[name], "global"
		} else if (entry.Type == "integer" || entry.Type == "decimal") && configured == float64(0) {
			effective, origin = entry.Default, "default"
		}
		if name == "allowed_types" && configured == nil {
			effective, origin = entry.Default, "default"
		}
		if name == "dry_run" && config.root["bridge"].(map[string]any)["dry_run"] == true {
			effective, origin = true, "global"
		}
		editable, reason := entry.Editable, ""
		if (name == "self_sender_ids" || name == "allowed_types") && !exists {
			editable, reason = false, "Upstream omits this field; null versus empty override cannot be verified"
		}
		result[name] = FieldValue{Configured: configured, Effective: effective, Origin: origin, Editable: editable, Reason: reason}
	}
	return result
}

func validateConfig(config snapshot) error {
	for name, entry := range globalSchema() {
		value := config.rules[name]
		if name == "dry_run" {
			value = config.root["bridge"].(map[string]any)["dry_run"]
		}
		if err := validateValue(name, value, entry, false); err != nil {
			return err
		}
	}
	if config.rules["max_batch_wait_sec"].(float64) < config.rules["idle_timeout_sec"].(float64) {
		return invalidField("max_batch_wait_sec")
	}
	profiles := map[string]bool{}
	for id, raw := range groups(config) {
		group, ok := raw.(map[string]any)
		srcPlatform, _, validSrc := contracts.ParseContainerID(id)
		if !ok || !validSrc {
			return invalidField("group_rules")
		}
		for name, value := range group {
			if entry, known := groupSchema()[name]; known {
				if err := validateValue(name, value, entry, true); err != nil {
					return err
				}
			}
		}
		view := fields(config, group)
		if view["max_batch_wait_sec"].Effective.(float64) < view["idle_timeout_sec"].Effective.(float64) {
			return invalidField("max_batch_wait_sec")
		}
		target, _ := group["target"].(string)
		targetID, _ := group["target_group_id"].(string)
		if strings.TrimSpace(target) == "" || strings.ContainsAny(target, "\r\n\x00") {
			return invalidField("target")
		}
		if targetID != "" {
			dstPlatform, _, validDst := contracts.ParseContainerID(targetID)
			if !validDst || dstPlatform != srcPlatform {
				return invalidField("target_group_id")
			}
		}
		profile, _ := group["hermes_profile"].(string)
		baseProfile, _ := config.root["hermes"].(map[string]any)["profile"].(string)
		if profile == "" {
			profile = baseProfile + "-" + contracts.SanitizeProfileSuffix(id)
		}
		if !profilePattern.MatchString(profile) || profile == baseProfile || profiles[profile] {
			return invalidField("hermes_profile")
		}
		profiles[profile] = true
		patterns, _ := group["regex_blocklist"].([]any)
		for _, pattern := range patterns {
			if _, err := regexp.Compile(pattern.(string)); err != nil {
				return invalidField("regex_blocklist")
			}
		}
		keywords, _ := group["promo_keywords"].([]any)
		if len(keywords) > 200 {
			return invalidField("promo_keywords")
		}
		for _, keyword := range keywords {
			length := utf8.RuneCountInString(strings.TrimSpace(keyword.(string)))
			if length < 2 || length > 40 {
				return invalidField("promo_keywords")
			}
		}
	}
	whitelist, ok := config.rules["group_whitelist"].([]any)
	if config.rules["group_whitelist"] != nil && !ok {
		return invalidField("group_whitelist")
	}
	for _, id := range whitelist {
		if _, exists := groups(config)[fmt.Sprint(id)]; !exists {
			return invalidField("group_whitelist")
		}
	}
	if fwdRules, err := canonicalForwardRules(config.rules["forward_rules"]); err != nil {
		return err
	} else {
		for _, r := range fwdRules {
			targetID, _ := r["target_container_id"].(string)
			if _, exists := groups(config)[targetID]; !exists {
				return invalidField("forward_rules")
			}
		}
	}
	return nil
}

func applyEdits(config snapshot, edits []Edit) (map[string]any, error) {
	patch := map[string]any{}
	for _, edit := range edits {
		switch edit.Kind {
		case "field":
			var value any
			if len(edit.Value) == 0 || json.Unmarshal(edit.Value, &value) != nil {
				return nil, invalidField(edit.Field)
			}
			schema, target := globalSchema(), config.rules
			if edit.Scope.Kind == "group" {
				var ok bool
				target, ok = groups(config)[edit.Scope.GroupID].(map[string]any)
				if !ok {
					return nil, invalidField("groupId")
				}
				if target["support_mode"] == true {
					return nil, failure("unsupported", "Isolated support routes require a dedicated editor")
				}
				schema = groupSchema()
			} else if edit.Scope.Kind != "global" {
				return nil, invalidField("scope")
			}
			entry, known := schema[edit.Field]
			if !known || !entry.Editable {
				return nil, failure("unsupported", "Field is not safely editable")
			}
			if err := validateValue(edit.Field, value, entry, edit.Scope.Kind == "group"); err != nil {
				return nil, err
			}
			if edit.Scope.Kind == "global" && edit.Field == "dry_run" {
				config.root["bridge"].(map[string]any)["dry_run"] = value
				patch["dry_run"] = value
			} else {
				target[edit.Field] = value
				if edit.Scope.Kind == "group" {
					patch["group_rules"] = groups(config)
				} else {
					patch[edit.Field] = value
				}
			}
		case "group_admission", "create_group", "delete_group":
			if !contracts.ValidContainerID(edit.GroupID) {
				return nil, invalidField("groupId")
			}
			raw, exists := groups(config)[edit.GroupID]
			if edit.Kind == "create_group" {
				if exists {
					return nil, failure("validation", "Group already exists")
				}
				raw = map[string]any{"enabled": false, "target": edit.TargetLabel, "target_group_id": edit.TargetGroupID, "self_sender_ids": nil, "allowed_types": nil}
				groups(config)[edit.GroupID] = raw
			} else if !exists {
				return nil, invalidField("groupId")
			}
			if edit.Kind == "delete_group" {
				delete(groups(config), edit.GroupID)
			} else if edit.Kind == "group_admission" {
				raw.(map[string]any)["enabled"] = edit.Enabled
			}
			whitelist := []any{}
			previous, _ := config.rules["group_whitelist"].([]any)
			for _, id := range previous {
				if id != edit.GroupID {
					whitelist = append(whitelist, id)
				}
			}
			if edit.Kind == "group_admission" && edit.Enabled {
				whitelist = append(whitelist, edit.GroupID)
			}
			config.rules["group_whitelist"] = whitelist
			patch["group_whitelist"], patch["group_rules"] = whitelist, groups(config)
		default:
			return nil, failure("unsupported", "This edit has no verified runtime implementation")
		}
	}
	if patch["group_rules"] != nil {
		if _, err := canonicalGroupRules(patch["group_rules"]); err != nil {
			return nil, err
		}
	}
	return patch, validateConfig(config)
}

func diff(before, after snapshot, patch map[string]any) []FieldChange {
	changes := []FieldChange{}
	for key := range patch {
		if key == "group_rules" {
			ids := map[string]bool{}
			for id := range groups(before) {
				ids[id] = true
			}
			for id := range groups(after) {
				ids[id] = true
			}
			for id := range ids {
				oldGroup, _ := groups(before)[id].(map[string]any)
				newGroup, _ := groups(after)[id].(map[string]any)
				if oldGroup == nil || newGroup == nil {
					var oldSafe, newSafe any
					if oldGroup != nil {
						oldSafe = publicFields(before, oldGroup)
					}
					if newGroup != nil {
						newSafe = publicFields(after, newGroup)
					}
					changes = append(changes, FieldChange{Component: "dispatcher", Path: "group_rules." + id, Before: oldSafe, After: newSafe})
					continue
				}
				oldFields, newFields := fields(before, oldGroup), fields(after, newGroup)
				for field := range groupSchema() {
					if !reflect.DeepEqual(oldGroup[field], newGroup[field]) {
						changes = append(changes, FieldChange{Component: "dispatcher", Path: "group_rules." + id + "." + field, Before: oldGroup[field], After: newGroup[field], EffectiveBefore: oldFields[field].Effective, EffectiveAfter: newFields[field].Effective})
					}
				}
			}
			continue
		}
		oldValue, newValue := before.rules[key], after.rules[key]
		if key == "dry_run" {
			oldValue, newValue = before.root["bridge"].(map[string]any)[key], after.root["bridge"].(map[string]any)[key]
		}
		if key == "forward_rules" {
			if normOld, err := canonicalForwardRules(oldValue); err == nil {
				oldValue = normOld
			}
			if normNew, err := canonicalForwardRules(newValue); err == nil {
				newValue = normNew
			}
		}
		if !reflect.DeepEqual(oldValue, newValue) {
			changes = append(changes, FieldChange{Component: "dispatcher", Path: key, Before: oldValue, After: newValue, EffectiveBefore: oldValue, EffectiveAfter: newValue})
		}
	}
	changes = append(changes, inheritedDiff(before, after, patch)...)
	sort.Slice(changes, func(first, second int) bool { return changes[first].Path < changes[second].Path })
	return changes
}
