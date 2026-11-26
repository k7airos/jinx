package console

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"regexp"
	"sort"
	"strings"

	"jinx/internal/contracts"
)

type RelayGroupControl struct {
	MonitorAll              bool              `json:"monitor_all"`
	MonitoredGroups         []string          `json:"monitored_groups"`
	ExposedGroups           []string          `json:"exposed_groups"`
	GroupAliases            map[string]string `json:"group_aliases"`
	DropUnexposedCompletely bool              `json:"drop_unexposed_completely"`
}

type RelayFilters struct {
	KeywordBlocklist []string `json:"keyword_blocklist"`
	RegexBlocklist   []string `json:"regex_blocklist"`
	SenderBlocklist  []string `json:"sender_blocklist"`
	AllowedTypes     []int    `json:"allowed_types"`
	CleanContent     bool     `json:"clean_content"`
	IgnoreEcho       bool     `json:"ignore_echo"`
}

type RelayRules struct {
	GroupControl RelayGroupControl `json:"group_control"`
	Filters      RelayFilters      `json:"filters"`
}

type RelayRulesView struct {
	Rules            RelayRules `json:"rules"`
	Versions         Versions   `json:"versions"`
	WriteConcurrency string     `json:"writeConcurrency"`
	Warnings         []Warning  `json:"warnings"`
}

type RelayConfigWriter interface {
	CompareAndSwapRelay(context.Context, string, RelayRules) (RelayWriteReceipt, error)
}

type RelayWriteReceipt struct {
	StatusCode int
}

type relaySnapshot struct {
	root   map[string]any
	rules  RelayRules
	digest string
	cas    bool
}

func decodeRelayRules(root map[string]any) (RelayRules, error) {
	var rules RelayRules
	group, groupOK := root["group_control"].(map[string]any)
	filters, filtersOK := root["filters"].(map[string]any)
	if !groupOK || !filtersOK {
		return rules, failure("unavailable", "Relay rules sections are unavailable")
	}
	for _, section := range []struct {
		raw    map[string]any
		schema map[string]string
	}{{group, relaySchema("relay_group_control")}, {filters, relaySchema("relay_filters")}} {
		for field, kind := range section.schema {
			value, exists := section.raw[field]
			if exists && value == nil && kind != "boolean" {
				continue
			}
			if kind == "groups" {
				kind = "string_list"
			}
			if !exists || validateRelayValue(field, kind, value) != nil {
				return rules, failure("unavailable", "Relay rule fields cannot be read losslessly")
			}
		}
	}
	data, _ := json.Marshal(root)
	if json.Unmarshal(data, &rules) != nil {
		return rules, failure("unavailable", "Relay rules have an invalid shape")
	}
	return rules, nil
}

func (backend *upstream) relayConfig(ctx context.Context) (relaySnapshot, error) {
	data, etag, status, err := backend.request(ctx, "relay", http.MethodGet, "/api/v1/console-config", "", nil)
	if err != nil {
		return relaySnapshot{}, err
	}
	cas := status == 200
	if status == 404 || status == 405 || status == 501 {
		data, etag, status, err = backend.request(ctx, "relay", http.MethodGet, "/api/v1/config", "", nil)
	}
	if err != nil {
		return relaySnapshot{}, err
	}
	if status == 404 || status == 405 || status == 501 {
		return relaySnapshot{}, failure("unsupported", "Relay configuration API is unavailable")
	}
	var root map[string]any
	if status != 200 || json.Unmarshal(data, &root) != nil || root == nil {
		return relaySnapshot{}, failure("unavailable", "Relay configuration could not be read")
	}
	rules, err := decodeRelayRules(root)
	if err != nil {
		return relaySnapshot{}, err
	}
	revision := digest(root)
	if cas {
		value, ok := root["revision"].(string)
		if !ok || safeToken(value) == "" || etag != `"`+value+`"` {
			return relaySnapshot{}, failure("unavailable", "Relay CAS revision and ETag are invalid")
		}
		revision = value
	}
	return relaySnapshot{root: root, rules: rules, digest: revision, cas: cas}, nil
}

func relaySchema(scope string) map[string]string {
	switch scope {
	case "relay_group_control":
		return map[string]string{"monitor_all": "boolean", "monitored_groups": "groups", "exposed_groups": "groups", "group_aliases": "aliases", "drop_unexposed_completely": "boolean"}
	case "relay_filters":
		return map[string]string{"keyword_blocklist": "string_list", "regex_blocklist": "regex", "sender_blocklist": "string_list", "allowed_types": "number_list", "clean_content": "boolean", "ignore_echo": "boolean"}
	}
	return nil
}

func validateRelayValue(field, kind string, value any) error {
	switch kind {
	case "groups", "regex", "string_list", "number_list":
		valueKind := "string_list"
		if kind == "number_list" {
			valueKind = kind
		}
		if err := validateValue(field, value, SchemaEntry{Type: valueKind}, false); err != nil {
			return err
		}
		for _, raw := range value.([]any) {
			if kind == "number_list" && raw.(float64) < 1 {
				return invalidField(field)
			}
			if kind == "string_list" && (strings.TrimSpace(raw.(string)) == "" || len(raw.(string)) > 1024) {
				return invalidField(field)
			}
			if kind == "groups" && !contracts.ValidContainerID(raw.(string)) {
				return invalidField(field)
			}
			if kind == "regex" {
				if _, err := regexp.Compile(raw.(string)); err != nil {
					return invalidField(field)
				}
			}
		}
	case "aliases":
		aliases, ok := value.(map[string]any)
		if !ok || len(aliases) > 1000 {
			return invalidField(field)
		}
		for id, raw := range aliases {
			text, ok := raw.(string)
			if !contracts.ValidContainerID(id) || !ok || strings.TrimSpace(text) == "" || len(text) > 256 || strings.ContainsAny(text, "\x00\r\n") {
				return invalidField(field)
			}
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return invalidField(field)
		}
	default:
		return invalidField(field)
	}
	return nil
}

func relayWarnings() []Warning {
	return []Warning{{Code: "relay_no_remote_cas", Message: "Relay POST replaces the complete configuration without remote CAS. Console serializes writes and rejects observed digest drift; another writer can still race between the final read and POST"}}
}

func (facade *Facade) GetRelayRules(ctx context.Context) (Envelope, error) {
	config, err := facade.backend.config(ctx)
	if err != nil {
		return Envelope{}, err
	}
	relay, err := facade.backend.relayConfig(ctx)
	if err != nil {
		return Envelope{}, err
	}
	base := versions(config)
	base.Relay = relay.digest
	view := RelayRulesView{Rules: relay.rules, Versions: base, WriteConcurrency: "digest_preflight_local_mutex", Warnings: relayWarnings()}
	if relay.cas {
		view.WriteConcurrency, view.Warnings = "remote_cas", []Warning{}
	} else if facade.relayWriter != nil {
		view.WriteConcurrency, view.Warnings = "provider_remote_cas", []Warning{}
	}
	return envelope(view, relay.digest, "relay_memory", "partial"), nil
}

func applyRelayEdits(before relaySnapshot, edits []Edit) (RelayRules, error) {
	data, _ := json.Marshal(before.rules)
	var root map[string]any
	_ = json.Unmarshal(data, &root)
	for _, edit := range edits {
		if edit.Kind == "group_collection" {
			if !contracts.ValidContainerID(edit.GroupID) {
				return RelayRules{}, invalidField("groupId")
			}
			control := root["group_control"].(map[string]any)
			if control["monitor_all"] == true {
				return RelayRules{}, failure("validation", "Group collection requires explicit monitoring scope: preview monitor_all=false with the intended monitored_groups first")
			}
			for _, field := range []string{"monitored_groups", "exposed_groups"} {
				list := []any{}
				previous, _ := control[field].([]any)
				for _, id := range previous {
					if id != edit.GroupID {
						list = append(list, id)
					}
				}
				if edit.Enabled {
					list = append(list, edit.GroupID)
				}
				control[field] = list
			}
			continue
		}
		if edit.Kind != "relay_field" {
			continue
		}
		kind := relaySchema(edit.Scope.Kind)[edit.Field]
		var value any
		if json.Unmarshal(edit.Value, &value) != nil || validateRelayValue(edit.Field, kind, value) != nil {
			return RelayRules{}, invalidField(edit.Field)
		}
		section := "filters"
		if edit.Scope.Kind == "relay_group_control" {
			section = "group_control"
		}
		root[section].(map[string]any)[edit.Field] = value
	}
	target, err := decodeRelayRules(root)
	if err != nil {
		return RelayRules{}, err
	}
	for _, edit := range edits {
		if edit.Kind != "group_collection" {
			continue
		}
		if target.GroupControl.MonitorAll {
			return RelayRules{}, failure("validation", "Group collection requires an explicit final monitoring scope")
		}
		for _, list := range [][]string{target.GroupControl.MonitoredGroups, target.GroupControl.ExposedGroups} {
			included := false
			for _, id := range list {
				if !contracts.ValidContainerID(id) {
					return RelayRules{}, failure("validation", "Replace wildcard or name-based collection scopes with explicit group IDs before changing collection")
				}
				included = included || id == edit.GroupID
			}
			if included != edit.Enabled {
				return RelayRules{}, failure("validation", "Final Relay collection fields contradict the collection edit")
			}
		}
	}
	return target, nil
}

func relayDiff(before, after RelayRules) []FieldChange {
	changes := []FieldChange{}
	var oldRoot, newRoot map[string]map[string]any
	oldData, _ := json.Marshal(before)
	newData, _ := json.Marshal(after)
	_ = json.Unmarshal(oldData, &oldRoot)
	_ = json.Unmarshal(newData, &newRoot)
	for section, fields := range oldRoot {
		for field, oldValue := range fields {
			newValue := newRoot[section][field]
			if !reflect.DeepEqual(oldValue, newValue) {
				changes = append(changes, FieldChange{Component: "relay", Path: section + "." + field, Before: oldValue, After: newValue, EffectiveBefore: oldValue, EffectiveAfter: newValue})
			}
		}
	}
	sort.Slice(changes, func(first, second int) bool { return changes[first].Path < changes[second].Path })
	return changes
}

func (facade *Facade) writeRelay(ctx context.Context, saved *savedPlan) (int, error) {
	current, err := facade.backend.relayConfig(ctx)
	if err != nil {
		return 0, err
	}
	if current.digest != saved.Plan.Base.Relay {
		return http.StatusConflict, failure("conflict", "Relay configuration drifted before its publication step")
	}
	if current.cas != saved.RelayCAS {
		return http.StatusConflict, failure("conflict", "Relay configuration API changed since preview")
	}
	if current.cas {
		body, _ := json.Marshal(saved.RelayPatch)
		_, _, status, err := facade.backend.request(ctx, "relay", http.MethodPut, "/api/v1/console-config", `"`+current.digest+`"`, body)
		return status, err
	}
	if facade.relayWriter != nil {
		receipt, err := facade.relayWriter.CompareAndSwapRelay(ctx, current.digest, *saved.RelayPatch)
		return receipt.StatusCode, err
	}
	data, _ := json.Marshal(saved.RelayPatch)
	var allowed map[string]any
	_ = json.Unmarshal(data, &allowed)
	for section, value := range allowed {
		target := current.root[section].(map[string]any)
		for field, raw := range value.(map[string]any) {
			target[field] = raw
		}
	}
	body, _ := json.Marshal(current.root)
	_, _, status, err := facade.backend.request(ctx, "relay", http.MethodPost, "/api/v1/config", "", body)
	return status, err
}
