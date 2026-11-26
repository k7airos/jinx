package console

import (
	"context"
	"encoding/json"
	"reflect"
	"time"
)

type ConfigurationReader interface {
	ReadConfiguration(context.Context, string) (ConfigurationObservation, error)
}

type ConfigurationObservation struct {
	Memory        map[string]any
	Disk          map[string]any
	MemoryVersion string
	DiskVersion   string
}

type ConfigurationSnapshot struct {
	CapturedAt time.Time      `json:"capturedAt"`
	Versions   Versions       `json:"versions"`
	Source     string         `json:"source"`
	Scope      []string       `json:"scope"`
	Dispatcher map[string]any `json:"dispatcher"`
	Relay      *RelayRules    `json:"relay"`
}

type ConfigurationDrift struct {
	Component     string  `json:"component"`
	Available     bool    `json:"available"`
	Reason        string  `json:"reason"`
	Drift         *bool   `json:"drift"`
	MemoryVersion *string `json:"memoryVersion"`
	DiskVersion   *string `json:"diskVersion"`
	Memory        any     `json:"memory"`
	Disk          any     `json:"disk"`
}

func copyJSON[T any](value T) T {
	data, _ := json.Marshal(value)
	var result T
	_ = json.Unmarshal(data, &result)
	return result
}

func dispatcherAllowed(config snapshot) map[string]any {
	rules := map[string]any{}
	for name := range globalSchema() {
		if name == "dry_run" {
			continue
		}
		if name == "forward_rules" {
			if norm, err := canonicalForwardRules(config.rules[name]); err == nil {
				rules[name] = norm
			} else if value, exists := config.rules[name]; exists {
				rules[name] = value
			} else {
				rules[name] = []any{}
			}
			continue
		}
		if value, exists := config.rules[name]; exists {
			rules[name] = value
		}
	}
	for _, name := range []string{"group_whitelist", "group_profiles", "group_other_bots", "default_profile"} {
		if value, exists := config.rules[name]; exists {
			rules[name] = value
		}
	}
	rules["group_rules"] = allowedGroups(groups(config))
	return copyJSON(map[string]any{"rules": rules, "bridge": map[string]any{"dry_run": config.root["bridge"].(map[string]any)["dry_run"]}})
}

func allowedGroups(groupMap map[string]any) map[string]any {
	result := map[string]any{}
	for id, raw := range groupMap {
		group, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		fields := map[string]any{}
		for name := range groupSchema() {
			if name != "hermes_profile" {
				if value, exists := group[name]; exists {
					fields[name] = value
				}
			}
		}
		result[id] = fields
	}
	return result
}

func protectedGroups(groupMap map[string]any) map[string]any {
	result := map[string]any{}
	for id, raw := range groupMap {
		group, _ := raw.(map[string]any)
		fields := map[string]any{}
		for name, value := range group {
			if _, known := groupSchema()[name]; !known || name == "hermes_profile" {
				if name == "hermes_profile" && value == "" {
					continue
				}
				fields[name] = value
			}
		}
		result[id] = fields
	}
	return result
}

func safePatch(patch map[string]any) map[string]any {
	result := copyJSON(patch)
	if raw, ok := result["group_rules"].(map[string]any); ok {
		result["group_rules"] = allowedGroups(raw)
	}
	return result
}

func restoreProtected(patch map[string]any, current snapshot) map[string]any {
	result := copyJSON(patch)
	if raw, ok := result["group_rules"].(map[string]any); ok {
		protected := protectedGroups(groups(current))
		for id, value := range raw {
			group := value.(map[string]any)
			if fields, ok := protected[id].(map[string]any); ok {
				for name, value := range fields {
					group[name] = value
				}
			}
		}
	}
	return result
}

func captureConfiguration(config snapshot, relay *relaySnapshot) *ConfigurationSnapshot {
	result := &ConfigurationSnapshot{CapturedAt: time.Now().UTC(), Versions: versions(config), Source: "dispatcher_memory", Scope: []string{"dispatcher.rules", "dispatcher.bridge.dry_run"}, Dispatcher: dispatcherAllowed(config)}
	if relay != nil {
		result.Relay = &relay.rules
		result.Versions.Relay = relay.digest
		result.Scope = append(result.Scope, "relay.group_control", "relay.filters")
		result.Source = "dispatcher_and_relay_memory"
	}
	return copyJSON(result)
}

func sameVersions(first, second Versions) bool { return digest(first) == digest(second) }

func (facade *Facade) GetConfigurationDrift(ctx context.Context, component string) (Envelope, error) {
	if component != "dispatcher" && component != "relay" {
		return Envelope{}, failure("validation", "Configuration component must be dispatcher or relay")
	}
	view := ConfigurationDrift{Component: component, Reason: "No authorized memory/disk configuration reader is connected"}
	if facade.configurationReader == nil {
		return envelope(view, "", "configuration_reader_missing", "partial"), nil
	}
	observation, err := facade.configurationReader.ReadConfiguration(ctx, component)
	if err != nil {
		return Envelope{}, failure("unavailable", "Authorized configuration read did not complete")
	}
	project := func(raw map[string]any) (any, error) {
		if component == "relay" {
			return decodeRelayRules(raw)
		}
		rules, ok := raw["rules"].(map[string]any)
		_, bridgeOK := raw["bridge"].(map[string]any)
		if !ok || !bridgeOK {
			return nil, failure("unavailable", "Configuration reader returned an invalid shape")
		}
		if _, ok := rules["group_rules"].(map[string]any); !ok {
			return nil, failure("unavailable", "Configuration reader omitted group rules")
		}
		return dispatcherAllowed(snapshot{root: raw, rules: rules}), nil
	}
	if observation.Memory == nil || observation.Disk == nil {
		return envelope(view, "", "authorized_configuration_reader", "partial"), nil
	}
	view.Memory, err = project(observation.Memory)
	if err != nil {
		return Envelope{}, err
	}
	view.Disk, err = project(observation.Disk)
	if err != nil {
		return Envelope{}, err
	}
	drift := !reflect.DeepEqual(observation.Memory, observation.Disk)
	view.Available, view.Drift, view.Reason = true, &drift, "Comparison covers the complete provider observation; displayed values are allowlisted"
	cleanVersion := func(value string) *string {
		if publicVersion.MatchString(value) {
			return nullable(value)
		}
		return nil
	}
	view.MemoryVersion, view.DiskVersion = cleanVersion(observation.MemoryVersion), cleanVersion(observation.DiskVersion)
	return envelope(view, "", "authorized_configuration_reader", "partial"), nil
}
