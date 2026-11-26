package console

import "reflect"

func canonicalGroupRules(value any) (map[string]any, error) {
	rules, ok := value.(map[string]any)
	if !ok {
		return nil, failure("unsupported", "Group rules cannot be normalized without a group map")
	}
	canonical := make(map[string]any, len(rules))
	for id, raw := range rules {
		group, ok := raw.(map[string]any)
		if !ok {
			return nil, failure("unsupported", "Group rules cannot be normalized without field objects")
		}
		for _, field := range []string{"self_sender_ids", "allowed_types"} {
			if _, exists := group[field]; !exists {
				return nil, failure("unsupported", "Group map cannot be verified losslessly without explicit self_sender_ids and allowed_types null or []")
			}
		}
		fields := make(map[string]any, len(group))
		for name, value := range group {
			fields[name] = value
		}
		for name, entry := range groupSchema() {
			value, exists := group[name]
			if exists {
				if err := validateValue(name, value, entry, true); err != nil {
					return nil, failure("unsupported", "Group rule value cannot be normalized with the current schema")
				}
			} else {
				switch entry.Type {
				case "boolean":
					value = false
				case "string":
					value = ""
				case "integer", "decimal":
					value = float64(0)
				case "string_list", "number_list":
					value = nil
				default:
					return nil, failure("unsupported", "Group rule type has no verified JSON normalization")
				}
			}
			if name == "promo_keywords" || name == "skills" || name == "tools" || name == "knowledge_bases" {
				if list, ok := value.([]any); ok && len(list) == 0 {
					value = nil
				}
			}
			fields[name] = value
		}
		canonical[id] = fields
	}
	return canonical, nil
}

func matchPatch(config snapshot, patch map[string]any) (bool, error) {
	matches := true
	for key, expected := range patch {
		actual, exists := config.rules[key]
		if key == "dry_run" {
			actual, exists = config.root["bridge"].(map[string]any)[key]
		}
		if !exists {
			matches = false
			continue
		}
		if key == "group_rules" {
			normalExpected, err := canonicalGroupRules(expected)
			if err != nil {
				return false, err
			}
			normalActual, err := canonicalGroupRules(actual)
			if err != nil {
				return false, err
			}
			expected, actual = normalExpected, normalActual
		}
		if !reflect.DeepEqual(expected, actual) {
			matches = false
		}
	}
	return matches, nil
}
