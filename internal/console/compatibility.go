package console

type CompatibilityView struct {
	GroupWhitelist any               `json:"groupWhitelist"`
	GroupProfiles  any               `json:"groupProfiles"`
	GroupOtherBots any               `json:"groupOtherBots"`
	DefaultProfile any               `json:"defaultProfile"`
	Editable       bool              `json:"editable"`
	Semantics      map[string]string `json:"semantics"`
}

func compatibility(config snapshot) CompatibilityView {
	return CompatibilityView{
		GroupWhitelist: config.rules["group_whitelist"], GroupProfiles: config.rules["group_profiles"], GroupOtherBots: config.rules["group_other_bots"], DefaultProfile: config.rules["default_profile"],
		Semantics: map[string]string{
			"groupWhitelist": "Admission requires both group_rules[id].enabled and membership in group_whitelist; neither alone grants admission",
			"groupProfiles":  "group_profiles[sourceGroupId] supplies prompt background; an empty value falls back to default_profile. This is not a backend or internal Hermes profile binding",
			"groupOtherBots": "group_other_bots[sourceGroupId] matches sender names for filtering and supplies prompt context; it does not grant permissions",
			"defaultProfile": "Fallback prompt background only; it does not select an execution backend",
		},
	}
}

func inheritedDiff(before, after snapshot, patch map[string]any) []FieldChange {
	changes := []FieldChange{}
	for id, raw := range groups(before) {
		oldGroup, _ := raw.(map[string]any)
		newGroup, exists := groups(after)[id].(map[string]any)
		if !exists {
			continue
		}
		oldFields, newFields := fields(before, oldGroup), fields(after, newGroup)
		for name, entry := range groupSchema() {
			_, changed := patch[name]
			if !entry.Inherited || !changed {
				continue
			}
			if digest(oldGroup[name]) != digest(newGroup[name]) || digest(oldFields[name].Effective) == digest(newFields[name].Effective) {
				continue
			}
			changes = append(changes, FieldChange{Component: "dispatcher", Path: "group_rules." + id + "." + name, Before: oldGroup[name], After: newGroup[name], EffectiveBefore: oldFields[name].Effective, EffectiveAfter: newFields[name].Effective})
		}
		if _, changed := patch["dry_run"]; changed {
			oldGlobal, _ := before.root["bridge"].(map[string]any)["dry_run"].(bool)
			newGlobal, _ := after.root["bridge"].(map[string]any)["dry_run"].(bool)
			oldLocal, _ := oldGroup["dry_run"].(bool)
			newLocal, _ := newGroup["dry_run"].(bool)
			if digest(oldGroup["dry_run"]) == digest(newGroup["dry_run"]) && (oldGlobal || oldLocal) != (newGlobal || newLocal) {
				changes = append(changes, FieldChange{Component: "dispatcher", Path: "group_rules." + id + ".dry_run", Before: oldGroup["dry_run"], After: newGroup["dry_run"], EffectiveBefore: oldGlobal || oldLocal, EffectiveAfter: newGlobal || newLocal})
			}
		}
	}
	return changes
}
