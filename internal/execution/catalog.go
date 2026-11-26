package execution

import (
	"context"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

var tokenPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var modelPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_./:@+-]{0,255}$`)

func loopbackURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", failure("validation", "loopback_base_url")
	}
	address := net.ParseIP(parsed.Hostname())
	if address == nil || !address.IsLoopback() || parsed.Port() == "" {
		return "", failure("validation", "loopback_base_url")
	}
	return strings.TrimSuffix(raw, "/"), nil
}

func validateBackend(option BackendOptions) error {
	if !tokenPattern.MatchString(option.ID) || len(option.Models) == 0 {
		return failure("validation", "backend")
	}
	seen := map[string]bool{}
	for _, model := range option.Models {
		if !modelPattern.MatchString(model.ID) || seen[model.ID] || strings.TrimSpace(model.Label) == "" {
			return failure("validation", "models")
		}
		seen[model.ID] = true
	}
	switch option.Kind {
	case AlmaPersonal:
		if _, err := loopbackURL(option.AlmaBaseURL); err != nil {
			return err
		}
	case TextOnly:
	case LegacyHermes:
		if !filepath.IsAbs(option.Hermes.Command) || !tokenPattern.MatchString(option.Hermes.Profile) || option.Hermes.MaxTurns < 1 || option.Hermes.MaxTurns > 100 {
			return failure("validation", "hermes_options")
		}
		seenTools := map[string]bool{}
		for _, tool := range option.Hermes.Toolsets {
			if (tool != "web" && tool != "memory" && tool != "skills") || seenTools[tool] {
				return failure("validation", "hermes_toolsets")
			}
			seenTools[tool] = true
		}
	default:
		return failure("validation", "backend_kind")
	}
	return nil
}

func backendView(option BackendOptions) Backend {
	view := Backend{ID: option.ID, Kind: option.Kind, Models: append([]Model{}, option.Models...), Available: true,
		AgentBinding: "unverified", SessionModes: []string{"independent_run"}, MinTimeoutSec: 1, MaxTimeoutSec: 3600,
		PermissionEvidence: []PermissionView{}}
	if option.Kind == AlmaPersonal {
		view.AgentBinding = "branch_only"
	}
	if option.Kind == TextOnly && option.Provider == nil {
		view.Available, view.Reason = false, "text_provider_not_configured"
	}
	for _, capability := range Capabilities() {
		evidence := PermissionView{Capability: capability, Enforcement: Unverified, Provenance: "operator_instance", EnforcedAt: []string{}, Reason: "not_implemented"}
		switch {
		case capability == AutoTextDelivery:
			evidence.Enforcement, evidence.Provenance, evidence.Reason = Host, "dispatcher", "external_delivery_gate_required"
		case capability == TextGeneration:
			evidence.Effective = boolean(view.Available)
			evidence.Enforcement, evidence.Provenance = Hard, "execution"
			evidence.EnforcedAt, evidence.Editable, evidence.Reason = []string{"execution.Execute.admission"}, true, ""
		case option.Kind == TextOnly:
			evidence.Effective = boolean(false)
			evidence.Enforcement, evidence.Provenance = Hard, "text_only_executor"
			evidence.EnforcedAt = []string{"execution.Provider.Generate.no_tool_dispatch"}
			evidence.Editable, evidence.Reason = true, "executor_has_no_tool_or_host_operation"
		case option.Kind == AlmaPersonal:
			evidence.Enforcement, evidence.Provenance = Soft, "personal_dispatch_prompt"
			evidence.Reason = "personal_api_has_no_request_tool_isolation"
		case option.Kind == LegacyHermes:
			evidence.Enforcement, evidence.Provenance = Soft, "hermes_cli_toolsets"
			evidence.Reason = "cli_selection_is_not_host_isolation"
		}
		view.PermissionEvidence = append(view.PermissionEvidence, evidence)
	}
	return view
}

func (engine *Engine) Catalog(ctx context.Context) ([]Backend, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	catalog := make([]Backend, 0, len(engine.options.Backends))
	for _, option := range engine.options.Backends {
		catalog = append(catalog, backendView(option))
	}
	return catalog, nil
}

func (engine *Engine) option(id string) (BackendOptions, bool) {
	for _, option := range engine.options.Backends {
		if option.ID == id {
			return option, true
		}
	}
	return BackendOptions{}, false
}

func (engine *Engine) permissions(attempt RouteAttempt, policy Policy, revision string) []PermissionView {
	option, _ := engine.option(attempt.BackendID)
	views := backendView(option).PermissionEvidence
	for index := range views {
		view := &views[index]
		view.Requested, view.PolicyVersion = boolean(policy.Capabilities[view.Capability]), revision
		if view.Capability == TextGeneration && !policy.Capabilities[TextGeneration] {
			view.Effective = boolean(false)
		}
	}
	return views
}

func (engine *Engine) admit(attempt RouteAttempt, policy Policy) error {
	option, exists := engine.option(attempt.BackendID)
	if !exists || !backendView(option).Available {
		return failure("unavailable", "backendId")
	}
	if !policy.Capabilities[TextGeneration] {
		return failure("policy_blocked", "text_generation")
	}
	for _, capability := range Capabilities() {
		requested := policy.Capabilities[capability]
		if capability == AutoTextDelivery || capability == TextGeneration {
			continue
		}
		if requested {
			return failure("unsupported", string(capability))
		}
		if option.Kind != TextOnly && (policy.NoTools || !policy.AllowSoft) {
			return failure("policy_mismatch", string(capability))
		}
	}
	return nil
}

func (engine *Engine) fallbackCompatible(primary, fallback RouteAttempt, policy Policy) error {
	if err := engine.admit(fallback, policy); err != nil {
		return failure("fallback_policy_mismatch", "fallbacks")
	}
	primaryOption, _ := engine.option(primary.BackendID)
	fallbackOption, _ := engine.option(fallback.BackendID)
	primaryEvidence := backendView(primaryOption).PermissionEvidence
	fallbackEvidence := backendView(fallbackOption).PermissionEvidence
	for index, evidence := range primaryEvidence {
		if evidence.Enforcement == Hard && evidence.Effective != nil && !*evidence.Effective && fallbackEvidence[index].Enforcement != Hard {
			return failure("fallback_policy_mismatch", string(evidence.Capability))
		}
	}
	return nil
}

func (engine *Engine) validate(config Config) error {
	if config.SchemaVersion != SchemaVersion || config.Groups == nil || config.Profiles == nil || config.Policies == nil {
		return failure("validation", "schemaVersion_or_maps")
	}
	if len(config.Groups) > 10000 || len(config.Policies) > 10000 || len(config.Profiles) > 10000 {
		return failure("too_large", "config")
	}
	for id, policy := range config.Policies {
		if !tokenPattern.MatchString(id) || len(policy.Capabilities) != len(Capabilities()) || (policy.NoTools && policy.AllowSoft) {
			return failure("validation", "policies")
		}
		for _, capability := range Capabilities() {
			if _, exists := policy.Capabilities[capability]; !exists {
				return failure("validation", "capabilities")
			}
			if capability != TextGeneration && capability != AutoTextDelivery && policy.Capabilities[capability] {
				return failure("unsupported", string(capability))
			}
		}
	}
	for groupID, group := range config.Groups {
		if groupID == "" || len(groupID) > 256 || strings.TrimSpace(groupID) != groupID || strings.ContainsAny(groupID, "\x00\r\n") || !tokenPattern.MatchString(group.RouteID) || group.Fallbacks == nil || len(group.Fallbacks) > 8 {
			return failure("validation", "groups")
		}
		policy, exists := config.Policies[group.PolicyID]
		if !exists {
			return failure("validation", "policyId")
		}
		admissionPolicy := intersectPolicies(policy, policy)
		admissionPolicy.Capabilities[TextGeneration] = true
		attempts := append([]RouteAttempt{group.Primary}, group.Fallbacks...)
		for index, attempt := range attempts {
			option, exists := engine.option(attempt.BackendID)
			if !exists || attempt.TimeoutSec < 1 || attempt.TimeoutSec > 3600 {
				return failure("validation", "backendId_or_timeoutSec")
			}
			modelExists := false
			for _, model := range option.Models {
				modelExists = modelExists || model.ID == attempt.ModelID
			}
			if !modelExists {
				return failure("validation", "modelId")
			}
			if attempt.Session.Mode != "independent_run" || attempt.Session.RotateAfterTurns < 0 || attempt.Session.RotateAfterSeconds < 0 || attempt.Session.RotateAfterTurns != 0 || attempt.Session.RotateAfterSeconds != 0 {
				return failure("unsupported", "session")
			}
			if index > 0 {
				if err := engine.fallbackCompatible(group.Primary, attempt, admissionPolicy); err != nil {
					return err
				}
				if err := engine.fallbackCompatible(attempts[index-1], attempt, admissionPolicy); err != nil {
					return err
				}
			} else {
				if err := engine.admit(attempt, admissionPolicy); err != nil {
					return err
				}
			}
		}
	}
	for profile, groupID := range config.Profiles {
		if !tokenPattern.MatchString(profile) {
			return failure("validation", "profiles")
		}
		if _, exists := config.Groups[groupID]; !exists {
			return failure("validation", "profile_group")
		}
	}
	return nil
}
