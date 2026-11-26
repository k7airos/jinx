package dispatcher

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"jinx/internal/contracts"
)

const (
	maxForwardRules         = 64
	maxForwardTemplateRunes = 1000
	defaultForwardTemplate  = "[{{platform}}] {{group}} · {{sender}}: {{content}}"
)

var forwardRuleIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// ForwardRule configures cross-platform passthrough forwarding from a source
// container directly to an allowlisted target container via DirectDeliver,
// bypassing message filtering, debouncing, and AI inference.
type ForwardRule struct {
	ID                string `json:"id"`
	Enabled           bool   `json:"enabled"`
	SourceContainerID string `json:"source_container_id"`
	TargetContainerID string `json:"target_container_id"`
	DryRun            bool   `json:"dry_run,omitempty"`
	IncludeEcho       bool   `json:"include_echo,omitempty"`
	Template          string `json:"template,omitempty"`
}

func validateForwardRules(rules []ForwardRule, groupRules map[string]GroupRule) ([]ForwardRule, error) {
	if len(rules) > maxForwardRules {
		return nil, fmt.Errorf("too many forward_rules (max %d)", maxForwardRules)
	}
	if len(rules) == 0 {
		return nil, nil
	}
	seenIDs := make(map[string]bool, len(rules))
	normalized := make([]ForwardRule, 0, len(rules))
	for i, r := range rules {
		r.ID = strings.TrimSpace(r.ID)
		r.SourceContainerID = strings.TrimSpace(r.SourceContainerID)
		r.TargetContainerID = strings.TrimSpace(r.TargetContainerID)
		if !forwardRuleIDPattern.MatchString(r.ID) {
			return nil, fmt.Errorf("forward_rules[%d]: invalid rule id %q", i, r.ID)
		}
		if seenIDs[r.ID] {
			return nil, fmt.Errorf("forward_rules[%d]: duplicate rule id %q", i, r.ID)
		}
		seenIDs[r.ID] = true
		if !contracts.ValidContainerID(r.SourceContainerID) {
			return nil, fmt.Errorf("forward_rules[%d]: invalid source_container_id %q", i, r.SourceContainerID)
		}
		if !contracts.ValidContainerID(r.TargetContainerID) {
			return nil, fmt.Errorf("forward_rules[%d]: invalid target_container_id %q", i, r.TargetContainerID)
		}
		if r.SourceContainerID == r.TargetContainerID {
			return nil, fmt.Errorf("forward_rules[%d]: source_container_id and target_container_id cannot be equal", i)
		}
		if _, exists := groupRules[r.TargetContainerID]; !exists {
			return nil, fmt.Errorf("forward_rules[%d]: target_container_id %q has no configured route", i, r.TargetContainerID)
		}
		if !utf8.ValidString(r.Template) || strings.ContainsRune(r.Template, '\x00') || utf8.RuneCountInString(r.Template) > maxForwardTemplateRunes {
			return nil, fmt.Errorf("forward_rules[%d]: invalid template", i)
		}
		normalized = append(normalized, r)
	}
	return normalized, nil
}

func (c Config) matchingForwardRules(sourceContainerID string) []ForwardRule {
	var matched []ForwardRule
	for _, r := range c.Rules.ForwardRules {
		if r.Enabled && r.SourceContainerID == sourceContainerID {
			matched = append(matched, r)
		}
	}
	return matched
}

func renderForwardTemplate(tmpl string, msg RelayMessage) string {
	if strings.TrimSpace(tmpl) == "" {
		tmpl = defaultForwardTemplate
	}
	platform := strings.TrimSpace(msg.Platform)
	if platform == "" {
		if parsed, _, ok := contracts.ParseContainerID(msg.GroupID); ok {
			platform = string(parsed)
		}
	}
	group := strings.TrimSpace(msg.GroupName)
	if group == "" {
		group = msg.GroupID
	}
	sender := strings.TrimSpace(msg.SenderName)
	if sender == "" {
		sender = msg.SenderID
	}
	replacer := strings.NewReplacer(
		"{{platform}}", platform,
		"{{source}}", msg.Source,
		"{{group}}", group,
		"{{group_id}}", msg.GroupID,
		"{{sender}}", sender,
		"{{sender_id}}", msg.SenderID,
		"{{content}}", msg.Content,
		"{{id}}", msg.ID,
	)
	rendered := strings.TrimSpace(replacer.Replace(tmpl))
	if rendered == "" {
		rendered = strings.TrimSpace(msg.Content)
	}
	if rendered == "" {
		rendered = "[empty]"
	}
	if utf8.RuneCountInString(rendered) > maxDirectDeliverRunes {
		runes := []rune(rendered)
		rendered = string(runes[:maxDirectDeliverRunes-1]) + "…"
	}
	return rendered
}

func (d *Dispatcher) runForwardRule(rule ForwardRule, msg RelayMessage) {
	idemKey := "fwd-" + messageDigest(rule.ID+"\x00"+msg.GroupID+"\x00"+msg.ID)
	text := renderForwardTemplate(rule.Template, msg)
	resp, err := d.DirectDeliver(d.ctx, DirectDeliverRequest{
		TargetContainerID: rule.TargetContainerID,
		Text:              text,
		IdempotencyKey:    idemKey,
		Source:            "forward",
		DryRun:            rule.DryRun,
	})
	if err != nil {
		d.Log("WARN", "直通转发规则 %q (%s -> %s) 执行失败: %v", rule.ID, rule.SourceContainerID, rule.TargetContainerID, err)
		return
	}
	if resp.DryRun {
		d.Log("INFO", "直通转发规则 %q 演练拦截 (%s -> %s, reason=%s)", rule.ID, rule.SourceContainerID, rule.TargetContainerID, resp.ReasonCode)
		return
	}
	d.Log("INFO", "直通转发规则 %q 已投递 (%s -> %s, outcome=%s, delivery=%s)", rule.ID, rule.SourceContainerID, rule.TargetContainerID, resp.Outcome, resp.Delivery)
}
