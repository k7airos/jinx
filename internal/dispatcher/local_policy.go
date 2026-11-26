package dispatcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"jinx/internal/contracts"
)

type localIdentity struct {
	Profile          string `json:"profile"`
	MemoryUser       string `json:"memory_user"`
	VectorCollection string `json:"vector_collection"`
}

type codeFactRule struct {
	Label   string `json:"label"`
	Pattern string `json:"pattern"`
}

type compiledCodeFactRule struct {
	label   string
	pattern *regexp.Regexp
}

type reminderPolicy struct {
	Enabled              bool   `json:"enabled"`
	AppPath              string `json:"app_path"`
	Application          string `json:"application"`
	BundleID             string `json:"bundle_id"`
	CalendarTitle        string `json:"calendar_title"`
	SourceTitle          string `json:"source_title"`
	SourceType           int    `json:"source_type"`
	RequireCloudVerified bool   `json:"require_cloud_verified"`
	TitlePrefix          string `json:"title_prefix"`
	MarkerPrefix         string `json:"marker_prefix"`
}

type supportPolicy struct {
	Enabled          bool           `json:"enabled"`
	GroupID          string         `json:"group_id"`
	SelfSenderIDs    []string       `json:"self_sender_ids"`
	CreatorSenderIDs []string       `json:"creator_sender_ids,omitempty"`
	InstructionsFile string         `json:"instructions_file"`
	InstructionsMode string         `json:"instructions_mode,omitempty"`
	KnowledgeFiles   []string       `json:"knowledge_files"`
	DocumentFiles    []string       `json:"document_files"`
	ReleaseFiles     []string       `json:"release_files"`
	CodeFactsFile    string         `json:"code_facts_file"`
	CodeFactRules    []codeFactRule `json:"code_fact_rules,omitempty"`
	Reminders        reminderPolicy `json:"reminders"`
	instructions     string
}

type scenarioPolicy struct {
	PromptFile     string   `json:"prompt_file,omitempty"`
	PromptMode     string   `json:"prompt_mode,omitempty"`
	KnowledgeFiles []string `json:"knowledge_files,omitempty"`
	Skills         []string `json:"skills,omitempty"`
	Tools          []string `json:"tools,omitempty"`
	MaxReplyRunes  int      `json:"max_reply_runes,omitempty"`
	prompt         string
}

type knowledgeBasePolicy struct {
	Files    []string `json:"files"`
	MaxBytes int      `json:"max_bytes,omitempty"`
}

type localPolicy struct {
	Version           int                            `json:"version"`
	CommunicationFile string                         `json:"communication_file"`
	CommunicationMode string                         `json:"communication_mode,omitempty"`
	Identities        map[string]localIdentity       `json:"identities"`
	Support           supportPolicy                  `json:"support"`
	Scenarios         map[string]scenarioPolicy      `json:"scenarios,omitempty"`
	KnowledgeBases    map[string]knowledgeBasePolicy `json:"knowledge_bases,omitempty"`
	communication     string
}

func readPrivateFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, fmt.Errorf("local policy files must be regular files with mode 0600")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("local policy file unavailable")
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) || opened.Mode().Perm() != 0600 {
		return nil, fmt.Errorf("local policy file changed")
	}
	data, err := io.ReadAll(io.LimitReader(file, (256<<10)+1))
	if err != nil || len(data) > 256<<10 {
		return nil, fmt.Errorf("local policy file unreadable or too large")
	}
	return data, nil
}

func loadLocalPolicy(path string) (*localPolicy, error) {
	if path == "" {
		return nil, nil
	}
	data, err := readPrivateFile(path)
	if err != nil {
		return nil, err
	}
	var policy localPolicy
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&policy) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("invalid local policy schema")
	}
	if err := policy.validate(); err != nil {
		return nil, err
	}
	for _, item := range []struct {
		path string
		text *string
	}{{policy.CommunicationFile, &policy.communication}, {policy.Support.InstructionsFile, &policy.Support.instructions}} {
		if item.path == "" {
			continue
		}
		data, err := readPrivateFile(item.path)
		if err != nil {
			return nil, err
		}
		*item.text = string(data)
	}
	if len(policy.Scenarios) > 0 {
		names := make([]string, 0, len(policy.Scenarios))
		for name := range policy.Scenarios {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			sc := policy.Scenarios[name]
			if sc.PromptFile == "" {
				continue
			}
			data, err := readPrivateFile(sc.PromptFile)
			if err != nil {
				return nil, err
			}
			sc.prompt = string(data)
			policy.Scenarios[name] = sc
		}
	}
	return &policy, nil
}

func compileCodeFactRules(rules []codeFactRule) ([]compiledCodeFactRule, error) {
	if len(rules) > 32 {
		return nil, fmt.Errorf("too many code fact rules")
	}
	compiled := make([]compiledCodeFactRule, 0, len(rules))
	for _, rule := range rules {
		label := strings.TrimSpace(rule.Label)
		if label == "" || !utf8.ValidString(rule.Label) || utf8.RuneCountInString(label) > 128 || strings.ContainsFunc(rule.Label, unicode.IsControl) {
			return nil, fmt.Errorf("invalid code fact rule label")
		}
		if rule.Pattern == "" || len(rule.Pattern) > 256 {
			return nil, fmt.Errorf("invalid code fact rule pattern length")
		}
		re, err := regexp.Compile(rule.Pattern)
		if err != nil || re.NumSubexp() != 1 {
			return nil, fmt.Errorf("code fact rule pattern must compile with one capture group")
		}
		compiled = append(compiled, compiledCodeFactRule{label: label, pattern: re})
	}
	return compiled, nil
}

func (p *localPolicy) validate() error {
	if p.Version != 1 {
		return fmt.Errorf("unsupported local policy version")
	}
	switch p.CommunicationMode {
	case "", "append", "replace":
	default:
		return fmt.Errorf("unsupported communication mode")
	}
	if p.CommunicationMode == "replace" && p.CommunicationFile == "" {
		return fmt.Errorf("replace communication mode requires communication_file")
	}
	profiles, users, collections := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for group, identity := range p.Identities {
		if !contracts.ValidContainerID(group) || !profilePattern.MatchString(identity.Profile) || strings.TrimSpace(identity.MemoryUser) == "" || strings.TrimSpace(identity.VectorCollection) == "" || profiles[identity.Profile] || users[identity.MemoryUser] || collections[identity.VectorCollection] {
			return fmt.Errorf("local identities must be explicit and isolated")
		}
		profiles[identity.Profile], users[identity.MemoryUser], collections[identity.VectorCollection] = true, true, true
	}
	support := p.Support
	switch support.InstructionsMode {
	case "", "append", "replace":
	default:
		return fmt.Errorf("unsupported support instructions_mode")
	}
	if support.InstructionsMode == "replace" && support.InstructionsFile == "" {
		return fmt.Errorf("replace support instructions_mode requires instructions_file")
	}
	if support.Enabled && (!contracts.ValidContainerID(support.GroupID) || len(support.SelfSenderIDs) != 1 || strings.TrimSpace(support.SelfSenderIDs[0]) == "" || p.Identities[support.GroupID].Profile == "") {
		return fmt.Errorf("support policy requires an isolated group and verified self identity")
	}
	if len(support.CreatorSenderIDs) > 16 {
		return fmt.Errorf("too many support creator_sender_ids")
	}
	for _, creatorID := range support.CreatorSenderIDs {
		if strings.TrimSpace(creatorID) == "" {
			return fmt.Errorf("support creator_sender_ids entries must not be empty")
		}
	}
	if _, err := compileCodeFactRules(support.CodeFactRules); err != nil {
		return err
	}
	paths := []string{p.CommunicationFile, support.InstructionsFile, support.CodeFactsFile}
	paths = append(paths, support.KnowledgeFiles...)
	paths = append(paths, support.DocumentFiles...)
	paths = append(paths, support.ReleaseFiles...)
	for _, path := range paths {
		if path != "" && !filepath.IsAbs(path) {
			return fmt.Errorf("local policy sources must be absolute paths")
		}
	}
	for _, list := range [][]string{support.KnowledgeFiles, support.DocumentFiles, support.ReleaseFiles} {
		for _, path := range list {
			if path == "" {
				return fmt.Errorf("local policy source entries must not be empty")
			}
		}
	}
	reminders := support.Reminders
	if reminders.Enabled && (!support.Enabled || !filepath.IsAbs(reminders.AppPath) || reminders.Application == "" || reminders.BundleID == "" || reminders.CalendarTitle == "" || reminders.SourceTitle == "" || reminders.SourceType != 2 || !reminders.RequireCloudVerified || reminders.TitlePrefix == "" || reminders.MarkerPrefix == "" || strings.ContainsAny(reminders.MarkerPrefix, "\r\n")) {
		return fmt.Errorf("reminders require an explicit verified app and calendar policy")
	}
	if len(p.Scenarios) > maxScenarioEntries {
		return fmt.Errorf("too many scenarios")
	}
	for name, sc := range p.Scenarios {
		if !mountIDPattern.MatchString(name) {
			return fmt.Errorf("invalid scenario name %q", name)
		}
		switch sc.PromptMode {
		case "", "append", "replace":
		default:
			return fmt.Errorf("unsupported prompt_mode for scenario %q", name)
		}
		if sc.PromptMode == "replace" && sc.PromptFile == "" {
			return fmt.Errorf("replace prompt_mode requires prompt_file for scenario %q", name)
		}
		if sc.PromptFile != "" && !filepath.IsAbs(sc.PromptFile) {
			return fmt.Errorf("scenario %q prompt_file must be an absolute path", name)
		}
		if len(sc.KnowledgeFiles) > maxMountEntries {
			return fmt.Errorf("too many knowledge_files for scenario %q", name)
		}
		for _, kf := range sc.KnowledgeFiles {
			if kf == "" || !filepath.IsAbs(kf) {
				return fmt.Errorf("scenario %q knowledge_files must be non-empty absolute paths", name)
			}
		}
		if err := validateMountList("scenario.skills", sc.Skills); err != nil {
			return err
		}
		if err := validateMountList("scenario.tools", sc.Tools); err != nil {
			return err
		}
		if sc.MaxReplyRunes < 0 || sc.MaxReplyRunes > 2000 {
			return fmt.Errorf("invalid max_reply_runes for scenario %q", name)
		}
	}
	if len(p.KnowledgeBases) > maxKnowledgeBaseEntries {
		return fmt.Errorf("too many knowledge_bases")
	}
	for name, kb := range p.KnowledgeBases {
		if !mountIDPattern.MatchString(name) {
			return fmt.Errorf("invalid knowledge_base name %q", name)
		}
		if len(kb.Files) == 0 || len(kb.Files) > maxMountEntries {
			return fmt.Errorf("knowledge_base %q requires 1..%d files", name, maxMountEntries)
		}
		for _, path := range kb.Files {
			if path == "" || !filepath.IsAbs(path) {
				return fmt.Errorf("knowledge_base %q files must be non-empty absolute paths", name)
			}
		}
		if kb.MaxBytes != 0 && (kb.MaxBytes < 256 || kb.MaxBytes > maxKnowledgeFileBytes) {
			return fmt.Errorf("invalid max_bytes for knowledge_base %q", name)
		}
	}
	return nil
}

func (c Config) supportPolicy() supportPolicy {
	if c.localPolicy != nil {
		return c.localPolicy.Support
	}
	return supportPolicy{}
}

func (c Config) identity(group string) localIdentity {
	if c.localPolicy != nil {
		if identity, exists := c.localPolicy.Identities[group]; exists {
			return identity
		}
	}
	if strings.HasSuffix(group, "@chatroom") {
		return localIdentity{MemoryUser: "group:" + group, VectorCollection: "group_" + strings.TrimSuffix(group, "@chatroom")}
	}
	return localIdentity{MemoryUser: "container:" + group, VectorCollection: "container_" + contracts.SanitizeCollectionSuffix(group)}
}

func (c Config) validateSupportRule(group string, rule GroupRule) error {
	policy := c.supportPolicy()
	if policy.Enabled && group == policy.GroupID && !rule.SupportMode {
		return fmt.Errorf("local support route cannot be downgraded")
	}
	if rule.SupportMode && (!policy.Enabled || group != policy.GroupID || (rule.TargetGroupID != "" && rule.TargetGroupID != group) || rule.SupportSinceMS <= 0 || rule.MentionOnly || rule.PromoShadow || len(rule.PromoKeywords) > 0 || len(rule.SelfSenderIDs) != 1 || rule.SelfSenderIDs[0] != policy.SelfSenderIDs[0]) {
		return fmt.Errorf("support mode requires explicit local authorization, activation time and verified self identity")
	}
	if rule.SupportMode {
		if len(rule.Skills) > 0 {
			return fmt.Errorf("support mode cannot mount skills")
		}
		for _, tool := range rule.Tools {
			if !allowedSupportTool(tool) {
				return fmt.Errorf("support mode disallows tool %q", tool)
			}
		}
		if rule.Scenario != "" && c.localPolicy != nil {
			if sc, exists := c.localPolicy.Scenarios[rule.Scenario]; exists {
				if len(sc.Skills) > 0 {
					return fmt.Errorf("support scenario %q cannot mount skills", rule.Scenario)
				}
				for _, tool := range sc.Tools {
					if !allowedSupportTool(tool) {
						return fmt.Errorf("support scenario %q disallows tool %q", rule.Scenario, tool)
					}
				}
			}
		}
	}
	return nil
}
