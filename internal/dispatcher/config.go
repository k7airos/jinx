package dispatcher

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"jinx/internal/contracts"
)

type ActionEndpointConfig struct {
	URL        string `json:"url"`
	DryRun     bool   `json:"dry_run,omitempty"`
	TimeoutSec int    `json:"timeout_sec,omitempty"`
}

type Config struct {
	localPolicy           *localPolicy
	allowDynamicEndpoints bool
	Server                struct {
		ListenAddr string `json:"listen_addr"`
	} `json:"server"`
	Relay struct {
		StreamURL string `json:"stream_url"`
	} `json:"relay"`
	Bridge struct {
		URL        string `json:"url"`
		DryRun     bool   `json:"dry_run"`
		TimeoutSec int    `json:"timeout_sec"`
	} `json:"bridge"`
	ActionEndpoints map[string]ActionEndpointConfig `json:"action_endpoints,omitempty"`
	Hermes          struct {
		BinPath       string `json:"bin_path"`
		Profile       string `json:"profile"`
		ProfilesDir   string `json:"profiles_dir"`
		TimeoutSec    int    `json:"timeout_sec"`
		MaxConcurrent int    `json:"max_concurrent"`
	} `json:"hermes"`
	Rules Rules `json:"rules"`
}

type Rules struct {
	BatchSize               int                  `json:"batch_size"`
	IdleTimeoutSec          int                  `json:"idle_timeout_sec"`
	MaxBatchWaitSec         int                  `json:"max_batch_wait_sec"`
	MaxMessageAgeSec        int                  `json:"max_message_age_sec"`
	LayaURL                 string               `json:"laya_url"`
	LayaFilter              bool                 `json:"laya_filter"`
	BypassDecisionOnMention bool                 `json:"bypass_decision_on_mention"`
	BotNames                []string             `json:"bot_names"`
	SelfSenderIDs           []string             `json:"self_sender_ids"`
	GroupWhitelist          []string             `json:"group_whitelist"`
	GroupProfiles           map[string]string    `json:"group_profiles"`
	GroupOtherBots          map[string][]string  `json:"group_other_bots"`
	GroupRules              map[string]GroupRule `json:"group_rules"`
	DefaultProfile          string               `json:"default_profile"`
	KeywordBlocklist        []string             `json:"keyword_blocklist"`
	IgnoreXML               bool                 `json:"ignore_xml"`
	// SessionMaxTurns rotates a group's Hermes session after N successful replies so a
	// long-running group does not keep one ever-growing conversation. 0 disables rotation.
	SessionMaxTurns int `json:"session_max_turns"`
	// SessionMaxAgeSec rotates a group's Hermes session once it is older than this. 0 disables it.
	SessionMaxAgeSec int           `json:"session_max_age_sec"`
	ForwardRules     []ForwardRule `json:"forward_rules,omitempty"`
}

// Routes are keyed only by source group ID. Target names never come from messages or model output.
type GroupRule struct {
	SupportMode        bool     `json:"support_mode,omitempty"`
	SupportSinceMS     int64    `json:"support_since_ms,omitempty"`
	SelfSenderIDs      []string `json:"self_sender_ids"`
	Enabled            bool     `json:"enabled"`
	TargetGroupID      string   `json:"target_group_id"`
	Target             string   `json:"target"`
	HermesProfile      string   `json:"hermes_profile"`
	DryRun             bool     `json:"dry_run"`
	MentionOnly        bool     `json:"mention_only"`
	AllowedTypes       []int    `json:"allowed_types"`
	BlockedSenderIDs   []string `json:"blocked_sender_ids"`
	KeywordBlocklist   []string `json:"keyword_blocklist"`
	RegexBlocklist     []string `json:"regex_blocklist"`
	BatchSize          int      `json:"batch_size"`
	IdleTimeoutSec     int      `json:"idle_timeout_sec"`
	MaxBatchWaitSec    int      `json:"max_batch_wait_sec"`
	MaxReplyRunes      int      `json:"max_reply_runes"`
	ReplyWindowSec     int      `json:"reply_window_sec"`
	MaxRepliesInWindow int      `json:"max_replies_in_window"`
	DecisionContext    string   `json:"decision_context,omitempty"`
	SensitiveCriteria  string   `json:"sensitive_criteria,omitempty"`
	NoiseCriteria      string   `json:"noise_criteria,omitempty"`
	SafeCriteria       string   `json:"safe_criteria,omitempty"`
	DecisionThreshold  float64  `json:"decision_threshold"`
	// PromoKeywords are business topics (case-insensitive substrings). A non-@ batch that hits
	// one skips the gate's noise verdict (sensitive content is still blocked) and goes to Hermes
	// with an "only speak if someone is clearly asking" instruction. Empty disables the path.
	PromoKeywords []string `json:"promo_keywords,omitempty"`
	// PromoShadow runs the whole promo path but only records what Hermes would have said.
	PromoShadow bool `json:"promo_shadow,omitempty"`
	// PromoDailyCap limits promo-path replies per group per local day; 0 means 1.
	PromoDailyCap int `json:"promo_daily_cap,omitempty"`
	// Declarative scenario & mount bindings (logical IDs only; file paths stay in localPolicy).
	Scenario       string   `json:"scenario,omitempty"`
	ResponseGoal   string   `json:"response_goal,omitempty"`
	Tone           string   `json:"tone,omitempty"`
	Skills         []string `json:"skills,omitempty"`
	Tools          []string `json:"tools,omitempty"`
	KnowledgeBases []string `json:"knowledge_bases,omitempty"`
}

var profilePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

func decodeConfig(data []byte) (Config, error) {
	return decodeConfigWithPolicy(data, nil)
}

func decodeConfigWithPolicy(data []byte, policy *localPolicy) (Config, error) {
	return decodeConfigWithOptions(data, policy, false)
}

func decodeConfigWithOptions(data []byte, policy *localPolicy, allowDynamicEndpoints bool) (Config, error) {
	cfg := Config{localPolicy: policy, allowDynamicEndpoints: allowDynamicEndpoints}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, err
	}
	return cfg, cfg.normalizeAndValidate()
}

func validateLoopbackHTTPURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return fmt.Errorf("invalid service URL")
	}
	if u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" {
		return fmt.Errorf("pipeline service URLs must not include queries or fragments")
	}
	host := strings.Trim(u.Hostname(), "[]")
	if !strings.EqualFold(host, "localhost") {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("pipeline services must use loopback URLs")
		}
	}
	portText := u.Port()
	port, err := strconv.Atoi(portText)
	if portText == "" || err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("pipeline service URLs must include an explicit loopback port")
	}
	return nil
}

func validateLoopbackOrigin(raw string) error {
	if err := validateLoopbackHTTPURL(raw); err != nil {
		return err
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Path != "" && u.Path != "/") {
		return fmt.Errorf("pipeline service origin must not include a path")
	}
	return nil
}

func (c Config) hasWeChatRoutes() bool {
	for id := range c.Rules.GroupRules {
		if p, _, ok := contracts.ParseContainerID(id); ok && p == contracts.PlatformWeChat {
			return true
		}
	}
	return false
}

func (c Config) actionEndpoint(containerID string) (contracts.Platform, ActionEndpointConfig, bool) {
	platform, _, ok := contracts.ParseContainerID(containerID)
	if !ok || platform == contracts.PlatformWeChat {
		return platform, ActionEndpointConfig{}, false
	}
	ep, exists := c.ActionEndpoints[string(platform)]
	return platform, ep, exists
}

func (c Config) endpointDryRun(containerID string) bool {
	_, ep, ok := c.actionEndpoint(containerID)
	return ok && ep.DryRun
}

func (c *Config) normalizeAndValidate() error {
	if c.Bridge.TimeoutSec == 0 {
		c.Bridge.TimeoutSec = 30
	}
	if c.Hermes.MaxConcurrent == 0 {
		c.Hermes.MaxConcurrent = 4
	}
	if c.Rules.MaxBatchWaitSec == 0 {
		c.Rules.MaxBatchWaitSec = 15
	}
	if c.Rules.MaxMessageAgeSec == 0 {
		c.Rules.MaxMessageAgeSec = 180
	}
	if c.Hermes.ProfilesDir == "" {
		c.Hermes.ProfilesDir = "profiles"
	}
	host, _, err := net.SplitHostPort(c.Server.ListenAddr)
	if err != nil || (host != "127.0.0.1" && host != "::1" && host != "localhost") {
		return fmt.Errorf("server.listen_addr must bind to loopback")
	}
	if err := validateLoopbackHTTPURL(c.Relay.StreamURL); err != nil {
		return err
	}
	if c.Bridge.URL != "" || (!c.allowDynamicEndpoints && len(c.ActionEndpoints) == 0) || c.hasWeChatRoutes() {
		if err := validateLoopbackHTTPURL(c.Bridge.URL); err != nil {
			return err
		}
	}
	if c.Rules.LayaFilter || c.Rules.LayaURL != "" {
		if err := validateLoopbackOrigin(c.Rules.LayaURL); err != nil {
			return err
		}
	}
	for key, ep := range c.ActionEndpoints {
		if !contracts.IsNamespacedPlatform(contracts.Platform(key)) {
			return fmt.Errorf("unsupported action endpoint platform %q", key)
		}
		if err := validateLoopbackOrigin(ep.URL); err != nil {
			return fmt.Errorf("action_endpoints.%s: %w", key, err)
		}
		if ep.TimeoutSec == 0 {
			ep.TimeoutSec = 30
		}
		if ep.TimeoutSec < 1 || ep.TimeoutSec > 120 {
			return fmt.Errorf("action_endpoints.%s timeout must be 1..120 seconds", key)
		}
		c.ActionEndpoints[key] = ep
	}
	if c.Hermes.BinPath == "" || c.Hermes.TimeoutSec < 1 || c.Hermes.TimeoutSec > 600 || c.Hermes.MaxConcurrent < 1 || c.Hermes.MaxConcurrent > 16 {
		return fmt.Errorf("invalid Hermes command, timeout or concurrency")
	}
	if c.Bridge.TimeoutSec < 1 || c.Bridge.TimeoutSec > 120 {
		return fmt.Errorf("bridge timeout must be 1..120 seconds")
	}
	if c.Rules.BatchSize < 1 || c.Rules.BatchSize > 50 || c.Rules.IdleTimeoutSec < 1 || c.Rules.IdleTimeoutSec > 300 || c.Rules.MaxBatchWaitSec < c.Rules.IdleTimeoutSec || c.Rules.MaxBatchWaitSec > 300 {
		return fmt.Errorf("invalid batch/debounce/max-wait settings")
	}
	if c.Rules.MaxMessageAgeSec < 1 || c.Rules.MaxMessageAgeSec > 3600 {
		return fmt.Errorf("invalid message expiry")
	}
	if c.Rules.SessionMaxTurns < 0 || c.Rules.SessionMaxTurns > 1000 {
		return fmt.Errorf("invalid session_max_turns")
	}
	if c.Rules.SessionMaxAgeSec < 0 || c.Rules.SessionMaxAgeSec > 30*24*3600 {
		return fmt.Errorf("invalid session_max_age_sec")
	}
	profiles := map[string]string{}
	for id, rule := range c.Rules.GroupRules {
		rule = c.resolveRule(id, rule)
		srcPlatform, _, ok := contracts.ParseContainerID(id)
		if !ok {
			return fmt.Errorf("invalid source group ID %q", id)
		}
		dstPlatform, _, ok := contracts.ParseContainerID(rule.TargetGroupID)
		if !ok || dstPlatform != srcPlatform || strings.TrimSpace(rule.Target) == "" || strings.ContainsAny(rule.Target, "\r\n\x00") {
			return fmt.Errorf("group %s needs an explicit target group ID and unique search name", id)
		}
		if !profilePattern.MatchString(rule.HermesProfile) || rule.HermesProfile == c.Hermes.Profile {
			return fmt.Errorf("group %s needs an isolated Hermes profile", id)
		}
		if previous := profiles[rule.HermesProfile]; previous != "" {
			return fmt.Errorf("groups %s and %s cannot share Hermes profile", previous, id)
		}
		profiles[rule.HermesProfile] = id
		if rule.BatchSize < 1 || rule.BatchSize > 50 || rule.IdleTimeoutSec < 1 || rule.MaxBatchWaitSec < rule.IdleTimeoutSec || rule.MaxBatchWaitSec > 300 || rule.MaxReplyRunes < 1 || rule.MaxReplyRunes > 2000 || rule.ReplyWindowSec < 1 || rule.ReplyWindowSec > 3600 || rule.MaxRepliesInWindow < 1 || rule.MaxRepliesInWindow > 20 || rule.DecisionThreshold < 0.5 || rule.DecisionThreshold > 1 {
			return fmt.Errorf("invalid rules for group %s", id)
		}
		if err := c.validateSupportRule(id, rule); err != nil {
			return err
		}
		if rule.Scenario != "" && !mountIDPattern.MatchString(rule.Scenario) {
			return fmt.Errorf("group %s: invalid scenario %q", id, rule.Scenario)
		}
		if err := validateShortText("response_goal", rule.ResponseGoal, 200); err != nil {
			return fmt.Errorf("group %s: %w", id, err)
		}
		if err := validateShortText("tone", rule.Tone, 200); err != nil {
			return fmt.Errorf("group %s: %w", id, err)
		}
		for _, check := range []struct {
			field string
			items []string
		}{{"skills", rule.Skills}, {"tools", rule.Tools}, {"knowledge_bases", rule.KnowledgeBases}} {
			if err := validateMountList(check.field, check.items); err != nil {
				return fmt.Errorf("group %s: %w", id, err)
			}
		}
		if len(rule.KnowledgeBases) > 0 {
			if c.localPolicy == nil {
				return fmt.Errorf("group %s: knowledge_bases require local policy", id)
			}
			for _, kb := range rule.KnowledgeBases {
				if _, exists := c.localPolicy.KnowledgeBases[kb]; !exists {
					return fmt.Errorf("group %s: unknown knowledge base %q", id, kb)
				}
			}
		}
		if identity := c.identity(id); identity.Profile != "" && identity.Profile != rule.HermesProfile {
			return fmt.Errorf("route profile does not match local identity")
		}
		if utf8.RuneCountInString(rule.DecisionContext) > 4000 || utf8.RuneCountInString(rule.SensitiveCriteria) > 2000 || utf8.RuneCountInString(rule.NoiseCriteria) > 2000 || utf8.RuneCountInString(rule.SafeCriteria) > 2000 {
			return fmt.Errorf("content gate rules too long for group %s", id)
		}
		if len(rule.PromoKeywords) > 200 || rule.PromoDailyCap < 0 || rule.PromoDailyCap > 20 {
			return fmt.Errorf("invalid promo settings for group %s", id)
		}
		for _, keyword := range rule.PromoKeywords {
			if n := utf8.RuneCountInString(strings.TrimSpace(keyword)); n < 2 || n > 40 {
				return fmt.Errorf("group %s: promo keyword must be 2..40 characters", id)
			}
		}
		for _, pattern := range rule.RegexBlocklist {
			if _, err := regexp.Compile(pattern); err != nil {
				return fmt.Errorf("group %s: %w", id, err)
			}
		}
	}
	if policy := c.supportPolicy(); policy.Enabled {
		if _, exists := c.Rules.GroupRules[policy.GroupID]; !exists {
			return fmt.Errorf("local support route cannot be removed")
		}
	}
	for _, id := range c.Rules.GroupWhitelist {
		if _, ok := c.Rules.GroupRules[id]; !ok {
			return fmt.Errorf("allowlisted group %s has no route", id)
		}
	}
	normalizedForward, err := validateForwardRules(c.Rules.ForwardRules, c.Rules.GroupRules)
	if err != nil {
		return err
	}
	c.Rules.ForwardRules = normalizedForward
	return nil
}

func (c Config) route(id string) (GroupRule, bool) {
	rule, ok := c.Rules.GroupRules[id]
	return c.resolveRule(id, rule), ok && rule.Enabled && contains(c.Rules.GroupWhitelist, id)
}

func (c Config) resolveRule(id string, rule GroupRule) GroupRule {
	if rule.TargetGroupID == "" {
		rule.TargetGroupID = id
	}
	if rule.HermesProfile == "" {
		rule.HermesProfile = c.Hermes.Profile + "-" + contracts.SanitizeProfileSuffix(id)
	}
	if rule.BatchSize == 0 {
		rule.BatchSize = c.Rules.BatchSize
	}
	if rule.IdleTimeoutSec == 0 {
		rule.IdleTimeoutSec = c.Rules.IdleTimeoutSec
	}
	if rule.MaxBatchWaitSec == 0 {
		rule.MaxBatchWaitSec = c.Rules.MaxBatchWaitSec
	}
	if rule.MaxReplyRunes == 0 {
		rule.MaxReplyRunes = 500
	}
	if rule.ReplyWindowSec == 0 {
		rule.ReplyWindowSec = 45
	}
	if rule.MaxRepliesInWindow == 0 {
		rule.MaxRepliesInWindow = 2
	}
	if rule.DecisionThreshold == 0 {
		rule.DecisionThreshold = 0.5
	}
	if rule.AllowedTypes == nil {
		rule.AllowedTypes = []int{1, 49, 81}
	}
	return rule
}

func contains(values []string, value string) bool {
	for _, item := range values {
		if item == value {
			return true
		}
	}
	return false
}

func atomicWrite(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".pipeline-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(f.Name(), path)
}
