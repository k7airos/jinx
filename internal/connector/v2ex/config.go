package v2ex

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strings"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

var (
	topicIDPattern        = regexp.MustCompile(`^[0-9]{1,12}$`)
	containerTopicPattern = regexp.MustCompile(`^(?:t-)?([0-9]{1,12})$`)
	usernamePattern       = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)
)

type EgoConfig struct {
	BinPath    string `json:"bin_path,omitempty"`
	TaskSpace  string `json:"task_space,omitempty"`
	TimeoutSec int    `json:"timeout_sec,omitempty"`
	KeepPage   bool   `json:"keep_page,omitempty"`
}

type TargetConfig struct {
	ContainerID string `json:"container_id"`
	Label       string `json:"label,omitempty"`
	TopicID     string `json:"topic_id,omitempty"`
	URL         string `json:"url,omitempty"`
	Enabled     bool   `json:"enabled"`
	PollLimit   int    `json:"poll_limit,omitempty"`
}

type Config struct {
	ListenAddr       string         `json:"listen_addr"`
	RelayURL         string         `json:"relay_url"`
	StateDir         string         `json:"state_dir"`
	AccountID        string         `json:"account_id,omitempty"`
	SelfUsernames    []string       `json:"self_usernames,omitempty"`
	DryRun           *bool          `json:"dry_run,omitempty"`
	AutoMention      *bool          `json:"auto_mention,omitempty"`
	PollIntervalSec  int            `json:"poll_interval_sec,omitempty"`
	MaxInitialAgeSec int            `json:"max_initial_age_sec,omitempty"`
	Ego              EgoConfig                      `json:"ego"`
	Targets          []TargetConfig                 `json:"targets"`
	Registry         connector.RegistryClientConfig `json:"registry,omitempty"`
}

func (c Config) IsDryRun() bool {
	if c.DryRun == nil {
		return true
	}
	return *c.DryRun
}

func (c Config) IsAutoMention() bool {
	if c.AutoMention == nil {
		return true
	}
	return *c.AutoMention
}

func DecodeConfig(data []byte) (Config, error) {
	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode v2ex connector config: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return Config{}, fmt.Errorf("decode v2ex connector config: trailing content")
	}
	if err := cfg.NormalizeAndValidate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return DecodeConfig(data)
}

func (c *Config) NormalizeAndValidate() error {
	if err := connector.ValidateLoopbackAddr(c.ListenAddr); err != nil {
		return err
	}
	if err := connector.ValidateLoopbackURL(c.RelayURL); err != nil {
		return err
	}
	if strings.TrimSpace(c.StateDir) == "" {
		return fmt.Errorf("state_dir is required")
	}
	if c.DryRun == nil {
		c.DryRun = connector.BoolPtr(true)
	}
	if c.AutoMention == nil {
		c.AutoMention = connector.BoolPtr(true)
	}
	if c.AccountID == "" {
		c.AccountID = "v2ex-ego"
	}
	if !contracts.ValidID(c.AccountID) {
		return fmt.Errorf("invalid account_id %q", c.AccountID)
	}
	if c.PollIntervalSec == 0 {
		c.PollIntervalSec = 60
	}
	if c.PollIntervalSec < 10 || c.PollIntervalSec > 3600 {
		return fmt.Errorf("poll_interval_sec must be 10..3600")
	}
	if c.MaxInitialAgeSec == 0 {
		c.MaxInitialAgeSec = 300
	}
	if c.MaxInitialAgeSec < 1 || c.MaxInitialAgeSec > 86400 {
		return fmt.Errorf("max_initial_age_sec must be 1..86400")
	}
	if c.Ego.TaskSpace == "" {
		c.Ego.TaskSpace = "jinx-v2ex-comments"
	}
	if c.Ego.TimeoutSec == 0 {
		c.Ego.TimeoutSec = 45
	}
	if c.Ego.TimeoutSec < 5 || c.Ego.TimeoutSec > 180 {
		return fmt.Errorf("ego.timeout_sec must be 5..180")
	}
	for i, username := range c.SelfUsernames {
		trimmed := strings.TrimSpace(username)
		if !usernamePattern.MatchString(trimmed) || !contracts.ValidID(trimmed) {
			return fmt.Errorf("invalid self_usernames[%d]", i)
		}
		c.SelfUsernames[i] = trimmed
	}
	seenTargets := make(map[string]bool)
	for i, target := range c.Targets {
		platform, local, ok := contracts.ParseContainerID(target.ContainerID)
		if !ok || platform != Platform {
			return fmt.Errorf("targets[%d]: invalid v2ex container_id %q", i, target.ContainerID)
		}
		if seenTargets[target.ContainerID] {
			return fmt.Errorf("duplicate target container_id %q", target.ContainerID)
		}
		seenTargets[target.ContainerID] = true

		match := containerTopicPattern.FindStringSubmatch(local)
		if target.TopicID == "" && match != nil {
			target.TopicID = match[1]
		}
		if !topicIDPattern.MatchString(target.TopicID) {
			return fmt.Errorf("targets[%d]: invalid topic_id %q", i, target.TopicID)
		}
		if match != nil && target.TopicID != match[1] {
			return fmt.Errorf("targets[%d]: container_id %q disagrees with topic_id %q", i, target.ContainerID, target.TopicID)
		}
		if target.PollLimit == 0 {
			target.PollLimit = 20
		}
		if target.PollLimit < 1 || target.PollLimit > 100 {
			return fmt.Errorf("targets[%d]: poll_limit must be 1..100", i)
		}
		if target.URL == "" {
			target.URL = "https://www.v2ex.com/t/" + target.TopicID
		}
		u, err := url.Parse(target.URL)
		cleanPath := strings.TrimSuffix(u.Path, "/")
		if err != nil || u.Scheme != "https" || u.User != nil ||
			(u.Hostname() != "www.v2ex.com" && u.Hostname() != "v2ex.com") ||
			cleanPath != "/t/"+target.TopicID || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("targets[%d]: url must point to https://www.v2ex.com/t/%s", i, target.TopicID)
		}
		target.URL = "https://" + u.Hostname() + "/t/" + target.TopicID
		c.Targets[i] = target
	}
	if err := connector.NormalizeRegistryClientConfig(&c.Registry); err != nil {
		return err
	}
	return nil
}
