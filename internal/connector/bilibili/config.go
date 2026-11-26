package bilibili

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

var (
	bvidPattern = regexp.MustCompile(`^BV[0-9A-Za-z]{6,32}$`)
	oidPattern  = regexp.MustCompile(`^[0-9]{1,24}$`)
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
	BVID        string `json:"bvid,omitempty"`
	OID         string `json:"oid,omitempty"`
	CommentType int    `json:"comment_type,omitempty"`
	URL         string `json:"url,omitempty"`
	Enabled     bool   `json:"enabled"`
	PollLimit   int    `json:"poll_limit,omitempty"`
}

type Config struct {
	ListenAddr       string         `json:"listen_addr"`
	RelayURL         string         `json:"relay_url"`
	StateDir         string         `json:"state_dir"`
	AccountID        string         `json:"account_id,omitempty"`
	SelfMIDs         []string       `json:"self_mids,omitempty"`
	DryRun           *bool          `json:"dry_run,omitempty"`
	PollIntervalSec  int            `json:"poll_interval_sec,omitempty"`
	MaxInitialAgeSec int            `json:"max_initial_age_sec,omitempty"`
	Ego              EgoConfig      `json:"ego"`
	Targets          []TargetConfig `json:"targets"`
}

func (c Config) IsDryRun() bool {
	if c.DryRun == nil {
		return true
	}
	return *c.DryRun
}

func DecodeConfig(data []byte) (Config, error) {
	var cfg Config
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode bilibili connector config: %w", err)
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
	if c.AccountID == "" {
		c.AccountID = "bilibili-ego"
	}
	if !contracts.ValidID(c.AccountID) {
		return fmt.Errorf("invalid account_id %q", c.AccountID)
	}
	if c.PollIntervalSec == 0 {
		c.PollIntervalSec = 30
	}
	if c.PollIntervalSec < 5 || c.PollIntervalSec > 3600 {
		return fmt.Errorf("poll_interval_sec must be 5..3600")
	}
	if c.MaxInitialAgeSec == 0 {
		c.MaxInitialAgeSec = 300
	}
	if c.MaxInitialAgeSec < 1 || c.MaxInitialAgeSec > 86400 {
		return fmt.Errorf("max_initial_age_sec must be 1..86400")
	}
	if c.Ego.TaskSpace == "" {
		c.Ego.TaskSpace = "jinx-bilibili-comments"
	}
	if c.Ego.TimeoutSec == 0 {
		c.Ego.TimeoutSec = 45
	}
	if c.Ego.TimeoutSec < 5 || c.Ego.TimeoutSec > 180 {
		return fmt.Errorf("ego.timeout_sec must be 5..180")
	}
	for i, mid := range c.SelfMIDs {
		trimmed := strings.TrimSpace(mid)
		if !contracts.ValidID(trimmed) {
			return fmt.Errorf("invalid self_mids[%d]", i)
		}
		c.SelfMIDs[i] = trimmed
	}
	seenTargets := make(map[string]bool)
	for i, target := range c.Targets {
		platform, local, ok := contracts.ParseContainerID(target.ContainerID)
		if !ok || platform != contracts.PlatformBilibili {
			return fmt.Errorf("targets[%d]: invalid bilibili container_id %q", i, target.ContainerID)
		}
		if seenTargets[target.ContainerID] {
			return fmt.Errorf("duplicate target container_id %q", target.ContainerID)
		}
		seenTargets[target.ContainerID] = true
		if target.BVID == "" && bvidPattern.MatchString(local) {
			target.BVID = local
		}
		if target.BVID != "" && !bvidPattern.MatchString(target.BVID) {
			return fmt.Errorf("targets[%d]: invalid bvid %q", i, target.BVID)
		}
		if target.OID != "" && !oidPattern.MatchString(target.OID) {
			return fmt.Errorf("targets[%d]: invalid oid %q", i, target.OID)
		}
		if target.BVID == "" && target.OID == "" {
			return fmt.Errorf("targets[%d]: bvid or oid is required", i)
		}
		if target.CommentType == 0 {
			target.CommentType = 1
		}
		if target.CommentType < 1 || target.CommentType > 100 {
			return fmt.Errorf("targets[%d]: invalid comment_type %d", i, target.CommentType)
		}
		if target.PollLimit == 0 {
			target.PollLimit = 20
		}
		if target.PollLimit < 1 || target.PollLimit > 50 {
			return fmt.Errorf("targets[%d]: poll_limit must be 1..50", i)
		}
		if target.URL == "" {
			if target.BVID != "" {
				target.URL = "https://www.bilibili.com/video/" + target.BVID
			} else {
				return fmt.Errorf("targets[%d]: url is required when bvid is empty", i)
			}
		}
		u, err := url.Parse(target.URL)
		if err != nil || u.Scheme != "https" || u.User != nil || (u.Hostname() != "www.bilibili.com" && u.Hostname() != "bilibili.com") {
			return fmt.Errorf("targets[%d]: url must point to https://www.bilibili.com/...", i)
		}
		c.Targets[i] = target
	}
	return nil
}
