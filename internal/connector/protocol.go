package connector

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"jinx/internal/contracts"
)

const (
	ProtocolVersion      = 1
	maxTextRunes         = 16384
	maxReplyContextBytes = 8 << 10

	DiscoverySourceStatic  = "static"
	DiscoverySourceDropIn  = "dropin"
	DiscoverySourceDynamic = "dynamic"

	HeartbeatAlive     = "alive"
	HeartbeatStale     = "stale"
	HeartbeatExpired   = "expired"
	HeartbeatUntracked = "untracked"

	DefaultHeartbeatIntervalSec = 10
	DefaultHeartbeatTTLSec      = 35
	MinHeartbeatTTLSec          = 5
	MaxHeartbeatTTLSec          = 300

	ConnectorTokenHeader = "X-JINX-Connector-Token"
)

type RegisterRequest struct {
	ProtocolVersion int            `json:"protocol_version"`
	Platform        string         `json:"platform"`
	URL             string         `json:"url"`
	InstanceID      string         `json:"instance_id,omitempty"`
	AccountID       string         `json:"account_id,omitempty"`
	ConnectorName   string         `json:"connector_name,omitempty"`
	Capabilities    []string       `json:"capabilities,omitempty"`
	Controls        []string       `json:"controls,omitempty"`
	Targets         []HealthTarget `json:"targets,omitempty"`
	HeartbeatTTLSec int            `json:"heartbeat_ttl_sec,omitempty"`
	DryRun          bool           `json:"dry_run"`
}

type RegisterResponse struct {
	OK              bool      `json:"ok"`
	Platform        string    `json:"platform,omitempty"`
	DiscoverySource string    `json:"discovery_source,omitempty"`
	HeartbeatTTLSec int       `json:"heartbeat_ttl_sec,omitempty"`
	RegisteredAt    time.Time `json:"registered_at,omitempty"`
	ExpiresAt       time.Time `json:"expires_at,omitempty"`
	Code            string    `json:"code,omitempty"`
	Error           string    `json:"error,omitempty"`
}

type HeartbeatRequest struct {
	Platform          string         `json:"platform"`
	URL               string         `json:"url"`
	InstanceID        string         `json:"instance_id,omitempty"`
	DryRun            bool           `json:"dry_run"`
	Ready             bool           `json:"ready"`
	Targets           []HealthTarget `json:"targets,omitempty"`
	LastPollAt        *time.Time     `json:"last_poll_at,omitempty"`
	LastPollErrorCode string         `json:"last_poll_error_code,omitempty"`
	HeartbeatTTLSec   int            `json:"heartbeat_ttl_sec,omitempty"`
}

type HeartbeatResponse struct {
	OK             bool      `json:"ok"`
	Platform       string    `json:"platform,omitempty"`
	HeartbeatState string    `json:"heartbeat_state,omitempty"`
	ExpiresAt      time.Time `json:"expires_at,omitempty"`
	Code           string    `json:"code,omitempty"`
	Error          string    `json:"error,omitempty"`
}

type UnregisterRequest struct {
	Platform   string `json:"platform"`
	URL        string `json:"url"`
	InstanceID string `json:"instance_id,omitempty"`
}

type RegistryEntry struct {
	Platform          string         `json:"platform"`
	URL               string         `json:"url"`
	DiscoverySource   string         `json:"discovery_source"`
	InstanceID        string         `json:"instance_id,omitempty"`
	DryRun            *bool          `json:"dry_run,omitempty"`
	Ready             *bool          `json:"ready,omitempty"`
	Capabilities      []string       `json:"capabilities,omitempty"`
	Controls          []string       `json:"controls,omitempty"`
	AccountID         string         `json:"account_id,omitempty"`
	ConnectorName     string         `json:"connector_name,omitempty"`
	Targets           []HealthTarget `json:"targets,omitempty"`
	LastPollAt        *time.Time     `json:"last_poll_at,omitempty"`
	LastPollErrorCode string         `json:"last_poll_error_code,omitempty"`
	HeartbeatState    string         `json:"heartbeat_state"`
	HeartbeatTTLSec   int            `json:"heartbeat_ttl_sec,omitempty"`
	RegisteredAt      *time.Time     `json:"registered_at,omitempty"`
	LastHeartbeatAt   *time.Time     `json:"last_heartbeat_at,omitempty"`
	ExpiresAt         *time.Time     `json:"expires_at,omitempty"`
	LastProbeAt       *time.Time     `json:"last_probe_at,omitempty"`
	LastProbeOK       *bool          `json:"last_probe_ok,omitempty"`
	LastProbeError    string         `json:"last_probe_error,omitempty"`
}

type DropInManifest struct {
	SchemaVersion int    `json:"schema_version,omitempty"`
	Platform      string `json:"platform"`
	URL           string `json:"url"`
	Enabled       *bool  `json:"enabled,omitempty"`
	DryRun        bool   `json:"dry_run,omitempty"`
	TimeoutSec    int    `json:"timeout_sec,omitempty"`
	ConnectorName string `json:"connector_name,omitempty"`
	AccountID     string `json:"account_id,omitempty"`
}

type RegistryClientConfig struct {
	URL                  string `json:"url"`
	TokenFile            string `json:"token_file,omitempty"`
	AdvertiseURL         string `json:"advertise_url,omitempty"`
	HeartbeatIntervalSec int    `json:"heartbeat_interval_sec,omitempty"`
	HeartbeatTTLSec      int    `json:"heartbeat_ttl_sec,omitempty"`
	TimeoutSec           int    `json:"timeout_sec,omitempty"`
}

func NormalizeRegistryClientConfig(cfg *RegistryClientConfig) error {
	if cfg == nil {
		return nil
	}
	cfg.URL = strings.TrimRight(strings.TrimSpace(cfg.URL), "/")
	cfg.TokenFile = strings.TrimSpace(cfg.TokenFile)
	cfg.AdvertiseURL = strings.TrimRight(strings.TrimSpace(cfg.AdvertiseURL), "/")
	if cfg.URL == "" {
		if cfg.TokenFile != "" || cfg.AdvertiseURL != "" || cfg.HeartbeatIntervalSec != 0 || cfg.HeartbeatTTLSec != 0 || cfg.TimeoutSec != 0 {
			return fmt.Errorf("registry.url is required when registry is configured")
		}
		return nil
	}
	if err := ValidateLoopbackURL(cfg.URL); err != nil {
		return fmt.Errorf("registry.url: %w", err)
	}
	if cfg.AdvertiseURL != "" {
		if err := ValidateLoopbackURL(cfg.AdvertiseURL); err != nil {
			return fmt.Errorf("registry.advertise_url: %w", err)
		}
	}
	if cfg.HeartbeatIntervalSec == 0 {
		cfg.HeartbeatIntervalSec = DefaultHeartbeatIntervalSec
	}
	if cfg.HeartbeatIntervalSec < 2 || cfg.HeartbeatIntervalSec > 120 {
		return fmt.Errorf("registry.heartbeat_interval_sec must be 2..120")
	}
	if cfg.HeartbeatTTLSec == 0 {
		cfg.HeartbeatTTLSec = DefaultHeartbeatTTLSec
	}
	if cfg.HeartbeatTTLSec < MinHeartbeatTTLSec || cfg.HeartbeatTTLSec > MaxHeartbeatTTLSec {
		return fmt.Errorf("registry.heartbeat_ttl_sec must be %d..%d", MinHeartbeatTTLSec, MaxHeartbeatTTLSec)
	}
	if cfg.HeartbeatTTLSec <= cfg.HeartbeatIntervalSec {
		return fmt.Errorf("registry.heartbeat_ttl_sec must exceed heartbeat_interval_sec")
	}
	if cfg.TimeoutSec == 0 {
		cfg.TimeoutSec = 5
	}
	if cfg.TimeoutSec < 1 || cfg.TimeoutSec > 30 {
		return fmt.Errorf("registry.timeout_sec must be 1..30")
	}
	return nil
}

type HealthTarget struct {
	ContainerID string `json:"container_id"`
	Label       string `json:"label,omitempty"`
	Enabled     bool   `json:"enabled"`
}

type HealthResponse struct {
	Status            string         `json:"status"`
	ProtocolVersion   int            `json:"protocol_version"`
	Platform          string         `json:"platform"`
	Ready             bool           `json:"ready"`
	DryRun            bool           `json:"dry_run"`
	Capabilities      []string       `json:"capabilities"`
	Controls          []string       `json:"controls,omitempty"`
	Targets           []HealthTarget `json:"targets,omitempty"`
	LastPollAt        *time.Time     `json:"last_poll_at,omitempty"`
	LastPollErrorCode string         `json:"last_poll_error_code,omitempty"`
	AccountID         string         `json:"account_id,omitempty"`
	ConnectorName     string         `json:"connector_name,omitempty"`
	Reason            string         `json:"reason,omitempty"`
}

type DryRunControlRequest struct {
	DryRun    bool   `json:"dry_run"`
	RequestID string `json:"request_id"`
}

type DryRunControlResponse struct {
	OK        bool   `json:"ok"`
	Platform  string `json:"platform"`
	DryRun    bool   `json:"dry_run"`
	Persisted bool   `json:"persisted"`
	RequestID string `json:"request_id"`
}

type ActionTarget struct {
	Platform       string `json:"platform"`
	AccountID      string `json:"account_id,omitempty"`
	ContainerID    string `json:"container_id"`
	ContainerLabel string `json:"container_label,omitempty"`
	ContentID      string `json:"content_id,omitempty"`
	ParentID       string `json:"parent_id,omitempty"`
	RootID         string `json:"root_id,omitempty"`
	TargetLabel    string `json:"target_label,omitempty"`
}

type ActionRequest struct {
	ProtocolVersion int             `json:"protocol_version"`
	ActionID        string          `json:"action_id"`
	RunID           string          `json:"run_id"`
	IdempotencyKey  string          `json:"idempotency_key"`
	Platform        string          `json:"platform"`
	Kind            string          `json:"kind"`
	Text            string          `json:"text"`
	ExpiresAt       string          `json:"expires_at,omitempty"`
	ExpiresAtUnixMS int64           `json:"expires_at_unix_ms,omitempty"`
	Target          ActionTarget    `json:"target"`
	ReplyContext    json.RawMessage `json:"reply_context,omitempty"`
}

type ActionResponse struct {
	ActionID   string `json:"action_id"`
	Status     string `json:"status"`
	Submitted  *bool  `json:"submitted,omitempty"`
	ExternalID string `json:"external_id,omitempty"`
	ReasonCode string `json:"reason_code,omitempty"`
	Detail     string `json:"detail,omitempty"`
}

type InboundPayload struct {
	ID           string          `json:"id"`
	Source       string          `json:"source"`
	Platform     string          `json:"platform"`
	AccountID    string          `json:"account_id,omitempty"`
	GroupID      string          `json:"group_id"`
	GroupName    string          `json:"group_name,omitempty"`
	SenderID     string          `json:"sender_id"`
	SenderName   string          `json:"sender_name,omitempty"`
	ParentID     string          `json:"parent_id,omitempty"`
	RootID       string          `json:"root_id,omitempty"`
	ReplyContext json.RawMessage `json:"reply_context,omitempty"`
	Type         int             `json:"type"`
	Content      string          `json:"content"`
	Timestamp    int64           `json:"timestamp"`
	IsEcho       bool            `json:"is_echo,omitempty"`
}

type RelayPushResponse struct {
	Status  string `json:"status"`
	ID      string `json:"id"`
	Allowed bool   `json:"allowed"`
	Action  string `json:"action"`
	Reason  string `json:"reason"`
}

func BoolPtr(v bool) *bool {
	return &v
}

func ValidateLoopbackAddr(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: %w", addr, err)
	}
	if host != "localhost" {
		ip := net.ParseIP(strings.Trim(host, "[]"))
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("listen address %q must bind to loopback", addr)
		}
	}
	return nil
}

func ValidateLoopbackURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || (u.Path != "" && u.Path != "/") {
		return fmt.Errorf("invalid service URL %q", raw)
	}
	portStr := u.Port()
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("service URL %q must specify an explicit port", raw)
	}
	hostname := strings.Trim(u.Hostname(), "[]")
	if hostname != "localhost" {
		ip := net.ParseIP(hostname)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("service URL %q must target loopback", raw)
		}
	}
	return nil
}

func validActionText(value string) bool {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > maxTextRunes {
		return false
	}
	for _, r := range value {
		if unicode.IsControl(r) && r != '\n' && r != '\t' {
			return false
		}
	}
	return true
}

func RequestExpiry(req ActionRequest) (time.Time, error) {
	if req.ExpiresAt != "" {
		parsed, err := time.Parse(time.RFC3339Nano, req.ExpiresAt)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid expires_at: %w", err)
		}
		if req.ExpiresAtUnixMS > 0 {
			msDiff := parsed.UnixMilli() - req.ExpiresAtUnixMS
			if msDiff < -1000 || msDiff > 1000 {
				return time.Time{}, fmt.Errorf("expires_at and expires_at_unix_ms disagree")
			}
		}
		return parsed, nil
	}
	if req.ExpiresAtUnixMS > 0 {
		return time.UnixMilli(req.ExpiresAtUnixMS).UTC(), nil
	}
	return time.Time{}, fmt.Errorf("action expiration is required")
}

func ValidateActionRequest(req ActionRequest, expectedPlatform contracts.Platform, allowedKinds []contracts.ActionKind) error {
	if req.ProtocolVersion < ProtocolVersion {
		return fmt.Errorf("unsupported protocol_version %d", req.ProtocolVersion)
	}
	if !contracts.ValidID(req.ActionID) || !contracts.ValidID(req.RunID) || !contracts.ValidID(req.IdempotencyKey) {
		return fmt.Errorf("invalid action identifiers")
	}
	if contracts.Platform(req.Platform) != expectedPlatform || req.Target.Platform != req.Platform {
		return fmt.Errorf("platform mismatch")
	}
	kindOK := false
	for _, allowed := range allowedKinds {
		if req.Kind == string(allowed) {
			kindOK = true
			break
		}
	}
	if !kindOK {
		return fmt.Errorf("unsupported action kind %q", req.Kind)
	}
	containerPlatform, _, ok := contracts.ParseContainerID(req.Target.ContainerID)
	if !ok || containerPlatform != expectedPlatform {
		return fmt.Errorf("invalid target container_id %q", req.Target.ContainerID)
	}
	for _, optional := range []string{req.Target.AccountID, req.Target.ContentID, req.Target.ParentID, req.Target.RootID} {
		if optional != "" && !contracts.ValidID(optional) {
			return fmt.Errorf("invalid target field identity")
		}
	}
	if req.Kind != string(contracts.ActionReplyText) && req.Target.ContentID == "" {
		return fmt.Errorf("content action requires target content_id")
	}
	if req.Kind == string(contracts.ActionDeleteContent) {
		if req.Text != "" {
			return fmt.Errorf("delete action cannot carry text")
		}
	} else {
		if strings.TrimSpace(req.Text) == "" || !validActionText(req.Text) {
			return fmt.Errorf("invalid action text")
		}
	}
	if len(req.ReplyContext) > 0 {
		if len(req.ReplyContext) > maxReplyContextBytes || !json.Valid(req.ReplyContext) {
			return fmt.Errorf("invalid reply_context")
		}
	}
	if _, err := RequestExpiry(req); err != nil {
		return err
	}
	return nil
}

func RequestDigest(req ActionRequest) string {
	var compacted json.RawMessage
	if len(req.ReplyContext) > 0 {
		var buf bytes.Buffer
		if json.Compact(&buf, req.ReplyContext) == nil {
			compacted = buf.Bytes()
		} else {
			compacted = req.ReplyContext
		}
	}
	canonical := struct {
		ActionID     string          `json:"action_id"`
		RunID        string          `json:"run_id"`
		Platform     string          `json:"platform"`
		Kind         string          `json:"kind"`
		Text         string          `json:"text"`
		Target       ActionTarget    `json:"target"`
		ReplyContext json.RawMessage `json:"reply_context,omitempty"`
	}{
		ActionID:     req.ActionID,
		RunID:        req.RunID,
		Platform:     req.Platform,
		Kind:         req.Kind,
		Text:         req.Text,
		Target:       req.Target,
		ReplyContext: compacted,
	}
	data, _ := json.Marshal(canonical)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
