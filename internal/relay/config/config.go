package config

import (
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// ServerConfig defines HTTP and network settings
type ServerConfig struct {
	ListenAddr string `json:"listen_addr"` // e.g. ":8080"
	EnableCORS bool   `json:"enable_cors"`
}

// UpstreamConfig defines connection to an upstream WebSocket service (e.g. WeFlow, WeChatBridge)
type UpstreamConfig struct {
	Enabled              bool   `json:"enabled"`
	WSURL                string `json:"ws_url"`                 // e.g. "ws://127.0.0.1:5032"
	AutoReconnect        bool   `json:"auto_reconnect"`         // true
	ReconnectIntervalSec int    `json:"reconnect_interval_sec"` // e.g. 3
}

// GroupControl defines monitoring scope and AI exposure whitelist
type GroupControl struct {
	MonitorAll              bool              `json:"monitor_all"`               // If true, monitor all incoming groups
	MonitoredGroups         []string          `json:"monitored_groups"`          // Specific groups to monitor if MonitorAll is false
	ExposedGroups           []string          `json:"exposed_groups"`            // Only these groups are relayed to AI consumers! Empty = none
	GroupAliases            map[string]string `json:"group_aliases"`             // GroupID -> Display Name map
	DropUnexposedCompletely bool              `json:"drop_unexposed_completely"` // If true, do not even store unexposed in memory buffer
}

// FilterConfig defines rules to drop unwanted raw messages
type FilterConfig struct {
	KeywordBlocklist []string `json:"keyword_blocklist"` // Words that cause message to be dropped
	RegexBlocklist   []string `json:"regex_blocklist"`   // Regular expressions
	SenderBlocklist  []string `json:"sender_blocklist"`  // Blocked sender wxid or names
	AllowedTypes     []int    `json:"allowed_types"`     // Allowed message types, empty = allow all. 1 = text, 81 = quote
	CleanContent     bool     `json:"clean_content"`     // Trim leading @mentions, extra whitespace
	IgnoreEcho       bool     `json:"ignore_echo"`       // Ignore self-sent messages
}

// Config represents full service configuration
type Config struct {
	Server       ServerConfig   `json:"server"`
	Upstream     UpstreamConfig `json:"upstream"`
	GroupControl GroupControl   `json:"group_control"`
	Filters      FilterConfig   `json:"filters"`
}

// DefaultConfig returns a sane default configuration
func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{
			ListenAddr: "127.0.0.1:8080",
			EnableCORS: false,
		},
		Upstream: UpstreamConfig{
			Enabled:              false,
			WSURL:                "ws://127.0.0.1:5032",
			AutoReconnect:        true,
			ReconnectIntervalSec: 3,
		},
		GroupControl: GroupControl{MonitorAll: false, MonitoredGroups: []string{}, ExposedGroups: []string{}, GroupAliases: map[string]string{}},
		Filters: FilterConfig{
			KeywordBlocklist: []string{
				"拍了拍",
				"[微信红包]",
				"加入了群聊",
				"移出了群聊",
				"邀请你加入了群聊",
				"撤回了一条消息",
			},
			RegexBlocklist:  []string{},
			SenderBlocklist: []string{},
			AllowedTypes:    []int{1, 81}, // Default only forward text and quote text to AI
			CleanContent:    true,
			IgnoreEcho:      false,
		},
	}
}

// Manager manages thread-safe config access and disk persistence
type Manager struct {
	filePath   string
	mu         sync.RWMutex
	cfg        Config
	generation uint64
	extensions map[string]json.RawMessage
}

// NewManager loads configuration from path or creates default if not exists
func NewManager(filePath string) (*Manager, error) {
	if filePath == "" {
		filePath = "config.json"
	}

	mgr := &Manager{
		filePath:   filePath,
		cfg:        DefaultConfig(),
		extensions: map[string]json.RawMessage{},
	}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Save default configuration
		if err := mgr.saveLocked(); err != nil {
			return nil, fmt.Errorf("failed to save initial default config: %w", err)
		}
		return mgr, nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	loaded := DefaultConfig()
	if err := json.Unmarshal(data, &loaded); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON %s: %w", filePath, err)
	}
	if err := json.Unmarshal(data, &mgr.extensions); err != nil {
		return nil, err
	}
	if mgr.extensions == nil {
		return nil, fmt.Errorf("configuration must be an object")
	}
	for _, name := range []string{"server", "upstream", "group_control", "filters"} {
		delete(mgr.extensions, name)
	}

	if err := mgr.prepare(&loaded); err != nil {
		return nil, err
	}
	mgr.cfg = loaded
	if err := mgr.saveLocked(); err != nil {
		return nil, err
	}
	return mgr, nil
}

// Get returns a clone of current config
func (m *Manager) Get() Config {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return clone(m.cfg)
}

// Update replaces configuration and persists to disk
func (m *Manager) Update(newCfg Config) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	newCfg = clone(newCfg)
	if err := m.prepare(&newCfg); err != nil {
		return err
	}
	return m.commitLocked(newCfg)
}

func (m *Manager) commitLocked(newCfg Config) error {
	return m.commitDocumentLocked(newCfg, m.extensions)
}

func (m *Manager) documentLocked(cfg Config, extensions map[string]json.RawMessage) ([]byte, error) {
	data, err := json.Marshal(cfg)
	if err != nil {
		return nil, err
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, err
	}
	for name, value := range extensions {
		document[name] = value
	}
	return json.MarshalIndent(document, "", "  ")
}

func (m *Manager) commitDocumentLocked(newCfg Config, extensions map[string]json.RawMessage) error {
	data, err := m.documentLocked(newCfg, extensions)
	if err != nil {
		return err
	}
	if err := WritePrivateJSON(m.filePath, data); err != nil {
		return err
	}
	m.cfg = newCfg
	m.extensions = extensions
	m.generation++
	return nil
}

func (m *Manager) saveLocked() error {
	data, err := m.documentLocked(m.cfg, m.extensions)
	if err != nil {
		return err
	}
	return WritePrivateJSON(m.filePath, data)
}

func clone(cfg Config) Config {
	cfg.GroupControl.MonitoredGroups = slices.Clone(cfg.GroupControl.MonitoredGroups)
	cfg.GroupControl.ExposedGroups = slices.Clone(cfg.GroupControl.ExposedGroups)
	cfg.GroupControl.GroupAliases = maps.Clone(cfg.GroupControl.GroupAliases)
	cfg.Filters.AllowedTypes = slices.Clone(cfg.Filters.AllowedTypes)
	cfg.Filters.KeywordBlocklist = slices.Clone(cfg.Filters.KeywordBlocklist)
	cfg.Filters.RegexBlocklist = slices.Clone(cfg.Filters.RegexBlocklist)
	cfg.Filters.SenderBlocklist = slices.Clone(cfg.Filters.SenderBlocklist)
	return cfg
}

func ExpandPath(path string) string {
	if strings.HasPrefix(path, "~/") {
		if dir, err := os.UserHomeDir(); err == nil {
			return filepath.Join(dir, path[2:])
		}
	}
	return path
}

// ValidateLoopbackWSURL ensures upstream WebSocket targets use loopback with an explicit port.
func ValidateLoopbackWSURL(raw string) error {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" {
		return fmt.Errorf("upstream.ws_url must be a valid ws:// or wss:// loopback URL")
	}
	portStr := u.Port()
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("upstream.ws_url must specify an explicit port")
	}
	host := strings.Trim(u.Hostname(), "[]")
	if host != "localhost" {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("upstream.ws_url must target loopback")
		}
	}
	return nil
}

func (m *Manager) prepare(cfg *Config) error {
	if cfg.Server.ListenAddr == "" {
		cfg.Server.ListenAddr = "127.0.0.1:8080"
	}
	if cfg.Upstream.Enabled {
		if err := ValidateLoopbackWSURL(cfg.Upstream.WSURL); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) GetExtension(name string) json.RawMessage {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Clone(m.extensions[name])
}

func (m *Manager) UpdateExtension(name string, value json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updateExtensionLocked(m.cfg, name, value)
}

func (m *Manager) UpdateWithExtension(cfg Config, name string, value json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cfg = clone(cfg)
	if err := m.prepare(&cfg); err != nil {
		return err
	}
	return m.updateExtensionLocked(cfg, name, value)
}

func (m *Manager) updateExtensionLocked(cfg Config, name string, value json.RawMessage) error {
	if name == "" || name == "server" || name == "upstream" || name == "group_control" || name == "filters" || !json.Valid(value) {
		return fmt.Errorf("invalid configuration extension")
	}
	extensions := maps.Clone(m.extensions)
	extensions[name] = slices.Clone(value)
	return m.commitDocumentLocked(cfg, extensions)
}

// WritePrivateJSON atomically replaces configuration without exposing partial files.
func WritePrivateJSON(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, ".wx-relay-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(file.Name(), path)
}

func MatchesGroup(list []string, id, name string) bool {
	for _, value := range list {
		value = strings.TrimSpace(value)
		if value != "" && (value == "*" || value == id || value == name) {
			return true
		}
	}
	return false
}
