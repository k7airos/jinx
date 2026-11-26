package config

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"jinx/internal/contracts"
)

type PublicConfig struct {
	GroupControl GroupControl `json:"group_control"`
	Filters      FilterConfig `json:"filters"`
}

type PublicSnapshot struct {
	Revision     string       `json:"revision"`
	GroupControl GroupControl `json:"group_control"`
	Filters      FilterConfig `json:"filters"`
}

var ErrRevisionConflict = errors.New("configuration revision conflict")
var ErrInvalidPublic = errors.New("invalid public configuration")

func (m *Manager) GetPublic() PublicSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.publicLocked()
}

func (m *Manager) publicLocked() PublicSnapshot {
	data, _ := m.documentLocked(m.cfg, m.extensions)
	digest := sha256.New()
	fmt.Fprintf(digest, "%d:", m.generation)
	digest.Write(data)
	cfg := clone(m.cfg)
	return PublicSnapshot{Revision: fmt.Sprintf("%x", digest.Sum(nil)), GroupControl: cfg.GroupControl, Filters: cfg.Filters}
}

func (m *Manager) UpdatePublic(public PublicConfig) (PublicSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.updatePublicLocked(public)
}

func (m *Manager) CompareAndSwap(revision string, public PublicConfig) (PublicSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if revision != m.publicLocked().Revision {
		return PublicSnapshot{}, ErrRevisionConflict
	}
	return m.updatePublicLocked(public)
}

func (m *Manager) updatePublicLocked(public PublicConfig) (PublicSnapshot, error) {
	next := clone(m.cfg)
	next.GroupControl = public.GroupControl
	next.Filters = public.Filters
	next = clone(next)
	if err := validatePublic(PublicConfig{GroupControl: next.GroupControl, Filters: next.Filters}); err != nil {
		return PublicSnapshot{}, fmt.Errorf("%w: %s", ErrInvalidPublic, err)
	}
	if err := m.commitLocked(next); err != nil {
		return PublicSnapshot{}, err
	}
	return m.publicLocked(), nil
}

func validatePublic(public PublicConfig) error {
	data, err := json.Marshal(public)
	if err != nil || len(data) > 256<<10 {
		return errors.New("public configuration must be at most 256 KiB")
	}
	for _, list := range []struct {
		name   string
		values []string
	}{
		{"monitored_groups", public.GroupControl.MonitoredGroups},
		{"exposed_groups", public.GroupControl.ExposedGroups},
	} {
		if len(list.values) > 1024 {
			return fmt.Errorf("%s must contain at most 1024 entries", list.name)
		}
		for _, value := range list.values {
			value = strings.TrimSpace(value)
			if !validLabel(value, 256) || (value != "*" && strings.ContainsAny(value, "*?")) || ((strings.Contains(value, "@") || strings.Contains(value, ":")) && !contracts.ValidRelayContainerID(value)) {
				return fmt.Errorf("%s requires group IDs, exact names, or a standalone * (at most 256 bytes)", list.name)
			}
		}
	}
	if len(public.GroupControl.GroupAliases) > 1024 {
		return errors.New("group_aliases must contain at most 1024 entries")
	}
	for id, alias := range public.GroupControl.GroupAliases {
		if len(id) > 256 || !contracts.ValidRelayContainerID(id) || !validLabel(strings.TrimSpace(alias), 256) {
			return errors.New("group_aliases requires valid group IDs and nonempty names of at most 256 bytes")
		}
	}
	for _, list := range []struct {
		name   string
		values []string
		limit  int
	}{
		{"keyword_blocklist", public.Filters.KeywordBlocklist, 1024},
		{"sender_blocklist", public.Filters.SenderBlocklist, 1024},
		{"regex_blocklist", public.Filters.RegexBlocklist, 4096},
	} {
		if len(list.values) > 1024 {
			return fmt.Errorf("%s must contain at most 1024 entries", list.name)
		}
		for _, value := range list.values {
			if strings.TrimSpace(value) == "" || len(value) > list.limit || !utf8.ValidString(value) {
				return fmt.Errorf("%s requires nonempty UTF-8 entries of at most %d bytes", list.name, list.limit)
			}
			if list.name == "regex_blocklist" {
				if _, err := regexp.Compile(strings.TrimSpace(value)); err != nil {
					return errors.New("regex_blocklist contains an invalid Go regular expression")
				}
			}
		}
	}
	if len(public.Filters.AllowedTypes) > 1024 {
		return errors.New("allowed_types must contain at most 1024 entries")
	}
	for _, messageType := range public.Filters.AllowedTypes {
		if messageType <= 0 {
			return errors.New("allowed_types requires positive integers")
		}
	}
	return nil
}

func validLabel(value string, limit int) bool {
	return value != "" && len(value) <= limit && utf8.ValidString(value) && !strings.ContainsFunc(value, unicode.IsControl)
}
