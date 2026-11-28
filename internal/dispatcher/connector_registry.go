package dispatcher

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

const (
	maxDynamicConnectors = 64
	maxRegistryBodyBytes = 64 << 10
	registryProbeEvery   = 15 * time.Second
	registryTickEvery    = 5 * time.Second
)

type registryRecord struct {
	entry      connector.RegistryEntry
	timeoutSec int
	dryRun     bool
}

type connectorRegistry struct {
	mu        sync.RWMutex
	dropInDir string
	tokenFile string
	token     string
	client    *http.Client
	now       func() time.Time
	logf      func(string, ...any)

	static  map[contracts.Platform]*registryRecord
	dropIn  map[contracts.Platform]*registryRecord
	dynamic map[contracts.Platform]*registryRecord
}

func newConnectorRegistry(dropInDir, tokenFile string, staticEndpoints map[string]ActionEndpointConfig, logf func(string, ...any)) (*connectorRegistry, error) {
	if logf == nil {
		logf = log.Printf
	}
	reg := &connectorRegistry{
		dropInDir: strings.TrimSpace(dropInDir),
		tokenFile: strings.TrimSpace(tokenFile),
		client: &http.Client{
			Timeout: 3 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now:     time.Now,
		logf:    logf,
		static:  make(map[contracts.Platform]*registryRecord),
		dropIn:  make(map[contracts.Platform]*registryRecord),
		dynamic: make(map[contracts.Platform]*registryRecord),
	}
	if reg.tokenFile != "" {
		token, err := ensureOrLoadConnectorToken(reg.tokenFile)
		if err != nil {
			return nil, err
		}
		reg.token = token
	}
	reg.setStaticEndpoints(staticEndpoints)
	if reg.dropInDir != "" {
		if err := reg.ScanDropIn(); err != nil {
			return nil, err
		}
	}
	return reg, nil
}

func ensureOrLoadConnectorToken(tokenFile string) (string, error) {
	clean := filepath.Clean(tokenFile)
	info, err := os.Lstat(clean)
	if os.IsNotExist(err) {
		if mkErr := os.MkdirAll(filepath.Dir(clean), 0700); mkErr != nil {
			return "", fmt.Errorf("create connector token dir: %w", mkErr)
		}
		var raw [32]byte
		if _, randErr := rand.Read(raw[:]); randErr != nil {
			return "", fmt.Errorf("generate connector token: %w", randErr)
		}
		token := hex.EncodeToString(raw[:])
		f, openErr := os.OpenFile(clean, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
		if openErr != nil {
			if !os.IsExist(openErr) {
				return "", fmt.Errorf("create connector token file: %w", openErr)
			}
			info, err = os.Lstat(clean)
		} else {
			_, writeErr := f.WriteString(token + "\n")
			syncErr := f.Sync()
			closeErr := f.Close()
			if writeErr != nil || syncErr != nil || closeErr != nil {
				_ = os.Remove(clean)
				return "", fmt.Errorf("write connector token file: %w", errors.Join(writeErr, syncErr, closeErr))
			}
			return token, nil
		}
	}
	if err != nil {
		return "", fmt.Errorf("stat connector token file: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", fmt.Errorf("connector token file must be a regular non-symlink file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("connector token file %s must have mode 0600 or stricter (got %04o)", clean, info.Mode().Perm())
	}
	data, err := os.ReadFile(clean)
	if err != nil || len(data) > 4096 {
		return "", fmt.Errorf("read connector token file: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("connector token file %s is empty", clean)
	}
	return token, nil
}

func (r *connectorRegistry) setStaticEndpoints(endpoints map[string]ActionEndpointConfig) {
	r.mu.Lock()
	defer r.mu.Unlock()
	next := make(map[contracts.Platform]*registryRecord, len(endpoints))
	for rawPlatform, ep := range endpoints {
		platform := contracts.Platform(rawPlatform)
		normURL := strings.TrimRight(strings.TrimSpace(ep.URL), "/")
		rec := &registryRecord{
			entry: connector.RegistryEntry{
				Platform:        rawPlatform,
				URL:             normURL,
				DiscoverySource: connector.DiscoverySourceStatic,
				HeartbeatState:  connector.HeartbeatUntracked,
			},
			timeoutSec: ep.TimeoutSec,
			dryRun:     ep.DryRun,
		}
		if prev, ok := r.static[platform]; ok && prev.entry.URL == normURL {
			preserveRuntimeState(rec, prev)
		}
		next[platform] = rec
	}
	r.static = next
}

func preserveRuntimeState(dst, src *registryRecord) {
	dst.entry.InstanceID = src.entry.InstanceID
	dst.entry.DryRun = src.entry.DryRun
	dst.entry.Ready = src.entry.Ready
	dst.entry.Capabilities = append([]string(nil), src.entry.Capabilities...)
	dst.entry.Controls = append([]string(nil), src.entry.Controls...)
	dst.entry.AccountID = src.entry.AccountID
	dst.entry.ConnectorName = src.entry.ConnectorName
	dst.entry.Targets = append([]connector.HealthTarget(nil), src.entry.Targets...)
	dst.entry.LastPollAt = src.entry.LastPollAt
	dst.entry.LastPollErrorCode = src.entry.LastPollErrorCode
	dst.entry.HeartbeatState = src.entry.HeartbeatState
	dst.entry.HeartbeatTTLSec = src.entry.HeartbeatTTLSec
	dst.entry.RegisteredAt = src.entry.RegisteredAt
	dst.entry.LastHeartbeatAt = src.entry.LastHeartbeatAt
	dst.entry.ExpiresAt = src.entry.ExpiresAt
	dst.entry.LastProbeAt = src.entry.LastProbeAt
	dst.entry.LastProbeOK = src.entry.LastProbeOK
	dst.entry.LastProbeError = src.entry.LastProbeError
}

func (r *connectorRegistry) ScanDropIn() error {
	if r.dropInDir == "" {
		return nil
	}
	info, err := os.Lstat(r.dropInDir)
	if os.IsNotExist(err) {
		r.mu.Lock()
		r.dropIn = make(map[contracts.Platform]*registryRecord)
		r.mu.Unlock()
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat connectors dir: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("connectors dir must be a real directory")
	}
	entries, err := os.ReadDir(r.dropInDir)
	if err != nil {
		return fmt.Errorf("read connectors dir: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	parsed := make(map[contracts.Platform]*registryRecord)
	for _, de := range entries {
		name := de.Name()
		if strings.HasPrefix(name, ".") || !strings.HasSuffix(name, ".json") {
			continue
		}
		path := filepath.Join(r.dropInDir, name)
		fi, err := os.Lstat(path)
		if err != nil || fi.Mode()&os.ModeSymlink != 0 || !fi.Mode().IsRegular() {
			r.logf("[registry] skip non-regular drop-in %s", name)
			continue
		}
		if fi.Mode().Perm()&0o022 != 0 {
			r.logf("[registry] skip group/world-writable drop-in %s (mode %04o)", name, fi.Mode().Perm())
			continue
		}
		if fi.Size() > 64<<10 {
			r.logf("[registry] skip oversized drop-in %s", name)
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			r.logf("[registry] read drop-in %s: %v", name, err)
			continue
		}
		var manifest connector.DropInManifest
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&manifest); err != nil {
			r.logf("[registry] invalid drop-in %s: %v", name, err)
			continue
		}
		if err := dec.Decode(new(any)); err != io.EOF {
			r.logf("[registry] trailing content in drop-in %s", name)
			continue
		}
		if manifest.SchemaVersion != 0 && manifest.SchemaVersion != 1 {
			r.logf("[registry] unsupported schema_version in drop-in %s", name)
			continue
		}
		if manifest.Enabled != nil && !*manifest.Enabled {
			continue
		}
		platform := contracts.Platform(strings.TrimSpace(manifest.Platform))
		if !contracts.IsNamespacedPlatform(platform) {
			r.logf("[registry] invalid platform %q in drop-in %s", manifest.Platform, name)
			continue
		}
		if err := validateLoopbackOrigin(strings.TrimSpace(manifest.URL)); err != nil {
			r.logf("[registry] invalid url in drop-in %s: %v", name, err)
			continue
		}
		timeoutSec := manifest.TimeoutSec
		if timeoutSec == 0 {
			timeoutSec = 30
		}
		if timeoutSec < 1 || timeoutSec > 120 {
			r.logf("[registry] invalid timeout_sec in drop-in %s", name)
			continue
		}
		if _, dup := parsed[platform]; dup {
			r.logf("[registry] duplicate drop-in for platform %s (%s skipped)", platform, name)
			continue
		}
		normURL := strings.TrimRight(strings.TrimSpace(manifest.URL), "/")
		parsed[platform] = &registryRecord{
			entry: connector.RegistryEntry{
				Platform:        string(platform),
				URL:             normURL,
				DiscoverySource: connector.DiscoverySourceDropIn,
				AccountID:       manifest.AccountID,
				ConnectorName:   manifest.ConnectorName,
				HeartbeatState:  connector.HeartbeatUntracked,
			},
			timeoutSec: timeoutSec,
			dryRun:     manifest.DryRun,
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for platform, rec := range parsed {
		if prev, ok := r.dropIn[platform]; ok && prev.entry.URL == rec.entry.URL {
			preserveRuntimeState(rec, prev)
			if rec.entry.AccountID == "" {
				rec.entry.AccountID = prev.entry.AccountID
			}
			if rec.entry.ConnectorName == "" {
				rec.entry.ConnectorName = prev.entry.ConnectorName
			}
		} else if dyn, ok := r.dynamic[platform]; ok && dyn.entry.URL == rec.entry.URL {
			// Promote dynamic heartbeat state into drop-in record cleanly.
			preserveRuntimeState(rec, dyn)
			delete(r.dynamic, platform)
		}
	}
	r.dropIn = parsed
	return nil
}

func (r *connectorRegistry) authorize(provided string) (int, string, string) {
	if r.token == "" {
		return http.StatusNotImplemented, "registration_disabled", "connector registration requires -connector-token-file"
	}
	trimmed := strings.TrimSpace(provided)
	if trimmed == "" || subtle.ConstantTimeCompare([]byte(trimmed), []byte(r.token)) != 1 {
		return http.StatusUnauthorized, "unauthorized", "invalid or missing connector token"
	}
	return 0, "", ""
}

func (r *connectorRegistry) probeHealth(ctx context.Context, baseURL string, expectedPlatform contracts.Platform) (connector.HealthResponse, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/health", nil)
	if err != nil {
		return connector.HealthResponse{}, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return connector.HealthResponse{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<10))
	if err != nil {
		return connector.HealthResponse{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return connector.HealthResponse{}, fmt.Errorf("health status %d", resp.StatusCode)
	}
	var health connector.HealthResponse
	if err := json.Unmarshal(raw, &health); err != nil {
		return connector.HealthResponse{}, fmt.Errorf("invalid health json")
	}
	if health.ProtocolVersion < connector.ProtocolVersion {
		return connector.HealthResponse{}, fmt.Errorf("unsupported protocol_version %d", health.ProtocolVersion)
	}
	if expectedPlatform != "" && contracts.Platform(health.Platform) != expectedPlatform {
		return connector.HealthResponse{}, fmt.Errorf("health platform %q does not match %q", health.Platform, expectedPlatform)
	}
	return health, nil
}

func cloneRegistryEntry(src connector.RegistryEntry) connector.RegistryEntry {
	out := src
	out.Capabilities = append([]string(nil), src.Capabilities...)
	out.Controls = append([]string(nil), src.Controls...)
	out.Targets = append([]connector.HealthTarget(nil), src.Targets...)
	if src.DryRun != nil {
		v := *src.DryRun
		out.DryRun = &v
	}
	if src.Ready != nil {
		v := *src.Ready
		out.Ready = &v
	}
	if src.LastPollAt != nil {
		v := *src.LastPollAt
		out.LastPollAt = &v
	}
	if src.RegisteredAt != nil {
		v := *src.RegisteredAt
		out.RegisteredAt = &v
	}
	if src.LastHeartbeatAt != nil {
		v := *src.LastHeartbeatAt
		out.LastHeartbeatAt = &v
	}
	if src.ExpiresAt != nil {
		v := *src.ExpiresAt
		out.ExpiresAt = &v
	}
	if src.LastProbeAt != nil {
		v := *src.LastProbeAt
		out.LastProbeAt = &v
	}
	if src.LastProbeOK != nil {
		v := *src.LastProbeOK
		out.LastProbeOK = &v
	}
	return out
}

func (r *connectorRegistry) findMatchingRecordLocked(platform contracts.Platform, normURL string) (*registryRecord, bool) {
	if rec, ok := r.static[platform]; ok {
		return rec, rec.entry.URL == normURL
	}
	if rec, ok := r.dropIn[platform]; ok {
		return rec, rec.entry.URL == normURL
	}
	if rec, ok := r.dynamic[platform]; ok {
		return rec, rec.entry.URL == normURL
	}
	return nil, false
}

func (r *connectorRegistry) Register(ctx context.Context, token string, req connector.RegisterRequest) (connector.RegisterResponse, int) {
	if status, code, msg := r.authorize(token); status != 0 {
		return connector.RegisterResponse{OK: false, Code: code, Error: msg}, status
	}
	if req.ProtocolVersion < connector.ProtocolVersion {
		return connector.RegisterResponse{OK: false, Code: "validation", Error: "protocol_version >= 1 is required"}, http.StatusBadRequest
	}
	platform := contracts.Platform(strings.TrimSpace(req.Platform))
	if !contracts.IsNamespacedPlatform(platform) {
		return connector.RegisterResponse{OK: false, Code: "validation", Error: "invalid namespaced platform"}, http.StatusBadRequest
	}
	if err := validateLoopbackOrigin(strings.TrimSpace(req.URL)); err != nil {
		return connector.RegisterResponse{OK: false, Code: "validation", Error: err.Error()}, http.StatusBadRequest
	}
	normURL := strings.TrimRight(strings.TrimSpace(req.URL), "/")
	ttl := req.HeartbeatTTLSec
	if ttl == 0 {
		ttl = connector.DefaultHeartbeatTTLSec
	}
	if ttl < connector.MinHeartbeatTTLSec || ttl > connector.MaxHeartbeatTTLSec {
		return connector.RegisterResponse{OK: false, Code: "validation", Error: "heartbeat_ttl_sec out of range"}, http.StatusBadRequest
	}

	var existingDynamicURL string
	r.mu.RLock()
	if rec, ok := r.static[platform]; ok && rec.entry.URL != normURL {
		r.mu.RUnlock()
		return connector.RegisterResponse{OK: false, Code: "platform_conflict", Error: "platform is statically configured at a different endpoint"}, http.StatusConflict
	}
	if rec, ok := r.dropIn[platform]; ok && rec.entry.URL != normURL {
		r.mu.RUnlock()
		return connector.RegisterResponse{OK: false, Code: "platform_conflict", Error: "platform is configured via connectors.d at a different endpoint"}, http.StatusConflict
	}
	if rec, ok := r.dynamic[platform]; ok && rec.entry.URL != normURL && rec.entry.HeartbeatState == connector.HeartbeatAlive {
		existingDynamicURL = rec.entry.URL
	}
	r.mu.RUnlock()

	if existingDynamicURL != "" {
		if _, err := r.probeHealth(ctx, existingDynamicURL, platform); err == nil {
			return connector.RegisterResponse{OK: false, Code: "platform_conflict", Error: "another live connector is registered for this platform"}, http.StatusConflict
		}
	}

	health, err := r.probeHealth(ctx, normURL, platform)
	if err != nil {
		return connector.RegisterResponse{OK: false, Code: "health_verification_failed", Error: err.Error()}, http.StatusBadGateway
	}

	now := r.now().UTC()
	expiresAt := now.Add(time.Duration(ttl) * time.Second)

	r.mu.Lock()
	defer r.mu.Unlock()
	if rec, ok := r.static[platform]; ok && rec.entry.URL != normURL {
		return connector.RegisterResponse{OK: false, Code: "platform_conflict", Error: "platform is statically configured at a different endpoint"}, http.StatusConflict
	}
	if rec, ok := r.dropIn[platform]; ok && rec.entry.URL != normURL {
		return connector.RegisterResponse{OK: false, Code: "platform_conflict", Error: "platform is configured via connectors.d at a different endpoint"}, http.StatusConflict
	}

	var target *registryRecord
	if rec, ok := r.static[platform]; ok && rec.entry.URL == normURL {
		target = rec
	} else if rec, ok := r.dropIn[platform]; ok && rec.entry.URL == normURL {
		target = rec
	} else {
		if existing, ok := r.dynamic[platform]; ok {
			target = existing
		} else {
			if len(r.dynamic) >= maxDynamicConnectors {
				return connector.RegisterResponse{OK: false, Code: "capacity_exceeded", Error: "dynamic connector limit reached"}, http.StatusConflict
			}
			target = &registryRecord{
				entry: connector.RegistryEntry{
					Platform:        string(platform),
					URL:             normURL,
					DiscoverySource: connector.DiscoverySourceDynamic,
				},
				timeoutSec: 30,
				dryRun:     false,
			}
			r.dynamic[platform] = target
		}
		target.entry.URL = normURL
		target.entry.DiscoverySource = connector.DiscoverySourceDynamic
	}

	registeredAt := now
	if target.entry.RegisteredAt != nil && target.entry.InstanceID == req.InstanceID && req.InstanceID != "" {
		registeredAt = *target.entry.RegisteredAt
	}
	dryRun := health.DryRun
	ready := health.Ready
	probeOK := true

	target.entry.InstanceID = req.InstanceID
	target.entry.DryRun = &dryRun
	target.entry.Ready = &ready
	if len(health.Capabilities) > 0 {
		target.entry.Capabilities = append([]string(nil), health.Capabilities...)
	} else {
		target.entry.Capabilities = append([]string(nil), req.Capabilities...)
	}
	if len(health.Controls) > 0 {
		target.entry.Controls = append([]string(nil), health.Controls...)
	} else {
		target.entry.Controls = append([]string(nil), req.Controls...)
	}
	if health.AccountID != "" {
		target.entry.AccountID = health.AccountID
	} else if req.AccountID != "" {
		target.entry.AccountID = req.AccountID
	}
	if health.ConnectorName != "" {
		target.entry.ConnectorName = health.ConnectorName
	} else if req.ConnectorName != "" {
		target.entry.ConnectorName = req.ConnectorName
	}
	if len(health.Targets) > 0 {
		target.entry.Targets = append([]connector.HealthTarget(nil), health.Targets...)
	} else {
		target.entry.Targets = append([]connector.HealthTarget(nil), req.Targets...)
	}
	target.entry.LastPollAt = health.LastPollAt
	target.entry.LastPollErrorCode = health.LastPollErrorCode
	target.entry.HeartbeatState = connector.HeartbeatAlive
	target.entry.HeartbeatTTLSec = ttl
	target.entry.RegisteredAt = &registeredAt
	target.entry.LastHeartbeatAt = &now
	target.entry.ExpiresAt = &expiresAt
	target.entry.LastProbeAt = &now
	target.entry.LastProbeOK = &probeOK
	target.entry.LastProbeError = ""

	return connector.RegisterResponse{
		OK:              true,
		Platform:        string(platform),
		DiscoverySource: target.entry.DiscoverySource,
		HeartbeatTTLSec: ttl,
		RegisteredAt:    registeredAt,
		ExpiresAt:       expiresAt,
	}, http.StatusOK
}

func (r *connectorRegistry) Heartbeat(token string, req connector.HeartbeatRequest) (connector.HeartbeatResponse, int) {
	if status, code, msg := r.authorize(token); status != 0 {
		return connector.HeartbeatResponse{OK: false, Code: code, Error: msg}, status
	}
	platform := contracts.Platform(strings.TrimSpace(req.Platform))
	if !contracts.IsNamespacedPlatform(platform) {
		return connector.HeartbeatResponse{OK: false, Code: "validation", Error: "invalid namespaced platform"}, http.StatusBadRequest
	}
	if err := validateLoopbackOrigin(strings.TrimSpace(req.URL)); err != nil {
		return connector.HeartbeatResponse{OK: false, Code: "validation", Error: err.Error()}, http.StatusBadRequest
	}
	normURL := strings.TrimRight(strings.TrimSpace(req.URL), "/")

	r.mu.Lock()
	defer r.mu.Unlock()
	rec, sameURL := r.findMatchingRecordLocked(platform, normURL)
	if rec == nil || !sameURL {
		return connector.HeartbeatResponse{OK: false, Code: "not_registered", Error: "connector is not registered"}, http.StatusNotFound
	}
	if rec.entry.DiscoverySource == connector.DiscoverySourceDynamic && rec.entry.HeartbeatState == "" {
		return connector.HeartbeatResponse{OK: false, Code: "not_registered", Error: "connector is not registered"}, http.StatusNotFound
	}
	if rec.entry.InstanceID != "" && req.InstanceID != "" && rec.entry.InstanceID != req.InstanceID {
		return connector.HeartbeatResponse{OK: false, Code: "instance_mismatch", Error: "connector instance_id does not match active registration"}, http.StatusConflict
	}

	now := r.now().UTC()
	ttl := req.HeartbeatTTLSec
	if ttl == 0 {
		ttl = rec.entry.HeartbeatTTLSec
	}
	if ttl == 0 {
		ttl = connector.DefaultHeartbeatTTLSec
	}
	if ttl < connector.MinHeartbeatTTLSec || ttl > connector.MaxHeartbeatTTLSec {
		return connector.HeartbeatResponse{OK: false, Code: "validation", Error: "heartbeat_ttl_sec out of range"}, http.StatusBadRequest
	}
	expiresAt := now.Add(time.Duration(ttl) * time.Second)
	dryRun := req.DryRun
	ready := req.Ready

	if rec.entry.RegisteredAt == nil {
		rec.entry.RegisteredAt = &now
	}
	if rec.entry.InstanceID == "" && req.InstanceID != "" {
		rec.entry.InstanceID = req.InstanceID
	}
	rec.entry.DryRun = &dryRun
	rec.entry.Ready = &ready
	if req.Targets != nil {
		rec.entry.Targets = append([]connector.HealthTarget(nil), req.Targets...)
	}
	if req.LastPollAt != nil {
		rec.entry.LastPollAt = req.LastPollAt
	}
	rec.entry.LastPollErrorCode = req.LastPollErrorCode
	rec.entry.HeartbeatState = connector.HeartbeatAlive
	rec.entry.HeartbeatTTLSec = ttl
	rec.entry.LastHeartbeatAt = &now
	rec.entry.ExpiresAt = &expiresAt

	return connector.HeartbeatResponse{
		OK:             true,
		Platform:       string(platform),
		HeartbeatState: connector.HeartbeatAlive,
		ExpiresAt:      expiresAt,
	}, http.StatusOK
}

func (r *connectorRegistry) Unregister(token string, req connector.UnregisterRequest) (map[string]any, int) {
	if status, code, msg := r.authorize(token); status != 0 {
		return map[string]any{"ok": false, "code": code, "error": msg}, status
	}
	platform := contracts.Platform(strings.TrimSpace(req.Platform))
	if !contracts.IsNamespacedPlatform(platform) {
		return map[string]any{"ok": false, "code": "validation", "error": "invalid namespaced platform"}, http.StatusBadRequest
	}
	if err := validateLoopbackOrigin(strings.TrimSpace(req.URL)); err != nil {
		return map[string]any{"ok": false, "code": "validation", "error": err.Error()}, http.StatusBadRequest
	}
	normURL := strings.TrimRight(strings.TrimSpace(req.URL), "/")

	r.mu.Lock()
	defer r.mu.Unlock()
	rec, sameURL := r.findMatchingRecordLocked(platform, normURL)
	if rec == nil || !sameURL {
		return map[string]any{"ok": false, "code": "not_registered", "error": "connector is not registered"}, http.StatusNotFound
	}
	if rec.entry.InstanceID != "" && req.InstanceID != "" && rec.entry.InstanceID != req.InstanceID {
		return map[string]any{"ok": false, "code": "instance_mismatch", "error": "connector instance_id mismatch"}, http.StatusConflict
	}
	if rec.entry.DiscoverySource == connector.DiscoverySourceDynamic {
		delete(r.dynamic, platform)
	} else {
		rec.entry.InstanceID = ""
		rec.entry.HeartbeatState = connector.HeartbeatUntracked
		rec.entry.HeartbeatTTLSec = 0
		rec.entry.RegisteredAt = nil
		rec.entry.LastHeartbeatAt = nil
		rec.entry.ExpiresAt = nil
	}
	return map[string]any{"ok": true, "platform": string(platform)}, http.StatusOK
}

func (r *connectorRegistry) Tick(ctx context.Context) {
	if err := r.ScanDropIn(); err != nil {
		r.logf("[registry] scan connectors.d: %v", err)
	}

	type probeTarget struct {
		source   string
		platform contracts.Platform
		url      string
	}
	var toProbe []probeTarget
	now := r.now().UTC()

	r.mu.Lock()
	processMap := func(source string, m map[contracts.Platform]*registryRecord) {
		for platform, rec := range m {
			needProbe := false
			if rec.entry.ExpiresAt != nil {
				ttlDur := time.Duration(rec.entry.HeartbeatTTLSec) * time.Second
				if ttlDur <= 0 {
					ttlDur = connector.DefaultHeartbeatTTLSec * time.Second
				}
				expires := *rec.entry.ExpiresAt
				switch {
				case !now.After(expires):
					rec.entry.HeartbeatState = connector.HeartbeatAlive
				case !now.After(expires.Add(ttlDur)):
					if rec.entry.HeartbeatState != connector.HeartbeatStale {
						rec.entry.HeartbeatState = connector.HeartbeatStale
						needProbe = true
					}
				default:
					if source == connector.DiscoverySourceDynamic && now.After(expires.Add(10*ttlDur)) {
						delete(m, platform)
						continue
					}
					if rec.entry.HeartbeatState != connector.HeartbeatExpired {
						rec.entry.HeartbeatState = connector.HeartbeatExpired
						needProbe = true
					}
				}
			}
			if !needProbe && (rec.entry.HeartbeatState == connector.HeartbeatUntracked || rec.entry.HeartbeatState == connector.HeartbeatExpired || rec.entry.HeartbeatState == connector.HeartbeatStale) {
				if rec.entry.LastProbeAt == nil || !now.Before(rec.entry.LastProbeAt.Add(registryProbeEvery)) {
					needProbe = true
				}
			}
			if needProbe {
				toProbe = append(toProbe, probeTarget{source: source, platform: platform, url: rec.entry.URL})
			}
		}
	}
	processMap(connector.DiscoverySourceStatic, r.static)
	processMap(connector.DiscoverySourceDropIn, r.dropIn)
	processMap(connector.DiscoverySourceDynamic, r.dynamic)
	r.mu.Unlock()

	for _, item := range toProbe {
		if ctx.Err() != nil {
			return
		}
		health, err := r.probeHealth(ctx, item.url, item.platform)
		probeTime := r.now().UTC()

		r.mu.Lock()
		var target *registryRecord
		switch item.source {
		case connector.DiscoverySourceStatic:
			target = r.static[item.platform]
		case connector.DiscoverySourceDropIn:
			target = r.dropIn[item.platform]
		case connector.DiscoverySourceDynamic:
			target = r.dynamic[item.platform]
		}
		if target != nil && target.entry.URL == item.url {
			target.entry.LastProbeAt = &probeTime
			ok := err == nil
			target.entry.LastProbeOK = &ok
			if err != nil {
				target.entry.LastProbeError = "health_probe_failed"
			} else {
				target.entry.LastProbeError = ""
				dry := health.DryRun
				ready := health.Ready
				target.entry.DryRun = &dry
				target.entry.Ready = &ready
				if len(health.Capabilities) > 0 {
					target.entry.Capabilities = append([]string(nil), health.Capabilities...)
				}
				if len(health.Controls) > 0 {
					target.entry.Controls = append([]string(nil), health.Controls...)
				}
				if health.AccountID != "" {
					target.entry.AccountID = health.AccountID
				}
				if health.ConnectorName != "" {
					target.entry.ConnectorName = health.ConnectorName
				}
				if health.Targets != nil {
					target.entry.Targets = append([]connector.HealthTarget(nil), health.Targets...)
				}
				target.entry.LastPollAt = health.LastPollAt
				target.entry.LastPollErrorCode = health.LastPollErrorCode
			}
		}
		r.mu.Unlock()
	}
}

func (r *connectorRegistry) effectiveMapLocked() map[contracts.Platform]*registryRecord {
	merged := make(map[contracts.Platform]*registryRecord)
	for p, rec := range r.dynamic {
		merged[p] = rec
	}
	for p, rec := range r.dropIn {
		merged[p] = rec
	}
	for p, rec := range r.static {
		merged[p] = rec
	}
	return merged
}

func (r *connectorRegistry) Resolve(containerID string) (contracts.Platform, ActionEndpointConfig, *connector.RegistryEntry, bool) {
	platform, _, ok := contracts.ParseContainerID(containerID)
	if !ok || platform == contracts.PlatformWeChat {
		return platform, ActionEndpointConfig{}, nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	var rec *registryRecord
	if s, exists := r.static[platform]; exists {
		rec = s
	} else if d, exists := r.dropIn[platform]; exists {
		rec = d
	} else if dyn, exists := r.dynamic[platform]; exists {
		rec = dyn
	}
	if rec == nil {
		return platform, ActionEndpointConfig{}, nil, false
	}
	ep := ActionEndpointConfig{
		URL:        rec.entry.URL,
		DryRun:     rec.dryRun,
		TimeoutSec: rec.timeoutSec,
	}
	if ep.TimeoutSec == 0 {
		ep.TimeoutSec = 30
	}
	entryCopy := cloneRegistryEntry(rec.entry)
	return platform, ep, &entryCopy, true
}

func (d *Dispatcher) resolveActionEndpoint(cfg Config, containerID string) (contracts.Platform, ActionEndpointConfig, *connector.RegistryEntry, bool) {
	if d != nil && d.connectors != nil {
		return d.connectors.Resolve(containerID)
	}
	platform, ep, ok := cfg.actionEndpoint(containerID)
	return platform, ep, nil, ok
}

func (d *Dispatcher) endpointDryRun(cfg Config, containerID string) bool {
	_, ep, _, ok := d.resolveActionEndpoint(cfg, containerID)
	return ok && ep.DryRun
}

func (r *connectorRegistry) List() []connector.RegistryEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	merged := r.effectiveMapLocked()
	items := make([]connector.RegistryEntry, 0, len(merged))
	for _, rec := range merged {
		items = append(items, cloneRegistryEntry(rec.entry))
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Platform < items[j].Platform
	})
	return items
}

func (d *Dispatcher) startConnectorWatchdog() {
	if d.connectors == nil {
		return
	}
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		d.connectors.Tick(d.ctx)
		ticker := time.NewTicker(registryTickEvery)
		defer ticker.Stop()
		for {
			select {
			case <-d.ctx.Done():
				return
			case <-ticker.C:
				d.connectors.Tick(d.ctx)
			}
		}
	}()
}

func (d *Dispatcher) handleConnectorsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		traceJSON(w, http.StatusMethodNotAllowed, map[string]string{"code": "method_not_allowed", "error": "GET method required"})
		return
	}
	var items []connector.RegistryEntry
	now := time.Now().UTC()
	if d.connectors != nil {
		items = d.connectors.List()
		now = d.connectors.now().UTC()
	} else {
		items = []connector.RegistryEntry{}
	}
	traceJSON(w, http.StatusOK, map[string]any{
		"connectors":  items,
		"observed_at": now,
	})
}

func decodeRegistryBody(w http.ResponseWriter, r *http.Request, dst any) (int, string, string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		return http.StatusMethodNotAllowed, "method_not_allowed", "POST method required"
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return http.StatusUnsupportedMediaType, "unsupported_media_type", "application/json is required"
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxRegistryBodyBytes+1))
	if err != nil || len(raw) > maxRegistryBodyBytes {
		return http.StatusBadRequest, "invalid_json", "request body unreadable or too large"
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return http.StatusBadRequest, "invalid_json", "invalid JSON or unknown field"
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return http.StatusBadRequest, "invalid_json", "single JSON object required"
	}
	return 0, "", ""
}

func (d *Dispatcher) handleConnectorRegister(w http.ResponseWriter, r *http.Request) {
	var req connector.RegisterRequest
	if status, code, msg := decodeRegistryBody(w, r, &req); status != 0 {
		traceJSON(w, status, connector.RegisterResponse{OK: false, Code: code, Error: msg})
		return
	}
	if d.connectors == nil {
		traceJSON(w, http.StatusNotImplemented, connector.RegisterResponse{OK: false, Code: "registration_disabled", Error: "connector registry unavailable"})
		return
	}
	resp, status := d.connectors.Register(r.Context(), r.Header.Get(connector.ConnectorTokenHeader), req)
	traceJSON(w, status, resp)
}

func (d *Dispatcher) handleConnectorHeartbeat(w http.ResponseWriter, r *http.Request) {
	var req connector.HeartbeatRequest
	if status, code, msg := decodeRegistryBody(w, r, &req); status != 0 {
		traceJSON(w, status, connector.HeartbeatResponse{OK: false, Code: code, Error: msg})
		return
	}
	if d.connectors == nil {
		traceJSON(w, http.StatusNotImplemented, connector.HeartbeatResponse{OK: false, Code: "registration_disabled", Error: "connector registry unavailable"})
		return
	}
	resp, status := d.connectors.Heartbeat(r.Header.Get(connector.ConnectorTokenHeader), req)
	traceJSON(w, status, resp)
}

func (d *Dispatcher) handleConnectorUnregister(w http.ResponseWriter, r *http.Request) {
	var req connector.UnregisterRequest
	if status, code, msg := decodeRegistryBody(w, r, &req); status != 0 {
		traceJSON(w, status, map[string]any{"ok": false, "code": code, "error": msg})
		return
	}
	if d.connectors == nil {
		traceJSON(w, http.StatusNotImplemented, map[string]any{"ok": false, "code": "registration_disabled", "error": "connector registry unavailable"})
		return
	}
	resp, status := d.connectors.Unregister(r.Header.Get(connector.ConnectorTokenHeader), req)
	traceJSON(w, status, resp)
}
