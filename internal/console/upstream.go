package console

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

const maxBody = 256 << 10

type Options struct {
	DispatcherURL        string
	RelayURL             string
	DecisionURL          string
	BridgeURL            string
	BilibiliConnectorURL string
	WebConnectorURL      string
	// Connectors maps additional namespaced platforms (e.g. "zhihu") to their
	// Action Connector loopback origins. BilibiliConnectorURL/WebConnectorURL
	// remain as aliases for "bilibili" and "web"; a platform set in both places
	// is rejected.
	Connectors map[string]string
	StateDir             string
	Timeout              time.Duration
	ConfigurationReader  ConfigurationReader
	RelayWriter          RelayConfigWriter
	ExecutionCatalog     ExecutionCatalogReader
}

type upstream struct {
	client      *http.Client
	traceClient *http.Client
	bases       map[string]string
}

func newUpstream(options Options) (*upstream, error) {
	if options.Timeout <= 0 {
		options.Timeout = 3 * time.Second
	}
	transport := &http.Transport{
		DialContext:           (&net.Dialer{Timeout: options.Timeout}).DialContext,
		ResponseHeaderTimeout: options.Timeout,
		DisableKeepAlives:     true,
	}
	backend := &upstream{client: &http.Client{
		Timeout:       options.Timeout,
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, bases: map[string]string{}}
	traceTransport := transport.Clone()
	traceTransport.ResponseHeaderTimeout = 25 * time.Second
	backend.traceClient = &http.Client{Timeout: 25 * time.Second, Transport: traceTransport, CheckRedirect: backend.client.CheckRedirect}
	origins := map[string]string{
		"dispatcher":         options.DispatcherURL,
		"relay":              options.RelayURL,
		"decision":           options.DecisionURL,
		"bridge":             options.BridgeURL,
		"connector:bilibili": options.BilibiliConnectorURL,
		"connector:web":      options.WebConnectorURL,
	}
	for platform, raw := range options.Connectors {
		if !contracts.IsNamespacedPlatform(contracts.Platform(platform)) {
			return nil, failure("validation", "Connector platform must be a lowercase namespaced platform name")
		}
		if origins["connector:"+platform] != "" {
			return nil, failure("validation", "Connector platform is configured more than once")
		}
		origins["connector:"+platform] = raw
	}
	for id, raw := range origins {
		if raw == "" {
			continue
		}
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") || parsed.Port() == "" || !loopback(parsed.Hostname()) {
			return nil, failure("validation", "Upstream must be an HTTP loopback origin with an explicit port")
		}
		backend.bases[id] = strings.TrimRight(raw, "/")
	}
	if backend.bases["dispatcher"] == "" {
		return nil, failure("validation", "Dispatcher origin is required")
	}
	return backend, nil
}

func loopback(host string) bool {
	return host == "localhost" || net.ParseIP(host).IsLoopback()
}

func validLoopbackOrigin(raw string) (string, bool) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") || parsed.Port() == "" || !loopback(parsed.Hostname()) {
		return "", false
	}
	return strings.TrimRight(parsed.String(), "/"), true
}

func (backend *upstream) request(ctx context.Context, service, method, path, etag string, body []byte) ([]byte, string, int, error) {
	return backend.boundedRequest(ctx, backend.client, maxBody, service, method, path, etag, body)
}

func (backend *upstream) requestOrigin(ctx context.Context, origin, method, path, etag string, body []byte) ([]byte, string, int, error) {
	return backend.boundedRequestOrigin(ctx, backend.client, maxBody, origin, method, path, etag, body)
}

func (backend *upstream) boundedRequest(ctx context.Context, client *http.Client, responseLimit int64, service, method, path, etag string, body []byte) ([]byte, string, int, error) {
	origin := backend.bases[service]
	if origin == "" {
		return nil, "", 0, failure("unsupported", "Service probe is not configured")
	}
	return backend.boundedRequestOrigin(ctx, client, responseLimit, origin, method, path, etag, body)
}

func (backend *upstream) boundedRequestOrigin(ctx context.Context, client *http.Client, responseLimit int64, origin, method, path, etag string, body []byte) ([]byte, string, int, error) {
	if len(body) > maxBody {
		return nil, "", 0, failure("too_large", "Configuration patch exceeds 256 KiB")
	}
	if origin == "" {
		return nil, "", 0, failure("unsupported", "Service probe is not configured")
	}
	req, err := http.NewRequestWithContext(ctx, method, origin+path, bytes.NewReader(body))
	if err != nil {
		return nil, "", 0, failure("internal", "Cannot construct upstream request")
	}
	if method == http.MethodPost || method == http.MethodPut {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("If-Match", etag)
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, "", 0, failure("unavailable", "Service request did not complete")
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, responseLimit+1))
	if err != nil || int64(len(data)) > responseLimit {
		return nil, "", response.StatusCode, failure("unavailable", "Service response is incomplete or too large")
	}
	return data, response.Header.Get("ETag"), response.StatusCode, nil
}

type snapshot struct {
	root  map[string]any
	rules map[string]any
	etag  string
}

func (backend *upstream) config(ctx context.Context) (snapshot, error) {
	data, etag, status, err := backend.request(ctx, "dispatcher", http.MethodGet, "/api/config", "", nil)
	if err != nil {
		return snapshot{}, err
	}
	if status != http.StatusOK || etag == "" {
		return snapshot{}, failure("unavailable", "Dispatcher configuration or ETag is unavailable")
	}
	var root map[string]any
	if json.Unmarshal(data, &root) != nil || root == nil {
		return snapshot{}, failure("unavailable", "Dispatcher configuration has an invalid shape")
	}
	rules, rulesOK := root["rules"].(map[string]any)
	_, bridgeOK := root["bridge"].(map[string]any)
	_, hermesOK := root["hermes"].(map[string]any)
	if !rulesOK || !bridgeOK || !hermesOK {
		return snapshot{}, failure("unavailable", "Dispatcher configuration is missing required sections")
	}
	if rules["group_rules"] == nil {
		rules["group_rules"] = map[string]any{}
	}
	if _, ok := rules["group_rules"].(map[string]any); !ok {
		return snapshot{}, failure("unavailable", "Dispatcher group rules have an invalid shape")
	}
	return snapshot{root: root, rules: rules, etag: etag}, nil
}

// connectorPlatforms lists the platforms with a configured connector origin.
func (backend *upstream) connectorPlatforms() []contracts.Platform {
	platforms := []contracts.Platform{}
	for id := range backend.bases {
		if platform, ok := strings.CutPrefix(id, "connector:"); ok {
			platforms = append(platforms, contracts.Platform(platform))
		}
	}
	sort.Slice(platforms, func(i, j int) bool { return platforms[i] < platforms[j] })
	return platforms
}

func (backend *upstream) discoveredConnectors(ctx context.Context) []connector.RegistryEntry {
	data, _, status, err := backend.request(ctx, "dispatcher", http.MethodGet, "/api/v1/connectors", "", nil)
	if err != nil || status != http.StatusOK {
		return nil
	}
	var payload struct {
		Connectors []connector.RegistryEntry `json:"connectors"`
	}
	if json.Unmarshal(data, &payload) != nil || len(payload.Connectors) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(payload.Connectors))
	out := make([]connector.RegistryEntry, 0, len(payload.Connectors))
	for _, entry := range payload.Connectors {
		platform := contracts.Platform(strings.TrimSpace(entry.Platform))
		if !contracts.IsNamespacedPlatform(platform) || seen[string(platform)] {
			continue
		}
		origin, ok := validLoopbackOrigin(entry.URL)
		if !ok {
			continue
		}
		entry.Platform = string(platform)
		entry.URL = origin
		seen[entry.Platform] = true
		out = append(out, entry)
	}
	return out
}
