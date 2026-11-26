package console

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxBody = 256 << 10

type Options struct {
	DispatcherURL        string
	RelayURL             string
	DecisionURL          string
	BridgeURL            string
	BilibiliConnectorURL string
	WebConnectorURL      string
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
	for id, raw := range map[string]string{
		"dispatcher":         options.DispatcherURL,
		"relay":              options.RelayURL,
		"decision":           options.DecisionURL,
		"bridge":             options.BridgeURL,
		"connector:bilibili": options.BilibiliConnectorURL,
		"connector:web":      options.WebConnectorURL,
	} {
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

func (backend *upstream) request(ctx context.Context, service, method, path, etag string, body []byte) ([]byte, string, int, error) {
	return backend.boundedRequest(ctx, backend.client, maxBody, service, method, path, etag, body)
}

func (backend *upstream) boundedRequest(ctx context.Context, client *http.Client, responseLimit int64, service, method, path, etag string, body []byte) ([]byte, string, int, error) {
	if len(body) > maxBody {
		return nil, "", 0, failure("too_large", "Configuration patch exceeds 256 KiB")
	}
	origin := backend.bases[service]
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
