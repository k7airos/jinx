package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	maxResponseBytes = 16 << 20
	maxSSEFrameBytes = 2 << 20
)

// HTTPClient communicates strictly with a loopback control-plane/gateway HTTP server.
type HTTPClient struct {
	baseURL    string
	http       *http.Client
	streamHTTP *http.Client
}

// ValidateLoopbackURL enforces that baseURL is a plain HTTP loopback URL with an explicit port.
func ValidateLoopbackURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if strings.Contains(trimmed, "#") {
		return "", errors.New("API URL must not include userinfo, query, or fragment")
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("invalid API URL: %w", err)
	}
	if u.Scheme != "http" {
		return "", fmt.Errorf("API URL must use http:// loopback (got %q)", u.Scheme)
	}
	if u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" {
		return "", errors.New("API URL must not include userinfo, query, or fragment")
	}
	path := strings.TrimRight(u.Path, "/")
	if path != "" {
		return "", fmt.Errorf("API URL must not include a base path (%q)", u.Path)
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if host != "127.0.0.1" && host != "::1" && host != "localhost" {
		return "", fmt.Errorf("API host must be loopback (127.0.0.1, ::1, localhost), got %q", host)
	}
	portStr := u.Port()
	if portStr == "" {
		return "", errors.New("API URL must specify an explicit loopback port")
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("API URL port is invalid (%q)", portStr)
	}
	u.Path = ""
	u.RawPath = ""
	return u.String(), nil
}

func newLoopbackTransport(responseHeaderTimeout time.Duration) *http.Transport {
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	return &http.Transport{
		Proxy:                 nil,
		ResponseHeaderTimeout: responseHeaderTimeout,
		IdleConnTimeout:       60 * time.Second,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, fmt.Errorf("invalid address: %w", err)
			}
			ips, err := net.DefaultResolver.LookupIPAddr(ctx, strings.Trim(host, "[]"))
			if err != nil || len(ips) == 0 {
				return nil, fmt.Errorf("loopback resolution failed for %q", host)
			}
			for _, ip := range ips {
				if !ip.IP.IsLoopback() {
					return nil, fmt.Errorf("refusing non-loopback connection to %s", ip.IP.String())
				}
			}
			var lastErr error
			for _, ip := range ips {
				conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.IP.String(), port))
				if dialErr == nil {
					return conn, nil
				}
				lastErr = dialErr
			}
			return nil, lastErr
		},
	}
}

// NewHTTPClient creates a loopback-validated HTTP/SSE console client.
func NewHTTPClient(baseURL string) (*HTTPClient, error) {
	clean, err := ValidateLoopbackURL(baseURL)
	if err != nil {
		return nil, err
	}
	noRedirect := func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &HTTPClient{
		baseURL: clean,
		http: &http.Client{
			Timeout:       15 * time.Second,
			Transport:     newLoopbackTransport(15 * time.Second),
			CheckRedirect: noRedirect,
		},
		streamHTTP: &http.Client{
			Transport:     newLoopbackTransport(25 * time.Second),
			CheckRedirect: noRedirect,
		},
	}, nil
}

func (c *HTTPClient) IsMock() bool  { return false }
func (c *HTTPClient) Label() string { return c.baseURL }

type resultEnvelope[T any] struct {
	OK      bool     `json:"ok"`
	Value   T        `json:"value"`
	Problem *Problem `json:"problem"`
}

func doJSON[T any](ctx context.Context, c *HTTPClient, method, path string, body any) (T, error) {
	var zero T
	var reqBody io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return zero, fmt.Errorf("marshal request: %w", err)
		}
		reqBody = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return zero, err
	}
	req.Header.Set("Accept", "application/json")
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return zero, fmt.Errorf("read response: %w", err)
	}
	var res resultEnvelope[T]
	if err := json.Unmarshal(data, &res); err != nil {
		if resp.StatusCode >= 400 {
			return zero, &APIError{
				Status: resp.StatusCode,
				Problem: &Problem{
					Code:    "unavailable",
					Message: fmt.Sprintf("HTTP %d: non-JSON response from %s", resp.StatusCode, path),
					Retry:   "read",
				},
			}
		}
		return zero, fmt.Errorf("decode %s: %w", path, err)
	}
	if !res.OK {
		p := res.Problem
		if p == nil {
			p = &Problem{Code: "internal", Message: fmt.Sprintf("request %s failed with status %d", path, resp.StatusCode), Retry: "none"}
		}
		return zero, &APIError{Status: resp.StatusCode, Problem: p}
	}
	return res.Value, nil
}

func (c *HTTPClient) Bootstrap(ctx context.Context) (Bootstrap, error) {
	return doJSON[Bootstrap](ctx, c, http.MethodGet, "/console/v1/bootstrap", nil)
}

func (c *HTTPClient) GetOverview(ctx context.Context) (Envelope[Overview], error) {
	return doJSON[Envelope[Overview]](ctx, c, http.MethodGet, "/console/v1/overview", nil)
}

func (c *HTTPClient) GetArchitecture(ctx context.Context) (ArchitectureSnapshot, error) {
	return doJSON[ArchitectureSnapshot](ctx, c, http.MethodGet, "/console/v1/architecture", nil)
}

func (c *HTTPClient) ListConnectors(ctx context.Context) (Envelope[[]ConnectorView], error) {
	return doJSON[Envelope[[]ConnectorView]](ctx, c, http.MethodGet, "/console/v1/connectors", nil)
}

func (c *HTTPClient) PlanConnectorDryRun(ctx context.Context, platform string, input ConnectorDryRunPlanInput) (ConnectorDryRunPlan, error) {
	return doJSON[ConnectorDryRunPlan](ctx, c, http.MethodPost, "/console/v1/connectors/"+url.PathEscape(platform)+"/dry-run/plan", input)
}

func (c *HTTPClient) ApplyConnectorDryRun(ctx context.Context, platform string, input ConnectorDryRunApplyInput) (ConnectorDryRunApplyResult, error) {
	return doJSON[ConnectorDryRunApplyResult](ctx, c, http.MethodPost, "/console/v1/connectors/"+url.PathEscape(platform)+"/dry-run/apply", input)
}

func (c *HTTPClient) GetCatalog(ctx context.Context) (Envelope[CatalogView], error) {
	return doJSON[Envelope[CatalogView]](ctx, c, http.MethodGet, "/console/v1/catalog", nil)
}

func (c *HTTPClient) ListGroups(ctx context.Context) (Envelope[GroupPage], error) {
	return doJSON[Envelope[GroupPage]](ctx, c, http.MethodGet, "/console/v1/groups", nil)
}

func (c *HTTPClient) GetGroup(ctx context.Context, id string) (Envelope[GroupView], error) {
	return doJSON[Envelope[GroupView]](ctx, c, http.MethodGet, "/console/v1/groups/"+url.PathEscape(id), nil)
}

func (c *HTTPClient) GetGlobalRules(ctx context.Context) (Envelope[GlobalRulesView], error) {
	return doJSON[Envelope[GlobalRulesView]](ctx, c, http.MethodGet, "/console/v1/rules/global", nil)
}

func (c *HTTPClient) GetRelayRules(ctx context.Context) (Envelope[RelayRulesView], error) {
	return doJSON[Envelope[RelayRulesView]](ctx, c, http.MethodGet, "/console/v1/rules/relay", nil)
}

func (c *HTTPClient) PlanChange(ctx context.Context, input PlanInput) (ChangePlan, error) {
	return doJSON[ChangePlan](ctx, c, http.MethodPost, "/console/v1/changes/plan", input)
}

func (c *HTTPClient) PlanRelayRules(ctx context.Context, input PlanInput) (ChangePlan, error) {
	return doJSON[ChangePlan](ctx, c, http.MethodPost, "/console/v1/rules/relay/plan", input)
}

func (c *HTTPClient) PublishChange(ctx context.Context, input PublishInput) (Operation, error) {
	return doJSON[Operation](ctx, c, http.MethodPost, "/console/v1/changes/publish", input)
}

func (c *HTTPClient) GetOperation(ctx context.Context, id string) (Operation, error) {
	return doJSON[Operation](ctx, c, http.MethodGet, "/console/v1/operations/"+url.PathEscape(id), nil)
}

func (c *HTTPClient) ListBackends(ctx context.Context) ([]Backend, error) {
	return doJSON[[]Backend](ctx, c, http.MethodGet, "/console/v1/backends", nil)
}

func (c *HTTPClient) GetGroupRouting(ctx context.Context, groupID string) (GroupRouting, error) {
	return doJSON[GroupRouting](ctx, c, http.MethodGet, "/console/v1/groups/"+url.PathEscape(groupID)+"/routing", nil)
}

func (c *HTTPClient) GetGroupPermissions(ctx context.Context, groupID string) (GroupPermissions, error) {
	return doJSON[GroupPermissions](ctx, c, http.MethodGet, "/console/v1/groups/"+url.PathEscape(groupID)+"/permissions", nil)
}

func (c *HTTPClient) ListRuns(ctx context.Context, filter RunFilter) (Envelope[RunPage], error) {
	q := url.Values{}
	if filter.GroupID != "" {
		q.Set("groupId", filter.GroupID)
	}
	for _, p := range filter.Phases {
		if p != "" {
			q.Add("phases", p)
		}
	}
	for _, o := range filter.Outcomes {
		if o != "" {
			q.Add("outcomes", o)
		}
	}
	for _, d := range filter.Deliveries {
		if d != "" {
			q.Add("deliveries", d)
		}
	}
	if filter.BackendID != "" {
		q.Set("backendId", filter.BackendID)
	}
	if filter.Completeness != "" {
		q.Set("completeness", filter.Completeness)
	}
	if filter.Query != "" {
		q.Set("query", filter.Query)
	}
	if filter.Limit > 0 {
		q.Set("limit", strconv.Itoa(filter.Limit))
	}
	if filter.Cursor != "" {
		q.Set("cursor", filter.Cursor)
	}
	path := "/console/v1/runs"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return doJSON[Envelope[RunPage]](ctx, c, http.MethodGet, path, nil)
}

func (c *HTTPClient) GetRun(ctx context.Context, id string) (Envelope[RunDetail], error) {
	return doJSON[Envelope[RunDetail]](ctx, c, http.MethodGet, "/console/v1/runs/"+url.PathEscape(id), nil)
}

func (c *HTTPClient) GetConfigSnapshot(ctx context.Context, id string) (Envelope[*ConfigSnapshot], error) {
	return doJSON[Envelope[*ConfigSnapshot]](ctx, c, http.MethodGet, "/console/v1/config-snapshots/"+url.PathEscape(id), nil)
}

func (c *HTTPClient) AcknowledgeAttention(ctx context.Context, input AcknowledgeInput) (Envelope[AttentionItem], error) {
	return doJSON[Envelope[AttentionItem]](ctx, c, http.MethodPost, "/console/v1/attention/acknowledge", input)
}

func (c *HTTPClient) GetControls(ctx context.Context) (ControlSnapshot, error) {
	return doJSON[ControlSnapshot](ctx, c, http.MethodGet, "/console/v1/controls", nil)
}

func (c *HTTPClient) RunControlAction(ctx context.Context, input ControlRequest) (ControlOperation, error) {
	return doJSON[ControlOperation](ctx, c, http.MethodPost, "/console/v1/controls/actions", input)
}

func (c *HTTPClient) ListServices(ctx context.Context) (Envelope[[]ServiceView], error) {
	return doJSON[Envelope[[]ServiceView]](ctx, c, http.MethodGet, "/console/v1/services", nil)
}

func (c *HTTPClient) GetRuntimeServices(ctx context.Context) (RuntimeServices, error) {
	return doJSON[RuntimeServices](ctx, c, http.MethodGet, "/console/v1/services/runtime", nil)
}

func (c *HTTPClient) PlanServiceAction(ctx context.Context, input ServicePlanInput) (ServicePlan, error) {
	return doJSON[ServicePlan](ctx, c, http.MethodPost, "/console/v1/services/plan", input)
}

func (c *HTTPClient) ExecuteServiceAction(ctx context.Context, input ServiceExecuteInput) (ServiceOperation, error) {
	return doJSON[ServiceOperation](ctx, c, http.MethodPost, "/console/v1/services/execute", input)
}

func (c *HTTPClient) ContinueMaintenance(ctx context.Context, id string) (ServiceOperation, error) {
	return doJSON[ServiceOperation](ctx, c, http.MethodPost, "/console/v1/services/maintenance/"+url.PathEscape(id)+"/continue", struct{}{})
}

func (c *HTTPClient) CancelMaintenance(ctx context.Context, id string) (ServiceOperation, error) {
	return doJSON[ServiceOperation](ctx, c, http.MethodPost, "/console/v1/services/maintenance/"+url.PathEscape(id)+"/cancel", struct{}{})
}

func (c *HTTPClient) ListChanges(ctx context.Context, filter ChangeFilter) (Envelope[ChangePage], error) {
	q := url.Values{}
	if filter.Origin != "" {
		q.Set("origin", filter.Origin)
	}
	if filter.Status != "" {
		q.Set("status", filter.Status)
	}
	if filter.Component != "" {
		q.Set("component", filter.Component)
	}
	if filter.GroupID != "" {
		q.Set("groupId", filter.GroupID)
	}
	if filter.Query != "" {
		q.Set("query", filter.Query)
	}
	if filter.Limit > 0 {
		q.Set("limit", strconv.Itoa(filter.Limit))
	}
	if filter.Cursor != "" {
		q.Set("cursor", filter.Cursor)
	}
	path := "/console/v1/changes"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return doJSON[Envelope[ChangePage]](ctx, c, http.MethodGet, path, nil)
}

func (c *HTTPClient) GetChange(ctx context.Context, id string) (Envelope[ChangeRecord], error) {
	return doJSON[Envelope[ChangeRecord]](ctx, c, http.MethodGet, "/console/v1/changes/"+url.PathEscape(id), nil)
}

func (c *HTTPClient) PlanRollback(ctx context.Context, input RollbackInput) (ChangePlan, error) {
	return doJSON[ChangePlan](ctx, c, http.MethodPost, "/console/v1/changes/rollback/plan", input)
}

func (c *HTTPClient) GetConfigurationDrift(ctx context.Context, component string) (Envelope[ConfigurationDrift], error) {
	return doJSON[Envelope[ConfigurationDrift]](ctx, c, http.MethodGet, "/console/v1/config/drift/"+url.PathEscape(component), nil)
}

func (c *HTTPClient) DirectDeliver(ctx context.Context, input DirectDeliverInput) (DirectDeliverResult, error) {
	return doJSON[DirectDeliverResult](ctx, c, http.MethodPost, "/console/v1/direct-deliver", input)
}

func (c *HTTPClient) SubscribeRuns(ctx context.Context, cursor string, cb RunCallbacks) func() {
	return c.subscribeSSE(ctx, "/console/v1/events", cursor, "", sseCallbacks{
		onConnection:    cb.OnConnection,
		onResetRequired: cb.OnResetRequired,
		onUnavailable:   cb.OnUnavailable,
		onFrame: func(kind, id string, data []byte) {
			if kind != "" && kind != "run" {
				return
			}
			var ev RunEvent
			if json.Unmarshal(data, &ev) == nil && cb.OnEvent != nil {
				if ev.Cursor == "" && id != "" {
					ev.Cursor = id
				}
				cb.OnEvent(ev)
			}
		},
	})
}

func (c *HTTPClient) SubscribeArchitecture(ctx context.Context, cursor string, domain string, cb ArchitectureCallbacks) func() {
	return c.subscribeSSE(ctx, "/console/v1/architecture/events", cursor, domain, sseCallbacks{
		onConnection:    cb.OnConnection,
		onResetRequired: cb.OnResetRequired,
		onUnavailable:   cb.OnUnavailable,
		onFrame: func(kind, id string, data []byte) {
			if kind != "" && kind != "architecture" {
				return
			}
			var ev ArchitectureEvent
			if json.Unmarshal(data, &ev) == nil && cb.OnEvent != nil {
				cb.OnEvent(ev, id)
			}
		},
	})
}

type sseCallbacks struct {
	onFrame         func(kind, id string, data []byte)
	onResetRequired func(Problem)
	onUnavailable   func(Problem)
	onConnection    func(state string)
}

func (c *HTTPClient) subscribeSSE(parent context.Context, endpoint, initialCursor, domain string, cb sseCallbacks) func() {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		cursor := initialCursor
		lastEventID := ""
		backoff := 250 * time.Millisecond
		for {
			if ctx.Err() != nil {
				return
			}
			q := url.Values{}
			if cursor != "" {
				q.Set("cursor", cursor)
			}
			if domain != "" {
				q.Set("domain", domain)
			}
			reqURL := c.baseURL + endpoint
			if encoded := q.Encode(); encoded != "" {
				reqURL += "?" + encoded
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
			if err != nil {
				return
			}
			req.Header.Set("Accept", "text/event-stream")
			if lastEventID != "" {
				req.Header.Set("Last-Event-ID", lastEventID)
			}
			resp, err := c.streamHTTP.Do(req)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if cb.onConnection != nil {
					cb.onConnection("disconnected")
				}
				if cb.onUnavailable != nil {
					cb.onUnavailable(Problem{Code: "unavailable", Message: err.Error(), Retry: "read"})
				}
				if !sleepContext(ctx, backoff) {
					return
				}
				if backoff < 3*time.Second {
					backoff *= 2
				}
				continue
			}
			if resp.StatusCode != http.StatusOK {
				body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
				resp.Body.Close()
				var res resultEnvelope[any]
				p := Problem{Code: "unavailable", Message: fmt.Sprintf("SSE HTTP %d", resp.StatusCode), Retry: "read"}
				if json.Unmarshal(body, &res) == nil && res.Problem != nil {
					p = *res.Problem
				}
				if cb.onConnection != nil {
					cb.onConnection("disconnected")
				}
				if p.Code == "reset_required" || resp.StatusCode == http.StatusConflict {
					if cb.onResetRequired != nil {
						cb.onResetRequired(p)
					}
					return
				}
				if cb.onUnavailable != nil {
					cb.onUnavailable(p)
				}
				if p.Code == "unsupported" || p.Code == "validation" || resp.StatusCode == http.StatusNotImplemented || resp.StatusCode == http.StatusBadRequest {
					return
				}
				if !sleepContext(ctx, backoff) {
					return
				}
				if backoff < 3*time.Second {
					backoff *= 2
				}
				continue
			}
			backoff = 250 * time.Millisecond
			if cb.onConnection != nil {
				cb.onConnection("fresh")
			}
			reset := readSSEStream(resp.Body, &cursor, &lastEventID, cb)
			resp.Body.Close()
			if reset || ctx.Err() != nil {
				return
			}
			if cb.onConnection != nil {
				cb.onConnection("stale")
			}
			if !sleepContext(ctx, backoff) {
				return
			}
		}
	}()
	return cancel
}

func readSSEStream(body io.Reader, cursor, lastEventID *string, cb sseCallbacks) (reset bool) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 64*1024), maxSSEFrameBytes)
	var evName, evID string
	var dataBuf bytes.Buffer
	dropFrame := false
	flush := func() bool {
		if dropFrame {
			dropFrame = false
			evName = ""
			evID = ""
			dataBuf.Reset()
			return false
		}
		if evName == "" && dataBuf.Len() == 0 && evID == "" {
			return false
		}
		defer func() {
			evName = ""
			evID = ""
			dataBuf.Reset()
		}()
		payload := bytes.TrimRight(dataBuf.Bytes(), "\n")
		if evID != "" {
			*lastEventID = evID
			*cursor = evID
		}
		switch evName {
		case "reset_required":
			var p Problem
			if json.Unmarshal(payload, &p) != nil {
				p = Problem{Code: "reset_required", Message: "Trace stream requires bootstrap reload", Retry: "read"}
			}
			if cb.onResetRequired != nil {
				cb.onResetRequired(p)
			}
			return true
		case "unavailable":
			var p Problem
			if json.Unmarshal(payload, &p) != nil {
				p = Problem{Code: "unavailable", Message: "Event stream unavailable", Retry: "read"}
			}
			if cb.onUnavailable != nil {
				cb.onUnavailable(p)
			}
		default:
			if len(payload) > 0 && cb.onFrame != nil {
				cb.onFrame(evName, evID, payload)
			}
		}
		return false
	}
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			if flush() {
				return true
			}
			continue
		}
		if dropFrame {
			continue
		}
		if strings.HasPrefix(line, ":") {
			// Server keepalive comment keeps connection fresh.
			if cb.onConnection != nil {
				cb.onConnection("fresh")
			}
			continue
		}
		field, val, hasColon := strings.Cut(line, ":")
		if hasColon && strings.HasPrefix(val, " ") {
			val = val[1:]
		}
		switch field {
		case "event":
			evName = val
		case "id":
			evID = val
		case "data":
			if dataBuf.Len()+len(val)+1 > maxSSEFrameBytes {
				dropFrame = true
				evName = ""
				evID = ""
				dataBuf.Reset()
				if cb.onUnavailable != nil {
					cb.onUnavailable(Problem{Code: "unavailable", Message: "SSE frame exceeded size limit", Retry: "read"})
				}
				continue
			}
			dataBuf.WriteString(val)
			dataBuf.WriteByte('\n')
		}
	}
	if scanner.Err() != nil {
		if cb.onUnavailable != nil {
			cb.onUnavailable(Problem{Code: "unavailable", Message: scanner.Err().Error(), Retry: "read"})
		}
		return false
	}
	if flush() {
		return true
	}
	return false
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
