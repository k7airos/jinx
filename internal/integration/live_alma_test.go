package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/execution"
)

type almaObservation struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	ThreadID string `json:"threadId"`
	Summary  string `json:"summary"`
}

type almaGuard struct {
	base       string
	model      string
	nonce      string
	transport  *http.Transport
	mu         sync.Mutex
	run        almaObservation
	dispatches int
	polls      int
	stops      int
}

func explicitAlmaOrigin(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || strings.Contains(raw, "#") || parsed.RawPath != "" || parsed.Opaque != "" || (parsed.Path != "" && parsed.Path != "/") || parsed.Port() == "" {
		return "", fmt.Errorf("JINX_ALMA_URL must be an explicit HTTP loopback origin with a port")
	}
	address := net.ParseIP(parsed.Hostname())
	port, portErr := strconv.Atoi(parsed.Port())
	if address == nil || !address.IsLoopback() || portErr != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("JINX_ALMA_URL requires a literal loopback address")
	}
	switch parsed.Port() {
	case "5032", "5033", "8080", "8081", "8088":
		return "", fmt.Errorf("real business service port is forbidden in integration tests")
	}
	return strings.TrimSuffix(raw, "/"), nil
}

func newAlmaGuard(base, model, nonce string) (*almaGuard, *http.Client) {
	guard := &almaGuard{base: base, model: model, nonce: nonce, transport: &http.Transport{
		Proxy: nil, DialContext: (&net.Dialer{Timeout: 3 * time.Second}).DialContext,
		ResponseHeaderTimeout: 15 * time.Second,
	}}
	return guard, &http.Client{Transport: guard, Timeout: 20 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("live Alma redirects are forbidden") },
	}
}

func (guard *almaGuard) RoundTrip(request *http.Request) (*http.Response, error) {
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if request.URL.Scheme+"://"+request.URL.Host != guard.base || request.URL.RawQuery != "" || request.URL.Fragment != "" {
		return nil, fmt.Errorf("Alma transport denied non-allowlisted origin")
	}
	allowed := request.Method == http.MethodGet && request.URL.Path == "/api/models"
	dispatch := request.Method == http.MethodPost && request.URL.Path == "/api/personal/dispatch"
	poll := guard.run.ID != "" && request.Method == http.MethodGet && request.URL.Path == "/api/personal/runs/"+url.PathEscape(guard.run.ID)
	stop := guard.run.ID != "" && request.Method == http.MethodPost && request.URL.Path == "/api/personal/stop"
	if dispatch || stop {
		payload, err := io.ReadAll(io.LimitReader(request.Body, 4097))
		if err != nil || len(payload) > 4096 {
			return nil, fmt.Errorf("live Alma body is invalid")
		}
		request.Body = io.NopCloser(bytes.NewReader(payload))
		var body map[string]any
		if json.Unmarshal(payload, &body) != nil {
			return nil, fmt.Errorf("live Alma body is not JSON")
		}
		if dispatch {
			task, taskOK := body["task"].(string)
			title, titleOK := body["title"].(string)
			if guard.dispatches != 0 || !taskOK || !titleOK || !strings.HasPrefix(title, "group execution ") || len(body) != 3 || body["model"] != guard.model || len(task) > 1024 || !strings.Contains(task, guard.nonce) || !strings.Contains(task, "不用工具、不读文件、不发送消息") || strings.ContainsAny(task, "/\\") {
				return nil, fmt.Errorf("live Alma dispatch is not the isolated fictional nonce task")
			}
			guard.dispatches++
		} else {
			if len(body) != 1 || body["runId"] != guard.run.ID || guard.run.Status == "done" || guard.stops != 0 {
				return nil, fmt.Errorf("stop is restricted to this test's unfinished run")
			}
			guard.stops++
		}
		allowed = true
	}
	if poll {
		allowed = true
		guard.polls++
	}
	if !allowed {
		return nil, fmt.Errorf("Alma transport denied method/path")
	}
	response, err := guard.transport.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if dispatch || poll {
		payload, readErr := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
		_ = response.Body.Close()
		if readErr != nil || len(payload) > 4<<20 {
			return nil, fmt.Errorf("Alma run response is incomplete")
		}
		response.Body = io.NopCloser(bytes.NewReader(payload))
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			var observed almaObservation
			if json.Unmarshal(payload, &observed) != nil || observed.ID == "" || observed.ThreadID == "" || strings.ContainsAny(observed.ID, "/\\?#") || observed.Status == "" || (guard.run.ID != "" && observed.ID != guard.run.ID) || (guard.run.ThreadID != "" && observed.ThreadID != guard.run.ThreadID) {
				return nil, fmt.Errorf("Alma did not return the same top-level run/thread identity")
			}
			guard.run = observed
		}
	}
	return response, nil
}

func (guard *almaGuard) observation() (almaObservation, int, int, int) {
	guard.mu.Lock()
	defer guard.mu.Unlock()
	return guard.run, guard.dispatches, guard.polls, guard.stops
}

func requireCatalogModel(t *testing.T, client *http.Client, base, model string) {
	t.Helper()
	var catalog json.RawMessage
	requestJSON(t, client, http.MethodGet, base+"/api/models", nil, &catalog)
	var entries any
	if json.Unmarshal(catalog, &entries) != nil {
		t.Fatal("Alma model catalog is not JSON")
	}
	var find func(any) bool
	find = func(value any) bool {
		switch item := value.(type) {
		case []any:
			for _, child := range item {
				if find(child) {
					return true
				}
			}
		case map[string]any:
			if item["id"] == model {
				return true
			}
			providerID, providerOK := item["providerId"].(string)
			modelID, modelOK := item["id"].(string)
			if providerOK && modelOK && providerID+":"+modelID == model {
				return true
			}
			for _, child := range item {
				if find(child) {
					return true
				}
			}
		}
		return false
	}
	if !find(entries) {
		t.Fatal("JINX_ALMA_MODEL is not an ID returned by GET /api/models")
	}
}

func TestLiveAlmaMessageChain(t *testing.T) {
	if os.Getenv("JINX_LIVE_ALMA") != "1" {
		t.Skip("live not run: set JINX_LIVE_ALMA=1 with explicit JINX_ALMA_URL and JINX_ALMA_MODEL only for final authorized verification")
	}
	base, err := explicitAlmaOrigin(os.Getenv("JINX_ALMA_URL"))
	if err != nil {
		t.Fatal(err)
	}
	model := os.Getenv("JINX_ALMA_MODEL")
	if strings.TrimSpace(model) == "" || strings.TrimSpace(model) != model {
		t.Fatal("JINX_ALMA_MODEL must be an explicit catalog ID")
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	nonce := "JINX_NONCE_" + hex.EncodeToString(random[:])
	inputID := "live-input-" + hex.EncodeToString(random[:])
	guard, client := newAlmaGuard(base, model, nonce)
	t.Cleanup(guard.transport.CloseIdleConnections)
	requireCatalogModel(t, client, base, model)
	var timedOut atomic.Bool
	t.Cleanup(func() {
		run, _, _, _ := guard.observation()
		if !timedOut.Load() || run.ID == "" || run.Status == "done" || run.Status == "failed" || run.Status == "skipped" {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		payload, _ := json.Marshal(map[string]string{"runId": run.ID})
		request, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/personal/stop", bytes.NewReader(payload))
		request.Header.Set("Content-Type", "application/json")
		response, err := client.Do(request)
		if err != nil {
			t.Errorf("test-run-only timeout cleanup failed: %v", err)
			return
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Errorf("test-run-only stop HTTP %d", response.StatusCode)
		}
	})
	root := t.TempDir()
	_, runner := newExecution(t, root, client, []execution.BackendOptions{{ID: "synthetic-primary", Kind: execution.AlmaPersonal,
		Models: []execution.Model{{ID: model, Label: "Explicit Alma catalog selection"}}, AlmaBaseURL: base, PollInterval: 200 * time.Millisecond}}, executionConfig(execution.AlmaPersonal, model, 60))
	runner.inboundOnly = true
	relay := newRelay(t, root)
	var posts, writes atomic.Int32
	deliveries := make(chan bridgeDelivery, 2)
	bridge := newBridge(t, relay.Client(), relay.URL, &posts, deliveries)
	path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
	var cfg map[string]any
	if json.Unmarshal(readFixtureFile(t, path), &cfg) != nil {
		t.Fatal("invalid test config")
	}
	cfg["hermes"].(map[string]any)["timeout_sec"] = 70
	writeJSON(t, path, cfg)
	services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes, runner)
	postMessage(t, relay.Client(), relay.URL, inputID, "@Jinx 只输出"+nonce+"，不用工具、不读文件、不发送消息。")
	var traceID string
	deadline := time.Now().Add(75 * time.Second)
	for time.Now().Before(deadline) {
		page := stackRuns(t, services)
		if len(page.Items) == 1 && page.Items[0].Phase == contracts.PhaseTerminal {
			traceID = page.Items[0].ID
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	run, dispatches, polls, stops := guard.observation()
	t.Logf("live evidence: Alma run=%s thread=%s trace=%s expected_nonce=%s sink_count=%d dispatches=%d polls=%d stops=%d status=%s", run.ID, run.ThreadID, traceID, nonce, posts.Load(), dispatches, polls, stops, run.Status)
	if traceID == "" {
		timedOut.Store(true)
		t.Fatal("live Alma message chain timed out")
	}
	result, executionErr := runner.observed(traceID)
	if errors.Is(executionErr, context.DeadlineExceeded) || execution.Code(executionErr) == "timeout" {
		timedOut.Store(true)
	}
	if executionErr != nil || result.Fallback || len(result.Attempts) != 1 || result.Attempts[0].ExternalID != run.ID || result.Text != nonce {
		t.Fatalf("live execution failed exact nonce/single actual attempt: err=%v attempts=%+v", executionErr, result.Attempts)
	}
	var independent almaObservation
	requestJSON(t, client, http.MethodGet, base+"/api/personal/runs/"+url.PathEscape(run.ID), nil, &independent)
	if independent.ID != run.ID || independent.ThreadID != run.ThreadID || independent.Status != "done" || strings.TrimSpace(independent.Summary) != nonce || dispatches != 1 || polls < 1 || stops != 0 {
		t.Fatalf("independent same-run GET did not prove done and exact nonce: run=%s thread=%s status=%s", independent.ID, independent.ThreadID, independent.Status)
	}
	view := waitRun(t, services, inputID)
	found := false
	for _, attempt := range view.Attempts {
		found = found || attempt.ExternalID != nil && *attempt.ExternalID == run.ID
	}
	if !found || view.Outcome != contracts.OutcomeReplied || view.Delivery != contracts.DeliveryConfirmed || posts.Load() != 1 {
		t.Fatalf("live trace/sink/echo correlation failed: %+v count=%d", view, posts.Load())
	}
	if delivery := <-deliveries; delivery.Text != nonce || delivery.GroupID != targetGroup {
		t.Fatal("live response did not reach the single fictional sink")
	}
	t.Logf("live passed: independent GET run=%s thread=%s done; trace=%s expected_nonce=%s sink_count=%d", run.ID, run.ThreadID, traceID, nonce, posts.Load())
}

func TestLiveAlmaOriginRejectsBusinessEndpoints(t *testing.T) {
	for _, endpoint := range []string{"", "http://localhost:4000", "https://127.0.0.1:4000", "http://example.org:4000", "http://127.0.0.1:5033", "http://127.0.0.1:8080", "http://127.0.0.1:8088", "http://127.0.0.1:4000/path", "http://127.0.0.1:4000?token=synthetic", "http://127.0.0.1:4000?", "http://127.0.0.1:4000#", "http://127.0.0.1:0", "http://127.0.0.1:65536"} {
		if _, err := explicitAlmaOrigin(endpoint); err == nil {
			t.Errorf("unsafe live origin accepted: %s", endpoint)
		}
	}
}
