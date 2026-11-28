package connector

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var errNotRegistered = errors.New("connector not registered")

// RegistryClient registers a connector with Dispatcher's connector registry and
// maintains its lease via periodic heartbeats.
type RegistryClient struct {
	cfg          RegistryClientConfig
	advertiseURL string
	instanceID   string
	executor     ActionExecutor
	dryRunFn     func() bool
	client       *http.Client
}

func NewRegistryClient(cfg RegistryClientConfig, listenAddr string, executor ActionExecutor, dryRunFn func() bool) (*RegistryClient, error) {
	if err := NormalizeRegistryClientConfig(&cfg); err != nil {
		return nil, err
	}
	if cfg.URL == "" {
		return nil, fmt.Errorf("registry.url is required")
	}
	if executor == nil {
		return nil, fmt.Errorf("executor is required")
	}
	advertiseURL := cfg.AdvertiseURL
	if advertiseURL == "" {
		if err := ValidateLoopbackAddr(listenAddr); err != nil {
			return nil, fmt.Errorf("advertise listen addr: %w", err)
		}
		advertiseURL = "http://" + listenAddr
	}
	var rawID [16]byte
	if _, err := rand.Read(rawID[:]); err != nil {
		return nil, fmt.Errorf("generate instance_id: %w", err)
	}
	if dryRunFn == nil {
		dryRunFn = func() bool { return true }
	}
	return &RegistryClient{
		cfg:          cfg,
		advertiseURL: strings.TrimRight(advertiseURL, "/"),
		instanceID:   hex.EncodeToString(rawID[:]),
		executor:     executor,
		dryRunFn:     dryRunFn,
		client: &http.Client{
			Timeout: time.Duration(cfg.TimeoutSec) * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

// InstanceID returns the unique ephemeral ID generated for this client process.
func (c *RegistryClient) InstanceID() string {
	return c.instanceID
}

func (c *RegistryClient) readToken() (string, error) {
	if c.cfg.TokenFile == "" {
		return "", nil
	}
	data, err := os.ReadFile(c.cfg.TokenFile)
	if err != nil {
		return "", fmt.Errorf("read connector token file: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if token == "" {
		return "", fmt.Errorf("connector token file is empty")
	}
	return token, nil
}

func (c *RegistryClient) doPost(ctx context.Context, path string, payload any, out any) (int, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.URL+path, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	token, err := c.readToken()
	if err != nil {
		return 0, err
	}
	if token != "" {
		req.Header.Set(ConnectorTokenHeader, token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<10))
	if err != nil {
		return resp.StatusCode, err
	}
	if out != nil && len(bytes.TrimSpace(raw)) > 0 {
		_ = json.Unmarshal(raw, out)
	}
	return resp.StatusCode, nil
}

// Register announces this connector instance to Dispatcher's connector registry.
func (c *RegistryClient) Register(ctx context.Context) error {
	caps := make([]string, 0, len(c.executor.Capabilities()))
	for _, capKind := range c.executor.Capabilities() {
		caps = append(caps, string(capKind))
	}
	var targets []HealthTarget
	if reporter, ok := c.executor.(StatusReporter); ok {
		targets = reporter.Status().Targets
	}
	req := RegisterRequest{
		ProtocolVersion: ProtocolVersion,
		Platform:        string(c.executor.Platform()),
		URL:             c.advertiseURL,
		InstanceID:      c.instanceID,
		AccountID:       c.executor.AccountID(),
		ConnectorName:   c.executor.ConnectorName(),
		Capabilities:    caps,
		Controls:        []string{"dry_run"},
		Targets:         targets,
		HeartbeatTTLSec: c.cfg.HeartbeatTTLSec,
		DryRun:          c.dryRunFn(),
	}
	var resp RegisterResponse
	status, err := c.doPost(ctx, "/api/v1/connectors/register", req, &resp)
	if err != nil {
		return err
	}
	if status < 200 || status >= 300 || !resp.OK {
		if resp.Error != "" {
			return fmt.Errorf("register failed (status=%d code=%s): %s", status, resp.Code, resp.Error)
		}
		return fmt.Errorf("register failed with HTTP %d", status)
	}
	return nil
}

// Heartbeat renews the connector lease and self-heals via Register on 404.
func (c *RegistryClient) Heartbeat(ctx context.Context) error {
	ready, _ := c.executor.Health(ctx)
	req := HeartbeatRequest{
		Platform:        string(c.executor.Platform()),
		URL:             c.advertiseURL,
		InstanceID:      c.instanceID,
		DryRun:          c.dryRunFn(),
		Ready:           ready,
		HeartbeatTTLSec: c.cfg.HeartbeatTTLSec,
	}
	if reporter, ok := c.executor.(StatusReporter); ok {
		snap := reporter.Status()
		req.Targets = snap.Targets
		req.LastPollAt = snap.LastPollAt
		req.LastPollErrorCode = snap.LastPollErrorCode
	}
	var resp HeartbeatResponse
	status, err := c.doPost(ctx, "/api/v1/connectors/heartbeat", req, &resp)
	if err != nil {
		return err
	}
	if status == http.StatusNotFound || resp.Code == "not_registered" {
		if regErr := c.Register(ctx); regErr != nil {
			return fmt.Errorf("%w (re-register failed: %v)", errNotRegistered, regErr)
		}
		return nil
	}
	if status < 200 || status >= 300 || !resp.OK {
		if resp.Error != "" {
			return fmt.Errorf("heartbeat failed (status=%d code=%s): %s", status, resp.Code, resp.Error)
		}
		return fmt.Errorf("heartbeat failed with HTTP %d", status)
	}
	return nil
}

// Unregister withdraws this connector instance on graceful shutdown.
func (c *RegistryClient) Unregister(ctx context.Context) error {
	req := UnregisterRequest{
		Platform:   string(c.executor.Platform()),
		URL:        c.advertiseURL,
		InstanceID: c.instanceID,
	}
	status, err := c.doPost(ctx, "/api/v1/connectors/unregister", req, nil)
	if err != nil {
		return err
	}
	if status >= 400 && status != http.StatusNotFound {
		return fmt.Errorf("unregister failed with HTTP %d", status)
	}
	return nil
}

// Run registers with Dispatcher using exponential backoff up to 5s and then
// emits heartbeats every HeartbeatIntervalSec until ctx is canceled.
func (c *RegistryClient) Run(ctx context.Context) {
	backoffs := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second}
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}
		if err := c.Register(ctx); err != nil {
			wait := backoffs[len(backoffs)-1]
			if attempt < len(backoffs) {
				wait = backoffs[attempt]
			}
			attempt++
			if ctx.Err() == nil {
				log.Printf("[registry] register %s failed (retry in %s): %v", c.executor.Platform(), wait, err)
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		log.Printf("[registry] registered %s at %s (ttl=%ds)", c.executor.Platform(), c.cfg.URL, c.cfg.HeartbeatTTLSec)
		break
	}

	ticker := time.NewTicker(time.Duration(c.cfg.HeartbeatIntervalSec) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.Heartbeat(ctx); err != nil && ctx.Err() == nil {
				log.Printf("[registry] heartbeat %s failed: %v", c.executor.Platform(), err)
			}
		}
	}
}
