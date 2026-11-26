package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type RelayClient struct {
	baseURL string
	client  *http.Client
}

func NewRelayClient(baseURL string, timeout time.Duration) (*RelayClient, error) {
	if err := ValidateLoopbackURL(baseURL); err != nil {
		return nil, err
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ResponseHeaderTimeout = timeout
	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &RelayClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  client,
	}, nil
}

func (c *RelayClient) Push(ctx context.Context, payload InboundPayload) (RelayPushResponse, error) {
	if strings.TrimSpace(payload.ID) == "" || strings.TrimSpace(payload.GroupID) == "" {
		return RelayPushResponse{}, fmt.Errorf("relay payload requires id and group_id")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return RelayPushResponse{}, fmt.Errorf("marshal relay payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/api/v1/messages", bytes.NewReader(body))
	if err != nil {
		return RelayPushResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return RelayPushResponse{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 16384))
	if err != nil {
		return RelayPushResponse{}, fmt.Errorf("read relay response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return RelayPushResponse{}, fmt.Errorf("relay returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var out RelayPushResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return RelayPushResponse{}, fmt.Errorf("decode relay response: %w", err)
	}
	if out.Status != "ok" {
		return RelayPushResponse{}, fmt.Errorf("unexpected relay push status %q", out.Status)
	}
	return out, nil
}
