package console

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"jinx/internal/contracts"
)

type DirectDeliverInput struct {
	TargetContainerID string          `json:"target_container_id"`
	Text              string          `json:"text"`
	IdempotencyKey    string          `json:"idempotency_key"`
	Source            string          `json:"source,omitempty"`
	DryRun            bool            `json:"dry_run,omitempty"`
	ContentID         string          `json:"content_id,omitempty"`
	ParentID          string          `json:"parent_id,omitempty"`
	RootID            string          `json:"root_id,omitempty"`
	ReplyContext      json.RawMessage `json:"reply_context,omitempty"`
}

type DirectDeliverResult struct {
	RunID             string                     `json:"run_id"`
	Replayed          bool                       `json:"replayed"`
	DryRun            bool                       `json:"dry_run"`
	TargetContainerID string                     `json:"target_container_id"`
	Trigger           string                     `json:"trigger"`
	Phase             contracts.Phase            `json:"phase"`
	Outcome           contracts.Outcome          `json:"outcome"`
	Delivery          contracts.Delivery         `json:"delivery"`
	ReasonCode        string                     `json:"reason_code,omitempty"`
	DeliveryEvidence  contracts.DeliveryEvidence `json:"delivery_evidence"`
}

func (facade *Facade) DirectDeliver(ctx context.Context, input DirectDeliverInput) (DirectDeliverResult, error) {
	if strings.TrimSpace(input.TargetContainerID) == "" || strings.TrimSpace(input.Text) == "" || strings.TrimSpace(input.IdempotencyKey) == "" {
		return DirectDeliverResult{}, failure("validation", "target_container_id, text, and idempotency_key are required")
	}
	payload, err := json.Marshal(input)
	if err != nil {
		return DirectDeliverResult{}, failure("validation", "Cannot encode direct delivery request")
	}
	data, _, status, err := facade.backend.boundedRequest(ctx, facade.backend.traceClient, maxBody, "dispatcher", http.MethodPost, "/api/v1/direct-deliver", "", payload)
	if err != nil {
		return DirectDeliverResult{}, err
	}
	if status != http.StatusOK {
		var upstreamErr struct {
			Code  string `json:"code"`
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &upstreamErr)
		msg := upstreamErr.Error
		if msg == "" {
			msg = "Direct delivery rejected by Dispatcher"
		}
		switch status {
		case http.StatusBadRequest:
			return DirectDeliverResult{}, failure("validation", msg)
		case http.StatusForbidden:
			return DirectDeliverResult{}, failure("forbidden", msg)
		case http.StatusConflict:
			return DirectDeliverResult{}, failure("conflict", msg)
		case http.StatusRequestEntityTooLarge:
			return DirectDeliverResult{}, failure("too_large", msg)
		default:
			return DirectDeliverResult{}, failure("unavailable", msg)
		}
	}
	var out DirectDeliverResult
	if err := json.Unmarshal(data, &out); err != nil {
		return DirectDeliverResult{}, failure("unavailable", "Dispatcher returned unreadable direct delivery response")
	}
	return out, nil
}
