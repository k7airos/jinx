package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"jinx/internal/contracts"
	"jinx/internal/observability"
)

const (
	maxDirectDeliverBodyBytes = 32 << 10
	maxDirectDeliverRunes     = 4000
)

var directSourcePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$`)

// DirectDeliverRequest represents an authenticated loopback request to send
// pre-formatted text directly through the delivery layer without AI generation.
type DirectDeliverRequest struct {
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

// DirectDeliverResponse reports the persisted trace outcome for a direct delivery.
type DirectDeliverResponse struct {
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

// DirectDeliverError is an HTTP-mapped validation or guard failure from DirectDeliver.
type DirectDeliverError struct {
	Status  int
	Code    string
	Message string
}

func (e *DirectDeliverError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func directError(status int, code, message string) error {
	return &DirectDeliverError{Status: status, Code: code, Message: message}
}

func relaySourceForPlatform(platform contracts.Platform) string {
	if contracts.IsNamespacedPlatform(platform) {
		return string(platform) + ":direct"
	}
	return "wechat"
}

func directDeliverResponseFromRun(run contracts.Run, replayed bool) DirectDeliverResponse {
	return DirectDeliverResponse{
		RunID:             run.ID,
		Replayed:          replayed,
		DryRun:            run.ReasonCode == "dry_run" || run.ReasonCode == "connector_dry_run",
		TargetContainerID: run.TargetGroupID,
		Trigger:           run.Trigger,
		Phase:             run.Phase,
		Outcome:           run.Outcome,
		Delivery:          run.Delivery,
		ReasonCode:        run.ReasonCode,
		DeliveryEvidence:  run.DeliveryEvidence,
	}
}

// DirectDeliver skips decision/agent generation and sends pre-formatted text
// through Dispatcher safety guards, trace WAL, Bridge/Connector, and echo confirmation.
func (d *Dispatcher) DirectDeliver(ctx context.Context, req DirectDeliverRequest) (DirectDeliverResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	targetID := strings.TrimSpace(req.TargetContainerID)
	platform, _, platformOK := contracts.ParseContainerID(targetID)
	if !platformOK {
		return DirectDeliverResponse{}, directError(http.StatusBadRequest, "invalid_target_container", "valid target_container_id is required")
	}
	idempotencyKey := strings.TrimSpace(req.IdempotencyKey)
	msgID := "direct:" + idempotencyKey
	if idempotencyKey == "" || !contracts.ValidID(idempotencyKey) || !contracts.ValidID(msgID) {
		return DirectDeliverResponse{}, directError(http.StatusBadRequest, "invalid_idempotency_key", "valid idempotency_key is required")
	}
	trimmedText := strings.TrimSpace(req.Text)
	if trimmedText == "" || !utf8.ValidString(trimmedText) || strings.IndexByte(trimmedText, 0) >= 0 || utf8.RuneCountInString(trimmedText) > maxDirectDeliverRunes {
		return DirectDeliverResponse{}, directError(http.StatusBadRequest, "invalid_text", fmt.Sprintf("non-empty UTF-8 text up to %d runes is required", maxDirectDeliverRunes))
	}
	rawSource := strings.TrimSpace(req.Source)
	trigger := "direct"
	senderName := "direct"
	if rawSource != "" {
		if !directSourcePattern.MatchString(rawSource) {
			return DirectDeliverResponse{}, directError(http.StatusBadRequest, "invalid_source", "source must match ^[A-Za-z0-9][A-Za-z0-9_-]{0,31}$")
		}
		lowerSource := strings.ToLower(rawSource)
		trigger = "direct:" + lowerSource
		senderName = lowerSource
	}
	if req.ContentID != "" && !mappingIdentity(req.ContentID) {
		return DirectDeliverResponse{}, directError(http.StatusBadRequest, "invalid_content_id", "invalid content_id")
	}
	if req.ParentID != "" && (!mappingIdentity(req.ParentID) || req.ContentID == "") {
		return DirectDeliverResponse{}, directError(http.StatusBadRequest, "invalid_parent_id", "parent_id requires valid content_id")
	}
	if req.RootID != "" && (!mappingIdentity(req.RootID) || req.ContentID == "") {
		return DirectDeliverResponse{}, directError(http.StatusBadRequest, "invalid_root_id", "root_id requires valid content_id")
	}
	if len(req.ReplyContext) > maxRelayReplyContextBytes || (len(req.ReplyContext) > 0 && !json.Valid(req.ReplyContext)) {
		return DirectDeliverResponse{}, directError(http.StatusBadRequest, "invalid_reply_context", "reply_context must be bounded valid JSON")
	}

	cfg := d.configSnapshot()
	rule, allowed := cfg.route(targetID)
	if !allowed {
		return DirectDeliverResponse{}, directError(http.StatusForbidden, "container_not_allowlisted", "target container is not enabled and allowlisted")
	}
	dstPlatform, _, dstOK := contracts.ParseContainerID(rule.TargetGroupID)
	if !dstOK || dstPlatform != platform {
		return DirectDeliverResponse{}, directError(http.StatusBadRequest, "invalid_route_target", "configured target route platform does not match request container")
	}

	identityBytes, _ := json.Marshal(struct {
		Source string
		IDs    []string
	}{targetID, []string{msgID}})
	runID := "run-" + messageDigest(string(identityBytes))
	reqDigest := messageDigest(strings.Join([]string{targetID, trimmedText, req.ContentID, req.ParentID, req.RootID}, "\x00"))

	d.directMu.Lock()
	if d.directInFlight[runID] {
		d.directMu.Unlock()
		return DirectDeliverResponse{}, directError(http.StatusConflict, "direct_delivery_in_progress", "identical idempotency_key is currently in progress")
	}
	d.traceMu.Lock()
	existing, traceErr := d.trace.GetRun(runID)
	d.traceMu.Unlock()
	if traceErr == nil {
		prevDigest := d.directDigests[runID]
		d.directMu.Unlock()
		if (prevDigest != "" && prevDigest != reqDigest) || existing.TargetGroupID != rule.TargetGroupID {
			return DirectDeliverResponse{}, directError(http.StatusConflict, "idempotency_key_conflict", "idempotency_key was already used with a different payload")
		}
		return directDeliverResponseFromRun(existing, true), nil
	}
	if !errors.Is(traceErr, observability.ErrNotFound) {
		d.directMu.Unlock()
		return DirectDeliverResponse{}, directError(http.StatusServiceUnavailable, "trace_unavailable", "trace store is unavailable")
	}
	d.directInFlight[runID] = true
	d.directDigests[runID] = reqDigest
	d.directMu.Unlock()

	defer func() {
		d.directMu.Lock()
		delete(d.directInFlight, runID)
		d.directMu.Unlock()
	}()

	now := time.Now().UTC()
	msg := RelayMessage{
		ReceivedAt:   &now,
		ID:           msgID,
		Platform:     string(platform),
		AccountID:    defaultMappingAccount(platform),
		Source:       relaySourceForPlatform(platform),
		GroupID:      targetID,
		GroupName:    rule.Target,
		SenderID:     "direct:" + senderName,
		SenderName:   senderName,
		ParentID:     req.ParentID,
		RootID:       req.RootID,
		ReplyContext: append(json.RawMessage(nil), req.ReplyContext...),
		Type:         1,
		Content:      "[direct]",
		Action:       "forward",
		Timestamp:    now.UnixMilli(),
	}
	b := batch{
		Direct:                true,
		DirectTargetContentID: req.ContentID,
		GroupID:               targetID,
		Messages:              []RelayMessage{msg},
		Rule:                  rule,
		Config:                cfg,
		Policy:                policyID(cfg),
		Reason:                trigger,
	}

	if err := d.ensureTrace(&b); err != nil {
		return DirectDeliverResponse{}, directError(http.StatusServiceUnavailable, "trace_unavailable", err.Error())
	}

	d.traceMu.Lock()
	run, err := d.trace.GetRun(b.TraceID)
	d.traceMu.Unlock()
	if err != nil {
		return DirectDeliverResponse{}, directError(http.StatusServiceUnavailable, "trace_unavailable", err.Error())
	}
	if run.Phase == contracts.PhaseTerminal {
		return directDeliverResponseFromRun(run, false), nil
	}

	targetName := rule.Target
	if targetName == "" {
		targetName = targetID
	}

	isDryRun := req.DryRun || cfg.Bridge.DryRun || rule.DryRun || cfg.endpointDryRun(rule.TargetGroupID)
	if isDryRun {
		if err := d.tracePhase(b, contracts.PhaseChecking, "direct_delivery"); err != nil {
			return DirectDeliverResponse{}, directError(http.StatusServiceUnavailable, "trace_unavailable", err.Error())
		}
		if !d.traceTerminal(b, contracts.OutcomeSuppressed, "dry_run") {
			return DirectDeliverResponse{}, directError(http.StatusServiceUnavailable, "trace_unavailable", "failed to record dry-run terminal state")
		}
		d.mu.Lock()
		d.state.Stats.TotalDryRun++
		_ = d.saveStateLocked()
		d.mu.Unlock()
		d.Log("DRY-RUN", "群「%s」直发演练已拦截 (%s)", targetName, trigger)

		d.traceMu.Lock()
		run, err = d.trace.GetRun(b.TraceID)
		d.traceMu.Unlock()
		if err != nil {
			return DirectDeliverResponse{}, directError(http.StatusServiceUnavailable, "trace_unavailable", err.Error())
		}
		return directDeliverResponseFromRun(run, false), nil
	}

	if err := d.tracePhase(b, contracts.PhaseChecking, "direct_delivery"); err != nil {
		return DirectDeliverResponse{}, directError(http.StatusServiceUnavailable, "trace_unavailable", err.Error())
	}

	deliverErr := d.deliver(ctx, b, trimmedText)

	d.traceMu.Lock()
	run, err = d.trace.GetRun(b.TraceID)
	d.traceMu.Unlock()
	if err != nil {
		if deliverErr != nil {
			return DirectDeliverResponse{}, directError(http.StatusServiceUnavailable, "trace_unavailable", deliverErr.Error())
		}
		return DirectDeliverResponse{}, directError(http.StatusServiceUnavailable, "trace_unavailable", err.Error())
	}
	d.Log("INFO", "群「%s」直发完成 (%s outcome=%s delivery=%s)", targetName, trigger, run.Outcome, run.Delivery)
	return directDeliverResponseFromRun(run, false), nil
}

func (d *Dispatcher) handleDirectDeliver(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		traceJSON(w, http.StatusMethodNotAllowed, map[string]string{"code": "method_not_allowed", "error": "POST method required"})
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		traceJSON(w, http.StatusUnsupportedMediaType, map[string]string{"code": "unsupported_media_type", "error": "application/json is required"})
		return
	}
	var req DirectDeliverRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxDirectDeliverBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			traceJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"code": "too_large", "error": "request body too large"})
			return
		}
		traceJSON(w, http.StatusBadRequest, map[string]string{"code": "validation", "error": "invalid JSON or unknown field"})
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			traceJSON(w, http.StatusRequestEntityTooLarge, map[string]string{"code": "too_large", "error": "request body too large"})
			return
		}
		traceJSON(w, http.StatusBadRequest, map[string]string{"code": "validation", "error": "exactly one JSON object is required"})
		return
	}

	resp, err := d.DirectDeliver(r.Context(), req)
	if err != nil {
		var typed *DirectDeliverError
		if errors.As(err, &typed) {
			traceJSON(w, typed.Status, map[string]string{"code": typed.Code, "error": typed.Message})
			return
		}
		traceJSON(w, http.StatusInternalServerError, map[string]string{"code": "internal", "error": err.Error()})
		return
	}
	traceJSON(w, http.StatusOK, resp)
}
