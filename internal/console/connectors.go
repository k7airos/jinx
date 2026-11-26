package console

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

type ConnectorTargetView struct {
	ContainerID string  `json:"containerId"`
	Label       *string `json:"label"`
	Enabled     bool    `json:"enabled"`
}

type ConnectorView struct {
	Platform          contracts.Platform     `json:"platform"`
	Configured        bool                   `json:"configured"`
	State             string                 `json:"state"`
	Availability      Availability           `json:"availability"`
	DryRun            *bool                  `json:"dryRun"`
	Capabilities      []contracts.ActionKind `json:"capabilities"`
	Controls          []string               `json:"controls"`
	AccountID         *string                `json:"accountId"`
	ConnectorName     *string                `json:"connectorName"`
	Targets           []ConnectorTargetView  `json:"targets"`
	LastPollAt        *time.Time             `json:"lastPollAt"`
	LastPollErrorCode *string                `json:"lastPollErrorCode"`
	Reason            *string                `json:"reason"`
	ProtocolVersion   *int                   `json:"protocolVersion"`
	ObservedAt        time.Time              `json:"observedAt"`
}

type ConnectorDryRunPlanInput struct {
	RequestID      string `json:"requestId"`
	ExpectedDryRun bool   `json:"expectedDryRun"`
	TargetDryRun   bool   `json:"targetDryRun"`
}

type ConnectorDryRunPlan struct {
	ID            string             `json:"id"`
	Token         string             `json:"token"`
	Platform      contracts.Platform `json:"platform"`
	CurrentDryRun bool               `json:"currentDryRun"`
	TargetDryRun  bool               `json:"targetDryRun"`
	Persisted     bool               `json:"persisted"`
	ExpiresAt     time.Time          `json:"expiresAt"`
	Warnings      []Warning          `json:"warnings"`
}

type ConnectorDryRunApplyInput struct {
	RequestID string `json:"requestId"`
	PlanID    string `json:"planId"`
	Token     string `json:"token"`
	Confirm   bool   `json:"confirm"`
}

type ConnectorDryRunApplyResult struct {
	OK               bool               `json:"ok"`
	Platform         contracts.Platform `json:"platform"`
	PreviousDryRun   bool               `json:"previousDryRun"`
	DryRun           bool               `json:"dryRun"`
	Persisted        bool               `json:"persisted"`
	ReadbackVerified bool               `json:"readbackVerified"`
	Status           string             `json:"status"`
	RequestID        string             `json:"requestId"`
	ObservedAt       time.Time          `json:"observedAt"`
}

type savedConnectorPlan struct {
	Plan      ConnectorDryRunPlan
	RequestID string
	InputHash string
	ApplyID   string
	Result    *ConnectorDryRunApplyResult
}

func connectorServiceID(platform contracts.Platform) (string, error) {
	switch platform {
	case contracts.PlatformBilibili, contracts.PlatformWeb:
		return "connector:" + string(platform), nil
	default:
		return "", failure("validation", "Connector platform must be bilibili or web")
	}
}

func cleanTargetLabel(raw string) *string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || !utf8.ValidString(trimmed) {
		return nil
	}
	var b strings.Builder
	count := 0
	for _, r := range trimmed {
		if unicode.IsControl(r) {
			if r == '\n' || r == '\r' || r == '\t' {
				b.WriteRune(' ')
				count++
			}
		} else {
			b.WriteRune(r)
			count++
		}
		if count >= 128 {
			break
		}
	}
	cleaned := strings.TrimSpace(b.String())
	if cleaned == "" {
		return nil
	}
	return &cleaned
}

func safeReasonToken(raw string) *string {
	if raw == "" {
		return nil
	}
	if tok := safeToken(raw); tok != "" {
		return &tok
	}
	redacted := string(contracts.ReasonUpstreamRedacted)
	return &redacted
}

func validActionKind(raw string) (contracts.ActionKind, bool) {
	switch contracts.ActionKind(raw) {
	case contracts.ActionReplyText, contracts.ActionEditText, contracts.ActionDeleteContent, contracts.ActionThreadReply:
		return contracts.ActionKind(raw), true
	default:
		return "", false
	}
}

func (facade *Facade) probeConnector(ctx context.Context, platform contracts.Platform) ConnectorView {
	observed := time.Now().UTC()
	view := ConnectorView{
		Platform:     platform,
		Configured:   false,
		State:        "unsupported",
		Availability: unsupported(),
		Capabilities: []contracts.ActionKind{},
		Controls:     []string{},
		Targets:      []ConnectorTargetView{},
		ObservedAt:   observed,
	}
	serviceID, err := connectorServiceID(platform)
	if err != nil || facade.backend.bases[serviceID] == "" {
		return view
	}
	view.Configured = true
	view.State = "unreachable"
	view.Availability = hostAvailability(true, "unreachable")

	data, _, status, err := facade.backend.request(ctx, serviceID, http.MethodGet, "/health", "", nil)
	view.ObservedAt = time.Now().UTC()
	if err != nil || (status != http.StatusOK && status != http.StatusServiceUnavailable) {
		return view
	}
	var raw connector.HealthResponse
	if json.Unmarshal(data, &raw) != nil || raw.ProtocolVersion < connector.ProtocolVersion || contracts.Platform(raw.Platform) != platform {
		return view
	}
	protoVer := raw.ProtocolVersion
	dryRun := raw.DryRun
	view.ProtocolVersion = &protoVer
	view.DryRun = &dryRun

	switch {
	case status == http.StatusServiceUnavailable || !raw.Ready:
		view.State = "not_ready"
	case raw.Status == "ready" || raw.Status == "ok" || raw.Status == "healthy":
		view.State = "ready"
	default:
		view.State = "not_ready"
	}
	view.Availability = hostAvailability(true, view.State)
	if raw.AccountID != "" && contracts.ValidID(raw.AccountID) {
		accountID := raw.AccountID
		view.AccountID = &accountID
	}
	if raw.ConnectorName != "" && contracts.ValidID(raw.ConnectorName) {
		name := raw.ConnectorName
		view.ConnectorName = &name
	}
	seenCap := map[contracts.ActionKind]bool{}
	for _, capName := range raw.Capabilities {
		if kind, ok := validActionKind(capName); ok && !seenCap[kind] {
			seenCap[kind] = true
			view.Capabilities = append(view.Capabilities, kind)
		}
	}
	sort.Slice(view.Capabilities, func(i, j int) bool { return view.Capabilities[i] < view.Capabilities[j] })

	seenCtrl := map[string]bool{}
	for _, ctrl := range raw.Controls {
		if ctrl == "dry_run" && !seenCtrl[ctrl] {
			seenCtrl[ctrl] = true
			view.Controls = append(view.Controls, ctrl)
		}
	}
	seenTarget := map[string]bool{}
	for _, target := range raw.Targets {
		targetPlatform, _, ok := contracts.ParseContainerID(target.ContainerID)
		if !ok || targetPlatform != platform || seenTarget[target.ContainerID] {
			continue
		}
		seenTarget[target.ContainerID] = true
		view.Targets = append(view.Targets, ConnectorTargetView{
			ContainerID: target.ContainerID,
			Label:       cleanTargetLabel(target.Label),
			Enabled:     target.Enabled,
		})
	}
	sort.Slice(view.Targets, func(i, j int) bool { return view.Targets[i].ContainerID < view.Targets[j].ContainerID })

	if raw.LastPollAt != nil && !raw.LastPollAt.IsZero() {
		ts := raw.LastPollAt.UTC()
		view.LastPollAt = &ts
	}
	view.LastPollErrorCode = safeReasonToken(raw.LastPollErrorCode)
	view.Reason = safeReasonToken(raw.Reason)
	return view
}

func (facade *Facade) ListConnectors(ctx context.Context) Envelope {
	items := make([]ConnectorView, 0, 2)
	freshness := "fresh"
	for _, platform := range []contracts.Platform{contracts.PlatformBilibili, contracts.PlatformWeb} {
		view := facade.probeConnector(ctx, platform)
		if view.Configured && view.State == "unreachable" {
			freshness = "unknown"
		}
		items = append(items, view)
	}
	result := envelope(items, "", "connector_health_probes", "partial")
	result.Freshness = freshness
	return result
}

func hasControl(controls []string, name string) bool {
	for _, c := range controls {
		if c == name {
			return true
		}
	}
	return false
}

func pruneConnectorPlansLocked(plans map[string]*savedConnectorPlan, now time.Time) {
	if len(plans) < 128 {
		return
	}
	for id, saved := range plans {
		if saved.ApplyID == "" && !now.Before(saved.Plan.ExpiresAt) {
			delete(plans, id)
		}
	}
	if len(plans) < 128 {
		return
	}
	var oldestID string
	var oldestAt time.Time
	for id, saved := range plans {
		if oldestID == "" || saved.Plan.ExpiresAt.Before(oldestAt) {
			oldestID = id
			oldestAt = saved.Plan.ExpiresAt
		}
	}
	if oldestID != "" {
		delete(plans, oldestID)
	}
}

func (facade *Facade) PlanConnectorDryRun(ctx context.Context, platform contracts.Platform, input ConnectorDryRunPlanInput) (ConnectorDryRunPlan, error) {
	serviceID, err := connectorServiceID(platform)
	if err != nil {
		return ConnectorDryRunPlan{}, err
	}
	if !validRequestID(input.RequestID) {
		return ConnectorDryRunPlan{}, failure("validation", "Request ID is required")
	}
	if facade.backend.bases[serviceID] == "" {
		return ConnectorDryRunPlan{}, failure("unsupported", "Connector is not configured on control-plane")
	}

	facade.connectorMu.Lock()
	defer facade.connectorMu.Unlock()

	inputHash := digest(struct {
		Platform contracts.Platform       `json:"platform"`
		Input    ConnectorDryRunPlanInput `json:"input"`
	}{Platform: platform, Input: input})

	for _, saved := range facade.connectorPlans {
		if saved.RequestID == input.RequestID {
			if saved.InputHash != inputHash {
				return ConnectorDryRunPlan{}, failure("conflict", "Request ID was already used for another connector plan")
			}
			return saved.Plan, nil
		}
	}

	current := facade.probeConnector(ctx, platform)
	if current.State == "unreachable" || current.DryRun == nil {
		return ConnectorDryRunPlan{}, failure("unavailable", "Connector health is unreachable")
	}
	if !hasControl(current.Controls, "dry_run") {
		return ConnectorDryRunPlan{}, failure("unsupported", "Connector does not advertise runtime dry_run control")
	}
	if *current.DryRun != input.ExpectedDryRun {
		return ConnectorDryRunPlan{}, failure("conflict", "Connector dry_run state changed; refresh and plan again")
	}
	if input.TargetDryRun == *current.DryRun {
		return ConnectorDryRunPlan{}, failure("validation", "Target dry_run matches the current connector state")
	}

	warnings := []Warning{
		{
			Code:    "runtime_only_not_persisted",
			Message: "Connector runtime dry_run toggle applies in-memory only; restarting the connector reverts to its startup config",
		},
	}
	if *current.DryRun && !input.TargetDryRun {
		warnings = append(warnings, Warning{
			Code:    "live_external_writes_enabled",
			Message: "Disabling dry_run enables LIVE external replies/actions from this connector immediately",
		})
	} else if !*current.DryRun && input.TargetDryRun {
		warnings = append(warnings, Warning{
			Code:    "external_writes_blocked",
			Message: "Enabling dry_run causes /v1/actions requests to fail pre-submission with reason dry_run",
		})
	}

	now := time.Now().UTC()
	pruneConnectorPlansLocked(facade.connectorPlans, now)
	plan := ConnectorDryRunPlan{
		ID:            newID(),
		Token:         newID(),
		Platform:      platform,
		CurrentDryRun: *current.DryRun,
		TargetDryRun:  input.TargetDryRun,
		Persisted:     false,
		ExpiresAt:     now.Add(5 * time.Minute),
		Warnings:      warnings,
	}
	facade.connectorPlans[plan.ID] = &savedConnectorPlan{
		Plan:      plan,
		RequestID: input.RequestID,
		InputHash: inputHash,
	}
	return plan, nil
}

func (facade *Facade) ApplyConnectorDryRun(ctx context.Context, platform contracts.Platform, input ConnectorDryRunApplyInput) (ConnectorDryRunApplyResult, error) {
	serviceID, err := connectorServiceID(platform)
	if err != nil {
		return ConnectorDryRunApplyResult{}, err
	}
	if !validRequestID(input.RequestID) {
		return ConnectorDryRunApplyResult{}, failure("validation", "Request ID is required")
	}
	if !input.Confirm {
		return ConnectorDryRunApplyResult{}, failure("validation", "Explicit confirmation is required before changing connector dry_run")
	}
	if facade.backend.bases[serviceID] == "" {
		return ConnectorDryRunApplyResult{}, failure("unsupported", "Connector is not configured on control-plane")
	}

	facade.connectorMu.Lock()
	defer facade.connectorMu.Unlock()

	saved := facade.connectorPlans[input.PlanID]
	if saved == nil || saved.Plan.Platform != platform || subtle.ConstantTimeCompare([]byte(input.Token), []byte(saved.Plan.Token)) != 1 {
		return ConnectorDryRunApplyResult{}, failure("forbidden", "Connector dry-run plan token is invalid")
	}
	for _, previous := range facade.connectorPlans {
		if previous.ApplyID == input.RequestID && previous != saved {
			return ConnectorDryRunApplyResult{}, failure("conflict", "Request ID was already used for another connector control request")
		}
	}
	if saved.ApplyID != "" {
		if saved.ApplyID != input.RequestID {
			return ConnectorDryRunApplyResult{}, failure("conflict", "Connector dry-run plan has already been submitted")
		}
		if saved.Result != nil {
			return *saved.Result, nil
		}
		return ConnectorDryRunApplyResult{}, failure("unknown", "Connector dry_run update outcome or readback was unverified; refresh connector health before planning again")
	}
	if !time.Now().Before(saved.Plan.ExpiresAt) {
		return ConnectorDryRunApplyResult{}, failure("stale", "Connector dry-run plan expired; refresh and plan again")
	}

	current := facade.probeConnector(ctx, platform)
	if current.State == "unreachable" || current.DryRun == nil {
		return ConnectorDryRunApplyResult{}, failure("unavailable", "Connector health is unreachable")
	}
	if !hasControl(current.Controls, "dry_run") {
		return ConnectorDryRunApplyResult{}, failure("unsupported", "Connector does not advertise runtime dry_run control")
	}
	if *current.DryRun != saved.Plan.CurrentDryRun {
		return ConnectorDryRunApplyResult{}, failure("conflict", "Connector dry_run state changed since preview")
	}

	saved.ApplyID = input.RequestID
	body, _ := json.Marshal(connector.DryRunControlRequest{
		DryRun:    saved.Plan.TargetDryRun,
		RequestID: input.RequestID,
	})
	respData, _, status, writeErr := facade.backend.request(ctx, serviceID, http.MethodPost, "/v1/control/dry-run", "", body)
	var ctrlResp connector.DryRunControlResponse
	decodeErr := json.Unmarshal(respData, &ctrlResp)

	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), facade.backend.client.Timeout)
	readback := facade.probeConnector(readCtx, platform)
	cancel()

	if writeErr == nil && status >= 200 && status < 300 && decodeErr == nil && ctrlResp.OK &&
		readback.State != "unreachable" && readback.DryRun != nil && *readback.DryRun == saved.Plan.TargetDryRun {
		res := ConnectorDryRunApplyResult{
			OK:               true,
			Platform:         platform,
			PreviousDryRun:   saved.Plan.CurrentDryRun,
			DryRun:           *readback.DryRun,
			Persisted:        ctrlResp.Persisted,
			ReadbackVerified: true,
			Status:           "succeeded",
			RequestID:        input.RequestID,
			ObservedAt:       readback.ObservedAt,
		}
		saved.Result = &res
		return res, nil
	}

	if writeErr == nil && status >= 400 && status < 500 &&
		readback.State != "unreachable" && readback.DryRun != nil && *readback.DryRun == saved.Plan.CurrentDryRun {
		if status == http.StatusConflict {
			return ConnectorDryRunApplyResult{}, failure("conflict", "Connector rejected dry_run change and state remains unchanged")
		}
		return ConnectorDryRunApplyResult{}, failure("validation", "Connector rejected dry_run change and state remains unchanged")
	}

	return ConnectorDryRunApplyResult{}, failure("unknown", "Connector dry_run update outcome or readback is unverified; inspect connector health before planning again")
}
