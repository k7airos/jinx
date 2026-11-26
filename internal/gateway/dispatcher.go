package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	"jinx/internal/observability"
	"jinx/internal/supervisor"
)

type DispatcherDrainCoordinator struct {
	base   string
	client *http.Client
}

var _ supervisor.DrainCoordinator = (*DispatcherDrainCoordinator)(nil)

func NewDispatcherDrainCoordinator(base string, client *http.Client) (*DispatcherDrainCoordinator, error) {
	validated, err := origin(base)
	if err != nil {
		return nil, err
	}
	configured := http.Client{Timeout: 3 * time.Second}
	if client != nil {
		configured = *client
		if configured.Timeout <= 0 {
			configured.Timeout = 3 * time.Second
		}
	}
	configured.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if configured.Transport == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		configured.Transport = transport
	}
	return &DispatcherDrainCoordinator{base: validated, client: &configured}, nil
}

func allowedEndpoint(method, path string) bool {
	if method == http.MethodPost {
		return path == "/api/control/action"
	}
	if method != http.MethodGet {
		return false
	}
	switch path {
	case "/api/control/snapshot", "/api/config", "/api/trace/snapshot":
		return true
	}
	return strings.HasPrefix(path, "/api/control/operations/") && safeID(strings.TrimPrefix(path, "/api/control/operations/"))
}

func (coordinator *DispatcherDrainCoordinator) request(ctx context.Context, method, path string, input, target any) error {
	if !allowedEndpoint(method, path) {
		return failure(http.StatusForbidden, "forbidden")
	}
	var body []byte
	if input != nil {
		var err error
		body, err = json.Marshal(input)
		if err != nil || len(body) > maxBody {
			return failure(http.StatusBadRequest, "validation")
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, coordinator.base+path, bytes.NewReader(body))
	if err != nil {
		return failure(http.StatusInternalServerError, "internal")
	}
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := coordinator.client.Do(request)
	if err != nil {
		return failure(http.StatusServiceUnavailable, "upstream_unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return failure(response.StatusCode, "upstream_rejected")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
	if err != nil || len(data) > 4<<20 || json.Unmarshal(data, target) != nil || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		return failure(http.StatusBadGateway, "upstream_invalid")
	}
	return nil
}

func (coordinator *DispatcherDrainCoordinator) Controls(ctx context.Context) (dispatcher.ControlSnapshot, error) {
	var snapshot dispatcher.ControlSnapshot
	var data json.RawMessage
	err := coordinator.request(ctx, http.MethodGet, "/api/control/snapshot", nil, &data)
	if err != nil {
		return snapshot, err
	}
	if !validControlSnapshot(data) || json.Unmarshal(data, &snapshot) != nil {
		return dispatcher.ControlSnapshot{}, failure(http.StatusBadGateway, "upstream_invalid")
	}
	return snapshot, nil
}

func controlValue(data json.RawMessage, target any) bool {
	return len(data) > 0 && !bytes.Equal(bytes.TrimSpace(data), []byte("null")) && json.Unmarshal(data, target) == nil
}

func controlString(data json.RawMessage) bool {
	var value string
	return controlValue(data, &value) && value != ""
}

func validScopeControl(data json.RawMessage) bool {
	var fields map[string]json.RawMessage
	var paused bool
	var boundary uint64
	var changedAt time.Time
	return controlValue(data, &fields) && controlValue(fields["paused"], &paused) && controlString(fields["revision"]) && controlValue(fields["boundary"], &boundary) && controlValue(fields["changedAt"], &changedAt)
}

func validControlSnapshot(data json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if strictObject(data, &fields) != nil || !controlString(fields["revision"]) || !validScopeControl(fields["global"]) {
		return false
	}
	var groups map[string]json.RawMessage
	if !controlValue(fields["groups"], &groups) {
		return false
	}
	for _, group := range groups {
		if !validScopeControl(group) {
			return false
		}
	}
	maintenance := fields["maintenance"]
	if len(maintenance) == 0 || bytes.Equal(bytes.TrimSpace(maintenance), []byte("null")) {
		return true
	}
	var view map[string]json.RawMessage
	if !controlValue(maintenance, &view) || !controlString(view["maintenanceId"]) || !controlString(view["revision"]) || !controlString(view["stage"]) {
		return false
	}
	var boundary uint64
	if !controlValue(view["boundary"], &boundary) {
		return false
	}
	for _, field := range []string{"active", "recoveryRequired"} {
		var value bool
		if !controlValue(view[field], &value) {
			return false
		}
	}
	for _, field := range []string{"startedAt", "deadline"} {
		var value time.Time
		if !controlValue(view[field], &value) {
			return false
		}
	}
	var scope map[string]json.RawMessage
	var kind, groupID string
	if !controlValue(view["scope"], &scope) || !controlValue(scope["kind"], &kind) {
		return false
	}
	if value, exists := scope["groupId"]; exists && !controlValue(value, &groupID) {
		return false
	}
	if kind != "global" && kind != "group" || kind == "global" && groupID != "" || kind == "group" && groupID == "" {
		return false
	}
	var counts map[string]json.RawMessage
	if !controlValue(view["counts"], &counts) {
		return false
	}
	for _, field := range []string{"pending", "activeWorkers", "unknownDelivery", "remoteUnsettled", "unresolvedRuns"} {
		var count int
		if !controlValue(counts[field], &count) || count < 0 {
			return false
		}
	}
	var phases map[string]json.RawMessage
	if !controlValue(counts["phases"], &phases) {
		return false
	}
	for _, value := range phases {
		var count int
		if !controlValue(value, &count) || count < 0 {
			return false
		}
	}
	var blockers []json.RawMessage
	if !controlValue(view["blockers"], &blockers) {
		return false
	}
	for _, value := range blockers {
		var blocker string
		if !controlValue(value, &blocker) {
			return false
		}
	}
	return true
}

func (coordinator *DispatcherDrainCoordinator) Action(ctx context.Context, input dispatcher.ControlRequest) (dispatcher.ControlOperation, error) {
	var operation dispatcher.ControlOperation
	err := coordinator.request(ctx, http.MethodPost, "/api/control/action", input, &operation)
	if err == nil && (operation.OperationID == "" || operation.RequestID != input.RequestID || operation.Revision == "") {
		err = failure(http.StatusBadGateway, "upstream_invalid")
	}
	return operation, err
}

func (coordinator *DispatcherDrainCoordinator) Operation(ctx context.Context, id string) (dispatcher.ControlOperation, error) {
	var operation dispatcher.ControlOperation
	if !safeID(id) {
		return operation, failure(http.StatusBadRequest, "validation")
	}
	err := coordinator.request(ctx, http.MethodGet, "/api/control/operations/"+id, nil, &operation)
	if err == nil && operation.OperationID != id {
		err = failure(http.StatusBadGateway, "upstream_invalid")
	}
	return operation, err
}

func paused(snapshot dispatcher.ControlSnapshot) bool {
	if snapshot.Global.Paused {
		return true
	}
	for _, group := range snapshot.Groups {
		if group.Paused {
			return true
		}
	}
	return false
}

func (coordinator *DispatcherDrainCoordinator) Snapshot(ctx context.Context) (supervisor.ControlSnapshot, error) {
	control, err := coordinator.Controls(ctx)
	if err != nil {
		return supervisor.ControlSnapshot{}, err
	}
	var config struct {
		Rules struct {
			DecisionRequired *bool `json:"laya_filter"`
		} `json:"rules"`
	}
	if err := coordinator.request(ctx, http.MethodGet, "/api/config", nil, &config); err != nil {
		return supervisor.ControlSnapshot{}, err
	}
	if config.Rules.DecisionRequired == nil {
		return supervisor.ControlSnapshot{}, failure(http.StatusBadGateway, "upstream_invalid")
	}
	var trace observability.Snapshot
	if err := coordinator.request(ctx, http.MethodGet, "/api/trace/snapshot", nil, &trace); err != nil {
		return supervisor.ControlSnapshot{}, err
	}
	if trace.StreamID == "" || trace.Runs == nil {
		return supervisor.ControlSnapshot{}, failure(http.StatusBadGateway, "upstream_invalid")
	}
	encoded, _ := json.Marshal(config)
	digest := sha256.Sum256(encoded)
	result := supervisor.ControlSnapshot{Version: control.Revision, DependencyVersion: hex.EncodeToString(digest[:]), DecisionRequired: *config.Rules.DecisionRequired, Paused: paused(control)}
	for _, run := range trace.Runs {
		if contracts.ValidateRun(run) != nil {
			return supervisor.ControlSnapshot{}, failure(http.StatusBadGateway, "upstream_invalid")
		}
		if run.Delivery == contracts.DeliveryUnknown || run.Delivery == contracts.DeliverySubmitting || run.Delivery == contracts.DeliverySubmitted {
			result.UnknownDelivery = true
		}
		for _, attempt := range run.Attempts {
			if attempt.RemoteExecution == contracts.RemoteMayBeRunning || attempt.RemoteExecution == contracts.RemoteUnknown && attempt.Status != contracts.AttemptSucceeded && attempt.Status != contracts.AttemptFailed && attempt.Status != contracts.AttemptCanceled {
				result.UnknownDelivery = true
			}
		}
	}
	if maintenance := control.Maintenance; maintenance != nil {
		result.UnknownDelivery = result.UnknownDelivery || maintenance.Counts.UnknownDelivery > 0 || maintenance.Counts.RemoteUnsettled > 0
	}
	return result, nil
}

func (coordinator *DispatcherDrainCoordinator) Drain(ctx context.Context, scope supervisor.Scope) (string, error) {
	if scope != supervisor.Global {
		return "", failure(http.StatusBadRequest, "validation")
	}
	operation, err := coordinator.maintenanceAction(ctx, "drain", "")
	if err != nil {
		return "", err
	}
	if !safeID(operation.MaintenanceID) || operation.Status != "succeeded" {
		return "", failure(http.StatusBadGateway, "drain_unknown")
	}
	return operation.MaintenanceID, nil
}

func (coordinator *DispatcherDrainCoordinator) Poll(ctx context.Context, id string) (supervisor.DrainStatus, error) {
	if !safeID(id) {
		return supervisor.DrainStatus{}, failure(http.StatusBadRequest, "validation")
	}
	snapshot, err := coordinator.Controls(ctx)
	if err != nil {
		return supervisor.DrainStatus{}, err
	}
	maintenance := snapshot.Maintenance
	if maintenance == nil || maintenance.ID != id || maintenance.Revision == "" {
		return supervisor.DrainStatus{}, failure(http.StatusConflict, "maintenance_changed")
	}
	counts := maintenance.Counts
	status := supervisor.DrainStatus{MaintenanceID: id, State: "blocked", Global: maintenance.Scope.Kind == "global" && maintenance.Scope.GroupID == "", Persisted: maintenance.Active && maintenance.Revision != "", Paused: paused(snapshot), UnknownDelivery: counts.UnknownDelivery > 0}
	validCounts := counts.Pending >= 0 && counts.ActiveWorkers >= 0 && counts.UnknownDelivery >= 0 && counts.RemoteUnsettled >= 0 && counts.UnresolvedRuns >= 0 && counts.Phases != nil
	phasesSettled := true
	for phase, count := range counts.Phases {
		validCounts = validCounts && count >= 0
		phasesSettled = phasesSettled && (phase == string(contracts.PhaseTerminal) || count == 0)
	}
	status.ConfirmedSettled = validCounts && phasesSettled && counts.UnknownDelivery == 0 && counts.UnresolvedRuns == 0
	status.RemoteSettled = validCounts && counts.RemoteUnsettled == 0 && counts.ActiveWorkers == 0
	for _, blocker := range maintenance.Blockers {
		switch blocker {
		case "blocked_by_pause":
			status.Paused = true
		case "unknown_delivery":
			status.UnknownDelivery = true
			status.ConfirmedSettled = false
		case "remote_unsettled":
			status.RemoteSettled = false
		case "deadline_exceeded":
			status.State = "timedout"
		}
	}
	if !maintenance.Active {
		status.State = "canceled"
	} else if maintenance.Stage == "draining" && len(maintenance.Blockers) == 0 && !maintenance.RecoveryRequired && validCounts {
		status.State = "draining"
	} else if maintenance.Stage == "ready" && maintenance.Blockers != nil && len(maintenance.Blockers) == 0 && !maintenance.RecoveryRequired && status.Global && status.Persisted && !status.Paused && status.ConfirmedSettled && status.RemoteSettled && counts.Pending == 0 && time.Now().Before(maintenance.Deadline) {
		status.State = "ready"
	}
	return status, nil
}

func (coordinator *DispatcherDrainCoordinator) maintenanceAction(ctx context.Context, kind, id string) (dispatcher.ControlOperation, error) {
	snapshot, err := coordinator.Controls(ctx)
	if err != nil {
		return dispatcher.ControlOperation{}, err
	}
	if id != "" && (snapshot.Maintenance == nil || snapshot.Maintenance.ID != id || !snapshot.Maintenance.Active || snapshot.Maintenance.Scope.Kind != "global") {
		return dispatcher.ControlOperation{}, failure(http.StatusConflict, "maintenance_changed")
	}
	var token [24]byte
	if _, err := rand.Read(token[:]); err != nil {
		return dispatcher.ControlOperation{}, failure(http.StatusInternalServerError, "internal")
	}
	return coordinator.Action(ctx, dispatcher.ControlRequest{RequestID: "supervisor-" + hex.EncodeToString(token[:]), ExpectedRevision: snapshot.Revision, Action: dispatcher.ControlAction{Kind: kind, Scope: dispatcher.ControlScope{Kind: "global"}, MaintenanceID: id}})
}

func (coordinator *DispatcherDrainCoordinator) Cancel(ctx context.Context, id string) error {
	return coordinator.changeMaintenance(ctx, "cancel_maintenance", id)
}

func (coordinator *DispatcherDrainCoordinator) Continue(ctx context.Context, id string) error {
	return coordinator.changeMaintenance(ctx, "continue_maintenance", id)
}

func (coordinator *DispatcherDrainCoordinator) changeMaintenance(ctx context.Context, kind, id string) error {
	if !safeID(id) {
		return failure(http.StatusBadRequest, "validation")
	}
	operation, err := coordinator.maintenanceAction(ctx, kind, id)
	if err != nil {
		return err
	}
	if operation.Status != "succeeded" || operation.MaintenanceID != id {
		return failure(http.StatusBadGateway, "drain_unknown")
	}
	return nil
}
