package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	gotesting "testing"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	"jinx/internal/observability"
)

func safetyControlPayload(testing *gotesting.T) map[string]any {
	testing.Helper()
	snapshot := readyControl()
	snapshot.Global = dispatcher.ScopeControl{Revision: "5", Boundary: 1, ChangedAt: time.Now().UTC()}
	snapshot.Groups["group"] = snapshot.Global
	snapshot.Maintenance.StartedAt = snapshot.Global.ChangedAt
	var payload map[string]any
	if err := json.Unmarshal([]byte(encoded(snapshot)), &payload); err != nil {
		testing.Fatal(err)
	}
	return payload
}

func changeSafetyField(payload map[string]any, path string, value any, remove bool) {
	parts := strings.Split(path, ".")
	for _, part := range parts[:len(parts)-1] {
		payload = payload[part].(map[string]any)
	}
	field := parts[len(parts)-1]
	if remove {
		delete(payload, field)
	} else {
		payload[field] = value
	}
}

func safetyCoordinator(testing *gotesting.T, control, trace any) *DispatcherDrainCoordinator {
	testing.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			testing.Errorf("unexpected write: %s %s", request.Method, request.URL.Path)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var value any
		switch request.URL.Path {
		case "/api/control/snapshot":
			value = control
		case "/api/config":
			value = json.RawMessage(`{"rules":{"laya_filter":false}}`)
		case "/api/trace/snapshot":
			value = trace
		default:
			testing.Errorf("unexpected endpoint: %s", request.URL.Path)
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewEncoder(writer).Encode(value); err != nil {
			testing.Error(err)
		}
	}))
	testing.Cleanup(server.Close)
	coordinator, err := NewDispatcherDrainCoordinator(server.URL, nil)
	if err != nil {
		testing.Fatal(err)
	}
	return coordinator
}

func requireUpstreamInvalid(testing *gotesting.T, err error) {
	testing.Helper()
	var problem *Problem
	if !errors.As(err, &problem) || problem.Status != http.StatusBadGateway || problem.Code != "upstream_invalid" {
		testing.Fatalf("want 502 upstream_invalid, got %v", err)
	}
}

func TestSafetyControlsRejectIncompleteFields(testing *gotesting.T) {
	fields := []string{
		"global", "global.paused", "maintenance.recoveryRequired",
		"revision", "global.revision", "global.boundary", "global.changedAt", "groups",
		"groups.group.paused", "groups.group.revision", "groups.group.boundary", "groups.group.changedAt",
		"maintenance.maintenanceId", "maintenance.revision", "maintenance.scope", "maintenance.scope.kind",
		"maintenance.boundary", "maintenance.startedAt", "maintenance.deadline", "maintenance.active",
		"maintenance.stage", "maintenance.counts", "maintenance.counts.pending", "maintenance.counts.activeWorkers",
		"maintenance.counts.unknownDelivery", "maintenance.counts.remoteUnsettled", "maintenance.counts.unresolvedRuns",
		"maintenance.counts.phases", "maintenance.blockers",
	}
	for _, field := range fields {
		for _, variant := range []string{"missing", "null", "wrong_type"} {
			testing.Run(field+"/"+variant, func(testing *gotesting.T) {
				payload := safetyControlPayload(testing)
				var value any
				if variant == "wrong_type" {
					value = []bool{false}
				}
				changeSafetyField(payload, field, value, variant == "missing")
				coordinator := safetyCoordinator(testing, payload, nil)
				status, err := coordinator.Poll(context.Background(), "maintenance-1")
				requireUpstreamInvalid(testing, err)
				if status.State == "ready" {
					testing.Fatal("invalid snapshot authorized ready")
				}
				response := invoke((&Gateway{dispatcher: coordinator}).Handler(), http.MethodGet, "/console/v1/controls", "")
				if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), `"code":"upstream_invalid"`) {
					testing.Fatalf("unexpected HTTP response: %d %s", response.Code, response.Body)
				}
			})
		}
	}
}

func TestSafetyControlsRejectInvalidValues(testing *gotesting.T) {
	for _, test := range []struct {
		field string
		value any
	}{
		{"revision", ""}, {"global.revision", ""}, {"groups.group.revision", ""},
		{"maintenance.maintenanceId", ""}, {"maintenance.revision", ""}, {"maintenance.stage", ""},
		{"global.changedAt", "not-a-time"}, {"groups.group.changedAt", "not-a-time"},
		{"maintenance.startedAt", "not-a-time"}, {"maintenance.deadline", "not-a-time"},
		{"global.boundary", -1}, {"groups.group.boundary", 0.5}, {"maintenance.boundary", -1},
		{"groups.group", nil}, {"groups.group", false},
		{"maintenance", false}, {"maintenance.scope.kind", "other"}, {"maintenance.scope.kind", "group"},
		{"maintenance.scope.groupId", "unexpected-group"}, {"maintenance.scope.groupId", nil}, {"maintenance.scope.groupId", false},
		{"maintenance.counts.pending", -1}, {"maintenance.counts.activeWorkers", -1},
		{"maintenance.counts.unknownDelivery", -1}, {"maintenance.counts.remoteUnsettled", -1},
		{"maintenance.counts.unresolvedRuns", -1}, {"maintenance.counts.pending", 0.5},
		{"maintenance.counts.phases", map[string]any{"terminal": -1}},
		{"maintenance.counts.phases", map[string]any{"terminal": nil}},
		{"maintenance.counts.phases", map[string]any{"terminal": false}},
		{"maintenance.blockers", []any{nil}},
		{"maintenance.blockers", []any{false}},
		{"capabilities", false},
	} {
		testing.Run(test.field+"/"+encoded(test.value), func(testing *gotesting.T) {
			payload := safetyControlPayload(testing)
			changeSafetyField(payload, test.field, test.value, false)
			coordinator := safetyCoordinator(testing, payload, nil)
			_, err := coordinator.Poll(context.Background(), "maintenance-1")
			requireUpstreamInvalid(testing, err)
		})
	}
	for _, payload := range []string{`[]`, `null`, `{"revision":"5","Revision":"6"}`} {
		testing.Run(payload, func(testing *gotesting.T) {
			_, err := safetyCoordinator(testing, json.RawMessage(payload), nil).Controls(context.Background())
			requireUpstreamInvalid(testing, err)
		})
	}
}

func TestSafetyControlsCompleteReady(testing *gotesting.T) {
	for _, variant := range []string{"populated_groups", "empty_groups", "explicit_global_groupId"} {
		testing.Run(variant, func(testing *gotesting.T) {
			payload := safetyControlPayload(testing)
			if variant == "empty_groups" {
				payload["groups"] = map[string]any{}
			}
			if variant == "explicit_global_groupId" {
				changeSafetyField(payload, "maintenance.scope.groupId", "", false)
			}
			status, err := safetyCoordinator(testing, payload, nil).Poll(context.Background(), "maintenance-1")
			if err != nil || status.State != "ready" {
				testing.Fatalf("complete snapshot was not ready: %+v %v", status, err)
			}
		})
	}
	for _, remove := range []bool{false, true} {
		payload := safetyControlPayload(testing)
		changeSafetyField(payload, "maintenance", nil, remove)
		if _, err := safetyCoordinator(testing, payload, nil).Controls(context.Background()); err != nil {
			testing.Fatalf("absent maintenance rejected: %v", err)
		}
	}
}

func TestSafetyControlsPreserveSafetySignals(testing *gotesting.T) {
	for _, test := range []struct {
		field string
		value any
	}{
		{"global.paused", true},
		{"groups.group.paused", true},
		{"maintenance.recoveryRequired", true},
		{"maintenance.scope", map[string]any{"kind": "group", "groupId": "group"}},
	} {
		testing.Run(test.field, func(testing *gotesting.T) {
			payload := safetyControlPayload(testing)
			changeSafetyField(payload, test.field, test.value, false)
			status, err := safetyCoordinator(testing, payload, nil).Poll(context.Background(), "maintenance-1")
			if err != nil || status.State != "blocked" {
				testing.Fatalf("unsafe snapshot did not block: %+v %v", status, err)
			}
		})
	}
}

func TestSafetySnapshotValidatesEveryRun(testing *gotesting.T) {
	now := time.Now().UTC()
	valid := contracts.Run{ID: "run-1", SourceGroupID: "source", TargetGroupID: "target", AcceptedAt: now, UpdatedAt: now, LastProgressAt: now, AdmissionWatermark: contracts.Watermark{Token: "boundary", RecordedAt: now}, Trigger: "mention", MessageIDs: []string{"message-1"}, Phase: contracts.PhaseAccepted, Outcome: contracts.OutcomePending, Delivery: contracts.DeliveryNotStarted, Completeness: contracts.Complete, Versions: contracts.Versions{Dispatcher: "d1", Fingerprint: "f1"}, MissingSnapshotReason: "not_retained"}
	if err := contracts.ValidateRun(valid); err != nil {
		testing.Fatal(err)
	}
	unknown := valid
	unknown.Phase, unknown.Outcome, unknown.Delivery, unknown.ReasonCode = contracts.PhaseUnknown, contracts.OutcomeUnknown, contracts.DeliveryUnknown, "confirmation_timeout"
	unknown.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &now, BridgeRequestID: "bridge-1"}
	missingDelivery := valid
	missingDelivery.Delivery = ""
	invalidAttempt := valid
	invalidAttempt.Attempts = []contracts.Attempt{{}}
	for _, test := range []struct {
		name    string
		runs    []contracts.Run
		invalid bool
		unknown bool
	}{
		{name: "empty", runs: []contracts.Run{}},
		{name: "valid", runs: []contracts.Run{valid}},
		{name: "unknown", runs: []contracts.Run{unknown}, unknown: true},
		{name: "zero_run", runs: []contracts.Run{{}}, invalid: true},
		{name: "second_run_invalid", runs: []contracts.Run{valid, {}}, invalid: true},
		{name: "missing_delivery", runs: []contracts.Run{missingDelivery}, invalid: true},
		{name: "invalid_attempt", runs: []contracts.Run{invalidAttempt}, invalid: true},
	} {
		testing.Run(test.name, func(testing *gotesting.T) {
			coordinator := safetyCoordinator(testing, safetyControlPayload(testing), observability.Snapshot{StreamID: "stream", Runs: test.runs})
			snapshot, err := coordinator.Snapshot(context.Background())
			if test.invalid {
				requireUpstreamInvalid(testing, err)
			} else if err != nil || snapshot.UnknownDelivery != test.unknown {
				testing.Fatalf("unexpected snapshot: %+v %v", snapshot, err)
			}
		})
	}
}
