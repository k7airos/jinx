package console

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"jinx/internal/contracts"
)

func TestAllowlistedSnapshotRollbackIsNewPublish(t *testing.T) {
	facade, stub, options := setup(t)
	stub.root["rules"].(map[string]any)["group_rules"].(map[string]any)["2@chatroom"].(map[string]any)["unknown_private"] = "/private/SECRET"
	plan := planEdit(t, facade, fieldEdit("1@chatroom", "target", `"after"`))
	operation, err := facade.PublishChange(context.Background(), PublishInput{RequestID: "first", PlanID: plan.ID, Token: plan.Token})
	if err != nil || operation.Status != "succeeded" {
		t.Fatalf("%+v %v", operation, err)
	}
	detail, err := facade.GetChange(plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	record := detail.Data.(ChangeRecord)
	if record.BeforeSnapshot == nil || record.TargetSnapshot == nil || record.ReadbackSnapshot == nil || record.BeforeSnapshot.Dispatcher["rules"] == nil {
		t.Fatalf("missing complete allowed snapshots: %+v", record)
	}
	data, err := os.ReadFile(filepath.Join(options.StateDir, "console-changes.json"))
	if err != nil || strings.Contains(string(data), "SECRET") || strings.Contains(string(data), "bin_path") || strings.Contains(string(data), "unknown_private") {
		t.Fatalf("private journal field: %s %v", data, err)
	}
	second := planEdit(t, facade, fieldEdit("1@chatroom", "batch_size", `9`))
	if _, err := facade.PublishChange(context.Background(), PublishInput{RequestID: "second", PlanID: second.ID, Token: second.Token}); err != nil {
		t.Fatal(err)
	}
	config, _ := facade.backend.config(context.Background())
	input := RollbackInput{RequestID: "rollback", ChangeID: plan.ID, Base: versions(config)}
	rollback, err := facade.PlanRollback(context.Background(), input)
	if err != nil || rollback.ID == plan.ID || rollback.Base.Dispatcher != config.etag {
		t.Fatalf("%+v %v", rollback, err)
	}
	repeated, err := facade.PlanRollback(context.Background(), input)
	if err != nil || repeated.ID != rollback.ID {
		t.Fatal("rollback plan is not idempotent")
	}
	if err := facade.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	operation, err = reopened.PublishChange(context.Background(), PublishInput{RequestID: "rollback-publish", PlanID: rollback.ID, Token: rollback.Token})
	if err != nil || operation.Status != "succeeded" || stub.writes != 3 {
		t.Fatalf("%+v %v writes=%d", operation, err, stub.writes)
	}
	group := groups(snapshot{rules: stub.root["rules"].(map[string]any)})["1@chatroom"].(map[string]any)
	if group["target"] != "one" || group["batch_size"] != nil {
		t.Fatalf("rollback did not restore full pre-change policy: %+v", group)
	}
	if stub.patch["group_rules"].(map[string]any)["2@chatroom"].(map[string]any)["unknown_private"] != "/private/SECRET" {
		t.Fatal("rollback discarded protected current fields")
	}
	read, _ := reopened.GetChange(rollback.ID)
	if read.Data.(ChangeRecord).RollbackOf == nil || *read.Data.(ChangeRecord).RollbackOf != plan.ID {
		t.Fatal("missing rollback provenance")
	}
}

func TestChangeFiltersPagingAndUnknown(t *testing.T) {
	facade, _, _ := setup(t)
	for _, value := range []string{`"changed-1"`, `"changed-2"`, `"changed-3"`} {
		plan := planEdit(t, facade, fieldEdit("1@chatroom", "target", value))
		if _, err := facade.PublishChange(context.Background(), PublishInput{RequestID: newID(), PlanID: plan.ID, Token: plan.Token}); err != nil {
			t.Fatal(err)
		}
	}
	query := url.Values{"limit": {"1"}, "status": {"succeeded"}, "component": {"dispatcher"}}
	first, err := facade.QueryChanges(query)
	if err != nil {
		t.Fatal(err)
	}
	page := first.Data.(ChangePage)
	if len(page.Items) != 1 || page.NextCursor == nil {
		t.Fatal("missing paging")
	}
	query.Set("cursor", *page.NextCursor)
	second, err := facade.QueryChanges(query)
	if err != nil || second.Data.(ChangePage).Items[0].ID == page.Items[0].ID {
		t.Fatal("unstable paging")
	}
	query.Set("status", "failed")
	if _, err := facade.QueryChanges(query); err == nil || err.(*Problem).Code != "reset_required" {
		t.Fatal("cursor accepted different filter")
	}
	for _, query := range []url.Values{{"secret": {"x"}}, {"status": {"invented"}}, {"limit": {"0"}}, {"from": {"today"}}, {"origin": {"console", "external"}}} {
		if _, err := facade.QueryChanges(query); err == nil {
			t.Fatalf("invalid query accepted: %+v", query)
		}
	}
	for path, status := range map[string]int{"changes/missing": 404, "changes?limit=1": 200, "config/drift/dispatcher": 200, "config/drift/private": 400} {
		if response := consoleRequest(facade, path); response.Code != status {
			t.Fatalf("%s %d %s", path, response.Code, response.Body.String())
		}
	}
}

func TestHistoricalSnapshotProxyAndOldMissing(t *testing.T) {
	for _, status := range []int{200, 404, 410, 501, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/api/trace/config-snapshots/snapshot-1" || request.Method != "GET" {
					t.Errorf("unexpected read: %s %s", request.Method, request.URL.Path)
				}
				writer.WriteHeader(status)
				if status == 200 {
					_, _ = writer.Write([]byte(`{"id":"snapshot-1","policy":"p1","recordedAt":"2026-11-19T00:00:00Z","sourceGroupId":"1@chatroom","rules":{"allowedTypes":[],"enabled":true,"decisionContext":"historical context","globalKeywordBlocklist":[],"bin_path":"/private/SECRET","secret":"SECRET"},"secret":"SECRET"}`))
				}
			}))
			defer server.Close()
			facade, err := New(Options{DispatcherURL: server.URL, StateDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer facade.Close()
			response := consoleRequest(facade, "config-snapshots/snapshot-1")
			if status == 503 {
				if response.Code != 503 {
					t.Fatal(response.Code)
				}
				return
			}
			if response.Code != 200 || strings.Contains(response.Body.String(), "SECRET") {
				t.Fatalf("%d %s", response.Code, response.Body.String())
			}
			var result struct {
				Value Envelope `json:"value"`
			}
			_ = json.Unmarshal(response.Body.Bytes(), &result)
			if status != 200 && (result.Value.Data != nil || result.Value.MissingReason == nil) {
				t.Fatal("old endpoint fabricated snapshot")
			}
			if status == 200 {
				view := result.Value.Data.(map[string]any)
				versions := view["versions"].(map[string]any)
				field := view["fields"].(map[string]any)["allowed_types"].(map[string]any)
				if versions["policy"] != "p1" || versions["dispatcher"] != "" || versions["fingerprint"] != "" || field["configured"] != nil || field["origin"] != "historical_resolved" || field["effective"] == nil {
					t.Fatalf("historical facts invented: %+v", view)
				}
			}
		})
	}
}

func TestStageCountsAndPersistentAttention(t *testing.T) {
	run := testRun("unknown-delivery", 1)
	run.Delivery, run.Phase = contracts.DeliveryUnknown, contracts.PhaseUnknown
	run.Outcome = contracts.OutcomeUnknown
	run.ReasonCode = "submission_unconfirmed"
	started := run.AcceptedAt.Add(time.Second)
	run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &started, BridgeRequestID: "synthetic-bridge"}
	facade, stub := traceSetup(t, run)
	view := facade.Overview(context.Background()).Data.(map[string]any)
	counts := view["currentWork"].(StageCounts)
	if counts.Executing == nil || *counts.Executing != 0 || *counts.UnknownDelivery != 1 || *counts.RemoteUnsettled != 1 {
		t.Fatalf("wrong counts %+v", counts)
	}
	items := view["attention"].([]AttentionItem)
	if len(items) != 1 || items[0].Acknowledged {
		t.Fatal(items)
	}
	ack, err := facade.AcknowledgeAttention(context.Background(), AcknowledgeInput{RequestID: "ack-1", AttentionID: items[0].ID, ExpectedRevision: items[0].Revision})
	if err != nil || !ack.Data.(AttentionItem).Acknowledged {
		t.Fatalf("%+v %v", ack, err)
	}
	options := Options{DispatcherURL: facade.backend.bases["dispatcher"], StateDir: facade.stateDir, Timeout: time.Second}
	if err := facade.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !reopened.attention(stub.snapshot, nil)[0].Acknowledged {
		t.Fatal("acknowledgement lost on restart")
	}
	if stageCounts(stub.snapshot, failure("unsupported", "missing")).Executing != nil {
		t.Fatal("missing counts became zero")
	}
}

type syntheticConfigurationReader struct{}

func (syntheticConfigurationReader) ReadConfiguration(context.Context, string) (ConfigurationObservation, error) {
	memory, disk := fixture(), fixture()
	disk["hermes"].(map[string]any)["bin_path"] = "/private/DRIFT"
	return ConfigurationObservation{Memory: memory, Disk: disk, MemoryVersion: "m1", DiskVersion: "d1"}, nil
}

func TestAuthorizedDriftAndMissingReader(t *testing.T) {
	facade, _, _ := setup(t)
	view, _ := facade.GetConfigurationDrift(context.Background(), "dispatcher")
	if view.Data.(ConfigurationDrift).Available || view.Data.(ConfigurationDrift).Drift != nil {
		t.Fatal("invented disk observation")
	}
	facade.configurationReader = syntheticConfigurationReader{}
	view, err := facade.GetConfigurationDrift(context.Background(), "dispatcher")
	if err != nil || view.Data.(ConfigurationDrift).Drift == nil || !*view.Data.(ConfigurationDrift).Drift {
		t.Fatalf("%+v %v", view, err)
	}
	data, _ := json.Marshal(view)
	if strings.Contains(string(data), "SECRET") || strings.Contains(string(data), "DRIFT") {
		t.Fatal("drift leaked private configuration")
	}
}

func TestCompatibilityAndInheritedPreview(t *testing.T) {
	facade, _, _ := setup(t)
	global, err := facade.GetGlobalRules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	compat := global.Data.(map[string]any)["compatibility"].(CompatibilityView)
	if compat.Editable || !strings.Contains(compat.Semantics["groupProfiles"], "prompt background") {
		t.Fatalf("ambiguous compatibility: %+v", compat)
	}
	plan := planEdit(t, facade, Edit{Kind: "field", Scope: Scope{Kind: "global"}, Field: "batch_size", Value: json.RawMessage(`9`)})
	seen := map[string]bool{}
	for _, change := range plan.Changes {
		if strings.HasPrefix(change.Path, "group_rules.") {
			if change.EffectiveBefore != float64(8) || change.EffectiveAfter != float64(9) || change.Before != nil || change.After != nil {
				t.Fatalf("incorrect inherited diff: %+v", change)
			}
			seen[change.Path] = true
		}
	}
	if !seen["group_rules.1@chatroom.batch_size"] || !seen["group_rules.2@chatroom.batch_size"] {
		t.Fatal("missing inherited groups")
	}
	group, _ := facade.GetGroup(context.Background(), "1@chatroom")
	view := group.Data.(GroupView)
	if view.Group.Label != "" || view.Group.Visibility != "not_observed" || view.UITargetLabel != "one" || view.LabelSource != "config_target_ui_only" {
		t.Fatal("configured target falsely became observed group name")
	}
	if view.Fields["dry_run"].Effective != true || view.Fields["dry_run"].Origin != "global" {
		t.Fatal("group effective dry_run ignored global policy")
	}
}
