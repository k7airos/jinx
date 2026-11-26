package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/console"
	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	"jinx/internal/observability"
)

func TestRelayDispatcherConsoleLifecycle(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HOME", root)
	t.Setenv("XDG_CONFIG_HOME", root)
	transport := &http.Transport{ResponseHeaderTimeout: 3 * time.Second}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	relay := newRelay(t, root)
	var bridgePosts, configWrites atomic.Int32
	deliveries := make(chan bridgeDelivery, 4)
	bridge := newBridge(t, client, relay.URL, &bridgePosts, deliveries)
	configPath, profile := prepareDispatcher(t, root, relay.URL, bridge.URL)
	backendResult(t, profile, syntheticReply, "synthetic-confirmed")
	journal := filepath.Join(root, "console")
	services := startStack(t, configPath, journal, relay.URL, bridge.URL, &configWrites)
	ctx := context.Background()

	bootstrap := services.facade.Bootstrap(ctx)
	if bootstrap.Cursor == nil || bootstrap.StreamID == nil || bootstrap.LastSeq == nil || *bootstrap.LastSeq != 0 ||
		!bootstrap.EventStream.Enabled || bootstrap.EventStream.Cursor == nil || *bootstrap.EventStream.Cursor != *bootstrap.Cursor ||
		!bootstrap.Capabilities["traceRuns"].Enabled || !bootstrap.Capabilities["resumableRunEvents"].Enabled {
		t.Fatalf("real trace bootstrap unavailable: %+v", bootstrap)
	}
	stream := openStream(t, client, services.console.URL, *bootstrap.Cursor, "")
	postMessage(t, client, relay.URL, "input-confirmed", "@Jinx synthetic integration question")
	confirmed := waitRun(t, services, "input-confirmed")
	assertRun(t, client, services, confirmed, contracts.OutcomeReplied, contracts.DeliveryConfirmed, "", "synthetic-confirmed")
	if bridgePosts.Load() != 1 {
		t.Fatalf("confirmed path submitted %d times", bridgePosts.Load())
	}
	delivery := <-deliveries
	if delivery.GroupID != targetGroup || delivery.Target != "integration target" || delivery.Text != syntheticReply ||
		delivery.RequestID == "" || delivery.ExpiresAt <= confirmed.AcceptedAt.UnixMilli() || delivery.Delivery.Type != "text" {
		t.Fatalf("bridge routing or submission payload lost: %+v", delivery)
	}
	evidence := confirmed.DeliveryEvidence
	if evidence.SubmissionStartedAt == nil || evidence.ConfirmedAt == nil || evidence.NoSubmission ||
		evidence.BridgeRequestID == nil || *evidence.BridgeRequestID != delivery.RequestID ||
		evidence.EchoMessageID == nil || *evidence.EchoMessageID != "echo-"+delivery.RequestID {
		t.Fatalf("correlated webhook echo evidence missing: %+v", evidence)
	}
	if evidence.SubmittedAtSource == nil {
		t.Fatal("submission evidence source missing")
	}
	switch *evidence.SubmittedAtSource {
	case "bridge_ack":
		if evidence.SubmittedAt == nil {
			t.Fatal("ack evidence lacks its observed timestamp")
		}
	case "echo_inferred":
		if evidence.SubmittedAt != nil {
			t.Fatal("echo evidence invented an exact ACK time")
		}
	default:
		t.Fatalf("unexpected submission evidence source: %s", *evidence.SubmittedAtSource)
	}
	arguments, err := os.ReadFile(filepath.Join(profile, "argv.txt"))
	if err != nil {
		t.Fatal(err)
	}
	workspace, err := filepath.EvalSymlinks(filepath.Join(profile, "workspace"))
	if err != nil {
		t.Fatal(err)
	}
	wantArguments := strings.Join([]string{workspace, profile, "-p", profileName, "chat", "--query-file", "-", "-Q", "--format", "stream-json", "--max-turns", "8", "-t", "memory,skills,web", ""}, "\n")
	if string(arguments) != wantArguments {
		t.Fatalf("real backend adapter did not invoke isolated shell stub: %q", arguments)
	}
	prompt, err := os.ReadFile(filepath.Join(profile, "prompt.txt"))
	if err != nil || !strings.Contains(string(prompt), "synthetic integration question") {
		t.Fatalf("webhook content did not reach backend stdin: %s %v", prompt, err)
	}
	var firstPage observability.EventPage
	requestJSON(t, client, http.MethodGet, services.upstream.URL+"/api/trace/events?"+url.Values{"cursor": {*bootstrap.Cursor}}.Encode(), nil, &firstPage)
	if len(firstPage.Events) < 2 || firstPage.HasMore {
		t.Fatalf("missing confirmed trace history: %+v", firstPage)
	}
	prefix := stream.read(t, len(firstPage.Events)/2)
	stream.close()
	expectedEvents := expectedPublicEvents(firstPage.Events)
	if !reflect.DeepEqual(prefix, expectedEvents[:len(prefix)]) {
		t.Fatal("live Facade SSE differs from real Dispatcher trace")
	}
	t.Log("confirmed: webhook -> real relay SSE -> real dispatcher adapter -> shell stub -> fake bridge -> webhook self echo")

	var before dispatcher.Config
	beforeHeader := requestJSON(t, client, http.MethodGet, services.upstream.URL+"/api/config", nil, &before)
	groupEnvelope, err := services.facade.GetGroup(ctx, sourceGroup)
	if err != nil {
		t.Fatal(err)
	}
	group := groupEnvelope.Data.(console.GroupView)
	plan, err := services.facade.PlanChange(ctx, console.PlanInput{
		RequestID: "integration-plan", Base: group.Versions, SchemaVersion: console.SchemaVersion,
		Edits: []console.Edit{{Kind: "field", Scope: console.Scope{Kind: "group", GroupID: sourceGroup}, Field: "dry_run", Value: json.RawMessage(`true`)}},
	})
	if err != nil || len(plan.Changes) != 1 || plan.Token == "" || configWrites.Load() != 0 {
		t.Fatalf("plan did not preview exactly one change without writing: %+v %v", plan, err)
	}
	var preview dispatcher.Config
	previewHeader := requestJSON(t, client, http.MethodGet, services.upstream.URL+"/api/config", nil, &preview)
	if !reflect.DeepEqual(before, preview) || beforeHeader.Get("ETag") != previewHeader.Get("ETag") {
		t.Fatal("plan mutated real dispatcher configuration")
	}
	publish := console.PublishInput{RequestID: "integration-publish", PlanID: plan.ID, Token: plan.Token}
	operation, err := services.facade.PublishChange(ctx, publish)
	if err != nil || operation.Status != "succeeded" || operation.ResultRevision == nil ||
		len(operation.Components) != 1 || operation.Components[0].State != "verified" ||
		operation.Components[0].PersistenceVerified == nil || !*operation.Components[0].PersistenceVerified || configWrites.Load() != 1 {
		t.Fatalf("real config publication/readback failed: %+v %v", operation, err)
	}
	groupEnvelope, err = services.facade.GetGroup(ctx, sourceGroup)
	if err != nil {
		t.Fatal(err)
	}
	group = groupEnvelope.Data.(console.GroupView)
	if !group.EffectiveDryRun || group.Versions.Dispatcher == beforeHeader.Get("ETag") || group.Versions.Dispatcher != *operation.ResultRevision {
		t.Fatalf("Facade did not observe updated config version and effective dry-run: %+v", group)
	}
	var saved dispatcher.Config
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		t.Fatal(err)
	}
	rule := before.Rules.GroupRules[sourceGroup]
	rule.DryRun = true
	before.Rules.GroupRules[sourceGroup] = rule
	if !reflect.DeepEqual(before, saved) {
		t.Fatal("persisted config differs from the single planned dry_run edit")
	}
	backendResult(t, profile, syntheticReply, "synthetic-dry-run")
	postMessage(t, client, relay.URL, "input-dry-run", "@Jinx synthetic dry-run question")
	dryRun := waitRun(t, services, "input-dry-run")
	assertRun(t, client, services, dryRun, contracts.OutcomeSuppressed, contracts.DeliveryBlocked, "dry_run", "synthetic-dry-run")
	if !dryRun.DeliveryEvidence.NoSubmission || dryRun.DeliveryEvidence.SubmissionStartedAt != nil || bridgePosts.Load() != 1 {
		t.Fatalf("published dry-run did not suppress bridge submission: %+v", dryRun)
	}
	t.Log("dry_run: real Facade plan -> one Dispatcher config write -> readback -> persisted config -> suppressed delivery")

	backendResult(t, profile, "NONE", "synthetic-none")
	postMessage(t, client, relay.URL, "input-none", "@Jinx synthetic silence question")
	silent := waitRun(t, services, "input-none")
	assertRun(t, client, services, silent, contracts.OutcomeSilent, contracts.DeliveryNotStarted, "output_none", "synthetic-none")
	if !silent.DeliveryEvidence.NoSubmission || silent.DeliveryEvidence.SubmissionStartedAt != nil || bridgePosts.Load() != 1 {
		t.Fatal("NONE output crossed the submission boundary")
	}
	var allEvents observability.EventPage
	requestJSON(t, client, http.MethodGet, services.upstream.URL+"/api/trace/events?"+url.Values{"cursor": {*bootstrap.Cursor}}.Encode(), nil, &allEvents)
	if allEvents.HasMore || len(allEvents.Events) <= len(firstPage.Events) {
		t.Fatal("later runs did not append trace events")
	}
	resumed := openStream(t, client, services.console.URL, *bootstrap.Cursor, prefix[len(prefix)-1].Cursor)
	suffix := resumed.read(t, len(allEvents.Events)-len(prefix))
	resumed.close()
	expectedEvents = expectedPublicEvents(allEvents.Events)
	if !reflect.DeepEqual(append(prefix, suffix...), expectedEvents) {
		t.Fatal("Last-Event-ID replay lost, duplicated, or changed real trace events")
	}
	for index, event := range allEvents.Events {
		if event.StreamID != *bootstrap.StreamID || event.Seq != uint64(index+1) {
			t.Fatalf("non-contiguous bootstrap event stream: %+v", event)
		}
	}
	filtered, err := services.facade.ListRuns(ctx, url.Values{"deliveries": {"confirmed"}, "groupId": {sourceGroup}})
	if err != nil {
		t.Fatal(err)
	}
	if page := filtered.Data.(console.RunPage); len(page.Items) != 1 || page.Items[0].ID != confirmed.ID {
		t.Fatalf("Facade did not filter actual confirmed run: %+v", page)
	}
	var snapshot observability.Snapshot
	requestJSON(t, client, http.MethodGet, services.upstream.URL+"/api/trace/snapshot", nil, &snapshot)
	if len(snapshot.Runs) != 3 || snapshot.Cursor != allEvents.Cursor {
		t.Fatalf("trace snapshot and event watermark disagree: %+v", snapshot)
	}
	t.Logf("NONE and SSE resume: 3 actual runs, %d contiguous events, mid-run disconnect/replay without duplicates", len(allEvents.Events))

	services.close()
	restarted := startStack(t, configPath, journal, relay.URL, bridge.URL, &configWrites)
	var reopened observability.Snapshot
	requestJSON(t, client, http.MethodGet, restarted.upstream.URL+"/api/trace/snapshot", nil, &reopened)
	if !reflect.DeepEqual(snapshot, reopened) {
		t.Fatal("reopening the real dispatcher trace store changed terminal runs or stream position")
	}
	for _, id := range []string{"input-confirmed", "input-dry-run", "input-none"} {
		lastID := restarted.dispatcher.HealthSnapshot().LastEventID
		postMessage(t, client, relay.URL, id, "@Jinx duplicate after restart")
		await(t, "duplicate relay cursor acknowledgement", func() bool {
			return restarted.dispatcher.HealthSnapshot().LastEventID != lastID
		})
	}
	var afterReplay observability.Snapshot
	requestJSON(t, client, http.MethodGet, restarted.upstream.URL+"/api/trace/snapshot", nil, &afterReplay)
	if !reflect.DeepEqual(snapshot, afterReplay) || bridgePosts.Load() != 1 {
		t.Fatal("replayed inputs created duplicate trace revisions or bridge submissions after restart")
	}
	calls, err := os.ReadFile(filepath.Join(profile, "calls.txt"))
	if err != nil || string(calls) != strings.Repeat("invocation\n", 3) {
		t.Fatalf("expected exactly 3 shell invocations across restart/replay: %q %v", calls, err)
	}
	replayedOperation, err := restarted.facade.PublishChange(ctx, publish)
	if err != nil || !reflect.DeepEqual(operation, replayedOperation) || configWrites.Load() != 1 {
		t.Fatalf("reopened Facade journal repeated config publication: %+v %v writes=%d", replayedOperation, err, configWrites.Load())
	}
	assertRun(t, client, restarted, confirmed, contracts.OutcomeReplied, contracts.DeliveryConfirmed, "", "synthetic-confirmed")
	finalBootstrap := restarted.facade.Bootstrap(ctx)
	if finalBootstrap.Cursor == nil || *finalBootstrap.Cursor != snapshot.Cursor || !finalBootstrap.EventStream.Enabled {
		t.Fatal("bootstrap did not retain a resumable cursor across store restart")
	}
	t.Log("restart: identical trace snapshot; 3 duplicates acknowledged; backend calls=3, bridge submissions=1, config writes=1")
}

func expectedPublicEvents(events []contracts.Event) []contracts.Event {
	expectedEvents := append([]contracts.Event(nil), events...)
	for index := range expectedEvents {
		if expectedEvents[index].Source != nil {
			source := *expectedEvents[index].Source
			source.ContainerLabel = ""
			expectedEvents[index].Source = &source
		}
	}
	return expectedEvents
}

func assertRun(t *testing.T, client *http.Client, services *stack, view console.RunView, outcome contracts.Outcome, delivery contracts.Delivery, reason, externalID string) {
	t.Helper()
	if view.Phase != contracts.PhaseTerminal || view.Outcome != outcome || view.Delivery != delivery ||
		view.Source.ID != sourceGroup || view.Target.ID != targetGroup || view.AdmissionWatermark == nil ||
		view.Completeness != contracts.Partial || view.MissingSnapshotReason == nil || view.Versions.Fingerprint == "" {
		t.Fatalf("run facts missing or incorrect: %+v", view)
	}
	if reason == "" {
		if view.ReasonCode != nil && *view.ReasonCode != "submission_time_echo_inferred" {
			t.Fatalf("unexpected confirmed delivery reason: %q", *view.ReasonCode)
		}
	} else if view.ReasonCode == nil || *view.ReasonCode != reason {
		t.Fatalf("run reason=%v, want %q", view.ReasonCode, reason)
	}
	if len(view.Attempts) != 2 || view.Attempts[0].BackendID != "reply_adapter" || view.Attempts[0].Status != contracts.AttemptSucceeded ||
		view.Attempts[0].StartedAt == nil || view.Attempts[0].EndedAt == nil || view.Attempts[1].BackendID != "Hermes" ||
		view.Attempts[1].Status != contracts.AttemptUnknown || view.Attempts[1].ExternalID == nil || *view.Attempts[1].ExternalID != externalID {
		t.Fatalf("real adapter attempt evidence missing: %+v", view.Attempts)
	}
	var raw struct {
		Run    contracts.Run     `json:"run"`
		Events []contracts.Event `json:"events"`
	}
	requestJSON(t, client, http.MethodGet, services.upstream.URL+"/api/trace/runs/"+view.ID, nil, &raw)
	if err := contracts.ValidateRun(raw.Run); err != nil {
		t.Fatal(err)
	}
	envelope, err := services.facade.GetRun(context.Background(), view.ID)
	if err != nil {
		t.Fatal(err)
	}
	detail := envelope.Data.(console.RunDetail)
	listView := view
	if detail.Run.MissingSnapshotReason == nil && view.ConfigSnapshotID != nil {
		historical, err := services.facade.GetConfigSnapshot(context.Background(), *view.ConfigSnapshotID)
		if err != nil || historical.Data == nil {
			t.Fatalf("detail cleared snapshot reason without a readable historical snapshot: %v", err)
		}
		listView.MissingSnapshotReason = nil
	}
	if !reflect.DeepEqual(detail.Run, listView) || envelope.Revision != raw.Run.Revision ||
		envelope.EntitySeq == nil || *envelope.EntitySeq != raw.Run.EntitySeq || len(detail.Timeline) != len(raw.Events) || len(raw.Events) == 0 {
		t.Fatal("Facade detail/list disagrees with actual trace revision or timeline")
	}
	for index, item := range detail.Timeline {
		if item.RunID != raw.Events[index].EntityID || item.Kind != raw.Events[index].Kind || item.Certainty != "observed" ||
			item.OccurredAt == nil || !item.OccurredAt.Equal(raw.Events[index].OccurredAt) {
			t.Fatalf("Facade timeline did not map trace event: %+v", item)
		}
	}
	data, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{syntheticReply, "synthetic integration question", "synthetic-backend.sh", services.upstream.URL} {
		if strings.Contains(string(data), private) {
			t.Fatalf("console trace detail exposed private fixture content %q", private)
		}
	}
}
