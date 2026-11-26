package console_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"jinx/internal/console"
	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	"jinx/internal/observability"
)

func TestArchitectureAgainstRealDispatcherTraceHandler(t *testing.T) {
	root := t.TempDir()
	store, err := observability.Open(filepath.Join(root, "logs", "trace.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	initial, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Second)
	run, err := store.CreateRun(contracts.Run{
		ID: "architecture-run", SourceGroupID: "source", TargetGroupID: "target",
		AcceptedAt: now, UpdatedAt: now, LastProgressAt: now,
		AdmissionWatermark: contracts.Watermark{Token: "watermark", RecordedAt: now}, Trigger: "mention",
		MessageIDs: []string{"message-1"}, Phase: contracts.PhaseAccepted, Outcome: contracts.OutcomePending,
		Delivery: contracts.DeliveryNotStarted, Completeness: contracts.Complete,
		Versions: contracts.Versions{Dispatcher: "config-1", Fingerprint: "fingerprint-1"}, MissingSnapshotReason: "not_retained",
		Inputs: []contracts.InboundReference{{EnvelopeID: "envelope-1", DedupKey: "dedup-1", ReceivedAt: &now,
			Source: contracts.SourceRef{Platform: contracts.PlatformWeChat, AccountID: "wechat-relay", ContainerID: "source", ContainerLabel: "/private/SECRET", ContentID: "message-1"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	run.UpdatedAt, run.LastProgressAt = now.Add(time.Millisecond), now.Add(time.Millisecond)
	run.Phase, run.ReasonCode = contracts.PhaseChecking, "stages_skipped"
	run, err = store.UpdateRun(run, run.Revision, "checking")
	if err != nil {
		t.Fatal(err)
	}
	run.UpdatedAt, run.LastProgressAt = now.Add(2*time.Millisecond), now.Add(2*time.Millisecond)
	run.Phase, run.Outcome, run.Delivery, run.ReasonCode = contracts.PhaseSubmitting, contracts.OutcomePending, contracts.DeliverySubmitting, "stages_skipped"
	started := run.UpdatedAt
	run.Action = &contracts.ActionRef{ActionID: "action-1", Kind: contracts.ActionReplyText, Platform: contracts.PlatformWeChat, IdempotencyKey: "action-dedup-1", AdmissionID: run.ID, AdmittedAt: &started}
	run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &started, BridgeRequestID: "action-1"}
	run, err = store.UpdateRun(run, run.Revision, "submission_started")
	if err != nil {
		t.Fatal(err)
	}
	run.UpdatedAt, run.LastProgressAt = now.Add(3*time.Millisecond), now.Add(3*time.Millisecond)
	run.Phase, run.Outcome, run.Delivery, run.ReasonCode = contracts.PhaseTerminal, contracts.OutcomeReplied, contracts.DeliveryConfirmed, ""
	confirmed := run.UpdatedAt
	run.DeliveryEvidence.SubmittedAtSource, run.DeliveryEvidence.ConfirmedAt, run.DeliveryEvidence.EchoMessageID = "echo_inferred", &confirmed, "echo-1"
	if _, err := store.UpdateRun(run, run.Revision, "echo_confirmed"); err != nil {
		t.Fatal(err)
	}
	persisted, err := store.EventsAfter(initial.Cursor, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	config := []byte(`{"server":{"listen_addr":"127.0.0.1:0"},"relay":{"stream_url":"http://127.0.0.1:1/events"},"bridge":{"url":"http://127.0.0.1:1","dry_run":true,"timeout_sec":1},"hermes":{"bin_path":"/synthetic/not-executed","profile":"architecture","profiles_dir":"` + filepath.ToSlash(root) + `","timeout_sec":1},"rules":{"batch_size":8,"idle_timeout_sec":5,"max_batch_wait_sec":15,"group_rules":{}}}`)
	path := filepath.Join(root, "dispatcher.json")
	if err := os.WriteFile(path, config, 0600); err != nil {
		t.Fatal(err)
	}
	dispatch, err := dispatcher.NewDispatcher(path)
	if err != nil {
		t.Fatal(err)
	}
	defer dispatch.Close()
	upstream := httptest.NewServer(dispatch.Handler())
	defer upstream.Close()
	facade, err := console.New(console.Options{DispatcherURL: upstream.URL, StateDir: filepath.Join(root, "console")})
	if err != nil {
		t.Fatal(err)
	}
	defer facade.Close()
	server := httptest.NewServer(facade.Handler())
	defer server.Close()
	response, err := server.Client().Get(server.URL + "/console/v1/architecture")
	if err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		OK    bool                         `json:"ok"`
		Value console.ArchitectureSnapshot `json:"value"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&snapshot)
	response.Body.Close()
	if decodeErr != nil || response.StatusCode != 200 || !snapshot.OK || !snapshot.Value.EventStreams[1].Supported || snapshot.Value.EventStreams[1].LastSeq == nil || *snapshot.Value.EventStreams[1].LastSeq != 4 {
		t.Fatalf("real snapshot unavailable: %+v %v", snapshot, decodeErr)
	}
	response, err = server.Client().Get(server.URL + "/console/v1/runs/" + run.ID)
	if err != nil {
		t.Fatal(err)
	}
	var detail struct {
		Value struct {
			Data console.RunDetail `json:"data"`
		} `json:"value"`
	}
	decodeErr = json.NewDecoder(response.Body).Decode(&detail)
	response.Body.Close()
	view := detail.Value.Data.Run
	if decodeErr != nil || response.StatusCode != 200 || len(view.Inputs) != 1 || view.Inputs[0].EnvelopeID != "envelope-1" || view.Inputs[0].Source.ContainerLabel != "" || view.Action == nil || view.Action.ActionID != "action-1" || view.Receipt == nil || view.Receipt.Status != contracts.ActionConfirmed || view.Receipt.Receipt == nil || view.Receipt.Receipt.Admission.AdmissionID != run.ID || view.Receipt.Receipt.SubmittedAt != nil || view.Receipt.Receipt.SubmittedAtSource != "echo_inferred" {
		t.Fatalf("persisted run references missing: %+v %v", detail, decodeErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/console/v1/architecture/events?domain=processing&cursor="+initial.Cursor, nil)
	response, err = server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	scanner := bufio.NewScanner(response.Body)
	count := 0
	var cursor string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "id: ") {
			cursor = strings.TrimPrefix(line, "id: ")
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event contracts.ArchitectureEvent
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		if err := contracts.ValidateArchitectureEvent(event); err != nil {
			t.Fatal(err)
		}
		if event.RunID != run.ID || event.Domain != contracts.DomainProcessing {
			t.Fatalf("invented identity: %+v", event)
		}
		if event.Source == nil || event.Source.ContainerID != "source" || event.Source.ContainerLabel != "" || event.EnvelopeID != "envelope-1" || cursor == "" {
			t.Fatalf("persisted event references missing: %+v", event)
		}
		count++
		if count > len(persisted.Events) || cursor != persisted.Events[count-1].Cursor || !event.OccurredAt.Equal(persisted.Events[count-1].OccurredAt) {
			t.Fatalf("persisted cursor or event time changed: cursor=%s event=%+v", cursor, event)
		}
		if count < 3 && event.Action != nil || count >= 3 && (event.Action == nil || event.Action.ActionID != "action-1") {
			t.Fatalf("historical action fabricated or current action missing: %+v", event)
		}
		if count == 1 && event.Phase != contracts.PhaseAccepted || count == 2 && event.Phase != contracts.PhaseChecking || count == 3 && event.Phase != contracts.PhaseSubmitting || count == 4 && (event.Phase != contracts.PhaseTerminal || !event.OccurredAt.Equal(run.UpdatedAt)) {
			t.Fatalf("real trace order/time changed: %+v", event)
		}
		if count == 4 {
			cancel()
			break
		}
	}
	if count != 4 {
		t.Fatalf("missing real trace events: %d %v", count, scanner.Err())
	}
}
