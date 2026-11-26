package observability

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"jinx/internal/contracts"
)

func TestMetadataWALCompatibilityCASAndCheckpoint(t *testing.T) {
	store, path := openTestStore(t)
	legacy := createTestRun(t, store, "legacy")
	input := testRun("metadata")
	receivedAt := input.AcceptedAt.Add(-time.Second)
	input.Inputs = []contracts.InboundReference{{EnvelopeID: "envelope-1", DedupKey: "dedup-1", Source: contracts.SourceRef{Platform: contracts.PlatformWeChat, AccountID: "wechat-relay", ContainerID: input.SourceGroupID, ContentID: input.MessageIDs[0]}, ReceivedAt: &receivedAt}}
	run, err := store.CreateRun(input)
	if err != nil {
		t.Fatal(err)
	}
	input.Inputs[0].Source.ContainerID = "caller-mutation"
	input.Inputs[0].ReceivedAt = nil
	run = updateTestRun(t, store, run, contracts.PhaseChecking, contracts.OutcomePending, contracts.DeliveryNotStarted)
	checking := clone(run)
	boundary := run.UpdatedAt
	run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &boundary, BridgeRequestID: "action-1"}
	run.Action = &contracts.ActionRef{ActionID: "action-1", Kind: contracts.ActionReplyText, Platform: contracts.PlatformWeChat, IdempotencyKey: "key-1", AdmissionID: "admission-1", AdmittedAt: &boundary}
	run = updateTestRun(t, store, run, contracts.PhaseSubmitting, contracts.OutcomePending, contracts.DeliverySubmitting)
	if _, err := store.UpdateRun(checking, checking.Revision, "stale"); !errors.Is(err, ErrConflict) {
		t.Fatal("stale CAS accepted", err)
	}
	for name, change := range map[string]func(*contracts.Run){
		"erase_inputs":           func(value *contracts.Run) { value.Inputs = nil },
		"replace_dedup":          func(value *contracts.Run) { value.Inputs[0].DedupKey = "other-key" },
		"erase_received":         func(value *contracts.Run) { value.Inputs[0].ReceivedAt = nil },
		"replace_source":         func(value *contracts.Run) { value.Inputs[0].Source.ContainerLabel = "other-label" },
		"replace_missing_reason": func(value *contracts.Run) { value.InputsMissingReason = "relay_input_metadata_unavailable" },
		"erase_action":           func(value *contracts.Run) { value.Action = nil },
		"replace_action":         func(value *contracts.Run) { value.Action.IdempotencyKey = "other-key" },
		"replace_admission":      func(value *contracts.Run) { value.Action.AdmissionID = "other-admission" },
	} {
		t.Run(name, func(t *testing.T) {
			value := clone(run)
			change(&value)
			if _, err := store.UpdateRun(value, run.Revision, "rewrite"); err == nil {
				t.Fatal("admitted metadata rewrite accepted")
			}
		})
	}
	late := clone(legacy)
	late.Inputs = clone(run.Inputs)
	late.Inputs[0].Source.ContentID = legacy.MessageIDs[0]
	if _, err := store.UpdateRun(late, late.Revision, "backfill"); err == nil {
		t.Fatal("legacy run accepted invented input history")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal("mixed legacy/new WAL did not reopen", err)
	}
	defer reopened.Close()
	stored, err := reopened.GetRun(run.ID)
	if err != nil || !reflect.DeepEqual(stored.Inputs, run.Inputs) || !reflect.DeepEqual(stored.Action, run.Action) || stored.Delivery != contracts.DeliveryUnknown {
		t.Fatal("restart lost metadata", stored, err)
	}
	events, err := reopened.RunEvents(run.ID)
	if err != nil || len(events) != 4 || events[0].Action != nil || events[1].Action != nil || events[2].Action == nil || events[3].Action == nil || events[0].EnvelopeID != "envelope-1" || events[0].Source.ContentID != input.MessageIDs[0] {
		t.Fatal("historical metadata was not retained", events, err)
	}
	events[2].Action.AdmissionID = "returned-mutation"
	events[0].Source.ContentID = "returned-mutation"
	again, _ := reopened.RunEvents(run.ID)
	if again[2].Action.AdmissionID != run.Action.AdmissionID || again[0].Source.ContentID != input.MessageIDs[0] {
		t.Fatal("events alias persistent metadata")
	}
	oldWAL, err := os.ReadFile(path + ".wal")
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	var state diskState
	if err != nil || decodeJSON(data, &state) != nil || validateDisk(state) != nil {
		t.Fatal("new metadata rejected by checkpoint validation", err)
	}
	last := len(state.History[run.ID]) - 1
	state.History[run.ID][last].Action.AdmissionID = "corrupt-admission"
	if validateDisk(state) == nil {
		t.Fatal("corrupted action accepted by disk validation")
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".wal", oldWAL, 0600); err != nil {
		t.Fatal(err)
	}
	checkpointAndOldWAL, err := Open(path)
	if err != nil {
		t.Fatal("checkpoint plus pre-rotation WAL rejected metadata", err)
	}
	defer checkpointAndOldWAL.Close()
	replayed, err := checkpointAndOldWAL.GetRun(run.ID)
	if err != nil || !reflect.DeepEqual(replayed.Inputs, run.Inputs) || !reflect.DeepEqual(replayed.Action, run.Action) {
		t.Fatal("checkpoint replay lost metadata", replayed, err)
	}
}

func TestActionCannotBeBackfilledAfterSubmission(t *testing.T) {
	store, _ := openTestStore(t)
	run := submittedRun(t, store, "old-action")
	run.Action = &contracts.ActionRef{ActionID: run.DeliveryEvidence.BridgeRequestID, Kind: contracts.ActionReplyText, Platform: contracts.PlatformWeChat, IdempotencyKey: "key-1", AdmissionID: "invented-admission", AdmittedAt: run.DeliveryEvidence.SubmissionStartedAt}
	if _, err := store.UpdateRun(run, run.Revision, "late_action"); err == nil {
		t.Fatal("legacy submission accepted fabricated admission")
	}
}
