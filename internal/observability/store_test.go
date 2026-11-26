package observability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"jinx/internal/contracts"
)

func testRun(id string) contracts.Run {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	return contracts.Run{ID: id, SourceGroupID: "synthetic-source", TargetGroupID: "synthetic-target", AcceptedAt: now, UpdatedAt: now, LastProgressAt: now, AdmissionWatermark: contracts.Watermark{Token: "synthetic-watermark", RecordedAt: now}, Trigger: "mention", MessageIDs: []string{"synthetic-message-" + id}, Phase: contracts.PhaseAccepted, Outcome: contracts.OutcomePending, Delivery: contracts.DeliveryNotStarted, Completeness: contracts.Complete, Versions: contracts.Versions{Dispatcher: "d1", Fingerprint: "f1"}, MissingSnapshotReason: "not_retained"}
}

func openTestStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "facts.json")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store, path
}

func createTestRun(t *testing.T, store *Store, id string) contracts.Run {
	t.Helper()
	run, err := store.CreateRun(testRun(id))
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func updateTestRun(t *testing.T, store *Store, run contracts.Run, phase contracts.Phase, outcome contracts.Outcome, delivery contracts.Delivery) contracts.Run {
	t.Helper()
	run.Phase, run.Outcome, run.Delivery = phase, outcome, delivery
	run.UpdatedAt = run.UpdatedAt.Add(time.Second)
	run.LastProgressAt = run.UpdatedAt
	run.ReasonCode = "synthetic_stage_evidence"
	next, err := store.UpdateRun(run, run.Revision, "stage_changed")
	if err != nil {
		t.Fatal(err)
	}
	return next
}

func submittedRun(t *testing.T, store *Store, id string) contracts.Run {
	t.Helper()
	run := createTestRun(t, store, id)
	boundary := run.UpdatedAt
	run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &boundary, BridgeRequestID: "synthetic-bridge-" + id}
	run = updateTestRun(t, store, run, contracts.PhaseSubmitting, contracts.OutcomePending, contracts.DeliverySubmitting)
	submission := run.UpdatedAt
	run.DeliveryEvidence.SubmittedAt = &submission
	return updateTestRun(t, store, run, contracts.PhaseConfirming, contracts.OutcomePending, contracts.DeliverySubmitted)
}

func TestPersistenceRecoveryLateConfirmationAndHistory(t *testing.T) {
	store, path := openTestStore(t)
	submitted := submittedRun(t, store, "pending")
	confirmed := submittedRun(t, store, "confirmed")
	confirmation := confirmed.UpdatedAt
	confirmed.DeliveryEvidence.ConfirmedAt, confirmed.DeliveryEvidence.EchoMessageID = &confirmation, "synthetic-echo-confirmed"
	confirmed = updateTestRun(t, store, confirmed, contracts.PhaseTerminal, contracts.OutcomeReplied, contracts.DeliveryConfirmed)
	accepted := createTestRun(t, store, "accepted")
	before, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	recovered, err := reopened.GetRun(submitted.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Phase != contracts.PhaseUnknown || recovered.Delivery != contracts.DeliveryUnknown || recovered.Outcome != contracts.OutcomeUnknown || recovered.ReasonCode != "restart_recovery" || recovered.Revision == submitted.Revision {
		t.Fatalf("bad recovery: %+v", recovered)
	}
	stable, _ := reopened.GetRun(confirmed.ID)
	if stable.Revision != confirmed.Revision || stable.Delivery != contracts.DeliveryConfirmed {
		t.Fatal("confirmed run changed on restart")
	}
	unknownExecution, _ := reopened.GetRun(accepted.ID)
	if unknownExecution.Phase != contracts.PhaseUnknown || unknownExecution.Delivery != contracts.DeliveryNotStarted {
		t.Fatal("execution uncertainty invented a submission")
	}
	page, err := reopened.EventsAfter(before.Cursor, 100)
	if err != nil || len(page.Events) != 2 {
		t.Fatalf("recovery stream not continuous: %+v %v", page, err)
	}
	confirmation = recovered.UpdatedAt
	recovered.DeliveryEvidence.ConfirmedAt, recovered.DeliveryEvidence.EchoMessageID = &confirmation, "synthetic-late-echo"
	recovered = updateTestRun(t, reopened, recovered, contracts.PhaseTerminal, contracts.OutcomeReplied, contracts.DeliveryConfirmed)
	history, err := reopened.RunEvents(recovered.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 5 || history[3].Delivery != contracts.DeliveryUnknown || history[4].Delivery != contracts.DeliveryConfirmed {
		t.Fatalf("lost unknown history: %+v", history)
	}
	if err := reopened.PruneEvents(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	history, _ = reopened.RunEvents(recovered.ID)
	if len(history) != 5 {
		t.Fatal("event retention removed durable run evidence")
	}
	if _, err := reopened.EventsAfter(before.Cursor, 100); !errors.Is(err, ErrResetRequired) {
		t.Fatalf("expired cursor: %v", err)
	}
}

func TestAtomicSnapshotReplayWaitAndReset(t *testing.T) {
	store, _ := openTestStore(t)
	snapshot, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Runs) != 0 || snapshot.CoverageSince.IsZero() {
		t.Fatal("invalid empty snapshot")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan EventPage, 1)
	errorsFound := make(chan error, 1)
	go func() { page, err := store.WaitEvents(ctx, snapshot.Cursor, 1); result <- page; errorsFound <- err }()
	createTestRun(t, store, "first")
	createTestRun(t, store, "second")
	page := <-result
	if err := <-errorsFound; err != nil {
		t.Fatal(err)
	}
	if len(page.Events) != 1 || page.Events[0].Seq != snapshot.LastSeq+1 {
		t.Fatal("snapshot to subscription gap")
	}
	replay, err := store.EventsAfter(snapshot.Cursor, 1)
	if err != nil || !replay.HasMore || replay.Events[0] != page.Events[0] {
		t.Fatal("replay lost duplicate identity or pagination")
	}
	last, err := store.EventsAfter(replay.Cursor, 1)
	if err != nil || last.HasMore || last.Events[0].Seq != 2 {
		t.Fatal("page continuation failed")
	}
	for _, invalid := range []string{"", "garbage", cursor("different-stream", 0), cursor(snapshot.StreamID, 99)} {
		if _, err := store.EventsAfter(invalid, 10); !errors.Is(err, ErrResetRequired) {
			t.Fatalf("invalid cursor accepted: %s: %v", invalid, err)
		}
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := store.WaitEvents(canceled, last.Cursor, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := store.EventsAfter(last.Cursor, 0); err == nil {
		t.Fatal("invalid limit accepted")
	}
}

func TestConflictsConcurrentUpdatesAndDetachedValues(t *testing.T) {
	store, _ := openTestStore(t)
	run := createTestRun(t, store, "concurrent")
	if _, err := store.CreateRun(testRun(run.ID)); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	detached, _ := store.GetRun(run.ID)
	detached.MessageIDs[0] = "changed"
	actual, _ := store.GetRun(run.ID)
	if actual.MessageIDs[0] == "changed" {
		t.Fatal("read aliases store memory")
	}
	var wait sync.WaitGroup
	results := make(chan error, 20)
	for index := 0; index < 20; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			next := run
			next.Phase = contracts.PhaseAggregating
			_, err := store.UpdateRun(next, run.Revision, "aggregating")
			results <- err
		}()
	}
	wait.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("%d successful concurrent updates", successes)
	}
	snapshot, _ := store.Snapshot()
	if snapshot.LastSeq != 2 {
		t.Fatal("rejected writes emitted events")
	}
	if _, err := store.GetRun("missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := store.UpdateRun(run, "", "invalid"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}

func TestLockCloseAndWaiterWakeup(t *testing.T) {
	store, path := openTestStore(t)
	if second, err := Open(path); !errors.Is(err, ErrLocked) {
		if second != nil {
			second.Close()
		}
		t.Fatalf("second writer: %v", err)
	}
	snapshot, _ := store.Snapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := store.WaitEvents(ctx, snapshot.Cursor, 10); result <- err }()
	store.Close()
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := store.Snapshot(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened.Close()
}

func TestFailedWriteDoesNotPublishRunOrEvent(t *testing.T) {
	store, path := openTestStore(t)
	before, _ := store.Snapshot()
	changed := store.changed
	store.wal = &faultWAL{walFile: store.wal, writeError: errors.New("synthetic write failure")}
	if _, err := store.CreateRun(testRun("must-not-exist")); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("failed persistence must be uncertain: %v", err)
	}
	if store.state.LastSeq != before.LastSeq || len(store.state.Runs) != 0 {
		t.Fatal("failed write published in memory")
	}
	select {
	case <-changed:
		t.Fatal("failed write broadcast a change")
	default:
	}
	if _, err := store.GetRun("must-not-exist"); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("file mode: %v %v", info, err)
	}
	leftovers, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".run-store-*"))
	if err != nil || len(leftovers) != 0 {
		t.Fatal("temporary file leak")
	}
}

func TestCorruptStoreIsRejectedWithoutOverwrite(t *testing.T) {
	for _, corruption := range []string{"invalid-json", "trailing-data", "sequence-gap", "invalid-state", "unknown-schema", "history-mismatch"} {
		t.Run(corruption, func(t *testing.T) {
			store, path := openTestStore(t)
			createTestRun(t, store, "persisted")
			if err := store.Checkpoint(); err != nil {
				t.Fatal(err)
			}
			store.Close()
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var state diskState
			if err := json.Unmarshal(data, &state); err != nil {
				t.Fatal(err)
			}
			switch corruption {
			case "invalid-json":
				data = []byte("{")
			case "trailing-data":
				data = append(data, []byte(" {}")...)
			case "sequence-gap":
				state.Events = nil
			case "invalid-state":
				run := state.Runs["persisted"]
				run.Outcome = contracts.OutcomeReplied
				state.Runs[run.ID] = run
			case "unknown-schema":
				state.Schema = 99
			case "history-mismatch":
				state.History["persisted"][0].Revision = "wrong"
			}
			if corruption != "invalid-json" && corruption != "trailing-data" {
				data, _ = json.Marshal(state)
			}
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if opened, err := Open(path); err == nil {
				opened.Close()
				t.Fatal("corruption accepted")
			}
			after, _ := os.ReadFile(path)
			if string(after) != string(data) {
				t.Fatal("corrupt store overwritten")
			}
		})
	}
}

func TestConcurrentCreateAndConsistentSnapshot(t *testing.T) {
	store, _ := openTestStore(t)
	var wait sync.WaitGroup
	errorsFound := make(chan error, 30)
	for index := 0; index < 30; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, err := store.CreateRun(testRun(fmt.Sprintf("run-%02d", index)))
			errorsFound <- err
		}(index)
	}
	for index := 0; index < 10; index++ {
		snapshot, err := store.Snapshot()
		if err != nil || snapshot.LastSeq != uint64(len(snapshot.Runs)) {
			t.Fatalf("non-atomic snapshot: %+v %v", snapshot, err)
		}
	}
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	snapshot, _ := store.Snapshot()
	if snapshot.LastSeq != 30 {
		t.Fatal("lost concurrent creates")
	}
}

func TestRecoveryOfSubmissionBoundaryAndActiveAttempt(t *testing.T) {
	store, path := openTestStore(t)
	run := createTestRun(t, store, "boundary-only")
	boundary := run.UpdatedAt
	run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &boundary, BridgeRequestID: "synthetic-submission"}
	run = updateTestRun(t, store, run, contracts.PhaseSubmitting, contracts.OutcomePending, contracts.DeliverySubmitting)
	executing := createTestRun(t, store, "executing")
	started := executing.UpdatedAt
	executing.Attempts = []contracts.Attempt{{ID: "active-attempt", BackendID: "synthetic-backend", Status: contracts.AttemptRunning, RemoteExecution: contracts.RemoteMayBeRunning, StartedAt: &started, MissingExternalIDReason: "not_exposed", Versions: executing.Versions}}
	updateTestRun(t, store, executing, contracts.PhaseExecuting, contracts.OutcomePending, contracts.DeliveryNotStarted)
	store.Close()
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	unknown, _ := reopened.GetRun(run.ID)
	if unknown.Delivery != contracts.DeliveryUnknown || unknown.DeliveryEvidence.SubmittedAt != nil || unknown.DeliveryEvidence.BridgeRequestID != "synthetic-submission" {
		t.Fatal("recovery invented bridge acceptance or lost boundary")
	}
	unknownExecution, _ := reopened.GetRun(executing.ID)
	if unknownExecution.Attempts[0].Status != contracts.AttemptUnknown || unknownExecution.Attempts[0].RemoteExecution != contracts.RemoteUnknown || unknownExecution.Delivery != contracts.DeliveryNotStarted {
		t.Fatal("active remote execution not preserved as uncertain")
	}
	before, _ := reopened.Snapshot()
	attemptedResend := unknown
	attemptedResend.Phase, attemptedResend.Outcome, attemptedResend.Delivery = contracts.PhaseSubmitting, contracts.OutcomePending, contracts.DeliverySubmitting
	if _, err := reopened.UpdateRun(attemptedResend, unknown.Revision, "unsafe_resubmit"); err == nil {
		t.Fatal("recovered submission could be resent")
	}
	after, _ := reopened.Snapshot()
	if after.LastSeq != before.LastSeq {
		t.Fatal("invalid transition emitted event")
	}
}

func TestPruningSurvivesRestartAndDoesNotResetCurrentCursor(t *testing.T) {
	store, path := openTestStore(t)
	submittedRun(t, store, "retained")
	snapshot, _ := store.Snapshot()
	if err := store.PruneEvents(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	page, err := store.EventsAfter(snapshot.Cursor, 10)
	if err != nil || len(page.Events) != 0 {
		t.Fatal("cursor at retention boundary must remain valid")
	}
	store.Close()
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reopened.Close() })
	page, err = reopened.EventsAfter(snapshot.Cursor, 10)
	if err != nil || len(page.Events) != 1 || page.Events[0].Kind != "run_recovered" {
		t.Fatalf("pruned journal recovery: %+v %v", page, err)
	}
	history, err := reopened.RunEvents("retained")
	if err != nil || len(history) != 4 {
		t.Fatal("pruning erased durable submission history")
	}
}

func TestCancellationAndSubmissionCompeteOnSameRevision(t *testing.T) {
	store, _ := openTestStore(t)
	run := createTestRun(t, store, "cancel-submit-race")
	run = updateTestRun(t, store, run, contracts.PhaseChecking, contracts.OutcomePending, contracts.DeliveryNotStarted)
	submitting := run
	boundary := run.UpdatedAt
	submitting.Phase, submitting.Delivery = contracts.PhaseSubmitting, contracts.DeliverySubmitting
	submitting.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &boundary, BridgeRequestID: "synthetic-boundary"}
	canceled := run
	canceled.Phase, canceled.Outcome, canceled.Delivery = contracts.PhaseTerminal, contracts.OutcomeCanceled, contracts.DeliveryBlocked
	canceled.ReasonCode = "user_canceled"
	canceled.DeliveryEvidence.NoSubmission = true
	results := make(chan error, 2)
	start := make(chan struct{})
	for _, candidate := range []contracts.Run{submitting, canceled} {
		go func(candidate contracts.Run) {
			<-start
			_, err := store.UpdateRun(candidate, run.Revision, "boundary_decided")
			results <- err
		}(candidate)
	}
	close(start)
	first, second := <-results, <-results
	if !((first == nil && errors.Is(second, ErrConflict)) || (second == nil && errors.Is(first, ErrConflict))) {
		t.Fatalf("boundary race: %v, %v", first, second)
	}
	actual, _ := store.GetRun(run.ID)
	if actual.Delivery != contracts.DeliverySubmitting && actual.Delivery != contracts.DeliveryBlocked {
		t.Fatal("neither boundary won")
	}
	history, _ := store.RunEvents(run.ID)
	if len(history) != 3 {
		t.Fatal("losing boundary emitted event")
	}
}
