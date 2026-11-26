package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"jinx/internal/contracts"
)

type faultWAL struct {
	walFile
	writeError error
	short      bool
	syncError  error
}

type countedWAL struct {
	walFile
	writes int
	syncs  int
}

func (file *countedWAL) Write(data []byte) (int, error) {
	file.writes++
	return file.walFile.Write(data)
}

func (file *countedWAL) Sync() error {
	file.syncs++
	return nil
}

func (file *faultWAL) Write(data []byte) (int, error) {
	if file.writeError != nil {
		return 0, file.writeError
	}
	if file.short {
		return file.walFile.Write(data[:len(data)/2])
	}
	return file.walFile.Write(data)
}

func (file *faultWAL) Sync() error {
	if file.syncError != nil {
		return file.syncError
	}
	return file.walFile.Sync()
}

func TestWALUnknownFailuresFailClosedWithoutBroadcast(t *testing.T) {
	for _, failure := range []string{"write", "short_write", "fsync"} {
		t.Run(failure, func(t *testing.T) {
			store, path := openTestStore(t)
			before, _ := store.Snapshot()
			changed := store.changed
			fault := &faultWAL{walFile: store.wal}
			switch failure {
			case "write":
				fault.writeError = errors.New("synthetic write failure")
			case "short_write":
				fault.short = true
			case "fsync":
				fault.syncError = errors.New("synthetic fsync failure")
			}
			store.wal = fault
			if _, err := store.CreateRun(testRun("uncertain")); !errors.Is(err, ErrPersistenceUnknown) {
				t.Fatalf("failure must be uncertain: %v", err)
			}
			if store.state.LastSeq != before.LastSeq || len(store.state.Runs) != 0 || len(store.state.Events) != 0 {
				t.Fatal("failed write published data")
			}
			select {
			case <-changed:
				t.Fatal("failed write broadcast")
			default:
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			checks := []func() error{
				func() error { _, err := store.CreateRun(testRun("later")); return err },
				func() error { _, err := store.UpdateRun(testRun("uncertain"), "revision", "kind"); return err },
				func() error { _, err := store.GetRun("uncertain"); return err },
				func() error { _, err := store.RunEvents("uncertain"); return err },
				func() error { _, err := store.Snapshot(); return err },
				func() error { _, err := store.EventsAfter(before.Cursor, 1); return err },
				func() error { _, err := store.WaitEvents(ctx, before.Cursor, 1); return err },
				func() error { return store.PruneEvents(time.Now()) },
				store.Checkpoint,
			}
			for index, check := range checks {
				if err := check(); !errors.Is(err, ErrPersistenceUnknown) {
					t.Fatalf("operation %d did not fail closed: %v", index, err)
				}
			}
			store.Close()
			reopened, err := Open(path)
			if failure == "short_write" {
				if !errors.Is(err, ErrWALCorrupt) {
					if reopened != nil {
						reopened.Close()
					}
					t.Fatalf("torn WAL must require intervention: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			if failure == "fsync" {
				run, err := reopened.GetRun("uncertain")
				if err != nil || run.Phase != contracts.PhaseUnknown {
					t.Fatalf("complete uncertain transaction must be recovered: %+v %v", run, err)
				}
			} else if _, err := reopened.GetRun("uncertain"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("zero-byte failure should leave no run: %v", err)
			}
		})
	}
}

func TestWALCorruptionRejectedWithoutChangingEitherFile(t *testing.T) {
	for _, corruption := range []string{"header", "checksum", "commit", "tail", "trailing", "empty", "foreign", "gap", "mismatch", "missing"} {
		t.Run(corruption, func(t *testing.T) {
			store, path := openTestStore(t)
			createTestRun(t, store, "durable")
			store.Close()
			snapshot, _ := os.ReadFile(path)
			data, err := os.ReadFile(path + ".wal")
			if err != nil {
				t.Fatal(err)
			}
			switch corruption {
			case "header":
				data[0] ^= 1
			case "checksum":
				data[len(data)-frameFooter-3] ^= 1
			case "commit":
				data[len(data)-frameFooter] ^= 1
			case "tail":
				data = data[:len(data)-1]
			case "trailing":
				data = append(data, 'x')
			case "empty":
				data = nil
			case "foreign":
				data, err = encodeFrame(walHeader{Schema: 1, StreamID: "foreign"})
			case "gap", "mismatch":
				file, openErr := os.Open(path + ".wal")
				if openErr != nil {
					t.Fatal(openErr)
				}
				var header walHeader
				offset, readErr := readFrame(file, 0, int64(len(data)), &header)
				if readErr != nil {
					t.Fatal(readErr)
				}
				var record walRecord
				_, readErr = readFrame(file, offset, int64(len(data)), &record)
				file.Close()
				if readErr != nil {
					t.Fatal(readErr)
				}
				if corruption == "gap" {
					record.Seq++
				} else {
					record.Event.Revision = "different"
				}
				frame, encodeErr := encodeFrame(record)
				if encodeErr != nil {
					t.Fatal(encodeErr)
				}
				data = append(data[:offset], frame...)
			}
			if err != nil {
				t.Fatal(err)
			}
			if corruption == "missing" {
				if err := os.Remove(path + ".wal"); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path+".wal", data, 0600); err != nil {
				t.Fatal(err)
			}
			opened, err := Open(path)
			if err == nil {
				opened.Close()
				t.Fatal("corrupt WAL accepted")
			}
			if corruption != "missing" && !errors.Is(err, ErrWALCorrupt) {
				t.Fatalf("wrong corruption error: %v", err)
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(snapshot, after) {
				t.Fatal("snapshot changed after corruption")
			}
			after, readErr := os.ReadFile(path + ".wal")
			if corruption == "missing" {
				if !errors.Is(readErr, os.ErrNotExist) {
					t.Fatal("missing WAL recreated")
				}
			} else if readErr != nil || !bytes.Equal(data, after) {
				t.Fatal("corrupt WAL changed")
			}
		})
	}
}

func TestLegacyJSONMigrationAndCheckpointCursor(t *testing.T) {
	store, path := openTestStore(t)
	run := submittedRun(t, store, "legacy")
	snapshot, _ := store.Snapshot()
	legacy := clone(store.state)
	legacy.Schema, legacy.WALSeq = 1, 0
	data, _ := json.Marshal(legacy)
	store.Close()
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path + ".wal"); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	page, err := reopened.EventsAfter(snapshot.Cursor, 10)
	if err != nil || len(page.Events) != 1 || page.Events[0].Seq != snapshot.LastSeq+1 || page.Events[0].Kind != "run_recovered" {
		t.Fatalf("legacy cursor lost: %+v %v", page, err)
	}
	unknown, _ := reopened.GetRun(run.ID)
	if unknown.Delivery != contracts.DeliveryUnknown || unknown.DeliveryEvidence.SubmittedAt == nil {
		t.Fatal("legacy evidence changed")
	}
	unknown.Phase, unknown.Outcome, unknown.Delivery = contracts.PhaseTerminal, contracts.OutcomeReplied, contracts.DeliveryConfirmed
	confirmation := unknown.UpdatedAt
	unknown.DeliveryEvidence.ConfirmedAt, unknown.DeliveryEvidence.EchoMessageID = &confirmation, "synthetic-legacy-echo"
	if _, err := reopened.UpdateRun(unknown, unknown.Revision, "late_echo"); err != nil {
		t.Fatal(err)
	}
	before, _ := reopened.Snapshot()
	if err := reopened.PruneEvents(run.AcceptedAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	oldWAL, _ := os.ReadFile(path + ".wal")
	if err := reopened.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(path + ".wal")
	if info.Size() >= int64(len(oldWAL)) {
		t.Fatal("checkpoint did not rotate WAL")
	}
	reopened.Close()
	for _, crashWindow := range []string{"rotated", "snapshot_before_rotation"} {
		if crashWindow == "snapshot_before_rotation" {
			if err := os.WriteFile(path+".wal", oldWAL, 0600); err != nil {
				t.Fatal(err)
			}
		}
		again, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		after, _ := again.Snapshot()
		if after.Cursor != before.Cursor || after.StreamID != snapshot.StreamID {
			t.Fatal("checkpoint changed cursor or duplicated transactions")
		}
		history, err := again.RunEvents(run.ID)
		if err != nil || len(history) != 5 {
			t.Fatalf("checkpoint lost history: %d %v", len(history), err)
		}
		if _, err := again.EventsAfter(cursor(snapshot.StreamID, 0), 10); !errors.Is(err, ErrResetRequired) {
			t.Fatal("prune floor lost")
		}
		again.Close()
	}
}

func TestCheckpointRejectsConflictingOldWAL(t *testing.T) {
	store, path := openTestStore(t)
	createTestRun(t, store, "checkpointed")
	file, err := os.Open(path + ".wal")
	if err != nil {
		t.Fatal(err)
	}
	info, _ := file.Stat()
	var header walHeader
	offset, err := readFrame(file, 0, info.Size(), &header)
	if err != nil {
		t.Fatal(err)
	}
	var record walRecord
	if _, err := readFrame(file, offset, info.Size(), &record); err != nil {
		t.Fatal(err)
	}
	file.Close()
	if err := store.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	store.Close()
	record.Run.ReasonCode, record.Event.ReasonCode = "conflicting-fact", "conflicting-fact"
	headerFrame, _ := encodeFrame(header)
	recordFrame, _ := encodeFrame(record)
	if err := os.WriteFile(path+".wal", append(headerFrame, recordFrame...), 0600); err != nil {
		t.Fatal(err)
	}
	opened, err := Open(path)
	if opened != nil {
		opened.Close()
	}
	if !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("checkpoint must not hide conflicting WAL history: %v", err)
	}
}

func TestCheckpointFailureLeavesWALAndFactsRecoverable(t *testing.T) {
	store, path := openTestStore(t)
	run := createTestRun(t, store, "retained")
	beforeWAL, _ := os.ReadFile(path + ".wal")
	original := store.path
	store.path = filepath.Join(path, "impossible-child")
	if err := store.Checkpoint(); err == nil {
		t.Fatal("checkpoint unexpectedly succeeded")
	}
	store.path = original
	stored, err := store.GetRun(run.ID)
	if err != nil || stored.Revision != run.Revision {
		t.Fatal("failed checkpoint changed facts")
	}
	afterWAL, _ := os.ReadFile(path + ".wal")
	if !bytes.Equal(beforeWAL, afterWAL) {
		t.Fatal("failed checkpoint changed WAL")
	}
	createTestRun(t, store, "after-failure")
	store.Close()
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, id := range []string{"retained", "after-failure"} {
		if _, err := reopened.GetRun(id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFailedCheckpointRotationRetainsRecoverableHistory(t *testing.T) {
	store, path := openTestStore(t)
	run := createTestRun(t, store, "rotation-failure")
	before, _ := store.Snapshot()
	changed := store.changed
	if err := os.Rename(path+".wal", path+".wal.saved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path+".wal", 0700); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("rotation failure must fail closed: %v", err)
	}
	select {
	case <-changed:
		t.Fatal("checkpoint failure broadcast")
	default:
	}
	store.Close()
	if err := os.Remove(path + ".wal"); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".wal.saved", path+".wal"); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	history, err := reopened.RunEvents(run.ID)
	if err != nil || len(history) != 2 || history[0].Revision != run.Revision {
		t.Fatalf("rotation failure lost history: %+v %v", history, err)
	}
	page, err := reopened.EventsAfter(before.Cursor, 10)
	if err != nil || len(page.Events) != 1 || page.Events[0].Kind != "run_recovered" {
		t.Fatalf("rotation failure lost cursor: %+v %v", page, err)
	}
}

func TestFailedUpdateAndPruneDoNotPublish(t *testing.T) {
	for _, operation := range []string{"update", "prune"} {
		t.Run(operation, func(t *testing.T) {
			store, _ := openTestStore(t)
			run := createTestRun(t, store, "unchanged")
			changed := store.changed
			store.wal = &faultWAL{walFile: store.wal, syncError: errors.New("synthetic sync failure")}
			var err error
			if operation == "update" {
				run.Phase = contracts.PhaseAggregating
				_, err = store.UpdateRun(run, run.Revision, "uncommitted_update")
			} else {
				err = store.PruneEvents(run.UpdatedAt.Add(time.Hour))
			}
			if !errors.Is(err, ErrPersistenceUnknown) {
				t.Fatal(err)
			}
			if store.state.LastSeq != 1 || store.state.WALSeq != 1 || store.state.FloorSeq != 0 || len(store.state.Events) != 1 || len(store.state.History[run.ID]) != 1 || store.state.Runs[run.ID].Revision != run.Revision || store.state.Runs[run.ID].Phase != contracts.PhaseAccepted {
				t.Fatal("failed operation changed memory")
			}
			select {
			case <-changed:
				t.Fatal("failed operation broadcast")
			default:
			}
		})
	}
}

func TestWaitingReaderGetsPersistenceFailureWithoutEventBroadcast(t *testing.T) {
	store, _ := openTestStore(t)
	snapshot, _ := store.Snapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := store.WaitEvents(ctx, snapshot.Cursor, 10)
		result <- err
	}()
	store.mu.Lock()
	changed := store.changed
	store.wal = &faultWAL{walFile: store.wal, syncError: errors.New("synthetic failure")}
	store.mu.Unlock()
	if _, err := store.CreateRun(testRun("failed")); !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrPersistenceUnknown) {
		t.Fatalf("waiter did not fail closed: %v", err)
	}
	select {
	case <-changed:
		t.Fatal("failed write broadcast an event change")
	default:
	}
}

func TestWALDetachedInputOutputAndEchoInferenceAcrossRestart(t *testing.T) {
	store, path := openTestStore(t)
	input := testRun("echo")
	run, err := store.CreateRun(input)
	if err != nil {
		t.Fatal(err)
	}
	input.MessageIDs[0] = "caller-change"
	run.MessageIDs[0] = "return-change"
	run, _ = store.GetRun("echo")
	if run.MessageIDs[0] != "synthetic-message-echo" {
		t.Fatal("input or output aliases state")
	}
	boundary := run.UpdatedAt
	run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &boundary, BridgeRequestID: "synthetic-request"}
	run = updateTestRun(t, store, run, contracts.PhaseSubmitting, contracts.OutcomePending, contracts.DeliverySubmitting)
	boundary = boundary.Add(time.Hour)
	stored, _ := store.GetRun("echo")
	if !stored.DeliveryEvidence.SubmissionStartedAt.Equal(run.AcceptedAt) {
		t.Fatal("boundary aliases input")
	}
	store.Close()
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	run, _ = reopened.GetRun("echo")
	confirmation := run.UpdatedAt.Add(time.Second)
	run.DeliveryEvidence.SubmittedAtSource = "echo_inferred"
	run.DeliveryEvidence.ConfirmedAt, run.DeliveryEvidence.EchoMessageID = &confirmation, "synthetic-new-echo"
	run.UpdatedAt, run.LastProgressAt = confirmation, confirmation
	run.Phase, run.Outcome, run.Delivery = contracts.PhaseTerminal, contracts.OutcomeReplied, contracts.DeliveryConfirmed
	run, err = reopened.UpdateRun(run, run.Revision, "echo_inferred")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := reopened.Snapshot()
	snapshot.Runs[0].MessageIDs[0] = "snapshot-change"
	history, _ := reopened.RunEvents("echo")
	history[0].Kind = "history-change"
	page, _ := reopened.EventsAfter(cursor(snapshot.StreamID, 0), 100)
	page.Events[0].Kind = "page-change"
	stored, _ = reopened.GetRun("echo")
	actualHistory, _ := reopened.RunEvents("echo")
	actualPage, _ := reopened.EventsAfter(cursor(snapshot.StreamID, 0), 100)
	if stored.MessageIDs[0] != "synthetic-message-echo" || actualHistory[0].Kind != "run_accepted" || actualPage.Events[0].Kind != "run_accepted" {
		t.Fatal("read values alias state")
	}
	if err := reopened.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	reopened.Close()
	again, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	stored, _ = again.GetRun("echo")
	after, _ := again.Snapshot()
	if stored.Revision != run.Revision || stored.DeliveryEvidence.SubmittedAt != nil || stored.DeliveryEvidence.SubmittedAtSource != "echo_inferred" || after.Cursor != snapshot.Cursor {
		t.Fatal("inferred evidence or terminal cursor changed")
	}
}

func TestIncrementalWALAtTenThousandRuns(t *testing.T) {
	store, path := openTestStore(t)
	initial, _ := os.ReadFile(path)
	realWAL := store.wal
	counter := &countedWAL{walFile: realWAL}
	store.wal = counter
	started := time.Now()
	var firstDuration time.Duration
	var firstSize int64
	measureUpdate := func(id string) (time.Duration, int64) {
		store.wal = realWAL
		run, err := store.GetRun(id)
		if err != nil {
			t.Fatal(err)
		}
		before, _ := os.Stat(path + ".wal")
		start := time.Now()
		for index := 0; index < 11; index++ {
			run.UpdatedAt = run.UpdatedAt.Add(time.Second)
			run, err = store.UpdateRun(run, run.Revision, "synthetic_progress")
			if err != nil {
				t.Fatal(err)
			}
		}
		elapsed := time.Since(start)
		after, _ := os.Stat(path + ".wal")
		store.wal = counter
		return elapsed, after.Size() - before.Size()
	}
	var firstUpdateDuration time.Duration
	var firstUpdateBytes int64
	for index := 0; index < 10000; index++ {
		if _, err := store.CreateRun(testRun(fmt.Sprintf("scale-%05d", index))); err != nil {
			t.Fatal(err)
		}
		if index == 999 {
			firstDuration = time.Since(started)
			info, err := os.Stat(path + ".wal")
			if err != nil {
				t.Fatal(err)
			}
			firstSize = info.Size()
			firstUpdateDuration, firstUpdateBytes = measureUpdate("scale-00999")
		}
	}
	createDuration := time.Since(started) - firstUpdateDuration
	info, err := os.Stat(path + ".wal")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() > 11*firstSize {
		t.Fatalf("nonlinear WAL growth: 1k=%d 10k=%d", firstSize, info.Size())
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(initial, after) {
		t.Fatal("mutation rewrote checkpoint")
	}
	updateDuration, updateBytes := measureUpdate("scale-09999")
	store.wal = realWAL
	if counter.writes != 10000 || counter.syncs != 10000 {
		t.Fatalf("each mutation must write and request sync once: writes=%d syncs=%d", counter.writes, counter.syncs)
	}
	if updateBytes > 11*4096 || updateBytes > firstUpdateBytes+100 {
		t.Fatalf("one-run updates rewrote history: %d bytes", updateBytes)
	}
	t.Logf("create with counted sync (real append, excludes disk flush latency): 1k=%s (%d WAL bytes), 10k=%s (%d WAL bytes); 11 updates with real File.Sync: 1k=%s (%d bytes), 10k=%s (%d bytes)", firstDuration, firstSize, createDuration, info.Size(), firstUpdateDuration, firstUpdateBytes, updateDuration, updateBytes)
}

func BenchmarkCreateTenThousandRunsCountedSync(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		path := filepath.Join(b.TempDir(), "facts.json")
		store, err := Open(path)
		if err != nil {
			b.Fatal(err)
		}
		realWAL := store.wal
		store.wal = &countedWAL{walFile: realWAL}
		for index := 0; index < 10000; index++ {
			if _, err := store.CreateRun(testRun(fmt.Sprintf("bench-%05d", index))); err != nil {
				store.Close()
				b.Fatal(err)
			}
		}
		store.wal = realWAL
		if err := realWAL.Sync(); err != nil {
			b.Fatal(err)
		}
		if err := store.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
