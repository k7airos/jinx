package observability

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"syscall"
	"time"

	"jinx/internal/contracts"
)

var (
	ErrNotFound           = errors.New("run not found")
	ErrConflict           = errors.New("revision conflict")
	ErrResetRequired      = errors.New("event snapshot reset required")
	ErrClosed             = errors.New("store closed")
	ErrLocked             = errors.New("store already open")
	ErrPersistenceUnknown = errors.New("persistence result unknown; close and reopen store")
)

type Snapshot struct {
	StreamID      string          `json:"streamId"`
	LastSeq       uint64          `json:"lastSeq"`
	Cursor        string          `json:"cursor"`
	CoverageSince time.Time       `json:"coverageSince"`
	Runs          []contracts.Run `json:"runs"`
}

type EventPage struct {
	Events  []contracts.Event `json:"events"`
	Cursor  string            `json:"cursor"`
	HasMore bool              `json:"hasMore"`
}

type diskState struct {
	Schema        int                          `json:"schema"`
	WALSeq        uint64                       `json:"walSeq,omitempty"`
	StreamID      string                       `json:"streamId"`
	LastSeq       uint64                       `json:"lastSeq"`
	FloorSeq      uint64                       `json:"floorSeq"`
	CoverageSince time.Time                    `json:"coverageSince"`
	Runs          map[string]contracts.Run     `json:"runs"`
	History       map[string][]contracts.Event `json:"history"`
	Events        []contracts.Event            `json:"events"`
}

type Store struct {
	mu        sync.Mutex
	path      string
	lock      *os.File
	wal       walFile
	state     diskState
	changed   chan struct{}
	failed    chan struct{}
	closed    bool
	uncertain bool
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, fmt.Errorf("store path required")
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("%w: %v", ErrLocked, err)
	}
	store := &Store{path: path, lock: lock, changed: make(chan struct{}), failed: make(chan struct{})}
	fail := func(err error) (*Store, error) { store.Close(); return nil, err }
	initialize := false
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		if _, walErr := os.Stat(path + ".wal"); !errors.Is(walErr, os.ErrNotExist) {
			return fail(fmt.Errorf("snapshot missing with existing or inaccessible WAL: %v", walErr))
		}
		streamID, err := newID()
		if err != nil {
			return fail(err)
		}
		store.state = diskState{Schema: 2, StreamID: streamID, CoverageSince: time.Now().UTC(), Runs: make(map[string]contracts.Run), History: make(map[string][]contracts.Event), Events: []contracts.Event{}}
		initialize = true
	} else if err != nil {
		return fail(err)
	} else {
		if err := decodeJSON(data, &store.state); err != nil {
			return fail(fmt.Errorf("decode store: %w", err))
		}
		if err := validateDisk(store.state); err != nil {
			return fail(err)
		}
		initialize = store.state.Schema == 1
	}
	if err := store.openWAL(initialize); err != nil {
		return fail(err)
	}
	if initialize {
		store.state.Schema = 2
		if err := store.writeSnapshot(); err != nil {
			return fail(err)
		}
	}
	now := time.Now().UTC()
	for _, runID := range sortedIDs(store.state.Runs) {
		run := clone(store.state.Runs[runID])
		if run.Phase == contracts.PhaseTerminal {
			continue
		}
		if now.Before(run.UpdatedAt) {
			now = run.UpdatedAt
		}
		run.Phase, run.Outcome = contracts.PhaseUnknown, contracts.OutcomeUnknown
		if run.Delivery == contracts.DeliverySubmitted || run.Delivery == contracts.DeliverySubmitting {
			run.Delivery = contracts.DeliveryUnknown
		}
		run.ReasonCode = "restart_recovery"
		run.UpdatedAt, run.LastProgressAt = now, now
		run.Completeness = contracts.Partial
		for index := range run.Attempts {
			attempt := &run.Attempts[index]
			if attempt.Status == contracts.AttemptRunning || attempt.Status == contracts.AttemptCancelRequested || attempt.Status == contracts.AttemptQueued {
				attempt.Status = contracts.AttemptUnknown
				if attempt.RemoteExecution != contracts.RemoteStopped {
					attempt.RemoteExecution = contracts.RemoteUnknown
				}
			}
		}
		if err := contracts.ValidateTransition(store.state.Runs[runID], run); err != nil {
			return fail(fmt.Errorf("recover run %s: %w", runID, err))
		}
		if _, err := store.commitRun(run, "run_recovered"); err != nil {
			return fail(err)
		}
	}
	return store, nil
}

func (store *Store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	close(store.changed)
	var walErr error
	if store.wal != nil {
		walErr = store.wal.Close()
	}
	return errors.Join(walErr, store.lock.Close())
}

func (store *Store) CreateRun(run contracts.Run) (contracts.Run, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready(); err != nil {
		return contracts.Run{}, err
	}
	if _, exists := store.state.Runs[run.ID]; exists {
		return contracts.Run{}, ErrConflict
	}
	if run.Revision != "" || run.EntitySeq != 0 || run.Phase != contracts.PhaseAccepted || run.Outcome != contracts.OutcomePending || run.Delivery != contracts.DeliveryNotStarted || len(run.Attempts) != 0 {
		return contracts.Run{}, fmt.Errorf("new run must be accepted with no attempts or store revision")
	}
	if err := contracts.ValidateRun(run); err != nil {
		return contracts.Run{}, err
	}
	return store.commitRun(run, "run_accepted")
}

func (store *Store) UpdateRun(run contracts.Run, expectedRevision, kind string) (contracts.Run, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready(); err != nil {
		return contracts.Run{}, err
	}
	previous, exists := store.state.Runs[run.ID]
	if !exists {
		return contracts.Run{}, ErrNotFound
	}
	if expectedRevision == "" || previous.Revision != expectedRevision || run.Revision != previous.Revision || run.EntitySeq != previous.EntitySeq {
		return contracts.Run{}, ErrConflict
	}
	if kind == "" {
		return contracts.Run{}, fmt.Errorf("event kind required")
	}
	if err := contracts.ValidateTransition(previous, run); err != nil {
		return contracts.Run{}, err
	}
	return store.commitRun(run, kind)
}

func (store *Store) commitRun(run contracts.Run, kind string) (contracts.Run, error) {
	run = clone(run)
	revision, err := newID()
	if err != nil {
		return contracts.Run{}, err
	}
	run.Revision, run.EntitySeq = revision, store.state.LastSeq+1
	event := contracts.Event{StreamID: store.state.StreamID, Seq: run.EntitySeq, Cursor: cursor(store.state.StreamID, run.EntitySeq), OccurredAt: run.UpdatedAt, Entity: "run", EntityID: run.ID, Revision: revision, Kind: kind, ReasonCode: run.ReasonCode, Phase: run.Phase, Outcome: run.Outcome, Delivery: run.Delivery}
	event.EnvelopeID, event.Source = inputCorrelation(run)
	event.Action = run.Action
	record := walRecord{Seq: store.state.WALSeq + 1, Run: &run, Event: &event}
	if err := store.appendWAL(record); err != nil {
		return contracts.Run{}, err
	}
	store.applyRecord(record)
	close(store.changed)
	store.changed = make(chan struct{})
	return clone(run), nil
}

func (store *Store) GetRun(id string) (contracts.Run, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready(); err != nil {
		return contracts.Run{}, err
	}
	run, exists := store.state.Runs[id]
	if !exists {
		return contracts.Run{}, ErrNotFound
	}
	return clone(run), nil
}

func (store *Store) RunEvents(id string) ([]contracts.Event, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready(); err != nil {
		return nil, err
	}
	if _, exists := store.state.Runs[id]; !exists {
		return nil, ErrNotFound
	}
	return clone(store.state.History[id]), nil
}

func (store *Store) Snapshot() (Snapshot, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready(); err != nil {
		return Snapshot{}, err
	}
	snapshot := Snapshot{StreamID: store.state.StreamID, LastSeq: store.state.LastSeq, Cursor: cursor(store.state.StreamID, store.state.LastSeq), CoverageSince: store.state.CoverageSince, Runs: []contracts.Run{}}
	for _, runID := range sortedIDs(store.state.Runs) {
		snapshot.Runs = append(snapshot.Runs, clone(store.state.Runs[runID]))
	}
	return snapshot, nil
}

func (store *Store) EventsAfter(after string, limit int) (EventPage, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.eventsAfter(after, limit)
}

func (store *Store) WaitEvents(ctx context.Context, after string, limit int) (EventPage, error) {
	for {
		store.mu.Lock()
		page, err := store.eventsAfter(after, limit)
		changed := store.changed
		failed := store.failed
		store.mu.Unlock()
		if err != nil || len(page.Events) != 0 {
			return page, err
		}
		select {
		case <-ctx.Done():
			return EventPage{}, ctx.Err()
		case <-changed:
		case <-failed:
		}
	}
}

func (store *Store) eventsAfter(after string, limit int) (EventPage, error) {
	if err := store.ready(); err != nil {
		return EventPage{}, err
	}
	if limit < 1 || limit > 1000 {
		return EventPage{}, fmt.Errorf("event limit must be 1..1000")
	}
	var position struct {
		StreamID string
		Seq      uint64
	}
	data, err := base64.RawURLEncoding.DecodeString(after)
	if err != nil || json.Unmarshal(data, &position) != nil || position.StreamID != store.state.StreamID || position.Seq < store.state.FloorSeq || position.Seq > store.state.LastSeq {
		return EventPage{}, ErrResetRequired
	}
	page := EventPage{Events: []contracts.Event{}, Cursor: after}
	for _, event := range store.state.Events[position.Seq-store.state.FloorSeq:] {
		if len(page.Events) == limit {
			page.HasMore = true
			break
		}
		page.Events = append(page.Events, event)
		page.Cursor = event.Cursor
	}
	return page, nil
}

func (store *Store) PruneEvents(before time.Time) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.ready(); err != nil {
		return err
	}
	floor := store.state.FloorSeq
	count := 0
	for _, event := range store.state.Events {
		if !event.OccurredAt.Before(before) {
			break
		}
		floor = event.Seq
		count++
	}
	if count == 0 {
		return nil
	}
	record := walRecord{Seq: store.state.WALSeq + 1, FloorSeq: &floor}
	if err := store.appendWAL(record); err != nil {
		return err
	}
	store.applyRecord(record)
	close(store.changed)
	store.changed = make(chan struct{})
	return nil
}

func (store *Store) ready() error {
	if store.closed {
		return ErrClosed
	}
	if store.uncertain {
		return ErrPersistenceUnknown
	}
	return nil
}

func cursor(streamID string, seq uint64) string {
	data, _ := json.Marshal(struct {
		StreamID string
		Seq      uint64
	}{streamID, seq})
	return base64.RawURLEncoding.EncodeToString(data)
}

func newID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(data[:]), nil
}

func clone[Value any](value Value) Value {
	data, _ := json.Marshal(value)
	var copy Value
	json.Unmarshal(data, &copy)
	return copy
}

func sortedIDs(runs map[string]contracts.Run) []string {
	ids := make([]string, 0, len(runs))
	for id := range runs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func validateDisk(state diskState) error {
	if (state.Schema != 1 && state.Schema != 2) || (state.Schema == 1 && state.WALSeq != 0) || state.StreamID == "" || state.CoverageSince.IsZero() || state.Runs == nil || state.History == nil || state.FloorSeq > state.LastSeq {
		return fmt.Errorf("invalid store header")
	}
	if uint64(len(state.Events)) != state.LastSeq-state.FloorSeq {
		return fmt.Errorf("event journal gap")
	}
	allEvents := make(map[uint64]contracts.Event)
	for id, run := range state.Runs {
		if id != run.ID || run.Revision == "" || run.EntitySeq == 0 || run.EntitySeq > state.LastSeq {
			return fmt.Errorf("invalid stored run identity")
		}
		if err := contracts.ValidateRun(run); err != nil {
			return fmt.Errorf("stored run %s: %w", id, err)
		}
		history := state.History[id]
		if len(history) == 0 {
			return fmt.Errorf("missing run history")
		}
		var previous uint64
		for _, event := range history {
			if event.Seq <= previous || event.Seq > state.LastSeq || event.StreamID != state.StreamID || event.Entity != "run" || event.EntityID != id || event.Revision == "" || event.Kind == "" || event.OccurredAt.IsZero() || event.Cursor != cursor(state.StreamID, event.Seq) {
				return fmt.Errorf("invalid run history")
			}
			if err := validateEventCorrelation(run, event); err != nil {
				return err
			}
			if _, exists := allEvents[event.Seq]; exists {
				return fmt.Errorf("duplicate history sequence")
			}
			allEvents[event.Seq] = event
			previous = event.Seq
		}
		last := history[len(history)-1]
		if last.Seq != run.EntitySeq || last.Revision != run.Revision || last.Phase != run.Phase || last.Outcome != run.Outcome || last.Delivery != run.Delivery || last.ReasonCode != run.ReasonCode || !last.OccurredAt.Equal(run.UpdatedAt) {
			return fmt.Errorf("run and history disagree")
		}
	}
	if len(state.History) != len(state.Runs) || uint64(len(allEvents)) != state.LastSeq {
		return fmt.Errorf("incomplete run history")
	}
	for index, event := range state.Events {
		if event.Seq != state.FloorSeq+uint64(index)+1 || !reflect.DeepEqual(allEvents[event.Seq], event) {
			return fmt.Errorf("event journal and history disagree")
		}
	}
	return nil
}

func inputCorrelation(run contracts.Run) (string, *contracts.SourceRef) {
	if len(run.Inputs) != 1 || len(run.MessageIDs) != 1 {
		return "", nil
	}
	source := run.Inputs[0].Source
	return run.Inputs[0].EnvelopeID, &source
}

func validateEventCorrelation(run contracts.Run, event contracts.Event) error {
	envelopeID, source := inputCorrelation(run)
	if event.EnvelopeID != envelopeID || !reflect.DeepEqual(event.Source, source) {
		return fmt.Errorf("event input correlation disagrees with accepted inputs")
	}
	if event.Action != nil {
		if !reflect.DeepEqual(event.Action, run.Action) || event.Action.AdmittedAt == nil || event.OccurredAt.Before(*event.Action.AdmittedAt) || event.Delivery == contracts.DeliveryNotStarted || event.Delivery == contracts.DeliveryBlocked {
			return fmt.Errorf("event action correlation disagrees with admission")
		}
	} else if run.Action != nil && event.Delivery != contracts.DeliveryNotStarted && event.Delivery != contracts.DeliveryBlocked {
		return fmt.Errorf("submission event is missing admitted action")
	}
	return nil
}
