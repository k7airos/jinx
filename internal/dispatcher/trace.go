package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/execution"
	"jinx/internal/observability"
)

type traceStore interface {
	CreateRun(contracts.Run) (contracts.Run, error)
	UpdateRun(contracts.Run, string, string) (contracts.Run, error)
	GetRun(string) (contracts.Run, error)
	RunEvents(string) ([]contracts.Event, error)
	Snapshot() (observability.Snapshot, error)
	EventsAfter(string, int) (observability.EventPage, error)
	WaitEvents(context.Context, string, int) (observability.EventPage, error)
	Close() error
}

type traceMaintainer interface {
	PruneEvents(time.Time) error
	Checkpoint() error
}

const (
	traceMaintenanceInterval = 256
	traceEventRetention      = 7 * 24 * time.Hour
)

func (d *Dispatcher) maintainTraceLocked() {
	d.traceOps++
	if d.traceOps%traceMaintenanceInterval != 0 {
		return
	}
	maintainer, ok := d.trace.(traceMaintainer)
	if !ok {
		return
	}
	_ = maintainer.PruneEvents(time.Now().UTC().Add(-traceEventRetention))
	_ = maintainer.Checkpoint()
}

var errTraceUnavailable = errors.New("trace unavailable; dispatcher stopped")

func (d *Dispatcher) traceFailure() error {
	d.traceFailed.Store(true)
	d.cancel()
	return errTraceUnavailable
}

func batchMessageIDs(b batch) []string {
	ids := make([]string, 0, len(b.Messages))
	for _, msg := range b.Messages {
		ids = append(ids, msg.ID)
	}
	sort.Strings(ids)
	return ids
}

func (d *Dispatcher) acceptTraceLocked(b *batch) error {
	if d.trace == nil || d.traceFailed.Load() || d.ctx.Err() != nil {
		return d.traceFailure()
	}
	ids := batchMessageIDs(*b)
	if b.Policy == "" {
		b.Policy = policyID(b.Config)
	}
	for _, msg := range b.Messages {
		if msg.TraceID != "" || d.tracedMessages[b.GroupID+"/"+msg.ID] != "" {
			return fmt.Errorf("historical traced batch cannot resume")
		}
	}
	identity, _ := json.Marshal(struct {
		Source string
		IDs    []string
	}{b.GroupID, ids})
	id := "run-" + messageDigest(string(identity))
	now := time.Now().UTC()
	for _, msg := range b.Messages {
		if msg.ReceivedAt != nil && now.Before(*msg.ReceivedAt) {
			now = *msg.ReceivedAt
		}
	}
	trigger := b.Reason
	if trigger == "" {
		trigger = "direct_batch"
	}
	missing := ""
	for _, msg := range b.Messages {
		if msg.LegacyQueued {
			missing = "legacy_queue_admission_history_unavailable"
		}
	}
	snapshotID := ""
	if missing == "" {
		var err error
		snapshotID, err = d.retainConfigSnapshotLocked(*b, now)
		if err != nil {
			return err
		}
	}
	backend := "Hermes"
	if b.Direct {
		backend = "direct_passthrough"
	} else if b.Rule.SupportMode {
		backend = "Alma"
	}
	if !b.Direct {
		if _, structured := d.runner.(runnerMetadata); structured {
			backend = "reply_runner"
		}
	}
	watermark := d.state.LastEventID
	if watermark == "" {
		watermark = "dispatcher:" + strconv.FormatUint(d.state.Sequence, 10)
	}
	run := contracts.Run{
		ID: id, SourceGroupID: b.GroupID, TargetGroupID: b.Rule.TargetGroupID,
		MessageIDs: ids, AcceptedAt: now, UpdatedAt: now, LastProgressAt: now,
		AdmissionWatermark: contracts.Watermark{Token: watermark, RecordedAt: now, SourceSequence: strconv.FormatUint(d.state.Sequence, 10)},
		Trigger:            trigger, Phase: contracts.PhaseAccepted, Outcome: contracts.OutcomePending, Delivery: contracts.DeliveryNotStarted,
		Completeness: contracts.Partial, Versions: contracts.Versions{Dispatcher: "dispatcher-trace-v1", Routing: b.Policy, Policy: b.Policy, Fingerprint: b.Policy},
		ConfigSnapshotID: snapshotID, MissingSnapshotReason: missing, PlannedBackendID: backend,
	}
	for _, msg := range b.Messages {
		envelope, err := NormalizeRelayMessage(msg)
		if err != nil {
			run.InputsMissingReason = "relay_input_metadata_unavailable"
			continue
		}
		receivedAt := envelope.ReceivedAt
		run.Inputs = append(run.Inputs, contracts.InboundReference{EnvelopeID: envelope.EnvelopeID, DedupKey: envelope.DedupKey, Source: envelope.Source, ReceivedAt: &receivedAt})
	}
	d.traceMu.Lock()
	stored, err := d.trace.CreateRun(run)
	if err == nil {
		d.maintainTraceLocked()
	}
	d.traceMu.Unlock()
	if err != nil {
		return d.traceFailure()
	}
	b.TraceID = stored.ID
	for index := range b.Messages {
		b.Messages[index].TraceID = stored.ID
		d.tracedMessages[b.GroupID+"/"+b.Messages[index].ID] = stored.ID
	}
	if d.pausedLocked(b.GroupID, b.Rule.TargetGroupID) {
		d.state.Control.BlockedRuns[b.TraceID] = RunBlock{Reason: "paused_boundary"}
		if err := d.persistControlLocked(); err != nil {
			return err
		}
		if !d.traceTerminal(*b, contracts.OutcomeSuppressed, "paused_boundary") {
			return errTraceUnavailable
		}
	}
	return nil
}

func (d *Dispatcher) ensureTrace(b *batch) error {
	if b.TraceID != "" {
		if d.traceFailed.Load() || d.ctx.Err() != nil {
			return errTraceUnavailable
		}
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.acceptTraceLocked(b)
}

func (d *Dispatcher) updateTrace(b batch, kind string, change func(*contracts.Run)) error {
	d.traceMu.Lock()
	defer d.traceMu.Unlock()
	if d.trace == nil || d.traceFailed.Load() {
		return d.traceFailure()
	}
	run, err := d.trace.GetRun(b.TraceID)
	if err != nil {
		return d.traceFailure()
	}
	if run.Phase == contracts.PhaseTerminal && kind != "attempt_finished" {
		return nil
	}
	revision := run.Revision
	now := time.Now().UTC()
	if now.Before(run.UpdatedAt) {
		now = run.UpdatedAt
	}
	run.UpdatedAt, run.LastProgressAt = now, now
	change(&run)
	if _, err := d.trace.UpdateRun(run, revision, kind); err != nil {
		return d.traceFailure()
	}
	d.maintainTraceLocked()
	return nil
}

func (d *Dispatcher) tracePhase(b batch, phase contracts.Phase, reason string) error {
	return d.updateTrace(b, "run_"+string(phase), func(run *contracts.Run) {
		run.Phase, run.ReasonCode = phase, reason
	})
}

func (d *Dispatcher) traceTerminal(b batch, outcome contracts.Outcome, reason string) bool {
	return d.updateTrace(b, "run_terminal", func(run *contracts.Run) {
		run.Phase, run.Outcome, run.ReasonCode = contracts.PhaseTerminal, outcome, reason
		run.DeliveryEvidence.NoSubmission = true
		if outcome == contracts.OutcomeBlocked || outcome == contracts.OutcomeSuppressed || outcome == contracts.OutcomeExpired || outcome == contracts.OutcomeCanceled {
			run.Delivery = contracts.DeliveryBlocked
		}
	}) == nil
}

func (d *Dispatcher) traceUnknown(b batch, reason string) error {
	return d.updateTrace(b, "run_unknown", func(run *contracts.Run) {
		run.Phase, run.Outcome, run.ReasonCode = contracts.PhaseUnknown, contracts.OutcomeUnknown, reason
		if run.DeliveryEvidence.SubmissionStartedAt != nil {
			run.Delivery = contracts.DeliveryUnknown
		}
	})
}

func (d *Dispatcher) traceAttemptStart(b batch) error {
	return d.updateTrace(b, "attempt_started", func(run *contracts.Run) {
		now := run.UpdatedAt
		run.Attempts = append(run.Attempts, contracts.Attempt{
			ID: run.ID + ":adapter", BackendID: "reply_adapter", Status: contracts.AttemptRunning,
			StartedAt: &now, RemoteExecution: contracts.RemoteUnknown, Versions: run.Versions,
			MissingExternalIDReason: "adapter_invocation_has_no_external_id",
		})
	})
}

func (d *Dispatcher) traceAttemptEnd(ctx context.Context, b batch, result HermesResult, backendErr error) error {
	var metadata execution.Result
	var hasMetadata bool
	if runner, ok := d.runner.(runnerMetadata); ok {
		metadata, hasMetadata = runner.Meta(b.TraceID)
	}
	return d.updateTrace(b, "attempt_finished", func(run *contracts.Run) {
		attempt := &run.Attempts[0]
		now := run.UpdatedAt
		if attempt.EndedAt == nil {
			provenStop := attempt.RemoteExecution == contracts.RemoteStopped
			attempt.EndedAt, attempt.Status = &now, contracts.AttemptSucceeded
			attempt.RemoteExecution = contracts.RemoteStopped
			if backendErr != nil {
				attempt.Status = contracts.AttemptFailed
				attempt.RemoteExecution = contracts.RemoteUnknown
				if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(backendErr, context.DeadlineExceeded) {
					attempt.Status = contracts.AttemptTimedOut
					attempt.RemoteExecution = contracts.RemoteMayBeRunning
				} else if ctx.Err() != nil {
					attempt.Status = contracts.AttemptUnknown
					attempt.RemoteExecution = contracts.RemoteMayBeRunning
				}
			}
			if result.Fallback {
				attempt.RemoteExecution = contracts.RemoteMayBeRunning
			}
			if provenStop {
				attempt.RemoteExecution = contracts.RemoteStopped
				if backendErr != nil {
					attempt.Status = contracts.AttemptCanceled
				}
			}
			if hasMetadata && len(metadata.Attempts) > 0 && !provenStop {
				attempt.RemoteExecution = contracts.RemoteNotApplicable
				if attempt.Status == contracts.AttemptTimedOut {
					attempt.RemoteExecution = contracts.RemoteUnknown
				}
			}
		}
		if run.Phase == contracts.PhaseTerminal {
			return
		}
		if hasMetadata {
			for index, actual := range metadata.Attempts {
				versions := contracts.Versions{Dispatcher: run.Versions.Dispatcher, Routing: metadata.RoutingRevision, Policy: actual.PolicyRevision}
				if versions.Policy == "" {
					versions.Policy = metadata.PolicyRevision
				}
				versions.Fingerprint = messageDigest(versions.Routing + ":" + versions.Policy)
				recorded := contracts.Attempt{
					ID: fmt.Sprintf("%s:execution:%d", run.ID, index+1), BackendID: actual.BackendID, ModelLabel: actual.ModelID,
					ExternalID: actual.ExternalID, FallbackReason: actual.FallbackReason, Versions: versions,
					Status: contracts.AttemptStatus(actual.Status), RemoteExecution: executionRemote(actual.RemoteExecution),
				}
				if recorded.ExternalID == "" {
					recorded.MissingExternalIDReason = "execution_backend_has_no_external_id"
				}
				if !actual.StartedAt.IsZero() {
					started := actual.StartedAt
					recorded.StartedAt = &started
				}
				if !actual.EndedAt.IsZero() {
					ended := actual.EndedAt
					recorded.EndedAt = &ended
				}
				if index == len(metadata.Attempts)-1 && attempt.Status == contracts.AttemptCanceled && attempt.RemoteExecution == contracts.RemoteStopped {
					recorded.RemoteExecution = contracts.RemoteStopped
				}
				if recorded.Status == contracts.AttemptCanceled && recorded.RemoteExecution != contracts.RemoteStopped {
					recorded.Status = contracts.AttemptUnknown
				}
				if recorded.Status == contracts.AttemptTimedOut && recorded.RemoteExecution == contracts.RemoteNotApplicable {
					recorded.RemoteExecution = contracts.RemoteUnknown
				}
				run.Attempts = append(run.Attempts, recorded)
			}
			return
		}
		if _, structured := d.runner.(runnerMetadata); structured {
			return
		}
		if result.Backend == "" && result.SessionID == "" && result.RunID == "" && !result.Fallback {
			return
		}
		fallback := ""
		if result.Fallback {
			fallback = "adapter_reported_fallback"
			for _, code := range []string{"alma_timeout", "alma_failed", "alma_empty_summary", "alma_invalid_output", "alma_transport_error"} {
				if result.FallbackReason == code {
					fallback = code
				}
			}
		}
		remote := contracts.RemoteStopped
		if result.Fallback {
			remote = contracts.RemoteMayBeRunning
		}
		run.Attempts = append(run.Attempts, contracts.Attempt{
			ID: run.ID + ":result", BackendID: result.actualBackend(), Status: contracts.AttemptUnknown,
			RemoteExecution: remote, ExternalID: result.RunID,
			MissingExternalIDReason: "adapter_result_only_attempt_details_unavailable", FallbackReason: fallback, Versions: run.Versions,
		})
	})
}

func executionRemote(remote string) contracts.RemoteExecution {
	switch value := contracts.RemoteExecution(remote); value {
	case contracts.RemoteStopped, contracts.RemoteMayBeRunning, contracts.RemoteNotApplicable:
		return value
	default:
		return contracts.RemoteUnknown
	}
}

func (d *Dispatcher) awaitingRunnerMetadata(run contracts.Run) bool {
	_, structured := d.runner.(runnerMetadata)
	return structured && len(run.Attempts) > 0 && run.Attempts[0].EndedAt == nil
}

func traceJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func traceHTTPError(w http.ResponseWriter, err error) {
	status, code := http.StatusServiceUnavailable, "unavailable"
	if errors.Is(err, observability.ErrResetRequired) {
		status, code = http.StatusConflict, "reset_required"
	} else if errors.Is(err, observability.ErrNotFound) {
		status, code = http.StatusNotFound, "not_found"
	}
	traceJSON(w, status, map[string]string{"code": code})
}

func (d *Dispatcher) traceReadAllowed(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		traceJSON(w, http.StatusMethodNotAllowed, map[string]string{"code": "method_not_allowed"})
		return false
	}
	if d.trace == nil || d.traceFailed.Load() {
		traceHTTPError(w, errTraceUnavailable)
		return false
	}
	return true
}

func (d *Dispatcher) handleTraceSnapshot(w http.ResponseWriter, r *http.Request) {
	if !d.traceReadAllowed(w, r) {
		return
	}
	snapshot, err := d.trace.Snapshot()
	if err != nil {
		traceHTTPError(w, err)
		return
	}
	traceJSON(w, http.StatusOK, snapshot)
}

func (d *Dispatcher) handleTraceEvents(w http.ResponseWriter, r *http.Request) {
	if !d.traceReadAllowed(w, r) {
		return
	}
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 1000 {
			traceHTTPError(w, errTraceUnavailable)
			return
		}
		limit = parsed
	}
	cursor := r.URL.Query().Get("cursor")
	var page observability.EventPage
	var err error
	if r.URL.Query().Get("wait") == "1" {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		page, err = d.trace.WaitEvents(ctx, cursor, limit)
		if errors.Is(err, context.DeadlineExceeded) && r.Context().Err() == nil {
			page, err = d.trace.EventsAfter(cursor, limit)
		}
	} else {
		page, err = d.trace.EventsAfter(cursor, limit)
	}
	if err != nil {
		traceHTTPError(w, err)
		return
	}
	if r.URL.Query().Get("projection") == "processing" {
		projected, err := d.processingTracePage(page)
		if err != nil {
			traceHTTPError(w, err)
			return
		}
		traceJSON(w, http.StatusOK, projected)
		return
	}
	traceJSON(w, http.StatusOK, page)
}

func (d *Dispatcher) handleTraceRun(w http.ResponseWriter, r *http.Request) {
	if !d.traceReadAllowed(w, r) {
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/trace/runs/")
	d.traceMu.Lock()
	run, err := d.trace.GetRun(id)
	var events []contracts.Event
	if err == nil {
		events, err = d.trace.RunEvents(id)
	}
	d.traceMu.Unlock()
	if err != nil {
		traceHTTPError(w, err)
		return
	}
	traceJSON(w, http.StatusOK, struct {
		Run    contracts.Run     `json:"run"`
		Events []contracts.Event `json:"events"`
	}{run, events})
}
