package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/execution"
)

type ControlScope struct {
	Kind    string `json:"kind"`
	GroupID string `json:"groupId,omitempty"`
}

type ScopeControl struct {
	Paused    bool      `json:"paused"`
	Revision  string    `json:"revision"`
	Boundary  uint64    `json:"boundary"`
	ChangedAt time.Time `json:"changedAt"`
}

type Maintenance struct {
	ID               string       `json:"maintenanceId"`
	Revision         string       `json:"revision"`
	Scope            ControlScope `json:"scope"`
	Boundary         uint64       `json:"boundary"`
	StartedAt        time.Time    `json:"startedAt"`
	Deadline         time.Time    `json:"deadline"`
	Active           bool         `json:"active"`
	RecoveryRequired bool         `json:"recoveryRequired"`
}

type RunBlock struct {
	Reason string `json:"reason"`
	Mode   string `json:"mode,omitempty"`
}

type ControlState struct {
	Revision    string                      `json:"revision"`
	Global      ScopeControl                `json:"global"`
	Groups      map[string]ScopeControl     `json:"groups"`
	Maintenance *Maintenance                `json:"maintenance,omitempty"`
	BlockedRuns map[string]RunBlock         `json:"blockedRuns,omitempty"`
	Operations  map[string]ControlOperation `json:"operations,omitempty"`
}

type ControlAction struct {
	Kind          string       `json:"kind"`
	Scope         ControlScope `json:"scope"`
	MaintenanceID string       `json:"maintenanceId,omitempty"`
	RunID         string       `json:"runId,omitempty"`
	CancelMode    string       `json:"cancelMode,omitempty"`
}

type ControlRequest struct {
	RequestID        string        `json:"requestId"`
	ExpectedRevision string        `json:"expectedRevision"`
	Action           ControlAction `json:"action"`
}

type ControlOperation struct {
	OperationID     string        `json:"operationId"`
	RequestID       string        `json:"requestId"`
	Revision        string        `json:"revision"`
	Status          string        `json:"status"`
	Control         *ScopeControl `json:"control,omitempty"`
	MaintenanceID   string        `json:"maintenanceId,omitempty"`
	RunID           string        `json:"runId,omitempty"`
	RunRevision     string        `json:"runRevision,omitempty"`
	RemoteExecution string        `json:"remoteExecution,omitempty"`
	Fingerprint     string        `json:"fingerprint"`
	Action          ControlAction `json:"action"`
	HTTPStatus      int           `json:"httpStatus"`
	Code            string        `json:"code,omitempty"`
}

type ControlCounts struct {
	Pending         int            `json:"pending"`
	Phases          map[string]int `json:"phases"`
	ActiveWorkers   int            `json:"activeWorkers"`
	UnknownDelivery int            `json:"unknownDelivery"`
	RemoteUnsettled int            `json:"remoteUnsettled"`
	UnresolvedRuns  int            `json:"unresolvedRuns"`
}

type MaintenanceView struct {
	Maintenance
	Stage    string        `json:"stage"`
	Counts   ControlCounts `json:"counts"`
	Blockers []string      `json:"blockers"`
}

type ControlSnapshot struct {
	Revision     string                  `json:"revision"`
	Global       ScopeControl            `json:"global"`
	Groups       map[string]ScopeControl `json:"groups"`
	Maintenance  *MaintenanceView        `json:"maintenance,omitempty"`
	Capabilities map[string]bool         `json:"capabilities"`
}

type activeRun struct {
	cancel        context.CancelFunc
	groupID       string
	targetGroupID string
}

func (scope ControlScope) includes(source, target string) bool {
	return scope.Kind == "global" || scope.GroupID == source || scope.GroupID == target
}

func (d *Dispatcher) initControlLocked() {
	control := &d.state.Control
	if control.Revision == "" {
		control.Revision = "1"
		control.Global.Revision = "1"
	}
	if control.Groups == nil {
		control.Groups = make(map[string]ScopeControl)
	}
	if control.BlockedRuns == nil {
		control.BlockedRuns = make(map[string]RunBlock)
	}
	if control.Operations == nil {
		control.Operations = make(map[string]ControlOperation)
	}
	for id := range d.cfg.Rules.GroupRules {
		if _, exists := control.Groups[id]; !exists {
			control.Groups[id] = ScopeControl{Revision: "1"}
		}
	}
}

func (d *Dispatcher) pausedLocked(source, target string) bool {
	control := &d.state.Control
	return control.Global.Paused || control.Groups[source].Paused || control.Groups[target].Paused
}

func (d *Dispatcher) executionHeldLocked(source, target string) bool {
	maintenance := d.state.Control.Maintenance
	return d.pausedLocked(source, target) || (maintenance != nil && maintenance.Active && maintenance.RecoveryRequired && maintenance.Scope.includes(source, target))
}

func (d *Dispatcher) intakeBlockedLocked(source, target string, sequence uint64) bool {
	if d.pausedLocked(source, target) {
		return true
	}
	control := &d.state.Control
	if sequence <= control.Global.Boundary || sequence <= control.Groups[source].Boundary || sequence <= control.Groups[target].Boundary {
		return true
	}
	maintenance := d.state.Control.Maintenance
	return maintenance != nil && maintenance.Active && maintenance.Scope.includes(source, target)
}

func (d *Dispatcher) batchBlockLocked(b batch) string {
	if block, exists := d.state.Control.BlockedRuns[b.TraceID]; exists {
		return block.Reason
	}
	if d.pausedLocked(b.GroupID, b.Rule.TargetGroupID) {
		return "paused_boundary"
	}
	return ""
}

const (
	maxControlOperations  = 1000
	maxControlBlockedRuns = 1000
)

func (d *Dispatcher) pruneControlLocked() {
	control := &d.state.Control
	if len(control.Operations) > maxControlOperations {
		type opEntry struct {
			id  string
			rev uint64
		}
		candidates := make([]opEntry, 0, len(control.Operations))
		for id, op := range control.Operations {
			if op.Status == "applying" || (control.Maintenance != nil && op.MaintenanceID != "" && op.MaintenanceID == control.Maintenance.ID) {
				continue
			}
			rev, _ := strconv.ParseUint(op.Revision, 10, 64)
			candidates = append(candidates, opEntry{id: id, rev: rev})
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].rev != candidates[j].rev {
				return candidates[i].rev < candidates[j].rev
			}
			return candidates[i].id < candidates[j].id
		})
		excess := len(control.Operations) - maxControlOperations
		for i := 0; i < excess && i < len(candidates); i++ {
			delete(control.Operations, candidates[i].id)
		}
	}
	if len(control.BlockedRuns) > maxControlBlockedRuns {
		protected := make(map[string]bool)
		for runID := range d.runContexts {
			protected[runID] = true
		}
		for _, g := range d.state.Groups {
			for _, msg := range g.InFlight {
				if msg.TraceID != "" {
					protected[msg.TraceID] = true
				}
			}
			for _, msg := range g.Pending {
				if msg.TraceID != "" {
					protected[msg.TraceID] = true
				}
			}
		}
		for _, op := range control.Operations {
			if op.RunID != "" {
				protected[op.RunID] = true
			}
		}
		candidates := make([]string, 0, len(control.BlockedRuns))
		for runID := range control.BlockedRuns {
			if !protected[runID] {
				candidates = append(candidates, runID)
			}
		}
		sort.Strings(candidates)
		excess := len(control.BlockedRuns) - maxControlBlockedRuns
		for i := 0; i < excess && i < len(candidates); i++ {
			delete(control.BlockedRuns, candidates[i])
		}
	}
}

func (d *Dispatcher) persistControlLocked() error {
	d.pruneControlLocked()
	if err := d.saveStateLocked(); err != nil {
		return d.traceFailure()
	}
	return nil
}

func (d *Dispatcher) recoverControlLocked() error {
	d.initControlLocked()
	if maintenance := d.state.Control.Maintenance; maintenance != nil && maintenance.Active {
		maintenance.RecoveryRequired = true
		previous, _ := strconv.ParseUint(d.state.Control.Revision, 10, 64)
		d.state.Control.Revision = strconv.FormatUint(previous+1, 10)
		maintenance.Revision = d.state.Control.Revision
	}
	for runID, block := range d.state.Control.BlockedRuns {
		run, err := d.trace.GetRun(runID)
		if err != nil {
			return d.traceFailure()
		}
		if run.Phase != contracts.PhaseTerminal && run.DeliveryEvidence.SubmissionStartedAt == nil {
			outcome := contracts.OutcomeSuppressed
			if block.Mode != "" {
				outcome = contracts.OutcomeCanceled
			}
			if !d.traceTerminal(batch{TraceID: runID}, outcome, block.Reason) {
				return errTraceUnavailable
			}
		}
	}
	if d.state.Control.Global.Paused {
		if err := d.suppressPausedLocked(ControlScope{Kind: "global"}, d.cfg); err != nil {
			return err
		}
	} else {
		for id, scope := range d.state.Control.Groups {
			if scope.Paused {
				if err := d.suppressPausedLocked(ControlScope{Kind: "group", GroupID: id}, d.cfg); err != nil {
					return err
				}
			}
		}
	}
	for id, operation := range d.state.Control.Operations {
		if operation.Status == "applying" {
			operation.Status = "succeeded"
			if operation.Code == "remote_cancel_unsupported" {
				operation.Status = "delivery_blocked"
			}
			if operation.RunID != "" {
				if run, err := d.trace.GetRun(operation.RunID); err == nil {
					operation.RunRevision = run.Revision
				}
			}
			d.state.Control.Operations[id] = operation
		}
	}
	return nil
}

func (d *Dispatcher) controlSnapshotLocked(cfg Config) (ControlSnapshot, error) {
	control := &d.state.Control
	view := ControlSnapshot{Revision: control.Revision, Global: control.Global, Groups: make(map[string]ScopeControl), Capabilities: map[string]bool{"pause": true, "drain": true, "blockDelivery": true, "remoteCancel": false}}
	_, gated := d.runner.(runnerDeliveryGate)
	view.Capabilities["deliveryGate"] = gated
	view.Capabilities["legacySoft"] = !gated
	_, stoppable := d.runner.(runnerStopper)
	view.Capabilities["stopExecution"] = stoppable
	for id := range cfg.Rules.GroupRules {
		scope, exists := control.Groups[id]
		if !exists {
			scope.Revision = "1"
		}
		view.Groups[id] = scope
	}
	for id, scope := range control.Groups {
		view.Groups[id] = scope
	}
	maintenance := control.Maintenance
	if maintenance == nil {
		return view, nil
	}
	status := &MaintenanceView{Maintenance: *maintenance, Stage: "draining", Counts: ControlCounts{Phases: map[string]int{}}, Blockers: []string{}}
	view.Maintenance = status
	if !maintenance.Active {
		status.Stage = "canceled"
		return view, nil
	}
	paused := control.Global.Paused
	for id := range cfg.Rules.GroupRules {
		rule, _ := cfg.route(id)
		if maintenance.Scope.includes(id, rule.TargetGroupID) {
			paused = paused || d.pausedLocked(id, rule.TargetGroupID)
		}
	}
	for id, group := range d.state.Groups {
		rule, _ := cfg.route(id)
		if maintenance.Scope.includes(id, rule.TargetGroupID) {
			for _, message := range group.Pending {
				if message.TraceID == "" && d.tracedMessages[id+"/"+message.ID] == "" {
					status.Counts.Pending++
				}
			}
			paused = paused || d.pausedLocked(id, rule.TargetGroupID)
		}
	}
	for _, worker := range d.runContexts {
		if maintenance.Scope.includes(worker.groupID, worker.targetGroupID) {
			status.Counts.ActiveWorkers++
		}
	}
	snapshot, err := d.trace.Snapshot()
	if err != nil {
		return view, err
	}
	if maintenance.Scope.Kind == "group" {
		paused = paused || control.Groups[maintenance.Scope.GroupID].Paused
	} else {
		for _, group := range control.Groups {
			paused = paused || group.Paused
		}
	}
	for _, run := range snapshot.Runs {
		if !maintenance.Scope.includes(run.SourceGroupID, run.TargetGroupID) {
			continue
		}
		status.Counts.Phases[string(run.Phase)]++
		if run.Phase != contracts.PhaseTerminal {
			status.Counts.UnresolvedRuns++
			paused = paused || d.pausedLocked(run.SourceGroupID, run.TargetGroupID)
		}
		if run.Delivery == contracts.DeliveryUnknown || run.Delivery == contracts.DeliverySubmitting || run.Delivery == contracts.DeliverySubmitted {
			status.Counts.UnknownDelivery++
		}
		unsettled := false
		for _, attempt := range run.Attempts {
			unsettled = unsettled || attempt.RemoteExecution == contracts.RemoteMayBeRunning || attempt.RemoteExecution == contracts.RemoteUnknown
		}
		if unsettled {
			status.Counts.RemoteUnsettled++
		}
	}
	if paused {
		status.Blockers = append(status.Blockers, "blocked_by_pause")
	}
	if maintenance.RecoveryRequired {
		status.Blockers = append(status.Blockers, "restart_recovery")
	}
	if status.Counts.UnknownDelivery > 0 {
		status.Blockers = append(status.Blockers, "unknown_delivery")
	}
	if status.Counts.RemoteUnsettled > 0 {
		status.Blockers = append(status.Blockers, "remote_unsettled")
	}
	if !time.Now().Before(maintenance.Deadline) {
		status.Blockers = append(status.Blockers, "deadline_exceeded")
	}
	if len(status.Blockers) > 0 {
		status.Stage = "blocked"
		if paused {
			status.Stage = "blocked_by_pause"
		}
	} else if status.Counts.Pending == 0 && status.Counts.ActiveWorkers == 0 && status.Counts.UnresolvedRuns == 0 {
		status.Stage = "ready"
	}
	return view, nil
}

func (d *Dispatcher) handleControlSnapshot(w http.ResponseWriter, r *http.Request) {
	if !d.traceReadAllowed(w, r) {
		return
	}
	cfg := d.configSnapshot()
	d.mu.Lock()
	snapshot, err := d.controlSnapshotLocked(cfg)
	d.mu.Unlock()
	if err != nil {
		traceHTTPError(w, err)
		return
	}
	traceJSON(w, http.StatusOK, snapshot)
}

func (d *Dispatcher) handleControlOperation(w http.ResponseWriter, r *http.Request) {
	if !d.traceReadAllowed(w, r) {
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/api/control/operations/")
	d.mu.Lock()
	var found *ControlOperation
	for _, operation := range d.state.Control.Operations {
		if operation.OperationID == id {
			copy := operation
			found = &copy
			break
		}
	}
	d.mu.Unlock()
	if found == nil {
		traceJSON(w, http.StatusNotFound, map[string]string{"code": "not_found"})
		return
	}
	traceJSON(w, http.StatusOK, found)
}

func validateControlRequest(request ControlRequest) bool {
	action := request.Action
	if strings.TrimSpace(request.RequestID) == "" || len(request.RequestID) > 200 || request.ExpectedRevision == "" {
		return false
	}
	if (action.Scope.Kind != "global" && action.Scope.Kind != "group") || (action.Scope.Kind == "group" && !contracts.ValidContainerID(action.Scope.GroupID)) || (action.Scope.Kind == "global" && action.Scope.GroupID != "") {
		return false
	}
	switch action.Kind {
	case "pause", "resume", "drain":
		return action.RunID == "" && action.CancelMode == "" && action.MaintenanceID == ""
	case "cancel_maintenance", "continue_maintenance":
		return action.MaintenanceID != "" && action.RunID == "" && action.CancelMode == ""
	case "cancel_run":
		return action.RunID != "" && action.MaintenanceID == "" && (action.CancelMode == "block_delivery" || action.CancelMode == "stop_execution")
	}
	return false
}

func (d *Dispatcher) handleControlAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		traceJSON(w, http.StatusMethodNotAllowed, map[string]string{"code": "method_not_allowed"})
		return
	}
	if mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mediaType != "application/json" {
		traceJSON(w, http.StatusBadRequest, map[string]string{"code": "json_required"})
		return
	}
	_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(10 * time.Second))
	var request ControlRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16384))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF || !validateControlRequest(request) {
		traceJSON(w, http.StatusBadRequest, map[string]string{"code": "invalid_action"})
		return
	}
	d.controlMu.Lock()
	defer d.controlMu.Unlock()
	d.submissionMu.Lock()
	submissionLocked := true
	defer func() {
		if submissionLocked {
			d.submissionMu.Unlock()
		}
	}()
	cfg := d.configSnapshot()
	d.mu.Lock()
	stateLocked := true
	defer func() {
		if stateLocked {
			d.mu.Unlock()
		}
	}()
	if d.closed || d.ctx.Err() != nil || d.traceFailed.Load() {
		traceHTTPError(w, errTraceUnavailable)
		return
	}
	control := &d.state.Control
	encoded, _ := json.Marshal(request)
	fingerprint := messageDigest(string(encoded))
	if previous, exists := control.Operations[request.RequestID]; exists {
		if previous.Fingerprint != fingerprint {
			traceJSON(w, http.StatusConflict, map[string]string{"code": "request_id_reused", "revision": control.Revision})
		} else {
			traceJSON(w, previous.HTTPStatus, previous)
		}
		return
	}
	action := request.Action
	var run contracts.Run
	if action.Kind == "cancel_run" {
		var err error
		run, err = d.trace.GetRun(action.RunID)
		if err != nil {
			traceHTTPError(w, err)
			return
		}
		if !action.Scope.includes(run.SourceGroupID, run.TargetGroupID) || run.Revision != request.ExpectedRevision || run.Phase == contracts.PhaseTerminal || run.DeliveryEvidence.SubmissionStartedAt != nil {
			traceJSON(w, http.StatusConflict, map[string]string{"code": "run_changed", "revision": run.Revision})
			return
		}
	} else if request.ExpectedRevision != control.Revision {
		traceJSON(w, http.StatusConflict, map[string]string{"code": "control_changed", "revision": control.Revision})
		return
	}
	maintenance := control.Maintenance
	if action.Kind == "drain" && maintenance != nil && maintenance.Active {
		traceJSON(w, http.StatusConflict, map[string]string{"code": "maintenance_active", "revision": control.Revision})
		return
	}
	if action.Kind == "cancel_maintenance" || action.Kind == "continue_maintenance" {
		if maintenance == nil || !maintenance.Active || maintenance.ID != action.MaintenanceID || maintenance.Scope != action.Scope {
			traceJSON(w, http.StatusConflict, map[string]string{"code": "maintenance_changed", "revision": control.Revision})
			return
		}
	}
	traceLocked := false
	defer func() {
		if traceLocked {
			d.traceMu.Unlock()
		}
	}()
	if action.Kind == "cancel_run" {
		d.traceMu.Lock()
		traceLocked = true
		current, err := d.trace.GetRun(run.ID)
		if err != nil {
			traceHTTPError(w, err)
			return
		}
		if current.Revision != request.ExpectedRevision {
			traceJSON(w, http.StatusConflict, map[string]string{"code": "run_changed", "revision": current.Revision})
			return
		}
	}
	previous, _ := strconv.ParseUint(control.Revision, 10, 64)
	control.Revision = strconv.FormatUint(previous+1, 10)
	now := time.Now().UTC()
	operation := ControlOperation{OperationID: "operation-" + messageDigest(request.RequestID), RequestID: request.RequestID, Revision: control.Revision, Status: "applying", Fingerprint: fingerprint, Action: action, HTTPStatus: http.StatusOK}
	boundary := d.intakeSequence.Load()
	if boundary > d.state.IncomingSequence {
		d.state.IncomingSequence = boundary
	}
	switch action.Kind {
	case "pause", "resume":
		scope := ScopeControl{Paused: action.Kind == "pause", Revision: control.Revision, Boundary: boundary, ChangedAt: now}
		if action.Scope.Kind == "global" {
			control.Global = scope
		} else {
			control.Groups[action.Scope.GroupID] = scope
		}
		operation.Control = &scope
	case "drain":
		control.Maintenance = &Maintenance{ID: "maintenance-" + messageDigest(request.RequestID), Revision: control.Revision, Scope: action.Scope, Boundary: boundary, StartedAt: now, Deadline: now.Add(5 * time.Minute), Active: true}
		operation.MaintenanceID = control.Maintenance.ID
	case "cancel_maintenance":
		maintenance.Active, maintenance.Revision = false, control.Revision
		operation.MaintenanceID = maintenance.ID
		if action.Scope.Kind == "global" {
			control.Global.Boundary = boundary
		} else {
			scope := control.Groups[action.Scope.GroupID]
			scope.Boundary = boundary
			control.Groups[action.Scope.GroupID] = scope
		}
	case "continue_maintenance":
		maintenance.RecoveryRequired, maintenance.Revision, maintenance.Deadline = false, control.Revision, now.Add(5*time.Minute)
		operation.MaintenanceID = maintenance.ID
	case "cancel_run":
		operation.RunID = run.ID
		operation.RemoteExecution = "stopped"
		for _, attempt := range run.Attempts {
			if attempt.RemoteExecution == contracts.RemoteMayBeRunning || attempt.RemoteExecution == contracts.RemoteUnknown {
				operation.RemoteExecution = "may_be_running"
				if action.CancelMode == "stop_execution" {
					operation.HTTPStatus, operation.Code = http.StatusNotImplemented, "remote_cancel_unsupported"
				}
			}
		}
	}
	if action.Kind == "cancel_run" {
		control.BlockedRuns[run.ID] = RunBlock{Reason: "run_canceled", Mode: action.CancelMode}
	}
	control.Operations[request.RequestID] = operation
	if err := d.persistControlLocked(); err != nil {
		traceHTTPError(w, err)
		return
	}
	if action.Kind == "pause" {
		if err := d.suppressPausedLocked(action.Scope, cfg); err != nil {
			traceHTTPError(w, err)
			return
		}
	}
	if action.Kind == "continue_maintenance" || action.Kind == "cancel_maintenance" {
		for id, group := range d.state.Groups {
			if rule, ok := cfg.route(id); ok {
				d.scheduleLocked(id, group, rule, cfg.Rules.BotNames)
			}
		}
	}
	if action.Kind == "cancel_run" {
		if err := d.cancelRunTraceLocked(run, action.CancelMode); err != nil {
			traceHTTPError(w, err)
			return
		}
		if current, err := d.trace.GetRun(run.ID); err == nil {
			operation.RunRevision = current.Revision
		}
		d.traceMu.Unlock()
		traceLocked = false
		if runner, ok := d.runner.(runnerStopper); ok && action.CancelMode == "stop_execution" && len(run.Attempts) > 0 {
			d.mu.Unlock()
			stateLocked = false
			d.submissionMu.Unlock()
			submissionLocked = false
			result, supported, stopErr := runner.StopExecution(r.Context(), run.ID)
			if err := d.recordExecutionStop(run.ID, result, supported, stopErr); err != nil {
				traceHTTPError(w, err)
				return
			}
			operation.RemoteExecution = string(executionRemote(result.RemoteExecution))
			if stopErr != nil || !supported {
				operation.RemoteExecution = "unknown"
			}
			if stopErr == nil && supported && result.RemoteExecution == "stopped" {
				operation.HTTPStatus, operation.Code = http.StatusOK, ""
			} else {
				operation.HTTPStatus, operation.Code = http.StatusNotImplemented, "remote_cancel_unsupported"
			}
			d.mu.Lock()
			stateLocked = true
			if current, err := d.trace.GetRun(run.ID); err == nil {
				operation.RunRevision = current.Revision
			}
		}
	}
	operation.Status = "succeeded"
	if operation.Code != "" {
		operation.Status = "delivery_blocked"
	}
	control.Operations[request.RequestID] = operation
	if err := d.persistControlLocked(); err != nil {
		traceHTTPError(w, err)
		return
	}
	traceJSON(w, operation.HTTPStatus, operation)
}

func (d *Dispatcher) suppressPausedLocked(scope ControlScope, cfg Config) error {
	snapshot, err := d.trace.Snapshot()
	if err != nil {
		return d.traceFailure()
	}
	for _, run := range snapshot.Runs {
		if scope.includes(run.SourceGroupID, run.TargetGroupID) && run.Phase != contracts.PhaseTerminal && run.DeliveryEvidence.SubmissionStartedAt == nil {
			d.state.Control.BlockedRuns[run.ID] = RunBlock{Reason: "paused_boundary"}
		}
	}
	if err := d.persistControlLocked(); err != nil {
		return err
	}
	for runID, block := range d.state.Control.BlockedRuns {
		if block.Reason == "paused_boundary" {
			run, err := d.trace.GetRun(runID)
			if err != nil {
				return d.traceFailure()
			}
			if !d.awaitingRunnerMetadata(run) && !d.traceTerminal(batch{TraceID: runID}, contracts.OutcomeSuppressed, block.Reason) {
				return errTraceUnavailable
			}
		}
	}
	for id, group := range d.state.Groups {
		rule, _ := cfg.route(id)
		if !scope.includes(id, rule.TargetGroupID) {
			continue
		}
		if group.Timer != nil {
			group.Timer.Stop()
		}
		group.TimerGeneration++
		for len(group.Pending) > 0 {
			count := len(group.Pending)
			if count > 25 {
				count = 25
			}
			messages := append([]RelayMessage(nil), group.Pending[:count]...)
			fresh := messages[:0]
			for _, message := range messages {
				if message.TraceID == "" && d.tracedMessages[id+"/"+message.ID] == "" {
					message.LegacyQueued = true
					fresh = append(fresh, message)
				}
			}
			if len(fresh) == 0 {
				group.Pending = group.Pending[count:]
				continue
			}
			work := batch{GroupID: id, Rule: rule, Config: cfg, Messages: fresh, Reason: "pause_boundary"}
			if err := d.acceptTraceLocked(&work); err != nil {
				return err
			}
			d.state.Control.BlockedRuns[work.TraceID] = RunBlock{Reason: "paused_boundary"}
			if err := d.persistControlLocked(); err != nil {
				return err
			}
			if !d.traceTerminal(work, contracts.OutcomeSuppressed, "paused_boundary") {
				return errTraceUnavailable
			}
			group.Pending = group.Pending[count:]
		}
		group.PendingSince = time.Time{}
	}
	return nil
}

func (d *Dispatcher) cancelRunTraceLocked(run contracts.Run, mode string) error {
	now := time.Now().UTC()
	if now.Before(run.UpdatedAt) {
		now = run.UpdatedAt
	}
	run.UpdatedAt, run.LastProgressAt = now, now
	if !d.awaitingRunnerMetadata(run) {
		run.Phase, run.Outcome, run.Delivery, run.ReasonCode = contracts.PhaseTerminal, contracts.OutcomeCanceled, contracts.DeliveryBlocked, "run_canceled"
		run.DeliveryEvidence.NoSubmission = true
	}
	for index := range run.Attempts {
		attempt := &run.Attempts[index]
		if attempt.Status == contracts.AttemptRunning {
			attempt.RemoteExecution = contracts.RemoteMayBeRunning
			if mode == "stop_execution" {
				attempt.Status = contracts.AttemptCancelRequested
			}
		}
	}
	if _, err := d.trace.UpdateRun(run, run.Revision, "run_cancel_requested"); err != nil {
		return d.traceFailure()
	}
	if mode == "stop_execution" || len(run.Attempts) == 0 {
		if worker, exists := d.runContexts[run.ID]; exists {
			worker.cancel()
		}
	}
	return nil
}

func (d *Dispatcher) beginControlledRun(b batch, cancel context.CancelFunc) bool {
	d.submissionMu.Lock()
	defer d.submissionMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.batchBlockLocked(b) != "" {
		return false
	}
	d.runContexts[b.TraceID] = activeRun{cancel: cancel, groupID: b.GroupID, targetGroupID: b.Rule.TargetGroupID}
	return true
}

func (d *Dispatcher) beginControlledExecution(b batch) error {
	d.submissionMu.Lock()
	defer d.submissionMu.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if reason := d.batchBlockLocked(b); reason != "" {
		return fmt.Errorf("%s", reason)
	}
	if err := d.tracePhase(b, contracts.PhaseExecuting, ""); err != nil {
		return err
	}
	return d.traceAttemptStart(b)
}

func (d *Dispatcher) runBlocked(b batch) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	reason := d.batchBlockLocked(b)
	if reason != "" {
		outcome := contracts.OutcomeSuppressed
		if reason == "run_canceled" {
			outcome = contracts.OutcomeCanceled
		}
		d.traceTerminal(b, outcome, reason)
	}
	return reason != ""
}

func (d *Dispatcher) recordExecutionStop(runID string, result execution.CancelResult, supported bool, stopErr error) error {
	return d.updateTrace(batch{TraceID: runID}, "attempt_finished", func(run *contracts.Run) {
		remote := executionRemote(result.RemoteExecution)
		if stopErr != nil || !supported {
			remote = contracts.RemoteUnknown
		}
		for index := range run.Attempts {
			attempt := &run.Attempts[index]
			if index != len(run.Attempts)-1 && attempt.Status != contracts.AttemptRunning && attempt.Status != contracts.AttemptCancelRequested {
				continue
			}
			if attempt.RemoteExecution == contracts.RemoteStopped || attempt.RemoteExecution == contracts.RemoteNotApplicable {
				continue
			}
			attempt.RemoteExecution = remote
			if remote == contracts.RemoteStopped && !d.awaitingRunnerMetadata(*run) && (attempt.Status == contracts.AttemptRunning || attempt.Status == contracts.AttemptCancelRequested || attempt.Status == contracts.AttemptUnknown) {
				attempt.Status = contracts.AttemptCanceled
				if attempt.EndedAt == nil {
					now := run.UpdatedAt
					attempt.EndedAt = &now
				}
			}
		}
	})
}
