package contracts

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"
)

func ValidateRun(run Run) error {
	if _, err := json.Marshal(run); err != nil {
		return fmt.Errorf("run cannot be encoded: %w", err)
	}
	if run.ID == "" || run.SourceGroupID == "" || run.TargetGroupID == "" || run.Trigger == "" {
		return fmt.Errorf("run identity, groups and trigger are required")
	}
	if run.AcceptedAt.IsZero() || run.UpdatedAt.Before(run.AcceptedAt) || run.LastProgressAt.Before(run.AcceptedAt) || run.LastProgressAt.After(run.UpdatedAt) {
		return fmt.Errorf("invalid run timestamps")
	}
	if run.AdmissionWatermark.Token == "" || run.AdmissionWatermark.RecordedAt.IsZero() || run.AdmissionWatermark.RecordedAt.After(run.AcceptedAt) {
		return fmt.Errorf("admission watermark is required and cannot follow acceptance")
	}
	if run.Versions.Dispatcher == "" || run.Versions.Fingerprint == "" || (run.ConfigSnapshotID == "" && run.MissingSnapshotReason == "") {
		return fmt.Errorf("configuration version and snapshot reference or missing reason are required")
	}
	if run.Completeness != Complete && run.Completeness != Partial {
		return fmt.Errorf("structured runs require complete or partial provenance")
	}
	if len(run.MessageIDs) == 0 {
		return fmt.Errorf("accepted batch requires message IDs")
	}
	seen := make(map[string]bool)
	for _, messageID := range run.MessageIDs {
		if messageID == "" || seen[messageID] {
			return fmt.Errorf("empty or duplicate message ID")
		}
		seen[messageID] = true
	}
	if len(run.Inputs) > len(run.MessageIDs) || (run.InputsMissingReason != "" && run.InputsMissingReason != "relay_input_metadata_unavailable") {
		return fmt.Errorf("invalid input metadata coverage")
	}
	seenEnvelopes, seenDedup, seenContent := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, input := range run.Inputs {
		if !validID(input.EnvelopeID) || !validID(input.DedupKey) || seenEnvelopes[input.EnvelopeID] || seenDedup[input.DedupKey] || seenContent[input.Source.ContentID] || !seen[input.Source.ContentID] || input.Source.ContainerID != run.SourceGroupID {
			return fmt.Errorf("invalid or duplicate input reference")
		}
		if err := ValidateSourceRef(input.Source); err != nil {
			return err
		}
		if input.ReceivedAt != nil && (input.ReceivedAt.IsZero() || input.ReceivedAt.After(run.AcceptedAt)) {
			return fmt.Errorf("input receipt must precede acceptance")
		}
		seenEnvelopes[input.EnvelopeID], seenDedup[input.DedupKey], seenContent[input.Source.ContentID] = true, true, true
	}
	if _, exists := phaseOrder[run.Phase]; !exists && run.Phase != PhaseUnknown {
		return fmt.Errorf("invalid phase %q", run.Phase)
	}
	evidence := run.DeliveryEvidence
	if run.Action != nil {
		action := run.Action
		if err := ValidateActionRef(*action); err != nil {
			return err
		}
		if !validID(action.IdempotencyKey) || !validID(action.AdmissionID) || action.AdmittedAt == nil || action.AdmittedAt.IsZero() || evidence.SubmissionStartedAt == nil || !action.AdmittedAt.Equal(*evidence.SubmissionStartedAt) || action.ActionID != evidence.BridgeRequestID {
			return fmt.Errorf("action requires durable correlated submission admission")
		}
	}
	switch evidence.SubmittedAtSource {
	case "":
	case "bridge_ack":
		if evidence.SubmittedAt == nil {
			return fmt.Errorf("bridge_ack requires an exact submission timestamp")
		}
	case "echo_inferred":
		if evidence.SubmittedAt != nil || evidence.SubmissionStartedAt == nil || evidence.ConfirmedAt == nil || evidence.EchoMessageID == "" || evidence.BridgeRequestID == "" || !evidence.ConfirmedAt.After(*evidence.SubmissionStartedAt) {
			return fmt.Errorf("echo_inferred requires a new correlated echo after the submission boundary and no invented ACK timestamp")
		}
	default:
		return fmt.Errorf("invalid submittedAtSource %q", evidence.SubmittedAtSource)
	}
	if evidence.SubmissionStartedAt != nil && evidence.BridgeRequestID == "" {
		return fmt.Errorf("submission boundary requires a correlation ID")
	}
	if evidence.SubmissionStartedAt == nil && evidence.BridgeRequestID != "" {
		return fmt.Errorf("submission correlation requires a boundary")
	}
	if evidence.NoSubmission && (evidence.SubmissionStartedAt != nil || evidence.SubmittedAt != nil || evidence.ConfirmedAt != nil) {
		return fmt.Errorf("no-submission evidence conflicts with submission evidence")
	}
	for _, timestamp := range []*time.Time{evidence.SubmissionStartedAt, evidence.SubmittedAt, evidence.ConfirmedAt} {
		if timestamp != nil && (timestamp.Before(run.AcceptedAt) || timestamp.After(run.UpdatedAt)) {
			return fmt.Errorf("delivery timestamp outside run lifetime")
		}
	}
	if evidence.SubmittedAt != nil && (evidence.SubmissionStartedAt == nil || evidence.SubmittedAt.Before(*evidence.SubmissionStartedAt)) {
		return fmt.Errorf("submitted requires preceding submission boundary")
	}
	if evidence.ConfirmedAt != nil && evidence.SubmittedAtSource != "echo_inferred" && (evidence.SubmittedAt == nil || evidence.ConfirmedAt.Before(*evidence.SubmittedAt) || evidence.EchoMessageID == "") {
		return fmt.Errorf("confirmation requires submission and reliable echo")
	}
	if evidence.EchoMessageID != "" && evidence.ConfirmedAt == nil {
		return fmt.Errorf("echo requires confirmation timestamp")
	}
	valid := false
	switch run.Delivery {
	case DeliveryConfirmed:
		valid = run.Phase == PhaseTerminal && run.Outcome == OutcomeReplied && evidence.ConfirmedAt != nil
	case DeliverySubmitted:
		valid = run.Phase == PhaseConfirming && run.Outcome == OutcomePending && evidence.SubmittedAt != nil && evidence.ConfirmedAt == nil
	case DeliverySubmitting:
		valid = run.Phase == PhaseSubmitting && run.Outcome == OutcomePending && evidence.SubmissionStartedAt != nil && evidence.SubmittedAt == nil && evidence.ConfirmedAt == nil
	case DeliveryUnknown:
		valid = run.Phase == PhaseUnknown && run.Outcome == OutcomeUnknown && evidence.SubmissionStartedAt != nil && evidence.ConfirmedAt == nil
	case DeliveryFailed:
		valid = run.Phase == PhaseTerminal && run.Outcome == OutcomeFailed && evidence.FailureEvidence != "" && evidence.SubmittedAt == nil && evidence.ConfirmedAt == nil
	case DeliveryBlocked:
		valid = run.Phase == PhaseTerminal && containsOutcome(run.Outcome, OutcomeBlocked, OutcomeSuppressed, OutcomeCanceled, OutcomeExpired) && evidence.NoSubmission
	case DeliveryNotStarted:
		valid = evidence.SubmissionStartedAt == nil && evidence.SubmittedAt == nil && evidence.ConfirmedAt == nil && ((run.Outcome == OutcomePending && run.Phase != PhaseTerminal && run.Phase != PhaseUnknown && phaseOrder[run.Phase] < phaseOrder[PhaseSubmitting]) || (run.Outcome == OutcomeUnknown && run.Phase == PhaseUnknown) || (run.Phase == PhaseTerminal && containsOutcome(run.Outcome, OutcomeSilent, OutcomeBlocked, OutcomeSuppressed, OutcomeExpired, OutcomeCanceled, OutcomeFailed) && evidence.NoSubmission))
	}
	if !valid {
		return fmt.Errorf("invalid phase/outcome/delivery combination: %s/%s/%s or missing evidence", run.Phase, run.Outcome, run.Delivery)
	}
	if run.Outcome != OutcomePending && run.Outcome != OutcomeReplied && run.ReasonCode == "" {
		return fmt.Errorf("non-success outcome requires reason code")
	}
	seen = make(map[string]bool)
	for _, attempt := range run.Attempts {
		if seen[attempt.ID] {
			return fmt.Errorf("duplicate attempt ID")
		}
		seen[attempt.ID] = true
		if err := validateAttempt(attempt, run); err != nil {
			return fmt.Errorf("attempt %s: %w", attempt.ID, err)
		}
	}
	return nil
}

var phaseOrder = map[Phase]int{PhaseAccepted: 0, PhaseAggregating: 1, PhaseGating: 2, PhaseQueued: 3, PhaseExecuting: 4, PhaseChecking: 5, PhaseSubmitting: 6, PhaseConfirming: 7, PhaseTerminal: 8}

func containsOutcome(outcome Outcome, allowed ...Outcome) bool {
	for _, candidate := range allowed {
		if outcome == candidate {
			return true
		}
	}
	return false
}

func validateAttempt(attempt Attempt, run Run) error {
	if attempt.ID == "" || attempt.BackendID == "" || (attempt.ExternalID == "" && attempt.MissingExternalIDReason == "") || attempt.Versions.Dispatcher == "" || attempt.Versions.Fingerprint == "" {
		return fmt.Errorf("identity, backend, versions and external ID or missing reason required")
	}
	if attempt.RemoteExecution != RemoteStopped && attempt.RemoteExecution != RemoteMayBeRunning && attempt.RemoteExecution != RemoteUnknown && attempt.RemoteExecution != RemoteNotApplicable {
		return fmt.Errorf("invalid remote execution")
	}
	if attempt.StartedAt != nil && (attempt.StartedAt.Before(run.AcceptedAt) || attempt.StartedAt.After(run.UpdatedAt)) {
		return fmt.Errorf("invalid start timestamp")
	}
	if attempt.EndedAt != nil && (attempt.StartedAt == nil || attempt.EndedAt.Before(*attempt.StartedAt) || attempt.EndedAt.After(run.UpdatedAt)) {
		return fmt.Errorf("invalid end timestamp")
	}
	switch attempt.Status {
	case AttemptQueued:
		if attempt.StartedAt != nil || attempt.EndedAt != nil {
			return fmt.Errorf("queued attempt cannot have execution timestamps")
		}
	case AttemptRunning, AttemptCancelRequested:
		if attempt.StartedAt == nil || attempt.EndedAt != nil {
			return fmt.Errorf("active attempt requires start and no end")
		}
	case AttemptSucceeded, AttemptFailed, AttemptTimedOut, AttemptCanceled:
		if attempt.StartedAt == nil || attempt.EndedAt == nil {
			return fmt.Errorf("finished attempt requires start and end")
		}
		if attempt.Status == AttemptCanceled && attempt.RemoteExecution != RemoteStopped {
			return fmt.Errorf("canceled requires proven remote stop")
		}
		if attempt.Status == AttemptTimedOut && attempt.RemoteExecution == RemoteNotApplicable {
			return fmt.Errorf("timeout requires explicit remote uncertainty or proven stop")
		}
	case AttemptUnknown:
	default:
		return fmt.Errorf("invalid attempt status")
	}
	return nil
}

func ValidateTransition(previous, next Run) error {
	if err := ValidateRun(next); err != nil {
		return err
	}
	if previous.ID != next.ID || previous.SourceGroupID != next.SourceGroupID || previous.TargetGroupID != next.TargetGroupID || !previous.AcceptedAt.Equal(next.AcceptedAt) || !reflect.DeepEqual(previous.MessageIDs, next.MessageIDs) || !reflect.DeepEqual(previous.AdmissionWatermark, next.AdmissionWatermark) || previous.Trigger != next.Trigger || previous.Versions != next.Versions || previous.ConfigSnapshotID != next.ConfigSnapshotID || previous.MissingSnapshotReason != next.MissingSnapshotReason || previous.PlannedBackendID != next.PlannedBackendID {
		return fmt.Errorf("accepted identity and execution snapshot are immutable")
	}
	if next.UpdatedAt.Before(previous.UpdatedAt) || next.LastProgressAt.Before(previous.LastProgressAt) {
		return fmt.Errorf("timestamps cannot move backwards")
	}
	if !reflect.DeepEqual(previous.Inputs, next.Inputs) || previous.InputsMissingReason != next.InputsMissingReason {
		return fmt.Errorf("accepted input references are immutable")
	}
	if previous.Action != nil && !reflect.DeepEqual(previous.Action, next.Action) {
		return fmt.Errorf("admitted action is immutable")
	}
	if previous.Action == nil && next.Action != nil && (previous.Phase != PhaseChecking || previous.Delivery != DeliveryNotStarted || previous.DeliveryEvidence.SubmissionStartedAt != nil || next.Phase != PhaseSubmitting || next.Delivery != DeliverySubmitting) {
		return fmt.Errorf("action can only be created at submission admission")
	}
	if previous.Phase == PhaseTerminal && (next.Phase != previous.Phase || next.Outcome != previous.Outcome || next.Delivery != previous.Delivery || next.ReasonCode != previous.ReasonCode) {
		return fmt.Errorf("terminal result cannot change")
	}
	if next.Phase != PhaseUnknown && previous.Phase != PhaseUnknown && phaseOrder[next.Phase] < phaseOrder[previous.Phase] {
		return fmt.Errorf("phase cannot move backwards")
	}
	if next.Phase != PhaseUnknown && next.Phase != PhaseTerminal && previous.Phase != PhaseUnknown && phaseOrder[next.Phase] > phaseOrder[previous.Phase]+1 && next.ReasonCode == "" {
		return fmt.Errorf("skipped stages require a reason code")
	}
	if previous.Phase == PhaseUnknown && next.Phase != PhaseUnknown && next.Phase != PhaseTerminal && next.Delivery != DeliverySubmitted {
		return fmt.Errorf("unknown run cannot automatically resume execution")
	}
	allowed := map[Delivery][]Delivery{
		DeliveryNotStarted: {DeliveryNotStarted, DeliveryBlocked, DeliverySubmitting},
		DeliverySubmitting: {DeliverySubmitting, DeliverySubmitted, DeliveryUnknown, DeliveryConfirmed, DeliveryFailed},
		DeliverySubmitted:  {DeliverySubmitted, DeliveryUnknown, DeliveryConfirmed},
		DeliveryUnknown:    {DeliveryUnknown, DeliverySubmitted, DeliveryConfirmed, DeliveryFailed},
	}
	permitted := previous.Phase == PhaseTerminal && next.Delivery == previous.Delivery
	for _, delivery := range allowed[previous.Delivery] {
		if next.Delivery == delivery {
			permitted = true
		}
	}
	if !permitted {
		return fmt.Errorf("delivery cannot move from %s to %s", previous.Delivery, next.Delivery)
	}
	before, after := previous.DeliveryEvidence, next.DeliveryEvidence
	if (before.SubmittedAtSource != "" && before.SubmittedAtSource != after.SubmittedAtSource) || (before.SubmittedAt != nil && after.SubmittedAtSource == "echo_inferred") {
		return fmt.Errorf("submission source cannot regress or replace an existing ACK")
	}
	if before.FailureEvidence != "" && before.FailureEvidence != after.FailureEvidence {
		return fmt.Errorf("proven delivery failure cannot be erased or replaced")
	}
	if (before.SubmissionStartedAt != nil && !reflect.DeepEqual(before.SubmissionStartedAt, after.SubmissionStartedAt)) || (before.SubmittedAt != nil && !reflect.DeepEqual(before.SubmittedAt, after.SubmittedAt)) || (before.ConfirmedAt != nil && !reflect.DeepEqual(before.ConfirmedAt, after.ConfirmedAt)) || (before.BridgeRequestID != "" && before.BridgeRequestID != after.BridgeRequestID) || (before.EchoMessageID != "" && before.EchoMessageID != after.EchoMessageID) {
		return fmt.Errorf("delivery evidence cannot be erased or replaced")
	}
	if len(next.Attempts) < len(previous.Attempts) {
		return fmt.Errorf("attempt history cannot be removed")
	}
	if (previous.Phase == PhaseTerminal || previous.Phase == PhaseUnknown || previous.Delivery != DeliveryNotStarted) && len(next.Attempts) != len(previous.Attempts) {
		return fmt.Errorf("terminal, uncertain or submitting run cannot start additional attempts")
	}
	for index, attempt := range previous.Attempts {
		updated := next.Attempts[index]
		if attempt.ID != updated.ID || attempt.BackendID != updated.BackendID || attempt.ModelLabel != updated.ModelLabel || attempt.Versions != updated.Versions || attempt.FallbackReason != updated.FallbackReason || (attempt.StartedAt != nil && !reflect.DeepEqual(attempt.StartedAt, updated.StartedAt)) || (attempt.EndedAt != nil && !reflect.DeepEqual(attempt.EndedAt, updated.EndedAt)) || (attempt.ExternalID != "" && attempt.ExternalID != updated.ExternalID) {
			return fmt.Errorf("attempt identity and evidence are immutable")
		}
		if attempt.Status != updated.Status && !attemptTransition(attempt.Status, updated.Status) {
			return fmt.Errorf("invalid attempt status transition")
		}
		if attempt.RemoteExecution == RemoteStopped && updated.RemoteExecution != RemoteStopped {
			return fmt.Errorf("proven remote stop cannot regress")
		}
	}
	return nil
}

func attemptTransition(previous, next AttemptStatus) bool {
	switch previous {
	case AttemptQueued:
		return next == AttemptRunning || next == AttemptUnknown
	case AttemptRunning:
		return next != AttemptQueued
	case AttemptCancelRequested:
		return next != AttemptQueued && next != AttemptRunning
	case AttemptTimedOut, AttemptUnknown:
		return next == AttemptSucceeded || next == AttemptFailed || next == AttemptCanceled || next == AttemptUnknown
	default:
		return false
	}
}
