package contracts

import (
	"testing"
	"time"
)

func validRun() Run {
	now := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	return Run{ID: "synthetic-run", SourceGroupID: "synthetic-source", TargetGroupID: "synthetic-target", AcceptedAt: now, UpdatedAt: now, LastProgressAt: now, AdmissionWatermark: Watermark{Token: "synthetic-boundary", RecordedAt: now}, Trigger: "mention", MessageIDs: []string{"synthetic-message"}, Phase: PhaseAccepted, Outcome: OutcomePending, Delivery: DeliveryNotStarted, Completeness: Complete, Versions: Versions{Dispatcher: "d1", Fingerprint: "f1"}, MissingSnapshotReason: "snapshot_not_retained"}
}

func stateRun(phase Phase, outcome Outcome, delivery Delivery) Run {
	run := validRun()
	run.Phase, run.Outcome, run.Delivery = phase, outcome, delivery
	run.ReasonCode = "synthetic_reason"
	return run
}

func TestStateCombinationMatrix(t *testing.T) {
	phases := []Phase{PhaseAccepted, PhaseAggregating, PhaseGating, PhaseQueued, PhaseExecuting, PhaseChecking, PhaseSubmitting, PhaseConfirming, PhaseTerminal, PhaseUnknown, "invalid"}
	outcomes := []Outcome{OutcomePending, OutcomeReplied, OutcomeSilent, OutcomeBlocked, OutcomeSuppressed, OutcomeExpired, OutcomeCanceled, OutcomeFailed, OutcomeUnknown, "invalid"}
	deliveries := []Delivery{DeliveryNotStarted, DeliveryBlocked, DeliverySubmitting, DeliverySubmitted, DeliveryConfirmed, DeliveryFailed, DeliveryUnknown, "invalid"}
	for _, phase := range phases {
		for _, outcome := range outcomes {
			for _, delivery := range deliveries {
				t.Run(string(phase)+"/"+string(outcome)+"/"+string(delivery), func(t *testing.T) {
					run := stateRun(phase, outcome, delivery)
					now := run.AcceptedAt
					terminal := phase == PhaseTerminal
					want := false
					switch delivery {
					case DeliveryNotStarted:
						run.DeliveryEvidence.NoSubmission = true
						active := phase == PhaseAccepted || phase == PhaseAggregating || phase == PhaseGating || phase == PhaseQueued || phase == PhaseExecuting || phase == PhaseChecking
						want = (active && outcome == OutcomePending) || (phase == PhaseUnknown && outcome == OutcomeUnknown) || (terminal && (outcome == OutcomeSilent || outcome == OutcomeBlocked || outcome == OutcomeSuppressed || outcome == OutcomeExpired || outcome == OutcomeCanceled || outcome == OutcomeFailed))
					case DeliveryBlocked:
						run.DeliveryEvidence.NoSubmission = true
						want = terminal && (outcome == OutcomeBlocked || outcome == OutcomeSuppressed || outcome == OutcomeExpired || outcome == OutcomeCanceled)
					case DeliverySubmitting:
						run.DeliveryEvidence = DeliveryEvidence{SubmissionStartedAt: &now, BridgeRequestID: "synthetic-bridge"}
						want = phase == PhaseSubmitting && outcome == OutcomePending
					case DeliverySubmitted:
						run.DeliveryEvidence = DeliveryEvidence{SubmissionStartedAt: &now, SubmittedAt: &now, BridgeRequestID: "synthetic-bridge"}
						want = phase == PhaseConfirming && outcome == OutcomePending
					case DeliveryConfirmed:
						run.DeliveryEvidence = DeliveryEvidence{SubmissionStartedAt: &now, SubmittedAt: &now, ConfirmedAt: &now, BridgeRequestID: "synthetic-bridge", EchoMessageID: "synthetic-echo"}
						want = terminal && outcome == OutcomeReplied
					case DeliveryUnknown:
						run.DeliveryEvidence = DeliveryEvidence{SubmissionStartedAt: &now, BridgeRequestID: "synthetic-bridge"}
						want = phase == PhaseUnknown && outcome == OutcomeUnknown
					case DeliveryFailed:
						run.DeliveryEvidence.FailureEvidence = "bridge_explicit_rejection"
						want = terminal && outcome == OutcomeFailed
					}
					if err := ValidateRun(run); (err == nil) != want {
						t.Fatalf("valid=%v, want %v: %v", err == nil, want, err)
					}
				})
			}
		}
	}
}

func TestRequiredEvidenceAndIdentity(t *testing.T) {
	tests := map[string]func(*Run){
		"missing_group":               func(run *Run) { run.SourceGroupID = "" },
		"missing_acceptance":          func(run *Run) { run.AcceptedAt = time.Time{} },
		"missing_watermark":           func(run *Run) { run.AdmissionWatermark.Token = "" },
		"missing_messages":            func(run *Run) { run.MessageIDs = nil },
		"duplicate_messages":          func(run *Run) { run.MessageIDs = []string{"same", "same"} },
		"missing_version":             func(run *Run) { run.Versions.Fingerprint = "" },
		"missing_snapshot_provenance": func(run *Run) { run.MissingSnapshotReason = "" },
		"legacy_is_not_structured":    func(run *Run) { run.Completeness = LegacyPartial },
		"unknown_completeness":        func(run *Run) { run.Completeness = "invented" },
		"progress_after_update":       func(run *Run) { run.LastProgressAt = run.UpdatedAt.Add(time.Second) },
		"unencodable_timestamp":       func(run *Run) { run.UpdatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"confirmation_without_echo": func(run *Run) {
			run.Phase, run.Outcome, run.Delivery = PhaseTerminal, OutcomeReplied, DeliveryConfirmed
			run.DeliveryEvidence.ConfirmedAt = &run.AcceptedAt
		},
		"blocked_without_boundary_proof": func(run *Run) {
			run.Phase, run.Outcome, run.Delivery, run.ReasonCode = PhaseTerminal, OutcomeBlocked, DeliveryBlocked, "permission_denied"
		},
		"failure_without_proof": func(run *Run) {
			run.Phase, run.Outcome, run.Delivery, run.ReasonCode = PhaseTerminal, OutcomeFailed, DeliveryFailed, "http_timeout"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			run := validRun()
			mutate(&run)
			if ValidateRun(run) == nil {
				t.Fatal("invalid run accepted")
			}
		})
	}
}

func TestTransitionsProtectSubmissionAndAllowLateEcho(t *testing.T) {
	submitting := stateRun(PhaseSubmitting, OutcomePending, DeliverySubmitting)
	now := submitting.AcceptedAt
	submitting.DeliveryEvidence = DeliveryEvidence{SubmissionStartedAt: &now, BridgeRequestID: "synthetic-bridge"}
	submitted := submitting
	submitted.Phase, submitted.Delivery = PhaseConfirming, DeliverySubmitted
	submitted.DeliveryEvidence.SubmittedAt = &now
	unknown := submitted
	unknown.Phase, unknown.Outcome, unknown.Delivery, unknown.ReasonCode = PhaseUnknown, OutcomeUnknown, DeliveryUnknown, "confirmation_timeout"
	confirmed := unknown
	confirmed.Phase, confirmed.Outcome, confirmed.Delivery = PhaseTerminal, OutcomeReplied, DeliveryConfirmed
	confirmed.DeliveryEvidence.ConfirmedAt, confirmed.DeliveryEvidence.EchoMessageID = &now, "synthetic-echo"
	for _, transition := range [][2]Run{{submitting, submitted}, {submitted, unknown}, {unknown, confirmed}} {
		if err := ValidateTransition(transition[0], transition[1]); err != nil {
			t.Fatal(err)
		}
	}
	for name, mutate := range map[string]func(*Run){
		"resubmit_unknown": func(run *Run) {
			run.Phase, run.Outcome, run.Delivery = PhaseSubmitting, OutcomePending, DeliverySubmitting
			run.DeliveryEvidence.SubmittedAt = nil
		},
		"cancel_submitted": func(run *Run) {
			run.Phase, run.Outcome, run.Delivery = PhaseTerminal, OutcomeCanceled, DeliveryBlocked
			run.DeliveryEvidence = DeliveryEvidence{NoSubmission: true}
		},
		"erase_evidence":      func(run *Run) { run.DeliveryEvidence.SubmittedAt = nil },
		"change_config":       func(run *Run) { run.Versions.Dispatcher = "d2" },
		"change_target":       func(run *Run) { run.TargetGroupID = "another-target" },
		"replace_correlation": func(run *Run) { run.DeliveryEvidence.BridgeRequestID = "another-request" },
	} {
		t.Run(name, func(t *testing.T) {
			next := unknown
			mutate(&next)
			if ValidateTransition(unknown, next) == nil {
				t.Fatal("unsafe transition accepted")
			}
		})
	}
	if ValidateTransition(confirmed, unknown) == nil {
		t.Fatal("confirmed regressed")
	}
	previous := validRun()
	next := previous
	next.Phase = PhaseChecking
	if ValidateTransition(previous, next) == nil {
		t.Fatal("skipped stages without reason accepted")
	}
	next.ReasonCode = "aggregation_and_gating_not_required"
	if err := ValidateTransition(previous, next); err != nil {
		t.Fatal(err)
	}
}

func TestAttemptValidationAndHistory(t *testing.T) {
	run := stateRun(PhaseExecuting, OutcomePending, DeliveryNotStarted)
	now := run.AcceptedAt
	base := Attempt{ID: "synthetic-attempt", BackendID: "synthetic-backend", Status: AttemptRunning, RemoteExecution: RemoteMayBeRunning, StartedAt: &now, MissingExternalIDReason: "adapter_has_no_external_id", Versions: run.Versions}
	run.Attempts = []Attempt{base}
	if err := ValidateRun(run); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Attempt){
		"invalid_status":              func(attempt *Attempt) { attempt.Status = "invented" },
		"invalid_remote":              func(attempt *Attempt) { attempt.RemoteExecution = "invented" },
		"queued_with_start":           func(attempt *Attempt) { attempt.Status = AttemptQueued },
		"finished_without_end":        func(attempt *Attempt) { attempt.Status = AttemptSucceeded },
		"running_without_start":       func(attempt *Attempt) { attempt.StartedAt = nil },
		"cancel_without_stop":         func(attempt *Attempt) { attempt.Status, attempt.EndedAt = AttemptCanceled, &now },
		"missing_external_provenance": func(attempt *Attempt) { attempt.MissingExternalIDReason = "" },
	} {
		t.Run(name, func(t *testing.T) {
			next := run
			attempt := base
			mutate(&attempt)
			next.Attempts = []Attempt{attempt}
			if ValidateRun(next) == nil {
				t.Fatal("invalid attempt accepted")
			}
		})
	}
	next := run
	timedOut := base
	timedOut.Status, timedOut.EndedAt = AttemptTimedOut, &now
	next.Attempts = []Attempt{timedOut}
	if err := ValidateTransition(run, next); err != nil {
		t.Fatal(err)
	}
	regressed := next
	regressed.Attempts = []Attempt{base}
	if ValidateTransition(next, regressed) == nil {
		t.Fatal("timed out attempt restarted")
	}
	regressed.Attempts = nil
	if ValidateTransition(next, regressed) == nil {
		t.Fatal("attempt history erased")
	}
}

func TestTerminalRunCanRecordRemoteStopWithoutChangingOutcome(t *testing.T) {
	run := stateRun(PhaseTerminal, OutcomeCanceled, DeliveryBlocked)
	run.DeliveryEvidence.NoSubmission = true
	now := run.AcceptedAt
	run.Attempts = []Attempt{{ID: "remote-attempt", BackendID: "synthetic", Status: AttemptRunning, RemoteExecution: RemoteMayBeRunning, StartedAt: &now, MissingExternalIDReason: "not_exposed", Versions: run.Versions}}
	next := run
	attempt := run.Attempts[0]
	attempt.Status, attempt.RemoteExecution, attempt.EndedAt = AttemptCanceled, RemoteStopped, &now
	next.Attempts = []Attempt{attempt}
	if err := ValidateTransition(run, next); err != nil {
		t.Fatal(err)
	}
	next.Outcome = OutcomeSuppressed
	if ValidateTransition(run, next) == nil {
		t.Fatal("terminal outcome overwritten")
	}
}
