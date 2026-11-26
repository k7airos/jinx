package contracts

import (
	"encoding/json"
	"testing"
	"time"
)

func echoInferredRun() Run {
	run := stateRun(PhaseTerminal, OutcomeReplied, DeliveryConfirmed)
	boundary := run.AcceptedAt
	confirmed := boundary.Add(time.Second)
	run.UpdatedAt, run.LastProgressAt = confirmed, confirmed
	run.DeliveryEvidence = DeliveryEvidence{SubmissionStartedAt: &boundary, SubmittedAtSource: "echo_inferred", ConfirmedAt: &confirmed, EchoMessageID: "synthetic-echo", BridgeRequestID: "synthetic-request"}
	return run
}

func TestSubmissionSourceEvidence(t *testing.T) {
	inferred := echoInferredRun()
	if err := ValidateRun(inferred); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(inferred)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Run
	if err := json.Unmarshal(data, &decoded); err != nil || decoded.DeliveryEvidence.SubmittedAtSource != "echo_inferred" || decoded.DeliveryEvidence.SubmittedAt != nil {
		t.Fatalf("source JSON round trip: %+v %v", decoded, err)
	}
	for name, mutate := range map[string]func(*Run){
		"unknown_source":       func(run *Run) { run.DeliveryEvidence.SubmittedAtSource = "invented" },
		"missing_boundary":     func(run *Run) { run.DeliveryEvidence.SubmissionStartedAt = nil },
		"missing_confirmation": func(run *Run) { run.DeliveryEvidence.ConfirmedAt = nil },
		"missing_echo":         func(run *Run) { run.DeliveryEvidence.EchoMessageID = "" },
		"missing_request":      func(run *Run) { run.DeliveryEvidence.BridgeRequestID = "" },
		"same_boundary":        func(run *Run) { run.DeliveryEvidence.ConfirmedAt = run.DeliveryEvidence.SubmissionStartedAt },
		"older_echo": func(run *Run) {
			timestamp := run.AcceptedAt.Add(-time.Second)
			run.DeliveryEvidence.ConfirmedAt = &timestamp
		},
		"invented_ack":        func(run *Run) { run.DeliveryEvidence.SubmittedAt = run.DeliveryEvidence.SubmissionStartedAt },
		"ack_without_time":    func(run *Run) { run.DeliveryEvidence.SubmittedAtSource = "bridge_ack" },
		"legacy_without_time": func(run *Run) { run.DeliveryEvidence.SubmittedAtSource = "" },
	} {
		t.Run(name, func(t *testing.T) {
			run := echoInferredRun()
			mutate(&run)
			if ValidateRun(run) == nil {
				t.Fatal("invalid submission provenance accepted")
			}
		})
	}
	for _, source := range []string{"", "bridge_ack"} {
		ack := inferred
		ack.DeliveryEvidence.SubmittedAtSource = source
		ack.DeliveryEvidence.SubmittedAt = ack.DeliveryEvidence.SubmissionStartedAt
		if err := ValidateRun(ack); err != nil {
			t.Fatalf("valid ACK source %q: %v", source, err)
		}
	}
}

func TestSubmissionSourceTransitions(t *testing.T) {
	confirmed := echoInferredRun()
	for _, delivery := range []Delivery{DeliverySubmitting, DeliveryUnknown} {
		previous := confirmed
		previous.DeliveryEvidence = DeliveryEvidence{SubmissionStartedAt: confirmed.DeliveryEvidence.SubmissionStartedAt, BridgeRequestID: "synthetic-request"}
		previous.Delivery = delivery
		previous.Phase, previous.Outcome = PhaseSubmitting, OutcomePending
		if delivery == DeliveryUnknown {
			previous.Phase, previous.Outcome = PhaseUnknown, OutcomeUnknown
		}
		if err := ValidateRun(previous); err != nil {
			t.Fatal(err)
		}
		if err := ValidateTransition(previous, confirmed); err != nil {
			t.Fatalf("lost ACK may be confirmed by echo: %v", err)
		}
	}
	legacy := confirmed
	legacy.DeliveryEvidence.SubmittedAtSource = ""
	legacy.DeliveryEvidence.SubmittedAt = legacy.DeliveryEvidence.SubmissionStartedAt
	ack := legacy
	ack.DeliveryEvidence.SubmittedAtSource = "bridge_ack"
	if err := ValidateTransition(legacy, ack); err != nil {
		t.Fatalf("legacy ACK may be labeled without changing evidence: %v", err)
	}
	if ValidateTransition(ack, legacy) == nil || ValidateTransition(ack, confirmed) == nil || ValidateTransition(legacy, confirmed) == nil {
		t.Fatal("existing ACK or explicit provenance was overwritten")
	}
	if ValidateTransition(confirmed, ack) == nil || ValidateTransition(confirmed, legacy) == nil {
		t.Fatal("inferred provenance was rewritten")
	}
	rewritten := ack
	timestamp := ack.AcceptedAt.Add(time.Nanosecond)
	rewritten.DeliveryEvidence.SubmittedAt = &timestamp
	if ValidateTransition(ack, rewritten) == nil {
		t.Fatal("ACK time was rewritten")
	}
}
