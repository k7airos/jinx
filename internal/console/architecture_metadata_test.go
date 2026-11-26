package console

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"jinx/internal/contracts"
)

func TestArchitectureEventUsesOnlyPersistedReferences(t *testing.T) {
	run := testRun("metadata", 1)
	run.Inputs = []contracts.InboundReference{{EnvelopeID: "envelope-1", DedupKey: "dedup-1", ReceivedAt: &run.AcceptedAt,
		Source: contracts.SourceRef{Platform: contracts.PlatformWeChat, AccountID: "wechat-relay", ContainerID: run.SourceGroupID, ContainerLabel: "/private/SECRET", ContentID: run.MessageIDs[0]}}}
	started := run.AcceptedAt.Add(time.Second)
	run.Phase, run.Delivery = contracts.PhaseSubmitting, contracts.DeliverySubmitting
	run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &started, BridgeRequestID: "action-1"}
	run.Action = &contracts.ActionRef{ActionID: "action-1", Kind: contracts.ActionReplyText, Platform: contracts.PlatformWeChat, IdempotencyKey: "dedup-action-1", AdmissionID: run.ID, AdmittedAt: &started}
	facade, stub := traceSetup(t, run)
	stub.mode = "reset"
	response := consoleRequest(facade, "architecture/events?cursor="+testCursor("stream-1", 0))
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"source":null,"action":null`) || !strings.Contains(response.Body.String(), `"envelopeId":""`) {
		t.Fatalf("latest run filled historical event: %d %s", response.Code, response.Body)
	}
	for _, path := range stub.paths {
		if path != "/api/trace/events" {
			t.Fatalf("SSE read latest run metadata: %s", path)
		}
	}
	event := testEvent(run)
	event.EnvelopeID, event.Source, event.Action = run.Inputs[0].EnvelopeID, &run.Inputs[0].Source, run.Action
	value, err := architectureEvent(event)
	if err != nil || value.EnvelopeID != "envelope-1" || value.Source == nil || value.Source.ContentID != run.MessageIDs[0] || value.Action == nil || value.Action.ActionID != "action-1" || value.Domain != contracts.DomainProcessing {
		t.Fatalf("persisted association missing: %+v %v", value, err)
	}
	if value.Source.ContainerLabel != "" || event.Source.ContainerLabel != "/private/SECRET" {
		t.Fatal("public projection leaked or mutated source label")
	}
	for name, mutate := range map[string]func(*contracts.Event){
		"source":   func(value *contracts.Event) { value.Source = &contracts.SourceRef{Platform: "invalid"} },
		"envelope": func(value *contracts.Event) { value.EnvelopeID = "/private/SECRET" },
		"action": func(value *contracts.Event) {
			value.Action = &contracts.ActionRef{ActionID: "action-1", Kind: "invalid", Platform: contracts.PlatformWeChat}
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := event
			mutate(&invalid)
			if _, err := architectureEvent(invalid); err == nil || err.(*Problem).Code != "reset_required" {
				t.Fatalf("invalid metadata did not reset: %v", err)
			}
			stub.events[0] = invalid
			response := consoleRequest(facade, "architecture/events?cursor="+testCursor("stream-1", 0))
			if response.Code != 409 || strings.Contains(response.Body.String(), "event: architecture") {
				t.Fatalf("invalid metadata advanced the stream: %d %s", response.Code, response.Body)
			}
			public := publicEvent(invalid)
			if public.Source != nil || public.EnvelopeID != "" || public.Action != nil || public.Cursor != invalid.Cursor {
				t.Fatalf("legacy SSE leaked invalid metadata or changed cursor: %+v", public)
			}
		})
	}
}

func TestRunMetadataAndProcessingReceiptProjection(t *testing.T) {
	legacy := testRun("legacy", 1)
	view := runView(legacy)
	if view.Inputs != nil || view.Action != nil || view.Receipt != nil {
		t.Fatalf("invented legacy associations: %+v", view)
	}
	data, err := json.Marshal(view)
	if err != nil || !strings.Contains(string(data), `"inputs":null,"inputsMissingReason":null,"action":null,"receipt":null`) {
		t.Fatalf("missing nullable legacy fields: %s %v", data, err)
	}
	started, confirmed := legacy.AcceptedAt.Add(time.Second), legacy.UpdatedAt
	for _, scenario := range []struct {
		name     string
		phase    contracts.Phase
		outcome  contracts.Outcome
		delivery contracts.Delivery
		expected contracts.ActionStatus
	}{
		{"not_started", contracts.PhaseChecking, contracts.OutcomePending, contracts.DeliveryNotStarted, contracts.ActionNotStarted},
		{"submitting", contracts.PhaseSubmitting, contracts.OutcomePending, contracts.DeliverySubmitting, contracts.ActionUnknown},
		{"submitted", contracts.PhaseConfirming, contracts.OutcomePending, contracts.DeliverySubmitted, contracts.ActionSubmitted},
		{"confirmed", contracts.PhaseTerminal, contracts.OutcomeReplied, contracts.DeliveryConfirmed, contracts.ActionConfirmed},
		{"failed", contracts.PhaseTerminal, contracts.OutcomeFailed, contracts.DeliveryFailed, contracts.ActionFailed},
		{"unknown", contracts.PhaseUnknown, contracts.OutcomeUnknown, contracts.DeliveryUnknown, contracts.ActionUnknown},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			run := legacy
			run.Phase, run.Outcome, run.Delivery = scenario.phase, scenario.outcome, scenario.delivery
			run.ReasonCode = "test_evidence"
			run.Action = &contracts.ActionRef{ActionID: "bridge-1", Kind: contracts.ActionReplyText, Platform: contracts.PlatformWeChat, IdempotencyKey: "action-dedup-1", AdmissionID: run.ID, AdmittedAt: &started}
			run.Inputs = []contracts.InboundReference{{EnvelopeID: "envelope-1", DedupKey: "dedup-1", ReceivedAt: &run.AcceptedAt,
				Source: contracts.SourceRef{Platform: contracts.PlatformWeChat, AccountID: "wechat-relay", ContainerID: run.SourceGroupID, ContainerLabel: "/private/SECRET", ContentID: run.MessageIDs[0]}}}
			run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &started, BridgeRequestID: "bridge-1"}
			if scenario.delivery == contracts.DeliveryNotStarted {
				run.Action = nil
				run.DeliveryEvidence = contracts.DeliveryEvidence{NoSubmission: true}
			}
			if scenario.delivery == contracts.DeliverySubmitted {
				run.DeliveryEvidence.SubmittedAt, run.DeliveryEvidence.SubmittedAtSource = &confirmed, "bridge_ack"
			}
			if scenario.delivery == contracts.DeliveryConfirmed {
				run.DeliveryEvidence.SubmittedAtSource, run.DeliveryEvidence.ConfirmedAt, run.DeliveryEvidence.EchoMessageID = "echo_inferred", &confirmed, "echo-1"
			}
			if scenario.delivery == contracts.DeliveryFailed {
				run.DeliveryEvidence.FailureEvidence = "/private/SECRET reply body"
			}
			if err := contracts.ValidateRun(run); err != nil {
				t.Fatalf("invalid receipt fixture: %v", err)
			}
			view := runView(run)
			if len(view.Inputs) != 1 || view.Inputs[0].Source.ContainerLabel != "" || view.Inputs[0].DedupKey != "dedup-1" || view.Receipt == nil || view.Receipt.Status != scenario.expected {
				t.Fatalf("metadata projection mismatch: %+v", view)
			}
			if run.Action == nil {
				if view.Action != nil || view.Receipt.Receipt != nil || view.Receipt.MissingReason == nil || *view.Receipt.MissingReason != "action_admission_not_recorded" {
					t.Fatal("invented missing admission")
				}
			} else if view.Action == nil || view.Receipt.Receipt == nil || contracts.ValidateActionReceipt(*view.Receipt.Receipt) != nil || view.Receipt.Receipt.Admission.AdmissionID != run.ID {
				t.Fatalf("persisted admission missing: %+v", view.Receipt)
			}
			if scenario.delivery == contracts.DeliverySubmitted {
				if view.Receipt.Evidence.SubmittedAt == nil || !view.Receipt.Evidence.SubmittedAt.Equal(confirmed) || view.Receipt.Evidence.SubmittedAtSource == nil || *view.Receipt.Evidence.SubmittedAtSource != "bridge_ack" {
					t.Fatal("lost exact ACK evidence")
				}
			} else if view.Receipt.Evidence.SubmittedAt != nil || scenario.delivery == contracts.DeliveryConfirmed && (view.Receipt.Evidence.SubmittedAtSource == nil || *view.Receipt.Evidence.SubmittedAtSource != "echo_inferred") {
				t.Fatal("invented ACK or lost confirmation provenance")
			}
			data, err := json.Marshal(view)
			if err != nil || strings.Contains(string(data), "SECRET") || strings.Contains(string(data), "reply body") {
				t.Fatalf("private content or fabricated admission: %s %v", data, err)
			}
		})
	}
}
