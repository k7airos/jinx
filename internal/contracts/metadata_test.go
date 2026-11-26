package contracts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestActionReceiptEchoInferenceRequiresCorrelatedEvidence(t *testing.T) {
	admittedAt := time.Now().UTC()
	confirmedAt := admittedAt.Add(time.Millisecond)
	receipt := ActionReceipt{
		ActionID: "action-1", Platform: PlatformWeChat, Status: ActionConfirmed,
		Admission:         ActionAdmission{Status: AdmissionAccepted, AdmissionID: "admission-1", RecordedAt: &admittedAt},
		SubmittedAtSource: "echo_inferred", ConfirmedAt: &confirmedAt, ExternalID: "echo-1",
		Confirmation: &ActionConfirmation{RequestID: "action-1", EchoMessageID: "echo-1"},
	}
	if err := ValidateActionReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*ActionReceipt){
		"invented_ack":         func(value *ActionReceipt) { value.SubmittedAt = &admittedAt },
		"same_boundary":        func(value *ActionReceipt) { value.ConfirmedAt = &admittedAt },
		"before_boundary":      func(value *ActionReceipt) { before := admittedAt.Add(-time.Millisecond); value.ConfirmedAt = &before },
		"missing_confirmation": func(value *ActionReceipt) { value.Confirmation = nil },
		"wrong_request":        func(value *ActionReceipt) { value.Confirmation.RequestID = "other-action" },
		"wrong_echo":           func(value *ActionReceipt) { value.Confirmation.EchoMessageID = "other-echo" },
		"missing_admission":    func(value *ActionReceipt) { value.Admission.RecordedAt = nil },
		"unaccepted": func(value *ActionReceipt) {
			value.Admission.Status = AdmissionUnknown
			value.ReasonCode = ReasonUnknown
		},
		"default_still_requires_ack":    func(value *ActionReceipt) { value.SubmittedAtSource = "" },
		"bridge_ack_still_requires_ack": func(value *ActionReceipt) { value.SubmittedAtSource = "bridge_ack" },
		"invalid_source":                func(value *ActionReceipt) { value.SubmittedAtSource = "model_claim" },
	} {
		t.Run(name, func(t *testing.T) {
			value := receipt
			confirmation := *receipt.Confirmation
			value.Confirmation = &confirmation
			change(&value)
			if ValidateActionReceipt(value) == nil {
				t.Fatal("invalid inferred receipt accepted", value)
			}
		})
	}
}

func TestRunOptionalMetadataAndLegacyJSON(t *testing.T) {
	run := stateRun(PhaseAccepted, OutcomePending, DeliveryNotStarted)
	data, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"inputs"`, `"action"`, `"inputsMissingReason"`} {
		if strings.Contains(string(data), field) {
			t.Fatal("empty metadata changed old JSON", string(data))
		}
	}
	receivedAt := run.AcceptedAt.Add(-time.Second)
	run.Inputs = []InboundReference{{EnvelopeID: "envelope-1", DedupKey: "dedup-1", Source: SourceRef{Platform: PlatformWeChat, AccountID: "wechat-relay", ContainerID: run.SourceGroupID, ContentID: run.MessageIDs[0]}, ReceivedAt: &receivedAt}}
	if err := ValidateRun(run); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*Run){
		"wrong_container":  func(value *Run) { value.Inputs[0].Source.ContainerID = "other-group" },
		"unknown_platform": func(value *Run) { value.Inputs[0].Source.Platform = "Unknown" },
		"unknown_content":  func(value *Run) { value.Inputs[0].Source.ContentID = "other-message" },
		"missing_envelope": func(value *Run) { value.Inputs[0].EnvelopeID = "" },
		"unsafe_dedup":     func(value *Run) { value.Inputs[0].DedupKey = "/private/db" },
		"future_receipt":   func(value *Run) { after := value.AcceptedAt.Add(time.Second); value.Inputs[0].ReceivedAt = &after },
		"duplicate":        func(value *Run) { value.Inputs = append(value.Inputs, value.Inputs[0]) },
		"invented_action": func(value *Run) {
			value.Action = &ActionRef{ActionID: "action-1", Kind: ActionReplyText, Platform: PlatformWeChat}
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := run
			value.Inputs = append([]InboundReference(nil), run.Inputs...)
			change(&value)
			if ValidateRun(value) == nil {
				t.Fatal("invalid metadata accepted", value)
			}
		})
	}
}
