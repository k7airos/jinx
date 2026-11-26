package contracts

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func architectureEnvelope() InboundEnvelope {
	return InboundEnvelope{EnvelopeID: "env-1", DedupKey: "dedup-1", Source: SourceRef{Platform: PlatformWeb, AccountID: "account-1", ContainerID: "thread-1", ContentID: "comment-1"}, Author: AuthorRef{ID: "author-1"}, Content: MessageContent{Kind: "text", Text: "hello"}, ReceivedAt: time.Now().UTC(), MetadataAllowlist: map[MetadataKey]string{MetadataLocale: "zh-CN", MetadataIngress: "realtime"}}
}

func TestInboundArchitectureValidation(t *testing.T) {
	if err := ValidateInboundEnvelope(architectureEnvelope()); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*InboundEnvelope){
		"platform":         func(value *InboundEnvelope) { value.Source.Platform = "unknown" },
		"identity":         func(value *InboundEnvelope) { value.EnvelopeID = "/private/secret" },
		"content identity": func(value *InboundEnvelope) { value.Source.ContentID = "" },
		"timestamp":        func(value *InboundEnvelope) { value.ReceivedAt = time.Time{} },
		"content kind":     func(value *InboundEnvelope) { value.Content.Kind = "prompt" },
		"content limit":    func(value *InboundEnvelope) { value.Content.Text = strings.Repeat("字", 16385) },
		"invalid utf8":     func(value *InboundEnvelope) { value.Content.Text = string([]byte{255}) },
		"control":          func(value *InboundEnvelope) { value.Content.Text = "hello\x00" },
		"reply capability": func(value *InboundEnvelope) { value.Capabilities.CanReply = true },
		"cross account": func(value *InboundEnvelope) {
			value.Capabilities.CanReply = true
			target := value.Source
			target.AccountID = "other"
			value.ReplyTarget = &ReplyTarget{Source: target}
		},
		"metadata secret": func(value *InboundEnvelope) { value.MetadataAllowlist["secret"] = "token" },
		"metadata prompt": func(value *InboundEnvelope) { value.MetadataAllowlist[MetadataIngress] = "raw prompt" },
		"metadata locale": func(value *InboundEnvelope) { value.MetadataAllowlist[MetadataLocale] = "secret" },
	} {
		t.Run(name, func(t *testing.T) {
			value := architectureEnvelope()
			mutate(&value)
			if ValidateInboundEnvelope(value) == nil {
				t.Fatal("accepted invalid envelope")
			}
		})
	}
}

func TestProcessingAllowlistAndEvidenceValidation(t *testing.T) {
	request := ProcessingRequest{RunID: "run-1", EnvelopeID: "env-1", AgentID: "agent-1", PolicyRevision: "policy-1", BackendID: "backend-1", ModelID: "model-1", Skills: []string{"skill-1"}}
	if err := ValidateProcessingRequest(request); err != nil {
		t.Fatal(err)
	}
	for _, refs := range [][]string{{"skill-1", "skill-1"}, {"/private/secret"}, {"raw prompt text"}, make([]string, 65)} {
		request.Tools = refs
		if ValidateProcessingRequest(request) == nil {
			t.Fatal("accepted invalid allowlist refs")
		}
	}
	now := time.Now().UTC()
	result := ProcessingResult{RunID: "run-1", Reply: &MessageContent{Kind: "text", Text: "model says it sent a message"}, Attempts: []ProcessingAttempt{{AttemptID: "attempt-1", BackendID: "backend-1", ModelID: "model-1", Status: AttemptTimedOut, RemoteExecution: RemoteMayBeRunning, StartedAt: &now, EndedAt: &now}}, Evidence: []EvidenceRef{{Kind: "backend_result", ReferenceID: "result-1", OccurredAt: now, Certainty: CertaintyObserved}}}
	if err := ValidateProcessingResult(result); err != nil {
		t.Fatal(err)
	}
	result.Evidence[0].Kind = "text_summary"
	if ValidateProcessingResult(result) == nil {
		t.Fatal("summary accepted as evidence")
	}
	result.Evidence = nil
	result.Attempts[0].Status = "invented"
	if ValidateProcessingResult(result) == nil {
		t.Fatal("unknown attempt enum")
	}
	result.Attempts[0].Status, result.Attempts[0].RemoteExecution = AttemptCanceled, RemoteMayBeRunning
	if ValidateProcessingResult(result) == nil {
		t.Fatal("unproven remote cancel")
	}
}

func TestActionAdmissionAndExternalEvidence(t *testing.T) {
	now := time.Now().UTC()
	request := ActionRequest{ActionID: "action-1", RunID: "run-1", Target: ReplyTarget{Source: architectureEnvelope().Source}, Kind: ActionReplyText, Text: "hello", ExpiresAt: now.Add(time.Minute), IdempotencyKey: "key-1"}
	if err := ValidateActionRequest(request); err != nil {
		t.Fatal(err)
	}
	request.Kind = "shell"
	if ValidateActionRequest(request) == nil {
		t.Fatal("unknown action kind")
	}
	request.Kind, request.Text = ActionDeleteContent, "hello"
	if ValidateActionRequest(request) == nil {
		t.Fatal("delete with text")
	}
	receipt := ActionReceipt{ActionID: "action-1", Platform: PlatformWeb, Admission: ActionAdmission{Status: AdmissionAccepted, AdmissionID: "admission-1", RecordedAt: &now}, Status: ActionNotStarted}
	if err := ValidateActionReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ActionReceipt){
		"admission missing":      func(value *ActionReceipt) { value.Admission.AdmissionID = "" },
		"ACK missing":            func(value *ActionReceipt) { value.Status = ActionSubmitted },
		"confirmation missing":   func(value *ActionReceipt) { value.Status = ActionConfirmed },
		"unknown reason missing": func(value *ActionReceipt) { value.Status = ActionUnknown },
		"unaccepted external":    func(value *ActionReceipt) { value.Admission.Status = AdmissionRejected; value.SubmittedAt = &now },
		"reason secret":          func(value *ActionReceipt) { value.ReasonCode = "secret-text" },
		"unknown enum":           func(value *ActionReceipt) { value.Status = "made-up" },
		"time ordering": func(value *ActionReceipt) {
			value.Status = ActionSubmitted
			earlier := now.Add(-time.Second)
			value.SubmittedAt = &earlier
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := receipt
			mutate(&value)
			if ValidateActionReceipt(value) == nil {
				t.Fatal("accepted invalid receipt")
			}
		})
	}
	receipt.Status, receipt.SubmittedAt = ActionSubmitted, &now
	if err := ValidateActionReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Status, receipt.ConfirmedAt, receipt.ExternalID = ActionConfirmed, &now, "external-1"
	if err := ValidateActionReceipt(receipt); err != nil {
		t.Fatal(err)
	}
	receipt.Status, receipt.ConfirmedAt, receipt.ReasonCode = ActionUnknown, nil, ReasonUnknown
	if err := ValidateActionReceipt(receipt); err != nil {
		t.Fatal(err)
	}
}

func TestArchitectureEventValidationAndWireSemantics(t *testing.T) {
	event := ArchitectureEvent{EventID: "event-1", Domain: DomainProcessing, Kind: KindRunChanged, RunID: "run-1", OccurredAt: time.Now().UTC(), Phase: PhaseExecuting, Outcome: OutcomePending, Certainty: CertaintyObserved}
	if err := ValidateArchitectureEvent(event); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ArchitectureEvent){
		"domain":          func(value *ArchitectureEvent) { value.Domain = "stats" },
		"kind":            func(value *ArchitectureEvent) { value.Kind = "summary" },
		"phase":           func(value *ArchitectureEvent) { value.Phase = "made-up" },
		"outcome":         func(value *ArchitectureEvent) { value.Outcome = "made-up" },
		"certainty":       func(value *ArchitectureEvent) { value.Certainty = "inferred" },
		"source evidence": func(value *ArchitectureEvent) { value.Domain, value.Kind = DomainSource, KindSourceReceived },
		"action evidence": func(value *ArchitectureEvent) { value.Domain, value.Kind = DomainAction, KindActionChanged },
	} {
		t.Run(name, func(t *testing.T) {
			value := event
			mutate(&value)
			if ValidateArchitectureEvent(value) == nil {
				t.Fatal("accepted invalid architecture event")
			}
		})
	}
	data, err := json.Marshal(ProcessingResult{RunID: "run-1", Attempts: []ProcessingAttempt{}, Evidence: []EvidenceRef{}})
	if err != nil || !strings.Contains(string(data), `"reply":null`) || !strings.Contains(string(data), `"attempts":[]`) || strings.Contains(string(data), "RunID") {
		t.Fatalf("wire semantics: %s %v", data, err)
	}
	capability := CapabilityEvidence{ID: "send", Source: CapabilityActionAdapter, Enforcement: EnforcementSoft, Reason: ReasonUnsupported}
	if err := ValidateCapabilityEvidence(capability); err != nil {
		t.Fatal(err)
	}
	capability.Enforcement = "magic"
	if ValidateCapabilityEvidence(capability) == nil {
		t.Fatal("unknown enforcement")
	}
}

func TestPortBoundaryValidation(t *testing.T) {
	query := CapabilityQuery{Domain: DomainProcessing, AgentID: "agent-1"}
	if err := ValidateCapabilityQuery(query); err != nil {
		t.Fatal(err)
	}
	query.Platform = PlatformWeChat
	if ValidateCapabilityQuery(query) == nil {
		t.Fatal("mixed capability query")
	}
	if ValidateRuntimeRef(RuntimeRef{AgentID: "agent-1", BackendID: "backend-1"}) != nil || ValidateRuntimeRef(RuntimeRef{}) == nil {
		t.Fatal("invalid runtime reference handling")
	}
	capability := CapabilityEvidence{ID: "reply", Supported: true, Source: CapabilityActionAdapter, Enforcement: EnforcementHost}
	if err := ValidateCapabilitySet(CapabilitySet{Revision: "revision-1", Capabilities: []CapabilityEvidence{capability}}); err != nil {
		t.Fatal(err)
	}
	if ValidateCapabilitySet(CapabilitySet{Revision: "revision-1", Capabilities: []CapabilityEvidence{capability, capability}}) == nil {
		t.Fatal("duplicate capability")
	}
	stream := SourceStreamRequest{Source: architectureEnvelope().Source}
	if err := ValidateSourceStreamRequest(stream); err != nil {
		t.Fatal(err)
	}
	for _, cursor := range []string{"frame\ninjection", strings.Repeat("x", 2049), "has space"} {
		stream.Cursor = cursor
		if ValidateSourceStreamRequest(stream) == nil {
			t.Fatal("invalid source cursor")
		}
	}
	delivery := SourceDelivery{Cursor: "opaque-source-cursor", Envelope: architectureEnvelope()}
	if err := ValidateSourceDelivery(delivery); err != nil {
		t.Fatal(err)
	}
	delivery.Cursor = ""
	if ValidateSourceDelivery(delivery) == nil {
		t.Fatal("missing source delivery position")
	}
	request := ProcessingRequest{RunID: "run-1", EnvelopeID: "env-1", AgentID: "agent-1", PolicyRevision: "policy-1", BackendID: "backend-1", ModelID: "model-1"}
	if err := ValidateProcessingInput(architectureEnvelope(), request); err != nil {
		t.Fatal(err)
	}
	request.EnvelopeID = "other"
	if ValidateProcessingInput(architectureEnvelope(), request) == nil {
		t.Fatal("unrelated processing envelope")
	}
}
