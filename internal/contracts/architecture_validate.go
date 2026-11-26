package contracts

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

var architectureID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,255}$`)

func validID(value string) bool { return architectureID.MatchString(value) }

// ValidID reports whether value satisfies the bounded contract identifier format.
func ValidID(value string) bool    { return validID(value) }
func optionalID(value string) bool { return value == "" || validID(value) }
func validPlatform(value Platform) bool { return ValidPlatform(value) }
func validText(value string, limit int) bool {
	if !utf8.ValidString(value) || utf8.RuneCountInString(value) > limit {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) && character != '\n' && character != '\t' {
			return false
		}
	}
	return true
}
func validReason(value ReasonCode) bool {
	switch value {
	case "", ReasonUnsupported, ReasonUnavailable, ReasonUnverified, ReasonPolicyBlocked, ReasonExpired, ReasonCanceled, ReasonFailed, ReasonUnknown, ReasonUpstreamRedacted:
		return true
	}
	return false
}
func validActionKind(value ActionKind) bool {
	return value == ActionReplyText || value == ActionEditText || value == ActionDeleteContent || value == ActionThreadReply
}
func validPhaseOutcome(phase Phase, outcome Outcome) bool {
	_, known := phaseOrder[phase]
	if !known && phase != PhaseUnknown {
		return false
	}
	switch outcome {
	case OutcomePending:
		return phase != PhaseTerminal && phase != PhaseUnknown
	case OutcomeUnknown:
		return phase == PhaseUnknown
	case OutcomeReplied, OutcomeSilent, OutcomeBlocked, OutcomeSuppressed, OutcomeExpired, OutcomeCanceled, OutcomeFailed:
		return phase == PhaseTerminal
	}
	return false
}

func ValidateSourceRef(value SourceRef) error {
	if !validPlatform(value.Platform) || !validID(value.AccountID) || !validID(value.ContainerID) || !optionalID(value.ContentID) || !optionalID(value.ParentID) || !optionalID(value.RootID) || !validText(value.ContainerLabel, 128) || strings.ContainsAny(value.ContainerLabel, "\n\t") {
		return fmt.Errorf("invalid source reference")
	}
	if value.ParentID != "" && value.ContentID == "" || value.RootID != "" && value.ContentID == "" {
		return fmt.Errorf("thread references require content identity")
	}
	return nil
}

func ValidateInboundEnvelope(value InboundEnvelope) error {
	if !validID(value.EnvelopeID) || !validID(value.DedupKey) || value.ReceivedAt.IsZero() || !validID(value.Author.ID) || !validText(value.Author.Label, 128) || strings.ContainsAny(value.Author.Label, "\n\t") || value.Content.Kind != "text" || strings.TrimSpace(value.Content.Text) == "" || !validText(value.Content.Text, 16384) {
		return fmt.Errorf("invalid inbound identity, author, content or timestamp")
	}
	if err := ValidateSourceRef(value.Source); err != nil {
		return err
	}
	if value.Source.ContentID == "" {
		return fmt.Errorf("inbound content identity required")
	}
	if value.Capabilities.CanReply != (value.ReplyTarget != nil) {
		return fmt.Errorf("reply capability and target disagree")
	}
	if value.Capabilities.CanThreadReply && !value.Capabilities.CanReply {
		return fmt.Errorf("thread reply requires reply capability")
	}
	if value.ReplyTarget != nil {
		if err := ValidateSourceRef(value.ReplyTarget.Source); err != nil {
			return err
		}
		if value.ReplyTarget.Source.Platform != value.Source.Platform || value.ReplyTarget.Source.AccountID != value.Source.AccountID {
			return fmt.Errorf("reply target must use the same source account")
		}
	}
	for key, entry := range value.MetadataAllowlist {
		switch key {
		case MetadataLocale:
			if entry != "zh-CN" && entry != "en" && entry != "und" {
				return fmt.Errorf("invalid locale metadata")
			}
		case MetadataIngress:
			if entry != "realtime" && entry != "replay" {
				return fmt.Errorf("invalid ingress metadata")
			}
		default:
			return fmt.Errorf("metadata key is not allowlisted")
		}
	}
	return nil
}

func validateRefs(refs []string) bool {
	if len(refs) > 64 {
		return false
	}
	seen := map[string]bool{}
	for _, reference := range refs {
		if !validID(reference) || seen[reference] {
			return false
		}
		seen[reference] = true
	}
	return true
}

func ValidateProcessingRequest(value ProcessingRequest) error {
	for _, identity := range []string{value.RunID, value.EnvelopeID, value.AgentID, value.PolicyRevision, value.BackendID, value.ModelID} {
		if !validID(identity) {
			return fmt.Errorf("processing requires bounded allowlist identities")
		}
	}
	if !validateRefs(value.Skills) || !validateRefs(value.Tools) || !validateRefs(value.KnowledgeBases) {
		return fmt.Errorf("invalid or duplicate allowlist references")
	}
	return nil
}

func ValidateActionRequest(value ActionRequest) error {
	if !validID(value.ActionID) || !validID(value.RunID) || !validID(value.IdempotencyKey) || !validActionKind(value.Kind) || value.ExpiresAt.IsZero() || !validText(value.Text, 16384) {
		return fmt.Errorf("invalid action request")
	}
	if err := ValidateSourceRef(value.Target.Source); err != nil {
		return err
	}
	if value.Kind == ActionDeleteContent {
		if value.Text != "" {
			return fmt.Errorf("delete cannot include text")
		}
	} else if strings.TrimSpace(value.Text) == "" {
		return fmt.Errorf("text action requires text")
	}
	if value.Kind != ActionReplyText && value.Target.Source.ContentID == "" {
		return fmt.Errorf("content action requires content identity")
	}
	return nil
}

func ValidateActionReceipt(value ActionReceipt) error {
	if !validID(value.ActionID) || !validPlatform(value.Platform) || !optionalID(value.ExternalID) || !validReason(value.ReasonCode) {
		return fmt.Errorf("invalid action receipt identity or reason")
	}
	switch value.Admission.Status {
	case AdmissionAccepted:
		if !validID(value.Admission.AdmissionID) || value.Admission.RecordedAt == nil || value.Admission.RecordedAt.IsZero() {
			return fmt.Errorf("accepted admission requires durable identity and timestamp")
		}
	case AdmissionRejected, AdmissionUnknown:
		if value.Admission.AdmissionID != "" || value.Admission.RecordedAt != nil || value.ExternalID != "" || value.SubmittedAt != nil || value.ConfirmedAt != nil || value.ReasonCode == "" || value.SubmittedAtSource != "" || value.Confirmation != nil {
			return fmt.Errorf("unaccepted admission cannot claim external evidence")
		}
		if value.Admission.Status == AdmissionRejected && value.Status != ActionNotStarted || value.Admission.Status == AdmissionUnknown && value.Status != ActionUnknown {
			return fmt.Errorf("admission and external status disagree")
		}
		return nil
	default:
		return fmt.Errorf("invalid admission status")
	}
	if value.SubmittedAt != nil && (value.SubmittedAt.IsZero() || value.SubmittedAt.Before(*value.Admission.RecordedAt)) {
		return fmt.Errorf("submission precedes durable admission")
	}
	if value.Confirmation != nil && (value.ConfirmedAt == nil || value.Confirmation.RequestID != value.ActionID || !validID(value.Confirmation.EchoMessageID) || value.Confirmation.EchoMessageID != value.ExternalID) {
		return fmt.Errorf("confirmation requires matching request and echo identities")
	}
	switch value.SubmittedAtSource {
	case "":
	case "bridge_ack":
		if value.SubmittedAt == nil {
			return fmt.Errorf("bridge_ack requires an exact submission timestamp")
		}
	case "echo_inferred":
		if value.Status != ActionConfirmed || value.SubmittedAt != nil || value.ConfirmedAt == nil || !value.ConfirmedAt.After(*value.Admission.RecordedAt) || value.Confirmation == nil {
			return fmt.Errorf("echo_inferred requires correlated confirmation strictly after admission and no ACK timestamp")
		}
	default:
		return fmt.Errorf("invalid submittedAtSource")
	}
	if value.ConfirmedAt != nil && (value.ConfirmedAt.IsZero() || value.ExternalID == "" || (value.SubmittedAtSource != "echo_inferred" && (value.SubmittedAt == nil || value.ConfirmedAt.Before(*value.SubmittedAt)))) {
		return fmt.Errorf("confirmation requires correlated external evidence after submission")
	}
	switch value.Status {
	case ActionNotStarted:
		if value.SubmittedAt != nil || value.ConfirmedAt != nil || value.ExternalID != "" {
			return fmt.Errorf("not started cannot claim submission")
		}
	case ActionSubmitted:
		if value.SubmittedAt == nil || value.ConfirmedAt != nil {
			return fmt.Errorf("submitted requires ACK and no confirmation")
		}
	case ActionConfirmed:
		if value.ConfirmedAt == nil {
			return fmt.Errorf("confirmed requires evidence")
		}
	case ActionFailed:
		if value.SubmittedAt != nil || value.ConfirmedAt != nil || value.ExternalID != "" || value.ReasonCode == "" {
			return fmt.Errorf("failed requires proven non-submission reason")
		}
	case ActionUnknown:
		if value.ConfirmedAt != nil || value.ReasonCode == "" {
			return fmt.Errorf("unknown cannot claim confirmation and requires reason")
		}
	default:
		return fmt.Errorf("invalid external action status")
	}
	return nil
}

func ValidateActionRef(value ActionRef) error {
	if !validID(value.ActionID) || !validActionKind(value.Kind) || !validPlatform(value.Platform) || !optionalID(value.IdempotencyKey) || !optionalID(value.AdmissionID) || value.AdmittedAt != nil && value.AdmittedAt.IsZero() {
		return fmt.Errorf("invalid action reference")
	}
	return nil
}

func ValidateProcessingResult(value ProcessingResult) error {
	if !validID(value.RunID) || len(value.Attempts) > 64 || len(value.Evidence) > 128 {
		return fmt.Errorf("invalid processing result")
	}
	if value.Reply != nil && (value.Reply.Kind != "text" || strings.TrimSpace(value.Reply.Text) == "" || !validText(value.Reply.Text, 16384)) {
		return fmt.Errorf("invalid reply")
	}
	if value.RequestedAction != nil {
		if err := ValidateActionRequest(*value.RequestedAction); err != nil {
			return err
		}
		if value.RequestedAction.RunID != value.RunID {
			return fmt.Errorf("requested action belongs to another run")
		}
	}
	seen := map[string]bool{}
	for _, attempt := range value.Attempts {
		if !validID(attempt.AttemptID) || seen[attempt.AttemptID] || !validID(attempt.BackendID) || !validID(attempt.ModelID) {
			return fmt.Errorf("invalid attempt identity")
		}
		seen[attempt.AttemptID] = true
		if attempt.RemoteExecution != RemoteStopped && attempt.RemoteExecution != RemoteMayBeRunning && attempt.RemoteExecution != RemoteUnknown && attempt.RemoteExecution != RemoteNotApplicable {
			return fmt.Errorf("invalid remote execution")
		}
		if attempt.StartedAt != nil && attempt.StartedAt.IsZero() || attempt.EndedAt != nil && (attempt.StartedAt == nil || attempt.EndedAt.Before(*attempt.StartedAt)) {
			return fmt.Errorf("invalid attempt times")
		}
		switch attempt.Status {
		case AttemptQueued:
			if attempt.StartedAt != nil || attempt.EndedAt != nil {
				return fmt.Errorf("queued attempt has execution times")
			}
		case AttemptRunning, AttemptCancelRequested:
			if attempt.StartedAt == nil || attempt.EndedAt != nil {
				return fmt.Errorf("active attempt requires start only")
			}
		case AttemptSucceeded, AttemptFailed, AttemptTimedOut, AttemptCanceled:
			if attempt.StartedAt == nil || attempt.EndedAt == nil || attempt.Status == AttemptCanceled && attempt.RemoteExecution != RemoteStopped || attempt.Status == AttemptTimedOut && attempt.RemoteExecution == RemoteNotApplicable {
				return fmt.Errorf("invalid finished attempt evidence")
			}
		case AttemptUnknown:
		default:
			return fmt.Errorf("invalid attempt status")
		}
	}
	for _, evidence := range value.Evidence {
		if evidence.Kind != "backend_result" && evidence.Kind != "adapter_receipt" && evidence.Kind != "policy_check" && evidence.Kind != "external_confirmation" {
			return fmt.Errorf("invalid evidence kind; summaries are not evidence")
		}
		if !validID(evidence.ReferenceID) || evidence.OccurredAt.IsZero() || evidence.Certainty != CertaintyObserved && evidence.Certainty != CertaintyUnknown {
			return fmt.Errorf("invalid evidence reference")
		}
		if evidence.Kind == "external_confirmation" && evidence.Certainty != CertaintyObserved {
			return fmt.Errorf("external confirmation requires observed evidence")
		}
	}
	return nil
}

func ValidateArchitectureEvent(value ArchitectureEvent) error {
	if !validID(value.EventID) || value.OccurredAt.IsZero() || !optionalID(value.RunID) || !optionalID(value.EnvelopeID) || !validReason(value.ReasonCode) || value.Certainty != CertaintyObserved && value.Certainty != CertaintyUnknown || !validPhaseOutcome(value.Phase, value.Outcome) {
		return fmt.Errorf("invalid architecture event")
	}
	if value.Source != nil {
		if err := ValidateSourceRef(*value.Source); err != nil {
			return err
		}
	}
	if value.Action != nil {
		if err := ValidateActionRef(*value.Action); err != nil {
			return err
		}
	}
	switch value.Domain {
	case DomainProcessing:
		if value.Kind != KindRunChanged || value.RunID == "" {
			return fmt.Errorf("invalid processing event")
		}
	case DomainSource:
		if value.Kind != KindSourceReceived || value.Source == nil || value.Source.ContentID == "" || value.EnvelopeID == "" || value.Action != nil || value.Phase != PhaseAccepted || value.Outcome != OutcomePending {
			return fmt.Errorf("invalid source event")
		}
	case DomainAction:
		if value.Kind != KindActionChanged || value.Action == nil || value.RunID == "" {
			return fmt.Errorf("invalid action event")
		}
	default:
		return fmt.Errorf("invalid architecture event domain")
	}
	return nil
}

func ValidateCapabilityEvidence(value CapabilityEvidence) error {
	if !validID(value.ID) || !validReason(value.Reason) || !value.Supported && value.Reason == "" {
		return fmt.Errorf("invalid capability identity or reason")
	}
	switch value.Enforcement {
	case EnforcementHard, EnforcementSoft, EnforcementHost, EnforcementUnverified:
	default:
		return fmt.Errorf("invalid enforcement")
	}
	switch value.Source {
	case CapabilitySourceAdapter, CapabilityAgentRuntime, CapabilityActionAdapter, CapabilityDispatcher, CapabilityExecution, CapabilityRelayHealth, CapabilityBridgeHealth, CapabilityConsole:
	default:
		return fmt.Errorf("invalid capability provenance")
	}
	return nil
}

func ValidateCapabilityQuery(value CapabilityQuery) error {
	switch value.Domain {
	case DomainSource, DomainAction:
		if !validPlatform(value.Platform) || value.AgentID != "" {
			return fmt.Errorf("platform capability query requires platform only")
		}
	case DomainProcessing:
		if value.Platform != "" || !validID(value.AgentID) {
			return fmt.Errorf("processing capability query requires agent only")
		}
	default:
		return fmt.Errorf("invalid capability domain")
	}
	return nil
}

func ValidateCapabilitySet(value CapabilitySet) error {
	if !validID(value.Revision) || len(value.Capabilities) > 64 {
		return fmt.Errorf("invalid capability set")
	}
	seen := map[string]bool{}
	for _, capability := range value.Capabilities {
		if err := ValidateCapabilityEvidence(capability); err != nil {
			return err
		}
		if seen[capability.ID] {
			return fmt.Errorf("duplicate capability")
		}
		seen[capability.ID] = true
	}
	return nil
}

func ValidateRuntimeRef(value RuntimeRef) error {
	if !validID(value.AgentID) || !validID(value.BackendID) {
		return fmt.Errorf("invalid runtime reference")
	}
	return nil
}

func validateSourceCursor(cursor string) bool {
	if len(cursor) > 2048 {
		return false
	}
	for _, character := range cursor {
		if character < '!' || character > '~' {
			return false
		}
	}
	return true
}

func ValidateSourceStreamRequest(value SourceStreamRequest) error {
	if err := ValidateSourceRef(value.Source); err != nil {
		return err
	}
	if !validateSourceCursor(value.Cursor) {
		return fmt.Errorf("invalid opaque source cursor")
	}
	return nil
}

func ValidateSourceDelivery(value SourceDelivery) error {
	if value.Cursor == "" || !validateSourceCursor(value.Cursor) {
		return fmt.Errorf("source delivery requires opaque cursor")
	}
	return ValidateInboundEnvelope(value.Envelope)
}

func ValidateProcessingInput(envelope InboundEnvelope, request ProcessingRequest) error {
	if err := ValidateInboundEnvelope(envelope); err != nil {
		return err
	}
	if err := ValidateProcessingRequest(request); err != nil {
		return err
	}
	if envelope.EnvelopeID != request.EnvelopeID {
		return fmt.Errorf("processing request and envelope identity disagree")
	}
	return nil
}
