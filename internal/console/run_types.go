package console

import (
	"regexp"
	"time"

	"jinx/internal/contracts"
)

type GroupRef struct {
	ID         string `json:"id"`
	Label      string `json:"label"`
	Visibility string `json:"visibility"`
}

type Watermark struct {
	Token          string    `json:"token"`
	RecordedAt     time.Time `json:"recordedAt"`
	SourceSequence *string   `json:"sourceSequence"`
}

type AttemptView struct {
	ID                      string                    `json:"id"`
	BackendID               string                    `json:"backendId"`
	ModelLabel              *string                   `json:"modelLabel"`
	Status                  contracts.AttemptStatus   `json:"status"`
	RemoteExecution         contracts.RemoteExecution `json:"remoteExecution"`
	StartedAt               *time.Time                `json:"startedAt"`
	EndedAt                 *time.Time                `json:"endedAt"`
	ExternalID              *string                   `json:"externalId"`
	MissingExternalIDReason *string                   `json:"missingExternalIdReason"`
	FallbackReason          *string                   `json:"fallbackReason"`
	Versions                Versions                  `json:"versions"`
}

type DeliveryEvidence struct {
	SubmissionStartedAt *time.Time `json:"submissionStartedAt"`
	SubmittedAt         *time.Time `json:"submittedAt"`
	SubmittedAtSource   *string    `json:"submittedAtSource"`
	ConfirmedAt         *time.Time `json:"confirmedAt"`
	BridgeRequestID     *string    `json:"bridgeRequestId"`
	EchoMessageID       *string    `json:"echoMessageId"`
	NoSubmission        bool       `json:"noSubmission"`
	FailureEvidence     *string    `json:"failureEvidence"`
}

type SideEffect struct {
	Kind       string     `json:"kind"`
	Status     string     `json:"status"`
	OccurredAt *time.Time `json:"occurredAt"`
	Summary    string     `json:"summary"`
}

type ProcessingReceipt struct {
	Status        contracts.ActionStatus   `json:"status"`
	Evidence      DeliveryEvidence         `json:"evidence"`
	Receipt       *contracts.ActionReceipt `json:"receipt"`
	MissingReason *string                  `json:"missingReason"`
}

type RunView struct {
	ID                    string                       `json:"id"`
	Revision              string                       `json:"revision"`
	EntitySeq             uint64                       `json:"entitySeq"`
	Source                GroupRef                     `json:"source"`
	Target                GroupRef                     `json:"target"`
	AcceptedAt            *time.Time                   `json:"acceptedAt"`
	UpdatedAt             time.Time                    `json:"updatedAt"`
	LastProgressAt        *time.Time                   `json:"lastProgressAt"`
	AdmissionWatermark    *Watermark                   `json:"admissionWatermark"`
	Trigger               string                       `json:"trigger"`
	MessageCount          *int                         `json:"messageCount"`
	MessageIDs            []string                     `json:"messageIds"`
	Phase                 contracts.Phase              `json:"phase"`
	Outcome               contracts.Outcome            `json:"outcome"`
	Delivery              contracts.Delivery           `json:"delivery"`
	ReasonCode            *string                      `json:"reasonCode"`
	Summary               string                       `json:"summary"`
	Completeness          contracts.Completeness       `json:"completeness"`
	Versions              Versions                     `json:"versions"`
	ConfigSnapshotID      *string                      `json:"configSnapshotId"`
	MissingSnapshotReason *string                      `json:"missingSnapshotReason"`
	PlannedBackendID      *string                      `json:"plannedBackendId"`
	Attempts              []AttemptView                `json:"attempts"`
	Excerpt               *string                      `json:"excerpt"`
	SideEffects           []SideEffect                 `json:"sideEffects"`
	DeliveryEvidence      DeliveryEvidence             `json:"deliveryEvidence"`
	Inputs                []contracts.InboundReference `json:"inputs"`
	InputsMissingReason   *string                      `json:"inputsMissingReason"`
	Action                *contracts.ActionRef         `json:"action"`
	Receipt               *ProcessingReceipt           `json:"receipt"`
	Actions               struct {
		BlockDelivery Availability `json:"blockDelivery"`
		StopExecution Availability `json:"stopExecution"`
	} `json:"actions"`
}

type TimelineItem struct {
	ID         string     `json:"id"`
	RunID      string     `json:"runId"`
	OccurredAt *time.Time `json:"occurredAt"`
	Source     string     `json:"source"`
	Kind       string     `json:"kind"`
	ReasonCode *string    `json:"reasonCode"`
	Summary    string     `json:"summary"`
	Certainty  string     `json:"certainty"`
	AttemptID  *string    `json:"attemptId"`
}

type RunDetail struct {
	Run      RunView        `json:"run"`
	Timeline []TimelineItem `json:"timeline"`
}

type RunPage struct {
	Items         []RunView `json:"items"`
	NextCursor    *string   `json:"nextCursor"`
	CoverageSince time.Time `json:"coverageSince"`
	Watermark     string    `json:"watermark"`
}

var publicToken = regexp.MustCompile(`^[a-zA-Z0-9_@.:-]{1,256}$`)
var publicVersion = regexp.MustCompile(`^[a-zA-Z0-9_@.:"-]{1,256}$`)

func safeToken(value string) string {
	if value == "." || value == ".." || !publicToken.MatchString(value) {
		return ""
	}
	return value
}

func nullable(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func factTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func publicSource(source *contracts.SourceRef) *contracts.SourceRef {
	if source == nil {
		return nil
	}
	value := *source
	value.ContainerLabel = ""
	return &value
}

func traceVersions(value contracts.Versions) Versions {
	clean := func(text string) string {
		if publicVersion.MatchString(text) {
			return text
		}
		return ""
	}
	return Versions{Dispatcher: clean(value.Dispatcher), Routing: nullable(clean(value.Routing)), Policy: nullable(clean(value.Policy)), Fingerprint: clean(value.Fingerprint)}
}

func runView(run contracts.Run) RunView {
	view := RunView{
		ID: safeToken(run.ID), Revision: safeToken(run.Revision), EntitySeq: run.EntitySeq,
		Source:     GroupRef{ID: safeToken(run.SourceGroupID), Visibility: "not_observed"},
		Target:     GroupRef{ID: safeToken(run.TargetGroupID), Visibility: "not_observed"},
		AcceptedAt: factTime(run.AcceptedAt), UpdatedAt: run.UpdatedAt, LastProgressAt: factTime(run.LastProgressAt),
		Trigger: safeToken(run.Trigger), MessageIDs: []string{}, Attempts: []AttemptView{}, SideEffects: []SideEffect{},
		Phase: run.Phase, Outcome: run.Outcome, Delivery: run.Delivery, Completeness: run.Completeness,
		ReasonCode: nullable(safeToken(run.ReasonCode)), Versions: traceVersions(run.Versions),
		ConfigSnapshotID: nullable(safeToken(run.ConfigSnapshotID)), PlannedBackendID: nullable(safeToken(run.PlannedBackendID)),
		Action: run.Action, InputsMissingReason: nullable(safeToken(run.InputsMissingReason)),
	}
	if run.Inputs != nil {
		view.Inputs = make([]contracts.InboundReference, 0, len(run.Inputs))
		for _, input := range run.Inputs {
			input.Source = *publicSource(&input.Source)
			view.Inputs = append(view.Inputs, input)
		}
	}
	view.MissingSnapshotReason = nullable("Historical configuration snapshot content is not available from trace")
	if reason := safeToken(run.MissingSnapshotReason); reason != "" {
		view.MissingSnapshotReason = nullable("Historical configuration snapshot content is unavailable: " + reason)
	}
	if run.AdmissionWatermark.Token != "" && !run.AdmissionWatermark.RecordedAt.IsZero() {
		view.AdmissionWatermark = &Watermark{Token: safeToken(run.AdmissionWatermark.Token), RecordedAt: run.AdmissionWatermark.RecordedAt, SourceSequence: nullable(safeToken(run.AdmissionWatermark.SourceSequence))}
	}
	if run.MessageIDs != nil {
		count := len(run.MessageIDs)
		view.MessageCount = &count
		for _, id := range run.MessageIDs {
			if clean := safeToken(id); clean != "" {
				view.MessageIDs = append(view.MessageIDs, clean)
			}
		}
	}
	for _, attempt := range run.Attempts {
		mapped := AttemptView{
			ID: safeToken(attempt.ID), BackendID: safeToken(attempt.BackendID), ModelLabel: nullable(safeToken(attempt.ModelLabel)),
			Status: attempt.Status, RemoteExecution: attempt.RemoteExecution, StartedAt: attempt.StartedAt, EndedAt: attempt.EndedAt,
			ExternalID: nullable(safeToken(attempt.ExternalID)), FallbackReason: nullable(safeToken(attempt.FallbackReason)), Versions: traceVersions(attempt.Versions),
		}
		if mapped.ExternalID == nil {
			mapped.MissingExternalIDReason = nullable(safeToken(attempt.MissingExternalIDReason))
			if mapped.MissingExternalIDReason == nil {
				mapped.MissingExternalIDReason = nullable("External execution identifier was not retained or is not public")
			}
		}
		view.Attempts = append(view.Attempts, mapped)
	}
	evidence := run.DeliveryEvidence
	view.DeliveryEvidence = DeliveryEvidence{
		SubmissionStartedAt: evidence.SubmissionStartedAt, SubmittedAt: evidence.SubmittedAt, ConfirmedAt: evidence.ConfirmedAt,
		SubmittedAtSource: nullable(safeSubmissionSource(evidence.SubmittedAtSource)),
		BridgeRequestID:   nullable(safeToken(evidence.BridgeRequestID)), EchoMessageID: nullable(safeToken(evidence.EchoMessageID)), NoSubmission: evidence.NoSubmission,
	}
	if evidence.FailureEvidence != "" {
		view.DeliveryEvidence.FailureEvidence = nullable("Dispatcher recorded delivery failure evidence; raw details are not exposed")
	}
	if run.Action != nil || evidence != (contracts.DeliveryEvidence{}) {
		status := contracts.ActionNotStarted
		switch run.Delivery {
		case contracts.DeliverySubmitted:
			status = contracts.ActionSubmitted
		case contracts.DeliveryConfirmed:
			status = contracts.ActionConfirmed
		case contracts.DeliverySubmitting, contracts.DeliveryUnknown:
			status = contracts.ActionUnknown
		case contracts.DeliveryFailed:
			status = contracts.ActionFailed
		}
		view.Receipt = &ProcessingReceipt{Status: status, Evidence: view.DeliveryEvidence}
		if run.Action == nil {
			view.Receipt.MissingReason = nullable("action_admission_not_recorded")
		} else {
			receipt := contracts.ActionReceipt{ActionID: run.Action.ActionID, Platform: run.Action.Platform, Status: status,
				Admission:   contracts.ActionAdmission{Status: contracts.AdmissionAccepted, AdmissionID: run.Action.AdmissionID, RecordedAt: run.Action.AdmittedAt},
				SubmittedAt: evidence.SubmittedAt, SubmittedAtSource: evidence.SubmittedAtSource, ConfirmedAt: evidence.ConfirmedAt, ReasonCode: architectureReason(run.ReasonCode)}
			if status == contracts.ActionUnknown {
				receipt.ReasonCode = contracts.ReasonUnknown
			} else if status == contracts.ActionFailed {
				receipt.ReasonCode = contracts.ReasonFailed
			}
			if evidence.ConfirmedAt != nil {
				receipt.ExternalID = evidence.EchoMessageID
				receipt.Confirmation = &contracts.ActionConfirmation{RequestID: evidence.BridgeRequestID, EchoMessageID: evidence.EchoMessageID}
			}
			if contracts.ValidateActionReceipt(receipt) == nil {
				view.Receipt.Receipt = &receipt
			} else {
				view.Receipt.MissingReason = nullable("action_receipt_unavailable")
			}
		}
	}
	view.Summary = runSummary(run.Phase, run.Outcome, run.Delivery)
	view.Actions.BlockDelivery, view.Actions.StopExecution = unsupported(), unsupported()
	return view
}

func runSummary(phase contracts.Phase, outcome contracts.Outcome, delivery contracts.Delivery) string {
	if delivery == contracts.DeliveryConfirmed {
		return "Delivery confirmed by correlated echo"
	}
	if delivery == contracts.DeliverySubmitted {
		return "Submission acknowledged; delivery confirmation pending"
	}
	if delivery == contracts.DeliveryUnknown {
		return "Delivery outcome is unknown"
	}
	if phase == contracts.PhaseTerminal {
		return "Run finished with outcome " + safeToken(string(outcome))
	}
	return "Run phase " + safeToken(string(phase))
}

func safeSubmissionSource(source string) string {
	switch source {
	case "bridge_ack", "echo_inferred":
		return source
	default:
		return ""
	}
}
