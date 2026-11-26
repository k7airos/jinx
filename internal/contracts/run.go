package contracts

import "time"

type Phase string
type Outcome string
type Delivery string
type AttemptStatus string
type RemoteExecution string
type Completeness string

const (
	PhaseAccepted          Phase           = "accepted"
	PhaseAggregating       Phase           = "aggregating"
	PhaseGating            Phase           = "gating"
	PhaseQueued            Phase           = "queued"
	PhaseExecuting         Phase           = "executing"
	PhaseChecking          Phase           = "checking"
	PhaseSubmitting        Phase           = "submitting"
	PhaseConfirming        Phase           = "confirming"
	PhaseTerminal          Phase           = "terminal"
	PhaseUnknown           Phase           = "unknown"
	OutcomePending         Outcome         = "pending"
	OutcomeReplied         Outcome         = "replied"
	OutcomeSilent          Outcome         = "silent"
	OutcomeBlocked         Outcome         = "blocked"
	OutcomeSuppressed      Outcome         = "suppressed"
	OutcomeExpired         Outcome         = "expired"
	OutcomeCanceled        Outcome         = "canceled"
	OutcomeFailed          Outcome         = "failed"
	OutcomeUnknown         Outcome         = "unknown"
	DeliveryNotStarted     Delivery        = "not_started"
	DeliveryBlocked        Delivery        = "blocked"
	DeliverySubmitting     Delivery        = "submitting"
	DeliverySubmitted      Delivery        = "submitted"
	DeliveryConfirmed      Delivery        = "confirmed"
	DeliveryFailed         Delivery        = "failed"
	DeliveryUnknown        Delivery        = "unknown"
	AttemptQueued          AttemptStatus   = "queued"
	AttemptRunning         AttemptStatus   = "running"
	AttemptSucceeded       AttemptStatus   = "succeeded"
	AttemptFailed          AttemptStatus   = "failed"
	AttemptTimedOut        AttemptStatus   = "timed_out"
	AttemptCancelRequested AttemptStatus   = "cancel_requested"
	AttemptCanceled        AttemptStatus   = "canceled"
	AttemptUnknown         AttemptStatus   = "unknown"
	RemoteStopped          RemoteExecution = "stopped"
	RemoteMayBeRunning     RemoteExecution = "may_be_running"
	RemoteUnknown          RemoteExecution = "unknown"
	RemoteNotApplicable    RemoteExecution = "not_applicable"
	Complete               Completeness    = "complete"
	Partial                Completeness    = "partial"
	LegacyPartial          Completeness    = "legacy_partial"
)

type Versions struct {
	Dispatcher  string `json:"dispatcher"`
	Routing     string `json:"routing"`
	Policy      string `json:"policy"`
	Fingerprint string `json:"fingerprint"`
}

type Watermark struct {
	Token          string    `json:"token"`
	RecordedAt     time.Time `json:"recordedAt"`
	SourceSequence string    `json:"sourceSequence"`
}

type Attempt struct {
	ID                      string          `json:"id"`
	BackendID               string          `json:"backendId"`
	ModelLabel              string          `json:"modelLabel"`
	Status                  AttemptStatus   `json:"status"`
	RemoteExecution         RemoteExecution `json:"remoteExecution"`
	StartedAt               *time.Time      `json:"startedAt"`
	EndedAt                 *time.Time      `json:"endedAt"`
	ExternalID              string          `json:"externalId"`
	MissingExternalIDReason string          `json:"missingExternalIdReason"`
	FallbackReason          string          `json:"fallbackReason"`
	Versions                Versions        `json:"versions"`
}

type DeliveryEvidence struct {
	SubmissionStartedAt *time.Time `json:"submissionStartedAt"`
	SubmittedAt         *time.Time `json:"submittedAt"`
	SubmittedAtSource   string     `json:"submittedAtSource"`
	ConfirmedAt         *time.Time `json:"confirmedAt"`
	BridgeRequestID     string     `json:"bridgeRequestId"`
	EchoMessageID       string     `json:"echoMessageId"`
	NoSubmission        bool       `json:"noSubmission"`
	FailureEvidence     string     `json:"failureEvidence"`
}

type Run struct {
	ID                    string             `json:"id"`
	Revision              string             `json:"revision"`
	EntitySeq             uint64             `json:"entitySeq"`
	SourceGroupID         string             `json:"sourceGroupId"`
	TargetGroupID         string             `json:"targetGroupId"`
	AcceptedAt            time.Time          `json:"acceptedAt"`
	UpdatedAt             time.Time          `json:"updatedAt"`
	LastProgressAt        time.Time          `json:"lastProgressAt"`
	AdmissionWatermark    Watermark          `json:"admissionWatermark"`
	Trigger               string             `json:"trigger"`
	MessageIDs            []string           `json:"messageIds"`
	Inputs                []InboundReference `json:"inputs,omitempty"`
	InputsMissingReason   string             `json:"inputsMissingReason,omitempty"`
	Action                *ActionRef         `json:"action,omitempty"`
	Phase                 Phase              `json:"phase"`
	Outcome               Outcome            `json:"outcome"`
	Delivery              Delivery           `json:"delivery"`
	ReasonCode            string             `json:"reasonCode"`
	Completeness          Completeness       `json:"completeness"`
	Versions              Versions           `json:"versions"`
	ConfigSnapshotID      string             `json:"configSnapshotId"`
	MissingSnapshotReason string             `json:"missingSnapshotReason"`
	PlannedBackendID      string             `json:"plannedBackendId"`
	Attempts              []Attempt          `json:"attempts"`
	DeliveryEvidence      DeliveryEvidence   `json:"deliveryEvidence"`
}

type Event struct {
	EnvelopeID string     `json:"envelopeId,omitempty"`
	Source     *SourceRef `json:"source,omitempty"`
	Action     *ActionRef `json:"action,omitempty"`
	StreamID   string     `json:"streamId"`
	Seq        uint64     `json:"seq"`
	Cursor     string     `json:"cursor"`
	OccurredAt time.Time  `json:"occurredAt"`
	Entity     string     `json:"entity"`
	EntityID   string     `json:"entityId"`
	Revision   string     `json:"revision"`
	Kind       string     `json:"kind"`
	ReasonCode string     `json:"reasonCode"`
	Phase      Phase      `json:"phase"`
	Outcome    Outcome    `json:"outcome"`
	Delivery   Delivery   `json:"delivery"`
}
