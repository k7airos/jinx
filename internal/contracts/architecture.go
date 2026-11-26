package contracts

import "time"

type Platform string
type EventDomain string
type ActionKind string
type Certainty string
type Enforcement string
type CapabilitySource string
type ReasonCode string
type ArchitectureKind string

const (
	PlatformWeChat          Platform         = "wechat"
	PlatformBilibili        Platform         = "bilibili"
	PlatformWeb             Platform         = "web"
	DomainSource            EventDomain      = "source"
	DomainProcessing        EventDomain      = "processing"
	DomainAction            EventDomain      = "action"
	ActionReplyText         ActionKind       = "reply_text"
	ActionEditText          ActionKind       = "edit_text"
	ActionDeleteContent     ActionKind       = "delete_content"
	ActionThreadReply       ActionKind       = "thread_reply"
	CertaintyObserved       Certainty        = "observed"
	CertaintyUnknown        Certainty        = "unknown"
	EnforcementHard         Enforcement      = "hard"
	EnforcementSoft         Enforcement      = "soft"
	EnforcementHost         Enforcement      = "host"
	EnforcementUnverified   Enforcement      = "unverified"
	CapabilitySourceAdapter CapabilitySource = "source_adapter"
	CapabilityAgentRuntime  CapabilitySource = "agent_runtime"
	CapabilityActionAdapter CapabilitySource = "action_adapter"
	CapabilityDispatcher    CapabilitySource = "dispatcher_trace"
	CapabilityExecution     CapabilitySource = "execution_catalog"
	CapabilityRelayHealth   CapabilitySource = "relay_health"
	CapabilityBridgeHealth  CapabilitySource = "bridge_health"
	CapabilityConsole       CapabilitySource = "console"
	ReasonUnsupported       ReasonCode       = "unsupported"
	ReasonUnavailable       ReasonCode       = "unavailable"
	ReasonUnverified        ReasonCode       = "unverified"
	ReasonPolicyBlocked     ReasonCode       = "policy_blocked"
	ReasonExpired           ReasonCode       = "expired"
	ReasonCanceled          ReasonCode       = "canceled"
	ReasonFailed            ReasonCode       = "failed"
	ReasonUnknown           ReasonCode       = "unknown"
	ReasonUpstreamRedacted  ReasonCode       = "upstream_reason_redacted"
	KindRunChanged          ArchitectureKind = "run_changed"
	KindSourceReceived      ArchitectureKind = "source_received"
	KindActionChanged       ArchitectureKind = "action_changed"
)

type SourceRef struct {
	Platform       Platform `json:"platform"`
	AccountID      string   `json:"accountId"`
	ContainerID    string   `json:"containerId"`
	ContainerLabel string   `json:"containerLabel"`
	ContentID      string   `json:"contentId"`
	ParentID       string   `json:"parentId"`
	RootID         string   `json:"rootId"`
}

type SourceCapabilities struct {
	CanReply       bool `json:"canReply"`
	CanEdit        bool `json:"canEdit"`
	CanDelete      bool `json:"canDelete"`
	CanThreadReply bool `json:"canThreadReply"`
	CanAttach      bool `json:"canAttach"`
}

type AuthorRef struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type MessageContent struct {
	Kind string `json:"kind"`
	Text string `json:"text"`
}

type ReplyTarget struct {
	Source SourceRef `json:"source"`
}

type MetadataKey string

const (
	MetadataLocale  MetadataKey = "locale"
	MetadataIngress MetadataKey = "ingress"
)

type InboundEnvelope struct {
	EnvelopeID        string                 `json:"envelopeId"`
	DedupKey          string                 `json:"dedupKey"`
	Source            SourceRef              `json:"source"`
	Author            AuthorRef              `json:"author"`
	Content           MessageContent         `json:"content"`
	ReceivedAt        time.Time              `json:"receivedAt"`
	ReplyTarget       *ReplyTarget           `json:"replyTarget"`
	Capabilities      SourceCapabilities     `json:"capabilities"`
	MetadataAllowlist map[MetadataKey]string `json:"metadataAllowlist"`
}

type InboundReference struct {
	EnvelopeID string     `json:"envelopeId"`
	DedupKey   string     `json:"dedupKey"`
	Source     SourceRef  `json:"source"`
	ReceivedAt *time.Time `json:"receivedAt,omitempty"`
}

type ProcessingRequest struct {
	RunID          string   `json:"runId"`
	EnvelopeID     string   `json:"envelopeId"`
	AgentID        string   `json:"agentId"`
	PolicyRevision string   `json:"policyRevision"`
	BackendID      string   `json:"backendId"`
	ModelID        string   `json:"modelId"`
	Skills         []string `json:"skills"`
	Tools          []string `json:"tools"`
	KnowledgeBases []string `json:"knowledgeBases"`
}

type ProcessingAttempt struct {
	AttemptID       string          `json:"attemptId"`
	BackendID       string          `json:"backendId"`
	ModelID         string          `json:"modelId"`
	Status          AttemptStatus   `json:"status"`
	RemoteExecution RemoteExecution `json:"remoteExecution"`
	StartedAt       *time.Time      `json:"startedAt"`
	EndedAt         *time.Time      `json:"endedAt"`
}

type EvidenceRef struct {
	Kind        string    `json:"kind"`
	ReferenceID string    `json:"referenceId"`
	OccurredAt  time.Time `json:"occurredAt"`
	Certainty   Certainty `json:"certainty"`
}

type ProcessingResult struct {
	RunID           string              `json:"runId"`
	Reply           *MessageContent     `json:"reply"`
	RequestedAction *ActionRequest      `json:"requestedAction"`
	Attempts        []ProcessingAttempt `json:"attempts"`
	Evidence        []EvidenceRef       `json:"evidence"`
}

type ActionRequest struct {
	ActionID       string      `json:"actionId"`
	RunID          string      `json:"runId"`
	Target         ReplyTarget `json:"target"`
	Kind           ActionKind  `json:"kind"`
	Text           string      `json:"text"`
	ExpiresAt      time.Time   `json:"expiresAt"`
	IdempotencyKey string      `json:"idempotencyKey"`
}

type AdmissionStatus string
type ActionStatus string

const (
	AdmissionAccepted AdmissionStatus = "accepted"
	AdmissionRejected AdmissionStatus = "rejected"
	AdmissionUnknown  AdmissionStatus = "unknown"
	ActionNotStarted  ActionStatus    = "not_started"
	ActionSubmitted   ActionStatus    = "submitted"
	ActionConfirmed   ActionStatus    = "confirmed"
	ActionFailed      ActionStatus    = "failed"
	ActionUnknown     ActionStatus    = "unknown"
)

type ActionAdmission struct {
	Status      AdmissionStatus `json:"status"`
	AdmissionID string          `json:"admissionId"`
	RecordedAt  *time.Time      `json:"recordedAt"`
}

type ActionReceipt struct {
	ActionID          string              `json:"actionId"`
	Admission         ActionAdmission     `json:"admission"`
	Status            ActionStatus        `json:"status"`
	ExternalID        string              `json:"externalId"`
	SubmittedAt       *time.Time          `json:"submittedAt"`
	ConfirmedAt       *time.Time          `json:"confirmedAt"`
	ReasonCode        ReasonCode          `json:"reasonCode"`
	Platform          Platform            `json:"platform"`
	SubmittedAtSource string              `json:"submittedAtSource,omitempty"`
	Confirmation      *ActionConfirmation `json:"confirmation,omitempty"`
}

type ActionConfirmation struct {
	RequestID     string `json:"requestId"`
	EchoMessageID string `json:"echoMessageId"`
}

type ActionRef struct {
	ActionID       string     `json:"actionId"`
	Kind           ActionKind `json:"kind"`
	Platform       Platform   `json:"platform"`
	IdempotencyKey string     `json:"idempotencyKey,omitempty"`
	AdmissionID    string     `json:"admissionId,omitempty"`
	AdmittedAt     *time.Time `json:"admittedAt,omitempty"`
}

type ArchitectureEvent struct {
	EventID    string           `json:"eventId"`
	Domain     EventDomain      `json:"domain"`
	Kind       ArchitectureKind `json:"kind"`
	OccurredAt time.Time        `json:"occurredAt"`
	RunID      string           `json:"runId"`
	EnvelopeID string           `json:"envelopeId"`
	Source     *SourceRef       `json:"source"`
	Action     *ActionRef       `json:"action"`
	Phase      Phase            `json:"phase"`
	Outcome    Outcome          `json:"outcome"`
	Certainty  Certainty        `json:"certainty"`
	ReasonCode ReasonCode       `json:"reasonCode"`
}

type CapabilityEvidence struct {
	ID          string           `json:"id"`
	Supported   bool             `json:"supported"`
	Enforcement Enforcement      `json:"enforcement"`
	Source      CapabilitySource `json:"source"`
	Reason      ReasonCode       `json:"reason"`
}

type CapabilityQuery struct {
	Domain   EventDomain `json:"domain"`
	Platform Platform    `json:"platform"`
	AgentID  string      `json:"agentId"`
}

type CapabilitySet struct {
	Revision     string               `json:"revision"`
	Capabilities []CapabilityEvidence `json:"capabilities"`
}

type SourceStreamRequest struct {
	Source SourceRef `json:"source"`
	Cursor string    `json:"cursor"`
}

type SourceDelivery struct {
	Cursor   string          `json:"cursor"`
	Envelope InboundEnvelope `json:"envelope"`
}

type RuntimeRef struct {
	AgentID   string `json:"agentId"`
	BackendID string `json:"backendId"`
}
