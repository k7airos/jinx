// Package client implements loopback HTTP/SSE communication and synthetic mock
// sandboxes for the JINX desktop console.
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const SchemaVersion = "console-rules-v1"

// TriMode distinguishes unset/omitted, explicit JSON null (inherit), explicit
// empty array [] (override empty), and non-empty list values.
type TriMode int

const (
	TriUnset TriMode = iota
	TriNull
	TriEmpty
	TriValues
)

// TriStateStringList preserves null vs [] vs [...] losslessly across JSON
// round-trips so UI edits never collapse [] to null or null to [].
type TriStateStringList struct {
	Mode   TriMode
	Values []string
}

func TriStringsNull() TriStateStringList  { return TriStateStringList{Mode: TriNull} }
func TriStringsEmpty() TriStateStringList { return TriStateStringList{Mode: TriEmpty} }
func TriStringsValues(vals ...string) TriStateStringList {
	if len(vals) == 0 {
		return TriStringsEmpty()
	}
	cp := make([]string, len(vals))
	copy(cp, vals)
	return TriStateStringList{Mode: TriValues, Values: cp}
}

func (t TriStateStringList) MarshalJSON() ([]byte, error) {
	switch t.Mode {
	case TriUnset, TriNull:
		return []byte("null"), nil
	case TriEmpty:
		return []byte("[]"), nil
	case TriValues:
		if len(t.Values) == 0 {
			return []byte("[]"), nil
		}
		return json.Marshal(t.Values)
	default:
		return []byte("null"), nil
	}
}

func (t *TriStateStringList) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*t = TriStringsNull()
		return nil
	}
	var vals []string
	if err := json.Unmarshal(trimmed, &vals); err != nil {
		return err
	}
	if len(vals) == 0 {
		*t = TriStringsEmpty()
		return nil
	}
	*t = TriStringsValues(vals...)
	return nil
}

// DecodeTriStateStringList converts an untyped FieldValue value (nil or []any or
// []string) into a lossless TriStateStringList.
func DecodeTriStateStringList(raw any) TriStateStringList {
	if raw == nil {
		return TriStringsNull()
	}
	switch v := raw.(type) {
	case TriStateStringList:
		return v
	case []string:
		if len(v) == 0 {
			return TriStringsEmpty()
		}
		return TriStringsValues(v...)
	case []any:
		if len(v) == 0 {
			return TriStringsEmpty()
		}
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, fmt.Sprint(item))
		}
		return TriStringsValues(out...)
	default:
		return TriStringsNull()
	}
}

// Label renders a compact display summary without blurring null and [].
func (t TriStateStringList) Label() string {
	switch t.Mode {
	case TriUnset, TriNull:
		return "Inherit (null)"
	case TriEmpty:
		return "Explicit Empty ([])"
	case TriValues:
		return strings.Join(t.Values, ", ")
	default:
		return "Inherit (null)"
	}
}

// TriStateIntList preserves null vs [] vs []int across JSON round-trips.
type TriStateIntList struct {
	Mode   TriMode
	Values []int
}

func TriIntsNull() TriStateIntList  { return TriStateIntList{Mode: TriNull} }
func TriIntsEmpty() TriStateIntList { return TriStateIntList{Mode: TriEmpty} }
func TriIntsValues(vals ...int) TriStateIntList {
	if len(vals) == 0 {
		return TriIntsEmpty()
	}
	cp := make([]int, len(vals))
	copy(cp, vals)
	return TriStateIntList{Mode: TriValues, Values: cp}
}

func (t TriStateIntList) MarshalJSON() ([]byte, error) {
	switch t.Mode {
	case TriUnset, TriNull:
		return []byte("null"), nil
	case TriEmpty:
		return []byte("[]"), nil
	case TriValues:
		if len(t.Values) == 0 {
			return []byte("[]"), nil
		}
		return json.Marshal(t.Values)
	default:
		return []byte("null"), nil
	}
}

func (t *TriStateIntList) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*t = TriIntsNull()
		return nil
	}
	var vals []int
	if err := json.Unmarshal(trimmed, &vals); err != nil {
		return err
	}
	if len(vals) == 0 {
		*t = TriIntsEmpty()
		return nil
	}
	*t = TriIntsValues(vals...)
	return nil
}

// DecodeTriStateIntList converts an untyped FieldValue value into TriStateIntList.
func DecodeTriStateIntList(raw any) TriStateIntList {
	if raw == nil {
		return TriIntsNull()
	}
	switch v := raw.(type) {
	case TriStateIntList:
		return v
	case []int:
		if len(v) == 0 {
			return TriIntsEmpty()
		}
		return TriIntsValues(v...)
	case []any:
		if len(v) == 0 {
			return TriIntsEmpty()
		}
		out := make([]int, 0, len(v))
		for _, item := range v {
			switch n := item.(type) {
			case float64:
				out = append(out, int(n))
			case int:
				out = append(out, n)
			}
		}
		return TriIntsValues(out...)
	default:
		return TriIntsNull()
	}
}

type Problem struct {
	Code            string `json:"code"`
	Message         string `json:"message"`
	RequestID       string `json:"requestId,omitempty"`
	Retry           string `json:"retry,omitempty"`
	CurrentRevision string `json:"currentRevision,omitempty"`
}

func (p *Problem) Error() string {
	if p == nil {
		return ""
	}
	if p.Code == "" {
		return p.Message
	}
	return fmt.Sprintf("%s: %s", p.Code, p.Message)
}

type Availability struct {
	Supported bool   `json:"supported"`
	Enabled   bool   `json:"enabled"`
	Reason    string `json:"reason"`
}

type Versions struct {
	Dispatcher  string  `json:"dispatcher"`
	Relay       string  `json:"relay,omitempty"`
	Routing     *string `json:"routing"`
	Policy      *string `json:"policy"`
	Fingerprint string  `json:"fingerprint"`
}

type Envelope[T any] struct {
	Data          T         `json:"data"`
	ObservedAt    time.Time `json:"observedAt"`
	Source        string    `json:"source"`
	Freshness     string    `json:"freshness"`
	Completeness  string    `json:"completeness"`
	Revision      string    `json:"revision"`
	StreamID      string    `json:"streamId,omitempty"`
	EntitySeq     *uint64   `json:"entitySeq,omitempty"`
	MissingReason *string   `json:"missingReason,omitempty"`
}

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
	ID                      string     `json:"id"`
	BackendID               string     `json:"backendId"`
	ModelLabel              *string    `json:"modelLabel"`
	Status                  string     `json:"status"`
	RemoteExecution         string     `json:"remoteExecution"`
	StartedAt               *time.Time `json:"startedAt"`
	EndedAt                 *time.Time `json:"endedAt"`
	ExternalID              *string    `json:"externalId"`
	MissingExternalIDReason *string    `json:"missingExternalIdReason"`
	FallbackReason          *string    `json:"fallbackReason"`
	Versions                Versions   `json:"versions"`
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

type SourceRef struct {
	Platform       string `json:"platform"`
	AccountID      string `json:"accountId"`
	ContainerID    string `json:"containerId"`
	ContainerLabel string `json:"containerLabel,omitempty"`
	ContentID      string `json:"contentId"`
	ParentID       string `json:"parentId"`
	RootID         string `json:"rootId"`
}

type InboundReference struct {
	EnvelopeID string     `json:"envelopeId"`
	DedupKey   string     `json:"dedupKey"`
	Source     SourceRef  `json:"source"`
	ReceivedAt *time.Time `json:"receivedAt,omitempty"`
}

type ActionRef struct {
	ActionID       string     `json:"actionId"`
	Kind           string     `json:"kind"`
	Platform       string     `json:"platform"`
	IdempotencyKey string     `json:"idempotencyKey,omitempty"`
	AdmissionID    string     `json:"admissionId,omitempty"`
	AdmittedAt     *time.Time `json:"admittedAt,omitempty"`
}

type ActionAdmission struct {
	Status      string     `json:"status"`
	AdmissionID string     `json:"admissionId"`
	RecordedAt  *time.Time `json:"recordedAt"`
}

type ActionConfirmation struct {
	RequestID     string `json:"requestId"`
	EchoMessageID string `json:"echoMessageId"`
}

type ActionReceipt struct {
	ActionID          string              `json:"actionId"`
	Admission         ActionAdmission     `json:"admission"`
	Status            string              `json:"status"`
	ExternalID        string              `json:"externalId"`
	SubmittedAt       *time.Time          `json:"submittedAt"`
	ConfirmedAt       *time.Time          `json:"confirmedAt"`
	ReasonCode        string              `json:"reasonCode"`
	Platform          string              `json:"platform"`
	SubmittedAtSource string              `json:"submittedAtSource,omitempty"`
	Confirmation      *ActionConfirmation `json:"confirmation,omitempty"`
}

type ProcessingReceipt struct {
	Status        string           `json:"status"`
	Evidence      DeliveryEvidence `json:"evidence"`
	Receipt       *ActionReceipt   `json:"receipt"`
	MissingReason *string          `json:"missingReason"`
}

type RunActions struct {
	BlockDelivery Availability `json:"blockDelivery"`
	StopExecution Availability `json:"stopExecution"`
}

type RunView struct {
	ID                    string             `json:"id"`
	Revision              string             `json:"revision"`
	EntitySeq             uint64             `json:"entitySeq"`
	Source                GroupRef           `json:"source"`
	Target                GroupRef           `json:"target"`
	AcceptedAt            *time.Time         `json:"acceptedAt"`
	UpdatedAt             time.Time          `json:"updatedAt"`
	LastProgressAt        *time.Time         `json:"lastProgressAt"`
	AdmissionWatermark    *Watermark         `json:"admissionWatermark"`
	Trigger               string             `json:"trigger"`
	MessageCount          *int               `json:"messageCount"`
	MessageIDs            []string           `json:"messageIds"`
	Phase                 string             `json:"phase"`
	Outcome               string             `json:"outcome"`
	Delivery              string             `json:"delivery"`
	ReasonCode            *string            `json:"reasonCode"`
	Summary               string             `json:"summary"`
	Completeness          string             `json:"completeness"`
	Versions              Versions           `json:"versions"`
	ConfigSnapshotID      *string            `json:"configSnapshotId"`
	MissingSnapshotReason *string            `json:"missingSnapshotReason"`
	PlannedBackendID      *string            `json:"plannedBackendId"`
	Attempts              []AttemptView      `json:"attempts"`
	Excerpt               *string            `json:"excerpt"`
	SideEffects           []SideEffect       `json:"sideEffects"`
	DeliveryEvidence      DeliveryEvidence   `json:"deliveryEvidence"`
	Inputs                []InboundReference `json:"inputs,omitempty"`
	InputsMissingReason   *string            `json:"inputsMissingReason,omitempty"`
	Action                *ActionRef         `json:"action,omitempty"`
	Receipt               *ProcessingReceipt `json:"receipt,omitempty"`
	Actions               RunActions         `json:"actions"`
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

type RunFilter struct {
	GroupID      string
	Phases       []string
	Outcomes     []string
	Deliveries   []string
	BackendID    string
	Completeness string
	Query        string
	Limit        int
	Cursor       string
}

type RunEvent struct {
	StreamID   string     `json:"streamId"`
	Seq        uint64     `json:"seq"`
	Cursor     string     `json:"cursor"`
	OccurredAt time.Time  `json:"occurredAt"`
	Entity     string     `json:"entity"`
	EntityID   string     `json:"entityId"`
	Revision   string     `json:"revision"`
	Kind       string     `json:"kind"`
	ReasonCode string     `json:"reasonCode"`
	Phase      string     `json:"phase"`
	Outcome    string     `json:"outcome"`
	Delivery   string     `json:"delivery"`
	EnvelopeID string     `json:"envelopeId,omitempty"`
	Source     *SourceRef `json:"source,omitempty"`
	Action     *ActionRef `json:"action,omitempty"`
}

type TraceState struct {
	Availability
	Domain        string     `json:"domain"`
	StreamID      *string    `json:"streamId"`
	Cursor        *string    `json:"cursor"`
	LastSeq       *uint64    `json:"lastSeq"`
	CoverageSince *time.Time `json:"coverageSince"`
}

type StageCounts struct {
	Aggregating     *int `json:"aggregating"`
	Queued          *int `json:"queued"`
	Executing       *int `json:"executing"`
	Submitting      *int `json:"submitting"`
	Confirming      *int `json:"confirming"`
	UnknownDelivery *int `json:"unknownDelivery"`
	RemoteUnsettled *int `json:"remoteUnsettled"`
}

type AttentionTarget struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

type AttentionItem struct {
	ID           string           `json:"id"`
	Revision     string           `json:"revision"`
	Code         string           `json:"code"`
	Summary      string           `json:"summary"`
	OccurredAt   time.Time        `json:"occurredAt"`
	Acknowledged bool             `json:"acknowledged"`
	Target       *AttentionTarget `json:"target"`
}

type AcknowledgeInput struct {
	RequestID        string `json:"requestId"`
	ExpectedRevision string `json:"expectedRevision"`
	AttentionID      string `json:"attentionId"`
}

type ServiceView struct {
	ID               string                  `json:"id"`
	Process          string                  `json:"process"`
	Health           string                  `json:"health"`
	HealthLatencyMS  int64                   `json:"healthLatencyMs"`
	HealthObservedAt time.Time               `json:"healthObservedAt"`
	Facts            map[string]any          `json:"facts"`
	Actions          map[string]Availability `json:"actions"`
	StartedAt        *time.Time              `json:"startedAt"`
	LastProgressAt   *time.Time              `json:"lastProgressAt"`
}

type OverviewStats struct {
	TotalReceived   *float64 `json:"total_received"`
	TotalTriggered  *float64 `json:"total_triggered"`
	TotalSubmitted  *float64 `json:"total_submitted"`
	TotalReplied    *float64 `json:"total_replied"`
	TotalDryRun     *float64 `json:"total_dry_run"`
	TotalSendFailed *float64 `json:"total_send_failed"`
	TotalUnknown    *float64 `json:"total_unknown"`
	ReplayGaps      *float64 `json:"replay_gaps"`
	PendingMessages *float64 `json:"pending_messages"`
	RelayConnected  *bool    `json:"relay_connected"`
}

type Overview struct {
	Stats          OverviewStats      `json:"stats"`
	StatsSource    string             `json:"statsSource"`
	StatsAvailable bool               `json:"statsAvailable"`
	PendingMeaning string             `json:"pendingMeaning"`
	Services       []ServiceView      `json:"services"`
	Controls       Availability       `json:"controls"`
	RecentRuns     []RunView          `json:"recentRuns"`
	Trace          TraceState         `json:"trace"`
	CurrentWork    StageCounts        `json:"currentWork"`
	WorkSource     string             `json:"workSource"`
	LegacyCounters map[string]*float64 `json:"legacyCounters"`
	RelayConnected *bool              `json:"relayConnected"`
	Attention      []AttentionItem    `json:"attention"`
}

type SchemaEntry struct {
	Type      string  `json:"type"`
	Nullable  bool    `json:"nullable"`
	Minimum   float64 `json:"minimum"`
	Maximum   float64 `json:"maximum"`
	MaxRunes  int     `json:"maxRunes"`
	Editable  bool    `json:"editable"`
	Inherited bool    `json:"inherited"`
	Default   any     `json:"default"`
}

type FormSchemas struct {
	SchemaVersion string                 `json:"schemaVersion"`
	Group         map[string]SchemaEntry `json:"group"`
	Global        map[string]SchemaEntry `json:"global"`
}

type Bootstrap struct {
	StreamID         *string                 `json:"streamId"`
	Cursor           *string                 `json:"cursor"`
	LastSeq          *uint64                 `json:"lastSeq"`
	Overview         Envelope[Overview]      `json:"overview"`
	ActiveOperations []Operation             `json:"activeOperations"`
	Capabilities     map[string]Availability `json:"capabilities"`
	Schemas          FormSchemas             `json:"schemas"`
	EventStream      TraceState              `json:"eventStream"`
}

type ArchitectureCapability struct {
	ID          string `json:"id"`
	Supported   bool   `json:"supported"`
	Enforcement string `json:"enforcement"`
	Source      string `json:"source"`
	Reason      string `json:"reason"`
}

type SourceView struct {
	Platform     string                   `json:"platform"`
	State        string                   `json:"state"`
	Availability Availability             `json:"availability"`
	AccountID    *string                  `json:"accountId"`
	ContainerIDs []string                 `json:"containerIds"`
	Capabilities []ArchitectureCapability `json:"capabilities"`
}

type ProcessingBackend struct {
	ID           string                   `json:"id"`
	Kind         string                   `json:"kind"`
	Available    bool                     `json:"available"`
	ModelIDs     []string                 `json:"modelIds"`
	AgentBinding string                   `json:"agentBinding"`
	Capabilities []ArchitectureCapability `json:"capabilities"`
}

type ProcessingAgent struct {
	ID string `json:"id"`
}

type ProcessingView struct {
	Agents       []ProcessingAgent        `json:"agents"`
	Backends     []ProcessingBackend      `json:"backends"`
	Capabilities []ArchitectureCapability `json:"capabilities"`
}

type ActionView struct {
	Platform     string                   `json:"platform"`
	Kind         string                   `json:"kind"`
	State        string                   `json:"state"`
	Availability Availability             `json:"availability"`
	Capabilities []ArchitectureCapability `json:"capabilities"`
}

type ArchitectureStream struct {
	Domain string `json:"domain"`
	ArchitectureCapability
	StreamID      *string    `json:"streamId"`
	Cursor        *string    `json:"cursor"`
	LastSeq       *uint64    `json:"lastSeq"`
	CoverageSince *time.Time `json:"coverageSince"`
}

type ArchitectureSnapshot struct {
	Revision     string               `json:"revision"`
	ObservedAt   time.Time            `json:"observedAt"`
	Layers       []string             `json:"layers"`
	Sources      []SourceView         `json:"sources"`
	Processing   ProcessingView       `json:"processing"`
	Actions      []ActionView         `json:"actions"`
	EventStreams []ArchitectureStream `json:"eventStreams"`
}

type ArchitectureEvent struct {
	EventID    string     `json:"eventId"`
	Domain     string     `json:"domain"`
	Kind       string     `json:"kind"`
	OccurredAt time.Time  `json:"occurredAt"`
	RunID      string     `json:"runId"`
	EnvelopeID string     `json:"envelopeId"`
	Source     *SourceRef `json:"source"`
	Action     *ActionRef `json:"action"`
	Phase      string     `json:"phase"`
	Outcome    string     `json:"outcome"`
	Certainty  string     `json:"certainty"`
	ReasonCode string     `json:"reasonCode"`
}

type ConnectorTargetView struct {
	ContainerID string  `json:"containerId"`
	Label       *string `json:"label"`
	Enabled     bool    `json:"enabled"`
}

type ConnectorView struct {
	Platform          string                `json:"platform"`
	Configured        bool                  `json:"configured"`
	State             string                `json:"state"`
	Availability      Availability          `json:"availability"`
	DryRun            *bool                 `json:"dryRun"`
	Capabilities      []string              `json:"capabilities"`
	Controls          []string              `json:"controls"`
	AccountID         *string               `json:"accountId"`
	ConnectorName     *string               `json:"connectorName"`
	Targets           []ConnectorTargetView `json:"targets"`
	LastPollAt        *time.Time            `json:"lastPollAt"`
	LastPollErrorCode *string               `json:"lastPollErrorCode"`
	Reason            *string               `json:"reason"`
	ProtocolVersion   *int                  `json:"protocolVersion"`
	ObservedAt        time.Time             `json:"observedAt"`
}

type ConnectorDryRunPlanInput struct {
	RequestID      string `json:"requestId"`
	ExpectedDryRun bool   `json:"expectedDryRun"`
	TargetDryRun   bool   `json:"targetDryRun"`
}

type Warning struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type ConnectorDryRunPlan struct {
	ID            string    `json:"id"`
	Token         string    `json:"token"`
	Platform      string    `json:"platform"`
	CurrentDryRun bool      `json:"currentDryRun"`
	TargetDryRun  bool      `json:"targetDryRun"`
	Persisted     bool      `json:"persisted"`
	ExpiresAt     time.Time `json:"expiresAt"`
	Warnings      []Warning `json:"warnings"`
}

type ConnectorDryRunApplyInput struct {
	RequestID string `json:"requestId"`
	PlanID    string `json:"planId"`
	Token     string `json:"token"`
	Confirm   bool   `json:"confirm"`
}

type ConnectorDryRunApplyResult struct {
	OK               bool      `json:"ok"`
	Platform         string    `json:"platform"`
	PreviousDryRun   bool      `json:"previousDryRun"`
	DryRun           bool      `json:"dryRun"`
	Persisted        bool      `json:"persisted"`
	ReadbackVerified bool      `json:"readbackVerified"`
	Status           string    `json:"status"`
	RequestID        string    `json:"requestId"`
	ObservedAt       time.Time `json:"observedAt"`
}

type CatalogScenario struct {
	ID             string `json:"id"`
	Builtin        bool   `json:"builtin"`
	HasLocalPolicy bool   `json:"hasLocalPolicy"`
}

type CatalogKnowledgeBase struct {
	ID string `json:"id"`
}

type CatalogTool struct {
	ID             string `json:"id"`
	SupportAllowed bool   `json:"supportAllowed"`
}

type CatalogSkill struct {
	ID string `json:"id"`
}

type CatalogView struct {
	Scenarios      []CatalogScenario      `json:"scenarios"`
	KnowledgeBases []CatalogKnowledgeBase `json:"knowledgeBases"`
	Tools          []CatalogTool          `json:"tools"`
	Skills         []CatalogSkill         `json:"skills"`
}

type FieldValue struct {
	Configured any    `json:"configured"`
	Effective  any    `json:"effective"`
	Origin     string `json:"origin"`
	Editable   bool   `json:"editable"`
	Reason     string `json:"reason"`
}

type GroupView struct {
	ID                   string                `json:"id"`
	Group                GroupRef              `json:"group"`
	LabelSource          string                `json:"labelSource"`
	UITargetLabel        string                `json:"uiTargetLabel"`
	Routing              any                   `json:"routing"`
	MissingRoutingReason string                `json:"missingRoutingReason"`
	Permissions          []any                 `json:"permissions"`
	BackendID            *string               `json:"backendId"`
	LatestConfirmedAt    *time.Time            `json:"latestConfirmedAt"`
	AttentionCodes       []string              `json:"attentionCodes"`
	Revision             string                `json:"revision"`
	Versions             Versions              `json:"versions"`
	RouteExists          bool                  `json:"routeExists"`
	Enabled              bool                  `json:"enabled"`
	Whitelisted          bool                  `json:"whitelisted"`
	EffectiveDryRun      bool                  `json:"effectiveDryRun"`
	Visibility           string                `json:"visibility"`
	Fields               map[string]FieldValue `json:"fields"`
}

type GroupPage struct {
	Items         []GroupView `json:"items"`
	NextCursor    *string     `json:"nextCursor"`
	CoverageSince *time.Time  `json:"coverageSince"`
}

type CompatibilityBlock struct {
	GroupWhitelist  []string            `json:"groupWhitelist"`
	GroupProfiles   map[string]string   `json:"groupProfiles"`
	GroupOtherBots  map[string][]string `json:"groupOtherBots"`
	DefaultProfile  *string             `json:"defaultProfile"`
	Editable        bool                `json:"editable"`
	Semantics       map[string]string   `json:"semantics"`
}

type GlobalRulesView struct {
	Fields        map[string]FieldValue `json:"fields"`
	Versions      Versions              `json:"versions"`
	Compatibility CompatibilityBlock    `json:"compatibility"`
}

type RelayGroupControl struct {
	MonitorAll              bool               `json:"monitor_all"`
	MonitoredGroups         TriStateStringList `json:"monitored_groups"`
	ExposedGroups           TriStateStringList `json:"exposed_groups"`
	GroupAliases            map[string]string  `json:"group_aliases"`
	DropUnexposedCompletely bool               `json:"drop_unexposed_completely"`
}

type RelayFilters struct {
	KeywordBlocklist TriStateStringList `json:"keyword_blocklist"`
	RegexBlocklist   TriStateStringList `json:"regex_blocklist"`
	SenderBlocklist  TriStateStringList `json:"sender_blocklist"`
	AllowedTypes     TriStateIntList    `json:"allowed_types"`
	CleanContent     bool               `json:"clean_content"`
	IgnoreEcho       bool               `json:"ignore_echo"`
}

type RelayRules struct {
	GroupControl RelayGroupControl `json:"group_control"`
	Filters      RelayFilters      `json:"filters"`
}

type RelayRulesView struct {
	Rules            RelayRules `json:"rules"`
	Versions         Versions   `json:"versions"`
	WriteConcurrency string     `json:"writeConcurrency"`
	Warnings         []Warning  `json:"warnings"`
}

type Scope struct {
	Kind    string `json:"kind"`
	GroupID string `json:"groupId"`
}

type Edit struct {
	Kind          string          `json:"kind"`
	Scope         Scope           `json:"scope"`
	Field         string          `json:"field"`
	Value         json.RawMessage `json:"value"`
	GroupID       string          `json:"groupId"`
	Enabled       bool            `json:"enabled"`
	TargetGroupID string          `json:"targetGroupId"`
	TargetLabel   string          `json:"targetLabel"`
}

type PlanInput struct {
	RequestID     string   `json:"requestId"`
	Base          Versions `json:"base"`
	SchemaVersion string   `json:"schemaVersion"`
	Edits         []Edit   `json:"edits"`
}

type FieldChange struct {
	Component       string `json:"component"`
	Path            string `json:"path"`
	Before          any    `json:"before"`
	After           any    `json:"after"`
	EffectiveBefore any    `json:"effectiveBefore"`
	EffectiveAfter  any    `json:"effectiveAfter"`
}

type ChangePlan struct {
	ID             string        `json:"id"`
	Token          string        `json:"token"`
	ExpiresAt      time.Time     `json:"expiresAt"`
	Base           Versions      `json:"base"`
	Changes        []FieldChange `json:"changes"`
	AffectedGroups []string      `json:"affectedGroups"`
	Effect         []string      `json:"effect"`
	Warnings       []Warning     `json:"warnings"`
	ComponentOrder []string      `json:"componentOrder"`
}

type PublishInput struct {
	RequestID string `json:"requestId"`
	PlanID    string `json:"planId"`
	Token     string `json:"token"`
}

type ComponentResult struct {
	ID                  string  `json:"id"`
	State               string  `json:"state"`
	ReadbackVersion     *string `json:"readbackVersion"`
	PersistenceVerified *bool   `json:"persistenceVerified"`
}

type Operation struct {
	ID             string            `json:"id"`
	RequestID      string            `json:"requestId"`
	Revision       string            `json:"revision"`
	ChangeID       string            `json:"changeId"`
	Status         string            `json:"status"`
	Summary        string            `json:"summary"`
	ResultRevision *string           `json:"resultRevision"`
	Components     []ComponentResult `json:"components"`
	MaintenanceID  *string           `json:"maintenanceId"`
	RunID          *string           `json:"runId"`
	CancelResult   any               `json:"cancelResult"`
}

type ConfigurationSnapshot struct {
	CapturedAt time.Time      `json:"capturedAt"`
	Versions   Versions       `json:"versions"`
	Source     string         `json:"source"`
	Scope      []string       `json:"scope"`
	Dispatcher map[string]any `json:"dispatcher"`
	Relay      *RelayRules    `json:"relay"`
}

type ChangeRecord struct {
	ID               string                 `json:"id"`
	CreatedAt        time.Time              `json:"createdAt"`
	Origin           string                 `json:"origin"`
	Base             Versions               `json:"base"`
	Readback         *Versions              `json:"readback"`
	Target           *Versions              `json:"target"`
	BeforeSnapshot   *ConfigurationSnapshot `json:"beforeSnapshot"`
	TargetSnapshot   *ConfigurationSnapshot `json:"targetSnapshot"`
	ReadbackSnapshot *ConfigurationSnapshot `json:"readbackSnapshot"`
	RollbackOf       *string                `json:"rollbackOf"`
	Changes          []FieldChange          `json:"changes"`
	Operation        Operation              `json:"operation"`
}

type ChangePage struct {
	Items      []ChangeRecord `json:"items"`
	NextCursor *string        `json:"nextCursor"`
}

type ChangeFilter struct {
	Origin    string
	Status    string
	Component string
	GroupID   string
	Query     string
	Limit     int
	Cursor    string
}

type RollbackInput struct {
	RequestID string   `json:"requestId"`
	ChangeID  string   `json:"changeId"`
	Base      Versions `json:"base"`
}

type ConfigSnapshot struct {
	ID           string                `json:"id"`
	Versions     Versions              `json:"versions"`
	CapturedAt   time.Time             `json:"capturedAt"`
	Group        GroupRef              `json:"group"`
	Fields       map[string]FieldValue `json:"fields"`
	GlobalFields map[string]FieldValue `json:"globalFields"`
	Routing      any                   `json:"routing"`
	Permissions  []any                 `json:"permissions"`
	Source       string                `json:"source"`
	Scope        []string              `json:"scope"`
}

type ConfigurationDrift struct {
	Component     string  `json:"component"`
	Available     bool    `json:"available"`
	Reason        string  `json:"reason"`
	Drift         *bool   `json:"drift"`
	MemoryVersion *string `json:"memoryVersion"`
	DiskVersion   *string `json:"diskVersion"`
	Memory        any     `json:"memory"`
	Disk          any     `json:"disk"`
}

type Permission struct {
	Capability    string   `json:"capability"`
	Requested     *bool    `json:"requested"`
	Effective     *bool    `json:"effective"`
	Enforcement   string   `json:"enforcement"`
	Provenance    string   `json:"provenance"`
	PolicyVersion string   `json:"policyVersion"`
	EnforcedAt    []string `json:"enforcedAt"`
	Editable      bool     `json:"editable"`
	Reason        string   `json:"reason"`
}

type ModelRef struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type Backend struct {
	ID                 string       `json:"id"`
	Kind               string       `json:"kind"`
	Models             []ModelRef   `json:"models"`
	Available          bool         `json:"available"`
	Reason             string       `json:"reason"`
	AgentBinding       string       `json:"agentBinding"`
	SessionModes       []string     `json:"sessionModes"`
	MinTimeoutSec      int          `json:"minTimeoutSec"`
	MaxTimeoutSec      int          `json:"maxTimeoutSec"`
	PermissionEvidence []Permission `json:"permissionEvidence"`
}

type SessionPolicy struct {
	Mode               string `json:"mode"`
	RotateAfterTurns   int    `json:"rotateAfterTurns"`
	RotateAfterSeconds int    `json:"rotateAfterSeconds"`
}

type RouteAttempt struct {
	BackendID  string        `json:"backendId"`
	ModelID    string        `json:"modelId"`
	TimeoutSec int           `json:"timeoutSec"`
	Session    SessionPolicy `json:"session"`
}

type Routing struct {
	RouteID   string         `json:"routeId"`
	PolicyID  string         `json:"policyId"`
	Primary   RouteAttempt   `json:"primary"`
	Fallbacks []RouteAttempt `json:"fallbacks"`
}

type Policy struct {
	Capabilities map[string]bool `json:"capabilities"`
	NoTools      bool            `json:"noTools"`
	AllowSoft    bool            `json:"allowSoft"`
}

type ExecutionConfig struct {
	SchemaVersion string             `json:"schemaVersion"`
	Groups        map[string]Routing `json:"groups"`
	Profiles      map[string]string  `json:"profiles"`
	Policies      map[string]Policy  `json:"policies"`
}

type ExecutionSnapshot struct {
	Revision string          `json:"revision"`
	Config   ExecutionConfig `json:"config"`
}

type ExecutionPlanInput struct {
	ExpectedRevision string          `json:"expectedRevision"`
	Config           ExecutionConfig `json:"config"`
}

type ExecutionApplyInput struct {
	ExpectedRevision string          `json:"expectedRevision"`
	RequestID        string          `json:"requestId"`
	Config           ExecutionConfig `json:"config"`
}

type ExecutionPlan struct {
	ExpectedRevision string   `json:"expectedRevision"`
	ConfigDigest     string   `json:"configDigest"`
	AffectedGroups   []string `json:"affectedGroups"`
	Effects          []string `json:"effects"`
	Warnings         []string `json:"warnings"`
}

type ExecutionApplyResult struct {
	Snapshot        ExecutionSnapshot `json:"snapshot"`
	AppliedRevision string            `json:"appliedRevision"`
	Replayed        bool              `json:"replayed"`
}

type GroupRouting struct {
	GroupID  string  `json:"groupId"`
	Revision string  `json:"revision"`
	Routing  Routing `json:"routing"`
}

type GroupPermissions struct {
	GroupID     string       `json:"groupId"`
	Revision    string       `json:"revision"`
	Policy      Policy       `json:"policy"`
	Permissions []Permission `json:"permissions"`
}

type ControlScope struct {
	Kind    string `json:"kind"`
	GroupID string `json:"groupId,omitempty"`
}

type ScopeControl struct {
	Paused    bool      `json:"paused"`
	Revision  string    `json:"revision"`
	Boundary  uint64    `json:"boundary"`
	ChangedAt time.Time `json:"changedAt"`
}

type ControlCounts struct {
	Pending         int            `json:"pending"`
	Phases          map[string]int `json:"phases"`
	ActiveWorkers   int            `json:"activeWorkers"`
	UnknownDelivery int            `json:"unknownDelivery"`
	RemoteUnsettled int            `json:"remoteUnsettled"`
	UnresolvedRuns  int            `json:"unresolvedRuns"`
}

type Maintenance struct {
	MaintenanceID    string        `json:"maintenanceId"`
	Revision         string        `json:"revision"`
	Scope            ControlScope  `json:"scope"`
	Boundary         uint64        `json:"boundary"`
	StartedAt        time.Time     `json:"startedAt"`
	Deadline         time.Time     `json:"deadline"`
	Active           bool          `json:"active"`
	RecoveryRequired bool          `json:"recoveryRequired"`
	Stage            string        `json:"stage"`
	Counts           ControlCounts `json:"counts"`
	Blockers         []string      `json:"blockers"`
}

type ControlSnapshot struct {
	Revision    string                  `json:"revision"`
	Global      ScopeControl            `json:"global"`
	Groups      map[string]ScopeControl `json:"groups"`
	Maintenance *Maintenance            `json:"maintenance,omitempty"`
}

type ControlAction struct {
	Kind          string       `json:"kind"`
	Scope         ControlScope `json:"scope"`
	MaintenanceID string       `json:"maintenanceId,omitempty"`
	RunID         string       `json:"runId,omitempty"`
	CancelMode    string       `json:"cancelMode,omitempty"`
}

type ControlRequest struct {
	RequestID        string        `json:"requestId"`
	ExpectedRevision string        `json:"expectedRevision"`
	Action           ControlAction `json:"action"`
}

type ControlOperation struct {
	OperationID     string        `json:"operationId"`
	RequestID       string        `json:"requestId"`
	Revision        string        `json:"revision"`
	Status          string        `json:"status"`
	Control         *ScopeControl `json:"control,omitempty"`
	MaintenanceID   string        `json:"maintenanceId,omitempty"`
	RunID           string        `json:"runId,omitempty"`
	RunRevision     string        `json:"runRevision,omitempty"`
	RemoteExecution string        `json:"remoteExecution,omitempty"`
	Fingerprint     string        `json:"fingerprint"`
	Action          ControlAction `json:"action"`
}

type ServiceProblem struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type RuntimeService struct {
	ID           string          `json:"id"`
	Label        string          `json:"label"`
	Registration string          `json:"registration"`
	State        string          `json:"state"`
	PID          int             `json:"pid,omitempty"`
	InstanceID   string          `json:"instanceId,omitempty"`
	LastExit     int             `json:"lastExit,omitempty"`
	Version      string          `json:"version"`
	Problem      *ServiceProblem `json:"problem,omitempty"`
}

type RuntimeControl struct {
	Version           string `json:"version"`
	DependencyVersion string `json:"dependencyVersion"`
	DecisionRequired  bool   `json:"decisionRequired"`
	Paused            bool   `json:"paused"`
	UnknownDelivery   bool   `json:"unknownDelivery"`
}

type RuntimeServices struct {
	Revision string           `json:"revision"`
	Services []RuntimeService `json:"services"`
	Control  *RuntimeControl  `json:"control,omitempty"`
	Problem  *ServiceProblem  `json:"problem,omitempty"`
}

type ServicePlanInput struct {
	ExpectedRevision string `json:"expectedRevision"`
	Kind             string `json:"kind"`
	ServiceID        string `json:"serviceId"`
}

type ServiceDependency struct {
	ServiceID string `json:"serviceId"`
	Version   string `json:"version"`
	Instance  string `json:"instance,omitempty"`
	Required  bool   `json:"required"`
	Ready     bool   `json:"ready"`
}

type ServicePlan struct {
	ID                 string              `json:"id"`
	Token              string              `json:"token,omitempty"`
	ExpectedRevision   string              `json:"expectedRevision"`
	Kind               string              `json:"kind"`
	ServiceID          string              `json:"serviceId"`
	ExpiresAt          time.Time           `json:"expiresAt"`
	ExpectedInstanceID string              `json:"expectedInstanceId,omitempty"`
	TargetVersion      string              `json:"targetVersion"`
	Dependencies       []ServiceDependency `json:"dependencies"`
	DrainRequired      bool                `json:"drainRequired"`
	DrainScope         string              `json:"drainScope,omitempty"`
	Eligible           bool                `json:"eligible"`
	Blockers           []ServiceProblem    `json:"blockers"`
	Base               RuntimeServices     `json:"base"`
}

type ServiceExecuteInput struct {
	PlanID    string `json:"planId"`
	Token     string `json:"token"`
	RequestID string `json:"requestId"`
}

type DrainStatus struct {
	MaintenanceID    string `json:"maintenanceId"`
	State            string `json:"state"`
	Global           bool   `json:"global"`
	Persisted        bool   `json:"persisted"`
	ConfirmedSettled bool   `json:"confirmedSettled"`
	RemoteSettled    bool   `json:"remoteSettled"`
	UnknownDelivery  bool   `json:"unknownDelivery"`
	Paused           bool   `json:"paused"`
}

type ServiceStep struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

type ServiceOperation struct {
	ID            string          `json:"id"`
	RequestID     string          `json:"requestId"`
	PlanID        string          `json:"planId"`
	ServiceID     string          `json:"serviceId"`
	Kind          string          `json:"kind"`
	State         string          `json:"state"`
	Revision      int             `json:"revision"`
	CreatedAt     time.Time       `json:"createdAt"`
	Deadline      time.Time       `json:"deadline"`
	MaintenanceID string          `json:"maintenanceId,omitempty"`
	Drain         *DrainStatus    `json:"drain,omitempty"`
	Steps         []ServiceStep   `json:"steps"`
	Readback      *RuntimeService `json:"readback,omitempty"`
	Problem       *ServiceProblem `json:"problem,omitempty"`
}
