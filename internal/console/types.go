package console

import (
	"encoding/json"
	"fmt"
	"time"
)

const SchemaVersion = "console-rules-v1"

type Problem struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
	Retry     string `json:"retry"`
}

func (problem *Problem) Error() string { return problem.Message }

func failure(code, message string) *Problem {
	retry := "none"
	if code == "reset_required" {
		retry = "read"
	} else if code == "conflict" || code == "stale" {
		retry = "replan"
	} else if code == "unavailable" || code == "unknown" {
		retry = "read"
	}
	return &Problem{Code: code, Message: message, Retry: retry}
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

type Envelope struct {
	Data          any       `json:"data"`
	ObservedAt    time.Time `json:"observedAt"`
	Source        string    `json:"source"`
	Freshness     string    `json:"freshness"`
	Completeness  string    `json:"completeness"`
	Revision      string    `json:"revision"`
	StreamID      string    `json:"streamId,omitempty"`
	EntitySeq     *uint64   `json:"entitySeq,omitempty"`
	MissingReason *string   `json:"missingReason,omitempty"`
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

type Scope struct {
	Kind    string `json:"kind"`
	GroupID string `json:"groupId"`
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

type Warning struct {
	Code    string `json:"code"`
	Message string `json:"message"`
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

func unsupported() Availability {
	return Availability{Reason: "No verified runtime implementation is connected"}
}

func invalidField(field string) error {
	return failure("validation", fmt.Sprintf("Invalid rule field: %s", field))
}
