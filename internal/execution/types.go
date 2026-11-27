package execution

import (
	"context"
	"errors"
	"net/http"
	"time"
)

const SchemaVersion = "1"

type Capability string

const (
	TextGeneration   Capability = "text_generation"
	AutoTextDelivery Capability = "auto_text_delivery"
	WebSearch        Capability = "web_search"
	WebFetch         Capability = "web_fetch"
	HostWrite        Capability = "host_write"
	ReminderSync     Capability = "reminder_sync"
	HostTools        Capability = "host_tools"
	RemoteDelegation Capability = "remote_delegation"
)

func Capabilities() []Capability {
	return []Capability{TextGeneration, AutoTextDelivery, WebSearch, WebFetch, HostWrite, ReminderSync, HostTools, RemoteDelegation}
}

type Enforcement string

const (
	Hard       Enforcement = "hard"
	Soft       Enforcement = "soft"
	Host       Enforcement = "host"
	Unverified Enforcement = "unverified"
)

type Kind string

const (
	AlmaPersonal Kind = "alma_personal"
	TextOnly     Kind = "text_only"
	LegacyHermes Kind = "legacy_hermes"
	AgyCLI       Kind = "agy_cli"
)

type Model struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

type PermissionView struct {
	Capability    Capability  `json:"capability"`
	Requested     *bool       `json:"requested"`
	Effective     *bool       `json:"effective"`
	Enforcement   Enforcement `json:"enforcement"`
	Provenance    string      `json:"provenance"`
	PolicyVersion string      `json:"policyVersion"`
	EnforcedAt    []string    `json:"enforcedAt"`
	Editable      bool        `json:"editable"`
	Reason        string      `json:"reason"`
}

type Backend struct {
	ID                 string           `json:"id"`
	Kind               Kind             `json:"kind"`
	Models             []Model          `json:"models"`
	Available          bool             `json:"available"`
	Reason             string           `json:"reason"`
	AgentBinding       string           `json:"agentBinding"`
	SessionModes       []string         `json:"sessionModes"`
	MinTimeoutSec      int              `json:"minTimeoutSec"`
	MaxTimeoutSec      int              `json:"maxTimeoutSec"`
	PermissionEvidence []PermissionView `json:"permissionEvidence"`
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

type Group struct {
	RouteID   string         `json:"routeId"`
	PolicyID  string         `json:"policyId"`
	Primary   RouteAttempt   `json:"primary"`
	Fallbacks []RouteAttempt `json:"fallbacks"`
}

type Policy struct {
	Capabilities map[Capability]bool `json:"capabilities"`
	NoTools      bool                `json:"noTools"`
	AllowSoft    bool                `json:"allowSoft"`
}

type Config struct {
	SchemaVersion string            `json:"schemaVersion"`
	Groups        map[string]Group  `json:"groups"`
	Profiles      map[string]string `json:"profiles"`
	Policies      map[string]Policy `json:"policies"`
}

type Snapshot struct {
	Revision string `json:"revision"`
	Config   Config `json:"config"`
}

type GroupView struct {
	GroupID     string           `json:"groupId"`
	Revision    string           `json:"revision"`
	Routing     Group            `json:"routing"`
	Policy      Policy           `json:"policy"`
	Permissions []PermissionView `json:"permissions"`
}

type ChangePlan struct {
	ExpectedRevision string   `json:"expectedRevision"`
	ConfigDigest     string   `json:"configDigest"`
	AffectedGroups   []string `json:"affectedGroups"`
	Effects          []string `json:"effects"`
	Warnings         []string `json:"warnings"`
}

type ApplyResult struct {
	Snapshot        Snapshot `json:"snapshot"`
	AppliedRevision string   `json:"appliedRevision"`
	Replayed        bool     `json:"replayed"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Request struct {
	TraceID          string    `json:"traceId"`
	GroupID          string    `json:"groupId"`
	Prompt           string    `json:"prompt"`
	Messages         []Message `json:"messages"`
	ExpectedRevision string    `json:"expectedRevision,omitempty"`
}

type Attempt struct {
	BackendID       string    `json:"backendId"`
	ModelID         string    `json:"modelId"`
	ExternalID      string    `json:"externalId"`
	Status          string    `json:"status"`
	Reason          string    `json:"reason"`
	FallbackReason  string    `json:"fallbackReason"`
	RemoteExecution string    `json:"remoteExecution"`
	StartedAt       time.Time `json:"startedAt"`
	EndedAt         time.Time `json:"endedAt"`
	PolicyRevision  string    `json:"policyRevision"`
}

type Result struct {
	Text            string    `json:"text"`
	Backend         string    `json:"backend"`
	Kind            Kind      `json:"kind"`
	Model           string    `json:"model"`
	RunID           string    `json:"runId"`
	GroupID         string    `json:"groupId"`
	RoutingRevision string    `json:"routingRevision"`
	PolicyRevision  string    `json:"policyRevision"`
	Attempts        []Attempt `json:"attempts"`
	Fallback        bool      `json:"fallback"`
	FallbackReason  string    `json:"fallbackReason"`
	Silent          bool      `json:"silent"`
	DeliveryAllowed bool      `json:"deliveryAllowed"`
}

type CancelResult struct {
	RunID                 string `json:"runId"`
	LocalStopRequested    bool   `json:"localStopRequested"`
	LocalStopped          bool   `json:"localStopped"`
	RemoteCancelSupported bool   `json:"remoteCancelSupported"`
	RemoteExecution       string `json:"remoteExecution"`
	Reason                string `json:"reason"`
}

type ModelRequest struct {
	ModelID  string
	Messages []Message
}

type Provider interface {
	Generate(context.Context, ModelRequest) (string, error)
}

type BackendOptions struct {
	ID           string
	Kind         Kind
	Models       []Model
	AlmaBaseURL  string
	PollInterval time.Duration
	Provider     Provider
	Hermes       HermesOptions
	Agy          AgyOptions
}

type HermesOptions struct {
	Command  string
	Dir      string
	Env      []string
	Profile  string
	Toolsets []string
	MaxTurns int
}

type AgyOptions struct {
	Command              string
	Dir                  string
	Env                  []string
	Agent                string
	Effort               string
	Sandbox              bool
	DisableSlashCommands bool
	SettingsFile         string
	AddDirs              []string
}

type Options struct {
	Backends []BackendOptions
	Client   *http.Client
}

type Error struct {
	Code            string `json:"code"`
	Field           string `json:"field"`
	CurrentRevision string `json:"currentRevision"`
}

func (err *Error) Error() string { return err.Code + ": " + err.Field }

func Code(err error) string {
	var problem *Error
	if errors.As(err, &problem) {
		return problem.Code
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "execution_failed"
}

func failure(code, field string) error { return &Error{Code: code, Field: field} }

func boolean(value bool) *bool { return &value }
