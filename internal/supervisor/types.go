package supervisor

import (
	"context"
	"time"
)

type ServiceID string

const (
	Relay      ServiceID = "relay"
	Dispatcher ServiceID = "dispatcher"
	Decision   ServiceID = "decision"
	Bridge     ServiceID = "bridge"
)

type Kind string

const (
	Start   Kind = "start"
	Stop    Kind = "stop"
	Restart Kind = "restart"
)

type Commander interface {
	Run(context.Context, string, ...string) (stdout string, stderr string, exit int, err error)
}

type Scope string

const Global Scope = "global"

type ControlSnapshot struct {
	Version           string `json:"version"`
	DependencyVersion string `json:"dependencyVersion"`
	DecisionRequired  bool   `json:"decisionRequired"`
	Paused            bool   `json:"paused"`
	UnknownDelivery   bool   `json:"unknownDelivery"`
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

type DrainCoordinator interface {
	Snapshot(context.Context) (ControlSnapshot, error)
	Drain(context.Context, Scope) (string, error)
	Poll(context.Context, string) (DrainStatus, error)
	Cancel(context.Context, string) error
	Continue(context.Context, string) error
}

type Options struct {
	UID              *uint32
	PlistDir         string
	DataPath         string
	EnableControl    bool
	ServiceLabels    map[ServiceID]string
	Commander        Commander
	Observer         Commander
	DrainCoordinator DrainCoordinator
	PlanTTL          time.Duration
	OperationTimeout time.Duration
	CommandTimeout   time.Duration
	VerifyTimeout    time.Duration
	PollInterval     time.Duration
}

type Problem struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (problem *Problem) Error() string { return problem.Code + ": " + problem.Message }

func failure(code string) *Problem {
	return &Problem{Code: code, Message: messages[code]}
}

var messages = map[string]string{
	"unsupported":          "The required observer, control implementation or durable drain boundary is unavailable",
	"readonly":             "Service control has not been explicitly enabled by the operator",
	"invalid_options":      "Control requires a trusted UID, absolute plist directory and independent data path",
	"invalid_service":      "The service is outside the fixed allowlist",
	"invalid_kind":         "Only start, stop and restart are supported",
	"invalid_request":      "A valid request ID, plan ID and token are required",
	"not_found":            "The requested plan or operation does not exist",
	"closed":               "The manager is closed",
	"journal_unavailable":  "The operation journal could not be durably written; control is blocked",
	"journal_invalid":      "The journal is invalid or belongs to a different control scope",
	"journal_locked":       "Another manager holds the control data lock",
	"request_conflict":     "The request ID is already bound to a different payload",
	"plan_used":            "This plan already belongs to an operation",
	"invalid_token":        "The plan token does not match",
	"plan_expired":         "The plan has expired; create a fresh plan",
	"stale_plan":           "The service or control revision changed; create a fresh plan",
	"wrong_instance":       "The planned service instance has changed",
	"observation_unknown":  "The launchd registration or instance cannot be verified",
	"dependency_not_ready": "A required dependency is not running; no other service will be started implicitly",
	"control_unknown":      "The Dispatcher control snapshot cannot be verified",
	"blocked_by_pause":     "An explicit user pause blocks this action; it will not be resumed automatically",
	"unknown_delivery":     "Unresolved delivery or remote execution blocks service control",
	"already_running":      "The service is already running",
	"already_stopped":      "The service is already unregistered",
	"not_running":          "Restart requires a registered, running and identifiable instance",
	"operation_active":     "An unfinished or unknown operation must be resolved before another control action",
	"drain_unknown":        "The global maintenance boundary could not be verified",
	"drain_blocked":        "Global maintenance has not settled confirmations and remote execution",
	"drain_timedout":       "Maintenance timed out; service control was not authorized",
	"deadline":             "The operation deadline expired; no further service command is authorized",
	"execute_required":     "Maintenance is ready; explicitly Execute the original request to authorize the service command",
	"canceled":             "Maintenance was explicitly canceled",
	"command_failed":       "The command failed and readback confirms the original service version",
	"command_unknown":      "The command outcome is uncertain; it will not be retried automatically",
	"readback_unknown":     "Readback does not prove the requested service state",
	"recovered_unknown":    "An interrupted operation was recovered; only readback is permitted",
}

type Service struct {
	ID           ServiceID `json:"id"`
	Label        string    `json:"label"`
	Registration string    `json:"registration"`
	State        string    `json:"state"`
	PID          int       `json:"pid,omitempty"`
	InstanceID   string    `json:"instanceId,omitempty"`
	LastExit     *int      `json:"lastExit,omitempty"`
	Version      string    `json:"version"`
	Problem      *Problem  `json:"problem,omitempty"`
}

type ServiceList struct {
	Revision string           `json:"revision"`
	Services []Service        `json:"services"`
	Control  *ControlSnapshot `json:"control,omitempty"`
	Problem  *Problem         `json:"problem,omitempty"`
}

type Availability struct {
	Supported bool     `json:"supported"`
	Enabled   bool     `json:"enabled"`
	Ready     bool     `json:"ready"`
	Problem   *Problem `json:"problem,omitempty"`
}

type Capabilities struct {
	Read    Availability          `json:"read"`
	Actions map[Kind]Availability `json:"actions"`
}

type Dependency struct {
	ServiceID ServiceID `json:"serviceId"`
	Version   string    `json:"version"`
	Instance  string    `json:"instanceId,omitempty"`
	Required  bool      `json:"required"`
	Ready     bool      `json:"ready"`
}

type ActionPlan struct {
	ID                 string       `json:"id"`
	Token              string       `json:"token,omitempty"`
	ExpiresAt          time.Time    `json:"expiresAt"`
	Kind               Kind         `json:"kind"`
	ServiceID          ServiceID    `json:"serviceId"`
	ExpectedRevision   string       `json:"expectedRevision"`
	ExpectedInstanceID string       `json:"expectedInstanceId,omitempty"`
	TargetVersion      string       `json:"targetVersion"`
	Dependencies       []Dependency `json:"dependencies"`
	DrainRequired      bool         `json:"drainRequired"`
	DrainScope         Scope        `json:"drainScope,omitempty"`
	Eligible           bool         `json:"eligible"`
	Blockers           []*Problem   `json:"blockers"`
	Base               ServiceList  `json:"base"`
}

type ExecuteInput struct {
	PlanID    string `json:"planId"`
	Token     string `json:"token"`
	RequestID string `json:"requestId"`
}

type Step struct {
	Name  string `json:"name"`
	State string `json:"state"`
}

type Operation struct {
	ID            string       `json:"id"`
	RequestID     string       `json:"requestId"`
	PlanID        string       `json:"planId"`
	ServiceID     ServiceID    `json:"serviceId"`
	Kind          Kind         `json:"kind"`
	State         string       `json:"state"`
	Revision      uint64       `json:"revision"`
	CreatedAt     time.Time    `json:"createdAt"`
	Deadline      time.Time    `json:"deadline"`
	MaintenanceID string       `json:"maintenanceId,omitempty"`
	Drain         *DrainStatus `json:"drain,omitempty"`
	Steps         []Step       `json:"steps"`
	Readback      *Service     `json:"readback,omitempty"`
	Problem       *Problem     `json:"problem,omitempty"`
}
