package client

import (
	"context"
	"fmt"
)

// APIError wraps a structured console/gateway Problem along with the HTTP status code.
type APIError struct {
	Status  int
	Problem *Problem
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	if e.Problem != nil {
		return fmt.Sprintf("http %d (%s): %s", e.Status, e.Problem.Code, e.Problem.Message)
	}
	return fmt.Sprintf("http %d", e.Status)
}

func (e *APIError) Unwrap() error {
	if e == nil || e.Problem == nil {
		return nil
	}
	return e.Problem
}

// RunCallbacks receives resumable run SSE notifications.
type RunCallbacks struct {
	OnEvent         func(RunEvent)
	OnResetRequired func(Problem)
	OnUnavailable   func(Problem)
	OnConnection    func(state string)
}

// ArchitectureCallbacks receives resumable architecture SSE notifications.
type ArchitectureCallbacks struct {
	OnEvent         func(ArchitectureEvent, string)
	OnResetRequired func(Problem)
	OnUnavailable   func(Problem)
	OnConnection    func(state string)
}

// ConsoleClient abstracts loopback control-plane/gateway HTTP+SSE calls and
// synthetic offline mock sandboxes.
type ConsoleClient interface {
	IsMock() bool
	Label() string

	Bootstrap(ctx context.Context) (Bootstrap, error)
	GetOverview(ctx context.Context) (Envelope[Overview], error)
	GetArchitecture(ctx context.Context) (ArchitectureSnapshot, error)
	ListConnectors(ctx context.Context) (Envelope[[]ConnectorView], error)
	PlanConnectorDryRun(ctx context.Context, platform string, input ConnectorDryRunPlanInput) (ConnectorDryRunPlan, error)
	ApplyConnectorDryRun(ctx context.Context, platform string, input ConnectorDryRunApplyInput) (ConnectorDryRunApplyResult, error)
	GetCatalog(ctx context.Context) (Envelope[CatalogView], error)

	ListGroups(ctx context.Context) (Envelope[GroupPage], error)
	GetGroup(ctx context.Context, id string) (Envelope[GroupView], error)
	GetGlobalRules(ctx context.Context) (Envelope[GlobalRulesView], error)
	GetRelayRules(ctx context.Context) (Envelope[RelayRulesView], error)
	PlanChange(ctx context.Context, input PlanInput) (ChangePlan, error)
	PlanRelayRules(ctx context.Context, input PlanInput) (ChangePlan, error)
	PublishChange(ctx context.Context, input PublishInput) (Operation, error)
	GetOperation(ctx context.Context, id string) (Operation, error)

	ListBackends(ctx context.Context) ([]Backend, error)
	GetGroupRouting(ctx context.Context, groupID string) (GroupRouting, error)
	GetGroupPermissions(ctx context.Context, groupID string) (GroupPermissions, error)

	ListRuns(ctx context.Context, filter RunFilter) (Envelope[RunPage], error)
	GetRun(ctx context.Context, id string) (Envelope[RunDetail], error)
	GetConfigSnapshot(ctx context.Context, id string) (Envelope[*ConfigSnapshot], error)

	AcknowledgeAttention(ctx context.Context, input AcknowledgeInput) (Envelope[AttentionItem], error)
	GetControls(ctx context.Context) (ControlSnapshot, error)
	RunControlAction(ctx context.Context, input ControlRequest) (ControlOperation, error)

	ListServices(ctx context.Context) (Envelope[[]ServiceView], error)
	GetRuntimeServices(ctx context.Context) (RuntimeServices, error)
	PlanServiceAction(ctx context.Context, input ServicePlanInput) (ServicePlan, error)
	ExecuteServiceAction(ctx context.Context, input ServiceExecuteInput) (ServiceOperation, error)
	ContinueMaintenance(ctx context.Context, id string) (ServiceOperation, error)
	CancelMaintenance(ctx context.Context, id string) (ServiceOperation, error)

	ListChanges(ctx context.Context, filter ChangeFilter) (Envelope[ChangePage], error)
	GetChange(ctx context.Context, id string) (Envelope[ChangeRecord], error)
	PlanRollback(ctx context.Context, input RollbackInput) (ChangePlan, error)
	GetConfigurationDrift(ctx context.Context, component string) (Envelope[ConfigurationDrift], error)

	SubscribeRuns(ctx context.Context, cursor string, cb RunCallbacks) (stop func())
	SubscribeArchitecture(ctx context.Context, cursor string, domain string, cb ArchitectureCallbacks) (stop func())
}
