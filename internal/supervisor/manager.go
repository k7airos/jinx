package supervisor

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Manager struct {
	mu        sync.Mutex
	options   Options
	labels    map[ServiceID]string
	commander Commander
	observer  Commander
	plans     map[string]ActionPlan
	journal   journal
	lock      *os.File
	poisoned  bool
	closed    bool
	ctx       context.Context
	cancel    context.CancelFunc
	wait      sync.WaitGroup
	workers   map[string]bool
}

func New(options Options) (*Manager, error) {
	if options.EnableControl && (options.UID == nil || !filepath.IsAbs(options.PlistDir) || !filepath.IsAbs(options.DataPath)) {
		return nil, failure("invalid_options")
	}
	if options.DataPath != "" && (!filepath.IsAbs(options.DataPath) || options.UID == nil) {
		return nil, failure("invalid_options")
	}
	if options.PlistDir != "" && !filepath.IsAbs(options.PlistDir) {
		return nil, failure("invalid_options")
	}
	if options.UID != nil {
		uid := *options.UID
		options.UID = &uid
	}
	if options.PlanTTL <= 0 {
		options.PlanTTL = 2 * time.Minute
	}
	if options.OperationTimeout <= 0 {
		options.OperationTimeout = 2 * time.Minute
	}
	if options.CommandTimeout <= 0 {
		options.CommandTimeout = 5 * time.Second
	}
	if options.VerifyTimeout <= 0 {
		options.VerifyTimeout = 10 * time.Second
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 100 * time.Millisecond
	}
	resolvedLabels, err := ResolveLabels(options.ServiceLabels)
	if err != nil {
		return nil, err
	}
	options.ServiceLabels = resolvedLabels
	manager := &Manager{options: options, labels: resolvedLabels, commander: options.Commander, observer: options.Observer, plans: make(map[string]ActionPlan), workers: make(map[string]bool)}
	if manager.commander == nil {
		manager.commander = ExecCommander{}
	}
	if manager.observer == nil && options.Commander != nil {
		manager.observer = options.Commander
	}
	manager.ctx, manager.cancel = context.WithCancel(context.Background())
	manager.journal = journal{Schema: 1, Scope: fingerprint(struct {
		UID      *uint32
		PlistDir string
	}{options.UID, options.PlistDir}), Operations: make(map[string]operationRecord)}
	if err := manager.openJournal(); err != nil {
		manager.cancel()
		if manager.lock != nil {
			manager.lock.Close()
		}
		return nil, err
	}
	return manager, nil
}

func (manager *Manager) Close() error {
	manager.mu.Lock()
	manager.closed = true
	manager.cancel()
	manager.mu.Unlock()
	manager.wait.Wait()
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.lock != nil {
		err := manager.lock.Close()
		manager.lock = nil
		return err
	}
	return nil
}

func (manager *Manager) availableLocked() *Problem {
	if manager.closed {
		return failure("closed")
	}
	if manager.poisoned {
		return failure("journal_unavailable")
	}
	return nil
}

func (manager *Manager) ListServices(ctx context.Context) (ServiceList, error) {
	manager.mu.Lock()
	closed := manager.closed
	manager.mu.Unlock()
	if closed {
		return ServiceList{}, failure("closed")
	}
	list := ServiceList{Services: make([]Service, 0, len(serviceOrder))}
	for _, id := range serviceOrder {
		list.Services = append(list.Services, manager.observe(ctx, id))
	}
	if manager.options.DrainCoordinator != nil {
		controlCtx, cancel := context.WithTimeout(ctx, manager.options.CommandTimeout)
		control, err := manager.options.DrainCoordinator.Snapshot(controlCtx)
		if err == nil && controlCtx.Err() == nil && control.Version != "" && control.DependencyVersion != "" {
			list.Control = &control
		} else {
			list.Problem = failure("control_unknown")
		}
		cancel()
	} else {
		list.Problem = failure("unsupported")
	}
	list.Revision = fingerprint(list)
	return list, nil
}

func (manager *Manager) ControlCapabilities(ctx context.Context) Capabilities {
	list, err := manager.ListServices(ctx)
	capabilities := Capabilities{Read: Availability{Supported: manager.observer != nil && manager.options.UID != nil}, Actions: make(map[Kind]Availability)}
	capabilities.Read.Enabled = capabilities.Read.Supported
	capabilities.Read.Ready = err == nil && capabilities.Read.Supported
	for _, service := range list.Services {
		if service.Registration == "unknown" {
			capabilities.Read.Ready = false
		}
	}
	if !capabilities.Read.Ready {
		capabilities.Read.Problem = failure("observation_unknown")
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	for _, kind := range []Kind{Start, Stop, Restart} {
		availability := Availability{Supported: capabilities.Read.Supported && manager.options.DataPath != "" && (kind == Start || manager.options.DrainCoordinator != nil)}
		availability.Enabled = availability.Supported && manager.options.EnableControl
		availability.Ready = availability.Enabled && capabilities.Read.Ready
		switch {
		case !availability.Supported:
			availability.Problem = failure("unsupported")
		case !availability.Enabled:
			availability.Problem = failure("readonly")
		case manager.availableLocked() != nil:
			availability.Problem = manager.availableLocked()
		case manager.activeLocked():
			availability.Problem = failure("operation_active")
		case kind != Start && list.Control == nil:
			availability.Problem = failure("control_unknown")
		case kind != Start && list.Control.Paused:
			availability.Problem = failure("blocked_by_pause")
		case kind != Start && list.Control.UnknownDelivery:
			availability.Problem = failure("unknown_delivery")
		}
		if availability.Problem != nil {
			availability.Ready = false
		}
		capabilities.Actions[kind] = availability
	}
	return capabilities
}

func validAction(kind Kind, id ServiceID) bool {
	_, allowed := labels[id]
	return allowed && (kind == Start || kind == Stop || kind == Restart)
}

func (manager *Manager) Plan(ctx context.Context, kind Kind, id ServiceID, expectedRevision string) (ActionPlan, error) {
	if _, allowed := labels[id]; !allowed {
		return ActionPlan{}, failure("invalid_service")
	}
	if kind != Start && kind != Stop && kind != Restart {
		return ActionPlan{}, failure("invalid_kind")
	}
	list, err := manager.ListServices(ctx)
	if err != nil {
		return ActionPlan{}, err
	}
	if expectedRevision == "" || expectedRevision != list.Revision {
		return ActionPlan{}, failure("stale_plan")
	}
	service := findService(list, id)
	plan := ActionPlan{Kind: kind, ServiceID: id, ExpectedRevision: list.Revision, ExpectedInstanceID: service.InstanceID, TargetVersion: service.Version, Base: list, DrainRequired: kind != Start, Dependencies: dependencies(list, id, kind), Blockers: []*Problem{}}
	if plan.DrainRequired {
		plan.DrainScope = Global
	}
	plan.Blockers = append(plan.Blockers, blockers(plan, list)...)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if problem := manager.availableLocked(); problem != nil {
		return ActionPlan{}, problem
	}
	if !manager.options.EnableControl {
		plan.Blockers = append(plan.Blockers, failure("readonly"))
	}
	if manager.observer == nil || manager.options.DataPath == "" || plan.DrainRequired && manager.options.DrainCoordinator == nil {
		plan.Blockers = append(plan.Blockers, failure("unsupported"))
	}
	if manager.activeLocked() {
		plan.Blockers = append(plan.Blockers, failure("operation_active"))
	}
	plan.Eligible = len(plan.Blockers) == 0
	plan.ID, err = randomID()
	if err != nil {
		return ActionPlan{}, err
	}
	plan.Token, err = randomID()
	if err != nil {
		return ActionPlan{}, err
	}
	plan.ExpiresAt = time.Now().Add(manager.options.PlanTTL)
	for id, previous := range manager.plans {
		if time.Now().After(previous.ExpiresAt) {
			delete(manager.plans, id)
		}
	}
	manager.plans[plan.ID] = clone(plan)
	return plan, nil
}

func dependencies(list ServiceList, id ServiceID, kind Kind) []Dependency {
	result := []Dependency{}
	if id != Dispatcher {
		if kind != Start {
			service := findService(list, Dispatcher)
			result = append(result, Dependency{ServiceID: Dispatcher, Version: service.Version, Instance: service.InstanceID, Required: true, Ready: service.Registration == "registered" && service.State == "running"})
		}
		return result
	}
	for _, dependencyID := range []ServiceID{Relay, Bridge, Decision} {
		service := findService(list, dependencyID)
		required := dependencyID != Decision || list.Control == nil || list.Control.DecisionRequired
		result = append(result, Dependency{ServiceID: dependencyID, Version: service.Version, Instance: service.InstanceID, Required: required, Ready: service.Registration == "registered" && service.State == "running"})
	}
	return result
}

func blockers(plan ActionPlan, list ServiceList) []*Problem {
	result := []*Problem{}
	service := findService(list, plan.ServiceID)
	for _, observed := range list.Services {
		if observed.Registration == "unknown" {
			result = append(result, failure("observation_unknown"))
			break
		}
	}
	if plan.Kind != Start {
		if list.Control == nil {
			result = append(result, failure("control_unknown"))
		} else if list.Control.Paused {
			result = append(result, failure("blocked_by_pause"))
		} else if list.Control.UnknownDelivery {
			result = append(result, failure("unknown_delivery"))
		}
	}
	for _, dependency := range dependencies(list, plan.ServiceID, plan.Kind) {
		if dependency.Required && !dependency.Ready && !(plan.ServiceID == Dispatcher && plan.Kind == Stop) {
			result = append(result, failure("dependency_not_ready"))
			break
		}
	}
	if plan.Kind == Start && service.State == "running" {
		result = append(result, failure("already_running"))
	}
	if plan.Kind == Stop && service.Registration == "unregistered" {
		result = append(result, failure("already_stopped"))
	}
	if plan.Kind == Restart && (service.State != "running" || service.InstanceID == "" || service.Registration != "registered") {
		result = append(result, failure("not_running"))
	}
	return result
}

func (manager *Manager) validatePlan(ctx context.Context, plan ActionPlan, afterDrain bool) *Problem {
	if time.Now().After(plan.ExpiresAt) {
		return failure("plan_expired")
	}
	list, err := manager.ListServices(ctx)
	if err != nil {
		return failure("observation_unknown")
	}
	if problems := blockers(plan, list); len(problems) != 0 {
		return problems[0]
	}
	service := findService(list, plan.ServiceID)
	if service.InstanceID != plan.ExpectedInstanceID {
		return failure("wrong_instance")
	}
	if !afterDrain && list.Revision != plan.ExpectedRevision {
		return failure("stale_plan")
	}
	if afterDrain {
		if fingerprint(list.Services) != fingerprint(plan.Base.Services) || list.Control == nil || plan.Base.Control == nil || list.Control.DependencyVersion != plan.Base.Control.DependencyVersion || list.Control.DecisionRequired != plan.Base.Control.DecisionRequired {
			return failure("stale_plan")
		}
	}
	return nil
}

func (manager *Manager) activeLocked() bool {
	for _, record := range manager.journal.Operations {
		switch record.Operation.State {
		case "succeeded", "failed", "canceled":
		default:
			return true
		}
	}
	return false
}

func randomID() (string, error) {
	var buffer [24]byte
	if _, err := rand.Read(buffer[:]); err != nil {
		return "", failure("journal_unavailable")
	}
	return hex.EncodeToString(buffer[:]), nil
}

func equalToken(expected, actual string) bool {
	return subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) == 1
}
