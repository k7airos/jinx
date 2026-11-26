package supervisor

import (
	"context"
	"time"
)

func (manager *Manager) Execute(ctx context.Context, input ExecuteInput) (Operation, error) {
	if !safeID(input.RequestID) || !safeID(input.PlanID) || !safeID(input.Token) {
		return Operation{}, failure("invalid_request")
	}
	payload := fingerprint(input)
	manager.mu.Lock()
	if problem := manager.availableLocked(); problem != nil {
		manager.mu.Unlock()
		return Operation{}, problem
	}
	for _, record := range manager.journal.Operations {
		if record.Operation.RequestID == input.RequestID {
			if record.Payload != payload {
				manager.mu.Unlock()
				return Operation{}, failure("request_conflict")
			}
			operation, err := manager.resumeLocked(record)
			manager.mu.Unlock()
			return operation, err
		}
		if record.Plan.ID == input.PlanID {
			manager.mu.Unlock()
			return Operation{}, failure("plan_used")
		}
	}
	plan, exists := manager.plans[input.PlanID]
	if !exists {
		manager.mu.Unlock()
		return Operation{}, failure("not_found")
	}
	if !equalToken(plan.Token, input.Token) {
		manager.mu.Unlock()
		return Operation{}, failure("invalid_token")
	}
	if !manager.options.EnableControl {
		manager.mu.Unlock()
		return Operation{}, failure("readonly")
	}
	if !plan.Eligible {
		manager.mu.Unlock()
		return Operation{}, clone(plan.Blockers[0])
	}
	if manager.activeLocked() {
		manager.mu.Unlock()
		return Operation{}, failure("operation_active")
	}
	manager.mu.Unlock()
	if problem := manager.validatePlan(ctx, plan, false); problem != nil {
		manager.mu.Lock()
		for _, record := range manager.journal.Operations {
			if record.Operation.RequestID == input.RequestID {
				if record.Payload != payload {
					manager.mu.Unlock()
					return Operation{}, failure("request_conflict")
				}
				operation, err := manager.resumeLocked(record)
				manager.mu.Unlock()
				return operation, err
			}
		}
		manager.mu.Unlock()
		return Operation{}, problem
	}
	if ctx.Err() != nil {
		return Operation{}, failure("deadline")
	}
	id, err := randomID()
	if err != nil {
		return Operation{}, err
	}
	operation := Operation{ID: id, RequestID: input.RequestID, PlanID: plan.ID, ServiceID: plan.ServiceID, Kind: plan.Kind, State: "pending", Revision: 1, CreatedAt: time.Now(), Deadline: time.Now().Add(manager.options.OperationTimeout), Steps: []Step{{Name: "validate", State: "pending"}, {Name: "drain", State: "pending"}, {Name: "command", State: "pending"}, {Name: "readback", State: "pending"}}}
	if operation.Deadline.After(plan.ExpiresAt) {
		operation.Deadline = plan.ExpiresAt
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if problem := manager.availableLocked(); problem != nil {
		return Operation{}, problem
	}
	for _, record := range manager.journal.Operations {
		if record.Operation.RequestID == input.RequestID {
			if record.Payload != payload {
				return Operation{}, failure("request_conflict")
			}
			return manager.resumeLocked(record)
		}
		if record.Plan.ID == input.PlanID {
			return Operation{}, failure("plan_used")
		}
	}
	if manager.activeLocked() {
		return Operation{}, failure("operation_active")
	}
	plan.Token = ""
	manager.journal.Operations[id] = operationRecord{Operation: operation, Plan: plan, Payload: payload}
	if err := manager.persistLocked(); err != nil {
		record := manager.journal.Operations[id]
		record.Operation.State, record.Operation.Problem = "unknown", failure("journal_unavailable")
		manager.journal.Operations[id] = record
		return Operation{}, err
	}
	manager.workers[id] = true
	manager.wait.Add(1)
	go manager.run(id)
	return clone(operation), nil
}

func (manager *Manager) resumeLocked(record operationRecord) (Operation, error) {
	operation := clone(record.Operation)
	if operation.State != "blocked" || operation.Problem == nil || operation.Problem.Code != "execute_required" || manager.workers[operation.ID] {
		return operation, nil
	}
	if !manager.options.EnableControl {
		return Operation{}, failure("readonly")
	}
	if time.Now().After(operation.Deadline) || time.Now().After(record.Plan.ExpiresAt) {
		return Operation{}, failure("deadline")
	}
	operation.State, operation.Problem = "pending", nil
	operation.Revision++
	record.Operation = operation
	manager.journal.Operations[operation.ID] = record
	if err := manager.persistLocked(); err != nil {
		return Operation{}, err
	}
	manager.workers[operation.ID] = true
	manager.wait.Add(1)
	go func() {
		defer manager.workerDone(operation.ID)
		manager.runOperation(operation.ID, true)
	}()
	return clone(operation), nil
}

func (manager *Manager) getRecord(id string) operationRecord {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	return clone(manager.journal.Operations[id])
}

func setStep(operation *Operation, name, state string) {
	for index := range operation.Steps {
		if operation.Steps[index].Name == name {
			operation.Steps[index].State = state
			return
		}
	}
	operation.Steps = append(operation.Steps, Step{Name: name, State: state})
}

func (manager *Manager) update(id string, change func(*Operation)) bool {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.poisoned {
		return false
	}
	record := manager.journal.Operations[id]
	if record.Operation.State == "canceled" {
		return false
	}
	change(&record.Operation)
	record.Operation.Revision++
	manager.journal.Operations[id] = record
	if err := manager.persistLocked(); err != nil {
		record.Operation.State = "unknown"
		record.Operation.Problem = failure("journal_unavailable")
		manager.journal.Operations[id] = record
		return false
	}
	return true
}

func (manager *Manager) finish(id, state string, problem *Problem) {
	manager.update(id, func(operation *Operation) {
		if state == "blocked" && operation.MaintenanceID == "" {
			state = "failed"
		}
		operation.State = state
		operation.Problem = problem
		for index := range operation.Steps {
			if operation.Steps[index].State == "running" {
				operation.Steps[index].State = state
			}
		}
	})
}

func (manager *Manager) workerDone(id string) {
	manager.mu.Lock()
	delete(manager.workers, id)
	manager.mu.Unlock()
	manager.wait.Done()
}

func (manager *Manager) run(id string) {
	defer manager.workerDone(id)
	manager.runOperation(id, false)
}

func (manager *Manager) runOperation(id string, resume bool) {
	record := manager.getRecord(id)
	ctx, cancel := context.WithDeadline(manager.ctx, record.Operation.Deadline)
	defer cancel()
	if manager.options.EnableControl {
		ctx = context.WithValue(ctx, executionPermission{}, executionGrant{Domain: manager.domain(), Args: manager.command(record.Plan), AllowedLabels: manager.allowedLabels()})
	}
	if !manager.update(id, func(operation *Operation) { setStep(operation, "validate", "running") }) {
		return
	}
	if problem := manager.validatePlan(ctx, record.Plan, resume); problem != nil {
		manager.finish(id, "blocked", problem)
		return
	}
	if !manager.update(id, func(operation *Operation) { setStep(operation, "validate", "verified") }) {
		return
	}
	if record.Plan.DrainRequired {
		if !resume && !manager.drain(ctx, id) {
			return
		}
	} else if !manager.update(id, func(operation *Operation) { setStep(operation, "drain", "skipped") }) {
		return
	}
	if problem := manager.validatePlan(ctx, record.Plan, record.Plan.DrainRequired); problem != nil {
		manager.finish(id, "blocked", problem)
		return
	}
	if record.Plan.DrainRequired {
		record = manager.getRecord(id)
		status, problem := manager.pollDrain(ctx, record.Operation.MaintenanceID)
		if problem != nil {
			manager.finish(id, "blocked", problem)
			return
		}
		if !manager.update(id, func(operation *Operation) { operation.Drain = &status }) {
			return
		}
	}
	if problem := manager.validatePlan(ctx, record.Plan, record.Plan.DrainRequired); problem != nil {
		manager.finish(id, "blocked", problem)
		return
	}
	if ctx.Err() != nil {
		manager.finish(id, "blocked", failure("deadline"))
		return
	}
	if !manager.update(id, func(operation *Operation) {
		operation.State = "executing"
		setStep(operation, "command", "running")
	}) {
		return
	}
	if ctx.Err() != nil {
		manager.finish(id, "unknown", failure("command_unknown"))
		return
	}
	commandCtx, commandCancel := context.WithTimeout(ctx, manager.options.CommandTimeout)
	_, _, exit, commandError := manager.commander.Run(commandCtx, launchctl, manager.command(record.Plan)...)
	uncertain := commandError != nil || commandCtx.Err() != nil
	commandCancel()
	if !manager.update(id, func(operation *Operation) {
		operation.State = "verifying"
		setStep(operation, "command", "completed")
		if uncertain {
			setStep(operation, "command", "unknown")
		} else if exit != 0 {
			setStep(operation, "command", "failed")
		}
		setStep(operation, "readback", "running")
	}) {
		return
	}
	verifyCtx, verifyCancel := context.WithTimeout(ctx, manager.options.VerifyTimeout)
	defer verifyCancel()
	service := unknownService(record.Plan.ServiceID)
	for {
		service = manager.observe(verifyCtx, record.Plan.ServiceID)
		if achieved(record.Plan, service) || verifyCtx.Err() != nil {
			break
		}
		if !waitInterval(verifyCtx, manager.options.PollInterval) {
			break
		}
	}
	state, problem := "unknown", failure("readback_unknown")
	if achieved(record.Plan, service) {
		state, problem = "succeeded", nil
	} else if uncertain {
		problem = failure("command_unknown")
	} else if exit != 0 && service.Version == record.Plan.TargetVersion {
		state, problem = "failed", failure("command_failed")
	}
	manager.update(id, func(operation *Operation) {
		operation.State, operation.Problem, operation.Readback = state, problem, &service
		setStep(operation, "readback", state)
	})
}

func achieved(plan ActionPlan, service Service) bool {
	if plan.Kind == Stop {
		return service.Registration == "unregistered"
	}
	if service.Registration != "registered" || service.State != "running" || service.InstanceID == "" {
		return false
	}
	return plan.Kind == Start || service.InstanceID != plan.ExpectedInstanceID
}

func waitInterval(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (manager *Manager) drain(ctx context.Context, id string) bool {
	if ctx.Err() != nil {
		manager.finish(id, "blocked", failure("deadline"))
		return false
	}
	if !manager.update(id, func(operation *Operation) {
		operation.State = "draining"
		setStep(operation, "drain", "running")
	}) {
		return false
	}
	drainCtx, cancel := context.WithTimeout(ctx, manager.options.CommandTimeout)
	maintenanceID, err := manager.options.DrainCoordinator.Drain(drainCtx, Global)
	uncertain := err != nil || drainCtx.Err() != nil || !safeID(maintenanceID)
	cancel()
	if uncertain {
		manager.finish(id, "unknown", failure("drain_unknown"))
		return false
	}
	if !manager.update(id, func(operation *Operation) { operation.MaintenanceID = maintenanceID }) {
		return false
	}
	for {
		status, problem := manager.pollDrain(ctx, maintenanceID)
		if problem == nil {
			return manager.update(id, func(operation *Operation) {
				operation.Drain = &status
				setStep(operation, "drain", "verified")
			})
		}
		if !manager.update(id, func(operation *Operation) { operation.Drain = &status }) {
			return false
		}
		if status.State != "draining" || problem.Code != "drain_blocked" {
			manager.finish(id, "blocked", problem)
			return false
		}
		if !waitInterval(ctx, manager.options.PollInterval) {
			manager.finish(id, "blocked", failure("deadline"))
			return false
		}
	}
}

func (manager *Manager) pollDrain(ctx context.Context, id string) (DrainStatus, *Problem) {
	if ctx.Err() != nil {
		return DrainStatus{}, failure("deadline")
	}
	pollCtx, cancel := context.WithTimeout(ctx, manager.options.CommandTimeout)
	defer cancel()
	status, err := manager.options.DrainCoordinator.Poll(pollCtx, id)
	if err != nil || pollCtx.Err() != nil || status.MaintenanceID != id {
		return status, failure("drain_unknown")
	}
	if status.Paused {
		return status, failure("blocked_by_pause")
	}
	if status.UnknownDelivery {
		return status, failure("unknown_delivery")
	}
	switch status.State {
	case "ready":
		if status.Global && status.Persisted && status.ConfirmedSettled && status.RemoteSettled {
			return status, nil
		}
		return status, failure("drain_unknown")
	case "blocked", "draining":
		return status, failure("drain_blocked")
	case "timedout":
		return status, failure("drain_timedout")
	default:
		return status, failure("drain_unknown")
	}
}

func (manager *Manager) GetOperation(ctx context.Context, id string) (Operation, error) {
	manager.mu.Lock()
	if manager.closed {
		manager.mu.Unlock()
		return Operation{}, failure("closed")
	}
	stored, exists := manager.journal.Operations[id]
	record := clone(stored)
	working := manager.workers[id]
	manager.mu.Unlock()
	if !exists {
		return Operation{}, failure("not_found")
	}
	if record.Operation.State == "unknown" && !working {
		service := manager.observe(ctx, record.Plan.ServiceID)
		record.Operation.Readback = &service
	}
	return clone(record.Operation), nil
}

func (manager *Manager) GetOperationByRequest(ctx context.Context, requestID string) (Operation, error) {
	manager.mu.Lock()
	var id string
	for _, record := range manager.journal.Operations {
		if record.Operation.RequestID == requestID {
			id = record.Operation.ID
			break
		}
	}
	manager.mu.Unlock()
	if id == "" {
		return Operation{}, failure("not_found")
	}
	return manager.GetOperation(ctx, id)
}

func (manager *Manager) ContinueMaintenance(ctx context.Context, id string) (Operation, error) {
	return manager.maintenance(ctx, id, false)
}

func (manager *Manager) CancelMaintenance(ctx context.Context, id string) (Operation, error) {
	return manager.maintenance(ctx, id, true)
}

func (manager *Manager) maintenance(ctx context.Context, id string, cancelMaintenance bool) (Operation, error) {
	manager.mu.Lock()
	if problem := manager.availableLocked(); problem != nil {
		manager.mu.Unlock()
		return Operation{}, problem
	}
	record, exists := manager.journal.Operations[id]
	if !exists {
		manager.mu.Unlock()
		return Operation{}, failure("not_found")
	}
	operation := record.Operation
	if !manager.options.EnableControl || manager.options.DrainCoordinator == nil || operation.MaintenanceID == "" {
		manager.mu.Unlock()
		return Operation{}, failure("unsupported")
	}
	commandPending := false
	for _, step := range operation.Steps {
		if step.Name == "command" && step.State == "pending" {
			commandPending = true
		}
	}
	cleanup := cancelMaintenance && (operation.State == "succeeded" || operation.State == "failed")
	if manager.workers[id] || !cleanup && (operation.State != "blocked" || !commandPending) {
		manager.mu.Unlock()
		return Operation{}, failure("operation_active")
	}
	if !cancelMaintenance && (time.Now().After(operation.Deadline) || time.Now().After(record.Plan.ExpiresAt)) {
		manager.mu.Unlock()
		return Operation{}, failure("deadline")
	}
	manager.workers[id] = true
	manager.wait.Add(1)
	manager.mu.Unlock()
	defer manager.workerDone(id)
	if !cancelMaintenance {
		if problem := manager.validatePlan(ctx, record.Plan, true); problem != nil {
			manager.finish(id, "blocked", problem)
			return clone(manager.getRecord(id).Operation), nil
		}
	}
	if !manager.update(id, func(operation *Operation) {
		operation.State = "draining"
		if cancelMaintenance {
			operation.State = "canceling"
			setStep(operation, "cancel_maintenance", "running")
		} else {
			setStep(operation, "continue_maintenance", "running")
		}
	}) {
		return Operation{}, failure("journal_unavailable")
	}
	actionCtx, cancel := context.WithTimeout(manager.ctx, manager.options.CommandTimeout)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	var err error
	if cancelMaintenance {
		err = manager.options.DrainCoordinator.Cancel(actionCtx, operation.MaintenanceID)
	} else {
		err = manager.options.DrainCoordinator.Continue(actionCtx, operation.MaintenanceID)
	}
	if err != nil || actionCtx.Err() != nil {
		manager.finish(id, "unknown", failure("drain_unknown"))
	} else if cancelMaintenance {
		manager.update(id, func(operation *Operation) {
			if cleanup {
				operation.State, operation.Problem = record.Operation.State, record.Operation.Problem
			} else {
				operation.State, operation.Problem = "canceled", failure("canceled")
			}
			setStep(operation, "cancel_maintenance", "verified")
		})
	} else {
		status, problem := manager.pollDrain(actionCtx, operation.MaintenanceID)
		manager.update(id, func(operation *Operation) {
			operation.Drain = &status
			operation.Problem = problem
			operation.State = "blocked"
			setStep(operation, "continue_maintenance", "verified")
			if problem == nil {
				operation.Problem = failure("execute_required")
			}
		})
	}
	return clone(manager.getRecord(id).Operation), nil
}
