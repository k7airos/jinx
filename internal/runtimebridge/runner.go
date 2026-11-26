package runtimebridge

import (
	"context"
	"errors"
	"sync"

	"jinx/internal/dispatcher"
	"jinx/internal/execution"
)

type Executor interface {
	Execute(context.Context, execution.Request) (execution.Result, error)
	WithDeliveryGate(context.Context, execution.Result, string, func(context.Context) error) error
}

type MetadataSource interface {
	Meta(string) (execution.Result, bool)
}

type DeliveryGater interface {
	DeliveryGate(context.Context, string, string, func(context.Context) error) error
	Forget(string)
}

type ExecutionStopper interface {
	StopExecution(context.Context, string) (execution.CancelResult, bool, error)
}

type runState struct {
	result    execution.Result
	executing bool
	gating    bool
}

type Runner struct {
	engine Executor
	mu     sync.Mutex
	runs   map[string]*runState
}

var _ dispatcher.Runner = (*Runner)(nil)

func NewRunner(engine Executor) (*Runner, error) {
	if engine == nil {
		return nil, errors.New("execution engine required")
	}
	return &Runner{engine: engine, runs: map[string]*runState{}}, nil
}

func ConvertRequest(request dispatcher.BackendRequest) execution.Request {
	converted := execution.Request{TraceID: request.TraceID, GroupID: request.GroupID}
	if request.Prompt != "" {
		converted.Messages = []execution.Message{{Role: "user", Content: request.Prompt}}
	} else {
		for _, message := range request.Messages {
			converted.Messages = append(converted.Messages, execution.Message{Role: "user", Content: message.Content})
		}
	}
	return converted
}

func ConvertResult(result execution.Result) dispatcher.Result {
	backend := ""
	switch result.Kind {
	case execution.AlmaPersonal, execution.TextOnly:
		backend = "Alma"
	case execution.LegacyHermes:
		backend = "Hermes"
	}
	sessionID := ""
	if result.RunID != "" {
		sessionID = "execution:" + result.RunID
	}
	return dispatcher.Result{Text: result.Text, Backend: backend, RunID: result.RunID,
		SessionID: sessionID, Fallback: result.Fallback, FallbackReason: result.FallbackReason}
}

func (runner *Runner) Run(ctx context.Context, request dispatcher.BackendRequest) (dispatcher.Result, error) {
	state := &runState{executing: true}
	runner.mu.Lock()
	if _, exists := runner.runs[request.TraceID]; exists {
		runner.mu.Unlock()
		return dispatcher.Result{}, &execution.Error{Code: "conflict", Field: "retained_traceId"}
	}
	runner.runs[request.TraceID] = state
	runner.mu.Unlock()
	result, err := runner.engine.Execute(ctx, ConvertRequest(request))
	runner.mu.Lock()
	if runner.runs[request.TraceID] == state {
		state.executing = false
		state.result = result
		state.result.Attempts = append([]execution.Attempt{}, result.Attempts...)
		if err != nil {
			state.result.Text = ""
			state.result.DeliveryAllowed = false
		}
	}
	runner.mu.Unlock()
	converted := ConvertResult(result)
	if err != nil {
		converted.Text = ""
	}
	return converted, err
}

func (runner *Runner) Meta(traceID string) (execution.Result, bool) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	state, exists := runner.runs[traceID]
	if !exists || state.executing {
		return execution.Result{}, false
	}
	result := state.result
	result.Text = ""
	result.Attempts = append([]execution.Attempt{}, result.Attempts...)
	return result, true
}

func (runner *Runner) Forget(traceID string) {
	runner.mu.Lock()
	delete(runner.runs, traceID)
	runner.mu.Unlock()
}

func (runner *Runner) DeliveryGate(ctx context.Context, traceID, groupID string, submit func(context.Context) error) error {
	runner.mu.Lock()
	state, exists := runner.runs[traceID]
	if !exists || state.executing || state.gating {
		runner.mu.Unlock()
		return &execution.Error{Code: "policy_blocked", Field: "execution_result_unavailable"}
	}
	state.gating = true
	result := state.result
	runner.mu.Unlock()
	defer func() {
		runner.mu.Lock()
		if runner.runs[traceID] == state {
			delete(runner.runs, traceID)
		}
		runner.mu.Unlock()
	}()
	if submit == nil {
		return &execution.Error{Code: "validation", Field: "submit"}
	}
	return runner.engine.WithDeliveryGate(ctx, result, groupID, submit)
}

func (runner *Runner) StopExecution(ctx context.Context, traceID string) (execution.CancelResult, bool, error) {
	if err := ctx.Err(); err != nil {
		return execution.CancelResult{}, false, err
	}
	canceler, supported := runner.engine.(interface {
		Cancel(string) (execution.CancelResult, error)
	})
	if !supported {
		return execution.CancelResult{RunID: traceID, Reason: "cancel_not_supported", RemoteExecution: "unknown"}, false, nil
	}
	result, err := canceler.Cancel(traceID)
	if err == nil {
		runner.Forget(traceID)
	}
	return result, true, err
}
