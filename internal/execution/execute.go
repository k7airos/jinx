package execution

import (
	"context"
	"strings"
	"time"
)

type executionOutput struct {
	text     string
	external string
	remote   string
}

func validateRequest(request Request) error {
	if !tokenPattern.MatchString(request.TraceID) || request.GroupID == "" || len(request.Prompt) > 1<<20 || len(request.Messages) > 256 {
		return failure("validation", "request")
	}
	size := len(request.Prompt)
	for _, message := range request.Messages {
		if message.Role != "user" && message.Role != "assistant" && message.Role != "system" {
			return failure("validation", "message.role")
		}
		size += len(message.Content)
	}
	if size == 0 || size > 1<<20 {
		return failure("validation", "prompt_size")
	}
	return nil
}

func requestMessages(request Request) []Message {
	messages := append([]Message{}, request.Messages...)
	if request.Prompt != "" {
		messages = append(messages, Message{Role: "user", Content: request.Prompt})
	}
	return messages
}

func requestText(request Request) string {
	var text strings.Builder
	for _, message := range request.Messages {
		text.WriteString(message.Role + ": " + message.Content + "\n")
	}
	text.WriteString(request.Prompt)
	return text.String()
}

func (engine *Engine) Execute(ctx context.Context, request Request) (Result, error) {
	if err := validateRequest(request); err != nil {
		return Result{}, err
	}
	ctx, cancel := context.WithCancel(ctx)
	engine.mu.Lock()
	if _, exists := engine.active[request.TraceID]; exists {
		engine.mu.Unlock()
		cancel()
		return Result{}, failure("conflict", "active_traceId")
	}
	engine.active[request.TraceID] = activeRun{cancel: cancel}
	engine.mu.Unlock()
	defer func() {
		cancel()
		engine.mu.Lock()
		delete(engine.active, request.TraceID)
		engine.mu.Unlock()
	}()
	snapshot, err := engine.Snapshot(ctx)
	if err != nil {
		return Result{}, err
	}
	if request.ExpectedRevision != "" && request.ExpectedRevision != snapshot.Revision {
		return Result{}, &Error{Code: "conflict", Field: "expectedRevision", CurrentRevision: snapshot.Revision}
	}
	group, exists := snapshot.Config.Groups[request.GroupID]
	if !exists {
		return Result{}, failure("not_found", "groupId")
	}
	policy := snapshot.Config.Policies[group.PolicyID]
	result := Result{RunID: request.TraceID, GroupID: request.GroupID, RoutingRevision: snapshot.Revision, PolicyRevision: snapshot.Revision, Attempts: []Attempt{}}
	attempts := append([]RouteAttempt{group.Primary}, group.Fallbacks...)
	var lastErr error
	for index, attempt := range attempts {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		effective, revision, err := engine.currentPolicy(ctx, request.GroupID, policy)
		if err == nil {
			err = engine.admit(attempt, effective)
		}
		if err == nil && index > 0 {
			err = engine.fallbackCompatible(group.Primary, attempt, effective)
			if err == nil {
				err = engine.fallbackCompatible(attempts[index-1], attempt, effective)
			}
		}
		if err != nil {
			if index > 0 {
				err = failure("fallback_policy_mismatch", "effective_policy")
			}
			return result, err
		}
		attemptRecord := Attempt{BackendID: attempt.BackendID, ModelID: attempt.ModelID, Status: "running", StartedAt: time.Now().UTC(),
			PolicyRevision: revision, RemoteExecution: "not_applicable"}
		if index > 0 {
			attemptRecord.FallbackReason = Code(lastErr)
		}
		attemptCtx, attemptCancel := context.WithTimeout(ctx, time.Duration(attempt.TimeoutSec)*time.Second)
		boundary := func(action func() error) error {
			return engine.locked(attemptCtx, func() error {
				current, err := engine.read()
				if err != nil {
					return err
				}
				currentGroup, exists := current.Config.Groups[request.GroupID]
				if !exists {
					return failure("policy_blocked", "group_removed")
				}
				if err := engine.admit(attempt, intersectPolicies(policy, current.Config.Policies[currentGroup.PolicyID])); err != nil {
					return err
				}
				return action()
			})
		}
		option, _ := engine.option(attempt.BackendID)
		output, runErr := engine.run(attemptCtx, option, attempt, request, boundary)
		if attemptCtx.Err() != nil {
			runErr = attemptCtx.Err()
		}
		attemptCancel()
		attemptRecord.ExternalID, attemptRecord.RemoteExecution = output.external, output.remote
		attemptRecord.EndedAt = time.Now().UTC()
		if runErr == nil {
			attemptRecord.Status = "succeeded"
			result.Text = strings.TrimSpace(output.text)
			result.Backend, result.Kind, result.Model = attempt.BackendID, option.Kind, attempt.ModelID
			result.Fallback, result.FallbackReason = index > 0, attemptRecord.FallbackReason
			result.Silent = result.Text == "NONE"
		} else {
			attemptRecord.Status, attemptRecord.Reason = "failed", Code(runErr)
			if Code(runErr) == "timeout" {
				attemptRecord.Status = "timed_out"
			} else if Code(runErr) == "canceled" {
				attemptRecord.Status = "canceled"
			}
		}
		result.Attempts = append(result.Attempts, attemptRecord)
		if runErr == nil {
			effective, revision, err = engine.currentPolicy(ctx, request.GroupID, policy)
			if err == nil {
				err = engine.admit(attempt, effective)
			}
			if err != nil {
				result.Text = ""
				return result, failure("policy_blocked", "result_boundary")
			}
			result.PolicyRevision = revision
			result.DeliveryAllowed = effective.Capabilities[AutoTextDelivery]
			return result, nil
		}
		if output.remote != "not_applicable" && output.remote != "stopped" && option.Kind != TextOnly {
			return result, failure("execution_uncertain", "fallback_blocked")
		}
		if Code(runErr) == "policy_blocked" || Code(runErr) == "policy_mismatch" {
			return result, runErr
		}
		lastErr = runErr
	}
	return result, lastErr
}

func (engine *Engine) Cancel(runID string) (CancelResult, error) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	run, exists := engine.active[runID]
	if !exists {
		return CancelResult{}, failure("not_found", "active_runId")
	}
	run.cancel()
	return CancelResult{RunID: runID, LocalStopRequested: true, LocalStopped: false, RemoteCancelSupported: false,
		RemoteExecution: "may_be_running", Reason: "local_context_canceled_only; remote_cancel_not_implemented"}, nil
}

func (engine *Engine) run(ctx context.Context, option BackendOptions, attempt RouteAttempt, request Request, boundary func(func() error) error) (executionOutput, error) {
	switch option.Kind {
	case AlmaPersonal:
		return engine.personal(ctx, option, attempt, request, boundary)
	case TextOnly:
		if option.Provider == nil {
			return executionOutput{}, failure("unavailable", "text_provider_not_configured")
		}
		if err := boundary(func() error { return nil }); err != nil {
			return executionOutput{}, err
		}
		text, err := option.Provider.Generate(ctx, ModelRequest{ModelID: attempt.ModelID, Messages: requestMessages(request)})
		if err == nil && strings.TrimSpace(text) == "" {
			err = failure("invalid_output", "empty_text")
		}
		return executionOutput{text: text, remote: "not_applicable"}, err
	case LegacyHermes:
		return runHermes(ctx, option.Hermes, attempt.ModelID, request, boundary)
	case AgyCLI:
		return runAgy(ctx, option.Agy, attempt.ModelID, attempt.TimeoutSec, request, boundary)
	}
	return executionOutput{}, failure("unsupported", "backend_kind")
}
