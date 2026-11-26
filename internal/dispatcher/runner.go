package dispatcher

import (
	"context"

	"jinx/internal/execution"
)

type BackendMessage struct {
	ID        string `json:"id"`
	SenderID  string `json:"senderId"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"`
}

type BackendRequest struct {
	Prompt           string
	Profile          string
	GroupID          string
	TraceID          string
	SessionID        string
	Scenario         string
	Skills           []string
	Tools            []string
	KnowledgeBases   []string
	ConfigSnapshotID string
	ConfigSnapshot   *ConfigSnapshot
	Messages         []BackendMessage
}

type Result = HermesResult

type Runner interface {
	Run(context.Context, BackendRequest) (Result, error)
}

type runnerMetadata interface {
	Meta(string) (execution.Result, bool)
}

type runnerDeliveryGate interface {
	DeliveryGate(context.Context, string, string, func(context.Context) error) error
}

type runnerForgetter interface {
	Forget(string)
}

type runnerStopper interface {
	StopExecution(context.Context, string) (execution.CancelResult, bool, error)
}

type RunFunc func(context.Context, BackendRequest) (Result, error)

func (run RunFunc) Run(ctx context.Context, request BackendRequest) (Result, error) {
	return run(ctx, request)
}

type Options struct {
	Runner          Runner
	LocalPolicyPath string
}

func (d *Dispatcher) backendRequest(b batch) BackendRequest {
	mounts := resolveMounts(b.Config, b.Rule)
	request := BackendRequest{
		Prompt: buildPrompt(b), Profile: b.Rule.HermesProfile, GroupID: b.GroupID,
		TraceID: b.TraceID, SessionID: b.SessionID,
		Scenario: mounts.Scenario, Skills: mounts.Skills, Tools: mounts.Tools, KnowledgeBases: mounts.KnowledgeBases,
	}
	if run, err := d.trace.GetRun(b.TraceID); err == nil {
		request.ConfigSnapshotID = run.ConfigSnapshotID
		d.mu.Lock()
		if snapshot, exists := d.state.ConfigSnapshots[run.ConfigSnapshotID]; exists {
			data := cloneSnapshot(snapshot)
			request.ConfigSnapshot = &data
		}
		d.mu.Unlock()
	}
	for _, message := range b.Messages {
		request.Messages = append(request.Messages, BackendMessage{
			ID: message.ID, SenderID: message.SenderID, Content: message.Content, Timestamp: message.Timestamp,
		})
	}
	return request
}
