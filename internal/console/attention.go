package console

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/observability"
)

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

type AttentionAcknowledgement struct {
	RequestID string        `json:"requestId"`
	Revision  string        `json:"revision"`
	Item      AttentionItem `json:"item"`
}

type AcknowledgeInput struct {
	RequestID        string `json:"requestId"`
	ExpectedRevision string `json:"expectedRevision"`
	AttentionID      string `json:"attentionId"`
}

func stageCounts(snapshot observability.Snapshot, err error) StageCounts {
	if err != nil {
		return StageCounts{}
	}
	counts := map[string]int{"aggregating": 0, "queued": 0, "executing": 0, "submitting": 0, "confirming": 0, "unknownDelivery": 0, "remoteUnsettled": 0}
	for _, run := range snapshot.Runs {
		if run.Phase != contracts.PhaseTerminal {
			switch run.Phase {
			case contracts.PhaseAggregating:
				counts["aggregating"]++
			case contracts.PhaseQueued:
				counts["queued"]++
			case contracts.PhaseExecuting:
				counts["executing"]++
			case contracts.PhaseSubmitting:
				counts["submitting"]++
			case contracts.PhaseConfirming:
				counts["confirming"]++
			}
		}
		if run.Delivery == contracts.DeliveryUnknown {
			counts["unknownDelivery"]++
		}
		for _, attempt := range run.Attempts {
			if attempt.RemoteExecution == contracts.RemoteMayBeRunning || attempt.RemoteExecution == contracts.RemoteUnknown {
				counts["remoteUnsettled"]++
				break
			}
		}
	}
	pointer := func(name string) *int { value := counts[name]; return &value }
	return StageCounts{Aggregating: pointer("aggregating"), Queued: pointer("queued"), Executing: pointer("executing"), Submitting: pointer("submitting"), Confirming: pointer("confirming"), UnknownDelivery: pointer("unknownDelivery"), RemoteUnsettled: pointer("remoteUnsettled")}
}

func (facade *Facade) attention(snapshot observability.Snapshot, traceErr error) []AttentionItem {
	items := []AttentionItem{}
	if traceErr == nil {
		for _, run := range snapshot.Runs {
			code := ""
			if run.Outcome == contracts.OutcomeFailed {
				code = "run_failed"
			}
			if run.Delivery == contracts.DeliveryUnknown {
				code = "delivery_unknown"
			}
			if code != "" {
				items = append(items, AttentionItem{ID: "run:" + safeToken(run.ID) + ":" + code, Revision: safeToken(run.Revision), Code: code, Summary: runSummary(run.Phase, run.Outcome, run.Delivery), OccurredAt: run.UpdatedAt, Target: &AttentionTarget{Kind: "run", ID: safeToken(run.ID)}})
			}
		}
	}
	facade.mu.Lock()
	defer facade.mu.Unlock()
	for _, saved := range facade.plans {
		if saved.Record == nil {
			continue
		}
		operation := saved.Record.Operation
		if operation.Status == "unknown" || operation.Status == "partial_apply" || operation.Status == "failed" {
			items = append(items, AttentionItem{ID: "operation:" + operation.ID, Revision: operation.Revision, Code: "publication_" + operation.Status, Summary: operation.Summary, OccurredAt: saved.Record.CreatedAt, Target: &AttentionTarget{Kind: "operation", ID: operation.ID}})
		}
	}
	for index := range items {
		ack, exists := facade.acknowledgements[items[index].ID]
		items[index].Acknowledged = exists && ack.Revision == items[index].Revision
	}
	sort.Slice(items, func(first, second int) bool {
		if items[first].OccurredAt.Equal(items[second].OccurredAt) {
			return items[first].ID < items[second].ID
		}
		return items[first].OccurredAt.After(items[second].OccurredAt)
	})
	return items
}

func (facade *Facade) ListAttention(ctx context.Context) Envelope {
	snapshot, err := facade.backend.traceSnapshot(ctx)
	return envelope(map[string]any{"items": facade.attention(snapshot, err), "runsAvailable": err == nil}, snapshot.Cursor, "trace_and_console_journal", "partial")
}

func (facade *Facade) AcknowledgeAttention(ctx context.Context, input AcknowledgeInput) (Envelope, error) {
	if !validRequestID(input.RequestID) || input.AttentionID == "" || input.ExpectedRevision == "" {
		return Envelope{}, failure("validation", "Request ID, attention ID and expected revision are required")
	}
	snapshot, traceErr := facade.backend.traceSnapshot(ctx)
	items := facade.attention(snapshot, traceErr)
	facade.mu.Lock()
	defer facade.mu.Unlock()
	if facade.lock == nil {
		return Envelope{}, failure("unavailable", "Console journal is closed")
	}
	for _, ack := range facade.acknowledgements {
		if ack.RequestID == input.RequestID {
			if ack.Item.ID == input.AttentionID && ack.Revision == input.ExpectedRevision {
				return envelope(ack.Item, ack.Revision, "console_attention_journal", "partial"), nil
			}
			return Envelope{}, failure("conflict", "Acknowledgement request ID was already used")
		}
	}
	for _, item := range items {
		if item.ID != input.AttentionID {
			continue
		}
		if item.Revision != input.ExpectedRevision {
			return Envelope{}, failure("conflict", "Attention evidence changed; read again")
		}
		if item.Target != nil && item.Target.Kind == "operation" {
			for _, saved := range facade.plans {
				if saved.Record != nil && saved.Record.Operation.ID == item.Target.ID && saved.Record.Operation.Revision != item.Revision {
					return Envelope{}, failure("conflict", "Publication evidence changed; read again")
				}
			}
		}
		item.Acknowledged = true
		previous, existed := facade.acknowledgements[item.ID]
		facade.acknowledgements[item.ID] = AttentionAcknowledgement{RequestID: input.RequestID, Revision: input.ExpectedRevision, Item: item}
		if err := facade.saveAcknowledgements(); err != nil {
			if existed {
				facade.acknowledgements[item.ID] = previous
			} else {
				delete(facade.acknowledgements, item.ID)
			}
			return Envelope{}, failure("internal", "Cannot persist attention acknowledgement")
		}
		return envelope(item, item.Revision, "console_attention_journal", "partial"), nil
	}
	if traceErr != nil {
		return Envelope{}, failure("unavailable", "Attention evidence could not be read")
	}
	return Envelope{}, failure("not_found", "Attention item no longer exists")
}

func (facade *Facade) loadAcknowledgements() error {
	data, err := os.ReadFile(filepath.Join(facade.stateDir, "console-attention.json"))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil || json.Unmarshal(data, &facade.acknowledgements) != nil || facade.acknowledgements == nil {
		return failure("internal", "Cannot read console attention journal")
	}
	return nil
}

func (facade *Facade) saveAcknowledgements() error {
	data, err := json.Marshal(facade.acknowledgements)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(facade.stateDir, ".console-attention-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(file.Name(), filepath.Join(facade.stateDir, "console-attention.json")); err != nil {
		return err
	}
	directory, err := os.Open(facade.stateDir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
