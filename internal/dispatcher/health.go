package dispatcher

import (
	"encoding/json"
	"net/http"
	"time"
)

type Health struct {
	Capabilities    map[string]TraceCapability `json:"capabilities"`
	Service         string                     `json:"service"`
	Status          string                     `json:"status"`
	ProtocolVersion int                        `json:"protocol_version"`
	StartedAt       time.Time                  `json:"started_at"`
	ObservedAt      time.Time                  `json:"observed_at"`
	RelayConnected  bool                       `json:"relay_connected"`
	LastEventID     string                     `json:"last_event_id"`
	Sequence        uint64                     `json:"sequence"`
	LastProgressAt  *time.Time                 `json:"last_progress_at"`
	PendingMessages int                        `json:"pending_messages"`
	ExecutingGroups int                        `json:"executing_groups"`
	AwaitingEcho    bool                       `json:"awaiting_echo"`
}

type TraceCapability struct {
	Supported bool `json:"supported"`
	ReadOnly  bool `json:"readOnly"`
}

func (d *Dispatcher) HealthSnapshot() Health {
	d.mu.Lock()
	defer d.mu.Unlock()
	health := Health{
		Service: "dispatcher", Status: "ready", ProtocolVersion: 1,
		StartedAt: d.startedAt, ObservedAt: time.Now(),
		RelayConnected: d.state.Stats.RelayConnected,
		LastEventID:    d.state.LastEventID, Sequence: d.state.Sequence,
		AwaitingEcho: d.awaitingEcho != nil,
		Capabilities: map[string]TraceCapability{"trace": {Supported: d.trace != nil, ReadOnly: true}},
	}
	if d.closed || d.ctx.Err() != nil || d.traceFailed.Load() {
		health.Status = "not_ready"
	}
	if !d.lastProgressAt.IsZero() {
		last := d.lastProgressAt
		health.LastProgressAt = &last
	}
	for _, group := range d.state.Groups {
		health.PendingMessages += len(group.Pending) + len(group.InFlight)
		if group.Executing {
			health.ExecutingGroups++
		}
	}
	return health
}

func (d *Dispatcher) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	health := d.HealthSnapshot()
	w.Header().Set("Content-Type", "application/json")
	if health.Status != "ready" {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(health)
}
