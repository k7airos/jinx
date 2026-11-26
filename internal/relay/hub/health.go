package hub

import (
	"fmt"
	"time"
)

type Health struct {
	Service           string     `json:"service"`
	Status            string     `json:"status"`
	ProtocolVersion   int        `json:"protocol_version"`
	StartedAt         time.Time  `json:"started_at"`
	ObservedAt        time.Time  `json:"observed_at"`
	UpstreamConnected bool       `json:"upstream_connected"`
	LastEventID       string     `json:"last_event_id"`
	Sequence          uint64     `json:"sequence"`
	LastProgressAt    *time.Time `json:"last_progress_at"`
	SSEClients        int        `json:"sse_clients"`
	WebSocketClients  int        `json:"websocket_clients"`
}

func (h *Hub) HealthSnapshot() Health {
	h.mu.RLock()
	defer h.mu.RUnlock()
	health := Health{
		Service: "relay", Status: "ready", ProtocolVersion: 1,
		StartedAt: h.startTime, ObservedAt: time.Now(),
		UpstreamConnected: h.upstreamConnected.Load(),
		LastEventID:       fmt.Sprintf("%s:%d", h.streamEpoch, h.streamSequence),
		Sequence:          h.streamSequence,
		SSEClients:        len(h.sseClients), WebSocketClients: len(h.wsClients),
	}
	if !h.lastProgressAt.IsZero() {
		last := h.lastProgressAt
		health.LastProgressAt = &last
	}
	return health
}
