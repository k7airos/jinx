package console

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"jinx/internal/observability"
)

func (facade *Facade) Services(ctx context.Context) Envelope {
	items := []ServiceView{}
	for _, id := range []string{"relay", "dispatcher", "decision", "bridge"} {
		started := time.Now()
		view := ServiceView{ID: id, Process: "unknown", Health: "unknown", Facts: map[string]any{}, Actions: map[string]Availability{"start": unsupported(), "stop": unsupported(), "restart": unsupported()}}
		data, _, status, err := facade.backend.request(ctx, id, http.MethodGet, "/health", "", nil)
		view.HealthObservedAt, view.HealthLatencyMS = time.Now().UTC(), time.Since(started).Milliseconds()
		switch {
		case facade.backend.bases[id] == "" || status == 404 || status == 405:
			view.Health = "unsupported"
		case err != nil:
			view.Health = "unreachable"
		default:
			var raw map[string]any
			if json.Unmarshal(data, &raw) == nil {
				for _, key := range []string{"status", "protocol_version", "group_routing", "serial_ui", "accessibility_trusted", "screen_capture_authorized", "dry_run", "relay_connected", "upstream_connected", "sequence", "pending_messages", "executing_groups", "awaiting_echo", "sse_clients", "websocket_clients"} {
					value := raw[key]
					view.Facts[key] = nil
					switch key {
					case "status":
						if text, ok := value.(string); ok && len(text) <= 64 {
							view.Facts[key] = text
						}
					case "protocol_version":
						if number, ok := value.(float64); ok {
							view.Facts[key] = number
						} else if text, ok := value.(string); ok && len(text) <= 32 {
							view.Facts[key] = text
						}
					case "sequence", "pending_messages", "executing_groups", "sse_clients", "websocket_clients":
						if number, ok := value.(float64); ok && number >= 0 {
							view.Facts[key] = number
						}
					default:
						if flag, ok := value.(bool); ok {
							view.Facts[key] = flag
						}
					}
				}
				for key, target := range map[string]**time.Time{"started_at": &view.StartedAt, "last_progress_at": &view.LastProgressAt} {
					if text, ok := raw[key].(string); ok {
						if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
							*target = &parsed
						}
					}
				}
				if status == 503 {
					view.Health = "not_ready"
				} else if status == 200 {
					switch raw["status"] {
					case "ok", "ready", "healthy":
						view.Health = "ready"
					case "degraded", "not_ready", "error":
						view.Health = "not_ready"
					}
				}
			}
		}
		if id == "bridge" && view.Health == "ready" {
			// Match the Dispatcher send boundary; a healthy process may still be
			// unable to send. Missing evidence is unknown, never an enabled action.
			if version, known := view.Facts["protocol_version"].(float64); known {
				if version < 2 || version != float64(int64(version)) {
					view.Health = "not_ready"
				}
			} else {
				view.Health = "unknown"
			}
			if dryRun, known := view.Facts["dry_run"].(bool); known {
				if dryRun {
					view.Health = "not_ready"
				}
			} else if view.Health == "ready" {
				view.Health = "unknown"
			}
			for _, key := range []string{"group_routing", "serial_ui", "accessibility_trusted", "screen_capture_authorized"} {
				if value, known := view.Facts[key].(bool); known && !value {
					view.Health = "not_ready"
				} else if !known && view.Health == "ready" {
					view.Health = "unknown"
				}
			}
		}
		if (id == "relay" || id == "dispatcher") && view.Health == "ready" && view.Facts["protocol_version"] != float64(1) && view.Facts["protocol_version"] != "1" {
			view.Health = "unknown"
		}
		items = append(items, view)
	}
	result := envelope(items, "", "read_only_health_probes", "partial")
	for _, item := range items {
		if item.Health == "unreachable" || item.Health == "unknown" {
			result.Freshness = "unknown"
		}
	}
	return result
}

func (facade *Facade) Overview(ctx context.Context) Envelope {
	snapshot, err := facade.backend.traceSnapshot(ctx)
	return facade.overview(ctx, snapshot, err)
}

func (facade *Facade) overview(ctx context.Context, snapshot observability.Snapshot, traceErr error) Envelope {
	stats := map[string]any{}
	data, _, status, err := facade.backend.request(ctx, "dispatcher", http.MethodGet, "/api/stats", "", nil)
	if err == nil && status == http.StatusOK {
		var raw map[string]any
		if json.Unmarshal(data, &raw) == nil {
			for _, field := range []string{"total_received", "total_triggered", "total_submitted", "total_replied", "total_dry_run", "total_send_failed", "total_unknown", "replay_gaps", "pending_messages", "relay_connected"} {
				switch value := raw[field].(type) {
				case bool:
					if field == "relay_connected" {
						stats[field] = value
					}
				case float64:
					if field != "relay_connected" && value >= 0 {
						stats[field] = value
					}
				}
			}
		}
	}
	recentRuns := []RunView{}
	if traceErr == nil {
		recentRuns = sortedRuns(snapshot)
		if len(recentRuns) > 10 {
			recentRuns = recentRuns[:10]
		}
	}
	legacy := map[string]any{}
	for _, name := range []string{"total_received", "total_triggered", "total_submitted", "total_replied", "total_dry_run", "total_send_failed", "total_unknown", "replay_gaps", "pending_messages"} {
		legacy[name] = stats[name]
	}
	result := envelope(map[string]any{"stats": stats, "legacyCounters": legacy, "relayConnected": stats["relay_connected"], "currentWork": stageCounts(snapshot, traceErr), "workSource": "retained_trace_snapshot", "attention": facade.attention(snapshot, traceErr), "statsSource": "persistent_cumulative", "statsAvailable": len(stats) > 0, "pendingMeaning": "pending_and_in_flight_messages", "services": facade.Services(ctx).Data, "controls": unsupported(), "recentRuns": recentRuns, "trace": traceState(snapshot, traceErr)}, "", "dispatcher_stats_health_and_trace", "partial")
	if len(stats) == 0 {
		result.Freshness = "unknown"
	}
	return result
}

func Capabilities() map[string]Availability {
	capabilities := map[string]Availability{}
	for _, name := range []string{"health", "rules.read", "rules.plan", "rules.publish", "changes.read", "rollback", "attention.acknowledge", "catalog"} {
		capabilities[name] = Availability{Supported: true, Enabled: true}
	}
	for _, name := range []string{"runs", "events.resume", "traceRuns", "resumableRunEvents", "configSnapshots", "relay.rules", "pause", "resume", "maintenance", "cancel", "service.actions", "routing.edit", "permissions.edit", "connectors"} {
		capabilities[name] = unsupported()
	}
	return capabilities
}
