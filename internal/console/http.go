package console

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"

	"jinx/internal/contracts"
)

func (facade *Facade) Handler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("X-Frame-Options", "DENY")
		writer.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		host, _, err := net.SplitHostPort(request.Host)
		if err != nil || !loopback(strings.Trim(host, "[]")) {
			writeResult(writer, nil, failure("forbidden", "Loopback same-origin access is required"))
			return
		}
		if fetchSite := request.Header.Get("Sec-Fetch-Site"); fetchSite != "" && fetchSite != "same-origin" && fetchSite != "none" {
			writeResult(writer, nil, failure("forbidden", "Loopback same-origin access is required"))
			return
		}
		if origin := request.Header.Get("Origin"); origin != "" {
			parsed, err := url.Parse(origin)
			if err != nil || parsed.Scheme != "http" || parsed.Host != request.Host || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
				writeResult(writer, nil, failure("forbidden", "Origin is not allowed"))
				return
			}
		}
		if request.Method == http.MethodPost {
			mediaType, _, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
			if err != nil || mediaType != "application/json" {
				writer.WriteHeader(http.StatusUnsupportedMediaType)
				_ = json.NewEncoder(writer).Encode(map[string]any{"ok": false, "problem": failure("validation", "application/json is required")})
				return
			}
		}
		var value any
		var problem error
		path := request.URL.Path
		switch {
		case request.Method == http.MethodGet && path == "/console/v1/architecture":
			if request.URL.RawQuery != "" || request.URL.ForceQuery {
				problem = failure("validation", "Architecture snapshot does not accept filters")
			} else {
				value = facade.Architecture(request.Context())
			}
		case request.Method == http.MethodGet && path == "/console/v1/architecture/events":
			facade.architectureEvents(writer, request)
			return
		case request.Method == http.MethodGet && path == "/console/v1/bootstrap":
			value = facade.Bootstrap(request.Context())
		case request.Method == http.MethodGet && path == "/console/v1/events":
			facade.events(writer, request)
			return
		case request.Method == http.MethodGet && path == "/console/v1/runs":
			value, problem = facade.ListRuns(request.Context(), request.URL.Query())
		case request.Method == http.MethodGet && strings.HasPrefix(path, "/console/v1/runs/"):
			value, problem = facade.GetRun(request.Context(), strings.TrimPrefix(path, "/console/v1/runs/"))
		case request.Method == http.MethodGet && path == "/console/v1/overview":
			value = facade.Overview(request.Context())
		case request.Method == http.MethodGet && path == "/console/v1/services":
			value = facade.Services(request.Context())
		case request.Method == http.MethodGet && path == "/console/v1/connectors":
			value = facade.ListConnectors(request.Context())
		case request.Method == http.MethodPost && strings.HasPrefix(path, "/console/v1/connectors/") && strings.HasSuffix(path, "/dry-run/plan"):
			platform := strings.TrimSuffix(strings.TrimPrefix(path, "/console/v1/connectors/"), "/dry-run/plan")
			var input ConnectorDryRunPlanInput
			problem = decodeInput(writer, request, &input)
			if problem == nil {
				value, problem = facade.PlanConnectorDryRun(request.Context(), contracts.Platform(platform), input)
			}
			if typed, ok := problem.(*Problem); ok {
				typed.RequestID = input.RequestID
			}
		case request.Method == http.MethodPost && strings.HasPrefix(path, "/console/v1/connectors/") && strings.HasSuffix(path, "/dry-run/apply"):
			platform := strings.TrimSuffix(strings.TrimPrefix(path, "/console/v1/connectors/"), "/dry-run/apply")
			var input ConnectorDryRunApplyInput
			problem = decodeInput(writer, request, &input)
			if problem == nil {
				value, problem = facade.ApplyConnectorDryRun(request.Context(), contracts.Platform(platform), input)
			}
			if typed, ok := problem.(*Problem); ok {
				typed.RequestID = input.RequestID
			}
		case request.Method == http.MethodGet && path == "/console/v1/catalog":
			value, problem = facade.GetCatalog(request.Context())
		case request.Method == http.MethodGet && path == "/console/v1/rules/global":
			value, problem = facade.GetGlobalRules(request.Context())
		case request.Method == http.MethodGet && (path == "/console/v1/rules/relay" || path == "/console/v1/relay/rules"):
			value, problem = facade.GetRelayRules(request.Context())
		case request.Method == http.MethodPost && (path == "/console/v1/rules/relay/plan" || path == "/console/v1/relay/rules/plan"):
			var input PlanInput
			problem = decodeInput(writer, request, &input)
			if problem == nil {
				for _, edit := range input.Edits {
					if edit.Kind != "relay_field" && edit.Kind != "group_collection" {
						problem = failure("validation", "Relay plans accept relay_field and group_collection edits only")
					}
				}
				if problem == nil {
					value, problem = facade.PlanChange(request.Context(), input)
				}
			}
			if typed, ok := problem.(*Problem); ok {
				typed.RequestID = input.RequestID
			}
		case request.Method == http.MethodGet && strings.HasPrefix(path, "/console/v1/config/drift/"):
			value, problem = facade.GetConfigurationDrift(request.Context(), strings.TrimPrefix(path, "/console/v1/config/drift/"))
		case request.Method == http.MethodGet && strings.HasPrefix(path, "/console/v1/config-snapshots/"):
			value, problem = facade.GetConfigSnapshot(request.Context(), strings.TrimPrefix(path, "/console/v1/config-snapshots/"))
		case request.Method == http.MethodGet && path == "/console/v1/attention":
			value = facade.ListAttention(request.Context())
		case request.Method == http.MethodPost && path == "/console/v1/attention/acknowledge":
			var input AcknowledgeInput
			problem = decodeInput(writer, request, &input)
			if problem == nil {
				value, problem = facade.AcknowledgeAttention(request.Context(), input)
			}
			if typed, ok := problem.(*Problem); ok {
				typed.RequestID = input.RequestID
			}
		case request.Method == http.MethodGet && path == "/console/v1/groups":
			value, problem = facade.ListGroups(request.Context())
		case request.Method == http.MethodGet && strings.HasPrefix(path, "/console/v1/groups/"):
			value, problem = facade.GetGroup(request.Context(), strings.TrimPrefix(path, "/console/v1/groups/"))
		case request.Method == http.MethodPost && path == "/console/v1/changes/plan":
			var input PlanInput
			problem = decodeInput(writer, request, &input)
			if problem == nil {
				value, problem = facade.PlanChange(request.Context(), input)
			}
			if typed, ok := problem.(*Problem); ok {
				typed.RequestID = input.RequestID
			}
		case request.Method == http.MethodPost && path == "/console/v1/changes/publish":
			var input PublishInput
			problem = decodeInput(writer, request, &input)
			if problem == nil {
				value, problem = facade.PublishChange(request.Context(), input)
			}
			if typed, ok := problem.(*Problem); ok {
				typed.RequestID = input.RequestID
			}
		case request.Method == http.MethodGet && path == "/console/v1/changes":
			value, problem = facade.QueryChanges(request.URL.Query())
		case request.Method == http.MethodPost && path == "/console/v1/changes/rollback/plan":
			var input RollbackInput
			problem = decodeInput(writer, request, &input)
			if problem == nil {
				value, problem = facade.PlanRollback(request.Context(), input)
			}
			if typed, ok := problem.(*Problem); ok {
				typed.RequestID = input.RequestID
			}
		case request.Method == http.MethodGet && strings.HasPrefix(path, "/console/v1/changes/"):
			value, problem = facade.GetChange(strings.TrimPrefix(path, "/console/v1/changes/"))
		case request.Method == http.MethodGet && strings.HasPrefix(path, "/console/v1/operations/by-request/"):
			value, problem = facade.GetOperationByRequest(request.Context(), strings.TrimPrefix(path, "/console/v1/operations/by-request/"))
		case request.Method == http.MethodGet && strings.HasPrefix(path, "/console/v1/operations/"):
			value, problem = facade.GetOperation(request.Context(), strings.TrimPrefix(path, "/console/v1/operations/"))
		case request.Method == http.MethodPost && path == "/console/v1/direct-deliver":
			var input DirectDeliverInput
			problem = decodeInput(writer, request, &input)
			if problem == nil {
				value, problem = facade.DirectDeliver(request.Context(), input)
			}
		default:
			problem = failure("unsupported", "This console route or method is not implemented")
		}
		writeResult(writer, value, problem)
	})
}

func decodeInput(writer http.ResponseWriter, request *http.Request, value any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxBody))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return failure("too_large", "Request exceeds 256 KiB")
		}
		return failure("validation", "Invalid JSON or unknown fields")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			return failure("too_large", "Request exceeds 256 KiB")
		}
		return failure("validation", "Exactly one JSON object is required")
	}
	return nil
}

func writeResult(writer http.ResponseWriter, value any, err error) {
	if err == nil {
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "value": value})
		return
	}
	var problem *Problem
	if !errors.As(err, &problem) {
		problem = failure("internal", "Console request failed")
	}
	status := http.StatusInternalServerError
	switch problem.Code {
	case "validation":
		status = http.StatusBadRequest
	case "conflict", "stale", "reset_required":
		status = http.StatusConflict
	case "not_found":
		status = http.StatusNotFound
	case "forbidden":
		status = http.StatusForbidden
	case "unsupported":
		status = http.StatusNotImplemented
	case "unavailable":
		status = http.StatusServiceUnavailable
	case "too_large":
		status = http.StatusRequestEntityTooLarge
	case "unknown":
		status = http.StatusAccepted
	}
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(map[string]any{"ok": false, "problem": problem})
}
