package gateway

import (
	"context"
	"net/http"
	"strings"

	"jinx/internal/console"
	"jinx/internal/dispatcher"
	"jinx/internal/execution"
	"jinx/internal/supervisor"
)

type Options struct {
	Console       *console.Facade
	Execution     *execution.Engine
	Supervisor    *supervisor.Manager
	DispatcherURL string
	HTTPClient    *http.Client
}

type Gateway struct {
	options    Options
	dispatcher *DispatcherDrainCoordinator
}

type ExecutionPlanInput struct {
	ExpectedRevision string           `json:"expectedRevision"`
	Config           execution.Config `json:"config"`
}

type ExecutionApplyInput struct {
	ExpectedRevision string           `json:"expectedRevision"`
	RequestID        string           `json:"requestId"`
	Config           execution.Config `json:"config"`
}

type ServicePlanInput struct {
	ExpectedRevision string               `json:"expectedRevision"`
	Kind             supervisor.Kind      `json:"kind"`
	ServiceID        supervisor.ServiceID `json:"serviceId"`
}

func New(options Options) (*Gateway, error) {
	gateway := &Gateway{options: options}
	if options.DispatcherURL != "" {
		coordinator, err := NewDispatcherDrainCoordinator(options.DispatcherURL, options.HTTPClient)
		if err != nil {
			return nil, err
		}
		gateway.dispatcher = coordinator
	}
	return gateway, nil
}

func (gateway *Gateway) Handler() http.Handler {
	var consoleHandler http.Handler
	if gateway.options.Console != nil {
		consoleHandler = gateway.options.Console.Handler()
	}
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Cache-Control", "no-store")
		writer.Header().Set("X-Content-Type-Options", "nosniff")
		writer.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		if err := guard(request); err != nil {
			writeResult(writer, nil, err)
			return
		}
		path, prefixed := strings.CutPrefix(request.URL.Path, "/console/v1")
		if !prefixed || !strings.HasPrefix(path, "/") {
			writeResult(writer, nil, failure(http.StatusNotFound, "not_found"))
			return
		}
		method := routeMethod(path)
		if method == "" {
			if consoleHandler != nil {
				consoleHandler.ServeHTTP(writer, request)
			} else {
				writeResult(writer, nil, failure(http.StatusNotImplemented, "unsupported"))
			}
			return
		}
		if request.Method != method {
			writer.Header().Set("Allow", method)
			writeResult(writer, nil, failure(http.StatusMethodNotAllowed, "method_not_allowed"))
			return
		}
		if request.URL.RawQuery != "" || request.URL.ForceQuery {
			writeResult(writer, nil, failure(http.StatusBadRequest, "validation"))
			return
		}
		value, err := gateway.handle(writer, request, path)
		writeResult(writer, value, err)
	})
}

func routeMethod(path string) string {
	switch path {
	case "/bootstrap", "/controls", "/backends", "/execution/snapshot", "/services/runtime":
		return http.MethodGet
	case "/controls/actions", "/execution/plan", "/execution/apply", "/services/plan", "/services/execute":
		return http.MethodPost
	}
	if strings.HasPrefix(path, "/controls/operations/") || strings.HasPrefix(path, "/services/operations/") || strings.HasPrefix(path, "/groups/") && (strings.HasSuffix(path, "/routing") || strings.HasSuffix(path, "/permissions")) {
		return http.MethodGet
	}
	if strings.HasPrefix(path, "/services/maintenance/") && (strings.HasSuffix(path, "/continue") || strings.HasSuffix(path, "/cancel")) {
		return http.MethodPost
	}
	return ""
}

func (gateway *Gateway) handle(writer http.ResponseWriter, request *http.Request, path string) (any, error) {
	ctx := request.Context()
	if path == "/bootstrap" {
		return gateway.bootstrap(ctx), nil
	}
	if strings.HasPrefix(path, "/controls") {
		if gateway.dispatcher == nil {
			return nil, failure(http.StatusNotImplemented, "unsupported")
		}
		switch path {
		case "/controls":
			return gateway.dispatcher.Controls(ctx)
		case "/controls/actions":
			var input dispatcher.ControlRequest
			if err := decodeInput(writer, request, &input); err != nil {
				return nil, err
			}
			return gateway.dispatcher.Action(ctx, input)
		default:
			return gateway.dispatcher.Operation(ctx, strings.TrimPrefix(path, "/controls/operations/"))
		}
	}
	if path == "/backends" || strings.HasPrefix(path, "/execution/") || strings.HasPrefix(path, "/groups/") {
		if gateway.options.Execution == nil {
			return nil, failure(http.StatusNotImplemented, "unsupported")
		}
		engine := gateway.options.Execution
		switch path {
		case "/backends":
			return engine.Catalog(ctx)
		case "/execution/snapshot":
			return engine.Snapshot(ctx)
		case "/execution/plan":
			var input ExecutionPlanInput
			if err := decodeInput(writer, request, &input); err != nil {
				return nil, err
			}
			return engine.Plan(ctx, input.ExpectedRevision, input.Config)
		case "/execution/apply":
			var input ExecutionApplyInput
			if err := decodeInput(writer, request, &input); err != nil {
				return nil, err
			}
			return engine.ApplyConfig(ctx, input.ExpectedRevision, input.RequestID, input.Config)
		default:
			id, suffix, _ := strings.Cut(strings.TrimPrefix(path, "/groups/"), "/")
			if id == "" || strings.ContainsAny(id, "\\\x00\r\n") || suffix != "routing" && suffix != "permissions" {
				return nil, failure(http.StatusBadRequest, "validation")
			}
			group, err := engine.GetGroup(id)
			if err != nil {
				return nil, err
			}
			if suffix == "routing" {
				return map[string]any{"groupId": group.GroupID, "revision": group.Revision, "routing": group.Routing}, nil
			}
			return map[string]any{"groupId": group.GroupID, "revision": group.Revision, "policy": group.Policy, "permissions": group.Permissions}, nil
		}
	}
	manager := gateway.options.Supervisor
	if manager == nil {
		return nil, failure(http.StatusNotImplemented, "unsupported")
	}
	switch path {
	case "/services/runtime":
		return manager.ListServices(ctx)
	case "/services/plan":
		var input ServicePlanInput
		if err := decodeInput(writer, request, &input); err != nil {
			return nil, err
		}
		return manager.Plan(ctx, input.Kind, input.ServiceID, input.ExpectedRevision)
	case "/services/execute":
		var input supervisor.ExecuteInput
		if err := decodeInput(writer, request, &input); err != nil {
			return nil, err
		}
		return manager.Execute(ctx, input)
	}
	if strings.HasPrefix(path, "/services/operations/") {
		id := strings.TrimPrefix(path, "/services/operations/")
		if !safeID(id) {
			return nil, failure(http.StatusBadRequest, "validation")
		}
		return manager.GetOperation(ctx, id)
	}
	id, action, _ := strings.Cut(strings.TrimPrefix(path, "/services/maintenance/"), "/")
	if !safeID(id) || action != "cancel" && action != "continue" {
		return nil, failure(http.StatusBadRequest, "validation")
	}
	var input struct{}
	if err := decodeInput(writer, request, &input); err != nil {
		return nil, err
	}
	if action == "cancel" {
		return manager.CancelMaintenance(ctx, id)
	}
	return manager.ContinueMaintenance(ctx, id)
}

func (gateway *Gateway) bootstrap(ctx context.Context) console.Bootstrap {
	result := console.Bootstrap{Capabilities: console.Capabilities()}
	if gateway.options.Console != nil {
		result = gateway.options.Console.Bootstrap(ctx)
	} else {
		for name := range result.Capabilities {
			result.Capabilities[name] = console.Availability{Reason: "Console facade is not connected"}
		}
	}
	if gateway.dispatcher != nil {
		_, err := gateway.dispatcher.Controls(ctx)
		availability := console.Availability{Supported: true, Enabled: err == nil}
		if err != nil {
			availability.Reason = "Dispatcher control snapshot is unavailable"
		}
		for _, name := range []string{"controls.read", "pause", "resume", "maintenance", "cancel"} {
			result.Capabilities[name] = availability
		}
	}
	if gateway.options.Execution != nil {
		for _, name := range []string{"backends.read", "routing.read", "routing.edit", "permissions.read", "permissions.edit", "execution.snapshot", "execution.plan", "execution.apply"} {
			result.Capabilities[name] = console.Availability{Supported: true, Enabled: true}
		}
		result.Capabilities["execution.dispatch"] = console.Availability{Reason: "Dispatcher Runner integration and delivery gate must be connected separately"}
	}
	if gateway.options.Supervisor != nil {
		capabilities := gateway.options.Supervisor.ControlCapabilities(ctx)
		result.Capabilities["services.runtime"] = console.Availability{Supported: capabilities.Read.Supported, Enabled: capabilities.Read.Enabled}
		combined := console.Availability{Supported: true, Enabled: true}
		for kind, availability := range capabilities.Actions {
			view := console.Availability{Supported: availability.Supported, Enabled: availability.Enabled}
			if availability.Problem != nil {
				view.Reason = availability.Problem.Code
			}
			result.Capabilities["service."+string(kind)] = view
			combined.Supported = combined.Supported && view.Supported
			combined.Enabled = combined.Enabled && view.Enabled
			if view.Reason != "" {
				combined.Reason = view.Reason
			}
		}
		result.Capabilities["service.actions"] = combined
	}
	return result
}
