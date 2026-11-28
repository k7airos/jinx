package console

import (
	"context"
	"net/url"
	"regexp"
	"sort"
	"time"

	"jinx/internal/contracts"
	"jinx/internal/execution"
)

// ExecutionCatalogReader can be satisfied by execution.Engine. It reads only the
// operator catalog; it must not execute agents or discover platform containers.
type ExecutionCatalogReader interface {
	Catalog(context.Context) ([]execution.Backend, error)
}

type ArchitectureCapability = contracts.CapabilityEvidence

type SourceView struct {
	Platform     contracts.Platform       `json:"platform"`
	State        string                   `json:"state"`
	Availability Availability             `json:"availability"`
	AccountID    *string                  `json:"accountId"`
	ContainerIDs []string                 `json:"containerIds"`
	Capabilities []ArchitectureCapability `json:"capabilities"`
}

type ProcessingBackend struct {
	ID           string                   `json:"id"`
	Kind         execution.Kind           `json:"kind"`
	Available    bool                     `json:"available"`
	ModelIDs     []string                 `json:"modelIds"`
	AgentBinding string                   `json:"agentBinding"`
	Capabilities []ArchitectureCapability `json:"capabilities"`
}

type ProcessingAgent struct {
	ID string `json:"id"`
}

type ProcessingView struct {
	Agents       []ProcessingAgent        `json:"agents"`
	Backends     []ProcessingBackend      `json:"backends"`
	Capabilities []ArchitectureCapability `json:"capabilities"`
}

type ActionView struct {
	Platform     contracts.Platform       `json:"platform"`
	Kind         contracts.ActionKind     `json:"kind"`
	State        string                   `json:"state"`
	Availability Availability             `json:"availability"`
	Capabilities []ArchitectureCapability `json:"capabilities"`
}

type ArchitectureStream struct {
	Domain contracts.EventDomain `json:"domain"`
	ArchitectureCapability
	StreamID      *string    `json:"streamId"`
	Cursor        *string    `json:"cursor"`
	LastSeq       *uint64    `json:"lastSeq"`
	CoverageSince *time.Time `json:"coverageSince"`
}

type ArchitectureSnapshot struct {
	Revision     string                  `json:"revision"`
	ObservedAt   time.Time               `json:"observedAt"`
	Layers       []contracts.EventDomain `json:"layers"`
	Sources      []SourceView            `json:"sources"`
	Processing   ProcessingView          `json:"processing"`
	Actions      []ActionView            `json:"actions"`
	EventStreams []ArchitectureStream    `json:"eventStreams"`
}

func architectureCapability(id string, supported bool, enforcement contracts.Enforcement, source contracts.CapabilitySource, reason contracts.ReasonCode) ArchitectureCapability {
	if !supported && reason == "" {
		reason = contracts.ReasonUnverified
	}
	return ArchitectureCapability{ID: id, Supported: supported, Enforcement: enforcement, Source: source, Reason: reason}
}

func missingArchitectureCapability(id string) ArchitectureCapability {
	return architectureCapability(id, false, contracts.EnforcementUnverified, contracts.CapabilityConsole, contracts.ReasonUnsupported)
}

func architectureReason(reason string) contracts.ReasonCode {
	if reason == "" {
		return ""
	}
	value := contracts.ReasonCode(reason)
	probe := contracts.CapabilityEvidence{ID: "probe", Supported: true, Enforcement: contracts.EnforcementUnverified, Source: contracts.CapabilityConsole, Reason: value}
	if contracts.ValidateCapabilityEvidence(probe) != nil {
		return contracts.ReasonUpstreamRedacted
	}
	return value
}

func hostAvailability(configured bool, state string) Availability {
	if !configured {
		return Availability{Reason: string(contracts.ReasonUnsupported)}
	}
	value := Availability{Supported: true, Enabled: state == "ready"}
	if state == "unreachable" || state == "not_ready" {
		value.Reason = string(contracts.ReasonUnavailable)
	} else if !value.Enabled {
		value.Reason = string(contracts.ReasonUnverified)
	}
	return value
}

func (facade *Facade) Architecture(ctx context.Context) ArchitectureSnapshot {
	result := ArchitectureSnapshot{
		Layers:  []contracts.EventDomain{contracts.DomainSource, contracts.DomainProcessing, contracts.DomainAction},
		Sources: []SourceView{}, Actions: []ActionView{}, EventStreams: []ArchitectureStream{},
		Processing: ProcessingView{Agents: []ProcessingAgent{}, Backends: []ProcessingBackend{}, Capabilities: []ArchitectureCapability{}},
	}
	health := map[string]ServiceView{}
	for _, service := range facade.Services(ctx).Data.([]ServiceView) {
		health[service.ID] = service
	}
	resolved := facade.resolveConnectorMap(ctx)
	for _, platform := range append([]contracts.Platform{contracts.PlatformWeChat}, facade.listedConnectorPlatformsWith(resolved)...) {
		source := SourceView{Platform: platform, State: "unsupported", Availability: hostAvailability(false, ""), Capabilities: []ArchitectureCapability{missingArchitectureCapability("ingest"), missingArchitectureCapability("adapter_contract")}}
		var probed *ConnectorView
		if platform == contracts.PlatformWeChat {
			source.State = health["relay"].Health
			source.Availability = hostAvailability(facade.backend.bases["relay"] != "", source.State)
			source.Capabilities[0] = architectureCapability("ingest", source.Availability.Supported, contracts.EnforcementHost, contracts.CapabilityRelayHealth, contracts.ReasonCode(source.Availability.Reason))
		} else if entry, ok := resolved[platform]; ok && entry.origin != "" {
			conn := facade.probeConnectorResolved(ctx, platform, resolved)
			probed = &conn
			source.State = conn.State
			source.Availability = hostAvailability(true, conn.State)
			source.AccountID = conn.AccountID
			enabledContainers := make([]string, 0, len(conn.Targets))
			for _, target := range conn.Targets {
				if target.Enabled {
					enabledContainers = append(enabledContainers, target.ContainerID)
				}
			}
			if len(enabledContainers) > 0 {
				source.ContainerIDs = enabledContainers
			}
			source.Capabilities[0] = architectureCapability("ingest", true, contracts.EnforcementHost, contracts.CapabilitySourceAdapter, contracts.ReasonCode(source.Availability.Reason))
			if conn.State != "unreachable" {
				source.Capabilities[1] = architectureCapability("adapter_contract", true, contracts.EnforcementHost, contracts.CapabilitySourceAdapter, "")
			} else {
				source.Capabilities[1] = architectureCapability("adapter_contract", false, contracts.EnforcementUnverified, contracts.CapabilitySourceAdapter, contracts.ReasonUnavailable)
			}
		}
		result.Sources = append(result.Sources, source)
		for _, kind := range []contracts.ActionKind{contracts.ActionReplyText, contracts.ActionEditText, contracts.ActionDeleteContent, contracts.ActionThreadReply} {
			action := ActionView{Platform: platform, Kind: kind, State: "unsupported", Availability: hostAvailability(false, ""), Capabilities: []ArchitectureCapability{missingArchitectureCapability("send"), missingArchitectureCapability("durable_admission"), missingArchitectureCapability("confirmation"), missingArchitectureCapability("adapter_contract")}}
			if platform == contracts.PlatformWeChat && kind == contracts.ActionReplyText {
				action.State = health["bridge"].Health
				action.Availability = hostAvailability(facade.backend.bases["bridge"] != "", action.State)
				action.Capabilities[0] = architectureCapability("send", action.Availability.Supported, contracts.EnforcementHost, contracts.CapabilityBridgeHealth, contracts.ReasonCode(action.Availability.Reason))
			} else if probed != nil {
				if probed.State == "unreachable" {
					if kind == contracts.ActionReplyText {
						action.State = "unreachable"
						action.Availability = hostAvailability(true, "unreachable")
						action.Capabilities[0] = architectureCapability("send", true, contracts.EnforcementHost, contracts.CapabilityActionAdapter, contracts.ReasonUnavailable)
						action.Capabilities[3] = architectureCapability("adapter_contract", false, contracts.EnforcementUnverified, contracts.CapabilityActionAdapter, contracts.ReasonUnavailable)
					}
				} else {
					declared := false
					for _, supportedKind := range probed.Capabilities {
						if supportedKind == kind {
							declared = true
							break
						}
					}
					if declared {
						actionState := probed.State
						if probed.DryRun != nil && *probed.DryRun {
							actionState = "not_ready"
						}
						action.State = actionState
						action.Availability = hostAvailability(true, actionState)
						sendReason := contracts.ReasonCode(action.Availability.Reason)
						action.Capabilities[0] = architectureCapability("send", true, contracts.EnforcementHost, contracts.CapabilityActionAdapter, sendReason)
						action.Capabilities[1] = architectureCapability("durable_admission", true, contracts.EnforcementHost, contracts.CapabilityActionAdapter, sendReason)
						action.Capabilities[2] = architectureCapability("confirmation", false, contracts.EnforcementUnverified, contracts.CapabilityActionAdapter, contracts.ReasonUnverified)
						action.Capabilities[3] = architectureCapability("adapter_contract", true, contracts.EnforcementHost, contracts.CapabilityActionAdapter, "")
					}
				}
			}
			result.Actions = append(result.Actions, action)
		}
	}
	snapshot, traceErr := facade.backend.traceSnapshot(ctx)
	streamErr := traceErr
	if traceErr == nil {
		page, err := facade.backend.traceEvents(ctx, snapshot.Cursor, false)
		streamErr = err
		if streamErr == nil {
			for _, event := range page.Events {
				if _, err := architectureEvent(event); err != nil {
					streamErr = err
					break
				}
			}
		}
	}
	result.Processing.Capabilities = append(result.Processing.Capabilities,
		architectureCapability("run_snapshot", traceErr == nil, contracts.EnforcementHost, contracts.CapabilityDispatcher, contracts.ReasonUnavailable),
		missingArchitectureCapability("agent_catalog"), missingArchitectureCapability("allowlist_refs"),
		missingArchitectureCapability("skills_catalog"), missingArchitectureCapability("tools_catalog"), missingArchitectureCapability("knowledge_catalog"))
	if traceErr == nil {
		result.Processing.Capabilities[0].Reason = ""
	}
	for _, domain := range result.Layers {
		stream := ArchitectureStream{Domain: domain, ArchitectureCapability: missingArchitectureCapability("events")}
		if domain == contracts.DomainProcessing {
			stream.ArchitectureCapability = architectureCapability("events", streamErr == nil, contracts.EnforcementHost, contracts.CapabilityDispatcher, contracts.ReasonUnavailable)
			if streamErr == nil {
				stream.Reason = ""
				stream.StreamID, stream.Cursor, stream.LastSeq, stream.CoverageSince = &snapshot.StreamID, &snapshot.Cursor, &snapshot.LastSeq, &snapshot.CoverageSince
			}
		}
		result.EventStreams = append(result.EventStreams, stream)
	}
	catalogCapability := missingArchitectureCapability("backend_catalog")
	if facade.executionCatalog != nil {
		catalog, err := facade.executionCatalog.Catalog(ctx)
		if err == nil {
			backends, valid := publicArchitectureBackends(catalog)
			if valid {
				result.Processing.Backends = backends
				catalogCapability = architectureCapability("backend_catalog", true, contracts.EnforcementHost, contracts.CapabilityExecution, "")
			} else {
				catalogCapability.Reason = contracts.ReasonUnavailable
			}
		} else {
			catalogCapability.Reason = contracts.ReasonUnavailable
		}
	}
	result.Processing.Capabilities = append(result.Processing.Capabilities, catalogCapability)
	result.Revision = digest(result)
	result.ObservedAt = time.Now().UTC()
	return result
}

var publicModelID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_./:@+-]{0,255}$`)

func publicArchitectureBackends(catalog []execution.Backend) ([]ProcessingBackend, bool) {
	result := []ProcessingBackend{}
	if len(catalog) > 128 {
		return result, false
	}
	seen := map[string]bool{}
	for _, backend := range catalog {
		if safeToken(backend.ID) == "" || seen[backend.ID] || len(backend.Models) > 128 || len(backend.PermissionEvidence) > 32 {
			return nil, false
		}
		seen[backend.ID] = true
		if backend.Kind != execution.TextOnly && backend.Kind != execution.AlmaPersonal && backend.Kind != execution.LegacyHermes && backend.Kind != execution.AgyCLI {
			return nil, false
		}
		if backend.AgentBinding != "branch_only" && backend.AgentBinding != "unverified" {
			return nil, false
		}
		view := ProcessingBackend{ID: backend.ID, Kind: backend.Kind, Available: backend.Available, AgentBinding: backend.AgentBinding, ModelIDs: []string{}, Capabilities: []ArchitectureCapability{}}
		for _, model := range backend.Models {
			if publicModelID.MatchString(model.ID) {
				view.ModelIDs = append(view.ModelIDs, model.ID)
			}
		}
		for _, permission := range backend.PermissionEvidence {
			known := false
			for _, allowed := range execution.Capabilities() {
				known = known || permission.Capability == allowed
			}
			if !known {
				return nil, false
			}
			supported := backend.Available && permission.Effective != nil && *permission.Effective && permission.Enforcement == execution.Hard
			capability := architectureCapability(string(permission.Capability), supported, contracts.Enforcement(permission.Enforcement), contracts.CapabilityExecution, architectureReason(permission.Reason))
			if contracts.ValidateCapabilityEvidence(capability) != nil {
				return nil, false
			}
			view.Capabilities = append(view.Capabilities, capability)
		}
		result = append(result, view)
	}
	sort.Slice(result, func(first, second int) bool { return result[first].ID < result[second].ID })
	return result, true
}

func architectureQuery(query url.Values) error {
	for key, values := range query {
		if (key != "cursor" && key != "domain") || len(values) != 1 || values[0] == "" {
			return failure("validation", "Unknown or repeated architecture stream filter")
		}
	}
	switch query.Get("domain") {
	case "", "processing":
		return nil
	case "source", "action":
		return failure("unsupported", "No real adapter events are available for this domain")
	default:
		return failure("validation", "Invalid architecture event domain")
	}
}
