package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/console"
	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	"jinx/internal/execution"
	"jinx/internal/observability"
)

func TestArchitectureRelayExecutionActionAndHistoricalSSE(t *testing.T) {
	for _, scenario := range []string{"confirmed", "none", "dry_run", "empty_route", "batch"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			var calls, posts, writes atomic.Int32
			provider := providerFunc(func(_ context.Context, request execution.ModelRequest) (string, error) {
				calls.Add(1)
				if request.ModelID != syntheticModel || len(request.Messages) == 0 || !strings.Contains(request.Messages[0].Content, "PRIVATE_ARCHITECTURE_INPUT") {
					t.Error("real runtimebridge did not pass synthetic input to execution")
				}
				if scenario == "none" {
					return "NONE", nil
				}
				return syntheticReply, nil
			})
			engine, runner := newExecution(t, root, nil, []execution.BackendOptions{{ID: "synthetic-primary", Kind: execution.TextOnly, Models: []execution.Model{{ID: syntheticModel, Label: "PRIVATE_MODEL_LABEL"}}, Provider: provider}}, executionConfig(execution.TextOnly, syntheticModel, 2))
			relay := newRelay(t, root)
			bridge := newArchitectureEchoBridge(t, relay, &posts)
			path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
			var config dispatcher.Config
			if err := json.Unmarshal(readFixtureFile(t, path), &config); err != nil {
				t.Fatal(err)
			}
			rule := config.Rules.GroupRules[sourceGroup]
			rule.DryRun = scenario == "dry_run"
			config.Rules.GroupRules[sourceGroup] = rule
			if scenario == "empty_route" {
				config.Rules.GroupRules = map[string]dispatcher.GroupRule{}
				config.Rules.GroupWhitelist = nil
			}
			if scenario == "batch" {
				config.Rules.BatchSize, config.Rules.IdleTimeoutSec, config.Rules.MaxBatchWaitSec = 2, 30, 30
			}
			writeJSON(t, path, config)
			services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes, runner)
			upstream, requests := architectureCountRequests(t, services.upstream.Config.Handler)
			server := newArchitectureGateway(t, root, upstream.URL, relay.URL, bridge.URL, engine, engine)
			var bootstrap console.Bootstrap
			gatewayGet(t, server, "/console/v1/bootstrap", &bootstrap)
			if bootstrap.Cursor == nil || bootstrap.StreamID == nil || bootstrap.LastSeq == nil || *bootstrap.LastSeq != 0 {
				t.Fatalf("HTTP bootstrap did not provide request view cursor: %+v", bootstrap)
			}
			var before console.ArchitectureSnapshot
			gatewayGet(t, server, "/console/v1/architecture", &before)
			assertArchitecturePosition(t, before, *bootstrap.StreamID, *bootstrap.Cursor, 0)
			id := "architecture-" + scenario
			if scenario == "empty_route" {
				last := services.dispatcher.HealthSnapshot().LastEventID
				postMessage(t, relay.Client(), relay.URL, id, "@Jinx PRIVATE_ARCHITECTURE_INPUT no route")
				await(t, "unrouted input cursor acknowledgement", func() bool { return services.dispatcher.HealthSnapshot().LastEventID != last })
				var snapshot observability.Snapshot
				requestJSON(t, services.upstream.Client(), http.MethodGet, services.upstream.URL+"/api/trace/snapshot", nil, &snapshot)
				page := architectureTraceEvents(t, services, *bootstrap.Cursor)
				if len(snapshot.Runs) != 0 || len(page.Events) != 0 || posts.Load() != 0 || calls.Load() != 0 {
					t.Fatalf("unrouted input fabricated execution/action: runs=%d events=%d model=%d bridge=%d", len(snapshot.Runs), len(page.Events), calls.Load(), posts.Load())
				}
				return
			}
			stream := openStream(t, server.Client(), server.URL, *bootstrap.Cursor, "")
			inputIDs := []string{id}
			if scenario == "batch" {
				inputIDs = []string{id + "-1", id + "-2"}
				postMessage(t, relay.Client(), relay.URL, inputIDs[0], "PRIVATE_ARCHITECTURE_INPUT first")
				postMessage(t, relay.Client(), relay.URL, inputIDs[1], "@Jinx PRIVATE_ARCHITECTURE_INPUT second")
			} else {
				postMessage(t, relay.Client(), relay.URL, id, "@Jinx PRIVATE_ARCHITECTURE_INPUT")
			}
			var runID string
			for {
				event := stream.read(t, 1)[0]
				if event.Phase == contracts.PhaseTerminal {
					runID = event.EntityID
					break
				}
			}
			stream.close()
			if scenario != "batch" {
				if view := waitRun(t, services, id); view.ID != runID {
					t.Fatal("Console run list disagrees with real run stream")
				}
			}
			raw := architectureRunDetail(t, services, runID)
			if err := contracts.ValidateRun(raw.Run); err != nil {
				t.Fatal(err)
			}
			result, err := runner.observed(runID)
			if err != nil || calls.Load() != 1 || len(result.Attempts) != 1 || result.Attempts[0].BackendID != "synthetic-primary" || result.Attempts[0].Status != "succeeded" {
				t.Fatalf("real execution/runtimebridge missing: result=%+v err=%v calls=%d", result, err, calls.Load())
			}
			if !reflect.DeepEqual(raw.Run.MessageIDs, inputIDs) {
				t.Fatalf("batch inputs changed: %v / %v", raw.Run.MessageIDs, inputIDs)
			}
			public := architectureConsoleRun(t, server, runID)
			assertArchitectureInputs(t, public, raw.Metadata, inputIDs)
			wantOutcome, wantDelivery, wantPosts := contracts.OutcomeReplied, contracts.DeliveryConfirmed, int32(1)
			if scenario == "none" {
				wantOutcome, wantDelivery, wantPosts = contracts.OutcomeSilent, contracts.DeliveryNotStarted, 0
			} else if scenario == "dry_run" {
				wantOutcome, wantDelivery, wantPosts = contracts.OutcomeSuppressed, contracts.DeliveryBlocked, 0
			}
			if raw.Run.Outcome != wantOutcome || raw.Run.Delivery != wantDelivery || posts.Load() != wantPosts {
				t.Fatalf("actual delivery mismatch: %s/%s bridge=%d", raw.Run.Outcome, raw.Run.Delivery, posts.Load())
			}
			if wantPosts == 0 {
				if raw.Metadata.Action != nil || public.Action != nil || raw.Run.DeliveryEvidence.SubmissionStartedAt != nil || !raw.Run.DeliveryEvidence.NoSubmission {
					t.Fatal("NONE/dry-run fabricated submitted Action")
				}
			} else {
				if raw.Metadata.Action == nil || raw.Metadata.Action.ActionID != raw.Run.DeliveryEvidence.BridgeRequestID || raw.Metadata.Action.Platform != contracts.PlatformWeChat || raw.Metadata.Action.Kind != contracts.ActionReplyText || !reflect.DeepEqual(public.Action, raw.Metadata.Action) {
					t.Errorf("committed bridge ActionRef missing: %+v / %+v", raw.Metadata.Action, public.Action)
				}
				assertArchitectureEchoReceipt(t, raw.Run, public)
			}
			page := architectureTraceEvents(t, services, *bootstrap.Cursor)
			if !reflect.DeepEqual(page.Events, raw.Events) || len(page.Events) < 2 {
				t.Fatal("global trace/history disagree")
			}
			readsBefore := requests.snapshot()
			frames := readArchitectureFrames(t, server, *bootstrap.Cursor, "", len(page.Events))
			assertArchitectureHistory(t, page.Events, frames, raw.Metadata, scenario == "batch")
			middle := len(frames) / 2
			resumed := readArchitectureFrames(t, server, *bootstrap.Cursor, frames[middle-1].Cursor, len(frames)-middle)
			if !reflect.DeepEqual(resumed, frames[middle:]) {
				t.Fatal("architecture Last-Event-ID changed/lost historical events")
			}
			for path, count := range requests.snapshot() {
				if path != "GET /api/trace/events" && count != readsBefore[path] {
					t.Errorf("SSE looked up current run/config instead of persisted Event: %s", path)
				}
			}
			var after console.ArchitectureSnapshot
			gatewayGet(t, server, "/console/v1/architecture", &after)
			assertArchitecturePosition(t, after, *bootstrap.StreamID, page.Cursor, uint64(len(page.Events)))
			if scenario == "confirmed" || scenario == "batch" {
				assertArchitectureClient(t, server, frames)
			}
			if scenario == "confirmed" {
				server.Close()
				upstream.Close()
				services.close()
				restarted := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes, runner)
				if reopened := architectureRunDetail(t, restarted, runID); !reflect.DeepEqual(raw, reopened) {
					t.Fatal("reopening real trace changed persisted input/action/history")
				}
				reopenedServer := newArchitectureGateway(t, filepath.Join(root, "reopened"), restarted.upstream.URL, relay.URL, bridge.URL, engine, engine)
				replayed := readArchitectureFrames(t, reopenedServer, *bootstrap.Cursor, "", len(frames))
				if !reflect.DeepEqual(frames, replayed) || calls.Load() != 1 || posts.Load() != 1 {
					t.Fatal("reopened SSE changed past correlation or replayed execution/action")
				}
			}
		})
	}
}

func TestArchitectureLegacyTraceKeepsEmptyCorrelations(t *testing.T) {
	root := t.TempDir()
	relay := newRelay(t, root)
	var posts atomic.Int32
	bridge := newBridge(t, relay.Client(), relay.URL, &posts, make(chan bridgeDelivery, 1))
	path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
	store, err := observability.Open(filepath.Join(root, "logs", "trace.json"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	initial, err := store.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	run, err := store.CreateRun(contracts.Run{
		ID: "legacy-architecture-run", SourceGroupID: sourceGroup, TargetGroupID: targetGroup, MessageIDs: []string{"legacy-message"},
		AcceptedAt: now, UpdatedAt: now, LastProgressAt: now, Trigger: "mention",
		AdmissionWatermark: contracts.Watermark{Token: "legacy-watermark", RecordedAt: now},
		Phase:              contracts.PhaseAccepted, Outcome: contracts.OutcomePending, Delivery: contracts.DeliveryNotStarted,
		Completeness: contracts.Partial, Versions: contracts.Versions{Dispatcher: "legacy", Fingerprint: "legacy"}, MissingSnapshotReason: "not_retained",
	})
	if err != nil {
		t.Fatal(err)
	}
	run.Phase, run.Outcome, run.ReasonCode = contracts.PhaseTerminal, contracts.OutcomeSilent, "output_none"
	run.DeliveryEvidence.NoSubmission = true
	run.UpdatedAt, run.LastProgressAt = now.Add(time.Millisecond), now.Add(time.Millisecond)
	if _, err := store.UpdateRun(run, run.Revision, "run_terminal"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	dispatch, err := dispatcher.NewDispatcher(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dispatch.Close)
	upstream := httptest.NewServer(dispatch.Handler())
	t.Cleanup(upstream.Close)
	services := &stack{upstream: upstream}
	server := newArchitectureGateway(t, root, upstream.URL, relay.URL, bridge.URL, nil, nil)
	var snapshot console.ArchitectureSnapshot
	gatewayGet(t, server, "/console/v1/architecture", &snapshot)
	page := architectureTraceEvents(t, services, initial.Cursor)
	assertArchitecturePosition(t, snapshot, initial.StreamID, page.Cursor, 2)
	public := architectureConsoleRun(t, server, run.ID)
	if len(public.Inputs) != 0 || public.Action != nil {
		t.Fatalf("old trace fabricated input/action: %+v", public)
	}
	frames := readArchitectureFrames(t, server, initial.Cursor, "", len(page.Events))
	assertArchitectureHistory(t, page.Events, frames, architectureRun{}, false)
	for _, frame := range frames {
		if frame.Event.Source != nil || frame.Event.EnvelopeID != "" || frame.Event.Action != nil {
			t.Fatalf("legacy trace manufactured historical correlation: %+v", frame)
		}
	}
	assertArchitectureUnsupportedDomains(t, server, snapshot.EventStreams[1].Cursor)
}

func TestArchitectureLayerSnapshotUsesEngineWithoutModelProbes(t *testing.T) {
	root := t.TempDir()
	var modelRequests, posts, writes atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		modelRequests.Add(1)
		t.Errorf("architecture read contacted model: %s %s", request.Method, request.URL.Path)
		http.NotFound(writer, request)
	}))
	t.Cleanup(model.Close)
	engine, err := execution.Open(filepath.Join(root, "execution.json"), execution.Options{Backends: []execution.BackendOptions{
		{ID: "catalog-personal", Kind: execution.AlmaPersonal, AlmaBaseURL: model.URL, Models: []execution.Model{{ID: "catalog-model", Label: "PRIVATE_MODEL_LABEL"}}},
		{ID: "catalog-unavailable", Kind: execution.TextOnly, Models: []execution.Model{{ID: "offline-model", Label: "Offline"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	relay := newRelay(t, root)
	bridge := newBridge(t, relay.Client(), relay.URL, &posts, make(chan bridgeDelivery, 1))
	path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
	services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes)
	dispatcherRead, dispatcherRequests := architectureCountRequests(t, services.upstream.Config.Handler)
	relayRead, relayRequests := architectureCountRequests(t, relay.Config.Handler)
	bridgeRead, bridgeRequests := architectureCountRequests(t, bridge.Config.Handler)
	catalog := &architectureCatalog{engine: engine}
	server := newArchitectureGateway(t, root, dispatcherRead.URL, relayRead.URL, bridgeRead.URL, engine, catalog)
	if modelRequests.Load() != 0 || catalog.reads.Load() != 0 || len(dispatcherRequests.snapshot()) != 0 || len(relayRequests.snapshot()) != 0 || len(bridgeRequests.snapshot()) != 0 {
		t.Fatal("construction contacted an upstream or read execution catalog")
	}
	var snapshot console.ArchitectureSnapshot
	gatewayGet(t, server, "/console/v1/architecture", &snapshot)
	if catalog.reads.Load() != 1 || modelRequests.Load() != 0 || posts.Load() != 0 || writes.Load() != 0 {
		t.Fatalf("read side effects: catalog=%d model=%d bridge=%d config=%d", catalog.reads.Load(), modelRequests.Load(), posts.Load(), writes.Load())
	}
	wantDispatcherRequests := map[string]int{"GET /health": 1, "GET /api/trace/snapshot": 1, "GET /api/trace/events": 1}
	wantPlatformRequests := map[string]int{"GET /health": 1}
	if !reflect.DeepEqual(dispatcherRequests.snapshot(), wantDispatcherRequests) || !reflect.DeepEqual(relayRequests.snapshot(), wantPlatformRequests) || !reflect.DeepEqual(bridgeRequests.snapshot(), wantPlatformRequests) {
		t.Fatalf("architecture performed unaccounted probes: dispatcher=%v relay=%v bridge=%v", dispatcherRequests.snapshot(), relayRequests.snapshot(), bridgeRequests.snapshot())
	}
	public, err := json.Marshal(snapshot)
	if err != nil || strings.Contains(string(public), "PRIVATE_MODEL_LABEL") || strings.Contains(string(public), model.URL) || strings.Contains(string(public), root) || len(snapshot.Processing.Agents) != 0 {
		t.Fatalf("catalog/health snapshot leaked private fields or invented agents: %s %v", public, err)
	}
	wantLayers := []contracts.EventDomain{contracts.DomainSource, contracts.DomainProcessing, contracts.DomainAction}
	if !reflect.DeepEqual(snapshot.Layers, wantLayers) || snapshot.Revision == "" || snapshot.ObservedAt.IsZero() || len(snapshot.Sources) != 3 || len(snapshot.Actions) != 12 {
		t.Fatalf("incomplete three-layer snapshot: %+v", snapshot)
	}
	var backends []execution.Backend
	gatewayGet(t, server, "/console/v1/backends", &backends)
	if len(snapshot.Processing.Backends) != len(backends) || len(backends) != 2 {
		t.Fatalf("architecture differs from Engine catalog: %+v / %+v", snapshot.Processing.Backends, backends)
	}
	for index, backend := range snapshot.Processing.Backends {
		actual := backends[index]
		if backend.ID != actual.ID || backend.Kind != actual.Kind || backend.Available != actual.Available || backend.AgentBinding != actual.AgentBinding || len(backend.ModelIDs) != 1 || backend.ModelIDs[0] != actual.Models[0].ID {
			t.Fatalf("architecture did not use Engine backend: %+v / %+v", backend, actual)
		}
		for _, capability := range backend.Capabilities {
			if err := contracts.ValidateCapabilityEvidence(capability); err != nil {
				t.Fatal(err)
			}
			if actual.Kind == execution.AlmaPersonal && capability.Enforcement == contracts.EnforcementSoft && capability.Supported {
				t.Fatal("soft permission advertised as enforced")
			}
		}
	}
	for _, source := range snapshot.Sources {
		if source.Platform == contracts.PlatformWeChat {
			if source.State != "ready" || !architectureCapability(t, source.Capabilities, "ingest").Supported {
				t.Fatalf("real Relay health missing: %+v", source)
			}
		} else if source.State != "unsupported" || architectureCapability(t, source.Capabilities, "ingest").Reason != contracts.ReasonUnsupported {
			t.Fatalf("unsupported source advertised: %+v", source)
		}
	}
	for _, action := range snapshot.Actions {
		send := architectureCapability(t, action.Capabilities, "send")
		if action.Platform == contracts.PlatformWeChat && action.Kind == contracts.ActionReplyText {
			if action.State != "ready" || !send.Supported || send.Source != contracts.CapabilityBridgeHealth {
				t.Fatalf("fake bridge health missing: %+v", action)
			}
		} else if action.State != "unsupported" || send.Supported || send.Reason != contracts.ReasonUnsupported {
			t.Fatalf("unsupported action advertised: %+v", action)
		}
	}
	if capability := architectureCapability(t, snapshot.Processing.Capabilities, "backend_catalog"); !capability.Supported || capability.Source != contracts.CapabilityExecution {
		t.Fatalf("catalog provenance missing: %+v", capability)
	}
	if catalog.reads.Load() != 1 || modelRequests.Load() != 0 {
		t.Fatal("Engine catalog read probed model")
	}
	assertArchitectureUnsupportedDomains(t, server, snapshot.EventStreams[1].Cursor)
	if !reflect.DeepEqual(dispatcherRequests.snapshot(), wantDispatcherRequests) || !reflect.DeepEqual(relayRequests.snapshot(), wantPlatformRequests) || !reflect.DeepEqual(bridgeRequests.snapshot(), wantPlatformRequests) {
		t.Fatal("unsupported domain requests performed platform/trace discovery")
	}
	t.Logf("construction upstream/model/catalog reads=0; snapshot dispatcher=3 relay=1 bridge=1 catalog=1 model=0; unsupported domains add no reads")
}

func TestArchitectureLayerUnreachableIsNotUnsupported(t *testing.T) {
	root := t.TempDir()
	relay := newRelay(t, root)
	var posts, writes atomic.Int32
	bridge := newBridge(t, relay.Client(), relay.URL, &posts, make(chan bridgeDelivery, 1))
	path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
	services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes)
	unreachable := httptest.NewServer(http.NotFoundHandler())
	endpoint := unreachable.URL
	unreachable.Close()
	server := newArchitectureGateway(t, root, services.upstream.URL, endpoint, endpoint, nil, nil)
	var snapshot console.ArchitectureSnapshot
	gatewayGet(t, server, "/console/v1/architecture", &snapshot)
	for _, source := range snapshot.Sources {
		if source.Platform == contracts.PlatformWeChat {
			capability := architectureCapability(t, source.Capabilities, "ingest")
			if source.State != "unreachable" || !capability.Supported || capability.Reason != contracts.ReasonUnavailable || capability.Source != contracts.CapabilityRelayHealth || !source.Availability.Supported || source.Availability.Enabled {
				t.Fatalf("unreachable Relay confused with unimplemented adapter: %+v", source)
			}
		}
	}
	for _, action := range snapshot.Actions {
		if action.Platform == contracts.PlatformWeChat && action.Kind == contracts.ActionReplyText {
			capability := architectureCapability(t, action.Capabilities, "send")
			if action.State != "unreachable" || !capability.Supported || capability.Reason != contracts.ReasonUnavailable || capability.Source != contracts.CapabilityBridgeHealth || !action.Availability.Supported || action.Availability.Enabled {
				t.Fatalf("unreachable bridge confused with unimplemented action: %+v", action)
			}
		}
	}
	if !snapshot.EventStreams[1].Supported {
		t.Fatal("platform outage disabled reachable Dispatcher trace")
	}
}

type architectureCatalog struct {
	engine *execution.Engine
	reads  atomic.Int32
}

func (catalog *architectureCatalog) Catalog(ctx context.Context) ([]execution.Backend, error) {
	catalog.reads.Add(1)
	return catalog.engine.Catalog(ctx)
}

func architectureCapability(t *testing.T, capabilities []contracts.CapabilityEvidence, id string) contracts.CapabilityEvidence {
	t.Helper()
	for _, capability := range capabilities {
		if capability.ID == id {
			return capability
		}
	}
	t.Fatalf("missing capability %s: %+v", id, capabilities)
	return contracts.CapabilityEvidence{}
}
