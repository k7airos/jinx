package console

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"jinx/internal/connector"
	"jinx/internal/contracts"
)

type stubConnectorExecutor struct {
	mu           sync.Mutex
	platform     contracts.Platform
	accountID    string
	name         string
	caps         []contracts.ActionKind
	ready        bool
	reason       string
	targets      []connector.HealthTarget
	lastPollAt   *time.Time
	lastPollErr  string
}

func (s *stubConnectorExecutor) Platform() contracts.Platform { return s.platform }
func (s *stubConnectorExecutor) AccountID() string            { return s.accountID }
func (s *stubConnectorExecutor) ConnectorName() string        { return s.name }
func (s *stubConnectorExecutor) Capabilities() []contracts.ActionKind {
	return append([]contracts.ActionKind(nil), s.caps...)
}
func (s *stubConnectorExecutor) Health(context.Context) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready, s.reason
}
func (s *stubConnectorExecutor) Status() connector.StatusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return connector.StatusSnapshot{
		Targets:           append([]connector.HealthTarget(nil), s.targets...),
		LastPollAt:        s.lastPollAt,
		LastPollErrorCode: s.lastPollErr,
	}
}
func (s *stubConnectorExecutor) Execute(_ context.Context, req connector.ActionRequest) (connector.ActionResponse, error) {
	return connector.ActionResponse{ActionID: req.ActionID, Status: "confirmed", ExternalID: "rpid-ok"}, nil
}

func consolePost(facade *Facade, path string, payload any) *httptest.ResponseRecorder {
	body, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8090/console/v1/"+path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	facade.Handler().ServeHTTP(rec, req)
	return rec
}

func TestConnectorsUnconfiguredAndBootstrapCapabilities(t *testing.T) {
	facade, _ := traceSetup(t)
	boot := facade.Bootstrap(context.Background())
	if boot.Capabilities["connectors"].Supported || boot.Capabilities["connectors"].Enabled {
		t.Fatalf("expected connectors capability unsupported when unset, got %+v", boot.Capabilities["connectors"])
	}
	res := consoleRequest(facade, "connectors")
	if res.Code != http.StatusOK {
		t.Fatalf("GET /connectors status=%d body=%s", res.Code, res.Body.String())
	}
	var env struct {
		OK    bool `json:"ok"`
		Value struct {
			Data      []ConnectorView `json:"data"`
			Freshness string          `json:"freshness"`
		} `json:"value"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &env); err != nil || !env.OK || len(env.Value.Data) != 2 {
		t.Fatalf("unexpected connectors envelope: %s %v", res.Body.String(), err)
	}
	if env.Value.Freshness != "fresh" {
		t.Fatalf("expected fresh when no connectors configured, got %s", env.Value.Freshness)
	}
	for _, item := range env.Value.Data {
		if item.Configured || item.State != "unsupported" || item.Availability.Supported {
			t.Fatalf("expected unconfigured unsupported view, got %+v", item)
		}
	}

	recPlan := consolePost(facade, "connectors/bilibili/dry-run/plan", ConnectorDryRunPlanInput{
		RequestID:      "req-unconfigured-1",
		ExpectedDryRun: true,
		TargetDryRun:   false,
	})
	if recPlan.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 unsupported when unconfigured, got %d %s", recPlan.Code, recPlan.Body.String())
	}
}

func TestArchitectureProjectsConnectorStatesAndPrivacy(t *testing.T) {
	polledAt := time.Date(2026, 11, 24, 6, 0, 0, 0, time.UTC)
	store, err := connector.OpenIdempotencyStore(filepath.Join(t.TempDir(), "idem.json"))
	if err != nil {
		t.Fatal(err)
	}
	exec := &stubConnectorExecutor{
		platform:   contracts.PlatformBilibili,
		accountID:  "bilibili-ego",
		name:       "bilibili-ego-connector",
		caps:       []contracts.ActionKind{contracts.ActionReplyText, contracts.ActionThreadReply},
		ready:      true,
		reason:     "/private/SECRET/raw-reason",
		lastPollAt: &polledAt,
		lastPollErr: "fetch_failed",
		targets: []connector.HealthTarget{
			{ContainerID: "bilibili:BV1enabled0001", Label: "Demo Video\nLine2", Enabled: true},
			{ContainerID: "bilibili:BV1disabled002", Label: "Disabled Video", Enabled: false},
			{ContainerID: "wechat:invalid", Label: "Wrong Platform", Enabled: true},
		},
	}
	connSrv := connector.NewServer(connector.ServerOptions{
		Executor: exec,
		Store:    store,
		DryRun:   true,
	})
	biliHTTP := httptest.NewServer(connSrv.Handler())
	defer biliHTTP.Close()

	// Unreachable web connector.
	deadWeb := httptest.NewServer(http.NotFoundHandler())
	deadWebURL := deadWeb.URL
	deadWeb.Close()

	facade, _ := traceSetup(t)
	facade.backend.bases["connector:bilibili"] = biliHTTP.URL
	facade.backend.bases["connector:web"] = deadWebURL

	// Bootstrap marks connectors supported without extra probes failing.
	boot := facade.Bootstrap(context.Background())
	if !boot.Capabilities["connectors"].Supported || !boot.Capabilities["connectors"].Enabled {
		t.Fatalf("expected connectors capability enabled, got %+v", boot.Capabilities["connectors"])
	}

	// GET /connectors validates targets, sanitizes reason/labels, preserves dry_run=true.
	connRec := consoleRequest(facade, "connectors")
	if connRec.Code != http.StatusOK || strings.Contains(connRec.Body.String(), "/private/SECRET") {
		t.Fatalf("connectors health check failed or leaked secret: %d %s", connRec.Code, connRec.Body.String())
	}
	var connOut struct {
		OK    bool `json:"ok"`
		Value struct {
			Data      []ConnectorView `json:"data"`
			Freshness string          `json:"freshness"`
		} `json:"value"`
	}
	if err := json.Unmarshal(connRec.Body.Bytes(), &connOut); err != nil {
		t.Fatal(err)
	}
	if connOut.Value.Freshness != "unknown" {
		t.Fatalf("expected unknown freshness with unreachable web connector, got %s", connOut.Value.Freshness)
	}
	bili := connOut.Value.Data[0]
	web := connOut.Value.Data[1]
	if !bili.Configured || bili.State != "ready" || !bili.Availability.Enabled || bili.DryRun == nil || !*bili.DryRun ||
		bili.Reason == nil || *bili.Reason != string(contracts.ReasonUpstreamRedacted) ||
		len(bili.Targets) != 2 || bili.Targets[0].ContainerID != "bilibili:BV1disabled002" ||
		bili.Targets[1].Label == nil || *bili.Targets[1].Label != "Demo Video Line2" {
		t.Fatalf("unexpected bilibili connector view: %+v", bili)
	}
	if !web.Configured || web.State != "unreachable" || !web.Availability.Supported || web.Availability.Enabled {
		t.Fatalf("unexpected web connector view: %+v", web)
	}

	// Architecture projects SourceView + ActionView truthfully, validating all CapabilityEvidence.
	arch := facade.Architecture(context.Background())
	archJSON, _ := json.Marshal(arch)
	if strings.Contains(string(archJSON), "/private/SECRET") || strings.Contains(string(archJSON), "Demo Video") {
		t.Fatalf("architecture leaked label or secret: %s", archJSON)
	}
	biliSource := arch.Sources[1]
	if biliSource.Platform != contracts.PlatformBilibili || biliSource.State != "ready" || !biliSource.Availability.Enabled ||
		biliSource.AccountID == nil || *biliSource.AccountID != "bilibili-ego" ||
		len(biliSource.ContainerIDs) != 1 || biliSource.ContainerIDs[0] != "bilibili:BV1enabled0001" {
		t.Fatalf("unexpected bilibili source view: %+v", biliSource)
	}
	webSource := arch.Sources[2]
	if webSource.Platform != contracts.PlatformWeb || webSource.State != "unreachable" || !webSource.Availability.Supported || webSource.Availability.Enabled {
		t.Fatalf("unexpected web source view: %+v", webSource)
	}
	for _, s := range arch.Sources {
		for _, cap := range s.Capabilities {
			if err := contracts.ValidateCapabilityEvidence(cap); err != nil {
				t.Fatalf("invalid source capability %+v: %v", cap, err)
			}
		}
	}
	// Find bilibili actions: index 4..7, web actions: 8..11.
	biliActions := map[contracts.ActionKind]ActionView{}
	for _, a := range arch.Actions {
		for _, cap := range a.Capabilities {
			if err := contracts.ValidateCapabilityEvidence(cap); err != nil {
				t.Fatalf("invalid action capability %+v: %v", cap, err)
			}
		}
		if a.Platform == contracts.PlatformBilibili {
			biliActions[a.Kind] = a
		}
	}
	for _, kind := range []contracts.ActionKind{contracts.ActionReplyText, contracts.ActionThreadReply} {
		act := biliActions[kind]
		if act.State != "not_ready" || !act.Availability.Supported || act.Availability.Enabled || act.Availability.Reason != "unavailable" {
			t.Fatalf("expected dry_run bilibili %s not_ready/unavailable, got %+v", kind, act)
		}
	}
	if biliActions[contracts.ActionEditText].State != "unsupported" || biliActions[contracts.ActionDeleteContent].State != "unsupported" {
		t.Fatalf("expected undeclared bilibili actions to stay unsupported: %+v", biliActions)
	}

	// Disable dry_run on connector -> Architecture actions become ready/enabled; driver not_ready downgrades both source & actions.
	recCtrl := httptest.NewRecorder()
	reqCtrl := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1/control/dry-run", strings.NewReader(`{"dry_run":false,"request_id":"arch-live-1"}`))
	reqCtrl.Header.Set("Content-Type", "application/json")
	connSrv.Handler().ServeHTTP(recCtrl, reqCtrl)
	if recCtrl.Code != http.StatusOK {
		t.Fatalf("toggle dry_run=false failed: %d", recCtrl.Code)
	}
	archLive := facade.Architecture(context.Background())
	for _, a := range archLive.Actions {
		if a.Platform == contracts.PlatformBilibili && (a.Kind == contracts.ActionReplyText || a.Kind == contracts.ActionThreadReply) {
			if a.State != "ready" || !a.Availability.Enabled || a.Availability.Reason != "" {
				t.Fatalf("expected live bilibili %s ready/enabled, got %+v", a.Kind, a)
			}
		}
	}

	exec.mu.Lock()
	exec.ready = false
	exec.reason = "not_logged_in"
	exec.mu.Unlock()
	archNotReady := facade.Architecture(context.Background())
	if archNotReady.Sources[1].State != "not_ready" || archNotReady.Sources[1].Availability.Enabled {
		t.Fatalf("expected bilibili source not_ready when driver unready, got %+v", archNotReady.Sources[1])
	}
	for _, a := range archNotReady.Actions {
		if a.Platform == contracts.PlatformBilibili && a.Kind == contracts.ActionReplyText && (a.State != "not_ready" || a.Availability.Enabled) {
			t.Fatalf("expected bilibili action not_ready when driver unready, got %+v", a)
		}
	}
}

func TestConfirmedConnectorDryRunPlanApplyAndConflictGuards(t *testing.T) {
	store, err := connector.OpenIdempotencyStore(filepath.Join(t.TempDir(), "idem.json"))
	if err != nil {
		t.Fatal(err)
	}
	exec := &stubConnectorExecutor{
		platform:  contracts.PlatformBilibili,
		accountID: "bilibili-ego",
		name:      "bilibili-ego-connector",
		caps:      []contracts.ActionKind{contracts.ActionReplyText, contracts.ActionThreadReply},
		ready:     true,
	}
	connSrv := connector.NewServer(connector.ServerOptions{
		Executor: exec,
		Store:    store,
		DryRun:   true,
	})
	connHTTP := httptest.NewServer(connSrv.Handler())
	defer connHTTP.Close()

	facade, _ := traceSetup(t)
	facade.backend.bases["connector:bilibili"] = connHTTP.URL

	// Plan with wrong expectedDryRun -> 409 conflict.
	badExpected := consolePost(facade, "connectors/bilibili/dry-run/plan", ConnectorDryRunPlanInput{
		RequestID:      "plan-bad-exp",
		ExpectedDryRun: false,
		TargetDryRun:   true,
	})
	if badExpected.Code != http.StatusConflict {
		t.Fatalf("expected 409 on wrong expectedDryRun, got %d %s", badExpected.Code, badExpected.Body.String())
	}

	// Plan no-op (true -> true) -> 400 validation.
	noOp := consolePost(facade, "connectors/bilibili/dry-run/plan", ConnectorDryRunPlanInput{
		RequestID:      "plan-noop",
		ExpectedDryRun: true,
		TargetDryRun:   true,
	})
	if noOp.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on no-op dry_run plan, got %d %s", noOp.Code, noOp.Body.String())
	}

	// Valid plan (true -> false) produces live_external_writes_enabled + runtime_only_not_persisted warnings.
	planRec := consolePost(facade, "connectors/bilibili/dry-run/plan", ConnectorDryRunPlanInput{
		RequestID:      "plan-live-1",
		ExpectedDryRun: true,
		TargetDryRun:   false,
	})
	if planRec.Code != http.StatusOK {
		t.Fatalf("plan failed: %d %s", planRec.Code, planRec.Body.String())
	}
	var planOut struct {
		OK    bool                `json:"ok"`
		Value ConnectorDryRunPlan `json:"value"`
	}
	if err := json.Unmarshal(planRec.Body.Bytes(), &planOut); err != nil || !planOut.OK {
		t.Fatalf("decode plan failed: %v %s", err, planRec.Body.String())
	}
	plan := planOut.Value
	if !plan.CurrentDryRun || plan.TargetDryRun || plan.Persisted || len(plan.Warnings) < 2 {
		t.Fatalf("unexpected plan: %+v", plan)
	}

	// Replay same requestId + different target -> 409 conflict.
	planConf := consolePost(facade, "connectors/bilibili/dry-run/plan", ConnectorDryRunPlanInput{
		RequestID:      "plan-live-1",
		ExpectedDryRun: true,
		TargetDryRun:   true,
	})
	if planConf.Code != http.StatusConflict {
		t.Fatalf("expected 409 on conflicting plan requestId, got %d", planConf.Code)
	}

	// Apply with confirm=false -> 400 validation (and plan not consumed!).
	unconfirmed := consolePost(facade, "connectors/bilibili/dry-run/apply", ConnectorDryRunApplyInput{
		RequestID: "apply-unconfirmed",
		PlanID:    plan.ID,
		Token:     plan.Token,
		Confirm:   false,
	})
	if unconfirmed.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 when confirm=false, got %d %s", unconfirmed.Code, unconfirmed.Body.String())
	}

	// Apply with bad token -> 403 forbidden.
	badToken := consolePost(facade, "connectors/bilibili/dry-run/apply", ConnectorDryRunApplyInput{
		RequestID: "apply-bad-token",
		PlanID:    plan.ID,
		Token:     "wrong-token",
		Confirm:   true,
	})
	if badToken.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for bad token, got %d", badToken.Code)
	}

	// Confirmed apply succeeds and verifies readback.
	applyRec := consolePost(facade, "connectors/bilibili/dry-run/apply", ConnectorDryRunApplyInput{
		RequestID: "apply-live-1",
		PlanID:    plan.ID,
		Token:     plan.Token,
		Confirm:   true,
	})
	if applyRec.Code != http.StatusOK {
		t.Fatalf("apply failed: %d %s", applyRec.Code, applyRec.Body.String())
	}
	var applyOut struct {
		OK    bool                       `json:"ok"`
		Value ConnectorDryRunApplyResult `json:"value"`
	}
	if err := json.Unmarshal(applyRec.Body.Bytes(), &applyOut); err != nil || !applyOut.OK {
		t.Fatalf("decode apply failed: %v %s", err, applyRec.Body.String())
	}
	if !applyOut.Value.PreviousDryRun || applyOut.Value.DryRun || !applyOut.Value.ReadbackVerified || applyOut.Value.Status != "succeeded" {
		t.Fatalf("unexpected apply result: %+v", applyOut.Value)
	}

	// Replay same apply requestId -> 200 idempotent; new requestId against used plan -> 409 conflict.
	replayApply := consolePost(facade, "connectors/bilibili/dry-run/apply", ConnectorDryRunApplyInput{
		RequestID: "apply-live-1",
		PlanID:    plan.ID,
		Token:     plan.Token,
		Confirm:   true,
	})
	if replayApply.Code != http.StatusOK {
		t.Fatalf("expected idempotent replay 200, got %d", replayApply.Code)
	}
	resubmitApply := consolePost(facade, "connectors/bilibili/dry-run/apply", ConnectorDryRunApplyInput{
		RequestID: "apply-live-2",
		PlanID:    plan.ID,
		Token:     plan.Token,
		Confirm:   true,
	})
	if resubmitApply.Code != http.StatusConflict {
		t.Fatalf("expected 409 on re-submitting used plan with new requestId, got %d", resubmitApply.Code)
	}

	// Ambiguous POST/readback mismatch yields 202 unknown.
	tampered := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			_ = json.NewEncoder(w).Encode(connector.HealthResponse{
				Status:          "ready",
				ProtocolVersion: 1,
				Platform:        "web",
				Ready:           true,
				DryRun:          true, // never flips even after POST
				Capabilities:    []string{"reply_text"},
				Controls:        []string{"dry_run"},
			})
		case "/v1/control/dry-run":
			_ = json.NewEncoder(w).Encode(connector.DryRunControlResponse{
				OK:        true,
				Platform:  "web",
				DryRun:    false,
				RequestID: "ambig-apply-1",
			})
		}
	}))
	defer tampered.Close()
	facade.backend.bases["connector:web"] = tampered.URL

	ambigPlanRec := consolePost(facade, "connectors/web/dry-run/plan", ConnectorDryRunPlanInput{
		RequestID:      "ambig-plan-1",
		ExpectedDryRun: true,
		TargetDryRun:   false,
	})
	if ambigPlanRec.Code != http.StatusOK {
		t.Fatalf("ambig plan failed: %d", ambigPlanRec.Code)
	}
	var ambigPlanOut struct {
		Value ConnectorDryRunPlan `json:"value"`
	}
	_ = json.Unmarshal(ambigPlanRec.Body.Bytes(), &ambigPlanOut)
	ambigApplyRec := consolePost(facade, "connectors/web/dry-run/apply", ConnectorDryRunApplyInput{
		RequestID: "ambig-apply-1",
		PlanID:    ambigPlanOut.Value.ID,
		Token:     ambigPlanOut.Value.Token,
		Confirm:   true,
	})
	if ambigApplyRec.Code != http.StatusAccepted || !strings.Contains(ambigApplyRec.Body.String(), `"code":"unknown"`) {
		t.Fatalf("expected 202 unknown on readback mismatch, got %d %s", ambigApplyRec.Code, ambigApplyRec.Body.String())
	}
}
