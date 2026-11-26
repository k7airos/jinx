package client

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTriStateRoundTripsAndLosslessDistinction(t *testing.T) {
	t.Run("string_list_null_vs_empty_vs_values", func(t *testing.T) {
		type holder struct {
			Inherit TriStateStringList `json:"inherit"`
			Empty   TriStateStringList `json:"empty"`
			Custom  TriStateStringList `json:"custom"`
		}
		in := holder{
			Inherit: TriStringsNull(),
			Empty:   TriStringsEmpty(),
			Custom:  TriStringsValues("alpha", "beta"),
		}
		raw, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if string(raw) != `{"inherit":null,"empty":[],"custom":["alpha","beta"]}` {
			t.Fatalf("unexpected JSON: %s", raw)
		}
		var out holder
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if out.Inherit.Mode != TriNull || out.Empty.Mode != TriEmpty || out.Custom.Mode != TriValues {
			t.Fatalf("tri-state modes lost on roundtrip: %+v", out)
		}
		if len(out.Custom.Values) != 2 || out.Custom.Values[0] != "alpha" || out.Custom.Values[1] != "beta" {
			t.Fatalf("custom values mismatch: %+v", out.Custom.Values)
		}
	})

	t.Run("int_list_null_vs_empty_vs_values", func(t *testing.T) {
		type holder struct {
			Inherit TriStateIntList `json:"inherit"`
			Empty   TriStateIntList `json:"empty"`
			Custom  TriStateIntList `json:"custom"`
		}
		in := holder{
			Inherit: TriIntsNull(),
			Empty:   TriIntsEmpty(),
			Custom:  TriIntsValues(1, 49, 81),
		}
		raw, err := json.Marshal(in)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if string(raw) != `{"inherit":null,"empty":[],"custom":[1,49,81]}` {
			t.Fatalf("unexpected JSON: %s", raw)
		}
		var out holder
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if out.Inherit.Mode != TriNull || out.Empty.Mode != TriEmpty || out.Custom.Mode != TriValues {
			t.Fatalf("tri-state int modes lost: %+v", out)
		}
	})
}

func TestLoopbackURLValidation(t *testing.T) {
	valid := []string{
		"http://127.0.0.1:8090",
		"http://127.0.0.1:8090/",
		"http://localhost:8099",
		"http://[::1]:8090",
	}
	for _, u := range valid {
		if _, err := NewHTTPClient(u); err != nil {
			t.Fatalf("expected valid loopback URL %q, got error: %v", u, err)
		}
	}

	invalid := []string{
		"",
		"https://127.0.0.1:8090",
		"http://127.0.0.1",
		"http://192.168.1.10:8090",
		"http://example.com:8090",
		"http://user:pass@127.0.0.1:8090",
		"http://127.0.0.1:8090/console",
		"http://127.0.0.1:8090?foo=bar",
		"http://127.0.0.1:8090?",
		"http://127.0.0.1:8090#frag",
		"http://127.0.0.1:8090#",
	}
	for _, u := range invalid {
		if _, err := NewHTTPClient(u); err == nil {
			t.Fatalf("expected loopback validator to reject %q", u)
		}
	}
}

func TestMockClientScenariosAndInvariants(t *testing.T) {
	ctx := context.Background()

	t.Run("full_connector_dry_run_confirmed_flow", func(t *testing.T) {
		m := NewMockClient("full")
		conns, err := m.ListConnectors(ctx)
		if err != nil || len(conns.Data) != 2 {
			t.Fatalf("ListConnectors failed: %v", err)
		}
		bili := conns.Data[0]
		if bili.Platform != "bilibili" || bili.DryRun == nil || !*bili.DryRun {
			t.Fatalf("unexpected initial bilibili state: %+v", bili)
		}

		plan, err := m.PlanConnectorDryRun(ctx, "bilibili", ConnectorDryRunPlanInput{
			RequestID: "req-bili-1", ExpectedDryRun: true, TargetDryRun: false,
		})
		if err != nil {
			t.Fatalf("PlanConnectorDryRun: %v", err)
		}
		if plan.Persisted {
			t.Fatalf("expected persisted=false warning in plan")
		}

		if _, err := m.ApplyConnectorDryRun(ctx, "bilibili", ConnectorDryRunApplyInput{
			RequestID: "req-bili-apply-bad", PlanID: plan.ID, Token: plan.Token, Confirm: false,
		}); err == nil {
			t.Fatalf("expected ApplyConnectorDryRun to reject confirm=false")
		}

		res, err := m.ApplyConnectorDryRun(ctx, "bilibili", ConnectorDryRunApplyInput{
			RequestID: "req-bili-apply-ok", PlanID: plan.ID, Token: plan.Token, Confirm: true,
		})
		if err != nil {
			t.Fatalf("ApplyConnectorDryRun: %v", err)
		}
		if !res.ReadbackVerified || res.DryRun != false || res.PreviousDryRun != true {
			t.Fatalf("unexpected apply readback: %+v", res)
		}
		arch, err := m.GetArchitecture(ctx)
		if err != nil {
			t.Fatalf("GetArchitecture: %v", err)
		}
		for _, a := range arch.Actions {
			if a.Platform == "bilibili" && a.Kind == "reply_text" && a.State != "ready" {
				t.Fatalf("expected bilibili reply_text ready after disabling dry_run, got %q", a.State)
			}
		}
	})

	t.Run("conflict_scenario_returns_409", func(t *testing.T) {
		m := NewMockClient("conflict")
		groups, err := m.ListGroups(ctx)
		if err != nil || len(groups.Data.Items) == 0 {
			t.Fatalf("ListGroups: %v", err)
		}
		g := groups.Data.Items[0]
		val, _ := json.Marshal(420)
		plan, err := m.PlanChange(ctx, PlanInput{
			RequestID:     "req-conflict-plan",
			Base:          g.Versions,
			SchemaVersion: SchemaVersion,
			Edits:         []Edit{{Kind: "field", Scope: Scope{Kind: "group", GroupID: g.ID}, Field: "max_reply_runes", Value: val}},
		})
		if err != nil {
			t.Fatalf("PlanChange: %v", err)
		}
		_, err = m.PublishChange(ctx, PublishInput{
			RequestID: "req-conflict-pub", PlanID: plan.ID, Token: plan.Token,
		})
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Status != 409 || apiErr.Problem.Code != "conflict" {
			t.Fatalf("expected 409 conflict on PublishChange, got %v", err)
		}
	})

	t.Run("unknown_delivery_and_degraded_invariants", func(t *testing.T) {
		m := NewMockClient("degraded")
		ov, err := m.GetOverview(ctx)
		if err != nil {
			t.Fatalf("GetOverview: %v", err)
		}
		if ov.Freshness != "stale" {
			t.Fatalf("expected stale overview freshness in degraded mode, got %q", ov.Freshness)
		}
		ctl, err := m.GetControls(ctx)
		if err != nil || ctl.Maintenance == nil || !ctl.Maintenance.Active {
			t.Fatalf("expected active maintenance in degraded scenario")
		}
		drift, err := m.GetConfigurationDrift(ctx, "dispatcher")
		if err != nil || drift.Data.Drift == nil || !*drift.Data.Drift {
			t.Fatalf("expected dispatcher drift=true in degraded scenario")
		}

		// echo_inferred with submittedAt=nil and attempt succeeded with delivery=unknown
		r1, err := m.GetRun(ctx, "run-demo-001")
		if err != nil {
			t.Fatalf("GetRun 001: %v", err)
		}
		ev := r1.Data.Run.DeliveryEvidence
		if ev.SubmittedAt != nil || ev.SubmittedAtSource == nil || *ev.SubmittedAtSource != "echo_inferred" || ev.ConfirmedAt == nil {
			t.Fatalf("echo_inferred evidence violated: %+v", ev)
		}
		r3, err := m.GetRun(ctx, "run-demo-003")
		if err != nil {
			t.Fatalf("GetRun 003: %v", err)
		}
		if r3.Data.Run.Attempts[0].Status != "succeeded" || r3.Data.Run.Delivery != "unknown" {
			t.Fatalf("expected attempt=succeeded + delivery=unknown, got %+v", r3.Data.Run)
		}
	})
}

func TestHTTPClientSecurityAndSSEBounds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://example.com/", http.StatusFound)
	}))
	defer srv.Close()

	hc, err := NewHTTPClient(srv.URL)
	if err != nil {
		t.Fatalf("NewHTTPClient: %v", err)
	}
	if _, err := hc.Bootstrap(context.Background()); err == nil {
		t.Fatal("expected HTTPClient to refuse HTTP redirect")
	}

	// Oversized multi-line SSE frame must be dropped cleanly without emitting a corrupted payload.
	chunk := strings.Repeat("x", 64*1024)
	var sse strings.Builder
	sse.WriteString("id: c-1\nevent: run\n")
	for i := 0; i < 40; i++ {
		sse.WriteString("data: ")
		sse.WriteString(chunk)
		sse.WriteByte('\n')
	}
	sse.WriteString("\n")
	sse.WriteString("id: c-2\nevent: run\ndata: {\"ok\":true}\n\n")

	var frames []string
	var unavailable int
	cursor, lastID := "", ""
	readSSEStream(strings.NewReader(sse.String()), &cursor, &lastID, sseCallbacks{
		onFrame: func(name, id string, payload []byte) {
			frames = append(frames, id+":"+string(payload))
		},
		onUnavailable: func(Problem) {
			unavailable++
		},
	})
	if unavailable == 0 {
		t.Fatal("expected oversized SSE frame to signal unavailable")
	}
	if len(frames) != 1 || frames[0] != `c-2:{"ok":true}` {
		t.Fatalf("expected only second valid SSE frame to pass, got %v", frames)
	}
}
