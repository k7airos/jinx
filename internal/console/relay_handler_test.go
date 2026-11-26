package console_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/console"
	"jinx/internal/relay/config"
	"jinx/internal/relay/filter"
	"jinx/internal/relay/hub"
	"jinx/internal/relay/web"
)

func relayFixture(t *testing.T, mode string) (*publicationHandler, *config.Manager, *atomic.Int32) {
	t.Helper()
	fixture := newPublicationHandler(t, "")
	mgr, err := config.NewManager(filepath.Join(t.TempDir(), "relay.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := mgr.Get()
	cfg.GroupControl.MonitorAll = false
	cfg.GroupControl.MonitoredGroups = []string{publicationGroupB}
	cfg.GroupControl.ExposedGroups = []string{publicationGroupB}
	cfg.Upstream.WSURL = "ws://synthetic.invalid/private-secret"
	if err := mgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	server := web.NewServer(mgr, hub.NewHub(8), filter.NewEngine())
	handler, err := server.Handler()
	if err != nil {
		t.Fatal(err)
	}
	writes := &atomic.Int32{}
	relay := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api/v1/console-config" {
			writer.WriteHeader(404)
			return
		}
		if request.URL.Path != "/api/v1/config" {
			t.Errorf("unsafe Relay route: %s", request.URL.Path)
			writer.WriteHeader(400)
			return
		}
		if request.Method == "POST" {
			writes.Add(1)
			if mode == "reject" {
				writer.WriteHeader(400)
				return
			}
			if mode == "saved500" {
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				writer.WriteHeader(500)
				return
			}
		}
		handler.ServeHTTP(writer, request)
	}))
	t.Cleanup(relay.Close)
	if err := fixture.facade.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.facade, err = console.New(console.Options{DispatcherURL: fixture.server.URL, RelayURL: relay.URL, StateDir: t.TempDir(), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = fixture.facade.Close() })
	return fixture, mgr, writes
}

func TestRealRelayAtomicCASFacadeRoundTrip(t *testing.T) {
	fixture := newPublicationHandler(t, "")
	mgr, err := config.NewManager(filepath.Join(t.TempDir(), "relay-cas.json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := mgr.Get()
	cfg.Upstream.WSURL = "ws://synthetic.invalid/private-secret"
	if err := mgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	server := web.NewServer(mgr, hub.NewHub(8), filter.NewEngine())
	handler, err := server.Handler()
	if err != nil {
		t.Fatal(err)
	}
	writes := atomic.Int32{}
	relay := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/console-config" {
			t.Errorf("CAS adapter read legacy/private path: %s", request.URL.Path)
			writer.WriteHeader(400)
			return
		}
		if request.Method == "PUT" {
			writes.Add(1)
		}
		handler.ServeHTTP(writer, request)
	}))
	defer relay.Close()
	if err := fixture.facade.Close(); err != nil {
		t.Fatal(err)
	}
	facade, err := console.New(console.Options{DispatcherURL: fixture.server.URL, RelayURL: relay.URL, StateDir: t.TempDir(), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer facade.Close()
	view, err := facade.GetRelayRules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	public := view.Data.(console.RelayRulesView)
	if public.WriteConcurrency != "remote_cas" || len(public.Warnings) != 0 {
		t.Fatalf("CAS not discovered: %+v", public)
	}
	input := console.PlanInput{RequestID: "cas", Base: public.Versions, SchemaVersion: console.SchemaVersion, Edits: []console.Edit{{Kind: "relay_field", Scope: console.Scope{Kind: "relay_filters"}, Field: "clean_content", Value: json.RawMessage(`false`)}}}
	plan, err := facade.PlanChange(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	operation, err := facade.PublishChange(context.Background(), console.PublishInput{RequestID: "cas-pub", PlanID: plan.ID, Token: plan.Token})
	if err != nil || operation.Status != "succeeded" || writes.Load() != 1 || mgr.Get().Filters.CleanContent || mgr.Get().Upstream.WSURL != cfg.Upstream.WSURL {
		t.Fatalf("%+v %v", operation, err)
	}
	view, _ = facade.GetRelayRules(context.Background())
	input.RequestID, input.Base, input.Edits[0].Value = "cas-drift", view.Data.(console.RelayRulesView).Versions, json.RawMessage(`true`)
	plan, err = facade.PlanChange(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	cfg = mgr.Get()
	cfg.Upstream.WSURL = "ws://synthetic.invalid/changed-private"
	if err := mgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := facade.PublishChange(context.Background(), console.PublishInput{RequestID: "cas-drift-pub", PlanID: plan.ID, Token: plan.Token}); err == nil || writes.Load() != 1 {
		t.Fatal("private legacy update did not invalidate CAS")
	}
	snapshot := mgr.GetPublic()
	body, _ := json.Marshal(config.PublicConfig{GroupControl: snapshot.GroupControl, Filters: snapshot.Filters})
	var workers sync.WaitGroup
	results := make(chan int, 8)
	start := make(chan struct{})
	for index := 0; index < cap(results); index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			request := httptest.NewRequest("PUT", "http://127.0.0.1:8080/api/v1/console-config", bytes.NewReader(body))
			request.RemoteAddr = "127.0.0.1:23456"
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("If-Match", `"`+snapshot.Revision+`"`)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			results <- response.Code
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	successes := 0
	for status := range results {
		if status == 200 {
			successes++
		} else if status != 409 {
			t.Fatalf("CAS loser returned %d", status)
		}
	}
	if successes != 1 {
		t.Fatalf("real Relay CAS allowed %d concurrent winners", successes)
	}
}

func TestRealRelayHandlerHasNoRemoteCAS(t *testing.T) {
	fixture, mgr, writes := relayFixture(t, "")
	cfg := mgr.Get()
	cfg.Filters.IgnoreEcho = true
	data, _ := json.Marshal(cfg)
	server := web.NewServer(mgr, hub.NewHub(8), filter.NewEngine())
	handler, err := server.Handler()
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "http://127.0.0.1:8080/api/v1/config", bytes.NewReader(data))
	request.RemoteAddr = "127.0.0.1:23456"
	request.Header.Set("If-Match", `"definitely-stale"`)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != 200 || !mgr.Get().Filters.IgnoreEcho || response.Header().Get("ETag") != "" {
		t.Fatalf("Relay CAS contract changed: %d %s", response.Code, response.Body.String())
	}
	view, err := fixture.facade.GetRelayRules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rules := view.Data.(console.RelayRulesView)
	if rules.WriteConcurrency != "digest_preflight_local_mutex" || len(rules.Warnings) == 0 || rules.Versions.Relay == "" {
		t.Fatalf("CAS claim: %+v", rules)
	}
	plan, err := fixture.facade.PlanChange(context.Background(), console.PlanInput{RequestID: "relay-only", Base: rules.Versions, SchemaVersion: console.SchemaVersion, Edits: []console.Edit{{Kind: "relay_field", Scope: console.Scope{Kind: "relay_filters"}, Field: "clean_content", Value: json.RawMessage(`false`)}}})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := fixture.facade.PublishChange(context.Background(), console.PublishInput{RequestID: "relay-pub", PlanID: plan.ID, Token: plan.Token})
	if err != nil || operation.Status != "succeeded" || writes.Load() != 1 || mgr.Get().Filters.CleanContent || mgr.Get().Upstream.WSURL != "ws://synthetic.invalid/private-secret" {
		t.Fatalf("%+v %v writes=%d", operation, err, writes.Load())
	}
}

func TestCrossComponentCollectionAndPartialNoCompensation(t *testing.T) {
	for _, mode := range []string{"", "reject", "saved500"} {
		t.Run(mode, func(t *testing.T) {
			fixture, mgr, writes := relayFixture(t, mode)
			view, err := fixture.facade.GetRelayRules(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			base := view.Data.(console.RelayRulesView).Versions
			plan, err := fixture.facade.PlanChange(context.Background(), console.PlanInput{RequestID: "collection", Base: base, SchemaVersion: console.SchemaVersion, Edits: []console.Edit{{Kind: "group_collection", GroupID: publicationGroupA, Enabled: false}}})
			if err != nil || len(plan.ComponentOrder) != 1 {
				t.Fatalf("disable preview %+v %v", plan, err)
			}
			operation, err := fixture.facade.PublishChange(context.Background(), console.PublishInput{RequestID: "disable", PlanID: plan.ID, Token: plan.Token})
			if err != nil || operation.Status != "succeeded" {
				t.Fatalf("%+v %v", operation, err)
			}
			view, _ = fixture.facade.GetRelayRules(context.Background())
			plan, err = fixture.facade.PlanChange(context.Background(), console.PlanInput{RequestID: "enable-collection", Base: view.Data.(console.RelayRulesView).Versions, SchemaVersion: console.SchemaVersion, Edits: []console.Edit{{Kind: "group_collection", GroupID: publicationGroupA, Enabled: true}}})
			if err != nil || strings.Join(plan.ComponentOrder, ",") != "dispatcher,relay" {
				t.Fatalf("%+v %v", plan, err)
			}
			input := console.PublishInput{RequestID: "enable", PlanID: plan.ID, Token: plan.Token}
			operation, err = fixture.facade.PublishChange(context.Background(), input)
			expected := "succeeded"
			if mode != "" {
				expected = "partial_apply"
			}
			if err != nil || operation.Status != expected || operation.Components[0].State != "verified" || writes.Load() != 1 || fixture.writes.Load() != 2 {
				t.Fatalf("%+v %v", operation, err)
			}
			if mode == "reject" && operation.Components[1].State != "failed_not_applied" {
				t.Fatal(operation)
			}
			if mode == "saved500" && (operation.Components[1].State != "applied_persistence_unverified" || operation.Components[1].PersistenceVerified != nil) {
				t.Fatal(operation)
			}
			_, _ = fixture.facade.PublishChange(context.Background(), input)
			if writes.Load() != 1 || fixture.writes.Load() != 2 {
				t.Fatal("partial publication was retried or compensated")
			}
			cfg := mgr.Get()
			if cfg.GroupControl.MonitorAll || len(cfg.GroupControl.ExposedGroups) > 2 {
				t.Fatal("collection expanded to all groups")
			}
		})
	}
}

func TestRelayDriftBlocksBeforeDispatcherWrite(t *testing.T) {
	fixture, mgr, writes := relayFixture(t, "")
	view, _ := fixture.facade.GetRelayRules(context.Background())
	plan, err := fixture.facade.PlanChange(context.Background(), console.PlanInput{RequestID: "drift", Base: view.Data.(console.RelayRulesView).Versions, SchemaVersion: console.SchemaVersion, Edits: []console.Edit{{Kind: "group_collection", GroupID: publicationGroupA, Enabled: false}, {Kind: "relay_field", Scope: console.Scope{Kind: "relay_filters"}, Field: "clean_content", Value: json.RawMessage(`false`)}}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := mgr.Get()
	cfg.Upstream.WSURL = "ws://synthetic.invalid/external-change"
	if err := mgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.facade.PublishChange(context.Background(), console.PublishInput{RequestID: "drift-pub", PlanID: plan.ID, Token: plan.Token}); err == nil || writes.Load() != 0 || fixture.writes.Load() != 0 {
		t.Fatalf("drift allowed %v", err)
	}
	for _, field := range []string{"local_db", "keys_file", "upstream", "ws_url", "unknown"} {
		current, _ := fixture.facade.GetRelayRules(context.Background())
		_, err := fixture.facade.PlanChange(context.Background(), console.PlanInput{RequestID: field, Base: current.Data.(console.RelayRulesView).Versions, SchemaVersion: console.SchemaVersion, Edits: []console.Edit{{Kind: "relay_field", Scope: console.Scope{Kind: "relay_filters"}, Field: field, Value: json.RawMessage(`true`)}}})
		if err == nil || err.(*console.Problem).Code != "validation" {
			t.Fatal("private Relay field accepted")
		}
	}
}

func TestRelayStrictFieldsAndCollectionScope(t *testing.T) {
	fixture, mgr, writes := relayFixture(t, "")
	for _, edit := range []console.Edit{
		{Kind: "relay_field", Scope: console.Scope{Kind: "relay_filters"}, Field: "allowed_types", Value: json.RawMessage(`null`)},
		{Kind: "relay_field", Scope: console.Scope{Kind: "relay_filters"}, Field: "regex_blocklist", Value: json.RawMessage(`["["]`)},
		{Kind: "relay_field", Scope: console.Scope{Kind: "relay_filters"}, Field: "private", Value: json.RawMessage(`true`)},
		{Kind: "relay_field", Scope: console.Scope{Kind: "relay_group_control"}, Field: "exposed_groups", Value: json.RawMessage(`["*"]`)},
	} {
		view, _ := fixture.facade.GetRelayRules(context.Background())
		_, err := fixture.facade.PlanChange(context.Background(), console.PlanInput{RequestID: edit.Field, Base: view.Data.(console.RelayRulesView).Versions, SchemaVersion: console.SchemaVersion, Edits: []console.Edit{edit}})
		if err == nil || err.(*console.Problem).Code != "validation" {
			t.Fatalf("unsafe Relay edit accepted %+v %v", edit, err)
		}
	}
	cfg := mgr.Get()
	cfg.GroupControl.MonitorAll = true
	if err := mgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	view, _ := fixture.facade.GetRelayRules(context.Background())
	input := console.PlanInput{RequestID: "scope", Base: view.Data.(console.RelayRulesView).Versions, SchemaVersion: console.SchemaVersion, Edits: []console.Edit{{Kind: "group_collection", GroupID: publicationGroupA, Enabled: true}}}
	if _, err := fixture.facade.PlanChange(context.Background(), input); err == nil {
		t.Fatal("collection silently retained monitor_all")
	}
	input.Edits = append([]console.Edit{{Kind: "relay_field", Scope: console.Scope{Kind: "relay_group_control"}, Field: "monitor_all", Value: json.RawMessage(`false`)}}, input.Edits...)
	if _, err := fixture.facade.PlanChange(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	input.RequestID = "scope-final-all"
	input.Edits = append(input.Edits, console.Edit{Kind: "relay_field", Scope: console.Scope{Kind: "relay_group_control"}, Field: "monitor_all", Value: json.RawMessage(`true`)})
	if _, err := fixture.facade.PlanChange(context.Background(), input); err == nil {
		t.Fatal("collection accepted final monitor_all override")
	}
	input.RequestID = "scope-wildcard"
	input.Edits = input.Edits[:len(input.Edits)-1]
	cfg = mgr.Get()
	cfg.GroupControl.MonitorAll = false
	cfg.GroupControl.ExposedGroups = []string{"*"}
	if err := mgr.Update(cfg); err != nil {
		t.Fatal(err)
	}
	view, _ = fixture.facade.GetRelayRules(context.Background())
	input.Base = view.Data.(console.RelayRulesView).Versions
	if _, err := fixture.facade.PlanChange(context.Background(), input); err == nil {
		t.Fatal("collection retained wildcard exposure")
	}
	if writes.Load() != 0 || fixture.writes.Load() != 0 {
		t.Fatal("preview wrote upstream")
	}
}
