package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"jinx/internal/dispatcher"
	"jinx/internal/execution"
	"jinx/internal/runtimebridge"
)

type fakeDispatcherRunner struct {
	runner  dispatcher.Runner
	starts  int
	closes  int
	handles int
}

func (fake *fakeDispatcherRunner) Start() {
	fake.starts++
}

func (fake *fakeDispatcherRunner) Close() {
	fake.closes++
}

func (fake *fakeDispatcherRunner) Handler() http.Handler {
	fake.handles++
	return http.NotFoundHandler()
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(value); err != nil {
		t.Fatal(err)
	}
}

func configuredArguments(t *testing.T, almaURL string, kind execution.Kind) arguments {
	t.Helper()
	directory := t.TempDir()
	args := arguments{config: filepath.Join(directory, "config.json"), executionConfig: filepath.Join(directory, "execution.json"), backendsJSON: filepath.Join(directory, "operator-private.json")}
	backend := runtimebridge.BackendDefinition{ID: "backend", Kind: kind, AlmaURL: almaURL, Models: []execution.Model{{ID: "mock-model", Label: "Mock model"}}}
	modelID := "mock-model"
	if kind == execution.TextOnly {
		backend.ProviderID = "mock-provider"
	}
	if kind == execution.LegacyHermes {
		work := filepath.Join(directory, "private-hermes-work")
		if err := os.Mkdir(work, 0700); err != nil {
			t.Fatal(err)
		}
		command := filepath.Join(directory, "private-hermes-stub")
		script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > args\npwd > cwd\ncat > stdin\nprintf '%s\\n' '{\"type\":\"result\",\"text\":\"mock legacy reply\",\"session_id\":\"mock-session\",\"exit_code\":0}'\n"
		if err := os.WriteFile(command, []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		backend.AlmaURL = ""
		backend.Hermes = &runtimebridge.HermesDefinition{Command: command, Dir: work, Profile: "private-fixed-profile", Toolsets: []string{"web", "memory"}, MaxTurns: 3}
		modelID = "chosen-provider:chosen/model@v2+exact"
		backend.Models = append(backend.Models, execution.Model{ID: modelID, Label: "Chosen model"})
	}
	writeJSON(t, args.backendsJSON, runtimebridge.BackendFile{SchemaVersion: execution.SchemaVersion, Backends: []runtimebridge.BackendDefinition{backend}})
	options, err := runtimebridge.LoadBackendOptions(args.backendsJSON)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := execution.Open(args.executionConfig, options)
	if err != nil {
		t.Fatal(err)
	}
	policy := execution.Policy{Capabilities: map[execution.Capability]bool{}, NoTools: kind == execution.TextOnly, AllowSoft: kind != execution.TextOnly}
	for _, capability := range execution.Capabilities() {
		policy.Capabilities[capability] = capability == execution.TextGeneration || capability == execution.AutoTextDelivery
	}
	config := execution.Config{SchemaVersion: execution.SchemaVersion, Profiles: map[string]string{"demo-100": "100@chatroom"},
		Policies: map[string]execution.Policy{"policy": policy}, Groups: map[string]execution.Group{"100@chatroom": {RouteID: "route", PolicyID: "policy",
			Primary: execution.RouteAttempt{BackendID: "backend", ModelID: modelID, TimeoutSec: 3, Session: execution.SessionPolicy{Mode: "independent_run"}}, Fallbacks: []execution.RouteAttempt{}}}}
	snapshot, err := engine.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.ApplyConfig(context.Background(), snapshot.Revision, "initial", config); err != nil {
		t.Fatal(err)
	}
	var dispatcherConfig dispatcher.Config
	dispatcherConfig.Server.ListenAddr = "127.0.0.1:0"
	dispatcherConfig.Relay.StreamURL = almaURL + "/mock-relay"
	dispatcherConfig.Bridge.URL = almaURL + "/mock-send"
	dispatcherConfig.Bridge.DryRun = true
	dispatcherConfig.Hermes.BinPath = "/nonexistent/not-executed"
	dispatcherConfig.Hermes.Profile = "demo"
	dispatcherConfig.Hermes.ProfilesDir = filepath.Join(directory, "profiles")
	dispatcherConfig.Hermes.TimeoutSec = 5
	dispatcherConfig.Rules.BatchSize = 1
	dispatcherConfig.Rules.IdleTimeoutSec = 1
	dispatcherConfig.Rules.MaxBatchWaitSec = 3
	writeJSON(t, args.config, dispatcherConfig)
	return args
}

func TestExplicitCLIFlags(t *testing.T) {
	for _, argv := range [][]string{nil, {"-config", "config.json"}, {"-execution-config", "execution.json", "-backends-json", "operator.json"},
		{"-config", "config.json", "-execution-config", "execution.json", "-backends-json", "operator.json", "extra"},
		{"-config", "config.json", "-execution-config", "execution.json", "-backends-json", "operator.json", "-proxy", "http://example.invalid"}} {
		if _, err := parseArgs(argv, io.Discard); err == nil {
			t.Fatal("incomplete or unknown flags accepted", argv)
		}
	}
	args, err := parseArgs([]string{"-config", "config.json", "-execution-config", "execution.json", "-backends-json", "operator.json"}, io.Discard)
	if err != nil || !filepath.IsAbs(args.config) || !filepath.IsAbs(args.executionConfig) || !filepath.IsAbs(args.backendsJSON) {
		t.Fatal(args, err)
	}
}

func TestLoadDispatcherInjectsWorkingRunnerWithoutStarting(t *testing.T) {
	for _, kind := range []execution.Kind{execution.AlmaPersonal, execution.TextOnly, execution.LegacyHermes} {
		t.Run(string(kind), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				switch request.Method + " " + request.URL.Path {
				case "POST /api/personal/dispatch":
					_, _ = io.WriteString(writer, `{"id":"mock-run","status":"running","sourceThreadId":"shared-context"}`)
				case "GET /api/personal/runs/mock-run":
					_, _ = io.WriteString(writer, `{"id":"mock-run","status":"done","summary":"mock personal reply"}`)
				case "POST /anthropic-proxy/mock-provider/v1/messages":
					_, _ = io.WriteString(writer, `{"type":"message","role":"assistant","model":"mock-model","stop_reason":"end_turn","content":[{"type":"text","text":"mock text reply"}]}`)
				default:
					t.Error("unexpected network request", request.URL)
					writer.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			args := configuredArguments(t, server.URL, kind)
			storeBefore, err := os.ReadFile(args.executionConfig)
			if err != nil {
				t.Fatal(err)
			}
			fake := &fakeDispatcherRunner{}
			factoryCalls := 0
			service, address, err := loadDispatcher(args, func(path string, options dispatcher.Options) (dispatcherService, error) {
				factoryCalls++
				if path != args.config || options.Runner == nil {
					t.Error("Runner not supplied to dispatcher options")
				}
				fake.runner = options.Runner
				return fake, nil
			})
			if err != nil || service != fake || address != "127.0.0.1:0" || calls.Load() != 0 || factoryCalls != 1 || fake.starts != 0 || fake.handles != 0 {
				t.Fatal("load performed execution or started lifecycle", address, calls.Load(), err)
			}
			storeAfter, err := os.ReadFile(args.executionConfig)
			if err != nil || string(storeBefore) != string(storeAfter) {
				t.Fatal("load changed existing execution store", err)
			}
			work := filepath.Join(filepath.Dir(args.config), "private-hermes-work")
			if kind == execution.LegacyHermes {
				entries, err := os.ReadDir(work)
				if err != nil || len(entries) != 0 {
					t.Fatal("load executed Hermes command", entries, err)
				}
			}
			result, err := fake.runner.Run(context.Background(), dispatcher.BackendRequest{TraceID: "cli-trace", GroupID: "100@chatroom", Prompt: "synthetic prompt"})
			if err != nil || !strings.HasPrefix(result.Text, "mock ") || result.RunID != "cli-trace" || result.SessionID != "execution:cli-trace" || (calls.Load() == 0) != (kind == execution.LegacyHermes) {
				t.Fatal("injected Runner did not execute", result, err)
			}
			modelID := "mock-model"
			if kind == execution.LegacyHermes {
				modelID = "chosen-provider:chosen/model@v2+exact"
				resolvedWork, err := filepath.EvalSymlinks(work)
				if err != nil {
					t.Fatal(err)
				}
				expected := "-p\nprivate-fixed-profile\nchat\n--query-file\n-\n-Q\n--format\nstream-json\n--max-turns\n3\n-m\n" + modelID + "\n-t\nweb,memory\n"
				for name, expectedContent := range map[string]string{"args": expected, "stdin": "user: synthetic prompt\n", "cwd": resolvedWork + "\n"} {
					data, err := os.ReadFile(filepath.Join(work, name))
					if err != nil || string(data) != expectedContent {
						t.Fatalf("unexpected Hermes %s: %q, %v", name, data, err)
					}
				}
				if result.Backend != "Hermes" {
					t.Fatal(result)
				}
			}
			metadata, ok := fake.runner.(runtimebridge.MetadataSource)
			if !ok {
				t.Fatal("Runner metadata unavailable")
			}
			meta, exists := metadata.Meta("cli-trace")
			if !exists || meta.Model != modelID || meta.Backend != "backend" || meta.PolicyRevision == "" || len(meta.Attempts) != 1 || meta.Attempts[0].ModelID != modelID {
				t.Fatal(meta)
			}
			gater, ok := fake.runner.(runtimebridge.DeliveryGater)
			if !ok {
				t.Fatal("Runner gate unavailable")
			}
			admitted := false
			if err := gater.DeliveryGate(context.Background(), "cli-trace", "100@chatroom", func(context.Context) error { admitted = true; return nil }); err != nil || !admitted {
				t.Fatal("mock submission admission failed", err)
			}
		})
	}
}

func TestLoadRealDispatcherDoesNotExposePrivateOperatorPaths(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		t.Error("load contacted backend or bridge", request.URL)
	}))
	defer server.Close()
	for _, kind := range []execution.Kind{execution.AlmaPersonal, execution.TextOnly, execution.LegacyHermes} {
		t.Run(string(kind), func(t *testing.T) {
			args := configuredArguments(t, server.URL, kind)
			service, _, err := loadDispatcher(args, func(path string, options dispatcher.Options) (dispatcherService, error) {
				return dispatcher.NewDispatcherWithOptions(path, options)
			})
			if err != nil {
				t.Fatal(err)
			}
			defer service.Close()
			for _, path := range []string{"/api/config", "/health", "/api/control/snapshot"} {
				request := httptest.NewRequest("GET", "http://127.0.0.1"+path, nil)
				request.RemoteAddr = "127.0.0.1:1234"
				response := httptest.NewRecorder()
				service.Handler().ServeHTTP(response, request)
				if response.Code != http.StatusOK {
					t.Fatal(path, response.Code, response.Body.String())
				}
				for _, private := range []string{args.backendsJSON, args.executionConfig, "mock-provider", "private-hermes-stub", "private-hermes-work", "private-fixed-profile"} {
					if strings.Contains(response.Body.String(), private) {
						t.Fatal("private operator configuration exposed", path, private)
					}
				}
				if path == "/api/control/snapshot" {
					var control dispatcher.ControlSnapshot
					if err := json.Unmarshal(response.Body.Bytes(), &control); err != nil || !control.Capabilities["deliveryGate"] || !control.Capabilities["stopExecution"] || control.Capabilities["legacySoft"] || control.Capabilities["remoteCancel"] {
						t.Fatal("execution runner capabilities not connected", control.Capabilities, err)
					}
				}
			}
			if kind == execution.LegacyHermes {
				entries, err := os.ReadDir(filepath.Join(filepath.Dir(args.config), "private-hermes-work"))
				if err != nil || len(entries) != 0 {
					t.Fatal("load or HTTP read executed Hermes command", entries, err)
				}
			}
		})
	}
}

func TestSharedLegacySingleInstanceLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	legacyLockPath := filepath.Join(filepath.Dir(path), ".dispatcher.lock")
	legacy, err := os.OpenFile(legacyLockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer legacy.Close()
	if err := syscall.Flock(int(legacy.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if lock, err := acquireLock(path); err == nil {
		lock.Close()
		t.Fatal("new CLI ignored legacy dispatcher lock")
	}
	if err := syscall.Flock(int(legacy.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	lock, err := acquireLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if lock.Name() != legacyLockPath {
		t.Fatal(lock.Name())
	}
	if second, err := acquireLock(path); err == nil {
		second.Close()
		t.Fatal("second new dispatcher acquired same lock")
	}
}

func TestInvalidBackendOrMissingStoreNeverConstructsDispatcher(t *testing.T) {
	for _, scenario := range []string{"missing_store", "unknown_backend_field"} {
		t.Run(scenario, func(t *testing.T) {
			args := configuredArguments(t, "http://127.0.0.1:1234", execution.TextOnly)
			if scenario == "missing_store" {
				args.executionConfig = filepath.Join(t.TempDir(), "missing.json")
			} else {
				if err := os.WriteFile(args.backendsJSON, []byte(`{"schemaVersion":"1","backends":[],"proxy":"http://example.invalid"}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, _, err := loadDispatcher(args, func(string, dispatcher.Options) (dispatcherService, error) {
				t.Fatal("invalid runtime constructed dispatcher")
				return nil, nil
			})
			if err == nil {
				t.Fatal("invalid configuration accepted:", scenario)
			}
		})
	}
}
