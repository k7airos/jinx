package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"jinx/internal/dispatcher"
)

func checkConfigFixture(t *testing.T, endpoint string) arguments {
	t.Helper()
	directory := t.TempDir()
	args := arguments{config: filepath.Join(directory, "config.json"), localPolicy: filepath.Join(directory, "policy.json")}
	var config dispatcher.Config
	config.Server.ListenAddr = strings.TrimPrefix(endpoint, "http://")
	config.Relay.StreamURL = endpoint + "/stream"
	config.Bridge.URL = endpoint + "/send"
	config.Hermes.BinPath = filepath.Join(directory, "backend-stub")
	config.Hermes.Profile = "demo"
	config.Hermes.TimeoutSec = 5
	config.Rules.BatchSize = 1
	config.Rules.IdleTimeoutSec = 1
	if err := os.WriteFile(config.Hermes.BinPath, []byte("#!/bin/sh\nprintf executed > \"${0}.executed\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, args.config, config)
	writeJSON(t, args.localPolicy, map[string]int{"version": 1})
	return args
}

func TestCheckConfigCLIFlags(t *testing.T) {
	t.Setenv("JINX_LOCAL_POLICY", "")
	for _, argv := range [][]string{
		{"-check-config"},
		{"-check-config", "-execution-config", "execution.json", "-backends-json", "operator.json"},
		{"-check-config", "-config", "config.json", "extra"},
		{"-check-config", "-config", "config.json", "-unknown"},
		{"-check-config=false", "-config", "config.json"},
	} {
		if _, err := parseArgs(argv, io.Discard); err == nil {
			t.Fatal("invalid check arguments accepted", argv)
		}
	}
	args, err := parseArgs([]string{"-check-config", "-config", "config.json"}, io.Discard)
	if err != nil || !args.checkConfig || !filepath.IsAbs(args.config) || args.executionConfig != "" || args.backendsJSON != "" {
		t.Fatal("check mode required runtime inputs", args, err)
	}
	t.Setenv("JINX_LOCAL_POLICY", "demo-environment-policy.json")
	for _, explicit := range []bool{false, true} {
		argv := []string{"-check-config", "-config", "config.json"}
		policy := "demo-environment-policy.json"
		if explicit {
			policy = "demo-explicit-policy.json"
			argv = append(argv, "-local-policy", policy)
		}
		expected, err := filepath.Abs(policy)
		if err != nil {
			t.Fatal(err)
		}
		args, err := parseArgs(argv, io.Discard)
		if err != nil || args.localPolicy != expected {
			t.Fatal("check policy precedence changed", args, err)
		}
	}
}

func TestCheckConfigReadOnlyBeforeLockAndBackends(t *testing.T) {
	t.Setenv("JINX_LOCAL_POLICY", "")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
	}))
	defer server.Close()
	for _, existingLock := range []bool{false, true} {
		t.Run(map[bool]string{false: "no_lock", true: "held_lock"}[existingLock], func(t *testing.T) {
			args := checkConfigFixture(t, server.URL)
			if existingLock {
				lock, err := acquireLock(args.config)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			}
			args.backendsJSON = filepath.Join(filepath.Dir(args.config), "invalid-backends.json")
			if err := os.WriteFile(args.backendsJSON, []byte("invalid backend definitions"), 0600); err != nil {
				t.Fatal(err)
			}
			args.executionConfig = filepath.Join(filepath.Dir(args.config), "missing-execution.json")
			before, err := os.ReadDir(filepath.Dir(args.config))
			if err != nil {
				t.Fatal(err)
			}
			contents := map[string]string{}
			for _, entry := range before {
				data, err := os.ReadFile(filepath.Join(filepath.Dir(args.config), entry.Name()))
				if err != nil {
					t.Fatal(err)
				}
				contents[entry.Name()] = string(data)
			}
			if err := dispatcher.ValidateConfigFile(args.config, args.localPolicy); err != nil {
				t.Fatal(err)
			}
			for _, runtimeFlags := range []bool{false, true} {
				argv := []string{"-check-config", "-config", args.config, "-local-policy", args.localPolicy}
				if runtimeFlags {
					argv = append(argv, "-execution-config", args.executionConfig, "-backends-json", args.backendsJSON)
				}
				var output strings.Builder
				if err := run(context.Background(), argv, &output); err != nil || output.String() != "configuration valid\n" {
					t.Fatal("read-only check failed", err, output.String())
				}
			}
			after, err := os.ReadDir(filepath.Dir(args.config))
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("check created lock, logs, execution store or backend output", err)
			}
			for name, expected := range contents {
				data, err := os.ReadFile(filepath.Join(filepath.Dir(args.config), name))
				if err != nil || string(data) != expected {
					t.Fatal("check changed an existing file", name, err)
				}
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("check contacted a backend, relay or bridge")
	}
}

func TestCheckConfigRejectsInvalidInputWithoutPrivateValues(t *testing.T) {
	t.Setenv("JINX_LOCAL_POLICY", "")
	for _, scenario := range []string{"unknown", "nested_unknown", "trailing_object", "trailing_scalar", "trailing_garbage", "malformed", "invalid_rule", "missing", "directory", "oversized", "policy_unknown", "policy_trailing", "policy_missing", "policy_permissions", "policy_symlink"} {
		t.Run(scenario, func(t *testing.T) {
			args := checkConfigFixture(t, "http://127.0.0.1:1234")
			data, err := os.ReadFile(args.config)
			if err != nil {
				t.Fatal(err)
			}
			expected := "invalid dispatcher configuration"
			switch scenario {
			case "unknown":
				data = []byte(`{"DEMO_PRIVATE_UNKNOWN_FIELD":"DEMO_PRIVATE_VALUE"}`)
			case "nested_unknown":
				data = []byte(`{"server":{"listen_addr":"127.0.0.1:0","DEMO_PRIVATE_UNKNOWN_FIELD":"DEMO_PRIVATE_VALUE"}}`)
			case "trailing_object":
				data = append(data, []byte(`{"private":"DEMO_PRIVATE_VALUE"}`)...)
			case "trailing_scalar":
				data = append(data, []byte(`"DEMO_PRIVATE_VALUE"`)...)
			case "trailing_garbage":
				data = append(data, []byte("DEMO_PRIVATE_VALUE")...)
			case "malformed":
				data = []byte(`{"DEMO_PRIVATE_VALUE"`)
			case "invalid_rule":
				var config dispatcher.Config
				if err := json.Unmarshal(data, &config); err != nil {
					t.Fatal(err)
				}
				config.Rules.GroupRules = map[string]dispatcher.GroupRule{"DEMO_PRIVATE_GROUP": {}}
				data, err = json.Marshal(config)
				if err != nil {
					t.Fatal(err)
				}
			case "oversized":
				data = append(data, []byte(strings.Repeat(" ", 4<<20))...)
				expected = "dispatcher config unreadable or too large"
			case "missing":
				args.config = filepath.Join(filepath.Dir(args.config), "DEMO_PRIVATE_MISSING.json")
				expected = "dispatcher config unreadable or too large"
			case "directory":
				args.config = filepath.Dir(args.config)
				expected = "dispatcher config unreadable or too large"
			case "policy_unknown":
				writeJSON(t, args.localPolicy, map[string]any{"version": 1, "DEMO_PRIVATE_UNKNOWN_FIELD": "DEMO_PRIVATE_VALUE"})
			case "policy_trailing":
				if err := os.WriteFile(args.localPolicy, []byte(`{"version":1} {"private":"DEMO_PRIVATE_VALUE"}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "policy_missing":
				args.localPolicy = filepath.Join(filepath.Dir(args.config), "DEMO_PRIVATE_MISSING_POLICY.json")
			case "policy_permissions":
				if err := os.Chmod(args.localPolicy, 0644); err != nil {
					t.Fatal(err)
				}
			case "policy_symlink":
				link := filepath.Join(filepath.Dir(args.config), "policy-link.json")
				if err := os.Symlink(args.localPolicy, link); err != nil {
					t.Fatal(err)
				}
				args.localPolicy = link
			}
			if strings.HasPrefix(scenario, "policy_") {
				expected = "local policy validation failed"
			}
			switch scenario {
			case "policy_missing", "policy_permissions", "policy_symlink":
				expected = "local policy validation failed: local policy files must be existing regular files with mode 0600 (chmod 600)"
			}
			if scenario != "missing" && scenario != "directory" {
				if err := os.WriteFile(args.config, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := dispatcher.ValidateConfigFile(args.config, args.localPolicy); err == nil || err.Error() != expected {
				t.Fatal("helper did not return the fixed validation error", err)
			}
			var output strings.Builder
			err = run(context.Background(), []string{"-check-config", "-config", args.config, "-local-policy", args.localPolicy}, &output)
			if err == nil || err.Error() != expected || output.Len() != 0 {
				t.Fatal("CLI did not return the fixed validation error", err, output.String())
			}
			for _, name := range []string{".dispatcher.lock", "logs"} {
				if _, err := os.Stat(filepath.Join(filepath.Dir(args.localPolicy), name)); !os.IsNotExist(err) {
					t.Fatal("failed check created state", name, err)
				}
			}
		})
	}
}

func TestCheckConfigLocalPolicyFallbackAndFlagPrecedence(t *testing.T) {
	args := checkConfigFixture(t, "http://127.0.0.1:1234")
	for _, useEnvironment := range []bool{false, true} {
		if useEnvironment {
			t.Setenv("JINX_LOCAL_POLICY", args.localPolicy)
		} else {
			t.Setenv("JINX_LOCAL_POLICY", "")
		}
		if err := dispatcher.ValidateConfigFile(args.config, ""); err != nil {
			t.Fatal("optional or environment policy rejected", err)
		}
		var output strings.Builder
		if err := run(context.Background(), []string{"-check-config", "-config", args.config}, &output); err != nil || output.String() != "configuration valid\n" {
			t.Fatal("CLI environment policy rejected", err, output.String())
		}
	}
	t.Setenv("JINX_LOCAL_POLICY", filepath.Join(filepath.Dir(args.config), "DEMO_PRIVATE_MISSING_POLICY.json"))
	if err := dispatcher.ValidateConfigFile(args.config, ""); err == nil || !strings.HasPrefix(err.Error(), "local policy validation failed") {
		t.Fatal("helper ignored environment policy", err)
	}
	if err := dispatcher.ValidateConfigFile(args.config, args.localPolicy); err != nil {
		t.Fatal("helper did not prefer explicit policy", err)
	}
	var output strings.Builder
	if err := run(context.Background(), []string{"-check-config", "-config", args.config}, &output); err == nil || !strings.HasPrefix(err.Error(), "local policy validation failed") {
		t.Fatal("CLI ignored environment policy", err)
	}
	if err := run(context.Background(), []string{"-check-config", "-config", args.config, "-local-policy", args.localPolicy}, &output); err != nil || output.String() != "configuration valid\n" {
		t.Fatal("CLI did not prefer explicit policy", err, output.String())
	}
	writeJSON(t, args.localPolicy, map[string]int{"version": 2})
	if err := dispatcher.ValidateConfigFile(args.config, args.localPolicy); err == nil {
		t.Fatal("helper ignored invalid explicit policy")
	}
}

func TestCheckConfigUsesLocalPolicyForSupportValidation(t *testing.T) {
	t.Setenv("JINX_LOCAL_POLICY", "")
	args := checkConfigFixture(t, "http://127.0.0.1:1234")
	data, err := os.ReadFile(args.config)
	if err != nil {
		t.Fatal(err)
	}
	var config dispatcher.Config
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	config.Rules.GroupWhitelist = []string{"100@chatroom"}
	config.Rules.GroupRules = map[string]dispatcher.GroupRule{"100@chatroom": {
		Enabled: true, Target: "Demo Room", HermesProfile: "demo-100", SupportMode: true, SupportSinceMS: 1, SelfSenderIDs: []string{"synthetic-self"},
	}}
	writeJSON(t, args.config, config)
	writeJSON(t, args.localPolicy, map[string]any{
		"version": 1,
		"identities": map[string]any{"100@chatroom": map[string]string{
			"profile": "demo-100", "memory_user": "demo-memory", "vector_collection": "demo-collection",
		}},
		"support": map[string]any{"enabled": true, "group_id": "100@chatroom", "self_sender_ids": []string{"synthetic-self"}},
	})
	if err := dispatcher.ValidateConfigFile(args.config, args.localPolicy); err != nil {
		t.Fatal("authorized support config rejected", err)
	}
	if err := dispatcher.ValidateConfigFile(args.config, ""); err == nil {
		t.Fatal("support config accepted without local authorization")
	}
	config.Rules.GroupRules = map[string]dispatcher.GroupRule{}
	config.Rules.GroupWhitelist = nil
	writeJSON(t, args.config, config)
	if err := dispatcher.ValidateConfigFile(args.config, args.localPolicy); err == nil {
		t.Fatal("support route removal accepted")
	}
}
