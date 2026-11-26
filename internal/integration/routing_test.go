package integration_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"

	"jinx/internal/contracts"
	"jinx/internal/execution"
)

type providerFunc func(context.Context, execution.ModelRequest) (string, error)

func (provider providerFunc) Generate(ctx context.Context, request execution.ModelRequest) (string, error) {
	return provider(ctx, request)
}

func TestRoutingFallbackAndPermissionBlock(t *testing.T) {
	for _, scenario := range []string{"compatible_fallback", "delivery_disabled", "generation_disabled"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			var primaryCalls, fallbackCalls, posts, writes atomic.Int32
			primary := providerFunc(func(context.Context, execution.ModelRequest) (string, error) {
				primaryCalls.Add(1)
				return "", fmt.Errorf("synthetic primary failure")
			})
			fallback := providerFunc(func(context.Context, execution.ModelRequest) (string, error) {
				fallbackCalls.Add(1)
				return syntheticReply, nil
			})
			config := executionConfig(execution.TextOnly, syntheticModel, 2)
			group := config.Groups[sourceGroup]
			if scenario == "generation_disabled" {
				policy := config.Policies[group.PolicyID]
				policy.Capabilities[execution.TextGeneration] = false
				config.Policies[group.PolicyID] = policy
			} else {
				attempt := group.Primary
				attempt.BackendID = "synthetic-fallback"
				group.Fallbacks = []execution.RouteAttempt{attempt}
				config.Groups[sourceGroup] = group
			}
			if scenario == "delivery_disabled" {
				policy := config.Policies[group.PolicyID]
				policy.Capabilities[execution.AutoTextDelivery] = false
				config.Policies[group.PolicyID] = policy
			}
			_, runner := newExecution(t, root, nil, []execution.BackendOptions{
				{ID: "synthetic-primary", Kind: execution.TextOnly, Models: []execution.Model{{ID: syntheticModel, Label: "Synthetic"}}, Provider: primary},
				{ID: "synthetic-fallback", Kind: execution.TextOnly, Models: []execution.Model{{ID: syntheticModel, Label: "Synthetic"}}, Provider: fallback},
			}, config)
			relay := newRelay(t, root)
			bridge := newBridge(t, relay.Client(), relay.URL, &posts, make(chan bridgeDelivery, 2))
			path, _ := prepareDispatcher(t, root, relay.URL, bridge.URL)
			services := startStack(t, path, filepath.Join(root, "console"), relay.URL, bridge.URL, &writes, runner)
			postMessage(t, relay.Client(), relay.URL, "routing-input", "@Jinx synthetic routing question")
			run := waitRun(t, services, "routing-input")
			result, err := runner.observed(run.ID)
			if scenario == "compatible_fallback" {
				if err != nil || run.Delivery != contracts.DeliveryConfirmed || posts.Load() != 1 || primaryCalls.Load() != 1 || fallbackCalls.Load() != 1 || !result.Fallback || len(result.Attempts) != 2 || result.Attempts[0].Status != "failed" || result.Attempts[1].FallbackReason == "" {
					t.Fatalf("fallback evidence lost: %+v err=%v trace=%+v", result, err, run)
				}
			} else {
				if posts.Load() != 0 || !run.DeliveryEvidence.NoSubmission || run.Outcome == contracts.OutcomeReplied {
					t.Fatalf("permission block crossed sink: result=%+v err=%v trace=%+v", result, err, run)
				}
				if scenario == "generation_disabled" && execution.Code(err) != "policy_blocked" {
					t.Fatalf("generation denial lost explicit policy failure: %v", err)
				}
				if scenario == "delivery_disabled" && (err != nil || !result.Fallback || result.DeliveryAllowed) {
					t.Fatalf("delivery denial misreported execution: %+v %v", result, err)
				}
				if scenario == "generation_disabled" && (primaryCalls.Load() != 0 || fallbackCalls.Load() != 0) {
					t.Fatal("denied generation still called a provider")
				}
			}
		})
	}
}

func TestRoutingRejectsPermissionWeakeningFallback(t *testing.T) {
	root := t.TempDir()
	engine, err := execution.Open(filepath.Join(root, "execution.json"), execution.Options{Backends: []execution.BackendOptions{
		{ID: "synthetic-primary", Kind: execution.TextOnly, Models: []execution.Model{{ID: syntheticModel, Label: "Synthetic"}}, Provider: providerFunc(func(context.Context, execution.ModelRequest) (string, error) {
			t.Error("validation called provider")
			return "", nil
		})},
		{ID: "synthetic-personal", Kind: execution.AlmaPersonal, Models: []execution.Model{{ID: syntheticModel, Label: "Synthetic"}}, AlmaBaseURL: "http://127.0.0.1:1"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	config := executionConfig(execution.TextOnly, syntheticModel, 1)
	group := config.Groups[sourceGroup]
	fallback := group.Primary
	fallback.BackendID = "synthetic-personal"
	group.Fallbacks = []execution.RouteAttempt{fallback}
	config.Groups[sourceGroup] = group
	before, err := engine.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Plan(context.Background(), before.Revision, config)
	if execution.Code(err) != "fallback_policy_mismatch" {
		t.Fatalf("hard no-tools route accepted soft personal fallback: %v", err)
	}
	after, err := engine.Snapshot(context.Background())
	if err != nil || after.Revision != before.Revision || len(after.Config.Groups) != 0 {
		t.Fatal("rejected fallback changed durable routing")
	}
}
