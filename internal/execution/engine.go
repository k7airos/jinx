package execution

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
)

type activeRun struct {
	cancel context.CancelFunc
}

type Engine struct {
	path    string
	options Options
	client  *http.Client
	mu      sync.Mutex
	active  map[string]activeRun
}

func Open(path string, options Options) (*Engine, error) {
	if path == "" {
		return nil, failure("validation", "path")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	options.Backends = append([]BackendOptions{}, options.Backends...)
	seen := map[string]bool{}
	for index := range options.Backends {
		option := &options.Backends[index]
		option.Models = append([]Model{}, option.Models...)
		option.Hermes.Toolsets = append([]string{}, option.Hermes.Toolsets...)
		option.Hermes.Env = append([]string{}, option.Hermes.Env...)
		if err := validateBackend(*option); err != nil {
			return nil, err
		}
		if seen[option.ID] {
			return nil, failure("validation", "duplicate_backend")
		}
		seen[option.ID] = true
	}
	client := http.Client{}
	if options.Client != nil {
		client = *options.Client
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return failure("forbidden", "redirect") }
	if client.Transport == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		client.Transport = transport
	}
	engine := &Engine{path: absolute, options: options, client: &client, active: map[string]activeRun{}}
	if err := os.MkdirAll(filepath.Dir(absolute), 0700); err != nil {
		return nil, err
	}
	err = engine.locked(context.Background(), func() error {
		_, err := engine.read()
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		revision, err := newRevision()
		if err != nil {
			return err
		}
		return engine.write(diskState{Snapshot: Snapshot{Revision: revision, Config: Config{SchemaVersion: SchemaVersion,
			Groups: map[string]Group{}, Profiles: map[string]string{}, Policies: map[string]Policy{}}}, Receipts: map[string]receipt{}})
	})
	return engine, err
}

func (engine *Engine) Snapshot(ctx context.Context) (Snapshot, error) {
	var state diskState
	err := engine.locked(ctx, func() error {
		var err error
		state, err = engine.read()
		return err
	})
	return state.Snapshot, err
}

func (engine *Engine) GetGroup(groupID string) (GroupView, error) {
	snapshot, err := engine.Snapshot(context.Background())
	if err != nil {
		return GroupView{}, err
	}
	group, exists := snapshot.Config.Groups[groupID]
	if !exists {
		return GroupView{}, failure("not_found", "groupId")
	}
	policy := snapshot.Config.Policies[group.PolicyID]
	return GroupView{GroupID: groupID, Revision: snapshot.Revision, Routing: group, Policy: policy,
		Permissions: engine.permissions(group.Primary, policy, snapshot.Revision)}, nil
}

func (engine *Engine) GroupForProfile(ctx context.Context, profile string) (string, error) {
	snapshot, err := engine.Snapshot(ctx)
	if err != nil {
		return "", err
	}
	groupID, exists := snapshot.Config.Profiles[profile]
	if !exists {
		return "", failure("not_found", "profile")
	}
	return groupID, nil
}

func (engine *Engine) Validate(ctx context.Context, config Config) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return engine.validate(config)
}

func (engine *Engine) Plan(ctx context.Context, expectedRev string, config Config) (ChangePlan, error) {
	if err := engine.Validate(ctx, config); err != nil {
		return ChangePlan{}, err
	}
	snapshot, err := engine.Snapshot(ctx)
	if err != nil {
		return ChangePlan{}, err
	}
	if snapshot.Revision != expectedRev {
		return ChangePlan{}, &Error{Code: "conflict", Field: "expectedRevision", CurrentRevision: snapshot.Revision}
	}
	plan := ChangePlan{ExpectedRevision: expectedRev, ConfigDigest: digest(config), AffectedGroups: []string{},
		Effects: []string{"new_execution", "immediate_permission_tightening"}, Warnings: []string{"dispatcher_publication_is_separate", "remote_work_cannot_be_recalled"}}
	groups := map[string]bool{}
	for id := range snapshot.Config.Groups {
		groups[id] = true
	}
	for id := range config.Groups {
		groups[id] = true
	}
	for id := range groups {
		before, after := snapshot.Config.Groups[id], config.Groups[id]
		if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(snapshot.Config.Policies[before.PolicyID], config.Policies[after.PolicyID]) || !reflect.DeepEqual(snapshot.Config.Profiles, config.Profiles) {
			plan.AffectedGroups = append(plan.AffectedGroups, id)
		}
	}
	sort.Strings(plan.AffectedGroups)
	return plan, nil
}

func (engine *Engine) ApplyConfig(ctx context.Context, expectedRev, requestID string, config Config) (ApplyResult, error) {
	if !tokenPattern.MatchString(requestID) {
		return ApplyResult{}, failure("validation", "requestId")
	}
	if err := engine.Validate(ctx, config); err != nil {
		return ApplyResult{}, err
	}
	requestDigest := digest(struct {
		Expected string
		Config   Config
	}{expectedRev, config})
	var result ApplyResult
	err := engine.locked(ctx, func() error {
		state, err := engine.read()
		if err != nil {
			return err
		}
		if previous, exists := state.Receipts[requestID]; exists {
			if previous.Digest != requestDigest {
				return failure("idempotency_conflict", "requestId")
			}
			if err := engine.write(state); err != nil {
				return err
			}
			result = ApplyResult{Snapshot: state.Snapshot, AppliedRevision: previous.Revision, Replayed: true}
			return nil
		}
		if state.Revision != expectedRev {
			return &Error{Code: "conflict", Field: "expectedRevision", CurrentRevision: state.Revision}
		}
		if digest(state.Config) != digest(config) {
			state.Revision, err = newRevision()
			if err != nil {
				return err
			}
			state.Config = config
		}
		state.Receipts[requestID] = receipt{Digest: requestDigest, Revision: state.Revision}
		if err := engine.write(state); err != nil {
			return err
		}
		readback, err := engine.read()
		if err != nil || readback.Revision != state.Revision || digest(readback.Config) != digest(config) {
			return failure("persistence_unknown", "readback")
		}
		result = ApplyResult{Snapshot: readback.Snapshot, AppliedRevision: state.Revision}
		return nil
	})
	return result, err
}

func (engine *Engine) Update(ctx context.Context, expectedRev, requestID string, config Config) (ApplyResult, error) {
	return engine.ApplyConfig(ctx, expectedRev, requestID, config)
}

func intersectPolicies(original, current Policy) Policy {
	policy := Policy{Capabilities: map[Capability]bool{}, NoTools: original.NoTools || current.NoTools, AllowSoft: original.AllowSoft && current.AllowSoft}
	for _, capability := range Capabilities() {
		policy.Capabilities[capability] = original.Capabilities[capability] && current.Capabilities[capability]
	}
	return policy
}

func (engine *Engine) currentPolicy(ctx context.Context, groupID string, original Policy) (Policy, string, error) {
	snapshot, err := engine.Snapshot(ctx)
	if err != nil {
		return Policy{}, "", err
	}
	group, exists := snapshot.Config.Groups[groupID]
	if !exists {
		return Policy{}, snapshot.Revision, failure("policy_blocked", "group_removed")
	}
	return intersectPolicies(original, snapshot.Config.Policies[group.PolicyID]), snapshot.Revision, nil
}

func (engine *Engine) CheckDelivery(ctx context.Context, result Result, groupID string) error {
	return engine.locked(ctx, func() error { return engine.checkDelivery(result, groupID) })
}

func (engine *Engine) checkDelivery(result Result, groupID string) error {
	state, err := engine.read()
	if err != nil {
		return err
	}
	group, exists := state.Config.Groups[groupID]
	if !exists || result.GroupID != groupID || !result.DeliveryAllowed || result.Silent || result.Text == "" || result.PolicyRevision != state.Revision || !state.Config.Policies[group.PolicyID].Capabilities[AutoTextDelivery] {
		return failure("policy_blocked", "delivery_revision_or_permission")
	}
	return nil
}

func (engine *Engine) WithDeliveryGate(ctx context.Context, result Result, groupID string, submit func(context.Context) error) error {
	return engine.locked(ctx, func() error {
		if err := engine.checkDelivery(result, groupID); err != nil {
			return err
		}
		if result.Silent || result.Text == "" {
			return failure("policy_blocked", "silent")
		}
		return submit(ctx)
	})
}
