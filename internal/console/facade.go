package console

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type savedPlan struct {
	Plan            ChangePlan             `json:"plan"`
	RequestID       string                 `json:"requestId"`
	InputHash       string                 `json:"inputHash"`
	Patch           map[string]any         `json:"patch"`
	PublishID       string                 `json:"publishId"`
	Record          *ChangeRecord          `json:"record"`
	Before          *ConfigurationSnapshot `json:"before,omitempty"`
	Target          *ConfigurationSnapshot `json:"target,omitempty"`
	RelayPatch      *RelayRules            `json:"relayPatch,omitempty"`
	RelayCAS        bool                   `json:"relayCAS,omitempty"`
	ProtectedHash   string                 `json:"protectedHash,omitempty"`
	RollbackOf      *string                `json:"rollbackOf,omitempty"`
	BeforeProtected map[string]string      `json:"beforeProtected,omitempty"`
}

type Facade struct {
	mu                  sync.Mutex
	backend             *upstream
	stateDir            string
	plans               map[string]*savedPlan
	lock                *os.File
	traceMu             sync.Mutex
	listings            map[string]*runListing
	connectorMu         sync.Mutex
	connectorPlans      map[string]*savedConnectorPlan
	configurationReader ConfigurationReader
	relayWriter         RelayConfigWriter
	acknowledgements    map[string]AttentionAcknowledgement
	executionCatalog    ExecutionCatalogReader
}

func New(options Options) (*Facade, error) {
	backend, err := newUpstream(options)
	if err != nil {
		return nil, err
	}
	if options.StateDir == "" {
		return nil, failure("validation", "An isolated console state directory is required")
	}
	facade := &Facade{backend: backend, stateDir: options.StateDir, plans: map[string]*savedPlan{}, listings: map[string]*runListing{}, connectorPlans: map[string]*savedConnectorPlan{}}
	facade.configurationReader, facade.relayWriter = options.ConfigurationReader, options.RelayWriter
	facade.executionCatalog = options.ExecutionCatalog
	facade.acknowledgements = map[string]AttentionAcknowledgement{}
	if err := os.MkdirAll(options.StateDir, 0700); err != nil {
		return nil, failure("internal", "Cannot create console journal directory")
	}
	lock, err := os.OpenFile(filepath.Join(options.StateDir, "console-changes.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, failure("internal", "Cannot open console journal lock")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, failure("conflict", "Another console process owns this journal")
	}
	facade.lock = lock
	opened := false
	defer func() {
		if !opened {
			facade.Close()
		}
	}()
	data, err := os.ReadFile(filepath.Join(options.StateDir, "console-changes.json"))
	if err != nil && !os.IsNotExist(err) {
		return nil, failure("internal", "Cannot read console change journal")
	}
	if err == nil {
		if json.Unmarshal(data, &facade.plans) != nil || facade.plans == nil {
			return nil, failure("internal", "Console change journal is invalid")
		}
		for _, saved := range facade.plans {
			if saved == nil || saved.Plan.ID == "" || saved.Plan.Token == "" || saved.Patch == nil || (saved.Record != nil && (len(saved.Record.Operation.Components) < 1 || len(saved.Record.Operation.Components) > 2)) {
				return nil, failure("internal", "Console change journal is invalid")
			}
			if saved.Record != nil && saved.Record.Operation.Status == "pending" {
				saved.Record.Operation.Status = "unknown"
				saved.Record.Operation.Summary = "Process ended during publication; readback is required, do not repeat the write"
				for index := range saved.Record.Operation.Components {
					if saved.Record.Operation.Components[index].State == "applying" {
						saved.Record.Operation.Components[index].State = "unknown"
					}
				}
			}
			if saved.ProtectedHash == "" {
				if groupMap, ok := saved.Patch["group_rules"].(map[string]any); ok {
					saved.ProtectedHash = digest(protectedGroups(groupMap))
				}
			}
			saved.Patch = safePatch(saved.Patch)
		}
	}
	if err := facade.loadAcknowledgements(); err != nil {
		return nil, err
	}
	opened = true
	return facade, nil
}

func (facade *Facade) Close() error {
	facade.mu.Lock()
	defer facade.mu.Unlock()
	if facade.lock == nil {
		return nil
	}
	err := facade.lock.Close()
	facade.lock = nil
	return err
}

func newID() string {
	var value [24]byte
	if _, err := rand.Read(value[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(value[:])
}

func digest(value any) string {
	data, _ := json.Marshal(value)
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func versions(config snapshot) Versions {
	return Versions{Dispatcher: config.etag, Fingerprint: digest(config.etag)}
}

func cloneConfig(config snapshot) snapshot {
	data, _ := json.Marshal(config.root)
	var root map[string]any
	_ = json.Unmarshal(data, &root)
	return snapshot{root: root, rules: root["rules"].(map[string]any), etag: config.etag}
}

const maxRetainedConsolePlans = 256

func (facade *Facade) prunePlansLocked() {
	if len(facade.plans) <= maxRetainedConsolePlans {
		return
	}
	now := time.Now()
	type candidate struct {
		id      string
		settled bool
		expires time.Time
	}
	candidates := make([]candidate, 0, len(facade.plans))
	for id, saved := range facade.plans {
		if saved == nil {
			delete(facade.plans, id)
			continue
		}
		settled := saved.Record != nil || !now.Before(saved.Plan.ExpiresAt)
		candidates = append(candidates, candidate{id: id, settled: settled, expires: saved.Plan.ExpiresAt})
	}
	if len(facade.plans) <= maxRetainedConsolePlans {
		return
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].settled != candidates[j].settled {
			return candidates[i].settled
		}
		if !candidates[i].expires.Equal(candidates[j].expires) {
			return candidates[i].expires.Before(candidates[j].expires)
		}
		return candidates[i].id < candidates[j].id
	})
	excess := len(facade.plans) - maxRetainedConsolePlans
	for i := 0; i < excess && i < len(candidates); i++ {
		if !candidates[i].settled {
			break
		}
		delete(facade.plans, candidates[i].id)
	}
}

func (facade *Facade) persist() error {
	facade.prunePlansLocked()
	if err := os.MkdirAll(facade.stateDir, 0700); err != nil {
		return err
	}
	data, err := json.Marshal(facade.plans)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(facade.stateDir, ".console-changes-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(file.Name(), filepath.Join(facade.stateDir, "console-changes.json")); err != nil {
		return err
	}
	directory, err := os.Open(facade.stateDir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func validRequestID(id string) bool { return len(id) >= 1 && len(id) <= 128 }

func (facade *Facade) PlanChange(ctx context.Context, input PlanInput) (ChangePlan, error) {
	facade.mu.Lock()
	defer facade.mu.Unlock()
	if facade.lock == nil {
		return ChangePlan{}, failure("unavailable", "Console journal is closed")
	}
	if !validRequestID(input.RequestID) || input.SchemaVersion != SchemaVersion || len(input.Edits) == 0 || len(input.Edits) > 100 {
		return ChangePlan{}, failure("validation", "A request ID, current schema version and 1..100 edits are required")
	}
	for _, saved := range facade.plans {
		if saved.RequestID == input.RequestID {
			if saved.InputHash != digest(input) {
				return ChangePlan{}, failure("conflict", "Request ID was already used for another plan")
			}
			return saved.Plan, nil
		}
	}
	before, err := facade.backend.config(ctx)
	if err != nil {
		return ChangePlan{}, err
	}
	var relayBefore *relaySnapshot
	dispatcherEdits := []Edit{}
	needsRelay := false
	for _, edit := range input.Edits {
		if edit.Kind == "relay_field" || edit.Kind == "group_collection" {
			needsRelay = true
		}
		if edit.Kind == "group_collection" {
			dispatcherEdits = append(dispatcherEdits, Edit{Kind: "group_admission", GroupID: edit.GroupID, Enabled: edit.Enabled})
		} else if edit.Kind != "relay_field" {
			dispatcherEdits = append(dispatcherEdits, edit)
		}
	}
	base := versions(before)
	if needsRelay {
		relay, err := facade.backend.relayConfig(ctx)
		if err != nil {
			return ChangePlan{}, err
		}
		relayBefore, base.Relay = &relay, relay.digest
	}
	if !sameVersions(input.Base, base) {
		return ChangePlan{}, failure("conflict", "Configuration changed; read and plan again")
	}
	after := cloneConfig(before)
	patch, err := applyEdits(after, dispatcherEdits)
	if err != nil {
		return ChangePlan{}, err
	}
	changes := diff(before, after, patch)
	var relayPatch *RelayRules
	if relayBefore != nil {
		target, err := applyRelayEdits(*relayBefore, input.Edits)
		if err != nil {
			return ChangePlan{}, err
		}
		relayChanges := relayDiff(relayBefore.rules, target)
		changes = append(changes, relayChanges...)
		if len(relayChanges) > 0 {
			relayPatch = &target
		}
		for _, edit := range input.Edits {
			if edit.Kind != "group_collection" {
				continue
			}
			group, _ := groups(after)[edit.GroupID].(map[string]any)
			enabled, _ := group["enabled"].(bool)
			if enabled != edit.Enabled || includes(after.rules["group_whitelist"], edit.GroupID) != edit.Enabled {
				return ChangePlan{}, failure("validation", "Final dispatcher admission contradicts the collection edit")
			}
		}
	}
	if len(changes) == 0 {
		return ChangePlan{}, failure("validation", "The plan contains no changes")
	}
	data, _ := json.Marshal(patch)
	if len(data) > maxBody {
		return ChangePlan{}, failure("too_large", "Merged rule patch exceeds 256 KiB")
	}
	affected := []string{}
	for id := range groups(before) {
		affected = append(affected, id)
	}
	for id := range groups(after) {
		if _, exists := groups(before)[id]; !exists {
			affected = append(affected, id)
		}
	}
	sort.Strings(affected)
	order := []string{}
	if len(diff(before, after, patch)) > 0 {
		order = append(order, "dispatcher")
	} else {
		patch = map[string]any{}
	}
	if relayPatch != nil {
		order = append(order, "relay")
	}
	plan := ChangePlan{
		ID: newID(), Token: newID(), ExpiresAt: time.Now().UTC().Add(5 * time.Minute), Base: base,
		Changes: changes, AffectedGroups: affected, Effect: []string{"hot_reload"}, ComponentOrder: order,
		Warnings: []Warning{
			{Code: "config_hash_changed", Message: "Any existing batch, including other groups, may have output discarded when the configuration hash changes"},
			{Code: "disable_clears_pending", Message: "Disabling or deleting a route clears its pending work; this is not pause"},
			{Code: "target_unverified", Message: "Target search uniqueness and Relay visibility are not verified; no test message will be sent"},
		},
	}
	if relayPatch != nil && !relayBefore.cas && facade.relayWriter == nil {
		plan.Warnings = append(plan.Warnings, relayWarnings()...)
	}
	if relayPatch != nil {
		plan.Warnings = append(plan.Warnings, Warning{Code: "cross_component_partial", Message: "Components publish and read back in order; failure stops later steps without automatic compensation"})
	}
	targetSnapshot := captureConfiguration(after, relayBefore)
	targetSnapshot.Source = "console_intended_policy"
	if relayPatch != nil {
		targetSnapshot.Relay = copyJSON(relayPatch)
	}
	targetSnapshot.Versions.Dispatcher, targetSnapshot.Versions.Fingerprint = "", ""
	if targetSnapshot.Relay != nil {
		targetSnapshot.Versions.Relay = ""
	}
	facade.plans[plan.ID] = &savedPlan{Plan: plan, RequestID: input.RequestID, InputHash: digest(input), Patch: safePatch(patch), ProtectedHash: digest(protectedGroups(groups(after))), BeforeProtected: protectedDigests(before), Before: captureConfiguration(before, relayBefore), Target: targetSnapshot, RelayPatch: relayPatch}
	if relayBefore != nil {
		facade.plans[plan.ID].RelayCAS = relayBefore.cas
	}
	if err := facade.persist(); err != nil {
		delete(facade.plans, plan.ID)
		return ChangePlan{}, failure("internal", "Cannot persist change plan")
	}
	return plan, nil
}

func (facade *Facade) PublishChange(ctx context.Context, input PublishInput) (Operation, error) {
	facade.mu.Lock()
	defer facade.mu.Unlock()
	if facade.lock == nil {
		return Operation{}, failure("unavailable", "Console journal is closed")
	}
	if !validRequestID(input.RequestID) {
		return Operation{}, failure("validation", "Request ID is required")
	}
	saved := facade.plans[input.PlanID]
	if saved == nil || subtle.ConstantTimeCompare([]byte(input.Token), []byte(saved.Plan.Token)) != 1 {
		return Operation{}, failure("forbidden", "Plan token is invalid")
	}
	for _, previous := range facade.plans {
		if previous.PublishID == input.RequestID && previous != saved {
			return Operation{}, failure("conflict", "Request ID was already used for another publication")
		}
	}
	if saved.Record != nil {
		if saved.PublishID != input.RequestID {
			return Operation{}, failure("conflict", "Plan has already been submitted")
		}
		return operationView(saved.Record.Operation), nil
	}
	if !time.Now().Before(saved.Plan.ExpiresAt) {
		return Operation{}, failure("stale", "Plan expired; read and plan again")
	}
	current, err := facade.backend.config(ctx)
	if err != nil {
		return Operation{}, err
	}
	base := versions(current)
	if saved.Plan.Base.Relay != "" {
		relay, err := facade.backend.relayConfig(ctx)
		if err != nil {
			return Operation{}, err
		}
		base.Relay = relay.digest
		if relay.cas != saved.RelayCAS {
			return Operation{}, failure("conflict", "Relay configuration API changed since preview")
		}
	}
	if !sameVersions(base, saved.Plan.Base) {
		return Operation{}, failure("conflict", "Configuration changed since preview")
	}
	saved.PublishID = input.RequestID
	components := []ComponentResult{}
	for _, id := range saved.Plan.ComponentOrder {
		components = append(components, ComponentResult{ID: id, State: "not_started"})
	}
	if len(components) == 0 {
		components = append(components, ComponentResult{ID: "dispatcher", State: "not_started"})
	}
	saved.Record = &ChangeRecord{
		ID: saved.Plan.ID, CreatedAt: time.Now().UTC(), Origin: "console", Base: saved.Plan.Base, Changes: saved.Plan.Changes,
		BeforeSnapshot: copyJSON(saved.Before), TargetSnapshot: copyJSON(saved.Target), RollbackOf: saved.RollbackOf,
		Operation: Operation{ID: newID(), RequestID: input.RequestID, Revision: newID(), ChangeID: saved.Plan.ID, Status: "pending", Summary: "Publication is awaiting readback", Components: components},
	}
	if err := facade.persist(); err != nil {
		saved.Record, saved.PublishID = nil, ""
		return Operation{}, failure("internal", "Cannot persist publication intent; no upstream write was issued")
	}
	for index := range saved.Record.Operation.Components {
		component := &saved.Record.Operation.Components[index]
		component.State = "applying"
		if err := facade.persist(); err != nil {
			return operationView(saved.Record.Operation), failure("unknown", "Cannot persist component intent; read the operation")
		}
		status, writeErr := 0, error(nil)
		if component.ID == "relay" {
			status, writeErr = facade.writeRelay(ctx, saved)
		} else {
			body, _ := json.Marshal(restoreProtected(saved.Patch, current))
			_, _, status, writeErr = facade.backend.request(ctx, "dispatcher", "POST", "/api/config", current.etag, body)
		}
		readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), facade.backend.client.Timeout)
		facade.readbackComponent(readCtx, saved, index, status, writeErr)
		cancel()
		if problem, ok := writeErr.(*Problem); ok && component.ID == "relay" && problem.Code == "conflict" && status == 409 {
			component.State, component.PersistenceVerified = "failed_not_applied", nil
		}
		if component.State != "verified" {
			break
		}
	}
	facade.summarizeOperation(saved)
	saved.Record.Operation.Revision = newID()
	if err := facade.persist(); err != nil {
		return operationView(saved.Record.Operation), failure("unknown", "Write was attempted but final journal persistence failed; read operation before further action")
	}
	return operationView(saved.Record.Operation), nil
}

func operationView(operation Operation) Operation {
	operation.Components = append([]ComponentResult(nil), operation.Components...)
	return operation
}

func (facade *Facade) readback(ctx context.Context, saved *savedPlan, status int, writeErr error) {
	previous := digest(saved.Record.Operation)
	for index := range saved.Record.Operation.Components {
		component := saved.Record.Operation.Components[index]
		if component.State == "unknown" || component.State == "applying" {
			facade.readbackComponent(ctx, saved, index, status, writeErr)
		}
	}
	facade.summarizeOperation(saved)
	if digest(saved.Record.Operation) != previous {
		saved.Record.Operation.Revision = newID()
	}
}

func (facade *Facade) readbackComponent(ctx context.Context, saved *savedPlan, index, status int, writeErr error) {
	operation := &saved.Record.Operation
	component := &operation.Components[index]
	component.State = "unknown"
	if component.ID == "relay" {
		current, err := facade.backend.relayConfig(ctx)
		if err != nil {
			return
		}
		component.ReadbackVersion = &current.digest
		if saved.Record.Readback == nil {
			value := saved.Plan.Base
			saved.Record.Readback = &value
		}
		saved.Record.Readback.Relay = current.digest
		if saved.Record.ReadbackSnapshot == nil {
			saved.Record.ReadbackSnapshot = &ConfigurationSnapshot{CapturedAt: time.Now().UTC(), Source: "relay_memory", Scope: []string{"relay.group_control", "relay.filters"}}
		}
		saved.Record.ReadbackSnapshot.Relay = copyJSON(&current.rules)
		if saved.Record.ReadbackSnapshot.Dispatcher != nil {
			saved.Record.ReadbackSnapshot.Source = "dispatcher_and_relay_memory"
			saved.Record.ReadbackSnapshot.Scope = []string{"dispatcher.rules", "dispatcher.bridge.dry_run", "relay.group_control", "relay.filters"}
		}
		saved.Record.ReadbackSnapshot.Versions = *saved.Record.Readback
		if saved.RelayPatch != nil && reflect.DeepEqual(*saved.RelayPatch, current.rules) {
			if status >= 200 && status < 300 && writeErr == nil {
				verified := true
				component.State, component.PersistenceVerified = "verified", &verified
			} else {
				component.State = "applied_persistence_unverified"
			}
		} else if current.digest == saved.Plan.Base.Relay && status >= 400 && status < 500 {
			component.State = "failed_not_applied"
		}
		return
	}
	current, err := facade.backend.config(ctx)
	if err != nil {
		return
	}
	readVersions := versions(current)
	if saved.Record.Readback != nil {
		readVersions.Relay = saved.Record.Readback.Relay
	}
	saved.Record.Readback = &readVersions
	var relayReadback *RelayRules
	if saved.Record.ReadbackSnapshot != nil {
		relayReadback = saved.Record.ReadbackSnapshot.Relay
	}
	saved.Record.ReadbackSnapshot = captureConfiguration(current, nil)
	if relayReadback != nil {
		saved.Record.ReadbackSnapshot.Relay = relayReadback
		saved.Record.ReadbackSnapshot.Source = "dispatcher_and_relay_memory"
		saved.Record.ReadbackSnapshot.Scope = append(saved.Record.ReadbackSnapshot.Scope, "relay.group_control", "relay.filters")
	}
	saved.Record.ReadbackSnapshot.Versions = readVersions
	operation.ResultRevision, component.ReadbackVersion = &current.etag, &current.etag
	public := dispatcherAllowed(current)
	comparable := snapshot{root: public, rules: public["rules"].(map[string]any), etag: current.etag}
	matches, err := matchPatch(comparable, saved.Patch)
	if saved.ProtectedHash != "" && saved.Patch["group_rules"] != nil && digest(protectedGroups(groups(current))) != saved.ProtectedHash {
		matches = false
	}
	if err != nil {
		operation.Summary = "Publication readback cannot be normalized losslessly with the current rule schema; do not automatically retry"
		return
	}
	if matches {
		if status >= 200 && status < 300 && writeErr == nil {
			verified := true
			component.State = "verified"
			component.PersistenceVerified = &verified
			operation.Summary = "Dispatcher acknowledged saving and the configuration matches readback"
		} else {
			component.State = "applied_persistence_unverified"
			operation.Summary = "Target configuration was read back; upstream persistence acknowledgement is unavailable"
		}
	} else if current.etag == saved.Plan.Base.Dispatcher && status >= 400 && status < 500 && writeErr == nil {
		component.State = "failed_not_applied"
		operation.Summary = "Dispatcher rejected the publication and the base configuration was read back"
	}
}

func (facade *Facade) summarizeOperation(saved *savedPlan) {
	operation := &saved.Record.Operation
	verified, applied, failed := 0, 0, 0
	for _, component := range operation.Components {
		if component.State == "verified" {
			verified++
			applied++
		}
		if component.State == "applied_persistence_unverified" {
			applied++
		}
		if component.State == "failed_not_applied" {
			failed++
		}
	}
	switch {
	case verified == len(operation.Components):
		operation.Status, operation.Summary = "succeeded", "All component writes were acknowledged and matched sequential readback"
	case applied > 0:
		operation.Status, operation.Summary = "partial_apply", "At least one component applied; remaining writes or persistence are unverified. No automatic compensation was attempted"
	case failed > 0:
		operation.Status, operation.Summary = "failed", "Publication was rejected before any component was verified"
	default:
		operation.Status = "unknown"
		if !strings.Contains(operation.Summary, "cannot be normalized losslessly") {
			operation.Summary = "Publication outcome is unknown; do not automatically retry"
		}
	}
	if applied == len(operation.Components) && saved.Record.Readback != nil {
		saved.Record.Target = copyJSON(saved.Record.Readback)
	}
}

func (facade *Facade) GetOperation(ctx context.Context, id string) (Operation, error) {
	operation, err := facade.getOperation(ctx, id, false)
	if err != nil {
		return Operation{}, err
	}
	if operation == nil {
		return Operation{}, failure("not_found", "No publication fact exists for this operation ID")
	}
	return *operation, nil
}

func (facade *Facade) GetOperationByRequest(ctx context.Context, requestID string) (*Operation, error) {
	return facade.getOperation(ctx, requestID, true)
}

func (facade *Facade) getOperation(ctx context.Context, id string, byRequest bool) (*Operation, error) {
	facade.mu.Lock()
	defer facade.mu.Unlock()
	if facade.lock == nil {
		return nil, failure("unavailable", "Console journal is closed")
	}
	var found *savedPlan
	for _, saved := range facade.plans {
		if saved.Record == nil {
			continue
		}
		if !byRequest && saved.Record.Operation.ID == id {
			found = saved
			break
		}
		if saved.PublishID == id {
			found = saved
		}
	}
	if found == nil {
		return nil, nil
	}
	needsReadback := found.Record.Operation.Status == "unknown"
	for _, component := range found.Record.Operation.Components {
		needsReadback = needsReadback || component.State == "unknown" || component.State == "applying"
	}
	if needsReadback {
		facade.readback(ctx, found, 0, nil)
		if err := facade.persist(); err != nil {
			return nil, failure("internal", "Cannot persist operation readback")
		}
	}
	operation := operationView(found.Record.Operation)
	return &operation, nil
}

func (facade *Facade) ActiveOperations() []Operation {
	facade.mu.Lock()
	defer facade.mu.Unlock()
	items := []Operation{}
	for _, saved := range facade.plans {
		if saved.Record != nil {
			operation := saved.Record.Operation
			if operation.Status == "pending" || operation.Status == "unknown" || operation.Status == "partial_apply" {
				items = append(items, operationView(operation))
			}
		}
	}
	sort.Slice(items, func(first, second int) bool { return items[first].ID < items[second].ID })
	return items
}

func (facade *Facade) ListChanges() []ChangeRecord {
	facade.mu.Lock()
	defer facade.mu.Unlock()
	items := []ChangeRecord{}
	for _, saved := range facade.plans {
		if saved.Record != nil {
			record := copyJSON(*saved.Record)
			items = append(items, record)
		}
	}
	sort.Slice(items, func(first, second int) bool { return items[first].CreatedAt.After(items[second].CreatedAt) })
	return items
}

func envelope(data any, revision, source, completeness string) Envelope {
	return Envelope{Data: data, ObservedAt: time.Now().UTC(), Source: source, Freshness: "fresh", Completeness: completeness, Revision: revision}
}

func (facade *Facade) GetGlobalRules(ctx context.Context) (Envelope, error) {
	config, err := facade.backend.config(ctx)
	if err != nil {
		return Envelope{}, err
	}
	values := map[string]FieldValue{}
	for name := range globalSchema() {
		value := config.rules[name]
		if name == "dry_run" {
			value = config.root["bridge"].(map[string]any)[name]
		}
		if name == "forward_rules" {
			if norm, err := canonicalForwardRules(value); err == nil {
				value = norm
			} else if value == nil {
				value = []any{}
			}
		}
		values[name] = FieldValue{Configured: value, Effective: value, Origin: "global", Editable: true}
	}
	return envelope(map[string]any{"fields": values, "versions": versions(config), "compatibility": compatibility(config)}, config.etag, "dispatcher_memory", "complete"), nil
}

func groupView(config snapshot, id string) GroupView {
	raw, exists := groups(config)[id].(map[string]any)
	label, _ := raw["target"].(string)
	view := GroupView{ID: id, Group: GroupRef{ID: id, Visibility: "not_observed"}, LabelSource: "unverified", Revision: config.etag, Versions: versions(config), RouteExists: exists, Whitelisted: includes(config.rules["group_whitelist"], id), Fields: publicFields(config, raw), Visibility: "not_observed", Permissions: []any{}, AttentionCodes: []string{}, MissingRoutingReason: "No routing provider is connected"}
	if label != "" {
		view.LabelSource = "config_target_ui_only"
		view.UITargetLabel = label
	}
	view.Enabled, _ = raw["enabled"].(bool)
	globalDry, _ := config.root["bridge"].(map[string]any)["dry_run"].(bool)
	groupDry, _ := raw["dry_run"].(bool)
	view.EffectiveDryRun = globalDry || groupDry
	for name, effective := range map[string]string{"target_group_id": id, "hermes_profile": ""} {
		field := view.Fields[name]
		if field.Configured == "" || field.Configured == nil {
			if name == "hermes_profile" {
				field.Effective, field.Reason = nil, "Internal profile is not exposed"
			} else {
				field.Effective = effective
			}
			field.Origin = "default"
		}
		if name == "hermes_profile" {
			field.Configured, field.Effective = nil, nil
		}
		view.Fields[name] = field
	}
	return view
}

func publicFields(config snapshot, group map[string]any) map[string]FieldValue {
	values := fields(config, group)
	profile := values["hermes_profile"]
	profile.Configured, profile.Effective = nil, nil
	profile.Reason = "Internal profile is not exposed"
	values["hermes_profile"] = profile
	return values
}

func (facade *Facade) ListGroups(ctx context.Context) (Envelope, error) {
	config, err := facade.backend.config(ctx)
	if err != nil {
		return Envelope{}, err
	}
	items := []GroupView{}
	for id := range groups(config) {
		items = append(items, groupView(config, id))
	}
	sort.Slice(items, func(first, second int) bool { return items[first].ID < items[second].ID })
	return envelope(map[string]any{"items": items, "nextCursor": nil, "coverageSince": nil}, config.etag, "dispatcher_memory", "partial"), nil
}

func (facade *Facade) GetGroup(ctx context.Context, id string) (Envelope, error) {
	config, err := facade.backend.config(ctx)
	if err != nil {
		return Envelope{}, err
	}
	if _, exists := groups(config)[id]; !exists {
		return Envelope{}, failure("validation", "Group is not configured")
	}
	return envelope(groupView(config, id), config.etag, "dispatcher_memory", "partial"), nil
}
