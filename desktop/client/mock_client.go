package client

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// MockClient provides an isolated, strictly synthetic in-memory console backend
// covering target-page-spec scenarios ("full", "degraded", "conflict",
// "unknown-delivery"). Zero real IDs, zero filesystem paths, zero network calls.
type MockClient struct {
	mu          sync.Mutex
	scenario    string
	now         time.Time
	seq         uint64
	planSeq     int
	revNum      int
	biliDryRun  bool
	webDryRun   bool
	controls    ControlSnapshot
	maint       *Maintenance
	catalog     CatalogView
	connectors  map[string]ConnectorView
	connPlans   map[string]ConnectorDryRunPlan
	groups      map[string]GroupView
	groupOrder  []string
	globalRules GlobalRulesView
	relayRules  RelayRulesView
	backends    []Backend
	routing     map[string]GroupRouting
	perms       map[string]GroupPermissions
	services    []ServiceView
	runtime     RuntimeServices
	svcPlans    map[string]ServicePlan
	runs        []RunView
	timelines   map[string][]TimelineItem
	snapshots   map[string]ConfigSnapshot
	changes     []ChangeRecord
	plans       map[string]ChangePlan
	planBaseRev map[string]Versions
	attention   []AttentionItem
	drift       map[string]ConfigurationDrift
	runSubs     map[int]RunCallbacks
	archSubs    map[int]ArchitectureCallbacks
	nextSubID   int
}

func ptrStr(s string) *string       { return &s }
func ptrBool(b bool) *bool          { return &b }
func ptrInt(i int) *int             { return &i }
func ptrFloat(f float64) *float64   { return &f }
func ptrUint64(u uint64) *uint64    { return &u }
func ptrTime(t time.Time) *time.Time { return &t }

// NewMockClient builds a synthetic sandbox populated for scenario.
// Supported scenarios: "full", "degraded", "conflict", "unknown-delivery".
func NewMockClient(scenario string) *MockClient {
	switch scenario {
	case "degraded", "conflict", "unknown-delivery", "full":
	default:
		scenario = "full"
	}
	now := time.Date(2026, 11, 24, 8, 0, 0, 0, time.UTC)
	m := &MockClient{
		scenario:    scenario,
		now:         now,
		seq:         42,
		revNum:      10,
		biliDryRun:  true,
		webDryRun:   true,
		connPlans:   map[string]ConnectorDryRunPlan{},
		groups:      map[string]GroupView{},
		routing:     map[string]GroupRouting{},
		perms:       map[string]GroupPermissions{},
		svcPlans:    map[string]ServicePlan{},
		timelines:   map[string][]TimelineItem{},
		snapshots:   map[string]ConfigSnapshot{},
		plans:       map[string]ChangePlan{},
		planBaseRev: map[string]Versions{},
		drift:       map[string]ConfigurationDrift{},
		runSubs:     map[int]RunCallbacks{},
		archSubs:    map[int]ArchitectureCallbacks{},
	}
	m.initData()
	return m
}

func (m *MockClient) IsMock() bool  { return true }
func (m *MockClient) Label() string { return "mock:" + m.scenario }

func (m *MockClient) versions() Versions {
	fp := fmt.Sprintf("fp-demo-%04d", m.revNum)
	r := fmt.Sprintf("route-v%d", m.revNum)
	p := fmt.Sprintf("pol-v%d", m.revNum)
	return Versions{
		Dispatcher:  fmt.Sprintf("disp-v%d", m.revNum),
		Relay:       fmt.Sprintf("relay-v%d", m.revNum),
		Routing:     &r,
		Policy:      &p,
		Fingerprint: fp,
	}
}

func (m *MockClient) initData() {
	v := m.versions()
	m.catalog = CatalogView{
		Scenarios: []CatalogScenario{
			{ID: "support-qa", Builtin: true, HasLocalPolicy: true},
			{ID: "community-chat", Builtin: true, HasLocalPolicy: false},
			{ID: "comment-helper", Builtin: true, HasLocalPolicy: false},
			{ID: "sandbox-lab", Builtin: true, HasLocalPolicy: false},
		},
		KnowledgeBases: []CatalogKnowledgeBase{
			{ID: "example-kb"},
			{ID: "example-faq"},
			{ID: "release-notes-demo"},
		},
		Tools: []CatalogTool{
			{ID: "knowledge-lookup", SupportAllowed: true},
			{ID: "issue-summary", SupportAllowed: true},
			{ID: "admin-export", SupportAllowed: false},
		},
		Skills: []CatalogSkill{
			{ID: "triage-checklist"},
			{ID: "concise-reply"},
			{ID: "thread-context"},
		},
	}

	biliLabel := "示例视频评论区"
	biliPoll := m.now.Add(-12 * time.Second)
	m.connectors = map[string]ConnectorView{
		"bilibili": {
			Platform:          "bilibili",
			Configured:        true,
			State:             "ready",
			Availability:      Availability{Supported: true, Enabled: true},
			DryRun:            ptrBool(true),
			Capabilities:      []string{"reply_text", "thread_reply"},
			Controls:          []string{"dry_run"},
			AccountID:         ptrStr("example_bili_acct"),
			ConnectorName:     ptrStr("bilibili-demo-connector"),
			Targets:           []ConnectorTargetView{{ContainerID: "bilibili:BV1example0001", Label: &biliLabel, Enabled: true}},
			LastPollAt:        &biliPoll,
			LastPollErrorCode: nil,
			ProtocolVersion:   ptrInt(1),
			ObservedAt:        m.now,
		},
		"web": {
			Platform:     "web",
			Configured:   false,
			State:        "unsupported",
			Availability: Availability{Supported: false, Enabled: false, Reason: "暂未配置接入"},
			Capabilities: []string{},
			Controls:     []string{},
			Targets:      []ConnectorTargetView{},
			ObservedAt:   m.now,
		},
	}
	if m.scenario == "degraded" {
		webPoll := m.now.Add(-3 * time.Minute)
		m.connectors["web"] = ConnectorView{
			Platform:          "web",
			Configured:        true,
			State:             "unreachable",
			Availability:      Availability{Supported: true, Enabled: false, Reason: "连接器心跳不可达"},
			Capabilities:      []string{"reply_text", "thread_reply"},
			Controls:          []string{},
			AccountID:         ptrStr("example_web_acct"),
			ConnectorName:     ptrStr("web-demo-connector"),
			Targets:           []ConnectorTargetView{{ContainerID: "web:demo-thread-1", Label: ptrStr("示例网页讨论帖"), Enabled: true}},
			LastPollAt:        &webPoll,
			LastPollErrorCode: ptrStr("unavailable"),
			Reason:            ptrStr("连接器心跳不可达"),
			ProtocolVersion:   ptrInt(1),
			ObservedAt:        m.now,
		}
	}

	m.backends = []Backend{
		{
			ID: "alma-demo", Kind: "alma_personal", Available: true, AgentBinding: "branch_only",
			SessionModes: []string{"independent_run"}, MinTimeoutSec: 5, MaxTimeoutSec: 120,
			Models: []ModelRef{{ID: "demo-fast", Label: "快速响应模型"}, {ID: "demo-reasoning", Label: "深度推理模型"}},
			PermissionEvidence: []Permission{
				{Capability: "text_generation", Requested: ptrBool(true), Effective: ptrBool(true), Enforcement: "hard", Provenance: "backend_contract", PolicyVersion: "pol-v10", EnforcedAt: []string{"dispatcher", "adapter"}, Editable: false, Reason: "运行时强制硬边界"},
				{Capability: "auto_text_delivery", Requested: ptrBool(true), Effective: ptrBool(true), Enforcement: "host", Provenance: "bridge_gate", PolicyVersion: "pol-v10", EnforcedAt: []string{"dispatcher", "bridge"}, Editable: true, Reason: "宿主发送网关门控"},
				{Capability: "web_search", Requested: ptrBool(true), Effective: ptrBool(false), Enforcement: "soft", Provenance: "prompt_policy", PolicyVersion: "pol-v10", EnforcedAt: []string{"prompt"}, Editable: false, Reason: "提示词软约束（只读展示，不可作为硬拦截）"},
				{Capability: "host_write", Requested: ptrBool(false), Effective: ptrBool(false), Enforcement: "unverified", Provenance: "not_reported", PolicyVersion: "pol-v10", EnforcedAt: []string{}, Editable: false, Reason: "宿主未上报该权限状态"},
			},
		},
		{
			ID: "text-fallback", Kind: "text_only", Available: true, AgentBinding: "branch_only",
			SessionModes: []string{"independent_run"}, MinTimeoutSec: 5, MaxTimeoutSec: 60,
			Models: []ModelRef{{ID: "demo-lite", Label: "Demo Lite"}},
			PermissionEvidence: []Permission{
				{Capability: "text_generation", Requested: ptrBool(true), Effective: ptrBool(true), Enforcement: "hard", Provenance: "backend_contract", PolicyVersion: "pol-v10", EnforcedAt: []string{"dispatcher"}, Editable: false},
			},
		},
	}

	makeField := func(cfg, eff any, origin string, editable bool, reason string) FieldValue {
		return FieldValue{Configured: cfg, Effective: eff, Origin: origin, Editable: editable, Reason: reason}
	}
	m.groupOrder = []string{"100001@chatroom", "100002@chatroom", "100003@chatroom", "bilibili:BV1example0001"}
	confirmed := m.now.Add(-2 * time.Minute)
	m.groups["100001@chatroom"] = GroupView{
		ID: "100001@chatroom",
		Group: GroupRef{ID: "100001@chatroom", Label: "示例客服群", Visibility: "visible"},
		LabelSource: "config_target_ui_only", UITargetLabel: "示例客服群",
		MissingRoutingReason: "", Permissions: []any{}, BackendID: ptrStr("alma-demo"),
		LatestConfirmedAt: &confirmed, AttentionCodes: []string{"delivery_unknown"},
		Revision: v.Fingerprint, Versions: v,
		RouteExists: true, Enabled: true, Whitelisted: true, EffectiveDryRun: false, Visibility: "visible",
		Fields: map[string]FieldValue{
			"enabled":             makeField(true, true, "group", true, ""),
			"support_mode":        makeField(true, true, "group", false, "受本地私有策略锁定"),
			"dry_run":             makeField(false, false, "group", true, ""),
			"mention_only":        makeField(false, false, "group", true, ""),
			"hermes_profile":      makeField("[已脱敏]", "[已脱敏]", "local_policy", false, "已屏蔽私有身份映射"),
			"scenario":            makeField("support-qa", "support-qa", "group", true, ""),
			"response_goal":       makeField("简明排查用户问题并定位根因", "简明排查用户问题并定位根因", "group", true, ""),
			"tone":                makeField("沉稳、细致、有条理", "沉稳、细致、有条理", "group", true, ""),
			"skills":              makeField([]string{"triage-checklist"}, []string{"triage-checklist"}, "group", true, ""),
			"tools":               makeField([]string{"knowledge-lookup"}, []string{"knowledge-lookup"}, "group", true, ""),
			"knowledge_bases":     makeField([]string{"example-kb"}, []string{"example-kb"}, "group", true, ""),
			"self_sender_ids":     makeField(nil, []string{"wxid_example_bot_0001"}, "global", true, ""),
			"allowed_types":       makeField([]int{1, 49}, []int{1, 49}, "group", true, ""),
			"blocked_sender_ids":  makeField([]string{}, []string{}, "group", true, ""),
			"keyword_blocklist":   makeField([]string{"spam-token"}, []string{"spam-token"}, "group", true, ""),
			"regex_blocklist":     makeField(nil, nil, "default", true, ""),
			"promo_keywords":      makeField(nil, nil, "default", true, ""),
			"promo_shadow":        makeField(false, false, "group", true, ""),
			"promo_daily_cap":     makeField(float64(1), float64(1), "group", true, ""),
			"batch_size":          makeField(float64(3), float64(3), "group", true, ""),
			"idle_timeout_sec":    makeField(float64(8), float64(8), "group", true, ""),
			"max_batch_wait_sec":  makeField(float64(25), float64(25), "group", true, ""),
			"max_reply_runes":     makeField(float64(500), float64(500), "group", true, ""),
			"reply_window_sec":    makeField(float64(45), float64(45), "group", true, ""),
			"max_replies_in_window": makeField(float64(2), float64(2), "group", true, ""),
			"decision_threshold":  makeField(0.65, 0.65, "group", true, ""),
		},
	}
	m.groups["100002@chatroom"] = GroupView{
		ID: "100002@chatroom",
		Group: GroupRef{ID: "100002@chatroom", Label: "示例社区交流群", Visibility: "visible"},
		LabelSource: "config_target_ui_only", UITargetLabel: "示例社区交流群",
		Permissions: []any{}, BackendID: ptrStr("alma-demo"), Revision: v.Fingerprint, Versions: v,
		RouteExists: true, Enabled: true, Whitelisted: true, EffectiveDryRun: true, Visibility: "visible",
		Fields: map[string]FieldValue{
			"enabled":            makeField(true, true, "group", true, ""),
			"support_mode":       makeField(false, false, "group", false, ""),
			"dry_run":            makeField(true, true, "group", true, ""),
			"mention_only":       makeField(true, true, "group", true, ""),
			"scenario":           makeField("community-chat", "community-chat", "group", true, ""),
			"response_goal":      makeField("友好解答社群日常提问", "友好解答社群日常提问", "group", true, ""),
			"tone":               makeField("自然亲切、简短清晰", "自然亲切、简短清晰", "group", true, ""),
			"skills":             makeField(nil, nil, "default", true, ""),
			"tools":              makeField([]string{}, nil, "group", true, ""),
			"knowledge_bases":    makeField([]string{"example-faq"}, []string{"example-faq"}, "group", true, ""),
			"self_sender_ids":    makeField([]string{"wxid_example_bot_0001"}, []string{"wxid_example_bot_0001"}, "group", true, ""),
			"allowed_types":      makeField(nil, []int{1, 49, 81}, "default", true, ""),
			"blocked_sender_ids": makeField(nil, nil, "default", true, ""),
			"keyword_blocklist":  makeField([]string{}, []string{}, "group", true, ""),
			"regex_blocklist":    makeField(nil, nil, "default", true, ""),
			"decision_threshold": makeField(0.55, 0.55, "group", true, ""),
		},
	}
	m.groups["100003@chatroom"] = GroupView{
		ID: "100003@chatroom",
		Group: GroupRef{ID: "100003@chatroom", Label: "合成实验群", Visibility: "not_observed"},
		LabelSource: "unverified", UITargetLabel: "合成实验群",
		Permissions: []any{}, BackendID: ptrStr("text-fallback"), Revision: v.Fingerprint, Versions: v,
		RouteExists: true, Enabled: false, Whitelisted: false, EffectiveDryRun: true, Visibility: "not_observed",
		Fields: map[string]FieldValue{
			"enabled":            makeField(false, false, "group", true, ""),
			"dry_run":            makeField(true, true, "group", true, ""),
			"mention_only":       makeField(true, true, "group", true, ""),
			"scenario":           makeField("sandbox-lab", "sandbox-lab", "group", true, ""),
			"response_goal":      makeField("隔离沙盒回归验证", "隔离沙盒回归验证", "group", true, ""),
			"tone":               makeField("中性客观", "中性客观", "group", true, ""),
			"self_sender_ids":    makeField([]string{}, []string{}, "group", true, ""),
			"allowed_types":      makeField(nil, []int{1, 49, 81}, "default", true, ""),
			"blocked_sender_ids": makeField(nil, nil, "default", true, ""),
			"keyword_blocklist":  makeField(nil, nil, "default", true, ""),
			"regex_blocklist":    makeField(nil, nil, "default", true, ""),
		},
	}
	m.groups["bilibili:BV1example0001"] = GroupView{
		ID: "bilibili:BV1example0001",
		Group: GroupRef{ID: "bilibili:BV1example0001", Label: "示例 B站评论区", Visibility: "visible"},
		LabelSource: "config_target_ui_only", UITargetLabel: "示例 B站评论区",
		Permissions: []any{}, BackendID: ptrStr("alma-demo"), Revision: v.Fingerprint, Versions: v,
		RouteExists: true, Enabled: true, Whitelisted: true, EffectiveDryRun: true, Visibility: "visible",
		Fields: map[string]FieldValue{
			"enabled":            makeField(true, true, "group", true, ""),
			"dry_run":            makeField(true, true, "group", true, ""),
			"mention_only":       makeField(false, false, "group", true, ""),
			"scenario":           makeField("comment-helper", "comment-helper", "group", true, ""),
			"response_goal":      makeField("补充楼中楼实用上下文", "补充楼中楼实用上下文", "group", true, ""),
			"tone":               makeField("友好自然、避免硬广", "友好自然、避免硬广", "group", true, ""),
			"knowledge_bases":    makeField([]string{"example-kb"}, []string{"example-kb"}, "group", true, ""),
			"skills":             makeField([]string{"thread-context"}, []string{"thread-context"}, "group", true, ""),
			"self_sender_ids":    makeField(nil, nil, "default", true, ""),
			"allowed_types":      makeField([]int{1}, []int{1}, "group", true, ""),
			"blocked_sender_ids": makeField([]string{}, []string{}, "group", true, ""),
			"keyword_blocklist":  makeField([]string{}, []string{}, "group", true, ""),
			"regex_blocklist":    makeField([]string{}, []string{}, "group", true, ""),
		},
	}

	for _, gid := range m.groupOrder {
		backendID := "alma-demo"
		modelID := "demo-fast"
		if gid == "100003@chatroom" {
			backendID = "text-fallback"
			modelID = "demo-lite"
		}
		m.routing[gid] = GroupRouting{
			GroupID: gid, Revision: v.Fingerprint,
			Routing: Routing{
				RouteID:  "rt-" + gid,
				PolicyID: "pol-" + gid,
				Primary:  RouteAttempt{BackendID: backendID, ModelID: modelID, TimeoutSec: 45, Session: SessionPolicy{Mode: "independent_run"}},
				Fallbacks: []RouteAttempt{{BackendID: "text-fallback", ModelID: "demo-lite", TimeoutSec: 20, Session: SessionPolicy{Mode: "independent_run"}}},
			},
		}
		m.perms[gid] = GroupPermissions{
			GroupID: gid, Revision: v.Fingerprint,
			Policy: Policy{
				Capabilities: map[string]bool{"text_generation": true, "auto_text_delivery": true, "web_search": false, "host_write": false},
				NoTools:      false, AllowSoft: false,
			},
			Permissions: m.backends[0].PermissionEvidence,
		}
	}

	m.globalRules = GlobalRulesView{
		Versions: v,
		Fields: map[string]FieldValue{
			"laya_filter":                makeField(true, true, "global", true, ""),
			"bypass_decision_on_mention": makeField(true, true, "global", true, ""),
			"ignore_xml":                 makeField(true, true, "global", true, ""),
			"dry_run":                    makeField(false, false, "global", true, ""),
			"bot_names":                  makeField([]string{"JINX-Demo"}, []string{"JINX-Demo"}, "global", true, ""),
			"self_sender_ids":            makeField([]string{"wxid_example_bot_0001"}, []string{"wxid_example_bot_0001"}, "global", true, ""),
			"keyword_blocklist":          makeField([]string{"demo-blocked"}, []string{"demo-blocked"}, "global", true, ""),
			"batch_size":                 makeField(float64(5), float64(5), "global", true, ""),
			"idle_timeout_sec":           makeField(float64(8), float64(8), "global", true, ""),
			"max_batch_wait_sec":         makeField(float64(25), float64(25), "global", true, ""),
			"max_message_age_sec":        makeField(float64(300), float64(300), "global", true, ""),
			"forward_rules": makeField(
				[]ForwardRule{{
					ID:                "demo-v2ex-to-wx",
					Enabled:           true,
					SourceContainerID: "v2ex:t-100001",
					TargetContainerID: "100002@chatroom",
					DryRun:            true,
				}},
				[]ForwardRule{{
					ID:                "demo-v2ex-to-wx",
					Enabled:           true,
					SourceContainerID: "v2ex:t-100001",
					TargetContainerID: "100002@chatroom",
					DryRun:            true,
				}},
				"global", true, "",
			),
		},
		Compatibility: CompatibilityBlock{
			GroupWhitelist: []string{"100001@chatroom", "100002@chatroom", "bilibili:BV1example0001"},
			GroupProfiles:  map[string]string{"100001@chatroom": "redacted-profile-a"},
			GroupOtherBots: map[string][]string{"100002@chatroom": {"wxid_example_other_bot_01"}},
			DefaultProfile: ptrStr("default"),
			Editable:       false,
			Semantics: map[string]string{
				"groupWhitelist": "Compatibility projection only; manage admission per container",
				"groupProfiles":  "Read-only legacy mapping",
				"groupOtherBots": "Read-only deduplication list",
				"defaultProfile": "Read-only runtime default",
			},
		},
	}
	m.relayRules = RelayRulesView{
		Versions: v,
		WriteConcurrency: "digest_preflight_local_mutex",
		Warnings: []Warning{{Code: "relay_cas", Message: "Relay config writes use digest preflight CAS"}},
		Rules: RelayRules{
			GroupControl: RelayGroupControl{
				MonitorAll:              false,
				MonitoredGroups:         TriStringsValues("100001@chatroom", "100002@chatroom", "100003@chatroom"),
				ExposedGroups:           TriStringsValues("100001@chatroom", "100002@chatroom"),
				GroupAliases:            map[string]string{"100001@chatroom": "Example Support Room"},
				DropUnexposedCompletely: true,
			},
			Filters: RelayFilters{
				KeywordBlocklist: TriStringsEmpty(),
				RegexBlocklist:   TriStringsNull(),
				SenderBlocklist:  TriStringsValues("wxid_synthetic_spam_99"),
				AllowedTypes:     TriIntsValues(1, 49, 81),
				CleanContent:     true,
				IgnoreEcho:       true,
			},
		},
	}

	bridgeHealth := "ready"
	bridgeDryRun := false
	bridgeAcc := true
	if m.scenario == "degraded" {
		bridgeHealth = "not_ready"
		bridgeDryRun = true
		bridgeAcc = false
	}
	started := m.now.Add(-4 * time.Hour)
	prog := m.now.Add(-20 * time.Second)
	m.services = []ServiceView{
		{
			ID: "relay", Process: "supervisor", Health: "ready", HealthLatencyMS: 3, HealthObservedAt: m.now,
			Facts:     map[string]any{"status": "ok", "protocol_version": float64(1), "sse_clients": float64(1), "websocket_clients": float64(1)},
			Actions:   map[string]Availability{"start": {Supported: true, Enabled: false, Reason: "服务当前已在运行"}, "stop": {Supported: true, Enabled: true}, "restart": {Supported: true, Enabled: true}},
			StartedAt: &started, LastProgressAt: &prog,
		},
		{
			ID: "dispatcher", Process: "supervisor", Health: "ready", HealthLatencyMS: 4, HealthObservedAt: m.now,
			Facts:     map[string]any{"status": "ok", "protocol_version": float64(1), "relay_connected": true, "pending_messages": float64(2), "executing_groups": float64(1)},
			Actions:   map[string]Availability{"start": {Supported: true, Enabled: false, Reason: "服务当前已在运行"}, "stop": {Supported: true, Enabled: true}, "restart": {Supported: true, Enabled: true}},
			StartedAt: &started, LastProgressAt: &prog,
		},
		{
			ID: "decision", Process: "unknown", Health: "ready", HealthLatencyMS: 9, HealthObservedAt: m.now,
			Facts:     map[string]any{"status": "ready", "upstream_connected": true},
			Actions:   map[string]Availability{"start": {Supported: false, Reason: "外部依赖服务，不支持启停控制"}, "stop": {Supported: false, Reason: "外部依赖服务，不支持启停控制"}, "restart": {Supported: false, Reason: "外部依赖服务，不支持启停控制"}},
			StartedAt: &started, LastProgressAt: &prog,
		},
		{
			ID: "bridge", Process: "supervisor", Health: bridgeHealth, HealthLatencyMS: 5, HealthObservedAt: m.now,
			Facts:     map[string]any{"status": "ok", "protocol_version": float64(2), "group_routing": true, "serial_ui": true, "accessibility_trusted": bridgeAcc, "screen_capture_authorized": true, "dry_run": bridgeDryRun},
			Actions:   map[string]Availability{"start": {Supported: true, Enabled: false, Reason: "服务当前已在运行"}, "stop": {Supported: true, Enabled: true}, "restart": {Supported: true, Enabled: true}},
			StartedAt: &started, LastProgressAt: &prog,
		},
	}

	m.runtime = RuntimeServices{
		Revision: "svc-rev-10",
		Services: []RuntimeService{
			{ID: "relay", Label: "Relay", Registration: "managed", State: "running", PID: 41001, InstanceID: "inst-relay-1", Version: "1.4.0"},
			{ID: "dispatcher", Label: "Dispatcher", Registration: "managed", State: "running", PID: 41002, InstanceID: "inst-disp-1", Version: "1.4.0"},
			{ID: "decision", Label: "Decision Engine", Registration: "external", State: "running", Version: "1.0.0"},
			{ID: "bridge", Label: "Bridge", Registration: "managed", State: bridgeState(bridgeHealth), PID: 41003, InstanceID: "inst-bridge-1", Version: "2.0.0"},
		},
		Control: &RuntimeControl{Version: "ctl-v10", DependencyVersion: "dep-v10", Paused: false, UnknownDelivery: m.scenario == "unknown-delivery"},
	}

	m.controls = ControlSnapshot{
		Revision: "ctl-rev-10",
		Global:   ScopeControl{Paused: false, Revision: "ctl-rev-10", Boundary: 42, ChangedAt: m.now.Add(-15 * time.Minute)},
		Groups: map[string]ScopeControl{
			"100001@chatroom": {Paused: false, Revision: "ctl-rev-10", Boundary: 40, ChangedAt: m.now.Add(-15 * time.Minute)},
		},
	}
	if m.scenario == "degraded" {
		m.maint = &Maintenance{
			MaintenanceID: "maint-demo-001", Revision: "maint-rev-1",
			Scope: ControlScope{Kind: "global"}, Boundary: 42,
			StartedAt: m.now.Add(-1 * time.Minute), Deadline: m.now.Add(4 * time.Minute),
			Active: true, Stage: "draining",
			Counts:   ControlCounts{Pending: 1, ActiveWorkers: 1, UnknownDelivery: 1, RemoteUnsettled: 1, UnresolvedRuns: 2, Phases: map[string]int{"submitting": 1, "confirming": 1}},
			Blockers: []string{"仍有 1 个活跃工作协程和 1 条投递未知任务未收敛，请等待完成后继续"},
		}
		m.controls.Maintenance = m.maint
	}

	// Build runs & snapshots covering all target-page-spec edge cases.
	t1 := m.now.Add(-8 * time.Minute)
	t2 := m.now.Add(-4 * time.Minute)
	t3 := m.now.Add(-90 * time.Second)
	t4 := m.now.Add(-30 * time.Second)
	t5 := m.now.Add(-10 * time.Second)

	m.snapshots["cfg-snap-001"] = ConfigSnapshot{
		ID: "cfg-snap-001", Versions: v, CapturedAt: t3.Add(-5 * time.Second),
		Group:        GroupRef{ID: "100001@chatroom", Label: "示例客服群", Visibility: "visible"},
		Fields:       map[string]FieldValue{"scenario": makeField("support-qa", "support-qa", "historical_resolved", false, "历史当场生效值"), "dry_run": makeField(false, false, "historical_resolved", false, "")},
		GlobalFields: map[string]FieldValue{"laya_filter": makeField(true, true, "historical_resolved", false, "")},
		Permissions:  []any{},
		Source:       "dispatcher_trace_config_snapshot",
		Scope:        []string{"dispatcher.resolved_group_policy", "dispatcher.selected_global_policy"},
	}

	m.runs = []RunView{
		{
			ID: "run-demo-003", Revision: "run-rev-301", EntitySeq: 38,
			Source: GroupRef{ID: "100001@chatroom", Label: "示例客服群", Visibility: "visible"},
			Target: GroupRef{ID: "100001@chatroom", Label: "示例客服群", Visibility: "visible"},
			AcceptedAt: &t3, UpdatedAt: t4, LastProgressAt: &t4,
			Trigger: "mention", MessageCount: ptrInt(2), MessageIDs: []string{"msg-demo-301", "msg-demo-302"},
			Phase: "terminal", Outcome: "unknown", Delivery: "unknown",
			ReasonCode: ptrStr("unknown"), Summary: "模型已生成候选回复，但 Bridge 投递状态未知；重试前请先人工核对现场",
			Completeness: "complete", Versions: v,
			ConfigSnapshotID: ptrStr("cfg-snap-001"), PlannedBackendID: ptrStr("alma-demo"),
			Attempts: []AttemptView{{
				ID: "att-301", BackendID: "alma-demo", ModelLabel: ptrStr("快速响应模型"), Status: "succeeded", RemoteExecution: "not_applicable",
				StartedAt: ptrTime(t3.Add(1 * time.Second)), EndedAt: ptrTime(t3.Add(6 * time.Second)), ExternalID: ptrStr("ext-301"), Versions: v,
			}},
			SideEffects: []SideEffect{{Kind: "knowledge_lookup", Status: "confirmed", OccurredAt: ptrTime(t3.Add(3 * time.Second)), Summary: "检索知识库 example-kb"}},
			DeliveryEvidence: DeliveryEvidence{
				SubmissionStartedAt: ptrTime(t3.Add(7 * time.Second)),
				BridgeRequestID:     ptrStr("br-demo-301"),
				FailureEvidence:     ptrStr("收到提交回执前 Bridge 传输通道关闭"),
			},
			Inputs: []InboundReference{{
				EnvelopeID: "env-301", DedupKey: "dedup-301",
				Source: SourceRef{Platform: "wechat", AccountID: "wxid_example_bot_0001", ContainerID: "100001@chatroom", ContentID: "msg-demo-301"},
				ReceivedAt: &t3,
			}},
			Action:  &ActionRef{ActionID: "act-301", Kind: "reply_text", Platform: "wechat", IdempotencyKey: "idem-301", AdmissionID: "adm-301", AdmittedAt: ptrTime(t3.Add(7 * time.Second))},
			Actions: RunActions{BlockDelivery: Availability{Supported: true, Enabled: false, Reason: "消息已离开调度器进入发送环节"}, StopExecution: Availability{Supported: true, Enabled: false, Reason: "模型执行已结束"}},
		},
		{
			ID: "run-demo-001", Revision: "run-rev-101", EntitySeq: 35,
			Source: GroupRef{ID: "100001@chatroom", Label: "示例客服群", Visibility: "visible"},
			Target: GroupRef{ID: "100001@chatroom", Label: "示例客服群", Visibility: "visible"},
			AcceptedAt: &t1, UpdatedAt: ptrTimeVal(t1.Add(12 * time.Second)), LastProgressAt: ptrTime(t1.Add(12 * time.Second)),
			Trigger: "mention", MessageCount: ptrInt(1), MessageIDs: []string{"msg-demo-101"},
			Phase: "terminal", Outcome: "replied", Delivery: "confirmed",
			Summary: "已通过自身回声确认送达；Bridge 原始提交时间未记录",
			Completeness: "complete", Versions: v,
			ConfigSnapshotID: ptrStr("cfg-snap-001"), PlannedBackendID: ptrStr("alma-demo"),
			Attempts: []AttemptView{{
				ID: "att-101", BackendID: "alma-demo", ModelLabel: ptrStr("快速响应模型"), Status: "succeeded", RemoteExecution: "not_applicable",
				StartedAt: ptrTime(t1.Add(1 * time.Second)), EndedAt: ptrTime(t1.Add(5 * time.Second)), ExternalID: ptrStr("ext-101"), Versions: v,
			}},
			SideEffects: []SideEffect{},
			DeliveryEvidence: DeliveryEvidence{
				SubmissionStartedAt: ptrTime(t1.Add(6 * time.Second)),
				SubmittedAt:         nil, // MUST stay nil to test echo_inferred invariant!
				SubmittedAtSource:   ptrStr("echo_inferred"),
				ConfirmedAt:         ptrTime(t1.Add(12 * time.Second)),
				BridgeRequestID:     ptrStr("br-demo-101"),
				EchoMessageID:       ptrStr("echo-msg-101"),
			},
			Receipt: &ProcessingReceipt{
				Status: "confirmed",
				Receipt: &ActionReceipt{
					ActionID: "act-101", Status: "confirmed", Platform: "wechat", ExternalID: "br-demo-101",
					SubmittedAtSource: "echo_inferred", ConfirmedAt: ptrTime(t1.Add(12 * time.Second)),
					Admission:    ActionAdmission{Status: "accepted", AdmissionID: "adm-101", RecordedAt: ptrTime(t1.Add(6 * time.Second))},
					Confirmation: &ActionConfirmation{RequestID: "br-demo-101", EchoMessageID: "echo-msg-101"},
				},
			},
			Actions: RunActions{BlockDelivery: Availability{Supported: true, Enabled: false, Reason: "已完成送达确认"}, StopExecution: Availability{Supported: true, Enabled: false, Reason: "运行已结束"}},
		},
		{
			ID: "run-demo-002", Revision: "run-rev-201", EntitySeq: 39,
			Source: GroupRef{ID: "100002@chatroom", Label: "示例社区交流群", Visibility: "visible"},
			Target: GroupRef{ID: "100002@chatroom", Label: "示例社区交流群", Visibility: "visible"},
			AcceptedAt: &t4, UpdatedAt: t5, LastProgressAt: &t5,
			Trigger: "batch_idle", MessageCount: ptrInt(3), MessageIDs: []string{"msg-demo-201", "msg-demo-202", "msg-demo-203"},
			Phase: "submitting", Outcome: "pending", Delivery: "submitting",
			Summary: "模型生成已完成，正在等待 Bridge 提交发送",
			Completeness: "partial", Versions: v,
			ConfigSnapshotID: nil, MissingSnapshotReason: ptrStr("配置快照将在运行结束后保留"),
			PlannedBackendID: ptrStr("alma-demo"),
			Attempts: []AttemptView{{
				ID: "att-201", BackendID: "alma-demo", ModelLabel: ptrStr("快速响应模型"), Status: "succeeded", RemoteExecution: "not_applicable",
				StartedAt: ptrTime(t4.Add(1 * time.Second)), EndedAt: ptrTime(t4.Add(7 * time.Second)), ExternalID: ptrStr("ext-201"), Versions: v,
			}},
			DeliveryEvidence: DeliveryEvidence{SubmissionStartedAt: ptrTime(t4.Add(8 * time.Second)), BridgeRequestID: ptrStr("br-demo-201")},
			Actions:          RunActions{BlockDelivery: Availability{Supported: true, Enabled: true}, StopExecution: Availability{Supported: true, Enabled: false, Reason: "执行尝试已完成"}},
		},
		{
			ID: "run-demo-004", Revision: "run-rev-401", EntitySeq: 40,
			Source: GroupRef{ID: "bilibili:BV1example0001", Label: "示例 B站评论区", Visibility: "visible"},
			Target: GroupRef{ID: "bilibili:BV1example0001", Label: "示例 B站评论区", Visibility: "visible"},
			AcceptedAt: &t2, UpdatedAt: ptrTimeVal(t2.Add(9 * time.Second)), LastProgressAt: ptrTime(t2.Add(9 * time.Second)),
			Trigger: "comment_reply", MessageCount: ptrInt(1), MessageIDs: []string{"bili-cmt-401"},
			Phase: "terminal", Outcome: "canceled", Delivery: "blocked",
			ReasonCode: ptrStr("canceled"), Summary: "运维人员主动请求终止；远端外部执行器进程可能仍在运行",
			Completeness: "complete", Versions: v, PlannedBackendID: ptrStr("alma-demo"),
			Attempts: []AttemptView{{
				ID: "att-401", BackendID: "alma-demo", ModelLabel: ptrStr("深度推理模型"), Status: "canceled", RemoteExecution: "may_be_running",
				StartedAt: ptrTime(t2.Add(1 * time.Second)), EndedAt: ptrTime(t2.Add(9 * time.Second)),
				MissingExternalIDReason: ptrStr("在分配外部句柄前已取消"), Versions: v,
			}},
			DeliveryEvidence: DeliveryEvidence{NoSubmission: true},
			Action:           &ActionRef{ActionID: "act-401", Kind: "thread_reply", Platform: "bilibili"},
			Actions:          RunActions{BlockDelivery: Availability{Supported: true, Enabled: false, Reason: "已拦截发送"}, StopExecution: Availability{Supported: true, Enabled: false, Reason: "已取消运行"}},
		},
		{
			ID: "run-demo-005", Revision: "run-rev-501", EntitySeq: 41,
			Source: GroupRef{ID: "100002@chatroom", Label: "示例社区交流群", Visibility: "visible"},
			Target: GroupRef{ID: "100002@chatroom", Label: "示例社区交流群", Visibility: "visible"},
			AcceptedAt: &t5, UpdatedAt: m.now, LastProgressAt: &m.now,
			Trigger: "mention", MessageCount: ptrInt(1), MessageIDs: []string{"msg-demo-501"},
			Phase: "executing", Outcome: "pending", Delivery: "not_started",
			Summary: "Agent 正在生成候选回复内容", Completeness: "partial", Versions: v,
			PlannedBackendID: ptrStr("alma-demo"),
			Attempts: []AttemptView{{
				ID: "att-501", BackendID: "alma-demo", ModelLabel: ptrStr("快速响应模型"), Status: "running", RemoteExecution: "not_applicable",
				StartedAt: &t5, Versions: v,
			}},
			Actions: RunActions{BlockDelivery: Availability{Supported: true, Enabled: true}, StopExecution: Availability{Supported: true, Enabled: true}},
		},
	}

	if m.scenario == "unknown-delivery" {
		m.runs[2].Delivery = "unknown"
		m.runs[2].Outcome = "unknown"
		m.runs[2].Summary = "Bridge 传输超时导致投递状态未知，禁止盲目重发"
	}

	for _, run := range m.runs {
		m.timelines[run.ID] = []TimelineItem{
			{ID: run.ID + ":1", RunID: run.ID, OccurredAt: run.AcceptedAt, Source: "dispatcher_trace", Kind: "消息受理", Summary: "消息聚合后准入执行管线", Certainty: "observed"},
			{ID: run.ID + ":2", RunID: run.ID, OccurredAt: &run.UpdatedAt, Source: "dispatcher_trace", Kind: "状态流转 (" + run.Phase + ")", ReasonCode: run.ReasonCode, Summary: run.Summary, Certainty: "observed"},
		}
	}
	m.timelines["run-demo-004"] = append(m.timelines["run-demo-004"], TimelineItem{
		ID: "run-demo-004:3", RunID: "run-demo-004", Source: "action_adapter", Kind: "动作投递",
		Summary: "因执行已取消，未进入平台外发投递环节", Certainty: "not_entered",
	})

	snapBefore := ConfigurationSnapshot{
		CapturedAt: m.now.Add(-20 * time.Minute), Versions: v, Source: "console_journal",
		Scope: []string{"dispatcher", "relay"},
		Dispatcher: map[string]any{"group_rules": map[string]any{"100001@chatroom": map[string]any{"max_reply_runes": 400}}},
		Relay:      &m.relayRules.Rules,
	}
	snapTarget := snapBefore
	snapTarget.CapturedAt = m.now.Add(-18 * time.Minute)
	m.changes = []ChangeRecord{
		{
			ID: "chg-demo-002", CreatedAt: m.now.Add(-10 * time.Minute), Origin: "console", Base: v, Readback: &v, Target: &v,
			BeforeSnapshot: &snapBefore, TargetSnapshot: &snapTarget, ReadbackSnapshot: nil,
			Changes: []FieldChange{{Component: "relay", Path: "filters.clean_content", Before: false, After: true, EffectiveBefore: false, EffectiveAfter: true}},
			Operation: Operation{
				ID: "op-demo-002", RequestID: "req-demo-002", Revision: "op-rev-2", ChangeID: "chg-demo-002",
				Status: "partial_apply", Summary: "Dispatcher 已应用，但 Relay 回读未能原子确认（不做静默自动补偿）",
				Components: []ComponentResult{
					{ID: "dispatcher", State: "succeeded", ReadbackVersion: ptrStr(v.Dispatcher), PersistenceVerified: ptrBool(true)},
					{ID: "relay", State: "unknown", PersistenceVerified: ptrBool(false)},
				},
			},
		},
		{
			ID: "chg-demo-001", CreatedAt: m.now.Add(-30 * time.Minute), Origin: "console", Base: v, Readback: &v, Target: &v,
			BeforeSnapshot: &snapBefore, TargetSnapshot: &snapTarget, ReadbackSnapshot: &snapTarget,
			Changes: []FieldChange{{Component: "dispatcher", Path: "group_rules.100001@chatroom.max_reply_runes", Before: 400, After: 500, EffectiveBefore: 400, EffectiveAfter: 500}},
			Operation: Operation{
				ID: "op-demo-001", RequestID: "req-demo-001", Revision: "op-rev-1", ChangeID: "chg-demo-001",
				Status: "succeeded", Summary: "已更新 max_reply_runes 并完成配置回读验证",
				ResultRevision: ptrStr(v.Fingerprint),
				Components: []ComponentResult{
					{ID: "dispatcher", State: "succeeded", ReadbackVersion: ptrStr(v.Dispatcher), PersistenceVerified: ptrBool(true)},
				},
			},
		},
	}

	m.attention = []AttentionItem{
		{
			ID: "run:run-demo-003:delivery_unknown", Revision: "run-rev-301", Code: "delivery_unknown",
			Summary: "已结束任务出现投递状态未知，需人工核对后方可决定后续操作",
			OccurredAt: t4, Acknowledged: false, Target: &AttentionTarget{Kind: "run", ID: "run-demo-003"},
		},
		{
			ID: "operation:op-demo-002", Revision: "op-rev-2", Code: "publication_partial_apply",
			Summary: "变更 chg-demo-002 结果为部分生效，重试或回滚前请核查各组件状态",
			OccurredAt: m.now.Add(-10 * time.Minute), Acknowledged: false, Target: &AttentionTarget{Kind: "operation", ID: "op-demo-002"},
		},
	}

	driftFlag := false
	memVer := v.Dispatcher
	diskVer := v.Dispatcher
	var memObj any = map[string]any{"max_reply_runes": 500, "scenario": "support-qa"}
	var diskObj any = map[string]any{"max_reply_runes": 500, "scenario": "support-qa"}
	reason := "可见内存配置与磁盘持久化快照一致"
	if m.scenario == "degraded" {
		driftFlag = true
		diskVer = "disp-disk-older"
		diskObj = map[string]any{"max_reply_runes": 400, "scenario": "support-qa"}
		reason = "Dispatcher 内存规则与磁盘配置存在差异（私有本地策略字段不在可见对比范围内）"
	}
	m.drift["dispatcher"] = ConfigurationDrift{
		Component: "dispatcher", Available: true, Reason: reason,
		Drift: &driftFlag, MemoryVersion: &memVer, DiskVersion: &diskVer, Memory: memObj, Disk: diskObj,
	}
	relayDrift := false
	rMem := v.Relay
	m.drift["relay"] = ConfigurationDrift{
		Component: "relay", Available: true, Reason: "可见 Relay 过滤配置与持久化快照一致",
		Drift: &relayDrift, MemoryVersion: &rMem, DiskVersion: &rMem, Memory: m.relayRules.Rules, Disk: m.relayRules.Rules,
	}
}

func ptrTimeVal(t time.Time) time.Time { return t }

func bridgeState(health string) string {
	if health == "ready" {
		return "running"
	}
	return "degraded"
}

func (m *MockClient) env(revision, source string) Envelope[any] {
	return Envelope[any]{
		ObservedAt:   m.now,
		Source:       source,
		Freshness:    "fresh",
		Completeness: "partial",
		Revision:     revision,
	}
}

func (m *MockClient) architectureLocked() ArchitectureSnapshot {
	bili := m.connectors["bilibili"]
	web := m.connectors["web"]
	relayHealth := m.services[0].Health
	bridgeHealth := m.services[3].Health

	biliActionState := bili.State
	biliSendReason := ""
	if bili.DryRun != nil && *bili.DryRun && bili.State == "ready" {
		biliActionState = "not_ready"
		biliSendReason = "unavailable"
	}
	biliActions := map[string]bool{"reply_text": true, "thread_reply": true}

	sources := []SourceView{
		{
			Platform: "wechat", State: relayHealth,
			Availability: Availability{Supported: true, Enabled: relayHealth == "ready"},
			Capabilities: []ArchitectureCapability{
				{ID: "ingest", Supported: true, Enforcement: "host", Source: "relay_health"},
				{ID: "adapter_contract", Supported: false, Enforcement: "unverified", Source: "console", Reason: "unsupported"},
			},
		},
		{
			Platform: "bilibili", State: bili.State, Availability: bili.Availability,
			AccountID: bili.AccountID, ContainerIDs: []string{"bilibili:BV1example0001"},
			Capabilities: []ArchitectureCapability{
				{ID: "ingest", Supported: true, Enforcement: "host", Source: "source_adapter"},
				{ID: "adapter_contract", Supported: true, Enforcement: "host", Source: "source_adapter"},
			},
		},
		{
			Platform: "web", State: web.State, Availability: web.Availability,
			Capabilities: []ArchitectureCapability{
				{ID: "ingest", Supported: web.Configured, Enforcement: "host", Source: "source_adapter", Reason: stringVal(web.Reason, "unsupported")},
				{ID: "adapter_contract", Supported: web.Configured && web.State != "unreachable", Enforcement: "unverified", Source: "source_adapter", Reason: stringVal(web.Reason, "unsupported")},
			},
		},
	}

	var actions []ActionView
	for _, platform := range []string{"wechat", "bilibili", "web"} {
		for _, kind := range []string{"reply_text", "edit_text", "delete_content", "thread_reply"} {
			av := ActionView{
				Platform: platform, Kind: kind, State: "unsupported",
				Availability: Availability{Reason: "unsupported"},
				Capabilities: []ArchitectureCapability{
					{ID: "send", Supported: false, Enforcement: "unverified", Source: "console", Reason: "unsupported"},
					{ID: "durable_admission", Supported: false, Enforcement: "unverified", Source: "console", Reason: "unsupported"},
					{ID: "confirmation", Supported: false, Enforcement: "unverified", Source: "console", Reason: "unsupported"},
					{ID: "adapter_contract", Supported: false, Enforcement: "unverified", Source: "console", Reason: "unsupported"},
				},
			}
			if platform == "wechat" && kind == "reply_text" {
				av.State = bridgeHealth
				av.Availability = Availability{Supported: true, Enabled: bridgeHealth == "ready"}
				av.Capabilities[0] = ArchitectureCapability{ID: "send", Supported: true, Enforcement: "host", Source: "bridge_health"}
			} else if platform == "bilibili" && biliActions[kind] {
				av.State = biliActionState
				av.Availability = Availability{Supported: true, Enabled: biliActionState == "ready", Reason: biliSendReason}
				av.Capabilities[0] = ArchitectureCapability{ID: "send", Supported: true, Enforcement: "host", Source: "action_adapter", Reason: biliSendReason}
				av.Capabilities[1] = ArchitectureCapability{ID: "durable_admission", Supported: true, Enforcement: "host", Source: "action_adapter", Reason: biliSendReason}
				av.Capabilities[2] = ArchitectureCapability{ID: "confirmation", Supported: false, Enforcement: "unverified", Source: "action_adapter", Reason: "unverified"}
				av.Capabilities[3] = ArchitectureCapability{ID: "adapter_contract", Supported: true, Enforcement: "host", Source: "action_adapter"}
			} else if platform == "web" && web.Configured && kind == "reply_text" {
				av.State = "unreachable"
				av.Availability = Availability{Supported: true, Enabled: false, Reason: "unavailable"}
				av.Capabilities[0] = ArchitectureCapability{ID: "send", Supported: true, Enforcement: "host", Source: "action_adapter", Reason: "unavailable"}
			}
			actions = append(actions, av)
		}
	}

	cur := fmt.Sprintf("mock-cur-%d", m.seq)
	cov := m.now.Add(-1 * time.Hour)
	return ArchitectureSnapshot{
		Revision:   fmt.Sprintf("arch-rev-%d", m.revNum),
		ObservedAt: m.now,
		Layers:     []string{"source", "processing", "action"},
		Sources:    sources,
		Processing: ProcessingView{
			Agents: []ProcessingAgent{{ID: "alma-demo"}},
			Backends: []ProcessingBackend{{
				ID: "alma-demo", Kind: "alma_personal", Available: true, ModelIDs: []string{"demo-fast", "demo-reasoning"}, AgentBinding: "branch_only",
				Capabilities: []ArchitectureCapability{{ID: "text_generation", Supported: true, Enforcement: "hard", Source: "execution_catalog"}},
			}},
			Capabilities: []ArchitectureCapability{{ID: "dispatch", Supported: true, Enforcement: "host", Source: "dispatcher_trace"}},
		},
		Actions: actions,
		EventStreams: []ArchitectureStream{
			{Domain: "source", ArchitectureCapability: ArchitectureCapability{ID: "source_events", Supported: false, Enforcement: "unverified", Source: "console", Reason: "unsupported"}},
			{Domain: "processing", ArchitectureCapability: ArchitectureCapability{ID: "processing_events", Supported: true, Enforcement: "host", Source: "dispatcher_trace"}, StreamID: ptrStr("mock-stream-1"), Cursor: &cur, LastSeq: ptrUint64(m.seq), CoverageSince: &cov},
			{Domain: "action", ArchitectureCapability: ArchitectureCapability{ID: "action_events", Supported: false, Enforcement: "unverified", Source: "console", Reason: "unsupported"}},
		},
	}
}

func stringVal(p *string, fallback string) string {
	if p == nil || *p == "" {
		return fallback
	}
	return *p
}

func (m *MockClient) overviewLocked() Envelope[Overview] {
	cur := fmt.Sprintf("mock-cur-%d", m.seq)
	cov := m.now.Add(-1 * time.Hour)
	unknownCount := 1
	if m.scenario == "unknown-delivery" {
		unknownCount = 2
	}
	recent := make([]RunView, len(m.runs))
	copy(recent, m.runs)
	svcs := make([]ServiceView, len(m.services))
	copy(svcs, m.services)
	att := make([]AttentionItem, len(m.attention))
	copy(att, m.attention)
	ov := Overview{
		Stats: OverviewStats{
			TotalReceived:   ptrFloat(184),
			TotalTriggered:  ptrFloat(47),
			TotalSubmitted:  ptrFloat(39),
			TotalReplied:    ptrFloat(36),
			TotalDryRun:     ptrFloat(5),
			TotalSendFailed: ptrFloat(1),
			TotalUnknown:    ptrFloat(float64(unknownCount)),
			ReplayGaps:      ptrFloat(0),
			PendingMessages: ptrFloat(2),
			RelayConnected:  ptrBool(true),
		},
		StatsSource:    "persistent_cumulative",
		StatsAvailable: true,
		PendingMeaning: "pending_and_in_flight_messages",
		Services:       svcs,
		Controls:       Availability{Supported: true, Enabled: true},
		RecentRuns:     recent,
		Trace: TraceState{
			Availability: Availability{Supported: true, Enabled: true}, Domain: "run",
			StreamID: ptrStr("mock-stream-1"), Cursor: &cur, LastSeq: ptrUint64(m.seq), CoverageSince: &cov,
		},
		CurrentWork: StageCounts{
			Aggregating: ptrInt(0), Queued: ptrInt(0), Executing: ptrInt(1),
			Submitting: ptrInt(1), Confirming: ptrInt(0),
			UnknownDelivery: ptrInt(unknownCount), RemoteUnsettled: ptrInt(1),
		},
		WorkSource:     "retained_trace_snapshot",
		RelayConnected: ptrBool(true),
		Attention:      att,
	}
	fresh := "fresh"
	if m.scenario == "degraded" {
		fresh = "stale"
	}
	return Envelope[Overview]{
		Data: ov, ObservedAt: m.now, Source: "dispatcher_stats_health_and_trace",
		Freshness: fresh, Completeness: "partial", Revision: cur,
	}
}

func (m *MockClient) Bootstrap(ctx context.Context) (Bootstrap, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := fmt.Sprintf("mock-cur-%d", m.seq)
	ov := m.overviewLocked()
	caps := map[string]Availability{
		"health":                {Supported: true, Enabled: true},
		"catalog":               {Supported: true, Enabled: true},
		"connectors":            {Supported: true, Enabled: true},
		"rules.read":            {Supported: true, Enabled: true},
		"rules.plan":            {Supported: true, Enabled: true},
		"rules.publish":         {Supported: true, Enabled: true},
		"relay.rules":           {Supported: true, Enabled: true},
		"changes.read":          {Supported: true, Enabled: true},
		"rollback":              {Supported: true, Enabled: true},
		"attention.acknowledge": {Supported: true, Enabled: true},
		"runs":                  {Supported: true, Enabled: true},
		"traceRuns":             {Supported: true, Enabled: true},
		"events.resume":         {Supported: true, Enabled: true},
		"resumableRunEvents":    {Supported: true, Enabled: true},
		"configSnapshots":       {Supported: true, Enabled: true},
		"controls.read":         {Supported: true, Enabled: true},
		"pause":                 {Supported: true, Enabled: true},
		"resume":                {Supported: true, Enabled: true},
		"maintenance":           {Supported: true, Enabled: true},
		"cancel":                {Supported: true, Enabled: true},
		"backends.read":         {Supported: true, Enabled: true},
		"routing.read":          {Supported: true, Enabled: true},
		"permissions.read":      {Supported: true, Enabled: true},
		"services.runtime":      {Supported: true, Enabled: true},
		"service.actions":       {Supported: true, Enabled: true},
		"service.start":         {Supported: true, Enabled: true},
		"service.stop":          {Supported: true, Enabled: true},
		"service.restart":       {Supported: true, Enabled: true},
		"routing.edit":          {Supported: false, Reason: "请使用显式执行计划接口修改路由策略"},
		"permissions.edit":      {Supported: false, Reason: "权限边界含软约束与未验证宿主条件，仅支持只读审查"},
	}
	return Bootstrap{
		StreamID: ptrStr("mock-stream-1"), Cursor: &cur, LastSeq: ptrUint64(m.seq),
		Overview: ov, ActiveOperations: []Operation{}, Capabilities: caps,
		Schemas:     FormSchemas{SchemaVersion: SchemaVersion, Group: map[string]SchemaEntry{}, Global: map[string]SchemaEntry{}},
		EventStream: ov.Data.Trace,
	}, nil
}

func (m *MockClient) GetOverview(ctx context.Context) (Envelope[Overview], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.overviewLocked(), nil
}

func (m *MockClient) GetArchitecture(ctx context.Context) (ArchitectureSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.architectureLocked(), nil
}

func (m *MockClient) ListConnectors(ctx context.Context) (Envelope[[]ConnectorView], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []ConnectorView{m.connectors["bilibili"], m.connectors["web"]}
	fresh := "fresh"
	if out[1].State == "unreachable" {
		fresh = "unknown"
	}
	return Envelope[[]ConnectorView]{
		Data: out, ObservedAt: m.now, Source: "connector_health_probes",
		Freshness: fresh, Completeness: "partial", Revision: fmt.Sprintf("conn-rev-%d", m.revNum),
	}, nil
}

func (m *MockClient) PlanConnectorDryRun(ctx context.Context, platform string, input ConnectorDryRunPlanInput) (ConnectorDryRunPlan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	conn, ok := m.connectors[platform]
	if !ok {
		return ConnectorDryRunPlan{}, &APIError{Status: 400, Problem: &Problem{Code: "validation", Message: "Unknown connector platform"}}
	}
	if !conn.Configured || conn.State == "unsupported" || conn.State == "unreachable" || conn.DryRun == nil {
		return ConnectorDryRunPlan{}, &APIError{Status: 503, Problem: &Problem{Code: "unavailable", Message: "Connector is not ready for dry_run planning", Retry: "read"}}
	}
	if !slices.Contains(conn.Controls, "dry_run") {
		return ConnectorDryRunPlan{}, &APIError{Status: 501, Problem: &Problem{Code: "unsupported", Message: "Connector does not advertise dry_run control"}}
	}
	if *conn.DryRun != input.ExpectedDryRun {
		return ConnectorDryRunPlan{}, &APIError{Status: 409, Problem: &Problem{Code: "conflict", Message: "Connector dry_run changed since last read", Retry: "replan"}}
	}
	m.planSeq++
	id := fmt.Sprintf("conn-plan-%03d", m.planSeq)
	plan := ConnectorDryRunPlan{
		ID: id, Token: "tok-" + id, Platform: platform,
		CurrentDryRun: *conn.DryRun, TargetDryRun: input.TargetDryRun,
		Persisted: false, ExpiresAt: m.now.Add(5 * time.Minute),
		Warnings: []Warning{{Code: "ephemeral_dry_run", Message: "该运行时开关仅当前连接器进程内生效（persisted: false），重启后将恢复默认配置。"}},
	}
	m.connPlans[id] = plan
	return plan, nil
}

func (m *MockClient) ApplyConnectorDryRun(ctx context.Context, platform string, input ConnectorDryRunApplyInput) (ConnectorDryRunApplyResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !input.Confirm {
		return ConnectorDryRunApplyResult{}, &APIError{Status: 400, Problem: &Problem{Code: "validation", Message: "Explicit confirm=true is required to mutate connector dry_run"}}
	}
	plan, ok := m.connPlans[input.PlanID]
	if !ok || plan.Platform != platform || plan.Token != input.Token {
		return ConnectorDryRunApplyResult{}, &APIError{Status: 409, Problem: &Problem{Code: "conflict", Message: "Invalid or expired connector dry_run plan", Retry: "replan"}}
	}
	conn := m.connectors[platform]
	if conn.DryRun == nil || *conn.DryRun != plan.CurrentDryRun {
		return ConnectorDryRunApplyResult{}, &APIError{Status: 409, Problem: &Problem{Code: "conflict", Message: "Connector dry_run changed before apply", Retry: "replan"}}
	}
	prev := *conn.DryRun
	target := plan.TargetDryRun
	conn.DryRun = &target
	conn.ObservedAt = m.now
	m.connectors[platform] = conn
	delete(m.connPlans, input.PlanID)
	m.revNum++
	return ConnectorDryRunApplyResult{
		OK: true, Platform: platform, PreviousDryRun: prev, DryRun: target,
		Persisted: false, ReadbackVerified: true, Status: "succeeded",
		RequestID: input.RequestID, ObservedAt: m.now,
	}, nil
}

func (m *MockClient) GetCatalog(ctx context.Context) (Envelope[CatalogView], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Envelope[CatalogView]{
		Data: m.catalog, ObservedAt: m.now, Source: "dispatcher_catalog",
		Freshness: "fresh", Completeness: "partial", Revision: fmt.Sprintf("cat-rev-%d", m.revNum),
	}, nil
}

func (m *MockClient) ListGroups(ctx context.Context) (Envelope[GroupPage], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]GroupView, 0, len(m.groupOrder))
	for _, id := range m.groupOrder {
		items = append(items, m.groups[id])
	}
	return Envelope[GroupPage]{
		Data: GroupPage{Items: items}, ObservedAt: m.now, Source: "dispatcher_rules",
		Freshness: "fresh", Completeness: "partial", Revision: m.versions().Fingerprint,
	}, nil
}

func (m *MockClient) GetGroup(ctx context.Context, id string) (Envelope[GroupView], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[id]
	if !ok {
		return Envelope[GroupView]{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Group not found"}}
	}
	return Envelope[GroupView]{
		Data: g, ObservedAt: m.now, Source: "dispatcher_rules",
		Freshness: "fresh", Completeness: "partial", Revision: g.Revision,
	}, nil
}

func (m *MockClient) GetGlobalRules(ctx context.Context) (Envelope[GlobalRulesView], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Envelope[GlobalRulesView]{
		Data: m.globalRules, ObservedAt: m.now, Source: "dispatcher_rules",
		Freshness: "fresh", Completeness: "partial", Revision: m.globalRules.Versions.Fingerprint,
	}, nil
}

func (m *MockClient) GetRelayRules(ctx context.Context) (Envelope[RelayRulesView], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return Envelope[RelayRulesView]{
		Data: m.relayRules, ObservedAt: m.now, Source: "relay_rules",
		Freshness: "fresh", Completeness: "partial", Revision: m.relayRules.Versions.Relay,
	}, nil
}

func (m *MockClient) PlanChange(ctx context.Context, input PlanInput) (ChangePlan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur := m.versions()
	if input.Base.Fingerprint != "" && input.Base.Fingerprint != cur.Fingerprint {
		return ChangePlan{}, &APIError{Status: 409, Problem: &Problem{Code: "conflict", Message: "Configuration changed since last read", Retry: "replan"}}
	}
	m.planSeq++
	id := fmt.Sprintf("plan-demo-%03d", m.planSeq)
	changes := make([]FieldChange, 0, len(input.Edits))
	affected := map[string]bool{}
	warnings := []Warning{}
	for _, ed := range input.Edits {
		var val any
		_ = json.Unmarshal(ed.Value, &val)
		path := ed.Field
		if ed.Scope.Kind == "group" {
			path = "group_rules." + ed.Scope.GroupID + "." + ed.Field
			affected[ed.Scope.GroupID] = true
		} else if ed.Kind == "group_admission" {
			path = "group_admission." + ed.GroupID
			affected[ed.GroupID] = true
			val = ed.Enabled
		}
		changes = append(changes, FieldChange{Component: "dispatcher", Path: path, After: val, EffectiveAfter: val})
		if ed.Field == "skills" || ed.Field == "tools" || ed.Field == "knowledge_bases" || ed.Field == "promo_keywords" {
			if arr, ok := val.([]any); ok && len(arr) == 0 {
				warnings = append(warnings, Warning{Code: "empty_mount_list_normalized", Message: fmt.Sprintf("%s: 提交的 [] 将在 Dispatcher 回读时规范化为 null", ed.Field)})
			}
		}
	}
	affList := make([]string, 0, len(affected))
	for k := range affected {
		affList = append(affList, k)
	}
	slices.Sort(affList)
	plan := ChangePlan{
		ID: id, Token: "tok-" + id, ExpiresAt: m.now.Add(5 * time.Minute), Base: cur,
		Changes: changes, AffectedGroups: affList,
		Effect:         []string{fmt.Sprintf("通过 Dispatcher CAS 原子提交 %d 项规则变更", len(changes))},
		Warnings:       warnings,
		ComponentOrder: []string{"dispatcher"},
	}
	m.plans[id] = plan
	m.planBaseRev[id] = cur
	if m.scenario == "conflict" {
		// Simulate concurrent edit arriving between Plan and Publish.
		m.revNum++
	}
	return plan, nil
}

func (m *MockClient) PlanRelayRules(ctx context.Context, input PlanInput) (ChangePlan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.planSeq++
	id := fmt.Sprintf("plan-relay-%03d", m.planSeq)
	changes := make([]FieldChange, 0, len(input.Edits))
	for _, ed := range input.Edits {
		var val any
		_ = json.Unmarshal(ed.Value, &val)
		changes = append(changes, FieldChange{Component: "relay", Path: ed.Scope.Kind + "." + ed.Field, After: val, EffectiveAfter: val})
	}
	plan := ChangePlan{
		ID: id, Token: "tok-" + id, ExpiresAt: m.now.Add(5 * time.Minute), Base: m.versions(),
		Changes: changes, Effect: []string{"应用 Relay 中继配置变更"},
		ComponentOrder: []string{"relay"},
	}
	m.plans[id] = plan
	return plan, nil
}

func (m *MockClient) PublishChange(ctx context.Context, input PublishInput) (Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	plan, ok := m.plans[input.PlanID]
	if !ok || plan.Token != input.Token {
		return Operation{}, &APIError{Status: 409, Problem: &Problem{Code: "conflict", Message: "Plan token mismatch or expired", Retry: "replan"}}
	}
	cur := m.versions()
	if plan.Base.Fingerprint != "" && plan.Base.Fingerprint != cur.Fingerprint {
		return Operation{}, &APIError{Status: 409, Problem: &Problem{Code: "conflict", Message: "Version fingerprint drifted since plan creation; reload & re-plan required", Retry: "replan"}}
	}
	// Apply changes to mock state.
	for _, ch := range plan.Changes {
		if strings.HasPrefix(ch.Path, "group_rules.") {
			rest := strings.TrimPrefix(ch.Path, "group_rules.")
			idx := strings.LastIndex(rest, ".")
			if idx > 0 {
				gid, field := rest[:idx], rest[idx+1:]
				if g, exists := m.groups[gid]; exists {
					after := ch.After
					if field == "skills" || field == "tools" || field == "knowledge_bases" || field == "promo_keywords" {
						if arr, ok := after.([]any); ok && len(arr) == 0 {
							after = nil
						}
					}
					fv := g.Fields[field]
					fv.Configured = after
					fv.Effective = after
					fv.Origin = "group"
					g.Fields[field] = fv
					switch field {
					case "enabled":
						if b, ok := after.(bool); ok {
							g.Enabled = b
						}
					case "dry_run":
						if b, ok := after.(bool); ok {
							g.EffectiveDryRun = b
						}
					}
					m.groups[gid] = g
				}
			}
				} else if ch.Component == "dispatcher" {
			if fv, ok := m.globalRules.Fields[ch.Path]; ok {
				fv.Configured = ch.After
				fv.Effective = ch.After
				fv.Origin = "global"
				m.globalRules.Fields[ch.Path] = fv
			}
		} else if ch.Component == "relay" {
			switch ch.Path {
			case "relay_filters.clean_content":
				if b, ok := ch.After.(bool); ok {
					m.relayRules.Rules.Filters.CleanContent = b
				}
			case "relay_filters.ignore_echo":
				if b, ok := ch.After.(bool); ok {
					m.relayRules.Rules.Filters.IgnoreEcho = b
				}
			case "relay_group_control.monitor_all":
				if b, ok := ch.After.(bool); ok {
					m.relayRules.Rules.GroupControl.MonitorAll = b
				}
			}
		}
	}
	m.revNum++
	nv := m.versions()
	for id, g := range m.groups {
		g.Revision = nv.Fingerprint
		g.Versions = nv
		m.groups[id] = g
	}
	m.globalRules.Versions = nv
	m.relayRules.Versions = nv
	chgID := fmt.Sprintf("chg-demo-%03d", len(m.changes)+1)
	op := Operation{
		ID: fmt.Sprintf("op-demo-%03d", len(m.changes)+1), RequestID: input.RequestID,
		Revision: nv.Fingerprint, ChangeID: chgID, Status: "succeeded",
		Summary: "已发布配置变更并通过回读确认", ResultRevision: &nv.Fingerprint,
		Components: []ComponentResult{{ID: "dispatcher", State: "succeeded", ReadbackVersion: &nv.Dispatcher, PersistenceVerified: ptrBool(true)}},
	}
	snap := ConfigurationSnapshot{CapturedAt: m.now, Versions: nv, Source: "console_journal", Scope: []string{"dispatcher"}, Dispatcher: map[string]any{}}
	rec := ChangeRecord{
		ID: chgID, CreatedAt: m.now, Origin: "console", Base: plan.Base, Readback: &nv, Target: &nv,
		BeforeSnapshot: &snap, TargetSnapshot: &snap, ReadbackSnapshot: &snap,
		Changes: plan.Changes, Operation: op,
	}
	m.changes = append([]ChangeRecord{rec}, m.changes...)
	delete(m.plans, input.PlanID)
	return op, nil
}

func (m *MockClient) GetOperation(ctx context.Context, id string) (Operation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, chg := range m.changes {
		if chg.Operation.ID == id {
			return chg.Operation, nil
		}
	}
	return Operation{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Operation not found"}}
}

func (m *MockClient) ListBackends(ctx context.Context) ([]Backend, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Backend, len(m.backends))
	copy(out, m.backends)
	return out, nil
}

func (m *MockClient) GetGroupRouting(ctx context.Context, groupID string) (GroupRouting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.routing[groupID]
	if !ok {
		return GroupRouting{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Routing not found"}}
	}
	return r, nil
}

func (m *MockClient) GetGroupPermissions(ctx context.Context, groupID string) (GroupPermissions, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.perms[groupID]
	if !ok {
		return GroupPermissions{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Permissions not found"}}
	}
	return p, nil
}

func (m *MockClient) ListRuns(ctx context.Context, filter RunFilter) (Envelope[RunPage], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	filtered := make([]RunView, 0, len(m.runs))
	q := strings.ToLower(strings.TrimSpace(filter.Query))
	for _, r := range m.runs {
		if filter.GroupID != "" && r.Source.ID != filter.GroupID && r.Target.ID != filter.GroupID {
			continue
		}
		if len(filter.Phases) > 0 && !slices.Contains(filter.Phases, r.Phase) {
			continue
		}
		if len(filter.Outcomes) > 0 && !slices.Contains(filter.Outcomes, r.Outcome) {
			continue
		}
		if len(filter.Deliveries) > 0 && !slices.Contains(filter.Deliveries, r.Delivery) {
			continue
		}
		if q != "" && !strings.Contains(strings.ToLower(r.ID), q) && !strings.Contains(strings.ToLower(r.Source.ID), q) {
			continue
		}
		filtered = append(filtered, r)
	}
	cur := fmt.Sprintf("mock-cur-%d", m.seq)
	cov := m.now.Add(-1 * time.Hour)
	return Envelope[RunPage]{
		Data: RunPage{Items: filtered, NextCursor: nil, CoverageSince: cov, Watermark: cur},
		ObservedAt: m.now, Source: "dispatcher_trace", Freshness: "fresh", Completeness: "partial", Revision: cur,
	}, nil
}

func (m *MockClient) GetRun(ctx context.Context, id string) (Envelope[RunDetail], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.runs {
		if r.ID == id {
			return Envelope[RunDetail]{
				Data: RunDetail{Run: r, Timeline: m.timelines[id]},
				ObservedAt: m.now, Source: "dispatcher_trace", Freshness: "fresh", Completeness: "partial", Revision: r.Revision,
			}, nil
		}
	}
	return Envelope[RunDetail]{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Run not found"}}
}

func (m *MockClient) GetConfigSnapshot(ctx context.Context, id string) (Envelope[*ConfigSnapshot], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap, ok := m.snapshots[id]
	if !ok {
		return Envelope[*ConfigSnapshot]{Data: nil, ObservedAt: m.now, Source: "dispatcher_trace_snapshot_missing", Freshness: "fresh", Completeness: "partial", Revision: id, MissingReason: ptrStr("Snapshot not retained")}, nil
	}
	cp := snap
	return Envelope[*ConfigSnapshot]{Data: &cp, ObservedAt: m.now, Source: "dispatcher_trace_config_snapshot", Freshness: "fresh", Completeness: "partial", Revision: id}, nil
}

func (m *MockClient) AcknowledgeAttention(ctx context.Context, input AcknowledgeInput) (Envelope[AttentionItem], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.attention {
		if m.attention[i].ID == input.AttentionID {
			if m.attention[i].Revision != input.ExpectedRevision {
				return Envelope[AttentionItem]{}, &APIError{Status: 409, Problem: &Problem{Code: "conflict", Message: "Attention item changed", Retry: "replan"}}
			}
			m.attention[i].Acknowledged = true
			return Envelope[AttentionItem]{
				Data: m.attention[i], ObservedAt: m.now, Source: "console_attention_journal",
				Freshness: "fresh", Completeness: "partial", Revision: m.attention[i].Revision,
			}, nil
		}
	}
	return Envelope[AttentionItem]{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Attention item not found"}}
}

func (m *MockClient) GetControls(ctx context.Context) (ControlSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.controls, nil
}

func (m *MockClient) RunControlAction(ctx context.Context, input ControlRequest) (ControlOperation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch input.Action.Kind {
	case "pause", "resume":
		paused := input.Action.Kind == "pause"
		m.controls.Revision = fmt.Sprintf("ctl-rev-%d", m.revNum+1)
		m.revNum++
		if input.Action.Scope.Kind == "global" {
			m.controls.Global.Paused = paused
			m.controls.Global.Revision = m.controls.Revision
			m.controls.Global.ChangedAt = m.now
			if m.runtime.Control != nil {
				m.runtime.Control.Paused = paused
			}
		}
		return ControlOperation{
			OperationID: "ctl-op-1", RequestID: input.RequestID, Revision: m.controls.Revision,
			Status: "succeeded", Control: &m.controls.Global, Action: input.Action,
		}, nil
	case "cancel_run":
		for i := range m.runs {
			if m.runs[i].ID == input.Action.RunID {
				m.runs[i].Outcome = "canceled"
				m.runs[i].Phase = "terminal"
				if input.Action.CancelMode == "block_delivery" {
					m.runs[i].Delivery = "blocked"
					m.runs[i].Actions.BlockDelivery = Availability{Supported: true, Enabled: false, Reason: "Already blocked"}
				} else {
					m.runs[i].Actions.StopExecution = Availability{Supported: true, Enabled: false, Reason: "Already stopped"}
				}
				return ControlOperation{
					OperationID: "ctl-op-run", RequestID: input.RequestID, Revision: m.controls.Revision,
					Status: "succeeded", RunID: input.Action.RunID, Action: input.Action,
				}, nil
			}
		}
		return ControlOperation{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Run not found"}}
	default:
		return ControlOperation{}, &APIError{Status: 400, Problem: &Problem{Code: "validation", Message: "Unsupported control action"}}
	}
}

func (m *MockClient) ListServices(ctx context.Context) (Envelope[[]ServiceView], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ServiceView, len(m.services))
	copy(out, m.services)
	return Envelope[[]ServiceView]{
		Data: out, ObservedAt: m.now, Source: "read_only_health_probes",
		Freshness: "fresh", Completeness: "partial", Revision: m.runtime.Revision,
	}, nil
}

func (m *MockClient) GetRuntimeServices(ctx context.Context) (RuntimeServices, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runtime, nil
}

func (m *MockClient) PlanServiceAction(ctx context.Context, input ServicePlanInput) (ServicePlan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.planSeq++
	id := fmt.Sprintf("svc-plan-%03d", m.planSeq)
	blockers := []ServiceProblem{}
	if input.ServiceID == "decision" {
		blockers = append(blockers, ServiceProblem{Code: "unsupported", Message: "外部托管服务不支持从控制台控制启停"})
	}
	plan := ServicePlan{
		ID: id, Token: "tok-" + id, ExpectedRevision: m.runtime.Revision,
		Kind: input.Kind, ServiceID: input.ServiceID, ExpiresAt: m.now.Add(5 * time.Minute),
		TargetVersion: "1.4.0", DrainRequired: input.Kind != "start", DrainScope: "global",
		Eligible: len(blockers) == 0, Blockers: blockers, Base: m.runtime,
	}
	m.svcPlans[id] = plan
	return plan, nil
}

func (m *MockClient) ExecuteServiceAction(ctx context.Context, input ServiceExecuteInput) (ServiceOperation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	plan, ok := m.svcPlans[input.PlanID]
	if !ok || plan.Token != input.Token {
		return ServiceOperation{}, &APIError{Status: 409, Problem: &Problem{Code: "conflict", Message: "Service plan invalid or expired"}}
	}
	delete(m.svcPlans, input.PlanID)
	m.maint = &Maintenance{
		MaintenanceID: "maint-demo-002", Revision: "maint-rev-2",
		Scope: ControlScope{Kind: "global"}, Boundary: m.seq,
		StartedAt: m.now, Deadline: m.now.Add(3 * time.Minute), Active: true,
		Stage: "ready", Counts: ControlCounts{Pending: 0, ActiveWorkers: 0, Phases: map[string]int{}},
	}
	m.controls.Maintenance = m.maint
	return ServiceOperation{
		ID: "svc-op-001", RequestID: input.RequestID, PlanID: input.PlanID,
		ServiceID: plan.ServiceID, Kind: plan.Kind, State: "succeeded", Revision: 1,
		CreatedAt: m.now, Deadline: m.now.Add(1 * time.Minute),
		Steps: []ServiceStep{{Name: "drain", State: "done"}, {Name: plan.Kind, State: "succeeded"}, {Name: "readback", State: "succeeded"}},
	}, nil
}

func (m *MockClient) ContinueMaintenance(ctx context.Context, id string) (ServiceOperation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.maint == nil || m.maint.MaintenanceID != id {
		return ServiceOperation{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Maintenance session not found"}}
	}
	m.maint.Active = false
	m.maint.Stage = "ready"
	m.controls.Maintenance = nil
	return ServiceOperation{
		ID: "svc-op-maint-cont", ServiceID: "dispatcher", Kind: "restart", State: "succeeded",
		Revision: 2, MaintenanceID: id, CreatedAt: m.now, Deadline: m.now,
		Steps: []ServiceStep{{Name: "drain_continue", State: "succeeded"}},
	}, nil
}

func (m *MockClient) CancelMaintenance(ctx context.Context, id string) (ServiceOperation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.maint == nil || m.maint.MaintenanceID != id {
		return ServiceOperation{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Maintenance session not found"}}
	}
	m.maint.Active = false
	m.maint.Stage = "canceled"
	m.controls.Maintenance = nil
	return ServiceOperation{
		ID: "svc-op-maint-cancel", ServiceID: "dispatcher", Kind: "restart", State: "canceled",
		Revision: 2, MaintenanceID: id, CreatedAt: m.now, Deadline: m.now,
		Steps: []ServiceStep{{Name: "drain_cancel", State: "canceled"}},
	}, nil
}

func (m *MockClient) ListChanges(ctx context.Context, filter ChangeFilter) (Envelope[ChangePage], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]ChangeRecord, 0, len(m.changes))
	for _, chg := range m.changes {
		if filter.Status != "" && chg.Operation.Status != filter.Status {
			continue
		}
		if filter.Origin != "" && chg.Origin != filter.Origin {
			continue
		}
		items = append(items, chg)
	}
	return Envelope[ChangePage]{
		Data: ChangePage{Items: items}, ObservedAt: m.now, Source: "console_change_journal",
		Freshness: "fresh", Completeness: "partial", Revision: fmt.Sprintf("chg-rev-%d", len(items)),
	}, nil
}

func (m *MockClient) GetChange(ctx context.Context, id string) (Envelope[ChangeRecord], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, chg := range m.changes {
		if chg.ID == id {
			return Envelope[ChangeRecord]{Data: chg, ObservedAt: m.now, Source: "console_change_journal", Freshness: "fresh", Completeness: "partial", Revision: chg.Operation.Revision}, nil
		}
	}
	return Envelope[ChangeRecord]{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Change record not found"}}
}

func (m *MockClient) PlanRollback(ctx context.Context, input RollbackInput) (ChangePlan, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, chg := range m.changes {
		if chg.ID == input.ChangeID {
			if chg.BeforeSnapshot == nil {
				return ChangePlan{}, &APIError{Status: 501, Problem: &Problem{Code: "unsupported", Message: "Change has no retained beforeSnapshot for rollback"}}
			}
			m.planSeq++
			id := fmt.Sprintf("plan-rb-%03d", m.planSeq)
			plan := ChangePlan{
				ID: id, Token: "tok-" + id, ExpiresAt: m.now.Add(5 * time.Minute), Base: m.versions(),
				Changes: []FieldChange{{Component: "dispatcher", Path: "rollbackOf." + chg.ID, Before: chg.Target, After: chg.Base}},
				Effect:  []string{"恢复变更 " + chg.ID + " 之前的公开可见配置快照（发布前需人工确认）"},
				Warnings: []Warning{{Code: "rollback_explicit_plan", Message: "回滚仅生成新的显式变更计划；控制面视图之外的私有本地策略保持不变。"}},
				ComponentOrder: []string{"dispatcher"},
			}
			m.plans[id] = plan
			return plan, nil
		}
	}
	return ChangePlan{}, &APIError{Status: 404, Problem: &Problem{Code: "not_found", Message: "Change record not found"}}
}

func (m *MockClient) GetConfigurationDrift(ctx context.Context, component string) (Envelope[ConfigurationDrift], error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.drift[component]
	if !ok {
		return Envelope[ConfigurationDrift]{}, &APIError{Status: 400, Problem: &Problem{Code: "validation", Message: "Invalid component"}}
	}
	return Envelope[ConfigurationDrift]{
		Data: d, ObservedAt: m.now, Source: "config_drift_reader",
		Freshness: "fresh", Completeness: "partial", Revision: m.versions().Fingerprint,
	}, nil
}

func (m *MockClient) DirectDeliver(ctx context.Context, input DirectDeliverInput) (DirectDeliverResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	targetID := strings.TrimSpace(input.TargetContainerID)
	text := strings.TrimSpace(input.Text)
	idemKey := strings.TrimSpace(input.IdempotencyKey)
	if targetID == "" || text == "" || idemKey == "" {
		return DirectDeliverResult{}, &APIError{Status: 400, Problem: &Problem{Code: "validation", Message: "target_container_id, text, and idempotency_key are required"}}
	}
	grp, exists := m.groups[targetID]
	if !exists || !grp.Enabled || !grp.Whitelisted {
		return DirectDeliverResult{}, &APIError{Status: 403, Problem: &Problem{Code: "forbidden", Message: "target container is not enabled and allowlisted"}}
	}
	src := strings.ToLower(strings.TrimSpace(input.Source))
	trigger := "direct"
	if src != "" {
		trigger = "direct:" + src
	}
	runID := fmt.Sprintf("run-direct-%03d", len(m.runs)+1)
	outcome := "replied"
	delivery := "confirmed"
	reasonCode := ""
	if input.DryRun {
		outcome = "suppressed"
		delivery = "blocked"
		reasonCode = "dry_run"
	}
	now := m.now
	ev := DeliveryEvidence{NoSubmission: input.DryRun}
	if !input.DryRun {
		ack := "bridge_ack"
		echoID := "echo-" + idemKey
		ev = DeliveryEvidence{
			SubmissionStartedAt: &now,
			SubmittedAt:         &now,
			SubmittedAtSource:   &ack,
			ConfirmedAt:         &now,
			BridgeRequestID:     &idemKey,
			EchoMessageID:       &echoID,
		}
	}
	backend := "direct_passthrough"
	var rcPtr *string
	if reasonCode != "" {
		rcPtr = &reasonCode
	}
	m.runs = append([]RunView{{
		ID: runID, Revision: "run-rev-direct-" + idemKey, EntitySeq: uint64(len(m.runs) + 1),
		Source: grp.Group, Target: grp.Group, AcceptedAt: &now, UpdatedAt: now, LastProgressAt: &now,
		Trigger: trigger, MessageCount: ptrInt(1), MessageIDs: []string{"direct:" + idemKey},
		Phase: "terminal", Outcome: outcome, Delivery: delivery, ReasonCode: rcPtr,
		Summary: "Direct passthrough delivery", Completeness: "complete", Versions: m.versions(),
		PlannedBackendID: &backend, Attempts: []AttemptView{}, SideEffects: []SideEffect{}, DeliveryEvidence: ev,
	}}, m.runs...)
	return DirectDeliverResult{
		RunID:             runID,
		Replayed:          false,
		DryRun:            input.DryRun,
		TargetContainerID: targetID,
		Trigger:           trigger,
		Phase:             "terminal",
		Outcome:           outcome,
		Delivery:          delivery,
		ReasonCode:        reasonCode,
		DeliveryEvidence:  ev,
	}, nil
}

func (m *MockClient) SubscribeRuns(ctx context.Context, cursor string, cb RunCallbacks) func() {
	m.mu.Lock()
	id := m.nextSubID
	m.nextSubID++
	m.runSubs[id] = cb
	m.mu.Unlock()
	if cb.OnConnection != nil {
		cb.OnConnection("fresh")
	}
	return func() {
		m.mu.Lock()
		delete(m.runSubs, id)
		m.mu.Unlock()
	}
}

func (m *MockClient) SubscribeArchitecture(ctx context.Context, cursor string, domain string, cb ArchitectureCallbacks) func() {
	m.mu.Lock()
	id := m.nextSubID
	m.nextSubID++
	m.archSubs[id] = cb
	m.mu.Unlock()
	if cb.OnConnection != nil {
		cb.OnConnection("fresh")
	}
	return func() {
		m.mu.Lock()
		delete(m.archSubs, id)
		m.mu.Unlock()
	}
}

// EmitRunEvent lets deterministic tests push a live RunEvent into active subscribers.
func (m *MockClient) EmitRunEvent(ev RunEvent) {
	m.mu.Lock()
	subs := make([]RunCallbacks, 0, len(m.runSubs))
	for _, cb := range m.runSubs {
		subs = append(subs, cb)
	}
	m.mu.Unlock()
	for _, cb := range subs {
		if cb.OnEvent != nil {
			cb.OnEvent(ev)
		}
	}
}

// EmitResetRequired lets tests trigger SSE reset_required deterministically.
func (m *MockClient) EmitResetRequired(p Problem) {
	m.mu.Lock()
	subs := make([]RunCallbacks, 0, len(m.runSubs))
	for _, cb := range m.runSubs {
		subs = append(subs, cb)
	}
	m.mu.Unlock()
	for _, cb := range subs {
		if cb.OnResetRequired != nil {
			cb.OnResetRequired(p)
		}
		if cb.OnConnection != nil {
			cb.OnConnection("disconnected")
		}
	}
}
