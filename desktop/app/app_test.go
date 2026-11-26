package app

import (
	"bytes"
	"context"
	"image/png"
	"strings"
	"sync"
	"testing"
	"time"

	"jinx-desktop/assets"
	"jinx-desktop/client"

	"github.com/egoist/mygo/ui"
)

func newTestedStore(t *testing.T, scenario string) (*Store, *client.MockClient, *ui.Tester) {
	t.Helper()
	mock := client.NewMockClient(scenario)
	store := NewStore(mock, false)
	store.LoadAll(context.Background())
	tt := ui.NewTester(store.View, 1380, 900)
	tt.Frame()
	return store, mock, tt
}

func assertHasText(t *testing.T, tt *ui.Tester, needle string) {
	t.Helper()
	if tt.HasText(needle) {
		return
	}
	for _, txt := range tt.Texts() {
		if strings.Contains(txt, needle) {
			return
		}
	}
	t.Fatalf("expected UI to contain text %q; visible sample=%v", needle, tt.Texts())
}

func assertNoText(t *testing.T, tt *ui.Tester, forbidden string) {
	t.Helper()
	for _, txt := range tt.Texts() {
		if strings.Contains(strings.ToLower(txt), strings.ToLower(forbidden)) {
			t.Fatalf("unexpected forbidden text %q found in %q", forbidden, txt)
		}
	}
}

func clickMust(t *testing.T, tt *ui.Tester, target string) {
	t.Helper()
	if err := tt.Click(target); err != nil {
		t.Fatalf("tt.Click(%q) failed: %v", target, err)
	}
	tt.Frame()
}

func TestDesktopNavigationAndThemeToggle(t *testing.T) {
	store, _, tt := newTestedStore(t, "full")

	assertHasText(t, tt, "沙盒演示")
	assertHasText(t, tt, "系统总览")
	assertHasText(t, tt, "消息处理漏斗（累计统计）")
	assertHasText(t, tt, "在途与待处理消息")
	assertHasText(t, tt, "核心服务健康")

	clickMust(t, tt, "深色模式")
	if !store.DarkMode {
		t.Fatalf("expected DarkMode=true after toggle")
	}
	assertHasText(t, tt, "浅色模式")

	for _, nav := range []struct {
		item     string
		expected string
	}{
		{"来源与动作", "第一层：信息来源矩阵"},
		{"容器与规则", "基础与过滤"},
		{"运行与追踪", "运行队列"},
		{"服务与维护", "全局准入闸门"},
		{"变更与漂移", "变更审计日志"},
	} {
		clickMust(t, tt, nav.item)
		assertHasText(t, tt, nav.expected)
	}
}

func TestSourcesConfirmedDryRunFlow(t *testing.T) {
	store, _, tt := newTestedStore(t, "full")
	clickMust(t, tt, "来源与动作")
	assertHasText(t, tt, "演练模式开启（安全拦截外发）")

	clickMust(t, tt, "切换演练模式 (B站)")
	if !store.DryRunModalOpen {
		t.Fatalf("expected DryRunModalOpen=true")
	}
	assertHasText(t, tt, "确认切换连接器演练模式")
	assertHasText(t, tt, "persisted: false")

	// Safety invariant: Apply button MUST stay disabled until confirmation checkbox is checked.
	applyAcc, ok := tt.Access("确认应用演练开关")
	if !ok || !applyAcc.Disabled {
		t.Fatalf("expected apply button disabled before confirmation checkbox (ok=%v disabled=%v)", ok, applyAcc.Disabled)
	}

	clickMust(t, tt, "我已确认更新运行中连接器的演练模式状态")
	applyAcc, ok = tt.Access("确认应用演练开关")
	if !ok || applyAcc.Disabled {
		t.Fatalf("expected apply button enabled after checking confirmation")
	}

	clickMust(t, tt, "确认应用演练开关")
	if store.DryRunModalOpen {
		t.Fatalf("expected DryRunModalOpen=false after apply")
	}
	assertHasText(t, tt, "B站连接器演练模式已切换（true → false）")
	assertHasText(t, tt, "回读校验通过：true")
}

func TestContainersTriStateMountsAndConflict(t *testing.T) {
	t.Run("tri_state_mounts_and_4_level_permissions", func(t *testing.T) {
		store, _, tt := newTestedStore(t, "full")
		clickMust(t, tt, "容器与规则")

		assertHasText(t, tt, "路由: 是")
		assertHasText(t, tt, "受保护的本地私有策略绑定（只读）")
		if store.Draft.SelfSenderIDs.Mode != client.TriNull {
			t.Fatalf("expected 100001 self_sender_ids TriNull, got %v", store.Draft.SelfSenderIDs.Mode)
		}
		if store.Draft.BlockedSenderIDs.Mode != client.TriEmpty {
			t.Fatalf("expected 100001 blocked_sender_ids TriEmpty, got %v", store.Draft.BlockedSenderIDs.Mode)
		}

		clickMust(t, tt, "场景与挂载")
		assertHasText(t, tt, "场景标识（来自逻辑目录）")
		clickMust(t, tt, "预览挂载变更")
		if !store.ChangePlanModalOpen {
			t.Fatalf("expected ChangePlanModalOpen=true")
		}
		assertHasText(t, tt, "预览配置变更计划")
		clickMust(t, tt, "发布变更")
		if store.LastOperation == nil || store.LastOperation.Status != "succeeded" {
			t.Fatalf("expected succeeded publish operation, got %+v", store.LastOperation)
		}

		clickMust(t, tt, "路由与权限")
		assertHasText(t, tt, "四级权限证据链")
		assertHasText(t, tt, "软规则 (仅只读证据)")
		assertHasText(t, tt, "硬拦截 (不可绕过)")
	})

	t.Run("409_conflict_prompts_reload_and_replan", func(t *testing.T) {
		store, _, tt := newTestedStore(t, "conflict")
		clickMust(t, tt, "容器与规则")
		clickMust(t, tt, "预览并对比差异")
		clickMust(t, tt, "发布变更")

		assertHasText(t, tt, "409 版本冲突")
		clickMust(t, tt, "重新拉取最新配置")
		if store.ChangePlanModalOpen || store.PublishConflict != "" {
			t.Fatalf("expected conflict modal cleared after reload")
		}
	})
}

func TestRunsInspectorTruthInvariantsAndSSE(t *testing.T) {
	store, mock, tt := newTestedStore(t, "unknown-delivery")
	store.StartStreams(context.Background())
	defer store.StopStreams()

	clickMust(t, tt, "运行与追踪")

	// Prominent card for run-demo-003 shows truth callouts right on the feed, never offering resend.
	assertHasText(t, tt, "模型执行成功 ≠ 回复已送达")
	assertHasText(t, tt, "投递状态未知 — 需人工核对（禁止盲目重发）")
	assertNoText(t, tt, "Resend Reply")
	if _, ok := tt.Access("一键重发"); ok {
		t.Fatalf("unexpected resend button present on delivery=unknown run")
	}
	if _, ok := tt.Access("重发回复"); ok {
		t.Fatalf("unexpected resend button present on delivery=unknown run")
	}

	// Clicking the run card opens the slide-over Drawer; Snapshot Modal opens above it.
	clickMust(t, tt, "查看 run-demo-003")
	if !store.RunDrawerOpen {
		t.Fatalf("expected RunDrawerOpen=true after clicking run card")
	}
	clickMust(t, tt, "查看历史配置快照")
	assertHasText(t, tt, "历史配置快照（只读）")
	clickMust(t, tt, "关闭快照")
	clickMust(t, tt, "关闭抽屉")
	if store.RunDrawerOpen {
		t.Fatalf("expected RunDrawerOpen=false after closing drawer")
	}

	// Inspect echo_inferred run (submittedAt == nil) inside Drawer
	clickMust(t, tt, "查看 run-demo-001")
	assertHasText(t, tt, "提交回执时间未记录（仅由自身回声反推，绝不伪造时间）")
	clickMust(t, tt, "关闭抽屉")

	// Inspect may_be_running + disabled controls + not_entered timeline inside Drawer
	clickMust(t, tt, "查看 run-demo-004")
	assertHasText(t, tt, "远端执行可能仍在运行")
	assertHasText(t, tt, "未进入")
	if acc, ok := tt.Access("拦截发送"); !ok || !acc.Disabled {
		t.Fatalf("expected 拦截发送 disabled on terminal blocked run")
	}
	clickMust(t, tt, "关闭抽屉")

	// Simulate SSE event + reset_required notification
	mock.EmitRunEvent(client.RunEvent{EntityID: "run-demo-005", Phase: "confirming"})
	tt.Frame()
	assertHasText(t, tt, "应用实时更新")

	mock.EmitResetRequired(client.Problem{Code: "reset_required", Message: "Cursor fell behind trace ring"})
	tt.Frame()
	assertHasText(t, tt, "事件流游标已过期（reset_required）")
	clickMust(t, tt, "重新同步事件流")
	if store.ResetRequired {
		t.Fatalf("expected ResetRequired cleared after reload")
	}
}

func TestServicesMaintenanceAndChangesRollback(t *testing.T) {
	store, _, tt := newTestedStore(t, "degraded")

	clickMust(t, tt, "服务与维护")
	assertHasText(t, tt, "活动维护排空会话 — maint-demo-001")
	assertHasText(t, tt, "仍有 1 个活跃工作协程和 1 条投递未知任务未收敛")

	if acc, ok := tt.Access("启动 relay"); !ok || !acc.Disabled {
		t.Fatalf("expected 启动 relay disabled when already running")
	}
	clickMust(t, tt, "重启 relay")
	assertHasText(t, tt, "确认服务生命周期计划")
	clickMust(t, tt, "执行服务计划")
	if store.LastServiceOp == nil || store.LastServiceOp.State != "succeeded" {
		t.Fatalf("expected service restart op succeeded, got %+v", store.LastServiceOp)
	}

	clickMust(t, tt, "变更与漂移")
	assertHasText(t, tt, "检测到配置漂移")

	// Direct-click change card opens Change Detail Drawer.
	clickMust(t, tt, "查看 chg-demo-002")
	if !store.ChangeDrawerOpen {
		t.Fatalf("expected ChangeDrawerOpen=true after clicking change card")
	}
	assertHasText(t, tt, "部分生效或状态未知 — 绝不做静默自动补偿")

	clickMust(t, tt, "生成回滚计划")
	assertHasText(t, tt, "预览配置变更计划")
	assertHasText(t, tt, "rollbackOf.chg-demo-002")
}

func TestDesktopLogoLoadedAndRendered(t *testing.T) {
	if !bytes.HasPrefix(assets.LogoPNG, []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}) {
		t.Fatalf("expected embedded LogoPNG to have valid PNG signature, got %d bytes", len(assets.LogoPNG))
	}
	appIconImg, err := png.Decode(bytes.NewReader(assets.AppIconPNG))
	if err != nil {
		t.Fatalf("expected embedded AppIconPNG to decode cleanly: %v", err)
	}
	if appIconImg.Bounds().Dx() != 1024 || appIconImg.Bounds().Dy() != 1024 {
		t.Fatalf("expected 1024x1024 AppIconPNG, got %dx%d", appIconImg.Bounds().Dx(), appIconImg.Bounds().Dy())
	}
	if _, _, _, cornerA := appIconImg.At(0, 0).RGBA(); cornerA != 0 {
		t.Fatalf("expected transparent corner (0,0) for HIG Dock padding, got alpha=%d", cornerA)
	}
	if _, _, _, centerA := appIconImg.At(512, 512).RGBA(); centerA != 0xffff {
		t.Fatalf("expected opaque center (512,512), got alpha=%d", centerA)
	}
	store, _, tt := newTestedStore(t, "full")
	if store.LogoBitmap() == nil {
		t.Fatalf("expected store.LogoBitmap() to be decoded cleanly")
	}
	if _, ok := tt.Access("JINX Logo"); !ok {
		t.Fatalf("expected accessibility node \"JINX Logo\" in sidebar")
	}
	img := ui.Render(store.View, 1380, 900, 1)
	if img == nil || img.Bounds().Dx() != 1380 || img.Bounds().Dy() != 900 {
		t.Fatalf("expected non-nil 1380x900 rendered frame, got %#v", img)
	}
}

func TestConnectionSettingsModalAndLoopbackGuard(t *testing.T) {
	store, _, tt := newTestedStore(t, "full")

	clickMust(t, tt, "连接设置")
	if !store.ConnModalOpen {
		t.Fatalf("expected ConnModalOpen=true after clicking 连接设置")
	}
	assertHasText(t, tt, "配置数据源连接")

	// Non-loopback URL must be blocked with validation error inside modal.
	clickMust(t, tt, "本地控制面")
	store.ConnAPIURL = "http://example.com:8090"
	tt.Frame()
	clickMust(t, tt, "应用并重新连接")
	if !store.ConnModalOpen || store.ConnModalErr == "" {
		t.Fatalf("expected non-loopback URL to remain in modal with validation error")
	}
	assertHasText(t, tt, "地址校验失败")

	// Switching to another mock scenario updates client and closes modal.
	clickMust(t, tt, "沙盒演示")
	store.ConnScenario = "降级与维护排空"
	tt.Frame()
	clickMust(t, tt, "应用并重新连接")
	if store.ConnModalOpen {
		t.Fatalf("expected ConnModalOpen=false after switching mock scenario")
	}
	assertHasText(t, tt, "沙盒演示 (降级与维护排空)")
}

type blockingRunClient struct {
	*client.MockClient
	started  chan struct{}
	release  chan struct{}
	canceled chan struct{}
}

func (b *blockingRunClient) GetRun(ctx context.Context, id string) (client.Envelope[client.RunDetail], error) {
	if id == "slow-stale-run" {
		close(b.started)
		select {
		case <-ctx.Done():
			close(b.canceled)
		case <-b.release:
		}
		env, err := b.MockClient.GetRun(context.Background(), "run-demo-001")
		if err == nil {
			env.Data.Run.ID = "slow-stale-run"
		}
		return env, err
	}
	return b.MockClient.GetRun(ctx, id)
}

func TestStoreEpochDiscardsStaleBackgroundResponseOnSwitch(t *testing.T) {
	slow := &blockingRunClient{
		MockClient: client.NewMockClient("full"),
		started:    make(chan struct{}),
		release:    make(chan struct{}),
		canceled:   make(chan struct{}),
	}
	store := NewStore(slow, false)
	store.LoadAll(context.Background())

	var mu sync.Mutex
	var queued []func()
	dispatched := make(chan struct{}, 4)
	store.SetDispatcher(func(fn func()) {
		mu.Lock()
		queued = append(queued, fn)
		mu.Unlock()
		dispatched <- struct{}{}
	})
	store.SetSyncOps(false)

	store.SelectRunByID("slow-stale-run")
	select {
	case <-slow.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background SelectRunByID did not start")
	}

	// Switch client synchronously on UI thread; this cancels epoch 0 and increments connEpoch.
	store.SetSyncOps(true)
	store.switchClient(nil, client.NewMockClient("degraded"), "已切换")
	wantRunID := ""
	if store.SelectedRun != nil {
		wantRunID = store.SelectedRun.Data.Run.ID
	}

	select {
	case <-slow.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("expected previous connCtx to be canceled upon switchClient")
	}
	close(slow.release)
	select {
	case <-dispatched:
	case <-time.After(2 * time.Second):
		t.Fatal("expected stale callback to attempt UI dispatch")
	}
	mu.Lock()
	for _, fn := range queued {
		fn()
	}
	mu.Unlock()

	if store.SelectedRun != nil && store.SelectedRun.Data.Run.ID == "slow-stale-run" {
		t.Fatalf("stale background response overwrote active store state")
	}
	if store.SelectedRun == nil && wantRunID != "" || (store.SelectedRun != nil && store.SelectedRun.Data.Run.ID != wantRunID) {
		t.Fatalf("unexpected SelectedRun after stale callback ignored: got=%v want=%q", store.SelectedRun, wantRunID)
	}
}

type liveLikeReadabilityClient struct {
	*client.MockClient
	longRunID  string
	longHash   string
	groupID    string
	groupLabel string
}

func (c *liveLikeReadabilityClient) GetArchitecture(ctx context.Context) (client.ArchitectureSnapshot, error) {
	arch, err := c.MockClient.GetArchitecture(ctx)
	if err != nil {
		return arch, err
	}
	seq := uint64(22399)
	arch.Revision = c.longHash
	for i := range arch.EventStreams {
		if arch.EventStreams[i].Domain == "processing" {
			id := strings.Repeat("c3", 16)
			arch.EventStreams[i].StreamID = &id
			arch.EventStreams[i].LastSeq = &seq
		}
	}
	return arch, nil
}

func (c *liveLikeReadabilityClient) ListGroups(ctx context.Context) (client.Envelope[client.GroupPage], error) {
	env, err := c.MockClient.ListGroups(ctx)
	if err != nil {
		return env, err
	}
	if len(env.Data.Items) > 0 {
		g := env.Data.Items[0]
		g.ID = c.groupID
		g.Group = client.GroupRef{ID: c.groupID, Label: "", Visibility: "not_observed"}
		g.UITargetLabel = c.groupLabel
		g.Revision = c.longHash
		g.MissingRoutingReason = "No routing provider is connected"
		env.Data.Items[0] = g
	}
	return env, nil
}

func (c *liveLikeReadabilityClient) ListServices(ctx context.Context) (client.Envelope[[]client.ServiceView], error) {
	env, err := c.MockClient.ListServices(ctx)
	if err != nil {
		return env, err
	}
	for i := range env.Data {
		for k, act := range env.Data[i].Actions {
			act.Enabled = false
			act.Reason = "No verified runtime implementation is connected"
			env.Data[i].Actions[k] = act
		}
	}
	return env, nil
}

func (c *liveLikeReadabilityClient) GetRuntimeServices(ctx context.Context) (client.RuntimeServices, error) {
	rt, err := c.MockClient.GetRuntimeServices(ctx)
	if err != nil {
		return rt, err
	}
	for i := range rt.Services {
		rt.Services[i].Version = c.longHash
	}
	return rt, nil
}

func (c *liveLikeReadabilityClient) ListRuns(ctx context.Context, f client.RunFilter) (client.Envelope[client.RunPage], error) {
	env, err := c.MockClient.ListRuns(ctx, f)
	if err != nil {
		return env, err
	}
	if len(env.Data.Items) > 0 {
		rc := "gate_blocked"
		msgCnt := 3
		r := env.Data.Items[0]
		r.ID = c.longRunID
		r.Revision = c.longHash
		r.Target = client.GroupRef{ID: c.groupID, Label: "", Visibility: "not_observed"}
		r.Source = client.GroupRef{ID: c.groupID, Label: "", Visibility: "not_observed"}
		r.Trigger = "batch"
		r.MessageCount = &msgCnt
		r.Phase = "terminal"
		r.Outcome = "blocked"
		r.Delivery = "blocked"
		r.ReasonCode = &rc
		r.Summary = "Run finished with outcome blocked"
		env.Data.Items = []client.RunView{r}
	}
	return env, nil
}

func (c *liveLikeReadabilityClient) GetRun(ctx context.Context, id string) (client.Envelope[client.RunDetail], error) {
	env, err := c.MockClient.GetRun(ctx, "run-demo-001")
	if err != nil {
		return env, err
	}
	rc := "gate_blocked"
	msgCnt := 3
	env.Data.Run.ID = c.longRunID
	env.Data.Run.Revision = c.longHash
	env.Data.Run.Target = client.GroupRef{ID: c.groupID, Label: "", Visibility: "not_observed"}
	env.Data.Run.Source = client.GroupRef{ID: c.groupID, Label: "", Visibility: "not_observed"}
	env.Data.Run.Trigger = "batch"
	env.Data.Run.MessageCount = &msgCnt
	env.Data.Run.Phase = "terminal"
	env.Data.Run.Outcome = "blocked"
	env.Data.Run.Delivery = "blocked"
	env.Data.Run.ReasonCode = &rc
	env.Data.Run.Summary = "Run finished with outcome blocked"
	now := time.Date(2026, 11, 24, 9, 30, 0, 0, time.UTC)
	env.Data.Timeline = []client.TimelineItem{
		{
			ID:         "tl-live-1",
			RunID:      c.longRunID,
			OccurredAt: &now,
			Source:     "dispatcher_trace",
			Kind:       "run_gating",
			ReasonCode: &rc,
			Summary:    "Run phase gating",
			Certainty:  "observed",
		},
	}
	return env, nil
}

func TestLivePayloadHumanReadabilityRegression(t *testing.T) {
	longRunID := "run-" + strings.Repeat("a1", 32)
	longHash := strings.Repeat("b2", 32)
	lc := &liveLikeReadabilityClient{
		MockClient: client.NewMockClient("full"),
		longRunID:  longRunID,
		longHash:   longHash,
		groupID:    "100099@chatroom",
		groupLabel: "示例真实群名",
	}
	store := NewStore(lc, false)
	store.LoadAll(context.Background())
	// Inject overview recent run mirroring live control plane payload with empty Label and 68-char run ID.
	if store.Overview != nil && len(store.RunsPage.Data.Items) > 0 {
		store.Overview.Data.RecentRuns = []client.RunView{store.RunsPage.Data.Items[0]}
	}
	tt := ui.NewTester(store.View, 1380, 900)
	tt.Frame()

	// 1. Overview resolves UITargetLabel and hides 68-char run IDs.
	assertHasText(t, tt, "示例真实群名")
	assertHasText(t, tt, "前置门禁拦截")
	assertNoText(t, tt, longRunID)
	assertNoText(t, tt, longHash)

	// 2. Containers view resolves UITargetLabel instead of duplicating raw @chatroom.
	clickMust(t, tt, "容器与规则")
	assertHasText(t, tt, "示例真实群名")
	assertNoText(t, tt, longHash)

	// 3. Sources view dereferences *uint64 LastSeq cleanly (no 0x pointer address).
	clickMust(t, tt, "来源与动作")
	assertHasText(t, tt, "22399")
	assertNoText(t, tt, "0x")
	assertNoText(t, tt, longHash)

	// 4. Services view translates gate reason and hides 64-char version hashes.
	clickMust(t, tt, "服务与维护")
	assertHasText(t, tt, "当前未启用运行时进程控制（只读展示）")
	assertNoText(t, tt, "vb2b2")
	assertNoText(t, tt, longHash)

	// 5. Runs view renders Chinese trigger/reason/timeline and keeps raw 68-char run ID inside toggle.
	clickMust(t, tt, "运行与追踪")
	assertHasText(t, tt, "示例真实群名")
	assertHasText(t, tt, "静默聚合触发")
	assertNoText(t, tt, longRunID)

	clickMust(t, tt, "查看 #a1a1a1a1")
	if !store.RunDrawerOpen {
		t.Fatalf("expected RunDrawerOpen=true after clicking #a1a1a1a1")
	}
	assertHasText(t, tt, "前置门禁检查")
	assertHasText(t, tt, "正在执行前置门禁判定")

	tt.SetSize(1380, 1500)
	tt.Frame()
	clickMust(t, tt, "展开技术标识与原始 ID")
	assertHasText(t, tt, longRunID)
}


func TestDrawerInstantFiltersAndDirectCardClick(t *testing.T) {
	store, _, tt := newTestedStore(t, "full")

	// 1. Overview recent run direct card click switches to Runs and opens Drawer immediately.
	clickMust(t, tt, "查看总览运行 run-demo-001")
	if store.ActiveView != ViewRuns || !store.RunDrawerOpen {
		t.Fatalf("expected Overview card click to open Run Drawer in ViewRuns (view=%s open=%v)", store.ActiveView, store.RunDrawerOpen)
	}
	if store.SelectedRun == nil || store.SelectedRun.Data.Run.ID != "run-demo-001" {
		t.Fatalf("expected run-demo-001 selected, got %v", store.SelectedRun)
	}
	// Close Drawer via Escape key.
	tt.Key(0, ui.KeyEscape)
	tt.Frame()
	if store.RunDrawerOpen {
		t.Fatalf("expected Escape key to close Run Drawer")
	}

	// 2. Instant preset chips filter immediately without any submit button.
	if _, ok := tt.Access("筛选运行记录"); ok {
		t.Fatalf("did not expect legacy manual submit button 筛选运行记录")
	}
	clickMust(t, tt, "已发送回复")
	if store.RunPreset != "已发送回复" || store.RunOutcomeFilter != "已回复" {
		t.Fatalf("expected instant preset 已发送回复 applied, got preset=%q outcome=%q", store.RunPreset, store.RunOutcomeFilter)
	}
	if store.RunsPage == nil || len(store.RunsPage.Data.Items) == 0 {
		t.Fatalf("expected replied runs after instant preset")
	}
	for _, r := range store.RunsPage.Data.Items {
		if r.Outcome != "replied" {
			t.Fatalf("unexpected non-replied run %s after 已发送回复 preset", r.ID)
		}
	}

	// Switch back to 全部 via 重置 and verify instant 隐藏已拦截 noise toggle.
	clickMust(t, tt, "重置")
	if store.RunPreset != "全部" || store.RunsPage == nil || len(store.RunsPage.Data.Items) < 2 {
		t.Fatalf("expected reset to restore full run list")
	}
	blockedCountBefore := 0
	for _, r := range store.RunsPage.Data.Items {
		if r.Outcome == "blocked" || r.Outcome == "silent" {
			blockedCountBefore++
		}
	}
	clickMust(t, tt, "隐藏已拦截")
	if !store.RunHideBlocked {
		t.Fatalf("expected RunHideBlocked=true after clicking 隐藏已拦截")
	}
	if blockedCountBefore > 0 {
		assertHasText(t, tt, "已隐藏")
	}

	// 3. Direct whole-card click on Containers (no "选择" button).
	clickMust(t, tt, "容器与规则")
	if _, ok := tt.Access("选择"); ok {
		t.Fatalf("unexpected manual 选择 button present on container cards")
	}
	clickMust(t, tt, "选择容器 100002@chatroom")
	if store.SelectedGroupID != "100002@chatroom" {
		t.Fatalf("expected SelectedGroupID=100002@chatroom after direct card click, got %q", store.SelectedGroupID)
	}
}


type directDisplayClient struct {
	*client.MockClient
}

func (c *directDisplayClient) ListRuns(ctx context.Context, f client.RunFilter) (client.Envelope[client.RunPage], error) {
	env, err := c.MockClient.ListRuns(ctx, f)
	if err != nil || len(env.Data.Items) == 0 {
		return env, err
	}
	backend := "direct_passthrough"
	r := env.Data.Items[0]
	r.ID = "run-direct-ui-1"
	r.Trigger = "direct:twitter"
	r.PlannedBackendID = &backend
	r.Attempts = nil
	r.Phase = "terminal"
	r.Outcome = "replied"
	r.Delivery = "confirmed"
	env.Data.Items = []client.RunView{r}
	return env, nil
}

func (c *directDisplayClient) GetRun(ctx context.Context, id string) (client.Envelope[client.RunDetail], error) {
	env, err := c.MockClient.GetRun(ctx, "run-demo-001")
	if err != nil {
		return env, err
	}
	backend := "direct_passthrough"
	env.Data.Run.ID = "run-direct-ui-1"
	env.Data.Run.Trigger = "direct:twitter"
	env.Data.Run.PlannedBackendID = &backend
	env.Data.Run.Attempts = nil
	env.Data.Run.Phase = "terminal"
	env.Data.Run.Outcome = "replied"
	env.Data.Run.Delivery = "confirmed"
	return env, nil
}

func TestDirectDeliveryDisplayAndDrawer(t *testing.T) {
	dc := &directDisplayClient{MockClient: client.NewMockClient("full")}
	store := NewStore(dc, false)
	store.LoadAll(context.Background())
	tt := ui.NewTester(store.View, 1380, 900)
	tt.Frame()

	clickMust(t, tt, "运行与追踪")
	assertHasText(t, tt, "直发 · Twitter/X（跳过 AI）")
	assertHasText(t, tt, "直发通道（未调用 AI）")

	clickMust(t, tt, "查看 run-direct-ui-1")
	if !store.RunDrawerOpen {
		t.Fatalf("expected RunDrawerOpen=true for direct run")
	}
	assertHasText(t, tt, "本次为直发投递模式，按设计跳过 AI 模型推理，直接交由发送层执行并追踪回声确认。")
}

type fallbackClarityClient struct {
	*client.MockClient
}

func (c *fallbackClarityClient) GetConfigurationDrift(ctx context.Context, comp string) (client.Envelope[client.ConfigurationDrift], error) {
	return client.Envelope[client.ConfigurationDrift]{
		Data: client.ConfigurationDrift{
			Component: comp,
			Available: false,
			Reason:    "No authorized memory/disk configuration reader is connected",
		},
	}, nil
}

func (c *fallbackClarityClient) ListRuns(ctx context.Context, f client.RunFilter) (client.Envelope[client.RunPage], error) {
	env, err := c.MockClient.ListRuns(ctx, f)
	if err != nil {
		return env, err
	}
	now := time.Date(2026, 11, 24, 12, 38, 18, 0, time.UTC)
	end := now.Add(22 * time.Second)
	ext := "controlled-web"
	fbReason := "alma_transport_error"
	rcSilent := "output_none"

	repliedFallback := client.RunView{
		ID:             "run-replied-1",
		Revision:       "rev-rf-1",
		Target:         client.GroupRef{ID: "100001@chatroom", Label: ""},
		Source:         client.GroupRef{ID: "100001@chatroom", Label: ""},
		AcceptedAt:     &now,
		UpdatedAt:      end,
		LastProgressAt: &end,
		Trigger:        "mention",
		Phase:          "terminal",
		Outcome:        "replied",
		Delivery:       "confirmed",
		Summary:        "Delivery confirmed by correlated echo",
		Completeness:   "complete",
		Attempts: []client.AttemptView{
			{
				ID:              "att-1",
				BackendID:       "reply_adapter",
				Status:          "succeeded",
				RemoteExecution: "may_be_running",
				StartedAt:       &now,
				EndedAt:         &end,
			},
			{
				ID:              "att-2",
				BackendID:       "Hermes",
				Status:          "unknown",
				RemoteExecution: "may_be_running",
				ExternalID:      &ext,
				FallbackReason:  &fbReason,
			},
		},
		DeliveryEvidence: client.DeliveryEvidence{
			SubmittedAt: &end,
			ConfirmedAt: &end,
		},
	}

	silentFallback := client.RunView{
		ID:             "run-silent-2",
		Revision:       "rev-sf-2",
		Target:         client.GroupRef{ID: "100001@chatroom", Label: ""},
		Source:         client.GroupRef{ID: "100001@chatroom", Label: ""},
		AcceptedAt:     &now,
		UpdatedAt:      end,
		LastProgressAt: &end,
		Trigger:        "debounce",
		Phase:          "terminal",
		Outcome:        "silent",
		Delivery:       "not_started",
		ReasonCode:     &rcSilent,
		Summary:        "Run finished with outcome silent",
		Completeness:   "partial",
		Attempts: []client.AttemptView{
			{
				ID:              "att-3",
				BackendID:       "reply_adapter",
				Status:          "succeeded",
				RemoteExecution: "may_be_running",
				StartedAt:       &now,
				EndedAt:         &end,
			},
			{
				ID:              "att-4",
				BackendID:       "Hermes",
				Status:          "unknown",
				RemoteExecution: "may_be_running",
				ExternalID:      &ext,
				FallbackReason:  &fbReason,
			},
		},
		DeliveryEvidence: client.DeliveryEvidence{
			NoSubmission: true,
		},
	}

	env.Data.Items = []client.RunView{repliedFallback, silentFallback}
	env.Data.NextCursor = nil
	return env, nil
}

func (c *fallbackClarityClient) GetRun(ctx context.Context, id string) (client.Envelope[client.RunDetail], error) {
	list, err := c.ListRuns(ctx, client.RunFilter{})
	if err != nil {
		return client.Envelope[client.RunDetail]{}, err
	}
	for _, r := range list.Data.Items {
		if r.ID == id {
			return client.Envelope[client.RunDetail]{Data: client.RunDetail{Run: r}}, nil
		}
	}
	return client.Envelope[client.RunDetail]{Data: client.RunDetail{Run: list.Data.Items[0]}}, nil
}

func TestVisualClarityAndPasteplusFallbackSemantics(t *testing.T) {
	fc := &fallbackClarityClient{MockClient: client.NewMockClient("full")}
	store := NewStore(fc, false)
	store.LoadAll(context.Background())
	tt := ui.NewTester(store.View, 1380, 900)
	tt.Frame()

	// 1. Changes Drift card translates unavailable reader reason cleanly without raw English.
	clickMust(t, tt, "变更与漂移")
	assertHasText(t, tt, "当前未接入内存/磁盘配置对比探针（仅展示可观测项）")
	assertNoText(t, tt, "No authorized memory/disk configuration reader")

	// 2. Runs list separates replied fallback (green, no false failure alarm) from intentional silent (compact, no not-delivered warning).
	clickMust(t, tt, "运行与追踪")
	assertHasText(t, tt, "已回复 1 · 投递未知 0 · 已拦截/静默 1")
	assertHasText(t, tt, "Alma 异常已降级")
	assertHasText(t, tt, "主引擎 Alma 通信异常，已降级兜底")
	assertNoText(t, tt, "模型执行成功 ≠ 回复已送达")

	// 3. Open silent run drawer: shows 远端执行可能仍在运行 AND translates attempt status=unknown + controlled-web.
	clickMust(t, tt, "查看 run-silent-2")
	assertHasText(t, tt, "状态未上报")
	assertHasText(t, tt, "受控网页工作区 (controlled-web)")
	assertHasText(t, tt, "主引擎 Alma 通信异常，已降级兜底")
	assertHasText(t, tt, "远端执行可能仍在运行")
	assertNoText(t, tt, "模型执行成功 ≠ 回复已送达")
	clickMust(t, tt, "关闭抽屉")

	// 4. Hiding blocked/silent hides silentFallback cleanly.
	clickMust(t, tt, "隐藏已拦截")
	assertHasText(t, tt, "已隐藏 1 条静默/拦截记录")

	// 5. SegmentedControl in Containers + Connection Modal works interactively.
	clickMust(t, tt, "容器与规则")
	assertHasText(t, tt, "路由: 是")
	clickMust(t, tt, "显式置空")
	if store.Draft.SelfSenderIDs.Mode != client.TriEmpty {
		t.Fatalf("expected SelfSenderIDs.Mode TriEmpty after clicking SegmentedControl, got %v", store.Draft.SelfSenderIDs.Mode)
	}
	clickMust(t, tt, "连接设置")
	clickMust(t, tt, "本地控制面")
	if store.ConnMode != 0 {
		t.Fatalf("expected ConnMode=0 after clicking 本地控制面, got %d", store.ConnMode)
	}
}

func TestForwardRulesAndDirectDeliverUI(t *testing.T) {
	mc := client.NewMockClient("full")
	store := NewStore(mc, false)
	store.LoadAll(context.Background())
	tt := ui.NewTester(store.View, 1380, 900)
	tt.Frame()

	clickMust(t, tt, "容器与规则")
	clickMust(t, tt, "直通转发")
	assertHasText(t, tt, "demo-v2ex-to-wx")
	if len(store.ForwardRulesDraft) != 1 {
		t.Fatalf("expected 1 seeded demo forward rule, got %d", len(store.ForwardRulesDraft))
	}

	// Add a new forward rule draft, plan, and publish.
	store.NewFwdID = "ui-v2ex-wx"
	store.NewFwdSource = "v2ex:t-200001"
	clickMust(t, tt, "添加到草稿列表")
	if !store.ForwardRulesDirty || len(store.ForwardRulesDraft) != 2 {
		t.Fatalf("expected 2 forward rules and dirty=true, got len=%d dirty=%v", len(store.ForwardRulesDraft), store.ForwardRulesDirty)
	}

	clickMust(t, tt, "预览并发布转发规则")
	if !store.ChangePlanModalOpen {
		t.Fatal("expected ChangePlanModalOpen=true")
	}
	clickMust(t, tt, "发布变更")
	if store.ForwardRulesDirty || store.LastOperation == nil || store.LastOperation.Status != "succeeded" {
		t.Fatalf("expected published forward rules without dirty flag, dirty=%v op=%+v", store.ForwardRulesDirty, store.LastOperation)
	}

	// Test manual DirectDeliver panel (dry_run=true by default).
	store.DirectText = "手动直发验证文本"
	clickMust(t, tt, "发送直发测试")
	if store.LastDirectResult == nil || !store.LastDirectResult.DryRun || store.LastDirectResult.ReasonCode != "dry_run" {
		t.Fatalf("expected dry_run DirectDeliverResult, got %+v", store.LastDirectResult)
	}
	assertHasText(t, tt, "最近直发结果")
}
