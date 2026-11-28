// Package app implements the JINX desktop console state store, navigation shell,
// modal workflows, and all 6 operational views using native MyGo UI.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"jinx-desktop/assets"
	"jinx-desktop/client"
	"jinx-desktop/components"
	"jinx-desktop/theme"

	"github.com/egoist/mygo"
	"github.com/egoist/mygo/ui"
)

const (
	ViewOverview   = "overview"
	ViewSources    = "sources"
	ViewContainers = "containers"
	ViewRuns       = "runs"
	ViewServices   = "services"
	ViewChanges    = "changes"
)

// GroupDraft holds unsaved edits for a single container rule set while
// preserving tri-state null vs [] distinctions.
type GroupDraft struct {
	GroupID string
	Dirty   bool

	Enabled     bool
	DryRun      bool
	MentionOnly bool

	MaxReplyRunes     string
	DecisionThreshold string
	BatchSize         string

	Scenario     string
	ResponseGoal string
	Tone         string

	SelfSenderIDs    client.TriStateStringList
	SelfSenderQuery  string
	BlockedSenderIDs client.TriStateStringList
	BlockedQuery     string
	KeywordBlocklist client.TriStateStringList
	KeywordQuery     string
	RegexBlocklist   client.TriStateStringList
	RegexQuery       string
	PromoKeywords    client.TriStateStringList
	PromoQuery       string

	Skills         client.TriStateStringList
	SkillsQuery    string
	Tools          client.TriStateStringList
	ToolsQuery     string
	KnowledgeBases client.TriStateStringList
	KBQuery        string

	AllowedTypesMode int // 0: Inherit(null), 1: Empty([]), 2: Custom
	AllowedTypesText string
}

// Store owns desktop console state, background SSE subscriptions, and action workflows.
type Store struct {
	client     client.ConsoleClient
	runCtx     context.Context
	connEpoch  uint64
	connCtx    context.Context
	connCancel context.CancelFunc
	win        *mygo.Window
	dispatch   func(func())
	syncOps    bool
	reqSeq     atomic.Uint64
	logoBitmap *ui.Bitmap

	ConnModalOpen bool
	ConnMode      int // 0: 本地控制面, 1: 沙盒演示
	ConnAPIURL    string
	ConnScenario  string
	ConnModalErr  string

	ActiveView string
	DarkMode   bool

	SecOps     bool
	SecConfig  bool
	SecRuntime bool

	SplitRuns    float32
	SplitChanges float32

	Busy         bool
	LastError    string
	LastToast    string
	pendingToast string

	Freshness         string
	SSEState          string
	PendingSSEUpdates bool
	ResetRequired     bool
	ResetMessage      string

	Boot            *client.Bootstrap
	Overview        *client.Envelope[client.Overview]
	Arch            *client.ArchitectureSnapshot
	Connectors      []client.ConnectorView
	Catalog         *client.Envelope[client.CatalogView]
	Groups          []client.GroupView
	GlobalRules     *client.Envelope[client.GlobalRulesView]
	RelayRules      *client.Envelope[client.RelayRulesView]
	Backends        []client.Backend
	Controls        *client.ControlSnapshot
	RuntimeSvcs     *client.RuntimeServices
	Services        []client.ServiceView
	RunsPage        *client.Envelope[client.RunPage]
	SelectedRun     *client.Envelope[client.RunDetail]
	ChangesPage     *client.Envelope[client.ChangePage]
	SelectedChange  *client.Envelope[client.ChangeRecord]
	DriftDispatcher *client.Envelope[client.ConfigurationDrift]
	DriftRelay      *client.Envelope[client.ConfigurationDrift]

	// Sources & Connectors modal state
	DryRunModalOpen  bool
	DryRunPlan       *client.ConnectorDryRunPlan
	DryRunConfirm    bool
	LastDryRunResult *client.ConnectorDryRunApplyResult

	// Containers & Rules state
	GroupList        ui.ListState
	SelectedGroupIdx int
	SelectedGroupID  string
	ContainerTab     int
	Draft            GroupDraft
	SelectedRouting  *client.GroupRouting
	SelectedPerms    *client.GroupPermissions

	GlobalDryRun        bool
	GlobalLayaFilter    bool
	GlobalBypassMention bool
	RelayCleanContent   bool
	RelayIgnoreEcho     bool
	RelayMonitorAll     bool
	GlobalRelayDirty    bool

	ForwardRulesDraft []client.ForwardRule
	ForwardRulesDirty bool
	NewFwdID          string
	NewFwdSource      string
	NewFwdTargetLabel string
	NewFwdTemplate    string
	NewFwdEnabled     bool
	NewFwdDryRun      bool
	NewFwdIncludeEcho bool
	DirectTargetLabel string
	DirectText        string
	DirectDryRun      bool
	LastDirectResult  *client.DirectDeliverResult

	ChangePlanModalOpen bool
	ActivePlan          *client.ChangePlan
	ActivePlanIsRelay   bool
	PublishConflict     string
	LastOperation       *client.Operation

	// Runs & Trace state
	RunList                ui.ListState
	SelectedRunIdx         int
	RunDrawerOpen          bool
	RunPreset              string
	RunPhaseFilter         string
	RunOutcomeFilter       string
	RunDeliveryFilter      string
	RunGroupFilter         string
	RunGroupFilterLabel    string
	RunQuery               string
	RunHideBlocked         bool
	ShowAdvancedRunFilters bool
	ShowRunTechIDs         bool
	SnapshotModalOpen      bool
	ActiveSnapshot         *client.ConfigSnapshot
	SnapshotMissingReason  string

	// Services & Maintenance state
	ServiceList          ui.ListState
	SelectedServiceIdx   int
	ServicePlanModalOpen bool
	ActiveServicePlan    *client.ServicePlan
	LastServiceOp        *client.ServiceOperation

	// Changes & Drift state
	ChangeList         ui.ListState
	SelectedChangeIdx  int
	ChangeDrawerOpen   bool
	ChangeStatusFilter string

	stopRunSSE   func()
	stopArchSSE  func()
	stopConnPoll func()
}

var (
	logoDecodeOnce sync.Once
	logoBitmapInst *ui.Bitmap
)

func cachedLogoBitmap() *ui.Bitmap {
	logoDecodeOnce.Do(func() {
		bmp, err := ui.DecodeBitmapScaled(assets.LogoPNG, 128)
		if err == nil {
			logoBitmapInst = bmp
		}
	})
	return logoBitmapInst
}

// NewStore constructs a console Store backed by c. In headless tests SyncOps
// runs client calls inline on the UI thread for race-free deterministic frames;
// SetWindow switches to non-blocking background calls with Window.Update.
func NewStore(c client.ConsoleClient, dark bool) *Store {
	apiURL := "http://127.0.0.1:8090"
	connMode := 0
	connScenario := "完整多来源"
	if c != nil {
		if c.IsMock() {
			connMode = 1
			connScenario = scenarioCN(strings.TrimPrefix(c.Label(), "mock:"))
		} else if c.Label() != "" {
			apiURL = c.Label()
		}
	}
	s := &Store{
		client:             c,
		dispatch:           func(fn func()) { fn() },
		syncOps:            true,
		logoBitmap:         cachedLogoBitmap(),
		ConnMode:           connMode,
		ConnAPIURL:         apiURL,
		ConnScenario:       connScenario,
		ActiveView:         ViewOverview,
		DarkMode:          dark,
		SecOps:            true,
		SecConfig:         true,
		SecRuntime:        true,
		SplitRuns:         540,
		SplitChanges:      520,
		Freshness:           "unknown",
		SSEState:            "disconnected",
		RunPreset:           "全部",
		RunPhaseFilter:      "全部",
		RunOutcomeFilter:    "全部",
		RunDeliveryFilter:   "全部",
		RunQuery:            "",
		RunGroupFilterLabel: "全部容器",
		ChangeStatusFilter:  "全部",
		NewFwdEnabled:       true,
		NewFwdDryRun:        true,
		DirectDryRun:        true,
	}
	s.GroupList.Selected = &s.SelectedGroupIdx
	s.RunList.Selected = &s.SelectedRunIdx
	s.ServiceList.Selected = &s.SelectedServiceIdx
	s.ChangeList.Selected = &s.SelectedChangeIdx
	return s
}

// SetWindow binds a native MyGo window so background callbacks marshal onto the UI thread.
func (s *Store) SetWindow(win *mygo.Window) {
	s.win = win
	s.syncOps = false
	s.dispatch = func(fn func()) {
		if s.win != nil && !s.win.IsDestroyed() {
			s.win.Update(func() {
				fn()
				s.win.Invalidate()
			})
		}
	}
}

// SetDispatcher overrides async UI marshaling for testing.
func (s *Store) SetDispatcher(fn func(func())) {
	if fn != nil {
		s.dispatch = fn
	}
}

// SetSyncOps controls whether button-triggered actions execute inline or in background goroutines.
func (s *Store) SetSyncOps(sync bool) {
	s.syncOps = sync
}

func (s *Store) Client() client.ConsoleClient { return s.client }

// LogoBitmap returns the decoded desktop logo bitmap for the top telemetry bar.
func (s *Store) LogoBitmap() *ui.Bitmap { return s.logoBitmap }

func (s *Store) Palette() theme.Palette { return theme.Get(s.DarkMode) }

func (s *Store) nextRequestID(prefix string) string {
	n := s.reqSeq.Add(1)
	return fmt.Sprintf("req-%s-%06d", prefix, n)
}

func (s *Store) Capability(name string) client.Availability {
	if s.Boot != nil {
		if a, ok := s.Boot.Capabilities[name]; ok {
			return a
		}
	}
	return client.Availability{Supported: false, Reason: "当前控制面未声明该能力"}
}

func (s *Store) cancelConn() {
	s.connEpoch++
	if s.connCancel != nil {
		s.connCancel()
		s.connCancel = nil
	}
	s.connCtx = nil
}

func (s *Store) ensureConn() (context.Context, client.ConsoleClient, uint64) {
	if s.connCtx == nil {
		base := s.runCtx
		if base == nil {
			base = context.Background()
		}
		s.connCtx, s.connCancel = context.WithCancel(base)
	}
	return s.connCtx, s.client, s.connEpoch
}

func (s *Store) dispatchEpoch(epoch uint64, fn func()) {
	s.dispatch(func() {
		if epoch != s.connEpoch {
			return
		}
		fn()
	})
}

func (s *Store) stopStreamHandles() {
	if s.stopRunSSE != nil {
		s.stopRunSSE()
		s.stopRunSSE = nil
	}
	if s.stopArchSSE != nil {
		s.stopArchSSE()
		s.stopArchSSE = nil
	}
	if s.stopConnPoll != nil {
		s.stopConnPoll()
		s.stopConnPoll = nil
	}
}

// StartStreams subscribes to /console/v1/events and /console/v1/architecture/events.
func (s *Store) StartStreams(ctx context.Context) {
	s.stopStreamHandles()
	connCtx, cli, epoch := s.ensureConn()
	if ctx != nil && s.connCtx == nil {
		connCtx = ctx
	}
	runCursor := ""
	if s.Boot != nil && s.Boot.Cursor != nil {
		runCursor = *s.Boot.Cursor
	}
	archCursor := runCursor
	if s.Arch != nil {
		for _, es := range s.Arch.EventStreams {
			if es.Domain == "processing" && es.Cursor != nil {
				archCursor = *es.Cursor
			}
		}
	}
	if runCursor != "" && s.Capability("events.resume").Supported {
		s.stopRunSSE = cli.SubscribeRuns(connCtx, runCursor, client.RunCallbacks{
			OnEvent: func(ev client.RunEvent) {
				s.dispatchEpoch(epoch, func() {
					s.SSEState = "fresh"
					s.PendingSSEUpdates = true
				})
			},
			OnResetRequired: func(p client.Problem) {
				s.dispatchEpoch(epoch, func() {
					s.SSEState = "disconnected"
					s.ResetRequired = true
					s.ResetMessage = p.Message
				})
			},
			OnUnavailable: func(p client.Problem) {
				s.dispatchEpoch(epoch, func() {
					s.SSEState = "stale"
				})
			},
			OnConnection: func(state string) {
				s.dispatchEpoch(epoch, func() {
					s.SSEState = state
				})
			},
		})
	}
	if archCursor != "" {
		s.stopArchSSE = cli.SubscribeArchitecture(connCtx, archCursor, "", client.ArchitectureCallbacks{
			OnEvent: func(ev client.ArchitectureEvent, cursor string) {
				s.dispatchEpoch(epoch, func() {
					s.SSEState = "fresh"
					s.PendingSSEUpdates = true
				})
			},
			OnResetRequired: func(p client.Problem) {
				s.dispatchEpoch(epoch, func() {
					s.ResetRequired = true
					if s.ResetMessage == "" {
						s.ResetMessage = p.Message
					}
				})
			},
		})
	}
	if !s.syncOps && cli != nil && !cli.IsMock() {
		pollCtx, pollCancel := context.WithCancel(connCtx)
		s.stopConnPoll = pollCancel
		go func() {
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-pollCtx.Done():
					return
				case <-ticker.C:
					viewCh := make(chan string, 1)
					s.dispatchEpoch(epoch, func() {
						select {
						case viewCh <- s.ActiveView:
						default:
						}
					})
					var activeView string
					dispatchTimeout := time.NewTimer(2 * time.Second)
					select {
					case activeView = <-viewCh:
						dispatchTimeout.Stop()
					case <-dispatchTimeout.C:
						continue
					case <-pollCtx.Done():
						dispatchTimeout.Stop()
						return
					}
					if activeView != ViewSources && activeView != ViewOverview {
						continue
					}
					conns, errConns := cli.ListConnectors(pollCtx)
					arch, errArch := cli.GetArchitecture(pollCtx)
					if errConns != nil && errArch != nil {
						continue
					}
					s.dispatchEpoch(epoch, func() {
						if errConns == nil {
							s.Connectors = conns.Data
						}
						if errArch == nil {
							s.Arch = &arch
						}
					})
				}
			}
		}()
	}
}

func (s *Store) StopStreams() {
	s.stopStreamHandles()
	s.cancelConn()
}

type loadedBundle struct {
	boot            *client.Bootstrap
	overview        *client.Envelope[client.Overview]
	arch            *client.ArchitectureSnapshot
	connectors      []client.ConnectorView
	catalog         *client.Envelope[client.CatalogView]
	groups          []client.GroupView
	globalRules     *client.Envelope[client.GlobalRulesView]
	relayRules      *client.Envelope[client.RelayRulesView]
	backends        []client.Backend
	controls        *client.ControlSnapshot
	runtimeSvcs     *client.RuntimeServices
	services        []client.ServiceView
	runsPage        *client.Envelope[client.RunPage]
	selectedRun     *client.Envelope[client.RunDetail]
	changesPage     *client.Envelope[client.ChangePage]
	selectedChange  *client.Envelope[client.ChangeRecord]
	driftDispatcher *client.Envelope[client.ConfigurationDrift]
	driftRelay      *client.Envelope[client.ConfigurationDrift]
	selectedRouting *client.GroupRouting
	selectedPerms   *client.GroupPermissions
	err             string
}

func (s *Store) fetchBundle(ctx context.Context, cli client.ConsoleClient, runFilter client.RunFilter, changeStatus, groupID string) loadedBundle {
	var b loadedBundle
	var errs []string
	recordErr := func(op string, err error) {
		if err != nil {
			var apiErr *client.APIError
			if errors.As(err, &apiErr) && apiErr.Problem != nil && apiErr.Problem.Code == "unsupported" {
				return
			}
			errs = append(errs, fmt.Sprintf("%s: %v", op, err))
		}
	}

	if boot, err := cli.Bootstrap(ctx); err == nil {
		b.boot = &boot
		ov := boot.Overview
		b.overview = &ov
		if len(ov.Data.Services) > 0 {
			b.services = ov.Data.Services
		}
	} else {
		recordErr("bootstrap", err)
	}
	if arch, err := cli.GetArchitecture(ctx); err == nil {
		b.arch = &arch
	} else {
		recordErr("architecture", err)
	}
	if conns, err := cli.ListConnectors(ctx); err == nil {
		b.connectors = conns.Data
	}
	if cat, err := cli.GetCatalog(ctx); err == nil {
		b.catalog = &cat
	}
	if svcs, err := cli.ListServices(ctx); err == nil {
		b.services = svcs.Data
	} else {
		recordErr("services", err)
	}
	if rt, err := cli.GetRuntimeServices(ctx); err == nil {
		b.runtimeSvcs = &rt
	}
	if ctl, err := cli.GetControls(ctx); err == nil {
		b.controls = &ctl
	}
	if gp, err := cli.ListGroups(ctx); err == nil {
		b.groups = gp.Data.Items
		if groupID == "" && len(b.groups) > 0 {
			groupID = b.groups[0].ID
		}
		if groupID != "" {
			if r, err := cli.GetGroupRouting(ctx, groupID); err == nil {
				b.selectedRouting = &r
			}
			if p, err := cli.GetGroupPermissions(ctx, groupID); err == nil {
				b.selectedPerms = &p
			}
		}
	} else {
		recordErr("groups", err)
	}
	if gr, err := cli.GetGlobalRules(ctx); err == nil {
		b.globalRules = &gr
	} else {
		recordErr("global_rules", err)
	}
	if rr, err := cli.GetRelayRules(ctx); err == nil {
		b.relayRules = &rr
	}
	if bks, err := cli.ListBackends(ctx); err == nil {
		b.backends = bks
	}
	if rp, err := cli.ListRuns(ctx, runFilter); err == nil {
		b.runsPage = &rp
		if len(rp.Data.Items) > 0 {
			if detail, err := cli.GetRun(ctx, rp.Data.Items[0].ID); err == nil {
				b.selectedRun = &detail
			}
		}
	}
	cf := client.ChangeFilter{Status: changeStatus, Limit: 50}
	if cp, err := cli.ListChanges(ctx, cf); err == nil {
		b.changesPage = &cp
		if len(cp.Data.Items) > 0 {
			rec := cp.Data.Items[0]
			b.selectedChange = &client.Envelope[client.ChangeRecord]{Data: rec, Revision: cp.Revision}
		}
	} else {
		recordErr("changes", err)
	}
	if dd, err := cli.GetConfigurationDrift(ctx, "dispatcher"); err == nil {
		b.driftDispatcher = &dd
	}
	if dr, err := cli.GetConfigurationDrift(ctx, "relay"); err == nil {
		b.driftRelay = &dr
	}
	if len(errs) > 0 {
		b.err = strings.Join(errs, "; ")
	}
	return b
}

func (s *Store) applyBundle(b loadedBundle) {
	s.Busy = false
	s.LastError = b.err
	if b.boot != nil {
		s.Boot = b.boot
	}
	if b.overview != nil {
		s.Overview = b.overview
		s.Freshness = b.overview.Freshness
	}
	if b.arch != nil {
		s.Arch = b.arch
	}
	if b.connectors != nil {
		s.Connectors = b.connectors
	}
	if b.catalog != nil {
		s.Catalog = b.catalog
	}
	if b.services != nil {
		s.Services = b.services
	}
	if b.runtimeSvcs != nil {
		s.RuntimeSvcs = b.runtimeSvcs
	}
	if b.controls != nil {
		s.Controls = b.controls
	}
	if b.groups != nil {
		s.Groups = b.groups
		s.syncSelectedGroupLocked()
		s.syncForwardTargetDefaults()
	}
	if b.selectedRouting != nil {
		s.SelectedRouting = b.selectedRouting
	}
	if b.selectedPerms != nil {
		s.SelectedPerms = b.selectedPerms
	}
	if b.globalRules != nil {
		s.GlobalRules = b.globalRules
		s.syncGlobalDraft()
	}
	if b.relayRules != nil {
		s.RelayRules = b.relayRules
		s.syncRelayDraft()
	}
	if b.backends != nil {
		s.Backends = b.backends
	}
	if b.runsPage != nil {
		s.RunsPage = b.runsPage
		s.SelectedRunIdx = clampIndex(s.SelectedRunIdx, len(b.runsPage.Data.Items))
	}
	if b.selectedRun != nil {
		s.SelectedRun = b.selectedRun
	}
	if b.changesPage != nil {
		s.ChangesPage = b.changesPage
		s.SelectedChangeIdx = clampIndex(s.SelectedChangeIdx, len(b.changesPage.Data.Items))
	}
	if b.selectedChange != nil {
		s.SelectedChange = b.selectedChange
	}
	if b.driftDispatcher != nil {
		s.DriftDispatcher = b.driftDispatcher
	}
	if b.driftRelay != nil {
		s.DriftRelay = b.driftRelay
	}
	s.PendingSSEUpdates = false
	s.ResetRequired = false
	s.ResetMessage = ""
}

// Start performs the initial console load in the background when a window is attached
// and arms SSE subscriptions safely on the UI thread via dispatch.
func (s *Store) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.stopStreamHandles()
	s.cancelConn()
	s.runCtx = ctx
	connCtx, cli, epoch := s.ensureConn()
	rf := s.currentRunFilter()
	cs := components.ChangeStatusFilterValue(s.ChangeStatusFilter)
	gid := s.SelectedGroupID
	s.Busy = true
	s.runOp(func() {
		b := s.fetchBundle(connCtx, cli, rf, cs, gid)
		s.dispatchEpoch(epoch, func() {
			s.applyBundle(b)
			s.StartStreams(connCtx)
		})
	})
}

// OpenConnectionModal opens the in-app connection settings dialog seeded from current state.
func (s *Store) OpenConnectionModal() {
	if s.client != nil && s.client.IsMock() {
		s.ConnMode = 1
		s.ConnScenario = scenarioCN(strings.TrimPrefix(s.client.Label(), "mock:"))
	} else {
		s.ConnMode = 0
		if s.client != nil && s.client.Label() != "" {
			s.ConnAPIURL = s.client.Label()
		}
	}
	if s.ConnAPIURL == "" {
		s.ConnAPIURL = "http://127.0.0.1:8090"
	}
	if s.ConnScenario == "" {
		s.ConnScenario = "完整多来源"
	}
	s.ConnModalErr = ""
	s.ConnModalOpen = true
}

// ApplyConnectionSettings validates the target connection and switches active client cleanly.
func (s *Store) ApplyConnectionSettings(c *ui.Context) {
	if s.ConnMode == 1 {
		next := client.NewMockClient(scenarioKeyFromCN(s.ConnScenario))
		s.switchClient(c, next, "已切换至沙盒演示模式（"+s.ConnScenario+"）")
		return
	}
	rawURL := strings.TrimSpace(s.ConnAPIURL)
	hc, err := client.NewHTTPClient(rawURL)
	if err != nil {
		s.ConnModalErr = err.Error()
		return
	}
	s.ConnAPIURL = rawURL
	s.switchClient(c, hc, "已切换至本地回环控制面（"+rawURL+"）")
}

func (s *Store) switchClient(c *ui.Context, next client.ConsoleClient, toastMsg string) {
	s.StopStreams()
	s.client = next
	s.ConnModalOpen = false
	s.ConnModalErr = ""
	s.LastError = ""
	s.ResetRequired = false
	s.ResetMessage = ""
	s.PendingSSEUpdates = false
	s.Freshness = "unknown"
	s.SSEState = "disconnected"

	s.Boot = nil
	s.Overview = nil
	s.Arch = nil
	s.Connectors = nil
	s.Catalog = nil
	s.Groups = nil
	s.GlobalRules = nil
	s.RelayRules = nil
	s.Backends = nil
	s.Controls = nil
	s.RuntimeSvcs = nil
	s.Services = nil
	s.RunsPage = nil
	s.SelectedRun = nil
	s.ChangesPage = nil
	s.SelectedChange = nil
	s.DriftDispatcher = nil
	s.DriftRelay = nil
	s.SelectedRouting = nil
	s.SelectedPerms = nil
	s.LastDryRunResult = nil
	s.LastOperation = nil
	s.LastServiceOp = nil
	s.ActivePlan = nil
	s.DryRunPlan = nil
	s.ActiveServicePlan = nil
	s.Draft = GroupDraft{}
	s.GlobalRelayDirty = false
	s.ForwardRulesDraft = nil
	s.ForwardRulesDirty = false
	s.LastDirectResult = nil
	s.SelectedGroupIdx = 0
	s.SelectedGroupID = ""
	s.SelectedRunIdx = 0
	s.SelectedChangeIdx = 0
	s.RunDrawerOpen = false
	s.ChangeDrawerOpen = false
	s.RunPreset = "全部"
	s.RunPhaseFilter = "全部"
	s.RunOutcomeFilter = "全部"
	s.RunDeliveryFilter = "全部"
	s.RunGroupFilter = ""
	s.RunGroupFilterLabel = "全部容器"
	s.RunQuery = ""
	s.RunHideBlocked = false
	s.ShowAdvancedRunFilters = false
	s.ShowRunTechIDs = false

	if s.syncOps {
		s.LoadAll(context.Background())
		s.showToast(c, toastMsg)
		return
	}
	ctx := s.runCtx
	if ctx == nil {
		ctx = context.Background()
	}
	s.Start(ctx)
	s.showToast(c, toastMsg)
}

func scenarioKeyFromCN(label string) string {
	switch label {
	case "降级与维护排空":
		return "degraded"
	case "409 并发冲突":
		return "conflict"
	case "未知投递证据":
		return "unknown-delivery"
	default:
		return "full"
	}
}

// LoadAll performs an initial or full refresh of console state synchronously on the caller's thread.
func (s *Store) LoadAll(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	rf := s.currentRunFilter()
	cs := components.ChangeStatusFilterValue(s.ChangeStatusFilter)
	gid := s.SelectedGroupID
	b := s.fetchBundle(ctx, s.client, rf, cs, gid)
	s.applyBundle(b)
}

// TriggerRefresh refreshes console data and re-arms SSE if reset_required was seen.
func (s *Store) TriggerRefresh(c *ui.Context) {
	connCtx, cli, epoch := s.ensureConn()
	rf := s.currentRunFilter()
	cs := components.ChangeStatusFilterValue(s.ChangeStatusFilter)
	gid := s.SelectedGroupID
	wasReset := s.ResetRequired
	s.Busy = true
	s.runOp(func() {
		b := s.fetchBundle(connCtx, cli, rf, cs, gid)
		s.dispatchEpoch(epoch, func() {
			s.applyBundle(b)
			if wasReset {
				s.StartStreams(connCtx)
			}
			if c != nil && b.err == "" {
				s.showToast(c, "控制台数据已刷新")
			}
		})
	})
}

func (s *Store) runOp(fn func()) {
	if s.syncOps {
		fn()
		return
	}
	go fn()
}

func (s *Store) showToast(c *ui.Context, msg string) {
	s.LastToast = msg
	if c != nil && c.Valid() {
		c.Toast(msg)
		return
	}
	s.pendingToast = msg
}

func clampIndex(idx, n int) int {
	if n == 0 {
		return -1
	}
	if idx < 0 || idx >= n {
		return 0
	}
	return idx
}

func (s *Store) syncSelectedGroupLocked() {
	if len(s.Groups) == 0 {
		s.SelectedGroupIdx = -1
		s.SelectedGroupID = ""
		return
	}
	if s.SelectedGroupID != "" {
		for i, g := range s.Groups {
			if g.ID == s.SelectedGroupID {
				s.SelectedGroupIdx = i
				if !s.Draft.Dirty || s.Draft.GroupID != g.ID {
					s.loadGroupDraft(g)
				}
				return
			}
		}
	}
	s.SelectedGroupIdx = clampIndex(s.SelectedGroupIdx, len(s.Groups))
	g := s.Groups[s.SelectedGroupIdx]
	s.SelectedGroupID = g.ID
	if !s.Draft.Dirty || s.Draft.GroupID != g.ID {
		s.loadGroupDraft(g)
	}
}

func fieldBool(fields map[string]client.FieldValue, name string) bool {
	f, ok := fields[name]
	if !ok {
		return false
	}
	b, _ := f.Configured.(bool)
	return b
}

func fieldStr(fields map[string]client.FieldValue, name string) string {
	f, ok := fields[name]
	if !ok || f.Configured == nil {
		return ""
	}
	if s, ok := f.Configured.(string); ok {
		return s
	}
	return fmt.Sprint(f.Configured)
}

func fieldNumStr(fields map[string]client.FieldValue, name string) string {
	f, ok := fields[name]
	if !ok || f.Configured == nil {
		return "0"
	}
	switch v := f.Configured.(type) {
	case float64:
		if v == float64(int64(v)) {
			return fmt.Sprintf("%d", int64(v))
		}
		return fmt.Sprintf("%.2f", v)
	case int:
		return strconv.Itoa(v)
	default:
		return fmt.Sprint(v)
	}
}

const unsetScenarioLabel = "未指定场景模板（默认）"

func (s *Store) loadGroupDraft(g client.GroupView) {
	at := client.DecodeTriStateIntList(g.Fields["allowed_types"].Configured)
	atMode := 0
	atText := ""
	switch at.Mode {
	case client.TriEmpty:
		atMode = 1
	case client.TriValues:
		atMode = 2
		parts := make([]string, 0, len(at.Values))
		for _, v := range at.Values {
			parts = append(parts, strconv.Itoa(v))
		}
		atText = strings.Join(parts, ", ")
	}
	s.Draft = GroupDraft{
		GroupID:           g.ID,
		Dirty:             false,
		Enabled:           fieldBool(g.Fields, "enabled"),
		DryRun:            fieldBool(g.Fields, "dry_run"),
		MentionOnly:       fieldBool(g.Fields, "mention_only"),
		MaxReplyRunes:     fieldNumStr(g.Fields, "max_reply_runes"),
		DecisionThreshold: fieldNumStr(g.Fields, "decision_threshold"),
		BatchSize:         fieldNumStr(g.Fields, "batch_size"),
		Scenario:          nonEmptyOr(fieldStr(g.Fields, "scenario"), unsetScenarioLabel),
		ResponseGoal:      fieldStr(g.Fields, "response_goal"),
		Tone:              fieldStr(g.Fields, "tone"),
		SelfSenderIDs:     client.DecodeTriStateStringList(g.Fields["self_sender_ids"].Configured),
		BlockedSenderIDs:  client.DecodeTriStateStringList(g.Fields["blocked_sender_ids"].Configured),
		KeywordBlocklist:  client.DecodeTriStateStringList(g.Fields["keyword_blocklist"].Configured),
		RegexBlocklist:    client.DecodeTriStateStringList(g.Fields["regex_blocklist"].Configured),
		PromoKeywords:     client.DecodeTriStateStringList(g.Fields["promo_keywords"].Configured),
		Skills:            client.DecodeTriStateStringList(g.Fields["skills"].Configured),
		Tools:             client.DecodeTriStateStringList(g.Fields["tools"].Configured),
		KnowledgeBases:    client.DecodeTriStateStringList(g.Fields["knowledge_bases"].Configured),
		AllowedTypesMode:  atMode,
		AllowedTypesText:  atText,
	}
}

func (s *Store) syncGlobalDraft() {
	if s.GlobalRules == nil {
		return
	}
	f := s.GlobalRules.Data.Fields
	if !s.GlobalRelayDirty {
		s.GlobalDryRun = fieldBool(f, "dry_run")
		s.GlobalLayaFilter = fieldBool(f, "laya_filter")
		s.GlobalBypassMention = fieldBool(f, "bypass_decision_on_mention")
	}
	if !s.ForwardRulesDirty {
		if fwd, ok := f["forward_rules"]; ok {
			s.ForwardRulesDraft = client.DecodeForwardRules(fwd.Configured)
		} else {
			s.ForwardRulesDraft = nil
		}
	}
}

func (s *Store) syncRelayDraft() {
	if s.GlobalRelayDirty || s.RelayRules == nil {
		return
	}
	r := s.RelayRules.Data.Rules
	s.RelayCleanContent = r.Filters.CleanContent
	s.RelayIgnoreEcho = r.Filters.IgnoreEcho
	s.RelayMonitorAll = r.GroupControl.MonitorAll
}

func (s *Store) SelectGroupByIndex(idx int, c *ui.Context) {
	if idx < 0 || idx >= len(s.Groups) {
		return
	}
	s.SelectedGroupIdx = idx
	g := s.Groups[idx]
	s.SelectedGroupID = g.ID
	s.loadGroupDraft(g)
	connCtx, cli, epoch := s.ensureConn()
	groupID := g.ID
	s.runOp(func() {
		rt, rtErr := cli.GetGroupRouting(connCtx, groupID)
		pm, pmErr := cli.GetGroupPermissions(connCtx, groupID)
		s.dispatchEpoch(epoch, func() {
			if rtErr == nil {
				s.SelectedRouting = &rt
			} else {
				s.SelectedRouting = nil
			}
			if pmErr == nil {
				s.SelectedPerms = &pm
			} else {
				s.SelectedPerms = nil
			}
		})
	})
}

func (s *Store) SelectedGroup() (client.GroupView, bool) {
	if len(s.Groups) == 0 {
		return client.GroupView{}, false
	}
	idx := clampIndex(s.SelectedGroupIdx, len(s.Groups))
	if idx < 0 {
		return client.GroupView{}, false
	}
	return s.Groups[idx], true
}

func (s *Store) PlanConnectorDryRun(c *ui.Context, conn client.ConnectorView) {
	if conn.DryRun == nil {
		return
	}
	reqID := s.nextRequestID("conn-plan")
	input := client.ConnectorDryRunPlanInput{
		RequestID:      reqID,
		ExpectedDryRun: *conn.DryRun,
		TargetDryRun:   !*conn.DryRun,
	}
	platform := conn.Platform
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		plan, err := cli.PlanConnectorDryRun(connCtx, platform, input)
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "生成演练切换计划失败: "+err.Error())
				return
			}
			s.LastError = ""
			s.DryRunPlan = &plan
			s.DryRunConfirm = false
			s.DryRunModalOpen = true
		})
	})
}

func (s *Store) ApplyConnectorDryRun(c *ui.Context) {
	if s.DryRunPlan == nil || !s.DryRunConfirm {
		return
	}
	plan := *s.DryRunPlan
	reqID := s.nextRequestID("conn-apply")
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		res, err := cli.ApplyConnectorDryRun(connCtx, plan.Platform, client.ConnectorDryRunApplyInput{
			RequestID: reqID, PlanID: plan.ID, Token: plan.Token, Confirm: true,
		})
		var conns []client.ConnectorView
		var arch *client.ArchitectureSnapshot
		if err == nil {
			if cResp, e2 := cli.ListConnectors(connCtx); e2 == nil {
				conns = cResp.Data
			}
			if aResp, e3 := cli.GetArchitecture(connCtx); e3 == nil {
				arch = &aResp
			}
		}
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "应用演练模式失败: "+err.Error())
				return
			}
			s.LastError = ""
			s.LastDryRunResult = &res
			s.DryRunModalOpen = false
			s.DryRunPlan = nil
			s.DryRunConfirm = false
			if conns != nil {
				s.Connectors = conns
			}
			if arch != nil {
				s.Arch = arch
			}
			s.showToast(c, fmt.Sprintf("%s 演练模式已更新 (%v) 并通过回读校验", components.PlatformLabel(res.Platform), res.DryRun))
		})
	})
}

func mustRaw(v any) json.RawMessage {
	raw, _ := json.Marshal(v)
	return raw
}

func (s *Store) buildGroupEdits(g client.GroupView) ([]client.Edit, error) {
	var edits []client.Edit
	addField := func(field string, val any) {
		if f, ok := g.Fields[field]; ok && !f.Editable {
			return
		}
		edits = append(edits, client.Edit{
			Kind:  "field",
			Scope: client.Scope{Kind: "group", GroupID: g.ID},
			Field: field,
			Value: mustRaw(val),
		})
	}
	addField("enabled", s.Draft.Enabled)
	addField("dry_run", s.Draft.DryRun)
	addField("mention_only", s.Draft.MentionOnly)

	if strings.TrimSpace(s.Draft.MaxReplyRunes) != "" {
		n, err := strconv.Atoi(strings.TrimSpace(s.Draft.MaxReplyRunes))
		if err != nil || n < 0 || n > 2000 {
			return nil, fmt.Errorf("max_reply_runes 必须为 0 至 2000 的整数")
		}
		addField("max_reply_runes", n)
	}
	if strings.TrimSpace(s.Draft.BatchSize) != "" {
		n, err := strconv.Atoi(strings.TrimSpace(s.Draft.BatchSize))
		if err != nil || n < 0 || n > 50 {
			return nil, fmt.Errorf("batch_size 必须为 0 至 50 的整数")
		}
		addField("batch_size", n)
	}
	if strings.TrimSpace(s.Draft.DecisionThreshold) != "" {
		f, err := strconv.ParseFloat(strings.TrimSpace(s.Draft.DecisionThreshold), 64)
		if err != nil || (f != 0 && (f < 0.5 || f > 1.0)) {
			return nil, fmt.Errorf("decision_threshold 必须为 0 或 0.5 至 1.0 之间的数值")
		}
		addField("decision_threshold", f)
	}

	scenVal := strings.TrimSpace(s.Draft.Scenario)
	if scenVal == unsetScenarioLabel {
		scenVal = ""
	}
	addField("scenario", scenVal)
	addField("response_goal", s.Draft.ResponseGoal)
	addField("tone", s.Draft.Tone)

	addField("self_sender_ids", s.Draft.SelfSenderIDs)
	addField("blocked_sender_ids", s.Draft.BlockedSenderIDs)
	addField("keyword_blocklist", s.Draft.KeywordBlocklist)
	addField("regex_blocklist", s.Draft.RegexBlocklist)
	addField("promo_keywords", s.Draft.PromoKeywords)
	addField("skills", s.Draft.Skills)
	addField("tools", s.Draft.Tools)
	addField("knowledge_bases", s.Draft.KnowledgeBases)

	var atVal client.TriStateIntList
	switch s.Draft.AllowedTypesMode {
	case 0:
		atVal = client.TriIntsNull()
	case 1:
		atVal = client.TriIntsEmpty()
	default:
		var nums []int
		for _, tok := range strings.Split(s.Draft.AllowedTypesText, ",") {
			tok = strings.TrimSpace(tok)
			if tok == "" {
				continue
			}
			n, err := strconv.Atoi(tok)
			if err != nil || n < 0 {
				return nil, fmt.Errorf("allowed_types 项 %q 必须为非负整数", tok)
			}
			nums = append(nums, n)
		}
		atVal = client.TriIntsValues(nums...)
	}
	addField("allowed_types", atVal)
	return edits, nil
}

// PlanGroupDraft computes a ChangePlan for the currently selected container.
func (s *Store) PlanGroupDraft(c *ui.Context) {
	g, ok := s.SelectedGroup()
	if !ok {
		return
	}
	edits, err := s.buildGroupEdits(g)
	if err != nil {
		s.LastError = err.Error()
		s.showToast(c, err.Error())
		return
	}
	input := client.PlanInput{
		RequestID:     s.nextRequestID("plan"),
		Base:          g.Versions,
		SchemaVersion: client.SchemaVersion,
		Edits:         edits,
	}
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		plan, err := cli.PlanChange(connCtx, input)
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "生成变更计划失败: "+err.Error())
				return
			}
			s.LastError = ""
			s.ActivePlan = &plan
			s.ActivePlanIsRelay = false
			s.PublishConflict = ""
			s.ChangePlanModalOpen = true
		})
	})
}

// PlanGlobalRulesDraft computes a ChangePlan for editable global dispatcher fields.
func (s *Store) PlanGlobalRulesDraft(c *ui.Context) {
	if s.GlobalRules == nil {
		return
	}
	input := client.PlanInput{
		RequestID:     s.nextRequestID("gplan"),
		Base:          s.GlobalRules.Data.Versions,
		SchemaVersion: client.SchemaVersion,
		Edits: []client.Edit{
			{Kind: "field", Scope: client.Scope{Kind: "global"}, Field: "dry_run", Value: mustRaw(s.GlobalDryRun)},
			{Kind: "field", Scope: client.Scope{Kind: "global"}, Field: "laya_filter", Value: mustRaw(s.GlobalLayaFilter)},
			{Kind: "field", Scope: client.Scope{Kind: "global"}, Field: "bypass_decision_on_mention", Value: mustRaw(s.GlobalBypassMention)},
		},
	}
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		plan, err := cli.PlanChange(connCtx, input)
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "生成全局规则计划失败: "+err.Error())
				return
			}
			s.LastError = ""
			s.ActivePlan = &plan
			s.ActivePlanIsRelay = false
			s.PublishConflict = ""
			s.ChangePlanModalOpen = true
		})
	})
}

// PlanRelayDraft computes a ChangePlan for relay filter toggles via /rules/relay/plan.
func (s *Store) PlanRelayDraft(c *ui.Context) {
	if s.RelayRules == nil {
		return
	}
	input := client.PlanInput{
		RequestID:     s.nextRequestID("rplan"),
		Base:          s.RelayRules.Data.Versions,
		SchemaVersion: client.SchemaVersion,
		Edits: []client.Edit{
			{Kind: "relay_field", Scope: client.Scope{Kind: "relay_filters"}, Field: "clean_content", Value: mustRaw(s.RelayCleanContent)},
			{Kind: "relay_field", Scope: client.Scope{Kind: "relay_filters"}, Field: "ignore_echo", Value: mustRaw(s.RelayIgnoreEcho)},
			{Kind: "relay_field", Scope: client.Scope{Kind: "relay_group_control"}, Field: "monitor_all", Value: mustRaw(s.RelayMonitorAll)},
		},
	}
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		plan, err := cli.PlanRelayRules(connCtx, input)
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "生成中继规则计划失败: "+err.Error())
				return
			}
			s.LastError = ""
			s.ActivePlan = &plan
			s.ActivePlanIsRelay = true
			s.PublishConflict = ""
			s.ChangePlanModalOpen = true
		})
	})
}

func (s *Store) TargetContainerOptions() []string {
	var wechatOpts []string
	var otherOpts []string
	for _, g := range s.Groups {
		label := fmt.Sprintf("%s (%s)", s.ContainerDisplayName(g.ID, g.Group.Label), g.ID)
		if strings.HasSuffix(g.ID, "@chatroom") {
			wechatOpts = append(wechatOpts, label)
		} else {
			otherOpts = append(otherOpts, label)
		}
	}
	return append(wechatOpts, otherOpts...)
}

func (s *Store) syncForwardTargetDefaults() {
	opts := s.TargetContainerOptions()
	if len(opts) == 0 {
		return
	}
	inOpts := func(val string) bool {
		for _, opt := range opts {
			if opt == val {
				return true
			}
		}
		return false
	}
	if !inOpts(s.NewFwdTargetLabel) {
		s.NewFwdTargetLabel = opts[0]
	}
	if !inOpts(s.DirectTargetLabel) {
		s.DirectTargetLabel = opts[0]
	}
}

func (s *Store) resolveTargetContainerID(label string) string {
	trimmed := strings.TrimSpace(label)
	if trimmed == "" {
		return ""
	}
	for _, g := range s.Groups {
		opt := fmt.Sprintf("%s (%s)", s.ContainerDisplayName(g.ID, g.Group.Label), g.ID)
		if trimmed == opt || trimmed == g.ID {
			return g.ID
		}
	}
	if idx := strings.LastIndex(trimmed, "("); idx >= 0 && strings.HasSuffix(trimmed, ")") {
		return strings.TrimSpace(trimmed[idx+1 : len(trimmed)-1])
	}
	return trimmed
}

func (s *Store) KnownSourceContainers() []string {
	seen := map[string]bool{}
	var out []string
	add := func(id string) {
		if id == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, id)
	}
	for _, conn := range s.Connectors {
		for _, t := range conn.Targets {
			add(t.ContainerID)
		}
	}
	for _, g := range s.Groups {
		if !strings.HasSuffix(g.ID, "@chatroom") {
			add(g.ID)
		}
	}
	return out
}

func (s *Store) AddForwardRuleDraft(c *ui.Context) {
	src := strings.TrimSpace(s.NewFwdSource)
	dst := s.resolveTargetContainerID(s.NewFwdTargetLabel)
	if src == "" {
		s.showToast(c, "请填写来源容器 ID（例如 v2ex:t-100001）")
		return
	}
	if dst == "" {
		s.showToast(c, "请选择已配置路由的目标容器")
		return
	}
	if src == dst {
		s.showToast(c, "来源容器与目标容器不能相同")
		return
	}
	id := strings.TrimSpace(s.NewFwdID)
	if id == "" {
		n := len(s.ForwardRulesDraft) + 1
		for {
			candidate := fmt.Sprintf("fwd-%d", n)
			dup := false
			for _, r := range s.ForwardRulesDraft {
				if r.ID == candidate {
					dup = true
					break
				}
			}
			if !dup {
				id = candidate
				break
			}
			n++
		}
	}
	for _, r := range s.ForwardRulesDraft {
		if r.ID == id {
			s.showToast(c, fmt.Sprintf("规则 ID %q 已存在", id))
			return
		}
	}
	s.ForwardRulesDraft = append(s.ForwardRulesDraft, client.ForwardRule{
		ID:                id,
		Enabled:           s.NewFwdEnabled,
		SourceContainerID: src,
		TargetContainerID: dst,
		DryRun:            s.NewFwdDryRun,
		IncludeEcho:       s.NewFwdIncludeEcho,
		Template:          s.NewFwdTemplate,
	})
	s.ForwardRulesDirty = true
	s.NewFwdID = ""
	s.NewFwdSource = ""
	s.NewFwdTemplate = ""
	s.showToast(c, fmt.Sprintf("已添加规则 %s 到草稿列表，请点击预览并发布", id))
}

func (s *Store) RemoveForwardRuleDraft(idx int) {
	if idx < 0 || idx >= len(s.ForwardRulesDraft) {
		return
	}
	s.ForwardRulesDraft = append(s.ForwardRulesDraft[:idx], s.ForwardRulesDraft[idx+1:]...)
	s.ForwardRulesDirty = true
}

func (s *Store) PlanForwardRulesDraft(c *ui.Context) {
	if s.GlobalRules == nil {
		s.showToast(c, "全局规则暂未加载，请先刷新控制台")
		return
	}
	rules := s.ForwardRulesDraft
	if rules == nil {
		rules = []client.ForwardRule{}
	}
	input := client.PlanInput{
		RequestID:     s.nextRequestID("fwdplan"),
		Base:          s.GlobalRules.Data.Versions,
		SchemaVersion: client.SchemaVersion,
		Edits: []client.Edit{
			{Kind: "field", Scope: client.Scope{Kind: "global"}, Field: "forward_rules", Value: mustRaw(rules)},
		},
	}
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		plan, err := cli.PlanChange(connCtx, input)
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "生成直通转发变更计划失败: "+err.Error())
				return
			}
			s.LastError = ""
			s.ActivePlan = &plan
			s.ActivePlanIsRelay = false
			s.PublishConflict = ""
			s.ChangePlanModalOpen = true
		})
	})
}

func (s *Store) SendDirectDeliver(c *ui.Context) {
	targetID := s.resolveTargetContainerID(s.DirectTargetLabel)
	text := strings.TrimSpace(s.DirectText)
	if targetID == "" {
		s.showToast(c, "请选择目标微信群或容器")
		return
	}
	if text == "" {
		s.showToast(c, "请输入要直发的文本内容")
		return
	}
	input := client.DirectDeliverInput{
		TargetContainerID: targetID,
		Text:              text,
		IdempotencyKey:    s.nextRequestID("direct"),
		Source:            "console",
		DryRun:            s.DirectDryRun,
	}
	rf := s.currentRunFilter()
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		res, err := cli.DirectDeliver(connCtx, input)
		var runsPage *client.Envelope[client.RunPage]
		if err == nil {
			if page, rErr := cli.ListRuns(connCtx, rf); rErr == nil {
				runsPage = &page
			}
		}
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "直发请求失败: "+err.Error())
				return
			}
			s.LastError = ""
			s.LastDirectResult = &res
			if runsPage != nil {
				s.RunsPage = runsPage
			}
			s.DirectText = ""
			if res.DryRun {
				s.showToast(c, fmt.Sprintf("直发演练已拦截（%s）", components.ShortReasonLabel(res.ReasonCode)))
			} else {
				s.showToast(c, fmt.Sprintf("直发投递已提交（%s）", components.DeliveryLabel(res.Delivery)))
			}
		})
	})
}

// PublishActivePlan publishes the reviewed ChangePlan and surfaces 409 conflicts safely.
func (s *Store) PublishActivePlan(c *ui.Context) {
	if s.ActivePlan == nil {
		return
	}
	plan := *s.ActivePlan
	reqID := s.nextRequestID("pub")
	connCtx, cli, epoch := s.ensureConn()
	rf := s.currentRunFilter()
	cs := components.ChangeStatusFilterValue(s.ChangeStatusFilter)
	gid := s.SelectedGroupID
	s.Busy = true
	s.runOp(func() {
		op, err := cli.PublishChange(connCtx, client.PublishInput{
			RequestID: reqID, PlanID: plan.ID, Token: plan.Token,
		})
		var b loadedBundle
		if err == nil {
			b = s.fetchBundle(connCtx, cli, rf, cs, gid)
		}
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				var apiErr *client.APIError
				if errors.As(err, &apiErr) && (apiErr.Status == 409 || (apiErr.Problem != nil && (apiErr.Problem.Code == "conflict" || apiErr.Problem.Code == "stale"))) {
					s.PublishConflict = err.Error()
					s.LastError = err.Error()
					return
				}
				s.LastError = err.Error()
				s.showToast(c, "发布变更失败: "+err.Error())
				return
			}
			s.Draft.Dirty = false
			s.GlobalRelayDirty = false
			s.ForwardRulesDirty = false
			s.applyBundle(b)
			s.LastOperation = &op
			s.ChangePlanModalOpen = false
			s.ActivePlan = nil
			s.PublishConflict = ""
			s.showToast(c, fmt.Sprintf("已发布变更 %s (%s)", op.ChangeID, components.HealthLabel(op.Status)))
		})
	})
}

func (s *Store) AcknowledgeAttention(c *ui.Context, item client.AttentionItem) {
	reqID := s.nextRequestID("ack")
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		updated, err := cli.AcknowledgeAttention(connCtx, client.AcknowledgeInput{
			RequestID:        reqID,
			ExpectedRevision: item.Revision,
			AttentionID:      item.ID,
		})
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "确认告警失败: "+err.Error())
				return
			}
			if s.Overview != nil {
				for i := range s.Overview.Data.Attention {
					if s.Overview.Data.Attention[i].ID == item.ID {
						s.Overview.Data.Attention[i] = updated.Data
					}
				}
			}
			s.showToast(c, "已确认 "+components.AttentionCodeLabel(item.Code))
		})
	})
}

func (s *Store) currentRunFilter() client.RunFilter {
	f := client.RunFilter{
		GroupID: strings.TrimSpace(s.RunGroupFilter),
		Query:   strings.TrimSpace(s.RunQuery),
		Limit:   50,
	}
	if ph := components.RunPhaseFilterValue(s.RunPhaseFilter); ph != "" {
		f.Phases = []string{ph}
	}
	if oc := components.RunOutcomeFilterValue(s.RunOutcomeFilter); oc != "" {
		f.Outcomes = []string{oc}
	}
	if dv := components.RunDeliveryFilterValue(s.RunDeliveryFilter); dv != "" {
		f.Deliveries = []string{dv}
	}
	return f
}

func (s *Store) ApplyRunFilter(c *ui.Context, cursor string) {
	f := s.currentRunFilter()
	f.Cursor = cursor
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		rp, err := cli.ListRuns(connCtx, f)
		var detail *client.Envelope[client.RunDetail]
		if err == nil && len(rp.Data.Items) > 0 {
			if d, e2 := cli.GetRun(connCtx, rp.Data.Items[0].ID); e2 == nil {
				detail = &d
			}
		}
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				return
			}
			s.RunsPage = &rp
			s.SelectedRunIdx = clampIndex(0, len(rp.Data.Items))
			s.SelectedRun = detail
			s.PendingSSEUpdates = false
		})
	})
}

func (s *Store) OpenRunDrawer(id string) {
	s.RunDrawerOpen = true
	if s.RunsPage != nil && (s.SelectedRun == nil || s.SelectedRun.Data.Run.ID != id) {
		for _, r := range s.RunsPage.Data.Items {
			if r.ID == id {
				s.SelectedRun = &client.Envelope[client.RunDetail]{
					Data: client.RunDetail{Run: r},
				}
				break
			}
		}
	}
	s.SelectRunByID(id)
}

func (s *Store) OpenChangeDrawer(idx int) {
	if s.ChangesPage != nil && idx >= 0 && idx < len(s.ChangesPage.Data.Items) {
		rec := s.ChangesPage.Data.Items[idx]
		if s.SelectedChange == nil || s.SelectedChange.Data.ID != rec.ID {
			s.SelectedChange = &client.Envelope[client.ChangeRecord]{
				Data:     rec,
				Revision: s.ChangesPage.Revision,
			}
		}
	}
	s.ChangeDrawerOpen = true
	s.SelectChangeByIndex(idx)
}

func (s *Store) ApplyRunPreset(c *ui.Context, preset string) {
	s.RunPreset = preset
	s.RunHideBlocked = false
	switch preset {
	case "已发送回复":
		s.RunPhaseFilter = "全部"
		s.RunOutcomeFilter = "已回复"
		s.RunDeliveryFilter = "全部"
	case "待核对":
		s.RunPhaseFilter = "全部"
		s.RunOutcomeFilter = "全部"
		s.RunDeliveryFilter = "未知"
	case "已拦截":
		s.RunPhaseFilter = "全部"
		s.RunOutcomeFilter = "已拦截"
		s.RunDeliveryFilter = "全部"
	default:
		s.RunPreset = "全部"
		s.RunPhaseFilter = "全部"
		s.RunOutcomeFilter = "全部"
		s.RunDeliveryFilter = "全部"
	}
	s.ApplyRunFilter(c, "")
}

func (s *Store) ResetRunFilters(c *ui.Context) {
	s.RunPreset = "全部"
	s.RunPhaseFilter = "全部"
	s.RunOutcomeFilter = "全部"
	s.RunDeliveryFilter = "全部"
	s.RunGroupFilter = ""
	s.RunGroupFilterLabel = "全部容器"
	s.RunQuery = ""
	s.RunHideBlocked = false
	s.ApplyRunFilter(c, "")
}

func (s *Store) HasActiveRunFilters() bool {
	return (s.RunPreset != "" && s.RunPreset != "全部") ||
		(s.RunPhaseFilter != "" && s.RunPhaseFilter != "全部") ||
		(s.RunOutcomeFilter != "" && s.RunOutcomeFilter != "全部") ||
		(s.RunDeliveryFilter != "" && s.RunDeliveryFilter != "全部") ||
		strings.TrimSpace(s.RunGroupFilter) != "" ||
		strings.TrimSpace(s.RunQuery) != "" ||
		s.RunHideBlocked
}

func (s *Store) SelectRunByID(id string) {
	if s.RunsPage != nil {
		for i, r := range s.RunsPage.Data.Items {
			if r.ID == id {
				s.SelectedRunIdx = i
			}
		}
	}
	connCtx, cli, epoch := s.ensureConn()
	s.runOp(func() {
		detail, err := cli.GetRun(connCtx, id)
		s.dispatchEpoch(epoch, func() {
			if err != nil {
				s.LastError = err.Error()
				return
			}
			s.SelectedRun = &detail
		})
	})
}

func (s *Store) OpenConfigSnapshot(c *ui.Context, snapID string) {
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		env, err := cli.GetConfigSnapshot(connCtx, snapID)
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "加载历史快照失败: "+err.Error())
				return
			}
			s.ActiveSnapshot = env.Data
			if env.MissingReason != nil {
				s.SnapshotMissingReason = *env.MissingReason
			} else if env.Data == nil {
				s.SnapshotMissingReason = "历史配置快照未留存"
			} else {
				s.SnapshotMissingReason = ""
			}
			s.SnapshotModalOpen = true
		})
	})
}

func (s *Store) CancelRun(c *ui.Context, run client.RunView, mode string) {
	expectedRev := ""
	if s.Controls != nil {
		expectedRev = s.Controls.Revision
	}
	reqID := s.nextRequestID("cancel-run")
	rf := s.currentRunFilter()
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		_, err := cli.RunControlAction(connCtx, client.ControlRequest{
			RequestID:        reqID,
			ExpectedRevision: expectedRev,
			Action: client.ControlAction{
				Kind:       "cancel_run",
				Scope:      client.ControlScope{Kind: "group", GroupID: run.Source.ID},
				RunID:      run.ID,
				CancelMode: mode,
			},
		})
		detail, _ := cli.GetRun(connCtx, run.ID)
		rp, _ := cli.ListRuns(connCtx, rf)
		ctl, _ := cli.GetControls(connCtx)
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "运行控制执行失败: "+err.Error())
				return
			}
			s.SelectedRun = &detail
			s.RunsPage = &rp
			s.Controls = &ctl
			s.showToast(c, "已应用运行控制指令: "+mode)
		})
	})
}

func (s *Store) PlanServiceAction(c *ui.Context, serviceID, kind string) {
	expectedRev := ""
	if s.RuntimeSvcs != nil {
		expectedRev = s.RuntimeSvcs.Revision
	}
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		plan, err := cli.PlanServiceAction(connCtx, client.ServicePlanInput{
			ExpectedRevision: expectedRev,
			Kind:             kind,
			ServiceID:        serviceID,
		})
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "生成服务计划失败: "+err.Error())
				return
			}
			s.ActiveServicePlan = &plan
			s.ServicePlanModalOpen = true
		})
	})
}

func (s *Store) ExecuteServicePlan(c *ui.Context) {
	if s.ActiveServicePlan == nil || !s.ActiveServicePlan.Eligible {
		return
	}
	plan := *s.ActiveServicePlan
	reqID := s.nextRequestID("svc-exec")
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		op, err := cli.ExecuteServiceAction(connCtx, client.ServiceExecuteInput{
			PlanID: plan.ID, Token: plan.Token, RequestID: reqID,
		})
		ctl, _ := cli.GetControls(connCtx)
		svcs, _ := cli.ListServices(connCtx)
		rt, _ := cli.GetRuntimeServices(connCtx)
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "执行服务操作失败: "+err.Error())
				return
			}
			s.LastServiceOp = &op
			s.ServicePlanModalOpen = false
			s.ActiveServicePlan = nil
			s.Controls = &ctl
			s.Services = svcs.Data
			s.RuntimeSvcs = &rt
			s.showToast(c, fmt.Sprintf("%s %s (%s)", components.ServiceDisplayName(op.ServiceID), serviceActionCN(op.Kind), components.HealthLabel(op.State)))
		})
	})
}

func (s *Store) MaintainAction(c *ui.Context, id string, cont bool) {
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		var op client.ServiceOperation
		var err error
		if cont {
			op, err = cli.ContinueMaintenance(connCtx, id)
		} else {
			op, err = cli.CancelMaintenance(connCtx, id)
		}
		ctl, _ := cli.GetControls(connCtx)
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "维护操作执行失败: "+err.Error())
				return
			}
			s.LastServiceOp = &op
			s.Controls = &ctl
			s.showToast(c, "维护任务状态已更新: "+components.HealthLabel(op.State))
		})
	})
}

func (s *Store) SelectChangeByIndex(idx int) {
	if s.ChangesPage == nil || idx < 0 || idx >= len(s.ChangesPage.Data.Items) {
		return
	}
	s.SelectedChangeIdx = idx
	id := s.ChangesPage.Data.Items[idx].ID
	connCtx, cli, epoch := s.ensureConn()
	s.runOp(func() {
		rec, err := cli.GetChange(connCtx, id)
		s.dispatchEpoch(epoch, func() {
			if err == nil {
				s.SelectedChange = &rec
			}
		})
	})
}

func (s *Store) FilterChanges(c *ui.Context) {
	status := components.ChangeStatusFilterValue(s.ChangeStatusFilter)
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		cp, err := cli.ListChanges(connCtx, client.ChangeFilter{Status: status, Limit: 50})
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				return
			}
			s.ChangesPage = &cp
			s.SelectedChangeIdx = clampIndex(0, len(cp.Data.Items))
			if len(cp.Data.Items) > 0 {
				rec := cp.Data.Items[0]
				s.SelectedChange = &client.Envelope[client.ChangeRecord]{Data: rec, Revision: cp.Revision}
			} else {
				s.SelectedChange = nil
			}
		})
	})
}

func (s *Store) PlanRollback(c *ui.Context, change client.ChangeRecord) {
	var base client.Versions
	if s.GlobalRules != nil {
		base = s.GlobalRules.Data.Versions
	} else {
		base = change.Base
	}
	reqID := s.nextRequestID("rb")
	connCtx, cli, epoch := s.ensureConn()
	s.Busy = true
	s.runOp(func() {
		plan, err := cli.PlanRollback(connCtx, client.RollbackInput{
			RequestID: reqID,
			ChangeID:  change.ID,
			Base:      base,
		})
		s.dispatchEpoch(epoch, func() {
			s.Busy = false
			if err != nil {
				s.LastError = err.Error()
				s.showToast(c, "生成回滚计划失败: "+err.Error())
				return
			}
			s.ActivePlan = &plan
			s.PublishConflict = ""
			s.ChangePlanModalOpen = true
		})
	})
}

// CatalogOptions helpers keep selectors safely populated.
func (s *Store) ScenarioOptions(current string) []string {
	seen := map[string]bool{}
	out := []string{unsetScenarioLabel}
	seen[""] = true
	seen[unsetScenarioLabel] = true
	if cur := strings.TrimSpace(current); cur != "" && !seen[cur] {
		out = append(out, cur)
		seen[cur] = true
	}
	if s.Catalog != nil {
		for _, item := range s.Catalog.Data.Scenarios {
			if !seen[item.ID] {
				out = append(out, item.ID)
				seen[item.ID] = true
			}
		}
	}
	sort.Strings(out[1:])
	return out
}

func (s *Store) CatalogSkillIDs() []string {
	if s.Catalog == nil {
		return nil
	}
	out := make([]string, 0, len(s.Catalog.Data.Skills))
	for _, sk := range s.Catalog.Data.Skills {
		out = append(out, sk.ID)
	}
	return out
}

func (s *Store) CatalogToolIDs() []string {
	if s.Catalog == nil {
		return nil
	}
	out := make([]string, 0, len(s.Catalog.Data.Tools))
	for _, t := range s.Catalog.Data.Tools {
		out = append(out, t.ID)
	}
	return out
}

func (s *Store) CatalogKBIDs() []string {
	if s.Catalog == nil {
		return nil
	}
	out := make([]string, 0, len(s.Catalog.Data.KnowledgeBases))
	for _, kb := range s.Catalog.Data.KnowledgeBases {
		out = append(out, kb.ID)
	}
	return out
}

// ContainerDisplayName resolves the best human-readable container name without leaking raw hashes/IDs.
func (s *Store) ContainerDisplayName(id, fallbackLabel string) string {
	id = strings.TrimSpace(id)
	fb := strings.TrimSpace(fallbackLabel)
	if fb != "" && fb != id {
		return fb
	}
	for _, g := range s.Groups {
		if g.ID == id {
			if lbl := strings.TrimSpace(g.Group.Label); lbl != "" && lbl != id {
				return lbl
			}
			if uiLbl := strings.TrimSpace(g.UITargetLabel); uiLbl != "" && uiLbl != id {
				return uiLbl
			}
		}
	}
	for _, conn := range s.Connectors {
		for _, t := range conn.Targets {
			if t.ContainerID == id && t.Label != nil {
				if lbl := strings.TrimSpace(*t.Label); lbl != "" && lbl != id {
					return lbl
				}
			}
		}
	}
	return components.ShortContainerID(id)
}

// ContainerPlatform identifies the platform tag ("微信", "B站", "网页", etc.) for a container ID.
func (s *Store) ContainerPlatform(id string) string {
	trimmed := strings.TrimSpace(id)
	switch {
	case strings.HasPrefix(trimmed, "bilibili:"):
		return "B站"
	case strings.HasPrefix(trimmed, "web:"):
		return "网页"
	case strings.HasSuffix(trimmed, "@chatroom"):
		return "微信"
	}
	for _, conn := range s.Connectors {
		for _, t := range conn.Targets {
			if t.ContainerID == trimmed {
				return components.PlatformLabel(conn.Platform)
			}
		}
	}
	if strings.Contains(trimmed, "@") {
		return "微信"
	}
	return "容器"
}

// ContainerDisplayWithPlatform combines resolved name and platform tag.
func (s *Store) ContainerDisplayWithPlatform(id, fallbackLabel string) string {
	name := s.ContainerDisplayName(id, fallbackLabel)
	plat := s.ContainerPlatform(id)
	if name == "" || name == "—" {
		return plat
	}
	return fmt.Sprintf("%s（%s）", name, plat)
}

// RunGroupFilterOptions builds dropdown labels mapped back to exact group IDs.
func (s *Store) RunGroupFilterOptions() ([]string, map[string]string) {
	options := []string{"全部容器"}
	labelToID := map[string]string{"全部容器": ""}
	seenIDs := map[string]bool{}
	usedLabels := map[string]bool{"全部容器": true}

	addGroup := func(id, labelHint string) {
		id = strings.TrimSpace(id)
		if id == "" || seenIDs[id] {
			return
		}
		seenIDs[id] = true
		label := s.ContainerDisplayWithPlatform(id, labelHint)
		if usedLabels[label] {
			label = fmt.Sprintf("%s [%s]", label, components.ShortContainerID(id))
		}
		usedLabels[label] = true
		options = append(options, label)
		labelToID[label] = id
	}

	for _, g := range s.Groups {
		addGroup(g.ID, g.Group.Label)
	}
	if s.RunsPage != nil {
		for _, r := range s.RunsPage.Data.Items {
			ref := runContainerRef(r)
			addGroup(ref.ID, ref.Label)
		}
	}
	return options, labelToID
}

// SyncRunGroupFilterFromLabel keeps RunGroupFilter and RunGroupFilterLabel consistent.
func (s *Store) SyncRunGroupFilterFromLabel() []string {
	options, labelToID := s.RunGroupFilterOptions()
	if id, ok := labelToID[s.RunGroupFilterLabel]; ok {
		s.RunGroupFilter = id
		return options
	}
	if strings.TrimSpace(s.RunGroupFilter) != "" {
		for _, opt := range options {
			if labelToID[opt] == s.RunGroupFilter {
				s.RunGroupFilterLabel = opt
				return options
			}
		}
	}
	s.RunGroupFilterLabel = "全部容器"
	s.RunGroupFilter = ""
	return options
}

func runContainerRef(run client.RunView) client.GroupRef {
	if strings.TrimSpace(run.Target.ID) != "" {
		return run.Target
	}
	return run.Source
}

func runMessageCount(run client.RunView) int {
	if run.MessageCount != nil && *run.MessageCount > 0 {
		return *run.MessageCount
	}
	if len(run.Inputs) > 0 {
		return len(run.Inputs)
	}
	if len(run.MessageIDs) > 0 {
		return len(run.MessageIDs)
	}
	return 0
}

func nonEmptyOr(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return v
}
