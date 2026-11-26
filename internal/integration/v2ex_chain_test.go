package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/connector"
	"jinx/internal/connector/v2ex"
	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	"jinx/internal/observability"
	relayconfig "jinx/internal/relay/config"
	"jinx/internal/relay/filter"
	"jinx/internal/relay/hub"
	"jinx/internal/relay/web"
)

const (
	v2exContainer = "v2ex:t-100001"
	v2exSelfUser  = "demov2exbot"
)

type fakeV2EXChainDriver struct {
	mu       sync.Mutex
	ready    bool
	reason   string
	viewer   string
	comments []v2ex.Comment
	replies  []v2ex.ReplyRequest
	postFunc func(ctx context.Context, req v2ex.ReplyRequest) (v2ex.ReplyResult, error)
}

func (f *fakeV2EXChainDriver) Health(context.Context) (bool, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready, f.reason
}

func (f *fakeV2EXChainDriver) FetchComments(context.Context, v2ex.TargetConfig) ([]v2ex.Comment, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]v2ex.Comment, len(f.comments))
	copy(out, f.comments)
	return out, f.viewer, nil
}

func (f *fakeV2EXChainDriver) PostReply(ctx context.Context, req v2ex.ReplyRequest) (v2ex.ReplyResult, error) {
	f.mu.Lock()
	f.replies = append(f.replies, req)
	fn := f.postFunc
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return v2ex.ReplyResult{
		Status:     "confirmed",
		Submitted:  connector.BoolPtr(true),
		ExternalID: "18339001",
	}, nil
}

func (f *fakeV2EXChainDriver) setComments(comments ...v2ex.Comment) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.comments = append([]v2ex.Comment(nil), comments...)
}

func (f *fakeV2EXChainDriver) snapshotReplies() []v2ex.ReplyRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]v2ex.ReplyRequest, len(f.replies))
	copy(out, f.replies)
	return out
}

type v2exChainStack struct {
	connector  *v2ex.Connector
	dispatcher *dispatcher.Dispatcher
	dispServer *httptest.Server
	client     *http.Client
}

type v2exStackOpts struct {
	connectorDryRun bool
	endpointDryRun  bool
	autoMention     *bool
	batchSize       int
	runner          dispatcher.Runner
	driver          *fakeV2EXChainDriver
}

func startV2EXChainStack(t *testing.T, opts v2exStackOpts) *v2exChainStack {
	t.Helper()
	root := t.TempDir()
	client := &http.Client{Timeout: 10 * time.Second}

	rCfg := relayconfig.Config{
		Server: relayconfig.ServerConfig{ListenAddr: "127.0.0.1:0"},
		GroupControl: relayconfig.GroupControl{
			MonitoredGroups: []string{v2exContainer},
			ExposedGroups:   []string{v2exContainer},
			GroupAliases:    map[string]string{},
		},
		Filters: relayconfig.FilterConfig{
			AllowedTypes:     []int{1},
			KeywordBlocklist: []string{},
			RegexBlocklist:   []string{},
			SenderBlocklist:  []string{},
		},
	}
	rPath := filepath.Join(root, "relay.json")
	writeJSON(t, rPath, rCfg)
	manager, err := relayconfig.NewManager(rPath)
	if err != nil {
		t.Fatal(err)
	}
	relayCore := web.NewServer(manager, hub.NewHub(100), filter.NewEngine())
	relayHandler, err := relayCore.Handler()
	if err != nil {
		t.Fatal(err)
	}
	relaySrv := httptest.NewServer(relayHandler)
	t.Cleanup(relaySrv.Close)

	autoMention := opts.autoMention
	if autoMention == nil {
		autoMention = connector.BoolPtr(true)
	}
	connCfg := v2ex.Config{
		ListenAddr:       "127.0.0.1:0",
		RelayURL:         relaySrv.URL,
		StateDir:         filepath.Join(root, "v2ex-state"),
		AccountID:        "v2ex-ego",
		SelfUsernames:    []string{v2exSelfUser},
		DryRun:           connector.BoolPtr(opts.connectorDryRun),
		AutoMention:      autoMention,
		PollIntervalSec:  60,
		MaxInitialAgeSec: 300,
		Targets: []v2ex.TargetConfig{
			{
				ContainerID: v2exContainer,
				Label:       "Synthetic V2EX Topic",
				TopicID:     "100001",
				Enabled:     true,
				PollLimit:   20,
			},
		},
	}
	conn, err := v2ex.NewConnector(connCfg, opts.driver)
	if err != nil {
		t.Fatal(err)
	}
	connSrv := httptest.NewServer(conn.Handler())
	t.Cleanup(connSrv.Close)

	batchSize := opts.batchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	var dCfg dispatcher.Config
	dCfg.Server.ListenAddr = "127.0.0.1:0"
	dCfg.Relay.StreamURL = relaySrv.URL + "/api/v1/stream"
	dCfg.ActionEndpoints = map[string]dispatcher.ActionEndpointConfig{
		"v2ex": {
			URL:        connSrv.URL,
			DryRun:     opts.endpointDryRun,
			TimeoutSec: 10,
		},
	}
	dCfg.Hermes.BinPath = "/bin/true"
	dCfg.Hermes.Profile = "v2ex-integration"
	dCfg.Hermes.ProfilesDir = filepath.Join(root, "profiles")
	dCfg.Hermes.TimeoutSec = 10
	dCfg.Hermes.MaxConcurrent = 1
	dCfg.Rules = dispatcher.Rules{
		BatchSize:        batchSize,
		IdleTimeoutSec:   1,
		MaxBatchWaitSec:  2,
		MaxMessageAgeSec: 180,
		SelfSenderIDs:    []string{v2exSelfUser},
		GroupWhitelist:   []string{v2exContainer},
		GroupRules: map[string]dispatcher.GroupRule{
			v2exContainer: {
				Enabled:            true,
				TargetGroupID:      v2exContainer,
				Target:             "Synthetic V2EX Topic",
				HermesProfile:      "v2ex-topic-1",
				MentionOnly:        false,
				AllowedTypes:       []int{1},
				BatchSize:          batchSize,
				IdleTimeoutSec:     1,
				MaxBatchWaitSec:    2,
				ReplyWindowSec:     60,
				MaxRepliesInWindow: 5,
			},
		},
	}
	dPath := filepath.Join(root, "dispatcher.json")
	writeJSON(t, dPath, dCfg)

	d, err := dispatcher.NewDispatcherWithOptions(dPath, dispatcher.Options{Runner: opts.runner})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	dispSrv := httptest.NewServer(d.Handler())
	t.Cleanup(dispSrv.Close)

	d.Start()
	await(t, "dispatcher relay SSE connected", func() bool {
		health := d.HealthSnapshot()
		return health.RelayConnected && health.LastEventID != ""
	})

	return &v2exChainStack{
		connector:  conn,
		dispatcher: d,
		dispServer: dispSrv,
		client:     client,
	}
}

func (s *v2exChainStack) traceSnapshot(t *testing.T) observability.Snapshot {
	t.Helper()
	var snap observability.Snapshot
	requestJSON(t, s.client, http.MethodGet, s.dispServer.URL+"/api/trace/snapshot", nil, &snap)
	return snap
}

func (s *v2exChainStack) awaitRunMatch(t *testing.T, desc string, pred func(contracts.Run) bool) contracts.Run {
	t.Helper()
	var matched contracts.Run
	await(t, desc, func() bool {
		snap := s.traceSnapshot(t)
		for _, run := range snap.Runs {
			if pred(run) {
				matched = run
				return true
			}
		}
		return false
	})
	return matched
}

func (s *v2exChainStack) awaitIdle(t *testing.T) {
	t.Helper()
	await(t, "dispatcher idle", func() bool {
		h := s.dispatcher.HealthSnapshot()
		return h.PendingMessages == 0 && h.ExecutingGroups == 0 && !h.AwaitingEcho
	})
}

func TestV2EXConnectorChain(t *testing.T) {
	t.Run("direct_confirmed_thread_reply", func(t *testing.T) {
		var runnerCalls atomic.Int32
		drv := &fakeV2EXChainDriver{
			ready:  true,
			viewer: v2exSelfUser,
			comments: []v2ex.Comment{
				{
					ReplyID:     "18330001",
					TopicID:     "100001",
					Floor:       1,
					SenderID:    "viewer-alice",
					SenderName:  "viewer-alice",
					Content:     "请问这个方案支持本地部署吗？",
					TimestampMS: time.Now().UnixMilli(),
				},
			},
		}
		runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
			runnerCalls.Add(1)
			return dispatcher.Result{
				Text:      "支持本地纯回环部署。",
				SessionID: "v2ex-sess-1",
				Backend:   "Hermes",
				RunID:     "run-v2ex-1",
			}, nil
		})
		stack := startV2EXChainStack(t, v2exStackOpts{driver: drv, runner: runner})

		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce #1 failed: %v", err)
		}

		run := stack.awaitRunMatch(t, "direct confirmed terminal run", func(r contracts.Run) bool {
			return r.Phase == contracts.PhaseTerminal
		})
		stack.awaitIdle(t)

		if run.Outcome != contracts.OutcomeReplied || run.Delivery != contracts.DeliveryConfirmed {
			t.Fatalf("expected OutcomeReplied/DeliveryConfirmed, got %+v", run)
		}
		if run.Action == nil || run.Action.Platform != v2ex.Platform || run.Action.Kind != contracts.ActionThreadReply {
			t.Fatalf("expected v2ex thread_reply ActionRef, got %+v", run.Action)
		}
		if run.DeliveryEvidence.EchoMessageID != "18339001" || run.DeliveryEvidence.SubmittedAtSource != "bridge_ack" {
			t.Fatalf("unexpected delivery evidence: %+v", run.DeliveryEvidence)
		}
		replies := drv.snapshotReplies()
		if len(replies) != 1 || replies[0].Text != "@viewer-alice 支持本地纯回环部署。" || replies[0].ReplyToID != "18330001" {
			t.Fatalf("unexpected PostReply args: %+v", replies)
		}

		// Subsequent poll with our own posted reply must not re-trigger.
		drv.setComments(
			v2ex.Comment{
				ReplyID:     "18339001",
				TopicID:     "100001",
				Floor:       2,
				SenderID:    v2exSelfUser,
				SenderName:  v2exSelfUser,
				Content:     "@viewer-alice 支持本地纯回环部署。",
				TimestampMS: time.Now().Add(20 * time.Millisecond).UnixMilli(),
				IsSelf:      true,
			},
		)
		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce #2 failed: %v", err)
		}
		time.Sleep(150 * time.Millisecond)
		if snap := stack.traceSnapshot(t); len(snap.Runs) != 1 || runnerCalls.Load() != 1 || len(drv.snapshotReplies()) != 1 {
			t.Fatalf("own reply re-triggered pipeline: runs=%d runnerCalls=%d replies=%d", len(snap.Runs), runnerCalls.Load(), len(drv.snapshotReplies()))
		}
	})

	t.Run("multi_comment_reply_to_selection", func(t *testing.T) {
		drv := &fakeV2EXChainDriver{
			ready:  true,
			viewer: v2exSelfUser,
			comments: []v2ex.Comment{
				{
					ReplyID:     "18330101",
					TopicID:     "100001",
					Floor:       1,
					SenderID:    "viewer-alice",
					SenderName:  "viewer-alice",
					Content:     "请问导出格式支持 JSON 吗？",
					TimestampMS: time.Now().UnixMilli(),
				},
				{
					ReplyID:     "18330102",
					TopicID:     "100001",
					Floor:       2,
					SenderID:    "viewer-bob",
					SenderName:  "viewer-bob",
					Content:     "路过帮顶一下",
					TimestampMS: time.Now().Add(10 * time.Millisecond).UnixMilli(),
				},
			},
		}
		runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
			return dispatcher.Result{
				Text:      "REPLY_TO: 1\n支持导出标准 JSON。",
				SessionID: "v2ex-sess-2",
				Backend:   "Hermes",
				RunID:     "run-v2ex-2",
			}, nil
		})
		stack := startV2EXChainStack(t, v2exStackOpts{driver: drv, runner: runner, batchSize: 2})

		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce failed: %v", err)
		}

		run := stack.awaitRunMatch(t, "multi comment reply_to terminal run", func(r contracts.Run) bool {
			return r.Phase == contracts.PhaseTerminal
		})
		stack.awaitIdle(t)
		if run.Outcome != contracts.OutcomeReplied || run.Delivery != contracts.DeliveryConfirmed {
			t.Fatalf("expected OutcomeReplied/DeliveryConfirmed, got %+v", run)
		}
		replies := drv.snapshotReplies()
		if len(replies) != 1 {
			t.Fatalf("expected 1 PostReply call, got %d", len(replies))
		}
		if replies[0].ReplyToID != "18330101" || replies[0].ReplyToMember != "viewer-alice" || replies[0].Text != "@viewer-alice 支持导出标准 JSON。" {
			t.Fatalf("expected reply targeting #1 viewer-alice, got %+v", replies[0])
		}
	})

	t.Run("async_echo_confirmation", func(t *testing.T) {
		drv := &fakeV2EXChainDriver{
			ready:  true,
			viewer: v2exSelfUser,
			comments: []v2ex.Comment{
				{
					ReplyID:     "18330201",
					TopicID:     "100001",
					Floor:       1,
					SenderID:    "viewer-carol",
					SenderName:  "viewer-carol",
					Content:     "异步回声确认测试",
					TimestampMS: time.Now().UnixMilli(),
				},
			},
			postFunc: func(context.Context, v2ex.ReplyRequest) (v2ex.ReplyResult, error) {
				return v2ex.ReplyResult{
					Status:     "submitted",
					Submitted:  connector.BoolPtr(true),
					ReasonCode: "missing_external_id",
				}, nil
			},
		}
		runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
			return dispatcher.Result{
				Text:      "异步确认回复。",
				SessionID: "v2ex-sess-3",
				Backend:   "Hermes",
				RunID:     "run-v2ex-3",
			}, nil
		})
		stack := startV2EXChainStack(t, v2exStackOpts{
			driver:      drv,
			runner:      runner,
			autoMention: connector.BoolPtr(false),
		})

		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce #1 failed: %v", err)
		}

		submitting := stack.awaitRunMatch(t, "run confirming pending submitted", func(r contracts.Run) bool {
			return r.Phase == contracts.PhaseConfirming && r.Outcome == contracts.OutcomePending && r.Delivery == contracts.DeliverySubmitted
		})
		if submitting.DeliveryEvidence.SubmissionStartedAt == nil {
			t.Fatalf("missing SubmissionStartedAt: %+v", submitting.DeliveryEvidence)
		}

		time.Sleep(25 * time.Millisecond)
		drv.setComments(v2ex.Comment{
			ReplyID:     "18339201",
			TopicID:     "100001",
			Floor:       2,
			SenderID:    v2exSelfUser,
			SenderName:  v2exSelfUser,
			Content:     "异步确认回复。",
			TimestampMS: time.Now().UnixMilli(),
			IsSelf:      true,
		})
		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce #2 failed: %v", err)
		}

		confirmed := stack.awaitRunMatch(t, "async echo confirmed", func(r contracts.Run) bool {
			return r.Phase == contracts.PhaseTerminal
		})
		stack.awaitIdle(t)
		if confirmed.Outcome != contracts.OutcomeReplied || confirmed.Delivery != contracts.DeliveryConfirmed || confirmed.DeliveryEvidence.EchoMessageID != "18339201" {
			t.Fatalf("expected async echo confirmation with EchoMessageID=18339201, got %+v", confirmed)
		}
	})

	t.Run("proven_pre_submission_failure", func(t *testing.T) {
		drv := &fakeV2EXChainDriver{
			ready:  true,
			viewer: v2exSelfUser,
			comments: []v2ex.Comment{
				{
					ReplyID:     "18330301",
					TopicID:     "100001",
					Floor:       1,
					SenderID:    "viewer-dave",
					SenderName:  "viewer-dave",
					Content:     "帖子已锁定测试",
					TimestampMS: time.Now().UnixMilli(),
				},
			},
			postFunc: func(context.Context, v2ex.ReplyRequest) (v2ex.ReplyResult, error) {
				return v2ex.ReplyResult{
					Status:     "failed",
					Submitted:  connector.BoolPtr(false),
					ReasonCode: "replies_closed",
					Detail:     "topic is locked",
				}, nil
			},
		}
		runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
			return dispatcher.Result{
				Text:      "回复测试",
				SessionID: "v2ex-sess-4",
				Backend:   "Hermes",
				RunID:     "run-v2ex-4",
			}, nil
		})
		stack := startV2EXChainStack(t, v2exStackOpts{driver: drv, runner: runner})

		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce failed: %v", err)
		}

		failed := stack.awaitRunMatch(t, "proven pre-submission failed run", func(r contracts.Run) bool {
			return r.Phase == contracts.PhaseTerminal
		})
		stack.awaitIdle(t)
		if failed.Outcome != contracts.OutcomeFailed || failed.Delivery != contracts.DeliveryFailed || failed.DeliveryEvidence.FailureEvidence != "replies_closed" {
			t.Fatalf("expected terminal failed/failed/replies_closed, got %+v", failed)
		}
	})

	t.Run("dry_run_safety_gates", func(t *testing.T) {
		drv := &fakeV2EXChainDriver{
			ready:  true,
			viewer: v2exSelfUser,
			comments: []v2ex.Comment{
				{
					ReplyID:     "18330401",
					TopicID:     "100001",
					Floor:       1,
					SenderID:    "viewer-eve",
					SenderName:  "viewer-eve",
					Content:     "演练开关测试",
					TimestampMS: time.Now().UnixMilli(),
				},
			},
		}
		runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
			return dispatcher.Result{
				Text:      "dry run reply",
				SessionID: "v2ex-sess-5",
				Backend:   "Hermes",
				RunID:     "run-v2ex-5",
			}, nil
		})
		stack := startV2EXChainStack(t, v2exStackOpts{
			driver:          drv,
			runner:          runner,
			connectorDryRun: true,
			endpointDryRun:  false,
		})

		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce failed: %v", err)
		}
		run := stack.awaitRunMatch(t, "connector dry_run terminal run", func(r contracts.Run) bool {
			return r.Phase == contracts.PhaseTerminal
		})
		stack.awaitIdle(t)
		if run.Outcome != contracts.OutcomeBlocked || run.Delivery != contracts.DeliveryBlocked || run.ReasonCode != "connector_dry_run" || !run.DeliveryEvidence.NoSubmission {
			t.Fatalf("unexpected connector dry_run result: %+v", run)
		}
		if len(drv.snapshotReplies()) != 0 {
			t.Fatalf("expected 0 PostReply calls, got %d", len(drv.snapshotReplies()))
		}
	})
}

func TestV2EXPassthroughForwardToWeChat(t *testing.T) {
	root := t.TempDir()
	wxGroup := "100001@chatroom"
	client := &http.Client{Timeout: 10 * time.Second}

	rCfg := relayconfig.Config{
		Server: relayconfig.ServerConfig{ListenAddr: "127.0.0.1:0"},
		GroupControl: relayconfig.GroupControl{
			MonitoredGroups: []string{v2exContainer, wxGroup},
			ExposedGroups:   []string{v2exContainer, wxGroup},
			GroupAliases:    map[string]string{},
		},
		Filters: relayconfig.FilterConfig{
			AllowedTypes:     []int{1},
			KeywordBlocklist: []string{},
			RegexBlocklist:   []string{},
			SenderBlocklist:  []string{},
		},
	}
	rPath := filepath.Join(root, "relay.json")
	writeJSON(t, rPath, rCfg)
	manager, err := relayconfig.NewManager(rPath)
	if err != nil {
		t.Fatal(err)
	}
	relayCore := web.NewServer(manager, hub.NewHub(100), filter.NewEngine())
	relayHandler, err := relayCore.Handler()
	if err != nil {
		t.Fatal(err)
	}
	relaySrv := httptest.NewServer(relayHandler)
	t.Cleanup(relaySrv.Close)

	drv := &fakeV2EXChainDriver{
		ready:  true,
		viewer: v2exSelfUser,
		comments: []v2ex.Comment{
			{
				ReplyID:     "18330999",
				TopicID:     "100001",
				Floor:       1,
				SenderID:    "viewer-alice",
				SenderName:  "viewer-alice",
				Content:     "测试免过滤直发微信",
				TimestampMS: time.Now().UnixMilli(),
			},
		},
	}
	connCfg := v2ex.Config{
		ListenAddr:       "127.0.0.1:0",
		RelayURL:         relaySrv.URL,
		StateDir:         filepath.Join(root, "v2ex-state"),
		AccountID:        "v2ex-ego",
		SelfUsernames:    []string{v2exSelfUser},
		DryRun:           connector.BoolPtr(true),
		PollIntervalSec:  60,
		MaxInitialAgeSec: 300,
		Targets: []v2ex.TargetConfig{
			{
				ContainerID: v2exContainer,
				Label:       "Synthetic V2EX Topic",
				TopicID:     "100001",
				Enabled:     true,
				PollLimit:   20,
			},
		},
	}
	conn, err := v2ex.NewConnector(connCfg, drv)
	if err != nil {
		t.Fatal(err)
	}

	var bridgeMu sync.Mutex
	var bridgeBodies []string
	var bridgePosts atomic.Int32
	bridgeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			_, _ = fmt.Fprint(w, `{"protocol_version":2,"group_routing":true,"serial_ui":true,"accessibility_trusted":true,"screen_capture_authorized":true,"dry_run":false}`)
			return
		}
		bridgePosts.Add(1)
		var body struct {
			GroupID string `json:"group_id"`
			Text    string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode bridge body: %v", err)
		}
		bridgeMu.Lock()
		bridgeBodies = append(bridgeBodies, body.GroupID+"|"+body.Text)
		bridgeMu.Unlock()

		// Push correlated WeChat echo through Relay so Dispatcher confirms delivery.
		go func(text string, seq int32) {
			time.Sleep(15 * time.Millisecond)
			var pushResp map[string]any
			requestJSON(t, client, http.MethodPost, relaySrv.URL+"/api/v1/messages", map[string]any{
				"id":          fmt.Sprintf("wx-echo-%d", seq),
				"group_id":    wxGroup,
				"group_name":  "Target WeChat Group",
				"sender_id":   "wx-bot",
				"sender_name": "wx-bot",
				"type":        1,
				"content":     text,
				"timestamp":   time.Now().UnixMilli(),
				"is_echo":     true,
			}, &pushResp)
		}(body.Text, bridgePosts.Load())
		_, _ = fmt.Fprint(w, "OK")
	}))
	t.Cleanup(bridgeSrv.Close)

	var dCfg dispatcher.Config
	dCfg.Server.ListenAddr = "127.0.0.1:0"
	dCfg.Relay.StreamURL = relaySrv.URL + "/api/v1/stream"
	dCfg.Bridge.URL = bridgeSrv.URL
	dCfg.Bridge.TimeoutSec = 5
	dCfg.Hermes.BinPath = "/bin/true"
	dCfg.Hermes.Profile = "v2ex-forward-int"
	dCfg.Hermes.ProfilesDir = filepath.Join(root, "profiles")
	dCfg.Hermes.TimeoutSec = 10
	dCfg.Hermes.MaxConcurrent = 1
	dCfg.Rules = dispatcher.Rules{
		BatchSize:        1,
		IdleTimeoutSec:   1,
		MaxBatchWaitSec:  2,
		MaxMessageAgeSec: 180,
		LayaFilter:       true,
		LayaURL:          "http://127.0.0.1:19999",
		KeywordBlocklist: []string{"免过滤"},
		SelfSenderIDs:    []string{"wx-bot"},
		GroupWhitelist:   []string{wxGroup},
		GroupRules: map[string]dispatcher.GroupRule{
			wxGroup: {
				Enabled:            true,
				TargetGroupID:      wxGroup,
				Target:             "Target WeChat Group",
				HermesProfile:      "wx-target-1",
				MentionOnly:        true,
				AllowedTypes:       []int{1},
				BatchSize:          1,
				IdleTimeoutSec:     1,
				MaxBatchWaitSec:    2,
				ReplyWindowSec:     60,
				MaxRepliesInWindow: 5,
			},
		},
		ForwardRules: []dispatcher.ForwardRule{
			{
				ID:                "v2ex-to-wx",
				Enabled:           true,
				SourceContainerID: v2exContainer,
				TargetContainerID: wxGroup,
				DryRun:            false,
				Template:          "[V2EX] {{sender}}: {{content}}",
			},
		},
	}
	dPath := filepath.Join(root, "dispatcher.json")
	writeJSON(t, dPath, dCfg)

	var runnerCalls atomic.Int32
	runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
		runnerCalls.Add(1)
		t.Fatal("AI runner must NOT be called for passthrough forwarding")
		return dispatcher.Result{}, nil
	})
	d, err := dispatcher.NewDispatcherWithOptions(dPath, dispatcher.Options{Runner: runner})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	dispSrv := httptest.NewServer(d.Handler())
	t.Cleanup(dispSrv.Close)

	d.Start()
	await(t, "dispatcher relay SSE connected", func() bool {
		h := d.HealthSnapshot()
		return h.RelayConnected && h.LastEventID != ""
	})

	if _, err := conn.PollOnce(context.Background()); err != nil {
		t.Fatalf("PollOnce failed: %v", err)
	}

	var matched contracts.Run
	await(t, "passthrough forward confirmed", func() bool {
		var snap observability.Snapshot
		requestJSON(t, client, http.MethodGet, dispSrv.URL+"/api/trace/snapshot", nil, &snap)
		for _, r := range snap.Runs {
			if r.Phase == contracts.PhaseTerminal && r.TargetGroupID == wxGroup {
				matched = r
				return true
			}
		}
		return false
	})

	if matched.Trigger != "direct:forward" || matched.Outcome != contracts.OutcomeReplied || matched.Delivery != contracts.DeliveryConfirmed {
		t.Fatalf("unexpected forward run: %+v", matched)
	}
	if runnerCalls.Load() != 0 || bridgePosts.Load() != 1 || len(drv.snapshotReplies()) != 0 {
		t.Fatalf("unexpected counts: runner=%d bridge=%d v2exReplies=%d", runnerCalls.Load(), bridgePosts.Load(), len(drv.snapshotReplies()))
	}
	bridgeMu.Lock()
	got := append([]string(nil), bridgeBodies...)
	bridgeMu.Unlock()
	if len(got) != 1 || got[0] != wxGroup+"|[V2EX] viewer-alice: 测试免过滤直发微信" {
		t.Fatalf("unexpected bridge payload: %+v", got)
	}
}
