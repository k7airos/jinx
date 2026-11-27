package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"jinx/internal/connector"
	"jinx/internal/connector/bilibili"
	"jinx/internal/contracts"
	"jinx/internal/dispatcher"
	"jinx/internal/observability"
	relayconfig "jinx/internal/relay/config"
	"jinx/internal/relay/filter"
	"jinx/internal/relay/hub"
	"jinx/internal/relay/web"
)

const (
	biliContainer = "bilibili:BV1example0001"
	biliSelfMID   = "50001"
)

type fakeBiliChainDriver struct {
	mu          sync.Mutex
	ready       bool
	reason      string
	viewer      string
	searchRes   []bilibili.VideoCandidate
	comments    []bilibili.Comment
	replies     []bilibili.ReplyRequest
	postFunc    func(ctx context.Context, req bilibili.ReplyRequest) (bilibili.ReplyResult, error)
}

func (f *fakeBiliChainDriver) Health(context.Context) (bool, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ready, f.reason
}

func (f *fakeBiliChainDriver) SearchVideos(context.Context, bilibili.SearchQuery) ([]bilibili.VideoCandidate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]bilibili.VideoCandidate, len(f.searchRes))
	copy(out, f.searchRes)
	return out, nil
}

func (f *fakeBiliChainDriver) FetchComments(context.Context, bilibili.TargetConfig) ([]bilibili.Comment, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]bilibili.Comment, len(f.comments))
	copy(out, f.comments)
	return out, f.viewer, nil
}

func (f *fakeBiliChainDriver) PostReply(ctx context.Context, req bilibili.ReplyRequest) (bilibili.ReplyResult, error) {
	f.mu.Lock()
	f.replies = append(f.replies, req)
	fn := f.postFunc
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return bilibili.ReplyResult{
		Status:     "confirmed",
		Submitted:  connector.BoolPtr(true),
		ExternalID: "90001",
	}, nil
}

func (f *fakeBiliChainDriver) setComments(comments ...bilibili.Comment) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.comments = append([]bilibili.Comment(nil), comments...)
}

func (f *fakeBiliChainDriver) snapshotReplies() []bilibili.ReplyRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]bilibili.ReplyRequest, len(f.replies))
	copy(out, f.replies)
	return out
}

type biliChainStack struct {
	relay      *httptest.Server
	connector  *bilibili.Connector
	connServer *httptest.Server
	dispatcher *dispatcher.Dispatcher
	dispServer *httptest.Server
	client     *http.Client
}

type biliStackOpts struct {
	connectorDryRun bool
	endpointDryRun  bool
	endpointURL     string
	runner          dispatcher.Runner
	driver          *fakeBiliChainDriver
}

func startBiliChainStack(t *testing.T, opts biliStackOpts) *biliChainStack {
	t.Helper()
	root := t.TempDir()
	client := &http.Client{Timeout: 10 * time.Second}

	// Real in-process Relay server monitoring synthetic Bilibili container.
	rCfg := relayconfig.Config{
		Server: relayconfig.ServerConfig{ListenAddr: "127.0.0.1:0"},
		GroupControl: relayconfig.GroupControl{
			MonitoredGroups: []string{biliContainer},
			ExposedGroups:   []string{biliContainer},
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

	// Real Bilibili connector backed by injected fake driver.
	connCfg := bilibili.Config{
		ListenAddr:       "127.0.0.1:0",
		RelayURL:         relaySrv.URL,
		StateDir:         filepath.Join(root, "conn-state"),
		AccountID:        "bilibili-ego",
		SelfMIDs:         []string{biliSelfMID},
		DryRun:           connector.BoolPtr(opts.connectorDryRun),
		PollIntervalSec:  30,
		MaxInitialAgeSec: 300,
		Targets: []bilibili.TargetConfig{
			{
				ContainerID: biliContainer,
				Label:       "Synthetic Video",
				BVID:        "BV1example0001",
				OID:         "88001",
				CommentType: 1,
				Enabled:     true,
				PollLimit:   20,
			},
		},
	}
	conn, err := bilibili.NewConnector(connCfg, opts.driver)
	if err != nil {
		t.Fatal(err)
	}
	connSrv := httptest.NewServer(conn.Handler())
	t.Cleanup(connSrv.Close)

	endpointURL := opts.endpointURL
	if endpointURL == "" {
		endpointURL = connSrv.URL
	}

	// Real Dispatcher configured without Bridge.URL (pure connector mode).
	var dCfg dispatcher.Config
	dCfg.Server.ListenAddr = "127.0.0.1:0"
	dCfg.Relay.StreamURL = relaySrv.URL + "/api/v1/stream"
	dCfg.ActionEndpoints = map[string]dispatcher.ActionEndpointConfig{
		"bilibili": {
			URL:        endpointURL,
			DryRun:     opts.endpointDryRun,
			TimeoutSec: 10,
		},
	}
	dCfg.Hermes.BinPath = "/bin/true"
	dCfg.Hermes.Profile = "bili-integration"
	dCfg.Hermes.ProfilesDir = filepath.Join(root, "profiles")
	dCfg.Hermes.TimeoutSec = 10
	dCfg.Hermes.MaxConcurrent = 1
	dCfg.Rules = dispatcher.Rules{
		BatchSize:        1,
		IdleTimeoutSec:   1,
		MaxBatchWaitSec:  2,
		MaxMessageAgeSec: 180,
		SelfSenderIDs:    []string{biliSelfMID},
		GroupWhitelist:   []string{biliContainer},
		GroupRules: map[string]dispatcher.GroupRule{
			biliContainer: {
				Enabled:            true,
				TargetGroupID:      biliContainer,
				Target:             "Synthetic Video",
				HermesProfile:      "bili-video-1",
				MentionOnly:        false,
				AllowedTypes:       []int{1},
				BatchSize:          1,
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

	return &biliChainStack{
		relay:      relaySrv,
		connector:  conn,
		connServer: connSrv,
		dispatcher: d,
		dispServer: dispSrv,
		client:     client,
	}
}

func (s *biliChainStack) traceSnapshot(t *testing.T) observability.Snapshot {
	t.Helper()
	var snap observability.Snapshot
	requestJSON(t, s.client, http.MethodGet, s.dispServer.URL+"/api/trace/snapshot", nil, &snap)
	return snap
}

func (s *biliChainStack) awaitRunMatch(t *testing.T, desc string, pred func(contracts.Run) bool) contracts.Run {
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

func (s *biliChainStack) awaitIdle(t *testing.T) {
	t.Helper()
	await(t, "dispatcher idle", func() bool {
		h := s.dispatcher.HealthSnapshot()
		return h.PendingMessages == 0 && h.ExecutingGroups == 0 && !h.AwaitingEcho
	})
}

func TestBilibiliConnectorChain(t *testing.T) {
	t.Run("direct_confirmed_thread_reply", func(t *testing.T) {
		var runnerCalls atomic.Int32
		drv := &fakeBiliChainDriver{
			ready:  true,
			viewer: biliSelfMID,
			comments: []bilibili.Comment{
				{
					RPID:        "70001",
					OID:         "88001",
					BVID:        "BV1example0001",
					CommentType: 1,
					SenderID:    "12345",
					SenderName:  "viewer-alice",
					Content:     "请问这个工具支持导出吗？",
					TimestampMS: time.Now().UnixMilli(),
				},
			},
		}
		runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
			runnerCalls.Add(1)
			return dispatcher.Result{
				Text:      "支持一键导出。",
				SessionID: "bili-sess-1",
				Backend:   "Hermes",
				RunID:     "run-bili-1",
			}, nil
		})
		stack := startBiliChainStack(t, biliStackOpts{driver: drv, runner: runner})

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
		if run.Action == nil || run.Action.Platform != contracts.PlatformBilibili || run.Action.Kind != contracts.ActionThreadReply {
			t.Fatalf("expected bilibili thread_reply ActionRef, got %+v", run.Action)
		}
		if run.DeliveryEvidence.EchoMessageID != "90001" || run.DeliveryEvidence.SubmittedAtSource != "bridge_ack" {
			t.Fatalf("unexpected delivery evidence: %+v", run.DeliveryEvidence)
		}
		receiptView, err := dispatcher.ProcessingActionReceipt(run)
		if err != nil || receiptView.Receipt == nil {
			t.Fatalf("ProcessingActionReceipt failed: %+v err=%v", receiptView, err)
		}
		if err := contracts.ValidateActionReceipt(*receiptView.Receipt); err != nil {
			t.Fatalf("ValidateActionReceipt failed: %v", err)
		}
		if runnerCalls.Load() != 1 {
			t.Fatalf("expected 1 runner call, got %d", runnerCalls.Load())
		}
		replies := drv.snapshotReplies()
		if len(replies) != 1 {
			t.Fatalf("expected 1 PostReply call, got %d", len(replies))
		}
		if replies[0].OID != "88001" || replies[0].CommentType != 1 || replies[0].RootID != "70001" || replies[0].ParentID != "70001" || replies[0].Text != "支持一键导出。" {
			t.Fatalf("unexpected PostReply args (expected trailing 。 preserved): %+v", replies[0])
		}

		// Subsequent poll including our own newly posted reply must NOT re-trigger.
		drv.setComments(
			bilibili.Comment{
				RPID:        "70001",
				OID:         "88001",
				BVID:        "BV1example0001",
				CommentType: 1,
				SenderID:    "12345",
				SenderName:  "viewer-alice",
				Content:     "请问支持导出吗？",
				TimestampMS: time.Now().Add(-time.Second).UnixMilli(),
			},
			bilibili.Comment{
				RPID:        "90001",
				OID:         "88001",
				BVID:        "BV1example0001",
				CommentType: 1,
				RootID:      "70001",
				ParentID:    "70001",
				SenderID:    biliSelfMID,
				SenderName:  "bot",
				Content:     "支持一键导出。",
				TimestampMS: time.Now().Add(20 * time.Millisecond).UnixMilli(),
				IsSelf:      true,
			},
		)
		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce #2 failed: %v", err)
		}
		await(t, "dispatcher sees self echo", func() bool {
			return stack.dispatcher.HealthSnapshot().LastEventID != ""
		})
		time.Sleep(150 * time.Millisecond)
		if snap := stack.traceSnapshot(t); len(snap.Runs) != 1 || runnerCalls.Load() != 1 || len(drv.snapshotReplies()) != 1 {
			t.Fatalf("own reply re-triggered pipeline: runs=%d runnerCalls=%d replies=%d", len(snap.Runs), runnerCalls.Load(), len(drv.snapshotReplies()))
		}
	})

	t.Run("async_echo_confirmation", func(t *testing.T) {
		drv := &fakeBiliChainDriver{
			ready:  true,
			viewer: biliSelfMID,
			comments: []bilibili.Comment{
				{
					RPID:        "70002",
					OID:         "88001",
					BVID:        "BV1example0001",
					CommentType: 1,
					SenderID:    "23456",
					SenderName:  "viewer-bob",
					Content:     "异步确认测试一下。",
					TimestampMS: time.Now().UnixMilli(),
				},
			},
			postFunc: func(context.Context, bilibili.ReplyRequest) (bilibili.ReplyResult, error) {
				return bilibili.ReplyResult{
					Status:     "submitted",
					Submitted:  connector.BoolPtr(true),
					ReasonCode: "missing_external_id",
				}, nil
			},
		}
		runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
			return dispatcher.Result{
				Text:      "异步确认回复。",
				SessionID: "bili-sess-2",
				Backend:   "Hermes",
				RunID:     "run-bili-2",
			}, nil
		})
		stack := startBiliChainStack(t, biliStackOpts{driver: drv, runner: runner})

		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce #1 failed: %v", err)
		}

		submitting := stack.awaitRunMatch(t, "run confirming pending submitted", func(r contracts.Run) bool {
			return r.Phase == contracts.PhaseConfirming && r.Outcome == contracts.OutcomePending && r.Delivery == contracts.DeliverySubmitted
		})
		if submitting.DeliveryEvidence.SubmissionStartedAt == nil {
			t.Fatalf("missing SubmissionStartedAt: %+v", submitting.DeliveryEvidence)
		}

		// Ensure timestamp is strictly after SubmissionStartedAt and not ahead of Relay receive time.
		time.Sleep(25 * time.Millisecond)
		drv.setComments(bilibili.Comment{
			RPID:        "90002",
			OID:         "88001",
			BVID:        "BV1example0001",
			CommentType: 1,
			RootID:      "70002",
			ParentID:    "70002",
			SenderID:    biliSelfMID,
			SenderName:  "bot",
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
		if confirmed.Outcome != contracts.OutcomeReplied || confirmed.Delivery != contracts.DeliveryConfirmed || confirmed.DeliveryEvidence.EchoMessageID != "90002" {
			t.Fatalf("expected async echo confirmation with EchoMessageID=90002, got %+v", confirmed)
		}
	})

	t.Run("proven_pre_submission_failure", func(t *testing.T) {
		drv := &fakeBiliChainDriver{
			ready:  true,
			viewer: biliSelfMID,
			comments: []bilibili.Comment{
				{
					RPID:        "70003",
					OID:         "88001",
					BVID:        "BV1example0001",
					CommentType: 1,
					SenderID:    "34567",
					SenderName:  "viewer-carol",
					Content:     "这条待会儿被删",
					TimestampMS: time.Now().UnixMilli(),
				},
			},
			postFunc: func(context.Context, bilibili.ReplyRequest) (bilibili.ReplyResult, error) {
				return bilibili.ReplyResult{
					Status:     "failed",
					Submitted:  connector.BoolPtr(false),
					ReasonCode: "comment_deleted",
					Detail:     "comment no longer exists",
				}, nil
			},
		}
		runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
			return dispatcher.Result{
				Text:      "回复测试",
				SessionID: "bili-sess-3",
				Backend:   "Hermes",
				RunID:     "run-bili-3",
			}, nil
		})
		stack := startBiliChainStack(t, biliStackOpts{driver: drv, runner: runner})

		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce failed: %v", err)
		}

		failed := stack.awaitRunMatch(t, "proven pre-submission failed run", func(r contracts.Run) bool {
			return r.Phase == contracts.PhaseTerminal
		})
		stack.awaitIdle(t)
		if failed.Outcome != contracts.OutcomeFailed || failed.Delivery != contracts.DeliveryFailed || failed.DeliveryEvidence.FailureEvidence != "comment_deleted" {
			t.Fatalf("expected terminal failed/failed/comment_deleted, got %+v", failed)
		}
	})

	t.Run("ambiguous_failure_unknown_no_retry", func(t *testing.T) {
		var actionHits atomic.Int32
		flakyConnector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/health":
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(connector.HealthResponse{
					Status:          "ready",
					ProtocolVersion: 1,
					Platform:        "bilibili",
					Ready:           true,
					DryRun:          false,
					Capabilities:    []string{"thread_reply", "reply_text"},
				})
			case "/v1/actions":
				actionHits.Add(1)
				http.Error(w, "ambiguous 500 error", http.StatusInternalServerError)
			default:
				http.NotFound(w, r)
			}
		}))
		defer flakyConnector.Close()

		drv := &fakeBiliChainDriver{
			ready:  true,
			viewer: biliSelfMID,
			comments: []bilibili.Comment{
				{
					RPID:        "70004",
					OID:         "88001",
					BVID:        "BV1example0001",
					CommentType: 1,
					SenderID:    "45678",
					SenderName:  "viewer-dave",
					Content:     "触发500模糊失败",
					TimestampMS: time.Now().UnixMilli(),
				},
			},
		}
		runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
			return dispatcher.Result{
				Text:      "回复500测试",
				SessionID: "bili-sess-4",
				Backend:   "Hermes",
				RunID:     "run-bili-4",
			}, nil
		})
		stack := startBiliChainStack(t, biliStackOpts{
			driver:      drv,
			runner:      runner,
			endpointURL: flakyConnector.URL,
		})

		if _, err := stack.connector.PollOnce(context.Background()); err != nil {
			t.Fatalf("PollOnce failed: %v", err)
		}

		unknownRun := stack.awaitRunMatch(t, "ambiguous unknown run", func(r contracts.Run) bool {
			return r.Phase == contracts.PhaseUnknown
		})
		stack.awaitIdle(t)
		if unknownRun.Outcome != contracts.OutcomeUnknown || unknownRun.Delivery != contracts.DeliveryUnknown {
			t.Fatalf("expected OutcomeUnknown/DeliveryUnknown, got %+v", unknownRun)
		}
		if actionHits.Load() != 1 {
			t.Fatalf("expected exactly 1 /v1/actions attempt (no retries), got %d", actionHits.Load())
		}
	})

	t.Run("dry_run_safety_gates", func(t *testing.T) {
		t.Run("endpoint_dry_run", func(t *testing.T) {
			drv := &fakeBiliChainDriver{
				ready:  true,
				viewer: biliSelfMID,
				comments: []bilibili.Comment{
					{
						RPID:        "70005",
						OID:         "88001",
						BVID:        "BV1example0001",
						CommentType: 1,
						SenderID:    "56789",
						SenderName:  "viewer-eve",
						Content:     "测试dispatcher dry_run",
						TimestampMS: time.Now().UnixMilli(),
					},
				},
			}
			runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
				return dispatcher.Result{
					Text:      "dry run reply",
					SessionID: "bili-sess-5a",
					Backend:   "Hermes",
					RunID:     "run-bili-5a",
				}, nil
			})
			stack := startBiliChainStack(t, biliStackOpts{
				driver:          drv,
				runner:          runner,
				endpointDryRun:  true,
				connectorDryRun: false,
			})

			if _, err := stack.connector.PollOnce(context.Background()); err != nil {
				t.Fatalf("PollOnce failed: %v", err)
			}
			run := stack.awaitRunMatch(t, "endpoint dry_run terminal run", func(r contracts.Run) bool {
				return r.Phase == contracts.PhaseTerminal
			})
			stack.awaitIdle(t)
			if run.Outcome != contracts.OutcomeSuppressed || run.Delivery != contracts.DeliveryBlocked || run.ReasonCode != "dry_run" || !run.DeliveryEvidence.NoSubmission {
				t.Fatalf("unexpected endpoint dry_run result: %+v", run)
			}
			if len(drv.snapshotReplies()) != 0 {
				t.Fatalf("expected 0 PostReply calls, got %d", len(drv.snapshotReplies()))
			}
		})

		t.Run("connector_dry_run", func(t *testing.T) {
			drv := &fakeBiliChainDriver{
				ready:  true,
				viewer: biliSelfMID,
				comments: []bilibili.Comment{
					{
						RPID:        "70006",
						OID:         "88001",
						BVID:        "BV1example0001",
						CommentType: 1,
						SenderID:    "67890",
						SenderName:  "viewer-frank",
						Content:     "测试connector dry_run",
						TimestampMS: time.Now().UnixMilli(),
					},
				},
			}
			runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
				return dispatcher.Result{
					Text:      "dry run reply",
					SessionID: "bili-sess-5b",
					Backend:   "Hermes",
					RunID:     "run-bili-5b",
				}, nil
			})
			stack := startBiliChainStack(t, biliStackOpts{
				driver:          drv,
				runner:          runner,
				endpointDryRun:  false,
				connectorDryRun: true,
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
	})
}

func TestBilibiliKeywordSearchChain(t *testing.T) {
	root := t.TempDir()
	client := &http.Client{Timeout: 10 * time.Second}
	discoveredBVID := "BV1search0001"
	discoveredContainer := "bilibili:" + discoveredBVID

	rCfg := relayconfig.Config{
		Server: relayconfig.ServerConfig{ListenAddr: "127.0.0.1:0"},
		GroupControl: relayconfig.GroupControl{
			MonitoredGroups: []string{discoveredContainer},
			ExposedGroups:   []string{discoveredContainer},
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

	drv := &fakeBiliChainDriver{
		ready:  true,
		viewer: biliSelfMID,
		searchRes: []bilibili.VideoCandidate{
			{
				BVID:         discoveredBVID,
				OID:          "99001",
				Title:        "AI Agent 实战拆解",
				Author:       "demo-author",
				URL:          "https://www.bilibili.com/video/" + discoveredBVID,
				CommentCount: 12,
			},
		},
		comments: []bilibili.Comment{
			{
				RPID:        "81001",
				OID:         "99001",
				BVID:        discoveredBVID,
				VideoTitle:  "AI Agent 实战拆解",
				CommentType: 1,
				SenderID:    "222333",
				SenderName:  "viewer-search",
				Content:     "这个工作流能本地运行吗？",
				TimestampMS: time.Now().UnixMilli(),
			},
		},
	}

	connCfg := bilibili.Config{
		ListenAddr:       "127.0.0.1:0",
		RelayURL:         relaySrv.URL,
		StateDir:         filepath.Join(root, "conn-state"),
		AccountID:        "bilibili-ego",
		SelfMIDs:         []string{biliSelfMID},
		DryRun:           connector.BoolPtr(false),
		PollIntervalSec:  30,
		MaxInitialAgeSec: 300,
		SearchTargets: []bilibili.SearchTargetConfig{
			{Keyword: "AI Agent", Order: "totalrank", MaxVideos: 1, MinComments: 1, Enabled: true},
		},
	}
	conn, err := bilibili.NewConnector(connCfg, drv)
	if err != nil {
		t.Fatal(err)
	}
	connSrv := httptest.NewServer(conn.Handler())
	t.Cleanup(connSrv.Close)

	var dCfg dispatcher.Config
	dCfg.Server.ListenAddr = "127.0.0.1:0"
	dCfg.Relay.StreamURL = relaySrv.URL + "/api/v1/stream"
	dCfg.ActionEndpoints = map[string]dispatcher.ActionEndpointConfig{
		"bilibili": {
			URL:        connSrv.URL,
			DryRun:     false,
			TimeoutSec: 10,
		},
	}
	dCfg.Hermes.BinPath = "/bin/true"
	dCfg.Hermes.Profile = "bili-search-integration"
	dCfg.Hermes.ProfilesDir = filepath.Join(root, "profiles")
	dCfg.Hermes.TimeoutSec = 10
	dCfg.Hermes.MaxConcurrent = 1
	dCfg.Rules = dispatcher.Rules{
		BatchSize:        1,
		IdleTimeoutSec:   1,
		MaxBatchWaitSec:  2,
		MaxMessageAgeSec: 180,
		SelfSenderIDs:    []string{biliSelfMID},
		GroupWhitelist:   []string{discoveredContainer},
		GroupRules: map[string]dispatcher.GroupRule{
			discoveredContainer: {
				Enabled:            true,
				TargetGroupID:      discoveredContainer,
				Target:             "AI Agent 实战拆解",
				HermesProfile:      "bili-search-video-1",
				MentionOnly:        false,
				AllowedTypes:       []int{1},
				BatchSize:          1,
				IdleTimeoutSec:     1,
				MaxBatchWaitSec:    2,
				ReplyWindowSec:     60,
				MaxRepliesInWindow: 5,
			},
		},
	}
	dPath := filepath.Join(root, "dispatcher.json")
	writeJSON(t, dPath, dCfg)

	runner := dispatcher.RunFunc(func(context.Context, dispatcher.BackendRequest) (dispatcher.Result, error) {
		return dispatcher.Result{
			Text:      "可以完全本地运行，只需要连接本地模型端点。",
			SessionID: "bili-search-sess-1",
			Backend:   "Hermes",
			RunID:     "run-bili-search-1",
		}, nil
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
		health := d.HealthSnapshot()
		return health.RelayConnected && health.LastEventID != ""
	})

	stack := &biliChainStack{
		relay:      relaySrv,
		connector:  conn,
		connServer: connSrv,
		dispatcher: d,
		dispServer: dispSrv,
		client:     client,
	}

	rep, err := conn.PollOnce(context.Background())
	if err != nil {
		t.Fatalf("PollOnce failed: %v", err)
	}
	if rep.VideosDiscovered != 1 || rep.Forwarded != 1 {
		t.Fatalf("unexpected search poll report: %+v", rep)
	}

	run := stack.awaitRunMatch(t, "search video reply confirmed", func(r contracts.Run) bool {
		return r.Phase == contracts.PhaseTerminal
	})
	stack.awaitIdle(t)

	if run.Outcome != contracts.OutcomeReplied || run.Delivery != contracts.DeliveryConfirmed {
		t.Fatalf("expected OutcomeReplied/DeliveryConfirmed, got %+v", run)
	}
	replies := drv.snapshotReplies()
	if len(replies) != 1 || replies[0].Target.ContainerID != discoveredContainer || replies[0].ParentID != "81001" {
		t.Fatalf("unexpected search reply call: %+v", replies)
	}
}
