package dispatcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestContainsKeyword(t *testing.T) {
	cases := []struct {
		text, keyword string
		want          bool
	}{
		{"有没有好用的笔记软件", "笔记", true},
		{"DemoTool 不错", "demotool", true},
		{"my capital city", "api", false}, // short ASCII keyword must be a whole word
		{"这个 API 多少钱", "api", true},       // ...but a CJK neighbour is fine
		{"api价格又涨了", "api价格", true},       // longer keywords are plain substrings
		{"nothing here", "", false},
	}
	for _, c := range cases {
		if got := containsKeyword(strings.ToLower(c.text), c.keyword); got != c.want {
			t.Errorf("containsKeyword(%q,%q)=%v want %v", c.text, c.keyword, got, c.want)
		}
	}
}

func TestPromoConfigValidation(t *testing.T) {
	cfg := testConfig()
	rule := cfg.Rules.GroupRules[groupA]
	rule.PromoKeywords = []string{"笔"}
	cfg.Rules.GroupRules[groupA] = rule
	data, _ := json.Marshal(cfg)
	if _, err := decodeConfig(data); err == nil {
		t.Fatal("one-character promo keyword must be rejected")
	}
	rule.PromoKeywords = []string{"笔记软件", "效率插件"}
	rule.PromoDailyCap = 21
	cfg.Rules.GroupRules[groupA] = rule
	data, _ = json.Marshal(cfg)
	if _, err := decodeConfig(data); err == nil {
		t.Fatal("daily cap above 20 must be rejected")
	}
	rule.PromoDailyCap = 2
	cfg.Rules.GroupRules[groupA] = rule
	data, _ = json.Marshal(cfg)
	if _, err := decodeConfig(data); err != nil {
		t.Fatalf("valid promo settings rejected: %v", err)
	}
}

func TestPromoCapCountsPerLocalDay(t *testing.T) {
	g := &GroupState{}
	rule := GroupRule{}
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.Local)
	if promoCapReached(g, rule, now) {
		t.Fatal("fresh group must have budget")
	}
	recordPromo(g, now)
	if !promoCapReached(g, rule, now) {
		t.Fatal("default cap is 1 per day")
	}
	if promoCapReached(g, rule, now.AddDate(0, 0, 1)) {
		t.Fatal("budget must reset the next day")
	}
	rule.PromoDailyCap = 2
	if promoCapReached(g, rule, now) {
		t.Fatal("cap of 2 allows a second promo")
	}
}

// promoFixture runs one human message through the real pipeline with a stub gate and Hermes.
// Shadow mode is on so nothing can reach the bridge, and the test asserts that too.
func promoFixture(t *testing.T, gateIntent, content string, prepare func(*Dispatcher)) (hermesCalls int32, d *Dispatcher) {
	t.Helper()
	gate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"should_reply":false,"probability":0.01,"intent":%q,"decision":"block"}`, gateIntent)
	}))
	t.Cleanup(gate.Close)
	cfg := testConfig()
	cfg.Rules.LayaFilter = true
	cfg.Rules.LayaURL = gate.URL
	rule := cfg.Rules.GroupRules[groupA]
	rule.PromoKeywords = []string{"笔记软件", "效率插件"}
	rule.PromoShadow = true
	cfg.Rules.GroupRules[groupA] = rule
	d = newTestDispatcher(t, cfg)
	if prepare != nil {
		prepare(d)
	}
	var calls atomic.Int32
	d.hermesRunner = func(_ context.Context, b batch) (HermesResult, error) {
		calls.Add(1)
		if b.Promo && !strings.Contains(buildPrompt(b), "只是命中了业务关键词") {
			t.Error("promo batch prompt lacks the speak-only-if-asked instruction")
		}
		return HermesResult{Text: "Mac 上可以看看 DemoDesk", SessionID: b.GroupID}, nil
	}
	msg := testMessage(groupA, "m1")
	msg.Content = content
	if err := d.handleIncomingMessage(msg, "e1"); err != nil {
		t.Fatal(err)
	}
	awaitCondition(t, func() bool { return idle(d) })
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state.Stats.TotalSubmitted != 0 || d.state.Stats.TotalSendFailed != 0 {
		t.Fatalf("shadow mode must never touch the bridge: %+v", d.state.Stats)
	}
	return calls.Load(), d
}

func logged(d *Dispatcher, needle string) bool {
	d.logMu.Lock()
	defer d.logMu.Unlock()
	for _, item := range d.logHistory {
		if strings.Contains(item.Message, needle) {
			return true
		}
	}
	return false
}

func TestPromoKeywordOverridesNoiseVerdictInShadowMode(t *testing.T) {
	calls, d := promoFixture(t, "noise", "有没有好用的笔记软件推荐？", nil)
	if calls != 1 {
		t.Fatalf("hermes calls %d, want 1: a business keyword must get past a noise verdict", calls)
	}
	if !logged(d, "影子模式：本会插话") {
		t.Fatal("shadow-mode outcome was not recorded")
	}
}

func TestPromoDoesNotOverrideSensitiveVerdict(t *testing.T) {
	if calls, _ := promoFixture(t, "sensitive", "笔记软件里这段内容很敏感", nil); calls != 0 {
		t.Fatalf("hermes calls %d, want 0: sensitive must stay blocked", calls)
	}
}

func TestNoiseWithoutKeywordStaysBlocked(t *testing.T) {
	if calls, _ := promoFixture(t, "noise", "中午吃什么好呢", nil); calls != 0 {
		t.Fatalf("hermes calls %d, want 0", calls)
	}
}

func TestPromoFallsBackToGateOnceDailyCapIsUsed(t *testing.T) {
	calls, d := promoFixture(t, "noise", "笔记软件哪个好用", func(d *Dispatcher) {
		d.mu.Lock()
		g := d.groupLocked(groupA)
		g.PromoDay, g.PromoCount = time.Now().Format("2006-01-02"), 1
		d.mu.Unlock()
	})
	if calls != 0 {
		t.Fatalf("hermes calls %d, want 0: with the cap used up the gate's noise verdict applies again", calls)
	}
	if !logged(d, "今日推广额度已用完") {
		t.Fatal("cap exhaustion was not logged")
	}
}

func TestHistoryStoreRecallsEarlierRelatedMessages(t *testing.T) {
	h := newHistoryStore(t.TempDir())
	now := time.Now()
	add := func(id, content string, echo bool) {
		h.Append(RelayMessage{ID: id, GroupID: groupA, SenderName: "u", Type: 1, Content: content, Timestamp: now.UnixMilli(), IsEcho: echo})
	}
	add("old1", "我之前用 argon 反代 gemini，一直报 429", false)
	add("old2", "中午吃啥", false)
	add("old3", "晚上去哪儿玩", false)
	add("old4", "明天天气不错", false)
	add("own", "argon 那个我也不确定", true)    // the bot's own reply is never recalled
	add("ctx", "argon 反代今天好像又挂了", false) // already in the prompt context
	add("cur", "argon 现在还能用吗", false)

	got := h.Search(groupA, []string{"argon 现在还能用吗"}, map[string]bool{"ctx": true, "cur": true}, now, []string{"DemoBot"})
	if len(got) != 1 || got[0].ID != "old1" {
		t.Fatalf("recall = %+v, want only old1", got)
	}
	// Chit-chat that shares no rare token recalls nothing.
	if got := h.Search(groupA, []string{"今天心情不错"}, nil, now, nil); len(got) != 0 {
		t.Fatalf("unrelated query recalled %+v", got)
	}
	// The bot's own name never counts as a topic.
	if got := h.Search(groupA, []string{"@DemoBot 你好"}, nil, now, []string{"DemoBot"}); len(got) != 0 {
		t.Fatalf("bot name recalled %+v", got)
	}
	// A nil store and a malformed group id are harmless.
	var nilStore *historyStore
	nilStore.Append(RelayMessage{GroupID: groupA, Content: "x"})
	if nilStore.Search(groupA, []string{"argon"}, nil, now, nil) != nil || h.Search("../etc", []string{"argon"}, nil, now, nil) != nil {
		t.Fatal("nil store or bad group id must return nothing")
	}
}

func TestIncomingRoutedMessagesAreStoredAndRecalled(t *testing.T) {
	cfg := testConfig()
	d := newTestDispatcher(t, cfg)
	old := testMessage(groupA, "old")
	old.Content = "ifind 接口一年要两万多"
	if err := d.handleIncomingMessage(old, "e1"); err != nil {
		t.Fatal(err)
	}
	other := testMessage(groupB, "other")
	other.Content = "ifind 在另一个群里"
	if err := d.handleIncomingMessage(other, "e2"); err != nil {
		t.Fatal(err)
	}
	b := batch{GroupID: groupA, Config: cfg, Messages: []RelayMessage{{ID: "new", Content: "九哥那个 ifind 接口多少钱来着", SenderID: "human"}}}
	got := d.recallEarlier(b)
	if len(got) != 1 || got[0].ID != "old" {
		t.Fatalf("recalled %+v, want only the message from the same group", got)
	}
	b.Recalled = got
	prompt := buildPrompt(b)
	if !strings.Contains(prompt, "earlier_today_related") || !strings.Contains(prompt, "两万多") {
		t.Fatalf("recalled message missing from prompt: %s", prompt)
	}
	if strings.Contains(prompt, "另一个群") {
		t.Fatal("another group's message leaked into the prompt")
	}
}

// TestMentionBypassesSendTimeRateLimit covers a synthetic scenario: a scheduled
// job's own send (e.g. the daily poster) used up the 45s/2-reply anti-flood budget, so a @
// mention that arrived soon after got a composed reply from Hermes but it was then dropped at
// delivery with "reply rate limit". A direct @ is a summon the whole group can see, so it must
// still go out even with the budget exhausted (deliver()'s two rateLimited checks, not just the
// earlier gate in processBatch, which already bypassed this before today's fix).
func TestMentionBypassesSendTimeRateLimit(t *testing.T) {
	var d *Dispatcher
	var submitted atomic.Int32
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		submitted.Add(1)
		var body map[string]interface{}
		_ = json.NewDecoder(r.Body).Decode(&body)
		echo := testMessage(body["group_id"].(string), "echo-"+body["request_id"].(string))
		echo.IsEcho = true
		echo.Content = body["text"].(string)
		if err := d.handleIncomingMessage(echo, ""); err != nil {
			t.Error(err)
		}
		fmt.Fprint(w, "OK")
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	d = newTestDispatcher(t, cfg)
	d.hermesRunner = func(_ context.Context, b batch) (HermesResult, error) {
		return HermesResult{Text: "reply", SessionID: b.GroupID}, nil
	}
	// Exhaust the budget as if two unrelated self-sends (e.g. a cron poster) just happened.
	d.mu.Lock()
	d.groupLocked(groupA).ReplyTimes = []time.Time{time.Now(), time.Now()}
	d.mu.Unlock()

	msg := testMessage(groupA, "mention")
	msg.Content = "@DemoBot help"
	if err := d.handleIncomingMessage(msg, ""); err != nil {
		t.Fatal(err)
	}
	awaitCondition(t, func() bool { return idle(d) })
	d.mu.Lock()
	outcome := d.state.Stats.TotalReplied
	d.mu.Unlock()
	if submitted.Load() != 1 || outcome != 1 {
		t.Fatalf("a direct @ must still be delivered once the anti-flood budget is used up: submitted=%d replied=%d", submitted.Load(), outcome)
	}
}

// TestNonMentionStillRespectsSendTimeRateLimit is the regression guard for the fix above: only
// an explicit @ bypasses the send-time anti-flood window. An ordinary reply (no @, decided by
// Hermes on a debounce/batch-size trigger) must still be throttled exactly as before.
func TestNonMentionStillRespectsSendTimeRateLimit(t *testing.T) {
	var submitted atomic.Int32
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		submitted.Add(1)
		fmt.Fprint(w, "OK")
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	d := newTestDispatcher(t, cfg)
	d.hermesRunner = func(_ context.Context, b batch) (HermesResult, error) {
		return HermesResult{Text: "reply", SessionID: b.GroupID}, nil
	}
	d.mu.Lock()
	d.groupLocked(groupA).ReplyTimes = []time.Time{time.Now(), time.Now()}
	d.mu.Unlock()

	if err := d.handleIncomingMessage(testMessage(groupA, "plain"), ""); err != nil {
		t.Fatal(err)
	}
	awaitCondition(t, func() bool { return idle(d) })
	if submitted.Load() != 0 {
		t.Fatalf("a non-mention reply must still be rate-limited: submitted=%d", submitted.Load())
	}
}

// TestMentionDoesNotBypassOwnReplyOvertaken checks the fix stayed narrow: a mention still must
// not be sent once the group's own, newer reply has already overtaken it (staleness, not
// anti-flood — see the comment on batch.Mentioned). Modeled on the existing
// TestOwnReplyDuringInferenceRejectsOlderReply: block Hermes mid-inference, inject a real
// self-echo (the normal path that advances LastOwnReplySequence), then let it finish.
func TestMentionDoesNotBypassOwnReplyOvertaken(t *testing.T) {
	var submitted atomic.Int32
	bridge := bridgeFixture(t, func(w http.ResponseWriter, r *http.Request) {
		submitted.Add(1)
		fmt.Fprint(w, "OK")
	})
	cfg := testConfig()
	cfg.Bridge.URL = bridge.URL
	d := newTestDispatcher(t, cfg)
	entered, release := make(chan struct{}), make(chan struct{})
	d.hermesRunner = func(ctx context.Context, b batch) (HermesResult, error) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return HermesResult{}, ctx.Err()
		}
		return HermesResult{Text: "reply", SessionID: b.GroupID}, nil
	}
	msg := testMessage(groupA, "mention")
	msg.Content = "@DemoBot help"
	if err := d.handleIncomingMessage(msg, ""); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("inference did not start")
	}
	// A newer own reply (echo) arrives and overtakes this batch while Hermes is still composing.
	echo := testMessage(groupA, "manual-reply")
	echo.IsEcho = true
	if err := d.handleIncomingMessage(echo, ""); err != nil {
		t.Fatal(err)
	}
	close(release)
	awaitCondition(t, func() bool { return idle(d) })
	if submitted.Load() != 0 {
		t.Fatalf("a stale mention must not be sent just because it was a direct @: submitted=%d", submitted.Load())
	}
}
