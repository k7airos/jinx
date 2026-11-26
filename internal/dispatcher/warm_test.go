package dispatcher

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWarmDecayAndBoundedReplies(t *testing.T) {
	now := time.Now()
	g := &GroupState{WarmSince: now}
	if warmLevel(g, now) != 6 {
		t.Fatal("summon not hot")
	}
	g.WarmMisses = 3
	if warmLevel(g, now) != 3 {
		t.Fatal("unrelated messages not cooling")
	}
	if warmLevel(g, now.Add(150*time.Second)) != 0 {
		t.Fatal("time decay failed")
	}
	g.WarmMisses = 0
	rule := GroupRule{ReplyWindowSec: 300, MaxRepliesInWindow: 1}
	g.ReplyTimes = []time.Time{now.Add(-20 * time.Second)}
	if !replyLimited(g, rule, now, false) || replyLimited(g, rule, now, true) {
		t.Fatal("followup remains blocked by baseline cap")
	}
	for i := 0; i < 4; i++ {
		g.ReplyTimes = append(g.ReplyTimes, now.Add(-time.Duration(30+i)*time.Second))
	}
	if replyLimited(g, rule, now, true) {
		t.Fatal("fifth reply must still allow a sixth")
	}
	g.ReplyTimes = append(g.ReplyTimes, now.Add(-40*time.Second))
	if !replyLimited(g, rule, now, true) {
		t.Fatal("warm flood cap failed")
	}
}
func TestWarmNoisePassesButSensitiveDoesNot(t *testing.T) {
	intent := "noise"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"intent":%q,"decision":"block","should_reply":false,"probability":0.9}`, intent)
	}))
	defer srv.Close()
	d := &Dispatcher{client: srv.Client()}
	b := batch{Warm: true, Messages: []RelayMessage{{Content: "差评，英伟达早上市了"}}}
	b.Config.Rules.LayaURL = srv.URL
	b.Rule.DecisionThreshold = 0.5
	if err := d.checkDecision(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	b.Warm = false
	if err := d.checkDecision(context.Background(), b); err == nil {
		t.Fatal("baseline noise passed")
	}
	b.Warm = true
	intent = "sensitive"
	if err := d.checkDecision(context.Background(), b); err == nil {
		t.Fatal("sensitive gate bypassed")
	}
	if !strings.Contains(buildPrompt(b), "纠错") || !strings.Contains(buildPrompt(b), "NONE") {
		t.Fatal("missing contextual followup instructions")
	}
}
