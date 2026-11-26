package hub

import (
	"jinx/internal/relay/model"
	"testing"
)

func replayClient(id string) *SSEClient { return &SSEClient{ID: id, Ch: make(chan *model.Message, 1)} }

func TestAIReconnectReplaysOnlyNewFilteredMessages(t *testing.T) {
	h := NewHub(3)
	_, baseline, _ := h.RegisterAIWithReplay(replayClient("first"), "")
	h.UnregisterSSE("first")
	h.BroadcastToAI(&model.Message{ID: "one", GroupID: "a", Action: model.ActionForward})
	h.BroadcastToAI(&model.Message{ID: "two", GroupID: "b", Action: model.ActionForward})
	client := replayClient("second")
	client.Filter.Groups = []string{"a"}
	got, _, gap := h.RegisterAIWithReplay(client, baseline)
	if gap || len(got) != 1 || got[0].ID != "one" {
		t.Fatalf("replay=%v gap=%v", got, gap)
	}
	cursor := got[0].StreamID
	h.UnregisterSSE("second")
	got, _, gap = h.RegisterAIWithReplay(replayClient("third"), cursor)
	if gap || len(got) != 1 || got[0].ID != "two" {
		t.Fatalf("resume replay=%v gap=%v", got, gap)
	}
}

func TestSlowAIClientDisconnectsAndCanRecover(t *testing.T) {
	h := NewHub(2)
	c := replayClient("slow")
	_, baseline, _ := h.RegisterAIWithReplay(c, "")
	h.BroadcastToAI(&model.Message{ID: "one"})
	h.BroadcastToAI(&model.Message{ID: "two"})
	<-c.Ch
	if _, ok := <-c.Ch; ok {
		t.Fatal("slow consumer was not disconnected")
	}
	got, _, gap := h.RegisterAIWithReplay(replayClient("resume"), baseline)
	if gap || len(got) != 2 {
		t.Fatal("buffered messages not recoverable")
	}
	h.BroadcastToAI(&model.Message{ID: "three"})
	_, _, gap = h.RegisterAIWithReplay(replayClient("old"), baseline)
	if !gap {
		t.Fatal("missing cursor must produce gap")
	}
	_, _, gap = h.RegisterAIWithReplay(replayClient("restarted"), "previous-process:1")
	if !gap {
		t.Fatal("process restart must produce gap")
	}
}
