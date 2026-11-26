package hub

import (
	"testing"

	"jinx/internal/relay/config"
	"jinx/internal/relay/model"
)

func TestDropUnexposedDoesNotDropBlockedMessagesFromExposedGroups(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.GroupControl.ExposedGroups = []string{"visible@chatroom"}
	cfg.GroupControl.DropUnexposedCompletely = true
	h := NewHub(200)
	for _, group := range []string{"visible@chatroom", "private@chatroom"} {
		h.RecordMessage(&model.Message{GroupID: group, Type: model.TypeImage, Action: model.ActionBlockedType}, false, cfg)
	}
	got := h.GetRecentMessages()
	if len(got) != 1 || got[0].GroupID != "visible@chatroom" {
		t.Fatalf("expected the exposed group's blocked image in history, got %v", got)
	}
}
