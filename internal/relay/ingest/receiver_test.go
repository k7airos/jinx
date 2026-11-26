package ingest

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeRawMessageWeChatOmitsMultiPlatformFields(t *testing.T) {
	raw := map[string]interface{}{
		"id":         "msg-wx-1",
		"group_id":   "123456@chatroom",
		"group_name": "Demo Group",
		"sender_id":  "wxid_exampleuser1",
		"senderName": "Alice",
		"type":       float64(1),
		"content":    "hello world",
		"timestamp":  float64(1700000000),
	}
	msg, err := NormalizeRawMessage(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.ID != "msg-wx-1" || msg.GroupID != "123456@chatroom" || msg.SenderID != "wxid_exampleuser1" || msg.Content != "hello world" {
		t.Fatalf("unexpected normalized message: %+v", msg)
	}
	if msg.Timestamp != 1700000000000 {
		t.Fatalf("expected ms conversion, got %d", msg.Timestamp)
	}
	msg.Raw = nil
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	for _, omitted := range []string{`"platform"`, `"account_id"`, `"parent_id"`, `"root_id"`, `"reply_context"`, `"source"`} {
		if strings.Contains(string(data), omitted) {
			t.Fatalf("expected omitted field %s in %s", omitted, string(data))
		}
	}
}

func TestNormalizeRawMessageMultiPlatformPreservation(t *testing.T) {
	raw := map[string]interface{}{
		"id":         "rpid-2002",
		"source":     "bilibili:ego",
		"platform":   "bilibili",
		"account_id": "bilibili-ego",
		"group_id":   "bilibili:BV1example0001",
		"group_name": "Example Video",
		"sender_id":  "900002",
		"senderName": "Commenter",
		"parent_id":  "rpid-1001",
		"root_id":    "rpid-1000",
		"type":       float64(1),
		"content":    "nice demo",
		"timestamp":  float64(1700000000123),
		"is_echo":    true,
		"reply_context": map[string]interface{}{
			"oid":          "123456",
			"comment_type": 1,
		},
	}
	msg, err := NormalizeRawMessage(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Source != "bilibili:ego" || msg.Platform != "bilibili" || msg.AccountID != "bilibili-ego" {
		t.Fatalf("lost platform/source/account: %+v", msg)
	}
	if msg.ParentID != "rpid-1001" || msg.RootID != "rpid-1000" || !msg.IsEcho {
		t.Fatalf("lost thread or echo fields: %+v", msg)
	}
	var ctx map[string]interface{}
	if err := json.Unmarshal(msg.ReplyContext, &ctx); err != nil {
		t.Fatalf("invalid reply_context JSON: %v", err)
	}
	if ctx["oid"] != "123456" {
		t.Fatalf("unexpected reply_context: %v", ctx)
	}

	// Also verify platform fallback for source + nested wrapper propagation + container_id extraction.
	wrapped := map[string]interface{}{
		"source":     "bilibili:ego",
		"platform":   "bilibili",
		"account_id": "bilibili-ego",
		"message": map[string]interface{}{
			"id":            "rpid-2003",
			"container_id":  "bilibili:BV1example0001",
			"sender_id":     "900003",
			"content":       "wrapped reply",
			"reply_context": `{"oid":"123456"}`,
		},
	}
	wrappedMsg, err := NormalizeRawMessage(wrapped)
	if err != nil {
		t.Fatalf("wrapped message failed: %v", err)
	}
	if wrappedMsg.GroupID != "bilibili:BV1example0001" || wrappedMsg.Source != "bilibili:ego" || wrappedMsg.Platform != "bilibili" || wrappedMsg.AccountID != "bilibili-ego" {
		t.Fatalf("wrapped message lost outer metadata: %+v", wrappedMsg)
	}
	if string(wrappedMsg.ReplyContext) != `{"oid":"123456"}` {
		t.Fatalf("unexpected compacted string reply_context: %s", string(wrappedMsg.ReplyContext))
	}
}

func TestNormalizeRawMessageRejectsInvalidOrOversizedReplyContext(t *testing.T) {
	if _, err := NormalizeRawMessage(map[string]interface{}{
		"id":            "rpid-1",
		"group_id":      "bilibili:BV1example0001",
		"content":       "hello",
		"reply_context": "{invalid-json",
	}); err == nil {
		t.Fatal("accepted invalid JSON reply_context")
	}

	oversized := map[string]interface{}{
		"oid": strings.Repeat("a", maxReplyContextBytes),
	}
	if _, err := NormalizeRawMessage(map[string]interface{}{
		"id":            "rpid-2",
		"group_id":      "bilibili:BV1example0001",
		"content":       "hello",
		"replyContext":  oversized,
	}); err == nil {
		t.Fatal("accepted oversized reply_context")
	}
}
