package filter

import (
	"testing"

	"jinx/internal/relay/config"
	"jinx/internal/relay/model"
)

func TestFilterEngine(t *testing.T) {
	engine := NewEngine()

	cfg := config.DefaultConfig()
	cfg.GroupControl.MonitorAll = true
	cfg.GroupControl.ExposedGroups = []string{"exposed-group@chatroom"}
	cfg.Filters.KeywordBlocklist = []string{"拍了拍", "[微信红包]"}
	cfg.Filters.SenderBlocklist = []string{"spammer_wxid"}
	cfg.Filters.AllowedTypes = []int{model.TypeText, model.TypeQuote}

	// Case 1: Normal text in exposed group -> should pass
	msg1 := &model.Message{
		GroupID:  "exposed-group@chatroom",
		SenderID: "good_user",
		Type:     model.TypeText,
		Content:  "今天讨论一下大模型架构",
	}
	res1 := engine.Evaluate(msg1, cfg)
	if !res1.Allowed || res1.Action != model.ActionForward {
		t.Fatalf("msg1 expected forward, got allowed=%v action=%s", res1.Allowed, res1.Action)
	}

	// Case 2: Message from unexposed group -> should be blocked unexposed
	msg2 := &model.Message{
		GroupID:  "private-group@chatroom",
		SenderID: "good_user",
		Type:     model.TypeText,
		Content:  "内部绝密信息",
	}
	res2 := engine.Evaluate(msg2, cfg)
	if res2.Allowed || res2.Action != model.ActionBlockedUnexposed {
		t.Fatalf("msg2 expected blocked_unexposed, got allowed=%v action=%s", res2.Allowed, res2.Action)
	}

	// Case 3: Message containing keyword "拍了拍" -> should be blocked keyword
	msg3 := &model.Message{
		GroupID:  "exposed-group@chatroom",
		SenderID: "good_user",
		Type:     model.TypeText,
		Content:  "张三 拍了拍 李四 的肩膀",
	}
	res3 := engine.Evaluate(msg3, cfg)
	if res3.Allowed || res3.Action != model.ActionBlockedKeyword {
		t.Fatalf("msg3 expected blocked_keyword, got allowed=%v action=%s", res3.Allowed, res3.Action)
	}

	// Case 4: Message from blocked sender -> should be blocked sender
	msg4 := &model.Message{
		GroupID:  "exposed-group@chatroom",
		SenderID: "spammer_wxid",
		Type:     model.TypeText,
		Content:  "点击领取礼品",
	}
	res4 := engine.Evaluate(msg4, cfg)
	if res4.Allowed || res4.Action != model.ActionBlockedSender {
		t.Fatalf("msg4 expected blocked_sender, got allowed=%v action=%s", res4.Allowed, res4.Action)
	}

	// Case 5: Message with image type (3) -> should be blocked type
	msg5 := &model.Message{
		GroupID:  "exposed-group@chatroom",
		SenderID: "good_user",
		Type:     model.TypeImage,
		Content:  "[图片]",
	}
	res5 := engine.Evaluate(msg5, cfg)
	if res5.Allowed || res5.Action != model.ActionBlockedType {
		t.Fatalf("msg5 expected blocked_type, got allowed=%v action=%s", res5.Allowed, res5.Action)
	}
}
