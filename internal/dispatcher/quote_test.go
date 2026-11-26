package dispatcher

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestQuoteSurvivesWireToPrompt(t *testing.T) {
	var msg RelayMessage
	err := json.Unmarshal([]byte(`{"content":"参考这个","details":{"quote":{"type":1,"author":"原作者","content":"引用原文 @DEMOBOT"}}}`), &msg)
	if err != nil {
		t.Fatal(err)
	}
	b := batch{Messages: []RelayMessage{msg}}
	prompt := buildPrompt(b)
	if !strings.Contains(prompt, "原作者") || !strings.Contains(prompt, "引用原文 @DEMOBOT") {
		t.Fatal("quote missing from generation prompt")
	}
	if mentioned(msg.Content, []string{"DEMOBOT"}) {
		t.Fatal("quoted mention triggered bot")
	}
}
