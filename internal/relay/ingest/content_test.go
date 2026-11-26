package ingest

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeMediaTypeEncodings(t *testing.T) {
	for _, value := range []interface{}{3, float64(3), "3", json.Number("3")} {
		msg, err := NormalizeRawMessage(map[string]interface{}{"type": value, "group_id": "test@chatroom"})
		if err != nil || msg.Type != 3 || msg.TypeName != "图片" {
			t.Fatalf("image type %v normalized as %+v (%v)", value, msg, err)
		}
	}
}

func TestSummarizeContentPreservesCardTitleAndUnknownTypes(t *testing.T) {
	for _, tc := range []struct {
		messageType int
		content     string
		want        string
	}{
		{244813135921, "<msg><appmsg><title><![CDATA[引用 <内容> & 标题]]></title></appmsg></msg>", "[引用消息] 引用 <内容> & 标题"},
		{49, "<msg><appmsg><title>文件 &amp; 链接</title></appmsg></msg>", "[卡片/应用/链接] 文件 & 链接"},
		{3, "<msg><img/></msg>", "[图片]"},
		{34, "", "[语音]"},
		{43, "", "[视频]"},
		{47, "", "[表情]"},
		{48, "", "[位置]"},
		{999, "", "[类型(999)]"},
		{999, "未知类型的原始内容", "未知类型的原始内容"},
	} {
		if got := SummarizeContent(tc.messageType, tc.content); got != tc.want {
			t.Errorf("type %d: got %q, want %q", tc.messageType, got, tc.want)
		}
	}
}

func TestStructuredDetailsAndMediaKeyRedaction(t *testing.T) {
	for _, tc := range []struct {
		typ       int
		xml, kind string
	}{
		{49, `<msg><appmsg><type>5</type><title>标题</title><des>说明</des><url>https://example.com/a</url></appmsg></msg>`, "link"},
		{49, `<msg><appmsg><type>6</type><title>report.txt</title><appattach><totallen>12</totallen></appattach></appmsg></msg>`, "file"},
		{49, `<msg><appmsg><type>33</type><title>小程序</title><url>javascript:alert(1)</url></appmsg></msg>`, "miniapp"},
		{34, `<msg><voicemsg voicelength="2500" aeskey="private-value"/></msg>`, "voice"},
	} {
		d := ParseMessageDetails(tc.typ, tc.xml)
		if d.Kind != tc.kind {
			t.Fatalf("wrong kind: %+v", d)
		}
		if tc.kind == "voice" && d.DurationSeconds != 2.5 {
			t.Fatal("voice duration not decoded")
		}
		if tc.kind == "file" && (d.FileName != "report.txt" || d.FileSize != 12) {
			t.Fatal("file metadata missing")
		}
		if tc.kind == "link" && (d.URL != "https://example.com/a" || d.Description != "说明") {
			t.Fatal("link metadata missing")
		}
		if tc.kind == "miniapp" && d.URL != "" {
			t.Fatal("unsafe URL retained")
		}
	}
	raw := `<msg aeskey="private-value"><cdnthumbaeskey><![CDATA[private-value]]></cdnthumbaeskey><title>visible</title></msg>`
	msg, _ := NormalizeRawMessage(map[string]interface{}{"type": 3, "group_id": "test@chatroom", "content": raw, "nested": map[string]interface{}{"enc_key": "private-value"}})
	SanitizeMessage(msg)
	data, _ := json.Marshal(msg)
	if strings.Contains(string(data), "private-value") || !strings.Contains(string(data), "visible") {
		t.Fatalf("media keys leaked or content removed: %s", data)
	}
}
