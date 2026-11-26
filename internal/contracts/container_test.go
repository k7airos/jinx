package contracts

import (
	"strings"
	"testing"
)

func TestParseAndValidContainerID(t *testing.T) {
	valid := []struct {
		id       string
		platform Platform
		local    string
	}{
		{"100@chatroom", PlatformWeChat, "100@chatroom"},
		{"bilibili:BV1xx411c7mD", PlatformBilibili, "BV1xx411c7mD"},
		{"bilibili:video:BV1xx411c7mD", PlatformBilibili, "video:BV1xx411c7mD"},
		{"web:v2ex:topic-42", PlatformWeb, "v2ex:topic-42"},
		{"web:site.example:post+1", PlatformWeb, "site.example:post+1"},
	}
	for _, tc := range valid {
		platform, local, ok := ParseContainerID(tc.id)
		if !ok || platform != tc.platform || local != tc.local || !ValidContainerID(tc.id) {
			t.Fatalf("ParseContainerID(%q) = (%q, %q, %v), want (%q, %q, true)", tc.id, platform, local, ok, tc.platform, tc.local)
		}
	}

	invalid := []string{
		"",
		"name",
		"attacker@chatroom",
		"bad@@chatroom",
		"bilibili:",
		"bilibili:../etc",
		"bilibili:a/b",
		"Unknown:123",
		"1forum:123",
		"http:example",
		"wechat:100@chatroom",
		"bilibili:BV1xx 411",
		"bilibili:BV1xx\n411",
		"bilibili:" + strings.Repeat("a", 202),
	}
	for _, id := range invalid {
		if ValidContainerID(id) {
			t.Fatalf("ValidContainerID(%q) = true, want false", id)
		}
	}
}

func TestValidRelayContainerID(t *testing.T) {
	for _, id := range []string{"100@chatroom", "synthetic@chatroom", "demo-group@chatroom", "bilibili:BV1xx411c7mD", "web:v2ex:topic-42", "zhihu:question-1"} {
		if !ValidRelayContainerID(id) {
			t.Fatalf("ValidRelayContainerID(%q) = false, want true", id)
		}
	}
	for _, id := range []string{"", "name", "bad@@chatroom", "bilibili:../bad", "Unknown:123", "https:x"} {
		if ValidRelayContainerID(id) {
			t.Fatalf("ValidRelayContainerID(%q) = true, want false", id)
		}
	}
}

func TestSanitizeSuffixes(t *testing.T) {
	if got := SanitizeProfileSuffix("100@chatroom"); got != "100" {
		t.Fatalf("SanitizeProfileSuffix wechat = %q, want 100", got)
	}
	if got := SanitizeProfileSuffix("bilibili:BV1xx411c7mD"); got != "bilibili-BV1xx411c7mD" {
		t.Fatalf("SanitizeProfileSuffix bilibili = %q", got)
	}
	if got := SanitizeProfileSuffix("web:v2ex:topic-42"); got != "web-v2ex-topic-42" {
		t.Fatalf("SanitizeProfileSuffix web = %q", got)
	}
	if got := SanitizeCollectionSuffix("bilibili:BV1xx411c7mD"); got != "bilibili_BV1xx411c7mD" {
		t.Fatalf("SanitizeCollectionSuffix bilibili = %q", got)
	}
	if got := SanitizeCollectionSuffix("web:v2ex:topic-42"); got != "web_v2ex_topic_42" {
		t.Fatalf("SanitizeCollectionSuffix web = %q", got)
	}
}

func TestNamespacedPlatforms(t *testing.T) {
	for _, platform := range []Platform{PlatformBilibili, PlatformWeb, "zhihu", "v2ex", "my-forum"} {
		if !IsNamespacedPlatform(platform) || !ValidPlatform(platform) {
			t.Fatalf("platform %q rejected", platform)
		}
		id := string(platform) + ":thread-1"
		if got, local, ok := ParseContainerID(id); !ok || got != platform || local != "thread-1" {
			t.Fatalf("ParseContainerID(%q) = %q %q %t", id, got, local, ok)
		}
	}
	if !ValidPlatform(PlatformWeChat) || IsNamespacedPlatform(PlatformWeChat) {
		t.Fatal("wechat must be valid but not namespaced")
	}
	for _, platform := range []Platform{"", "a", "Zhihu", "1x", "zh_hu", "zh.hu", "http", "https", "ws", "wss", "file", Platform(strings.Repeat("a", 33))} {
		if IsNamespacedPlatform(platform) || ValidPlatform(platform) {
			t.Fatalf("platform %q accepted", platform)
		}
	}
}
