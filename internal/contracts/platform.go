package contracts

import "regexp"

// namespacedPlatformPattern is the syntax for every non-WeChat platform name.
// Built-in names (bilibili, web) satisfy it; operators may add others such as
// "zhihu" or "v2ex" without code changes. A syntactically valid platform does
// not establish an installed adapter or capability: that still requires an
// explicitly configured connector (dispatcher action_endpoints, control-plane
// -connector) whose /health proves readiness.
var namespacedPlatformPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{1,31}$`)

// reservedPlatformNames cannot be used as namespaced platforms: "wechat" has its
// own @chatroom container syntax, and URL schemes would make container IDs
// ambiguous with URLs.
var reservedPlatformNames = map[Platform]bool{
	PlatformWeChat: true,
	"http":         true,
	"https":        true,
	"ws":           true,
	"wss":          true,
	"file":         true,
}

// IsNamespacedPlatform reports whether value can prefix a "<platform>:<local>"
// container ID, i.e. it is any valid platform other than WeChat.
func IsNamespacedPlatform(value Platform) bool {
	return namespacedPlatformPattern.MatchString(string(value)) && !reservedPlatformNames[value]
}

// ValidPlatform reports whether value is WeChat or a valid namespaced platform.
func ValidPlatform(value Platform) bool {
	return value == PlatformWeChat || IsNamespacedPlatform(value)
}
