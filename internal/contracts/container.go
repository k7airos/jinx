package contracts

import (
	"regexp"
	"strings"
)

var (
	wechatNumericGroupPattern      = regexp.MustCompile(`^[0-9]+@chatroom$`)
	wechatRelayGroupPattern        = regexp.MustCompile(`^[A-Za-z0-9_-]+@chatroom$`)
	namespacedContainerLocalPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:+-]{0,200}$`)
)

// ParseContainerID parses a supported container identifier and returns its platform
// and platform-local identifier.
func ParseContainerID(id string) (Platform, string, bool) {
	if wechatNumericGroupPattern.MatchString(id) {
		return PlatformWeChat, id, true
	}
	if len(id) > 256 {
		return "", "", false
	}
	prefix, local, ok := strings.Cut(id, ":")
	if !ok {
		return "", "", false
	}
	platform := Platform(prefix)
	if platform != PlatformBilibili && platform != PlatformWeb {
		return "", "", false
	}
	if !namespacedContainerLocalPattern.MatchString(local) || strings.Contains(local, "..") {
		return "", "", false
	}
	return platform, local, true
}

// ValidContainerID reports whether id is a valid strict container identifier.
func ValidContainerID(id string) bool {
	_, _, ok := ParseContainerID(id)
	return ok
}

// ValidRelayContainerID reports whether id is valid in Relay configuration and
// filtering routes, including synthetic @chatroom test identifiers.
func ValidRelayContainerID(id string) bool {
	if len(id) <= 256 && wechatRelayGroupPattern.MatchString(id) {
		return true
	}
	return ValidContainerID(id)
}

// SanitizeProfileSuffix converts a container ID into a safe profile suffix.
func SanitizeProfileSuffix(id string) string {
	if strings.HasSuffix(id, "@chatroom") {
		return strings.TrimSuffix(id, "@chatroom")
	}
	var out strings.Builder
	out.Grow(len(id))
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			out.WriteRune(r)
		} else {
			out.WriteByte('-')
		}
	}
	if out.Len() == 0 {
		return "container"
	}
	return out.String()
}

// SanitizeCollectionSuffix converts a container ID into a safe collection suffix.
func SanitizeCollectionSuffix(id string) string {
	var out strings.Builder
	out.Grow(len(id))
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			out.WriteRune(r)
		} else {
			out.WriteByte('_')
		}
	}
	if out.Len() == 0 {
		return "container"
	}
	return out.String()
}
