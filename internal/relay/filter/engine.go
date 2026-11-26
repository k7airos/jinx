package filter

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"jinx/internal/relay/config"
	"jinx/internal/relay/model"
)

var atRegex = regexp.MustCompile(`^@\S+\s*`)

// Engine evaluates incoming messages against active rules
type Engine struct {
	mu         sync.RWMutex
	regexCache map[string]*regexp.Regexp
}

// NewEngine creates an initialized filter engine
func NewEngine() *Engine {
	return &Engine{
		regexCache: make(map[string]*regexp.Regexp),
	}
}

// Evaluate evaluates a message against the supplied configuration
func (e *Engine) Evaluate(msg *model.Message, cfg config.Config) model.FilterResult {
	// 1. Group Monitoring Check
	if !cfg.GroupControl.MonitorAll {
		isMonitored := config.MatchesGroup(cfg.GroupControl.MonitoredGroups, msg.GroupID, msg.GroupName)
		if !isMonitored {
			return model.FilterResult{
				Allowed: false,
				Action:  model.ActionBlockedUnmonitored,
				Reason:  fmt.Sprintf("群 %s 未在监控名单中", msg.GroupID),
			}
		}
	}

	// 2. Ignore Echo Check
	if cfg.Filters.IgnoreEcho && msg.IsEcho {
		return model.FilterResult{
			Allowed: false,
			Action:  model.ActionBlockedSender,
			Reason:  "已配置忽略自身发出的回显消息",
		}
	}

	// 3. Sender Blocklist Check
	for _, blocked := range cfg.Filters.SenderBlocklist {
		blockedTrim := strings.TrimSpace(blocked)
		if blockedTrim != "" && (blockedTrim == msg.SenderID || blockedTrim == msg.SenderName) {
			return model.FilterResult{
				Allowed: false,
				Action:  model.ActionBlockedSender,
				Reason:  fmt.Sprintf("发送者 %s 在黑名单中", blockedTrim),
			}
		}
	}

	// 4. Allowed Types Check
	if len(cfg.Filters.AllowedTypes) > 0 {
		typeAllowed := false
		baseType := msg.Type & 0xFFFFFFFF
		for _, t := range cfg.Filters.AllowedTypes {
			if msg.Type == t || (baseType > 0 && int(baseType) == t) {
				typeAllowed = true
				break
			}
		}
		if !typeAllowed {
			return model.FilterResult{
				Allowed: false,
				Action:  model.ActionBlockedType,
				Reason:  fmt.Sprintf("消息类型 %d (%s) 不在允许转发的类型中", msg.Type, msg.TypeName),
			}
		}
	}

	// 5. Keyword Blocklist Check
	content := msg.Content
	for _, kw := range cfg.Filters.KeywordBlocklist {
		kwTrim := strings.TrimSpace(kw)
		if kwTrim != "" && strings.Contains(content, kwTrim) {
			return model.FilterResult{
				Allowed: false,
				Action:  model.ActionBlockedKeyword,
				Reason:  fmt.Sprintf("内容包含阻断关键字: [%s]", kwTrim),
			}
		}
	}

	// 6. Regex Blocklist Check
	for _, pattern := range cfg.Filters.RegexBlocklist {
		pattern = strings.TrimSpace(pattern)
		if pattern == "" {
			continue
		}
		re, err := e.getCompiledRegex(pattern)
		if err == nil && re.MatchString(content) {
			return model.FilterResult{
				Allowed: false,
				Action:  model.ActionBlockedRegex,
				Reason:  fmt.Sprintf("内容命中阻断正则: [%s]", pattern),
			}
		}
	}

	// 7. Exposed Groups Whitelist Check (Only exposed groups are relayed to AI)
	isExposed := false
	if len(cfg.GroupControl.ExposedGroups) == 0 {
		// If exposed list is empty, default to blocked (must explicitly designate exposed groups for safety)
		return model.FilterResult{
			Allowed: false,
			Action:  model.ActionBlockedUnexposed,
			Reason:  "未配置任何对外暴露群（对外暴露白名单为空）",
		}
	}

	for _, exp := range cfg.GroupControl.ExposedGroups {
		expTrim := strings.TrimSpace(exp)
		if expTrim == "*" || expTrim == msg.GroupID || (msg.GroupName != "" && expTrim == msg.GroupName) {
			isExposed = true
			break
		}
	}

	if !isExposed {
		return model.FilterResult{
			Allowed: false,
			Action:  model.ActionBlockedUnexposed,
			Reason:  fmt.Sprintf("群 %s 未加入对外暴露白名单", msg.GroupID),
		}
	}

	// 8. Clean Content if enabled
	cleaned := content
	if cfg.Filters.CleanContent {
		cleaned = cleanMessageContent(cleaned)
	}

	return model.FilterResult{
		Allowed:     true,
		Action:      model.ActionForward,
		Reason:      "群已开放且消息通过过滤，允许对外提供数据",
		CleanedText: cleaned,
	}
}

func (e *Engine) getCompiledRegex(pattern string) (*regexp.Regexp, error) {
	e.mu.RLock()
	re, ok := e.regexCache[pattern]
	e.mu.RUnlock()
	if ok {
		return re, nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	// double check
	if re, ok := e.regexCache[pattern]; ok {
		return re, nil
	}
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}
	if len(e.regexCache) >= 512 {
		clear(e.regexCache)
	}
	e.regexCache[pattern] = compiled
	return compiled, nil
}

func cleanMessageContent(s string) string {
	// Remove leading @username
	res := atRegex.ReplaceAllString(s, "")
	return strings.TrimSpace(res)
}
