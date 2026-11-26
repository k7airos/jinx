package ingest

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"jinx/internal/relay/model"
)

func ParseMessageDetails(messageType int, content string) *model.MessageDetails {
	base := messageType & 0xFFFFFFFF
	kinds := map[int]string{model.TypeText: "text", model.TypeImage: "image", model.TypeVoice: "voice", model.TypeVideo: "video", model.TypeEmoji: "emoji", model.TypeLocation: "location", model.TypeApp: "app", model.TypeQuote: "quote", model.TypeSystem: "system", 10002: "system"}
	details := &model.MessageDetails{Kind: kinds[base]}
	if details.Kind == "" {
		details.Kind = "unknown"
	}
	subType := messageType >> 32
	decoder := xml.NewDecoder(strings.NewReader(content))
	for {
		token, err := decoder.Token()
		if err != nil {
			break
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		if start.Name.Local == "voicemsg" && base == model.TypeVoice {
			for _, attr := range start.Attr {
				if attr.Name.Local == "voicelength" {
					ms, _ := strconv.ParseFloat(attr.Value, 64)
					if ms > 0 {
						details.DurationSeconds = ms / 1000
					}
				}
			}
		}
		if start.Name.Local != "appmsg" || (base != model.TypeApp && base != model.TypeQuote) {
			continue
		}
		var app struct {
			Refer struct {
				Type    int    `xml:"type"`
				Author  string `xml:"displayname"`
				Content string `xml:"content"`
			} `xml:"refermsg"`
			Type        int    `xml:"type"`
			Title       string `xml:"title"`
			Description string `xml:"des"`
			URL         string `xml:"url"`
			Attach      struct {
				Length    int64  `xml:"totallen"`
				Extension string `xml:"fileext"`
			} `xml:"appattach"`
		}
		if decoder.DecodeElement(&app, &start) != nil {
			break
		}
		if app.Type > 0 {
			subType = app.Type
		}
		if subType == 57 && (app.Refer.Content != "" || app.Refer.Author != "") {
			body := app.Refer.Content
			// Text quotes contain plain text; non-text quotes must not leak raw attachment XML.
			if app.Refer.Type != 1 && strings.HasPrefix(strings.TrimSpace(body), "<") {
				body = SummarizeContent(app.Refer.Type, body)
			}
			runes := []rune(redactContent(body))
			if len(runes) > 8000 {
				runes = append(runes[:8000], []rune("[引用已截短]")...)
			}
			author := []rune(app.Refer.Author)
			if len(author) > 200 {
				author = author[:200]
			}
			details.Quote = &model.QuotedMessage{Type: app.Refer.Type, Author: string(author), Content: string(runes)}
		}
		details.Title = app.Title
		details.Description = app.Description
		if parsed, err := url.Parse(app.URL); err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" {
			details.URL = app.URL
		}
		if subType == 6 {
			details.FileName = app.Title
			details.FileSize = app.Attach.Length
		}
		break
	}
	if base == model.TypeApp {
		switch subType {
		case 5:
			details.Kind = "link"
		case 6:
			details.Kind = "file"
		case 33, 36, 44:
			details.Kind = "miniapp"
		case 57:
			details.Kind = "quote"
		}
	}
	return details
}

// SummarizeContent runs after filters evaluate the decoded original content.
func SummarizeContent(messageType int, content string) string {
	details := ParseMessageDetails(messageType, content)
	if strings.HasPrefix(content, "[") || details.Kind == "text" || details.Kind == "system" {
		return content
	}
	label := GetTypeName(messageType)
	switch details.Kind {
	case "link":
		label = "链接"
	case "file":
		label = "文件"
	case "miniapp":
		label = "小程序"
	case "quote":
		label = "引用消息"
	}
	if details.Kind == "voice" && details.DurationSeconds > 0 {
		return fmt.Sprintf("[语音，%.1f 秒]", details.DurationSeconds)
	}
	if details.Title != "" {
		return "[" + label + "] " + details.Title
	}
	switch details.Kind {
	case "image", "voice", "video", "emoji", "location":
		return "[" + label + "]"
	}
	if content == "" || strings.HasPrefix(strings.TrimSpace(content), "<") {
		return "[" + label + "]"
	}
	return content
}

var secretAttribute = regexp.MustCompile(`(?i)(\b(?:aeskey|cdnthumbaeskey|encryptkey|enc_key)\s*=\s*)(?:"[^"]*"|'[^']*')`)
var secretElement = regexp.MustCompile(`(?is)(<(?:aeskey|cdnthumbaeskey|encryptkey|enc_key)(?:\s[^>]*)?>).*?(</(?:aeskey|cdnthumbaeskey|encryptkey|enc_key)>)`)

func redactContent(content string) string {
	content = secretAttribute.ReplaceAllString(content, `${1}"[已隐藏]"`)
	return secretElement.ReplaceAllString(content, `${1}[已隐藏]${2}`)
}
func sanitizeValue(value interface{}) interface{} {
	switch item := value.(type) {
	case string:
		return redactContent(item)
	case map[string]interface{}:
		result := make(map[string]interface{}, len(item))
		for key, val := range item {
			switch strings.ToLower(key) {
			case "aeskey", "cdnthumbaeskey", "encryptkey", "enc_key":
				result[key] = "[已隐藏]"
			default:
				result[key] = sanitizeValue(val)
			}
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(item))
		for i, val := range item {
			result[i] = sanitizeValue(val)
		}
		return result
	default:
		return value
	}
}
func SanitizeMessage(msg *model.Message) {
	msg.Content = redactContent(msg.Content)
	msg.RawContent = redactContent(msg.RawContent)
	if msg.Raw != nil {
		msg.Raw = sanitizeValue(msg.Raw).(map[string]interface{})
	}
}
