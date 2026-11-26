package dispatcher

import "regexp"

// Current speech only: quote metadata is never scanned. The relay's [引用消息]
// prefix labels a reply; the text following it is still the new speaker's text.
var explicitAddressee = regexp.MustCompile(`(?:^|[\s，,。；;：:！？!?\]])@[^\s@，,。；;：:！？!?]{1,40}`)

func addressedElsewhere(msg RelayMessage, cfg Config) bool {
	return explicitAddressee.MatchString(msg.Content) && !mentioned(msg.Content, cfg.Rules.BotNames)
}

func pendingForAssistant(b batch) []RelayMessage {
	var pending []RelayMessage
	for _, msg := range b.Messages {
		if !addressedElsewhere(msg, b.Config) {
			pending = append(pending, msg)
		}
	}
	return pending
}
