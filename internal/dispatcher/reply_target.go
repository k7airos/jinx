package dispatcher

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"jinx/internal/contracts"
)

// Reply-target selection for threaded (non-WeChat) platforms.
//
// A batch may hold several comments. The model only ever sees a batch-local
// ref number (1..N) per pending message and names the one it answers with a
// first line "REPLY_TO: <ref>". The dispatcher resolves that ref back to the
// message it already stored, so the platform IDs and reply_context used for
// delivery come from inbound evidence, never from model output. A missing,
// malformed or out-of-range ref fails closed instead of guessing.

const (
	reasonReplyTargetMissing = "reply_target_missing"
	reasonReplyTargetInvalid = "reply_target_invalid"
)

var (
	replyTargetPrefix = regexp.MustCompile(`(?i)^REPLY_TO\b`)
	replyTargetLine   = regexp.MustCompile(`(?i)^REPLY_TO\s*[:：]\s*#?\s*([0-9]{1,4})(?:\s+(.*))?$`)
)

// promptPendingMessage is a pending message as shown to the model on threaded
// platforms: the stored message plus its batch-local ref.
type promptPendingMessage struct {
	Ref int `json:"ref"`
	RelayMessage
}

func usesReplyTargetSelection(b batch) bool {
	if b.Direct {
		return false
	}
	platform, _, ok := contracts.ParseContainerID(b.Rule.TargetGroupID)
	return ok && platform != contracts.PlatformWeChat
}

func promptPendingMessages(messages []RelayMessage) []promptPendingMessage {
	out := make([]promptPendingMessage, len(messages))
	for i, msg := range messages {
		out[i] = promptPendingMessage{Ref: i + 1, RelayMessage: msg}
	}
	return out
}

func replyTargetInstruction(pending int) string {
	if pending > 1 {
		return fmt.Sprintf("pending_messages 共 %d 条，每条带 ref 编号。若决定回复，只能选其中一条：第一行单独写 REPLY_TO: <ref>（例如 REPLY_TO: 2），第二行起写回复正文；ref 只能取 pending_messages 里出现的编号，不要写平台 ID。缺少或写错 REPLY_TO 时不会发送。不回复则只输出 NONE。", pending)
	}
	return "pending_messages 只有 1 条（ref 为 1），回复即针对这一条，可省略 REPLY_TO 行。"
}

// splitReplyTarget removes a leading REPLY_TO directive ("REPLY_TO: 2" on its
// own line, or followed by the reply on the same line). present reports whether
// the first line looked like a directive at all; ref is 0 when it was malformed.
func splitReplyTarget(text string) (ref int, body string, present bool) {
	first, rest, _ := strings.Cut(text, "\n")
	first = strings.TrimSpace(first)
	if !replyTargetPrefix.MatchString(first) {
		return 0, text, false
	}
	body = strings.TrimSpace(rest)
	match := replyTargetLine.FindStringSubmatch(first)
	if match == nil {
		return 0, body, true
	}
	if inline := strings.TrimSpace(match[2]); inline != "" {
		body = strings.TrimSpace(inline + "\n" + body)
	}
	value, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, body, true
	}
	return value, body, true
}

// resolveReplyTarget validates a parsed ref against the batch size. It returns
// the 1-based index to deliver to, or a fail-closed reason code.
func resolveReplyTarget(ref int, present bool, pending int) (int, string) {
	if present {
		if ref < 1 || ref > pending {
			return 0, reasonReplyTargetInvalid
		}
		return ref, ""
	}
	if pending == 1 {
		return 1, ""
	}
	return 0, reasonReplyTargetMissing
}
