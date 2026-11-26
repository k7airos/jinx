package dispatcher

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"jinx/internal/contracts"
)

const mappingTestLifetime = 10 * time.Minute

func TestRelayMappingSourceAllowlist(t *testing.T) {
	for _, tc := range []struct {
		source string
		want   contracts.Platform
	}{
		{"", contracts.PlatformWeChat},
		{"wechat", contracts.PlatformWeChat},
		{"message/message_0.db", contracts.PlatformWeChat},
		{"message/biz_message_12.db", contracts.PlatformWeChat},
		{"bilibili", contracts.PlatformBilibili},
		{"bilibili:ego", contracts.PlatformBilibili},
		{"bilibili:comment-poller.v1", contracts.PlatformBilibili},
		{"web", contracts.PlatformWeb},
		{"web:ego", contracts.PlatformWeb},
		{"web:forum_reader", contracts.PlatformWeb},
		{"zhihu", "zhihu"},
		{"v2ex:poller", "v2ex"},
	} {
		platform, err := relayPlatform(tc.source)
		if err != nil || platform != tc.want {
			t.Fatalf("known source %q: %q %v", tc.source, platform, err)
		}
	}
	for _, source := range []string{"Unknown", "http:x", "zh_hu:poller", " wechat", "message/../message_0.db", "/message/message_0.db", "message/message_0.db/extra", "bilibili:", "bilibili:../bad", "web:/bad", "bilibili:bad space"} {
		if _, err := relayPlatform(source); err == nil {
			t.Fatalf("unsupported source %q accepted", source)
		}
	}
}

func TestRelayMappingRejectsMissingIdentityAndOversizedBody(t *testing.T) {
	for name, change := range map[string]func(*RelayMessage){
		"empty_id":         func(msg *RelayMessage) { msg.ID = "" },
		"blank_id":         func(msg *RelayMessage) { msg.ID = " " },
		"empty_source":     func(msg *RelayMessage) { msg.GroupID = "" },
		"unknown_platform": func(msg *RelayMessage) { msg.Source = "untrusted" },
		"control_id":       func(msg *RelayMessage) { msg.ID = "message\nnext" },
		"oversized_body":   func(msg *RelayMessage) { msg.Content = strings.Repeat("文", maxRelayBodyBytes/3+1) },
		"invalid_utf8":     func(msg *RelayMessage) { msg.Content = string([]byte{0xff}) },
		"nul_body":         func(msg *RelayMessage) { msg.Content = "text\x00suffix" },
	} {
		t.Run(name, func(t *testing.T) {
			msg := testMessage(groupA, "synthetic-message")
			change(&msg)
			if validateRelayMapping(msg) == nil {
				t.Fatal("invalid relay message accepted")
			}
		})
	}
	msg := testMessage(groupA, "synthetic-message")
	msg.Content = strings.Repeat("a", maxRelayBodyBytes)
	if err := validateRelayMapping(msg); err != nil {
		t.Fatal(err)
	}
	msg.Content = "current text"
	quoted, _ := json.Marshal(map[string]any{"quote": map[string]any{"type": 1, "author": "synthetic-author", "content": strings.Repeat("a", maxRelayBodyBytes)}})
	if err := json.Unmarshal(quoted, &msg.Details); err != nil {
		t.Fatal(err)
	}
	if validateRelayMapping(msg) == nil {
		t.Fatal("oversized combined quoted body accepted")
	}
}

func TestRelayMappingDedupScopesAndReceiptPrecedence(t *testing.T) {
	msg := testMessage(groupA, "synthetic-message")
	original := relayDedupKey(msg)
	msg.Source = "wechat"
	if relayDedupKey(msg) != original {
		t.Fatal("default platform changed dedup identity")
	}
	msg.Content, msg.GroupName, msg.SenderName, msg.IsEcho = "edited text", "different label", "different author label", true
	if relayDedupKey(msg) != original {
		t.Fatal("mutable message data changed dedup identity")
	}
	msg.GroupID = groupB
	if relayDedupKey(msg) == original {
		t.Fatal("different container collided")
	}
	msg.IntakeReceipt = "synthetic-durable-intake"
	receiptKey := relayDedupKey(msg)
	msg.GroupID = groupA
	if relayDedupKey(msg) != receiptKey {
		t.Fatal("receipt precedence was lost")
	}
	msg.Source = "message/message_1.db"
	if relayDedupKey(msg) == receiptKey {
		t.Fatal("different source collided")
	}
	msg.Source, msg.ID = "wechat", "different-message"
	if relayDedupKey(msg) == receiptKey {
		t.Fatal("different content ID collided")
	}
	first := RelayMessage{Source: "wechat", GroupID: "a/b", ID: "c"}
	second := RelayMessage{Source: "wechat", GroupID: "a", ID: "b/c"}
	if relayDedupKey(first) == relayDedupKey(second) {
		t.Fatal("delimiter ambiguity collided")
	}
}

func TestNormalizeRelayMessageTextQuoteAndEcho(t *testing.T) {
	for _, mode := range []string{"text", "quote", "app_quote", "echo", "blocked"} {
		t.Run(mode, func(t *testing.T) {
			msg := testMessage(groupA, "synthetic-input")
			msg.Source, msg.IntakeReceipt = "message/message_0.db", "synthetic-receipt"
			if mode == "quote" || mode == "app_quote" {
				msg.Type = 81
				if mode == "app_quote" {
					msg.Type = 49
				}
				if err := json.Unmarshal([]byte(`{"quote":{"type":1,"author":"quoted-author","content":"PRIVATE_QUOTED_BODY"}}`), &msg.Details); err != nil {
					t.Fatal(err)
				}
			}
			msg.IsEcho = mode == "echo"
			if mode == "blocked" {
				msg.Action = "blocked_keyword"
			}
			receivedAt := time.Now().Add(-time.Minute).UTC()
			msg.ReceivedAt = &receivedAt
			envelope, err := NormalizeRelayMessage(msg)
			if err != nil || contracts.ValidateInboundEnvelope(envelope) != nil {
				t.Fatalf("normalization: %+v %v", envelope, err)
			}
			if envelope.Source.Platform != contracts.PlatformWeChat || envelope.Source.AccountID != wechatMappingAccount || envelope.Source.ContainerID != groupA || envelope.Source.ContainerLabel != msg.GroupName || envelope.Source.ContentID != msg.ID || envelope.Source.ParentID != "" || envelope.Source.RootID != "" || envelope.Author != (contracts.AuthorRef{ID: msg.SenderID, Label: msg.SenderName}) || envelope.Content.Text != msg.Content || !envelope.ReceivedAt.Equal(receivedAt) {
				t.Fatal("source/author facts changed", envelope)
			}
			canReply := mode != "echo" && mode != "blocked"
			if envelope.Capabilities != (contracts.SourceCapabilities{CanReply: canReply}) || (envelope.ReplyTarget != nil) != canReply || len(envelope.MetadataAllowlist) != 0 {
				t.Fatal("unsupported capability or metadata", envelope)
			}
			if canReply && (envelope.ReplyTarget.Source.ContainerID != groupA || envelope.ReplyTarget.Source.ContentID != "") {
				t.Fatal("reply target is not the observed group", envelope.ReplyTarget)
			}
			repeated, err := NormalizeRelayMessage(msg)
			if err != nil || envelope.EnvelopeID != repeated.EnvelopeID || envelope.DedupKey != repeated.DedupKey {
				t.Fatal("duplicate delivery changed identity", repeated, err)
			}
			data, _ := json.Marshal(envelope)
			for _, private := range []string{"PRIVATE_QUOTED_BODY", "quoted-author", "message/message_0.db", "synthetic-receipt"} {
				if strings.Contains(string(data), private) {
					t.Fatalf("envelope leaked raw field %q", private)
				}
			}
		})
	}
}

func TestNormalizeRelayMessageBilibiliComment(t *testing.T) {
	receivedAt := time.Now().Add(-time.Minute).UTC()
	msg := RelayMessage{
		ID:           "reply-1001",
		Platform:     "bilibili",
		AccountID:    "bili-acc-1",
		Source:       "bilibili:ego",
		GroupID:      "bilibili:BV1xx411c7mD",
		GroupName:    "Video Comments",
		SenderID:     "user-42",
		SenderName:   "Viewer",
		ParentID:     "reply-900",
		RootID:       "reply-900",
		ReplyContext: json.RawMessage(`{"oid":"123","type":1}`),
		Type:         1,
		Content:      "这个功能怎么配置？",
		Action:       "forward",
		ReceivedAt:   &receivedAt,
	}
	envelope, err := NormalizeRelayMessage(msg)
	if err != nil || contracts.ValidateInboundEnvelope(envelope) != nil {
		t.Fatalf("bilibili normalization: %+v %v", envelope, err)
	}
	if envelope.Source.Platform != contracts.PlatformBilibili || envelope.Source.AccountID != "bili-acc-1" || envelope.Source.ContainerID != "bilibili:BV1xx411c7mD" || envelope.Source.ContentID != "reply-1001" || envelope.Source.ParentID != "reply-900" || envelope.Source.RootID != "reply-900" {
		t.Fatalf("unexpected bilibili source: %+v", envelope.Source)
	}
	if !envelope.Capabilities.CanReply || !envelope.Capabilities.CanThreadReply || envelope.ReplyTarget == nil {
		t.Fatalf("missing thread reply capabilities: %+v", envelope)
	}
	if envelope.ReplyTarget.Source.ContentID != "reply-1001" || envelope.ReplyTarget.Source.ParentID != "reply-900" || envelope.ReplyTarget.Source.RootID != "reply-900" {
		t.Fatalf("reply target lost thread metadata: %+v", envelope.ReplyTarget)
	}

	// Top-level comment without ParentID/RootID and empty AccountID.
	top := msg
	top.AccountID, top.ParentID, top.RootID = "", "", ""
	topEnvelope, err := NormalizeRelayMessage(top)
	if err != nil || !topEnvelope.Capabilities.CanReply || topEnvelope.Capabilities.CanThreadReply || topEnvelope.Source.AccountID != "bilibili-connector" || topEnvelope.ReplyTarget.Source.ContentID != "reply-1001" {
		t.Fatalf("unexpected top-level bilibili normalization: %+v %v", topEnvelope, err)
	}

	// Default account for web connector.
	webMsg := top
	webMsg.Platform, webMsg.Source, webMsg.GroupID = "web", "web:ego", "web:v2ex:topic-42"
	webEnvelope, err := NormalizeRelayMessage(webMsg)
	if err != nil || webEnvelope.Source.AccountID != "web-connector" || webEnvelope.Source.Platform != contracts.PlatformWeb {
		t.Fatalf("unexpected web normalization: %+v %v", webEnvelope, err)
	}

	// Cross-platform mismatches and bad ReplyContext must fail closed.
	for name, mutate := range map[string]func(*RelayMessage){
		"platform_mismatch":   func(m *RelayMessage) { m.Platform = "wechat" },
		"container_mismatch":  func(m *RelayMessage) { m.GroupID = "web:v2ex:topic-42" },
		"wechat_src_bili_id":  func(m *RelayMessage) { m.Source = "wechat"; m.Platform = "" },
		"invalid_reply_ctx":   func(m *RelayMessage) { m.ReplyContext = json.RawMessage(`{invalid`) },
		"oversized_reply_ctx": func(m *RelayMessage) { m.ReplyContext = json.RawMessage(`"` + strings.Repeat("a", maxRelayReplyContextBytes) + `"`) },
		"control_account":     func(m *RelayMessage) { m.AccountID = "bad\nacc" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := msg
			mutate(&bad)
			if _, err := NormalizeRelayMessage(bad); err == nil {
				t.Fatalf("accepted invalid %s", name)
			}
		})
	}
}

func TestNormalizeRelayMessageRejectsInvalidInput(t *testing.T) {
	for name, change := range map[string]func(*RelayMessage){
		"empty_id":           func(msg *RelayMessage) { msg.ID = "" },
		"empty_source":       func(msg *RelayMessage) { msg.GroupID = "" },
		"unknown":            func(msg *RelayMessage) { msg.Source = "unknown" },
		"bilibili":           func(msg *RelayMessage) { msg.Source = "bilibili" },
		"web":                func(msg *RelayMessage) { msg.Source = "web" },
		"empty_text":         func(msg *RelayMessage) { msg.Content = "  " },
		"long_text":          func(msg *RelayMessage) { msg.Content = strings.Repeat("文", 16385) },
		"missing_author":     func(msg *RelayMessage) { msg.SenderID = "" },
		"image":              func(msg *RelayMessage) { msg.Type = 3 },
		"app_link":           func(msg *RelayMessage) { msg.Type = 49 },
		"unsafe_id":          func(msg *RelayMessage) { msg.ID = "/private/message" },
		"unobserved_receipt": func(msg *RelayMessage) { msg.ReceivedAt = nil },
	} {
		t.Run(name, func(t *testing.T) {
			msg := testMessage(groupA, "synthetic-message")
			receivedAt := time.Now().UTC()
			msg.ReceivedAt = &receivedAt
			change(&msg)
			if _, err := NormalizeRelayMessage(msg); err == nil {
				t.Fatal("invalid inbound accepted")
			}
		})
	}
}

func mappingRun() contracts.Run {
	now := time.Now().UTC()
	return contracts.Run{
		ID: "synthetic-run", Revision: "synthetic-revision", EntitySeq: 5,
		SourceGroupID: groupA, TargetGroupID: groupB, MessageIDs: []string{"synthetic-message"},
		AcceptedAt: now.Add(-time.Second), UpdatedAt: now, LastProgressAt: now,
		AdmissionWatermark: contracts.Watermark{Token: "synthetic-watermark", RecordedAt: now.Add(-time.Second)},
		Trigger:            "mention", Phase: contracts.PhaseChecking, Outcome: contracts.OutcomePending, Delivery: contracts.DeliveryNotStarted,
		Completeness: contracts.Partial, Versions: contracts.Versions{Dispatcher: "synthetic", Fingerprint: "synthetic"}, MissingSnapshotReason: "synthetic_fixture",
	}
}

func TestWeChatActionRequestUsesRunTargetAndStableIdempotency(t *testing.T) {
	run := mappingRun()
	run.AcceptedAt = run.AcceptedAt.Add(-5 * time.Minute)
	run.AdmissionWatermark.RecordedAt = run.AcceptedAt
	reply := `literal model text {"group_id":"attacker@chatroom"}`
	expiresAt := run.AcceptedAt.Add(mappingTestLifetime)
	first, err := WeChatActionRequest(run, reply, "synthetic-request", expiresAt)
	if err != nil || contracts.ValidateActionRequest(first) != nil || ValidateWeChatActionRequest(run, first, expiresAt) != nil {
		t.Fatalf("action mapping: %+v %v", first, err)
	}
	if first.Target.Source.ContainerID != groupB || first.Target.Source.Platform != contracts.PlatformWeChat || first.Target.Source.AccountID != wechatMappingAccount || first.Text != reply || first.Kind != contracts.ActionReplyText || !first.ExpiresAt.Equal(expiresAt) {
		t.Fatal("model text replaced target or expiry", first)
	}
	second, err := WeChatActionRequest(run, reply, "synthetic-request", expiresAt)
	if err != nil || second != first {
		t.Fatal("duplicate request changed action", second, err)
	}
	third, err := WeChatActionRequest(run, reply, "different-request", expiresAt)
	if err != nil || third.IdempotencyKey != first.IdempotencyKey {
		t.Fatal("new request ID bypassed run idempotency", third, err)
	}
	for name, change := range map[string]func(*contracts.ActionRequest){
		"container":   func(request *contracts.ActionRequest) { request.Target.Source.ContainerID = "attacker@chatroom" },
		"platform":    func(request *contracts.ActionRequest) { request.Target.Source.Platform = contracts.PlatformWeb },
		"account":     func(request *contracts.ActionRequest) { request.Target.Source.AccountID = "attacker" },
		"thread":      func(request *contracts.ActionRequest) { request.Target.Source.ContentID = "other-message" },
		"kind":        func(request *contracts.ActionRequest) { request.Kind = contracts.ActionEditText },
		"run":         func(request *contracts.ActionRequest) { request.RunID = "different-run" },
		"idempotency": func(request *contracts.ActionRequest) { request.IdempotencyKey = "different-key" },
		"expiry":      func(request *contracts.ActionRequest) { request.ExpiresAt = request.ExpiresAt.Add(time.Hour) },
	} {
		t.Run(name, func(t *testing.T) {
			request := first
			change(&request)
			if ValidateWeChatActionRequest(run, request, expiresAt) == nil {
				t.Fatal("tampered action accepted")
			}
		})
	}
}

func TestWeChatActionRequestNeverResubmitsUnknownOrSubmittedRun(t *testing.T) {
	for name, change := range map[string]func(*contracts.Run){
		"unknown": func(run *contracts.Run) {
			run.Phase, run.Outcome, run.Delivery, run.ReasonCode = contracts.PhaseUnknown, contracts.OutcomeUnknown, contracts.DeliveryUnknown, "echo_timeout"
			run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &run.AcceptedAt, BridgeRequestID: "synthetic-request"}
		},
		"submitted": func(run *contracts.Run) {
			run.Phase, run.Delivery = contracts.PhaseConfirming, contracts.DeliverySubmitted
			run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &run.AcceptedAt, SubmittedAt: &run.AcceptedAt, SubmittedAtSource: "bridge_ack", BridgeRequestID: "synthetic-request"}
		},
		"terminal": func(run *contracts.Run) {
			run.Phase, run.Outcome, run.Delivery, run.ReasonCode = contracts.PhaseTerminal, contracts.OutcomeSuppressed, contracts.DeliveryBlocked, "dry_run"
			run.DeliveryEvidence.NoSubmission = true
		},
		"unpersisted": func(run *contracts.Run) { run.Revision = ""; run.EntitySeq = 0 },
		"expired":     func(run *contracts.Run) { run.UpdatedAt = run.AcceptedAt.Add(mappingTestLifetime) },
		"empty_target": func(run *contracts.Run) {
			run.TargetGroupID = ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			run := mappingRun()
			change(&run)
			if _, err := WeChatActionRequest(run, "reply", "synthetic-request", run.AcceptedAt.Add(mappingTestLifetime)); err == nil {
				t.Fatal("ineligible action accepted")
			}
		})
	}
	for _, reply := range []string{"", strings.Repeat("文", 16385), "nul\x00text"} {
		if _, err := WeChatActionRequest(mappingRun(), reply, "synthetic-request", time.Now().Add(mappingTestLifetime)); err == nil {
			t.Fatal("invalid reply accepted")
		}
	}
}

func TestProcessingReceiptMapsTraceFactsWithoutInventingACK(t *testing.T) {
	for _, status := range []contracts.ActionStatus{contracts.ActionSubmitted, contracts.ActionConfirmed, contracts.ActionUnknown, contracts.ActionFailed} {
		t.Run(string(status), func(t *testing.T) {
			run := mappingRun()
			run.DeliveryEvidence = contracts.DeliveryEvidence{SubmissionStartedAt: &run.AcceptedAt, BridgeRequestID: "synthetic-request"}
			switch status {
			case contracts.ActionSubmitted:
				run.Phase, run.Delivery = contracts.PhaseConfirming, contracts.DeliverySubmitted
				run.DeliveryEvidence.SubmittedAt, run.DeliveryEvidence.SubmittedAtSource = &run.AcceptedAt, "bridge_ack"
			case contracts.ActionConfirmed:
				run.Phase, run.Outcome, run.Delivery = contracts.PhaseTerminal, contracts.OutcomeReplied, contracts.DeliveryConfirmed
				run.DeliveryEvidence.ConfirmedAt, run.DeliveryEvidence.EchoMessageID, run.DeliveryEvidence.SubmittedAtSource = &run.UpdatedAt, "synthetic-echo", "echo_inferred"
			case contracts.ActionUnknown:
				run.Phase, run.Outcome, run.Delivery, run.ReasonCode = contracts.PhaseUnknown, contracts.OutcomeUnknown, contracts.DeliveryUnknown, "echo_timeout"
			case contracts.ActionFailed:
				run.Phase, run.Outcome, run.Delivery, run.ReasonCode = contracts.PhaseTerminal, contracts.OutcomeFailed, contracts.DeliveryFailed, "proven_not_submitted"
				run.DeliveryEvidence.FailureEvidence = "PRIVATE_FAILURE_TEXT"
			}
			receipt, err := ProcessingActionReceipt(run)
			if err != nil || receipt.Status != status {
				t.Fatal(receipt, err)
			}
			if status == contracts.ActionConfirmed && (receipt.Evidence.SubmittedAt != nil || receipt.Evidence.SubmittedAtSource != "echo_inferred") {
				t.Fatal("invented ACK timestamp", receipt)
			}
			data, _ := json.Marshal(receipt)
			if strings.Contains(string(data), "PRIVATE_FAILURE_TEXT") {
				t.Fatal("unbounded failure evidence leaked")
			}
			receipt.Evidence.SubmissionStartedAt = nil
			if run.DeliveryEvidence.SubmissionStartedAt == nil {
				t.Fatal("receipt mutated run")
			}
			if receipt.Receipt != nil || receipt.MissingReason != "action_admission_not_recorded" {
				t.Fatal("legacy run invented action admission", receipt)
			}
			if _, err := WeChatActionRequest(run, "reply", "new-request", run.AcceptedAt.Add(mappingTestLifetime)); err == nil {
				t.Fatal("receipt authorized resubmission")
			}
		})
	}
}

func TestProcessingArchitectureEventUsesPersistedRunAndHistoricalState(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	work := traceBatch(d, "synthetic-input")
	work.Messages[0].Content = "PRIVATE_INPUT_BODY"
	if err := d.ensureTrace(&work); err != nil {
		t.Fatal(err)
	}
	if err := d.tracePhase(work, contracts.PhaseAggregating, "PRIVATE_PROMPT_OR_REPLY"); err != nil {
		t.Fatal(err)
	}
	run := onlyTraceRun(t, d)
	events, err := d.trace.RunEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	value, err := ProcessingArchitectureEvent(run, events[0])
	if err != nil || value.Domain != contracts.DomainProcessing || value.Phase != contracts.PhaseAccepted || value.Source == nil || value.Source.ContainerID != groupA || value.Source.ContentID != "synthetic-input" || value.EnvelopeID != run.Inputs[0].EnvelopeID || value.Action != nil {
		t.Fatal("historical facts replaced or fabricated", value, err)
	}
	latest, err := ProcessingArchitectureEvent(run, events[1])
	if err != nil || latest.ReasonCode != contracts.ReasonUpstreamRedacted {
		t.Fatal(latest, err)
	}
	for _, change := range []func(*contracts.Event){
		func(event *contracts.Event) { event.EntityID = "different-run" },
		func(event *contracts.Event) { event.Revision = "different-revision" },
		func(event *contracts.Event) { event.Seq = run.EntitySeq + 1 },
		func(event *contracts.Event) { event.OccurredAt = run.AcceptedAt.Add(-time.Second) },
		func(event *contracts.Event) { event.StreamID = "other-stream" },
		func(event *contracts.Event) { event.Cursor = "invalid-cursor" },
		func(event *contracts.Event) { event.Delivery = "invented" },
	} {
		event := events[1]
		change(&event)
		if _, err := ProcessingArchitectureEvent(run, event); err == nil {
			t.Fatal("event identity mismatch accepted")
		}
	}
	run.MessageIDs = append(run.MessageIDs, "second-message")
	run.Inputs = nil
	events[0].EnvelopeID, events[0].Source = "", nil
	value, err = ProcessingArchitectureEvent(run, events[0])
	if err != nil || value.Source != nil || value.EnvelopeID != "" {
		t.Fatal("batch invented a single source content", value, err)
	}
}

func TestProcessingProjectionKeepsTraceCursorAndReadPurity(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	bootstrap, err := d.trace.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	work := traceBatch(d, "synthetic-input")
	work.Messages[0].Content = "PRIVATE_INPUT_BODY"
	if err := d.ensureTrace(&work); err != nil {
		t.Fatal(err)
	}
	if err := d.tracePhase(work, contracts.PhaseAggregating, "PRIVATE_REPLY_OR_PROMPT"); err != nil {
		t.Fatal(err)
	}
	before, err := d.trace.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	response := traceRequest(d, "/api/trace/events?projection=processing&cursor="+bootstrap.Cursor)
	if response.Code != http.StatusOK {
		t.Fatal(response.Code, response.Body.String())
	}
	var projected processingEventPage
	if err := json.Unmarshal(response.Body.Bytes(), &projected); err != nil {
		t.Fatal(err)
	}
	raw, err := d.trace.EventsAfter(bootstrap.Cursor, 100)
	if err != nil || len(projected.Events) != len(raw.Events) || projected.Cursor != raw.Cursor || projected.HasMore != raw.HasMore {
		t.Fatal(projected, raw, err)
	}
	for index, item := range projected.Events {
		original := raw.Events[index]
		if item.StreamID != original.StreamID || item.Seq != original.Seq || item.Cursor != original.Cursor || item.Delivery != original.Delivery || item.Event.Phase != original.Phase || item.Event.Outcome != original.Outcome || contracts.ValidateArchitectureEvent(item.Event) != nil {
			t.Fatal("second sequence or changed historical state", item, original)
		}
		if (item.Receipt != nil) != (original.Revision == before.Runs[0].Revision) {
			t.Fatal("receipt attached to wrong revision", item)
		}
	}
	for _, private := range []string{"PRIVATE_INPUT_BODY", "PRIVATE_REPLY_OR_PROMPT", "What changed?", "prompt", "reply"} {
		if strings.Contains(response.Body.String(), private) {
			t.Fatalf("projection leaked %q", private)
		}
	}
	empty := traceRequest(d, "/api/trace/events?projection=processing&cursor="+projected.Cursor)
	var emptyPage processingEventPage
	if empty.Code != http.StatusOK || json.Unmarshal(empty.Body.Bytes(), &emptyPage) != nil || len(emptyPage.Events) != 0 || emptyPage.Cursor != projected.Cursor {
		t.Fatal("resume cursor changed", empty.Body.String())
	}
	after, err := d.trace.Snapshot()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("read projection changed trace", err)
	}
	legacy := traceRequest(d, "/api/trace/events?cursor="+bootstrap.Cursor)
	if legacy.Code != http.StatusOK || strings.Contains(legacy.Body.String(), `"domain"`) {
		t.Fatal("legacy response changed", legacy.Body.String())
	}
}

func TestProcessingAttemptAndUnknownEventFacts(t *testing.T) {
	d := newTestDispatcher(t, testConfig())
	work := traceBatch(d, "synthetic-attempt-input")
	if err := d.ensureTrace(&work); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []contracts.Phase{contracts.PhaseAggregating, contracts.PhaseGating, contracts.PhaseQueued, contracts.PhaseExecuting} {
		if err := d.tracePhase(work, phase, ""); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.traceAttemptStart(work); err != nil {
		t.Fatal(err)
	}
	if err := d.traceAttemptEnd(context.Background(), work, HermesResult{}, context.DeadlineExceeded); err != nil {
		t.Fatal(err)
	}
	if err := d.traceUnknown(work, "backend_timeout"); err != nil {
		t.Fatal(err)
	}
	run := onlyTraceRun(t, d)
	before, _ := json.Marshal(run)
	events, err := d.trace.RunEvents(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, event := range events {
		value, err := ProcessingArchitectureEvent(run, event)
		if err != nil || value.Domain != contracts.DomainProcessing || value.Phase != event.Phase || value.Outcome != event.Outcome || value.RunID != run.ID || value.Source.ContainerID != run.SourceGroupID {
			t.Fatal("run/attempt event changed facts", value, err)
		}
		if event.Phase == contracts.PhaseUnknown && value.Certainty != contracts.CertaintyUnknown {
			t.Fatal("unknown processing was presented as known", value)
		}
		seen[event.Kind] = true
	}
	if !seen["attempt_started"] || !seen["attempt_finished"] || !seen["run_unknown"] || run.Attempts[0].RemoteExecution != contracts.RemoteMayBeRunning {
		t.Fatal("attempt/uncertainty evidence missing", seen, run)
	}
	after, _ := json.Marshal(run)
	if string(before) != string(after) {
		t.Fatal("pure mapping mutated run")
	}
}
