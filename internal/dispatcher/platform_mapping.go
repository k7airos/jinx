package dispatcher

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"jinx/internal/contracts"
	"jinx/internal/observability"
)

const maxRelayBodyBytes = 64 << 10
const wechatMappingAccount = "wechat-relay"
const maxRelayReplyContextBytes = 8 << 10

var wechatRelayShard = regexp.MustCompile(`^message/(?:biz_)?message_[0-9]+\.db$`)
var relaySourceSuffixPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$`)

func relayPlatform(source string) (contracts.Platform, error) {
	if source == "" || source == string(contracts.PlatformWeChat) || wechatRelayShard.MatchString(source) {
		return contracts.PlatformWeChat, nil
	}
	prefix, suffix, hasSuffix := strings.Cut(source, ":")
	platform := contracts.Platform(prefix)
	if !contracts.IsNamespacedPlatform(platform) {
		return "", fmt.Errorf("unsupported relay source")
	}
	if hasSuffix && (!relaySourceSuffixPattern.MatchString(suffix) || strings.Contains(suffix, "..")) {
		return "", fmt.Errorf("unsupported relay source")
	}
	return platform, nil
}

func validateRelayMapping(msg RelayMessage) error {
	for _, identity := range []string{msg.ID, msg.GroupID} {
		if !mappingIdentity(identity) {
			return fmt.Errorf("message and source container IDs are required")
		}
	}
	platform, err := relayPlatform(msg.Source)
	if err != nil {
		return err
	}
	if msg.Platform != "" && contracts.Platform(msg.Platform) != platform {
		return fmt.Errorf("relay platform disagrees with source")
	}
	containerPlatform, _, ok := contracts.ParseContainerID(msg.GroupID)
	if platform != contracts.PlatformWeChat {
		if !ok || containerPlatform != platform {
			return fmt.Errorf("container platform disagrees with relay source")
		}
	} else if ok && containerPlatform != platform {
		return fmt.Errorf("container platform disagrees with relay source")
	}
	for _, optional := range []string{msg.AccountID, msg.ParentID, msg.RootID} {
		if optional != "" && !mappingIdentity(optional) {
			return fmt.Errorf("invalid optional relay identity")
		}
	}
	if len(msg.ReplyContext) > 0 && (len(msg.ReplyContext) > maxRelayReplyContextBytes || !json.Valid(msg.ReplyContext)) {
		return fmt.Errorf("invalid relay reply context")
	}
	bodyBytes := len(msg.Content)
	if !utf8.ValidString(msg.Content) || strings.IndexByte(msg.Content, 0) >= 0 {
		return fmt.Errorf("invalid message text")
	}
	if msg.Details != nil && msg.Details.Quote != nil {
		bodyBytes += len(msg.Details.Quote.Content)
		if !utf8.ValidString(msg.Details.Quote.Content) || strings.IndexByte(msg.Details.Quote.Content, 0) >= 0 {
			return fmt.Errorf("invalid quoted text")
		}
	}
	if bodyBytes > maxRelayBodyBytes {
		return fmt.Errorf("relay body exceeds mapping limit")
	}
	return nil
}

func mappingIdentity(value string) bool {
	return value != "" && len(value) <= 512 && value == strings.TrimSpace(value) && utf8.ValidString(value) && !strings.ContainsFunc(value, unicode.IsControl)
}

func defaultMappingAccount(platform contracts.Platform) string {
	if contracts.IsNamespacedPlatform(platform) {
		return string(platform) + "-connector"
	}
	return wechatMappingAccount
}

func relayDedupKey(msg RelayMessage) string {
	source := msg.Source
	if source == "" {
		source = "wechat"
	}
	parts := []string{source, msg.GroupID, msg.ID}
	if msg.IntakeReceipt != "" {
		parts = []string{source, msg.IntakeReceipt, msg.ID}
	}
	encoded, _ := json.Marshal(parts)
	return "relay-" + messageDigest(string(encoded))
}

func NormalizeRelayMessage(msg RelayMessage) (contracts.InboundEnvelope, error) {
	if err := validateRelayMapping(msg); err != nil {
		return contracts.InboundEnvelope{}, err
	}
	if msg.ReceivedAt == nil || msg.ReceivedAt.IsZero() {
		return contracts.InboundEnvelope{}, fmt.Errorf("relay receipt time was not observed")
	}
	kind := msg.Type & 0xffffffff
	if kind != 1 && kind != 81 && !(kind == 49 && msg.Details != nil && msg.Details.Quote != nil && msg.Details.Quote.Type == 1) {
		return contracts.InboundEnvelope{}, fmt.Errorf("relay message has no supported text evidence")
	}
	platform, err := relayPlatform(msg.Source)
	if err != nil {
		return contracts.InboundEnvelope{}, err
	}
	accountID := msg.AccountID
	if accountID == "" {
		accountID = defaultMappingAccount(platform)
	}
	source := contracts.SourceRef{
		Platform: platform, AccountID: accountID,
		ContainerID: msg.GroupID, ContainerLabel: msg.GroupName, ContentID: msg.ID,
		ParentID: msg.ParentID, RootID: msg.RootID,
	}
	dedupKey := relayDedupKey(msg)
	envelope := contracts.InboundEnvelope{
		EnvelopeID: "envelope-" + strings.TrimPrefix(dedupKey, "relay-"), DedupKey: dedupKey,
		Source: source, Author: contracts.AuthorRef{ID: msg.SenderID, Label: msg.SenderName},
		Content: contracts.MessageContent{Kind: "text", Text: msg.Content}, ReceivedAt: *msg.ReceivedAt,
	}
	if !msg.IsEcho && msg.Action == "forward" {
		envelope.Capabilities.CanReply = true
		replySource := source
		if platform == contracts.PlatformWeChat {
			replySource.ContentID = ""
			replySource.ParentID = ""
			replySource.RootID = ""
		} else {
			envelope.Capabilities.CanThreadReply = msg.ParentID != "" || msg.RootID != ""
		}
		envelope.ReplyTarget = &contracts.ReplyTarget{Source: replySource}
	}
	if err := contracts.ValidateInboundEnvelope(envelope); err != nil {
		return contracts.InboundEnvelope{}, err
	}
	return envelope, nil
}

func WeChatActionRequest(run contracts.Run, reply string, requestID string, expiresAt time.Time) (contracts.ActionRequest, error) {
	if err := contracts.ValidateRun(run); err != nil {
		return contracts.ActionRequest{}, err
	}
	if run.Revision == "" || run.EntitySeq == 0 || run.Action != nil || run.Phase != contracts.PhaseChecking || run.Outcome != contracts.OutcomePending || run.Delivery != contracts.DeliveryNotStarted || run.DeliveryEvidence != (contracts.DeliveryEvidence{}) {
		return contracts.ActionRequest{}, fmt.Errorf("run is not eligible for a new text action")
	}
	if !run.UpdatedAt.Before(expiresAt) {
		return contracts.ActionRequest{}, fmt.Errorf("run action has expired")
	}
	request := contracts.ActionRequest{
		ActionID: requestID, RunID: run.ID, Kind: contracts.ActionReplyText, Text: reply,
		Target: contracts.ReplyTarget{Source: contracts.SourceRef{
			Platform: contracts.PlatformWeChat, AccountID: wechatMappingAccount, ContainerID: run.TargetGroupID,
		}},
		ExpiresAt: expiresAt, IdempotencyKey: messageDigest(run.ID + ":bridge"),
	}
	if err := contracts.ValidateSourceRef(contracts.SourceRef{Platform: contracts.PlatformWeChat, AccountID: wechatMappingAccount, ContainerID: run.SourceGroupID}); err != nil {
		return contracts.ActionRequest{}, err
	}
	if err := contracts.ValidateActionRequest(request); err != nil {
		return contracts.ActionRequest{}, err
	}
	return request, nil
}

func ValidateWeChatActionRequest(run contracts.Run, request contracts.ActionRequest, expiresAt time.Time) error {
	expected, err := WeChatActionRequest(run, request.Text, request.ActionID, expiresAt)
	if err != nil {
		return err
	}
	if request != expected {
		return fmt.Errorf("action differs from persisted run target or admission facts")
	}
	return nil
}

func ActionTargetForBatch(b batch) (contracts.SourceRef, contracts.ActionKind, json.RawMessage, error) {
	platform, _, ok := contracts.ParseContainerID(b.Rule.TargetGroupID)
	if !ok {
		return contracts.SourceRef{}, "", nil, fmt.Errorf("invalid target container ID %q", b.Rule.TargetGroupID)
	}
	if platform == contracts.PlatformWeChat {
		return contracts.SourceRef{
			Platform:    contracts.PlatformWeChat,
			AccountID:   wechatMappingAccount,
			ContainerID: b.Rule.TargetGroupID,
		}, contracts.ActionReplyText, nil, nil
	}
	if len(b.Messages) == 0 {
		return contracts.SourceRef{}, "", nil, fmt.Errorf("batch has no messages for target selection")
	}
	targetMsg := b.Messages[len(b.Messages)-1]
	if b.ReplyTargetIndex != 0 {
		if b.ReplyTargetIndex < 0 || b.ReplyTargetIndex > len(b.Messages) {
			return contracts.SourceRef{}, "", nil, fmt.Errorf("reply target index outside batch")
		}
		targetMsg = b.Messages[b.ReplyTargetIndex-1]
	} else if len(b.Messages) > 1 && !b.Direct {
		return contracts.SourceRef{}, "", nil, fmt.Errorf("multi-message batch has no selected reply target")
	}
	accountID := targetMsg.AccountID
	if accountID == "" {
		accountID = defaultMappingAccount(platform)
	}
	contentID := targetMsg.ID
	if b.Direct {
		contentID = b.DirectTargetContentID
	}
	target := contracts.SourceRef{
		Platform:    platform,
		AccountID:   accountID,
		ContainerID: b.Rule.TargetGroupID,
		ContentID:   contentID,
		ParentID:    targetMsg.ParentID,
		RootID:      targetMsg.RootID,
	}
	if target.ContentID == "" {
		target.ParentID = ""
		target.RootID = ""
	}
	kind := contracts.ActionReplyText
	if target.ContentID != "" {
		kind = contracts.ActionThreadReply
	}
	var replyContext json.RawMessage
	if len(targetMsg.ReplyContext) > 0 {
		replyContext = append(json.RawMessage(nil), targetMsg.ReplyContext...)
	}
	return target, kind, replyContext, nil
}

func PlatformActionRequest(run contracts.Run, target contracts.SourceRef, kind contracts.ActionKind, reply string, requestID string, expiresAt time.Time) (contracts.ActionRequest, error) {
	if err := contracts.ValidateRun(run); err != nil {
		return contracts.ActionRequest{}, err
	}
	srcPlatform, _, srcOK := contracts.ParseContainerID(run.SourceGroupID)
	dstPlatform, _, dstOK := contracts.ParseContainerID(run.TargetGroupID)
	if !srcOK || !dstOK || srcPlatform != dstPlatform {
		return contracts.ActionRequest{}, fmt.Errorf("invalid or mismatched run container platforms")
	}
	if dstPlatform == contracts.PlatformWeChat {
		return WeChatActionRequest(run, reply, requestID, expiresAt)
	}
	if run.Revision == "" || run.EntitySeq == 0 || run.Action != nil || run.Phase != contracts.PhaseChecking || run.Outcome != contracts.OutcomePending || run.Delivery != contracts.DeliveryNotStarted || run.DeliveryEvidence != (contracts.DeliveryEvidence{}) {
		return contracts.ActionRequest{}, fmt.Errorf("run is not eligible for a new action")
	}
	if !run.UpdatedAt.Before(expiresAt) {
		return contracts.ActionRequest{}, fmt.Errorf("run action has expired")
	}
	if target.Platform != dstPlatform || target.ContainerID != run.TargetGroupID {
		return contracts.ActionRequest{}, fmt.Errorf("action target disagrees with persisted run route")
	}
	if target.AccountID == "" {
		target.AccountID = defaultMappingAccount(dstPlatform)
	}
	if err := contracts.ValidateSourceRef(contracts.SourceRef{
		Platform: srcPlatform, AccountID: target.AccountID, ContainerID: run.SourceGroupID,
	}); err != nil {
		return contracts.ActionRequest{}, err
	}
	request := contracts.ActionRequest{
		ActionID:       requestID,
		RunID:          run.ID,
		Kind:           kind,
		Text:           reply,
		Target:         contracts.ReplyTarget{Source: target},
		ExpiresAt:      expiresAt,
		IdempotencyKey: messageDigest(run.ID + ":action:" + string(dstPlatform)),
	}
	if err := contracts.ValidateActionRequest(request); err != nil {
		return contracts.ActionRequest{}, err
	}
	return request, nil
}

func ValidatePlatformActionRequest(run contracts.Run, target contracts.SourceRef, kind contracts.ActionKind, request contracts.ActionRequest, expiresAt time.Time) error {
	expected, err := PlatformActionRequest(run, target, kind, request.Text, request.ActionID, expiresAt)
	if err != nil {
		return err
	}
	if request != expected {
		return fmt.Errorf("action differs from persisted run target or admission facts")
	}
	return nil
}

func ProcessingArchitectureEvent(run contracts.Run, event contracts.Event) (contracts.ArchitectureEvent, error) {
	if err := contracts.ValidateRun(run); err != nil {
		return contracts.ArchitectureEvent{}, err
	}
	if run.Revision == "" || run.EntitySeq == 0 || event.Entity != "run" || event.EntityID != run.ID || event.Seq == 0 || event.Seq > run.EntitySeq || event.StreamID == "" || event.Cursor == "" || event.Revision == "" || event.OccurredAt.Before(run.AcceptedAt) || event.OccurredAt.After(run.UpdatedAt) {
		return contracts.ArchitectureEvent{}, fmt.Errorf("event does not identify the persisted run")
	}
	var position struct {
		StreamID string
		Seq      uint64
	}
	encoded, err := base64.RawURLEncoding.DecodeString(event.Cursor)
	if err != nil || json.Unmarshal(encoded, &position) != nil || position.StreamID != event.StreamID || position.Seq != event.Seq {
		return contracts.ArchitectureEvent{}, fmt.Errorf("event cursor disagrees with trace position")
	}
	switch event.Delivery {
	case contracts.DeliveryNotStarted, contracts.DeliveryBlocked, contracts.DeliverySubmitting, contracts.DeliverySubmitted, contracts.DeliveryConfirmed, contracts.DeliveryFailed, contracts.DeliveryUnknown:
	default:
		return contracts.ArchitectureEvent{}, fmt.Errorf("unknown trace delivery state")
	}
	if event.Seq == run.EntitySeq && (event.Revision != run.Revision || !event.OccurredAt.Equal(run.UpdatedAt) || event.Phase != run.Phase || event.Outcome != run.Outcome || event.Delivery != run.Delivery || event.ReasonCode != run.ReasonCode) {
		return contracts.ArchitectureEvent{}, fmt.Errorf("event and run revisions disagree")
	}
	value := contracts.ArchitectureEvent{
		EventID: "trace-" + messageDigest(event.Cursor), Domain: contracts.DomainProcessing, Kind: contracts.KindRunChanged,
		OccurredAt: event.OccurredAt, RunID: run.ID, EnvelopeID: event.EnvelopeID,
		Phase: event.Phase, Outcome: event.Outcome, Certainty: contracts.CertaintyObserved,
		ReasonCode: processingReason(event.ReasonCode),
	}
	if event.Source != nil {
		source := *event.Source
		value.Source = &source
	}
	if event.Action != nil {
		action := *event.Action
		if action.AdmittedAt != nil {
			admittedAt := *action.AdmittedAt
			action.AdmittedAt = &admittedAt
		}
		value.Action = &action
	}
	if event.Phase == contracts.PhaseUnknown || event.Outcome == contracts.OutcomeUnknown || event.Delivery == contracts.DeliveryUnknown {
		value.Certainty = contracts.CertaintyUnknown
	}
	if err := contracts.ValidateArchitectureEvent(value); err != nil {
		return contracts.ArchitectureEvent{}, err
	}
	return value, nil
}

func processingReason(reason string) contracts.ReasonCode {
	value := contracts.ReasonCode(reason)
	probe := contracts.CapabilityEvidence{ID: "reason", Supported: true, Enforcement: contracts.EnforcementUnverified, Source: contracts.CapabilityDispatcher, Reason: value}
	if contracts.ValidateCapabilityEvidence(probe) != nil {
		return contracts.ReasonUpstreamRedacted
	}
	return value
}

type ProcessingReceipt struct {
	Status        contracts.ActionStatus     `json:"status"`
	Evidence      contracts.DeliveryEvidence `json:"evidence"`
	Receipt       *contracts.ActionReceipt   `json:"receipt,omitempty"`
	MissingReason string                     `json:"missingReason,omitempty"`
}

func ProcessingActionReceipt(run contracts.Run) (ProcessingReceipt, error) {
	if err := contracts.ValidateRun(run); err != nil {
		return ProcessingReceipt{}, err
	}
	status := contracts.ActionNotStarted
	switch run.Delivery {
	case contracts.DeliverySubmitted:
		status = contracts.ActionSubmitted
	case contracts.DeliveryConfirmed:
		status = contracts.ActionConfirmed
	case contracts.DeliveryUnknown, contracts.DeliverySubmitting:
		status = contracts.ActionUnknown
	case contracts.DeliveryFailed:
		status = contracts.ActionFailed
	}
	evidence := run.DeliveryEvidence
	for _, timestamp := range []**time.Time{&evidence.SubmissionStartedAt, &evidence.SubmittedAt, &evidence.ConfirmedAt} {
		if *timestamp != nil {
			detached := **timestamp
			*timestamp = &detached
		}
	}
	if evidence.FailureEvidence != "" {
		evidence.FailureEvidence = string(contracts.ReasonFailed)
	}
	value := ProcessingReceipt{Status: status, Evidence: evidence}
	if run.Action == nil {
		value.MissingReason = "action_admission_not_recorded"
		return value, nil
	}
	admittedAt := *run.Action.AdmittedAt
	receipt := contracts.ActionReceipt{
		ActionID: run.Action.ActionID, Platform: run.Action.Platform, Status: status,
		Admission:   contracts.ActionAdmission{Status: contracts.AdmissionAccepted, AdmissionID: run.Action.AdmissionID, RecordedAt: &admittedAt},
		SubmittedAt: evidence.SubmittedAt, SubmittedAtSource: evidence.SubmittedAtSource, ConfirmedAt: evidence.ConfirmedAt,
		ReasonCode: processingReason(run.ReasonCode),
	}
	if status == contracts.ActionUnknown {
		receipt.ReasonCode = contracts.ReasonUnknown
	}
	if status == contracts.ActionFailed {
		receipt.ReasonCode = contracts.ReasonFailed
	}
	if evidence.ConfirmedAt != nil {
		receipt.ExternalID = evidence.EchoMessageID
		receipt.Confirmation = &contracts.ActionConfirmation{RequestID: evidence.BridgeRequestID, EchoMessageID: evidence.EchoMessageID}
	}
	if err := contracts.ValidateActionReceipt(receipt); err != nil {
		return ProcessingReceipt{}, err
	}
	value.Receipt = &receipt
	return value, nil
}

type processingTraceEvent struct {
	StreamID string                      `json:"streamId"`
	Seq      uint64                      `json:"seq"`
	Cursor   string                      `json:"cursor"`
	Event    contracts.ArchitectureEvent `json:"event"`
	Delivery contracts.Delivery          `json:"delivery"`
	Receipt  *ProcessingReceipt          `json:"receipt,omitempty"`
}

type processingEventPage struct {
	Events  []processingTraceEvent `json:"events"`
	Cursor  string                 `json:"cursor"`
	HasMore bool                   `json:"hasMore"`
}

func (d *Dispatcher) processingTracePage(page observability.EventPage) (processingEventPage, error) {
	projected := processingEventPage{Events: []processingTraceEvent{}, Cursor: page.Cursor, HasMore: page.HasMore}
	for _, event := range page.Events {
		run, err := d.trace.GetRun(event.EntityID)
		if err != nil {
			return processingEventPage{}, err
		}
		value, err := ProcessingArchitectureEvent(run, event)
		if err != nil {
			return processingEventPage{}, err
		}
		item := processingTraceEvent{StreamID: event.StreamID, Seq: event.Seq, Cursor: event.Cursor, Event: value, Delivery: event.Delivery}
		if event.Revision == run.Revision {
			receipt, err := ProcessingActionReceipt(run)
			if err != nil {
				return processingEventPage{}, err
			}
			item.Receipt = &receipt
		}
		projected.Events = append(projected.Events, item)
	}
	return projected, nil
}
