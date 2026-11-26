package dispatcher

import (
	"fmt"
	"strings"
	"time"

	"jinx/internal/contracts"
)

type sourceWatermark struct {
	LocalID     int64  `json:"local_id"`
	PrecisionMS int64  `json:"precision_ms"`
	Sequence    uint64 `json:"sequence"`
}

type confirmationEcho struct {
	MessageID        string    `json:"message_id"`
	ReceivedAt       time.Time `json:"received_at"`
	ReceivedSequence uint64    `json:"received_sequence"`
	EventID          string    `json:"event_id,omitempty"`
	Source           string    `json:"source,omitempty"`
	LocalID          int64     `json:"local_id,omitempty"`
	Timestamp        int64     `json:"timestamp"`
	PrecisionMS      int64     `json:"precision_ms"`
}

type awaitingConfirmation struct {
	RunID               string                     `json:"run_id"`
	RequestID           string                     `json:"request_id"`
	SourceGroupID       string                     `json:"source_group_id"`
	TargetGroupID       string                     `json:"target_group_id"`
	TextDigest          string                     `json:"text_digest"`
	SubmissionStartedAt time.Time                  `json:"submission_started_at"`
	IncomingSequence    uint64                     `json:"incoming_sequence"`
	DispatcherSequence  uint64                     `json:"dispatcher_sequence"`
	EventID             string                     `json:"event_id,omitempty"`
	SeenIDs             map[string]bool            `json:"seen_ids"`
	SourceWatermarks    map[string]sourceWatermark `json:"source_watermarks"`
	SelfSenderIDs       []string                   `json:"self_sender_ids,omitempty"`
	AckReceivedAt       *time.Time                 `json:"ack_received_at,omitempty"`
	SubmittedAtSource   string                     `json:"submitted_at_source,omitempty"`
	Echo                *confirmationEcho          `json:"echo,omitempty"`
	SubmittedCounted    bool                       `json:"submitted_counted,omitempty"`
	ConfirmedCounted    bool                       `json:"confirmed_counted,omitempty"`
	UnknownCounted      bool                       `json:"unknown_counted,omitempty"`
}

func messageSource(msg RelayMessage) string {
	if msg.Source == "" || msg.LocalID <= 0 {
		return ""
	}
	prefix := ""
	if separator := strings.LastIndex(msg.ID, ":"); separator >= 0 {
		prefix = msg.ID[:separator]
	}
	return msg.GroupID + "/" + msg.Source + "/" + prefix
}

func timestampPrecisionMS(msg RelayMessage) int64 {
	if messageSource(msg) != "" {
		return 1000
	}
	return 1
}

func (d *Dispatcher) observeIncomingLocked(msg RelayMessage, sequence uint64) {
	if sequence > d.state.IncomingSequence {
		d.state.IncomingSequence = sequence
	}
	if source := messageSource(msg); source != "" {
		if d.state.SourceWatermarks == nil {
			d.state.SourceWatermarks = make(map[string]sourceWatermark)
		}
		watermark := d.state.SourceWatermarks[source]
		if msg.LocalID > watermark.LocalID {
			d.state.SourceWatermarks[source] = sourceWatermark{LocalID: msg.LocalID, PrecisionMS: timestampPrecisionMS(msg), Sequence: sequence}
		}
	}
}

func (d *Dispatcher) newConfirmationLocked(b batch, reply, requestID string) *awaitingConfirmation {
	if d.state.AwaitingConfirmations == nil {
		d.state.AwaitingConfirmations = make(map[string]*awaitingConfirmation)
	}
	confirmation := &awaitingConfirmation{
		RunID: b.TraceID, RequestID: requestID, SourceGroupID: b.GroupID, TargetGroupID: b.Rule.TargetGroupID,
		TextDigest: messageDigest(strings.TrimSpace(reply)), SubmissionStartedAt: time.Now().UTC(),
		IncomingSequence: d.intakeSequence.Load(), DispatcherSequence: d.state.Sequence, EventID: d.state.LastEventID,
		SeenIDs: make(map[string]bool), SourceWatermarks: make(map[string]sourceWatermark),
		SelfSenderIDs: append([]string(nil), b.Config.Rules.SelfSenderIDs...),
	}
	if rule, exists := b.Config.Rules.GroupRules[confirmation.TargetGroupID]; exists && rule.SelfSenderIDs != nil {
		confirmation.SelfSenderIDs = append([]string(nil), rule.SelfSenderIDs...)
	}
	for key := range d.state.Seen {
		if strings.HasPrefix(key, confirmation.TargetGroupID+"/") {
			confirmation.SeenIDs[key] = true
		}
	}
	for _, msg := range d.groupLocked(confirmation.TargetGroupID).History {
		confirmation.SeenIDs[confirmation.TargetGroupID+"/"+msg.ID] = true
	}
	for source, watermark := range d.state.SourceWatermarks {
		if strings.HasPrefix(source, confirmation.TargetGroupID+"/") {
			confirmation.SourceWatermarks[source] = watermark
		}
	}
	if confirmation.IncomingSequence > d.state.IncomingSequence {
		d.state.IncomingSequence = confirmation.IncomingSequence
	}
	d.state.AwaitingConfirmations[confirmation.RunID] = confirmation
	return confirmation
}

func (d *Dispatcher) matchConfirmationLocked(msg RelayMessage, receivedAt time.Time, sequence uint64) *awaitingConfirmation {
	if msg.ID == "" || msg.Action != "forward" || msg.Timestamp <= 0 || msg.Timestamp > receivedAt.Add(time.Minute).UnixMilli() {
		return nil
	}
	key := msg.GroupID + "/" + msg.ID
	if _, seen := d.state.Seen[key]; seen || d.tracedMessages[key] != "" {
		return nil
	}
	digest := messageDigest(strings.TrimSpace(msg.Content))
	var match *awaitingConfirmation
	for _, candidate := range d.state.AwaitingConfirmations {
		if candidate.Echo != nil && candidate.Echo.MessageID == msg.ID && candidate.TargetGroupID == msg.GroupID {
			return nil
		}
		if candidate.Echo != nil || msg.GroupID != candidate.TargetGroupID || digest != candidate.TextDigest || (!msg.IsEcho && (msg.SenderID == "" || !contains(candidate.SelfSenderIDs, msg.SenderID))) || candidate.SeenIDs[key] || sequence <= candidate.IncomingSequence || receivedAt.Before(candidate.SubmissionStartedAt) {
			continue
		}
		precision := timestampPrecisionMS(msg)
		if !time.UnixMilli(msg.Timestamp).After(candidate.SubmissionStartedAt) || (precision == 1000 && msg.Timestamp%1000 != 0) {
			continue
		}
		if watermark, exists := candidate.SourceWatermarks[messageSource(msg)]; exists && msg.LocalID <= watermark.LocalID {
			continue
		}
		if match != nil {
			return nil
		}
		match = candidate
	}
	return match
}

func (d *Dispatcher) persistConfirmationsLocked() error {
	if err := d.saveStateLocked(); err != nil {
		return d.traceFailure()
	}
	return nil
}

func (d *Dispatcher) receiveConfirmationLocked(confirmation *awaitingConfirmation, msg RelayMessage, eventID string, receivedAt time.Time, sequence uint64) error {
	confirmation.Echo = &confirmationEcho{
		MessageID: msg.ID, ReceivedAt: receivedAt, ReceivedSequence: sequence, EventID: eventID,
		Source: msg.Source, LocalID: msg.LocalID, Timestamp: msg.Timestamp, PrecisionMS: timestampPrecisionMS(msg),
	}
	d.observeIncomingLocked(msg, sequence)
	d.lastProgressAt = receivedAt
	d.state.Stats.TotalReceived++
	d.state.Sequence++
	msg.Sequence, msg.IsEcho = d.state.Sequence, true
	g := d.groupLocked(msg.GroupID)
	g.LastOwnReplySequence = msg.Sequence
	appendHistory(g, msg)
	d.history.Append(msg)
	d.state.Seen[msg.GroupID+"/"+msg.ID] = receivedAt
	if eventID != "" {
		d.state.LastEventID = eventID
	}
	if err := d.persistConfirmationsLocked(); err != nil {
		return err
	}
	if waiter := d.awaitingEcho; waiter != nil && waiter.runID == confirmation.RunID && waiter.responsePending {
		return nil
	}
	return d.confirmEchoLocked(confirmation)
}

func (d *Dispatcher) acceptSubmissionLocked(confirmation *awaitingConfirmation) error {
	if err := d.updateTrace(batch{TraceID: confirmation.RunID}, "submission_accepted", func(run *contracts.Run) {
		if run.DeliveryEvidence.SubmittedAt != nil || run.Delivery == contracts.DeliveryConfirmed {
			return
		}
		run.Phase, run.Outcome, run.Delivery = contracts.PhaseConfirming, contracts.OutcomePending, contracts.DeliverySubmitted
		run.DeliveryEvidence.SubmittedAt = confirmation.AckReceivedAt
		run.DeliveryEvidence.SubmittedAtSource = "bridge_ack"
	}); err != nil {
		return err
	}
	confirmation.SubmittedAtSource = "bridge_ack"
	if !confirmation.SubmittedCounted {
		d.state.Stats.TotalSubmitted++
		confirmation.SubmittedCounted = true
	}
	return d.persistConfirmationsLocked()
}

func (d *Dispatcher) confirmEchoLocked(confirmation *awaitingConfirmation) error {
	if confirmation.Echo == nil {
		return fmt.Errorf("echo evidence missing")
	}
	if err := d.updateTrace(batch{TraceID: confirmation.RunID}, "delivery_confirmed", func(run *contracts.Run) {
		run.Phase, run.Outcome, run.Delivery = contracts.PhaseTerminal, contracts.OutcomeReplied, contracts.DeliveryConfirmed
		run.ReasonCode = ""
		if run.DeliveryEvidence.SubmittedAt == nil {
			if confirmation.AckReceivedAt != nil && confirmation.SubmittedAtSource == "bridge_ack" {
				ackAt := *confirmation.AckReceivedAt
				run.DeliveryEvidence.SubmittedAt = &ackAt
				run.DeliveryEvidence.SubmittedAtSource = "bridge_ack"
			} else {
				run.DeliveryEvidence.SubmittedAtSource = "echo_inferred"
				run.ReasonCode = "submission_time_echo_inferred"
			}
		}
		confirmation.SubmittedAtSource = run.DeliveryEvidence.SubmittedAtSource
		run.DeliveryEvidence.ConfirmedAt = &confirmation.Echo.ReceivedAt
		run.DeliveryEvidence.EchoMessageID = confirmation.Echo.MessageID
	}); err != nil {
		return err
	}
	return d.finishConfirmationLocked(confirmation)
}

func (d *Dispatcher) confirmDirectLocked(confirmation *awaitingConfirmation, externalID string, observedAt time.Time, replyText string, senderID string) error {
	if !contracts.ValidID(externalID) {
		return fmt.Errorf("invalid external confirmation ID")
	}
	if !observedAt.After(confirmation.SubmissionStartedAt) {
		observedAt = confirmation.SubmissionStartedAt.Add(time.Nanosecond)
	}
	ackAt := observedAt.UTC()
	confirmation.AckReceivedAt = &ackAt
	confirmation.SubmittedAtSource = "bridge_ack"
	confirmation.Echo = &confirmationEcho{
		MessageID:   externalID,
		ReceivedAt:  ackAt,
		Timestamp:   ackAt.UnixMilli(),
		PrecisionMS: 1,
	}
	d.lastProgressAt = ackAt
	d.state.Sequence++
	echoMsg := RelayMessage{
		ID:         externalID,
		GroupID:    confirmation.TargetGroupID,
		SenderID:   senderID,
		SenderName: senderID,
		Type:       1,
		Content:    replyText,
		Action:     "forward",
		Timestamp:  ackAt.UnixMilli(),
		CreatedAt:  ackAt.Format("2006-01-02 15:04:05"),
		IsEcho:     true,
		Sequence:   d.state.Sequence,
	}
	g := d.groupLocked(confirmation.TargetGroupID)
	g.LastOwnReplySequence = echoMsg.Sequence
	appendHistory(g, echoMsg)
	d.history.Append(echoMsg)
	d.state.Seen[confirmation.TargetGroupID+"/"+externalID] = ackAt
	if err := d.persistConfirmationsLocked(); err != nil {
		return err
	}
	if err := d.updateTrace(batch{TraceID: confirmation.RunID}, "delivery_confirmed", func(run *contracts.Run) {
		if run.UpdatedAt.Before(ackAt) {
			run.UpdatedAt = ackAt
			run.LastProgressAt = ackAt
		}
		run.Phase, run.Outcome, run.Delivery = contracts.PhaseTerminal, contracts.OutcomeReplied, contracts.DeliveryConfirmed
		run.ReasonCode = ""
		run.DeliveryEvidence.SubmittedAt = &ackAt
		run.DeliveryEvidence.SubmittedAtSource = "bridge_ack"
		run.DeliveryEvidence.ConfirmedAt = &ackAt
		run.DeliveryEvidence.EchoMessageID = externalID
	}); err != nil {
		return err
	}
	return d.finishConfirmationLocked(confirmation)
}

func sanitizeFailureReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if len(reason) > 64 || !contracts.ValidID(reason) {
		return "connector_rejected"
	}
	return reason
}

func (d *Dispatcher) failSubmissionLocked(confirmation *awaitingConfirmation, reason string) error {
	if d.traceFailed.Load() {
		return errTraceUnavailable
	}
	safeReason := sanitizeFailureReason(reason)
	if err := d.updateTrace(batch{TraceID: confirmation.RunID}, "delivery_failed", func(run *contracts.Run) {
		run.Phase = contracts.PhaseTerminal
		run.Outcome = contracts.OutcomeFailed
		run.Delivery = contracts.DeliveryFailed
		run.ReasonCode = safeReason
		run.DeliveryEvidence.FailureEvidence = safeReason
	}); err != nil {
		return err
	}
	d.state.Stats.TotalSendFailed++
	d.groupLocked(confirmation.SourceGroupID).SendStarted = false
	if waiter := d.awaitingEcho; waiter != nil && waiter.runID == confirmation.RunID {
		select {
		case <-waiter.done:
		default:
			close(waiter.done)
		}
	}
	delete(d.state.AwaitingConfirmations, confirmation.RunID)
	return d.persistConfirmationsLocked()
}

func (d *Dispatcher) finishConfirmationLocked(confirmation *awaitingConfirmation) error {
	d.tracedMessages[confirmation.TargetGroupID+"/"+confirmation.Echo.MessageID] = confirmation.RunID
	if !confirmation.SubmittedCounted {
		d.state.Stats.TotalSubmitted++
		confirmation.SubmittedCounted = true
	}
	if !confirmation.ConfirmedCounted {
		d.state.Stats.TotalReplied++
		confirmation.ConfirmedCounted = true
		g := d.groupLocked(confirmation.SourceGroupID)
		g.ReplyTimes = confirmedReplyTimes(g.ReplyTimes, confirmation.Echo.ReceivedAt)
		if confirmation.TargetGroupID != confirmation.SourceGroupID {
			target := d.groupLocked(confirmation.TargetGroupID)
			target.ReplyTimes = confirmedReplyTimes(target.ReplyTimes, confirmation.Echo.ReceivedAt)
		}
		g.SendStarted = false
	}
	if waiter := d.awaitingEcho; waiter != nil && waiter.runID == confirmation.RunID {
		select {
		case <-waiter.done:
		default:
			close(waiter.done)
		}
	}
	delete(d.state.AwaitingConfirmations, confirmation.RunID)
	return d.persistConfirmationsLocked()
}

func confirmedReplyTimes(previous []time.Time, confirmedAt time.Time) []time.Time {
	kept := make([]time.Time, 0, len(previous)+1)
	for _, timestamp := range previous {
		if confirmedAt.Sub(timestamp) < time.Hour {
			kept = append(kept, timestamp)
		}
	}
	return append(kept, confirmedAt)
}

func (d *Dispatcher) confirmOrUnknownLocked(confirmation *awaitingConfirmation, reason string) error {
	if d.traceFailed.Load() {
		return errTraceUnavailable
	}
	if confirmation.ConfirmedCounted {
		return nil
	}
	if confirmation.Echo != nil {
		return d.confirmEchoLocked(confirmation)
	}
	if err := d.traceUnknown(batch{TraceID: confirmation.RunID}, reason); err != nil {
		return err
	}
	if !confirmation.UnknownCounted {
		d.state.Stats.TotalUnknown++
		d.state.Stats.TotalSendFailed++
		confirmation.UnknownCounted = true
	}
	if err := d.persistConfirmationsLocked(); err != nil {
		return err
	}
	return fmt.Errorf("delivery outcome unknown (%s); not retried", reason)
}

func (d *Dispatcher) hasConfirmationForGroup(groupID string) bool {
	for _, confirmation := range d.state.AwaitingConfirmations {
		if confirmation != nil && confirmation.SourceGroupID == groupID {
			return true
		}
	}
	return false
}

func (d *Dispatcher) recoverConfirmationsLocked() error {
	for runID, confirmation := range d.state.AwaitingConfirmations {
		if confirmation == nil || confirmation.RunID != runID || confirmation.RequestID == "" || confirmation.TextDigest == "" || confirmation.TargetGroupID == "" || confirmation.SubmissionStartedAt.IsZero() {
			return fmt.Errorf("invalid awaiting confirmation index")
		}
		run, err := d.trace.GetRun(runID)
		if err != nil {
			return d.traceFailure()
		}
		if run.DeliveryEvidence.SubmissionStartedAt == nil {
			delete(d.state.AwaitingConfirmations, runID)
			continue
		}
		if run.DeliveryEvidence.BridgeRequestID != confirmation.RequestID || !run.DeliveryEvidence.SubmissionStartedAt.Equal(confirmation.SubmissionStartedAt) || run.TargetGroupID != confirmation.TargetGroupID || run.SourceGroupID != confirmation.SourceGroupID {
			return fmt.Errorf("awaiting confirmation boundary conflicts with trace")
		}
		if confirmation.IncomingSequence > d.state.IncomingSequence {
			d.state.IncomingSequence = confirmation.IncomingSequence
			d.intakeSequence.Store(confirmation.IncomingSequence)
		}
		if run.DeliveryEvidence.SubmittedAt != nil && !confirmation.SubmittedCounted {
			d.state.Stats.TotalSubmitted++
			confirmation.SubmittedCounted = true
		}
		if run.Delivery == contracts.DeliveryConfirmed {
			if confirmation.Echo == nil || run.DeliveryEvidence.EchoMessageID != confirmation.Echo.MessageID {
				return fmt.Errorf("awaiting confirmation echo conflicts with trace")
			}
			if err := d.finishConfirmationLocked(confirmation); err != nil {
				return err
			}
			continue
		}
		if confirmation.Echo != nil {
			if err := d.confirmEchoLocked(confirmation); err != nil {
				return err
			}
		} else {
			if run.DeliveryEvidence.SubmittedAt == nil && confirmation.AckReceivedAt != nil {
				if err := d.acceptSubmissionLocked(confirmation); err != nil {
					return err
				}
				if err := d.traceUnknown(batch{TraceID: runID}, "restart_recovery"); err != nil {
					return err
				}
			}
			if !confirmation.UnknownCounted {
				d.state.Stats.TotalUnknown++
				confirmation.UnknownCounted = true
			}
		}
	}
	return nil
}
