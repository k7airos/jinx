// Package ports defines the three-layer adapter boundary. These interfaces are
// extension contracts; existing platform integrations do not implement them yet.
package ports

import (
	"context"

	"jinx/internal/contracts"
)

// SourceAdapter opens a configured account/container without discovering other
// containers. Stream preserves adapter order and opaque replay positions. A
// canceled context or Close must close the channel and release resources; errors
// close the stream and require explicit resumption, never invented deliveries.
// Every input/delivery must pass its contracts Validate function before use.
type SourceAdapter interface {
	Open(context.Context, contracts.SourceRef) error
	Stream(context.Context, contracts.SourceStreamRequest) (<-chan contracts.SourceDelivery, error)
	Close(context.Context, contracts.SourceRef) error
}

// AgentRuntime is opened before execution and closed after in-flight calls end.
// Execute calls contracts.ValidateProcessingInput, resolves skills,
// tools and knowledgeBases exclusively from operator allowlists, and observes
// cancellation without claiming an unproven remote stop. Reply text is untrusted
// output, never capability evidence. RequestedAction requires separate admission.
// Results must pass contracts.ValidateProcessingResult, including partial/error
// results; attempts and evidence must retain uncertainty rather than hide it.
type AgentRuntime interface {
	Open(context.Context, contracts.RuntimeRef) error
	Execute(context.Context, contracts.InboundEnvelope, contracts.ProcessingRequest) (contracts.ProcessingResult, error)
	Close(context.Context, contracts.RuntimeRef) error
}

// ActionAdapter.Validate checks request, expiry, target and current policy without
// sending. Send repeats validation at the send boundary and durably records the
// idempotency key before external I/O. Its receipt separates durable Admission
// from external Status: acceptance is not submission, an ACK is not confirmation.
// Repeated keys return the same admission; conflicts are rejected. Any unknown
// admission/submission or transport error prohibits automatic resend. Confirm
// reads correlated external evidence only and never sends; missing evidence
// remains unknown. Receipts must pass contracts.ValidateActionReceipt even on
// error. Cancellation after admission must retain a queryable receipt.
type ActionAdapter interface {
	Validate(context.Context, contracts.ActionRequest) (contracts.CapabilityEvidence, error)
	Send(context.Context, contracts.ActionRequest) (contracts.ActionReceipt, error)
	Confirm(context.Context, contracts.ActionReceipt) (contracts.ActionReceipt, error)
}

// CapabilityProvider reports observed adapter/runtime guarantees and their
// enforcement source. Model prose and configured allowlist refs cannot establish
// supported capabilities. A missing implementation must be explicitly unsupported.
type CapabilityProvider interface {
	Capabilities(context.Context, contracts.CapabilityQuery) (contracts.CapabilitySet, error)
}
