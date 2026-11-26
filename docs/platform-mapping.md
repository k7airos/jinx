# Platform mapping in the real Dispatcher pipeline

The running WeChat Dispatcher calls the shared conversions during intake and
delivery. Safe input references, submission admission and historical event
correlations use the existing trace WAL and sequence. No second journal,
collector, worker or platform sender is introduced.

The public core owns message processing, delivery orchestration, adapter
contracts and generic webhook/SSE ingress. Local WeChat collection and its
implementation or deployment details belong to a separate private module that
only cooperates with core on the local host. This page documents the public
message boundary, not the private source implementation.

## Current WeChat evidence

- `RelayMessage.GroupID` identifies the source container; `GroupName` is its
  observed label. `ID` identifies the received content. Author evidence is only
  `SenderID` and `SenderName`.
- Relay `Source` is a bounded compatibility provenance token. It is not an
  unrestricted platform name or an action destination; new adapters should use
  the explicit platform identity.
- `QuoteDetails` currently carries quoted type, author and content, but no
  reliable quoted message ID, thread root or parent ID. Those references remain
  absent until an adapter supplies actual identity evidence. Quoted text is not
  an identifier.
- `IsEcho` is an observed self-message flag. It does not by itself prove delivery
  for a particular request. Confirmation still requires the current target,
  self identity, content digest, freshness, sequence and uniqueness checks.
- Bridge ACK means submitted. A correlated fresh echo means confirmed, not read
  by a person. Missing ACK or echo remains unknown and never authorizes an
  automatic resend. Echo-inferred submission keeps its missing ACK timestamp.

## Future adapter boundaries

A `SourceAdapter` must validate its own platform allowlist, stable source and
content identities, size limits, timestamps and deduplication scope before
producing the shared inbound envelope. It must distinguish observed author,
parent and root identities from absent evidence. Only capabilities established by
the adapter may be advertised; raw payloads and quoted text must not be copied
into general metadata.

An `ActionAdapter` accepts a validated shared action request. The target must
come from immutable source/reply-target evidence and the accepted Run's trusted
routing facts. Model output supplies text only. The adapter must enforce action
kind, target scope, expiry, idempotency and delivery admission before submitting,
and distinguish submitted, confirmed, unknown and proven failed receipts.
Unknown results require reconciliation, not automatic retry.

Platforms are not a closed enum: any lowercase namespaced name (see
`contracts.IsNamespacedPlatform`) is syntactically valid, and availability is
established only by configured connectors. On threaded platforms a batch may
contain several comments; the model picks one by batch-local `ref`
(`REPLY_TO: <ref>`) and `ActionTargetForBatch` maps that ref back to the stored
message. A multi-message batch without a valid ref fails closed
(`reply_target_missing` / `reply_target_invalid`) instead of defaulting to the
last message.

Bilibili and Web adapters must implement these boundaries against the shared
contracts. No collector or sender for either platform is implemented here. A
platform enum value alone does not establish an installed adapter or capability.

Processing events are read projections of the durable Dispatcher trace. Their
stream ID, sequence and cursor remain the original trace values. Source, envelope
and action references come from each persisted Event's own revision; phase,
outcome and delivery also describe that revision. Neither chat bodies, generated
replies nor prompts belong in those events.

## Implemented functions

`NormalizeRelayMessage(RelayMessage) (contracts.InboundEnvelope, error)` accepts
text type 1, quote type 81, and type 49 with an explicit text quote. Empty
`Source` defaults to WeChat. Explicit `wechat` and a narrowly validated legacy
provenance token are also accepted for compatibility; Bilibili, Web, arbitrary
paths and unknown sources require their own adapter. Empty message/container/author IDs and invalid
contract identities are rejected. Message plus quoted body is bounded to 64 KiB;
the shared envelope contract additionally limits outgoing text to 16,384 runes.
`ReceivedAt` must be the time observed by `handleIncomingMessage`, which overwrites
any relay-supplied receipt time before enqueueing. It is not message creation or
later mapping time. Queued messages retain it across restarts; old queued inputs
without an observed time do not receive an invented timestamp.

`acceptTraceLocked` calls this mapper for every accepted message and persists only
the safe `EnvelopeID`, `DedupKey`, `Source` and observed `ReceivedAt` in optional
`Run.Inputs`. Unsupported legacy inputs continue through the original pipeline
with `inputsMissingReason=relay_input_metadata_unavailable`; successfully mapped
inputs in a mixed batch are retained. Existing Runs are never backfilled.

The relay has no account identity field. `AccountID=wechat-relay` denotes this
single local adapter scope, not a verified WeChat account or permission. It must
be replaced with configured, verified account evidence before multi-account
routing is introduced. No account is inferred from a sender or a file path.
Only non-echo forwarded text has a group reply target and `CanReply`; all other
capabilities are false. Echoes remain observable with no reply capability.
Quoted bodies are not copied into metadata or source references. Deduplication
hashes an unambiguous tuple: source/intakeReceipt/message ID when a trusted local
receipt exists, otherwise source/group/message ID. Call this mapper after the
existing intake boundary clears untrusted receipts; it does not authenticate a
caller-supplied receipt. Repeated normalization preserves envelope/dedup IDs.

`WeChatActionRequest(contracts.Run, string, string, time.Time)` maps only a persisted,
checking/pending/not_started Run with no submission evidence. It uses the
accepted `TargetGroupID`, never a group ID parsed from reply text. The key is the
existing `messageDigest(run.ID + ":bridge")`. The caller supplies trusted expiry:
the earlier of the delivery context deadline (including the current bridge
timeout) and the last message timestamp plus configured `MaxMessageAgeSec`.
There is no additional three-minute mapping lifetime.
`ValidateWeChatActionRequest` compares the complete request against this mapping
and rejects target/account/platform/kind/run/key/expiry changes. An adapter must
still read the current persisted Run and enforce wall-clock expiry and policy
under the existing submission boundary. `deliver` now executes both conversions
after the original route, dry-run, rate, policy and bridge-health checks. It uses
the converted target, controlled reply text, action ID, idempotency key and expiry
in the bridge payload. The existing submission lock, pause/cancel checks and
optional runtime permission gate still govern every POST. At admission it
re-reads the Run, validates the request again and checks expiry.

Only the durable `submission_started` transition sets `Run.Action`, including
action ID, kind, platform, idempotency key, admission ID and observed admission
time. This is the same commit as `DeliveryEvidence.SubmissionStartedAt` and
`BridgeRequestID`; WAL sync still precedes publication and bridge POST. Dry-run,
failed health, rejected permissions and other unsubmitted paths create no action.
Inputs and their missing reason are immutable after acceptance. An Action can
only be added from checking/not_started into submitting, never changed, erased or
backfilled onto an old submission. Its time equals the submission boundary and
its action ID equals the bridge request ID. Optional fields use `omitempty`;
absent fields preserve old Run/Event JSON. Old snapshots/WAL remain readable
without a schema/stream reset; reference comparisons use values, not pointer
addresses. Unknown, submitted and confirmed Runs cannot re-enter delivery.
Existing late/restart echo matching and explicit focus-failure retry behavior
remain unchanged; HTTP 500 and uncertain transport do not cause another send.

`ProcessingArchitectureEvent(Run, Event)` uses the durable event's historical
phase/outcome/time and original cursor identity. `Store.commitRun` saves
`envelopeId`/`source` only for exactly one accepted message with exactly one mapped
input; full multi-input references remain on the Run. It saves `action` only from
the revision where admission occurred. The projection reads these persisted Event
fields, never the latest Run's future action. Legacy events without source
evidence remain absent; unknown platform provenance is not labeled WeChat.
Unknown phase/outcome/delivery retains unknown certainty. Reasons outside the
shared allowlist become `upstream_reason_redacted`.

`ProcessingActionReceipt(Run)` projects existing delivery evidence as
`not_started`, `submitted`, `confirmed`, `unknown` or proven `failed`.
`submitting` is unknown external status until ACK/echo; blocked is not_started.
Free-form failure evidence is reduced to `failed`. Timestamps are detached and
echo-inferred confirmation keeps `SubmittedAt=nil`. This read DTO intentionally
keeps `DeliveryEvidence` for backward compatibility and adds optional
`receipt: contracts.ActionReceipt` only when a real durable `Run.Action` exists.
Admission ID/time come from that Action, never an invented admission for an old
Run. Old Runs keep their evidence and report
`missingReason=action_admission_not_recorded`.

Public ActionReceipt adds optional `submittedAtSource` and `confirmation`
(`requestId`, `echoMessageId`). For `echo_inferred`, exact `SubmittedAt` stays null,
confirmation must be strictly after durable admission, request ID must match the
action and echo ID must match the external confirmation ID. Invented ACKs,
missing correlation and same-time/older echoes are rejected. Empty/default source
preserves the old precise-ACK rule; `bridge_ack` also requires an exact ACK.
Rejected/unknown admissions cannot claim external evidence. This covers reliable
echoes before ACK, after HTTP 500/transport loss and after restart.

## Read-only trace projection

`GET /api/trace/events?projection=processing&cursor=<bootstrap cursor>` supports
the existing `limit` and `wait=1`. Obtain the cursor from
`GET /api/trace/snapshot`. Page `cursor`/`hasMore` and item `streamId`/`seq`/`cursor`
are copied unchanged from the existing trace. Each item contains `event`
(`contracts.ArchitectureEvent`) and the event's historical `delivery`. A detached
`receipt` is included only when the retained Run revision exactly matches that
event; later evidence is never attached to an old revision. Missing evidence is
not reconstructed. The same guards, reset-required response, long polling and
retention rules apply. Without the projection parameter, the original JSON is
unchanged. This adds no journal, sequence, background worker or external call.
`GET /api/trace/runs/<id>` and trace snapshot expose full `Run.Inputs` and
`Run.Action`. Console now reads these persisted references in run details and
architecture SSE. It clears the free-text container label while retaining
validated identities, and never reads the latest Run to fill a historical
event. The control-plane injects its shared Engine catalog into Console.
The same public projection also applies to the original Console run SSE;
sequence, cursor, revisions and all non-redacted fields remain unchanged.

Future adapters implement the actual interfaces in `internal/ports/ports.go`:
`SourceAdapter.Open/Stream/Close` preserves opaque source cursors and validates
every delivery. `ActionAdapter.Validate/Send/Confirm` revalidates at admission,
durably deduplicates before external I/O, preserves queryable uncertain receipts,
and never sends from `Confirm`. The conversions now execute in the real WeChat
pipeline, but do not implement a standalone `ports.ActionAdapter` lifecycle;
Dispatcher still owns submission, durable deduplication and echo reconciliation.
No unsupported Bilibili/Web collector, sender or capability is added.

Offline checks (all fixtures are synthetic):

```sh
go test -race -ldflags=-linkmode=external ./internal/contracts ./internal/observability ./internal/dispatcher
go vet ./internal/contracts ./internal/observability ./internal/dispatcher
```
