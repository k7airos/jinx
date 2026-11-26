# Actual console HTTP contract

This is the implemented HTTP subset, not the full document 1.2 ConsoleClient.
`client-contract.ts` contains types and an adapter interface. The standalone
`architecture-client.ts` implements architecture reads/SSE only; the full client
and UI are not implemented. All paths start with **`/console/v1`**. Do not prepend `/api`.
All non-SSE responses are `{ok:true,value:T}` or `{ok:false,problem:Problem}`.
Operation reads return bare Operation values, not Envelope values.

## Three-layer architecture

`GET /architecture` returns a bare `ArchitectureSnapshot` in Result.value with
revision, observedAt, layers=[source,processing,action], sources, processing,
actions and eventStreams. Capability entries carry id/supported/enforcement/source/
reason. WeChat ingest uses Relay health; WeChat reply_text send uses Bridge health.
For configured hosts, supported remains true during health failures; availability
separately reports supported/enabled/reason, and state retains the observed health.
Bridge permissions establish host availability only; adapter_contract remains
unsupported. Source accountId/containerIds are null when configuration identity is
unknown; no logged-in WeChat account is guessed or discovered.
Bilibili/Web, other actions, generic adapter contracts, durable action admission
and independent action confirmation are explicitly unsupported. No Relay DB
enumeration, content, logs, prompts, paths or upstream error bodies are used.

Processing reads the real Dispatcher trace snapshot and independently probes
replay. An optional `Options.ExecutionCatalog` reads an actual Engine catalog;
without injection backend_catalog is unsupported and backends=[]. agents=[] with
agent_catalog unsupported. Catalog soft/hard evidence is retained conservatively;
model prose and allowlist refs never establish capabilities. Existing executable
control-plane wiring injects the same Engine used by Gateway; standalone console-api
has no Engine. Dynamic skills/tools/knowledge registries remain unsupported.
revision hashes public content,
not observedAt, and is neither a CAS nor an atomic multi-service position.

`GET /architecture/events?cursor=...&domain=processing` shares the strict trace
replay/longpoll/cancellation/keepalive/reset behavior described below. Missing
domain defaults to processing. Source/action return 501 unsupported before SSE;
invalid/unknown/repeated/empty filters return 400. Last-Event-ID takes precedence.
Frames use event:architecture, id=opaque Dispatcher cursor and data is validated
contracts.ArchitectureEvent, currently domain=processing, kind=run_changed,
runId=actual trace entity ID, occurredAt=original event time, and envelopeId/source/
action from that persisted Event. Old events retain empty envelopeId and null
source/action even when the latest Run has newer associations. Public source labels
are cleared; no content or private paths are returned. eventId is a stable cursor digest, not a replay cursor.
Reasons are enum-only; unknown reasons become upstream_reason_redacted. Unknown
run states preserve certainty=unknown. No source/action events are synthesized.
Read a new architecture snapshot after reset and use its processing stream cursor.
The original `/events` event:run wire format remains unchanged.

RunView adds optional inputs/inputsMissingReason/action/receipt fields for older
clients. Inputs contain only persisted envelopeId/dedupKey/source and optional
receivedAt. ActionRef retains persisted idempotencyKey/admissionId/admittedAt.
Receipt is a processing projection `{status,evidence,receipt,missingReason}` from
DeliveryEvidence and the persisted ActionRef admission. Its nested ActionReceipt
is validated; absent admission gives receipt=null and action_admission_not_recorded.
It preserves bridge_ack versus echo_inferred, unknown submission outcomes and null
ACK timestamps; a legacy run without association or evidence has receipt=null.
These recorded admission facts do not claim a full ActionAdapter implementation.

`architecture-client.ts` implements getArchitecture and subscribeArchitecture
as a small standalone same-origin client; the complete CurrentConsoleHTTPClient
and the three-layer UI remain interface-only. It validates current event shapes,
passes the opaque cursor separately and closes on errors/reset before callbacks.
See `docs/architecture-v2.md` for exact boundaries, fields and extension protocol.

## Runs

`GET /runs` returns `Envelope<RunPage>`. Query fields:

| Field | Actual semantics |
| --- | --- |
| `from`, `to` | Optional RFC3339 timestamps, acceptedAt interval `[from,to)` |
| `groupId` | Exact source OR target group ID |
| `phases`, `outcomes`, `deliveries` | Repeated or comma-separated enum values; OR within one field, AND across fields |
| `backendId` | Exact planned backend OR any actual attempt backend |
| `fallback` | `true` means an attempt records fallbackReason; `false` means no recorded fallback evidence, not proof that none occurred; attempt count alone is insufficient |
| `version` | Exact dispatcher/routing/policy version or fingerprint on the run |
| `completeness` | `complete`, `partial`, `legacy_partial`; no synthetic legacy runs are created |
| `query` | Case-insensitive substring of run ID, source/target ID or message IDs only; no body search |
| `limit` | 1..100, default 50 |
| `cursor` | Opaque nextCursor; resend the same filters AND limit |

Unknown fields, invalid enums and repeated scalar fields return 400 validation.
Sort order is acceptedAt descending, then ID ascending for identical timestamps.
The first page captures an atomic upstream snapshot. All subsequent pages use a
frozen **public DTO** result set, so new runs and updates cannot move or remove rows.
`watermark` and Envelope.revision are the upstream snapshot cursor;
Envelope.entitySeq/streamId refer only to this run snapshot. observedAt stays at
the first page's capture time. Pagination does not represent a live query.

Frozen pages are memory-only, expire after five minutes, and share a 16-query /
32 MiB budget. Cursor bindings include filters, limit, watermark, offset and a
private random token; editing any of those, eviction or a console restart requires
a new query. Failures return 409 `reset_required`, retry `read`. Cursors are opaque
and must not be parsed by the UI. No trace file is opened and no run content is
written to the console journal.

`GET /runs/{id}` returns `Envelope<{run:RunView,timeline:TimelineItem[]}>` or 404
`not_found`. Timeline records are actual dispatcher events with their original
occurredAt, revision and run identity. Global sequence gaps between a single run's
timeline items are normal because other runs can interleave. Missing history is
an explicitly `missing` item with null occurredAt, never a reconstructed event.

RunView follows the document 1.2 fields with `entitySeq` and extra delivery
evidence: submissionStartedAt, noSubmission and a redacted failureEvidence marker.
Attempts preserve execution status and remote uncertainty. A submission ACK is
`submitted`, never `confirmed`; confirmedAt/echoMessageId come from actual evidence.
Group labels are empty and visibility is `not_observed` because trace has IDs only.
Message IDs/count are retained; excerpt is always null; sideEffects is [] because
the trace contract does not provide these facts. Summaries are generated from
phase/outcome/delivery, not reply text. Actions remain unsupported.

Historical config IDs resolve through `GET /config-snapshots/{id}`. Run list rows
keep a conservative missingSnapshotReason until content has been read; run detail
clears it only after a successful historical content read. Never substitute current
rules. Internal paths, free-form upstream errors/replies and extra JSON fields
are not returned. Optional machine metadata is allowlisted as bounded tokens;
raw delivery failure details are replaced by a fixed evidence marker.

## Bootstrap and event resumption

`GET /bootstrap` returns Bootstrap. Its streamId/cursor/lastSeq and
overview.data.trace come from **one** `/api/trace/snapshot` response.
overview.data.recentRuns contains up to ten RunViews from that same snapshot.
An empty supported snapshot has recentRuns=[] and a real lastSeq=0.
No trace API produces null positions and unsupported trace capabilities, without
breaking stats, health or rules. Unreachable/invalid/oversized trace responses
also degrade bootstrap; direct run reads return unavailable instead of fabricated
empty results. Snapshot responses are bounded to 16 MiB.

`traceRuns` (and compatibility alias `runs`) is true only after a valid trace
snapshot. `resumableRunEvents` (alias `events.resume`) additionally requires a
successful nonwaiting `/api/trace/events` probe at the snapshot cursor.
eventStream describes that event capability separately. A snapshot can be usable
while event resumption is unavailable. Read capabilities are discovered on every
bootstrap, not inferred from version strings.

`GET /events?cursor=<bootstrap cursor>` is SSE, with optional `Last-Event-ID`.
Last-Event-ID takes precedence on reconnect. Missing cursor is 400 validation.
Initial replay failure is a normal JSON Problem (409 reset_required, 501
unsupported, 503 unavailable) before SSE headers. A successful response uses:

```text
id: <dispatcher event cursor>
event: run
data: {"streamId":"...","seq":1,"cursor":"...","occurredAt":"...","entity":"run",...}

: keepalive

event: reset_required
data: {"code":"reset_required","message":"...","requestId":"","retry":"read"}
```

Replay pages use dispatcher cursor+limit=100. Drain hasMore pages without wait;
then call wait=1 (upstream bounds it to 20 seconds, console deadline 25 seconds).
The normal short config timeout is not applied to that longpoll. Every event must
be the next sequence in the same stream and page cursor must match its final event.
An upstream 409, sequence gap, cursor inconsistency or stream change becomes
reset_required and closes SSE. Do not reconnect automatically with the bad cursor:
discard stale run state/pages, bootstrap, load required views and resume from the
new snapshot cursor. Events carry change facts rather than full RunViews; refetch
the indicated run and ignore events already covered by its entitySeq/revision.

Once SSE starts, other upstream failures emit `event: unavailable` then close.
Keepalives are comments every 15 seconds, contain no IDs or business timestamps,
and must not advance lastSeq, occurredAt or lastProgressAt. A client disconnect
cancels the in-flight upstream longpoll. Writers use a bounded per-frame deadline;
flushed streams clear the ordinary server write deadline.

**Only the run domain is resumable.** Rule, operation, stats and health reads do
not have a trace sequence. Overview's mixed Envelope deliberately omits streamId
and entitySeq; its trace child is the run position. No config/operation event is
inserted into dispatcher trace. Poll operations after publish/reopen and refetch
rules/changes after relevant actions. Old logs and Relay SSE are never converted
into run events. Pause/resume/cancel/maintenance/service actions remain unsupported.

## Operations and recovery

Publication readback compares the submitted flat patch to the current Dispatcher
configuration. Global fields retain exact JSON values and presence. group_rules
uses a console-owned normalization of the current GroupRule storage schema on
both expected and actual objects; it does not import dispatcher.Config or compare
runtime resolved values. Third-party sources can return either sparse known
fields or explicit JSON zero values under this same contract.

| Group JSON value | Comparison semantics |
| --- | --- |
| Missing known boolean/string/number | Equivalent to false/empty string/0 respectively; a missing field never equals a nonzero or nonempty expected value |
| Missing blocked_sender_ids, keyword_blocklist, regex_blocklist | Equivalent to null; [] remains distinct |
| promo_keywords | Missing, null and [] are equivalent (omitempty, all disable promo); nonempty lists compare exactly |
| self_sender_ids, allowed_types | Both keys required in every retained group; null and [] remain distinct |
| Group IDs and unknown fields | Exact key presence and recursive JSON values, including unknown null versus omission |

Stored zero values retain inheritance/default intent: batch_size=0 does not equal
the current inherited batch size; max_reply_runes=0 does not equal 500. A deleted
group must be absent from readback, even when the retained rule map otherwise
matches. Clearing promo_shadow=false or an optional string to empty can therefore
verify successfully when the Dispatcher omits the zero field on readback, while
a changed actual value stays unknown.

Planning rejects an unprovable group-map replacement with 501 unsupported, before
any write. Missing self_sender_ids or allowed_types are exposed as noneditable.
Invalid known field shapes/types are not silently coerced or dropped. If readback
cannot be normalized losslessly, the operation remains unknown and its summary
explicitly identifies that limitation. Unknown fields are preserved and compared,
not stripped to fit the console schema.

A 2xx acknowledgement plus matching normalized readback is succeeded/verified.
A 500 or missing acknowledgement plus matching readback is still
partial_apply/applied_persistence_unverified, even if the target was saved.
A definite 4xx plus nonmatching unchanged base is failed/failed_not_applied;
other mismatches remain unknown. Matching memory alone never proves persistence.
Readback and operation recovery never retry the write.

`GET /operations/{id}` looks up operation ID first; legacy requestId lookup is
retained as a fallback. Missing fact is 404 not_found. Use
`GET /operations/by-request/{requestId}` for unambiguous request lookup; missing
fact is success with null. These endpoints return bare Operation, preserving the
published wire format. Unknown operations attempt readback only, never a write.

bootstrap.activeOperations contains journaled pending, unknown and partial_apply
operations, sorted by ID. Interrupted pending intents become unknown on reopen.
The bootstrap list does not retry writes or assert successful completion. A known
partial_apply remains recoverable because disk persistence was not proven.

## Configuration history and rollback

`GET /changes/{id}` returns `Envelope<ChangeRecord>`, 404 if not journaled. New
records retain complete allowlisted before, intended target and observed readback
snapshots, with capture time, source and scope. The dispatcher snapshot covers all
registered global and group policy fields except private hermes_profile, plus
group_whitelist, compatibility maps and bridge.dry_run. Relay snapshots cover only
group_control and filters. Unknown/private group fields are retained in the actual
upstream replacement but only their digests enter the journal. Secrets, executable
paths, URLs and whole upstream configs are not journaled. This is a policy snapshot,
not a service backup. Target version is null until all component targets match
readback, including persistence-unverified matches; target snapshot
versions are empty rather than guessed future hashes. Legacy records have null
snapshots and cannot be rolled back.

`GET /changes` accepts scalar `from`, `to` (RFC3339 `[from,to)`), `origin`
(console/external/unknown), `status` (pending/unknown/partial_apply/succeeded/failed),
`component` (dispatcher/relay), `groupId` (affected by the policy), `query`
(case-insensitive change/request ID), `limit` (1..100, default 50), `cursor`.
Unknown/repeated fields are rejected. Sort: createdAt descending, ID ascending.
The cursor binds filters and the exact journal result. Publication/readback updates
between pages return reset_required rather than shift rows. Missing cursor means
first page. Existing `items` and `nextCursor` shape is preserved.

`POST /changes/rollback/plan` takes `{requestId,changeId,base:Versions}` and returns
ChangePlan. It previews the complete previous allowlisted policy against the
**current** base, issues a new plan/token, then uses the ordinary `/changes/publish`.
It never reverses sent messages, session effects or cleared queues. Current private
and unknown fields survive. Recreating a removed group with unrecoverable private
fields is unsupported. Rollback plans and operations retain `rollbackOf` and all
existing request-id, expiry, CAS and crash recovery semantics. Components execute
in `componentOrder`, stop after any unverified step, and never auto-compensate.

## Relay rules

`GET /rules/relay` (alias `/relay/rules`) returns `Envelope<RelayRulesView>` with
only `rules.group_control`, `rules.filters`, `versions`, concurrency and warnings.
`versions.relay` is the opaque CAS revision on a new Relay, or the digest of the
**complete** memory configuration on a legacy Relay, so even drift in a hidden URL
blocks publication. The dispatcher-only version shape
omits relay for backward compatibility. Always reuse the whole returned base.

`POST /rules/relay/plan` (alias `/relay/rules/plan`) accepts PlanInput but restricts
edits to relay_field/group_collection. `/changes/plan` also accepts mixed plans:

```json
{"kind":"relay_field","scope":{"kind":"relay_filters"},"field":"clean_content","value":true}
```

Relay field scopes are relay_group_control and relay_filters. Fields are exactly
the DTO keys in client-contract.ts. Lists/maps replace the specified field; aliases
and explicit group lists require exact numeric @chatroom IDs. No local_db, keys,
server, upstream or URL editor is exposed. Null list edits are rejected; existing
null values remain readable and rollback preserves their original representation.
[] is explicit.
Existing legacy wildcard/name scopes remain readable without being expanded.

`{kind:"group_collection",groupId,enabled}` previews dispatcher enabled/whitelist
and Relay monitored_groups/exposed_groups together. monitor_all must be explicitly
false (it can be set in an earlier relay_field edit in the same plan). This never
silently enables all-group collection. The preview records every changed component;
unchanged components do not receive writes. Dispatcher writes/readback run first,
Relay writes/readback second. A later failure produces partial_apply after an
earlier applied step and no compensating write. Relay-only edits remain possible.

The preferred upstream contract is `GET /api/v1/console-config`, returning only
`{revision,group_control,filters}` with a matching quoted ETag. Console performs a
complete allowed `PUT /api/v1/console-config` using that ETag as If-Match. Relay
compares and persists under its own lock, preserves private fields and invalidates
the revision after legacy updates. This reports writeConcurrency=remote_cas. An
invalid/unavailable CAS endpoint fails closed; only 404/405/501 enables legacy
fallback. An API-mode change since preview requires replanning.

The verified legacy contract: `GET /api/v1/config` returns full JSON without
ETag; `POST /api/v1/config` replaces the full configuration and ignores If-Match.
The console never exposes that full JSON to clients. It merges allowed fields into
a fresh full upstream read, preserving private settings in memory only. A local
mutex plus base digest and immediate preflight reject **observed** external drift;
they cannot close the remote read/write race. writeConcurrency therefore reports
digest_preflight_local_mutex and warnings include relay_no_remote_cas. The new
endpoint provides true CAS; RelayWriter remains an injection point for another
verified atomic implementation on a legacy deployment.
2xx + matching readback verifies persistence acknowledgement; 500 + matching memory
is still applied_persistence_unverified. Every attempted write gets readback.

## Historical content and drift

`GET /config-snapshots/{id}` requests only the new dispatcher endpoint
`/api/trace/config-snapshots/{id}`. Its verified native shape is
`{id,policy,recordedAt,sourceGroupId,rules}`. Console maps the retained camelCase
resolved rules to registered snake_case FieldValue entries; configured is null,
origin=historical_resolved, editable=false. The recorded policy populates
versions.policy; dispatcher/routing/fingerprint are empty/null because this source
does not retain those independent facts. Group label stays empty/not_observed.
Routing/permissions remain null/[] until their historical contract is connected.
Unknown keys are dropped. Source/scope explicitly describe the retained policy.
404/405/410/501 returns success with data=null and Envelope.missingReason. Malformed
or unavailable content fails without reading current settings. Bootstrap advertises
configSnapshots only after successfully reading a retained run snapshot.

`GET /config/drift/{dispatcher|relay}` uses only Options.ConfigurationReader.
Without that hook: available=false, drift=null, memory/disk=null with a missing
reader reason. There are no fixed-path filesystem reads. The hook supplies
authorized observations; drift compares complete provider objects while public
memory/disk projections are allowlisted. Drift in a hidden field can be true while
the displayed projections match. Observation errors return unavailable.

## Overview and attention

Existing stats/controls/service shapes remain backward compatible. New currentWork
counts come from the same retained atomic trace snapshot as recentRuns. Phase
counts are exact phase matches; unknownDelivery counts delivery=unknown;
remoteUnsettled counts runs with at least one may_be_running/unknown remote attempt,
including terminal runs with unsettled execution. These count retained observed
runs, not an assertion of full runtime queue coverage. Missing trace yields null
counts. legacyCounters supplies explicit null for missing counters; relayConnected
preserves false separately from missing. No runtime capability is inferred.

Overview attention and `GET /attention` contain actual failed/unknown-delivery run
facts and failed/partial/unknown publication facts. `GET /attention` additionally
reports runsAvailable. `POST /attention/acknowledge` takes
`{requestId,expectedRevision,attentionId}` and returns Envelope<AttentionItem>.
Acknowledgements persist in console-attention.json with private atomic writes and
survive restart. They only acknowledge a specific evidence revision, never change
business state or suppress a future revision. An unchanged unknown readback keeps
its operation revision stable. Missing evidence is 404; changed evidence is 409.

GlobalRulesView.compatibility preserves all four existing compatibility maps and
adds editable=false with verified semantics: admission requires enabled plus
whitelist; group_profiles supplies prompt background with default_profile fallback;
group_other_bots matches source-group sender names and supplies prompt context.
Neither profile map selects a backend. Global inherited field changes additionally
list every changed group effective value in plan diff, even when configured values
are unchanged. No compatibility map editor is exposed.

GroupView additionally exposes group:GroupRef, uiTargetLabel, labelSource,
routing=null, missingRoutingReason, permissions=[], backendId=null,
latestConfirmedAt=null and attentionCodes=[]. uiTargetLabel is from config.target
and labelSource=config_target_ui_only; it is a GUI search target, not a verified
source group name. Existing id/fields remain. Operation adds nullable maintenanceId,
runId and cancelResult without claiming actions are connected.

## Provider hooks

The command flags are unchanged. Parent integration may construct Options with:

```go
type ConfigurationReader interface {
    ReadConfiguration(context.Context, string) (ConfigurationObservation, error)
}
type ConfigurationObservation struct {
    Memory, Disk map[string]any
    MemoryVersion, DiskVersion string
}
type RelayConfigWriter interface {
    CompareAndSwapRelay(context.Context, string, RelayRules) (RelayWriteReceipt, error)
}
type RelayWriteReceipt struct { StatusCode int }
```

Options fields are ConfigurationReader and RelayWriter. The CAS expected string is
the canonical JSON SHA256 of the complete current Relay config (map keys sorted by
encoding/json). A RelayWriter must compare and persist atomically and preserve
hidden fields remotely; it must not implement another unchecked GET/POST pair.
Receipt status is upstream persistence acknowledgement, not merely memory success.
Services, runtime controls, routing and permissions remain for parent integration;
this package does not install mock providers or enable their capability flags.

## Frontend handoff and target gaps

Use `CurrentConsoleHTTPClient` as the present adapter contract. The document 1.2
ConsoleClient is the full product target and cannot be declared implemented by
casting this interface. The adapter supplies HTTP transport and SSE callbacks;
the server paths and actual DTOs are described here and in README.md.

| Page / target | Real subset available | Still mock in a full target UI |
| --- | --- | --- |
| Overview | Stats, health, recentRuns, stage counts, attention/ack, activeOperations | ControlState[], full group overview |
| Runs + detail | Filters, frozen pages, timeline, attempts, delivery evidence, snapshots, run SSE | Cancel actions, excerpts, side effects |
| Groups + rules | Configured groups, current registered fields, plan/publish | Routing, permissions, backend catalog, runtime controls, group search/paging |
| Services | Health probes and allowlisted facts | Process supervision, actions, maintenance, full target ServiceView |
| Changes + operations | Filtered pages, detail/snapshots, rollback, cross-component publication, reopen recovery | Maintenance/cancel operations |

Current GroupView retains `id` alongside `group:GroupRef`; ServiceView.facts is a
map, not a target fact array; schemas expose type/default/inherited rather than
the target widget/help/effect registry. Overview still uses stats and a single
unsupported controls Availability. Operation maintenanceId/runId/cancelResult are
explicit null placeholders. Availability.reason and FieldValue.reason are strings (empty when
not needed), not nullable. Only trace Envelopes add optional streamId/entitySeq.
Group and service lists remain unpaginated. These differences are intentional in the
current TS file; do not silently adapt missing facts to zero, false or success.

Unsupported target methods: getBackends, planAction, executeAction,
continueMaintenance, cancelMaintenance, global-domain subscribe. All unimplemented routes return
501 unsupported. Mock adapters must be visibly separate from the real adapter,
use no real WeChat data, and never turn a mock capability into a live control.

Both Go and TypeScript numbers currently encode uint64 sequences as JSON numbers.
Clients must use safe integers; at Number.MAX_SAFE_INTEGER the adapter must stop
and report incompatibility rather than round a sequence. Cursor values stay
opaque strings. All time values include a timezone. Preserve null versus [] in
rule edits, especially self_sender_ids and allowed_types.

Submission evidence additionally exposes `submittedAtSource` (`bridge_ack`,
`echo_inferred`, or null for older facts). For echo-inferred delivery the exact
ACK timestamp is unknown and `submittedAt` stays null, even when delivery is
confirmed. Display confirmation evidence without inventing an ACK time.

## Logical catalog, scenario/mount fields, and connectors

- **Extended per-container rule fields** (`group` schema in `Bootstrap.schemas` & group `fields`):
  - `scenario` (string, `<= 64` runes, `^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$`)
  - `response_goal` (string, `<= 200` runes, single-line/non-control)
  - `tone` (string, `<= 200` runes, single-line/non-control)
  - `skills`, `tools`, `knowledge_bases` (`string_list`, nullable; `null` inherits/defaults while `[]` explicitly clears/overrides)
- **`GET /catalog`**: returns `Envelope<CatalogView>` containing sanitized logical IDs only (`scenarios`, `knowledgeBases`, `tools`, `skills`) read from Dispatcher `GET /api/catalog`, without filesystem paths, prompts, or secrets. Older Dispatchers return `501 unsupported`.
- **`GET /connectors`**: returns `Envelope<ConnectorView[]>` (`[bilibili, web]`). Unconfigured connectors make no upstream probe and report `configured: false`, `state: "unsupported"`. Configured connectors probe `GET /health` only and expose `dryRun`, `capabilities`, `controls`, sanitized `targets`, `lastPollAt`, and token-bounded `lastPollErrorCode`.
- **`GET /architecture` connector projection**: when `-bilibili-connector` or `-web-connector` is configured on `control-plane`, `SourceView` (`ingest` + `adapter_contract`, `accountId`, enabled `containerIds`) and declared `ActionView` rows project live connector `/health`; when connector `dryRun: true`, action rows report `state: "not_ready"`, `availability.enabled: false`, `reason: "unavailable"`. Unconfigured platforms remain `unsupported`.
- **`POST /connectors/{platform}/dry-run/plan` & `POST /connectors/{platform}/dry-run/apply`**: confirmed two-step runtime toggle for `bilibili` or `web`. Plan checks `expectedDryRun` (`409 conflict`), enforces 5m TTL and idempotent `requestId`, and returns warnings (`runtime_only_not_persisted`, `live_external_writes_enabled` / `external_writes_blocked`). Apply requires `confirm: true` (`400 validation`), verifies `planId` + `token` (`403`), re-checks live `/health`, invokes `POST /v1/control/dry-run`, and requires a mandatory `GET /health` readback verification (returns HTTP `202 unknown` if outcome or readback is unverified).
