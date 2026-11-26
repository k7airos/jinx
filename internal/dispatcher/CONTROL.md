# Dispatcher control and Runner integration

All control and trace endpoints use the existing loopback host guard, the exact
scheme/host Origin guard, and the cross-site Fetch Metadata guard. GET endpoints
do not modify state or contact a backend. Control responses contain IDs, counts,
revisions and evidence codes, never chat bodies, generated text or prompts.

## HTTP contract

`GET /api/control/snapshot` returns `revision`, `global`, all configured and
persisted `groups`, `capabilities` and optional `maintenance`. Each scope exposes
`paused`, `revision`, the incoming-sequence `boundary` and `changedAt`.

`POST /api/control/action` requires `Content-Type: application/json`:

```json
{
  "requestId": "unique-client-request",
  "expectedRevision": "snapshot-revision",
  "action": {
    "kind": "pause",
    "scope": {"kind": "group", "groupId": "100@chatroom"}
  }
}
```

Kinds: `pause`, `resume`, `drain`, `cancel_maintenance`,
`continue_maintenance`, `cancel_run`. Scope is `global` or `group` with a group
ID. Maintenance cancel/continue require `maintenanceId` and the same scope.
Cancel run requires `runId`, `cancelMode` (`block_delivery` or `stop_execution`),
and **the run revision** as `expectedRevision`. Other actions use the control
snapshot revision. Cancel validates the run revision again under the trace lock
and the submission boundary lock. A conflict returns 409 and requires refresh.

Responses contain `operationId`, `requestId`, resulting control `revision`,
`status`, `httpStatus`, and optional scope `control`, `maintenanceId`, `runId`,
`runRevision`, `remoteExecution`, `code`. The durable receipt includes the action
and a request fingerprint. Exact retries return the original result, including
after restart. Reusing a request ID with different content returns 409. Invalid
input returns 400; missing IDs return 404. `GET /api/control/operations/{id}`
queries a receipt without rerunning the action.

`stop_execution` durably blocks delivery before canceling the local context.
Queued runs with no backend attempt are reliably stopped (200). If a remote
attempt has no proven stop, the response is **501**, `status=delivery_blocked`,
`code=remote_cancel_unsupported`, `remoteExecution=may_be_running`. Delivery is
already blocked despite 501; this response must not be presented as no action
taken or as a successful remote stop. Retry is idempotent. `block_delivery`
returns 200 and lets an active execution finish; queued runs are removed from
execution. A run with any durable submission boundary cannot be canceled (409).
Attempt facts and events remain in trace, including later local completion.
When the Runner exposes `StopExecution`, it is called after the durable delivery
block and local context cancellation, outside the state, submission and trace
locks. Only an error-free, supported result with `remoteExecution=stopped` proves
remote stop. Personal API polling cancellation remains 501 with
`delivery_blocked` and `may_be_running`; an unknown/error result never claims a
remote stop. A pending structured execution keeps its trace open just long enough
to append its actual attempt metadata, then records the blocked terminal outcome.
Control writes remain serialized while a stop hook is pending, so concurrent
retries receive the same completed durable receipt; read endpoints remain usable.

## Boundaries and maintenance

Control writes and send admission share `submissionMu`; the durable submission
record is the linearization point. Pause wins before that record: the run becomes
`terminal/suppressed/blocked`, reason `paused_boundary`. Submission wins first:
pause leaves it untouched and echo confirmation continues. Execution admission
also takes this boundary lock. Already executing work may finish, with delivery
blocked. Global OR source-group OR target-group pause is effective. Paused input
continues to advance observation/Seen watermarks and counts without accumulating
Pending or performing support intake. Resume starts from a new receipt watermark;
paused messages are not replayed.

**Pending** means messages durably placed in `GroupState.Pending`, not yet moved
to an executing batch. It is bounded by the existing 200-message limit. Trace
admission and immutable config capture occur when a batch is accepted from that
queue. Pause terminalizes queued messages with `paused_boundary`; because no
execution admission snapshot existed for those messages, their trace explicitly
retains `legacy_queue_admission_history_unavailable` instead of inventing one.

Drain keeps pre-boundary Pending and admitted runs eligible to finish; new input
is observed without entering task or support queues. Its durable maintenance ID,
revision, boundary, start time and five-minute deadline survive restart. Stages
are `draining`, `blocked_by_pause`, `blocked`, `ready`, `canceled`. Counts include
Pending, current run phases, active workers, unresolved runs, `unknownDelivery`
and `remoteUnsettled`. Unproven remote execution and any unresolved run prevent
ready. Pause takes priority in the stage. A deadline expiry blocks ready; continue
renews the deadline. Cancel maintenance neither cancels runs nor resumes user
pauses, and opens intake only from a new watermark.

On restart, active maintenance requires explicit `continue_maintenance`; unstarted
Pending is held until continue or cancel. Previously executed/traced runs are
never regenerated. Observability restart recovery keeps interrupted attempts
unknown; continue does not clear that evidence or its blockers. Applying durable
control receipts are completed during recovery after durable delivery blocks and
pause suppression are restored. No process shutdown, launchctl or service manager
operation is performed here. A service manager must drain, poll a matching active
maintenance until `stage=ready`, and only then stop/restart the process.

## Confirmation precision

Without a request ID carried by trusted echo evidence, new message ID plus a
larger local watermark does not prove a message was created after submission.
Example: watermark 40; unseen message 41 created at T+.1; submission T+.5; receipt
T+.7; timestamp T rounded to seconds. This echo is rejected. A genuinely new echo
created within that same second is indistinguishable and is also rejected; a
timeout remains unknown. This is a deliberate evidence gap, not a healthy
confirmation. A seconds timestamp from the following second, or a millisecond
timestamp strictly after the precise boundary, can match if all existing group,
self identity, digest, ID, incoming sequence and uniqueness checks also pass.
An equal millisecond timestamp is insufficient. There is no new trusted echo
request-correlation source in the current relay payload.

## Configuration snapshots

New accepted runs reference `configSnapshotId` and
`GET /api/trace/config-snapshots/{id}`. Snapshot contents are an explicit allowlist
of effective routing/rule values and user-authored gate criteria. No executable,
profile path, service URL, background persona, secret, full prompt or chat body
is included. Snapshots are persisted before trace admission, detached from mutable
configuration, and never regenerated from current settings. Existing runs retain
their historical missing-snapshot reason. Unknown snapshot IDs return 404.

## Runner API

```go
func NewDispatcher(cfgPath string) (*Dispatcher, error)
func NewDispatcherWithOptions(cfgPath string, options Options) (*Dispatcher, error)

type Options struct { Runner Runner }
type Runner interface {
    Run(context.Context, BackendRequest) (Result, error)
}
type RunFunc func(context.Context, BackendRequest) (Result, error)
type Result = HermesResult

// Optional structural interfaces; types come from internal/execution.
interface { Meta(string) (execution.Result, bool) }
interface {
    DeliveryGate(context.Context, string, string, func(context.Context) error) error
}
interface { Forget(string) }
interface {
    StopExecution(context.Context, string) (execution.CancelResult, bool, error)
}
```

`BackendRequest` exposes `Prompt`, isolated profile name `Profile`, `GroupID`,
`TraceID`, `SessionID`, `ConfigSnapshotID`, detached `*ConfigSnapshot`, and
`[]BackendMessage` (`ID`, `SenderID`, `Content`, `Timestamp`). `Result` currently
contains `Text`, `SessionID`, `Backend`, `Fallback`, `FallbackReason`, `RunID`,
`WebEvidence`. The adapter receives prompt/message content in memory; it is not
included in the control/read API or trace snapshot. Nil Runner retains production
`runHermes` behavior. Dispatcher retains the original `Options.Runner` and detects
these interfaces structurally, without importing runtimebridge. All execution
backend kinds use the same hooks; `Run` is invoked exactly once per batch.

`DeliveryGate` runs under `submissionMu` and receives the trace ID and source group
ID. Its callback performs the final configuration, context, pause/cancel, expiry
and rate validations and persists submission admission. The callback ends before
bridge POST or echo waiting, so the execution policy lock never spans network
delivery or cancellation. A rejected policy/revision/auto-delivery gate produces
`terminal/suppressed`, `permission_changed`, `noSubmission=true`, with no POST.
A gate that returns without calling admission cannot submit. A callback failure
does not cross the boundary. Policy tightening blocks results not yet admitted;
policy expansion does not make an old result eligible retroactively.

`Meta` is read immediately after `Run`, before any terminal decision or gate can
release it. Real backend/model, external IDs, fallback evidence, timestamps and
policy/routing revisions are appended as execution attempts. The initial
`reply_adapter` attempt describes only the invocation. Run `versions` remain the
immutable source dispatcher configuration; execution attempt `versions` record
the actual execution revisions. No generated `Text` is retained in trace.
Legacy adapters retain unknown internal-attempt evidence instead of fabricated
models, timings or internal attempts. Every `processBatch` exit defers `Forget`,
including silent, failed, dry-run, blocked and interrupted paths. Late echo
confirmation uses the durable confirmation index and never depends on Runner
metadata.

Control capabilities expose `deliveryGate`, `stopExecution` and `legacySoft`.
Ungated legacy runners remain compatible and report `legacySoft=true`;
`remoteCancel` stays false because a stop hook alone does not establish remote
cancellation for every backend. Each stop operation reports its actual evidence.

## Offline validation

```sh
go test -race -ldflags=-linkmode=external ./internal/dispatcher -count=1
go vet ./internal/dispatcher
```

Control tests use temporary config/state/trace files, injected runners, and
httptest servers. They never start the live dispatcher, read a live WeChat DB,
or send a real message. Coverage includes pause/submit races, submitted echo
completion while paused, revision recheck, both cancel modes, durable receipts,
read purity, same-second ambiguity, drain isolation, unknown/remote blockers,
maintenance cancellation, restart holds, immutable snapshots, public Runner,
gate denial and revision changes before admission, gate release before HTTP/echo,
actual execution metadata, terminal cleanup, and proven versus unknown remote stop.
