# Execution engine

Go 1.22.5 / standard library only. This package is isolated from dispatcher,
console and contracts. It does not send messages, load live configuration, bind
managed agents, run migrations or discover service ports. Tests use httptest and
a temporary fictional Hermes executable only.

## API

```go
func Open(path string, options Options) (*Engine, error)
func DecodeConfig(reader io.Reader) (Config, error)
func Capabilities() []Capability
func Code(err error) string
func NewHTTPTextProvider(options HTTPTextOptions) (*HTTPTextProvider, error)

func (engine *Engine) Catalog(ctx context.Context) ([]Backend, error)
func (engine *Engine) Snapshot(ctx context.Context) (Snapshot, error)
func (engine *Engine) GetGroup(groupID string) (GroupView, error)
func (engine *Engine) GroupForProfile(ctx context.Context, profile string) (string, error)
func (engine *Engine) Validate(ctx context.Context, config Config) error
func (engine *Engine) Plan(ctx context.Context, expectedRev string, config Config) (ChangePlan, error)
func (engine *Engine) ApplyConfig(ctx context.Context, expectedRev, requestID string, config Config) (ApplyResult, error)
func (engine *Engine) Update(ctx context.Context, expectedRev, requestID string, config Config) (ApplyResult, error)
func (engine *Engine) Execute(ctx context.Context, request Request) (Result, error)
func (engine *Engine) Cancel(runID string) (CancelResult, error)
func (engine *Engine) CheckDelivery(ctx context.Context, result Result, groupID string) error
func (engine *Engine) WithDeliveryGate(ctx context.Context, result Result, groupID string, submit func(context.Context) error) error

type Provider interface {
    Generate(context.Context, ModelRequest) (string, error)
}
type ModelRequest struct {
    ModelID string
    Messages []Message
}
```

`Request` has TraceID, GroupID, Prompt, Messages and optional ExpectedRevision.
`Result` has Text, Backend (catalog ID), Kind, Model, RunID (local TraceID), GroupID,
RoutingRevision, PolicyRevision, Attempts, Fallback, FallbackReason, Silent and
DeliveryAllowed. Remote IDs belong to Attempts[].ExternalID. Attempts include
status, reason, timestamps, fallback reason, policy revision and remote execution
certainty. Errors expose Code, Field and CurrentRevision; backend response bodies,
Hermes stderr, URLs and prompt contents are not included in errors.

All exported DTO fields have JSON tags. These DTOs are an execution-specific
contract; they do not silently replace the console's current HTTP DTOs.

## Operator configuration and catalog

Options.Backends is an operator-owned fixed instance list. Each BackendOptions
specifies ID, Kind, Models and either AlmaBaseURL, a Provider, or Hermes options.
The catalog is generated from that list without network requests. Routing stores
only catalog IDs. URLs, commands, profiles, environment variables, credentials and
provider configuration cannot be supplied through Config. Options must be kept
in the trusted backend process, separate from console input. No provider secrets
are persisted or returned by the engine.

Alma personal requires an explicit HTTP base URL with a numeric loopback IP and
port. Redirects are refused and environment HTTP proxies are disabled by default.
An injected Client/Transport is trusted operator code. POST `/api/personal/dispatch`
contains exactly title/task/model, followed by GET `/api/personal/runs/{id}`. Both
top-level runs and `{ "run": ... }` envelopes work. Only done with a nonempty
summary succeeds. queued/running/pending/waiting continue polling. failed and
skipped are stopped terminal failures. Every GET must include the exact dispatched
run ID before its status or summary is trusted. Malformed IDs/status/summary,
HTTP errors and deadline expiry are recorded as failed attempts. No agent ID is
sent or inferred.

The inspected local Alma api-spec documents GET `/api/models` with model IDs of
the form `providerId:modelId`; it does not document a model-generation endpoint.
The old support_web implementation uses a provider-specific Anthropic proxy. This
package does not guess an Alma `/api/model...` endpoint or claim that proxy is a
stable API. An Alma-specific Provider must be supplied by the operator after its
protocol is verified. A text-only backend without a Provider is unavailable and
cannot be published.

`NewHTTPTextProvider` supplies a concrete controlled executor for an explicitly
configured loopback OpenAI-compatible `/v1/chat/completions` service. It makes one
nonstreaming request with `tool_choice: "none"`, sends no tool/function schemas,
and never dispatches tools. It accepts one final text choice with finish_reason
stop; tool/function calls and truncated/empty responses fail. It is not an
automatic Alma API integration. A custom Provider must satisfy the same pure
generation contract, honor context cancellation, and never execute tools, files,
commands, reminders or agent delegation. Arbitrary plugin/agent runners do not
satisfy that contract. Trusted service internals and server-side retention remain
outside this process's isolation evidence.

Legacy Hermes uses a fixed absolute operator command, profile, working directory,
environment additions, model catalog, max turns and toolsets. No shell, caller
command or arbitrary CLI flags are accepted. Known toolsets are memory/skills/web;
their selection is CLI evidence only, not a sandbox. Every call creates a fresh
session. Hermes is never an implicit fallback.

## Permissions and fallback

| Capability | Effective implementation / evidence |
| --- | --- |
| text_generation | hard admission at Execute and immediately before execution |
| tool/host operations on TextOnly | hard denial in the controlled executor: no tool dispatch exists |
| Alma personal tool/host restrictions | soft prompt convention; effective value unknown |
| Legacy Hermes tool/host restrictions | soft CLI selection; effective value unknown |
| auto_text_delivery | host at catalog level; dispatcher must enforce delivery gate |
| web_search / web_fetch enablement | unsupported; no controlled web executor in this package |
| host_write / reminder_sync / host_tools / remote_delegation enablement | blocked; no safe integration |

Every policy must explicitly list all capabilities returned by Capabilities().
False on a soft backend is an intention, not proof of denial. Such a route requires
the explicit `allowSoft: true` opt-in and `noTools: false`. `noTools: true` or
`allowSoft: false` requires a controlled TextOnly backend. Alma personal and Hermes
are rejected for such policies. Console should display permissionEvidence and
enforcedAt; it must not turn a soft declaration into a hard switch. Unsupported
enablement is rejected rather than saved as a pretend capability.

Each fallback is checked against policy and against the primary's and preceding
attempt's hard denials, so later fallback steps cannot undo earlier restrictions.
A text-only primary cannot fall back to personal/Hermes even with allowSoft.
Each runtime fallback repeats this check against the intersection of original
and current policy. A failed attempt also requires remote execution certainty:
only not_applicable or stopped allows fallback, except for a controlled TextOnly
executor whose contract permits pure generation retries through the configured
fallback chain. Personal dispatch response loss, malformed dispatch responses,
poll failures, unknown statuses, wrong/missing poll IDs, local cancellation and
timeouts leave may_be_running. They return the typed error
execution_uncertain/fallback_blocked with the original attempt reason and remote
ID preserved in Result.Attempts, without invoking a fallback. Legacy Hermes is
unknown after command start because stopping its local process cannot prove
delegated work stopped; a failed started command also blocks fallback. Failure to
start the command remains not_applicable. The original routing revision, model, timeouts and fallback
chain stay fixed for the run; policy expansion cannot grant permissions to it.
Tightening blocks subsequent execution/poll/result/delivery boundaries. Already
entered remote work cannot be undone. Personal dispatch holds the configuration
lock through the initial dispatch HTTP response to order submission against CAS;
an in-flight dispatch can delay publication until its attempt deadline. Polling
does not hold that lock over network IO. Remote tools inside an existing personal
run cannot be intercepted; this is why personal is only soft.

Independent sessions are the only advertised mode. The schema includes mode,
rotateAfterTurns and rotateAfterSeconds, but only `independent_run` with both
rotation values zero is currently accepted. Reuse/rotation needs a separate
implementation and is rejected explicitly. No cross-group/backend resume occurs.

## Persistence and publication

`path` is one independent JSON routing/policy store, not dispatcher config. The
single envelope contains revision, config and persistent idempotency receipts;
routing and policy therefore publish in one atomic replacement. The complete
Config shape is illustrated below (all values are fictional):

```json
{
  "schemaVersion": "1",
  "profiles": { "demo-profile": "demo-group" },
  "policies": {
    "demo-policy": {
      "capabilities": {
        "text_generation": true,
        "auto_text_delivery": false,
        "web_search": false,
        "web_fetch": false,
        "host_write": false,
        "reminder_sync": false,
        "host_tools": false,
        "remote_delegation": false
      },
      "noTools": true,
      "allowSoft": false
    }
  },
  "groups": {
    "demo-group": {
      "routeId": "demo-route",
      "policyId": "demo-policy",
      "primary": {
        "backendId": "demo-text",
        "modelId": "demo-provider:demo-model",
        "timeoutSec": 30,
        "session": { "mode": "independent_run", "rotateAfterTurns": 0, "rotateAfterSeconds": 0 }
      },
      "fallbacks": []
    }
  }
}
```

Use DecodeConfig for untrusted JSON: missing/unknown/duplicate fields, nulls,
oversized input and trailing JSON are rejected. Validate checks schema version,
all capability keys, route references, fixed model catalog IDs, session modes,
timeout bounds (1–3600 seconds), primary/fallback permissions and profile mapping.
Open initializes an empty store; deploy code publishes through ApplyConfig.

All cooperating readers/writers use a stable sidecar `.lock` with OS flock.
Updates re-read under the lock, verify expected revision and idempotency receipt,
write a mode-0600 same-directory temporary file, fsync it, rename, fsync the
directory and read back. Errors after rename return persistence_unknown; retry
the same request ID and original expected revision to inspect its receipt.
Reusing a request ID with different input returns idempotency_conflict. A retry
returns both the original AppliedRevision and the latest Snapshot, which may
already be newer. Receipts are retained until the 4 MiB store limit, after which
updates are rejected; there is no silent deduplication expiry. Lock acquisition
is context-cancelable. OS flock currently targets macOS/Linux, not Windows.

Plan returns a digest and affected groups; it is a preview, not an authorization
token or distributed transaction. ApplyConfig/Update revalidate and CAS the whole
Config. Console can stage dispatcher and execution component publication with
separate readbacks and report partial application. Do not report them as one
transaction. All policy versions are the store revision. A run's fixed routing
snapshot is represented by RoutingRevision; full historical snapshot retention
and event persistence belong to the integrating caller.

## Dispatcher integration

The future dispatcher can import `jinx/internal/execution`, construct operator
Options once, Open an independent path, and call:

```go
result, err := engine.Execute(ctx, execution.Request{
    TraceID: traceID, GroupID: groupID, Prompt: prompt,
})
if err != nil { return err }
if result.Silent { return nil }
return engine.WithDeliveryGate(ctx, result, groupID, func(ctx context.Context) error {
    return submitText(ctx, result.Text)
})
```

The callback is the immediate submission boundary, not an echo/confirmation wait.
It holds the routing lock until submission returns; it must be bounded, must not
re-enter engine methods, and must also apply dispatcher pause, freshness, rules,
expiry, dry-run and target checks. Confirmation happens outside this callback.
WithDeliveryGate checks original delivery permission, group and current revision;
any intervening revision invalidates old output, including unrelated changes.
Only pass the server-generated Result to these Go gates; never accept a Result
or DeliveryAllowed assertion from a console client as authorization.
CheckDelivery is useful for preview/CLI handoff only: checking and sending later
is not atomic. No automatic text-delivery hard claim is valid until the caller
actually uses the gate and its own dispatcher boundary. This patch does not
modify that caller.

Cancel takes the local TraceID, requests local context cancellation, and reports
LocalStopRequested=true, LocalStopped=false and remote may_be_running. Execute
returning records a canceled attempt; an uncertain remote run returns
execution_uncertain/fallback_blocked. Personal POST `/api/personal/stop` with
`{ "runId": "..." }` exists but is not implemented by this executor;
RemoteCancelSupported remains false. Cancellation/timeout stops local polling
and never claims a remote run stopped. Late results are not fetched or delivered. Execution requests are
not persisted/deduplicated across restarts; config requests are idempotent.

## Verification

```sh
go test -race -ldflags=-linkmode=external -timeout=60s ./internal/execution ./cmd/backend-adapter
go vet ./internal/execution ./cmd/backend-adapter
```

External linking is required by this machine's macOS Go toolchain. Tests cover
dispatch/poll envelopes and payload, waiting to done/failed, skipped, NONE,
remote failure/empty summary safe fallback, invalid/missing/wrong IDs, timeout,
lost/partial dispatch responses and poll failures without fallback, pure text
retry, legacy execution uncertainty, hard/soft validation, model/session checks, missing Provider,
CAS/reopen/idempotency, concurrent engines, strict JSON, live policy tightening,
delivery gates, local cancellation, HTTP text generation and rejected tool calls,
redirect refusal, fixed Hermes arguments and the legacy stdin/stream-json CLI.
