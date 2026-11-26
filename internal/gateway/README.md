# Control-plane gateway

`gateway.New(Options).Handler()` mounts the Console Facade, execution Engine and
supervisor Manager at one loopback HTTP origin. Construction does not probe or
invoke a provider, execute a service action or start an HTTP listener. The CLI
opens independent journals and starts the HTTP listener only when run.

## HTTP contract

All paths have the `/console/v1` prefix. Responses use `{ok:true,value:...}` or
`{ok:false,problem:{code,message,currentRevision?}}`. Existing Facade responses
keep their existing problem format. Upstream HTTP failures retain their status,
including conflicts and unavailable responses, but never expose upstream error
bodies, headers, URLs, filesystem paths or command output.

| Method | Suffix | Value / request |
| --- | --- | --- |
| GET | `/controls` | Dispatcher ControlSnapshot |
| POST | `/controls/actions` | ControlRequest → ControlOperation |
| GET | `/controls/operations/{id}` | ControlOperation |
| GET | `/backends` | execution.Backend[] |
| GET | `/groups/{id}/routing` | `{groupId,revision,routing}` |
| GET | `/groups/{id}/permissions` | `{groupId,revision,policy,permissions}` |
| GET | `/execution/snapshot` | execution.Snapshot |
| POST | `/execution/plan` | `{expectedRevision,config}` → execution.ChangePlan |
| POST | `/execution/apply` | `{expectedRevision,requestId,config}` → execution.ApplyResult |
| GET | `/services/runtime` | supervisor.ServiceList |
| POST | `/services/plan` | `{expectedRevision,kind,serviceId}` → supervisor.ActionPlan |
| POST | `/services/execute` | `{planId,token,requestId}` → supervisor.Operation |
| GET | `/services/operations/{id}` | supervisor.Operation |
| POST | `/services/maintenance/{id}/continue` | `{}` → supervisor.Operation |
| POST | `/services/maintenance/{id}/cancel` | `{}` → supervisor.Operation |

Maintenance route `{id}` is the **supervisor operation ID**, which the Manager
resolves to the Dispatcher maintenance ID. The direct `/controls/actions` route
uses the Dispatcher `maintenanceId` in its typed action instead.
Continue clears the Dispatcher recovery boundary and polls it. The Manager keeps
the operation blocked with `execute_required` when ready; submit the original
`/services/execute` payload again to authorize command execution. Cancel can also
clean up the boundary after a succeeded or failed operation without changing its
service result, as long as the Dispatcher remains reachable.

`GET /bootstrap` retains the Facade bootstrap shape and augments implemented
capabilities only. `/services` remains the Facade health envelope; clients join
its `data[].id` with `/services/runtime`'s `services[].id` for runtime state.
All other existing Facade routes, including rules, changes, history, attention,
run details, trace snapshots and SSE, are delegated to the existing handler.

Use `gateway-contract.ts` alongside `../console/client-contract.ts`. Added routes
accept no query parameters. POST requires JSON, exactly one object, no unknown
fields, no duplicate keys and a maximum of 256 KiB. Configs contain backend and
model IDs, never backend definitions, environment, URLs or commands. Host must
be loopback with a port. A browser Origin must exactly match the HTTP Host;
same-site and cross-site Fetch Metadata requests are rejected. The gateway
provides no arbitrary upstream URL or path proxy.

## Execution permission strength

`routing.edit` and `permissions.edit` mean typed configuration CAS is implemented,
not that every backend can enforce every requested permission. Plan and apply
validate the Engine's actual catalog. All eight capability keys must be present.
Only `text_generation` and `auto_text_delivery` can be requested true. Only
`independent_run` sessions with zero rotation counts are currently supported.

`hard` is enforced at the named Engine boundary. A configured text-only provider
cannot dispatch tools, so tool denial is hard. `soft` is a prompt or Hermes CLI
convention; Alma Personal and legacy Hermes must not be presented as sandboxed.
`host` requires the Dispatcher delivery gate. `unverified`, or `effective:null`,
is unknown, never false or proof of enforcement. Read `editable`, `reason`,
`provenance`, `policyVersion` and `enforcedAt` rather than inferring isolation from
a backend name. Catalog `available` is configuration availability, not a live
provider health probe. Fallbacks may not weaken the primary's hard denial.

Applying execution config does not publish Dispatcher rules or replace its
Runner. Bootstrap keeps `execution.dispatch` unsupported until that separate
integration is completed. The Runner owner must inject `dispatcher.Options.Runner`,
map BackendRequest to Engine.Execute, propagate trace/backend/model/attempt and
remote execution facts into HermesResult, and enforce Engine.WithDeliveryGate
around actual Bridge submission. Merely reading the execution config in a
second process does not connect either execution or delivery enforcement.

## Drain coordination

The HTTP adapter implements `supervisor.DrainCoordinator`. Snapshot reads the
Dispatcher control revision, the explicit decision dependency from `/api/config`
and retained `/api/trace/snapshot` evidence. Unknown delivery and unsettled remote
attempts block actions even before a maintenance boundary exists. Dependency
version is derived from the dependency settings, separate from control revision.

Drain posts `drain` with global scope, a fresh request ID and expected revision.
Poll reads only the same maintenance boundary; it requires `stage:ready`, active
global persisted maintenance, valid explicit counts, zero pending/workers/
unresolved/unknown/remote counts, no blockers, no recovery, no pause and an
unexpired deadline. Cumulative stats or `pending_messages:0` never authorize a
service command. Continue and cancel post the corresponding maintenance actions
with a freshly read revision. Neither resumes an explicit user pause. If the
Dispatcher is stopped or unreachable, the adapter fails and the Manager retains
an unknown outcome instead of claiming cleanup or resume succeeded.

## CLI

Build without starting any service:

```sh
go build -ldflags=-linkmode=external -o /tmp/jinx-control-plane ./cmd/control-plane
go build -ldflags=-linkmode=external -o /tmp/jinx-dispatcher ./cmd/jinx-dispatcher
```

On macOS versions whose loader rejects Go 1.22.5's internal linker output with
`missing LC_UUID`, add `-ldflags=-linkmode=external` to build and test commands.

| Flag | Default | Meaning |
| --- | --- | --- |
| `-listen` | `127.0.0.1:8090` | Loopback HTTP address |
| `-dispatcher` | required | Dispatcher loopback HTTP origin |
| `-relay` | empty | Optional Relay health/config origin |
| `-decision` | empty | Optional Decision health origin |
| `-bridge` | empty | Optional Bridge health origin |
| `-state-dir` | required | Absolute independent control-plane data directory |
| `-execution-config` | required | Absolute Engine state file under state-dir; created empty if absent |
| `-backend-definitions` | required | Absolute operator-owned schemaVersion 1 backend JSON, shared with jinx-dispatcher `-backends-json` |
| `-service-uid` | `-1` | Explicit GUI UID; absent means runtime observation unsupported |
| `-service-plist-dir` | empty | Absolute trusted plist directory for enabled service actions |
| `-enable-service-control` | `false` | Allows only explicit plan/execute service actions |

State files are `state-dir/console/*`, `state-dir/supervisor.json` when a UID is
set, and the configured execution state file. The execution file must be separate
from console and supervisor state. Do not point state-dir at an existing Relay,
Dispatcher or original wx-controller data directory. The execution state is the
Engine Snapshot plus its internal idempotency receipts, not a naked Config input;
use the typed plan/apply API to configure an initially empty state file.

The canonical backend definitions are the same versioned file consumed by
`cmd/jinx-dispatcher -backends-json`. Both entrypoints call
`runtimebridge.LoadBackendOptions`; the Gateway uses its `Options.Backends`
unchanged, so catalogs, availability and permission evidence match. The file is
local operator configuration, never a web DTO. For example, save the following
as `/absolute/operator/backends.private.json`, replacing model/provider IDs and
the numeric loopback origin with the operator's values:

```json
{
  "schemaVersion": "1",
  "backends": [
    {
      "id": "personal",
      "kind": "alma_personal",
      "models": [{"id": "operator-provider:operator-model", "label": "Personal model"}],
      "almaURL": "http://127.0.0.1:3000"
    },
    {
      "id": "text",
      "kind": "text_only",
      "models": [{"id": "operator-model", "label": "Text model"}],
      "almaURL": "http://127.0.0.1:3000",
      "providerID": "operator-provider"
    }
  ]
}
```

Personal model IDs are the complete opaque `providerID:modelID` values expected
by Personal dispatch, preserved verbatim in catalog, routing and requests. Text
models use the proxy's bare model ID; `providerID` selects the endpoint separately.
Do not strip the personal prefix or add it to a text model when sharing routing.

`text_only` uses `runtimebridge.NewAnthropicProvider` and POSTs only to
`/anthropic-proxy/<providerID>/v1/messages`, with tools omitted and non-text or
incomplete responses rejected. This executor's no-tool boundary supplies the
hard tool denial; Alma `/api/chat/completions` is not used as that boundary.
Personal dispatch retains soft prompt conventions. Version 1 requires a
nonempty catalog, an explicit numeric loopback HTTP origin and a provider ID for
text backends. Unknown fields, duplicate keys, unsupported versions, credentials,
subprocess commands and environment overrides are rejected. Loading, assembly,
catalog reads and config plan/apply do not probe or invoke models.

Both commands can share the exact backend file and Engine state path:

```sh
/tmp/jinx-control-plane \
  -dispatcher http://127.0.0.1:8091 \
  -state-dir /absolute/operator/control-plane \
  -execution-config /absolute/operator/control-plane/execution.json \
  -backend-definitions /absolute/operator/backends.private.json

/tmp/jinx-dispatcher \
  -config /absolute/operator/dispatcher.json \
  -execution-config /absolute/operator/control-plane/execution.json \
  -backends-json /absolute/operator/backends.private.json
```

These are operator run commands, not part of verification. The Dispatcher config
must select the matching loopback listen address. For a new installation, the
control plane creates the empty Engine store; configure routes through typed
plan/apply before starting the Dispatcher, which requires an existing store.
Both commands load backend definitions at construction; reload both processes
after changing this file. Sharing definitions/state does not supply the separate
Dispatcher delivery integration described above.

**Deprecated compatibility:** only the control plane still accepts the old
unversioned envelope, with `almaBaseURL`, trusted `hermes` definitions or an
unconfigured text backend, and logs a deprecation warning. A `schemaVersion`
field, even null or empty, prevents legacy fallback. Legacy `textBaseURL` is
rejected because it lacks a provider ID; migrate it to the versioned format.
Unconfigured legacy text remains unavailable. Legacy Hermes commands are never
executed during assembly. The old format is not shareable with the Dispatcher
and is not a second canonical schema.

Service readback uses a dedicated observer restricted to `launchctl print` of
the four Manager labels; mutating commands remain owned by Manager.

Verification from the module root (temporary state and local test doubles only):

```sh
go test -race -ldflags=-linkmode=external ./cmd/control-plane -count=1 -timeout=90s
go vet ./cmd/control-plane
```
