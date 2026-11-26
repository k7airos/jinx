# Console Facade incremental API

This package and `cmd/console-api` implement real upstream adapters. They have not
been deployed or connected to running services during development. The API is an
incremental subset of the console design specification, not the complete target
ConsoleClient contract. `docs/backend-integration.md` is maintained by the parent
integration task.

## HTTP routes

All routes use `/console/v1`, returning `{ok:true,value:...}` or
`{ok:false,problem:{code,message,requestId,retry}}`.

| Method | Path suffix | Behavior |
| --- | --- | --- |
| GET | `/bootstrap` | Overview, capability flags and current editable schemas |
| GET | `/overview` | Allowlisted cumulative stats, health and trace recentRuns |
| GET | `/runs` | Real trace RunViews; filter-bound, watermark-frozen pagination |
| GET | `/runs/{id}` | Run, attempts, delivery evidence and observed timeline |
| GET | `/events?cursor=...` | Run-only SSE replay and bounded upstream longpoll |
| GET | `/architecture` | Three-layer public capability snapshot from health/trace and optional Engine catalog |
| GET | `/architecture/events?cursor=...&domain=processing` | Real run updates as ArchitectureEvent; source/action unsupported |
| GET | `/services` | Fixed `/health` probes only; process state remains unknown |
| GET | `/rules/global` | Editable global fields, versions and compatibility data |
| GET | `/groups` | Configured groups only; no Relay DB enumeration |
| GET | `/groups/{id}` | Rule fields, inheritance, admission and effective dry-run |
| POST | `/changes/plan` | PlanInput; validation, field diff, effect warnings, token |
| POST | `/changes/publish` | PublishInput; durable intent, version check, one write and readback |
| GET | `/operations/{id}` | Operation ID lookup, legacy requestId fallback; readback only |
| GET | `/operations/by-request/{requestId}` | Explicit request lookup, null if absent |
| GET | `/changes` | Filtered, cursor-paged journal records |
| GET | `/changes/{id}` | Complete allowlisted before/target/readback snapshots |
| POST | `/changes/rollback/plan` | Restore previous allowed policy as a new publish |
| GET / POST | `/rules/relay` / `/rules/relay/plan` | Allowlisted Relay rules and plan |
| GET | `/config-snapshots/{id}` | Historical dispatcher policy content or explicit missing |
| GET | `/config/drift/{component}` | Authorized memory/disk reader, missing by default |
| GET / POST | `/attention` / `/attention/acknowledge` | Observed attention and durable acknowledgement |

Read responses include source, observedAt, freshness, completeness and revision.
Run reads consume the Dispatcher's read-only `/api/trace/snapshot`, `/api/trace/events`
and `/api/trace/runs/{id}` HTTP API using the library's actual JSON fields. The
console never opens the trace store or its lock. Bootstrap reuses one atomic
snapshot for cursor/lastSeq/streamId and overview recentRuns, probes replay support,
and exposes traceRuns/resumableRunEvents only when supported. Missing trace APIs
degrade gracefully. Bootstrap also restores journaled activeOperations.

Run queries support time/state/group/backend/version/completeness/ID filters.
Pagination freezes public DTOs for five minutes, sorts equal timestamps by ID,
and binds cursors to filters, limit and snapshot watermark. Cache eviction or a
restart returns reset_required. The cache is memory-only, bounded to 16 queries
and 32 MiB encoded DTOs. Change pages reset when their journal result changes.

SSE drains dispatcher cursor pages and longpolls with wait=1. Only run events are
resumable; config/operation updates do not claim membership of this stream.
Gaps and expired cursors require frontend bootstrap reset. Keepalives are comments
every 15 seconds and do not invent business timestamps. Disconnect cancels the
upstream request. No legacy logs are used. Historical policy content uses the new
dispatcher endpoint; old/missing content returns null with a reason. Service
actions, pause, maintenance, cancellation and permission/routing edits await parent
integration. Stage counts and attention use observed trace/journal facts.

See [API.md](API.md) for precise filters, wire responses, SSE recovery, degradation
and target-page gaps. [client-contract.ts](client-contract.ts) aligns with the actual
Go DTOs; it is a types-only handoff, not a complete implementation of the document
1.2 ConsoleClient. Full target-page controls and unavailable facts need a separate
mock adapter.

The three-layer protocol and implemented subset are documented in
[architecture-v2.md](../../docs/architecture-v2.md). `Options.ExecutionCatalog`
accepts the real Engine's read-only Catalog method; `cmd/control-plane` now injects
the same Engine used by Gateway. Standalone `cmd/console-api` has no Engine and
reports backend_catalog unsupported explicitly. Agent and dynamic resource
catalog, generic source/action adapters and their event streams remain unsupported.
`architecture-client.ts` is an implemented standalone snapshot/SSE client; it does
not implement the rest of CurrentConsoleHTTPClient or a three-layer UI. SSE uses
real Dispatcher processing events only, preserving the existing `/events` route.
Persisted Event source/envelope/action associations are retained without filling
old events from the latest Run. Source labels are omitted from public projections.
RunView adds nullable inputs/action and a processing receipt from delivery evidence
and persisted admission, preserving missing facts and echo-inferred ACK nulls.
This does not claim a complete ActionAdapter. Host support and health are
separate through availability; configured unreachable hosts remain supported.

The request types and response structs live in `types.go`; schemas come from
`rules.go`. PlanInput requires a requestId, the exact versions returned by a rule
read, schemaVersion `console-rules-v1` and 1..100 edits. Supported edits are field
updates, group_admission, create_group, delete_group, relay_field and
group_collection. The latter previews explicit Relay monitoring/exposure plus
dispatcher admission together. Internal profiles and
specialized support controls are read-only. Plans expire after five minutes.

## Publication guarantees

- Upstream URLs are operator-supplied HTTP loopback origins with explicit ports.
  Redirects and proxy environment variables are not used. Frontend fields cannot
  supply request URLs, paths or commands.
- Host and Origin must identify the same loopback HTTP origin. Cross-site requests
  are rejected, CORS is not enabled, JSON input is strict and limited to 256 KiB.
- A group update preserves all sibling entries and uses a flat POST patch with the
  original Dispatcher ETag as If-Match. Admission updates both enabled and whitelist.
- Missing self_sender_ids or allowed_types in a retained group prevents whole-map
  replacement: an older omitempty API cannot prove whether null or [] was
  configured. Explicit null inherits identities or default message types; [] is
  preserved as an empty override. Such missing fields are shown as noneditable.
- Validation is repeated against the current snapshot. Dedicated support routes
  require their own editor. The upstream Dispatcher remains the final validator.
- Preview warns that every group may be affected by the full configuration hash,
  disabling clears Pending, and GUI target uniqueness/Relay visibility are unknown.
- Intent is saved and synced before the upstream write. The private journal uses
  an exclusive process lock, atomic rename and directory sync. Use one dedicated
  state directory per console instance, never an existing service data directory.
- Every attempted write is followed by a readback using a fresh bounded context,
  including when the caller disconnects or the write returns 500. No automatic
  write retry, compensation or rollback occurs.
- An acknowledged 2xx plus matching readback is succeeded/verified. An error plus
  matching readback is partial_apply/applied_persistence_unverified. A definite 4xx
  rejection plus unchanged base is failed/failed_not_applied. Other outcomes remain
  unknown. Runtime readback alone does not prove disk persistence.
- Group readback compares both sides using the console-owned GroupRule JSON
  schema: missing known scalar fields equal their stored zero values, not runtime
  effective defaults. Known nullable lists default to null; promo_keywords alone
  treats null, [] and omission alike because its JSON field uses omitempty and all
  disable the promo path. self_sender_ids and allowed_types must be present and
  preserve null versus []; other non-omitempty lists also preserve null versus [].
  Group keys and unknown fields retain exact presence and values. Missing nonzero
  values and differing actual values never match. Unprovable shapes or field types
  reject planning with unsupported; unprovable readback stays unknown with an
  explicit summary. See API.md for the comparison and recovery contract.
- requestId replay returns the persisted operation without writing again, including
  after reopening. A pending intent found after restart becomes unknown; querying
  the operation attempts readback only. Reusing a plan or request for another
  publication is rejected.

The journal stores complete allowlisted policy snapshots and safe patches with mode
0600. Unknown/private retained group fields are digest-checked and merged from
current memory before publication. Public DTOs omit full service configuration, executable paths, upstream error
bodies and internal profile values. No chat history, raw logs or full prompts are
retrieved. Trace DTOs expose IDs, states, timestamps and version references only;
reply/excerpt content is never returned or journaled. Missing historical config
content is explicit and never replaced with current rules. Console prefers Relay's
new allowlisted console-config GET/PUT endpoint with remote CAS. Legacy full
replacement fallback explicitly warns of the remaining remote race.
Options.RelayWriter can connect another true remote CAS implementation;
Options.ConfigurationReader can connect authorized memory/disk observations. No
default disk paths are read. See API.md for the exact hook contract.
Plan tokens are local capabilities; this is a single-user local boundary,
not authentication or remote access control.

## Offline verification

From the module root:

```sh
GOPROXY=off GOSUMDB=off go test -race -ldflags=-linkmode=external ./internal/console ./cmd/console-api ./cmd/control-plane
GOPROXY=off GOSUMDB=off go vet ./internal/console ./cmd/console-api ./cmd/control-plane
tsc --noEmit --strict --target ES2020 internal/console/client-contract.ts internal/console/architecture-client.ts internal/gateway/gateway-contract.ts
node --test internal/console/architecture-client.test.cjs
```

The tests use httptest servers and temporary journals only, including external
console_test regressions against the real Dispatcher Handler for delete/recreate,
sparse create, empty/default edits and readback uncertainty. They do not start the
Relay database queries, sources/history routes or local monitoring. Real Relay
handler regressions verify its absent CAS, private setting preservation, collection
scope, drift rejection and partial publication without compensation. They do not start the
Dispatcher's workers or contact live services. macOS with this
machine's Go 1.22.5 needs external linking because its loader rejects the default
test binary without LC_UUID. Set GOCACHE to a writable temporary directory if
the default cache is restricted.

The command requires explicit `-dispatcher` and `-state-dir`; optional `-relay`,
`-decision`, `-bridge` configure additional health sources. It defaults to listening
on `127.0.0.1:8090`, validates loopback binding and shuts down on SIGINT/SIGTERM.
No service-management commands or sending endpoints are exposed.
