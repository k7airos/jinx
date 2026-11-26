# Relay Console configuration API

Go 1.22.5. Console should use `/health`, `/api/v1/console-config`, and the
existing streams. Configuration and health read memory only and do not invoke a
source adapter or replay messages. The public module accepts messages through
Webhook or an explicitly configured upstream and does not include local collection.
An operator-owned extension can provide additional source management routes; those
routes are separate from the public Console configuration API.

## GET /api/v1/console-config

Returns **200**, `Content-Type: application/json; charset=utf-8`,
`Cache-Control: no-store`, and a strong `ETag: "<revision>"`.
The JSON has exactly these three top-level fields:

```json
{
  "revision": "<opaque revision string>",
  "group_control": {
    "monitor_all": false,
    "monitored_groups": ["synthetic@chatroom"],
    "exposed_groups": ["synthetic@chatroom"],
    "group_aliases": {"synthetic@chatroom": "Synthetic group"},
    "drop_unexposed_completely": false
  },
  "filters": {
    "keyword_blocklist": [],
    "regex_blocklist": [],
    "sender_blocklist": [],
    "allowed_types": [1, 81],
    "clean_content": true,
    "ignore_echo": false
  }
}
```

`group_control` and `filters` use the original Relay types and JSON field names.
No server, upstream, database configuration, key, or filesystem path is returned.
Revision is a SHA-256 digest of the in-memory full configuration and an internal
write generation; it exposes only the opaque digest, never its input. Successful
legacy writes, including private-field-only writes and same-value writes,
invalidate outstanding revisions. Revisions are equality tokens, not timestamps
or ordered counters. Clients must GET a fresh revision after a restart/conflict.

## PUT /api/v1/console-config

Send `Content-Type: application/json` (an optional charset parameter is accepted)
and `If-Match: "<revision>"`, copying the ETag from GET verbatim. The body contains
exactly `group_control` and `filters` from the example, **without `revision`**.
Both objects and every field within them are required; this is a full replacement
of the public fields. Omitted fields, unknown fields at any level, trailing JSON,
null objects, null booleans, and incorrectly typed values are rejected. Collection
fields may be `null`, `[]`, or `{}` as appropriate. Collection elements cannot be
null. Maximum request body size is **256 KiB**, including whitespace.

Success returns **200**, the same exact three-field snapshot shape as GET, and
its new strong ETag. The saved snapshot is captured while holding the manager
lock; a concurrent write cannot mix the returned data and revision.

| Status | Meaning |
| --- | --- |
| 400 | Invalid/incomplete JSON, unknown field, invalid value, or malformed If-Match (including `*`, a weak ETag, multiple tags, or an unquoted revision) |
| 403 | Non-loopback peer/Host or cross-origin request |
| 405 | Method other than GET/PUT; `Allow: GET, PUT` |
| 409 | Well-formed If-Match does not match the current revision; GET again before retrying |
| 413 | Request body exceeds 256 KiB |
| 415 | Missing/unsupported Content-Type on PUT |
| 428 | Missing/blank If-Match on PUT |
| 500 | Private persistence failed; memory/revision remain unchanged; error omits private paths |

Errors are plain text, not configuration snapshots. JSON shape/type decoding
precedes CAS; value validation follows a successful comparison. If-Match syntax is checked first,
then Content-Type, body decoding, CAS comparison, value validation, and persistence.

## Rule semantics and validation

- Newly created configurations do not monitor or expose any group. Loaded
  configurations retain their explicitly saved scope. PUT does not fill defaults.
- `monitor_all: true` explicitly monitors all incoming groups. A standalone `*`
  in monitored/exposed lists explicitly matches all groups; it is allowed.
  Partial globs such as `room*` and `room?` are rejected. Neither option is added
  implicitly. Exposure still requires the exposed list even with monitor-all.
- Group selectors are trimmed IDs (`[A-Za-z0-9_-]+@chatroom`), exact display names,
  or a standalone `*`. Names cannot contain `@`, `*`, or `?`. Aliases require a
  valid ID key and a nonempty name. IDs/names are at most 256 UTF-8 bytes, without
  control characters. Lists/maps have at most 1024 entries.
- Null/empty monitored lists monitor nothing when monitor-all is false.
  Null/empty exposed lists expose nothing, regardless of monitor-all.
- **Relay `allowed_types: null` and `allowed_types: []` both allow all types.**
  They do not mean “block all”. This is Relay's existing filter behavior; do not
  apply Dispatcher defaults/semantics. A nonempty list allows exact positive Go
  `int` values and matching low-32-bit base types. Extended values such as
  `244813135921` are supported on the 64-bit runtime. Zero, negative, fractional,
  string, null-element, and overflowing types are rejected; at most 1024 entries.
- Null/empty blocklists disable those blocks. Keyword/sender entries are nonempty
  UTF-8 strings of at most 1024 bytes. Regex entries are nonempty, at most 4096
  bytes, and must compile with Go's regexp implementation after trimming.
  Each blocklist has at most 1024 entries. The serialized public configuration
  must also fit within 256 KiB. No rule is silently discarded or repaired.

The endpoint uses the admin loopback and same-origin guard, including matching
Origin scheme/Host/port and rejecting cross-site fetches. It does not grant CORS
access or accept a cross-origin preflight, even if legacy CORS is enabled.
Existing `/api/v1/config` GET and full-replacement POST remain compatible, without
an ETag requirement; they are private administrative endpoints and do not provide
the new public-field isolation. Console should not use them.

## Manager API and atomicity

```go
func (m *Manager) GetPublic() PublicSnapshot
func (m *Manager) UpdatePublic(public PublicConfig) (PublicSnapshot, error)
func (m *Manager) CompareAndSwap(revision string, public PublicConfig) (PublicSnapshot, error)
```

`PublicConfig` has only `GroupControl` and `Filters`. `PublicSnapshot` adds
`Revision`. GET returns a detached copy preserving nil versus empty collections.
UpdatePublic is an unconditional in-process public replacement. CompareAndSwap
accepts the raw revision string (without HTTP quotes) and returns
`ErrRevisionConflict` on mismatch; invalid public values wrap `ErrInvalidPublic`.
HTTP PUT always uses CompareAndSwap.

All writes, including legacy `Update(Config)`, hold `m.mu` through clone,
validation/preparation, private atomic file replacement, memory publication,
and generation advance. Public writes clone current config, apply only public
fields, validate, and commit; opaque extension fields are preserved without interpretation.
Failed writes do not publish memory or advance revision. Files use temporary
files with mode 0600 followed by rename. Revision comparison and writes are
atomic for one Manager; external file edits/multiple processes are not coordinated.

## Lifecycle and verification

The public `pkg/relay` facade provides an optional `Extension` with group discovery,
route registration and Start/Stop hooks. It shares one message pipeline, hub and HTTP
listener with the core. Extension routes use the same loopback/same-origin admin
guard. Invalid or conflicting route registrations fail startup.

Server.Ready closes only after a successful bind. Start reports bind/serve errors;
normal `http.ErrServerClosed` is a clean return. Stop is concurrency-safe, signals
SSE/WS exit, waits for handlers and Serve, and closes HTTP connections if graceful
shutdown exceeds its context deadline. Failed/stopped servers return health 503
with status `failed`/`stopped`. Health reports backend memory state, not source
readiness. The app starts its extension and upstream only after bind succeeds,
stops source callbacks before HTTP shutdown, and returns nonzero on listener failure.

Tests use synthetic input and `t.TempDir()`. Public tests need no private module,
account data or collection dependency. No production relay or upstream needs to be started:

```sh
go test -race ./internal/relay/... ./cmd/relay
go vet ./internal/relay/... ./cmd/relay
```

On macOS versions that reject Go 1.22.5's internally linked binaries with
`missing LC_UUID load command`, use the installed external linker:

```sh
go test -race -ldflags=-linkmode=external ./internal/relay/... ./cmd/relay
```
