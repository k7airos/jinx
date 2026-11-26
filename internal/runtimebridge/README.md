# Dispatcher execution bridge

`Runner.Run` implements `dispatcher.Runner` and calls `execution.Engine.Execute` directly. A Dispatcher prompt already includes instructions, recent history and pending messages. `ConvertRequest` sends that prompt as one user message, without appending `BackendRequest.Messages` a second time. If the prompt is absent, it converts only the supplied batch messages. Profile, previous session, dispatcher snapshot and sender metadata are not forwarded as model history. The group ID, not the profile, selects the execution route.

`ConvertResult` maps text, fallback and execution run ID into the existing Dispatcher result. The compatibility backend is `Alma` for personal/text-only execution and `Hermes` for legacy execution. The exact operator backend ID, model, attempts and revisions are available through `Meta`. Every nonempty session marker is `execution:<runID>`; it is not an Alma thread or a Hermes resume session. For personal dispatch, the external run ID is in `Meta(traceID).Attempts[].ExternalID`.

Only execution results are retained in memory by trace ID. `Meta` returns a defensive copy with `Text` removed; requests, prompts and HTTP bodies are not retained or persisted by this bridge. `Forget` clears terminal outcomes such as dry run, silence, generation failure, output rejection and cancellation. Dispatcher must call it even when delivery is never attempted.

## Dispatcher integration

`Options{Runner: runner}` connects execution, metadata, delivery admission, terminal cleanup and cancellation through structural interfaces using execution types. Dispatcher reads `Meta` immediately after generation, before cleanup, and appends real backend/model, attempts, external IDs and execution revisions to trace. Dispatcher configuration versions remain immutable. Support mode uses execution metadata for structured runners.

Dispatcher calls `DeliveryGate(ctx, traceID, sourceGroupID, admit)` under its submission lock. The callback rechecks local delivery controls and persists admission. The execution policy lock is released before bridge POST and echo waiting. The source group owns the policy even when delivery targets another group. A policy/revision denial suppresses submission; a started gate consumes the result, and missing, active or already consumed results fail closed. `Forget` runs on every batch exit, including silence, dry run, blocked output, failed generation and interruption.

`stop_execution` calls `StopExecution`, which forwards `Engine.Cancel(traceID)` and reports the actual cancellation evidence. Support for this hook does not prove a remote task stopped. `/api/control/snapshot` reports `deliveryGate=true`, `stopExecution=true`, `legacySoft=false`, and `remoteCancel=false` for this runner. `legacySoft` describes an ungated Dispatcher runner; a `legacy_hermes` backend still retains soft tool-permission evidence inside the connected engine. See [Dispatcher control integration](../dispatcher/CONTROL.md).

## Operator backend file

The private file is read only at CLI construction and is not added to Dispatcher config or HTTP responses. Schema version `1` supports all three backend kinds:

```json
{
  "schemaVersion": "1",
  "backends": [
    {
      "id": "personal",
      "kind": "alma_personal",
      "models": [{"id": "operator-model-id", "label": "Operator model"}],
      "almaURL": "http://127.0.0.1:PORT"
    },
    {
      "id": "text",
      "kind": "text_only",
      "models": [{"id": "operator-model-id", "label": "Operator model"}],
      "almaURL": "http://127.0.0.1:PORT",
      "providerID": "operator-provider-id"
    },
    {
      "id": "legacy",
      "kind": "legacy_hermes",
      "models": [{"id": "operator-model-id", "label": "Operator model"}],
      "hermes": {
        "command": "/absolute/operator/bin/hermes",
        "dir": "/absolute/operator/work",
        "profile": "fixed-profile",
        "toolsets": ["web", "memory", "skills"],
        "maxTurns": 3
      }
    }
  ]
}
```

`PORT` and model/provider IDs must be supplied by the operator. Existing personal/text-only schema version `1` files remain compatible. `alma_personal` and `text_only` reject any `hermes` field, including null; `legacy_hermes` requires `hermes` and rejects `almaURL` or `providerID`, including empty/null fields. Unknown fields, duplicate keys/IDs/models, invalid model IDs and empty labels are rejected during loading. Loading does not contact a backend, probe a model or execute a command; `execution.Open` validates the resulting backend options again.

Hermes command, optional working directory, profile, toolsets and turn limit are fixed by this operator-owned file. The command must be absolute; a supplied directory must be absolute. The profile is a token of at most 128 characters starting with a letter or digit, with only letters, digits, `_`, `.`, and `-`. `maxTurns` is 1–100; toolsets allow only unique `web`, `memory`, and `skills` entries. There is no `env`, shell argument list, credential, proxy, arbitrary protocol path or HTTP client field. The engine inherits the operator process environment. Chat input goes to stdin; the engine passes the selected catalog model as the single `-m` argument (Hermes' model flag), alongside the fixed CLI settings, with no resume session. HTTP routing selects only existing backend/model IDs. Backend catalogs and other HTTP DTOs contain no Hermes command, directory or profile, and HTTP configuration cannot inject them.

CLI toolsets remain **soft** evidence (`hermes_cli_toolsets`), not host isolation. A hard no-tools policy rejects Hermes, hard text-only restrictions cannot fall back to Hermes, and unknown execution after command start blocks fallback. These are the existing engine admission/fallback rules.

For Alma backends, only numeric loopback HTTP origins with an explicit port are allowed; redirects and environment proxies are disabled. `text_only` POSTs only to `/anthropic-proxy/<providerID>/v1/messages`. It omits tools and tool choice, never dispatches tools, rejects all non-text content blocks (including tool use), incomplete replies and a response model that differs from the selected model. Request JSON is bounded to 1 MiB and response JSON to 4 MiB; backend error bodies are not surfaced.

Personal dispatch uses the existing execution implementation and its prompt convention. Real sourceThread binding is not proof of agent, session or tool isolation; this work makes no live API binding/isolation claim. Synthetic `sourceThreadId` fields in tests only exercise response compatibility.

## Verification

With Go 1.22.5, from the module root:

```sh
go test -ldflags=-linkmode=external ./internal/runtimebridge ./cmd/jinx-dispatcher -count=1 -timeout=60s
go test -race -ldflags=-linkmode=external ./internal/runtimebridge ./cmd/jinx-dispatcher -count=1 -timeout=90s
go vet ./internal/runtimebridge ./cmd/jinx-dispatcher
```

Tests use temporary configuration/state, `httptest` Alma dispatch/poll and Anthropic servers, a fixed temporary shell stub for Hermes arguments/stdin, HTTP DTO/injection checks, a fake Dispatcher factory that invokes the injected runner, real Dispatcher construction without `Start`, and local lock checks. No coding-agent CLI, real upstream, model probe, group message, production state or LaunchAgent is used.
