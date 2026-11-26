# Unified execution entrypoint

This command constructs a real `runtimebridge.Runner` and injects it with `dispatcher.NewDispatcherWithOptions`. It does not use a subprocess adapter. The three runtime file flags are required. The optional `-local-policy` selects a trusted operator-owned file, falling back to the operator process's `JINX_LOCAL_POLICY` environment variable. An explicit flag takes precedence. No proxy flag, provider URL override from HTTP or automatic model probe is provided.

From `<PROJECT_ROOT>`, build explicitly:

```sh
go build -ldflags=-linkmode=external -o /tmp/jinx-dispatcher ./cmd/jinx-dispatcher
```

An operator can later run the built command explicitly:

```sh
/tmp/jinx-dispatcher \
  -config /absolute/operator/dispatcher.json \
  -execution-config /absolute/operator/execution.json \
  -backends-json /absolute/operator/backends.private.json
```

Add `-local-policy /absolute/operator/local-policy.private.json` when private identity, communication or support behavior is needed. The path is passed directly through `dispatcher.Options.LocalPolicyPath`; Dispatcher validates the private schema and file permissions. Policy files and communication/instruction sources must be regular files with mode `0600`. These inputs are read from local disk only and cannot be supplied through HTTP configuration. An invalid or unavailable policy fails startup. Omitting the policy retains generic defaults and disables private support behavior.

Validate a dispatcher config and its optional local policy without starting the service:

```sh
/tmp/jinx-dispatcher -check-config -config /absolute/operator/dispatcher.json
```

Check mode requires only `-config`; `-local-policy` retains the same environment fallback and explicit-flag precedence. It calls `dispatcher.ValidateConfigFile`, rejects unknown fields and trailing JSON, and prints exactly `configuration valid` on success. It reads the config and policy without acquiring a lock, creating logs, loading backend definitions or opening the execution store. Errors use fixed messages without private input values. Execution mode still requires all three runtime file flags.

`-execution-config` is an existing execution engine store (snapshot plus receipts), as created and configured through `execution.Open` / `ApplyConfig`; it is not a bare routing `Config` object. The CLI does not create an empty execution store or apply sample routes. Backend catalogs must match the selected route/model configuration. Loading validates local config only and does not call a model.

The running service uses the existing Dispatcher `Start`, `Handler` and `Close` lifecycle and config-selected listen address, with SIGINT/SIGTERM shutdown. It acquires exactly `<directory-of-config>/.dispatcher.lock` before constructing Dispatcher and holds it through shutdown, sharing the legacy command's lock and `logs/state.json` directory. Use the same config directory for an eventual replacement; a separate directory is a separate lock/state namespace. No replacement, service startup or production switch is performed by this task.

The shared [operator backend file](../../internal/runtimebridge/README.md#operator-backend-file) supports `alma_personal`, `text_only` and `legacy_hermes` with schema version `1`. Legacy Hermes settings are fixed in the private operator file: absolute `command`, optional absolute `dir`, `profile`, allowlisted `toolsets`, and `maxTurns`. No `env` field is accepted. Legacy definitions reject Alma URL/provider fields; Alma definitions reject Hermes settings. HTTP routing and catalog DTOs expose no operator command/path settings. Loading an existing store does not execute Hermes or contact Alma.

Dispatcher already consumes the runner's [delivery gate, metadata and cancellation interfaces](../../internal/runtimebridge/README.md#dispatcher-integration). The gate checks the source group's execution permission at durable submission admission, then releases the execution lock before bridge POST and echo waiting. Traces retain actual execution attempts/revisions; terminal paths release runner state. `/api/control/snapshot` reports `deliveryGate=true`, `stopExecution=true`, `legacySoft=false`, and `remoteCancel=false` for this entrypoint. The stop hook reports local cancellation evidence without asserting a remote stop. Hermes CLI toolset selection retains soft permission evidence and the existing engine fallback restrictions.

Offline verification from the module root:

```sh
go test -race -ldflags=-linkmode=external ./internal/runtimebridge ./cmd/jinx-dispatcher -count=1 -timeout=90s
go vet ./internal/runtimebridge ./cmd/jinx-dispatcher
```

Tests use temporary operator files/stores, local HTTP fixtures and a fixed shell stub. They check exact model CLI arguments, read-only construction, private HTTP output and connected control capabilities. The LocalPolicy acceptance test starts an isolated Dispatcher with a fake Runner and owned relay/Bridge fixtures, verifies actual private prompt content and confirms a synthetic echo, then checks HTTP/trace/config snapshots for leaks and rejects policy downgrades. It never uses production state or a real group.
