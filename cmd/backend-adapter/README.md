# backend-adapter

Independent compatibility executable for an operator-approved migration. It does
not modify the current adapter, LaunchAgents or dispatcher configuration. See
`../../internal/execution/README.md` for the engine API and permission boundaries.

Build with Go 1.22.5:

```sh
go build -ldflags=-linkmode=external -o /tmp/jinx-backend-adapter ./cmd/backend-adapter
```

Set JINX_OPERATOR_PATH and JINX_ROUTING_PATH explicitly, or pass `--operator` and
`--routing`. The former is a trusted operator file; the latter is the independent
engine store created and published through the Go API. Neither file is copied
from live services. A fictional operator file:

```json
{
  "schemaVersion": "1",
  "backends": [
    {
      "id": "demo-personal",
      "kind": "alma_personal",
      "models": [{ "id": "demo-provider:demo-model", "label": "Fictional model" }],
      "baseURL": "http://127.0.0.1:39001"
    },
    {
      "id": "demo-text",
      "kind": "text_only",
      "models": [{ "id": "demo-provider:demo-model", "label": "Fictional model" }],
      "baseURL": "http://127.0.0.1:39002",
      "textAPI": "/v1/chat/completions"
    }
  ]
}
```

The text example is an explicitly configured OpenAI-compatible model service,
not a claim that Alma exposes that route. No generation endpoint is inferred.
Legacy Hermes operator entries use kind legacy_hermes and hermes fields command,
dir, profile, toolsets and maxTurns. Only operator files/code control commands,
URLs and models. No secrets are needed in either JSON schema.

Compatible invocation:

```sh
/tmp/jinx-backend-adapter --operator /fictional/operator.json --routing /fictional/execution.json \
  -p demo-profile chat --query-file - -Q --format stream-json
```

stdin is bounded to 1 MiB. stdout has exactly one result envelope with text,
session_id, exit_code, backend, backend_id, run_id, fallback, fallback_reason and
an execution DTO containing attempt evidence. Alma personal uses backend Alma
and session_id alma:<remote ID>; legacy uses Hermes and its actual session ID;
controlled generation uses TextOnly and an execution:<local ID> marker. Failures
emit an unsuccessful result and a nonzero process exit. No diagnostics/tool
stdout is forwarded as reply text.
Execution failures retain the execution DTO, including attempts and their remote
IDs and certainty. A remote run that may still be running returns
`execution_uncertain` without dispatching a fallback; the original failure code
remains in the attempt reason.

Known compatibility arguments are -p/--profile, chat, --query-file -, -Q,
--format stream-json, --resume, --max-turns, -t/--toolsets, -m/--model and
--accept-hooks. Unknown/duplicate flags and arbitrary file/command/provider
arguments fail. -m must match the published primary model. --resume is accepted
as a legacy marker but never resumed, because only independent_run is supported.
--max-turns, toolsets and --accept-hooks do not override operator/runtime policy.
`-t none` additionally requires a published hard noTools policy. Support mode and
recharge are not implemented and are rejected; this executable is not an
automatic replacement for those specialized live paths.

Profile-to-group mapping lives in Config.profiles. The revision read during
profile/model checks is required again at Execute admission. No missing mapping
or runtime failure implicitly launches Hermes: only the validated fallback chain
can do so. Text-only fallback never widens to personal/Hermes.

CLI output is generation/handoff evidence only. This process checks delivery
policy before returning but cannot atomically control a later send in its parent
process. The dispatcher must integrate Engine.WithDeliveryGate or an equivalent
current policy check at its actual send boundary before permissions can be
advertised as hard for delivery. Existing specialized support validation also
needs an explicit integration update; this patch does not alter dispatcher.

Verification is fully offline:

```sh
go test -race -ldflags=-linkmode=external -timeout=60s ./internal/execution ./cmd/backend-adapter
go vet ./internal/execution ./cmd/backend-adapter
```
