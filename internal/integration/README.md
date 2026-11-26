# Isolated public-API integration test

`TestRelayDispatcherConsoleLifecycle` wires real Relay config/filter/hub/webhook
and SSE handlers to `dispatcher.NewDispatcher`, its HTTP handler and trace store,
then to `console.New` with its actual HTTP upstream clients and SSE handler.
No private fields, test options, production source edits, or dispatcher-package
test helpers are needed. The test code uses only the Go standard library and
the existing `jinx` packages; it adds no dependencies.

The only backend executable is an absolute path to a temporary `/bin/sh` script.
Its commands are shell builtins: it records arguments and stdin, and prints one
synthetic stream-json result. A temporary fictional mem0 identity satisfies the
adapter's local preflight without contacting a vector store. The Bridge is an
`httptest` server returning synthetic health flags and `OK`; it sends its self
echo through the real Relay webhook and SSE, including cross-group routing.
Those health flags never inspect or request operating-system permissions.

The test verifies:

- Backend stdin and exact launch arguments originate from the real pipeline.
- Confirmed delivery retains the Bridge request ID and correlated webhook echo.
- Real Facade plan/publish/readback persists exactly one group `dry_run` edit,
  which suppresses the next pipeline delivery. A later `NONE` result is silent.
- Facade list, filters, details, attempts and timeline agree with real trace HTTP
  responses. Bootstrap opens a live SSE stream; a mid-run disconnect followed by
  `Last-Event-ID` replay returns the exact contiguous trace history.
- Reopening Dispatcher and Facade against the same temporary stores preserves
  terminal runs, revisions, stream ID and cursor. Replayed message IDs and a
  repeated publication do not rerun the backend, resubmit, or rewrite config.

All input is fictional, all service state and profiles use `t.TempDir`, and all
listeners are random loopback `httptest` ports. Relay local DB and upstream
listeners, the decision gate and support/reminder paths are disabled. No real
WeChat, LLM, credentials, fixed service ports or running services are used.
The test does not establish real GUI delivery or real backend availability.

Run from `<PROJECT_ROOT>` with Go 1.22 and cached module dependencies:

```sh
GOCACHE="$(mktemp -d /tmp/jinx-integration-cache.XXXXXX)" GOPROXY=off GOSUMDB=off go test -race -ldflags=-linkmode=external -count=1 -v ./internal/integration
GOCACHE="$(mktemp -d /tmp/jinx-integration-vet-cache.XXXXXX)" GOPROXY=off GOSUMDB=off go vet ./internal/integration
```

External linking avoids this macOS Go 1.22.5 loader's missing-`LC_UUID` error.
Only this directory is owned by the integration task.

Verified with Go 1.22.5 on darwin/arm64: the verbose race run passes with three
runs and 29 contiguous events; five consecutive race runs pass in 6.874s, and
`go vet ./internal/integration` exits 0. Each lifecycle finishes with three
backend invocations, one synthetic Bridge submission and one config write,
including restart and replay.
