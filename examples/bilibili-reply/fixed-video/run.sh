#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <BVID> [--inspect | --start]" >&2
  exit 1
fi

BVID="$1"
shift
MODE="inspect"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --inspect) MODE="inspect"; shift ;;
    --start)   MODE="start"; shift ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

if [[ ! "$BVID" =~ ^BV[0-9A-Za-z]{6,32}$ ]]; then
  echo "Invalid BVID: $BVID (expected ^BV[0-9A-Za-z]{6,32}$)" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
RUN_DIR="$(mktemp -d)"

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

cp "$SCRIPT_DIR/bilibili.example.json" "$RUN_DIR/bilibili.json"
cp "$SCRIPT_DIR/relay.example.json" "$RUN_DIR/relay.json"
cp "$SCRIPT_DIR/dispatcher.example.json" "$RUN_DIR/dispatcher.json"
cp "$ROOT_DIR/configs/execution-store.example.json" "$RUN_DIR/execution.json"
cp "$ROOT_DIR/configs/backends.example.json" "$RUN_DIR/backends.json"
cp "$ROOT_DIR/internal/dispatcher/local-policy.example.json" "$RUN_DIR/local-policy.json"

python3 - "$RUN_DIR" "$BVID" <<'PY'
import json, pathlib, sys
run_dir = pathlib.Path(sys.argv[1])
bvid = sys.argv[2]
cid = f"bilibili:{bvid}"

for name in ("bilibili.json", "relay.json", "dispatcher.json"):
    p = run_dir / name
    text = p.read_text(encoding="utf-8").replace("BV1example0001", bvid)
    if name == "bilibili.json":
        data = json.loads(text)
        data["state_dir"] = str(run_dir / "state-bilibili-connector")
        text = json.dumps(data, ensure_ascii=False, indent=2)
    p.write_text(text, encoding="utf-8")
PY
chmod 600 "$RUN_DIR"/*.json

cd "$ROOT_DIR"
go run -ldflags=-linkmode=external ./cmd/jinx-dispatcher \
  -check-config \
  -config "$RUN_DIR/dispatcher.json" \
  -local-policy "$RUN_DIR/local-policy.json"

if [[ "$MODE" == "inspect" ]]; then
  echo "==> Inspecting real comments for $BVID (read-only, skip-relay)..."
  go run -ldflags=-linkmode=external ./cmd/bilibili-connector \
    -config "$RUN_DIR/bilibili.json" \
    -once \
    -skip-relay \
    -show-comments
  exit 0
fi

echo "==> Runtime directory: $RUN_DIR"
echo "==> Starting relay (127.0.0.1:18080), bilibili-connector (127.0.0.1:18092), jinx-dispatcher (127.0.0.1:18088) in dry_run=true..."

go run -ldflags=-linkmode=external ./cmd/relay -config "$RUN_DIR/relay.json" &
PIDS+=($!)
sleep 1

go run -ldflags=-linkmode=external ./cmd/bilibili-connector -config "$RUN_DIR/bilibili.json" &
PIDS+=($!)

go run -ldflags=-linkmode=external ./cmd/jinx-dispatcher \
  -config "$RUN_DIR/dispatcher.json" \
  -execution-config "$RUN_DIR/execution.json" \
  -backends-json "$RUN_DIR/backends.json" \
  -local-policy "$RUN_DIR/local-policy.json" &
PIDS+=($!)

echo "==> Health & trace endpoints:"
echo "    Connector : http://127.0.0.1:18092/health"
echo "    Relay     : http://127.0.0.1:18080/health"
echo "    Dispatcher: http://127.0.0.1:18088/api/trace/snapshot"
echo "==> Press Ctrl+C to stop."
wait
