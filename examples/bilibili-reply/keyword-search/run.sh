#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <KEYWORD> [--order totalrank|pubdate|click|dm|stow] [--pick N] [--inspect | --start]" >&2
  exit 1
fi

KEYWORD="$1"
shift
ORDER="totalrank"
PICK=""
MODE="inspect"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --order)   ORDER="$2"; shift 2 ;;
    --pick)    PICK="$2"; shift 2 ;;
    --inspect) MODE="inspect"; shift ;;
    --start)   MODE="start"; shift ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

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

cd "$ROOT_DIR"
echo "==> Searching Bilibili videos for keyword: $KEYWORD (order=$ORDER)..."
SEARCH_JSON="$RUN_DIR/search-results.json"
go run -ldflags=-linkmode=external ./cmd/bilibili-connector \
  -search "$KEYWORD" \
  -search-order "$ORDER" \
  -search-limit 5 \
  -json > "$SEARCH_JSON"

cp "$SCRIPT_DIR/bilibili-search.example.json" "$RUN_DIR/bilibili.json"
cp "$SCRIPT_DIR/relay.example.json" "$RUN_DIR/relay.json"
cp "$SCRIPT_DIR/dispatcher.example.json" "$RUN_DIR/dispatcher.json"
cp "$ROOT_DIR/configs/execution-store.example.json" "$RUN_DIR/execution.json"
cp "$ROOT_DIR/configs/backends.example.json" "$RUN_DIR/backends.json"
cp "$ROOT_DIR/internal/dispatcher/local-policy.example.json" "$RUN_DIR/local-policy.json"

python3 - "$RUN_DIR" "$SEARCH_JSON" "$KEYWORD" "$ORDER" "$PICK" <<'PY'
import json, pathlib, sys

run_dir = pathlib.Path(sys.argv[1])
search_path = pathlib.Path(sys.argv[2])
keyword = sys.argv[3]
order = sys.argv[4]
pick_raw = sys.argv[5]

videos = json.loads(search_path.read_text(encoding="utf-8"))
if not videos:
    raise SystemExit("No videos returned for keyword")

for idx, v in enumerate(videos, 1):
    print(f"  [{idx}] {v['bvid']} | oid={v.get('oid','')} | comments={v.get('comment_count',0)} | author={v.get('author','')} | {v.get('title','')}")

chosen = None
if pick_raw:
    idx = int(pick_raw) - 1
    if idx < 0 or idx >= len(videos):
        raise SystemExit(f"--pick {pick_raw} out of range (1..{len(videos)})")
    chosen = videos[idx]
else:
    for v in videos:
        if v.get("comment_count", 0) > 0:
            chosen = v
            break
    if chosen is None:
        chosen = videos[0]

bvid = chosen["bvid"]
oid = chosen.get("oid", "")
title = chosen.get("title") or f"Bilibili {bvid}"
cid = f"bilibili:{bvid}"
print(f"==> Selected target: {cid} ({title})")

bili_cfg = json.loads((run_dir / "bilibili.json").read_text(encoding="utf-8"))
bili_cfg["state_dir"] = str(run_dir / "state-bilibili-connector")
bili_cfg["targets"] = [{
    "container_id": cid,
    "label": title,
    "bvid": bvid,
    "oid": oid,
    "comment_type": 1,
    "enabled": True,
    "poll_limit": 20,
    "sort_mode": "time",
    "sub_reply_limit": 5,
}]
bili_cfg["search_targets"] = [{
    "keyword": keyword,
    "order": order,
    "max_videos": 1,
    "min_comments": 1,
    "comment_type": 1,
    "poll_limit": 20,
    "sort_mode": "time",
    "sub_reply_limit": 5,
    "enabled": False,
}]
(run_dir / "bilibili.json").write_text(json.dumps(bili_cfg, ensure_ascii=False, indent=2), encoding="utf-8")

for name in ("relay.json", "dispatcher.json"):
    p = run_dir / name
    text = p.read_text(encoding="utf-8").replace("BV1example0001", bvid).replace("Example Search Video", title)
    p.write_text(text, encoding="utf-8")
PY
chmod 600 "$RUN_DIR"/*.json

go run -ldflags=-linkmode=external ./cmd/jinx-dispatcher \
  -check-config \
  -config "$RUN_DIR/dispatcher.json" \
  -local-policy "$RUN_DIR/local-policy.json"

if [[ "$MODE" == "inspect" ]]; then
  echo "==> Collecting real comments from selected video (read-only, skip-relay)..."
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
