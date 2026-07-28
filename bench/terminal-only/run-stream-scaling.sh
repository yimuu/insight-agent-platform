#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command jq
require_command k6
require_command python3
require_nonempty BASE_URL "${BASE_URL:-}"
stream_agent_id=${GATE_D_STREAM_AGENT_ID:-terminal_stream_fixture}

batch_id=${GATE_D_STREAM_BATCH_ID:-"$(date -u +%Y%m%dT%H%M%S)-${RANDOM}"}
result_dir=${1:-"$terminal_bench_root/bench/results/terminal-only-stream-scaling-$batch_id"}
mkdir -p "$result_dir"
assert_postgres_durability

for scale in 1 10; do
  scale_dir="$result_dir/${scale}x"
  mkdir -p "$scale_dir"
  postgres_command -qAt -c 'SELECT pg_stat_statements_reset();' >/dev/null
  capture_postgres_snapshot "$scale_dir/postgres-before.json"
  BASE_URL="$BASE_URL" \
  AGENT_ID="$stream_agent_id" \
  BATCH_ID="$batch_id-$scale" \
  OUTPUT_SCALE="$scale" \
  SUMMARY_PATH="$scale_dir/k6-summary.json" \
    k6 run "$script_dir/k6/conversation-stream.js" >"$scale_dir/k6.log"
  capture_postgres_snapshot "$scale_dir/postgres-after.json"
  postgres_file "$script_dir/sql/gate-a-write-statements.sql" \
    -qAt >"$scale_dir/postgres-write-statements.json"
  python3 "$script_dir/report.py" gate-a \
    --before "$scale_dir/postgres-before.json" \
    --after "$scale_dir/postgres-after.json" \
    --statements "$scale_dir/postgres-write-statements.json" \
    --output "$scale_dir/write-path-report.json" \
    --conversation
  jq -e '
    (.metrics.conversation_stream_persisted_messages.values.count // 0) == 2 and
    (.metrics.conversation_stream_terminal_frames.values.count // 0) == 1 and
    (.metrics.conversation_stream_calibrated.values.count // 0) == 1 and
    (.metrics.checks.values.rate // 0) == 1
  ' "$scale_dir/k6-summary.json" >/dev/null
done

one_x_deltas=$(jq -r \
  '.metrics.conversation_stream_delta_frames.values.count // 0' \
  "$result_dir/1x/k6-summary.json")
ten_x_deltas=$(jq -r \
  '.metrics.conversation_stream_delta_frames.values.count // 0' \
  "$result_dir/10x/k6-summary.json")
if (( one_x_deltas != 4 || ten_x_deltas != 40 ||
      ten_x_deltas != one_x_deltas * 10 )); then
  printf 'fixture emitted %s/%s deltas; expected exact 4/40 (10x)\n' \
    "$one_x_deltas" "$ten_x_deltas" >&2
  exit 1
fi
assert_postgres_durability
printf '%s\n' \
  "{\"passed\":true,\"one_x_delta_frames\":$one_x_deltas,\"ten_x_delta_frames\":$ten_x_deltas,\"messages_per_turn\":2,\"stream_terminal_get_messages_content_calibrated\":true}" \
  >"$result_dir/stream-scaling-report.json"
printf 'Attached stream scaling evidence: %s\n' "$result_dir"
