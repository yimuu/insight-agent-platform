#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command jq
require_command python3
require_nonempty BASE_URL "${BASE_URL:-}"

mode=${1:-standalone}
case "$mode" in
  standalone|conversation) ;;
  *)
    printf 'usage: run-gate-a.sh [standalone|conversation] [result-directory]\n' >&2
    exit 2
    ;;
esac
result_dir=${2:-"$terminal_bench_root/bench/results/terminal-only-gate-a-$mode"}
mkdir -p "$result_dir"
assert_postgres_durability

request_id="gate-a-${mode}-$(date -u +%Y%m%dT%H%M%S)-${RANDOM}"
if [[ "$mode" == "conversation" ]]; then
  agent_id=${AGENT_ID:-conversation_demo}
else
  agent_id=${AGENT_ID:-action_demo}
fi
tenant_id=${TENANT_ID:-gate-a-tenant}
user_id=${USER_ID:-gate-a-user}

if [[ "$mode" == "conversation" ]]; then
  conversation_request_id="${request_id}-conversation"
  api_curl -X POST "$BASE_URL/v1/conversations" \
    -H 'content-type: application/json' \
    -H "x-request-id: $conversation_request_id" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $user_id" \
    --data-binary "{\"agent_id\":\"$agent_id\"}" \
    >"$result_dir/create-conversation.json"
  conversation_id=$(jq -er '.data.conversation_id' \
    "$result_dir/create-conversation.json")
fi

capture_postgres_snapshot "$result_dir/postgres-before.json"
# Start statement accounting after the before snapshot. The after snapshot
# creates a session-local census table, so its catalog/accounting cost remains
# inside both the pg_stat_wal interval and statement attribution.
postgres_command -qAt -c 'SELECT pg_stat_statements_reset();' >/dev/null

if [[ "$mode" == "standalone" ]]; then
  http_status=$(curl --silent --show-error \
    --output "$result_dir/create-run.json" \
    --write-out '%{http_code}' \
    -X POST "$BASE_URL/v1/agents/$agent_id/runs" \
    -H 'content-type: application/json' \
    -H "x-request-id: $request_id" \
    --data-binary '{"text":"terminal-only Gate A fixed small action"}')
  [[ "$http_status" == "202" ]] || {
    printf 'standalone admission returned HTTP %s\n' "$http_status" >&2
    cat "$result_dir/create-run.json" >&2
    exit 1
  }
  run_id=$(jq -er '.data.run_id' "$result_dir/create-run.json")
  jq -e '
    .data.persistence_mode == "terminal_only" and
    .data.recovery_capability == "none" and
    .data.event_replay == false
  ' "$result_dir/create-run.json" >/dev/null
else
  http_status=$(curl --silent --show-error \
    --output "$result_dir/create-turn.json" \
    --write-out '%{http_code}' \
    -X POST "$BASE_URL/v1/conversations/$conversation_id/messages" \
    -H 'content-type: application/json' \
    -H "x-request-id: $request_id" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $user_id" \
    --data-binary '{"content":{"text":"terminal-only Gate A conversation turn"}}')
  [[ "$http_status" == "202" ]] || {
    printf 'conversation turn admission returned HTTP %s\n' "$http_status" >&2
    cat "$result_dir/create-turn.json" >&2
    exit 1
  }
  run_id=$(jq -er '.data.run.run_id' "$result_dir/create-turn.json")
  jq -e '
    .data.run.persistence_mode == "terminal_only" and
    .data.run.recovery_capability == "none" and
    .data.run.event_replay == false
  ' "$result_dir/create-turn.json" >/dev/null
fi

deadline=$((SECONDS + ${RUN_TIMEOUT_SECONDS:-30}))
status=
while (( SECONDS < deadline )); do
  if [[ "$mode" == "standalone" ]]; then
    api_curl "$BASE_URL/v1/runs/$run_id" >"$result_dir/get-run.json"
    status=$(jq -er '.data.status' "$result_dir/get-run.json")
    case "$status" in
      completed|succeeded) break ;;
      failed|cancelled|timed_out|interrupted)
        printf 'Gate A run reached unexpected terminal state %s\n' "$status" >&2
        exit 1
        ;;
    esac
  else
    api_curl \
      "$BASE_URL/v1/conversations/$conversation_id/messages?limit=2" \
      -H "x-tenant-id: $tenant_id" \
      -H "x-user-id: $user_id" \
      >"$result_dir/messages.json"
    if jq -e --arg run_id "$run_id" '
      any(.data.messages[];
          .role == "assistant" and .run_id == $run_id)
    ' "$result_dir/messages.json" >/dev/null; then
      api_curl "$BASE_URL/v1/runs/$run_id" \
        -H "x-tenant-id: $tenant_id" \
        -H "x-user-id: $user_id" >"$result_dir/get-run.json"
      status=$(jq -er '.data.status' "$result_dir/get-run.json")
      case "$status" in
        completed|succeeded) break ;;
        failed|cancelled|timed_out|interrupted)
          printf 'Gate A conversation run reached unexpected terminal state %s\n' \
            "$status" >&2
          exit 1
          ;;
      esac
    fi
  fi
  sleep 0.05
done
if [[ "$status" != "completed" && "$status" != "succeeded" ]]; then
  printf 'Gate A run did not complete before the deadline\n' >&2
  exit 1
fi

capture_postgres_snapshot "$result_dir/postgres-after.json"
postgres_file "$script_dir/sql/gate-a-write-statements.sql" \
  -qAt >"$result_dir/postgres-write-statements.json"
capture_top_wal_statements "$result_dir/postgres-top-wal-statements.csv"

conversation_flag=
if [[ "$mode" == "conversation" ]]; then
  conversation_flag=--conversation
fi
python3 "$script_dir/report.py" gate-a \
  --before "$result_dir/postgres-before.json" \
  --after "$result_dir/postgres-after.json" \
  --statements "$result_dir/postgres-write-statements.json" \
  --output "$result_dir/gate-a-report.json" \
  ${conversation_flag:+"$conversation_flag"}

printf 'Gate A passed (%s): %s\n' "$mode" "$result_dir"
