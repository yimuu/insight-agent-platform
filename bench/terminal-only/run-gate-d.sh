#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command jq
require_command k6
require_nonempty BASE_URL "${BASE_URL:-}"

profile=${1:-qualification}
case "$profile" in
  qualification)
    conversation_count=100
    turns=100
    max_duration=30m
    ;;
  smoke)
    conversation_count=${GATE_D_CONVERSATIONS:-2}
    turns=${GATE_D_TURNS:-3}
    max_duration=${GATE_D_MAX_DURATION:-3m}
    ;;
  *)
    printf 'usage: run-gate-d.sh [qualification|smoke] [result-directory]\n' >&2
    exit 2
    ;;
esac

if [[ "$profile" == "qualification" ]]; then
  agent_id=conversation_demo
  stream_agent_id=terminal_stream_fixture
  summary_trigger_messages=30
  recent_context_messages=20
  summary_trigger_tokens=24000
  page_size=17
  replay_every=10
  content_repeat=1
  capacity_retry_timeout_seconds=60
  capacity_retry_interval_seconds=0.1
  capacity_retry_max_attempts=64
else
  agent_id=${AGENT_ID:-conversation_demo}
  stream_agent_id=${GATE_D_STREAM_AGENT_ID:-terminal_stream_fixture}
  summary_trigger_messages=${SUMMARY_TRIGGER_MESSAGES:-30}
  recent_context_messages=${RECENT_CONTEXT_MESSAGES:-20}
  summary_trigger_tokens=${SUMMARY_TRIGGER_TOKENS:-24000}
  page_size=${GATE_D_PAGE_SIZE:-2}
  replay_every=${GATE_D_REPLAY_EVERY:-10}
  content_repeat=${GATE_D_CONTENT_REPEAT:-1}
  capacity_retry_timeout_seconds=${GATE_D_CAPACITY_RETRY_TIMEOUT_SECONDS:-60}
  capacity_retry_interval_seconds=${GATE_D_CAPACITY_RETRY_INTERVAL_SECONDS:-0.1}
  capacity_retry_max_attempts=${GATE_D_CAPACITY_RETRY_MAX_ATTEMPTS:-64}
fi

batch_id=${GATE_D_BATCH_ID:-"$(date -u +%Y%m%dT%H%M%S)-${RANDOM}"}
[[ "$batch_id" =~ ^[A-Za-z0-9-]+$ ]] || {
  printf 'GATE_D_BATCH_ID may contain only letters, digits, and hyphens\n' >&2
  exit 2
}
tenant_id="gate-d-$batch_id"
result_dir=${2:-"$terminal_bench_root/bench/results/terminal-only-gate-d-$profile-$batch_id"}
mkdir -p "$result_dir"
assert_postgres_durability
statistics_reset_evidence=null
if [[ "$profile" == "qualification" ]]; then
  database_stats_reset_before=$(postgres_command -qAt -c "
    SELECT COALESCE(to_jsonb(stats_reset) #>> '{}', '')
    FROM pg_stat_database
    WHERE datname=current_database();
  ")
  postgres_command -qAt -c 'SELECT pg_stat_reset();' >/dev/null
  database_stats_reset_after=$(postgres_command -qAt -c "
    SELECT COALESCE(to_jsonb(stats_reset) #>> '{}', '')
    FROM pg_stat_database
    WHERE datname=current_database();
  ")
  jq -n \
    --arg before "$database_stats_reset_before" \
    --arg after "$database_stats_reset_after" \
    '{
      operation: "pg_stat_reset",
      database_stats_reset_before:
        (if $before == "" then null else $before end),
      database_stats_reset_after:
        (if $after == "" then null else $after end),
      passed: ($after != "" and $after != $before)
    }' >"$result_dir/statistics-reset-before-gate-d.json"
  jq -e '.passed == true' \
    "$result_dir/statistics-reset-before-gate-d.json" >/dev/null || {
    printf 'Gate D database statistics reset did not establish a fresh epoch\n' >&2
    exit 1
  }
  statistics_reset_evidence=$(
    jq -c . "$result_dir/statistics-reset-before-gate-d.json"
  )
fi
capture_postgres_snapshot "$result_dir/postgres-before.json"

BASE_URL="$BASE_URL" \
AGENT_ID="$agent_id" \
BATCH_ID="$batch_id" \
TENANT_ID="$tenant_id" \
CONVERSATIONS="$conversation_count" \
TURNS="$turns" \
PAGE_SIZE="$page_size" \
REPLAY_EVERY="$replay_every" \
CONTENT_REPEAT="$content_repeat" \
CAPACITY_RETRY_TIMEOUT_SECONDS="$capacity_retry_timeout_seconds" \
CAPACITY_RETRY_INTERVAL_SECONDS="$capacity_retry_interval_seconds" \
CAPACITY_RETRY_MAX_ATTEMPTS="$capacity_retry_max_attempts" \
MAX_DURATION="$max_duration" \
SUMMARY_PATH="$result_dir/k6-summary.json" \
  k6 run "$script_dir/k6/conversations.js" >"$result_dir/k6.log"

# Summary jobs are asynchronous and must not block turns. This bounded wait is
# evidence collection only; the workload has already closed successfully.
if [[ "$profile" == "qualification" ]]; then
  sleep "${GATE_D_SUMMARY_SETTLE_SECONDS:-30}"
fi
postgres_file "$script_dir/sql/gate-d-assertions.sql" \
  -qAt -v "tenant_id=$tenant_id" >"$result_dir/database-assertions.json"
capture_postgres_snapshot "$result_dir/postgres-after.json"

expected_messages=$((conversation_count * turns * 2))
expected_turns=$((conversation_count * turns))
expected_replays=$((conversation_count * ((turns + replay_every - 1) / replay_every)))
expected_pages=$((conversation_count * ((turns * 2 + page_size - 1) / page_size)))
jq -e \
  --argjson conversations "$conversation_count" \
  --argjson turns "$expected_turns" \
  --argjson messages "$expected_messages" '
    .conversations == $conversations and
    .admissions == $turns and
    .results == $turns and
    .succeeded_results == $turns and
    .messages == $messages and
    .user_messages == $turns and
    .distinct_admission_user_messages == $turns and
    .user_without_admission == 0 and
    .assistant_messages == $turns and
    .assistant_without_result == 0 and
    .result_without_assistant == 0 and
    .admission_without_user == 0 and
    .turn_order_violations == 0 and
    .missing_context_hash_after_first_turn == 0
  ' "$result_dir/database-assertions.json" >/dev/null

jq -e \
  --argjson conversations "$conversation_count" \
  --argjson turns "$expected_turns" \
  --argjson replays "$expected_replays" \
  --argjson pages "$expected_pages" '
    (.metrics.conversation_turn_capacity_rejected.values.count // 0) as
      $capacity_rejections |
    (.metrics.conversation_created.values.count // 0) == $conversations and
    (.metrics.conversation_turn_attempts.values.count // 0) ==
      ($turns + $capacity_rejections) and
    (.metrics.conversation_turn_accepted.values.count // 0) == $turns and
    (.metrics.conversation_turn_fresh_acceptance.values.count // 0) ==
      $turns and
    (.metrics.conversation_turn_succeeded.values.count // 0) == $turns and
    (.metrics.conversation_replay_verified.values.count // 0) == $replays and
    (.metrics.conversation_pages_read.values.count // 0) == $pages and
    (.metrics.conversation_pagination_verified.values.count // 0) == $conversations and
    (.metrics.http_req_failed.values.passes // 0) == $capacity_rejections and
    (.metrics.checks.values.rate // 0) == 1
  ' "$result_dir/k6-summary.json" >/dev/null

if [[ "$profile" == "qualification" ]]; then
  jq -e \
    --argjson conversations "$conversation_count" '
      .conversations_with_summary == $conversations
    ' "$result_dir/database-assertions.json" >/dev/null

  BASE_URL="$BASE_URL" \
  CONTEXT_BATCH_ID="$batch_id" \
  SUMMARY_TRIGGER_MESSAGES="$summary_trigger_messages" \
  RECENT_CONTEXT_MESSAGES="$recent_context_messages" \
  SUMMARY_TRIGGER_TOKENS="$summary_trigger_tokens" \
  BENCH_NAMESPACE="${BENCH_NAMESPACE:-insight-bench}" \
  BENCH_RUNTIME_SELECTOR="${BENCH_RUNTIME_SELECTOR:-app.kubernetes.io/component=runtime}" \
  BENCH_ARTIFACT_ROOT="${BENCH_ARTIFACT_ROOT:-/data/artifacts}" \
    "$script_dir/run-context-summary.sh" "$result_dir/context-summary"

  BASE_URL="$BASE_URL" \
  GATE_D_STREAM_AGENT_ID="$stream_agent_id" \
    "$script_dir/run-stream-scaling.sh" "$result_dir/stream-scaling"

  BASE_URL="$BASE_URL" \
  AGENT_ID="$agent_id" \
  PRIVACY_STREAM_AGENT_ID="$stream_agent_id" \
  PRIVACY_BATCH_ID="$batch_id" \
  BENCH_NAMESPACE="${BENCH_NAMESPACE:-insight-bench}" \
  BENCH_RUNTIME_SELECTOR="${BENCH_RUNTIME_SELECTOR:-app.kubernetes.io/component=runtime}" \
  BENCH_ARTIFACT_ROOT="${BENCH_ARTIFACT_ROOT:-/data/artifacts}" \
    "$script_dir/run-privacy-delete.sh" "$result_dir/privacy-delete"

  AGED_BATCH_ID="$batch_id" \
    "$script_dir/run-aged-query.sh" qualification "$result_dir/aged-query"

  jq -s -e 'all(.[]; .passed == true)' \
    "$result_dir/context-summary/context-summary-report.json" \
    "$result_dir/stream-scaling/stream-scaling-report.json" \
    "$result_dir/privacy-delete/privacy-report.json" \
    "$result_dir/aged-query/aged-query-report.json" >/dev/null
fi

assert_postgres_durability
if [[ "$profile" == "qualification" ]]; then
  composite=true
else
  composite=false
fi
jq -n \
  --arg profile "$profile" \
  --arg tenant_id "$tenant_id" \
  --argjson conversations "$conversation_count" \
  --argjson turns_per_conversation "$turns" \
  --argjson composite "$composite" \
  --argjson statistics_reset "$statistics_reset_evidence" \
  --slurpfile k6_summary "$result_dir/k6-summary.json" \
  '{
    passed: true,
    profile: $profile,
    qualification_composite: $composite,
    tenant_id: $tenant_id,
    conversations: $conversations,
    turns_per_conversation: $turns_per_conversation,
    statistics_reset: $statistics_reset,
    capacity_retries: {
      attempts:
        ($k6_summary[0].metrics.conversation_turn_attempts.values.count // 0),
      rejected:
        ($k6_summary[0].metrics.conversation_turn_capacity_rejected.values.count // 0),
      accepted:
        ($k6_summary[0].metrics.conversation_turn_accepted.values.count // 0),
      fresh_non_replayed_acceptances:
        ($k6_summary[0].metrics.conversation_turn_fresh_acceptance.values.count // 0),
      all_http_non_successes_were_capacity_rejections: (
        ($k6_summary[0].metrics.http_req_failed.values.passes // 0) ==
        ($k6_summary[0].metrics.conversation_turn_capacity_rejected.values.count // 0)
      ),
      harness_invariant_same_request_and_payload_retry: true,
      strict_positive_integer_retry_after_required: true,
      rejection_run_and_message_identity_headers_forbidden: true
    },
    checks: {
      atomic_turns_and_ordering: true,
      idempotent_replay_and_cursor_paging: true,
      summary_and_context_bounds: $composite,
      summary_failure_fallback: $composite,
      stream_one_x_vs_ten_x: $composite,
      stream_terminal_get_messages_content_calibrated: $composite,
      aged_one_million_query: $composite,
      large_object_privacy_delete: $composite,
      tenant_artifact_encryption_and_delete: $composite
    }
  }' >"$result_dir/gate-d-report.json"
printf 'Gate D conversation evidence: %s\n' "$result_dir"
