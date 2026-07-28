#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command jq
require_command python3
require_nonempty BASE_URL "${BASE_URL:-}"

batch_id=${CONTEXT_BATCH_ID:-"$(date -u +%Y%m%dT%H%M%S)-${RANDOM}"}
[[ "$batch_id" =~ ^[A-Za-z0-9-]+$ ]] || {
  printf 'CONTEXT_BATCH_ID may contain only letters, digits, and hyphens\n' >&2
  exit 2
}
trigger_messages=${SUMMARY_TRIGGER_MESSAGES:-30}
recent_limit=${RECENT_CONTEXT_MESSAGES:-20}
token_budget=${SUMMARY_TRIGGER_TOKENS:-24000}
active_key_version=${TENANT_ARTIFACT_KEY_VERSION:-qualification-v1}
[[ "$trigger_messages" =~ ^[1-9][0-9]*$ &&
  "$recent_limit" =~ ^[1-9][0-9]*$ &&
  "$token_budget" =~ ^[1-9][0-9]*$ ]] || {
  printf 'summary trigger, recent limit, and token budget must be positive integers\n' >&2
  exit 2
}
((recent_limit <= trigger_messages)) || {
  printf 'RECENT_CONTEXT_MESSAGES must not exceed SUMMARY_TRIGGER_MESSAGES\n' >&2
  exit 2
}
[[ "$active_key_version" =~ ^[A-Za-z0-9._-]{1,64}$ ]] || {
  printf 'TENANT_ARTIFACT_KEY_VERSION is invalid\n' >&2
  exit 2
}

tenant_id="context-tenant-$batch_id"
user_id="context-user-$batch_id"
result_dir=${1:-"$terminal_bench_root/bench/results/terminal-only-context-$batch_id"}
mkdir -p "$result_dir"

api_curl -X POST "$BASE_URL/v1/conversations" \
  -H 'content-type: application/json' \
  -H "x-request-id: context-create-$batch_id" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id" \
  --data-binary '{"agent_id":"conversation_context_probe"}' \
  >"$result_dir/create.json"
conversation_id=$(jq -er '.data.conversation_id' "$result_dir/create.json")
[[ "$conversation_id" =~ ^[A-Za-z0-9_-]+$ ]] || {
  printf 'context fixture returned an unsafe Conversation identity\n' >&2
  exit 1
}

last_run_id=
last_user_message_order=
append_and_wait() {
  local turn=$1
  local output=$2
  api_curl -X POST "$BASE_URL/v1/conversations/$conversation_id/messages" \
    -H 'content-type: application/json' \
    -H "x-request-id: context-turn-$batch_id-$turn" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $user_id" \
    --data-binary "{\"content\":{\"turn\":$turn,\"text\":\"bounded context fixture\"}}" \
    >"$output"
  last_run_id=$(jq -er '.data.run.run_id' "$output")
  last_user_message_order=$(jq -er '.data.user_message.message_order' "$output")
  local deadline=$((SECONDS + ${RUN_TIMEOUT_SECONDS:-30}))
  while ((SECONDS < deadline)); do
    api_curl "$BASE_URL/v1/conversations/$conversation_id/messages?limit=2" \
      -H "x-tenant-id: $tenant_id" \
      -H "x-user-id: $user_id" \
      >"$result_dir/latest-messages.json"
    if jq -e --arg run_id "$last_run_id" '
      any(.data.messages[];
          .role == "assistant" and .run_id == $run_id)
    ' "$result_dir/latest-messages.json" >/dev/null; then
      return
    fi
    sleep 0.05
  done
  printf 'context fixture turn %s did not complete\n' "$turn" >&2
  exit 1
}

summary_count() {
  postgres_command -qAt -c "
    SELECT COUNT(*)
    FROM conversation_summaries
    WHERE conversation_id='$conversation_id';
  "
}

wait_for_summary_count() {
  local expected=$1
  local deadline=$((SECONDS + ${SUMMARY_SETTLE_TIMEOUT_SECONDS:-30}))
  local observed
  while ((SECONDS < deadline)); do
    observed=$(summary_count)
    if [[ "$observed" =~ ^[0-9]+$ ]] && ((observed >= expected)); then
      return
    fi
    sleep 0.1
  done
  printf 'context fixture produced %s summaries, expected at least %s\n' \
    "$(summary_count)" "$expected" >&2
  exit 1
}

summary_state() {
  postgres_command -qAt -c "
    SELECT json_build_object(
      'summary_count', (
        SELECT COUNT(*)
        FROM conversation_summaries
        WHERE conversation_id='$conversation_id'
      ),
      'latest_boundary', COALESCE((
        SELECT through_message_order
        FROM conversation_summaries
        WHERE conversation_id='$conversation_id'
        ORDER BY through_message_order DESC
        LIMIT 1
      ), 0),
      'latest_hash', COALESCE((
        SELECT summary_hash
        FROM conversation_summaries
        WHERE conversation_id='$conversation_id'
        ORDER BY through_message_order DESC
        LIMIT 1
      ), '')
    )::text;
  "
}

wait_for_summary_idle_stable() {
  local label=$1
  local destination=$2
  local deadline=$((SECONDS + ${SUMMARY_SETTLE_TIMEOUT_SECONDS:-30}))
  local required_stable_observations=${SUMMARY_STABLE_OBSERVATIONS:-10}
  local metrics_file="$result_dir/.summary-settle-metrics.txt"
  local active_runs=
  local active_summary_jobs=
  local state=
  local previous_state=
  local stable_observations=0
  [[ "$required_stable_observations" =~ ^[1-9][0-9]*$ ]] || {
    printf 'SUMMARY_STABLE_OBSERVATIONS must be a positive integer\n' >&2
    exit 2
  }
  while ((SECONDS < deadline)); do
    api_curl "$BASE_URL/metrics" >"$metrics_file"
    active_runs=$(awk '
      $1 == "terminal_run_active" {print $2}
    ' "$metrics_file" | tail -n 1)
    active_summary_jobs=$(awk '
      $1 == "conversation_summary_jobs_active{persistence_mode=\"terminal_only\"}" {print $2}
    ' "$metrics_file" | tail -n 1)
    state=$(summary_state)
    if [[ "$active_runs" == "0" &&
      "$active_summary_jobs" == "0" &&
      "$state" == "$previous_state" ]]; then
      stable_observations=$((stable_observations + 1))
    else
      stable_observations=0
    fi
    previous_state=$state
    if ((stable_observations >= required_stable_observations)); then
      jq -n \
        --arg label "$label" \
        --argjson active_runs "$active_runs" \
        --argjson active_summary_jobs "$active_summary_jobs" \
        --argjson state "$state" \
        --argjson stable_observations "$stable_observations" \
        '{
          label: $label,
          terminal_run_active: $active_runs,
          summary_jobs_active: $active_summary_jobs,
          stable_observations: $stable_observations,
          state: $state,
          passed: (
            $active_runs == 0 and
            $active_summary_jobs == 0 and
            $stable_observations > 0 and
            $state.summary_count >= 1
          )
        }' >"$destination"
      rm -f "$metrics_file"
      jq -e '.passed == true' "$destination" >/dev/null
      return
    fi
    sleep 0.1
  done
  rm -f "$metrics_file"
  printf 'summary worker did not become stably idle: label=%s runs=%s jobs=%s state=%s\n' \
    "$label" "${active_runs:-missing}" "${active_summary_jobs:-missing}" \
    "${state:-missing}" >&2
  exit 1
}

capture_latest_summary_row() {
  local destination=$1
  postgres_command -qAt -c "
    SELECT json_build_object(
      'through_message_order', through_message_order,
      'summary_hash', summary_hash,
      'summary_ref', summary_ref::json,
      'model_revision', model_revision,
      'created_at', created_at
    )::text
    FROM conversation_summaries
    WHERE conversation_id='$conversation_id'
    ORDER BY through_message_order DESC
    LIMIT 1;
  " >"$destination"
  jq -e '
    .through_message_order >= 1 and
    .summary_hash == .summary_ref.content_hash and
    (.summary_ref.artifact_id | type == "string")
  ' "$destination" >/dev/null
}

initial_turns=$((trigger_messages / 2 + 1))
for ((turn = 0; turn < initial_turns; turn += 1)); do
  append_and_wait "$turn" "$result_dir/seed-turn-$turn.json"
done
wait_for_summary_count 1

first_boundary=$(postgres_command -qAt -c "
  SELECT through_message_order
  FROM conversation_summaries
  WHERE conversation_id='$conversation_id'
  ORDER BY through_message_order DESC
  LIMIT 1;
")
[[ "$first_boundary" =~ ^[1-9][0-9]*$ ]] || {
  printf 'first summary boundary is invalid: %s\n' "$first_boundary" >&2
  exit 1
}
messages_after_first_boundary=$(postgres_command -qAt -c "
  SELECT COUNT(*)
  FROM conversation_messages
  WHERE conversation_id='$conversation_id'
    AND message_order > $first_boundary;
")
[[ "$messages_after_first_boundary" =~ ^[0-9]+$ ]] || {
  printf 'message count after first summary boundary is invalid\n' >&2
  exit 1
}
additional_messages=$((trigger_messages - messages_after_first_boundary))
((additional_messages > 0)) || additional_messages=2
additional_turns=$(((additional_messages + 1) / 2))
((additional_turns > 0)) || additional_turns=1
turn=$initial_turns
for ((offset = 0; offset < additional_turns; offset += 1)); do
  append_and_wait "$turn" "$result_dir/second-generation-turn-$turn.json"
  turn=$((turn + 1))
done
wait_for_summary_count 2
wait_for_summary_idle_stable \
  "before-probe" "$result_dir/summary-settle-before-probe.json"

postgres_command -qAt -c "
  SELECT COALESCE(
    json_agg(
      json_build_object(
        'through_message_order', through_message_order,
        'summary_hash', summary_hash,
        'summary_ref', summary_ref::json,
        'model_revision', model_revision,
        'created_at', created_at
      )
      ORDER BY through_message_order
    ),
    '[]'::json
  )::text
  FROM (
    SELECT through_message_order,summary_hash,summary_ref,model_revision,created_at
    FROM conversation_summaries
    WHERE conversation_id='$conversation_id'
    ORDER BY through_message_order DESC
    LIMIT 2
  ) generations;
" >"$result_dir/summary-generations.json"

jq -e '
  length == 2 and
  .[0].through_message_order < .[1].through_message_order and
  .[0].summary_hash == .[0].summary_ref.content_hash and
  .[1].summary_hash == .[1].summary_ref.content_hash and
  .[0].summary_hash != .[1].summary_hash and
  .[0].summary_ref.artifact_id != .[1].summary_ref.artifact_id
' "$result_dir/summary-generations.json" >/dev/null
latest_boundary=$(jq -er '.[1].through_message_order' "$result_dir/summary-generations.json")
latest_summary_hash=$(jq -er '.[1].summary_hash' "$result_dir/summary-generations.json")
latest_summary_ref=$(jq -cer '.[1].summary_ref' "$result_dir/summary-generations.json")

artifact_hash_hex() {
  jq -er '.content_hash | sub("^sha256:"; "")' <<<"$1"
}

artifact_host_root() {
  if [[ -n "${ARTIFACT_ROOT:-}" ]]; then
    printf '%s\n' "$ARTIFACT_ROOT"
  elif [[ -n "${BENCH_ARTIFACT_HOST_ROOT:-}" ]]; then
    printf '%s\n' "$BENCH_ARTIFACT_HOST_ROOT"
  fi
}

capture_artifact_object() {
  local reference=$1
  local destination=$2
  local hash
  hash=$(artifact_hash_hex "$reference")
  local host_root
  host_root=$(artifact_host_root)
  if [[ -n "$host_root" ]]; then
    local object_path="$host_root/${hash:0:2}/$hash"
    [[ -f "$object_path" ]] || {
      printf 'summary object is missing at %s\n' "$object_path" >&2
      exit 1
    }
    cp "$object_path" "$destination"
    return
  fi
  require_command kubectl
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local pod
  pod=$(runtime_pod_name)
  kubectl -n "$namespace" exec "$pod" -- \
    cat "${BENCH_ARTIFACT_ROOT:-/data/artifacts}/${hash:0:2}/$hash" \
    >"$destination"
}

remove_artifact_object() {
  local reference=$1
  local hash
  hash=$(artifact_hash_hex "$reference")
  local host_root
  host_root=$(artifact_host_root)
  if [[ -n "$host_root" ]]; then
    rm -f "$host_root/${hash:0:2}/$hash"
    return
  fi
  require_command kubectl
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local pod
  pod=$(runtime_pod_name)
  kubectl -n "$namespace" exec "$pod" -- \
    rm -f "${BENCH_ARTIFACT_ROOT:-/data/artifacts}/${hash:0:2}/$hash"
}

assert_artifact_object_missing() {
  local reference=$1
  local hash
  hash=$(artifact_hash_hex "$reference")
  local host_root
  host_root=$(artifact_host_root)
  if [[ -n "$host_root" ]]; then
    [[ ! -f "$host_root/${hash:0:2}/$hash" ]]
    return
  fi
  require_command kubectl
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local pod
  pod=$(runtime_pod_name)
  kubectl -n "$namespace" exec "$pod" -- \
    test ! -f "${BENCH_ARTIFACT_ROOT:-/data/artifacts}/${hash:0:2}/$hash"
}

latest_summary_raw="$result_dir/latest-summary-object.bin"
latest_summary_envelope="$result_dir/latest-summary-envelope.json"
capture_artifact_object "$latest_summary_ref" "$latest_summary_raw"
summary_envelope_status=0
python3 "$script_dir/encrypted_artifact_probe.py" \
  --input "$latest_summary_raw" \
  --tenant-id "$tenant_id" \
  --marker conversation_summary \
  --expected-key-version "$active_key_version" \
  --output "$latest_summary_envelope" || summary_envelope_status=$?
rm -f "$latest_summary_raw"
((summary_envelope_status == 0)) || {
  printf 'latest summary object is not a complete encrypted envelope\n' >&2
  exit "$summary_envelope_status"
}
jq -e --arg active_key_version "$active_key_version" '
  .passed == true and
  .magic == "IAPTEA01" and
  .active_key_version == $active_key_version and
  .framing_complete == true and
  .tenant_id_plaintext_absent == true and
  .marker_plaintext_absent == true
' "$latest_summary_envelope" >/dev/null

capture_expected_recent() {
  local boundary=$1
  local before_order=$2
  local destination=$3
  postgres_command -qAt -c "
    SELECT COALESCE(
      json_agg(
        json_build_object(
          'message_order', message_order,
          'role', role,
          'content_hash', content_hash
        )
        ORDER BY message_order
      ),
      '[]'::json
    )::text
    FROM (
      SELECT message_order,role,content_hash
      FROM conversation_messages
      WHERE conversation_id='$conversation_id'
        AND message_order > $boundary
        AND message_order < $before_order
      ORDER BY message_order DESC
      LIMIT $recent_limit
    ) recent;
  " >"$destination"
}

capture_bounded_expected_recent() {
  local boundary=$1
  local before_order=$2
  local context_file=$3
  local page_file=$4
  local destination=$5
  api_curl \
    "$BASE_URL/v1/conversations/$conversation_id/messages?limit=200" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $user_id" \
    >"$page_file"
  python3 "$script_dir/context_window_oracle.py" \
    --messages "$page_file" \
    --context "$context_file" \
    --boundary "$boundary" \
    --before-order "$before_order" \
    --recent-limit "$recent_limit" \
    --token-budget "$token_budget" \
    --output "$destination"
}

capture_probe_context() {
  local run_id=$1
  local destination=$2
  jq -e --arg run_id "$run_id" '
    [.data.messages[]
      | select(.role == "assistant" and .run_id == $run_id)
      | .content][0]
  ' "$result_dir/latest-messages.json" >"$destination"
}

assert_exact_recent_window() {
  local context_file=$1
  local expected_file=$2
  local boundary=$3
  jq -e \
    --argjson boundary "$boundary" \
    --argjson recent_limit "$recent_limit" \
    --slurpfile expected "$expected_file" '
      [.messages[] | {
        message_order: .message_order,
        role: .role,
        content_hash: .content_hash
      }] as $actual |
      $expected[0].messages as $candidates |
      ($actual | length) <= $recent_limit and
      all($actual[]; .message_order > $boundary) and
      $actual == $candidates
    ' "$context_file" >/dev/null
}

probe_turn=$turn
append_and_wait "$probe_turn" "$result_dir/probe-turn.json"
probe_run_id=$last_run_id
probe_user_order=$last_user_message_order
capture_probe_context "$probe_run_id" "$result_dir/context-with-summary.json"
capture_expected_recent \
  "$latest_boundary" "$probe_user_order" "$result_dir/expected-recent-with-summary.json"
capture_bounded_expected_recent \
  "$latest_boundary" \
  "$probe_user_order" \
  "$result_dir/context-with-summary.json" \
  "$result_dir/recent-candidate-page-with-summary.json" \
  "$result_dir/expected-bounded-recent-with-summary.json"
# `conversation_context_probe` is an ordinary qualification Agent invoked
# through the authenticated Conversation route. Its input is assembled only
# after ArtifactStore decrypts the summary and verifies the plaintext
# size/content hash against `summary_ref`; no key or raw plaintext endpoint is
# exposed by this harness.
jq -e '.summary | select(. != null)' "$result_dir/context-with-summary.json" \
  >"$result_dir/latest-summary-value.json"
jq -e --argjson boundary "$latest_boundary" '
  .kind == "conversation_summary" and
  .version == 2 and
  .through_message_order == $boundary
' "$result_dir/latest-summary-value.json" >/dev/null
assert_exact_recent_window \
  "$result_dir/context-with-summary.json" \
  "$result_dir/expected-bounded-recent-with-summary.json" \
  "$latest_boundary"

wait_for_summary_idle_stable \
  "before-missing-object-injection" \
  "$result_dir/summary-settle-before-missing-object.json"
capture_latest_summary_row "$result_dir/missing-object-target.json"
fault_summary_boundary=$(jq -er '.through_message_order' \
  "$result_dir/missing-object-target.json")
fault_summary_hash=$(jq -er '.summary_hash' \
  "$result_dir/missing-object-target.json")
fault_summary_ref=$(jq -cer '.summary_ref' \
  "$result_dir/missing-object-target.json")
fault_summary_raw="$result_dir/missing-object-target.bin"
capture_artifact_object "$fault_summary_ref" "$fault_summary_raw"
rm -f "$fault_summary_raw"
api_curl "$BASE_URL/metrics" >"$result_dir/metrics-before-missing-object.txt"
remove_artifact_object "$fault_summary_ref"
assert_artifact_object_missing "$fault_summary_ref"
capture_latest_summary_row \
  "$result_dir/missing-object-latest-row-after-delete.json"
jq -s -e 'length == 2 and .[0] == .[1]' \
  "$result_dir/missing-object-target.json" \
  "$result_dir/missing-object-latest-row-after-delete.json" >/dev/null
fault_turn=$((probe_turn + 1))
append_and_wait "$fault_turn" "$result_dir/missing-object-turn.json"
fault_run_id=$last_run_id
fault_user_order=$last_user_message_order
capture_probe_context "$fault_run_id" "$result_dir/context-after-missing-object.json"
capture_expected_recent \
  "0" "$fault_user_order" "$result_dir/expected-fallback-tail.json"
capture_bounded_expected_recent \
  "0" \
  "$fault_user_order" \
  "$result_dir/context-after-missing-object.json" \
  "$result_dir/recent-candidate-page-after-missing-object.json" \
  "$result_dir/expected-bounded-fallback-tail.json"
wait_for_summary_idle_stable \
  "after-missing-object-fallback" \
  "$result_dir/summary-settle-after-missing-object.json"
api_curl "$BASE_URL/metrics" >"$result_dir/metrics-after-missing-object.txt"

jq -e '.summary == null' "$result_dir/context-after-missing-object.json" >/dev/null
assert_exact_recent_window \
  "$result_dir/context-after-missing-object.json" \
  "$result_dir/expected-bounded-fallback-tail.json" \
  "0"

context_messages=$(awk '
  $1 == "conversation_context_messages{persistence_mode=\"terminal_only\"}" {print $2}
' \
  "$result_dir/metrics-after-missing-object.txt" | tail -n 1)
context_tokens=$(awk '
  $1 == "conversation_context_tokens{persistence_mode=\"terminal_only\"}" {print $2}
' \
  "$result_dir/metrics-after-missing-object.txt" | tail -n 1)
summary_failures_before=$(awk '
  $1 == "conversation_summary_jobs_total{result=\"failed\",persistence_mode=\"terminal_only\"}" {print $2}
' "$result_dir/metrics-before-missing-object.txt" | tail -n 1)
summary_failures_after=$(awk '
  $1 == "conversation_summary_jobs_total{result=\"failed\",persistence_mode=\"terminal_only\"}" {print $2}
' "$result_dir/metrics-after-missing-object.txt" | tail -n 1)
[[ "$context_messages" =~ ^[0-9]+$ && "$context_tokens" =~ ^[0-9]+$ ]] || {
  printf 'context metrics were absent or non-integral\n' >&2
  exit 1
}
((context_messages <= recent_limit && context_tokens <= token_budget)) || {
  printf 'context bound exceeded: messages=%s tokens=%s\n' \
    "$context_messages" "$context_tokens" >&2
  exit 1
}
awk -v before="${summary_failures_before:-0}" \
  -v after="${summary_failures_after:-0}" \
  'BEGIN { exit !(after > before) }' || {
  printf 'missing summary object did not increment the summary read-failure metric\n' >&2
  exit 1
}

actual_summary_orders=$(jq -c \
  '[.messages[].message_order]' "$result_dir/context-with-summary.json")
fallback_orders=$(jq -c \
  '[.messages[].message_order]' "$result_dir/context-after-missing-object.json")
jq -n \
  --arg conversation_id "$conversation_id" \
  --slurpfile encryption "$latest_summary_envelope" \
  --slurpfile generations "$result_dir/summary-generations.json" \
  --slurpfile fault_target "$result_dir/missing-object-target.json" \
  --slurpfile fault_target_after_delete \
    "$result_dir/missing-object-latest-row-after-delete.json" \
  --arg latest_summary_hash "$latest_summary_hash" \
  --arg fault_summary_hash "$fault_summary_hash" \
  --argjson latest_boundary "$latest_boundary" \
  --argjson fault_summary_boundary "$fault_summary_boundary" \
  --argjson summary_context_orders "$actual_summary_orders" \
  --argjson fallback_context_orders "$fallback_orders" \
  --argjson messages "$context_messages" \
  --argjson tokens "$context_tokens" \
  --argjson recent_limit "$recent_limit" \
  --argjson token_budget "$token_budget" \
  '{
    passed: true,
    conversation_id: $conversation_id,
    summary_generations: $generations[0],
    latest_summary: {
      through_message_order: $latest_boundary,
      hash: $latest_summary_hash,
      ref_hash_matches_database: true,
      raw_encrypted_envelope: $encryption[0],
      semantic_read_path: "authenticated_conversation_context_probe",
      semantic_plaintext_hash_verified_by_artifact_store: true,
      probe_selected_latest_database_boundary: true
    },
    recent_window: {
      exact_database_window: true,
      strictly_after_latest_boundary: true,
      message_orders: $summary_context_orders
    },
    missing_object_fallback: {
      injection: "delete_latest_summary_object",
      deleted_summary: {
        through_message_order: $fault_summary_boundary,
        hash: $fault_summary_hash,
        exact_latest_database_row_at_injection: $fault_target[0],
        latest_database_row_unchanged_after_delete:
          ($fault_target_after_delete[0] == $fault_target[0]),
        object_absent_before_fault_turn: true
      },
      worker_crash: false,
      summary_was_null: true,
      reloaded_from_conversation_start: true,
      exact_database_tail_window: true,
      message_orders: $fallback_context_orders,
      read_failure_metric_incremented: true
    },
    context_messages: $messages,
    context_message_limit: $recent_limit,
    context_tokens: $tokens,
    context_token_budget: $token_budget
  }' >"$result_dir/context-summary-report.json"
printf 'Context/summary evidence: %s\n' "$result_dir"
