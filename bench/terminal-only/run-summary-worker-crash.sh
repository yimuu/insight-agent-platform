#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command jq
require_command helm
require_command kubectl
require_command python3
require_nonempty BASE_URL "${BASE_URL:-}"

batch_id=${SUMMARY_CRASH_BATCH_ID:-"$(date -u +%Y%m%dT%H%M%S)-${RANDOM}"}
[[ "$batch_id" =~ ^[A-Za-z0-9-]+$ ]] || {
  printf 'SUMMARY_CRASH_BATCH_ID may contain only letters, digits, and hyphens\n' >&2
  exit 2
}
delay_ms=${SUMMARY_WORKER_CRASH_DELAY_MS:-30000}
trigger_messages=${SUMMARY_TRIGGER_MESSAGES:-30}
recent_limit=${RECENT_CONTEXT_MESSAGES:-20}
token_budget=${SUMMARY_TRIGGER_TOKENS:-24000}
max_recovery_seconds=${SUMMARY_RECOVERY_MAX_SECONDS:-15}
[[ "$delay_ms" =~ ^[1-9][0-9]*$ ]] &&
  ((delay_ms <= 30000)) || {
  printf 'SUMMARY_WORKER_CRASH_DELAY_MS must be between 1 and 30000\n' >&2
  exit 2
}
[[ "$trigger_messages" =~ ^[1-9][0-9]*$ &&
  "$recent_limit" =~ ^[1-9][0-9]*$ &&
  "$token_budget" =~ ^[1-9][0-9]*$ &&
  "$max_recovery_seconds" =~ ^[1-9][0-9]*$ ]] || {
  printf 'summary crash numeric settings must be positive integers\n' >&2
  exit 2
}
((recent_limit <= trigger_messages)) || {
  printf 'RECENT_CONTEXT_MESSAGES must not exceed SUMMARY_TRIGGER_MESSAGES\n' >&2
  exit 2
}

result_dir=${1:-"$terminal_bench_root/bench/results/terminal-only-summary-crash-$batch_id"}
mkdir -p "$result_dir"
namespace=${BENCH_NAMESPACE:-insight-bench}
release=${BENCH_RELEASE:-bench}
chart=${BENCH_HELM_CHART:-"$terminal_bench_root/deploy/archive/helm/insight-agent-platform"}
deployment="${release}-insight-agent-platform"
tenant_id="summary-crash-tenant-$batch_id"
user_id="summary-crash-user-$batch_id"
faults_reset=0

set_summary_delay() {
  local milliseconds=$1
  local label=$2
  local helm_status=0
  local rollout_status=0
  helm upgrade "$release" "$chart" -n "$namespace" --reuse-values \
    --set qualification.enabled=true \
    --set runtime.qualificationFaults.admissionDelayMs=0 \
    --set runtime.qualificationFaults.postCommitDelayMs=0 \
    --set "runtime.qualificationFaults.summaryDelayMs=$milliseconds" \
    >"$result_dir/helm-$label.txt" \
    2>"$result_dir/helm-$label.stderr" ||
    helm_status=$?
  kubectl -n "$namespace" rollout status \
    "deployment/$deployment" --timeout=180s \
    >"$result_dir/rollout-$label.txt" \
    2>"$result_dir/rollout-$label.stderr" ||
    rollout_status=$?
  jq -n \
    --arg label "$label" \
    --argjson summary_delay_ms "$milliseconds" \
    --argjson helm_status "$helm_status" \
    --argjson rollout_status "$rollout_status" \
    '{
      label: $label,
      requested: {
        admission_delay_ms: 0,
        post_commit_delay_ms: 0,
        summary_delay_ms: $summary_delay_ms
      },
      helm_status: $helm_status,
      rollout_status: $rollout_status,
      applied: ($helm_status == 0 and $rollout_status == 0)
    }' >"$result_dir/fault-apply-$label.json" ||
    return $?
  ((helm_status == 0 && rollout_status == 0))
}

reset_summary_fault() {
  local label=$1
  local apply_status=0
  local verification_status=0
  set_summary_delay 0 "$label" || apply_status=$?
  qualification_assert_faults_zero \
    "$result_dir/fault-zero-$label.json" ||
    verification_status=$?
  jq -n \
    --arg label "$label" \
    --argjson apply_status "$apply_status" \
    --argjson verification_status "$verification_status" \
    '{
      label: $label,
      apply_status: $apply_status,
      verification_status: $verification_status,
      reset_confirmed:
        ($apply_status == 0 and $verification_status == 0)
    }' >"$result_dir/fault-reset-$label.json" ||
    return $?
  ((apply_status == 0 && verification_status == 0))
}

cleanup_summary_fault() {
  local original_status=$?
  local reset_status=0
  local final_status=$original_status
  trap - EXIT

  if ((faults_reset == 0)); then
    reset_summary_fault cleanup || reset_status=$?
  elif ! qualification_assert_faults_zero \
    "$result_dir/fault-zero-exit.json"; then
    reset_status=1
    reset_summary_fault cleanup-retry || reset_status=$?
  fi
  if ((original_status == 0 && reset_status != 0)); then
    final_status=1
  fi
  if ! jq -n \
    --argjson original_status "$original_status" \
    --argjson reset_status "$reset_status" \
    --argjson final_status "$final_status" \
    '{
      original_status: $original_status,
      reset_status: $reset_status,
      final_status: $final_status,
      reset_failure_forced_summary_failure:
        ($original_status == 0 and $reset_status != 0)
    }' >"$result_dir/summary-cleanup.json"; then
    ((final_status != 0)) || final_status=1
  fi
  exit "$final_status"
}
trap cleanup_summary_fault EXIT

set_summary_delay "$delay_ms" enable-summary-delay

api_curl -X POST "$BASE_URL/v1/conversations" \
  -H 'content-type: application/json' \
  -H "x-request-id: summary-crash-create-$batch_id" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id" \
  --data-binary '{"agent_id":"conversation_context_probe"}' \
  >"$result_dir/create.json"
conversation_id=$(jq -er '.data.conversation_id' "$result_dir/create.json")
[[ "$conversation_id" =~ ^[A-Za-z0-9_-]+$ ]] || {
  printf 'summary crash fixture returned an unsafe Conversation identity\n' >&2
  exit 1
}

last_run_id=
last_user_message_order=
append_and_wait() {
  local turn=$1
  local output=$2
  api_curl -X POST "$BASE_URL/v1/conversations/$conversation_id/messages" \
    -H 'content-type: application/json' \
    -H "x-request-id: summary-crash-turn-$batch_id-$turn" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $user_id" \
    --data-binary "{\"content\":{\"turn\":$turn,\"text\":\"summary worker crash fixture\"}}" \
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
  printf 'summary crash turn %s did not complete\n' "$turn" >&2
  exit 1
}

seed_turns=$((trigger_messages / 2 + 1))
for ((turn = 0; turn < seed_turns; turn += 1)); do
  append_and_wait "$turn" "$result_dir/seed-turn-$turn.json"
done

worker_deadline=$((SECONDS + ${SUMMARY_WORKER_WINDOW_TIMEOUT_SECONDS:-10}))
active_jobs=
eligible_messages=
summary_rows=
while ((SECONDS < worker_deadline)); do
  api_curl "$BASE_URL/metrics" >"$result_dir/metrics-worker-window.txt"
  active_jobs=$(awk '
    $1 == "conversation_summary_jobs_active{persistence_mode=\"terminal_only\"}" {print $2}
  ' \
    "$result_dir/metrics-worker-window.txt" | tail -n 1)
  read -r eligible_messages summary_rows < <(
    postgres_command -qAt -F ' ' -c "
      SELECT
        (SELECT COUNT(*) FROM conversation_messages
         WHERE conversation_id='$conversation_id'),
        (SELECT COUNT(*) FROM conversation_summaries
         WHERE conversation_id='$conversation_id');
    "
  )
  if [[ "$active_jobs" == "1" &&
    "$eligible_messages" =~ ^[0-9]+$ &&
    "$summary_rows" == "0" ]] &&
    ((eligible_messages >= trigger_messages)); then
    break
  fi
  sleep 0.1
done
[[ "$active_jobs" == "1" &&
  "$eligible_messages" =~ ^[0-9]+$ &&
  "$summary_rows" == "0" ]] &&
  ((eligible_messages >= trigger_messages)) || {
  printf 'did not observe an eligible delayed summary job: active=%s messages=%s summaries=%s\n' \
    "${active_jobs:-missing}" "${eligible_messages:-missing}" "${summary_rows:-missing}" >&2
  exit 1
}

postgres_command -qAt -c "
  SELECT json_build_object(
    'eligible_messages', (
      SELECT COUNT(*) FROM conversation_messages
      WHERE conversation_id='$conversation_id'
    ),
    'assistant_messages', (
      SELECT COUNT(*) FROM conversation_messages
      WHERE conversation_id='$conversation_id' AND role='assistant'
    ),
    'terminal_results', (
      SELECT COUNT(*)
      FROM terminal_run_results r
      JOIN terminal_run_admissions a ON a.run_id=r.run_id
      WHERE a.conversation_id='$conversation_id'
    ),
    'summary_rows', (
      SELECT COUNT(*) FROM conversation_summaries
      WHERE conversation_id='$conversation_id'
    )
  )::text;
" >"$result_dir/worker-window-database.json"
jq -e \
  --argjson seed_turns "$seed_turns" \
  --argjson trigger "$trigger_messages" '
    .eligible_messages >= $trigger and
    .assistant_messages == $seed_turns and
    .terminal_results == $seed_turns and
    .summary_rows == 0
  ' "$result_dir/worker-window-database.json" >/dev/null

old_pod=$(runtime_pod_name)
old_uid=$(kubectl -n "$namespace" get pod "$old_pod" -o jsonpath='{.metadata.uid}')
kubectl -n "$namespace" get pod "$old_pod" -o json \
  >"$result_dir/pod-before-hard-kill.json"
if ! qualification_trigger_container_death \
  runtime_self_abort \
  "$old_pod" \
  "$old_uid" \
  "${QUALIFICATION_RUNTIME_CONTAINER:-runtime}" \
  "$result_dir"; then
  printf 'summary worker crash did not confirm runtime process death\n' >&2
  exit 1
fi
kubectl -n "$namespace" delete pod "$old_pod" --wait=false \
  >"$result_dir/hard-kill.txt"

replacement_deadline=$((SECONDS + ${SUMMARY_RESTART_TIMEOUT_SECONDS:-180}))
replacement_pod=
replacement_uid=
while ((SECONDS < replacement_deadline)); do
  replacement_pod=$(runtime_pod_name 2>/dev/null || true)
  if [[ -n "$replacement_pod" ]]; then
    replacement_uid=$(kubectl -n "$namespace" get pod "$replacement_pod" \
      -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    ready=$(kubectl -n "$namespace" get pod "$replacement_pod" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' \
      2>/dev/null || true)
    if [[ -n "$replacement_uid" &&
      "$replacement_uid" != "$old_uid" &&
      "$ready" == "True" ]]; then
      break
    fi
  fi
  sleep 0.25
done
[[ -n "$replacement_uid" && "$replacement_uid" != "$old_uid" ]] || {
  printf 'runtime replacement Pod did not become ready after hard kill\n' >&2
  exit 1
}
kubectl -n "$namespace" get pod "$replacement_pod" -o json \
  >"$result_dir/pod-after-hard-kill.json"

summary_rows_after_restart=$(postgres_command -qAt -c "
  SELECT COUNT(*) FROM conversation_summaries
  WHERE conversation_id='$conversation_id';
")
[[ "$summary_rows_after_restart" == "0" ]] || {
  printf 'summary unexpectedly committed across the hard-kill window\n' >&2
  exit 1
}

reset_summary_fault clear-summary-delay
faults_reset=1
kubectl -n "$namespace" get pod "$(runtime_pod_name)" -o json \
  >"$result_dir/pod-after-delay-clear.json"

recovery_turn=$seed_turns
recovery_started=$SECONDS
append_and_wait "$recovery_turn" "$result_dir/recovery-turn.json"
recovery_elapsed_seconds=$((SECONDS - recovery_started))
recovery_run_id=$last_run_id
recovery_user_order=$last_user_message_order
((recovery_elapsed_seconds < max_recovery_seconds)) || {
  printf 'post-crash turn took %ss, expected less than %ss\n' \
    "$recovery_elapsed_seconds" "$max_recovery_seconds" >&2
  exit 1
}
jq -e --arg run_id "$recovery_run_id" '
  [.data.messages[]
    | select(.role == "assistant" and .run_id == $run_id)
    | .content][0]
' "$result_dir/latest-messages.json" >"$result_dir/recovery-context.json"
jq -e '.summary == null' "$result_dir/recovery-context.json" >/dev/null

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
      AND message_order < $recovery_user_order
    ORDER BY message_order DESC
    LIMIT $recent_limit
  ) recent;
" >"$result_dir/expected-recovery-tail.json"
api_curl \
  "$BASE_URL/v1/conversations/$conversation_id/messages?limit=200" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id" \
  >"$result_dir/recovery-candidate-page.json"
python3 "$script_dir/context_window_oracle.py" \
  --messages "$result_dir/recovery-candidate-page.json" \
  --context "$result_dir/recovery-context.json" \
  --boundary 0 \
  --before-order "$recovery_user_order" \
  --recent-limit "$recent_limit" \
  --token-budget "$token_budget" \
  --output "$result_dir/expected-bounded-recovery-tail.json"
jq -e \
  --argjson recent_limit "$recent_limit" \
  --slurpfile expected "$result_dir/expected-bounded-recovery-tail.json" '
    [.messages[] | {
      message_order: .message_order,
      role: .role,
      content_hash: .content_hash
    }] as $actual |
    $expected[0].messages as $candidates |
    ($actual | length) > 0 and
    ($actual | length) <= $recent_limit and
    $actual == $candidates
  ' "$result_dir/recovery-context.json" >/dev/null

summary_deadline=$((SECONDS + ${SUMMARY_RETRY_SETTLE_TIMEOUT_SECONDS:-30}))
latest_summary=
while ((SECONDS < summary_deadline)); do
  latest_summary=$(postgres_command -qAt -c "
    SELECT json_build_object(
      'through_message_order', through_message_order,
      'summary_hash', summary_hash,
      'summary_ref', summary_ref::json,
      'model_revision', model_revision
    )::text
    FROM conversation_summaries
    WHERE conversation_id='$conversation_id'
    ORDER BY through_message_order DESC
    LIMIT 1;
  ")
  [[ -n "$latest_summary" ]] && break
  sleep 0.1
done
[[ -n "$latest_summary" ]] || {
  printf 'post-crash terminal turn did not retry the missing summary\n' >&2
  exit 1
}
printf '%s\n' "$latest_summary" >"$result_dir/retried-summary.json"
jq -e '
  .through_message_order > 0 and
  .summary_hash == .summary_ref.content_hash
' "$result_dir/retried-summary.json" >/dev/null

postgres_command -qAt -c "
  SELECT json_build_object(
    'terminal_results', (
      SELECT COUNT(*)
      FROM terminal_run_results r
      JOIN terminal_run_admissions a ON a.run_id=r.run_id
      WHERE a.conversation_id='$conversation_id'
    ),
    'assistant_messages', (
      SELECT COUNT(*) FROM conversation_messages
      WHERE conversation_id='$conversation_id' AND role='assistant'
    ),
    'orphan_assistants', (
      SELECT COUNT(*)
      FROM conversation_messages m
      LEFT JOIN terminal_run_results r ON r.run_id=m.run_id
      WHERE m.conversation_id='$conversation_id'
        AND m.role='assistant'
        AND r.run_id IS NULL
    )
  )::text;
" >"$result_dir/post-recovery-database.json"
jq -e \
  --argjson expected "$((seed_turns + 1))" '
    .terminal_results == $expected and
    .assistant_messages == $expected and
    .orphan_assistants == 0
  ' "$result_dir/post-recovery-database.json" >/dev/null

fallback_orders=$(jq -c '[.messages[].message_order]' \
  "$result_dir/recovery-context.json")
jq -n \
  --arg conversation_id "$conversation_id" \
  --arg old_pod "$old_pod" \
  --arg old_uid "$old_uid" \
  --arg replacement_pod "$replacement_pod" \
  --arg replacement_uid "$replacement_uid" \
  --argjson delay_ms "$delay_ms" \
  --argjson eligible_messages "$eligible_messages" \
  --argjson recovery_elapsed_seconds "$recovery_elapsed_seconds" \
  --argjson fallback_orders "$fallback_orders" \
  --slurpfile process_death "$result_dir/process-death-evidence.json" \
  --slurpfile retried_summary "$result_dir/retried-summary.json" \
  '{
    passed: true,
    conversation_id: $conversation_id,
    injection: {
      kind: "qualification_summary_worker_delay",
      delay_ms: $delay_ms,
      qualification_only: true,
      latest_object_deleted: false
    },
    worker_window: {
      active_jobs: 1,
      eligible_messages: $eligible_messages,
      summaries_committed: 0,
      terminal_turns_preserved: true
    },
    hard_kill: {
      old_pod: $old_pod,
      old_uid: $old_uid,
      replacement_pod: $replacement_pod,
      replacement_uid: $replacement_uid,
      uid_changed: true,
      process_death: $process_death[0]
    },
    post_restart_turn: {
      elapsed_seconds: $recovery_elapsed_seconds,
      blocked_by_summary: false,
      summary_fallback_was_null: true,
      exact_recent_tail_window: true,
      recent_message_orders: $fallback_orders
    },
    retry: {
      generated_after_new_terminal_turn: true,
      latest_summary: $retried_summary[0]
    },
    atomic_terminal_assistant_pairs: true
  }' >"$result_dir/summary-worker-crash-report.json"
printf 'Summary worker crash evidence: %s\n' "$result_dir"
