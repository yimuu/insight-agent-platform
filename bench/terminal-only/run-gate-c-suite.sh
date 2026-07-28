#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command jq
require_command helm
require_command kubectl
require_command curl
require_nonempty BASE_URL "${BASE_URL:-}"

batch_id=${GATE_C_SUITE_BATCH_ID:-"$(date -u +%Y%m%dT%H%M%S)-${RANDOM}"}
[[ "$batch_id" =~ ^[A-Za-z0-9-]+$ ]] || {
  printf 'GATE_C_SUITE_BATCH_ID may contain only letters, digits, and hyphens\n' >&2
  exit 2
}
result_dir=${1:-"$terminal_bench_root/bench/results/terminal-only-gate-c-suite-$batch_id"}
mkdir -p "$result_dir"
namespace=${BENCH_NAMESPACE:-insight-bench}
release=${BENCH_RELEASE:-bench}
chart=${BENCH_HELM_CHART:-"$terminal_bench_root/deploy/helm/insight-agent-platform"}
stream_fixture_name=${STREAM_FIXTURE_NAME:-terminal-stream-mock}
kubectl -n "$namespace" get service "$stream_fixture_name" >/dev/null

set_faults() {
  local admission_ms=$1
  local post_commit_ms=$2
  local label=$3
  local summary_ms=${4:-0}
  local helm_status=0
  local rollout_status=0
  helm upgrade "$release" "$chart" -n "$namespace" --reuse-values \
    --set qualification.enabled=true \
    --set "runtime.qualificationFaults.admissionDelayMs=$admission_ms" \
    --set "runtime.qualificationFaults.postCommitDelayMs=$post_commit_ms" \
    --set "runtime.qualificationFaults.summaryDelayMs=$summary_ms" \
    >"$result_dir/helm-$label.txt" \
    2>"$result_dir/helm-$label.stderr" ||
    helm_status=$?
  kubectl -n "$namespace" rollout status \
    "deployment/${release}-insight-agent-platform" --timeout=180s \
    >"$result_dir/rollout-$label.txt" \
    2>"$result_dir/rollout-$label.stderr" ||
    rollout_status=$?
  jq -n \
    --arg label "$label" \
    --argjson admission_delay_ms "$admission_ms" \
    --argjson post_commit_delay_ms "$post_commit_ms" \
    --argjson summary_delay_ms "$summary_ms" \
    --argjson helm_status "$helm_status" \
    --argjson rollout_status "$rollout_status" \
    '{
      label: $label,
      requested: {
        admission_delay_ms: $admission_delay_ms,
        post_commit_delay_ms: $post_commit_delay_ms,
        summary_delay_ms: $summary_delay_ms
      },
      helm_status: $helm_status,
      rollout_status: $rollout_status,
      applied: ($helm_status == 0 and $rollout_status == 0)
    }' >"$result_dir/fault-apply-$label.json" ||
    return $?
  ((helm_status == 0 && rollout_status == 0))
}

reset_faults() {
  local label=${1:-reset}
  local apply_status=0
  local verification_status=0
  set_faults 0 0 "$label" || apply_status=$?
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

faults_reset=0
port_forward_pid=
stop_suite_port_forward() {
  [[ -n "$port_forward_pid" ]] || return 0
  kill "$port_forward_pid" >/dev/null 2>&1 || true
  wait "$port_forward_pid" >/dev/null 2>&1 || true
  port_forward_pid=
}
cleanup_suite() {
  local original_status=$?
  local reset_status=0
  local final_status=$original_status
  trap - EXIT

  if ((faults_reset == 0)); then
    reset_faults cleanup || reset_status=$?
  elif ! qualification_assert_faults_zero \
    "$result_dir/fault-zero-exit.json"; then
    reset_status=1
    reset_faults cleanup-retry || reset_status=$?
  fi
  stop_suite_port_forward
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
      reset_failure_forced_suite_failure:
        ($original_status == 0 and $reset_status != 0)
    }' >"$result_dir/suite-cleanup.json"; then
    ((final_status != 0)) || final_status=1
  fi
  exit "$final_status"
}
trap cleanup_suite EXIT

kubectl -n "$namespace" port-forward \
  "service/$stream_fixture_name" 0:8080 \
  >"$result_dir/provider-port-forward.log" 2>&1 &
port_forward_pid=$!
provider_port=
deadline=$((SECONDS + 30))
while (( SECONDS < deadline )); do
  if ! kill -0 "$port_forward_pid" 2>/dev/null; then
    cat "$result_dir/provider-port-forward.log" >&2
    exit 1
  fi
  provider_port=$(sed -nE \
    's/^Forwarding from 127\.0\.0\.1:([0-9]+) -> 8080$/\1/p' \
    "$result_dir/provider-port-forward.log" | head -1)
  [[ -n "$provider_port" ]] && break
  sleep 0.1
done
[[ "$provider_port" =~ ^[1-9][0-9]*$ ]] || {
  printf 'provider stats port-forward did not become ready\n' >&2
  cat "$result_dir/provider-port-forward.log" >&2
  exit 1
}
provider_stats_url="http://127.0.0.1:$provider_port/stats"
api_curl "$provider_stats_url" >"$result_dir/provider-port-forward-ready.json"

common_environment=(
  "BASE_URL=$BASE_URL"
  "BENCH_NAMESPACE=${BENCH_NAMESPACE:-insight-bench}"
  "BENCH_RELEASE=${BENCH_RELEASE:-bench}"
  "BENCH_RUNTIME_SELECTOR=${BENCH_RUNTIME_SELECTOR:-app.kubernetes.io/component=runtime}"
  "BENCH_ARTIFACT_ROOT=${BENCH_ARTIFACT_ROOT:-/data/artifacts}"
  "GATE_C_RUN_COUNT=50"
  "GATE_C_QUALIFICATION=1"
  "TENANT_ID=gate-c-tenant-$batch_id"
  "USER_ID=gate-c-user-$batch_id"
  "GATE_C_OWNER_EXPIRY_WAIT_SECONDS=${GATE_C_OWNER_EXPIRY_WAIT_SECONDS:-35}"
)

set_faults "${GATE_C_ADMISSION_DELAY_MS:-10000}" 0 admission-delay
env "${common_environment[@]}" \
  GATE_C_BATCH_ID="$batch_id-admission-hard" \
  GATE_C_AGENT_ID=terminal_failure_fixture \
  GATE_C_KILL_TARGET=runtime \
  GATE_C_RUNTIME_SHUTDOWN=hard \
  GATE_C_VERIFY_EFFECTS=1 \
  GATE_C_EXPECT_EFFECTS_BEFORE_KILL=0 \
  "$script_dir/run-gate-c.sh" "$result_dir/admission-before-execution"

set_faults 0 0 no-faults
env "${common_environment[@]}" \
  GATE_C_BATCH_ID="$batch_id-action-hard" \
  GATE_C_AGENT_ID=terminal_failure_fixture \
  GATE_C_KILL_TARGET=runtime \
  GATE_C_RUNTIME_SHUTDOWN=hard \
  GATE_C_VERIFY_EFFECTS=1 \
  GATE_C_EXPECT_EFFECTS_BEFORE_KILL=50 \
  "$script_dir/run-gate-c.sh" "$result_dir/action-hard-kill"

env "${common_environment[@]}" \
  GATE_C_BATCH_ID="$batch_id-postgres" \
  GATE_C_AGENT_ID=terminal_failure_fixture \
  GATE_C_KILL_TARGET=postgres \
  GATE_C_RUNTIME_SHUTDOWN=hard \
  GATE_C_VERIFY_EFFECTS=1 \
  GATE_C_EXPECT_EFFECTS_BEFORE_KILL=50 \
  "$script_dir/run-gate-c.sh" "$result_dir/postgres-restart"

env "${common_environment[@]}" \
  GATE_C_BATCH_ID="$batch_id-graceful" \
  GATE_C_AGENT_ID=terminal_failure_fixture \
  GATE_C_KILL_TARGET=runtime \
  GATE_C_RUNTIME_SHUTDOWN=graceful \
  GATE_C_VERIFY_EFFECTS=1 \
  GATE_C_EXPECT_EFFECTS_BEFORE_KILL=50 \
  "$script_dir/run-gate-c.sh" "$result_dir/graceful-shutdown"

env "${common_environment[@]}" \
  GATE_C_BATCH_ID="$batch_id-llm-hard" \
  GATE_C_AGENT_ID=terminal_llm_failure_fixture \
  GATE_C_KILL_TARGET=runtime \
  GATE_C_RUNTIME_SHUTDOWN=hard \
  GATE_C_VERIFY_EFFECTS=0 \
  GATE_C_EXPECT_EFFECTS_BEFORE_KILL=0 \
  GATE_C_PROVIDER_STATS_URL="$provider_stats_url" \
  "$script_dir/run-gate-c.sh" "$result_dir/llm-hard-kill"

set_faults 0 "${GATE_C_POST_COMMIT_DELAY_MS:-10000}" post-commit-delay
env "${common_environment[@]}" \
  COMMIT_SSE_BATCH_ID="$batch_id-commit-sse" \
  "$script_dir/run-commit-before-sse.sh" "$result_dir/commit-before-sse"
set_faults 0 0 post-commit-reset

env "${common_environment[@]}" \
  CONTEXT_BATCH_ID="$batch_id-summary-object" \
  SUMMARY_TRIGGER_MESSAGES=30 \
  RECENT_CONTEXT_MESSAGES=20 \
  SUMMARY_TRIGGER_TOKENS=24000 \
  "$script_dir/run-context-summary.sh" "$result_dir/summary-object-fallback"

env "${common_environment[@]}" \
  SUMMARY_CRASH_BATCH_ID="$batch_id-summary-crash" \
  SUMMARY_WORKER_CRASH_DELAY_MS=30000 \
  SUMMARY_TRIGGER_MESSAGES=30 \
  RECENT_CONTEXT_MESSAGES=20 \
  SUMMARY_TRIGGER_TOKENS=24000 \
  BENCH_HELM_CHART="$chart" \
  "$script_dir/run-summary-worker-crash.sh" "$result_dir/summary-worker-crash"

reset_faults final
faults_reset=1

jq -s -e 'all(.[]; .passed == true)' \
  "$result_dir/admission-before-execution/gate-c-report.json" \
  "$result_dir/action-hard-kill/gate-c-report.json" \
  "$result_dir/postgres-restart/gate-c-report.json" \
  "$result_dir/graceful-shutdown/gate-c-report.json" \
  "$result_dir/llm-hard-kill/gate-c-report.json" \
  "$result_dir/commit-before-sse/commit-before-sse-report.json" \
  "$result_dir/summary-object-fallback/context-summary-report.json" \
  "$result_dir/summary-worker-crash/summary-worker-crash-report.json" >/dev/null

jq -n \
  --arg batch_id "$batch_id" \
  '{
    passed: true,
    batch_id: $batch_id,
    scenarios: {
      admission_commit_before_execution_hard_kill: true,
      action_external_effect_hard_kill: true,
      postgres_restart_owner_reregistration: true,
      graceful_shutdown: true,
      llm_execution_hard_kill: true,
      terminal_commit_before_sse_hard_kill: true,
      missing_summary_object_fallback: true,
      summary_worker_hard_kill_and_retry: true,
      same_request_no_effect_replay: true,
      new_request_explicit_effect_retry: true,
      conversation_atomicity: true
    }
  }' >"$result_dir/gate-c-suite-report.json"
printf 'Gate C composite evidence: %s\n' "$result_dir"
