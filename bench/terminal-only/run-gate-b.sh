#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command k6
require_command jq
require_command python3
require_nonempty BASE_URL "${BASE_URL:-}"

profile=${1:-qualification}
case "$profile" in
  qualification)
    warmup_duration=1m
    measured_duration=2h
    arrival_rate=10
    preallocated_vus=20
    max_vus=50
    agent_id=action_demo
    runtime_sample_interval_seconds=1
    qualification_flag=--qualification
    ;;
  smoke)
    warmup_duration=${WARMUP_DURATION:-10s}
    measured_duration=${MEASURED_DURATION:-30s}
    arrival_rate=${ARRIVAL_RATE:-10}
    preallocated_vus=${PREALLOCATED_VUS:-20}
    max_vus=${MAX_VUS:-50}
    agent_id=${AGENT_ID:-action_demo}
    runtime_sample_interval_seconds=${RUNTIME_SAMPLE_INTERVAL_SECONDS:-1}
    qualification_flag=
    ;;
  *)
    printf 'usage: run-gate-b.sh [qualification|smoke] [result-directory]\n' >&2
    exit 2
    ;;
esac
[[ "$runtime_sample_interval_seconds" =~ ^[1-9][0-9]*$ ]] || {
  printf 'runtime sample interval must be a positive integer number of seconds\n' >&2
  exit 2
}

result_dir=${2:-"$terminal_bench_root/bench/results/terminal-only-gate-b-${profile}"}
mkdir -p "$result_dir"
preflight_args=()
if [[ "$profile" == qualification ]]; then
  require_nonempty \
    GATE_B_PREFLIGHT_EVIDENCE \
    "${GATE_B_PREFLIGHT_EVIDENCE:-}"
  require_nonempty BENCH_NAMESPACE "${BENCH_NAMESPACE:-}"
  require_nonempty BENCH_RELEASE "${BENCH_RELEASE:-}"
  "$script_dir/validate-fresh-qualification.sh" \
    "$GATE_B_PREFLIGHT_EVIDENCE" \
    "$result_dir/infrastructure-freshness.json"
  capture_gate_b_database_preflight \
    "$result_dir/database-freshness-before-warmup.json"
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
    }' >"$result_dir/statistics-reset-before-warmup.json"
  jq -e '.passed == true' \
    "$result_dir/statistics-reset-before-warmup.json" >/dev/null || {
    printf 'Gate B database statistics reset did not establish a fresh epoch\n' >&2
    exit 1
  }
  preflight_args=(
    --infrastructure-freshness
    "$result_dir/infrastructure-freshness.json"
    --database-preflight
    "$result_dir/database-freshness-before-warmup.json"
    --statistics-reset
    "$result_dir/statistics-reset-before-warmup.json"
  )
fi
ensure_gate_b_walinspect >"$result_dir/pg-walinspect-version.txt"
assert_postgres_durability

printf 'Running terminal-only warm-up (%s); this interval is not sampled.\n' \
  "$warmup_duration"
BASE_URL="$BASE_URL" \
AGENT_ID="$agent_id" \
PROFILE="gate-b-warmup" \
DURATION="$warmup_duration" \
ARRIVAL_RATE="$arrival_rate" \
PREALLOCATED_VUS="$preallocated_vus" \
MAX_VUS="$max_vus" \
SUMMARY_PATH="$result_dir/warmup-summary.json" \
  k6 run "$script_dir/k6/terminal-runs.js" \
  >"$result_dir/warmup.log"

warmup_seconds=$(duration_seconds "$warmup_duration")
warmup_expected_arrivals=$((arrival_rate * warmup_seconds))
warmup_minimum_duration_ms=$((warmup_seconds * 1000))
warmup_maximum_duration_ms=$(((warmup_seconds + 30) * 1000))
jq \
  --argjson expected "$warmup_expected_arrivals" \
  --argjson arrival_rate "$arrival_rate" \
  --argjson minimum_duration_ms "$warmup_minimum_duration_ms" \
  --argjson maximum_duration_ms "$warmup_maximum_duration_ms" '
    def count($name): (.metrics[$name].values.count // 0);
    {
      expected_arrivals: $expected,
      actual_duration_ms: .state.testRunDurationMs,
      iterations: count("iterations"),
      scheduled_arrivals: count("terminal_run_arrivals_scheduled"),
      late_arrivals: count("terminal_run_arrivals_late"),
      max_arrival_lateness_ms:
        (.metrics.terminal_run_arrival_lateness.values.max // null),
      dropped_iterations: count("dropped_iterations"),
      accepted: count("terminal_run_accepted"),
      terminal_observed: count("terminal_run_terminal_observed"),
      succeeded: count("terminal_run_succeeded"),
      rejected: count("terminal_run_rejected"),
      failed: count("terminal_run_failed"),
      interrupted: count("terminal_run_interrupted")
    } |
    .passed = (
      .iterations == $expected and
      .scheduled_arrivals == $expected and
      .iterations == .scheduled_arrivals and
      .late_arrivals == 0 and
      (.max_arrival_lateness_ms | type == "number") and
      .max_arrival_lateness_ms >= 0 and
      .max_arrival_lateness_ms < (1000 / $arrival_rate) and
      .dropped_iterations == 0 and
      .accepted == $expected and
      .terminal_observed == .accepted and
      .succeeded == .accepted and
      .rejected == 0 and
      .failed == 0 and
      .interrupted == 0 and
      (.actual_duration_ms | type == "number") and
      .actual_duration_ms >= $minimum_duration_ms and
      .actual_duration_ms <= $maximum_duration_ms
    )
  ' "$result_dir/warmup-summary.json" \
  >"$result_dir/warmup-closure-evidence.json"
jq -e '.passed == true' \
  "$result_dir/warmup-closure-evidence.json" >/dev/null || {
  printf 'Gate B warm-up did not close before the measured LSN boundary\n' >&2
  exit 1
}

# The reset happens inside the boundary snapshot after its read-only census and
# immediately before pg_stat_wal/top-statement metadata are captured.
assert_postgres_durability
capture_gate_b_before_snapshot "$result_dir/postgres-before.json"
capture_runtime_metrics "$result_dir/runtime-before.prom"
capture_runtime_process_snapshot "$result_dir/runtime-process-before.txt"
capture_runtime_pod_state "$result_dir/runtime-pod-before.json"
capture_runtime_topology "$result_dir/runtime-topology-before.json"
capture_artifact_bytes "$result_dir/artifact-bytes-before.txt"

printf 'Running measured terminal-only profile (%s at %s arrivals/s).\n' \
  "$measured_duration" "$arrival_rate"
sampler_pid=
cleanup_sampler() {
  if [[ -n "$sampler_pid" ]] && kill -0 "$sampler_pid" 2>/dev/null; then
    kill "$sampler_pid" 2>/dev/null || true
    wait "$sampler_pid" 2>/dev/null || true
  fi
  sampler_pid=
}
stop_sampler() {
  if [[ -z "$sampler_pid" ]]; then
    return
  fi
  if ! kill -0 "$sampler_pid" 2>/dev/null; then
    local sampler_status=0
    wait "$sampler_pid" || sampler_status=$?
    sampler_pid=
    printf 'runtime sampler exited unexpectedly with status %s\n' \
      "$sampler_status" >&2
    return 1
  fi
  kill "$sampler_pid" 2>/dev/null || true
  local sampler_status=0
  wait "$sampler_pid" || sampler_status=$?
  sampler_pid=
  if (( sampler_status != 0 && sampler_status != 143 )); then
    printf 'runtime sampler stopped with unexpected status %s\n' \
      "$sampler_status" >&2
    return 1
  fi
}
trap cleanup_sampler EXIT INT TERM
(
  while true; do
    printf '# sample_epoch_seconds %s\n' "$(date +%s)"
    api_curl --max-time "${RUNTIME_SAMPLE_TIMEOUT_SECONDS:-5}" \
      "$BASE_URL/metrics"
    sleep "$runtime_sample_interval_seconds"
  done
) >"$result_dir/runtime-samples.prom" &
sampler_pid=$!
BASE_URL="$BASE_URL" \
AGENT_ID="$agent_id" \
PROFILE="gate-b-${profile}" \
DURATION="$measured_duration" \
ARRIVAL_RATE="$arrival_rate" \
PREALLOCATED_VUS="$preallocated_vus" \
MAX_VUS="$max_vus" \
SUMMARY_PATH="$result_dir/summary.json" \
  k6 run "$script_dir/k6/terminal-runs.js" \
  >"$result_dir/k6.log"
stop_sampler
trap - EXIT INT TERM

capture_postgres_snapshot "$result_dir/postgres-after.json"
before_lsn=$(jq -er '.boundary.wal_insert_lsn | strings' \
  "$result_dir/postgres-before.json")
after_lsn=$(jq -er '.boundary.wal_insert_lsn | strings' \
  "$result_dir/postgres-after.json")
capture_physical_wal_records \
  "$before_lsn" \
  "$after_lsn" \
  "$result_dir/postgres-physical-wal.json"
extract_physical_wal_csv \
  "$result_dir/postgres-physical-wal.json" \
  "$result_dir/postgres-physical-wal.csv"
assert_postgres_durability
capture_runtime_metrics "$result_dir/runtime-after.prom"
capture_runtime_process_snapshot "$result_dir/runtime-process-after.txt"
capture_runtime_pod_state "$result_dir/runtime-pod-after.json"
capture_runtime_topology "$result_dir/runtime-topology-after.json"
capture_artifact_bytes "$result_dir/artifact-bytes-after.txt"
extract_embedded_top_wal_csv \
  "$result_dir/postgres-after.json" \
  "$result_dir/postgres-top-wal-statements.csv"
expected_seconds=$(duration_seconds "$measured_duration")
expected_arrivals=$((arrival_rate * expected_seconds))
jq -n \
  --arg profile "$profile" \
  --arg warmup_duration "$warmup_duration" \
  --arg duration "$measured_duration" \
  --arg agent_id "$agent_id" \
  --argjson expected_seconds "$expected_seconds" \
  --argjson warmup_seconds "$warmup_seconds" \
  --argjson warmup_expected_arrivals "$warmup_expected_arrivals" \
  --argjson arrival_rate "$arrival_rate" \
  --argjson expected_arrivals "$expected_arrivals" \
  --argjson preallocated_vus "$preallocated_vus" \
  --argjson max_vus "$max_vus" \
  --argjson runtime_sample_interval_seconds "$runtime_sample_interval_seconds" \
  '{
    profile: $profile,
    warmup_duration: $warmup_duration,
    warmup_seconds: $warmup_seconds,
    warmup_expected_arrivals: $warmup_expected_arrivals,
    duration: $duration,
    expected_seconds: $expected_seconds,
    arrival_rate_per_second: $arrival_rate,
    expected_arrivals: $expected_arrivals,
    preallocated_vus: $preallocated_vus,
    max_vus: $max_vus,
    agent_id: $agent_id,
    runtime_sample_interval_seconds: $runtime_sample_interval_seconds
  }' >"$result_dir/profile.json"

python3 "$script_dir/report.py" gate-b \
  --before "$result_dir/postgres-before.json" \
  --after "$result_dir/postgres-after.json" \
  --warmup "$result_dir/warmup-summary.json" \
  --k6 "$result_dir/summary.json" \
  --runtime-before "$result_dir/runtime-before.prom" \
  --runtime-after "$result_dir/runtime-after.prom" \
  --runtime-samples "$result_dir/runtime-samples.prom" \
  --process-before "$result_dir/runtime-process-before.txt" \
  --process-after "$result_dir/runtime-process-after.txt" \
  --pod-before "$result_dir/runtime-pod-before.json" \
  --pod-after "$result_dir/runtime-pod-after.json" \
  --topology-before "$result_dir/runtime-topology-before.json" \
  --topology-after "$result_dir/runtime-topology-after.json" \
  --artifact-before "$result_dir/artifact-bytes-before.txt" \
  --artifact-after "$result_dir/artifact-bytes-after.txt" \
  --top-wal "$result_dir/postgres-top-wal-statements.csv" \
  --physical-wal "$result_dir/postgres-physical-wal.json" \
  --physical-wal-csv "$result_dir/postgres-physical-wal.csv" \
  --warmup-seconds "$warmup_seconds" \
  --warmup-expected-arrivals "$warmup_expected_arrivals" \
  --expected-seconds "$expected_seconds" \
  --expected-arrivals "$expected_arrivals" \
  --sample-interval-seconds "$runtime_sample_interval_seconds" \
  "${preflight_args[@]}" \
  --output "$result_dir/gate-b-report.json" \
  ${qualification_flag:+"$qualification_flag"}

printf 'Gate B evidence: %s\n' "$result_dir"
