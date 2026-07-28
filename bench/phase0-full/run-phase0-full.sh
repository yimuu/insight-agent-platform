#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
bench_root=$(cd "$script_dir/.." && pwd)
workspace_root=$(cd "$bench_root/.." && pwd)
# Reuse only transport/snapshot helpers. Phase 0 has its own workload,
# physical attribution SQL, evaluator, thresholds, and report schema.
# shellcheck source=bench/terminal-only/lib.sh
source "$bench_root/terminal-only/lib.sh"

require_command k6
require_command jq
require_command python3
require_nonempty BASE_URL "${BASE_URL:-}"

profile=${1:-qualification}
case "$profile" in
  qualification)
    warmup_duration=1m
    measured_duration=10m
    arrival_rate=10
    preallocated_vus=20
    max_vus=50
    agent_id=action_demo
    qualification_flag=--qualification
    ;;
  smoke)
    warmup_duration=${WARMUP_DURATION:-10s}
    measured_duration=${MEASURED_DURATION:-30s}
    arrival_rate=${ARRIVAL_RATE:-10}
    preallocated_vus=${PREALLOCATED_VUS:-20}
    max_vus=${MAX_VUS:-50}
    agent_id=${AGENT_ID:-action_demo}
    qualification_flag=
    ;;
  *)
    printf 'usage: run-phase0-full.sh [qualification|smoke] [result-directory]\n' >&2
    exit 2
    ;;
esac

result_dir=${2:-"$workspace_root/bench/results/phase0-full-${profile}"}
mkdir -p "$result_dir"

infrastructure_args=()
database_preflight_args=()
statistics_reset_args=()
if [[ "$profile" == qualification ]]; then
  require_nonempty \
    PHASE0_FULL_PREFLIGHT_EVIDENCE \
    "${PHASE0_FULL_PREFLIGHT_EVIDENCE:-}"
  require_nonempty BENCH_NAMESPACE "${BENCH_NAMESPACE:-}"
  require_nonempty BENCH_RELEASE "${BENCH_RELEASE:-}"
  "$script_dir/validate-fresh-deployment.sh" \
    "$PHASE0_FULL_PREFLIGHT_EVIDENCE" \
    "$result_dir/infrastructure-freshness.json"
  infrastructure_args=(
    --infrastructure-freshness
    "$result_dir/infrastructure-freshness.json"
  )
fi

ensure_gate_b_walinspect >"$result_dir/pg-walinspect-version.txt"
assert_postgres_durability

if [[ "$profile" == qualification ]]; then
  postgres_file "$script_dir/sql/database-preflight.sql" \
    -qAt >"$result_dir/database-freshness-before-warmup.json"
  jq -e '.passed == true' \
    "$result_dir/database-freshness-before-warmup.json" >/dev/null || {
    printf 'Phase 0 full database preflight failed; see %s\n' \
      "$result_dir/database-freshness-before-warmup.json" >&2
    exit 1
  }
  database_preflight_args=(
    --database-preflight
    "$result_dir/database-freshness-before-warmup.json"
  )

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
    printf 'Phase 0 database statistics reset did not establish a fresh epoch\n' >&2
    exit 1
  }
  statistics_reset_args=(
    --statistics-reset
    "$result_dir/statistics-reset-before-warmup.json"
  )
fi

printf 'Running full-runtime Phase 0 warm-up (%s); this interval is excluded.\n' \
  "$warmup_duration"
BASE_URL="$BASE_URL" \
AGENT_ID="$agent_id" \
PROFILE="phase0-full-warmup" \
DURATION="$warmup_duration" \
ARRIVAL_RATE="$arrival_rate" \
PREALLOCATED_VUS="$preallocated_vus" \
MAX_VUS="$max_vus" \
SUMMARY_PATH="$result_dir/warmup-summary.json" \
  k6 run "$script_dir/k6/full-runs.js" >"$result_dir/warmup.log"

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
      scheduled_arrivals: count("phase0_full_arrivals_scheduled"),
      late_arrivals: count("phase0_full_arrivals_late"),
      max_arrival_lateness_ms:
        (.metrics.phase0_full_arrival_lateness.values.max // null),
      dropped_iterations: count("dropped_iterations"),
      accepted: count("phase0_full_run_accepted"),
      terminal_observed: count("phase0_full_run_terminal_observed"),
      succeeded: count("phase0_full_run_succeeded"),
      rejected: count("phase0_full_run_rejected"),
      failed: count("phase0_full_run_failed"),
      interrupted: count("phase0_full_run_interrupted")
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
  printf 'Phase 0 warm-up did not close before the measured LSN boundary\n' >&2
  exit 1
}

assert_postgres_durability
postgres_file "$script_dir/sql/relation-snapshot.sql" \
  -qAt >"$result_dir/relations-before.json"
capture_artifact_bytes "$result_dir/artifact-bytes-before.txt"
capture_runtime_pod_state "$result_dir/runtime-pod-before.json"
capture_runtime_topology "$result_dir/runtime-topology-before.json"
# Reset only pg_stat_statements. pg_stat_wal and the exact LSN boundary remain
# monotonic and are differenced by the evaluator.
capture_gate_b_before_snapshot "$result_dir/postgres-before.json"

printf 'Running measured full-runtime Phase 0 profile (%s at %s arrivals/s).\n' \
  "$measured_duration" "$arrival_rate"
BASE_URL="$BASE_URL" \
AGENT_ID="$agent_id" \
PROFILE="phase0-full-${profile}" \
DURATION="$measured_duration" \
ARRIVAL_RATE="$arrival_rate" \
PREALLOCATED_VUS="$preallocated_vus" \
MAX_VUS="$max_vus" \
SUMMARY_PATH="$result_dir/summary.json" \
  k6 run "$script_dir/k6/full-runs.js" >"$result_dir/k6.log"

capture_postgres_snapshot "$result_dir/postgres-after.json"
postgres_file "$script_dir/sql/relation-snapshot.sql" \
  -qAt >"$result_dir/relations-after.json"
capture_artifact_bytes "$result_dir/artifact-bytes-after.txt"
capture_runtime_pod_state "$result_dir/runtime-pod-after.json"
capture_runtime_topology "$result_dir/runtime-topology-after.json"
assert_postgres_durability

before_lsn=$(jq -er '.boundary.wal_insert_lsn | strings' \
  "$result_dir/postgres-before.json")
after_lsn=$(jq -er '.boundary.wal_insert_lsn | strings' \
  "$result_dir/postgres-after.json")
postgres_file "$script_dir/sql/physical-wal-attribution.sql" \
  -qAt \
  -v start_lsn="$before_lsn" \
  -v end_lsn="$after_lsn" \
  >"$result_dir/postgres-physical-wal.json"
extract_physical_wal_csv \
  "$result_dir/postgres-physical-wal.json" \
  "$result_dir/postgres-physical-wal.csv"
extract_embedded_top_wal_csv \
  "$result_dir/postgres-after.json" \
  "$result_dir/postgres-top-wal-statements.csv"

expected_seconds=$(duration_seconds "$measured_duration")
expected_arrivals=$((arrival_rate * expected_seconds))
jq -n \
  --arg profile "$profile" \
  --arg mode full \
  --arg agent_id "$agent_id" \
  --arg warmup_duration "$warmup_duration" \
  --arg duration "$measured_duration" \
  --arg fixed_input_text "phase0 full WAL baseline fixture" \
  --argjson warmup_seconds "$warmup_seconds" \
  --argjson expected_seconds "$expected_seconds" \
  --argjson arrival_rate "$arrival_rate" \
  --argjson expected_arrivals "$expected_arrivals" \
  --argjson preallocated_vus "$preallocated_vus" \
  --argjson max_vus "$max_vus" \
  '{
    profile: $profile,
    persistence_mode: $mode,
    agent_id: $agent_id,
    warmup_duration: $warmup_duration,
    warmup_seconds: $warmup_seconds,
    duration: $duration,
    expected_seconds: $expected_seconds,
    arrival_rate_per_second: $arrival_rate,
    expected_arrivals: $expected_arrivals,
    preallocated_vus: $preallocated_vus,
    max_vus: $max_vus,
    fixture: {
      request_body: {text: $fixed_input_text},
      variable_fields: ["x-request-id"]
    },
    minimum_wal_keep_size_bytes: 8589934592
  }' >"$result_dir/profile.json"

python3 "$script_dir/report.py" \
  --before "$result_dir/postgres-before.json" \
  --after "$result_dir/postgres-after.json" \
  --relations-before "$result_dir/relations-before.json" \
  --relations-after "$result_dir/relations-after.json" \
  --physical-wal "$result_dir/postgres-physical-wal.json" \
  --top-wal "$result_dir/postgres-top-wal-statements.csv" \
  --warmup "$result_dir/warmup-summary.json" \
  --k6 "$result_dir/summary.json" \
  --profile "$result_dir/profile.json" \
  --artifact-before "$result_dir/artifact-bytes-before.txt" \
  --artifact-after "$result_dir/artifact-bytes-after.txt" \
  --pod-before "$result_dir/runtime-pod-before.json" \
  --pod-after "$result_dir/runtime-pod-after.json" \
  --topology-before "$result_dir/runtime-topology-before.json" \
  --topology-after "$result_dir/runtime-topology-after.json" \
  "${infrastructure_args[@]}" \
  "${database_preflight_args[@]}" \
  "${statistics_reset_args[@]}" \
  --output "$result_dir/phase0-full-report.json" \
  ${qualification_flag:+"$qualification_flag"}

printf 'Phase 0 full evidence: %s\n' "$result_dir"
