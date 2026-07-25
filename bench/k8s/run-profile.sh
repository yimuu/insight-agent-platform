#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
chart_dir="$workspace_root/deploy/helm/insight-agent-platform"
namespace=${BENCH_NAMESPACE:-insight-bench}
release=${BENCH_RELEASE:-bench}
profile=${1:?usage: run-profile.sh PROFILE VUS DURATION [RESULT_DIR]}
virtual_users=${2:?usage: run-profile.sh PROFILE VUS DURATION [RESULT_DIR]}
duration=${3:?usage: run-profile.sh PROFILE VUS DURATION [RESULT_DIR]}
result_dir=${4:-"$workspace_root/bench/results/$profile"}
run_id=$(printf '%s-%s' "$profile" "$(date -u +%H%M%S)" | tr '_' '-')
job_name="${release}-insight-agent-platform-k6-${run_id}"
runtime_name="${release}-insight-agent-platform"
postgresql_pod="${runtime_name}-postgresql-0"

mkdir -p "$result_dir"

# Remove the previous load Job and wait for the runtime to be steady before
# resetting per-profile database statistics.
helm upgrade --install "$release" "$chart_dir" \
  --namespace "$namespace" \
  --create-namespace \
  --values "$chart_dir/values-benchmark.yaml" \
  --set loadTest.enabled=false \
  --wait \
  --timeout 5m

runtime_pod=$(kubectl -n "$namespace" get pods \
  -l "app.kubernetes.io/component=runtime" \
  -o jsonpath='{.items[0].metadata.name}')

capture_cgroup() {
  local pod=$1
  local container=$2
  local destination=$3
  kubectl -n "$namespace" exec "$pod" -c "$container" -- sh -c '
    for metric in cpu.stat memory.current memory.peak memory.events pids.current; do
      echo "[$metric]"
      cat "/sys/fs/cgroup/$metric" 2>/dev/null || true
    done
  ' >"$destination"
}

capture_database_stats() {
  local destination=$1
  kubectl -n "$namespace" exec "$postgresql_pod" -- \
    psql -U insight -d insight_agent_platform --csv -c "
      SELECT datname, xact_commit, xact_rollback, blks_read, blks_hit,
             temp_files, temp_bytes, deadlocks, conflicts,
             tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted
      FROM pg_stat_database
      WHERE datname = current_database();
    " >"$destination"
}

kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform -v ON_ERROR_STOP=1 -qAt -c \
  "SELECT pg_stat_reset(); SELECT pg_stat_statements_reset();" >/dev/null
capture_cgroup "$runtime_pod" runtime "$result_dir/runtime-cgroup-before.txt"
capture_cgroup "$postgresql_pod" postgresql "$result_dir/postgresql-cgroup-before.txt"
capture_database_stats "$result_dir/database-before.csv"

helm upgrade --install "$release" "$chart_dir" \
  --namespace "$namespace" \
  --create-namespace \
  --values "$chart_dir/values-benchmark.yaml" \
  --set loadTest.enabled=true \
  --set loadTest.runId="$run_id" \
  --set loadTest.virtualUsers="$virtual_users" \
  --set-string loadTest.duration="$duration" \
  --wait \
  --timeout 5m

sample_file="$result_dir/resources.csv"
printf 'timestamp,pod,container,cpu,memory,restarts,phase\n' >"$sample_file"
database_activity_file="$result_dir/database-activity.csv"
printf 'timestamp,active_connections,lock_waiting_connections,ungranted_locks\n' \
  >"$database_activity_file"
(
  while kubectl -n "$namespace" get "job/$job_name" >/dev/null 2>&1; do
    timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    kubectl -n "$namespace" top pods --containers --no-headers 2>/dev/null |
      awk -v timestamp="$timestamp" -v phase="$profile" \
        '{print timestamp "," $1 "," $2 "," $3 "," $4 ",," phase}' >>"$sample_file" || true
    sleep 5
  done
) &
sampler_pid=$!
(
  while kubectl -n "$namespace" get "job/$job_name" >/dev/null 2>&1; do
    kubectl -n "$namespace" exec "$postgresql_pod" -- \
      psql -U insight -d insight_agent_platform -qAt -F, -c "
        SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
               count(*) FILTER (WHERE state = 'active'),
               count(*) FILTER (WHERE wait_event_type = 'Lock'),
               (SELECT count(*) FROM pg_locks WHERE NOT granted)
        FROM pg_stat_activity
        WHERE datname = current_database();
      " >>"$database_activity_file" 2>/dev/null || true
    sleep 2
  done
) &
database_sampler_pid=$!

set +e
kubectl -n "$namespace" wait --for=condition=complete "job/$job_name" --timeout=10m
wait_status=$?
set -e

kill "$sampler_pid" >/dev/null 2>&1 || true
wait "$sampler_pid" >/dev/null 2>&1 || true
kill "$database_sampler_pid" >/dev/null 2>&1 || true
wait "$database_sampler_pid" >/dev/null 2>&1 || true

pod_name=$(kubectl -n "$namespace" get pods \
  -l "job-name=$job_name" \
  -o jsonpath='{.items[0].metadata.name}')
kubectl -n "$namespace" logs "$pod_name" >"$result_dir/k6.log"
awk '
  /^K6_SUMMARY_JSON_BEGIN$/ { capture = 1; next }
  /^K6_SUMMARY_JSON_END$/ { capture = 0; next }
  capture { print }
' "$result_dir/k6.log" >"$result_dir/summary.json"
if [[ ! -s "$result_dir/summary.json" ]]; then
  printf 'k6 summary JSON was not present in %s\n' "$result_dir/k6.log" >&2
  exit 1
fi
kubectl -n "$namespace" get pods -o wide >"$result_dir/pods.txt"
kubectl -n "$namespace" get events --sort-by=.lastTimestamp >"$result_dir/events.txt"
capture_cgroup "$runtime_pod" runtime "$result_dir/runtime-cgroup-after.txt"
capture_cgroup "$postgresql_pod" postgresql "$result_dir/postgresql-cgroup-after.txt"
capture_database_stats "$result_dir/database-after.csv"
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT calls, total_exec_time, mean_exec_time, rows,
           left(regexp_replace(query, E'[\\n\\r\\t ]+', ' ', 'g'), 240) AS query
    FROM pg_stat_statements
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    ORDER BY total_exec_time DESC
    LIMIT 25;
  " >"$result_dir/database-top-statements.csv"
printf 'profile=%s\nvirtual_users=%s\nduration=%s\nrun_id=%s\n' \
  "$profile" "$virtual_users" "$duration" "$run_id" >"$result_dir/profile.env"

if [[ "$wait_status" -ne 0 ]]; then
  printf 'k6 Job did not complete successfully; inspect %s\n' "$result_dir/k6.log" >&2
  exit "$wait_status"
fi

printf 'Saved profile %s (%s VUs for %s) to %s\n' \
  "$profile" "$virtual_users" "$duration" "$result_dir"
