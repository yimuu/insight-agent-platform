#!/usr/bin/env bash
set -euo pipefail

namespace=${BENCH_NAMESPACE:-insight-bench}
release=${BENCH_RELEASE:-bench}
idle_seconds=${BENCH_IDLE_SECONDS:-300}
result_dir=${1:?usage: capture-idle-discovery.sh RESULT_DIR}
runtime_name="${release}-insight-agent-platform"
postgresql_pod="${runtime_name}-postgresql-0"

if [[ ! "$idle_seconds" =~ ^[0-9]+$ ]] || ((idle_seconds < 1)); then
  printf 'BENCH_IDLE_SECONDS must be a positive integer\n' >&2
  exit 2
fi

mkdir -p "$result_dir"

capture_table_stats() {
  local destination=$1
  kubectl -n "$namespace" exec "$postgresql_pod" -- \
    psql -U insight -d insight_agent_platform --csv -c "
      SELECT relname,seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_live_tup,n_dead_tup
      FROM pg_stat_user_tables
      WHERE relname IN (
        'workflow_runs','execution_events','scheduler_checkpoints',
        'task_outbox','model_tool_calls','timers',
        'public_event_outbox','public_event_delivery_heads',
        'wait_late_audit_outbox'
      )
      ORDER BY relname;
    " >"$destination"
}

active_runs=$(kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform -qAt -c "
    SELECT count(*)
    FROM workflow_runs
    WHERE lifecycle IN ('created','active','waiting','completing','terminating');
  ")
active_runs=${active_runs##*$'\n'}
if [[ "$active_runs" != "0" ]]; then
  printf 'idle discovery capture requires zero active runs; found %s\n' "$active_runs" >&2
  exit 1
fi

kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform -v ON_ERROR_STOP=1 -qAt -c \
  "SELECT pg_stat_reset(); SELECT pg_stat_statements_reset();" >/dev/null

capture_table_stats "$result_dir/table-stats-before.csv"
kubectl get --raw \
  "/api/v1/namespaces/${namespace}/services/${runtime_name}:3000/proxy/metrics" \
  >"$result_dir/runtime-metrics-before.prom"

started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
elapsed=0
while ((elapsed < idle_seconds)); do
  remaining=$((idle_seconds - elapsed))
  interval=5
  if ((remaining < interval)); then
    interval=$remaining
  fi
  sleep "$interval"
  elapsed=$((elapsed + interval))
done
finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)

capture_table_stats "$result_dir/table-stats-after.csv"
kubectl get --raw \
  "/api/v1/namespaces/${namespace}/services/${runtime_name}:3000/proxy/metrics" \
  >"$result_dir/runtime-metrics-after.prom"
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT calls,total_exec_time,mean_exec_time,rows,
           left(regexp_replace(query, E'[\\n\\r\\t ]+', ' ', 'g'), 240) AS query
    FROM pg_stat_statements
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    ORDER BY total_exec_time DESC
    LIMIT 50;
  " >"$result_dir/database-top-statements.csv"
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT
      (SELECT count(*) FROM workflow_runs) AS workflow_runs,
      (SELECT count(*) FROM execution_events) AS execution_events,
      (SELECT count(*) FROM scheduler_checkpoints) AS scheduler_checkpoints,
      (SELECT count(*) FROM task_outbox) AS task_outbox,
      (SELECT count(*) FROM model_tool_calls) AS model_tool_calls;
  " >"$result_dir/key-row-counts.csv"
printf 'namespace=%s\nrelease=%s\nidle_seconds=%s\nstarted_at=%s\nfinished_at=%s\n' \
  "$namespace" "$release" "$idle_seconds" "$started_at" "$finished_at" \
  >"$result_dir/profile.env"

printf 'Saved %ss idle discovery evidence to %s\n' "$idle_seconds" "$result_dir"
