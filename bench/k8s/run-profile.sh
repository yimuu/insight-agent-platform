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
run_id=$(printf '%s-%s' "$profile" "$(date -u +%H%M%S)" |
  tr '[:upper:]_' '[:lower:]-')
chart_run_id=${run_id:0:24}
chart_run_id=${chart_run_id%-}
if [[ ! "$chart_run_id" =~ ^[a-z0-9-]+$ ]]; then
  printf 'profile must contain only letters, digits, underscores, or hyphens\n' >&2
  exit 2
fi
raw_job_name="${release}-insight-agent-platform-k6-${chart_run_id}"
job_name=${raw_job_name:0:63}
job_name=${job_name%-}
runtime_name="${release}-insight-agent-platform"
postgresql_pod="${runtime_name}-postgresql-0"
profile_values=${BENCH_PROFILE_VALUES:-}
if [[ -z "$profile_values" ]]; then
  case "$profile" in
    limited*) profile_values="$chart_dir/values-benchmark-limited.yaml" ;;
    c1*) profile_values="$chart_dir/values-benchmark-c1.yaml" ;;
    c2*) profile_values="$chart_dir/values-benchmark-c2.yaml" ;;
  esac
fi

duration_seconds() {
  local value=$1
  awk -v duration="$value" '
    BEGIN {
      if (duration ~ /^[0-9]+s$/) {
        sub(/s$/, "", duration)
        print duration
      } else if (duration ~ /^[0-9]+m$/) {
        sub(/m$/, "", duration)
        print duration * 60
      } else if (duration ~ /^[0-9]+h$/) {
        sub(/h$/, "", duration)
        print duration * 3600
      } else {
        print 1800
      }
    }
  '
}

values_args=(--values "$chart_dir/values-benchmark.yaml")
if [[ -n "$profile_values" ]]; then
  values_args+=(--values "$profile_values")
fi
if [[ -n "${BENCH_IMAGE_REPOSITORY:-}" ]]; then
  values_args+=(--set-string "image.repository=$BENCH_IMAGE_REPOSITORY")
fi
if [[ -n "${BENCH_IMAGE_TAG:-}" ]]; then
  values_args+=(--set-string "image.tag=$BENCH_IMAGE_TAG")
fi
if [[ -n "${BENCH_LOADTEST_MEMORY_REQUEST:-}" ]]; then
  values_args+=(
    --set-string "loadTest.resources.requests.memory=$BENCH_LOADTEST_MEMORY_REQUEST"
  )
fi
if [[ -n "${BENCH_LOADTEST_MEMORY_LIMIT:-}" ]]; then
  values_args+=(
    --set-string "loadTest.resources.limits.memory=$BENCH_LOADTEST_MEMORY_LIMIT"
  )
fi
loadtest_script=run-lifecycle.js
hold_duration_seconds=1800
loadtest_executor=constant-vus
loadtest_iterations=$virtual_users
loadtest_arrival_rate=10
loadtest_max_vus=50
loadtest_round_interval_seconds=0
profile_duration_seconds=$(duration_seconds "$duration")
scenario=${BENCH_SCENARIO:-}
if [[ -z "$scenario" ]]; then
  case "$profile" in
    *wait*) scenario=wait ;;
    *burst*) scenario=burst ;;
    *sustained*) scenario=sustained ;;
    *) scenario=lifecycle ;;
  esac
fi
if [[ "$scenario" == "wait" ]]; then
  loadtest_script=wait-capacity.js
  hold_duration_seconds=$profile_duration_seconds
elif [[ "$scenario" == "burst" ]]; then
  loadtest_executor=per-vu-iterations
  loadtest_iterations=${BENCH_BURST_ROUNDS:-20}
  loadtest_round_interval_seconds=${BENCH_BURST_ROUND_INTERVAL_SECONDS:-10}
elif [[ "$scenario" == "sustained" ]]; then
  loadtest_executor=constant-arrival-rate
  loadtest_arrival_rate=$virtual_users
  loadtest_max_vus=50
elif [[ "$scenario" != "lifecycle" ]]; then
  printf 'BENCH_SCENARIO must be wait, burst, sustained, or lifecycle\n' >&2
  exit 2
fi
job_wait_timeout_seconds=$((profile_duration_seconds + 300))

mkdir -p "$result_dir"

# Remove the previous load Job and wait for the runtime to be steady before
# resetting per-profile database statistics.
helm upgrade --install "$release" "$chart_dir" \
  --namespace "$namespace" \
  --create-namespace \
  "${values_args[@]}" \
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

capture_database_shared_stats() {
  local destination=$1
  kubectl -n "$namespace" exec "$postgresql_pod" -- \
    psql -U insight -d insight_agent_platform --csv -c "
      SELECT * FROM pg_stat_bgwriter;
      SELECT * FROM pg_stat_wal;
    " >"$destination"
}

capture_key_row_counts() {
  local destination=$1
  kubectl -n "$namespace" exec "$postgresql_pod" -- \
    psql -U insight -d insight_agent_platform --csv -c "
      SELECT
        (SELECT count(*) FROM workflow_runs) AS workflow_runs,
        (SELECT count(*) FROM execution_events) AS execution_events,
        (SELECT count(*) FROM scheduler_checkpoints) AS scheduler_checkpoints,
        (SELECT count(*) FROM control_transition_results) AS control_transition_results,
        (SELECT count(*) FROM task_outbox) AS task_outbox,
        (SELECT count(*) FROM model_tool_calls) AS model_tool_calls,
        (SELECT count(*) FROM public_event_outbox) AS public_event_outbox,
        (SELECT count(*) FROM wait_late_audit_outbox) AS wait_late_audit_outbox;
    " >"$destination"
}

capture_table_stats() {
  local destination=$1
  kubectl -n "$namespace" exec "$postgresql_pod" -- \
    psql -U insight -d insight_agent_platform --csv -c "
      SELECT relname,seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_live_tup,n_dead_tup
      FROM pg_stat_user_tables
      WHERE relname IN (
        'workflow_runs','execution_events','scheduler_checkpoints',
        'task_outbox','model_tool_calls','timers',
        'public_event_delivery_heads','wait_late_audit_outbox'
      )
      ORDER BY relname;
    " >"$destination"
}

capture_runtime_metrics() {
  local destination=$1
  kubectl get --raw \
    "/api/v1/namespaces/${namespace}/services/${runtime_name}:3000/proxy/metrics" \
    >"$destination" 2>/dev/null || true
}

kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform -v ON_ERROR_STOP=1 -qAt -c \
  "SELECT pg_stat_reset(); SELECT pg_stat_statements_reset();" >/dev/null
capture_cgroup "$runtime_pod" runtime "$result_dir/runtime-cgroup-before.txt"
capture_cgroup "$postgresql_pod" postgresql "$result_dir/postgresql-cgroup-before.txt"
capture_database_stats "$result_dir/database-before.csv"
capture_database_shared_stats "$result_dir/database-shared-before.csv"
capture_key_row_counts "$result_dir/key-row-counts-before.csv"
capture_table_stats "$result_dir/table-stats-before.csv"
capture_runtime_metrics "$result_dir/runtime-metrics-before.prom"
kubectl -n "$namespace" get pods -o wide >"$result_dir/pods-before.txt"
kubectl -n "$namespace" get pod "$runtime_pod" \
  -o jsonpath='{range .status.containerStatuses[*]}{.name}{","}{.restartCount}{","}{.image}{","}{.imageID}{"\n"}{end}' \
  >"$result_dir/runtime-image-before.txt"

helm upgrade --install "$release" "$chart_dir" \
  --namespace "$namespace" \
  --create-namespace \
  "${values_args[@]}" \
  --set loadTest.enabled=true \
  --set loadTest.runId="$run_id" \
  --set-string loadTest.script="$loadtest_script" \
  --set-string loadTest.executor="$loadtest_executor" \
  --set-string loadTest.iterations="$loadtest_iterations" \
  --set-string loadTest.arrivalRate="$loadtest_arrival_rate" \
  --set-string loadTest.maxVirtualUsers="$loadtest_max_vus" \
  --set loadTest.virtualUsers="$virtual_users" \
  --set-string loadTest.duration="$duration" \
  --set-string loadTest.holdDurationSeconds="$hold_duration_seconds" \
  --set-string loadTest.roundIntervalSeconds="$loadtest_round_interval_seconds" \
  --wait \
  --timeout 5m

sample_file="$result_dir/resources.csv"
printf 'timestamp,pod,container,cpu,memory,restarts,phase\n' >"$sample_file"
database_activity_file="$result_dir/database-activity.csv"
printf 'timestamp,active_connections,lock_waiting_connections,ungranted_locks,active_runs,waiting_runs,open_waits,queue_oldest_age_seconds\n' \
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
               (SELECT count(*) FROM pg_locks WHERE NOT granted),
               (SELECT count(*) FROM workflow_runs
                WHERE lifecycle IN ('created','active','waiting','completing','terminating')),
               (SELECT count(*) FROM workflow_runs WHERE lifecycle='waiting'),
               (SELECT count(*) FROM scheduler_wait_registrations w
                JOIN workflow_runs r ON r.run_id=w.run_id
                WHERE w.winner_kind IS NULL
                  AND r.lifecycle IN ('created','active','waiting','completing','terminating'))
               ,
               GREATEST(
                 0,
                 COALESCE((SELECT EXTRACT(EPOCH FROM (clock_timestamp()-min(available_at)))
                           FROM task_outbox
                           WHERE task_state='pending'
                             AND available_at<=clock_timestamp()), 0),
                 COALESCE((SELECT EXTRACT(EPOCH FROM (clock_timestamp()-min(claim_expires_at)))
                           FROM task_outbox
                           WHERE task_state='claimed'
                             AND claim_expires_at<=clock_timestamp()), 0),
                 COALESCE((SELECT EXTRACT(EPOCH FROM (clock_timestamp()-min(available_at)))
                           FROM model_tool_calls
                           WHERE call_status='pending'
                             AND available_at<=clock_timestamp()), 0),
                 COALESCE((SELECT EXTRACT(EPOCH FROM (clock_timestamp()-min(claim_expires_at)))
                           FROM model_tool_calls
                           WHERE call_status IN ('claimed','running')
                             AND claim_expires_at<=clock_timestamp()), 0),
                 COALESCE((SELECT EXTRACT(EPOCH FROM (clock_timestamp()-min(deadline_at)))
                           FROM timers
                           WHERE timer_state='scheduled'
                             AND deadline_at<=clock_timestamp()), 0),
                 COALESCE((SELECT EXTRACT(EPOCH FROM (clock_timestamp()-min(received_at)))
                           FROM signals_inbox
                           WHERE signal_state='pending'), 0),
                 COALESCE((SELECT EXTRACT(EPOCH FROM (clock_timestamp()-min(due_at)))
                           FROM public_event_delivery_heads
                           WHERE head_state='ready'
                             AND due_at<=clock_timestamp()), 0),
                 COALESCE((SELECT EXTRACT(EPOCH FROM (clock_timestamp()-min(due_at)))
                           FROM wait_late_audit_outbox
                           WHERE audit_state='pending'
                             AND due_at<=clock_timestamp()), 0)
               )
        FROM pg_stat_activity
        WHERE datname = current_database();
      " >>"$database_activity_file" 2>/dev/null || true
    sleep 2
  done
) &
database_sampler_pid=$!
runtime_activity_file="$result_dir/runtime-activity.csv"
printf 'timestamp,active_runs,executing_scheduler,executing_model_tool,executing_recovery\n' \
  >"$runtime_activity_file"
(
  while kubectl -n "$namespace" get "job/$job_name" >/dev/null 2>&1; do
    timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    metrics=$(kubectl get --raw \
      "/api/v1/namespaces/${namespace}/services/${runtime_name}:3000/proxy/metrics" \
      2>/dev/null || true)
    active_runs=$(awk '
      $1 == "insight_runtime_active_runs{lifecycle_class=\"nonterminal\"}" {print $2}
    ' <<<"$metrics")
    executing_scheduler=$(awk '
      $1 == "insight_runtime_executing_operations{work_class=\"scheduler_task\"}" {print $2}
    ' <<<"$metrics")
    executing_model_tool=$(awk '
      $1 == "insight_runtime_executing_operations{work_class=\"model_tool_task\"}" {print $2}
    ' <<<"$metrics")
    executing_recovery=$(awk '
      $1 == "insight_runtime_executing_operations{work_class=\"recovery\"}" {print $2}
    ' <<<"$metrics")
    printf '%s,%s,%s,%s,%s\n' \
      "$timestamp" "${active_runs:-0}" "${executing_scheduler:-0}" \
      "${executing_model_tool:-0}" "${executing_recovery:-0}" \
      >>"$runtime_activity_file"
    sleep 2
  done
) &
runtime_sampler_pid=$!
process_memory_file="$result_dir/process-memory.csv"
printf 'timestamp,pod,container,vmrss_kib,rss_anon_kib,rss_file_kib,pss_kib,cgroup_memory_bytes\n' \
  >"$process_memory_file"
(
  while kubectl -n "$namespace" get "job/$job_name" >/dev/null 2>&1; do
    timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    current_runtime_pod=$(kubectl -n "$namespace" get pods \
      -l "app.kubernetes.io/component=runtime" \
      --field-selector=status.phase=Running \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
    for pod_and_container in \
      "${current_runtime_pod:-},runtime" \
      "$postgresql_pod,postgresql"; do
      pod=${pod_and_container%%,*}
      container=${pod_and_container#*,}
      [[ -n "$pod" ]] || continue
      values=$(kubectl -n "$namespace" exec "$pod" -c "$container" -- sh -c '
        awk "
          /^VmRSS:/ {vmrss=\$2}
          /^RssAnon:/ {anon=\$2}
          /^RssFile:/ {file=\$2}
          END {printf \"%s,%s,%s\", vmrss+0, anon+0, file+0}
        " /proc/1/status
        printf ","
        awk "/^Pss:/ {print \$2; exit}" /proc/1/smaps_rollup 2>/dev/null || printf "0\n"
        cat /sys/fs/cgroup/memory.current 2>/dev/null || printf "0\n"
      ' 2>/dev/null | tr '\n' ',' | sed 's/,$//' || true)
      if [[ -n "$values" ]]; then
        printf '%s,%s,%s,%s\n' "$timestamp" "$pod" "$container" "$values" \
          >>"$process_memory_file"
      fi
    done
    sleep 10
  done
) &
process_memory_sampler_pid=$!

set +e
kubectl -n "$namespace" wait --for=condition=complete "job/$job_name" \
  --timeout="${job_wait_timeout_seconds}s"
wait_status=$?
set -e

kill "$sampler_pid" >/dev/null 2>&1 || true
wait "$sampler_pid" >/dev/null 2>&1 || true
kill "$database_sampler_pid" >/dev/null 2>&1 || true
wait "$database_sampler_pid" >/dev/null 2>&1 || true
kill "$runtime_sampler_pid" >/dev/null 2>&1 || true
wait "$runtime_sampler_pid" >/dev/null 2>&1 || true
kill "$process_memory_sampler_pid" >/dev/null 2>&1 || true
wait "$process_memory_sampler_pid" >/dev/null 2>&1 || true

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
runtime_pod=$(kubectl -n "$namespace" get pods \
  -l "app.kubernetes.io/component=runtime" \
  -o jsonpath='{.items[0].metadata.name}')
capture_cgroup "$runtime_pod" runtime "$result_dir/runtime-cgroup-after.txt"
capture_cgroup "$postgresql_pod" postgresql "$result_dir/postgresql-cgroup-after.txt"
capture_database_stats "$result_dir/database-after.csv"
capture_database_shared_stats "$result_dir/database-shared-after.csv"
capture_table_stats "$result_dir/table-stats-after.csv"
capture_runtime_metrics "$result_dir/runtime-metrics-after.prom"
kubectl -n "$namespace" logs "$runtime_pod" -c runtime \
  >"$result_dir/runtime.log" 2>&1 || true
kubectl -n "$namespace" logs "$postgresql_pod" -c postgresql \
  >"$result_dir/postgresql.log" 2>&1 || true
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT calls, total_exec_time, mean_exec_time, rows,
           shared_blks_hit, shared_blks_read,
           temp_blks_read, temp_blks_written, wal_bytes,
           left(regexp_replace(query, E'[\\n\\r\\t ]+', ' ', 'g'), 240) AS query
    FROM pg_stat_statements
    WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
    ORDER BY total_exec_time DESC
    LIMIT 25;
  " >"$result_dir/database-top-statements.csv"
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT COALESCE(sum(calls),0) AS notification_calls,
           COALESCE(sum(total_exec_time),0) AS notification_total_exec_ms
    FROM pg_stat_statements
    WHERE query ILIKE '%pg_notify%';
  " >"$result_dir/database-notification-statements.csv"
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT pg_database_size(current_database()) AS database_bytes;
    SELECT relname,
           pg_total_relation_size(relid) AS total_bytes,
           pg_relation_size(relid) AS heap_bytes,
           pg_indexes_size(relid) AS index_bytes
    FROM pg_catalog.pg_statio_user_tables
    ORDER BY pg_total_relation_size(relid) DESC
    LIMIT 25;
  " >"$result_dir/database-size.csv"
helm get values "$release" -n "$namespace" --all \
  >"$result_dir/helm-values.yaml"
helm get manifest "$release" -n "$namespace" \
  >"$result_dir/helm-manifest.yaml"
helm status "$release" -n "$namespace" \
  >"$result_dir/helm-release.txt"
kubectl -n "$namespace" get pod "$runtime_pod" \
  -o jsonpath='{range .status.containerStatuses[*]}{.name}{","}{.image}{","}{.imageID}{"\n"}{end}' \
  >"$result_dir/runtime-image.txt"
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT contract_id,backend,installed_at
    FROM durable_schema_contract
    WHERE singleton=1;
  " >"$result_dir/schema-contract.csv"
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT lifecycle,count(*) AS runs
    FROM workflow_runs
    GROUP BY lifecycle
    ORDER BY lifecycle;
  " >"$result_dir/run-state-counts.csv"
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    WITH sampled AS (
      SELECT run_id,lifecycle,projection_version,next_event_seq,
             terminal_event_id,terminal_public_event_id,response_id
      FROM workflow_runs
      WHERE lifecycle IN ('succeeded','failed','cancelled','interrupted','timed_out')
      ORDER BY terminal_at DESC,run_id DESC
      LIMIT 100
    )
    SELECT
      count(*) AS sampled_runs,
      count(*) FILTER (WHERE lifecycle<>'succeeded') AS non_succeeded,
      count(*) FILTER (
        WHERE COALESCE((
          SELECT min(seq)<>1 OR max(seq)<>sampled.next_event_seq-1
                 OR count(*)<>sampled.next_event_seq-1
          FROM execution_events
          WHERE run_id=sampled.run_id
        ),TRUE)
      ) AS event_sequence_violations,
      count(*) FILTER (
        WHERE COALESCE((
          SELECT max(projection_version_after)
          FROM execution_events
          WHERE run_id=sampled.run_id
        ),-1)<>sampled.projection_version
      ) AS projection_version_violations,
      count(*) FILTER (
        WHERE NOT EXISTS (
          SELECT 1
          FROM public_event_outbox outbox
          WHERE outbox.run_id=sampled.run_id
            AND outbox.public_event_id=sampled.terminal_public_event_id
            AND outbox.is_terminal
            AND outbox.publish_state='published'
        )
        OR NOT EXISTS (
          SELECT 1
          FROM public_event_projection_decisions decision
          WHERE decision.run_id=sampled.run_id
            AND decision.public_event_id=sampled.terminal_public_event_id
            AND decision.is_terminal
            AND decision.decision='public'
        )
      ) AS public_event_violations,
      count(*) FILTER (
        WHERE NOT EXISTS (
          SELECT 1
          FROM public_event_delivery_heads head
          WHERE head.run_id=sampled.run_id
            AND head.head_state='drained'
        )
      ) AS delivery_head_violations,
      count(*) FILTER (
        WHERE NOT EXISTS (
          SELECT 1
          FROM response_snapshots snapshot
          WHERE snapshot.run_id=sampled.run_id
            AND snapshot.response_id=sampled.response_id
        )
      ) AS terminal_snapshot_violations
    FROM sampled;
  " >"$result_dir/consistency-sample.csv"
capture_key_row_counts "$result_dir/key-row-counts.csv"
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform -P pager=off -c "
    EXPLAIN (ANALYZE,BUFFERS)
    SELECT run_id,task_id
    FROM task_outbox
    WHERE task_state='pending' AND available_at<=statement_timestamp()
    ORDER BY available_at,run_id,task_id
    LIMIT 8;
    EXPLAIN (ANALYZE,BUFFERS)
    SELECT run_id,task_id
    FROM task_outbox
    WHERE task_state='claimed' AND claim_expires_at<=statement_timestamp()
    ORDER BY claim_expires_at,run_id,task_id
    LIMIT 8;
    EXPLAIN (ANALYZE,BUFFERS)
    SELECT run_id,call_id
    FROM model_tool_calls
    WHERE call_status='pending' AND available_at<=statement_timestamp()
    ORDER BY available_at,run_id,call_id
    LIMIT 8;
    EXPLAIN (ANALYZE,BUFFERS)
    SELECT run_id,call_id
    FROM model_tool_calls
    WHERE call_status IN ('claimed','running')
      AND claim_expires_at<=statement_timestamp()
    ORDER BY claim_expires_at,run_id,call_id
    LIMIT 8;
    EXPLAIN (ANALYZE,BUFFERS)
    SELECT r.run_id
    FROM workflow_runs r
    WHERE r.lifecycle='terminating'
       OR (
         r.lifecycle IN ('created','active','waiting')
         AND r.admission_state='open'
         AND (
           NOT EXISTS (
             SELECT 1
             FROM scheduler_checkpoints checkpoint
             WHERE checkpoint.run_id=r.run_id
               AND checkpoint.scheduler_projection_version>=r.projection_version
           )
           OR (
             SELECT checkpoint.checkpoint_kind
             FROM scheduler_checkpoints checkpoint
             WHERE checkpoint.run_id=r.run_id
             ORDER BY checkpoint.scheduler_projection_version DESC,
                      checkpoint.created_at DESC,
                      checkpoint.checkpoint_id DESC
             LIMIT 1
           )='task_completed'
         )
       )
    ORDER BY r.updated_at,r.run_id
    LIMIT 256;
    EXPLAIN (ANALYZE,BUFFERS)
    SELECT run_id,public_event_id
    FROM public_event_outbox
    WHERE publish_state='published'
      AND NOT is_terminal
      AND retain_until IS NOT NULL
      AND retain_until<=statement_timestamp()
    ORDER BY retain_until,run_id,public_event_id
    FOR UPDATE SKIP LOCKED
    LIMIT 256;
  " >"$result_dir/discovery-explain.txt"
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT count(*) AS signals,
           percentile_cont(0.50) WITHIN GROUP (
             ORDER BY EXTRACT(EPOCH FROM (terminal_at-received_at)) * 1000
           ) AS authority_p50_ms,
           percentile_cont(0.95) WITHIN GROUP (
             ORDER BY EXTRACT(EPOCH FROM (terminal_at-received_at)) * 1000
           ) AS authority_p95_ms,
           percentile_cont(0.99) WITHIN GROUP (
             ORDER BY EXTRACT(EPOCH FROM (terminal_at-received_at)) * 1000
           ) AS authority_p99_ms,
           max(EXTRACT(EPOCH FROM (terminal_at-received_at)) * 1000) AS authority_max_ms
    FROM signals_inbox
    WHERE message_id LIKE 'k6-${chart_run_id}-%'
      AND terminal_at IS NOT NULL;
  " >"$result_dir/signal-authority-latency.csv"
kubectl version -o yaml >"$result_dir/kubernetes-version.yaml"
kubectl get nodes -o wide >"$result_dir/nodes.txt"
git -C "$workspace_root" rev-parse HEAD >"$result_dir/commit-sha.txt"
git -C "$workspace_root" status --short >"$result_dir/worktree-status.txt"
git -C "$workspace_root" diff --binary >"$result_dir/worktree.patch"
printf 'profile=%s\nscenario=%s\nvirtual_users=%s\nduration=%s\nrun_id=%s\n' \
  "$profile" "$scenario" "$virtual_users" "$duration" "$run_id" \
  >"$result_dir/profile.env"

if [[ "$wait_status" -ne 0 ]]; then
  printf 'k6 Job did not complete successfully; inspect %s\n' "$result_dir/k6.log" >&2
  exit "$wait_status"
fi

printf 'Saved profile %s (%s VUs for %s) to %s\n' \
  "$profile" "$virtual_users" "$duration" "$result_dir"
