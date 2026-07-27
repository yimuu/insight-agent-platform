#!/usr/bin/env bash
set -euo pipefail

namespace=${BENCH_NAMESPACE:-insight-bench}
release=${BENCH_RELEASE:-bench}
result_dir=${1:?usage: inject-listener-fault.sh RESULT_DIR}
runtime_name="${release}-insight-agent-platform"
postgresql_pod="${runtime_name}-postgresql-0"

mkdir -p "$result_dir"

metrics_before=$(kubectl get --raw \
  "/api/v1/namespaces/${namespace}/services/${runtime_name}:3000/proxy/metrics")
printf '%s\n' "$metrics_before" >"$result_dir/runtime-metrics-before.prom"
reconnects_before=$(awk '
  $1 == "insight_runtime_notification_listener_reconnects_total{backend=\"postgres\"}" {
    print $2
  }
' <<<"$metrics_before")

listener_pid=$(kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform -qAt -c "
    SELECT pid
    FROM pg_stat_activity
    WHERE datname=current_database()
      AND query LIKE 'LISTEN \"iap_work_%'
    ORDER BY pid
    LIMIT 1;
  ")
listener_pid=${listener_pid##*$'\n'}
if [[ ! "$listener_pid" =~ ^[0-9]+$ ]]; then
  printf 'durable work LISTEN backend was not found\n' >&2
  exit 1
fi

terminated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform -v ON_ERROR_STOP=1 -qAt -c \
  "SELECT pg_terminate_backend(${listener_pid});" \
  >"$result_dir/terminate-result.txt"

reconnected=false
for _ in $(seq 1 120); do
  metrics_after=$(kubectl get --raw \
    "/api/v1/namespaces/${namespace}/services/${runtime_name}:3000/proxy/metrics")
  reconnects_after=$(awk '
    $1 == "insight_runtime_notification_listener_reconnects_total{backend=\"postgres\"}" {
      print $2
    }
  ' <<<"$metrics_after")
  listener_state=$(awk '
    $1 == "insight_runtime_notification_listener_state{backend=\"postgres\"}" {print $2}
  ' <<<"$metrics_after")
  if [[ "${listener_state:-0}" == "1" ]] &&
    (( ${reconnects_after:-0} > ${reconnects_before:-0} )); then
    reconnected=true
    break
  fi
  sleep 0.25
done

printf '%s\n' "$metrics_after" >"$result_dir/runtime-metrics-after.prom"
printf 'namespace=%s\nrelease=%s\nlistener_pid=%s\nterminated_at=%s\nreconnects_before=%s\nreconnects_after=%s\nlistener_state=%s\n' \
  "$namespace" "$release" "$listener_pid" "$terminated_at" \
  "${reconnects_before:-0}" "${reconnects_after:-0}" "${listener_state:-0}" \
  >"$result_dir/fault.env"

if [[ "$reconnected" != "true" ]]; then
  printf 'listener did not reconnect within 30 seconds\n' >&2
  exit 1
fi

printf 'Terminated LISTEN backend %s; reconnect counter advanced to %s\n' \
  "$listener_pid" "$reconnects_after"
