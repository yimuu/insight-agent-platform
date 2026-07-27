#!/usr/bin/env bash
set -euo pipefail

namespace=${BENCH_NAMESPACE:-insight-bench}
release=${BENCH_RELEASE:-bench}
wait_seconds=${BENCH_CLAIM_WAIT_SECONDS:-120}
result_dir=${1:?usage: inject-runtime-restart-during-claim.sh RESULT_DIR}
runtime_name="${release}-insight-agent-platform"
postgresql_pod="${runtime_name}-postgresql-0"

if [[ ! "$wait_seconds" =~ ^[0-9]+$ ]] || ((wait_seconds < 1)); then
  printf 'BENCH_CLAIM_WAIT_SECONDS must be a positive integer\n' >&2
  exit 2
fi

mkdir -p "$result_dir"
deadline=$((SECONDS + wait_seconds))
claimed=0
while ((SECONDS < deadline)); do
  claimed=$(kubectl -n "$namespace" exec "$postgresql_pod" -- \
    psql -U insight -d insight_agent_platform -qAt -c "
      SELECT count(*)
      FROM task_outbox
      WHERE task_state='claimed';
    ")
  claimed=${claimed##*$'\n'}
  if [[ "$claimed" =~ ^[0-9]+$ ]] && ((claimed > 0)); then
    break
  fi
  sleep 0.1
done
if [[ ! "$claimed" =~ ^[0-9]+$ ]] || ((claimed == 0)); then
  printf 'no claimed scheduler task was observed within %ss\n' "$wait_seconds" >&2
  exit 1
fi

runtime_pod=$(kubectl -n "$namespace" get pods \
  -l app.kubernetes.io/component=runtime \
  -o jsonpath='{.items[0].metadata.name}')
if [[ -z "$runtime_pod" ]]; then
  printf 'runtime Pod was not found\n' >&2
  exit 1
fi
kubectl -n "$namespace" get pod "$runtime_pod" -o yaml \
  >"$result_dir/runtime-pod-before.yaml"
kubectl get --raw \
  "/api/v1/namespaces/${namespace}/services/${runtime_name}:3000/proxy/metrics" \
  >"$result_dir/runtime-metrics-before.prom"
deleted_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
printf 'namespace=%s\nrelease=%s\nclaimed_tasks_before=%s\ndeleted_pod=%s\ndeleted_at=%s\n' \
  "$namespace" "$release" "$claimed" "$runtime_pod" "$deleted_at" \
  >"$result_dir/fault.env"
kubectl -n "$namespace" delete pod "$runtime_pod" --wait=false \
  >"$result_dir/delete-result.txt"
kubectl -n "$namespace" rollout status "deployment/$runtime_name" --timeout=120s \
  >"$result_dir/rollout-status.txt"

replacement_pod=
for _ in $(seq 1 120); do
  replacement_pod=$(kubectl -n "$namespace" get pods \
    -l app.kubernetes.io/component=runtime \
    --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' |
    awk -v deleted="$runtime_pod" '$0 != deleted {print; exit}')
  if [[ -n "$replacement_pod" ]] &&
    kubectl -n "$namespace" wait --for=condition=Ready \
      "pod/$replacement_pod" --timeout=1s >/dev/null 2>&1; then
    break
  fi
  replacement_pod=
  sleep 0.25
done
if [[ -z "$replacement_pod" ]]; then
  printf 'Deployment did not produce a distinct Ready replacement Pod\n' >&2
  exit 1
fi

kubectl -n "$namespace" get pod "$replacement_pod" -o yaml \
  >"$result_dir/runtime-pod-after.yaml"
metrics_captured=false
for _ in $(seq 1 120); do
  if kubectl get --raw \
    "/api/v1/namespaces/${namespace}/services/${runtime_name}:3000/proxy/metrics" \
    >"$result_dir/runtime-metrics-after.prom" 2>/dev/null; then
    metrics_captured=true
    break
  fi
  sleep 0.25
done
if [[ "$metrics_captured" != "true" ]]; then
  printf 'replacement Pod became Ready but metrics endpoint did not recover\n' >&2
  exit 1
fi

kubectl -n "$namespace" exec "$postgresql_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT task_state,count(*)
    FROM task_outbox
    GROUP BY task_state
    ORDER BY task_state;
  " >"$result_dir/task-states-after.csv"
printf 'replacement_pod=%s\n' "$replacement_pod" >>"$result_dir/fault.env"

if [[ "$replacement_pod" == "$runtime_pod" ]]; then
  printf 'Deployment did not replace the deleted runtime Pod\n' >&2
  exit 1
fi

printf 'Deleted runtime Pod %s with %s claimed task(s); replacement %s is Ready\n' \
  "$runtime_pod" "$claimed" "$replacement_pod"
