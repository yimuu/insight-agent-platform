#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
chart="$repo_root/deploy/helm/insight-agent-platform"
backend=${1:?usage: run-k8s-database-regression.sh in_memory|nats_core [RESULT_DIR]}
stamp=$(date -u +%Y%m%dT%H%M%SZ)
result_dir=${2:-"$repo_root/bench/results/run-stream-$backend-db-$stamp"}
image_repository=${QUALIFICATION_IMAGE_REPOSITORY:-insight-agent-platform}
image_tag=${QUALIFICATION_IMAGE_TAG:-run-stream-nats-core-qualification}
deployment_image_tag="${image_tag}-k8s-$$"
namespace="insight-rs-${backend//_/-}-$$"
release=qualification
scratch=$(mktemp -d "${TMPDIR:-/tmp}/insight-run-stream-k8s.XXXXXX")
runtime_forward_pid=
postgres_forward_pid=
nats_forward_pid=
runtime_pod=
qualification_succeeded=0

cleanup() {
  if [[ "$qualification_succeeded" != 1 && -n "$runtime_pod" ]]; then
    kubectl -n "$namespace" logs "$runtime_pod" \
      >"$result_dir/runtime-failure.log" 2>&1 || true
    kubectl -n "$namespace" describe pod "$runtime_pod" \
      >"$result_dir/runtime-failure-describe.txt" 2>&1 || true
  fi
  if [[ "$qualification_succeeded" != 1 && "$backend" == nats_core ]]; then
    kubectl -n "$namespace" get all -o wide \
      >"$result_dir/nats-failure-resources.txt" 2>&1 || true
    kubectl -n "$namespace" logs deployment/nats --all-containers=true \
      >"$result_dir/nats-failure.log" 2>&1 || true
    kubectl -n "$namespace" describe pod -l app=run-stream-nats \
      >"$result_dir/nats-failure-describe.txt" 2>&1 || true
  fi
  for pid in "$runtime_forward_pid" "$postgres_forward_pid" "$nats_forward_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  helm uninstall "$release" -n "$namespace" >/dev/null 2>&1 || true
  kubectl delete namespace "$namespace" --wait=false >/dev/null 2>&1 || true
  docker image rm "$image_repository:$deployment_image_tag" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup EXIT

if [[ "$backend" != in_memory && "$backend" != nats_core ]]; then
  printf 'backend must be in_memory or nats_core\n' >&2
  exit 2
fi
for command in cargo curl docker helm jq kubectl openssl rg; do
  command -v "$command" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command" >&2
    exit 2
  }
done
mkdir -p "$result_dir"

if [[ "${BUILD_QUALIFICATION_IMAGE:-0}" == 1 ]]; then
  docker build --platform linux/arm64 \
    -t "$image_repository:$image_tag" "$repo_root"
fi
docker tag "$image_repository:$image_tag" \
  "$image_repository:$deployment_image_tag"
docker image inspect "$image_repository:$deployment_image_tag" \
  >"$result_dir/image-inspect.json"
kubectl create namespace "$namespace" >/dev/null
management_token=$(openssl rand -hex 32)
kubectl -n "$namespace" create secret generic qualification-management-token \
  --from-literal=token="$management_token" >/dev/null
cat >"$scratch/management-values.yaml" <<'EOF'
management:
  enabled: true
  operatorCredentials:
    - identity: qualification-operator
      tokenEnv: INSIGHT_QUALIFICATION_MANAGEMENT_TOKEN
      capabilities:
        - agent.read
        - agent.write
        - agent.validate
        - agent.publish
        - agent.deploy
        - agent.activate
mcp:
  secretEnv:
    - name: INSIGHT_QUALIFICATION_MANAGEMENT_TOKEN
      secretName: qualification-management-token
      secretKey: token
EOF

values=(
  --values "$chart/values-benchmark.yaml"
  --values "$scratch/management-values.yaml"
  --set-string "image.repository=$image_repository"
  --set-string "image.tag=$deployment_image_tag"
  --set image.pullPolicy=IfNotPresent
  --set runtime.databasePoolMaxConnections=10
  --set runtime.maxConcurrentRuns=50
  --set runtime.maxConcurrentOperations=16
  --set runtime.maxConcurrentOperationsPerRun=4
  --set runtime.terminalOnly.maxConcurrentRuns=50
  --set runtime.scheduler.claimBatchSize=8
  --set resources.requests.cpu=500m
  --set resources.requests.memory=512Mi
  --set resources.limits.cpu=2
  --set resources.limits.memory=1Gi
  --set postgresql.resources.requests.cpu=500m
  --set postgresql.resources.requests.memory=256Mi
  --set postgresql.resources.limits.cpu=2
  --set postgresql.resources.limits.memory=1Gi
  --set postgresql.maxConnections=100
  --set artifacts.persistence.enabled=false
  --set loadTest.enabled=false
)

if [[ "$backend" == nats_core ]]; then
  nats_image=${NATS_IMAGE:-nats:2.12.4-alpine}
  nats_box_image=${NATS_BOX_IMAGE:-natsio/nats-box:0.18.0}
  mkdir -p "$scratch/nsc" "$scratch/tls"
  nats_host="nats.$namespace.svc.cluster.local"
  openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
    -subj "/CN=$nats_host" \
    -addext "subjectAltName=DNS:$nats_host,DNS:nats" \
    -keyout "$scratch/tls/server-key.pem" \
    -out "$scratch/tls/server.pem" >/dev/null 2>&1
  nsc() {
    docker run --rm -v "$scratch/nsc:/nsc" "$nats_box_image" nsc -H /nsc "$@"
  }
  nsc add operator --name Insight --sys >/dev/null
  nsc add account --name APP >/dev/null
  nsc add user --account APP --name runtime \
    --allow-pub 'insight.qualification.run_stream.v1.*' \
    --allow-sub 'insight.qualification.run_stream.v1.*' >/dev/null
  nsc generate config --mem-resolver --force \
    --config-file /nsc/nats-operator.conf >/dev/null
  cat >"$scratch/nats-server.conf" <<EOF
include nats-operator.conf
port: 4222
http: 8222
tls {
  cert_file: /etc/nats/server.pem
  key_file: /etc/nats/server-key.pem
  timeout: 2
}
EOF
  kubectl -n "$namespace" create secret generic nats-server-material \
    --from-file=nats-server.conf="$scratch/nats-server.conf" \
    --from-file=nats-operator.conf="$scratch/nsc/nats-operator.conf" \
    --from-file=server.pem="$scratch/tls/server.pem" \
    --from-file=server-key.pem="$scratch/tls/server-key.pem" >/dev/null
  kubectl -n "$namespace" create secret generic insight-run-stream-nats-credentials \
    --from-file=credentials="$scratch/nsc/creds/Insight/APP/runtime.creds" >/dev/null
  kubectl -n "$namespace" create secret generic insight-run-stream-nats-tls \
    --from-file=ca.pem="$scratch/tls/server.pem" >/dev/null
  cat >"$scratch/nats-k8s.yaml" <<EOF
apiVersion: v1
kind: Service
metadata:
  name: nats
spec:
  selector:
    app: run-stream-nats
  ports:
    - {name: client, port: 4222, targetPort: 4222}
    - {name: monitor, port: 8222, targetPort: 8222}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nats
spec:
  replicas: 1
  selector:
    matchLabels: {app: run-stream-nats}
  template:
    metadata:
      labels: {app: run-stream-nats}
    spec:
      containers:
        - name: nats
          image: $nats_image
          args: ["-c", "/etc/nats/nats-server.conf"]
          ports:
            - {name: client, containerPort: 4222}
            - {name: monitor, containerPort: 8222}
          readinessProbe:
            tcpSocket: {port: client}
            periodSeconds: 1
          volumeMounts:
            - {name: material, mountPath: /etc/nats, readOnly: true}
      volumes:
        - name: material
          secret: {secretName: nats-server-material, defaultMode: 0440}
EOF
  kubectl -n "$namespace" apply -f "$scratch/nats-k8s.yaml" >/dev/null
  kubectl -n "$namespace" rollout status deployment/nats --timeout=2m
  values+=(
    --values "$chart/values-nats-core-qualification.yaml"
    --set-string "runtime.runStream.natsCore.servers[0]=tls://$nats_host:4222"
    --set-string runtime.runStream.natsCore.tls.clientCertificateKey=
    --set-string runtime.runStream.natsCore.tls.clientPrivateKeyKey=
  )
fi

helm upgrade --install "$release" "$chart" \
  --namespace "$namespace" \
  "${values[@]}" \
  --wait --timeout 10m

runtime_service="$release-insight-agent-platform"
postgres_service="$runtime_service-postgresql"
port_base=$((46000 + ($$ % 400) * 3))
runtime_port=$port_base
postgres_port=$((port_base + 1))
nats_monitor_port=$((port_base + 2))
kubectl -n "$namespace" port-forward "service/$runtime_service" \
  "$runtime_port:3000" >"$result_dir/runtime-port-forward.log" 2>&1 &
runtime_forward_pid=$!
kubectl -n "$namespace" port-forward "service/$postgres_service" \
  "$postgres_port:5432" >"$result_dir/postgres-port-forward.log" 2>&1 &
postgres_forward_pid=$!
if [[ "$backend" == nats_core ]]; then
  kubectl -n "$namespace" port-forward service/nats \
    "$nats_monitor_port:8222" >"$result_dir/nats-port-forward.log" 2>&1 &
  nats_forward_pid=$!
fi

base_url="http://127.0.0.1:$runtime_port"
for _ in $(seq 1 300); do
  if curl --fail --silent "$base_url/health/ready" >/dev/null; then
    break
  fi
  sleep 0.2
done
curl --fail --silent "$base_url/health/ready" \
  >"$result_dir/readiness-before.json"

postgres_pod=$(kubectl -n "$namespace" get pod \
  -l app.kubernetes.io/component=postgresql \
  -o jsonpath='{.items[0].metadata.name}')
runtime_pod=$(kubectl -n "$namespace" get pod \
  -l app.kubernetes.io/component=runtime \
  -o jsonpath='{.items[0].metadata.name}')
(
  cd "$repo_root"
  cargo build --bin agentctl --locked
)
INSIGHT_QUALIFICATION_MANAGEMENT_TOKEN="$management_token" \
  "$repo_root/target/debug/agentctl" import \
    --server "$base_url" \
    --token-env INSIGHT_QUALIFICATION_MANAGEMENT_TOKEN \
    --agent-dir "$repo_root/agents/benchmark_wait" \
    --activate >"$result_dir/agent-import.json"
curl --fail --silent "$base_url/v1/agents" >"$result_dir/agents.json"
jq -e --arg agent benchmark_wait \
  'any(.data[]?; .id == $agent)' \
  "$result_dir/agents.json" >/dev/null
kubectl -n "$namespace" exec "$postgres_pod" -- \
  psql -U insight -d insight_agent_platform -qAt \
  -c 'SELECT pg_stat_statements_reset();' >/dev/null

(
  cd "$repo_root"
  cargo build --example run_stream_attached_qualification --locked
)
probe="$repo_root/target/debug/examples/run_stream_attached_qualification"
probe_env=(
  BASE_URL="$base_url"
  QUALIFICATION_DATABASE_URL="postgres://insight:insight-benchmark@127.0.0.1:$postgres_port/insight_agent_platform?sslmode=require"
  AGENT_ID=benchmark_wait
  ATTACHED_RUNS=50
  HOLD_SECONDS=20
  PEAK_METRICS_PATH="$result_dir/runtime-metrics-peak.prom"
)
if [[ "$backend" == nats_core ]]; then
  probe_env+=(
    NATS_MONITOR_URL="http://127.0.0.1:$nats_monitor_port"
    PEAK_NATS_VARZ_PATH="$result_dir/nats-varz-peak.json"
  )
fi
env "${probe_env[@]}" "$probe" | tee "$result_dir/attached-probe.json"
jq -e '.passed == true and .attached_runs == 50 and .terminal_success == 50 and .snapshot_hashes_verified == 50' \
  "$result_dir/attached-probe.json" >/dev/null

curl --fail --silent "$base_url/metrics" >"$result_dir/runtime-metrics-after.prom"
kubectl -n "$namespace" exec "$postgres_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT pid,application_name,state,wait_event_type,wait_event,
           left(query,160) AS query
    FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid;
  " >"$result_dir/postgres-activity-after.csv"
kubectl -n "$namespace" exec "$postgres_pod" -- \
  psql -U insight -d insight_agent_platform --csv -c "
    SELECT calls,rows,total_exec_time,left(query,240) AS query
    FROM pg_stat_statements
    WHERE query ILIKE '%pg_notify%' OR query ILIKE '%insight_live_run_stream%'
    ORDER BY calls DESC;
  " >"$result_dir/postgres-run-stream-statements.csv"
kubectl -n "$namespace" exec "$runtime_pod" -- sh -c '
  for metric in cpu.stat memory.current memory.peak memory.events pids.current; do
    echo "[$metric]"
    cat "/sys/fs/cgroup/$metric" 2>/dev/null || true
  done
' >"$result_dir/runtime-cgroup-after.txt"
kubectl -n "$namespace" logs "$runtime_pod" >"$result_dir/runtime.log"
helm get values "$release" -n "$namespace" --all >"$result_dir/helm-values.yaml"
helm get manifest "$release" -n "$namespace" >"$result_dir/helm-manifest.yaml"
kubectl version -o json >"$result_dir/kubernetes-version.json"
git -C "$repo_root" rev-parse HEAD >"$result_dir/git-commit.txt"
git -C "$repo_root" status --short >"$result_dir/git-status.txt"

if rg -n 'PostgresLiveRunStreamBroker|insight_live_run_stream_|postgres_notify' \
  "$repo_root/src" "$repo_root/crates" "$repo_root/config" \
  >"$result_dir/legacy-run-stream-hygiene.txt"; then
  printf 'legacy PostgreSQL Run Stream transport remains in production source\n' >&2
  exit 1
fi

jq -n \
  --arg completed_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg backend "$backend" \
  --arg namespace "$namespace" \
  --slurpfile probe "$result_dir/attached-probe.json" \
  '{
    passed: true,
    completed_at: $completed_at,
    backend: $backend,
    ephemeral_namespace: $namespace,
    database_pool_max_connections: 10,
    attached_probe: $probe[0],
    legacy_run_stream_source_matches: 0,
    environment_cleaned_on_exit: true
  }' >"$result_dir/report.json"
qualification_succeeded=1
printf 'Kubernetes database regression evidence: %s\n' "$result_dir"
