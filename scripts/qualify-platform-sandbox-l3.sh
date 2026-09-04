#!/usr/bin/env bash
set -euo pipefail

# Real CR-216 component qualification. The caller must install the checked-out Sandbox chart into
# a disposable Kubernetes cluster with a real NetworkPolicy-capable CNI and configure Dispatcher
# against the same freshly provisioned PostgreSQL database supplied below.

required_environment=(
  KUBECONFIG
  PLATFORM_TEST_DATABASE_URL
  PLATFORM_OPENSANDBOX_L3_API_KEY
  PLATFORM_OPENSANDBOX_L3_IMAGE
  PLATFORM_OPENSANDBOX_L3_RUNTIME_DIGEST
  PLATFORM_OPENSANDBOX_L3_PROBE_ADDRESS
  PLATFORM_OPENSANDBOX_L3_PROBE_PORT
)
for name in "${required_environment[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    printf '%s is required\n' "$name" >&2
    exit 2
  fi
done

for command_name in cargo curl kubectl; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf '%s is required\n' "$command_name" >&2
    exit 2
  fi
done

control_namespace="${PLATFORM_OPENSANDBOX_L3_CONTROL_NAMESPACE:-platform-sandbox}"
workloads_namespace="${PLATFORM_OPENSANDBOX_L3_WORKLOADS_NAMESPACE:-platform-sandbox-workloads}"
dispatcher_deployment="${PLATFORM_OPENSANDBOX_L3_DISPATCHER_DEPLOYMENT:-sandbox-dispatcher}"
server_deployment="${PLATFORM_OPENSANDBOX_L3_SERVER_DEPLOYMENT:-opensandbox-server}"
controller_deployment="${PLATFORM_OPENSANDBOX_L3_CONTROLLER_DEPLOYMENT:-opensandbox-controller}"
server_service="${PLATFORM_OPENSANDBOX_L3_SERVER_SERVICE:-opensandbox-server}"
server_port="${PLATFORM_OPENSANDBOX_L3_SERVER_PORT:-8080}"
forward_port="${PLATFORM_OPENSANDBOX_L3_FORWARD_PORT:-18081}"
original_dispatcher_replicas="$(
  kubectl get deployment "$dispatcher_deployment" -n "$control_namespace" \
    -o jsonpath='{.spec.replicas}'
)"
original_server_replicas="$(
  kubectl get deployment "$server_deployment" -n "$control_namespace" \
    -o jsonpath='{.spec.replicas}'
)"
port_forward_pid=""
port_forward_log="$(mktemp -t platform-opensandbox-l3-port-forward.XXXXXX)"

cleanup() {
  if [[ -n "$port_forward_pid" ]]; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
  fi
  kubectl scale deployment "$dispatcher_deployment" -n "$control_namespace" \
    --replicas="$original_dispatcher_replicas" >/dev/null 2>&1 || true
  kubectl scale deployment "$server_deployment" -n "$control_namespace" \
    --replicas="$original_server_replicas" >/dev/null 2>&1 || true
  rm -f "$port_forward_log"
}
trap cleanup EXIT INT TERM

start_port_forward() {
  if [[ -n "$port_forward_pid" ]]; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
  fi
  kubectl port-forward -n "$control_namespace" "service/$server_service" \
    "$forward_port:$server_port" >"$port_forward_log" 2>&1 &
  port_forward_pid="$!"
  for _ in $(seq 1 120); do
    if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
      printf 'OpenSandbox port-forward exited early\n' >&2
      sed -n '1,120p' "$port_forward_log" >&2
      exit 1
    fi
    if curl --fail --silent --show-error --max-time 1 \
      "http://127.0.0.1:$forward_port/health" >/dev/null 2>&1; then
      export PLATFORM_OPENSANDBOX_L3_URL="http://127.0.0.1:$forward_port/v1/"
      return
    fi
    sleep 0.25
  done
  printf 'OpenSandbox port-forward did not become healthy\n' >&2
  sed -n '1,120p' "$port_forward_log" >&2
  exit 1
}

run_provider_phase() {
  local phase="$1"
  local test_name="$2"
  PLATFORM_OPENSANDBOX_L3_PHASE="$phase" \
    PLATFORM_OPENSANDBOX_L3_WORKLOADS_NAMESPACE="$workloads_namespace" \
    cargo test -p insight-platform-opensandbox-client --test kubernetes_l3 \
      "$test_name" -- --exact --nocapture
}

for deployment in "$dispatcher_deployment" "$server_deployment" "$controller_deployment"; do
  kubectl rollout status "deployment/$deployment" -n "$control_namespace" --timeout=120s
done
PLATFORM_DATABASE_URL="$PLATFORM_TEST_DATABASE_URL" \
  cargo run -p insight-platform-postgres --bin platform-schema -- verify

# Keep point-read orphan cleanup from racing provider-only scenarios. Dispatcher is restored before
# the orphan and kill/reclaim proofs.
kubectl scale deployment "$dispatcher_deployment" -n "$control_namespace" --replicas=0
kubectl wait --for=delete pod -n "$control_namespace" \
  -l app.kubernetes.io/component=dispatcher --timeout=60s
start_port_forward

# A Server pod is not an allowed lifecycle caller. Its connection to the Server Service must time
# out under the installed CNI policy, while the Dispatcher path exercised below remains available.
server_cluster_ip="$(
  kubectl get service "$server_service" -n "$control_namespace" -o jsonpath='{.spec.clusterIP}'
)"
kubectl exec -n "$control_namespace" "deployment/$server_deployment" -- \
  python -c 'import socket' >/dev/null
if kubectl exec -n "$control_namespace" "deployment/$server_deployment" -- \
  python -c 'import socket,sys; socket.create_connection((sys.argv[1], int(sys.argv[2])), 2)' \
  "$server_cluster_ip" "$server_port" >/dev/null 2>&1; then
  printf 'wrong-source lifecycle connection unexpectedly succeeded\n' >&2
  exit 1
fi

run_provider_phase core opensandbox_kubernetes_l3_concurrent_response_loss_and_network_modes
run_provider_phase readiness opensandbox_kubernetes_l3_full_readiness_probe
run_provider_phase activation-boundary \
  opensandbox_kubernetes_l3_signed_activation_is_candidate_bound
run_provider_phase package-boundary \
  opensandbox_kubernetes_l3_package_cannot_cross_runner_boundary_or_survive
run_provider_phase boot-rollover \
  opensandbox_kubernetes_l3_runner_boot_changes_after_workload_pod_recreation

# Start a real long-running Sandbox, persist cancel intent, and let the local Dispatcher process
# exit. The business terminal must commit while Server is unavailable; only physical cleanup waits
# for Server recovery.
if PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE=cancel-intent \
  PLATFORM_OPENSANDBOX_L3_ABORT_AFTER_INTENT=1 \
    cargo test -p insight-platform-postgres --test phase3_opensandbox \
      opensandbox_kubernetes_l3_running_cancel_intent_survives_dispatcher_exit \
      -- --exact --nocapture; then
  printf 'cancel-intent Dispatcher fixture did not terminate abruptly\n' >&2
  exit 1
fi
kubectl scale deployment "$server_deployment" -n "$control_namespace" --replicas=0
kubectl wait --for=delete pod -n "$control_namespace" \
  -l app.kubernetes.io/component=server --timeout=60s
PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE=cancel-terminal \
  cargo test -p insight-platform-postgres --test phase3_opensandbox \
    opensandbox_kubernetes_l3_cancel_terminal_commits_while_server_is_unavailable \
    -- --exact --nocapture
kubectl scale deployment "$server_deployment" -n "$control_namespace" --replicas=1
kubectl rollout status "deployment/$server_deployment" -n "$control_namespace" --timeout=120s
start_port_forward
PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE=cancel-cleanup \
  cargo test -p insight-platform-postgres --test phase3_opensandbox \
    opensandbox_kubernetes_l3_cancel_cleanup_resumes_after_server_recovery \
    -- --exact --nocapture
PLATFORM_OPENSANDBOX_L3_CONTROL_PHASE=timeout \
  cargo test -p insight-platform-postgres --test phase3_opensandbox \
    opensandbox_kubernetes_l3_running_deadline_terminal_and_cleanup \
    -- --exact --nocapture

run_provider_phase persistent-create opensandbox_kubernetes_l3_persistent_candidate_create

kubectl rollout restart "deployment/$controller_deployment" -n "$control_namespace"
kubectl rollout status "deployment/$controller_deployment" -n "$control_namespace" --timeout=120s
kubectl rollout restart "deployment/$server_deployment" -n "$control_namespace"
kubectl rollout status "deployment/$server_deployment" -n "$control_namespace" --timeout=120s
start_port_forward
run_provider_phase persistent-recover \
  opensandbox_kubernetes_l3_persistent_candidate_recovers_after_provider_restart
run_provider_phase ttl opensandbox_kubernetes_l3_ttl_removes_candidate
run_provider_phase orphan-create opensandbox_kubernetes_l3_orphan_candidate_create

kubectl scale deployment "$dispatcher_deployment" -n "$control_namespace" --replicas=1
kubectl rollout status "deployment/$dispatcher_deployment" -n "$control_namespace" --timeout=120s
run_provider_phase orphan-verify opensandbox_kubernetes_l3_orphan_candidate_was_deleted

PLATFORM_OPENSANDBOX_L3_DISPATCHER=1 \
  cargo test -p insight-platform-postgres --test phase3_opensandbox \
    opensandbox_kubernetes_l3_dispatcher_kill_reclaims_same_started_runner \
    -- --exact --nocapture

remaining="$(
  kubectl get batchsandboxes -n "$workloads_namespace" -o name
)"
if [[ -n "$remaining" ]]; then
  printf 'BatchSandbox resources remain after L3 qualification:\n%s\n' "$remaining" >&2
  exit 1
fi
for deployment in "$dispatcher_deployment" "$server_deployment" "$controller_deployment"; do
  kubectl rollout status "deployment/$deployment" -n "$control_namespace" --timeout=120s
done
printf 'CR-216 real OpenSandbox Kubernetes L3 passed\n'
