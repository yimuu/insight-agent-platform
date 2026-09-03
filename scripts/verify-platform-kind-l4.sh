#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
bootstrap_output=${INSIGHT_KIND_OUTPUT_DIR:-$root/target/kind-local-insight-platform-local}
kubeconfig=${INSIGHT_KIND_KUBECONFIG:-$bootstrap_output/kubeconfig}
output=${INSIGHT_KIND_EVIDENCE_DIR:-$bootstrap_output/evidence}
kubectl_bin=${INSIGHT_KIND_KUBECTL:-kubectl}
opensandbox_forward_port=${INSIGHT_KIND_OPENSANDBOX_FORWARD_PORT:-18080}
results="$output/results.tsv"

for command_name in "$kubectl_bin" docker jq curl python3; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'required command is unavailable: %s\n' "$command_name" >&2
    exit 2
  fi
done
if [[ ! -f "$kubeconfig" || ! -f "$bootstrap_output/environment.json" ]]; then
  printf 'bootstrap output is incomplete: %s\n' "$bootstrap_output" >&2
  exit 2
fi
if [[ -e "$output" ]]; then
  printf 'evidence directory must be fresh: %s\n' "$output" >&2
  exit 2
fi
mkdir -p "$output/raw" "$output/faults"
: >"$results"
export KUBECONFIG="$kubeconfig"

pass() {
  printf '%s\tpass\t%s\n' "$1" "$2" >>"$results"
  printf 'PASS %-28s %s\n' "$1" "$2"
}

fail() {
  printf '%s\tfail\t%s\n' "$1" "$2" >>"$results"
  printf 'FAIL %s: %s\n' "$1" "$2" >&2
  exit 1
}

wait_platform_ready() {
  local attempt pending
  for attempt in $(seq 1 90); do
    pending=$(
      "$kubectl_bin" get deployments --all-namespaces -o json | jq -r '
        [.items[] | select(.metadata.namespace | startswith("platform-")) |
          select((.status.readyReplicas // 0) != (.spec.replicas // 0)) |
          "\(.metadata.namespace)/\(.metadata.name):\(.status.readyReplicas // 0)/\(.spec.replicas // 0)"] |
        join(",")
      '
    )
    if [[ -z "$pending" ]]; then
      return 0
    fi
    if (( attempt % 5 == 0 )); then
      printf 'waiting for Platform deployments: %s\n' "$pending"
    fi
    sleep 2
  done
  return 1
}

ready_endpoints() {
  "$kubectl_bin" -n "$1" get endpointslices \
    -l "kubernetes.io/service-name=$2" -o json | jq '
      [.items[].endpoints[]? | select(.conditions.ready == true)] | length
    '
}

wait_endpoint_minimum() {
  local namespace=$1 service=$2 minimum=$3 attempt observed
  for attempt in $(seq 1 60); do
    observed=$(ready_endpoints "$namespace" "$service")
    if [[ "$observed" -ge "$minimum" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

config_backup=""
registry_original_image=""
registry_container=""
node_stopped=""
port_forward_pid=""
sandbox_id=""

stop_port_forward() {
  if [[ -n "$port_forward_pid" ]]; then
    kill "$port_forward_pid" >/dev/null 2>&1 || true
    wait "$port_forward_pid" >/dev/null 2>&1 || true
    port_forward_pid=""
  fi
}

start_port_forward() {
  stop_port_forward
  : >"$output/faults/opensandbox-port-forward.log"
  "$kubectl_bin" -n platform-sandbox port-forward service/opensandbox-server \
    "$opensandbox_forward_port:8080" >"$output/faults/opensandbox-port-forward.log" 2>&1 &
  port_forward_pid=$!
  for _ in $(seq 1 60); do
    if grep -q 'Forwarding from' "$output/faults/opensandbox-port-forward.log"; then
      return 0
    fi
    if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
      sed -n '1,120p' "$output/faults/opensandbox-port-forward.log" >&2
      return 1
    fi
    sleep 0.25
  done
  return 1
}

cleanup() {
  set +e
  if [[ -n "$node_stopped" ]]; then
    docker start "$node_stopped" >/dev/null 2>&1
  fi
  if [[ -n "$config_backup" && -f "$config_backup" ]]; then
    "$kubectl_bin" apply -f "$config_backup" >/dev/null 2>&1
    "$kubectl_bin" -n platform-registry-validation rollout restart \
      deployment/insight-platform-registry-validation-worker >/dev/null 2>&1
  fi
  if [[ -n "$registry_original_image" && -n "$registry_container" ]]; then
    "$kubectl_bin" -n platform-registry-validation set image \
      deployment/insight-platform-registry-validation-worker \
      "$registry_container=$registry_original_image" >/dev/null 2>&1
  fi
  if [[ -n "$sandbox_id" && -n "$port_forward_pid" ]]; then
    curl --silent --max-time 3 -X DELETE \
      -H "OPEN-SANDBOX-API-KEY: $opensandbox_api_key" \
      "http://127.0.0.1:$opensandbox_forward_port/v1/sandboxes/$sandbox_id" >/dev/null 2>&1
  fi
  stop_port_forward
  "$kubectl_bin" delete -f "$root/deploy/kind/probes/mtls.yaml" \
    --ignore-not-found --wait=false >/dev/null 2>&1
  "$kubectl_bin" delete namespace insight-kind-netpol-probe \
    --ignore-not-found --wait=false >/dev/null 2>&1
}
trap cleanup EXIT INT TERM

collect_inventory() {
  "$kubectl_bin" version -o json >"$output/raw/kubernetes-version.json"
  "$kubectl_bin" get nodes -o json >"$output/raw/nodes.json"
  "$kubectl_bin" get customresourcedefinition batchsandboxes.sandbox.opensandbox.io -o json \
    >"$output/raw/batchsandbox-crd.json"
  "$kubectl_bin" get services --all-namespaces -o json >"$output/raw/services.json"
  "$kubectl_bin" get ingresses --all-namespaces -o json >"$output/raw/ingresses.json"
  "$kubectl_bin" get namespaces -o json >"$output/raw/namespaces.json"
  "$kubectl_bin" get serviceaccounts --all-namespaces -o json >"$output/raw/serviceaccounts.json"
  "$kubectl_bin" get roles --all-namespaces -o json >"$output/raw/roles.json"
  "$kubectl_bin" get rolebindings --all-namespaces -o json >"$output/raw/rolebindings.json"
  "$kubectl_bin" get clusterroles -o json >"$output/raw/clusterroles.json"
  "$kubectl_bin" get clusterrolebindings -o json >"$output/raw/clusterrolebindings.json"
  "$kubectl_bin" get validatingadmissionpolicies -o json >"$output/raw/validatingadmissionpolicies.json"
  "$kubectl_bin" get validatingadmissionpolicybindings -o json >"$output/raw/validatingadmissionpolicybindings.json"
  "$kubectl_bin" get deployments --all-namespaces -o json >"$output/raw/deployments.json"
  "$kubectl_bin" get daemonsets --all-namespaces -o json >"$output/raw/daemonsets.json"
  "$kubectl_bin" get networkpolicies --all-namespaces -o json >"$output/raw/networkpolicies.json"
  "$kubectl_bin" get poddisruptionbudgets --all-namespaces -o json >"$output/raw/poddisruptionbudgets.json"
  "$kubectl_bin" get horizontalpodautoscalers --all-namespaces -o json >"$output/raw/horizontalpodautoscalers.json"
}

collect_inventory
python3 "$root/scripts/check-platform-production-topology.py" \
  --version "$output/raw/kubernetes-version.json" \
  --nodes "$output/raw/nodes.json" \
  --batchsandbox-crd "$output/raw/batchsandbox-crd.json" \
  --services "$output/raw/services.json" \
  --ingresses "$output/raw/ingresses.json" \
  --service-accounts "$output/raw/serviceaccounts.json" \
  --roles "$output/raw/roles.json" \
  --role-bindings "$output/raw/rolebindings.json" \
  --cluster-roles "$output/raw/clusterroles.json" \
  --cluster-role-bindings "$output/raw/clusterrolebindings.json" \
  --validating-admission-policies "$output/raw/validatingadmissionpolicies.json" \
  --validating-admission-policy-bindings "$output/raw/validatingadmissionpolicybindings.json" \
  --output "$output/topology.json"
python3 "$root/scripts/check-platform-production-workloads.py" \
  --candidate "$bootstrap_output/generated/local-workload-candidate.json" \
  --capacity "$bootstrap_output/generated/local-workload-capacity.json" \
  --namespaces "$output/raw/namespaces.json" \
  --service-accounts "$output/raw/serviceaccounts.json" \
  --deployments "$output/raw/deployments.json" \
  --daemonsets "$output/raw/daemonsets.json" \
  --networkpolicies "$output/raw/networkpolicies.json" \
  --pdbs "$output/raw/poddisruptionbudgets.json" \
  --hpas "$output/raw/horizontalpodautoscalers.json" \
  --output "$output/workloads.json"
pass inventory "exact topology and 16-role workload closure accepted"

"$kubectl_bin" apply -f "$root/deploy/kind/probes/network-policy-base.yaml" >/dev/null
"$kubectl_bin" -n insight-kind-netpol-probe wait --for=condition=Ready pod/server pod/client --timeout=90s
network_get() {
  "$kubectl_bin" -n insight-kind-netpol-probe exec client -- \
    wget -qO- -T 2 http://server:8080
}
[[ "$(network_get)" == "ok" ]] || fail network-policy "baseline route did not connect"
"$kubectl_bin" apply -f "$root/deploy/kind/probes/network-policy-deny.yaml" >/dev/null
denied=false
for _ in $(seq 1 20); do
  if ! network_get >/dev/null 2>&1; then
    denied=true
    break
  fi
  sleep 1
done
[[ "$denied" == true ]] || fail network-policy "deny policy did not isolate cross-node traffic"
"$kubectl_bin" apply -f "$root/deploy/kind/probes/network-policy-allow.yaml" >/dev/null
allowed=false
for _ in $(seq 1 20); do
  if [[ "$(network_get 2>/dev/null || true)" == "ok" ]]; then
    allowed=true
    break
  fi
  sleep 1
done
[[ "$allowed" == true ]] || fail network-policy "label-scoped allow did not restore traffic"
pass network-policy "cross-node allow/deny/allow transitions enforced"
"$kubectl_bin" delete namespace insight-kind-netpol-probe --wait=true >/dev/null

expect_can() {
  local identity=$1 verb=$2 resource=$3 namespace=$4 observed
  observed=$("$kubectl_bin" auth can-i "$verb" "$resource" -n "$namespace" --as "$identity")
  [[ "$observed" == yes ]] || fail rbac "$identity cannot $verb $resource in $namespace"
}
expect_cannot() {
  local identity=$1 verb=$2 resource=$3 namespace=$4 observed
  observed=$("$kubectl_bin" auth can-i "$verb" "$resource" -n "$namespace" --as "$identity" || true)
  [[ "$observed" == no ]] || fail rbac "$identity can unexpectedly $verb $resource in $namespace"
}
server_identity=system:serviceaccount:platform-sandbox:opensandbox-server
controller_identity=system:serviceaccount:platform-sandbox:opensandbox-controller
workload_identity=system:serviceaccount:platform-sandbox-workloads:sandbox-workload
expect_can "$server_identity" create batchsandboxes.sandbox.opensandbox.io platform-sandbox-workloads
expect_cannot "$server_identity" create secrets platform-sandbox-workloads
expect_can "$controller_identity" patch pods platform-sandbox-workloads
expect_cannot "$controller_identity" patch nodes platform-sandbox-workloads
expect_cannot "$workload_identity" create pods platform-sandbox-workloads
pass rbac "Server, Controller, and workload ServiceAccounts match least privilege"

if "$kubectl_bin" -n platform-sandbox-workloads create secret generic kind-local-forbidden \
  --from-literal=value=forbidden --dry-run=server -o yaml \
  >"$output/faults/admission-secret.stdout" 2>"$output/faults/admission-secret.stderr"; then
  fail admission "workload namespace accepted a Secret"
fi
if "$kubectl_bin" -n platform-sandbox-workloads run kind-local-forbidden \
  --image=busybox:1.37.0 --restart=Never --dry-run=server -o yaml \
  >"$output/faults/admission-pod.stdout" 2>"$output/faults/admission-pod.stderr"; then
  fail admission "workload namespace accepted a caller-unowned Pod"
fi
pass admission "forbidden Secret and caller-unowned Pod failed closed"

"$kubectl_bin" apply -f "$root/deploy/kind/probes/mtls.yaml" >/dev/null
"$kubectl_bin" -n platform-model-worker wait --for=condition=Ready \
  pod/kind-local-mtls-probe --timeout=90s
egress_service=l4-security-insight-platform-security-egress-egress
egress_ip=$("$kubectl_bin" -n platform-egress get service "$egress_service" -o jsonpath='{.spec.clusterIP}')
set +e
"$kubectl_bin" -n platform-model-worker exec kind-local-mtls-probe -- \
  curl --silent --show-error --max-time 5 \
    --cacert /etc/insight/client-tls/ca.pem \
    --resolve "localhost:8443:$egress_ip" https://localhost:8443/ \
    -o /tmp/no-client-cert >"$output/faults/mtls-no-client-cert.stdout" \
    2>"$output/faults/mtls-no-client-cert.stderr"
without_certificate=$?
set -e
[[ "$without_certificate" -ne 0 ]] || fail mtls "Egress accepted a client without a certificate"
"$kubectl_bin" -n platform-model-worker exec kind-local-mtls-probe -- \
  curl --silent --show-error --max-time 5 \
    --cacert /etc/insight/client-tls/ca.pem \
    --cert /etc/insight/client-tls/client.pem \
    --key /etc/insight/client-tls/client-key.pem \
    --resolve "localhost:8443:$egress_ip" https://localhost:8443/ \
    -o /tmp/with-client-cert >"$output/faults/mtls-with-client-cert.stdout" \
    2>"$output/faults/mtls-with-client-cert.stderr"
pass mtls "missing client certificate rejected; approved identity completed TLS"
"$kubectl_bin" delete -f "$root/deploy/kind/probes/mtls.yaml" --wait=true >/dev/null

registry_namespace=platform-registry-validation
registry_deployment=insight-platform-registry-validation-worker
registry_configmap=insight-platform-registry-validation-worker-candidate
config_backup="$output/faults/registry-configmap-backup.json"
"$kubectl_bin" -n "$registry_namespace" get configmap "$registry_configmap" -o json | jq '
  {apiVersion,kind,metadata:{name:.metadata.name,namespace:.metadata.namespace},data}
' >"$config_backup"
"$kubectl_bin" -n "$registry_namespace" get configmap "$registry_configmap" -o json | jq '
  .data["registry-validation-worker.json"] |=
    (fromjson | .scan_interval_milliseconds += 1 | tojson)
' | "$kubectl_bin" apply -f - >/dev/null
"$kubectl_bin" -n "$registry_namespace" rollout restart deployment/"$registry_deployment" >/dev/null
if "$kubectl_bin" -n "$registry_namespace" rollout status deployment/"$registry_deployment" \
  --timeout=25s >"$output/faults/config-drift-rollout.stdout" \
  2>"$output/faults/config-drift-rollout.stderr"; then
  fail config-drift "semantically changed configuration became Ready under the old digest"
fi
"$kubectl_bin" -n "$registry_namespace" logs \
  -l app.kubernetes.io/component=registry-validation-worker --all-containers \
  --tail=80 >"$output/faults/config-drift-logs.txt" 2>&1 || true
"$kubectl_bin" apply -f "$config_backup" >/dev/null
config_backup=""
"$kubectl_bin" -n "$registry_namespace" rollout restart deployment/"$registry_deployment" >/dev/null
"$kubectl_bin" -n "$registry_namespace" rollout status deployment/"$registry_deployment" --timeout=120s
pass config-drift "semantic drift stayed unready and exact configuration recovered"

registry_container=$(
  "$kubectl_bin" -n "$registry_namespace" get deployment "$registry_deployment" \
    -o jsonpath='{.spec.template.spec.containers[0].name}'
)
registry_original_image=$(
  "$kubectl_bin" -n "$registry_namespace" get deployment "$registry_deployment" \
    -o jsonpath='{.spec.template.spec.containers[0].image}'
)
invalid_image=insight-agent-platform@sha256:0000000000000000000000000000000000000000000000000000000000000000
"$kubectl_bin" -n "$registry_namespace" set image deployment/"$registry_deployment" \
  "$registry_container=$invalid_image" >/dev/null
if "$kubectl_bin" -n "$registry_namespace" rollout status deployment/"$registry_deployment" \
  --timeout=20s >"$output/faults/image-drift-rollout.stdout" \
  2>"$output/faults/image-drift-rollout.stderr"; then
  fail image-drift "unavailable image digest completed rollout"
fi
"$kubectl_bin" -n "$registry_namespace" get pods -o json \
  >"$output/faults/image-drift-pods.json"
if ! jq -e '
  any(.items[].status.containerStatuses[]?.state.waiting.reason;
    . == "ImagePullBackOff" or . == "ErrImagePull")
' "$output/faults/image-drift-pods.json" >/dev/null; then
  fail image-drift "rollout stalled without an image pull rejection"
fi
"$kubectl_bin" -n "$registry_namespace" set image deployment/"$registry_deployment" \
  "$registry_container=$registry_original_image" >/dev/null
registry_original_image=""
registry_container=""
"$kubectl_bin" -n "$registry_namespace" rollout status deployment/"$registry_deployment" --timeout=120s
pass image-drift "unknown immutable digest stayed unready and approved digest recovered"

runtime_namespace=platform-public
runtime_deployment=l4-gateway-runtime-api
runtime_service=l4-gateway-runtime-api
"$kubectl_bin" -n "$runtime_namespace" rollout restart deployment/"$runtime_deployment" >/dev/null
for _ in $(seq 1 20); do
  endpoints=$(ready_endpoints "$runtime_namespace" "$runtime_service")
  [[ "$endpoints" -ge 1 ]] || fail rolling-restart "Runtime API Service reached zero Ready endpoints"
  if "$kubectl_bin" -n "$runtime_namespace" rollout status deployment/"$runtime_deployment" \
    --timeout=1s >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
"$kubectl_bin" -n "$runtime_namespace" rollout status deployment/"$runtime_deployment" --timeout=120s
pass rolling-restart "Runtime API retained at least one Ready endpoint"

egress_deployment=l4-security-insight-platform-security-egress-egress
egress_pod=$(
  "$kubectl_bin" -n platform-egress get pods \
    -l app.kubernetes.io/component=egress-broker -o jsonpath='{.items[0].metadata.name}'
)
"$kubectl_bin" -n platform-egress delete pod "$egress_pod" --wait=false >/dev/null
wait_endpoint_minimum platform-egress "$egress_service" 1 || \
  fail pod-fault "Egress lost every Ready endpoint after one Pod deletion"
"$kubectl_bin" -n platform-egress rollout status deployment/"$egress_deployment" --timeout=120s
wait_endpoint_minimum platform-egress "$egress_service" 2 || \
  fail pod-fault "Egress did not return to two Ready endpoints"
pass pod-fault "one Egress Pod deletion preserved service and recovered"

worker_node=$(
  "$kubectl_bin" get nodes -l topology.kubernetes.io/zone=local-a \
    -o jsonpath='{.items[0].metadata.name}'
)
docker stop "$worker_node" >/dev/null
node_stopped="$worker_node"
node_not_ready=false
for attempt in $(seq 1 24); do
  ready=$(
    "$kubectl_bin" get node "$worker_node" -o json | jq -r '
      [.status.conditions[] | select(.type == "Ready")][0].status
    '
  )
  if [[ "$ready" != True ]]; then
    node_not_ready=true
    break
  fi
  if (( attempt % 2 == 0 )); then
    printf 'waiting for stopped node to become NotReady: %ss\n' "$((attempt * 5))"
  fi
  sleep 5
done
[[ "$node_not_ready" == true ]] || fail node-fault "stopped worker never became NotReady"
wait_endpoint_minimum platform-egress "$egress_service" 1 || \
  fail node-fault "Egress lost every Ready endpoint after worker loss"
docker start "$worker_node" >/dev/null
node_stopped=""
"$kubectl_bin" wait --for=condition=Ready node/"$worker_node" --timeout=120s
wait_platform_ready || fail node-fault "Platform deployments did not recover after worker restart"
pass node-fault "worker became NotReady, service survived, and all deployments recovered"

opensandbox_api_key=$(
  "$kubectl_bin" -n platform-sandbox get secret opensandbox-api-key \
    -o jsonpath='{.data.api-key}' | base64 --decode
)
start_port_forward || fail opensandbox-recovery "could not establish Server port-forward"
curl --fail --silent --show-error --max-time 90 \
  -H "OPEN-SANDBOX-API-KEY: $opensandbox_api_key" \
  -H 'Content-Type: application/json' \
  --data-binary "@$root/deploy/kind/probes/opensandbox-smoke-request.json" \
  "http://127.0.0.1:$opensandbox_forward_port/v1/sandboxes" \
  >"$output/faults/opensandbox-create.json"
sandbox_id=$(jq -r '.id // empty' "$output/faults/opensandbox-create.json")
[[ -n "$sandbox_id" ]] || fail opensandbox-recovery "create response had no sandbox id"
"$kubectl_bin" -n platform-sandbox rollout restart \
  deployment/opensandbox-server deployment/opensandbox-controller >/dev/null
stop_port_forward
"$kubectl_bin" -n platform-sandbox rollout status deployment/opensandbox-server --timeout=120s
"$kubectl_bin" -n platform-sandbox rollout status deployment/opensandbox-controller --timeout=120s
start_port_forward || fail opensandbox-recovery "could not restore Server port-forward"
curl --fail --silent --show-error --max-time 10 \
  -H "OPEN-SANDBOX-API-KEY: $opensandbox_api_key" \
  "http://127.0.0.1:$opensandbox_forward_port/v1/sandboxes/$sandbox_id" \
  >"$output/faults/opensandbox-after-restart.json"
curl --fail --silent --show-error --max-time 10 -X DELETE \
  -H "OPEN-SANDBOX-API-KEY: $opensandbox_api_key" \
  "http://127.0.0.1:$opensandbox_forward_port/v1/sandboxes/$sandbox_id" >/dev/null
sandbox_id=""
for _ in $(seq 1 60); do
  remaining=$("$kubectl_bin" -n platform-sandbox-workloads get batchsandboxes -o json | jq '.items | length')
  [[ "$remaining" -eq 0 ]] && break
  sleep 1
done
[[ "$remaining" -eq 0 ]] || fail opensandbox-recovery "BatchSandbox was not removed"
stop_port_forward
pass opensandbox-recovery "physical object survived Server/Controller restart and deleted cleanly"

wait_platform_ready || fail final-readiness "Platform has non-Ready deployments"
non_ready=$(
  "$kubectl_bin" get pods --all-namespaces -o json | jq '
    [.items[] | select(.metadata.namespace | startswith("platform-")) |
      select(.status.phase != "Succeeded") |
      select(any(.status.containerStatuses[]?; .ready != true))] | length
  '
)
[[ "$non_ready" -eq 0 ]] || fail final-readiness "$non_ready Platform Pods are not Ready"
pass final-readiness "all Platform deployments and Pods recovered"

jq -Rn '
  [inputs | split("\t") | {check:.[0],status:.[1],detail:.[2]}]
' <"$results" >"$output/checks.json"
jq -n \
  --slurpfile environment "$bootstrap_output/environment.json" \
  --slurpfile topology "$output/topology.json" \
  --slurpfile workloads "$output/workloads.json" \
  --slurpfile checks "$output/checks.json" \
  '{schema_version:1,kind:"insight.platform/kind-local-l4-mechanics-evidence/v1",
    production:false,environment:$environment[0],topology:$topology[0],
    workloads:$workloads[0],checks:$checks[0]}' >"$output/summary.json"
trap - EXIT INT TERM
printf 'Kind local L4 dynamic matrix passed\nevidence=%s\n' "$output"
