#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cluster_name=${INSIGHT_KIND_CLUSTER_NAME:-insight-platform-local}
output=${INSIGHT_KIND_OUTPUT_DIR:-$root/target/kind-local-$cluster_name}
kubeconfig=${INSIGHT_KIND_KUBECONFIG:-$output/kubeconfig}
seed_project=${INSIGHT_KIND_SEED_PROJECT:-$output/seed-project}
download_cache=${INSIGHT_KIND_DOWNLOAD_CACHE:-$output/downloads}
kubectl_bin=${INSIGHT_KIND_KUBECTL:-kubectl}
kind_bin=${INSIGHT_KIND_BIN:-kind}
helm_bin=${INSIGHT_KIND_HELM:-helm}
insight_bin=${INSIGHT_KIND_INSIGHT_BIN:-$root/target/debug/insight}
schema_bin=${INSIGHT_KIND_SCHEMA_BIN:-$root/target/release/platform-schema}
postgres_forward_port=${INSIGHT_KIND_POSTGRES_FORWARD_PORT:-15432}
platform_image=${INSIGHT_KIND_PLATFORM_IMAGE:-insight-agent-platform:cr216-l3-runtime-v2}
platform_digest=${INSIGHT_KIND_PLATFORM_DIGEST:-sha256:c7aeb3c8010fcfa6f5e6f0ddace7622a02dde9de8572eb6d04ca695c30e8c40f}
sandbox_package_image=${INSIGHT_KIND_SANDBOX_PACKAGE_IMAGE:-insight-agent-platform:cr216-l3-package}
sandbox_package_digest=${INSIGHT_KIND_SANDBOX_PACKAGE_DIGEST:-sha256:18e9d07f90c6d7791c9bafe23b4471652c67bd8f06a84e2f116b2a14a50056da}

for command_name in "$kubectl_bin" "$kind_bin" "$helm_bin" docker ruby jq curl openssl shasum; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'required command is unavailable: %s\n' "$command_name" >&2
    exit 2
  fi
done
for path in "$insight_bin" "$schema_bin"; do
  if [[ ! -x "$path" ]]; then
    printf 'required prebuilt binary is unavailable: %s\n' "$path" >&2
    exit 2
  fi
done
if "$kind_bin" get clusters | grep -Fxq "$cluster_name"; then
  printf 'Kind cluster already exists; choose a fresh INSIGHT_KIND_CLUSTER_NAME: %s\n' "$cluster_name" >&2
  exit 2
fi
if [[ -e "$output" ]]; then
  printf 'output directory must be fresh: %s\n' "$output" >&2
  exit 2
fi

mkdir -p "$output" "$download_cache"

download() {
  local url=$1
  local path=$2
  local expected=$3
  if [[ ! -f "$path" ]]; then
    curl --fail --location --silent --show-error "$url" -o "$path"
  fi
  local observed
  observed=$(shasum -a 256 "$path" | awk '{print $1}')
  if [[ "$observed" != "$expected" ]]; then
    printf 'download digest mismatch for %s: expected %s, observed %s\n' \
      "$path" "$expected" "$observed" >&2
    exit 1
  fi
}

download \
  https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.8.1/components.yaml \
  "$download_cache/metrics-server-v0.8.1.yaml" \
  4a672c4891902573a3ff753cece5de1bf1f55dd053403dfec39df9d1636b7ff1
download \
  https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/v0.93.0/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml \
  "$download_cache/servicemonitors-v0.93.0.yaml" \
  a99047972c9dd7679ce2050b9968e8a08773492e671b3e17abd476d42aa78a32
download \
  https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/v0.93.0/example/prometheus-operator-crd/monitoring.coreos.com_prometheusrules.yaml \
  "$download_cache/prometheusrules-v0.93.0.yaml" \
  78be98554b9c8506ab2b4f4feb91c5f3a56e536a0a510f3a8e876a459970d56d

ensure_image() {
  local repository=$1
  local digest=$2
  local tag=$3
  if ! docker image inspect "$repository@$digest" >/dev/null 2>&1; then
    docker pull "$repository@$digest"
  fi
  docker tag "$repository@$digest" "$tag"
}

verify_local_image() {
  local image=$1
  local expected=$2
  local observed
  observed=$(docker image inspect "$image" --format '{{.Id}}' 2>/dev/null || true)
  if [[ "$observed" != "$expected" ]]; then
    printf 'local candidate image %s must have exact image ID %s; observed %s\n' \
      "$image" "$expected" "${observed:-missing}" >&2
    exit 1
  fi
}

verify_local_image "$platform_image" "$platform_digest"
verify_local_image "$sandbox_package_image" "$sandbox_package_digest"
ensure_image kindest/node sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0 kindest/node:v1.35.8
ensure_image docker.io/library/postgres sha256:57c72fd2a128e416c7fcc499958864df5301e940bca0a56f58fddf30ffc07777 insight-kind/postgres:57c72fd2a128
ensure_image docker.io/library/nats sha256:b83efabe3e7def1e0a4a31ec6e078999bb17c80363f881df35edc70fcb6bb927 insight-kind/nats:b83efabe3e7d
ensure_image docker.io/localstack/localstack sha256:b279c01f4cfb8f985a482e4014cabc1e2697b9d7a6c8c8db2e40f4d9f93687c7 insight-kind/localstack:b279c01f4cfb
ensure_image sandbox-registry.cn-zhangjiakou.cr.aliyuncs.com/opensandbox/server sha256:ae8dfbb277f40a39ff01ef35e5e1c10675acfe0fa9db15259b8f323e5efab778 opensandbox/server:insight-local-ae8dfbb2
ensure_image sandbox-registry.cn-zhangjiakou.cr.aliyuncs.com/opensandbox/controller sha256:a9a5f73c1785ebd955336ffa313973a35c1a1b662cb7afc4ea82d92021b3532a opensandbox/controller:insight-local-a9a5f73c
ensure_image sandbox-registry.cn-zhangjiakou.cr.aliyuncs.com/opensandbox/execd sha256:0d8f44cf4194732719aa79999d4b120c98bdab02bc61e9ad13f75f83af4c2684 opensandbox/execd:insight-local-0d8f44cf
ensure_image registry.k8s.io/metrics-server/metrics-server sha256:b2d2efaf5ac3b366ed0f839d2412a2c4279d4fc2a2a733f12c52133faed36c41 registry.k8s.io/metrics-server/metrics-server:v0.8.1
ensure_image docker.io/library/busybox sha256:9db7b59979c38555a39def84a31fb98b5296952f9e3afd4f6f11f05b07adfab0 busybox:1.37.0
ensure_image docker.io/curlimages/curl sha256:935d9100e9ba842cdb060de42472c7ca90cfe9a7c96e4dacb55e79e560b3ff40 curlimages/curl:8.17.0

if [[ ! -d "$seed_project/.insight/runtime/config" ]]; then
  "$insight_bin" init --path "$seed_project" --name kind-local-seed
  "$insight_bin" dev --path "$seed_project" --features all --from-source
  "$insight_bin" stop --path "$seed_project"
fi
seed_runtime="$seed_project/.insight/runtime"
for path in "$seed_runtime/config" "$seed_runtime/tls" "$seed_runtime/run-event-cursor-key"; do
  if [[ ! -e "$path" ]]; then
    printf 'seed runtime input is missing: %s\n' "$path" >&2
    exit 1
  fi
done

"$kind_bin" create cluster \
  --name "$cluster_name" \
  --image kindest/node:v1.35.8 \
  --config "$root/deploy/kind/cluster.yaml" \
  --kubeconfig "$kubeconfig" \
  --wait 180s
export KUBECONFIG="$kubeconfig"

worker_names=$(
  "$kubectl_bin" get nodes -o json | jq -r \
    '.items[] | select(.metadata.labels["node-role.kubernetes.io/control-plane"] == null) | .metadata.name' | sort
)
worker_count=$(printf '%s\n' "$worker_names" | awk 'NF { count += 1 } END { print count + 0 }')
if [[ "$worker_count" -ne 2 ]]; then
  printf 'expected exactly two Kind workers, observed %s\n' "$worker_count" >&2
  exit 1
fi
worker_a=$(printf '%s\n' "$worker_names" | sed -n '1p')
worker_b=$(printf '%s\n' "$worker_names" | sed -n '2p')
"$kubectl_bin" label node "$worker_a" topology.kubernetes.io/zone=local-a --overwrite
"$kubectl_bin" label node "$worker_b" topology.kubernetes.io/zone=local-b --overwrite

docker_os=$(docker version --format '{{.Server.Os}}')
docker_arch=$(docker version --format '{{.Server.Arch}}')
docker_platform="$docker_os/$docker_arch"
case "$docker_platform" in
  linux/amd64 | linux/arm64) ;;
  *)
    printf 'unsupported Kind image platform: %s\n' "$docker_platform" >&2
    exit 1
    ;;
esac

# Docker 29/OrbStack exports a partial multi-architecture OCI index when Kind calls `docker save`
# without a platform. Import one complete host-platform archive per image instead. This also keeps
# every node offline during workload startup, so registry availability cannot change the result.
image_archive="$output/kind-image.tar"
load_image_into_kind() {
  local image=$1
  local node
  printf 'loading %s for %s\n' "$image" "$docker_platform"
  docker image save --platform "$docker_platform" "$image" -o "$image_archive"
  for node in $("$kind_bin" get nodes --name "$cluster_name"); do
    docker exec --privileged -i "$node" ctr --namespace=k8s.io images import \
      --digests --snapshotter=overlayfs - <"$image_archive" >/dev/null
  done
  rm -f "$image_archive"
}

for image in \
  "$platform_image" "$sandbox_package_image" \
  insight-kind/postgres:57c72fd2a128 insight-kind/nats:b83efabe3e7d \
  insight-kind/localstack:b279c01f4cfb \
  opensandbox/server:insight-local-ae8dfbb2 \
  opensandbox/controller:insight-local-a9a5f73c \
  opensandbox/execd:insight-local-0d8f44cf \
  registry.k8s.io/metrics-server/metrics-server:v0.8.1 \
  busybox:1.37.0 curlimages/curl:8.17.0; do
  load_image_into_kind "$image"
done

"$kubectl_bin" apply --server-side -f "$download_cache/servicemonitors-v0.93.0.yaml"
"$kubectl_bin" apply --server-side -f "$download_cache/prometheusrules-v0.93.0.yaml"
"$kubectl_bin" apply -f "$download_cache/metrics-server-v0.8.1.yaml"
"$kubectl_bin" -n kube-system patch deployment metrics-server --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]'
"$kubectl_bin" -n kube-system rollout status deployment/metrics-server --timeout=180s

ensure_namespace() {
  local namespace=$1
  local workload_label=${2:-}
  local security_level=${3:-restricted}
  "$kubectl_bin" create namespace "$namespace" --dry-run=client -o yaml | "$kubectl_bin" apply -f - >/dev/null
  "$kubectl_bin" label namespace "$namespace" \
    kubernetes.io/metadata.name="$namespace" \
    pod-security.kubernetes.io/enforce="$security_level" \
    pod-security.kubernetes.io/enforce-version=latest --overwrite >/dev/null
  if [[ -n "$workload_label" ]]; then
    "$kubectl_bin" label namespace "$namespace" \
      insight.platform/workload-namespace="$workload_label" --overwrite >/dev/null
  fi
}

ensure_namespace platform-deps "" privileged
ensure_namespace platform-artifacts artifact
"$kubectl_bin" label namespace platform-artifacts insight.platform/artifact-namespace=true --overwrite >/dev/null
ensure_namespace platform-capability-native capability-native-worker
ensure_namespace platform-capability-remote capability-remote-worker
ensure_namespace platform-context-worker context-worker
ensure_namespace platform-control callback-api
ensure_namespace platform-egress egress
"$kubectl_bin" label namespace platform-egress insight.platform/egress-namespace=true --overwrite >/dev/null
ensure_namespace platform-mcp-cleanup mcp-cleanup-worker
ensure_namespace platform-mcp-host mcp-host
ensure_namespace platform-model-worker model-worker
ensure_namespace platform-orchestration-worker orchestration-worker
ensure_namespace platform-public public-api
"$kubectl_bin" label namespace platform-public insight.platform/public-gateway-namespace=true --overwrite >/dev/null
ensure_namespace platform-registry-validation registry-validation-worker
ensure_namespace platform-remote-context-worker remote-context-worker
ensure_namespace platform-sandbox sandbox-control
"$kubectl_bin" label namespace platform-sandbox insight.platform/sandbox-control-namespace=true --overwrite >/dev/null
ensure_namespace platform-sandbox-workloads "" baseline
"$kubectl_bin" label namespace platform-sandbox-workloads insight.platform/sandbox-workload-namespace=true --overwrite >/dev/null
ensure_namespace platform-security-authority security-authority
"$kubectl_bin" label namespace platform-security-authority insight.platform/security-authority-namespace=true --overwrite >/dev/null

tls_output="$output/tls"
mkdir -p "$tls_output"
issue_server_certificate() {
  local name=$1
  local common_name=$2
  local subject_alt_names=$3
  local extension="$tls_output/$name.ext"
  printf '%s\n' \
    'basicConstraints=critical,CA:FALSE' \
    'keyUsage=critical,digitalSignature,keyEncipherment' \
    'extendedKeyUsage=serverAuth' \
    "subjectAltName=$subject_alt_names" >"$extension"
  openssl ecparam -name prime256v1 -genkey -noout -out "$tls_output/$name-key.pem"
  openssl req -new -key "$tls_output/$name-key.pem" -subj "/CN=$common_name" -out "$tls_output/$name.csr"
  openssl x509 -req -in "$tls_output/$name.csr" \
    -CA "$seed_runtime/tls/ca.pem" -CAkey "$seed_runtime/tls/ca-key.pem" \
    -CAserial "$tls_output/ca.srl" -CAcreateserial -days 3650 -sha256 \
    -extfile "$extension" -out "$tls_output/$name.pem" >/dev/null 2>&1
}

issue_server_certificate nats-kind-server nats.platform-deps.svc \
  DNS:localhost,DNS:nats,DNS:nats.platform-deps,DNS:nats.platform-deps.svc,DNS:nats.platform-deps.svc.cluster.local
issue_server_certificate artifact-gateway-kind-server insight-platform-artifact-gateway.platform-artifacts.svc \
  DNS:localhost,DNS:insight-platform-artifact-gateway,DNS:insight-platform-artifact-gateway.platform-artifacts,DNS:insight-platform-artifact-gateway.platform-artifacts.svc,DNS:insight-platform-artifact-gateway.platform-artifacts.svc.cluster.local

"$kubectl_bin" -n platform-deps create secret generic nats-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" \
  --from-file=server.pem="$tls_output/nats-kind-server.pem" \
  --from-file=server-key.pem="$tls_output/nats-kind-server-key.pem" \
  --dry-run=client -o yaml | "$kubectl_bin" apply -f - >/dev/null
"$kubectl_bin" apply -f "$root/deploy/kind/dependencies.yaml"
for dependency in postgres localstack nats; do
  "$kubectl_bin" -n platform-deps rollout status "deployment/$dependency" --timeout=180s
done

kms_key_arn=$(
  "$kubectl_bin" -n platform-deps exec deployment/localstack -- \
    awslocal kms create-key --description insight-kind-local \
      --query KeyMetadata.Arn --output text
)
readiness_secret_arn=$(
  "$kubectl_bin" -n platform-deps exec deployment/localstack -- \
    awslocal secretsmanager create-secret --name insight/platform/readiness-local \
      --secret-string local-readiness --query ARN --output text
)
"$kubectl_bin" -n platform-deps exec deployment/localstack -- \
  awslocal s3api create-bucket --bucket insight-platform-artifacts >/dev/null

"$kubectl_bin" -n kube-system get configmap coredns -o json | jq \
  '.data.Corefile |= sub("    forward \\. /etc/resolv.conf"; "    rewrite name regex ^localhost\\\\.localstack\\\\.cloud(\\\\..*)?$ localstack.platform-deps.svc.cluster.local answer auto\\n    forward . /etc/resolv.conf")' | \
  "$kubectl_bin" apply -f - >/dev/null
"$kubectl_bin" -n kube-system rollout restart deployment/coredns >/dev/null
"$kubectl_bin" -n kube-system rollout status deployment/coredns --timeout=180s

port_forward_log="$output/postgres-port-forward.log"
"$kubectl_bin" -n platform-deps port-forward service/postgres "$postgres_forward_port:5432" >"$port_forward_log" 2>&1 &
port_forward_pid=$!
cleanup_port_forward() {
  kill "$port_forward_pid" >/dev/null 2>&1 || true
  wait "$port_forward_pid" >/dev/null 2>&1 || true
}
trap cleanup_port_forward EXIT INT TERM
for _ in $(seq 1 60); do
  if grep -q 'Forwarding from' "$port_forward_log"; then
    break
  fi
  if ! kill -0 "$port_forward_pid" >/dev/null 2>&1; then
    sed -n '1,120p' "$port_forward_log" >&2
    exit 1
  fi
  sleep 0.25
done
PLATFORM_DATABASE_URL="postgresql://insight:insight-local-only@127.0.0.1:$postgres_forward_port/insight" \
  "$schema_bin" provision
cleanup_port_forward
trap - EXIT INT TERM

postgres_ip=$(
  "$kubectl_bin" -n platform-deps get pod -l app=postgres -o jsonpath='{.items[0].status.podIP}'
)
nats_ip=$(
  "$kubectl_bin" -n platform-deps get pod -l app=nats -o jsonpath='{.items[0].status.podIP}'
)
localstack_ip=$(
  "$kubectl_bin" -n platform-deps get pod -l app=localstack -o jsonpath='{.items[0].status.podIP}'
)
localstack_service_ip=$(
  "$kubectl_bin" -n platform-deps get service localstack -o jsonpath='{.spec.clusterIP}'
)
kubernetes_service_ip=$(
  "$kubectl_bin" -n default get service kubernetes -o jsonpath='{.spec.clusterIP}'
)
kubernetes_endpoint_ip=$(
  "$kubectl_bin" -n default get endpoints kubernetes -o jsonpath='{.subsets[0].addresses[0].ip}'
)
kubernetes_endpoint_port=$(
  "$kubectl_bin" -n default get endpoints kubernetes -o jsonpath='{.subsets[0].ports[0].port}'
)
git_commit=$(git -C "$root" rev-parse HEAD)

deployment_digest=$(
  ruby "$root/scripts/prepare-platform-kind-local.rb" \
    --seed-runtime "$seed_runtime" \
    --output "$output/generated" \
    --git-commit "$git_commit" \
    --platform-image-digest "$platform_digest" \
    --postgres-cidr "$postgres_ip/32" \
    --nats-cidr "$nats_ip/32" \
    --localstack-pod-cidr "$localstack_ip/32" \
    --localstack-service-cidr "$localstack_service_ip/32" \
    --kubernetes-api-service-cidr "$kubernetes_service_ip/32" \
    --kubernetes-api-endpoint-cidr "$kubernetes_endpoint_ip/32" \
    --kubernetes-api-endpoint-port "$kubernetes_endpoint_port" \
    --kms-key-arn "$kms_key_arn" \
    --readiness-secret-arn "$readiness_secret_arn"
)

config_map() {
  local namespace=$1
  local name=$2
  local key=$3
  local path=$4
  "$kubectl_bin" -n "$namespace" create configmap "$name" \
    "--from-file=$key=$path" --dry-run=client -o yaml | "$kubectl_bin" apply -f - >/dev/null
}

database_secret() {
  local namespace=$1
  local name=$2
  "$kubectl_bin" -n "$namespace" create secret generic "$name" \
    --from-literal=database-url=postgresql://insight:insight-local-only@postgres.platform-deps.svc.cluster.local:5432/insight \
    --dry-run=client -o yaml | "$kubectl_bin" apply -f - >/dev/null
}

generic_secret() {
  local namespace=$1
  local name=$2
  shift 2
  "$kubectl_bin" -n "$namespace" create secret generic "$name" "$@" \
    --dry-run=client -o yaml | "$kubectl_bin" apply -f - >/dev/null
}

config_dir="$output/generated/configs"
config_map platform-orchestration-worker insight-platform-orchestration-worker-candidate orchestration-worker.json "$config_dir/orchestration-worker.json"
config_map platform-model-worker insight-platform-model-worker-candidate model-worker.json "$config_dir/model-worker.json"
config_map platform-capability-native insight-platform-capability-native-worker-candidate capability-native-worker.json "$config_dir/capability-native-worker.json"
config_map platform-capability-remote insight-platform-capability-remote-worker-candidate capability-remote-worker.json "$config_dir/capability-remote-worker.json"
config_map platform-context-worker insight-platform-context-worker-candidate context-worker.json "$config_dir/context-worker.json"
config_map platform-context-worker insight-platform-context-dataset-worker-candidate context-dataset-worker.json "$config_dir/context-dataset-worker.json"
config_map platform-context-worker insight-platform-subscription-context-worker-candidate subscription-context-worker.json "$config_dir/subscription-context-worker.json"
config_map platform-remote-context-worker insight-platform-remote-context-worker-candidate remote-context-worker.json "$config_dir/remote-context-worker.json"
config_map platform-registry-validation insight-platform-registry-validation-worker-candidate registry-validation-worker.json "$config_dir/registry-validation-worker.json"
config_map platform-public insight-platform-management-api-candidate gateway.json "$config_dir/management-gateway.json"
config_map platform-public insight-platform-runtime-api-candidate gateway.json "$config_dir/runtime-gateway.json"
config_map platform-egress insight-platform-egress-candidate egress.json "$config_dir/egress-broker.json"
config_map platform-security-authority insight-platform-security-authority-candidate authority.json "$config_dir/security-authority.json"
config_map platform-artifacts insight-platform-artifact-gateway-config artifact-gateway.json "$config_dir/artifact-gateway.json"
config_map platform-artifacts insight-platform-artifact-data-worker-config artifact-data-worker.json "$config_dir/artifact-data-worker.json"
config_map platform-artifacts insight-platform-artifact-maintenance-config artifact-maintenance.json "$config_dir/artifact-maintenance.json"
config_map platform-mcp-host insight-platform-mcp-host-candidate mcp-host.json "$config_dir/mcp-host.json"
config_map platform-mcp-host insight-platform-mcp-resource-host-candidate mcp-resource-host.json "$config_dir/mcp-resource-host.json"
config_map platform-mcp-host insight-platform-mcp-discovery-worker-candidate mcp-discovery-worker.json "$config_dir/mcp-discovery-worker.json"
config_map platform-mcp-host insight-platform-mcp-subscription-worker-candidate mcp-subscription-worker.json "$config_dir/mcp-subscription-worker.json"
config_map platform-mcp-cleanup insight-platform-mcp-cleanup-worker-candidate mcp-cleanup-worker.json "$config_dir/mcp-cleanup-worker.json"
config_map platform-control insight-platform-callback-api-candidate callback-api.json "$config_dir/callback-api.json"

while read -r namespace name; do
  database_secret "$namespace" "$name"
done <<'DATABASE_SECRETS'
platform-sandbox insight-platform-sandbox-database
platform-orchestration-worker insight-platform-orchestration-worker-database
platform-model-worker insight-platform-model-worker-database
platform-capability-native insight-platform-capability-native-worker-database
platform-capability-remote insight-platform-capability-remote-worker-database
platform-context-worker insight-platform-context-worker-database
platform-context-worker insight-platform-context-dataset-worker-database
platform-remote-context-worker insight-platform-remote-context-worker-database
platform-registry-validation insight-platform-registry-validation-worker-database
platform-public insight-platform-management-api-database
platform-public insight-platform-runtime-api-database
platform-security-authority insight-platform-security-authority-database
platform-artifacts insight-platform-artifact-gateway-database
platform-artifacts insight-platform-artifact-data-reader-database
platform-artifacts insight-platform-artifact-data-worker-database
platform-artifacts insight-platform-artifact-maintenance-database
platform-mcp-host insight-platform-mcp-resource-host-database
platform-mcp-host insight-platform-mcp-discovery-worker-database
platform-mcp-host insight-platform-mcp-subscription-worker-database
platform-mcp-cleanup insight-platform-mcp-cleanup-worker-database
platform-control insight-platform-callback-api-database
DATABASE_SECRETS

generic_secret platform-sandbox opensandbox-api-key --from-literal=api-key="$(openssl rand -hex 24)"
generic_secret platform-public insight-platform-runtime-api-run-event-cursor --from-file=cursor-key="$seed_runtime/run-event-cursor-key"
generic_secret platform-public insight-platform-runtime-api-artifact-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/gateway-client.pem" --from-file=client-key.pem="$seed_runtime/tls/gateway-client-key.pem"
generic_secret platform-orchestration-worker insight-platform-orchestration-worker-artifact-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/orchestration-client.pem" --from-file=client-key.pem="$seed_runtime/tls/orchestration-client-key.pem"
generic_secret platform-model-worker insight-platform-model-worker-egress-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/model-worker-client.pem" --from-file=client-key.pem="$seed_runtime/tls/model-worker-client-key.pem"
generic_secret platform-model-worker insight-platform-model-worker-nats-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/nats-client.pem" --from-file=client-key.pem="$seed_runtime/tls/nats-client-key.pem"
generic_secret platform-capability-remote insight-platform-capability-remote-worker-egress-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/capability-remote-client.pem" --from-file=client-key.pem="$seed_runtime/tls/capability-remote-client-key.pem"
generic_secret platform-capability-remote insight-platform-capability-remote-worker-mcp-host-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/capability-remote-client.pem" --from-file=client-key.pem="$seed_runtime/tls/capability-remote-client-key.pem"
generic_secret platform-remote-context-worker insight-platform-remote-context-worker-egress-tls \
  --from-file=ca.crt="$seed_runtime/tls/ca.pem" --from-file=tls.crt="$seed_runtime/tls/context-worker-client.pem" --from-file=tls.key="$seed_runtime/tls/context-worker-client-key.pem"
generic_secret platform-egress insight-platform-egress-mcp-state-keys --from-file=current="$seed_runtime/mcp-state-keys/current"
generic_secret platform-egress insight-platform-egress-server-tls \
  --from-file=client-ca.pem="$seed_runtime/tls/ca.pem" --from-file=server.pem="$seed_runtime/tls/egress-broker.pem" --from-file=server-key.pem="$seed_runtime/tls/egress-broker-key.pem"
generic_secret platform-egress insight-platform-egress-authority-client-tls \
  --from-file=authority-ca.pem="$seed_runtime/tls/ca.pem" --from-file=authority-client.pem="$seed_runtime/tls/egress-broker-client.pem" --from-file=authority-client-key.pem="$seed_runtime/tls/egress-broker-client-key.pem"
generic_secret platform-security-authority insight-platform-security-authority-server-tls \
  --from-file=client-ca.pem="$seed_runtime/tls/ca.pem" --from-file=server.pem="$seed_runtime/tls/security-authority.pem" --from-file=server-key.pem="$seed_runtime/tls/security-authority-key.pem"
generic_secret platform-artifacts insight-platform-artifact-gateway-server-tls \
  --from-file=client-ca.pem="$seed_runtime/tls/ca.pem" --from-file=server.pem="$tls_output/artifact-gateway-kind-server.pem" --from-file=server-key.pem="$tls_output/artifact-gateway-kind-server-key.pem"
generic_secret platform-artifacts insight-platform-artifact-data-worker-server-tls \
  --from-file=client-ca.pem="$seed_runtime/tls/ca.pem" --from-file=server.pem="$seed_runtime/tls/artifact-data.pem" --from-file=server-key.pem="$seed_runtime/tls/artifact-data-key.pem"
generic_secret platform-mcp-host insight-platform-mcp-host-server-tls \
  --from-file=client-ca.pem="$seed_runtime/tls/ca.pem" --from-file=server.pem="$seed_runtime/tls/mcp-host.pem" --from-file=server-key.pem="$seed_runtime/tls/mcp-host-key.pem"
generic_secret platform-mcp-host insight-platform-mcp-host-egress-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/mcp-host-egress-client.pem" --from-file=client-key.pem="$seed_runtime/tls/mcp-host-egress-client-key.pem"
generic_secret platform-mcp-host insight-platform-mcp-resource-host-server-tls \
  --from-file=client-ca.pem="$seed_runtime/tls/ca.pem" --from-file=server.pem="$seed_runtime/tls/mcp-resource-host.pem" --from-file=server-key.pem="$seed_runtime/tls/mcp-resource-host-key.pem"
generic_secret platform-mcp-host insight-platform-mcp-discovery-worker-client-tls \
  --from-file=client.pem="$seed_runtime/tls/mcp-discovery-client.pem" --from-file=client-key.pem="$seed_runtime/tls/mcp-discovery-client-key.pem"
generic_secret platform-mcp-host insight-platform-mcp-discovery-worker-egress-ca --from-file=ca.pem="$seed_runtime/tls/ca.pem"
generic_secret platform-mcp-host insight-platform-mcp-discovery-worker-artifact-ca --from-file=ca.pem="$seed_runtime/tls/ca.pem"
generic_secret platform-mcp-host insight-platform-mcp-subscription-worker-client-tls \
  --from-file=client.pem="$seed_runtime/tls/mcp-subscription-client.pem" --from-file=client-key.pem="$seed_runtime/tls/mcp-subscription-client-key.pem"
generic_secret platform-mcp-host insight-platform-mcp-subscription-worker-egress-ca --from-file=ca.pem="$seed_runtime/tls/ca.pem"
generic_secret platform-mcp-cleanup insight-platform-mcp-cleanup-worker-egress-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/mcp-cleanup-client.pem" --from-file=client-key.pem="$seed_runtime/tls/mcp-cleanup-client-key.pem"
generic_secret platform-control insight-platform-callback-api-oauth-state-keys --from-file=current="$seed_runtime/mcp-oauth-state-keys/current"
generic_secret platform-control insight-platform-callback-api-egress-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/callback-client.pem" --from-file=client-key.pem="$seed_runtime/tls/callback-client-key.pem"
generic_secret platform-context-worker insight-platform-context-dataset-worker-artifact-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/context-dataset-client.pem" --from-file=client-key.pem="$seed_runtime/tls/context-dataset-client-key.pem"
generic_secret platform-context-worker insight-platform-subscription-context-worker-host-client-tls \
  --from-file=ca.pem="$seed_runtime/tls/ca.pem" --from-file=client.pem="$seed_runtime/tls/context-subscription-client.pem" --from-file=client-key.pem="$seed_runtime/tls/context-subscription-client-key.pem"

"$kubectl_bin" apply -f "$root/deploy/kind/localstack-targetport-networkpolicies.yaml"

install_chart() {
  local release=$1
  local namespace=$2
  local chart=$3
  local values=$4
  "$helm_bin" upgrade --install "$release" "$root/deploy/helm/$chart" \
    --namespace "$namespace" --create-namespace --values "$output/generated/helm-values/$values.yaml" \
    --wait --timeout 5m
}

install_chart l4-security platform-egress insight-platform-security-egress security
install_chart l4-artifact platform-artifacts insight-platform-artifact artifact
install_chart l4-mcp platform-mcp-host insight-platform-mcp-host mcp
install_chart l4-gateway platform-public insight-platform-gateway gateway
install_chart l4-context-native platform-context-worker insight-platform-context-worker context
install_chart l4-orchestration platform-orchestration-worker insight-platform-orchestration-worker orchestration
install_chart l4-model platform-model-worker insight-platform-model-worker model
install_chart l4-capability-native platform-capability-native insight-platform-capability-native-worker capability-native
install_chart l4-capability-remote platform-capability-remote insight-platform-capability-remote-worker capability-remote
install_chart l4-remote-context platform-remote-context-worker insight-platform-remote-context-worker remote-context
install_chart l4-registry-validation platform-registry-validation insight-platform-registry-validation-worker registry
install_chart l4-mcp-cleanup platform-mcp-cleanup insight-platform-mcp-cleanup-worker mcp-cleanup
install_chart l4-callback platform-control insight-platform-callback-api callback
install_chart insight-sandbox platform-sandbox insight-platform-sandbox sandbox

"$kubectl_bin" get deployments --all-namespaces -o json | jq -e '
  [.items[] | select(.metadata.namespace | startswith("platform-")) |
    select((.status.readyReplicas // 0) != (.spec.replicas // 0))] | length == 0
' >/dev/null

jq --arg cluster "$cluster_name" --arg kubeconfig "$kubeconfig" \
  --arg deployment_config_digest "$deployment_digest" \
  '. + {cluster_name:$cluster,kubeconfig:$kubeconfig,deployment_config_digest:$deployment_config_digest}' \
  "$output/generated/environment.json" >"$output/environment.json"

printf 'Kind local Platform bootstrap passed\ncluster=%s\nkubeconfig=%s\noutput=%s\ndeployment_config_digest=%s\n' \
  "$cluster_name" "$kubeconfig" "$output" "$deployment_digest"
