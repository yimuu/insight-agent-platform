#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart="$workspace_root/deploy/helm/insight-platform-sandbox"
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT

python3 - "$workspace_root" <<'PY'
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")


def stage_body(source, name):
    match = re.search(
        rf"^FROM [^\n]+ AS {re.escape(name)}\s*$\n(?P<body>.*?)(?=^FROM [^\n]+ AS |\Z)",
        source,
        re.MULTILINE | re.DOTALL,
    )
    return match.group("body") if match else None


def instructions(body):
    parsed = []
    pending = ""
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.endswith("\\"):
            pending += line[:-1].strip() + " "
            continue
        parsed.append(" ".join((pending + line).split()))
        pending = ""
    if pending:
        parsed.append(" ".join(pending.split()))
    return parsed


def validate(source):
    failures = []
    expected_builder_from = "FROM rust:1.94-bullseye@sha256:f4f82b80e5f2945fed4ba17af177c6d6be85d98cde38ff318fc7666ce4505617 AS builder"
    expected_runtime_from = "FROM debian:bullseye-slim@sha256:f313b4bd62667092a59b3a664d7d3ab8b5e65f41675f48e81455a15dc5abe792 AS runtime-base"
    if source.splitlines()[0] != expected_builder_from:
        failures.append("builder base must be immutable and execute on the requested target platform")
    if expected_runtime_from not in source.splitlines():
        failures.append("runtime base must use the reviewed immutable multi-architecture digest")
    if "$BUILDPLATFORM" in source or "ARG TARGETPLATFORM" in source:
        failures.append("builder may not place build-host binaries in target-platform images")
    provider_build = (
        "cargo build --locked --release -p insight-platform-sandbox-microvm "
        "--bin platform-sandbox-microvm-provider"
    )
    if provider_build not in source:
        failures.append("builder does not compile platform-sandbox-microvm-provider")

    base = stage_body(source, "runtime-base")
    executor = stage_body(source, "sandbox-microvm-executor-runtime")
    provider = stage_body(source, "sandbox-microvm-provider-runtime")
    shared = stage_body(source, "runtime")
    expected_base = [
        "RUN apt-get update && apt-get install --yes --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* && groupadd --gid 10001 insight && useradd --uid 10001 --gid insight --create-home --home-dir /app insight",
        "WORKDIR /app",
    ]
    expected_executor = [
        "COPY --from=builder /workspace/target/release/platform-sandbox-executor /usr/local/bin/platform-sandbox-executor",
        "USER 10001:10001",
        'ENTRYPOINT ["/usr/local/bin/platform-sandbox-executor"]',
    ]
    expected_provider = [
        "COPY --from=builder /workspace/target/release/platform-sandbox-microvm-provider /usr/local/bin/platform-sandbox-microvm-provider",
        "USER 0:10001",
        'ENTRYPOINT ["/usr/local/bin/platform-sandbox-microvm-provider"]',
    ]
    expected_shared = [
        "COPY --from=builder /workspace/target/release/insight-agent-platform /usr/local/bin/insight-agent-platform",
        "COPY --from=builder /workspace/target/release/platform-callback-api /usr/local/bin/platform-callback-api",
        "COPY --from=builder /workspace/target/release/platform-mcp-cleanup-worker /usr/local/bin/platform-mcp-cleanup-worker",
        "COPY --from=builder /workspace/target/release/platform-model-worker /usr/local/bin/platform-model-worker",
        "COPY --from=builder /workspace/target/release/platform-artifact-broker /usr/local/bin/platform-artifact-broker",
        "COPY --from=builder /workspace/target/release/platform-egress-broker /usr/local/bin/platform-egress-broker",
        "COPY --from=builder /workspace/target/release/platform-security-authority /usr/local/bin/platform-security-authority",
        "COPY --from=builder /workspace/target/release/platform-sandbox-controller /usr/local/bin/platform-sandbox-controller",
        "COPY --from=builder /workspace/target/release/platform-sandbox-attestor /usr/local/bin/platform-sandbox-attestor",
        "COPY --from=builder /workspace/target/release/platform-sandbox-executor /usr/local/bin/platform-sandbox-executor",
        "COPY agents /app/agents",
        "COPY config /app/config",
        "COPY database /app/database",
        "RUN mkdir -p /data/artifacts && chown -R insight:insight /app /data",
        "USER 10001:10001",
        "ENV PLATFORM_CONFIG=/app/config/platform.yaml",
        "EXPOSE 3000",
        'ENTRYPOINT ["/usr/local/bin/insight-agent-platform"]',
    ]
    if base is None:
        failures.append("missing runtime-base image target")
    elif instructions(base) != expected_base:
        failures.append("shared runtime base instruction closure drifted")
    if executor is None:
        failures.append("missing sandbox-microvm-executor-runtime image target")
    else:
        if instructions(executor) != expected_executor:
            failures.append("microVM Executor target instruction closure drifted")
    if provider is None:
        failures.append("missing sandbox-microvm-provider-runtime image target")
    else:
        if instructions(provider) != expected_provider:
            failures.append("microVM Provider target instruction closure drifted")
    if shared is None:
        failures.append("missing default runtime image target")
    elif instructions(shared) != expected_shared:
        failures.append("shared runtime target instruction closure drifted")
    return failures


failures = validate(dockerfile)
mutations = {
    "missing Provider build": dockerfile.replace(
        "    && cargo build --locked --release -p insight-platform-sandbox-microvm --bin platform-sandbox-microvm-provider\n",
        "",
        1,
    ),
    "missing Provider copy": dockerfile.replace(
        "COPY --from=builder /workspace/target/release/platform-sandbox-microvm-provider /usr/local/bin/platform-sandbox-microvm-provider\n",
        "",
        1,
    ),
    "Provider leaked into shared image": dockerfile.replace(
        "FROM runtime-base AS runtime\n",
        "FROM runtime-base AS runtime\n\nCOPY --from=builder /workspace/target/release/platform-sandbox-microvm-provider /usr/local/bin/platform-sandbox-microvm-provider\n",
        1,
    ),
    "Executor escalated after the reviewed USER": dockerfile.replace(
        'ENTRYPOINT ["/usr/local/bin/platform-sandbox-executor"]\n',
        'USER 0:0\n\nENTRYPOINT ["/bin/sh"]\n',
        1,
    ),
    "Provider binary removed after copy": dockerfile.replace(
        "USER 0:10001\n\nENTRYPOINT",
        "RUN rm /usr/local/bin/platform-sandbox-microvm-provider\n\nUSER 0:10001\n\nENTRYPOINT",
        1,
    ),
    "Provider receives the complete builder output": dockerfile.replace(
        "COPY --from=builder /workspace/target/release/platform-sandbox-microvm-provider /usr/local/bin/platform-sandbox-microvm-provider\n",
        "COPY --from=builder /workspace/target/release/ /usr/local/bin/\n",
        1,
    ),
    "shared runtime receives the complete builder output": dockerfile.replace(
        "COPY --from=builder /workspace/target/release/insight-agent-platform /usr/local/bin/insight-agent-platform\n",
        "COPY --from=builder /workspace/target/release/ /usr/local/bin/\n",
        1,
    ),
    "Provider leaked through the shared runtime base": dockerfile.replace(
        "WORKDIR /app\n\nFROM runtime-base AS sandbox-microvm-executor-runtime",
        "WORKDIR /app\n\nCOPY --from=builder /workspace/target/release/platform-sandbox-microvm-provider /usr/local/bin/platform-sandbox-microvm-provider\n\nFROM runtime-base AS sandbox-microvm-executor-runtime",
        1,
    ),
    "build-host binary copied into a target-platform image": dockerfile.replace(
        "FROM rust:1.94-bullseye@sha256:f4f82b80e5f2945fed4ba17af177c6d6be85d98cde38ff318fc7666ce4505617 AS builder",
        "FROM --platform=$BUILDPLATFORM rust:1.94-bullseye@sha256:f4f82b80e5f2945fed4ba17af177c6d6be85d98cde38ff318fc7666ce4505617 AS builder\n\nARG TARGETPLATFORM",
        1,
    ),
    "mutable runtime base": dockerfile.replace(
        "FROM debian:bullseye-slim@sha256:f313b4bd62667092a59b3a664d7d3ab8b5e65f41675f48e81455a15dc5abe792 AS runtime-base",
        "FROM debian:bullseye-slim AS runtime-base",
        1,
    ),
}
for label, mutated in mutations.items():
    if mutated == dockerfile or not validate(mutated):
        failures.append(f"Docker image contract negative fixture was not rejected: {label}")

if failures:
    raise SystemExit("\n".join(f"sandbox image contract: {failure}" for failure in failures))
print("Sandbox image target contract passed (separate microVM Executor and Provider images).")
PY

helm lint "$chart"
helm template sandbox "$chart" --include-crds >"$rendered"
if helm template sandbox "$chart" \
  --set microVmExecutor.workerManifest.max_concurrency=65 \
  --set microVmExecutor.provider.maximumInstances=64 >/dev/null 2>&1; then
  echo "sandbox deployment: microVM worker capacity drift was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.image.digest=latest >/dev/null 2>&1; then
  echo "sandbox deployment: mutable microVM Executor image was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.image.digest=latest >/dev/null 2>&1; then
  echo "sandbox deployment: mutable microVM Provider image was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set admission.schedulerName=custom-scheduler >/dev/null 2>&1; then
  echo "sandbox deployment: non-default scheduler was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set admission.schedulerUsername=system:anonymous >/dev/null 2>&1; then
  echo "sandbox deployment: anonymous binding principal was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set admission.daemonSetControllerUsername=system:anonymous >/dev/null 2>&1; then
  echo "sandbox deployment: anonymous DaemonSet controller principal was accepted" >&2
  exit 1
fi
if ! helm template sandbox "$chart" \
  --set-string 'executor.nodeSelector.kubernetes\.io/arch=arm64' \
  --set-string 'attestor.nodeSelector.kubernetes\.io/arch=arm64' \
  --set-string 'microVmExecutor.nodeSelector.kubernetes\.io/arch=arm64' >/dev/null; then
  echo "sandbox deployment: supported arm64 execution topology did not render" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set-string 'microVmExecutor.nodeSelector.kubernetes\.io/arch=ppc64le' >/dev/null 2>&1; then
  echo "sandbox deployment: unsupported microVM node architecture was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set-string 'microVmExecutor.nodeSelector.kubernetes\.io/os=windows' >/dev/null 2>&1; then
  echo "sandbox deployment: non-Linux microVM node was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set-string 'microVmExecutor.nodeSelector.insight\.platform\.node-restriction\.kubernetes\.io/sandbox-microvm=' >/dev/null 2>&1; then
  echo "sandbox deployment: missing protected microVM node label was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set-string 'executor.nodeSelector.insight\.platform\.node-restriction\.kubernetes\.io/sandbox-wasi=' >/dev/null 2>&1; then
  echo "sandbox deployment: missing NodeRestriction-protected WASI node label was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set 'attestor.tolerations=' >/dev/null 2>&1; then
  echo "sandbox deployment: attestor without KVM-node toleration was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set 'microVmExecutor.tolerations[1].key=unexpected' \
  --set 'microVmExecutor.tolerations[1].operator=Exists' >/dev/null 2>&1; then
  echo "sandbox deployment: extra microVM node toleration was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.image.repository=insight-platform-sandbox-microvm-executor >/dev/null 2>&1; then
  echo "sandbox deployment: shared microVM Executor/Provider image repository was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.image.digest=sha256:a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1 >/dev/null 2>&1; then
  echo "sandbox deployment: identical microVM Executor/Provider image digest was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.image.repository=insight-agent-platform >/dev/null 2>&1; then
  echo "sandbox deployment: shared control-plane/Provider image repository was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.hostPaths.runtimeAssets=/tmp/microvm-assets >/dev/null 2>&1; then
  echo "sandbox deployment: unreviewed microVM runtime asset host path was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.hostPaths.stateDirectory=/opt/insight/microvm-runtime-assets >/dev/null 2>&1; then
  echo "sandbox deployment: writable Provider state alias over runtime assets was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.hostPaths.chrootBaseDirectory=/ >/dev/null 2>&1; then
  echo "sandbox deployment: host root exposed as the Provider jail authority" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.hostPaths.kvm=/dev/mem >/dev/null 2>&1; then
  echo "sandbox deployment: unreviewed Provider host device was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.installation.version=1.12.1 \
  --set microVmExecutor.provider.installation.firecracker_path=/opt/insight/microvm-runtime-assets/firecracker/1.12.1/firecracker \
  --set microVmExecutor.provider.installation.jailer_path=/opt/insight/microvm-runtime-assets/firecracker/1.12.1/jailer >/dev/null 2>&1; then
  echo "sandbox deployment: Firecracker version affected by CVE-2026-1386 was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.installation.version=1.14.0 \
  --set microVmExecutor.provider.installation.firecracker_path=/opt/insight/microvm-runtime-assets/firecracker/1.14.0/firecracker \
  --set microVmExecutor.provider.installation.jailer_path=/opt/insight/microvm-runtime-assets/firecracker/1.14.0/jailer >/dev/null 2>&1; then
  echo "sandbox deployment: Firecracker 1.14.0 affected by CVE-2026-1386 was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.installation.version=1.16.2 \
  --set microVmExecutor.provider.installation.firecracker_path=/opt/insight/microvm-runtime-assets/firecracker/1.16.2/firecracker \
  --set microVmExecutor.provider.installation.jailer_path=/opt/insight/microvm-runtime-assets/firecracker/1.16.2/jailer >/dev/null 2>&1; then
  echo "sandbox deployment: unreviewed future Firecracker version was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.installation.firecracker_path=/opt/insight/microvm-runtime-assets/firecracker/1.12.1/firecracker >/dev/null 2>&1; then
  echo "sandbox deployment: Firecracker asset path did not match the declared version" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set tls.microVmProviderSecret=insight-platform-sandbox-executor-microvm-tls >/dev/null 2>&1; then
  echo "sandbox deployment: shared microVM Executor/Provider Secret was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set tls.microVmProviderSecret=insight-platform-sandbox-nats-tls >/dev/null 2>&1; then
  echo "sandbox deployment: shared Provider/NATS Secret was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.installation.firecracker_path=/tmp/firecracker >/dev/null 2>&1; then
  echo "sandbox deployment: Firecracker outside the read-only asset root was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.provider.installation.firecracker_path=/opt/insight/microvm-runtime-assets/../firecracker >/dev/null 2>&1; then
  echo "sandbox deployment: traversal in the microVM runtime asset path was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set-string 'microVmExecutor.provider.runtimes[0].rootfs_path=/tmp/rootfs.ext4' >/dev/null 2>&1; then
  echo "sandbox deployment: rootfs outside the read-only asset root was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.managedRecovery.shardIndex=1 \
  --set microVmExecutor.managedRecovery.shardCount=1 >/dev/null 2>&1; then
  echo "sandbox deployment: invalid Managed recovery shard was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set networkPolicy.artifactBrokerPodSelector= >/dev/null 2>&1; then
  echo "sandbox deployment: empty Artifact Broker selector was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set networkPolicy.artifactBrokerPodSelector.insight\\.platform/workload-role=artifact-broker-model >/dev/null 2>&1; then
  echo "sandbox deployment: Model Artifact Broker selector was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set networkPolicy.artifactBrokerPort=0 >/dev/null 2>&1; then
  echo "sandbox deployment: invalid Artifact Broker port was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set controller.serviceAccount.annotations.eks\\.amazonaws\\.com/role-arn=arn:aws:iam::111122223333:role/forbidden >/dev/null 2>&1; then
  echo "sandbox deployment: Controller workload-identity annotation was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set controller.artifactBroker.endpoint=https://insight-platform-artifact-broker-sandbox.platform-artifacts.svc:9555 >/dev/null 2>&1; then
  echo "sandbox deployment: Artifact Broker endpoint/NetworkPolicy port drift was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set controller.artifactBroker.maximumRequestBytes=1048577 >/dev/null 2>&1; then
  echo "sandbox deployment: oversized Artifact Broker request bound was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set controller.artifactBroker.maximumChunkBytes=262145 >/dev/null 2>&1; then
  echo "sandbox deployment: oversized Artifact Broker chunk bound was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set controller.artifactBroker.maximumInFlightResponses=5 >/dev/null 2>&1; then
  echo "sandbox deployment: excessive in-flight Artifact response capacity was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set tls.keys.artifactBrokerClientPrivateKey= >/dev/null 2>&1; then
  echo "sandbox deployment: empty Artifact Broker client private-key name was accepted" >&2
  exit 1
fi

ruby -rjson -ryaml -e '
docs = YAML.load_stream(File.read(ARGV.fetch(0))).compact
failures = []

container_security_projection = lambda do |container|
  keys = %w[name image command args resources securityContext volumeMounts volumeDevices env envFrom ports startupProbe readinessProbe livenessProbe lifecycle restartPolicy]
  keys.to_h { |key| [key, container[key]] }
end
pod_security_projection = lambda do |spec|
  {
    "serviceAccountName" => spec["serviceAccountName"],
    "automountServiceAccountToken" => spec["automountServiceAccountToken"],
    "hostNetwork" => spec["hostNetwork"],
    "hostPID" => spec["hostPID"],
    "hostIPC" => spec["hostIPC"],
    "shareProcessNamespace" => spec["shareProcessNamespace"],
    "nodeName" => spec["nodeName"],
    "schedulerName" => spec["schedulerName"],
    "runtimeClassName" => spec["runtimeClassName"],
    "resources" => spec["resources"],
    "resourceClaims" => spec["resourceClaims"],
    "nodeSelector" => spec["nodeSelector"],
    "securityContext" => spec["securityContext"],
    "ephemeralContainers" => spec["ephemeralContainers"],
    "volumes" => spec["volumes"],
    "initContainers" => spec.fetch("initContainers", []).map(&container_security_projection),
    "containers" => spec.fetch("containers", []).map(&container_security_projection),
  }
end

docs.select { |doc| doc["kind"] == "ConfigMap" }.each do |doc|
  doc.fetch("data").each_value { |value| JSON.parse(value) }
end

workloads = docs.select { |doc| ["Deployment", "DaemonSet"].include?(doc["kind"]) }
by_component = workloads.to_h do |doc|
  [doc.dig("spec", "template", "metadata", "labels", "app.kubernetes.io/component"), doc]
end
controller = by_component["controller"]
executor = by_component["executor-wasi"]
microvm = by_component["executor-microvm"]
attestor = by_component["attestor"]
failures << "missing Controller/WASI Executor/microVM Executor/attestor workload" unless controller && executor && microvm && attestor

if controller && executor && microvm && attestor
  failures << "Controller and Executor must use different namespaces" if controller.dig("metadata", "namespace") == executor.dig("metadata", "namespace")
  failures << "Executor must not use hostPID" if executor.dig("spec", "template", "spec", "hostPID")
  failures << "microVM Executor must not use hostPID" if microvm.dig("spec", "template", "spec", "hostPID")
  failures << "attestor must use hostPID" unless attestor.dig("spec", "template", "spec", "hostPID") == true
  failures << "all workloads must disable automatic API tokens" unless [controller, executor, microvm, attestor].all? { |doc| doc.dig("spec", "template", "spec", "automountServiceAccountToken") == false }

  host_paths = ->(doc) { doc.dig("spec", "template", "spec", "volumes").to_a.select { |volume| volume.key?("hostPath") } }
  failures << "Controller must have no hostPath" unless host_paths.call(controller).empty?
  controller_env = controller.dig("spec", "template", "spec", "containers", 0, "env").to_a.to_h { |entry| [entry["name"], entry["value"]] }
  required_artifact_env = %w[PLATFORM_SANDBOX_ARTIFACT_CA_PATH PLATFORM_SANDBOX_ARTIFACT_CERT_PATH PLATFORM_SANDBOX_ARTIFACT_KEY_PATH]
  failures << "Controller is missing Artifact Broker mTLS client configuration" unless required_artifact_env.all? { |name| controller_env[name]&.start_with?("/etc/insight/controller-tls/") }
  failures << "Controller must not receive AWS/object-store credentials" if controller_env.keys.any? { |name| name.start_with?("AWS_") || name.include?("KMS") || name.include?("S3") }
  failures << "Controller must not mount workload-identity tokens" if controller.dig("spec", "template", "spec", "volumes").to_a.any? { |volume| volume["name"] == "workload-identity" || volume.key?("projected") }
  controller_service_account = docs.find { |doc| doc["kind"] == "ServiceAccount" && doc.dig("metadata", "name") == controller.dig("spec", "template", "spec", "serviceAccountName") }
  failures << "Controller ServiceAccount must have no annotations" unless controller_service_account && controller_service_account.dig("metadata", "annotations").to_h.empty?
  executor_paths = host_paths.call(executor)
  failures << "Executor may mount only the node-local attestor socket" unless executor_paths.length == 1 && executor_paths.first["name"] == "socket"
  microvm_paths = host_paths.call(microvm)
  failures << "microVM pod must expose only the exact attestor/KVM/runtime/jail/state/cgroup host paths" unless microvm_paths.map { |volume| volume["name"] }.sort == %w[attestor-socket firecracker-jails host-cgroup kvm provider-state runtime-assets]
  runtime_asset_volume = microvm_paths.find { |volume| volume["name"] == "runtime-assets" }
  failures << "microVM runtime asset volume must fail closed on the reviewed node path" unless runtime_asset_volume&.dig("hostPath", "path") == "/opt/insight/microvm-runtime-assets" && runtime_asset_volume&.dig("hostPath", "type") == "Directory"
  failures << "attestor must own exact proc/node/registry/socket host paths" unless host_paths.call(attestor).map { |volume| volume["name"] }.sort == %w[host-proc node-uid registry socket]

  executor_spec = executor.dig("spec", "template", "spec")
  attestor_spec = attestor.dig("spec", "template", "spec")
  microvm_spec = microvm.dig("spec", "template", "spec")
  expected_arch = "amd64"
  expected_wasi_selector = {"kubernetes.io/os" => "linux", "kubernetes.io/arch" => expected_arch, "insight.platform.node-restriction.kubernetes.io/sandbox-wasi" => "true"}
  expected_attestor_selector = {"kubernetes.io/os" => "linux", "kubernetes.io/arch" => expected_arch, "insight.platform.node-restriction.kubernetes.io/sandbox-attestor" => "true"}
  expected_microvm_selector = {"kubernetes.io/os" => "linux", "kubernetes.io/arch" => expected_arch, "insight.platform.node-restriction.kubernetes.io/sandbox-microvm" => "true"}
  expected_microvm_tolerations = [{"key" => "insight.platform/sandbox-microvm", "operator" => "Equal", "value" => "true", "effect" => "NoSchedule"}]
  failures << "WASI Executor must use its exact NodeRestriction-protected pool" unless executor_spec.fetch("nodeSelector") == expected_wasi_selector && executor_spec.fetch("tolerations") == []
  failures << "attestor must cover both execution pools without widening node identity" unless attestor_spec.fetch("nodeSelector") == expected_attestor_selector && attestor_spec.fetch("tolerations") == expected_microvm_tolerations
  failures << "microVM Executor must use its exact NodeRestriction-protected pool" unless microvm_spec.fetch("nodeSelector") == expected_microvm_selector
  failures << "microVM Executor must tolerate only its exact KVM pool taint" unless microvm_spec.fetch("tolerations") == expected_microvm_tolerations
  microvm_containers = microvm_spec.fetch("containers")
  microvm_init = microvm_spec.fetch("initContainers", []).find { |container| container["name"] == "wait-for-node-attestor" }
  microvm_executor = microvm_containers.find { |container| container["name"] == "executor" }
  provider = microvm_containers.find { |container| container["name"] == "provider" }
  failures << "microVM pod must contain exactly the attestor wait, unprivileged Executor and Provider" unless microvm_spec.fetch("initContainers", []).length == 1 && microvm_init && microvm_containers.length == 2 && microvm_executor && provider
  if microvm_executor && provider
    executor_mounts = microvm_executor.fetch("volumeMounts", []).map { |mount| mount["name"] }
    provider_mount_entries = provider.fetch("volumeMounts", [])
    provider_mounts = provider_mount_entries.map { |mount| mount["name"] }
    failures << "microVM Executor received Provider host authority or credentials" unless (executor_mounts & %w[kvm runtime-assets firecracker-jails provider-state host-cgroup provider-tls]).empty?
    failures << "microVM Provider received Executor queue/authority credentials" unless (provider_mounts & %w[executor-tls nats-tls attestor-socket]).empty?
    failures << "only Provider may own KVM, runtime assets and lifecycle mounts" unless %w[kvm runtime-assets firecracker-jails provider-state host-cgroup].all? { |name| provider_mounts.include?(name) }
    runtime_assets = provider_mount_entries.find { |mount| mount["name"] == "runtime-assets" }
    failures << "Provider runtime assets must be mounted recursively read-only at the closed path" unless runtime_assets && runtime_assets["mountPath"] == "/opt/insight/microvm-runtime-assets" && runtime_assets["readOnly"] == true && runtime_assets["recursiveReadOnly"] == "Enabled"
    executor_security = microvm_executor.fetch("securityContext")
    provider_security = provider.fetch("securityContext")
    failures << "microVM Executor must remain unprivileged and capability-free" unless executor_security["runAsNonRoot"] == true && executor_security.dig("capabilities", "add").to_a.empty?
    expected_capabilities = %w[CHOWN DAC_OVERRIDE KILL SETGID SETUID SYS_ADMIN SYS_RESOURCE]
    failures << "microVM Provider capability set drifted" unless provider_security["privileged"] == false && provider_security.dig("capabilities", "add") == expected_capabilities
    provider_env = provider.fetch("env", []).to_h { |entry| [entry["name"], entry["value"]] }
    required_egress_env = %w[PLATFORM_SANDBOX_PROVIDER_EGRESS_CA_PATH PLATFORM_SANDBOX_PROVIDER_EGRESS_CERT_PATH PLATFORM_SANDBOX_PROVIDER_EGRESS_KEY_PATH]
    failures << "microVM Provider is missing its dedicated Egress mTLS client configuration" unless required_egress_env.all? { |name| provider_env[name]&.start_with?("/etc/insight/provider-tls/") }
    executor_image_digest = microvm_executor["image"]&.split("@", 2)&.last
    provider_image_digest = provider["image"]&.split("@", 2)&.last
    failures << "microVM Executor and Provider must use distinct immutable image content" unless executor_image_digest && provider_image_digest && executor_image_digest != provider_image_digest
    failures << "microVM Executor command drifted" unless microvm_executor["command"] == ["sh", "-ec", "until test -S /run/insight-sandbox-provider/provider.sock; do sleep 1; done; exec /usr/local/bin/platform-sandbox-executor"]
    failures << "microVM Provider command drifted" unless provider["command"] == ["/usr/local/bin/platform-sandbox-microvm-provider"]
    failures << "attestor wait must use the same least-authority image as the microVM Executor" unless microvm_init && microvm_init["image"] == microvm_executor["image"]
  end

  expected_microvm_projection = pod_security_projection.call(microvm_spec)
  mutation_builders = {
    "Provider binary subPath overlay" => lambda do |spec|
      spec.fetch("volumes") << {"name" => "evil", "emptyDir" => {}}
      spec.fetch("containers").find { |container| container["name"] == "provider" }.fetch("volumeMounts") << {
        "name" => "evil", "mountPath" => "/usr/local/bin/platform-sandbox-microvm-provider", "subPath" => "payload"
      }
    end,
    "Provider TLS Secret alias" => lambda do |spec|
      provider_tls = spec.fetch("volumes").find { |volume| volume["name"] == "provider-tls" }
      executor_tls = spec.fetch("volumes").find { |volume| volume["name"] == "executor-tls" }
      provider_tls.fetch("secret")["secretName"] = executor_tls.fetch("secret").fetch("secretName")
    end,
    "Provider default capabilities restored" => lambda do |spec|
      security = spec.fetch("containers").find { |container| container["name"] == "provider" }.fetch("securityContext")
      security.fetch("capabilities").delete("drop")
      security["allowPrivilegeEscalation"] = true
    end,
    "runtime assets mounted through a writable alias" => lambda do |spec|
      spec.fetch("containers").find { |container| container["name"] == "provider" }.fetch("volumeMounts") << {
        "name" => "runtime-assets", "mountPath" => "/mnt/runtime-assets", "readOnly" => false
      }
    end,
    "runtime assets descendant mount left writable" => lambda do |spec|
      mount = spec.fetch("containers").find { |container| container["name"] == "provider" }.fetch("volumeMounts").find { |entry| entry["name"] == "runtime-assets" }
      mount["recursiveReadOnly"] = "Disabled"
    end,
    "runtime class device injection" => lambda { |spec| spec["runtimeClassName"] = "device-injecting-runtime" },
    "custom scheduler placement" => lambda { |spec| spec["schedulerName"] = "unreviewed-scheduler" },
    "Pod-level resource authority" => lambda do |spec|
      spec["resources"] = {"requests" => {"cpu" => "1", "memory" => "1Gi"}, "limits" => {"cpu" => "2", "memory" => "2Gi"}}
    end,
    "Pod DRA device claim" => lambda do |spec|
      spec["resourceClaims"] = [{"name" => "kvm", "resourceClaimName" => "kvm-device"}]
    end,
    "container extended-resource device request" => lambda do |spec|
      resources = spec.fetch("containers").find { |container| container["name"] == "executor" }.fetch("resources")
      resources.fetch("limits")["devices.kubevirt.io/kvm"] = "1"
    end,
    "container DRA device claim" => lambda do |spec|
      resources = spec.fetch("containers").find { |container| container["name"] == "executor" }.fetch("resources")
      resources["claims"] = [{"name" => "kvm"}]
    end,
    "malicious init command" => lambda do |spec|
      spec.fetch("initContainers").first["command"] = ["sh", "-ec", "cp /payload/provider /shared/provider"]
    end,
    "Provider lifecycle exec" => lambda do |spec|
      spec.fetch("containers").find { |container| container["name"] == "provider" }["lifecycle"] = {
        "postStart" => {"exec" => {"command" => ["sh", "-ec", "id"]}}
      }
    end,
    "direct nodeName bypass" => lambda { |spec| spec["nodeName"] = "unreviewed-node" },
    "Pod-level MAC override" => lambda do |spec|
      spec.fetch("securityContext")["appArmorProfile"] = {"type" => "Unconfined"}
      spec.fetch("securityContext")["seLinuxOptions"] = {"type" => "spc_t"}
    end,
    "ephemeral debugger" => lambda do |spec|
      spec["ephemeralContainers"] = [{"name" => "debug", "image" => "busybox", "command" => ["sh"]}]
    end,
  }
  mutation_builders.each do |label, mutate|
    mutated = Marshal.load(Marshal.dump(microvm_spec))
    mutate.call(mutated)
    if pod_security_projection.call(mutated) == expected_microvm_projection
      failures << "Pod security-projection mutation was not detected: #{label}"
    end
  end
  microvm_metadata = microvm.dig("spec", "template", "metadata")
  expected_metadata_projection = {
    "labels" => microvm_metadata.fetch("labels"),
    "annotations" => microvm_metadata.fetch("annotations"),
  }
  mutated_metadata = Marshal.load(Marshal.dump(microvm_metadata))
  mutated_metadata.fetch("annotations")["k8s.v1.cni.cncf.io/networks"] = "privileged-secondary-network"
  mutated_metadata_projection = {
    "labels" => mutated_metadata.fetch("labels"),
    "annotations" => mutated_metadata.fetch("annotations"),
  }
  failures << "Pod metadata security-projection mutation was not detected: secondary CNI network" if mutated_metadata_projection == expected_metadata_projection

  provider_config_map = docs.find { |doc| doc["kind"] == "ConfigMap" && doc.dig("metadata", "name")&.end_with?("-executor-microvm-provider") }
  provider_config = provider_config_map && JSON.parse(provider_config_map.fetch("data").fetch("provider.json"))
  failures << "microVM Provider config is missing exact Egress Broker routing" unless provider_config && provider_config["egress_broker_endpoint"]&.start_with?("https://") && !provider_config["egress_broker_tls_server_name"].to_s.empty?
  if provider_config
    asset_root = "/opt/insight/microvm-runtime-assets/"
    installation = provider_config.fetch("installation")
    paths = [installation["firecracker_path"], installation["jailer_path"]]
    provider_config.fetch("runtimes").each do |runtime|
      paths.concat([runtime["guest_kernel_path"], runtime["rootfs_path"]])
    end
    failures << "Provider config contains a runtime path outside the read-only asset root" unless paths.all? { |path| path&.start_with?(asset_root) && !path.split("/").include?("..") }
    digests = [installation["firecracker_digest"], installation["jailer_digest"]]
    provider_config.fetch("runtimes").each do |runtime|
      digests.concat([runtime["runtime_digest"], runtime["guest_kernel_digest"], runtime["guest_agent_digest"]])
    end
    failures << "Provider config contains a non-exact runtime asset digest" unless digests.all? { |digest| digest&.match?(/\Asha256:[0-9a-f]{64}\z/) }
  end
  executor_config_map = docs.find { |doc| doc["kind"] == "ConfigMap" && doc.dig("metadata", "name")&.end_with?("-executor-microvm") }
  executor_config = executor_config_map && JSON.parse(executor_config_map.fetch("data").fetch("executor.json"))
  recovery = executor_config&.dig("backend", "managed_recovery")
  failures << "microVM Executor config is missing bounded Managed recovery controls" unless recovery && recovery["shard_index"] == 0 && recovery["shard_count"] == 1 && recovery["scan_milliseconds"] > recovery["scan_jitter_milliseconds"] && recovery["failure_backoff_milliseconds"].between?(1, recovery["scan_milliseconds"])
  controller_config_map = docs.find { |doc| doc["kind"] == "ConfigMap" && doc.dig("metadata", "name")&.end_with?("-controller") }
  controller_config = controller_config_map && JSON.parse(controller_config_map.fetch("data").fetch("controller.json"))
  broker = controller_config&.dig("artifact_broker")
  expected_broker_endpoint = broker && "https://#{broker["tls_server_name"]}:9443"
  failures << "Controller config must route Artifact reads to one bounded mTLS Broker" unless broker && broker["endpoint"] == expected_broker_endpoint && broker["maximum_request_bytes"].to_i.between?(1, 1_048_576) && broker["maximum_chunk_bytes"].to_i.between?(1, 262_144)
  failures << "Controller config must not contain an embedded Artifact provider catalog" if controller_config&.key?("artifact_provider_catalog")

  images = workloads.flat_map { |doc| doc.dig("spec", "template", "spec", "containers").to_a + doc.dig("spec", "template", "spec", "initContainers").to_a }.map { |container| container["image"] }
  failures << "all Sandbox workload images must use immutable sha256 digests" unless images.all? { |image| image&.match?(/@sha256:[0-9a-f]{64}\z/) }
  shared_image = controller.dig("spec", "template", "spec", "containers", 0, "image")
  if microvm_executor && provider
    shared_digest = shared_image&.split("@", 2)&.last
    microvm_digests = [microvm_executor["image"], provider["image"]].map { |image| image&.split("@", 2)&.last }
    failures << "microVM image content must be isolated from the shared control-plane image" if shared_digest.nil? || microvm_digests.any? { |digest| digest.nil? || digest == shared_digest }
  end

  services = docs.select { |doc| doc["kind"] == "Service" }
  failures << "attestor listener must not be published as a Service" if services.any? { |doc| doc.dig("spec", "selector", "app.kubernetes.io/component") == "attestor" }
end

network_policies = docs.select { |doc| doc["kind"] == "NetworkPolicy" }
failures << "expected six default-deny/role NetworkPolicies" unless network_policies.length == 6
admission_policies = docs.select { |doc| doc["kind"] == "ValidatingAdmissionPolicy" }
admission_bindings = docs.select { |doc| doc["kind"] == "ValidatingAdmissionPolicyBinding" }
admission_policy = admission_policies.find { |doc| doc.dig("metadata", "name")&.end_with?("-executor-pods") }
subresource_policy = admission_policies.find { |doc| doc.dig("metadata", "name")&.end_with?("-restricted-subresources") }
binding_policy = admission_policies.find { |doc| doc.dig("metadata", "name")&.end_with?("-pod-binding") }
failures << "missing fail-closed Pod, subresource, and binding ValidatingAdmissionPolicies" unless admission_policies.length == 3 && admission_policy && subresource_policy && binding_policy
failures << "missing Pod, subresource, and binding ValidatingAdmissionPolicyBindings" unless admission_bindings.length == 3
expected_binding_selector = {"kubernetes.io/metadata.name" => executor&.dig("metadata", "namespace")}
failures << "admission bindings must use the immutable executor namespace-name label" unless admission_bindings.all? { |binding| binding.dig("spec", "matchResources", "namespaceSelector", "matchLabels") == expected_binding_selector }
if admission_policy
  resources = admission_policy.dig("spec", "matchConstraints", "resourceRules").to_a.flat_map { |rule| rule.fetch("resources", []) }
  failures << "Executor admission policy does not cover ephemeral-container injection" unless resources.include?("pods/ephemeralcontainers")
  expressions = admission_policy.dig("spec", "validations").to_a.map { |validation| validation.fetch("expression", "") }.join("\n")
  required_fragments = [
    "object.metadata.annotations.all", "object.metadata.labels.all", "request.userInfo.username", "object.metadata.ownerReferences", "oldObject.metadata.ownerReferences", "r.blockOwnerDeletion", "object.spec.schedulerName", "object.spec.nodeName", "object.spec.runtimeClassName", "object.spec.resources", "object.spec.resourceClaims", "object.spec.ephemeralContainers", "object.spec.volumes.map(v, v.name)",
    "v.secret.secretName", "v.subPath", "v.subPathExpr", "v.mountPropagation", "c.volumeDevices",
    "c.securityContext.capabilities.drop == [", "c.securityContext.allowPrivilegeEscalation == false",
    "c.securityContext.readOnlyRootFilesystem", "c.resources.requests", "c.resources.limits", "c.resources.claims", "c.env.map(e, e.name)", "!has(c.envFrom)", "!has(c.lifecycle)",
    "c.startupProbe.exec.command", "c.readinessProbe.exec.command", "c.livenessProbe.exec.command",
  ]
  required_fragments.each do |fragment|
    failures << "Executor admission policy is missing closed invariant #{fragment}" unless expressions.include?(fragment)
  end
  validations_by_message = admission_policy.dig("spec", "validations").to_a.to_h { |validation| [validation.fetch("message", ""), validation.fetch("expression", "")] }
  microvm_pod_expression = validations_by_message.find { |message, _| message.start_with?("microVM Pod") }&.last.to_s
  microvm_provider_expression = validations_by_message.find { |message, _| message.start_with?("microVM Provider") }&.last.to_s
  microvm_executor_expression = validations_by_message.find { |message, _| message.start_with?("microVM Executor") }&.last.to_s
  {
    "microVM Pod" => [microvm_pod_expression, ["executor-config", "provider-config", "executor-tls", "provider-tls", "runtime-assets", "v.secret.secretName"]],
    "microVM Executor" => [microvm_executor_expression, ["c.volumeMounts.map", "v.subPath", "c.env.map", "!has(c.lifecycle)", "capabilities.drop"]],
    "microVM Provider" => [microvm_provider_expression, ["c.volumeMounts.map", "runtime-assets", "recursiveReadOnly", "c.resources.requests", "c.resources.claims", "v.subPath", "c.env.map", "!has(c.lifecycle)", "capabilities.drop"]],
  }.each do |branch, (expression, fragments)|
    failures << "missing #{branch} admission branch" if expression.empty?
    fragments.each do |fragment|
      failures << "#{branch} admission branch is missing #{fragment}" unless expression.include?(fragment)
    end
  end
end
if subresource_policy
  subresource_rules = subresource_policy.dig("spec", "matchConstraints", "resourceRules").to_a
  subresource_operations = subresource_rules.flat_map { |rule| rule.fetch("operations", []) }.sort
  subresource_resources = subresource_rules.flat_map { |rule| rule.fetch("resources", []) }.sort
  subresource_expressions = subresource_policy.dig("spec", "validations").to_a.map { |validation| validation.fetch("expression", "") }
  failures << "restricted-subresources admission policy must deny CONNECT and resize unconditionally" unless subresource_operations == %w[CONNECT UPDATE] && subresource_resources == %w[pods/attach pods/exec pods/portforward pods/resize] && subresource_expressions == ["false"]
end
if binding_policy
  binding_rules = binding_policy.dig("spec", "matchConstraints", "resourceRules").to_a
  binding_expression = binding_policy.dig("spec", "validations", 0, "expression").to_s
  failures << "Pod binding policy must cover only CREATE pods/binding" unless binding_rules.length == 1 && binding_rules.first.fetch("operations") == ["CREATE"] && binding_rules.first.fetch("resources") == ["pods/binding"]
  ["request.userInfo.username", "object.target.kind", "object.target.name", "object.metadata.annotations", "object.metadata.labels", "topology.kubernetes.io/region", "topology.kubernetes.io/zone"].each do |fragment|
    failures << "Pod binding admission policy is missing #{fragment}" unless binding_expression.include?(fragment)
  end
end

if failures.any?
  failures.each { |failure| warn "sandbox deployment: #{failure}" }
  exit 1
end
puts "Sandbox deployment contract passed (#{workloads.length} workloads, #{network_policies.length} NetworkPolicies)."
' "$rendered"
