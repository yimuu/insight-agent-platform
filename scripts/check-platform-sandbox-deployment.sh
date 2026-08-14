#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart="$workspace_root/deploy/helm/insight-platform-sandbox"
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT

helm lint "$chart"
helm template sandbox "$chart" --include-crds >"$rendered"
if helm template sandbox "$chart" \
  --set microVmExecutor.workerManifest.max_concurrency=65 \
  --set microVmExecutor.provider.maximumInstances=64 >/dev/null 2>&1; then
  echo "sandbox deployment: microVM worker capacity drift was accepted" >&2
  exit 1
fi
if helm template sandbox "$chart" \
  --set microVmExecutor.managedRecovery.shardIndex=1 \
  --set microVmExecutor.managedRecovery.shardCount=1 >/dev/null 2>&1; then
  echo "sandbox deployment: invalid Managed recovery shard was accepted" >&2
  exit 1
fi

ruby -rjson -ryaml -e '
docs = YAML.load_stream(File.read(ARGV.fetch(0))).compact
failures = []

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
  executor_paths = host_paths.call(executor)
  failures << "Executor may mount only the node-local attestor socket" unless executor_paths.length == 1 && executor_paths.first["name"] == "socket"
  microvm_paths = host_paths.call(microvm)
  failures << "microVM pod must expose only the exact attestor/KVM/jail/state/cgroup host paths" unless microvm_paths.map { |volume| volume["name"] }.sort == %w[attestor-socket firecracker-jails host-cgroup kvm provider-state]
  failures << "attestor must own exact proc/node/registry/socket host paths" unless host_paths.call(attestor).map { |volume| volume["name"] }.sort == %w[host-proc node-uid registry socket]

  microvm_spec = microvm.dig("spec", "template", "spec")
  failures << "microVM Executor must use its dedicated node pool" unless microvm_spec.dig("nodeSelector", "insight.platform/sandbox-microvm-node") == "true" && !microvm_spec.fetch("nodeSelector", {}).key?("insight.platform/sandbox-node")
  failures << "microVM Executor must tolerate only an explicitly configured KVM pool" unless microvm_spec.fetch("tolerations", []).any? { |entry| entry["key"] == "insight.platform/sandbox-microvm" && entry["effect"] == "NoSchedule" }
  microvm_containers = microvm_spec.fetch("containers")
  microvm_executor = microvm_containers.find { |container| container["name"] == "executor" }
  provider = microvm_containers.find { |container| container["name"] == "provider" }
  failures << "microVM pod must contain exactly the unprivileged Executor and Provider" unless microvm_containers.length == 2 && microvm_executor && provider
  if microvm_executor && provider
    executor_mounts = microvm_executor.fetch("volumeMounts", []).map { |mount| mount["name"] }
    provider_mounts = provider.fetch("volumeMounts", []).map { |mount| mount["name"] }
    failures << "microVM Executor received Provider host authority or credentials" unless (executor_mounts & %w[kvm firecracker-jails provider-state host-cgroup provider-tls]).empty?
    failures << "microVM Provider received Executor queue/authority credentials" unless (provider_mounts & %w[executor-tls nats-tls attestor-socket]).empty?
    failures << "only Provider may own KVM and lifecycle mounts" unless %w[kvm firecracker-jails provider-state host-cgroup].all? { |name| provider_mounts.include?(name) }
    executor_security = microvm_executor.fetch("securityContext")
    provider_security = provider.fetch("securityContext")
    failures << "microVM Executor must remain unprivileged and capability-free" unless executor_security["runAsNonRoot"] == true && executor_security.dig("capabilities", "add").to_a.empty?
    expected_capabilities = %w[CHOWN DAC_OVERRIDE KILL SETGID SETUID SYS_ADMIN SYS_RESOURCE]
    failures << "microVM Provider capability set drifted" unless provider_security["privileged"] == false && provider_security.dig("capabilities", "add") == expected_capabilities
    provider_env = provider.fetch("env", []).to_h { |entry| [entry["name"], entry["value"]] }
    required_egress_env = %w[PLATFORM_SANDBOX_PROVIDER_EGRESS_CA_PATH PLATFORM_SANDBOX_PROVIDER_EGRESS_CERT_PATH PLATFORM_SANDBOX_PROVIDER_EGRESS_KEY_PATH]
    failures << "microVM Provider is missing its dedicated Egress mTLS client configuration" unless required_egress_env.all? { |name| provider_env[name]&.start_with?("/etc/insight/provider-tls/") }
  end

  provider_config_map = docs.find { |doc| doc["kind"] == "ConfigMap" && doc.dig("metadata", "name")&.end_with?("-executor-microvm-provider") }
  provider_config = provider_config_map && JSON.parse(provider_config_map.fetch("data").fetch("provider.json"))
  failures << "microVM Provider config is missing exact Egress Broker routing" unless provider_config && provider_config["egress_broker_endpoint"]&.start_with?("https://") && !provider_config["egress_broker_tls_server_name"].to_s.empty?
  executor_config_map = docs.find { |doc| doc["kind"] == "ConfigMap" && doc.dig("metadata", "name")&.end_with?("-executor-microvm") }
  executor_config = executor_config_map && JSON.parse(executor_config_map.fetch("data").fetch("executor.json"))
  recovery = executor_config&.dig("backend", "managed_recovery")
  failures << "microVM Executor config is missing bounded Managed recovery controls" unless recovery && recovery["shard_index"] == 0 && recovery["shard_count"] == 1 && recovery["scan_milliseconds"] > recovery["scan_jitter_milliseconds"] && recovery["failure_backoff_milliseconds"].between?(1, recovery["scan_milliseconds"])

  images = workloads.flat_map { |doc| doc.dig("spec", "template", "spec", "containers").to_a + doc.dig("spec", "template", "spec", "initContainers").to_a }.map { |container| container["image"] }
  failures << "all Sandbox workload images must use immutable sha256 digests" unless images.all? { |image| image&.match?(/@sha256:[0-9a-f]{64}\z/) }

  services = docs.select { |doc| doc["kind"] == "Service" }
  failures << "attestor listener must not be published as a Service" if services.any? { |doc| doc.dig("spec", "selector", "app.kubernetes.io/component") == "attestor" }
end

network_policies = docs.select { |doc| doc["kind"] == "NetworkPolicy" }
failures << "expected six default-deny/role NetworkPolicies" unless network_policies.length == 6
failures << "missing fail-closed Executor ValidatingAdmissionPolicy" unless docs.count { |doc| doc["kind"] == "ValidatingAdmissionPolicy" } == 1
failures << "missing Executor ValidatingAdmissionPolicyBinding" unless docs.count { |doc| doc["kind"] == "ValidatingAdmissionPolicyBinding" } == 1

if failures.any?
  failures.each { |failure| warn "sandbox deployment: #{failure}" }
  exit 1
end
puts "Sandbox deployment contract passed (#{workloads.length} workloads, #{network_policies.length} NetworkPolicies)."
' "$rendered"
