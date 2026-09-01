#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart="$root/deploy/helm/insight-platform-sandbox"
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT

command -v helm >/dev/null
command -v ruby >/dev/null

if grep -R -n -i -E \
  'wasi|wasmtime|gvisor|runsc|attestor|sandbox-guest|platform-sandbox-controller|platform-sandbox-executor|docker\.sock|containerd\.sock' \
  "$chart" "$root/Dockerfile"; then
  echo "sandbox deployment: removed execution surface remains in active composition" >&2
  exit 1
fi

for binary in platform-sandbox-dispatcher platform-sandbox-runner; do
  if ! grep -q -- "--bin $binary" "$root/Dockerfile"; then
    echo "sandbox deployment: release build omits $binary" >&2
    exit 1
  fi
done

helm lint "$chart" >/dev/null
helm template sandbox "$chart" >"$rendered"

reject_values() {
  if helm template sandbox "$chart" "$@" >/dev/null 2>&1; then
    echo "sandbox deployment: invalid values were accepted: $*" >&2
    exit 1
  fi
}

reject_values --set-string images.server.digest=latest
reject_values --set-string runtimeContract.runnerProtocolDigest=not-a-digest
reject_values --set-string global.deploymentConfigDigest=not-a-digest
reject_values --set-string global.sourceCommit=0000000000000000000000000000000000000000
reject_values --set dispatcher.replicas=2
reject_values --set server.replicas=2
reject_values --set controller.replicas=2
reject_values --set networkPolicy.enabled=false
reject_values --set-json networkPolicy.kubernetesApiCidrs=[]
reject_values --set-json networkPolicy.postgresCidrs=[]
reject_values --set-json networkPolicy.directDeniedCidrs=[]
reject_values --set dispatcher.worker.maximumConcurrency=0
reject_values --set dispatcher.worker.criticalControlReservedSlots=0
reject_values --set dispatcher.opensandbox.requestTimeoutMilliseconds=5000
reject_values --set-string dispatcher.database.existingSecret=
reject_values --set-string sandbox.serviceAccountName=default
reject_values --set sandbox.runnerPort=8080
reject_values --set sandbox.runAsUser=0

ruby - "$root" "$chart" "$rendered" <<'RUBY'
require "digest"
require "json"
require "yaml"

root, chart, rendered_path = ARGV
rendered = File.read(rendered_path)
docs = rendered.split(/^---\s*$/).map do |source|
  YAML.safe_load(source, permitted_classes: [], aliases: true) unless source.strip.empty?
end.compact
values = YAML.safe_load(File.read(File.join(chart, "values.yaml")), permitted_classes: [], aliases: true)
failures = []

find = lambda do |kind, name, namespace = nil|
  docs.find do |doc|
    doc["kind"] == kind && doc.dig("metadata", "name") == name &&
      (namespace.nil? || doc.dig("metadata", "namespace") == namespace)
  end
end

control = values.dig("namespaces", "control")
workloads_namespace = values.dig("namespaces", "workloads")

expected_hashes = {
  "lifecycleSchemaDigest" => "vendor/opensandbox/sandbox-lifecycle.yml",
  "batchSandboxCrdDigest" => "templates/crds/batchsandboxes.yaml",
  "kubernetesProviderTemplateDigest" => "files/batchsandbox-template.yaml",
  "runnerProtocolDigest" => "vendor/runner-protocol-v1.schema.json",
  "containerRuntimeDigest" => "vendor/containerd-runc-runtime-v1.json",
  "networkPolicyDigest" => "templates/networkpolicies.yaml",
}
expected_hashes.each do |key, relative_path|
  actual = "sha256:#{Digest::SHA256.file(File.join(chart, relative_path)).hexdigest}"
  configured = values.dig("runtimeContract", key)
  failures << "runtimeContract.#{key} does not bind #{relative_path}" unless configured == actual
end

expected_images = {
  "server" => "sha256:ae8dfbb277f40a39ff01ef35e5e1c10675acfe0fa9db15259b8f323e5efab778",
  "controller" => "sha256:a9a5f73c1785ebd955336ffa313973a35c1a1b662cb7afc4ea82d92021b3532a",
  "execd" => "sha256:0d8f44cf4194732719aa79999d4b120c98bdab02bc61e9ad13f75f83af4c2684",
}
expected_images.each do |name, digest|
  failures << "official #{name} image digest drifted" unless values.dig("images", name, "digest") == digest
end
unless values.dig("global", "sourceCommit") == "c39b814f36ded4c61d5ac6f9332ee4dfbab86c00"
  failures << "OpenSandbox source commit drifted"
end

expected_crds = %w[
  batchsandboxes.sandbox.opensandbox.io
  pools.sandbox.opensandbox.io
  sandboxsnapshots.sandbox.opensandbox.io
]
actual_crds = docs.select { |doc| doc["kind"] == "CustomResourceDefinition" }
                  .map { |doc| doc.dig("metadata", "name") }.sort
failures << "source-pinned CRD set drifted: #{actual_crds.inspect}" unless actual_crds == expected_crds.sort

namespaces = docs.select { |doc| doc["kind"] == "Namespace" }
unless namespaces.map { |doc| doc.dig("metadata", "name") }.sort == [control, workloads_namespace].sort
  failures << "control/workload namespace set drifted"
end
control_namespace = find.call("Namespace", control)
workload_namespace = find.call("Namespace", workloads_namespace)
failures << "control namespace is not PSA restricted" unless control_namespace&.dig("metadata", "labels", "pod-security.kubernetes.io/enforce") == "restricted"
failures << "workload namespace is not explicitly PSA baseline" unless workload_namespace&.dig("metadata", "labels", "pod-security.kubernetes.io/enforce") == "baseline"

accounts = docs.select { |doc| doc["kind"] == "ServiceAccount" }
account_shape = accounts.map do |account|
  [account.dig("metadata", "namespace"), account.dig("metadata", "name"), account["automountServiceAccountToken"]]
end.sort_by { |entry| entry[0..1] }
expected_accounts = [
  [control, "opensandbox-controller", true],
  [control, "opensandbox-server", true],
  [control, "sandbox-dispatcher", false],
  [workloads_namespace, "sandbox-workload", false],
].sort_by { |entry| entry[0..1] }
failures << "ServiceAccount authority drifted: #{account_shape.inspect}" unless account_shape == expected_accounts

deployments = docs.select { |doc| doc["kind"] == "Deployment" }
components = deployments.map do |deployment|
  deployment.dig("spec", "template", "metadata", "labels", "app.kubernetes.io/component")
end.sort
expected_components = %w[dispatcher opensandbox-controller opensandbox-server]
failures << "deployment composition drifted: #{components.inspect}" unless components == expected_components

deployments.each do |deployment|
  component = deployment.dig("spec", "template", "metadata", "labels", "app.kubernetes.io/component")
  pod = deployment.dig("spec", "template", "spec") || {}
  failures << "#{component} is not a single replica" unless deployment.dig("spec", "replicas") == 1
  failures << "#{component} uses host networking/IPC/PID" if pod["hostNetwork"] || pod["hostIPC"] || pod["hostPID"]
  failures << "#{component} mounts a host path" if pod.fetch("volumes", []).any? { |volume| volume.key?("hostPath") }
  expected_token = component != "dispatcher"
  failures << "#{component} API-token authority drifted" unless pod["automountServiceAccountToken"] == expected_token
  pod.fetch("containers", []).each do |container|
    failures << "#{component}/#{container['name']} image is not digest-pinned" unless container["image"]&.match?(/@sha256:[0-9a-f]{64}\z/)
    security = container["securityContext"] || {}
    failures << "#{component}/#{container['name']} permits privilege escalation" unless security["allowPrivilegeEscalation"] == false
    failures << "#{component}/#{container['name']} has writable root" unless security["readOnlyRootFilesystem"] == true
    failures << "#{component}/#{container['name']} does not drop all capabilities" unless security.dig("capabilities", "drop") == ["ALL"]
  end
end

dispatcher = find.call("Deployment", "sandbox-dispatcher", control)
server = find.call("Deployment", "opensandbox-server", control)
controller = find.call("Deployment", "opensandbox-controller", control)
failures << "Dispatcher command drifted" unless dispatcher&.dig("spec", "template", "spec", "containers", 0, "command") == ["/usr/local/bin/platform-sandbox-dispatcher"]
server_image = "#{values.dig('images', 'server', 'repository')}@#{values.dig('images', 'server', 'digest')}"
controller_image = "#{values.dig('images', 'controller', 'repository')}@#{values.dig('images', 'controller', 'digest')}"
failures << "Server image drifted from official digest" unless server&.dig("spec", "template", "spec", "containers", 0, "image") == server_image
failures << "Controller image drifted from official digest" unless controller&.dig("spec", "template", "spec", "containers", 0, "image") == controller_image
failures << "Controller entrypoint drifted" unless controller&.dig("spec", "template", "spec", "containers", 0, "command") == ["/workspace/server"]

services = docs.select { |doc| doc["kind"] == "Service" }
failures << "expected three private ClusterIP services" unless services.length == 3
services.each do |service|
  spec = service["spec"] || {}
  unless spec["type"] == "ClusterIP" && !spec.key?("externalIPs") && !spec.key?("loadBalancerIP")
    failures << "#{service.dig('metadata', 'name')} is not private ClusterIP"
  end
end
forbidden_kinds = %w[DaemonSet Ingress Job PersistentVolumeClaim RuntimeClass Secret]
present_forbidden = docs.select { |doc| forbidden_kinds.include?(doc["kind"]) }.map { |doc| doc["kind"] }.uniq
failures << "forbidden active Kubernetes surfaces rendered: #{present_forbidden.inspect}" unless present_forbidden.empty?

server_role = find.call("Role", "opensandbox-server", workloads_namespace)
expected_server_rules = [
  {"apiGroups" => ["sandbox.opensandbox.io"], "resources" => ["batchsandboxes"], "verbs" => %w[create delete get list]},
  {"apiGroups" => [""], "resources" => ["pods"], "verbs" => %w[get list]},
  {"apiGroups" => [""], "resources" => ["events"], "verbs" => %w[get list]},
]
failures << "Server workload RBAC drifted" unless server_role&.dig("rules") == expected_server_rules

controller_role = docs.find do |doc|
  doc["kind"] == "ClusterRole" && doc.dig("metadata", "name")&.end_with?("-opensandbox-controller")
end
controller_rules = controller_role&.dig("rules") || []
unless controller_rules.any? { |rule| rule["resources"] == ["pods"] && rule.fetch("verbs", []).include?("create") }
  failures << "Controller cannot materialize BatchSandbox Pods"
end
controller_rules.each do |rule|
  resources = rule.fetch("resources", [])
  verbs = rule.fetch("verbs", [])
  if (resources & %w[secrets persistentvolumeclaims runtimeclasses]).any?
    failures << "Controller received forbidden credential/storage/runtime authority"
  end
  if verbs.include?("create") && (resources & %w[jobs pools sandboxsnapshots batchsandboxes]).any?
    failures << "Controller received create authority for an inactive/provider-owned surface"
  end
end

policy_names = docs.select { |doc| doc["kind"] == "NetworkPolicy" }.map do |doc|
  [doc.dig("metadata", "namespace"), doc.dig("metadata", "name")]
end.sort
expected_policy_names = [
  [control, "default-deny"], [control, "opensandbox-controller"],
  [control, "opensandbox-server"], [control, "sandbox-dispatcher"],
  [workloads_namespace, "armed-runner-direct"], [workloads_namespace, "armed-runner-disabled"],
  [workloads_namespace, "armed-runner-ingress"], [workloads_namespace, "default-deny"],
].sort
failures << "NetworkPolicy closure drifted: #{policy_names.inspect}" unless policy_names == expected_policy_names
disabled_policy = find.call("NetworkPolicy", "armed-runner-disabled", workloads_namespace)
failures << "Disabled runner has egress" unless disabled_policy&.dig("spec", "egress") == []
direct_policy = find.call("NetworkPolicy", "armed-runner-direct", workloads_namespace)
direct_selector = direct_policy&.dig("spec", "podSelector", "matchLabels")
expected_direct_selector = {"platform.insight.dev/schema" => "v1", "platform.insight.dev/network" => "direct"}
failures << "Direct policy selector drifted" unless direct_selector == expected_direct_selector
direct_ip_block = direct_policy&.dig("spec", "egress", 1, "to", 0, "ipBlock")
expected_ip_block = {"cidr" => values.dig("networkPolicy", "directExternalCidr"), "except" => values.dig("networkPolicy", "directDeniedCidrs")}
failures << "Direct policy is not external-only" unless direct_ip_block == expected_ip_block

admissions = docs.select { |doc| doc["kind"] == "ValidatingAdmissionPolicy" }
bindings = docs.select { |doc| doc["kind"] == "ValidatingAdmissionPolicyBinding" }
%w[opensandbox-inactive-surfaces opensandbox-batchsandbox opensandbox-pods].each do |suffix|
  policy = admissions.find { |doc| doc.dig("metadata", "name")&.end_with?(suffix) }
  binding = bindings.find { |doc| doc.dig("metadata", "name")&.end_with?(suffix) }
  failures << "fail-closed #{suffix} admission is missing" unless policy&.dig("spec", "failurePolicy") == "Fail"
  failures << "#{suffix} admission binding does not deny" unless binding&.dig("spec", "validationActions") == ["Deny"]
end
failures << "admission policy count drifted" unless admissions.length == 3 && bindings.length == 3
admission_text = admissions.flat_map { |doc| doc.dig("spec", "validations") || [] }
                           .map { |validation| validation["expression"] }.join("\n")
%w[sandbox-workload armed-runner-v1 execd-installer platform-sandbox-runner runtimeClassName hostPath persistentVolumeClaim].each do |term|
  failures << "admission closure omits #{term}" unless admission_text.include?(term)
end
failures << "BatchSandbox admission does not bind Server identity" unless admission_text.include?("system:serviceaccount:#{control}:opensandbox-server")
failures << "Pod admission does not bind Controller identity" unless admission_text.include?("system:serviceaccount:#{control}:opensandbox-controller")

server_config = find.call("ConfigMap", "opensandbox-server-config", control)
toml = server_config&.dig("data", "config.toml") || ""
required_toml = [
  'type = "kubernetes"', 'execd_run_as_init = true', 'allowed_host_paths = []',
  'mode = "direct"', 'informer_enabled = false', 'workload_provider = "batchsandbox"',
  'batchsandbox_template_file = "/etc/opensandbox/batchsandbox-template.yaml"',
]
required_toml.each { |line| failures << "Server config omits #{line}" unless toml.include?(line) }
%w[[secure_runtime] [agent_sandbox] [gateway] [tenants]].each do |section|
  failures << "Server config enabled forbidden #{section} surface" if toml.include?(section)
end
template = server_config&.dig("data", "batchsandbox-template.yaml") || ""
%w[sandbox-workload armed-runner-v1 automountServiceAccountToken readOnlyRootFilesystem runner-state sandbox-tmp].each do |term|
  failures << "BatchSandbox template omits #{term}" unless template.include?(term)
end

dispatcher_config = find.call("ConfigMap", "sandbox-dispatcher-config", control)
config_json = dispatcher_config&.dig("data", "config.json")
begin
  config = JSON.parse(config_json)
  runtime = config.fetch("runtime_contract")
  failures << "Dispatcher provider is not OpenSandbox Kubernetes" unless runtime["provider"] == "open_sandbox_kubernetes"
  failures << "Dispatcher worker role drifted" unless config.dig("worker_manifest", "worker_role") == "sandbox-dispatcher"
  expected_url = "http://opensandbox-server.#{control}.svc.cluster.local:8080/v1/"
  failures << "Dispatcher lifecycle URL is not private Server service" unless config.dig("opensandbox", "lifecycle_base_url") == expected_url
  expected_runtime_fields = {
    "opensandbox_server_release_digest" => values.dig("images", "server", "digest"),
    "batchsandbox_controller_digest" => values.dig("images", "controller", "digest"),
    "lifecycle_schema_digest" => values.dig("runtimeContract", "lifecycleSchemaDigest"),
    "batchsandbox_crd_digest" => values.dig("runtimeContract", "batchSandboxCrdDigest"),
    "kubernetes_provider_template_digest" => values.dig("runtimeContract", "kubernetesProviderTemplateDigest"),
    "runner_protocol_digest" => values.dig("runtimeContract", "runnerProtocolDigest"),
    "container_runtime_digest" => values.dig("runtimeContract", "containerRuntimeDigest"),
    "network_policy_digest" => values.dig("runtimeContract", "networkPolicyDigest"),
  }
  expected_runtime_fields.each do |key, expected|
    failures << "Dispatcher runtime contract #{key} drifted" unless runtime[key] == expected
  end
  computed_runtime_digest = "sha256:#{Digest::SHA256.hexdigest(JSON.generate(runtime))}"
  failures << "Dispatcher runtime contract digest is not canonical" unless config["runtime_contract_digest"] == computed_runtime_digest
  failures << "worker/runtime contract digest mismatch" unless config.dig("worker_manifest", "adapter_runtime_digest") == computed_runtime_digest
rescue JSON::ParserError, KeyError, TypeError => error
  failures << "Dispatcher config is not closed JSON: #{error.message}"
end

service_monitors = docs.select { |doc| doc["kind"] == "ServiceMonitor" }
monitor_names = service_monitors.map { |doc| doc.dig("metadata", "name") }.sort
failures << "Dispatcher/Controller monitoring composition drifted" unless monitor_names == %w[opensandbox-controller sandbox-dispatcher]
unless rendered.include?("path: /readyz") && rendered.scan("path: /metrics").length >= 2
  failures << "readiness/metrics wiring is incomplete"
end

unless failures.empty?
  warn failures.join("\n")
  exit 1
end
RUBY

echo "Sandbox deployment contract passed (Dispatcher -> OpenSandbox Server -> BatchSandbox Controller -> Docker/runc)."
