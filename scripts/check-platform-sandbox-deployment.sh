#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart="$workspace_root/deploy/helm/insight-platform-sandbox"
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT

helm lint "$chart"
helm template sandbox "$chart" --include-crds >"$rendered"

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
attestor = by_component["attestor"]
failures << "missing Controller/Executor/attestor workload" unless controller && executor && attestor

if controller && executor && attestor
  failures << "Controller and Executor must use different namespaces" if controller.dig("metadata", "namespace") == executor.dig("metadata", "namespace")
  failures << "Executor must not use hostPID" if executor.dig("spec", "template", "spec", "hostPID")
  failures << "attestor must use hostPID" unless attestor.dig("spec", "template", "spec", "hostPID") == true
  failures << "all workloads must disable automatic API tokens" unless [controller, executor, attestor].all? { |doc| doc.dig("spec", "template", "spec", "automountServiceAccountToken") == false }

  host_paths = ->(doc) { doc.dig("spec", "template", "spec", "volumes").to_a.select { |volume| volume.key?("hostPath") } }
  failures << "Controller must have no hostPath" unless host_paths.call(controller).empty?
  executor_paths = host_paths.call(executor)
  failures << "Executor may mount only the node-local attestor socket" unless executor_paths.length == 1 && executor_paths.first["name"] == "socket"
  failures << "attestor must own exact proc/node/registry/socket host paths" unless host_paths.call(attestor).map { |volume| volume["name"] }.sort == %w[host-proc node-uid registry socket]

  images = workloads.flat_map { |doc| doc.dig("spec", "template", "spec", "containers").to_a + doc.dig("spec", "template", "spec", "initContainers").to_a }.map { |container| container["image"] }
  failures << "all Sandbox workload images must use immutable sha256 digests" unless images.all? { |image| image&.match?(/@sha256:[0-9a-f]{64}\z/) }

  services = docs.select { |doc| doc["kind"] == "Service" }
  failures << "attestor listener must not be published as a Service" if services.any? { |doc| doc.dig("spec", "selector", "app.kubernetes.io/component") == "attestor" }
end

network_policies = docs.select { |doc| doc["kind"] == "NetworkPolicy" }
failures << "expected five default-deny/role NetworkPolicies" unless network_policies.length == 5
failures << "missing fail-closed Executor ValidatingAdmissionPolicy" unless docs.count { |doc| doc["kind"] == "ValidatingAdmissionPolicy" } == 1
failures << "missing Executor ValidatingAdmissionPolicyBinding" unless docs.count { |doc| doc["kind"] == "ValidatingAdmissionPolicyBinding" } == 1

if failures.any?
  failures.each { |failure| warn "sandbox deployment: #{failure}" }
  exit 1
end
puts "Sandbox deployment contract passed (#{workloads.length} workloads, #{network_policies.length} NetworkPolicies)."
' "$rendered"
