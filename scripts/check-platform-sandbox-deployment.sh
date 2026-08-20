#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart="$root/deploy/helm/insight-platform-sandbox"
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT

command -v helm >/dev/null
command -v ruby >/dev/null

if rg -n -i 'micro.?vm|firecracker|/dev/kvm|managed.?stdio' \
  "$chart" "$root/deploy/helm/insight-platform-security-egress/values.yaml" \
  "$root/Dockerfile" "$root/crates/platform-sandbox-attestor/src/main.rs" \
  "$root/crates/platform-sandbox-controller/src/main.rs" \
  "$root/crates/platform-sandbox-executor/src/main.rs"; then
  echo "sandbox deployment: deferred backend or persistent-session surface remains" >&2
  exit 1
fi

if rg -n 'platform-sandbox-microvm' "$root/Dockerfile"; then
  echo "sandbox deployment: deferred backend is present in the release build" >&2
  exit 1
fi
if ! rg -q '^    "crates/platform-sandbox-microvm",$' "$root/Cargo.toml"; then
  echo "sandbox deployment: deferred backend is not explicitly outside the release workspace" >&2
  exit 1
fi

helm lint "$chart" >/dev/null
helm template sandbox "$chart" >"$rendered"

for mutation in \
  '--set image.digest=latest' \
  '--set controller.replicas=1' \
  '--set networkPolicy.enabled=false' \
  '--set-string executor.nodeSelector.kubernetes\.io/os=windows' \
  '--set-string executor.nodeSelector.insight\.platform\.node-restriction\.kubernetes\.io/sandbox-wasi=' \
  '--set tls.attestorSecret=insight-platform-sandbox-executor-wasi-tls'; do
  # shellcheck disable=SC2086
  if helm template sandbox "$chart" $mutation >/dev/null 2>&1; then
    echo "sandbox deployment: invalid values were accepted: $mutation" >&2
    exit 1
  fi
done

ruby - "$rendered" <<'RUBY'
require "yaml"

docs = File.read(ARGV.fetch(0)).split(/^---\s*$/).map do |source|
  YAML.safe_load(source, permitted_classes: [], aliases: true) unless source.strip.empty?
end.compact
failures = []

workloads = docs.select { |doc| %w[Deployment DaemonSet].include?(doc["kind"]) }
components = workloads.map { |doc| doc.dig("spec", "template", "metadata", "labels", "app.kubernetes.io/component") }.compact.sort
expected_components = %w[attestor controller executor-wasi]
failures << "workload composition drifted: #{components.inspect}" unless components == expected_components

workloads.each do |workload|
  pod = workload.dig("spec", "template", "spec") || {}
  component = workload.dig("spec", "template", "metadata", "labels", "app.kubernetes.io/component")
  failures << "#{component} received a Kubernetes API token" unless pod["automountServiceAccountToken"] == false
  failures << "#{component} uses host networking or host IPC" if pod["hostNetwork"] || pod["hostIPC"]
  failures << "#{component} image is not digest-pinned" unless pod.fetch("containers", []).all? { |c| c["image"]&.match?(/@sha256:[0-9a-f]{64}\z/) }
  pod.fetch("containers", []).each do |container|
    security = container["securityContext"] || {}
    failures << "#{component}/#{container['name']} permits privilege escalation" unless security["allowPrivilegeEscalation"] == false
    failures << "#{component}/#{container['name']} has writable root filesystem" unless security["readOnlyRootFilesystem"] == true
    failures << "#{component}/#{container['name']} does not drop all capabilities" unless security.dig("capabilities", "drop") == ["ALL"]
  end
end

executor = workloads.find { |doc| doc.dig("spec", "template", "metadata", "labels", "app.kubernetes.io/component") == "executor-wasi" }
attestor = workloads.find { |doc| doc.dig("spec", "template", "metadata", "labels", "app.kubernetes.io/component") == "attestor" }
executor_paths = executor.dig("spec", "template", "spec", "volumes").map { |v| v.dig("hostPath", "path") }.compact
attestor_paths = attestor.dig("spec", "template", "spec", "volumes").map { |v| v.dig("hostPath", "path") }.compact.sort
failures << "WASI Executor host authority drifted" unless executor_paths == ["/var/run/insight-sandbox-attestor"]
expected_attestor_paths = ["/proc", "/var/lib/insight-sandbox-attestor", "/var/lib/insight/node-uid", "/var/run/insight-sandbox-attestor"].sort
failures << "attestor host authority drifted" unless attestor_paths == expected_attestor_paths
failures << "only the attestor may use hostPID" unless workloads.count { |w| w.dig("spec", "template", "spec", "hostPID") == true } == 1 && attestor.dig("spec", "template", "spec", "hostPID") == true

policies = docs.select { |doc| doc["kind"] == "NetworkPolicy" }
policy_names = policies.map { |doc| [doc.dig("metadata", "namespace"), doc.dig("metadata", "name")] }
expected_policy_names = [
  ["platform-sandbox", "default-deny"], ["platform-sandbox", "controller"],
  ["platform-sandbox-exec", "default-deny"], ["platform-sandbox-exec", "executor-wasi"],
  ["platform-sandbox-exec", "attestor"]
]
failures << "NetworkPolicy closure drifted" unless policy_names.sort == expected_policy_names.sort

admission = docs.find { |doc| doc["kind"] == "ValidatingAdmissionPolicy" }
binding = docs.find { |doc| doc["kind"] == "ValidatingAdmissionPolicyBinding" }
failures << "fail-closed execution admission policy is missing" unless admission&.dig("spec", "failurePolicy") == "Fail"
failures << "execution admission binding does not deny" unless binding&.dig("spec", "validationActions") == ["Deny"]

unless failures.empty?
  warn failures.join("\n")
  exit 1
end
RUBY

echo "Sandbox Phase 1 deployment contract passed (controller, restricted WASI Executor, attestor)."
