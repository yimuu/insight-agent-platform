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
if ! rg -q 'insight-platform-observability.workspace = true' "$root/crates/platform-sandbox-controller/Cargo.toml" ||
   ! rg -q 'process_observability_router' "$root/crates/platform-sandbox-controller/src/main.rs"; then
  echo "sandbox deployment: Controller shared observability composition is missing" >&2
  exit 1
fi

helm lint "$chart" >/dev/null
helm template sandbox "$chart" >"$rendered"

for mutation in \
  '--set image.digest=latest' \
  '--set controller.replicas=1' \
  '--set controller.observabilityPort=7443' \
  '--set-json networkPolicy.monitoringPodSelector=null' \
  '--set networkPolicy.enabled=false' \
  '--set-string executor.nodeSelector.kubernetes\.io/os=windows' \
  '--set-string executor.nodeSelector.insight\.platform\.node-restriction\.kubernetes\.io/sandbox-wasi=' \
  '--set tls.attestorSecret=insight-platform-sandbox-executor-wasi-tls' \
  '--set gvisor.runtimeClassName=runc' \
  '--set namespaces.guest=platform-sandbox-exec' \
  '--set tls.gvisorExecutorSecret=insight-platform-sandbox-executor-wasi-tls' \
  '--set-string gvisor.nodeSelector.insight\.platform\.node-restriction\.kubernetes\.io/sandbox-gvisor=' \
  '--set-json networkPolicy.kubernetesApiCidrs=[]'; do
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

service_monitors = docs.select { |doc| doc["kind"] == "ServiceMonitor" }
failures << "Sandbox Controller ServiceMonitor is missing" unless service_monitors.length == 1
unless File.read(ARGV.fetch(0)).include?("path: /readyz") && File.read(ARGV.fetch(0)).include?("path: /metrics")
  failures << "Sandbox Controller HTTP readiness/metrics contract is missing"
end

workloads = docs.select { |doc| %w[Deployment DaemonSet].include?(doc["kind"]) }
components = workloads.map { |doc| doc.dig("spec", "template", "metadata", "labels", "app.kubernetes.io/component") }.compact.sort
expected_components = %w[attestor controller executor-gvisor executor-wasi]
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
gvisor = workloads.find { |doc| doc.dig("spec", "template", "metadata", "labels", "app.kubernetes.io/component") == "executor-gvisor" }
attestor = workloads.find { |doc| doc.dig("spec", "template", "metadata", "labels", "app.kubernetes.io/component") == "attestor" }
executor_paths = executor.dig("spec", "template", "spec", "volumes").map { |v| v.dig("hostPath", "path") }.compact
attestor_paths = attestor.dig("spec", "template", "spec", "volumes").map { |v| v.dig("hostPath", "path") }.compact.sort
failures << "WASI Executor host authority drifted" unless executor_paths == ["/var/run/insight-sandbox-attestor"]
expected_attestor_paths = ["/proc", "/var/lib/insight-sandbox-attestor", "/var/lib/insight/node-uid", "/var/run/insight-sandbox-attestor"].sort
failures << "attestor host authority drifted" unless attestor_paths == expected_attestor_paths
failures << "only the attestor may use hostPID" unless workloads.count { |w| w.dig("spec", "template", "spec", "hostPID") == true } == 1 && attestor.dig("spec", "template", "spec", "hostPID") == true
gvisor_spec = gvisor.dig("spec", "template", "spec") || {}
gvisor_paths = gvisor_spec.fetch("volumes", []).map { |v| v.dig("hostPath", "path") }.compact
failures << "gVisor Launcher received host authority" unless gvisor_paths.empty?
failures << "gVisor Launcher process identity is not Pod-local" unless gvisor_spec["shareProcessNamespace"] == true && gvisor_spec.fetch("containers", []).map { |c| c["name"] }.sort == %w[launcher process-attestor]
failures << "gVisor Launcher is not a two-replica Deployment" unless gvisor["kind"] == "Deployment" && gvisor.dig("spec", "replicas").to_i >= 2

policies = docs.select { |doc| doc["kind"] == "NetworkPolicy" }
policy_names = policies.map { |doc| [doc.dig("metadata", "namespace"), doc.dig("metadata", "name")] }
expected_policy_names = [
  ["platform-sandbox", "default-deny"], ["platform-sandbox", "controller"],
  ["platform-sandbox-exec", "default-deny"], ["platform-sandbox-exec", "executor-wasi"],
  ["platform-sandbox-exec", "attestor"], ["platform-sandbox-exec", "executor-gvisor"],
  ["platform-sandbox-guests", "default-deny"], ["platform-sandbox-guests", "gvisor-single-job"]
]
failures << "NetworkPolicy closure drifted" unless policy_names.sort == expected_policy_names.sort

admissions = docs.select { |doc| doc["kind"] == "ValidatingAdmissionPolicy" }
bindings = docs.select { |doc| doc["kind"] == "ValidatingAdmissionPolicyBinding" }
expected_admissions = %w[executor-pods executor-role-closure gvisor-launcher gvisor-guests]
expected_admissions.each do |suffix|
  policy = admissions.find { |doc| doc.dig("metadata", "name")&.end_with?(suffix) }
  binding = bindings.find { |doc| doc.dig("metadata", "name")&.end_with?(suffix) }
  failures << "fail-closed #{suffix} admission policy is missing" unless policy&.dig("spec", "failurePolicy") == "Fail"
  failures << "#{suffix} admission binding does not deny" unless binding&.dig("spec", "validationActions") == ["Deny"]
end
guest_admission = admissions.find { |doc| doc.dig("metadata", "name")&.end_with?("gvisor-guests") }
guest_expressions = guest_admission&.dig("spec", "validations")&.map { |validation| validation["expression"] } || []
unless guest_expressions.any? { |expression| expression.include?("request.operation == 'CREATE'") && expression.include?("object.spec.schedulingGates == [{'name': 'insight.platform/await-fenced-start'}]") }
  failures << "gVisor guest CREATE does not require the exact fenced-start scheduling gate"
end
unless guest_expressions.any? { |expression| expression.include?("!has(object.spec.nodeName)") && expression.include?("object.spec.schedulerName == \"default-scheduler\"") }
  failures << "gVisor guest can bypass its exact scheduler/node selector"
end
unless guest_expressions.any? { |expression| expression.include?("env.size() == 10") && expression.include?("!has(object.spec.containers[0].envFrom)") }
  failures << "gVisor guest environment is not closed against Secret/ConfigMap injection"
end
unless guest_expressions.any? { |expression| expression.include?("volumeMounts.size() == 2") && expression.include?("/var/run/secrets/insight.platform") }
  failures << "gVisor guest volume mounts are not closed"
end

runtime_class = docs.find { |doc| doc["kind"] == "RuntimeClass" }
failures << "runsc RuntimeClass is absent or drifted" unless runtime_class&.dig("metadata", "name") == "runsc" && runtime_class["handler"] == "runsc"
launcher_role = docs.find { |doc| doc["kind"] == "Role" && doc.dig("metadata", "namespace") == "platform-sandbox-guests" }
expected_rules = [
  {"apiGroups" => [""], "resources" => ["pods"], "verbs" => %w[create get watch patch delete]},
  {"apiGroups" => [""], "resources" => ["pods/status"], "verbs" => ["get"]}
]
failures << "gVisor Launcher RBAC drifted" unless launcher_role&.dig("rules") == expected_rules

guest_account = docs.find { |doc| doc["kind"] == "ServiceAccount" && doc.dig("metadata", "namespace") == "platform-sandbox-guests" }
failures << "guest ServiceAccount is missing or receives an API token" unless guest_account&.dig("automountServiceAccountToken") == false

unless failures.empty?
  warn failures.join("\n")
  exit 1
end
RUBY

echo "Sandbox deployment contract passed (controller, WASI, gVisor Launcher/guest, and process attestors)."
