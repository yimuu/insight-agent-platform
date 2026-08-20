#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart="$workspace_root/deploy/helm/insight-platform-artifact-broker"
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT

python3 - "$workspace_root" <<'PY'
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
rpc_manifest = (root / "crates/platform-artifact-rpc/Cargo.toml").read_text(encoding="utf-8")
service_manifest = (root / "crates/platform-artifact-service/Cargo.toml").read_text(encoding="utf-8")
broker_manifest = (root / "crates/platform-artifact-broker/Cargo.toml").read_text(encoding="utf-8")
service_source = (root / "crates/platform-artifact-service/src/main.rs").read_text(encoding="utf-8")
proto = (root / "proto/insight/platform/v1/artifact_internal.proto").read_text(encoding="utf-8")
grants = (root / "crates/platform-postgres/artifact-broker-grants.sql").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
failures = []

for dependency in (
    "insight-platform-artifact-broker.workspace = true",
    "insight-platform-artifact-rpc.workspace = true",
    "insight-platform-postgres.workspace = true",
):
    if dependency not in service_manifest:
        failures.append(f"Artifact service is missing {dependency}")
for composition in (
    "ArtifactBrokerAudience",
    "PLATFORM_ARTIFACT_BROKER_AUDIENCE",
    "BrokeredSandboxArtifactBroker",
    "ArtifactSandboxBrokerGrpcService",
    "add_service(sandbox_service)",
):
    if composition not in service_source:
        failures.append(f"Artifact service is missing audience-isolated composition {composition}")
if "BrokeredArtifactRuntime" in service_source:
    failures.append("Artifact service must not construct a shared Model/Sandbox Broker runtime")
if "sqlx.workspace = true" not in service_manifest:
    failures.append("Artifact service must own its restricted PostgreSQL adapter")
if re.search(r"^sqlx(?:\.|\s*=)", rpc_manifest, re.MULTILINE):
    failures.append("Artifact RPC must not own SQL")
for sdk in ("aws-config", "aws-sdk-kms", "aws-sdk-s3"):
    if sdk in rpc_manifest:
        failures.append(f"Artifact RPC must not own provider SDK {sdk}")
    if sdk not in broker_manifest:
        failures.append(f"Artifact Broker core is missing provider SDK {sdk}")

expected_methods = {
    "rpc ReadWasiArtifact(ClosedArtifactReadRequest) returns (stream ArtifactReadChunk);",
    "rpc ReadMicroVmArtifact(ClosedArtifactReadRequest) returns (stream ArtifactReadChunk);",
}
if any(method not in proto for method in expected_methods) or proto.count("  rpc ") != len(expected_methods):
    failures.append("Artifact internal RPC must expose exactly the reviewed Sandbox read methods")
if not re.search(r"service ArtifactSandboxBrokerService\s*\{\s*rpc ReadWasiArtifact.*rpc ReadMicroVmArtifact", proto, re.DOTALL):
    failures.append("Sandbox audience service is missing its two typed read methods")
if "insight-platform-sandbox.workspace = true" not in rpc_manifest:
    failures.append("Artifact RPC is missing the closed Sandbox read contracts")
if "/usr/local/bin/platform-artifact-broker" not in dockerfile:
    failures.append("runtime image is missing platform-artifact-broker")

required_select = {
    "schema_migrations", "invocations", "jobs", "run_values", "artifact_links",
    "artifacts", "artifact_blobs",
}
for table in required_select:
    if table not in grants:
        failures.append(f"Artifact restricted role is missing SELECT authority for {table}")
if re.search(r"GRANT\s+(INSERT|UPDATE|DELETE|TRUNCATE)", grants, re.IGNORECASE):
    failures.append("Artifact restricted role must remain read-only")

if failures:
    raise SystemExit("\n".join(f"Artifact Broker deployment: {failure}" for failure in failures))
PY

helm lint "$chart"
helm template platform "$chart" --include-crds >"$rendered"

negative_values=(
  "--set|brokers.sandbox.replicas=1"
  "--set|image.digest=latest"
  "--set|brokers.extra.replicas=2"
  "--set-json|brokers.sandbox=null"
  "--set-json|networkPolicy.postgresCidrs=[]"
  "--set-json|networkPolicy.storageProviderCidrs=[]"
  "--set|networkPolicy.sandboxControllerPodSelector="
  "--set|brokers.sandbox.serviceAccount.annotations="
  "--set|brokers.sandbox.autoscaling.minReplicas=1"
)
for case in "${negative_values[@]}"; do
  flag=${case%%|*}
  assignment=${case#*|}
  if helm template platform "$chart" "$flag" "$assignment" >/dev/null 2>&1; then
    echo "Artifact Broker deployment: chart accepted invalid override $assignment" >&2
    exit 1
  fi
done

ruby -ryaml - "$rendered" <<'RUBY'
docs = YAML.load_stream(File.read(ARGV.fetch(0))).compact
failures = []

deployments = docs.select { |doc| doc["kind"] == "Deployment" }
services = docs.select { |doc| doc["kind"] == "Service" }
service_accounts = docs.select { |doc| doc["kind"] == "ServiceAccount" }
pdbs = docs.select { |doc| doc["kind"] == "PodDisruptionBudget" }
hpas = docs.select { |doc| doc["kind"] == "HorizontalPodAutoscaler" }
policies = docs.select { |doc| doc["kind"] == "NetworkPolicy" }

failures << "must render one Sandbox Deployment" unless deployments.length == 1
failures << "must render one Sandbox Service" unless services.length == 1
failures << "must render one Sandbox ServiceAccount" unless service_accounts.length == 1
failures << "must render one PodDisruptionBudget" unless pdbs.length == 1
failures << "must render one HorizontalPodAutoscaler" unless hpas.length == 1
failures << "must render default-deny plus one caller policy" unless policies.length == 2

identities = {}
%w[sandbox].each do |audience|
  role = "artifact-broker-#{audience}"
  name = "insight-platform-artifact-broker-#{audience}"
  deployment = deployments.find { |doc| doc.dig("metadata", "name") == name }
  service = services.find { |doc| doc.dig("metadata", "name") == name }
  account = service_accounts.find { |doc| doc.dig("metadata", "name") == name }
  policy = policies.find { |doc| doc.dig("metadata", "name") == name }
  unless deployment && service && account && policy
    failures << "missing complete #{audience} audience deployment boundary"
    next
  end

  pod_spec = deployment.dig("spec", "template", "spec")
  labels = deployment.dig("spec", "template", "metadata", "labels")
  container = pod_spec.fetch("containers").first
  env = container.fetch("env").to_h { |entry| [entry["name"], entry] }
  volumes = pod_spec.fetch("volumes").to_h { |entry| [entry["name"], entry] }
  identities[audience] = {
    service_account: pod_spec["serviceAccountName"],
    annotations: account.dig("metadata", "annotations"),
    database_secret: env.dig("PLATFORM_ARTIFACT_BROKER_DATABASE_URL", "valueFrom", "secretKeyRef", "name"),
    config_map: volumes.dig("config", "configMap", "name"),
    tls_secret: volumes.dig("tls", "secret", "secretName"),
  }

  failures << "#{audience} workload role label drifted" unless labels["insight.platform/workload-role"] == role
  failures << "#{audience} Service selector drifted" unless service.dig("spec", "selector", "insight.platform/workload-role") == role
  failures << "#{audience} Deployment must keep at least two replicas" unless deployment.dig("spec", "replicas").to_i >= 2
  failures << "#{audience} workload must disable API token automount" unless pod_spec["automountServiceAccountToken"] == false
  failures << "#{audience} ServiceAccount must disable API token automount" unless account["automountServiceAccountToken"] == false
  failures << "#{audience} workload image must be immutable" unless container["image"]&.match?(/@sha256:[0-9a-f]{64}\z/)
  failures << "#{audience} workload command drifted" unless container["command"] == ["/usr/local/bin/platform-artifact-broker"]
  security = container.fetch("securityContext")
  failures << "#{audience} workload lost Restricted security" unless security["allowPrivilegeEscalation"] == false && security["readOnlyRootFilesystem"] == true && security.dig("capabilities", "drop") == ["ALL"]
  required_env = %w[PLATFORM_ARTIFACT_BROKER_AUDIENCE PLATFORM_ARTIFACT_BROKER_CONFIG PLATFORM_ARTIFACT_BROKER_CONFIG_DIGEST PLATFORM_ARTIFACT_BROKER_DATABASE_URL PLATFORM_ARTIFACT_BROKER_CLIENT_CA_PATH PLATFORM_ARTIFACT_BROKER_CERT_PATH PLATFORM_ARTIFACT_BROKER_KEY_PATH]
  failures << "#{audience} workload is missing closed config/database/mTLS inputs" unless required_env.all? { |key| env.key?(key) }
  failures << "#{audience} workload is not bound to its closed audience" unless env.dig("PLATFORM_ARTIFACT_BROKER_AUDIENCE", "value") == audience
  failures << "#{audience} workload identity is missing" if account.dig("metadata", "annotations").to_h.empty?

  ingress = policy.dig("spec", "ingress")
  expected_namespace = {"insight.platform/sandbox-controller-namespace" => "true"}
  expected_pod = {"insight.platform/workload-role" => "sandbox-controller"}
  from = ingress&.first&.fetch("from", [])
  caller = from&.first
  failures << "#{audience} ingress must contain exactly its caller" unless from&.length == 1 && caller&.dig("namespaceSelector", "matchLabels") == expected_namespace && caller&.dig("podSelector", "matchLabels") == expected_pod
  service_port = service.dig("spec", "ports", 0, "port")
  failures << "#{audience} ingress port must match its Service" unless ingress&.first&.dig("ports", 0, "port") == service_port
end

rendered = File.read(ARGV.fetch(0))
%w[kind:\ Ingress hostNetwork:\ true hostPID:\ true privileged:\ true AWS_ACCESS_KEY AWS_SECRET_ACCESS_KEY SECRET_MANAGER].each do |forbidden|
  failures << "rendered boundary has forbidden capability #{forbidden.gsub('\\ ', ' ')}" if rendered.include?(forbidden.gsub('\\ ', ' '))
end

if failures.any?
  failures.each { |failure| warn "Artifact Broker deployment: #{failure}" }
  exit 1
end
puts "Artifact Broker deployment contract passed (Sandbox-only materialization, 2 NetworkPolicies)."
RUBY
