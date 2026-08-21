#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
chart="$workspace_root/deploy/helm/insight-platform-artifact"
rendered=$(mktemp)
trap 'rm -f "$rendered"' EXIT

python3 - "$workspace_root" <<'PY'
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
manifest = (root / "crates/platform-artifact-service/Cargo.toml").read_text()
dockerfile = (root / "Dockerfile").read_text()
postgres = (root / "crates/platform-postgres/src/repository.rs").read_text()
artifact_repository = (root / "crates/platform-postgres/src/artifact_repository.rs").read_text()
gateway = (root / "crates/platform-artifact-service/src/bin/gateway.rs").read_text()
grants = (root / "crates/platform-postgres/artifact-role-grants.sql").read_text()
failures = []
for binary in ("platform-artifact-gateway", "platform-artifact-data-worker", "platform-artifact-maintenance"):
    if f'name = "{binary}"' not in manifest or f"/usr/local/bin/{binary}" not in dockerfile:
        failures.append(f"missing immutable runtime binary {binary}")
for boundary in (
    "ArtifactWorkerRole::DataWorker",
    "ArtifactWorkerRole::Maintenance",
    "payload ->> 'kind' = ANY",
):
    if boundary not in postgres:
        failures.append(f"missing role-gated claim boundary {boundary}")
for authority in (
    "ArtifactScanObjectReadAuthority",
    "ArtifactDeleteObjectAuthority",
    "ArtifactObjectReadAuthority<GatewayArtifactReadRequest>",
    "require_raw_artifact_job_fence",
):
    if authority not in artifact_repository:
        failures.append(f"missing exact Artifact authority {authority}")
for route in (
    "/v1/artifacts:prepare-upload",
    '"/v1/artifacts/{artifact_action}"',
    "get(get_artifact).post(mutate_artifact)",
    'strip_suffix(":complete-upload")',
    "/v1/artifacts/{artifact_id}/content",
    'strip_suffix(":delete")',
):
    if route not in gateway:
        failures.append(f"missing public Artifact Gateway route {route}")
for role in ("artifact_gateway_role", "artifact_data_reader_role", "artifact_data_worker_role", "artifact_maintenance_role"):
    if role not in grants:
        failures.append(f"missing PostgreSQL role grant matrix entry {role}")
if "GRANT DELETE" in grants.upper() or "GRANT TRUNCATE" in grants.upper():
    failures.append("Artifact roles must not receive destructive PostgreSQL table privileges")
for boundary in (
    "ExactMtlsListener",
    "PUBLIC_GATEWAY_WORKLOAD_IDENTITY",
    "x-insight-verified-principal-id",
    "x-insight-idempotency-key-digest",
):
    if boundary not in gateway:
        failures.append(f"missing authenticated public Artifact hop boundary {boundary}")
if failures:
    raise SystemExit("\n".join(f"Artifact deployment: {failure}" for failure in failures))
PY

helm lint "$chart"
helm template platform "$chart" --include-crds >"$rendered"

negative_values=(
  "--set|roles.gateway.replicas=1"
  "--set|image.digest=latest"
  "--set|roles.extra.replicas=2"
  "--set-json|roles.maintenance=null"
  "--set-json|networkPolicy.storageProviderCidrs=[]"
  "--set|roles.gateway.database.existingSecret=insight-platform-artifact-maintenance-database"
  "--set|roles.gateway.serviceAccount.annotations.eks.amazonaws.com/role-arn=arn:aws:iam::111122223333:role/insight-platform-artifact-maintenance"
  "--set|roles.data-worker.tls.existingSecret="
  "--set|roles.gateway.tls.existingSecret="
)
for case in "${negative_values[@]}"; do
  flag=${case%%|*}
  assignment=${case#*|}
  if helm template platform "$chart" "$flag" "$assignment" >/dev/null 2>&1; then
    echo "Artifact deployment: chart accepted invalid override $assignment" >&2
    exit 1
  fi
done

ruby -ryaml - "$rendered" <<'RUBY'
docs = YAML.load_stream(File.read(ARGV.fetch(0))).compact
failures = []
deployments = docs.select { |doc| doc["kind"] == "Deployment" }
services = docs.select { |doc| doc["kind"] == "Service" }
accounts = docs.select { |doc| doc["kind"] == "ServiceAccount" }
policies = docs.select { |doc| doc["kind"] == "NetworkPolicy" }
pdbs = docs.select { |doc| doc["kind"] == "PodDisruptionBudget" }
hpas = docs.select { |doc| doc["kind"] == "HorizontalPodAutoscaler" }

failures << "must render exactly three Artifact Deployments" unless deployments.length == 3
failures << "must render exactly three Artifact Services" unless services.length == 3
failures << "must render exactly three Artifact ServiceAccounts" unless accounts.length == 3
failures << "must render one PDB per role" unless pdbs.length == 3
failures << "must render one HPA per role" unless hpas.length == 3
failures << "must render default deny and one policy per role" unless policies.length == 4

expected = {
  "gateway" => ["/usr/local/bin/platform-artifact-gateway", "PLATFORM_ARTIFACT_GATEWAY_DATABASE_URL"],
  "data-worker" => ["/usr/local/bin/platform-artifact-data-worker", ["PLATFORM_ARTIFACT_DATA_WORKER_READ_DATABASE_URL", "PLATFORM_ARTIFACT_DATA_WORKER_WORK_DATABASE_URL"]],
  "maintenance" => ["/usr/local/bin/platform-artifact-maintenance", "PLATFORM_ARTIFACT_MAINTENANCE_DATABASE_URL"],
}
identities = []
expected.each do |role, (command, database_env)|
  name = "insight-platform-artifact-#{role}"
  deployment = deployments.find { |doc| doc.dig("metadata", "name") == name }
  service = services.find { |doc| doc.dig("metadata", "name") == name }
  account = accounts.find { |doc| doc.dig("metadata", "name") == name }
  policy = policies.find { |doc| doc.dig("metadata", "name") == name }
  unless deployment && service && account && policy
    failures << "missing complete #{role} boundary"
    next
  end
  pod = deployment.dig("spec", "template", "spec")
  labels = deployment.dig("spec", "template", "metadata", "labels")
  container = pod.fetch("containers").first
  env = container.fetch("env").to_h { |entry| [entry["name"], entry] }
  database_envs = database_env.is_a?(Array) ? database_env : [database_env]
  db_secrets = database_envs.map { |key| env.dig(key, "valueFrom", "secretKeyRef", "name") }
  identity = account.dig("metadata", "annotations").to_h
  identities << [db_secrets, identity]
  failures << "#{role} workload role drifted" unless labels["insight.platform/workload-role"] == "artifact-#{role}"
  failures << "#{role} Service selector drifted" unless service.dig("spec", "selector", "insight.platform/workload-role") == "artifact-#{role}"
  failures << "#{role} command drifted" unless container["command"] == [command]
  failures << "#{role} image is mutable" unless container["image"]&.match?(/@sha256:[0-9a-f]{64}\z/)
  failures << "#{role} API token automount is enabled" unless pod["automountServiceAccountToken"] == false && account["automountServiceAccountToken"] == false
  security = container.fetch("securityContext")
  failures << "#{role} lost Restricted security" unless security["allowPrivilegeEscalation"] == false && security["readOnlyRootFilesystem"] == true && security.dig("capabilities", "drop") == ["ALL"]
  if role == "gateway"
    failures << "Artifact Gateway lost mTLS client CA" unless env.key?("PLATFORM_ARTIFACT_GATEWAY_CLIENT_CA_PATH")
    tls_mount = container.fetch("volumeMounts", []).find { |mount| mount["name"] == "tls" }
    failures << "Artifact Gateway lost read-only TLS mount" unless tls_mount && tls_mount["readOnly"] == true
  end
end
failures << "Artifact roles share a database Secret" unless identities.flat_map(&:first).uniq.length == 4
failures << "Artifact roles share a storage identity" unless identities.map(&:last).uniq.length == 3

data_policy = policies.find { |doc| doc.dig("metadata", "name") == "insight-platform-artifact-data-worker" }
data_ports = data_policy.to_h.dig("spec", "ingress").to_a.flat_map { |entry| entry.fetch("ports", []).map { |port| port["port"] } }.sort
failures << "Data Worker ingress must be split across controller and guest listeners" unless data_ports == [9443, 9444]
maintenance_policy = policies.find { |doc| doc.dig("metadata", "name") == "insight-platform-artifact-maintenance" }
failures << "Maintenance must deny all ingress" unless maintenance_policy.to_h.dig("spec", "ingress") == []

rendered = File.read(ARGV.fetch(0))
%w[hostNetwork:\ true hostPID:\ true privileged:\ true AWS_ACCESS_KEY AWS_SECRET_ACCESS_KEY].each do |forbidden|
  value = forbidden.gsub('\\ ', ' ')
  failures << "rendered boundary includes #{value}" if rendered.include?(value)
end
if failures.any?
  failures.each { |failure| warn "Artifact deployment: #{failure}" }
  exit 1
end
puts "Artifact deployment contract passed (Gateway/Data Worker/Maintenance isolated)."
RUBY
