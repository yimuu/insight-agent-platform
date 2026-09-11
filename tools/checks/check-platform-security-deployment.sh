#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import re
import subprocess

root = pathlib.Path.cwd()
grants = (root / "crates/adapters/platform-postgres/security-authority-grants.sql").read_text(encoding="utf-8")
authority = (root / "apps/services/platform-security-authority/Cargo.toml").read_text(encoding="utf-8")
authority_source = (root / "apps/services/platform-security-authority/src/main.rs").read_text(encoding="utf-8")
egress_core = (root / "crates/adapters/platform-egress/Cargo.toml").read_text(encoding="utf-8")
egress = (root / "apps/services/platform-egress-broker/Cargo.toml").read_text(encoding="utf-8")
egress_source = (root / "apps/services/platform-egress-broker/src/main.rs").read_text(encoding="utf-8")
egress_capacity = (root / "apps/services/platform-egress-broker/src/capacity.rs").read_text(encoding="utf-8")
egress_rpc = (root / "crates/protocols/platform-egress-rpc/Cargo.toml").read_text(encoding="utf-8")
broker = (root / "crates/adapters/platform-secret-broker/Cargo.toml").read_text(encoding="utf-8")
proto = (root / "contracts/proto/insight/platform/v1/security_internal.proto").read_text(encoding="utf-8")
egress_proto = (root / "contracts/proto/insight/platform/v1/egress_internal.proto").read_text(encoding="utf-8")
dockerfile = (root / "deploy/images/platform.Dockerfile").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-security-egress"

failures = []
if "sqlx.workspace = true" not in authority:
    failures.append("Security Authority must own the restricted PostgreSQL adapter")
for name, manifest, source in (
    ("Security Authority", authority, authority_source),
    ("Egress Broker", egress, egress_source),
):
    if "insight-platform-observability.workspace = true" not in manifest:
        failures.append(f"{name} must depend on shared process observability")
    for required in (
        "observability_listen_address",
        "process_observability_router",
        "ProcessHttpMetrics",
        "mark_ready",
    ):
        if required not in source:
            failures.append(f"{name} production composition is missing {required}")
for required in (
    "ProcessHttpMetrics::install_with_capacities",
    "PostgresPoolCapacity",
    "postgresql_connections",
    "pool.clone()",
):
    if required not in authority_source:
        failures.append(f"Security Authority capacity composition is missing {required}")
for required in (
    "ProcessHttpMetrics::install_with_capacities",
    "secret_resolution",
    "secret_store",
    "model_provider",
    "capability_http",
    "capability_grpc",
    "remote_context",
    "mcp_oauth",
    "mcp_request",
    "mcp_subscription",
    "mcp_subscription_bridge",
):
    if required not in egress_source:
        failures.append(f"Egress Broker capacity composition is missing {required}")
for required in (
    "EgressCapacitySnapshot",
    "SecretBrokerCapacitySnapshot",
    "EgressMcpSubscriptionBridgeCapacitySnapshot",
    "OperationalCapacitySource",
    "mcp_subscription_pending",
    "mcp_subscription_active",
):
    if required not in egress_capacity:
        failures.append(f"Egress Broker capacity adapter is missing {required}")
for name, manifest in (
    ("Egress core", egress_core),
    ("Egress Broker", egress),
    ("Egress RPC", egress_rpc),
    ("Secret Broker", broker),
):
    if re.search(r"^sqlx(?:\.|\s*=)", manifest, re.MULTILINE):
        failures.append(f"{name} must not depend on SQLx")

required_methods = {
    "rpc LoadSecretBinding(ClosedSecurityEnvelope) returns (ClosedSecurityEnvelope);",
    "rpc RegisterPreparedSecretBinding(ClosedSecurityEnvelope) returns (ClosedSecurityEnvelope);",
    "rpc AuthorizeModelCredentialImport(ClosedSecurityEnvelope) returns (ClosedSecurityEnvelope);",
    "rpc AuthorizeModelConnectionProbe(ClosedSecurityEnvelope) returns (ClosedSecurityEnvelope);",
    "rpc AuthorizeModelDispatch(ClosedSecurityEnvelope) returns (ClosedSecurityEnvelope);",
    "rpc AuthorizeContextDispatch(ClosedSecurityEnvelope) returns (ClosedSecurityEnvelope);",
}
for method in required_methods:
    if method not in proto:
        failures.append(f"Security internal RPC is missing exact method: {method}")
if proto.count("  rpc ") != len(required_methods):
    failures.append("Security internal RPC must expose exactly the reviewed authority methods")

for dependency in (
    "insight-platform-egress-rpc.workspace = true",
    "insight-platform-secret-broker.workspace = true",
    "insight-platform-security-rpc.workspace = true",
):
    if dependency not in egress:
        failures.append(f"deployable Egress Broker is missing {dependency}")
if "insight-platform-postgres" in egress or "sqlx" in egress:
    failures.append("deployable Egress Broker must not have a PostgreSQL dependency")
required_egress_methods = {
    "rpc ImportModelCredential(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc ProbeModelConnection(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc OpenModelProvider(ClosedEgressEnvelope) returns (stream ClosedEgressEnvelope);",
    "rpc CancelModelProvider(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc RoundTripCapabilityHttp(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc CancelCapabilityHttp(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc UnaryCapabilityGrpc(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc CancelCapabilityGrpc(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc QueryRemoteContext(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc ExchangeMcpOAuthAuthorizationCode(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc DeleteMcpOAuthPkceSecret(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc DiscoverMcpStreamableHttp(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc ExecuteMcpStreamableHttp(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc RefreshMcpResources(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc CancelMcpRemoteTask(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc StreamMcpStreamableHttpSubscription(stream ClosedEgressEnvelope) returns (stream ClosedEgressEnvelope);",
}
for method in required_egress_methods:
    if method not in egress_proto:
        failures.append(f"Egress internal RPC is missing exact method: {method}")
if egress_proto.count("  rpc ") != len(required_egress_methods):
    failures.append("Egress internal RPC must expose exactly the reviewed remote-only methods")
for binary in ("platform-egress-broker", "platform-security-authority"):
    if f"/usr/local/bin/{binary}" not in dockerfile:
        failures.append(f"runtime image is missing {binary}")

try:
    subprocess.run(
        ["helm", "lint", str(chart)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    rendered = subprocess.run(
        ["helm", "template", "platform", str(chart)],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ).stdout
except (FileNotFoundError, subprocess.CalledProcessError) as error:
    failures.append(f"Security/Egress Helm contract did not render: {error}")
    rendered = ""

deployments = [item for item in rendered.split("---") if "kind: Deployment" in item]
egress_deployments = [item for item in deployments if "app.kubernetes.io/component: egress-broker" in item]
authority_deployments = [item for item in deployments if "app.kubernetes.io/component: security-authority" in item]
if len(egress_deployments) != 1 or len(authority_deployments) != 1:
    failures.append("Helm must render exactly one Egress and one Security Authority Deployment")
else:
    if re.search(r"DATABASE|POSTGRES|platform-postgres|sqlx", egress_deployments[0], re.IGNORECASE):
        failures.append("rendered Egress Deployment must not receive a database credential")
    if "/etc/insight/mcp-state-keys" not in egress_deployments[0]:
        failures.append("rendered Egress Deployment must mount the MCP state-key Secret")
    if re.search(r"AWS_|KMS|SECRET_MANAGER|workload-identity", authority_deployments[0], re.IGNORECASE):
        failures.append("rendered Security Authority Deployment must not receive external-provider authority")
if rendered.count("kind: Namespace") != 2:
    failures.append("Helm must render two isolated namespaces")
if rendered.count("name: default-deny") != 2:
    failures.append("both Security and Egress namespaces require default-deny NetworkPolicy")
for needle in (
    "kind: ServiceMonitor",
    "name: observability",
    "path: /livez",
    "path: /readyz",
    "path: /metrics",
):
    if needle not in rendered:
        failures.append(f"rendered Security/Egress observability contract is missing {needle}")
if rendered.count("kind: ServiceMonitor") != 2:
    failures.append("Security/Egress chart must render one ServiceMonitor per isolated workload pool")

negative_values = (
    ("--set", "egress.observabilityPort=8443", "egress.observabilityPort must be distinct"),
    ("--set", "securityAuthority.observabilityPort=0", "securityAuthority.observabilityPort must be distinct"),
    ("--set-json", "networkPolicy.monitoringPodSelector=null", "monitoring requires exact"),
)
for flag, assignment, expected in negative_values:
    result = subprocess.run(
        ["helm", "template", "platform", str(chart), flag, assignment],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    if result.returncode == 0 or expected not in result.stderr:
        failures.append(f"Security/Egress chart accepted invalid override {assignment}")

required_select = {
    "secret_bindings",
    "principals",
    "tenant_principals",
    "receipts",
}
required_insert = {"secret_bindings", "receipts", "events", "outbox_events"}
for table in required_select | required_insert:
    if f"insight_platform.{table}" not in grants:
        failures.append(f"Security Authority grant contract is missing {table}")
expected_artifact_grants = [
    "GRANT SELECT (tenant_id, artifact_id, blob_id, state, terminal_at, verified_media_type, classification) ON insight_platform.artifacts TO %I",
    "GRANT SELECT (tenant_id, blob_id, state, deleted_at, content_digest, size_bytes) ON insight_platform.artifact_blobs TO %I",
]
actual_artifact_grants = [
    statement
    for statement in re.findall(r"'(GRANT [^']+ TO %I)'", grants)
    if re.search(r"\binsight_platform\.artifact(?:s|_[a-z_]+)\b", statement)
]
if sorted(actual_artifact_grants) != sorted(expected_artifact_grants):
    failures.append("Security Authority Artifact reads must use only the exact readiness metadata columns")
for forbidden in (
    "GRANT UPDATE ON insight_platform.secret_bindings",
    "GRANT DELETE",
    "insight_platform.resources TO %I",
    "insight_platform.runs TO %I",
    "insight_platform.jobs TO %I",
    "insight_platform.tasks TO %I",
    "insight_platform.invocations TO %I",
    "insight_platform.quota_accounts TO %I",
    "insight_platform.quota_ledger TO %I",
):
    if forbidden in grants:
        failures.append(f"Security Authority grant contract contains forbidden privilege: {forbidden}")

if failures:
    raise SystemExit("\n".join(f"security deployment: {failure}" for failure in failures))
print("Security Authority/Egress static deployment boundary passed.")
PY
