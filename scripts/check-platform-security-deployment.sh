#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import pathlib
import re
import subprocess

root = pathlib.Path.cwd()
grants = (root / "crates/platform-postgres/security-authority-grants.sql").read_text(encoding="utf-8")
authority = (root / "crates/platform-security-authority/Cargo.toml").read_text(encoding="utf-8")
egress_core = (root / "crates/platform-egress/Cargo.toml").read_text(encoding="utf-8")
egress = (root / "crates/platform-egress-broker/Cargo.toml").read_text(encoding="utf-8")
egress_rpc = (root / "crates/platform-egress-rpc/Cargo.toml").read_text(encoding="utf-8")
broker = (root / "crates/platform-secret-broker/Cargo.toml").read_text(encoding="utf-8")
proto = (root / "proto/insight/platform/v1/security_internal.proto").read_text(encoding="utf-8")
egress_proto = (root / "proto/insight/platform/v1/egress_internal.proto").read_text(encoding="utf-8")
dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
chart = root / "deploy/helm/insight-platform-security-egress"

failures = []
if "sqlx.workspace = true" not in authority:
    failures.append("Security Authority must own the restricted PostgreSQL adapter")
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
}
for method in required_methods:
    if method not in proto:
        failures.append(f"Security internal RPC is missing exact method: {method}")
if proto.count("  rpc ") != 2:
    failures.append("Security internal RPC must expose exactly two methods")

for dependency in (
    "insight-platform-egress-rpc.workspace = true",
    "insight-platform-secret-broker.workspace = true",
    "insight-platform-security-rpc.workspace = true",
    "insight-platform-sandbox-rpc.workspace = true",
):
    if dependency not in egress:
        failures.append(f"deployable Egress Broker is missing {dependency}")
if "insight-platform-postgres" in egress or "sqlx" in egress:
    failures.append("deployable Egress Broker must not have a PostgreSQL dependency")
required_egress_methods = {
    "rpc OpenModelProvider(ClosedEgressEnvelope) returns (stream ClosedEgressEnvelope);",
    "rpc CancelModelProvider(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc RoundTripCapabilityHttp(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc CancelCapabilityHttp(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc UnaryCapabilityGrpc(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc CancelCapabilityGrpc(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc ExchangeMcpOAuthAuthorizationCode(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc DeleteMcpOAuthPkceSecret(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc ExecuteMcpStreamableHttp(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc CancelMcpRemoteTask(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
    "rpc StreamMcpStreamableHttpSubscription(stream ClosedEgressEnvelope) returns (stream ClosedEgressEnvelope);",
    "rpc ResolveManagedMcpSandboxSecret(ClosedEgressEnvelope) returns (ClosedEgressEnvelope);",
}
for method in required_egress_methods:
    if method not in egress_proto:
        failures.append(f"Egress internal RPC is missing exact method: {method}")
if egress_proto.count("  rpc ") != len(required_egress_methods):
    failures.append("Egress internal RPC must expose exactly the twelve reviewed methods")
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

required_select = {
    "schema_migrations",
    "secret_bindings",
    "principals",
    "tenant_principals",
    "receipts",
}
required_insert = {"secret_bindings", "receipts", "events", "outbox_events"}
for table in required_select | required_insert:
    if f"insight_platform.{table}" not in grants:
        failures.append(f"Security Authority grant contract is missing {table}")
for forbidden in (
    "GRANT UPDATE ON insight_platform.secret_bindings",
    "GRANT DELETE",
    "insight_platform.resources TO %I",
    "insight_platform.runs TO %I",
    "insight_platform.jobs TO %I",
    "insight_platform.tasks TO %I",
    "insight_platform.invocations TO %I",
    "insight_platform.artifacts TO %I",
    "insight_platform.quota_accounts TO %I",
    "insight_platform.quota_ledger TO %I",
):
    if forbidden in grants:
        failures.append(f"Security Authority grant contract contains forbidden privilege: {forbidden}")

if failures:
    raise SystemExit("\n".join(f"security deployment: {failure}" for failure in failures))
print("Security Authority/Egress static deployment boundary passed.")
PY
