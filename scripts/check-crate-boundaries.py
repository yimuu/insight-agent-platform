import difflib
import json
import re
import sys
from collections import deque
from pathlib import Path


INTERNAL_ROLES = {
    "insight-agent-platform": "root",
    "insight-engine": "engine",
    "insight-dsl": "dsl",
    "insight-durable": "durable",
    "insight-resources": "resources",
    "insight-mcp": "mcp",
    "insight-storage": "storage",
    "insight-runtime": "runtime",
    "insight-api": "api",
    "insight-platform-artifacts": "artifacts_domain",
    "insight-platform-artifact-broker": "artifact_broker",
    "insight-platform-api": "platform_api",
    "insight-platform-capability-adapters": "capability_adapters",
    "insight-platform-callback-api": "callback_api",
    "insight-platform-contracts": "contracts",
    "insight-platform-context": "context_domain",
    "insight-platform-egress": "egress_core",
    "insight-platform-egress-broker": "egress_broker",
    "insight-platform-egress-rpc": "egress_rpc",
    "insight-platform-invocations": "invocations_domain",
    "insight-platform-jobs": "jobs_domain",
    "insight-platform-mcp-cleanup-worker": "mcp_cleanup_worker",
    "insight-platform-mcp-host": "mcp_host",
    "insight-platform-model-adapters": "model_adapters",
    "insight-platform-models": "models_domain",
    "insight-platform-orchestrator": "orchestrator_domain",
    "insight-platform-postgres": "platform_postgres",
    "insight-platform-registry": "registry_domain",
    "insight-platform-runtime": "platform_runtime",
    "insight-platform-sandbox": "sandbox_domain",
    "insight-platform-sandbox-attestor": "sandbox_attestor",
    "insight-platform-sandbox-controller": "sandbox_controller",
    "insight-platform-sandbox-executor": "sandbox_executor",
    "insight-platform-sandbox-rpc": "sandbox_rpc",
    "insight-platform-sandbox-microvm": "sandbox_microvm_provider",
    "insight-platform-sandbox-wasi": "sandbox_wasi_executor",
    "insight-platform-scheduler": "scheduler_domain",
    "insight-platform-secret-broker": "secret_broker",
    "insight-platform-security": "security_domain",
    "insight-platform-security-authority": "security_authority",
    "insight-platform-security-rpc": "security_rpc",
    "insight-platform-tasks": "tasks_domain",
    "insight-platform-worker": "platform_worker",
}

ALLOWED_INTERNAL = {
    "root": {"engine", "dsl", "durable", "resources", "mcp", "storage", "runtime", "api", "artifacts_domain", "platform_api", "capability_adapters", "contracts", "context_domain", "egress_core", "invocations_domain", "jobs_domain", "mcp_host", "model_adapters", "models_domain", "orchestrator_domain", "registry_domain", "sandbox_domain", "scheduler_domain", "secret_broker", "security_domain", "tasks_domain", "platform_postgres", "platform_runtime", "platform_worker"},
    "engine": set(),
    "dsl": {"engine"},
    "durable": {"engine", "dsl"},
    "resources": {"engine", "mcp"},
    "mcp": set(),
    "storage": {"engine", "durable", "dsl"},
    "runtime": {"engine", "durable", "dsl", "resources", "mcp"},
    "api": {"engine", "dsl", "durable", "resources", "runtime", "mcp"},
    "artifacts_domain": {"contracts", "jobs_domain"},
    "artifact_broker": {"artifacts_domain", "contracts", "sandbox_domain"},
    "platform_api": {"mcp_host"},
    "capability_adapters": {"contracts", "invocations_domain", "jobs_domain", "mcp_host"},
    "callback_api": {"platform_api", "contracts", "egress_rpc", "mcp_host", "platform_postgres"},
    "contracts": set(),
    "context_domain": {"contracts", "invocations_domain", "jobs_domain"},
    "egress_core": {"capability_adapters", "contracts", "mcp_host", "model_adapters"},
    "egress_broker": {"contracts", "egress_core", "egress_rpc", "model_adapters", "secret_broker", "security_rpc"},
    "egress_rpc": {"capability_adapters", "contracts", "mcp_host", "model_adapters"},
    "invocations_domain": {"contracts", "jobs_domain"},
    "jobs_domain": {"contracts"},
    "mcp_cleanup_worker": {"contracts", "egress_rpc", "mcp_host", "platform_postgres"},
    "mcp_host": {"contracts", "jobs_domain"},
    "model_adapters": {"contracts", "jobs_domain", "models_domain"},
    "models_domain": {"contracts", "invocations_domain", "jobs_domain"},
    "orchestrator_domain": {"contracts", "jobs_domain"},
    "registry_domain": {"contracts"},
    "scheduler_domain": {"contracts"},
    "secret_broker": {"contracts", "egress_core", "mcp_host", "security_domain"},
    "security_domain": {"contracts"},
    "security_authority": {"contracts", "platform_postgres", "security_domain", "security_rpc"},
    "security_rpc": {"contracts", "security_domain"},
    "tasks_domain": {"contracts"},
    "platform_postgres": {"artifact_broker", "artifacts_domain", "capability_adapters", "contracts", "context_domain", "invocations_domain", "jobs_domain", "mcp_host", "model_adapters", "models_domain", "orchestrator_domain", "registry_domain", "sandbox_domain", "scheduler_domain", "security_domain", "tasks_domain"},
    "platform_runtime": {"contracts", "orchestrator_domain", "platform_postgres", "platform_worker", "sandbox_domain", "sandbox_rpc", "security_domain"},
    "sandbox_domain": {"contracts", "invocations_domain", "jobs_domain", "mcp_host"},
    "sandbox_attestor": {"contracts", "sandbox_domain", "sandbox_rpc"},
    "sandbox_controller": {"artifact_broker", "contracts", "platform_postgres", "sandbox_domain", "sandbox_rpc"},
    "sandbox_executor": {"contracts", "sandbox_domain", "sandbox_rpc", "sandbox_wasi_executor", "platform_worker"},
    "sandbox_rpc": {"contracts", "sandbox_domain"},
    "sandbox_microvm_provider": {"capability_adapters", "contracts", "invocations_domain", "jobs_domain", "mcp_host", "sandbox_domain", "sandbox_rpc"},
    "sandbox_wasi_executor": {"contracts", "sandbox_domain"},
    "platform_worker": {"contracts"},
}

FORBIDDEN_DIRECT = {
    "engine": {
        "axum",
        "sqlx",
        "reqwest",
        "dotenvy",
        "tracing-subscriber",
        "yaml-rust2",
        "yaml_serde",
        "serde_yaml",
    },
    "dsl": {"axum", "sqlx", "reqwest"},
    "durable": {"axum", "sqlx", "reqwest"},
    "resources": {"axum", "sqlx"},
    "mcp": {"axum", "sqlx", "dotenvy", "tracing-subscriber", "yaml-rust2", "yaml_serde", "serde_yaml"},
    "storage": {"axum", "reqwest"},
    "runtime": {"axum", "sqlx", "reqwest"},
    # MCP HTTP authorization lives at the API transport boundary and uses the
    # shared pinned/SSRF-restricted client from insight-mcp for issuer/JWKS
    # discovery. Direct SQL remains forbidden.
    "api": {"sqlx"},
    "artifacts_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "artifact_broker": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "platform_api": {"sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "capability_adapters": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "callback_api": {"reqwest", "dotenvy", "tracing-subscriber", "aws-config", "aws-sdk-kms", "aws-sdk-s3", "aws-sdk-secretsmanager"},
    "contracts": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "context_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "egress_core": {"axum", "sqlx", "dotenvy", "tracing-subscriber"},
    "egress_broker": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "egress_rpc": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber", "aws-config", "aws-sdk-kms", "aws-sdk-s3", "aws-sdk-secretsmanager"},
    "invocations_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "jobs_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "mcp_cleanup_worker": {"axum", "reqwest", "dotenvy", "tracing-subscriber", "aws-config", "aws-sdk-kms", "aws-sdk-s3", "aws-sdk-secretsmanager"},
    "mcp_host": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "model_adapters": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "models_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "orchestrator_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "registry_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "scheduler_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "secret_broker": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "security_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "security_authority": {"axum", "reqwest", "dotenvy", "tracing-subscriber", "aws-config", "aws-sdk-kms", "aws-sdk-s3", "aws-sdk-secretsmanager"},
    "security_rpc": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber", "aws-config", "aws-sdk-kms", "aws-sdk-s3", "aws-sdk-secretsmanager"},
    "tasks_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "platform_postgres": {"axum", "reqwest", "dotenvy", "tracing-subscriber"},
    "platform_runtime": {"axum", "reqwest", "dotenvy", "tracing-subscriber"},
    "sandbox_domain": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "sandbox_attestor": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "sandbox_controller": {"axum", "reqwest", "dotenvy", "tracing-subscriber"},
    "sandbox_executor": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "sandbox_rpc": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "sandbox_microvm_provider": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "sandbox_wasi_executor": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
    "platform_worker": {"axum", "sqlx", "reqwest", "dotenvy", "tracing-subscriber"},
}

TRANSITIVELY_FORBIDDEN = {"axum", "sqlx", "reqwest"}
CRITICAL_FEATURE_PACKAGES = {"axum", "hyper", "hyper-rustls", "hyper-util", "sqlx", "reqwest", "tokio"}
REQUIRED_RUSTLS_PROVIDER_FEATURE = "aws_lc_rs"
FORBIDDEN_RUSTLS_PROVIDER_FEATURE = "ring"

IO_PATTERNS = (
    (
        "std filesystem/environment/network/process API",
        re.compile(r"\bstd\s*::\s*(?:fs|env|net|process)\b"),
    ),
    (
        "std grouped filesystem/environment/network/process import",
        re.compile(
            r"\bstd\s*::\s*\{[^;]{0,4000}\b(?:fs|env|net|process)\b[^;]{0,4000};",
            re.DOTALL,
        ),
    ),
    (
        "Tokio filesystem/network/process API",
        re.compile(r"\btokio\s*::\s*(?:fs|net|process)\b"),
    ),
    (
        "Tokio grouped filesystem/network/process import",
        re.compile(
            r"\btokio\s*::\s*\{[^;]{0,4000}\b(?:fs|net|process)\b[^;]{0,4000};",
            re.DOTALL,
        ),
    ),
    (
        "compile-time environment access",
        re.compile(r"\b(?:env|option_env)!\s*\("),
    ),
)

CLOCK_PATTERNS = (
    (
        "wall-clock import",
        re.compile(
            r"\buse\s+chrono\s*::[^;]{0,4000}\b(?:Utc|Local)\b[^;]{0,4000};",
            re.DOTALL,
        ),
    ),
    (
        "aliased wall-clock crate import",
        re.compile(r"\buse\s+(?:chrono|time)\s+as\s+[A-Za-z_][A-Za-z0-9_]*\s*;"),
    ),
    (
        "instant/system/timer import",
        re.compile(
            r"\buse\s+(?:std|tokio)\s*::[^;]{0,4000}\b(?:time|thread)\b"
            r"[^;]{0,4000}\b(?:Instant|SystemTime|UNIX_EPOCH|sleep|sleep_until|"
            r"interval|interval_at|timeout|timeout_at)\b[^;]{0,4000};",
            re.DOTALL,
        ),
    ),
    (
        "aliased time module import",
        re.compile(
            r"\buse\s+(?:std|tokio)\s*::[^;]{0,4000}\btime\b\s+as\s+"
            r"[A-Za-z_][A-Za-z0-9_]*\s*;",
            re.DOTALL,
        ),
    ),
    (
        "time crate wall-clock import",
        re.compile(
            r"\buse\s+time\s*::[^;]{0,4000}\bOffsetDateTime\b[^;]{0,4000};",
            re.DOTALL,
        ),
    ),
    (
        "wall clock access",
        re.compile(r"\b(?:chrono\s*::\s*)?(?:Utc|Local)\s*::\s*(?:now|today)\s*\("),
    ),
    (
        "instant/system clock access",
        re.compile(
            r"\b(?:(?:std|tokio)\s*::\s*time\s*::\s*)?(?:Instant|SystemTime)\s*::\s*now\s*\("
        ),
    ),
    (
        "time crate wall clock access",
        re.compile(r"\bOffsetDateTime\s*::\s*now_(?:utc|local)\s*\("),
    ),
    (
        "implicit elapsed wall clock access",
        re.compile(r"\bUNIX_EPOCH\s*\.\s*elapsed\s*\("),
    ),
    (
        "scheduler timer access",
        re.compile(
            r"\b(?:tokio\s*::\s*time\s*::\s*(?:sleep|sleep_until|interval|interval_at|timeout|timeout_at)|std\s*::\s*thread\s*::\s*sleep)\s*\("
        ),
    ),
)

RUNTIME_STACK_PATTERN = re.compile(
    r"\b(?:sqlx|axum|reqwest)\s*::|\bextern\s+crate\s+(?:sqlx|axum|reqwest)\b|\buse\s+(?:sqlx|axum|reqwest)\s*(?:;|as\b)"
)

ROOT_FACADE_PATTERN = re.compile(r"\binsight_agent_platform\b")
DEEP_ASSET_INCLUDE_PATTERN = re.compile(
    r"\binclude_(?:str|bytes)!\s*\([^)]{0,500}(?:\.\./){2}", re.DOTALL
)
MANIFEST_PARENT_PATH_PATTERN = re.compile(
    r"CARGO_MANIFEST_DIR[^;]{0,500}(?:\.\./){2}", re.DOTALL
)
ALLOWED_FIXED_ASSET_LOCATORS = {
    # The contract owner embeds the versioned hard-limit profile once; all
    # consumers use its typed loader instead of reaching across the workspace.
    "crates/platform-contracts/src/limits.rs",
    # Test-only Schema provisioning reads the workspace-owned baseline at
    # runtime; production storage builds contain no embedded DDL.
    "crates/storage/src/repository/schema_contract.rs",
}


def feature_snapshot(metadata):
    workspace_ids = set(metadata["workspace_members"])
    packages = {package["id"]: package for package in metadata["packages"]}
    rows = []
    for node in metadata["resolve"]["nodes"]:
        if node["id"] in workspace_ids:
            continue
        package = packages[node["id"]]
        rows.append(
            (
                package["name"],
                package["version"],
                ",".join(sorted(node.get("features", []))) or "-",
            )
        )
    rows.sort()
    body = "".join("\t".join(row) + "\n" for row in rows)
    return "# crate-boundary-third-party-features-v1\n# name\tversion\tenabled-features\n" + body


def dependency_kinds(dependency):
    kinds = dependency.get("dep_kinds") or []
    if not kinds:
        return ["normal"]
    return sorted({item.get("kind") or "normal" for item in kinds})


def line_number(text, offset):
    return text.count("\n", 0, offset) + 1


def source_files(source_dir):
    if not source_dir.is_dir():
        return []
    return sorted(source_dir.rglob("*.rs"))


def scan_patterns(errors, role, source_dir, patterns, file_filter=lambda _path: True):
    for path in source_files(source_dir):
        if not file_filter(path):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            errors.append(f"{role}: Rust source is not UTF-8: {path}")
            continue
        for label, pattern in patterns:
            for match in pattern.finditer(text):
                errors.append(
                    f"{role}: {label}: {path}:{line_number(text, match.start())}"
                )


def dependency_path(start_id, target_name, nodes, packages):
    queue = deque([start_id])
    parent = {start_id: None}
    while queue:
        current = queue.popleft()
        for dependency in nodes[current].get("deps", []):
            dependency_id = dependency["pkg"]
            if dependency_id in parent:
                continue
            parent[dependency_id] = current
            if packages[dependency_id]["name"] == target_name:
                path = [dependency_id]
                while parent[path[-1]] is not None:
                    path.append(parent[path[-1]])
                path.reverse()
                return path
            queue.append(dependency_id)
    return None


def format_package(package):
    return f"{package['name']}@{package['version']}"


def check(metadata, baseline_path, workspace_root):
    errors = []
    workspace_root = workspace_root.resolve()
    packages = {package["id"]: package for package in metadata["packages"]}
    nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    workspace_ids = set(metadata["workspace_members"])
    role_by_id = {}
    id_by_role = {}

    expected_workspace_names = set(INTERNAL_ROLES)
    actual_workspace_names = {
        packages[package_id]["name"] for package_id in workspace_ids
    }
    if actual_workspace_names != expected_workspace_names or len(workspace_ids) != len(
        expected_workspace_names
    ):
        missing = sorted(expected_workspace_names - actual_workspace_names)
        unexpected = sorted(actual_workspace_names - expected_workspace_names)
        errors.append(
            "workspace package set must contain exactly the declared packages; "
            f"missing={missing or 'none'}, unexpected={unexpected or 'none'}, "
            f"package_count={len(workspace_ids)}"
        )

    for package_id in sorted(workspace_ids):
        package = packages[package_id]
        role = INTERNAL_ROLES.get(package["name"])
        if role is None:
            errors.append(f"unexpected workspace package: {package['name']}")
            continue
        if role in id_by_role:
            errors.append(f"duplicate workspace package for internal role {role}")
            continue
        role_by_id[package_id] = role
        id_by_role[role] = package_id

    if "root" not in id_by_role:
        errors.append("workspace does not contain the insight-agent-platform root package")

    for package_id, package in sorted(packages.items()):
        if package_id not in workspace_ids and package.get("source") is None:
            errors.append(
                "local path package is not a workspace member: "
                f"{format_package(package)} ({package['manifest_path']}); "
                f"expected it under {workspace_root} and in the explicit member list"
            )

    rustls_nodes = [
        (package_id, package)
        for package_id, package in packages.items()
        if package["name"] == "rustls" and package_id in nodes
    ]
    if not rustls_nodes:
        errors.append("workspace TLS provider policy requires a resolved rustls package")
    for package_id, package in rustls_nodes:
        features = set(nodes[package_id].get("features", []))
        if REQUIRED_RUSTLS_PROVIDER_FEATURE not in features:
            errors.append(
                "workspace rustls provider must be AWS-LC; required feature is absent: "
                f"{format_package(package)}"
            )
        if FORBIDDEN_RUSTLS_PROVIDER_FEATURE in features:
            errors.append(
                "workspace rustls provider must be AWS-LC-only; Ring feature is enabled: "
                f"{format_package(package)}"
            )

    for package_id, role in sorted(role_by_id.items(), key=lambda item: item[1]):
        package = packages[package_id]
        node = nodes.get(package_id)
        if node is None:
            errors.append(f"{role}: package is absent from the Cargo resolve graph")
            continue
        for dependency in node.get("deps", []):
            dependency_id = dependency["pkg"]
            dependency_package = packages[dependency_id]
            dependency_role = role_by_id.get(dependency_id)
            kinds = "/".join(dependency_kinds(dependency))
            if dependency_role is not None and dependency_role not in ALLOWED_INTERNAL[role]:
                errors.append(
                    f"{role}: forbidden {kinds} internal edge to {dependency_role} "
                    f"({format_package(dependency_package)})"
                )
            if dependency_package["name"] in FORBIDDEN_DIRECT.get(role, set()):
                errors.append(
                    f"{role}: forbidden direct {kinds} dependency on "
                    f"{format_package(dependency_package)}"
                )

        if role != "root" and package.get("features"):
            errors.append(
                f"{role}: internal crate feature matrix is not allowed in the first cutover: "
                f"{sorted(package['features'])}"
            )

    for role in ("engine", "dsl", "durable"):
        package_id = id_by_role.get(role)
        if package_id is None:
            continue
        for target_name in sorted(TRANSITIVELY_FORBIDDEN):
            path = dependency_path(package_id, target_name, nodes, packages)
            if path is not None:
                rendered = " -> ".join(format_package(packages[item]) for item in path)
                errors.append(f"{role}: forbidden transitive reachability: {rendered}")

    for role in ("engine", "dsl", "durable", "runtime"):
        package_id = id_by_role.get(role)
        if package_id is None:
            continue
        package = packages[package_id]
        manifest_dir = Path(package["manifest_path"]).parent
        source_dir = manifest_dir / "src"
        if not source_dir.is_dir():
            errors.append(f"{role}: expected production source directory is absent: {source_dir}")
            continue

        if role in {"engine", "dsl", "durable"}:
            custom_builds = [
                target["src_path"]
                for target in package.get("targets", [])
                if "custom-build" in target.get("kind", [])
            ]
            if custom_builds or (manifest_dir / "build.rs").exists():
                locations = custom_builds or [str(manifest_dir / "build.rs")]
                errors.append(f"{role}: lower crate must not have build.rs: {', '.join(locations)}")
            scan_patterns(errors, role, source_dir, IO_PATTERNS)

        if role == "engine":
            scan_patterns(
                errors,
                role,
                source_dir,
                CLOCK_PATTERNS,
                lambda path: any("scheduler" in part.lower() for part in path.parts),
            )
        elif role == "runtime":
            scan_patterns(
                errors,
                role,
                source_dir,
                (("direct SQLx/Axum/Reqwest source use", RUNTIME_STACK_PATTERN),),
            )

    shared_asset_helper = workspace_root / "tests/support/workspace_assets.rs"
    if not shared_asset_helper.is_file():
        errors.append(f"shared workspace asset helper is absent: {shared_asset_helper}")

    for package_id, role in sorted(role_by_id.items(), key=lambda item: item[1]):
        if role == "root":
            continue
        package = packages[package_id]
        manifest_dir = Path(package["manifest_path"]).parent
        for path in source_files(manifest_dir):
            text = path.read_text(encoding="utf-8")
            relative = path.resolve().relative_to(workspace_root).as_posix()
            for match in ROOT_FACADE_PATTERN.finditer(text):
                errors.append(
                    f"{role}: member source/test must import its owner crates directly: "
                    f"{path}:{line_number(text, match.start())}"
                )
            if relative in ALLOWED_FIXED_ASSET_LOCATORS:
                continue
            for label, pattern in (
                ("deep relative include bypasses the shared asset locator", DEEP_ASSET_INCLUDE_PATTERN),
                ("CARGO_MANIFEST_DIR parent traversal bypasses the shared asset locator", MANIFEST_PARENT_PATH_PATTERN),
            ):
                for match in pattern.finditer(text):
                    errors.append(
                        f"{role}: {label}: {path}:{line_number(text, match.start())}"
                    )

    actual_snapshot = feature_snapshot(metadata)
    if not baseline_path.is_file():
        errors.append(f"feature baseline is absent: {baseline_path}")
    else:
        expected_snapshot = baseline_path.read_text(encoding="utf-8")
        if expected_snapshot != actual_snapshot:
            diff = list(
                difflib.unified_diff(
                    expected_snapshot.splitlines(),
                    actual_snapshot.splitlines(),
                    fromfile=str(baseline_path),
                    tofile="current cargo metadata",
                    lineterm="",
                )
            )
            errors.append("third-party version/feature snapshot changed:\n" + "\n".join(diff[:200]))

        expected_critical = {
            line.split("\t", 1)[0]
            for line in expected_snapshot.splitlines()
            if line and not line.startswith("#")
        }
        current_critical = {
            line.split("\t", 1)[0]
            for line in actual_snapshot.splitlines()
            if line and not line.startswith("#")
        }
        for name in sorted(CRITICAL_FEATURE_PACKAGES):
            if name not in expected_critical or name not in current_critical:
                errors.append(f"critical feature baseline does not contain {name}")

    if errors:
        for error in errors:
            print(f"crate boundary: {error}", file=sys.stderr)
        return 1

    print(
        f"Crate boundary scan passed ({len(workspace_ids)} workspace package(s), "
        f"{len(nodes)} resolved package(s))."
    )
    return 0


def main():
    if len(sys.argv) < 3:
        print(
            "usage: check-crate-boundaries.py snapshot METADATA | "
            "check METADATA BASELINE WORKSPACE_ROOT",
            file=sys.stderr,
        )
        return 2
    mode = sys.argv[1]
    metadata = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
    if mode == "snapshot" and len(sys.argv) == 3:
        sys.stdout.write(feature_snapshot(metadata))
        return 0
    if mode == "check" and len(sys.argv) == 5:
        return check(metadata, Path(sys.argv[3]), Path(sys.argv[4]))
    print("invalid arguments", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
