#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
output=""
environment=""
expected_revision=""
release_candidate_closure=""

usage() {
  cat <<'EOF'
Usage: scripts/run-productization-sandbox-qualification.sh --environment <path> --source-revision <sha> --output <path> [--release-candidate-closure <path>]

Runs the fail-closed real OpenSandbox Kubernetes L3 qualification and writes exact-revision
productization evidence. All PLATFORM_OPENSANDBOX_L3_* variables required by the owning L3
qualifier, KUBECONFIG, and PLATFORM_TEST_DATABASE_URL must already be set.
EOF
}

while (($# > 0)); do
  case "$1" in
    --output)
      (($# >= 2)) || { echo "--output requires a value" >&2; exit 2; }
      output=$2
      shift 2
      ;;
    --environment)
      (($# >= 2)) || { echo "--environment requires a value" >&2; exit 2; }
      environment=$2
      shift 2
      ;;
    --source-revision)
      (($# >= 2)) || { echo "--source-revision requires a value" >&2; exit 2; }
      expected_revision=$2
      shift 2
      ;;
    --release-candidate-closure)
      (($# >= 2)) || { echo "--release-candidate-closure requires a value" >&2; exit 2; }
      release_candidate_closure=$2
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unsupported option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$output" || -z "$environment" || -z "$expected_revision" ]]; then
  echo "--environment, --source-revision, and --output are required" >&2
  exit 2
fi
if [[ ! "$expected_revision" =~ ^[0-9a-f]{40}$ ]]; then
  echo "--source-revision must be an exact lowercase 40-character Git commit" >&2
  exit 2
fi
required_environment=(
  KUBECONFIG
  PLATFORM_TEST_DATABASE_URL
  PLATFORM_OPENSANDBOX_L3_API_KEY
  PLATFORM_OPENSANDBOX_L3_IMAGE
  PLATFORM_OPENSANDBOX_L3_RUNTIME_DIGEST
  PLATFORM_OPENSANDBOX_L3_PROBE_ADDRESS
  PLATFORM_OPENSANDBOX_L3_PROBE_PORT
)
for name in "${required_environment[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    printf '%s is required\n' "$name" >&2
    exit 2
  fi
done
if [[ -n "$(git -C "$root" status --porcelain)" ]]; then
  echo "exact-revision OpenSandbox evidence requires a clean Git working tree" >&2
  exit 2
fi
if [[ ! -f "$environment" || -L "$environment" ]]; then
  echo "bootstrap environment must be a regular, non-symlink file" >&2
  exit 2
fi
if [[ -n "$release_candidate_closure" && (! -f "$release_candidate_closure" || -L "$release_candidate_closure") ]]; then
  echo "release candidate closure must be a regular, non-symlink file" >&2
  exit 2
fi
environment=$(cd "$(dirname "$environment")" && pwd)/$(basename "$environment")
if [[ -n "$release_candidate_closure" ]]; then
  release_candidate_closure=$(cd "$(dirname "$release_candidate_closure")" && pwd)/$(basename "$release_candidate_closure")
fi
source_revision=$(git -C "$root" rev-parse HEAD)
if [[ "$source_revision" != "$expected_revision" ]]; then
  echo "checked-out source differs from --source-revision" >&2
  exit 2
fi
python3 - "$environment" "$source_revision" "$KUBECONFIG" <<'PY'
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

path, revision, kubeconfig = sys.argv[1:]
value = json.loads(Path(path).read_text(encoding="utf-8"))
expected = {
    "schema_version", "kind", "production", "git_commit", "platform_image_digest",
    "platform_image_repository", "platform_image_identity", "deployment_config_digest",
    "sandbox_runner_image_digest", "sandbox_runner_image_repository",
    "sandbox_runner_image_identity",
    "generated_at", "cluster_name", "kubeconfig",
}
if not isinstance(value, dict) or set(value) != expected:
    raise SystemExit("bootstrap environment is not the closed Kind environment contract")
if value["schema_version"] != 2 or value["kind"] != "insight.platform/kind-local-mechanics/v2":
    raise SystemExit("bootstrap environment kind is invalid")
if value["production"] is not False or value["git_commit"] != revision:
    raise SystemExit("bootstrap environment does not identify the current source revision")
if Path(value["kubeconfig"]).resolve() != Path(kubeconfig).resolve():
    raise SystemExit("bootstrap environment kubeconfig differs from KUBECONFIG")
for field in ("platform_image_digest", "sandbox_runner_image_digest", "deployment_config_digest"):
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", value[field]):
        raise SystemExit(f"bootstrap {field} is not exact")
identity_keys = {
    "kind", "repository", "reference", "config_digest", "index_digest", "platform",
    "platform_digest",
}
def validate_image_identity(label, identity_field, repository_field, digest_field):
    identity = value[identity_field]
    if not isinstance(identity, dict) or set(identity) != identity_keys:
        raise SystemExit(f"bootstrap {label} image identity is not closed")
    if identity["repository"] != value[repository_field]:
        raise SystemExit(f"bootstrap {label} image repository identities differ")
    if not isinstance(identity["repository"], str) or not re.fullmatch(
        r"(?:[a-z0-9.-]+(?::[0-9]+)?/)?[a-z0-9._/-]+", identity["repository"]
    ):
        raise SystemExit(f"bootstrap {label} image repository is invalid")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", identity["config_digest"]):
        raise SystemExit(f"bootstrap {label} config digest is invalid")
    if identity["platform"] not in {"linux/amd64", "linux/arm64"}:
        raise SystemExit(f"bootstrap {label} platform is unsupported")
    if identity["kind"] == "signed_release_candidate":
        if identity["platform_digest"] != value[digest_field] or not re.fullmatch(
            r"sha256:[0-9a-f]{64}", identity["index_digest"] or ""
        ):
            raise SystemExit(f"bootstrap {label} release index or host manifest identity differs")
    elif identity["kind"] == "source_oci_manifest":
        if (
            identity["index_digest"] is not None
            or identity["platform_digest"] != value[digest_field]
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", identity["platform_digest"] or "")
        ):
            raise SystemExit(f"source OCI {label} image must identify its exact host manifest")
    else:
        raise SystemExit(f"bootstrap {label} image identity kind is invalid")
    if identity["reference"] != f'{identity["repository"]}@{identity["platform_digest"]}':
        raise SystemExit(f"bootstrap {label} reference is not its exact host manifest")
    return identity

platform_identity = validate_image_identity(
    "platform", "platform_image_identity", "platform_image_repository", "platform_image_digest"
)
runner_identity = validate_image_identity(
    "Sandbox runner", "sandbox_runner_image_identity", "sandbox_runner_image_repository",
    "sandbox_runner_image_digest"
)
if runner_identity["kind"] != platform_identity["kind"] or runner_identity["platform"] != platform_identity["platform"]:
    raise SystemExit("bootstrap runtime and Sandbox runner image identity classes differ")
generated = datetime.fromisoformat(value["generated_at"].replace("Z", "+00:00"))
age = datetime.now(timezone.utc) - generated.astimezone(timezone.utc)
if age.total_seconds() < 0 or age.total_seconds() > 3 * 60 * 60:
    raise SystemExit("bootstrap environment was not created in this bounded qualification window")
if not isinstance(value["cluster_name"], str) or not value["cluster_name"]:
    raise SystemExit("bootstrap cluster name is invalid")
PY
cluster_name=$(jq -r '.cluster_name' "$environment")
if ! kind get clusters | grep -Fxq "$cluster_name"; then
  echo "bootstrap Kind cluster is not running" >&2
  exit 2
fi
if [[ "$(kubectl config current-context)" != "kind-$cluster_name" ]]; then
  echo "KUBECONFIG does not select the bootstrap Kind cluster" >&2
  exit 2
fi
kind_nodes=$(kind get nodes --name "$cluster_name" | sort)
kubectl_nodes=$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | sort)
if [[ "$kind_nodes" != "$kubectl_nodes" || "$(printf '%s\n' "$kind_nodes" | awk 'NF {count++} END {print count+0}')" -ne 3 ]]; then
  echo "bootstrap environment and live three-node Kind inventory differ" >&2
  exit 2
fi
if [[ -n "$(kubectl -n platform-sandbox-workloads get batchsandboxes -o name)" ]]; then
  echo "fresh qualification requires no pre-existing BatchSandbox resources" >&2
  exit 2
fi

mkdir -p "$(dirname "$output")"
output=$(cd "$(dirname "$output")" && pwd)/$(basename "$output")
rm -f "$output"

qualification_run_id=$(python3 - <<'PY'
import hashlib
import secrets

print(f"sha256:{hashlib.sha256(secrets.token_bytes(32)).hexdigest()}")
PY
)
started_at=$(python3 - <<'PY'
from datetime import datetime, timezone

print(datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"))
PY
)
python3 - "$environment" "$started_at" <<'PY'
from datetime import datetime
import json
from pathlib import Path
import sys

environment_path, started_at = sys.argv[1:]
environment = json.loads(Path(environment_path).read_bytes())
generated_at = datetime.fromisoformat(environment["generated_at"].replace("Z", "+00:00"))
started_at = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
age = started_at - generated_at
if age.total_seconds() < 0 or age.total_seconds() > 3 * 60 * 60:
    raise SystemExit("bootstrap environment is outside the raw L3 qualification window")
PY
"$root/scripts/qualify-platform-sandbox-l3.sh"

control_namespace=${PLATFORM_OPENSANDBOX_L3_CONTROL_NAMESPACE:-platform-sandbox}
workloads_namespace=${PLATFORM_OPENSANDBOX_L3_WORKLOADS_NAMESPACE:-platform-sandbox-workloads}
for deployment in sandbox-dispatcher opensandbox-server opensandbox-controller; do
  kubectl -n "$control_namespace" wait --for=condition=Available \
    "deployment/$deployment" --timeout=120s >/dev/null
done
if [[ -n "$(kubectl -n "$workloads_namespace" get batchsandboxes -o name)" ]]; then
  echo "OpenSandbox qualification left BatchSandbox resources behind" >&2
  exit 1
fi
finished_at=$(python3 - <<'PY'
from datetime import datetime, timezone

print(datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"))
PY
)

chart_digest=$(python3 - "$root/deploy/helm/insight-platform-sandbox" <<'PY'
import hashlib
from pathlib import Path
import sys

root = Path(sys.argv[1])
digest = hashlib.sha256()
for path in sorted(item for item in root.rglob("*") if item.is_file()):
    relative = path.relative_to(root).as_posix().encode("utf-8")
    payload = path.read_bytes()
    digest.update(len(relative).to_bytes(8, "big"))
    digest.update(relative)
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)
print(f"sha256:{digest.hexdigest()}")
PY
)
environment_digest=$(python3 - "$environment" <<'PY'
import hashlib
from pathlib import Path
import sys
print(f"sha256:{hashlib.sha256(Path(sys.argv[1]).read_bytes()).hexdigest()}")
PY
)
cluster_name=$(jq -r '.cluster_name' "$environment")
platform_image_digest=$(jq -r '.platform_image_digest' "$environment")
release_candidate_json=null
if [[ -n "$release_candidate_closure" ]]; then
  release_candidate_json=$(python3 - \
    "$release_candidate_closure" "$source_revision" "$environment" <<'PY'
import json
from pathlib import Path
import re
import sys

closure_path, revision, environment_path = sys.argv[1:]
closure = json.loads(Path(closure_path).read_bytes())
expected = {
    "schema_version", "kind", "source_revision", "version", "release_bundle_digest",
    "cli", "console_assets", "images",
}
if not isinstance(closure, dict) or set(closure) != expected:
    raise SystemExit("release candidate closure is not closed")
if closure["schema_version"] != 1 or closure["kind"] != "insight.productization.release-candidate/v1":
    raise SystemExit("release candidate closure kind is invalid")
if closure["source_revision"] != revision or not re.fullmatch(r"sha256:[0-9a-f]{64}", closure["release_bundle_digest"]):
    raise SystemExit("release candidate closure does not bind the qualification revision")
images = closure["images"]
if not isinstance(images, dict) or set(images) != {"runtime", "sandbox_runner", "console"}:
    raise SystemExit("release candidate image closure is invalid")
component_keys = {"subject", "index_digest", "platform", "platform_digest"}
for name, component in images.items():
    if not isinstance(component, dict) or set(component) != component_keys:
        raise SystemExit(f"release candidate {name} identity is not closed")
    if component["platform"] not in {"linux/amd64", "linux/arm64"}:
        raise SystemExit(f"release candidate {name} platform is unsupported")
    if any(not re.fullmatch(r"sha256:[0-9a-f]{64}", component[field]) for field in ("index_digest", "platform_digest")):
        raise SystemExit(f"release candidate {name} digest is invalid")
environment = json.loads(Path(environment_path).read_bytes())
runtime = images["runtime"]
identity = environment["platform_image_identity"]
if identity["kind"] != "signed_release_candidate" or any(
    identity[field] != runtime[field]
    for field in ("repository", "index_digest", "platform", "platform_digest")
    if field != "repository"
):
    raise SystemExit("bootstrap runtime image differs from the signed release candidate")
if identity["repository"] != runtime["subject"]:
    raise SystemExit("bootstrap runtime repository differs from the signed release candidate")
runner = images["sandbox_runner"]
runner_identity = environment["sandbox_runner_image_identity"]
if runner_identity["kind"] != "signed_release_candidate" or any(
    runner_identity[field] != runner[field]
    for field in ("index_digest", "platform", "platform_digest")
):
    raise SystemExit("bootstrap Sandbox runner image differs from the signed release candidate")
if runner_identity["repository"] != runner["subject"]:
    raise SystemExit("bootstrap Sandbox runner repository differs from the signed release candidate")
result = {"release_bundle_digest": closure["release_bundle_digest"]}
result.update(images)
print(json.dumps(result, sort_keys=True, separators=(",", ":")))
PY
  )
else
  if [[ "$(jq -r '.platform_image_identity.kind' "$environment")" != "source_oci_manifest" || \
        "$(jq -r '.sandbox_runner_image_identity.kind' "$environment")" != "source_oci_manifest" ]]; then
    echo "signed release bootstrap requires --release-candidate-closure" >&2
    exit 2
  fi
fi
python3 - \
  "$output" "$source_revision" "$PLATFORM_OPENSANDBOX_L3_RUNTIME_DIGEST" \
  "$PLATFORM_OPENSANDBOX_L3_IMAGE" "$chart_digest" "$environment_digest" \
  "$cluster_name" "$platform_image_digest" "$release_candidate_json" \
  "$qualification_run_id" "$started_at" "$finished_at" <<'PY'
import json
from pathlib import Path
import platform
import sys

(
    output,
    revision,
    runtime_digest,
    package_image,
    chart_digest,
    environment_digest,
    cluster_name,
    platform_image_digest,
    release_candidate_json,
    qualification_run_id,
    started_at,
    finished_at,
) = sys.argv[1:]
system = platform.system()
rust_os = {
    "Darwin": "macos",
    "Linux": "linux",
    "Windows": "windows",
}.get(system, system.lower())
machine = platform.machine().lower()
rust_arch = {
    "amd64": "x86_64",
    "arm64": "aarch64",
}.get(machine, machine)
evidence = {
    "schema_version": 1,
    "report_kind": "insight.productization.opensandbox-qualification/v1",
    "source_revision": revision,
    "qualification_run_id": qualification_run_id,
    "started_at": started_at,
    "finished_at": finished_at,
    "environment": {
        "os": rust_os,
        "architecture": rust_arch,
        "fresh_cluster": True,
        "cluster_name": cluster_name,
    },
    "runtime_contract_digest": runtime_digest,
    "package_image": package_image,
    "platform_image_digest": platform_image_digest,
    "sandbox_chart_digest": chart_digest,
    "bootstrap_environment_digest": environment_digest,
    "release_candidate": json.loads(release_candidate_json),
    "qualifier": "scripts/qualify-platform-sandbox-l3.sh",
    "checks": [
        {"id": "opensandbox_lifecycle", "status": "passed"},
        {"id": "current_runtime_contract", "status": "passed"},
        {"id": "direct_and_disabled_network", "status": "passed"},
        {"id": "package_process_isolation", "status": "passed"},
        {"id": "deadline_limit_enforced", "status": "passed"},
        {"id": "dispatcher_recovery", "status": "passed"},
    ],
    "status": "passed",
}
Path(output).write_text(
    json.dumps(evidence, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
    encoding="utf-8",
)
PY

printf 'Productization OpenSandbox qualification passed\nevidence=%s\n' "$output"
