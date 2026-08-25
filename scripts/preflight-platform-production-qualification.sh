#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
profile=${PLATFORM_QUALIFICATION_PROFILE:-$root/contracts/platform-v1/qualification/production-release-profile.json}
candidate=${PLATFORM_CANDIDATE_MANIFEST:-}
capacity=${PLATFORM_CAPACITY_PROFILE:-}
output=${PLATFORM_QUALIFICATION_OUTPUT_DIR:-}
kubectl_bin=${PLATFORM_QUALIFICATION_KUBECTL:-kubectl}
qualification_bin=${PLATFORM_QUALIFICATION_BIN:-}

if [[ -z "$candidate" || -z "$capacity" || -z "$output" ]]; then
  echo "PLATFORM_CANDIDATE_MANIFEST, PLATFORM_CAPACITY_PROFILE and PLATFORM_QUALIFICATION_OUTPUT_DIR are required" >&2
  exit 2
fi
if [[ ! -f "$profile" || ! -f "$candidate" || ! -f "$capacity" ]]; then
  echo "qualification, capacity and candidate manifests must be readable files" >&2
  exit 2
fi
if [[ -e "$output" ]]; then
  echo "qualification output directory must be a fresh path: $output" >&2
  exit 2
fi
command -v "$kubectl_bin" >/dev/null
command -v python3 >/dev/null
command -v helm >/dev/null

if [[ -n "$qualification_bin" ]]; then
  "$qualification_bin" validate-production-candidate "$profile" "$candidate"
  "$qualification_bin" validate-production-capacity "$capacity" "$candidate"
else
  command -v cargo >/dev/null
  cargo run --locked --manifest-path "$root/Cargo.toml" \
    -p insight-platform-contracts --bin platform-qualification -- \
    validate-production-candidate "$profile" "$candidate"
  cargo run --locked --manifest-path "$root/Cargo.toml" \
    -p insight-platform-contracts --bin platform-qualification -- \
    validate-production-capacity "$capacity" "$candidate"
fi

mkdir -p "$output/raw"
cp "$profile" "$output/qualification-profile.json"
cp "$candidate" "$output/candidate-manifest.json"
cp "$capacity" "$output/capacity-profile.json"

"$kubectl_bin" version -o json >"$output/raw/kubernetes-version.json"
"$kubectl_bin" get nodes -o json >"$output/raw/nodes.json"
"$kubectl_bin" get runtimeclass runsc -o json >"$output/raw/runtimeclass-runsc.json"
"$kubectl_bin" get deployments --all-namespaces -o json >"$output/raw/deployments.json"
"$kubectl_bin" get daemonsets --all-namespaces -o json >"$output/raw/daemonsets.json"
"$kubectl_bin" get networkpolicies --all-namespaces -o json >"$output/raw/networkpolicies.json"
"$kubectl_bin" get poddisruptionbudgets --all-namespaces -o json >"$output/raw/poddisruptionbudgets.json"
"$kubectl_bin" get horizontalpodautoscalers --all-namespaces -o json >"$output/raw/horizontalpodautoscalers.json"
"$kubectl_bin" api-resources --api-group=admissionregistration.k8s.io -o name \
  >"$output/raw/admission-api-resources.txt"
if ! grep -qx 'validatingadmissionpolicies.admissionregistration.k8s.io' \
  "$output/raw/admission-api-resources.txt"; then
  echo "cluster does not serve v1 ValidatingAdmissionPolicy" >&2
  exit 1
fi

python3 "$root/scripts/check-platform-production-topology.py" \
  --version "$output/raw/kubernetes-version.json" \
  --nodes "$output/raw/nodes.json" \
  --runtime-class "$output/raw/runtimeclass-runsc.json" \
  --output "$output/topology.json"

python3 "$root/scripts/check-platform-production-workloads.py" \
  --candidate "$candidate" \
  --capacity "$capacity" \
  --deployments "$output/raw/deployments.json" \
  --daemonsets "$output/raw/daemonsets.json" \
  --networkpolicies "$output/raw/networkpolicies.json" \
  --pdbs "$output/raw/poddisruptionbudgets.json" \
  --hpas "$output/raw/horizontalpodautoscalers.json" \
  --output "$output/workloads.json"

"$kubectl_bin" config current-context >"$output/raw/kubernetes-context.txt"
"$kubectl_bin" version --client -o json >"$output/raw/kubectl-version.json"
helm version --template '{{ .Version }}' >"$output/raw/helm-version.txt"
python3 --version >"$output/raw/python-version.txt" 2>&1

echo "production qualification topology preflight passed; evidence root: $output"
