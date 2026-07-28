#!/usr/bin/env bash
set -euo pipefail

for command_name in kubectl helm jq openssl; do
  command -v "$command_name" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command_name" >&2
    exit 2
  }
done

namespace=${BENCH_NAMESPACE:-}
release=${BENCH_RELEASE:-}
output=${1:-}
[[ -n "$namespace" && -n "$release" && -n "$output" ]] || {
  printf 'usage: BENCH_NAMESPACE=... BENCH_RELEASE=... %s evidence.json\n' \
    "$0" >&2
  exit 2
}
for value in "$namespace" "$release"; do
  [[ "$value" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || {
    printf 'namespace/release is not a DNS label: %s\n' "$value" >&2
    exit 2
  }
done

context=$(kubectl config current-context)
[[ -n "$context" ]] || {
  printf 'kubectl current context is empty\n' >&2
  exit 2
}
namespace_name=$(kubectl get namespace "$namespace" --ignore-not-found -o name)
releases=$(helm list --all-namespaces -o json)
pvcs=$(kubectl get pvc --all-namespaces -o json)
matching_release_count=$(jq \
  --arg namespace "$namespace" \
  --arg release "$release" \
  '[.[] | select(.namespace == $namespace and .name == $release)] | length' \
  <<<"$releases")
matching_pvc_count=$(jq \
  --arg namespace "$namespace" \
  '[.items[] | select(.metadata.namespace == $namespace)] | length' \
  <<<"$pvcs")

if [[ -n "$namespace_name" ]] ||
   ((matching_release_count != 0 || matching_pvc_count != 0)); then
  printf '%s/%s is not fresh: namespace=%s releases=%s pvcs=%s\n' \
    "$namespace" \
    "$release" \
    "${namespace_name:-absent}" \
    "$matching_release_count" \
    "$matching_pvc_count" >&2
  exit 1
fi

preflight_id="$(date -u +%Y%m%dT%H%M%SZ)-$(openssl rand -hex 8)"
kubectl create namespace "$namespace" >/dev/null
kubectl annotate namespace "$namespace" \
  "iap.openai.com/qualification-preflight-id=$preflight_id" \
  "iap.openai.com/qualification-release=$release" \
  --overwrite >/dev/null
namespace_document=$(kubectl get namespace "$namespace" -o json)
mkdir -p "$(dirname "$output")"
jq -n \
  --arg captured_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg cluster_context "$context" \
  --arg namespace "$namespace" \
  --arg release "$release" \
  --arg preflight_id "$preflight_id" \
  --arg namespace_uid "$(jq -r '.metadata.uid' <<<"$namespace_document")" \
  --arg namespace_created_at \
    "$(jq -r '.metadata.creationTimestamp' <<<"$namespace_document")" \
  '{
    passed: true,
    captured_at: $captured_at,
    cluster_context: $cluster_context,
    namespace: $namespace,
    release: $release,
    namespace_absent_before_preflight: true,
    matching_helm_releases_before_preflight: 0,
    matching_pvcs_before_preflight: 0,
    preflight_id: $preflight_id,
    namespace_uid: $namespace_uid,
    namespace_created_at: $namespace_created_at
  }' >"$output"
printf 'Fresh qualification namespace prepared: %s (evidence %s)\n' \
  "$namespace" "$output"
