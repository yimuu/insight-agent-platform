#!/usr/bin/env bash
set -euo pipefail

for command_name in kubectl helm jq; do
  command -v "$command_name" >/dev/null 2>&1 || {
    printf '%s is required\n' "$command_name" >&2
    exit 2
  }
done

evidence=${1:-}
output=${2:-}
namespace=${BENCH_NAMESPACE:-}
release=${BENCH_RELEASE:-}
[[ -f "$evidence" && -n "$output" && -n "$namespace" && -n "$release" ]] || {
  printf 'usage: BENCH_NAMESPACE=... BENCH_RELEASE=... %s preflight.json validation.json\n' \
    "$0" >&2
  exit 2
}

context=$(kubectl config current-context)
namespace_document=$(kubectl get namespace "$namespace" -o json)
releases=$(helm list -n "$namespace" -o json)
pvcs=$(kubectl -n "$namespace" get pvc -o json)
preflight_id=$(jq -er '.preflight_id | strings' "$evidence")
namespace_uid=$(jq -er '.metadata.uid | strings' <<<"$namespace_document")
namespace_created_at=$(jq -er '.metadata.creationTimestamp | strings' \
  <<<"$namespace_document")

jq -e \
  --arg context "$context" \
  --arg namespace "$namespace" \
  --arg release "$release" \
  --arg preflight_id "$preflight_id" \
  --arg namespace_uid "$namespace_uid" \
  '
    .passed == true and
    .cluster_context == $context and
    .namespace == $namespace and
    .release == $release and
    .namespace_absent_before_preflight == true and
    .matching_helm_releases_before_preflight == 0 and
    .matching_pvcs_before_preflight == 0 and
    .preflight_id == $preflight_id and
    .namespace_uid == $namespace_uid
  ' "$evidence" >/dev/null

jq -e \
  --arg preflight_id "$preflight_id" \
  --arg release "$release" \
  '
    .metadata.annotations["iap.openai.com/qualification-preflight-id"]
      == $preflight_id and
    .metadata.annotations["iap.openai.com/qualification-release"] == $release
  ' <<<"$namespace_document" >/dev/null
jq -e \
  --arg release "$release" \
  '[.[] | select(.name == $release and .status == "deployed")] | length == 1' \
  <<<"$releases" >/dev/null
jq -e \
  --arg namespace_created_at "$namespace_created_at" \
  '
    (.items | length) == 2 and
    all(.items[];
      .metadata.creationTimestamp >= $namespace_created_at and
      .status.phase == "Bound") and
    any(.items[]; .metadata.name | startswith("data-")) and
    any(.items[];
      (.metadata.name | endswith("-artifacts")) and
      .spec.resources.requests.storage == "2Gi") and
    any(.items[];
      (.metadata.name | startswith("data-")) and
      .spec.resources.requests.storage == "8Gi")
  ' <<<"$pvcs" >/dev/null

mkdir -p "$(dirname "$output")"
jq -n \
  --arg validated_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg cluster_context "$context" \
  --arg namespace "$namespace" \
  --arg release "$release" \
  --arg preflight_id "$preflight_id" \
  --arg namespace_uid "$namespace_uid" \
  --arg namespace_created_at "$namespace_created_at" \
  --argjson pvc_count "$(jq '.items | length' <<<"$pvcs")" \
  '{
    passed: true,
    validated_at: $validated_at,
    cluster_context: $cluster_context,
    namespace: $namespace,
    release: $release,
    preflight_id: $preflight_id,
    namespace_uid: $namespace_uid,
    namespace_created_at: $namespace_created_at,
    deployed_release_count: 1,
    bound_fresh_pvc_count: $pvc_count,
    postgres_pvc_size: "8Gi",
    artifact_pvc_size: "2Gi"
  }' >"$output"
