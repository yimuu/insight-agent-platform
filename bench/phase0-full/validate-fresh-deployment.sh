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
# Helm 4 lists every status by default and removed Helm 3's --all status flag.
# The unfiltered form is also correct on Helm 3 for the deployed release that
# this validator requires below.
releases=$(helm list -n "$namespace" -o json)
values=$(helm get values "$release" -n "$namespace" --all -o json)
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

# Validate the effective Helm configuration, not only the checked-in overlay.
# Do not persist the whole values document because it may contain credentials.
jq -e '
  .replicaCount == 1 and
  .agents.enabled == ["action_demo"] and
  .qualification.enabled == false and
  .runtime.defaultPersistenceMode == "full" and
  .runtime.terminalOnly.enabled == false and
  .artifacts.persistence.enabled == true and
  .artifacts.persistence.size == "2Gi" and
  .postgresql.enabled == true and
  .postgresql.persistence.enabled == true and
  .postgresql.persistence.size == "24Gi" and
  .postgresql.maxWalSize == "4GB" and
  .postgresql.walKeepSize == "8GB" and
  .postgresql.checkpointTimeout == "30min"
' <<<"$values" >/dev/null

jq -e \
  --arg namespace_created_at "$namespace_created_at" \
  '
    (.items | length) == 2 and
    all(.items[];
      .metadata.creationTimestamp >= $namespace_created_at and
      .status.phase == "Bound") and
    any(.items[];
      (.metadata.name | startswith("data-")) and
      .spec.resources.requests.storage == "24Gi") and
    any(.items[];
      (.metadata.name | endswith("-artifacts")) and
      .spec.resources.requests.storage == "2Gi")
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
  '{
    passed: true,
    validated_at: $validated_at,
    cluster_context: $cluster_context,
    namespace: $namespace,
    release: $release,
    preflight_id: $preflight_id,
    namespace_uid: $namespace_uid,
    namespace_created_at: $namespace_created_at,
    deployment: {
      replicas: 1,
      agents: ["action_demo"],
      persistence_mode: "full",
      terminal_only_enabled: false,
      qualification_enabled: false
    },
    storage: {
      postgres_pvc_size: "24Gi",
      artifact_pvc_size: "2Gi",
      postgres_max_wal_size: "4GB",
      postgres_wal_keep_size: "8GB",
      postgres_checkpoint_timeout: "30min"
    },
    bound_fresh_pvc_count: 2
  }' >"$output"
