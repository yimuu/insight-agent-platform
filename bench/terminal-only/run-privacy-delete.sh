#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command jq
require_command python3
require_nonempty BASE_URL "${BASE_URL:-}"

batch_id=${PRIVACY_BATCH_ID:-"$(date -u +%Y%m%dT%H%M%S)-${RANDOM}"}
[[ "$batch_id" =~ ^[A-Za-z0-9-]+$ ]] || {
  printf 'PRIVACY_BATCH_ID may contain only letters, digits, and hyphens\n' >&2
  exit 2
}
tenant_id="privacy-tenant-$batch_id"
user_id="privacy-user-$batch_id"
control_tenant_id="privacy-control-tenant-$batch_id"
control_user_id="privacy-control-user-$batch_id"
secret_marker="privacy-content-$batch_id"
active_key_version=${TENANT_ARTIFACT_KEY_VERSION:-qualification-v1}
[[ "$active_key_version" =~ ^[A-Za-z0-9._-]{1,64}$ ]] || {
  printf 'TENANT_ARTIFACT_KEY_VERSION is invalid\n' >&2
  exit 2
}
# Force both user and assistant content through the object path under the
# default 8 KiB threshold. The marker remains easy to search for leakage.
secret=$(python3 -c \
  'import sys; marker=sys.argv[1]; print(marker + ":" + "x" * 12288)' \
  "$secret_marker")
result_dir=${1:-"$terminal_bench_root/bench/results/terminal-only-privacy-$batch_id"}
mkdir -p "$result_dir"
assert_postgres_durability

stream_port_forward_pid=
stop_privacy_stream_port_forward() {
  local reason=${1:-exit}
  [[ -n "$stream_port_forward_pid" ]] || return 0
  local pid=$stream_port_forward_pid
  local alive_before_stop=false
  local kill_status=null
  local wait_status=0
  local cleanup_confirmed=false
  if kill -0 "$pid" 2>/dev/null; then
    alive_before_stop=true
    kill_status=0
    kill "$pid" >/dev/null 2>&1 || kill_status=$?
  fi
  wait "$pid" >/dev/null 2>&1 || wait_status=$?
  if ! kill -0 "$pid" 2>/dev/null; then
    cleanup_confirmed=true
  fi
  jq -n \
    --arg reason "$reason" \
    --argjson pid "$pid" \
    --argjson alive_before_stop "$alive_before_stop" \
    --argjson kill_status "$kill_status" \
    --argjson wait_status "$wait_status" \
    --argjson cleanup_confirmed "$cleanup_confirmed" \
    '{
      reason: $reason,
      pid: $pid,
      alive_before_stop: $alive_before_stop,
      kill_status: $kill_status,
      wait_status: $wait_status,
      reaped: $cleanup_confirmed,
      cleanup_confirmed: $cleanup_confirmed
    }' >"$result_dir/stream-api-port-forward-cleanup.json" || true
  stream_port_forward_pid=
}
trap stop_privacy_stream_port_forward EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

api_curl -X POST "$BASE_URL/v1/conversations" \
  -H 'content-type: application/json' \
  -H "x-request-id: privacy-create-$batch_id" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id" \
  --data-binary "{\"agent_id\":\"${AGENT_ID:-conversation_demo}\"}" \
  >"$result_dir/create.json"
conversation_id=$(jq -er '.data.conversation_id' "$result_dir/create.json")
api_curl -X POST "$BASE_URL/v1/conversations/$conversation_id/messages" \
  -H 'content-type: application/json' \
  -H "x-request-id: privacy-turn-$batch_id" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id" \
  --data-binary "{\"content\":{\"text\":\"$secret\"}}" \
  >"$result_dir/turn.json"
run_id=$(jq -er '.data.run.run_id' "$result_dir/turn.json")

deadline=$((SECONDS + ${RUN_TIMEOUT_SECONDS:-30}))
status=
while (( SECONDS < deadline )); do
  api_curl "$BASE_URL/v1/conversations/$conversation_id/messages?limit=2" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $user_id" \
    >"$result_dir/messages-before-delete.json"
  if jq -e --arg run_id "$run_id" '
    any(.data.messages[];
        .role == "assistant" and .run_id == $run_id)
  ' "$result_dir/messages-before-delete.json" >/dev/null; then
    status=completed
    break
  fi
  sleep 0.05
done
[[ "$status" == "completed" || "$status" == "succeeded" ]] || {
  printf 'privacy fixture run did not complete\n' >&2
  exit 1
}

api_curl -X POST "$BASE_URL/v1/conversations" \
  -H 'content-type: application/json' \
  -H "x-request-id: privacy-control-create-$batch_id" \
  -H "x-tenant-id: $control_tenant_id" \
  -H "x-user-id: $control_user_id" \
  --data-binary "{\"agent_id\":\"${AGENT_ID:-conversation_demo}\"}" \
  >"$result_dir/control-create.json"
control_conversation_id=$(jq -er '.data.conversation_id' \
  "$result_dir/control-create.json")
api_curl -X POST \
  "$BASE_URL/v1/conversations/$control_conversation_id/messages" \
  -H 'content-type: application/json' \
  -H "x-request-id: privacy-control-turn-$batch_id" \
  -H "x-tenant-id: $control_tenant_id" \
  -H "x-user-id: $control_user_id" \
  --data-binary "{\"content\":{\"text\":\"$secret\"}}" \
  >"$result_dir/control-turn.json"
control_run_id=$(jq -er '.data.run.run_id' "$result_dir/control-turn.json")
deadline=$((SECONDS + ${RUN_TIMEOUT_SECONDS:-30}))
control_status=
while (( SECONDS < deadline )); do
  api_curl \
    "$BASE_URL/v1/conversations/$control_conversation_id/messages?limit=2" \
    -H "x-tenant-id: $control_tenant_id" \
    -H "x-user-id: $control_user_id" \
    >"$result_dir/control-messages-before-delete.json"
  if jq -e --arg run_id "$control_run_id" '
    any(.data.messages[];
        .role == "assistant" and .run_id == $run_id)
  ' "$result_dir/control-messages-before-delete.json" >/dev/null; then
    api_curl "$BASE_URL/v1/runs/$control_run_id" \
      -H "x-tenant-id: $control_tenant_id" \
      -H "x-user-id: $control_user_id" \
      >"$result_dir/control-run-before-delete.json"
    control_status=$(jq -er '.data.status' \
      "$result_dir/control-run-before-delete.json")
    case "$control_status" in
      completed|succeeded) break ;;
      failed|cancelled|timed_out|interrupted)
        printf 'privacy control run reached unexpected state %s\n' \
          "$control_status" >&2
        exit 1
        ;;
    esac
  fi
  sleep 0.05
done
[[ "$control_status" == "completed" || "$control_status" == "succeeded" ]] || {
  printf 'privacy control run did not complete\n' >&2
  exit 1
}

postgres_command -qAt -c "
  SELECT jsonb_build_object(
    'user_content_ref', (
      SELECT content_ref
      FROM conversation_messages
      WHERE conversation_id='$conversation_id'
        AND role='user'
        AND content_ref IS NOT NULL
      ORDER BY message_order
      LIMIT 1
    ),
    'input_refs', coalesce((
      SELECT jsonb_agg(a.input_ref ORDER BY a.run_id)
      FROM terminal_run_admissions a
      WHERE a.conversation_id='$conversation_id' AND a.input_ref IS NOT NULL
    ), '[]'::jsonb),
    'content_refs', coalesce((
      SELECT jsonb_agg(content_ref ORDER BY message_order)
      FROM conversation_messages
      WHERE conversation_id='$conversation_id' AND content_ref IS NOT NULL
    ), '[]'::jsonb),
    'output_refs', coalesce((
      SELECT jsonb_agg(r.output_ref ORDER BY r.run_id)
      FROM terminal_run_results r
      JOIN terminal_run_admissions a USING (run_id)
      WHERE a.conversation_id='$conversation_id' AND r.output_ref IS NOT NULL
    ), '[]'::jsonb)
  )::text;
" >"$result_dir/object-refs-before-delete.json"
postgres_command -qAt -c "
  SELECT jsonb_build_object(
    'user_content_ref', (
      SELECT content_ref
      FROM conversation_messages
      WHERE conversation_id='$control_conversation_id'
        AND role='user'
        AND content_ref IS NOT NULL
      ORDER BY message_order
      LIMIT 1
    ),
    'input_refs', coalesce((
      SELECT jsonb_agg(a.input_ref ORDER BY a.run_id)
      FROM terminal_run_admissions a
      WHERE a.conversation_id='$control_conversation_id'
        AND a.input_ref IS NOT NULL
    ), '[]'::jsonb),
    'content_refs', coalesce((
      SELECT jsonb_agg(content_ref ORDER BY message_order)
      FROM conversation_messages
      WHERE conversation_id='$control_conversation_id'
        AND content_ref IS NOT NULL
    ), '[]'::jsonb),
    'output_refs', coalesce((
      SELECT jsonb_agg(r.output_ref ORDER BY r.run_id)
      FROM terminal_run_results r
      JOIN terminal_run_admissions a USING (run_id)
      WHERE a.conversation_id='$control_conversation_id'
        AND r.output_ref IS NOT NULL
    ), '[]'::jsonb)
  )::text;
" >"$result_dir/control-object-refs.json"
object_ref_count=$(jq '
  (.input_refs | length) + (.content_refs | length) + (.output_refs | length)
' "$result_dir/object-refs-before-delete.json")
(( object_ref_count >= 2 )) || {
  printf 'privacy fixture did not exercise large object-backed content\n' >&2
  exit 1
}
control_object_ref_count=$(jq '
  (.input_refs | length) + (.content_refs | length) + (.output_refs | length)
' "$result_dir/control-object-refs.json")
(( control_object_ref_count >= 2 )) || {
  printf 'privacy control did not exercise large object-backed content\n' >&2
  exit 1
}
target_user_hash=$(jq -er '.user_content_ref | fromjson | .content_hash' \
  "$result_dir/object-refs-before-delete.json")
control_user_hash=$(jq -er '.user_content_ref | fromjson | .content_hash' \
  "$result_dir/control-object-refs.json")
[[ "$target_user_hash" != "$control_user_hash" ]] || {
  printf 'tenant-scoped identical content reused the same object hash\n' >&2
  exit 1
}
jq -r '
  [.input_refs[], .content_refs[], .output_refs[]] |
  .[] as $raw |
  $raw,
  (($raw | fromjson) | .. | strings)
' "$result_dir/object-refs-before-delete.json" |
  awk 'length > 0 && !seen[$0]++' >"$result_dir/target-ref-needles.txt"
[[ -s "$result_dir/target-ref-needles.txt" ]] || {
  printf 'privacy target reference scan has no needles\n' >&2
  exit 1
}

assert_no_target_material() {
  local response_file=$1
  local description=$2
  if grep -Fq "$secret_marker" "$response_file"; then
    printf 'deleted message content leaked through %s\n' "$description" >&2
    exit 1
  fi
  if grep -Fqf "$result_dir/target-ref-needles.txt" "$response_file"; then
    printf 'deleted object reference leaked through %s\n' "$description" >&2
    exit 1
  fi
}

artifact_object_exists() {
  local reference=$1
  local hash
  hash=$(jq -er '.content_hash | sub("^sha256:"; "")' <<<"$reference")
  if [[ -n "${ARTIFACT_ROOT:-}" ]]; then
    [[ -d "$ARTIFACT_ROOT" ]] || return 2
    [[ -f "$ARTIFACT_ROOT/${hash:0:2}/$hash" ]] && return 0
    return 1
  fi
  require_command kubectl
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local selector=${BENCH_RUNTIME_SELECTOR:-app.kubernetes.io/component=runtime}
  local pod
  pod=$(kubectl -n "$namespace" get pods -l "$selector" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}') || return 2
  [[ -n "$pod" ]] || return 2
  local state
  state=$(kubectl -n "$namespace" exec "$pod" -- sh -ec '
    if [ -f "$1" ]; then
      printf "exists\n"
    else
      printf "missing\n"
    fi
  ' sh "${BENCH_ARTIFACT_ROOT:-/data/artifacts}/${hash:0:2}/$hash") ||
    return 2
  case "$state" in
    exists) return 0 ;;
    missing) return 1 ;;
    *) return 2 ;;
  esac
}

capture_artifact_object_raw() {
  local reference=$1
  local destination=$2
  local hash
  hash=$(jq -er '.content_hash | sub("^sha256:"; "")' <<<"$reference")
  if [[ -n "${ARTIFACT_ROOT:-}" ]]; then
    cp "$ARTIFACT_ROOT/${hash:0:2}/$hash" "$destination"
    return
  fi
  require_command kubectl
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local selector=${BENCH_RUNTIME_SELECTOR:-app.kubernetes.io/component=runtime}
  local pod
  pod=$(kubectl -n "$namespace" get pods -l "$selector" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}')
  [[ -n "$pod" ]] || {
    printf 'no runtime Pod can read the Artifact store\n' >&2
    return 1
  }
  kubectl -n "$namespace" exec "$pod" -- \
    cat "${BENCH_ARTIFACT_ROOT:-/data/artifacts}/${hash:0:2}/$hash" \
    >"$destination"
}

encryption_evidence_dir="$result_dir/encryption-evidence"
mkdir -p "$encryption_evidence_dir"
target_encryption_index=0
while IFS= read -r reference; do
  if artifact_object_exists "$reference"; then
    target_encryption_index=$((target_encryption_index + 1))
    raw_object="$encryption_evidence_dir/target-$target_encryption_index.bin"
    capture_artifact_object_raw "$reference" "$raw_object"
    python3 "$script_dir/encrypted_artifact_probe.py" \
      --input "$raw_object" \
      --tenant-id "$tenant_id" \
      --marker "$secret_marker" \
      --expected-key-version "$active_key_version" \
      --output \
        "$encryption_evidence_dir/target-$target_encryption_index.json"
    rm -f "$raw_object"
  else
    probe_status=$?
    if (( probe_status == 1 )); then
      printf 'referenced privacy object was absent before deletion\n' >&2
    else
      printf 'privacy object probe failed before deletion\n' >&2
    fi
    exit 1
  fi
done < <(jq -cr '.input_refs[], .content_refs[], .output_refs[]' \
  "$result_dir/object-refs-before-delete.json")
control_encryption_index=0
while IFS= read -r reference; do
  if artifact_object_exists "$reference"; then
    control_encryption_index=$((control_encryption_index + 1))
    raw_object="$encryption_evidence_dir/control-$control_encryption_index.bin"
    capture_artifact_object_raw "$reference" "$raw_object"
    python3 "$script_dir/encrypted_artifact_probe.py" \
      --input "$raw_object" \
      --tenant-id "$control_tenant_id" \
      --marker "$secret_marker" \
      --expected-key-version "$active_key_version" \
      --output \
        "$encryption_evidence_dir/control-$control_encryption_index.json"
    rm -f "$raw_object"
  else
    probe_status=$?
    if (( probe_status == 1 )); then
      printf 'referenced control object was absent before target deletion\n' >&2
    else
      printf 'control object probe failed before target deletion\n' >&2
    fi
    exit 1
  fi
done < <(jq -cr '.input_refs[], .content_refs[], .output_refs[]' \
  "$result_dir/control-object-refs.json")
(( target_encryption_index == object_ref_count &&
   control_encryption_index == control_object_ref_count )) || {
  printf 'tenant encryption inspection did not cover every object reference\n' >&2
  exit 1
}
jq -s --arg active_key_version "$active_key_version" '
  select(length > 0) |
  {
    passed: (
      all(.[]; .passed == true) and
      (map(.active_key_version) | unique) == [$active_key_version]
    ),
    inspected_objects: length,
    magic: "IAPTEA01",
    active_key_version: $active_key_version,
    tenant_id_plaintext_absent:
      all(.[]; .tenant_id_plaintext_absent == true),
    marker_plaintext_absent:
      all(.[]; .marker_plaintext_absent == true)
  }
' "$encryption_evidence_dir"/target-*.json \
  >"$encryption_evidence_dir/target-summary.json"
jq -s --arg active_key_version "$active_key_version" '
  select(length > 0) |
  {
    passed: (
      all(.[]; .passed == true) and
      (map(.active_key_version) | unique) == [$active_key_version]
    ),
    inspected_objects: length,
    magic: "IAPTEA01",
    active_key_version: $active_key_version,
    tenant_id_plaintext_absent:
      all(.[]; .tenant_id_plaintext_absent == true),
    marker_plaintext_absent:
      all(.[]; .marker_plaintext_absent == true)
  }
' "$encryption_evidence_dir"/control-*.json \
  >"$encryption_evidence_dir/control-summary.json"
jq -n \
  --slurpfile target "$encryption_evidence_dir/target-summary.json" \
  --slurpfile control "$encryption_evidence_dir/control-summary.json" \
  '{
    passed: (
      $target[0].passed == true and
      $control[0].passed == true and
      $target[0].active_key_version == $control[0].active_key_version
    ),
    magic: "IAPTEA01",
    active_key_version: $target[0].active_key_version,
    target: $target[0],
    control: $control[0],
    secret_key_material_saved: false
  }' >"$result_dir/tenant-encryption-report.json"
jq -e '.passed == true and .secret_key_material_saved == false' \
  "$result_dir/tenant-encryption-report.json" >/dev/null

api_curl -X DELETE "$BASE_URL/v1/conversations/$conversation_id" \
  -H "x-request-id: privacy-delete-$batch_id" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id" \
  >"$result_dir/delete.json"
jq -e '.data.deleted == true' "$result_dir/delete.json" >/dev/null

get_status=$(curl --silent --show-error \
  --output "$result_dir/get-after-delete.json" \
  --write-out '%{http_code}' \
  "$BASE_URL/v1/conversations/$conversation_id" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id")
case "$get_status" in
  404|410) ;;
  *)
  printf 'deleted conversation remained readable (HTTP %s)\n' "$get_status" >&2
  exit 1
  ;;
esac
messages_status=$(curl --silent --show-error \
  --output "$result_dir/messages-after-delete.json" \
  --write-out '%{http_code}' \
  "$BASE_URL/v1/conversations/$conversation_id/messages?limit=10" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id")
case "$messages_status" in
  404|410) ;;
  *)
    printf 'deleted Conversation messages remained readable (HTTP %s)\n' \
      "$messages_status" >&2
    exit 1
    ;;
esac
run_status=$(curl --silent --show-error \
  --output "$result_dir/run-after-delete.json" \
  --write-out '%{http_code}' \
  "$BASE_URL/v1/runs/$run_id" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id")
case "$run_status" in
  200|404|410) ;;
  *)
    printf 'deleted Run lookup returned unexpected HTTP %s\n' "$run_status" >&2
    exit 1
    ;;
esac
assert_no_target_material "$result_dir/get-after-delete.json" \
  'Conversation GET'
assert_no_target_material "$result_dir/messages-after-delete.json" \
  'Conversation messages GET'
assert_no_target_material "$result_dir/run-after-delete.json" 'Run GET'

postgres_command -qAt -c "
  SELECT jsonb_build_object(
    'conversations', (
      SELECT count(*) FROM conversations
      WHERE conversation_id='$conversation_id'
    ),
    'messages', (
      SELECT count(*) FROM conversation_messages
      WHERE conversation_id='$conversation_id'
    ),
    'summaries', (
      SELECT count(*) FROM conversation_summaries
      WHERE conversation_id='$conversation_id'
    )
  )::text;
" >"$result_dir/database-after-delete.json"
jq -e '
  .conversations == 0 and .messages == 0 and .summaries == 0
' "$result_dir/database-after-delete.json" >/dev/null

deadline=$((SECONDS + ${PRIVACY_OBJECT_DELETE_TIMEOUT_SECONDS:-30}))
objects_remaining=$object_ref_count
jobs_remaining=-1
while (( SECONDS < deadline )); do
  objects_remaining=0
  while IFS= read -r reference; do
    if artifact_object_exists "$reference"; then
      objects_remaining=$((objects_remaining + 1))
    else
      probe_status=$?
      if (( probe_status != 1 )); then
        printf 'privacy object deletion probe failed\n' >&2
        exit 1
      fi
    fi
  done < <(jq -cr '.input_refs[], .content_refs[], .output_refs[]' \
    "$result_dir/object-refs-before-delete.json")
  jobs_remaining=$(postgres_command -qAt -c "
    SELECT count(*) FROM terminal_content_deletion_jobs
    WHERE tenant_id='$tenant_id';
  ")
  if (( objects_remaining == 0 && jobs_remaining == 0 )); then
    break
  fi
  sleep 0.25
done
(( objects_remaining == 0 && jobs_remaining == 0 )) || {
  printf 'privacy object deletion incomplete: %s objects, %s jobs remain\n' \
    "$objects_remaining" "$jobs_remaining" >&2
  exit 1
}

control_get_status=$(curl --silent --show-error \
  --output "$result_dir/control-messages-after-delete.json" \
  --write-out '%{http_code}' \
  "$BASE_URL/v1/conversations/$control_conversation_id/messages?limit=2" \
  -H "x-tenant-id: $control_tenant_id" \
  -H "x-user-id: $control_user_id")
[[ "$control_get_status" == "200" ]] || {
  printf 'control Conversation became unreadable (HTTP %s)\n' \
    "$control_get_status" >&2
  exit 1
}
grep -Fq "$secret_marker" "$result_dir/control-messages-after-delete.json" || {
  printf 'control Conversation lost identical content after target deletion\n' >&2
  exit 1
}
control_run_status=$(curl --silent --show-error \
  --output "$result_dir/control-run-after-delete.json" \
  --write-out '%{http_code}' \
  "$BASE_URL/v1/runs/$control_run_id" \
  -H "x-tenant-id: $control_tenant_id" \
  -H "x-user-id: $control_user_id")
[[ "$control_run_status" == "200" ]] || {
  printf 'control Run became unreadable (HTTP %s)\n' \
    "$control_run_status" >&2
  exit 1
}
jq -e '
  .data.status == "completed" or .data.status == "succeeded"
' "$result_dir/control-run-after-delete.json" >/dev/null
while IFS= read -r reference; do
  if artifact_object_exists "$reference"; then
    :
  else
    probe_status=$?
    if (( probe_status == 1 )); then
      printf 'control object was removed by target tenant privacy delete\n' >&2
    else
      printf 'control object probe failed after target tenant privacy delete\n' >&2
    fi
    exit 1
  fi
done < <(jq -cr '.input_refs[], .content_refs[], .output_refs[]' \
  "$result_dir/control-object-refs.json")

api_curl -X DELETE "$BASE_URL/v1/conversations/$control_conversation_id" \
  -H "x-request-id: privacy-control-delete-$batch_id" \
  -H "x-tenant-id: $control_tenant_id" \
  -H "x-user-id: $control_user_id" \
  >"$result_dir/control-delete.json"
jq -e '.data.deleted == true' "$result_dir/control-delete.json" >/dev/null

# Prove that DELETE also fences an already-open Attached stream. A single
# process timestamps complete SSE frames and the complete successful DELETE
# response under the same lock, so there is no shell grep/response race window.
stream_tenant_id="privacy-stream-tenant-$batch_id"
stream_user_id="privacy-stream-user-$batch_id"
api_curl -X POST "$BASE_URL/v1/conversations" \
  -H 'content-type: application/json' \
  -H "x-request-id: privacy-stream-create-$batch_id" \
  -H "x-tenant-id: $stream_tenant_id" \
  -H "x-user-id: $stream_user_id" \
  --data-binary \
    "{\"agent_id\":\"${PRIVACY_STREAM_AGENT_ID:-terminal_stream_fixture}\"}" \
  >"$result_dir/stream-create.json"
stream_conversation_id=$(jq -er '.data.conversation_id' \
  "$result_dir/stream-create.json")

# The shell and k6 honor HTTP_PROXY, while this synchronized Python probe uses
# http.client deliberately and therefore connects directly. Use a local,
# auditable tunnel to the same Kubernetes Service so the privacy race does not
# depend on a host route to the ClusterIP or on proxy-specific SSE buffering.
require_command kubectl
stream_namespace=${BENCH_NAMESPACE:-insight-bench}
stream_release=${BENCH_RELEASE:-bench}
stream_api_service=${PRIVACY_STREAM_API_SERVICE:-"${stream_release}-insight-agent-platform"}
stream_api_target_port=${PRIVACY_STREAM_API_TARGET_PORT:-3000}
[[ "$stream_api_target_port" =~ ^[1-9][0-9]*$ ]] || {
  printf 'PRIVACY_STREAM_API_TARGET_PORT must be a positive integer\n' >&2
  exit 2
}
kubectl -n "$stream_namespace" get service "$stream_api_service" -o json \
  >"$result_dir/stream-api-service.json"
jq -e \
  --arg release "$stream_release" \
  --argjson target_port "$stream_api_target_port" '
  .metadata.labels["app.kubernetes.io/instance"] == $release and
  .spec.selector["app.kubernetes.io/instance"] == $release and
  .spec.selector["app.kubernetes.io/component"] == "runtime" and
  .spec.selector["app.kubernetes.io/name"] == "insight-agent-platform" and
  ([.spec.ports[]? |
    select(
      .name == "http" and
      .protocol == "TCP" and
      .port == $target_port
    )
  ] | length) == 1
' "$result_dir/stream-api-service.json" >/dev/null || {
  printf 'privacy stream API Service identity/port %s is not qualified\n' \
    "$stream_api_target_port" >&2
  exit 1
}
kubectl -n "$stream_namespace" port-forward \
  --address 127.0.0.1 \
  "service/$stream_api_service" "0:$stream_api_target_port" \
  >"$result_dir/stream-api-port-forward.log" 2>&1 &
stream_port_forward_pid=$!
stream_api_local_port=
deadline=$((SECONDS + ${PRIVACY_STREAM_PORT_FORWARD_TIMEOUT_SECONDS:-30}))
while (( SECONDS < deadline )); do
  if ! kill -0 "$stream_port_forward_pid" 2>/dev/null; then
    cat "$result_dir/stream-api-port-forward.log" >&2
    exit 1
  fi
  stream_api_local_port=$(sed -nE \
    "s/^Forwarding from 127\\.0\\.0\\.1:([0-9]+) -> ${stream_api_target_port}\$/\\1/p" \
    "$result_dir/stream-api-port-forward.log" | head -1)
  [[ -n "$stream_api_local_port" ]] && break
  sleep 0.1
done
[[ "$stream_api_local_port" =~ ^[1-9][0-9]*$ ]] || {
  printf 'privacy stream API port-forward did not become ready\n' >&2
  cat "$result_dir/stream-api-port-forward.log" >&2
  exit 1
}
stream_probe_base_url="http://127.0.0.1:$stream_api_local_port"
api_curl "$stream_probe_base_url/health/ready" \
  >"$result_dir/stream-api-port-forward-ready.json"
jq -e '.code == "OK" and .data.status == "ok"' \
  "$result_dir/stream-api-port-forward-ready.json" >/dev/null
jq -n \
  --arg namespace "$stream_namespace" \
  --arg service "$stream_api_service" \
  --arg service_uid "$(jq -er '.metadata.uid' \
    "$result_dir/stream-api-service.json")" \
  --arg cluster_ip "$(jq -er '.spec.clusterIP' \
    "$result_dir/stream-api-service.json")" \
  --argjson selector "$(jq -ec '.spec.selector' \
    "$result_dir/stream-api-service.json")" \
  --argjson target_port "$stream_api_target_port" \
  --argjson local_port "$stream_api_local_port" \
  '{
    passed: true,
    transport: "kubectl_port_forward",
    namespace: $namespace,
    service: $service,
    service_uid: $service_uid,
    cluster_ip: $cluster_ip,
    selector: $selector,
    target_port: $target_port,
    local_port: $local_port,
    readiness_verified: true,
    cleanup_registered: true
  }' >"$result_dir/stream-api-transport.json"

python3 "$script_dir/privacy_stream_probe.py" \
  --base-url "$stream_probe_base_url" \
  --conversation-id "$stream_conversation_id" \
  --tenant-id "$stream_tenant_id" \
  --user-id "$stream_user_id" \
  --stream-request-id "privacy-stream-turn-$batch_id" \
  --delete-request-id "privacy-stream-delete-$batch_id" \
  --transcript "$result_dir/stream.sse" \
  --delete-output "$result_dir/stream-delete.json" \
  --report "$result_dir/stream-probe-report.json" \
  --start-timeout "${PRIVACY_STREAM_START_TIMEOUT_SECONDS:-30}" \
  --delete-timeout "${PRIVACY_DELETE_TIMEOUT_SECONDS:-10}" \
  --close-timeout "${PRIVACY_STREAM_CLOSE_TIMEOUT_SECONDS:-10}" \
  --stream-timeout "${PRIVACY_STREAM_MAX_SECONDS:-60}" \
  >"$result_dir/stream-probe.stdout"
stop_privacy_stream_port_forward after-probe
jq -e '.cleanup_confirmed == true and .reaped == true' \
  "$result_dir/stream-api-port-forward-cleanup.json" >/dev/null
jq -e '
  .passed == true and
  .stream_http_status == 200 and
  .delete_http_status == 200 and
  .stream_closed == true and
  .frames_after_delete == 0 and
  (.frame_counts["response.output_text.delta"] // 0) > 0
' "$result_dir/stream-probe-report.json" >/dev/null
stream_deltas_before_delete=$(jq -er \
  '.frame_counts["response.output_text.delta"]' \
  "$result_dir/stream-probe-report.json")
stream_frames_before_delete=$(jq -er '.frames_before_or_at_delete' \
  "$result_dir/stream-probe-report.json")
stream_frames_after_delete=$(jq -er '.frames_after_delete' \
  "$result_dir/stream-probe-report.json")

stream_get_status=$(curl --silent --show-error \
  --output "$result_dir/stream-get-after-delete.json" \
  --write-out '%{http_code}' \
  "$BASE_URL/v1/conversations/$stream_conversation_id" \
  -H "x-tenant-id: $stream_tenant_id" \
  -H "x-user-id: $stream_user_id")
stream_messages_status=$(curl --silent --show-error \
  --output "$result_dir/stream-messages-after-delete.json" \
  --write-out '%{http_code}' \
  "$BASE_URL/v1/conversations/$stream_conversation_id/messages?limit=10" \
  -H "x-tenant-id: $stream_tenant_id" \
  -H "x-user-id: $stream_user_id")
case "$stream_get_status/$stream_messages_status" in
  404/404|404/410|410/404|410/410) ;;
  *)
    printf 'stream-deleted Conversation remained readable (HTTP %s/%s)\n' \
      "$stream_get_status" "$stream_messages_status" >&2
    exit 1
    ;;
esac
postgres_command -qAt -c "
  SELECT jsonb_build_object(
    'conversations', (
      SELECT count(*) FROM conversations
      WHERE conversation_id='$stream_conversation_id'
    ),
    'messages', (
      SELECT count(*) FROM conversation_messages
      WHERE conversation_id='$stream_conversation_id'
    ),
    'summaries', (
      SELECT count(*) FROM conversation_summaries
      WHERE conversation_id='$stream_conversation_id'
    )
  )::text;
" >"$result_dir/stream-database-after-delete.json"
jq -e '
  .conversations == 0 and .messages == 0 and .summaries == 0
' "$result_dir/stream-database-after-delete.json" >/dev/null

jq -n \
  --argjson object_refs "$object_ref_count" \
  --argjson control_object_refs "$control_object_ref_count" \
  --argjson jobs_remaining "$jobs_remaining" \
  --argjson stream_deltas_before_delete "$stream_deltas_before_delete" \
  --argjson stream_frames_before_delete "$stream_frames_before_delete" \
  --argjson stream_frames_after_delete "$stream_frames_after_delete" \
  --arg get_status "$get_status" \
  --arg messages_status "$messages_status" \
  --arg run_status "$run_status" \
  --arg stream_get_status "$stream_get_status" \
  --arg stream_messages_status "$stream_messages_status" \
  --slurpfile tenant_encryption "$result_dir/tenant-encryption-report.json" \
  '{
    passed: true,
    large_object_path_exercised: true,
    object_refs_deleted: $object_refs,
    control_object_refs_preserved: $control_object_refs,
    tenant_scoped_content_hashes_distinct: true,
    tenant_artifact_encryption: $tenant_encryption[0],
    control_conversation_readable_after_target_delete: true,
    control_run_readable_after_target_delete: true,
    attached_stream_delete_fenced: true,
    stream_deltas_before_delete: $stream_deltas_before_delete,
    stream_frames_before_or_at_delete: $stream_frames_before_delete,
    stream_frames_after_delete: $stream_frames_after_delete,
    target_http_statuses: {
      conversation: ($get_status | tonumber),
      messages: ($messages_status | tonumber),
      run: ($run_status | tonumber)
    },
    stream_target_http_statuses: {
      conversation: ($stream_get_status | tonumber),
      messages: ($stream_messages_status | tonumber)
    },
    deletion_jobs_remaining: $jobs_remaining,
    database_rows_deleted: true,
    stream_database_rows_deleted: true,
    target_content_and_object_refs_absent_from_http: true,
    http_content_unreadable: true
  }' >"$result_dir/privacy-report.json"
assert_postgres_durability
printf 'Privacy-delete evidence: %s\n' "$result_dir"
