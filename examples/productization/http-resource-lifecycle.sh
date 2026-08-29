#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: examples/productization/http-resource-lifecycle.sh \
  --project <local-project> --file <insight-apply-manifest.json> \
  [--timeout-seconds <1..3600>]

Executes the same public /v1 Resource lifecycle as `insight apply` using only curl and jq.
The local profile must already be running. The script writes one closed JSON report to stdout and
never prints the local OIDC token.
USAGE
}

project=""
manifest=""
timeout_seconds=120
while (($# > 0)); do
  case "$1" in
    --project)
      (($# >= 2)) || { usage >&2; exit 2; }
      project=$2
      shift 2
      ;;
    --file)
      (($# >= 2)) || { usage >&2; exit 2; }
      manifest=$2
      shift 2
      ;;
    --timeout-seconds)
      (($# >= 2)) || { usage >&2; exit 2; }
      timeout_seconds=$2
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      usage >&2
      exit 2
      ;;
  esac
done

[[ -n "$project" && -n "$manifest" && -d "$project" && -f "$manifest" ]] || {
  usage >&2
  exit 2
}
[[ "$timeout_seconds" =~ ^[0-9]+$ ]] \
  && ((timeout_seconds >= 1 && timeout_seconds <= 3600)) || {
  printf 'timeout must be in 1..=3600 seconds\n' >&2
  exit 2
}
for dependency in curl jq od awk tr tail mktemp; do
  command -v "$dependency" >/dev/null 2>&1 || {
    printf 'required command is unavailable: %s\n' "$dependency" >&2
    exit 2
  }
done

profile="$project/.insight/runtime/profile.json"
token_path="$project/.insight/identity/developer-access-token.jwt"
[[ -f "$profile" && -f "$token_path" ]] || {
  printf 'local profile or identity is absent; run insight init/dev first\n' >&2
  exit 2
}

jq -e '
  .schema_version == 1 and
  .kind == "insight.platform.apply/v1" and
  (.resource_noun | IN("agents", "skills", "capabilities", "contexts", "models", "mcp-servers", "policies", "sandboxes")) and
  (.create | type == "object") and
  (.publish | type == "object") and
  (.deployment.environment | type == "string" and length > 0) and
  (.deployment.closure | type == "object")
' "$manifest" >/dev/null || {
  printf 'manifest is not the closed insight.platform.apply/v1 envelope\n' >&2
  exit 2
}

management_port=$(jq -er '.ports.gateway_management | numbers | select(. >= 1 and . <= 65535)' "$profile")
base_url="http://127.0.0.1:${management_port}"
token=$(tr -d '\r\n' < "$token_path")
[[ -n "$token" && "$token" != *[[:space:]]* ]] || {
  printf 'local access token is empty or malformed\n' >&2
  exit 2
}

work=$(mktemp -d "${TMPDIR:-/tmp}/insight-http-lifecycle.XXXXXX")
cleanup() {
  token=""
  rm -rf "$work"
}
trap cleanup EXIT HUP INT TERM

random_hex() {
  local bytes=$1
  od -An -N "$bytes" -tx1 /dev/urandom | tr -d ' \n'
}

header_value() {
  local file=$1
  local name=$2
  awk -v wanted="$name" '
    tolower($1) == tolower(wanted ":") {
      sub(/^[^:]+:[[:space:]]*/, "")
      sub(/\r$/, "")
      print
    }
  ' "$file" | tail -n 1
}

require_json_envelope() {
  local step=$1
  local expected_trace=${2:-}
  local headers="$work/$step.headers"
  local body="$work/$step.json"
  local content_type cache_control response_trace
  content_type=$(header_value "$headers" content-type)
  cache_control=$(header_value "$headers" cache-control)
  response_trace=$(header_value "$headers" trace-id)
  [[ "$content_type" == application/json* ]] || {
    printf '%s response Content-Type is not application/json\n' "$step" >&2
    exit 1
  }
  [[ "$cache_control" == "no-store, private, max-age=0" ]] || {
    printf '%s response Cache-Control is not the closed private no-store value\n' "$step" >&2
    exit 1
  }
  [[ "$response_trace" =~ ^[0-9a-f]{32}$ ]] || {
    printf '%s response trace-id is invalid\n' "$step" >&2
    exit 1
  }
  [[ -z "$expected_trace" || "$response_trace" == "$expected_trace" ]] || {
    printf '%s response trace-id does not match traceparent\n' "$step" >&2
    exit 1
  }
  jq -e . "$body" >/dev/null || {
    printf '%s response is not JSON\n' "$step" >&2
    exit 1
  }
}

request_json() {
  local step=$1
  local method=$2
  local path=$3
  local expected_status=$4
  local body_file=${5:-}
  local receipt=${6:-}
  local if_match=${7:-}
  local trace=""
  local span=""
  local -a args
  args=(
    --silent --show-error
    --request "$method"
    --connect-timeout 5 --max-time 30
    --max-redirs 0 --noproxy '*'
    --header 'accept: application/json'
    --header "authorization: Bearer $token"
    --dump-header "$work/$step.headers"
    --output "$work/$step.json"
    --write-out '%{http_code}'
  )
  if [[ "$method" == POST || "$method" == PUT ]]; then
    trace=$(random_hex 16)
    span=$(random_hex 8)
    args+=(--header "traceparent: 00-${trace}-${span}-01")
    [[ -n "$receipt" ]] && args+=(--header "idempotency-key: $receipt")
    [[ -n "$if_match" ]] && args+=(--header "if-match: $if_match")
    if [[ -n "$body_file" ]]; then
      args+=(--header 'content-type: application/json' --data-binary "@$body_file")
    fi
  fi
  local status
  status=$(curl "${args[@]}" "${base_url}${path}")
  [[ "$status" == "$expected_status" ]] || {
    printf '%s expected HTTP %s but received %s\n' "$step" "$expected_status" "$status" >&2
    jq -c '{status, code, request_id, trace_id, retryable, retry_after_ms, detail}' \
      "$work/$step.json" >&2 2>/dev/null || true
    exit 1
  }
  require_json_envelope "$step" "$trace"
}

body_etag() {
  local step=$1
  local header_etag body_etag
  header_etag=$(header_value "$work/$step.headers" etag)
  body_etag=$(jq -er '.etag | strings | select(length > 0)' "$work/$step.json")
  [[ "$header_etag" == "$body_etag" ]] || {
    printf '%s body/header ETag mismatch\n' "$step" >&2
    exit 1
  }
  printf '%s' "$body_etag"
}

manifest_hash() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$manifest" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$manifest" | awk '{print $1}'
  else
    printf 'sha256sum or shasum is required for deterministic Receipts\n' >&2
    exit 2
  fi
}

manifest_digest=$(manifest_hash)
noun=$(jq -er '.resource_noun' "$manifest")
base_path="/v1/$noun"
jq -cS '.create' "$manifest" > "$work/create.request.json"
create_receipt="insight-http-v1-${manifest_digest}-create"
request_json create POST "$base_path" 201 "$work/create.request.json" "$create_receipt"
resource_id=$(jq -er '.resource_id | strings | select(length > 0)' "$work/create.json")
create_etag=$(body_etag create)
[[ $(header_value "$work/create.headers" location) == "$base_path/$resource_id" ]] || {
  printf 'create Location does not identify the created Resource\n' >&2
  exit 1
}

# Exact Receipt replay must identify the same effect. Reusing it with different canonical input
# must produce a closed 409 Problem rather than a second Resource.
request_json create_replay POST "$base_path" 201 "$work/create.request.json" "$create_receipt"
[[ $(jq -er '.resource_id' "$work/create_replay.json") == "$resource_id" ]] \
  && [[ $(body_etag create_replay) == "$create_etag" ]] || {
  printf 'exact create Receipt replay returned a different effect\n' >&2
  exit 1
}
jq -cS '.display_name += " (conflict probe)"' "$work/create.request.json" \
  > "$work/create-conflict.request.json"
request_json create_conflict POST "$base_path" 409 "$work/create-conflict.request.json" "$create_receipt"
jq -e '
  .status == 409 and .code == "idempotency_conflict" and
  .retryable == false and .retry_after_ms == null and
  (.request_id | type == "string") and (.trace_id | type == "string")
' "$work/create_conflict.json" >/dev/null || {
  printf 'create conflict is not the closed idempotency_conflict Problem\n' >&2
  exit 1
}

validate_receipt="insight-http-v1-${manifest_digest}-validate"
request_json validate POST "$base_path/$resource_id/draft:validate" 202 "" "$validate_receipt" "$create_etag"
operation_id=$(jq -er '.operation_id | strings | select(startswith("job_"))' "$work/validate.json")
[[ $(header_value "$work/validate.headers" location) == "/v1/operations/$operation_id" ]] || {
  printf 'validation Location does not identify the Operation\n' >&2
  exit 1
}
body_etag validate >/dev/null

started=$SECONDS
while true; do
  request_json operation GET "/v1/operations/$operation_id" 200
  body_etag operation >/dev/null
  operation_state=$(jq -er '.state' "$work/operation.json")
  case "$operation_state" in
    succeeded)
      break
      ;;
    failed|cancelled|timed_out|reconciliation_required)
      jq -c '{operation_id, state, error}' "$work/operation.json" >&2
      exit 1
      ;;
    queued|running|waiting)
      ((SECONDS - started < timeout_seconds)) || {
        printf 'validation Operation did not become terminal within %s seconds\n' "$timeout_seconds" >&2
        exit 1
      }
      sleep 0.2
      ;;
    *)
      printf 'validation Operation returned an unknown state\n' >&2
      exit 1
      ;;
  esac
done

request_json validated GET "$base_path/$resource_id" 200
validated_etag=$(body_etag validated)
jq -e --arg id "$resource_id" '.resource_id == $id and .draft.validation != null' \
  "$work/validated.json" >/dev/null || {
  printf 'succeeded validation did not produce the exact validated Draft\n' >&2
  exit 1
}

jq -cS '.publish' "$manifest" > "$work/publish.request.json"
publish_receipt="insight-http-v1-${manifest_digest}-publish"
request_json publish POST "$base_path/$resource_id/draft:publish" 200 \
  "$work/publish.request.json" "$publish_receipt" "$validated_etag"
publish_etag=$(body_etag publish)
jq -e --arg id "$resource_id" '.resource_id == $id and (.published_versions | length > 0)' \
  "$work/publish.json" >/dev/null || {
  printf 'publish response omitted the exact Resource or Versions\n' >&2
  exit 1
}

jq -n -cS --slurpfile manifest "$manifest" --slurpfile published "$work/publish.json" '
  ($manifest[0]) as $m |
  ($published[0].published_versions) as $versions |
  def exact($prefix; $kind):
    first($versions[] | select(.resource_version_id | startswith($prefix + "_")) |
      {revision_id: .resource_version_id, resource_kind: $kind, semantic_digest: .content_digest})
      // error("publish response omitted " + $kind);
  if $m.resource_noun == "agents" then
    (exact("aif"; "agent_interface_revision")) as $interface |
    (exact("arev"; "agent_plan_revision")) as $primary |
    {resource_version_id: $primary.revision_id, environment: $m.deployment.environment,
     closure: ($m.deployment.closure |
       .bindings = ({interface: $interface, plan: $primary} + .bindings))}
  elif $m.resource_noun == "skills" then
    (exact("srev"; "skill_revision")) as $primary |
    {resource_version_id: $primary.revision_id, environment: $m.deployment.environment,
     closure: ($m.deployment.closure | .bindings = ({skill_revision: $primary} + .bindings))}
  elif $m.resource_noun == "capabilities" then
    (exact("cirev"; "capability_interface_revision")) as $primary |
    {resource_version_id: $primary.revision_id, environment: $m.deployment.environment,
     closure: ($m.deployment.closure | .bindings = ({interface: $primary} + .bindings))}
  elif $m.resource_noun == "contexts" then
    (exact("xirev"; "context_source_interface_revision")) as $primary |
    {resource_version_id: $primary.revision_id, environment: $m.deployment.environment,
     closure: ($m.deployment.closure | .bindings = ({interface: $primary} + .bindings))}
  elif $m.resource_noun == "models" then
    (exact("mdrev"; "model_profile_revision")) as $primary |
    {resource_version_id: $primary.revision_id, environment: $m.deployment.environment,
     closure: ($m.deployment.closure | .bindings = ({profile_revision: $primary} + .bindings))}
  elif $m.resource_noun == "mcp-servers" then
    (exact("mrev"; "mcp_server_revision")) as $primary |
    {resource_version_id: $primary.revision_id, environment: $m.deployment.environment,
     closure: ($m.deployment.closure | .bindings = ({server_revision: $primary} + .bindings))}
  elif $m.resource_noun == "policies" then
    (exact("prev"; "policy_revision")) as $primary |
    {resource_version_id: $primary.revision_id, environment: $m.deployment.environment,
     closure: ($m.deployment.closure | .bindings = ({policy_revision: $primary} + .bindings))}
  elif $m.resource_noun == "sandboxes" then
    (exact("sxrev"; "sandbox_profile_revision")) as $primary |
    {resource_version_id: $primary.revision_id, environment: $m.deployment.environment,
     closure: ($m.deployment.closure | .bindings = ({profile_revision: $primary} + .bindings))}
  else error("unsupported Resource noun") end
' > "$work/deployment.request.json"

deployment_receipt="insight-http-v1-${manifest_digest}-deploy"
request_json deployment POST "$base_path/$resource_id/deployments" 201 \
  "$work/deployment.request.json" "$deployment_receipt" "$publish_etag"
deployment_id=$(jq -er '.deployment_id | strings | select(length > 0)' "$work/deployment.json")
deployment_etag=$(body_etag deployment)
[[ $(header_value "$work/deployment.headers" location) == "$base_path/$resource_id/deployments/$deployment_id" ]] || {
  printf 'Deployment Location does not identify the created Deployment\n' >&2
  exit 1
}
jq -e --slurpfile request "$work/deployment.request.json" --arg id "$resource_id" '
  .resource_id == $id and .resource_version_id == $request[0].resource_version_id and
  .environment == $request[0].environment and .closure == $request[0].closure
' "$work/deployment.json" >/dev/null || {
  printf 'Deployment authority projection differs from the exact request\n' >&2
  exit 1
}

request_json deployed GET "$base_path/$resource_id" 200
deployed_resource_etag=$(body_etag deployed)
activate_receipt="insight-http-v1-${manifest_digest}-activate"
request_json activate POST "$base_path/$resource_id/deployments/$deployment_id:activate" 200 \
  "" "$activate_receipt" "$deployed_resource_etag"
active_etag=$(body_etag activate)
jq -e --arg id "$resource_id" '.resource_id == $id and .gate_state == "enabled"' \
  "$work/activate.json" >/dev/null || {
  printf 'activation did not enable the exact Resource\n' >&2
  exit 1
}

jq -n -cS \
  --arg input_sha256 "sha256:$manifest_digest" \
  --arg resource_id "$resource_id" \
  --arg operation_id "$operation_id" \
  --arg deployment_id "$deployment_id" \
  --arg deployment_etag "$deployment_etag" \
  --arg final_resource_etag "$active_etag" \
  --arg create_trace "$(header_value "$work/create.headers" trace-id)" \
  --arg replay_trace "$(header_value "$work/create_replay.headers" trace-id)" \
  --arg conflict_trace "$(header_value "$work/create_conflict.headers" trace-id)" \
  --arg validate_trace "$(header_value "$work/validate.headers" trace-id)" \
  --arg publish_trace "$(header_value "$work/publish.headers" trace-id)" \
  --arg deployment_trace "$(header_value "$work/deployment.headers" trace-id)" \
  --arg activation_trace "$(header_value "$work/activate.headers" trace-id)" '
  {
    schema_version: 1,
    kind: "insight.platform.http-resource-lifecycle-report/v1",
    input_sha256: $input_sha256,
    resource_id: $resource_id,
    validation_operation_id: $operation_id,
    deployment_id: $deployment_id,
    deployment_etag: $deployment_etag,
    final_resource_etag: $final_resource_etag,
    receipt_replay: "same_effect",
    conflict_problem: "idempotency_conflict",
    step_trace_ids: {
      create: $create_trace,
      create_replay: $replay_trace,
      create_conflict: $conflict_trace,
      validate: $validate_trace,
      publish: $publish_trace,
      deployment: $deployment_trace,
      activate: $activation_trace
    }
  }
'
