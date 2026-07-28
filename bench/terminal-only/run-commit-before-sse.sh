#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command jq
require_command kubectl
require_nonempty BASE_URL "${BASE_URL:-}"

batch_id=${COMMIT_SSE_BATCH_ID:-"$(date -u +%Y%m%dT%H%M%S)-${RANDOM}"}
[[ "$batch_id" =~ ^[A-Za-z0-9-]+$ ]] || {
  printf 'COMMIT_SSE_BATCH_ID may contain only letters, digits, and hyphens\n' >&2
  exit 2
}
namespace=${BENCH_NAMESPACE:-insight-bench}
release=${BENCH_RELEASE:-bench}
runtime_selector=${BENCH_RUNTIME_SELECTOR:-app.kubernetes.io/component=runtime}
tenant_id="commit-sse-tenant-$batch_id"
user_id="commit-sse-user-$batch_id"
request_id="commit-sse-turn-$batch_id"
result_dir=${1:-"$terminal_bench_root/bench/results/terminal-only-commit-sse-$batch_id"}
mkdir -p "$result_dir"

stream_pid=
stop_commit_stream() {
  [[ -n "$stream_pid" ]] || return 0
  kill "$stream_pid" >/dev/null 2>&1 || true
  wait "$stream_pid" >/dev/null 2>&1 || true
  stream_pid=
}
wait_commit_stream() {
  [[ -n "$stream_pid" ]] || return 0
  wait "$stream_pid" >/dev/null 2>&1 || true
  stream_pid=
}
cleanup_commit_stream() {
  local original_status=$?
  trap - EXIT
  stop_commit_stream
  exit "$original_status"
}
trap cleanup_commit_stream EXIT

assert_postgres_durability

api_curl -X POST "$BASE_URL/v1/conversations" \
  -H 'content-type: application/json' \
  -H "x-request-id: commit-sse-create-$batch_id" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id" \
  --data-binary '{"agent_id":"terminal_commit_fixture"}' \
  >"$result_dir/create.json"
conversation_id=$(jq -er '.data.conversation_id' "$result_dir/create.json")

curl --silent --show-error --no-buffer \
  --max-time "${COMMIT_SSE_STREAM_MAX_SECONDS:-60}" \
  -X POST "$BASE_URL/v1/conversations/$conversation_id/messages/stream" \
  -H 'content-type: application/json' \
  -H "x-request-id: $request_id" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id" \
  --data-binary '{"content":{"text":"commit is authoritative before terminal SSE"}}' \
  >"$result_dir/stream.txt" 2>"$result_dir/stream.stderr" &
stream_pid=$!

deadline=$((SECONDS + ${COMMIT_SSE_TIMEOUT_SECONDS:-30}))
run_id=
while (( SECONDS < deadline )); do
  run_id=$(postgres_command -qAt -c "
    SELECT a.run_id
    FROM terminal_run_admissions a
    JOIN terminal_run_results r USING (run_id)
    WHERE a.tenant_id='$tenant_id' AND a.request_id='$request_id';
  ")
  [[ -n "$run_id" ]] && break
  sleep 0.01
done
[[ -n "$run_id" ]] || {
  kill "$stream_pid" >/dev/null 2>&1 || true
  printf 'terminal result was not committed inside the qualification delay\n' >&2
  exit 1
}
postgres_command -qAt -c "
  SELECT jsonb_build_object(
    'run_id', a.run_id,
    'result_committed', r.run_id IS NOT NULL,
    'assistant_committed', m.message_id IS NOT NULL
  )::text
  FROM terminal_run_admissions a
  JOIN terminal_run_results r USING (run_id)
  LEFT JOIN conversation_messages m
    ON m.conversation_id=a.conversation_id
   AND m.run_id=a.run_id
   AND m.role='assistant'
  WHERE a.run_id='$run_id';
" >"$result_dir/committed-before-kill.json"
jq -e '.result_committed and .assistant_committed' \
  "$result_dir/committed-before-kill.json" >/dev/null

killed_identity=$(kubectl -n "$namespace" get pods -l "$runtime_selector" \
  -o json |
  jq -er '
    [
      .items[] |
      select(
        .metadata.deletionTimestamp == null and
        .status.phase == "Running" and
        any(.status.conditions[]?;
            .type == "Ready" and .status == "True")
      )
    ] |
    select(length == 1) |
    .[0] |
    [.metadata.name, .metadata.uid] | @tsv
  ')
IFS=$'\t' read -r killed_pod killed_pod_uid <<<"$killed_identity"
[[ -n "$killed_pod" && -n "$killed_pod_uid" ]] || {
  kill "$stream_pid" >/dev/null 2>&1 || true
  printf 'commit-before-SSE requires exactly one Ready runtime Pod\n' >&2
  exit 1
}
printf '%s\n' "$killed_pod_uid" >"$result_dir/killed-pod-uid.txt"
if ! qualification_trigger_container_death \
  runtime_self_abort \
  "$killed_pod" \
  "$killed_pod_uid" \
  "${QUALIFICATION_RUNTIME_CONTAINER:-runtime}" \
  "$result_dir"; then
  stop_commit_stream
  printf 'commit-before-SSE did not confirm runtime process death\n' >&2
  exit 1
fi
kubectl -n "$namespace" delete pod "$killed_pod" --wait=false \
  >"$result_dir/pod-delete.txt"
wait_commit_stream
if grep -Eq \
  '^event: (response\.(completed|failed|incomplete)|workflow\.response\.(timed_out|cancelled|interrupted)|error)' \
  "$result_dir/stream.txt"; then
  printf 'terminal SSE escaped before the confirmed commit-barrier crash\n' >&2
  exit 1
fi

kubectl -n "$namespace" rollout status \
  "deployment/${release}-insight-agent-platform" \
  --timeout="${GATE_C_RESTART_TIMEOUT:-180s}" >"$result_dir/rollout.txt"
replacement_timeout_seconds=${COMMIT_SSE_REPLACEMENT_TIMEOUT_SECONDS:-180}
[[ "$replacement_timeout_seconds" =~ ^[1-9][0-9]*$ ]] || {
  printf 'COMMIT_SSE_REPLACEMENT_TIMEOUT_SECONDS must be a positive integer\n' >&2
  exit 2
}
deadline=$((SECONDS + replacement_timeout_seconds))
replacement_identity=
while (( SECONDS < deadline )); do
  replacement_identity=$(kubectl -n "$namespace" get pods \
    -l "$runtime_selector" -o json |
    jq -r --arg killed_uid "$killed_pod_uid" '
      [
        .items[] |
        select(
          .metadata.uid != $killed_uid and
          .metadata.deletionTimestamp == null and
          .status.phase == "Running" and
          any(.status.conditions[]?;
              .type == "Ready" and .status == "True")
        )
      ] |
      if length == 1 then
        [.[0].metadata.name, .[0].metadata.uid] | @tsv
      else
        empty
      end
    ')
  [[ -n "$replacement_identity" ]] && break
  sleep 0.25
done
[[ -n "$replacement_identity" ]] || {
  printf 'commit-before-SSE did not observe a Ready replacement Pod\n' >&2
  exit 1
}
IFS=$'\t' read -r replacement_pod replacement_pod_uid \
  <<<"$replacement_identity"
[[ "$replacement_pod_uid" != "$killed_pod_uid" ]] || {
  printf 'commit-before-SSE replacement retained killed Pod UID\n' >&2
  exit 1
}
printf '%s\n' "$replacement_pod_uid" \
  >"$result_dir/replacement-pod-uid.txt"
deadline=$((SECONDS + ${COMMIT_SSE_TIMEOUT_SECONDS:-30}))
while (( SECONDS < deadline )); do
  http_status=$(curl --silent --show-error \
    --output "$result_dir/messages-after-restart.json" \
    --write-out '%{http_code}' \
    "$BASE_URL/v1/conversations/$conversation_id/messages?limit=2" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $user_id")
  if [[ "$http_status" == "200" ]] &&
    jq -e --arg run_id "$run_id" '
      any(.data.messages[];
          .role == "assistant" and .run_id == $run_id)
    ' "$result_dir/messages-after-restart.json" >/dev/null; then
    break
  fi
  sleep 0.1
done
[[ "$http_status" == "200" ]] &&
  jq -e --arg run_id "$run_id" '
    any(.data.messages[];
        .role == "assistant" and .run_id == $run_id)
  ' "$result_dir/messages-after-restart.json" >/dev/null

api_curl "$BASE_URL/v1/runs/$run_id" \
  -H "x-tenant-id: $tenant_id" \
  -H "x-user-id: $user_id" >"$result_dir/run-after-restart.json"
jq -e '
  .data.status == "completed" and
  .data.persistence_mode == "terminal_only"
' "$result_dir/run-after-restart.json" >/dev/null

jq -n \
  --arg run_id "$run_id" \
  --arg killed_pod "$killed_pod" \
  --arg killed_pod_uid "$killed_pod_uid" \
  --arg replacement_pod "$replacement_pod" \
  --arg replacement_pod_uid "$replacement_pod_uid" \
  --slurpfile process_death "$result_dir/process-death-evidence.json" \
  '{
    passed: true,
    run_id: $run_id,
    killed_pod: {name: $killed_pod, uid: $killed_pod_uid},
    replacement_pod: {
      name: $replacement_pod,
      uid: $replacement_pod_uid
    },
    replacement_pod_uid_changed: ($killed_pod_uid != $replacement_pod_uid),
    process_death: $process_death[0],
    result_and_assistant_committed_before_kill: true,
    terminal_sse_absent_before_kill: true,
    get_and_messages_calibrated_after_restart: true
  }' >"$result_dir/commit-before-sse-report.json"
assert_postgres_durability
printf 'Commit-before-SSE evidence: %s\n' "$result_dir"
