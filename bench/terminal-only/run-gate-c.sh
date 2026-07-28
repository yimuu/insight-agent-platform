#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=bench/terminal-only/lib.sh
source "$script_dir/lib.sh"

require_command jq
require_command k6
require_command kubectl
require_command python3
require_nonempty BASE_URL "${BASE_URL:-}"
require_nonempty GATE_C_AGENT_ID "${GATE_C_AGENT_ID:-}"

run_count=${GATE_C_RUN_COUNT:-50}
[[ "$run_count" =~ ^[1-9][0-9]*$ ]] || {
  printf 'GATE_C_RUN_COUNT must be a positive integer\n' >&2
  exit 2
}
if [[ "${GATE_C_QUALIFICATION:-1}" == "1" && "$run_count" != "50" ]]; then
  printf 'Gate C qualification requires exactly 50 active Runs\n' >&2
  exit 2
fi
batch_id=${GATE_C_BATCH_ID:-"$(date -u +%Y%m%dT%H%M%S)-${RANDOM}"}
[[ "$batch_id" =~ ^[A-Za-z0-9-]+$ ]] || {
  printf 'GATE_C_BATCH_ID may contain only letters, digits, and hyphens\n' >&2
  exit 2
}
result_dir=${1:-"$terminal_bench_root/bench/results/terminal-only-gate-c-$batch_id"}
mkdir -p "$result_dir"

k6_pid=
stop_gate_c_k6() {
  [[ -n "$k6_pid" ]] || return 0
  kill "$k6_pid" >/dev/null 2>&1 || true
  wait "$k6_pid" >/dev/null 2>&1 || true
  k6_pid=
}
cleanup_gate_c_background() {
  local original_status=$?
  trap - EXIT
  stop_gate_c_k6
  exit "$original_status"
}
trap cleanup_gate_c_background EXIT

assert_postgres_durability

namespace=${BENCH_NAMESPACE:-insight-bench}
release=${BENCH_RELEASE:-bench}
runtime_selector=${BENCH_RUNTIME_SELECTOR:-app.kubernetes.io/component=runtime}
kill_target=${GATE_C_KILL_TARGET:-runtime}
if [[ "$kill_target" != "runtime" && "$kill_target" != "postgres" ]]; then
  printf 'GATE_C_KILL_TARGET must be runtime or postgres\n' >&2
  exit 2
fi
shutdown_mode=${GATE_C_RUNTIME_SHUTDOWN:-hard}
if [[ "$shutdown_mode" != "hard" && "$shutdown_mode" != "graceful" ]]; then
  printf 'GATE_C_RUNTIME_SHUTDOWN must be hard or graceful\n' >&2
  exit 2
fi
if [[ "$kill_target" == "postgres" && "$shutdown_mode" != "hard" ]]; then
  printf 'GATE_C_RUNTIME_SHUTDOWN applies only to runtime kill_target\n' >&2
  exit 2
fi
tenant_id=${TENANT_ID:-gate-c-tenant}
user_id=${USER_ID:-gate-c-user}
[[ "$tenant_id" =~ ^[A-Za-z0-9._:-]+$ &&
   "$user_id" =~ ^[A-Za-z0-9._:-]+$ ]] || {
  printf 'Gate C tenant/user IDs contain unsupported characters\n' >&2
  exit 2
}
conversation_id=

effect_batch_occurrences() {
  local target_batch_id=$1
  if [[ -n "${QUALIFICATION_EFFECT_LEDGER:-}" ]]; then
    if [[ ! -d "$QUALIFICATION_EFFECT_LEDGER" ]]; then
      printf '0\n'
      return
    fi
    (grep -h "^gate-c-effect-${target_batch_id}-" \
      "$QUALIFICATION_EFFECT_LEDGER"/*.ledger 2>/dev/null || true) |
      wc -l | tr -d ' '
    return
  fi
  local pod
  pod=$(kubectl -n "$namespace" get pods -l "$runtime_selector" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}')
  kubectl -n "$namespace" exec "$pod" -- sh -ec \
    "grep -h '^gate-c-effect-${target_batch_id}-' '${BENCH_ARTIFACT_ROOT:-/data/artifacts}/qualification-effects/'*.ledger 2>/dev/null | wc -l" |
    tr -d '[:space:]'
}

effect_batch_attempts() {
  local target_batch_id=$1
  if [[ -n "${QUALIFICATION_EFFECT_LEDGER:-}" ]]; then
    if [[ ! -d "$QUALIFICATION_EFFECT_LEDGER" ]]; then
      printf '0\n'
      return
    fi
    (grep -h "^gate-c-effect-${target_batch_id}-" \
      "$QUALIFICATION_EFFECT_LEDGER"/*.attempts 2>/dev/null || true) |
      wc -l | tr -d ' '
    return
  fi
  local pod
  pod=$(kubectl -n "$namespace" get pods -l "$runtime_selector" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}')
  kubectl -n "$namespace" exec "$pod" -- sh -ec \
    "grep -h '^gate-c-effect-${target_batch_id}-' '${BENCH_ARTIFACT_ROOT:-/data/artifacts}/qualification-effects/'*.attempts 2>/dev/null | wc -l" |
    tr -d '[:space:]'
}

effect_record_count() {
  local effect_id=$1
  local suffix=$2
  local hash
  hash=$(python3 -c \
    'import hashlib,sys; print(hashlib.sha256(sys.argv[1].encode()).hexdigest())' \
    "$effect_id")
  if [[ -n "${QUALIFICATION_EFFECT_LEDGER:-}" ]]; then
    if [[ ! -f "$QUALIFICATION_EFFECT_LEDGER/$hash.$suffix" ]]; then
      printf '0\n'
      return
    fi
    wc -l <"$QUALIFICATION_EFFECT_LEDGER/$hash.$suffix" | tr -d ' '
    return
  fi
  local pod
  pod=$(kubectl -n "$namespace" get pods -l "$runtime_selector" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}')
  kubectl -n "$namespace" exec "$pod" -- sh -ec \
    "test ! -f '${BENCH_ARTIFACT_ROOT:-/data/artifacts}/qualification-effects/$hash.$suffix' || wc -l < '${BENCH_ARTIFACT_ROOT:-/data/artifacts}/qualification-effects/$hash.$suffix'" |
    tr -d '[:space:]'
}

effect_occurrence() {
  effect_record_count "$1" ledger
}

effect_attempts() {
  effect_record_count "$1" attempts
}

runtime_active_count() {
  api_curl "$BASE_URL/metrics" |
    awk '
      $1 ~ /^terminal_run_active($|[{])/ {
        value=$NF + 0
        total += value
        found=1
      }
      END {
        if (!found) exit 1
        printf "%.0f\n", total
      }
    '
}

ready_selected_pod_identity() {
  local selector=$1
  local excluded_uid=${2:-}
  kubectl -n "$namespace" get pods -l "$selector" -o json |
    jq -r --arg excluded_uid "$excluded_uid" '
      [
        .items[] |
        select(
          .metadata.uid != $excluded_uid and
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
    '
}

ready_named_pod_identity() {
  local pod_name=$1
  local excluded_uid=${2:-}
  kubectl -n "$namespace" get pod "$pod_name" -o json 2>/dev/null |
    jq -r --arg excluded_uid "$excluded_uid" '
      select(
        .metadata.uid != $excluded_uid and
        .metadata.deletionTimestamp == null and
        .status.phase == "Running" and
        any(.status.conditions[]?;
            .type == "Ready" and .status == "True")
      ) |
      [.metadata.name, .metadata.uid] | @tsv
    '
}

expected_effects_before_kill=${GATE_C_EXPECT_EFFECTS_BEFORE_KILL:-}
if [[ -z "$expected_effects_before_kill" ]]; then
  if [[ "${GATE_C_VERIFY_EFFECTS:-1}" == "1" ]]; then
    expected_effects_before_kill=$run_count
  else
    expected_effects_before_kill=0
  fi
fi
[[ "$expected_effects_before_kill" =~ ^[0-9]+$ ]] || {
  printf 'GATE_C_EXPECT_EFFECTS_BEFORE_KILL must be a non-negative integer\n' >&2
  exit 2
}
effect_occurrences_before=$(effect_batch_occurrences "$batch_id")
provider_started_before=0
provider_active_before=0
if [[ -n "${GATE_C_PROVIDER_STATS_URL:-}" ]]; then
  api_curl "$GATE_C_PROVIDER_STATS_URL" >"$result_dir/provider-baseline.json"
  provider_started_before=$(jq -er '.started' "$result_dir/provider-baseline.json")
  provider_active_before=$(jq -er '.active' "$result_dir/provider-baseline.json")
  (( provider_active_before == 0 )) || {
    printf 'Gate C provider baseline has %s active requests\n' \
      "$provider_active_before" >&2
    exit 1
  }
fi

if [[ "${GATE_C_CONVERSATION:-1}" == "1" ]]; then
  api_curl -X POST "$BASE_URL/v1/conversations" \
    -H 'content-type: application/json' \
    -H "x-request-id: gate-c-conversation-$batch_id" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $user_id" \
    --data-binary "{\"agent_id\":\"$GATE_C_AGENT_ID\"}" \
    >"$result_dir/conversation.json"
  conversation_id=$(jq -er '.data.conversation_id' "$result_dir/conversation.json")
fi

capture_postgres_snapshot "$result_dir/postgres-before.json"
BASE_URL="$BASE_URL" \
AGENT_ID="$GATE_C_AGENT_ID" \
BATCH_ID="$batch_id" \
CONVERSATION_ID="$conversation_id" \
TENANT_ID="$tenant_id" \
USER_ID="$user_id" \
RUN_COUNT="$run_count" \
HOLD_SECONDS="${GATE_C_CLIENT_HOLD_SECONDS:-45}" \
SUMMARY_PATH="$result_dir/k6-summary.json" \
  k6 run "$script_dir/k6/failure-admit.js" >"$result_dir/k6.log" 2>&1 &
k6_pid=$!

deadline=$((SECONDS + ${GATE_C_ADMISSION_TIMEOUT_SECONDS:-30}))
admitted=0
unresolved=0
while (( SECONDS < deadline )); do
  read -r admitted unresolved < <(
    postgres_command -qAt -F' ' -c "
      SELECT count(*), count(*) FILTER (WHERE r.run_id IS NULL)
      FROM terminal_run_admissions a
      LEFT JOIN terminal_run_results r USING (run_id)
      WHERE a.request_id LIKE 'gate-c-${batch_id}-%';
    "
  )
  if (( admitted == run_count && unresolved == run_count )); then
    break
  fi
  sleep 0.05
done
if (( admitted != run_count )); then
  kill "$k6_pid" >/dev/null 2>&1 || true
  printf 'Gate C admitted %s/%s runs before deadline\n' "$admitted" "$run_count" >&2
  exit 1
fi
if (( unresolved != run_count )); then
  kill "$k6_pid" >/dev/null 2>&1 || true
  printf 'Gate C has %s unresolved Runs before kill, expected %s active Runs\n' \
    "$unresolved" "$run_count" >&2
  exit 1
fi

if (( expected_effects_before_kill > 0 )); then
  deadline=$((SECONDS + ${GATE_C_EFFECT_TIMEOUT_SECONDS:-30}))
  effect_occurrences=$effect_occurrences_before
  while (( SECONDS < deadline )); do
    effect_occurrences=$(effect_batch_occurrences "$batch_id")
    (( effect_occurrences - effect_occurrences_before >= expected_effects_before_kill )) &&
      break
    sleep 0.05
  done
else
  effect_occurrences=$(effect_batch_occurrences "$batch_id")
fi
effect_occurrence_delta=$((effect_occurrences - effect_occurrences_before))
(( effect_occurrence_delta == expected_effects_before_kill )) || {
    kill "$k6_pid" >/dev/null 2>&1 || true
    printf 'Gate C observed %s external effects before kill, expected %s\n' \
      "$effect_occurrence_delta" "$expected_effects_before_kill" >&2
    exit 1
}

provider_started_delta=0
provider_active_before_kill=0
if [[ -n "${GATE_C_PROVIDER_STATS_URL:-}" ]]; then
  deadline=$((SECONDS + ${GATE_C_PROVIDER_TIMEOUT_SECONDS:-30}))
  while (( SECONDS < deadline )); do
    api_curl "$GATE_C_PROVIDER_STATS_URL" >"$result_dir/provider-before-kill.json"
    provider_started=$(jq -er '.started' "$result_dir/provider-before-kill.json")
    provider_active_before_kill=$(jq -er '.active' \
      "$result_dir/provider-before-kill.json")
    provider_started_delta=$((provider_started - provider_started_before))
    provider_active_delta=$((provider_active_before_kill - provider_active_before))
    if (( provider_started_delta == run_count &&
          provider_active_delta == run_count )); then
      break
    fi
    sleep 0.05
  done
  (( provider_started_delta == run_count &&
     provider_active_before_kill - provider_active_before == run_count )) || {
    kill "$k6_pid" >/dev/null 2>&1 || true
    printf 'Gate C provider starts/active delta are %s/%s, expected %s/%s\n' \
      "$provider_started_delta" \
      "$((provider_active_before_kill - provider_active_before))" \
      "$run_count" "$run_count" >&2
    exit 1
  }
fi

deadline=$((SECONDS + ${GATE_C_ACTIVE_TIMEOUT_SECONDS:-30}))
active_before_kill=-1
while (( SECONDS < deadline )); do
  active_before_kill=$(runtime_active_count)
  (( active_before_kill == run_count )) && break
  sleep 0.05
done
(( active_before_kill == run_count )) || {
  kill "$k6_pid" >/dev/null 2>&1 || true
  printf 'Gate C runtime active metric is %s before kill, expected exactly %s\n' \
    "$active_before_kill" "$run_count" >&2
  exit 1
}
printf '%s\n' "$active_before_kill" \
  >"$result_dir/runtime-active-before-kill.txt"

postgres_postmaster_before=
postgres_postmaster_after=
if [[ "$kill_target" == "runtime" ]]; then
  killed_identity=$(ready_selected_pod_identity "$runtime_selector")
  [[ -n "$killed_identity" ]] || {
    kill "$k6_pid" >/dev/null 2>&1 || true
    printf 'Gate C requires exactly one Ready runtime Pod before kill\n' >&2
    exit 1
  }
  IFS=$'\t' read -r killed_pod killed_pod_uid <<<"$killed_identity"
  rollout_target="deployment/${release}-insight-agent-platform"
else
  killed_pod=${BENCH_POSTGRES_POD:-"${release}-insight-agent-platform-postgresql-0"}
  killed_identity=$(ready_named_pod_identity "$killed_pod")
  [[ -n "$killed_identity" ]] || {
    kill "$k6_pid" >/dev/null 2>&1 || true
    printf 'Gate C PostgreSQL Pod is not uniquely Ready before kill\n' >&2
    exit 1
  }
  IFS=$'\t' read -r _ killed_pod_uid <<<"$killed_identity"
  postgres_postmaster_before=$(postgres_command -qAt -c \
    'SELECT pg_postmaster_start_time()::text;')
  [[ -n "$postgres_postmaster_before" ]] || {
    kill "$k6_pid" >/dev/null 2>&1 || true
    printf 'Gate C could not capture PostgreSQL postmaster start time\n' >&2
    exit 1
  }
  rollout_target="statefulset/${release}-insight-agent-platform-postgresql"
fi
printf '%s\n' "$killed_pod" >"$result_dir/killed-pod.txt"
printf '%s\n' "$killed_pod_uid" >"$result_dir/killed-pod-uid.txt"
if [[ "$kill_target" == "runtime" && "$shutdown_mode" == "graceful" ]]; then
  jq -n \
    --arg kill_target "$kill_target" \
    --arg pod_name "$killed_pod" \
    --arg pod_uid "$killed_pod_uid" \
    '{
      kill_target: $kill_target,
      shutdown_mode: "graceful",
      pod_name: $pod_name,
      pod_uid: $pod_uid,
      container_name: null,
      hard_process_death_required: false,
      hard_process_death_confirmed: null
    }' >"$result_dir/process-death-evidence.json"
  kubectl -n "$namespace" delete pod "$killed_pod" --wait=false \
    >"$result_dir/pod-delete.txt"
else
  if [[ "$kill_target" == "runtime" ]]; then
    killed_container=${GATE_C_RUNTIME_CONTAINER:-${QUALIFICATION_RUNTIME_CONTAINER:-runtime}}
    process_trigger=runtime_self_abort
  else
    killed_container=${GATE_C_POSTGRES_CONTAINER:-${QUALIFICATION_POSTGRES_CONTAINER:-postgresql}}
    process_trigger=postgres_immediate_shutdown
  fi
  if ! qualification_trigger_container_death \
    "$process_trigger" \
    "$killed_pod" \
    "$killed_pod_uid" \
    "$killed_container" \
    "$result_dir"; then
    stop_gate_c_k6
    printf 'Gate C did not confirm hard process death\n' >&2
    exit 1
  fi

  # The hard failure is the confirmed process trigger above. Delete the now
  # restarted Pod normally only to obtain a distinct workload Pod UID; a
  # force-delete acknowledgement is not process-death evidence.
  kubectl -n "$namespace" delete pod "$killed_pod" --wait=false \
    >"$result_dir/pod-delete.txt"
fi
stop_gate_c_k6

kubectl -n "$namespace" rollout status "$rollout_target" \
  --timeout="${GATE_C_RESTART_TIMEOUT:-180s}" \
  >"$result_dir/rollout.txt"
if [[ "$kill_target" == "postgres" ]]; then
  kubectl -n "$namespace" rollout status \
    "deployment/${release}-insight-agent-platform" \
    --timeout="${GATE_C_RESTART_TIMEOUT:-180s}" \
    >>"$result_dir/rollout.txt"
fi

replacement_timeout_seconds=${GATE_C_REPLACEMENT_TIMEOUT_SECONDS:-180}
[[ "$replacement_timeout_seconds" =~ ^[1-9][0-9]*$ ]] || {
  printf 'GATE_C_REPLACEMENT_TIMEOUT_SECONDS must be a positive integer\n' >&2
  exit 2
}
deadline=$((SECONDS + replacement_timeout_seconds))
replacement_identity=
while (( SECONDS < deadline )); do
  if [[ "$kill_target" == "runtime" ]]; then
    replacement_identity=$(
      ready_selected_pod_identity "$runtime_selector" "$killed_pod_uid" ||
        true
    )
  else
    replacement_identity=$(
      ready_named_pod_identity "$killed_pod" "$killed_pod_uid" ||
        true
    )
  fi
  [[ -n "$replacement_identity" ]] && break
  sleep 0.25
done
[[ -n "$replacement_identity" ]] || {
  printf 'Gate C did not observe a Ready replacement Pod with a new UID\n' >&2
  exit 1
}
IFS=$'\t' read -r replacement_pod replacement_pod_uid \
  <<<"$replacement_identity"
[[ "$replacement_pod_uid" != "$killed_pod_uid" ]] || {
  printf 'Gate C replacement Pod retained killed UID %s\n' \
    "$killed_pod_uid" >&2
  exit 1
}
printf '%s\n' "$replacement_pod" >"$result_dir/replacement-pod.txt"
printf '%s\n' "$replacement_pod_uid" \
  >"$result_dir/replacement-pod-uid.txt"

if [[ "$kill_target" == "postgres" ]]; then
  deadline=$((SECONDS + replacement_timeout_seconds))
  while (( SECONDS < deadline )); do
    postgres_postmaster_after=$(
      postgres_command -qAt -c \
        'SELECT pg_postmaster_start_time()::text;' 2>/dev/null || true
    )
    if [[ -n "$postgres_postmaster_after" &&
          "$postgres_postmaster_after" != "$postgres_postmaster_before" ]]; then
      break
    fi
    sleep 0.25
  done
  [[ -n "$postgres_postmaster_after" &&
     "$postgres_postmaster_after" != "$postgres_postmaster_before" ]] || {
    printf 'Gate C PostgreSQL postmaster identity did not change\n' >&2
    exit 1
  }
  process_death_tmp="$result_dir/process-death-evidence.tmp.json"
  jq \
    --arg before "$postgres_postmaster_before" \
    --arg after "$postgres_postmaster_after" '
      ($before != "" and $after != "" and $before != $after) as $changed |
      .cause_calibration.postgres_postmaster_start_time_before = $before |
      .cause_calibration.postgres_postmaster_start_time_after = $after |
      .cause_calibration.postgres_postmaster_changed = $changed |
      .cause_calibration.calibrated =
        (.cause_calibration.calibrated == true and $changed) |
      .hard_process_death_confirmed =
        (.cause_calibration.calibrated == true)
    ' "$result_dir/process-death-evidence.json" >"$process_death_tmp"
  mv "$process_death_tmp" "$result_dir/process-death-evidence.json"
  jq -e '
    .hard_process_death_confirmed == true and
    .cause_calibration.postgres_postmaster_changed == true
  ' "$result_dir/process-death-evidence.json" >/dev/null
fi
jq -n \
  --arg kill_target "$kill_target" \
  --arg killed_pod "$killed_pod" \
  --arg killed_pod_uid "$killed_pod_uid" \
  --arg replacement_pod "$replacement_pod" \
  --arg replacement_pod_uid "$replacement_pod_uid" \
  --arg postgres_postmaster_before "$postgres_postmaster_before" \
  --arg postgres_postmaster_after "$postgres_postmaster_after" \
  --slurpfile process_death "$result_dir/process-death-evidence.json" \
  '{
    kill_target: $kill_target,
    killed_pod: {name: $killed_pod, uid: $killed_pod_uid},
    replacement_pod: {
      name: $replacement_pod,
      uid: $replacement_pod_uid
    },
    pod_uid_changed: ($killed_pod_uid != $replacement_pod_uid),
    process_death: $process_death[0],
    postgres_postmaster_start_time_before:
      (if $postgres_postmaster_before == "" then null
       else $postgres_postmaster_before end),
    postgres_postmaster_start_time_after:
      (if $postgres_postmaster_after == "" then null
       else $postgres_postmaster_after end),
    postgres_postmaster_changed:
      (if $kill_target == "postgres"
       then $postgres_postmaster_before != $postgres_postmaster_after
       else null end)
  }' >"$result_dir/replacement-identity.json"
sleep "${GATE_C_OWNER_EXPIRY_WAIT_SECONDS:-35}"

postgres_command -qAt -c "
  WITH batch AS (
    SELECT a.*, r.run_id AS result_run_id,
           (i.instance_id IS NOT NULL
            AND i.owner_epoch=a.owner_epoch
            AND i.lease_expires_at>clock_timestamp()) AS owner_active
    FROM terminal_run_admissions a
    LEFT JOIN terminal_run_results r USING (run_id)
    LEFT JOIN terminal_runtime_instances i
      ON i.instance_id=a.owner_instance_id
     AND i.owner_epoch=a.owner_epoch
    WHERE a.request_id LIKE 'gate-c-${batch_id}-%'
  )
  SELECT jsonb_build_object(
    'admitted', count(*),
    'terminal', count(*) FILTER (WHERE result_run_id IS NOT NULL),
    'interrupted',
      count(*) FILTER (WHERE result_run_id IS NULL AND NOT owner_active),
    'incorrectly_active',
      count(*) FILTER (WHERE result_run_id IS NULL AND owner_active)
  )::text
  FROM batch;
" >"$result_dir/classification-after-restart.json"

admitted_after=$(jq -er '.admitted' "$result_dir/classification-after-restart.json")
terminal_after=$(jq -er '.terminal' "$result_dir/classification-after-restart.json")
interrupted_after=$(jq -er '.interrupted' "$result_dir/classification-after-restart.json")
active_after=$(jq -er '.incorrectly_active' "$result_dir/classification-after-restart.json")
(( admitted_after == run_count )) || {
  printf 'Gate C lost admissions: %s/%s\n' "$admitted_after" "$run_count" >&2
  exit 1
}
(( terminal_after + interrupted_after == run_count && active_after == 0 )) || {
  printf 'Gate C terminal/interrupted closure is invalid\n' >&2
  exit 1
}
if [[ "$shutdown_mode" == "graceful" ]]; then
  (( terminal_after == run_count && interrupted_after == 0 )) || {
    printf 'graceful shutdown did not commit every admitted run terminally\n' >&2
    exit 1
  }
else
  (( interrupted_after > 0 )) || {
    printf 'Gate C did not observe any interrupted run\n' >&2
    exit 1
  }
fi

postgres_command -qAt -c "
  SELECT coalesce(
    jsonb_agg(
      jsonb_build_object(
        'run_id', a.run_id,
        'request_id', a.request_id,
        'expected_status',
          CASE
            WHEN r.run_id IS NULL THEN 'interrupted'
            WHEN r.terminal_state='succeeded' THEN 'completed'
            WHEN r.terminal_state='timed_out' THEN 'failed'
            ELSE r.terminal_state
          END
      )
      ORDER BY a.request_id
    ),
    '[]'::jsonb
  )::text
  FROM terminal_run_admissions a
  LEFT JOIN terminal_run_results r USING (run_id)
  WHERE a.tenant_id='${tenant_id}'
    AND a.request_id LIKE 'gate-c-${batch_id}-%';
" >"$result_dir/public-get-expectations.json"
public_get_count=$(jq 'length' "$result_dir/public-get-expectations.json")
(( public_get_count == run_count )) || {
  printf 'Gate C public GET expectation count is %s, expected %s\n' \
    "$public_get_count" "$run_count" >&2
  exit 1
}
public_get_index=0
while IFS= read -r expectation; do
  expected_run_id=$(jq -er '.run_id' <<<"$expectation")
  expected_status=$(jq -er '.expected_status' <<<"$expectation")
  public_get_index=$((public_get_index + 1))
  get_status=$(curl --silent --show-error \
    --output "$result_dir/public-get-$public_get_index.json" \
    --write-out '%{http_code}' \
    "$BASE_URL/v1/runs/$expected_run_id" \
    -H "x-tenant-id: $tenant_id" \
    -H "x-user-id: $user_id")
  [[ "$get_status" == "200" ]] || {
    printf 'Gate C public GET for %s returned HTTP %s\n' \
      "$expected_run_id" "$get_status" >&2
    exit 1
  }
  actual_status=$(jq -er '.data.status' \
    "$result_dir/public-get-$public_get_index.json")
  [[ "$actual_status" == "$expected_status" ]] || {
    printf 'Gate C public GET for %s returned %s, expected %s\n' \
      "$expected_run_id" "$actual_status" "$expected_status" >&2
    exit 1
  }
done < <(jq -c '.[]' "$result_dir/public-get-expectations.json")

# Wait once more to prove the new owner does not discover/recover old work.
batch_attempts_before_observation=0
if [[ "${GATE_C_VERIFY_EFFECTS:-1}" == "1" ]]; then
  batch_attempts_before_observation=$(effect_batch_attempts "$batch_id")
  (( batch_attempts_before_observation == expected_effects_before_kill )) || {
    printf 'Gate C batch has %s provider attempts before recovery window, expected %s\n' \
      "$batch_attempts_before_observation" "$expected_effects_before_kill" >&2
    exit 1
  }
fi
provider_started_before_observation=0
provider_active_before_observation=0
provider_expected_started=$((provider_started_before + run_count))
if [[ -n "${GATE_C_PROVIDER_STATS_URL:-}" ]]; then
  deadline=$((SECONDS + ${GATE_C_PROVIDER_DRAIN_TIMEOUT_SECONDS:-30}))
  while (( SECONDS < deadline )); do
    api_curl "$GATE_C_PROVIDER_STATS_URL" \
      >"$result_dir/provider-before-recovery-window.json"
    provider_started_before_observation=$(jq -er '.started' \
      "$result_dir/provider-before-recovery-window.json")
    provider_active_before_observation=$(jq -er '.active' \
      "$result_dir/provider-before-recovery-window.json")
    if (( provider_started_before_observation > provider_expected_started )); then
      break
    fi
    if (( provider_started_before_observation == provider_expected_started &&
          provider_active_before_observation == provider_active_before )); then
      break
    fi
    sleep 0.05
  done
  (( provider_started_before_observation == provider_expected_started &&
     provider_active_before_observation == provider_active_before )) || {
    printf 'Gate C provider starts/active before recovery window are %s/%s, expected %s/%s\n' \
      "$provider_started_before_observation" "$provider_active_before_observation" \
      "$provider_expected_started" "$provider_active_before" >&2
    exit 1
  }
fi
sleep "${GATE_C_NO_RECOVERY_OBSERVATION_SECONDS:-10}"
results_later=$(postgres_command -qAt -c "
  SELECT count(*)
  FROM terminal_run_results r
  JOIN terminal_run_admissions a USING (run_id)
  WHERE a.request_id LIKE 'gate-c-${batch_id}-%';
")
(( results_later == terminal_after )) || {
  printf 'old unresolved admissions acquired terminal results after restart\n' >&2
  exit 1
}
batch_attempts_after_observation=0
if [[ "${GATE_C_VERIFY_EFFECTS:-1}" == "1" ]]; then
  batch_attempts_after_observation=$(effect_batch_attempts "$batch_id")
  (( batch_attempts_after_observation == batch_attempts_before_observation )) || {
    printf 'old unresolved admissions were re-executed during recovery window\n' >&2
    exit 1
  }
fi
provider_started_after_observation=0
provider_active_after_observation=0
if [[ -n "${GATE_C_PROVIDER_STATS_URL:-}" ]]; then
  api_curl "$GATE_C_PROVIDER_STATS_URL" \
    >"$result_dir/provider-after-recovery-window.json"
  provider_started_after_observation=$(jq -er '.started' \
    "$result_dir/provider-after-recovery-window.json")
  provider_active_after_observation=$(jq -er '.active' \
    "$result_dir/provider-after-recovery-window.json")
  (( provider_started_after_observation == provider_expected_started &&
     provider_active_after_observation == provider_active_before )) || {
    printf 'old unresolved LLM admissions were re-executed during recovery window: starts/active %s/%s, expected %s/%s\n' \
      "$provider_started_after_observation" "$provider_active_after_observation" \
      "$provider_expected_started" "$provider_active_before" >&2
    exit 1
  }
fi

read -r replay_request replay_run_id < <(
  postgres_command -qAt -F' ' -c "
    SELECT a.request_id, a.run_id
    FROM terminal_run_admissions a
    LEFT JOIN terminal_run_results r USING (run_id)
    WHERE a.request_id LIKE 'gate-c-${batch_id}-%'
      AND (
        '$shutdown_mode' = 'graceful'
        OR r.run_id IS NULL
      )
    ORDER BY a.request_id
    LIMIT 1;
  "
)
suffix=${replay_request#"gate-c-$batch_id-"}
vu=${suffix%%-*}
iteration=${suffix#*-}
replay_content="{\"text\":\"terminal Gate C $batch_id $vu/$iteration\"}"
effect_id="gate-c-effect-$batch_id-$vu-$iteration"
replay_content="{\"text\":\"terminal Gate C $batch_id $vu/$iteration\",\"effect_id\":\"$effect_id\",\"idempotency_key\":\"$replay_request\"}"
if [[ "${GATE_C_VERIFY_EFFECTS:-1}" == "1" ]]; then
  replay_occurrence_before=$(effect_occurrence "$effect_id")
  replay_attempts_before=$(effect_attempts "$effect_id")
  selected_effects_expected=0
  if (( expected_effects_before_kill > 0 )); then
    selected_effects_expected=1
  fi
  (( replay_occurrence_before == selected_effects_expected &&
     replay_attempts_before == selected_effects_expected )) || {
    printf 'selected effect has %s effects/%s attempts before replay, expected %s/%s\n' \
      "$replay_occurrence_before" "$replay_attempts_before" \
      "$selected_effects_expected" "$selected_effects_expected" >&2
    exit 1
  }
fi
if [[ -n "$conversation_id" ]]; then
  replay_url="$BASE_URL/v1/conversations/$conversation_id/messages"
  replay_body="{\"content\":$replay_content}"
  replay_headers=(
    -H "x-tenant-id: $tenant_id"
    -H "x-user-id: $user_id"
  )
else
  replay_url="$BASE_URL/v1/agents/$GATE_C_AGENT_ID/runs"
  replay_body=$replay_content
  replay_headers=()
fi
api_curl -X POST "$replay_url" \
  -H 'content-type: application/json' \
  -H "x-request-id: $replay_request" \
  "${replay_headers[@]}" \
  --data-binary "$replay_body" >"$result_dir/same-request-replay.json"
replayed_run_id=$(jq -er '
  if .data.run then .data.run.run_id else .data.run_id end
' "$result_dir/same-request-replay.json")
[[ "$replayed_run_id" == "$replay_run_id" ]] || {
  printf 'same request ID returned a different run\n' >&2
  exit 1
}
replay_expected_status=$(jq -er --arg run_id "$replay_run_id" '
  first(.[] | select(.run_id == $run_id) | .expected_status)
' "$result_dir/public-get-expectations.json")
replay_status=$(jq -er '
  if .data.run then .data.run.status else .data.status end
' "$result_dir/same-request-replay.json")
[[ "$replay_status" == "$replay_expected_status" ]] || {
  printf 'same request replay status is %s, expected original %s\n' \
    "$replay_status" "$replay_expected_status" >&2
  exit 1
}
if [[ -n "$conversation_id" ]]; then
  jq -e '.data.replayed == true' \
    "$result_dir/same-request-replay.json" >/dev/null
fi
if [[ "${GATE_C_VERIFY_EFFECTS:-1}" == "1" ]]; then
  replay_occurrence=$(effect_occurrence "$effect_id")
  replay_attempts=$(effect_attempts "$effect_id")
  sleep "${GATE_C_REPLAY_STABILITY_SECONDS:-2}"
  replay_occurrence_stable=$(effect_occurrence "$effect_id")
  replay_attempts_stable=$(effect_attempts "$effect_id")
  (( replay_occurrence == replay_occurrence_before &&
     replay_attempts == replay_attempts_before &&
     replay_occurrence_stable == replay_occurrence_before &&
     replay_attempts_stable == replay_attempts_before )) || {
    printf 'same request replay changed effect/attempt counts from %s/%s to %s/%s (stable %s/%s)\n' \
      "$replay_occurrence_before" "$replay_attempts_before" \
      "$replay_occurrence" "$replay_attempts" \
      "$replay_occurrence_stable" "$replay_attempts_stable" >&2
    exit 1
  }
fi
provider_started_after_replay=0
provider_active_after_replay=0
if [[ -n "${GATE_C_PROVIDER_STATS_URL:-}" ]]; then
  sleep "${GATE_C_REPLAY_STABILITY_SECONDS:-2}"
  api_curl "$GATE_C_PROVIDER_STATS_URL" \
    >"$result_dir/provider-after-same-request-replay.json"
  provider_started_after_replay=$(jq -er '.started' \
    "$result_dir/provider-after-same-request-replay.json")
  provider_active_after_replay=$(jq -er '.active' \
    "$result_dir/provider-after-same-request-replay.json")
  (( provider_started_after_replay == provider_expected_started &&
     provider_active_after_replay == provider_active_before )) || {
    printf 'same request replay changed LLM provider starts/active to %s/%s, expected %s/%s\n' \
      "$provider_started_after_replay" "$provider_active_after_replay" \
      "$provider_expected_started" "$provider_active_before" >&2
    exit 1
  }
fi

new_request="gate-c-explicit-retry-$batch_id"
if [[ -n "$conversation_id" ]]; then
  retry_url="$BASE_URL/v1/conversations/$conversation_id/messages"
  retry_body="{\"content\":{\"text\":\"explicit retry after interruption\",\"effect_id\":\"$effect_id\",\"idempotency_key\":\"$new_request\"}}"
else
  retry_url="$BASE_URL/v1/agents/$GATE_C_AGENT_ID/runs"
  retry_body="{\"text\":\"explicit retry after interruption\",\"effect_id\":\"$effect_id\",\"idempotency_key\":\"$new_request\"}"
fi
api_curl -X POST "$retry_url" \
  -H 'content-type: application/json' \
  -H "x-request-id: $new_request" \
  "${replay_headers[@]}" \
  --data-binary "$retry_body" >"$result_dir/new-request-retry.json"
new_run_id=$(jq -er '
  if .data.run then .data.run.run_id else .data.run_id end
' "$result_dir/new-request-retry.json")
[[ "$new_run_id" != "$replay_run_id" ]] || {
  printf 'new request ID did not create a new run\n' >&2
  exit 1
}
if [[ "${GATE_C_VERIFY_EFFECTS:-1}" == "1" ]]; then
  deadline=$((SECONDS + ${GATE_C_EFFECT_TIMEOUT_SECONDS:-30}))
  retry_occurrence=0
  retry_effects_expected=$((replay_occurrence_before + 1))
  while (( SECONDS < deadline )); do
    retry_occurrence=$(effect_occurrence "$effect_id")
    (( retry_occurrence >= retry_effects_expected )) && break
    sleep 0.05
  done
  (( retry_occurrence == retry_effects_expected )) || {
    printf 'explicit new-request retry effect occurrence is %s, expected %s\n' \
      "$retry_occurrence" "$retry_effects_expected" >&2
    exit 1
  }
fi

deadline=$((SECONDS + ${GATE_C_CLEANUP_TIMEOUT_SECONDS:-90}))
new_run_terminal=0
active_after_retry=-1
while (( SECONDS < deadline )); do
  new_run_terminal=$(postgres_command -qAt -c "
    SELECT count(*) FROM terminal_run_results WHERE run_id='${new_run_id}';
  ")
  active_after_retry=$(runtime_active_count)
  if (( new_run_terminal == 1 && active_after_retry == 0 )); then
    break
  fi
  sleep 0.1
done
(( new_run_terminal == 1 && active_after_retry == 0 )) || {
  printf 'Gate C cleanup left retry terminal/active at %s/%s\n' \
    "$new_run_terminal" "$active_after_retry" >&2
  exit 1
}
if [[ "${GATE_C_VERIFY_EFFECTS:-1}" == "1" ]]; then
  retry_attempts=$(effect_attempts "$effect_id")
  sleep "${GATE_C_RETRY_STABILITY_SECONDS:-2}"
  retry_occurrence_stable=$(effect_occurrence "$effect_id")
  retry_attempts_stable=$(effect_attempts "$effect_id")
  (( retry_attempts == retry_effects_expected &&
     retry_occurrence_stable == retry_effects_expected &&
     retry_attempts_stable == retry_effects_expected )) || {
    printf 'new request retry effect/attempt counts are %s/%s after stability, expected %s/%s\n' \
      "$retry_occurrence_stable" "$retry_attempts_stable" \
      "$retry_effects_expected" "$retry_effects_expected" >&2
    exit 1
  }
fi
provider_started_after_retry=0
provider_active_after_retry=0
if [[ -n "${GATE_C_PROVIDER_STATS_URL:-}" ]]; then
  provider_retry_expected=$((provider_expected_started + 1))
  deadline=$((SECONDS + ${GATE_C_PROVIDER_DRAIN_TIMEOUT_SECONDS:-30}))
  while (( SECONDS < deadline )); do
    api_curl "$GATE_C_PROVIDER_STATS_URL" \
      >"$result_dir/provider-after-explicit-retry.json"
    provider_started_after_retry=$(jq -er '.started' \
      "$result_dir/provider-after-explicit-retry.json")
    provider_active_after_retry=$(jq -er '.active' \
      "$result_dir/provider-after-explicit-retry.json")
    if (( provider_started_after_retry > provider_retry_expected )); then
      break
    fi
    if (( provider_started_after_retry == provider_retry_expected &&
          provider_active_after_retry == provider_active_before )); then
      break
    fi
    sleep 0.05
  done
  (( provider_started_after_retry == provider_retry_expected &&
     provider_active_after_retry == provider_active_before )) || {
    printf 'explicit new-request retry LLM provider starts/active are %s/%s, expected %s/%s\n' \
      "$provider_started_after_retry" "$provider_active_after_retry" \
      "$provider_retry_expected" "$provider_active_before" >&2
    exit 1
  }
fi

postgres_command -qAt -c "
  WITH selected_admissions AS (
    SELECT a.*
    FROM terminal_run_admissions a
    WHERE a.tenant_id='${tenant_id}'
      AND (
        a.request_id LIKE 'gate-c-${batch_id}-%'
        OR a.request_id='${new_request}'
      )
      AND a.conversation_id IS NOT DISTINCT FROM NULLIF('${conversation_id}', '')
  ),
  selected_messages AS (
    SELECT m.*
    FROM conversation_messages m
    WHERE NULLIF('${conversation_id}', '') IS NOT NULL
      AND m.conversation_id=NULLIF('${conversation_id}', '')
  ),
  selected_results AS (
    SELECT r.*
    FROM terminal_run_results r
    JOIN selected_admissions a USING (run_id)
  )
  SELECT jsonb_build_object(
    'assistant_without_admission', (
      SELECT count(*)
      FROM selected_messages m
      LEFT JOIN selected_admissions a
        ON a.conversation_id=m.conversation_id
       AND a.run_id=m.run_id
      WHERE m.role='assistant' AND a.run_id IS NULL
    ),
    'assistant_without_result', (
      SELECT count(*)
      FROM selected_messages m
      LEFT JOIN selected_results r ON r.run_id=m.run_id
      WHERE m.role='assistant' AND r.run_id IS NULL
    ),
    'user_without_admission', (
      SELECT count(*)
      FROM selected_messages m
      LEFT JOIN selected_admissions a
        ON a.conversation_id=m.conversation_id
       AND a.user_message_id=m.message_id
      WHERE m.role='user' AND a.run_id IS NULL
    ),
    'admission_without_user', (
      SELECT count(*)
      FROM selected_admissions a
      LEFT JOIN selected_messages m
        ON m.conversation_id=a.conversation_id
       AND m.message_id=a.user_message_id
       AND m.role='user'
      WHERE a.conversation_id IS NOT NULL AND m.message_id IS NULL
    ),
    'conversation_result_without_assistant', (
      SELECT count(*)
      FROM selected_results r
      JOIN selected_admissions a USING (run_id)
      LEFT JOIN selected_messages m
        ON m.conversation_id=a.conversation_id
       AND m.run_id=a.run_id
       AND m.role='assistant'
      WHERE a.conversation_id IS NOT NULL AND m.message_id IS NULL
    ),
    'duplicate_terminal_results', (
      SELECT count(*)
      FROM (
        SELECT run_id
        FROM selected_results
        GROUP BY run_id
        HAVING count(*) > 1
      ) duplicate_results
    ),
    'duplicate_assistant_messages', (
      SELECT count(*)
      FROM (
        SELECT conversation_id,run_id
        FROM selected_messages
        WHERE role='assistant'
        GROUP BY conversation_id,run_id
        HAVING count(*) > 1
      ) duplicate_assistants
    ),
    'admission_user_message_reuse', (
      SELECT count(*)
      FROM (
        SELECT conversation_id,user_message_id
        FROM selected_admissions
        WHERE conversation_id IS NOT NULL
        GROUP BY conversation_id,user_message_id
        HAVING count(*) > 1
      ) reused_user_messages
    )
  )::text;
" >"$result_dir/conversation-atomicity.json"
jq -e '
  .assistant_without_admission == 0 and
  .assistant_without_result == 0 and
  .user_without_admission == 0 and
  .admission_without_user == 0 and
  .conversation_result_without_assistant == 0 and
  .duplicate_terminal_results == 0 and
  .duplicate_assistant_messages == 0 and
  .admission_user_message_reuse == 0
' "$result_dir/conversation-atomicity.json" >/dev/null

capture_postgres_snapshot "$result_dir/postgres-after.json"
assert_postgres_durability
jq -n \
  --arg kill_target "$kill_target" \
  --arg shutdown_mode "$shutdown_mode" \
  --argjson admitted "$admitted_after" \
  --argjson terminal "$terminal_after" \
  --argjson interrupted "$interrupted_after" \
  --argjson active_before_kill "$active_before_kill" \
  --argjson external_effects_before_kill "$effect_occurrence_delta" \
  --argjson expected_external_effects_before_kill "$expected_effects_before_kill" \
  --argjson provider_started_before_kill "$provider_started_delta" \
  --argjson provider_active_before_kill "$provider_active_before_kill" \
  --argjson provider_started_before_observation "$provider_started_before_observation" \
  --argjson provider_active_before_observation "$provider_active_before_observation" \
  --argjson provider_started_after_observation "$provider_started_after_observation" \
  --argjson provider_active_after_observation "$provider_active_after_observation" \
  --argjson provider_started_after_replay "$provider_started_after_replay" \
  --argjson provider_active_after_replay "$provider_active_after_replay" \
  --argjson provider_started_after_retry "$provider_started_after_retry" \
  --argjson provider_active_after_retry "$provider_active_after_retry" \
  --argjson external_effect_replay_occurrence "${replay_occurrence:-0}" \
  --argjson external_effect_explicit_retry_occurrence "${retry_occurrence:-0}" \
  --argjson external_effect_replay_attempts "${replay_attempts_stable:-0}" \
  --argjson external_effect_explicit_retry_attempts "${retry_attempts_stable:-0}" \
  --argjson active_after_retry "$active_after_retry" \
  --argjson public_get_runs_verified "$public_get_count" \
  --argjson no_recovery_attempts_before "$batch_attempts_before_observation" \
  --argjson no_recovery_attempts_after "$batch_attempts_after_observation" \
  --slurpfile conversation_atomicity "$result_dir/conversation-atomicity.json" \
  --slurpfile replacement_identity "$result_dir/replacement-identity.json" \
  '{
    passed: true,
    kill_target: $kill_target,
    shutdown_mode: $shutdown_mode,
    admitted: $admitted,
    terminal: $terminal,
    interrupted: $interrupted,
    active_before_kill: $active_before_kill,
    active_population_verified: ($active_before_kill == $admitted),
    replacement_identity: $replacement_identity[0],
    external_effects_before_kill: $external_effects_before_kill,
    expected_external_effects_before_kill: $expected_external_effects_before_kill,
    provider_started_before_kill: $provider_started_before_kill,
    provider_active_before_kill: $provider_active_before_kill,
    provider_started_before_observation: $provider_started_before_observation,
    provider_active_before_observation: $provider_active_before_observation,
    provider_started_after_observation: $provider_started_after_observation,
    provider_active_after_observation: $provider_active_after_observation,
    provider_started_after_same_request_replay: $provider_started_after_replay,
    provider_active_after_same_request_replay: $provider_active_after_replay,
    provider_started_after_explicit_retry: $provider_started_after_retry,
    provider_active_after_explicit_retry: $provider_active_after_retry,
    no_automatic_recovery: true,
    same_request_replayed_original_run: true,
    new_request_created_new_run: true,
    external_effect_replay_occurrence: $external_effect_replay_occurrence,
    external_effect_explicit_retry_occurrence: $external_effect_explicit_retry_occurrence,
    external_effect_replay_attempts: $external_effect_replay_attempts,
    external_effect_explicit_retry_attempts: $external_effect_explicit_retry_attempts,
    active_after_explicit_retry: $active_after_retry,
    public_get_runs_verified: $public_get_runs_verified,
    no_recovery_attempts_before: $no_recovery_attempts_before,
    no_recovery_attempts_after: $no_recovery_attempts_after,
    conversation_atomicity: $conversation_atomicity[0]
  }' >"$result_dir/gate-c-report.json"
printf 'Gate C %s-kill evidence: %s\n' "$kill_target" "$result_dir"
