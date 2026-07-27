#!/usr/bin/env bash
set -euo pipefail

profile=${1:?usage: run-gate-d.sh PROFILE DURATION RESULT_DIR}
duration=${2:?usage: run-gate-d.sh PROFILE DURATION RESULT_DIR}
result_dir=${3:?usage: run-gate-d.sh PROFILE DURATION RESULT_DIR}
listener_delay_seconds=${BENCH_LISTENER_FAULT_DELAY_SECONDS:-1200}
restart_delay_seconds=${BENCH_RUNTIME_RESTART_DELAY_SECONDS:-2700}

for value_name in listener_delay_seconds restart_delay_seconds; do
  value=${!value_name}
  if [[ ! "$value" =~ ^[0-9]+$ ]] || ((value < 1)); then
    printf '%s must be a positive integer\n' "$value_name" >&2
    exit 2
  fi
done
if ((restart_delay_seconds <= listener_delay_seconds)); then
  printf 'restart_delay_seconds must be greater than listener_delay_seconds\n' >&2
  exit 2
fi

mkdir -p "$result_dir"
started_at=$SECONDS
profile_pid=
fault_pid=

cleanup() {
  if [[ -n "$fault_pid" ]] && kill -0 "$fault_pid" >/dev/null 2>&1; then
    kill "$fault_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$profile_pid" ]] && kill -0 "$profile_pid" >/dev/null 2>&1; then
    kill "$profile_pid" >/dev/null 2>&1 || true
  fi
}
trap cleanup INT TERM

wait_until_offset() {
  local offset=$1
  while ((SECONDS - started_at < offset)); do
    if ! kill -0 "$profile_pid" >/dev/null 2>&1; then
      printf 'profile ended before the %ss fault offset\n' "$offset" >&2
      return 1
    fi
    sleep 5
  done
}

BENCH_SCENARIO=sustained \
  bash "$(dirname "$0")/run-profile.sh" \
  "$profile" 10 "$duration" "$result_dir" &
profile_pid=$!

(
  set +e
  wait_until_offset "$listener_delay_seconds" &&
    bash "$(dirname "$0")/inject-listener-fault.sh" \
      "$result_dir/listener-fault" &&
    wait_until_offset "$restart_delay_seconds" &&
    bash "$(dirname "$0")/inject-runtime-restart-during-claim.sh" \
      "$result_dir/runtime-restart"
  schedule_status=$?
  if ((schedule_status != 0)); then
    kill "$profile_pid" >/dev/null 2>&1 || true
  fi
  exit "$schedule_status"
) &
fault_pid=$!

set +e
wait "$profile_pid"
profile_status=$?
wait "$fault_pid"
fault_status=$?
set -e
trap - INT TERM

if ((profile_status != 0)); then
  printf 'Gate D profile failed with status %s\n' "$profile_status" >&2
  exit "$profile_status"
fi
if ((fault_status != 0)); then
  printf 'Gate D fault schedule failed with status %s\n' "$fault_status" >&2
  exit "$fault_status"
fi

printf 'Gate D profile and both fault injections completed: %s\n' "$result_dir"
