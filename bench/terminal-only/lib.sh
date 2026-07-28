#!/usr/bin/env bash
set -euo pipefail

terminal_bench_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    printf '%s is required\n' "$1" >&2
    exit 2
  }
}

require_nonempty() {
  local name=$1
  local value=$2
  if [[ -z "$value" ]]; then
    printf '%s must be set\n' "$name" >&2
    exit 2
  fi
}

duration_seconds() {
  local value=$1
  awk -v duration="$value" '
    BEGIN {
      if (duration ~ /^[0-9]+s$/) {
        sub(/s$/, "", duration); print duration
      } else if (duration ~ /^[0-9]+m$/) {
        sub(/m$/, "", duration); print duration * 60
      } else if (duration ~ /^[0-9]+h$/) {
        sub(/h$/, "", duration); print duration * 3600
      } else {
        exit 2
      }
    }
  '
}

postgres_command() {
  if [[ -n "${TERMINAL_BENCH_POSTGRES_URL:-}" ]]; then
    require_command psql
    psql "$TERMINAL_BENCH_POSTGRES_URL" -X -v ON_ERROR_STOP=1 "$@"
    return
  fi

  require_command kubectl
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local release=${BENCH_RELEASE:-bench}
  local pod=${BENCH_POSTGRES_POD:-"${release}-insight-agent-platform-postgresql-0"}
  kubectl -n "$namespace" exec -i "$pod" -- \
    psql -U "${BENCH_POSTGRES_USER:-insight}" \
    -d "${BENCH_POSTGRES_DATABASE:-insight_agent_platform}" \
    -X -v ON_ERROR_STOP=1 "$@"
}

postgres_file() {
  local file=$1
  shift
  postgres_command "$@" <"$file"
}

capture_postgres_snapshot() {
  local destination=$1
  postgres_file "$terminal_bench_root/bench/terminal-only/sql/snapshot.sql" \
    -qAt >"$destination"
}

capture_gate_b_before_snapshot() {
  local destination=$1
  postgres_file "$terminal_bench_root/bench/terminal-only/sql/snapshot.sql" \
    -qAt -v reset_statements=1 >"$destination"
}

extract_embedded_top_wal_csv() {
  local snapshot=$1
  local destination=$2
  require_command jq
  jq -r '
    [
      "queryid", "toplevel", "calls", "rows", "total_exec_ms", "mean_exec_ms",
      "shared_blks_hit", "shared_blks_read", "temp_blks_read",
      "temp_blks_written", "wal_records", "wal_fpi", "wal_bytes", "query"
    ] as $columns |
    ($columns | @csv),
    (.top_wal_statements[] |
      [
        .queryid, .toplevel, .calls, .rows, .total_exec_ms, .mean_exec_ms,
        .shared_blks_hit, .shared_blks_read, .temp_blks_read,
        .temp_blks_written, .wal_records, .wal_fpi, .wal_bytes, .query
      ] | @csv)
  ' "$snapshot" >"$destination"
}

ensure_gate_b_walinspect() {
  # Extension creation is deliberately outside both warm-up and the measured
  # LSN/statistics interval.
  postgres_command -qAt -c \
    'CREATE EXTENSION IF NOT EXISTS pg_walinspect;' >/dev/null
  postgres_command -qAt -c \
    "SELECT extversion FROM pg_extension WHERE extname='pg_walinspect';"
}

capture_gate_b_database_preflight() {
  local destination=$1
  require_command jq
  postgres_file \
    "$terminal_bench_root/bench/terminal-only/sql/gate-b-database-preflight.sql" \
    -qAt >"$destination"
  jq -e '.passed == true' "$destination" >/dev/null || {
    printf 'Gate B database is not fresh; see %s\n' "$destination" >&2
    return 1
  }
}

capture_physical_wal_records() {
  local start_lsn=$1
  local end_lsn=$2
  local destination=$3
  postgres_file \
    "$terminal_bench_root/bench/terminal-only/sql/physical-wal-records.sql" \
    -qAt \
    -v start_lsn="$start_lsn" \
    -v end_lsn="$end_lsn" \
    >"$destination"
}

extract_physical_wal_csv() {
  local snapshot=$1
  local destination=$2
  require_command jq
  jq -r '
    [
      "resource_manager", "record_type", "record_count",
      "record_length_bytes", "main_data_length_bytes", "fpi_length_bytes"
    ] as $columns |
    ($columns | @csv),
    (.groups[] |
      [
        .resource_manager, .record_type, .record_count,
        .record_length_bytes, .main_data_length_bytes, .fpi_length_bytes
      ] | @csv)
  ' "$snapshot" >"$destination"
}

capture_top_wal_statements() {
  local destination=$1
  postgres_file "$terminal_bench_root/bench/terminal-only/sql/top-wal-statements.sql" \
    --csv >"$destination"
}

assert_postgres_durability() {
  postgres_file "$terminal_bench_root/bench/terminal-only/sql/assert-durability.sql" \
    -qAt >/dev/null
}

api_curl() {
  require_command curl
  curl --fail-with-body --silent --show-error "$@"
}

qualification_container_status() {
  local pod_name=$1
  local pod_uid=$2
  local container_name=$3
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  kubectl -n "$namespace" get pod "$pod_name" -o json 2>/dev/null |
    jq -ce \
      --arg pod_uid "$pod_uid" \
      --arg container_name "$container_name" '
        select(.metadata.uid == $pod_uid) |
        [
          .status.containerStatuses[]? |
          select(.name == $container_name)
        ] |
        select(length == 1) |
        .[0] |
        {
          observed_at: (now | todateiso8601),
          pod_uid: $pod_uid,
          container_name: .name,
          container_id: (.containerID // null),
          restart_count: .restartCount,
          ready: .ready,
          state: (
            if .state.running then "running"
            elif .state.waiting then "waiting"
            elif .state.terminated then "terminated"
            else "unknown"
            end
          ),
          current_terminated_container_id:
            (.state.terminated.containerID // null),
          current_terminated_exit_code:
            (.state.terminated.exitCode // null),
          current_terminated_signal:
            (.state.terminated.signal // null),
          current_terminated_reason:
            (.state.terminated.reason // null),
          last_terminated_container_id:
            (.lastState.terminated.containerID // null),
          last_terminated_exit_code:
            (.lastState.terminated.exitCode // null),
          last_terminated_signal:
            (.lastState.terminated.signal // null),
          last_terminated_reason:
            (.lastState.terminated.reason // null)
        }
      '
}

qualification_container_death_observation() {
  local pod_name=$1
  local pod_uid=$2
  local container_name=$3
  local original_container_id=$4
  local original_restart_count=$5
  local status
  status=$(
    qualification_container_status "$pod_name" "$pod_uid" "$container_name" ||
      true
  )
  [[ -n "$status" ]] || return 1
  jq -ce \
    --arg original_container_id "$original_container_id" \
    --argjson original_restart_count "$original_restart_count" '
      . + {
        original_container_id: $original_container_id,
        original_restart_count: $original_restart_count,
        current_original_container_terminated:
          (.current_terminated_container_id == $original_container_id),
        last_original_container_terminated:
          (.last_terminated_container_id == $original_container_id),
        restart_count_increased:
          (.restart_count > $original_restart_count),
        container_id_changed: (
          .container_id != null and
          .container_id != $original_container_id
        ),
        original_terminated_exit_code: (
          if .current_terminated_container_id == $original_container_id
          then .current_terminated_exit_code
          elif .last_terminated_container_id == $original_container_id
          then .last_terminated_exit_code
          else null
          end
        ),
        original_terminated_signal: (
          if .current_terminated_container_id == $original_container_id
          then .current_terminated_signal
          elif .last_terminated_container_id == $original_container_id
          then .last_terminated_signal
          else null
          end
        ),
        original_terminated_reason: (
          if .current_terminated_container_id == $original_container_id
          then .current_terminated_reason
          elif .last_terminated_container_id == $original_container_id
          then .last_terminated_reason
          else null
          end
        )
      } |
      . + {
        original_container_terminated: (
          .current_original_container_terminated or
          .last_original_container_terminated
        ),
        original_termination_reason_recorded: (
          (.original_terminated_reason | type) == "string" and
          (.original_terminated_reason | length) > 0
        ),
        original_termination_not_oom: (
          (.original_terminated_reason | type) == "string" and
          .original_terminated_reason != "OOMKilled"
        )
      } |
      . + {
        hard_process_death_confirmed: (
          .original_container_terminated and
          .last_original_container_terminated and
          .restart_count_increased and
          .container_id_changed
        )
      }
    ' <<<"$status"
}

qualification_wait_for_container_death() {
  local pod_name=$1
  local pod_uid=$2
  local container_name=$3
  local original_container_id=$4
  local original_restart_count=$5
  local timeout_seconds=$6
  local deadline=$((SECONDS + timeout_seconds))
  local observation=
  while ((SECONDS < deadline)); do
    observation=$(
      qualification_container_death_observation \
        "$pod_name" \
        "$pod_uid" \
        "$container_name" \
        "$original_container_id" \
        "$original_restart_count" ||
        true
    )
    if [[ -n "$observation" ]] &&
      jq -e '.hard_process_death_confirmed == true' \
        <<<"$observation" >/dev/null; then
      printf '%s\n' "$observation"
      return 0
    fi
    sleep 0.1
  done
  [[ -z "$observation" ]] || printf '%s\n' "$observation"
  return 1
}

qualification_capture_watched_container_death() {
  local watch_file=$1
  local evidence_file=$2
  local pod_name=$3
  local pod_uid=$4
  local container_name=$5
  local original_container_id=$6
  local original_restart_count=$7
  local attach_resource_version=$8
  local attach_line_number=$9
  local row=
  row=$(awk -F '|' \
    -v pod_uid="$pod_uid" \
    -v original_container_id="$original_container_id" \
    -v original_restart_count="$original_restart_count" \
    -v attach_resource_version="$attach_resource_version" \
    -v attach_line_number="$attach_line_number" '
      NR > attach_line_number &&
      $1 != "" &&
      $1 != attach_resource_version &&
      $2 == pod_uid &&
      $5 == original_container_id &&
      ($4 + 0) == original_restart_count + 1 &&
      $3 != "" &&
      $3 != original_container_id &&
      $8 != "" &&
      $9 != "" {
        selected = $0
      }
      END {
        if (selected == "") exit 1
        print selected
      }
    ' "$watch_file" 2>/dev/null || true)
  if [[ -z "$row" ]]; then
    jq -n \
      --arg pod_name "$pod_name" \
      --arg pod_uid "$pod_uid" \
      --arg container_name "$container_name" \
      --arg original_container_id "$original_container_id" \
      --arg attach_resource_version "$attach_resource_version" \
      --argjson original_restart_count "$original_restart_count" \
      --argjson attach_line_number "$attach_line_number" \
      '{
        source: "kubernetes_pod_status_watch",
        pod_name: $pod_name,
        pod_uid: $pod_uid,
        container_name: $container_name,
        original_container_id: $original_container_id,
        original_restart_count: $original_restart_count,
        attach_resource_version: $attach_resource_version,
        attach_line_number: $attach_line_number,
        exact_original_termination_captured: false,
        hard_process_death_confirmed: false
      }' >"$evidence_file"
    return 1
  fi

  local resource_version
  local observed_pod_uid
  local replacement_container_id
  local restart_count
  local terminated_container_id
  local terminated_exit_code
  local terminated_signal
  local terminated_reason
  local terminated_finished_at
  IFS='|' read -r \
    resource_version \
    observed_pod_uid \
    replacement_container_id \
    restart_count \
    terminated_container_id \
    terminated_exit_code \
    terminated_signal \
    terminated_reason \
    terminated_finished_at <<<"$row"
  jq -n \
    --arg resource_version "$resource_version" \
    --arg pod_name "$pod_name" \
    --arg pod_uid "$observed_pod_uid" \
    --arg container_name "$container_name" \
    --arg original_container_id "$original_container_id" \
    --arg replacement_container_id "$replacement_container_id" \
    --arg terminated_container_id "$terminated_container_id" \
    --arg terminated_exit_code "$terminated_exit_code" \
    --arg terminated_signal "$terminated_signal" \
    --arg terminated_reason "$terminated_reason" \
    --arg terminated_finished_at "$terminated_finished_at" \
    --arg attach_resource_version "$attach_resource_version" \
    --argjson original_restart_count "$original_restart_count" \
    --argjson restart_count "$restart_count" \
    --argjson attach_line_number "$attach_line_number" \
    '{
      source: "kubernetes_pod_status_watch",
      resource_version: $resource_version,
      captured_at: (now | todateiso8601),
      pod_name: $pod_name,
      pod_uid: $pod_uid,
      container_name: $container_name,
      container_id: $replacement_container_id,
      restart_count: $restart_count,
      ready: null,
      state: null,
      current_terminated_container_id: null,
      current_terminated_exit_code: null,
      current_terminated_signal: null,
      current_terminated_reason: null,
      last_terminated_container_id: $terminated_container_id,
      last_terminated_exit_code:
        (if $terminated_exit_code == ""
         then null else ($terminated_exit_code | tonumber) end),
      last_terminated_signal:
        (if $terminated_signal == ""
         then null else ($terminated_signal | tonumber) end),
      last_terminated_reason: $terminated_reason,
      last_terminated_finished_at: $terminated_finished_at,
      original_container_id: $original_container_id,
      original_restart_count: $original_restart_count,
      attach_resource_version: $attach_resource_version,
      attach_line_number: $attach_line_number,
      current_original_container_terminated: false,
      last_original_container_terminated:
        ($terminated_container_id == $original_container_id),
      restart_count_increased: ($restart_count > $original_restart_count),
      container_id_changed:
        ($replacement_container_id != $original_container_id),
      original_terminated_exit_code:
        (if $terminated_exit_code == ""
         then null else ($terminated_exit_code | tonumber) end),
      original_terminated_signal:
        (if $terminated_signal == ""
         then null else ($terminated_signal | tonumber) end),
      original_terminated_reason: $terminated_reason,
      original_terminated_finished_at: $terminated_finished_at,
      original_container_terminated:
        ($terminated_container_id == $original_container_id),
      original_termination_reason_recorded:
        ($terminated_reason != ""),
      original_termination_not_oom:
        ($terminated_reason != "" and $terminated_reason != "OOMKilled"),
      exact_original_termination_captured: (
        $pod_uid != "" and
        $resource_version != $attach_resource_version and
        $terminated_container_id == $original_container_id and
        $restart_count == ($original_restart_count + 1) and
        $replacement_container_id != $original_container_id and
        $terminated_finished_at != ""
      ),
      hard_process_death_confirmed: (
        $pod_uid != "" and
        $resource_version != $attach_resource_version and
        $terminated_container_id == $original_container_id and
        $restart_count == ($original_restart_count + 1) and
        $replacement_container_id != $original_container_id and
        $terminated_finished_at != ""
      )
    }' >"$evidence_file"
  jq -e '.hard_process_death_confirmed == true' "$evidence_file" >/dev/null
}

qualification_log_token_count() {
  local token=$1
  local log_file=$2
  awk -v token="$token" '
    {
      remaining = $0
      while ((position = index(remaining, token)) > 0) {
        count += 1
        remaining = substr(remaining, position + length(token))
      }
    }
    END { print count + 0 }
  ' "$log_file"
}

qualification_capture_previous_container_logs() {
  local pod_name=$1
  local pod_uid=$2
  local container_name=$3
  local original_container_id=$4
  local original_restart_count=$5
  local timeout_seconds=$6
  local result_dir=$7
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local request_timeout=${QUALIFICATION_PROCESS_TRIGGER_TIMEOUT:-10s}
  local log_file="$result_dir/previous-container.log"
  local stderr_file="$result_dir/previous-container-log.stderr"
  local evidence_file="$result_dir/previous-container-log-evidence.json"
  local deadline=$((SECONDS + timeout_seconds))
  local log_status=1
  local exact_original_container=false
  local observation=
  : >"$log_file"
  : >"$stderr_file"

  while ((SECONDS < deadline)); do
    observation=$(
      qualification_container_death_observation \
        "$pod_name" \
        "$pod_uid" \
        "$container_name" \
        "$original_container_id" \
        "$original_restart_count" ||
        true
    )
    if [[ -n "$observation" ]] &&
      jq -e \
        --arg original_container_id "$original_container_id" '
          .last_terminated_container_id == $original_container_id
        ' <<<"$observation" >/dev/null; then
      if kubectl -n "$namespace" --request-timeout="$request_timeout" logs \
        "$pod_name" -c "$container_name" --previous \
        >"$log_file" 2>"$stderr_file"; then
        log_status=0
      else
        log_status=$?
      fi
      observation=$(
        qualification_container_death_observation \
          "$pod_name" \
          "$pod_uid" \
          "$container_name" \
          "$original_container_id" \
          "$original_restart_count" ||
          true
      )
      if ((log_status == 0)) &&
        [[ -n "$observation" ]] &&
        jq -e \
          --arg original_container_id "$original_container_id" '
            .last_terminated_container_id == $original_container_id
          ' <<<"$observation" >/dev/null; then
        exact_original_container=true
        break
      fi
    fi
    sleep 0.1
  done

  local runtime_abort_marker_count
  local postgres_immediate_shutdown_log_lines
  runtime_abort_marker_count=$(
    qualification_log_token_count QUALIFICATION_SELF_ABORT "$log_file"
  )
  postgres_immediate_shutdown_log_lines=$(
    awk '
      BEGIN { IGNORECASE = 1 }
      /received (an )?immediate shutdown request/ ||
      /database system is shut down/ {
        count += 1
      }
      END { print count + 0 }
    ' "$log_file"
  )
  local observation_json=${observation:-null}
  jq -n \
    --arg pod_name "$pod_name" \
    --arg pod_uid "$pod_uid" \
    --arg container_name "$container_name" \
    --arg container_id "$original_container_id" \
    --argjson original_restart_count "$original_restart_count" \
    --argjson kubectl_logs_status "$log_status" \
    --argjson exact_original_container "$exact_original_container" \
    --argjson runtime_abort_marker_count "$runtime_abort_marker_count" \
    --argjson postgres_immediate_shutdown_log_lines \
      "$postgres_immediate_shutdown_log_lines" \
    --argjson final_container_observation "$observation_json" \
    '{
      pod_name: $pod_name,
      pod_uid: $pod_uid,
      container_name: $container_name,
      original_container_id: $container_id,
      original_restart_count: $original_restart_count,
      kubectl_logs_status: $kubectl_logs_status,
      exact_original_container: $exact_original_container,
      runtime_abort_marker: "QUALIFICATION_SELF_ABORT",
      runtime_abort_marker_count: $runtime_abort_marker_count,
      postgres_immediate_shutdown_log_lines:
        $postgres_immediate_shutdown_log_lines,
      final_container_observation: $final_container_observation
    }' >"$evidence_file"

  ((log_status == 0)) && [[ "$exact_original_container" == true ]]
}

qualification_capture_process_incarnation() {
  local trigger_kind=$1
  local pod_name=$2
  local pod_uid=$3
  local container_name=$4
  local original_container_id=$5
  local original_restart_count=$6
  local result_dir=$7
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local request_timeout=${QUALIFICATION_PROCESS_TRIGGER_TIMEOUT:-10s}
  local token_file="$result_dir/process-incarnation-token.txt"
  local stderr_file="$result_dir/process-incarnation-token.stderr"
  local evidence_file="$result_dir/process-incarnation-before.json"
  local capture_status
  : >"$token_file"
  : >"$stderr_file"

  case "$trigger_kind" in
    runtime_self_abort)
      if kubectl -n "$namespace" --request-timeout="$request_timeout" exec \
        -c "$container_name" "$pod_name" -- \
        sh -ec '
          read -r process_stat </proc/1/stat
          process_tail=${process_stat##*) }
          set -- $process_tail
          [ "$#" -ge 20 ]
          shift 19
          case "$1" in ""|*[!0-9]*) exit 2 ;; esac
          printf "1|%s\n" "$1"
        ' >"$token_file" 2>"$stderr_file"; then
        capture_status=0
      else
        capture_status=$?
      fi
      ;;
    postgres_immediate_shutdown)
      if kubectl -n "$namespace" --request-timeout="$request_timeout" exec \
        -c "$container_name" "$pod_name" -- \
        sh -ec '
          postmaster_pid=$(head -n 1 "$PGDATA/postmaster.pid")
          case "$postmaster_pid" in ""|*[!0-9]*) exit 2 ;; esac
          read -r process_stat <"/proc/$postmaster_pid/stat"
          process_tail=${process_stat##*) }
          set -- $process_tail
          [ "$#" -ge 20 ]
          shift 19
          case "$1" in ""|*[!0-9]*) exit 2 ;; esac
          printf "%s|%s\n" "$postmaster_pid" "$1"
        ' >"$token_file" 2>"$stderr_file"; then
        capture_status=0
      else
        capture_status=$?
      fi
      ;;
    *)
      printf 'unsupported process-incarnation trigger: %s\n' \
        "$trigger_kind" >&2
      return 2
      ;;
  esac

  local process_pid=
  local process_start_time_ticks=
  local token_valid=false
  local token_line_count
  token_line_count=$(wc -l <"$token_file")
  if ((capture_status == 0 && token_line_count == 1)); then
    IFS='|' read -r process_pid process_start_time_ticks <"$token_file"
    if [[ "$process_pid" =~ ^[1-9][0-9]*$ &&
          "$process_start_time_ticks" =~ ^[1-9][0-9]*$ ]]; then
      token_valid=true
    fi
  fi

  local container_after_token
  container_after_token=$(
    qualification_container_status \
      "$pod_name" "$pod_uid" "$container_name" ||
      true
  )
  local container_after_token_json=${container_after_token:-null}
  jq -n \
    --arg trigger_kind "$trigger_kind" \
    --arg pod_name "$pod_name" \
    --arg pod_uid "$pod_uid" \
    --arg container_name "$container_name" \
    --arg original_container_id "$original_container_id" \
    --arg process_pid "$process_pid" \
    --arg process_start_time_ticks "$process_start_time_ticks" \
    --argjson original_restart_count "$original_restart_count" \
    --argjson capture_status "$capture_status" \
    --argjson token_line_count "$token_line_count" \
    --argjson token_valid "$token_valid" \
    --argjson container_after_token "$container_after_token_json" \
    '(
      $container_after_token != null and
      $container_after_token.pod_uid == $pod_uid and
      $container_after_token.container_name == $container_name and
      $container_after_token.container_id == $original_container_id and
      $container_after_token.restart_count == $original_restart_count and
      $container_after_token.state == "running"
    ) as $container_identity_bound |
    {
      captured_at: (now | todateiso8601),
      trigger_kind: $trigger_kind,
      pod_name: $pod_name,
      pod_uid: $pod_uid,
      container_name: $container_name,
      original_container_id: $original_container_id,
      original_restart_count: $original_restart_count,
      kubectl_exec_status: $capture_status,
      token_line_count: $token_line_count,
      process_token: {
        pid: (if $token_valid then ($process_pid | tonumber) else null end),
        start_time_ticks:
          (if $token_valid
           then ($process_start_time_ticks | tonumber)
           else null
           end)
      },
      token_valid: $token_valid,
      container_after_token: $container_after_token,
      container_identity_bound: $container_identity_bound,
      captured_and_bound: (
        $capture_status == 0 and
        $token_valid and
        $container_identity_bound
      )
    }' >"$evidence_file"
  jq -e '.captured_and_bound == true' "$evidence_file" >/dev/null
}

qualification_cleanup_process_death_backgrounds() {
  [[ "${trigger_background_cleanup_active:-false}" == true ]] || return 0
  trigger_background_cleanup_active=false
  if [[ -n "${status_watch_pid:-}" ]]; then
    kill "$status_watch_pid" 2>/dev/null || true
    wait "$status_watch_pid" 2>/dev/null || true
    status_watch_pid=
  fi
  if [[ -n "${live_log_pid:-}" ]]; then
    kill "$live_log_pid" 2>/dev/null || true
    wait "$live_log_pid" 2>/dev/null || true
    live_log_pid=
  fi
  trap - RETURN INT TERM
  [[ -z "${saved_return_trap:-}" ]] || eval "$saved_return_trap"
  [[ -z "${saved_int_trap:-}" ]] || eval "$saved_int_trap"
  [[ -z "${saved_term_trap:-}" ]] || eval "$saved_term_trap"
}

qualification_trigger_container_death() {
  local trigger_kind=$1
  local pod_name=$2
  local pod_uid=$3
  local container_name=$4
  local result_dir=$5
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local death_timeout_seconds=${QUALIFICATION_PROCESS_DEATH_TIMEOUT_SECONDS:-30}
  local trigger_timeout=${QUALIFICATION_PROCESS_TRIGGER_TIMEOUT:-10s}
  local previous_log_timeout_seconds=${QUALIFICATION_PREVIOUS_LOG_TIMEOUT_SECONDS:-15}
  [[ "$death_timeout_seconds" =~ ^[1-9][0-9]*$ ]] || {
    printf 'QUALIFICATION_PROCESS_DEATH_TIMEOUT_SECONDS must be a positive integer\n' >&2
    return 2
  }
  [[ "$trigger_timeout" =~ ^[1-9][0-9]*s$ ]] || {
    printf 'QUALIFICATION_PROCESS_TRIGGER_TIMEOUT must be a positive integer number of seconds\n' >&2
    return 2
  }
  [[ "$previous_log_timeout_seconds" =~ ^[1-9][0-9]*$ ]] || {
    printf 'QUALIFICATION_PREVIOUS_LOG_TIMEOUT_SECONDS must be a positive integer\n' >&2
    return 2
  }
  [[ "$container_name" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || {
    printf 'qualification target container name is invalid: %s\n' \
      "$container_name" >&2
    return 2
  }
  case "$trigger_kind" in
    runtime_self_abort|postgres_immediate_shutdown) ;;
    *)
      printf 'unsupported qualification process-death trigger: %s\n' \
        "$trigger_kind" >&2
      return 2
      ;;
  esac

  local container_before
  container_before=$(
    qualification_container_status "$pod_name" "$pod_uid" "$container_name" ||
      true
  )
  if [[ -z "$container_before" ]] ||
    ! jq -e '
      .state == "running" and
      (.container_id | type == "string" and length > 0) and
      (.restart_count | type == "number" and . >= 0)
    ' <<<"$container_before" >/dev/null; then
    printf 'could not capture the running qualification target container\n' >&2
    return 1
  fi
  printf '%s\n' "$container_before" \
    >"$result_dir/hard-container-before.json"
  local original_container_id
  local original_restart_count
  original_container_id=$(jq -er '.container_id' <<<"$container_before")
  original_restart_count=$(jq -er '.restart_count' <<<"$container_before")
  if ! qualification_capture_process_incarnation \
    "$trigger_kind" \
    "$pod_name" \
    "$pod_uid" \
    "$container_name" \
    "$original_container_id" \
    "$original_restart_count" \
    "$result_dir"; then
    printf 'could not bind the process trigger to the original container incarnation\n' >&2
    return 1
  fi
  local expected_process_pid
  local expected_process_start_time_ticks
  expected_process_pid=$(
    jq -er '.process_token.pid' \
      "$result_dir/process-incarnation-before.json"
  )
  expected_process_start_time_ticks=$(
    jq -er '.process_token.start_time_ticks' \
      "$result_dir/process-incarnation-before.json"
  )

  # A single Kubernetes watch records every Pod status update delivered by
  # the API server after this attach point.  Kubelet can restart a replacement
  # process twice before a polling client observes the first `lastState`;
  # without the watch, the exact original record can be overwritten.  The
  # parser fails closed if the API server never emits that intermediate state.
  local status_watch_file="$result_dir/container-status-watch.tsv"
  local status_watch_stderr="$result_dir/container-status-watch.stderr"
  local status_watch_evidence="$result_dir/container-status-watch-evidence.json"
  local status_watch_attach_evidence="$result_dir/container-status-watch-attach.json"
  local status_watch_pid=
  local live_log_pid=
  local saved_return_trap
  local saved_int_trap
  local saved_term_trap
  local trigger_background_cleanup_active=false
  saved_return_trap=$(trap -p RETURN || true)
  saved_int_trap=$(trap -p INT || true)
  saved_term_trap=$(trap -p TERM || true)
  local status_watch_timeout_seconds=$((death_timeout_seconds + previous_log_timeout_seconds + 10))
  : >"$status_watch_file"
  : >"$status_watch_stderr"
  kubectl -n "$namespace" get pod "$pod_name" \
    --watch \
    --request-timeout="${status_watch_timeout_seconds}s" \
    -o 'jsonpath={.metadata.resourceVersion}{"|"}{.metadata.uid}{"|"}{.status.containerStatuses[?(@.name=="'"$container_name"'")].containerID}{"|"}{.status.containerStatuses[?(@.name=="'"$container_name"'")].restartCount}{"|"}{.status.containerStatuses[?(@.name=="'"$container_name"'")].lastState.terminated.containerID}{"|"}{.status.containerStatuses[?(@.name=="'"$container_name"'")].lastState.terminated.exitCode}{"|"}{.status.containerStatuses[?(@.name=="'"$container_name"'")].lastState.terminated.signal}{"|"}{.status.containerStatuses[?(@.name=="'"$container_name"'")].lastState.terminated.reason}{"|"}{.status.containerStatuses[?(@.name=="'"$container_name"'")].lastState.terminated.finishedAt}{"\n"}' \
    >"$status_watch_file" 2>"$status_watch_stderr" &
  status_watch_pid=$!
  trigger_background_cleanup_active=true
  trap qualification_cleanup_process_death_backgrounds RETURN
  trap 'qualification_cleanup_process_death_backgrounds; return 130' INT
  trap 'qualification_cleanup_process_death_backgrounds; return 143' TERM

  # Follow the exact current container before sending the signal. Some local
  # CRI implementations can lose `kubectl logs --previous` as soon as the
  # first replacement process itself exits on the still-live owner lease.
  # This live copy keeps the in-process marker without weakening the separate
  # container-ID/restart/termination checks below.
  local live_log_file="$result_dir/original-container-live.log"
  local live_log_stderr="$result_dir/original-container-live.stderr"
  local live_log_evidence="$result_dir/original-container-live-evidence.json"
  local live_log_started_at
  local live_log_attached=true
  live_log_started_at=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
  : >"$live_log_file"
  : >"$live_log_stderr"
  kubectl -n "$namespace" --request-timeout="$trigger_timeout" logs \
    "$pod_name" -c "$container_name" --follow --tail=0 \
    >"$live_log_file" 2>"$live_log_stderr" &
  live_log_pid=$!
  local status_watch_attached=false
  local status_watch_attach_anchor=
  local status_watch_attach_line=
  local status_watch_attach_resource_version=
  local status_watch_attach_deadline=$((SECONDS + 5))
  while ((SECONDS < status_watch_attach_deadline)); do
    status_watch_attach_anchor=$(
      awk -F '|' \
        -v pod_uid="$pod_uid" \
        -v original_container_id="$original_container_id" \
        -v original_restart_count="$original_restart_count" '
          $1 != "" &&
          $2 == pod_uid &&
          $3 == original_container_id &&
          ($4 + 0) == original_restart_count {
            print NR "|" $1
            exit
          }
        ' "$status_watch_file" 2>/dev/null ||
        true
    )
    if [[ -n "$status_watch_attach_anchor" ]]; then
      status_watch_attached=true
      break
    fi
    kill -0 "$status_watch_pid" 2>/dev/null || break
    sleep 0.05
  done
  if [[ "$status_watch_attached" != true ]]; then
    printf 'qualification Pod status watch did not attach before trigger\n' >&2
    return 1
  fi
  IFS='|' read -r \
    status_watch_attach_line \
    status_watch_attach_resource_version <<<"$status_watch_attach_anchor"
  [[ "$status_watch_attach_line" =~ ^[1-9][0-9]*$ &&
     -n "$status_watch_attach_resource_version" ]] || {
    printf 'qualification Pod status watch attach anchor is invalid\n' >&2
    return 1
  }
  jq -n \
    --arg captured_at "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    --arg pod_name "$pod_name" \
    --arg pod_uid "$pod_uid" \
    --arg container_name "$container_name" \
    --arg container_id "$original_container_id" \
    --arg resource_version "$status_watch_attach_resource_version" \
    --argjson restart_count "$original_restart_count" \
    --argjson line_number "$status_watch_attach_line" \
    '{
      captured_at: $captured_at,
      pod_name: $pod_name,
      pod_uid: $pod_uid,
      container_name: $container_name,
      container_id: $container_id,
      restart_count: $restart_count,
      resource_version: $resource_version,
      raw_line_number: $line_number,
      bound_to_original_container: true
    }' >"$status_watch_attach_evidence"
  if ! kill -0 "$live_log_pid" 2>/dev/null; then
    live_log_attached=false
  fi

  local trigger_started_at
  local trigger_signal
  local terminal_signal
  local trigger_status
  trigger_started_at=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
  case "$trigger_kind" in
    runtime_self_abort)
      trigger_signal=SIGUSR2
      terminal_signal=SIGABRT
      if kubectl -n "$namespace" --request-timeout="$trigger_timeout" exec \
        -c "$container_name" "$pod_name" -- \
        sh -ec '
          expected_pid=$1
          expected_start_time_ticks=$2
          [ "$expected_pid" = 1 ]
          read -r process_stat </proc/1/stat
          process_tail=${process_stat##*) }
          set -- $process_tail
          [ "$#" -ge 20 ]
          shift 19
          [ "$1" = "$expected_start_time_ticks" ]
          printf "%s|%s\n" "$expected_pid" "$expected_start_time_ticks"
          kill -USR2 1
        ' sh "$expected_process_pid" "$expected_process_start_time_ticks" \
        >"$result_dir/process-trigger-exec.txt" 2>&1; then
        trigger_status=0
      else
        trigger_status=$?
      fi
      ;;
    postgres_immediate_shutdown)
      trigger_signal=SIGQUIT
      terminal_signal=SIGQUIT
      if kubectl -n "$namespace" --request-timeout="$trigger_timeout" exec \
        -c "$container_name" "$pod_name" -- \
        sh -ec '
          expected_pid=$1
          expected_start_time_ticks=$2
          postmaster_pid=$(head -n 1 "$PGDATA/postmaster.pid")
          case "$postmaster_pid" in
            ""|*[!0-9]*) exit 2 ;;
          esac
          [ "$postmaster_pid" = "$expected_pid" ]
          read -r process_stat <"/proc/$postmaster_pid/stat"
          process_tail=${process_stat##*) }
          set -- $process_tail
          [ "$#" -ge 20 ]
          shift 19
          [ "$1" = "$expected_start_time_ticks" ]
          printf "%s|%s\n" "$expected_pid" "$expected_start_time_ticks"
          kill -QUIT "$postmaster_pid"
        ' sh "$expected_process_pid" "$expected_process_start_time_ticks" \
        >"$result_dir/process-trigger-exec.txt" 2>&1; then
        trigger_status=0
      else
        trigger_status=$?
      fi
      ;;
  esac

  local trigger_incarnation_token_echoed=false
  if grep -Fxq \
    "${expected_process_pid}|${expected_process_start_time_ticks}" \
    "$result_dir/process-trigger-exec.txt"; then
    trigger_incarnation_token_echoed=true
  fi
  jq -n \
    --arg trigger_kind "$trigger_kind" \
    --arg trigger_signal "$trigger_signal" \
    --arg terminal_signal "$terminal_signal" \
    --arg pod_name "$pod_name" \
    --arg pod_uid "$pod_uid" \
    --arg container_name "$container_name" \
    --arg container_id "$original_container_id" \
    --arg process_pid "$expected_process_pid" \
    --arg process_start_time_ticks "$expected_process_start_time_ticks" \
    --arg started_at "$trigger_started_at" \
    --arg request_timeout "$trigger_timeout" \
    --argjson restart_count "$original_restart_count" \
    --argjson kubectl_exec_status "$trigger_status" \
    --argjson incarnation_token_echoed \
      "$trigger_incarnation_token_echoed" \
    '{
      trigger_kind: $trigger_kind,
      trigger_signal: $trigger_signal,
      terminal_signal: $terminal_signal,
      pod_name: $pod_name,
      pod_uid: $pod_uid,
      container_name: $container_name,
      container_id: $container_id,
      restart_count: $restart_count,
      expected_process_token: {
        pid: ($process_pid | tonumber),
        start_time_ticks: ($process_start_time_ticks | tonumber)
      },
      incarnation_match_required: true,
      incarnation_token_echoed: $incarnation_token_echoed,
      started_at: $started_at,
      request_timeout: $request_timeout,
      kubectl_exec_status: $kubectl_exec_status
    }' >"$result_dir/process-trigger-command.json"

  local death_observation
  death_observation=$(
    qualification_wait_for_container_death \
      "$pod_name" \
      "$pod_uid" \
      "$container_name" \
      "$original_container_id" \
      "$original_restart_count" \
      "$death_timeout_seconds" ||
      true
  )
  local death_after
  if [[ -n "$death_observation" ]]; then
    death_after=$death_observation
  else
    death_after=null
  fi

  local live_log_status=0
  local live_log_deadline=$((SECONDS + previous_log_timeout_seconds))
  while kill -0 "$live_log_pid" 2>/dev/null &&
    ((SECONDS < live_log_deadline)); do
    sleep 0.1
  done
  if kill -0 "$live_log_pid" 2>/dev/null; then
    kill "$live_log_pid" 2>/dev/null || true
  fi
  if wait "$live_log_pid"; then
    live_log_status=0
  else
    live_log_status=$?
  fi
  live_log_pid=
  local live_runtime_abort_marker_count
  local live_postgres_shutdown_log_lines
  live_runtime_abort_marker_count=$(
    qualification_log_token_count QUALIFICATION_SELF_ABORT "$live_log_file"
  )
  live_postgres_shutdown_log_lines=$(
    awk '
      BEGIN { IGNORECASE = 1 }
      /received (an )?immediate shutdown request/ ||
      /database system is shut down/ {
        count += 1
      }
      END { print count + 0 }
    ' "$live_log_file"
  )
  jq -n \
    --arg pod_name "$pod_name" \
    --arg pod_uid "$pod_uid" \
    --arg container_name "$container_name" \
    --arg original_container_id "$original_container_id" \
    --arg started_at "$live_log_started_at" \
    --argjson attached "$live_log_attached" \
    --argjson process_status "$live_log_status" \
    --argjson runtime_abort_marker_count \
      "$live_runtime_abort_marker_count" \
    --argjson postgres_immediate_shutdown_log_lines \
      "$live_postgres_shutdown_log_lines" \
    '{
      pod_name: $pod_name,
      pod_uid: $pod_uid,
      container_name: $container_name,
      original_container_id: $original_container_id,
      started_before_trigger: $started_at,
      attached_before_trigger: $attached,
      process_status: $process_status,
      runtime_abort_marker: "QUALIFICATION_SELF_ABORT",
      runtime_abort_marker_count: $runtime_abort_marker_count,
      postgres_immediate_shutdown_log_lines:
        $postgres_immediate_shutdown_log_lines
    }' >"$live_log_evidence"

  local previous_logs_status
  if qualification_capture_previous_container_logs \
    "$pod_name" \
    "$pod_uid" \
    "$container_name" \
    "$original_container_id" \
    "$original_restart_count" \
    "$previous_log_timeout_seconds" \
    "$result_dir"; then
    previous_logs_status=0
  else
    previous_logs_status=$?
  fi

  # Retain the exact resourceVersion that still names the original container
  # in `lastState`.  Polling may now see a later replacement's termination.
  local status_watch_capture_status=1
  local status_watch_capture_deadline=$((SECONDS + 5))
  while ((SECONDS < status_watch_capture_deadline)); do
    if qualification_capture_watched_container_death \
      "$status_watch_file" \
      "$status_watch_evidence" \
      "$pod_name" \
      "$pod_uid" \
      "$container_name" \
      "$original_container_id" \
      "$original_restart_count" \
      "$status_watch_attach_resource_version" \
      "$status_watch_attach_line"; then
      status_watch_capture_status=0
      break
    else
      status_watch_capture_status=$?
    fi
    kill -0 "$status_watch_pid" 2>/dev/null || break
    sleep 0.1
  done

  local status_watch_process_status=0
  local status_watch_stopped_by_harness=false
  if kill -0 "$status_watch_pid" 2>/dev/null; then
    status_watch_stopped_by_harness=true
    kill "$status_watch_pid" 2>/dev/null || true
  fi
  if wait "$status_watch_pid" 2>/dev/null; then
    status_watch_process_status=0
  else
    status_watch_process_status=$?
  fi
  status_watch_pid=
  if ((status_watch_capture_status != 0)); then
    if qualification_capture_watched_container_death \
      "$status_watch_file" \
      "$status_watch_evidence" \
      "$pod_name" \
      "$pod_uid" \
      "$container_name" \
      "$original_container_id" \
      "$original_restart_count" \
      "$status_watch_attach_resource_version" \
      "$status_watch_attach_line"; then
      status_watch_capture_status=0
    else
      status_watch_capture_status=$?
    fi
  fi
  jq \
    --argjson attached_before_trigger "$status_watch_attached" \
    --argjson capture_status "$status_watch_capture_status" \
    --argjson process_status "$status_watch_process_status" \
    --argjson stopped_by_harness "$status_watch_stopped_by_harness" \
    '. + {
      attached_before_trigger: $attached_before_trigger,
      capture_status: $capture_status,
      watch_process_status: $process_status,
      stopped_by_harness: $stopped_by_harness,
      stop_reason: (
        if $capture_status == 0 and $stopped_by_harness
        then "captured"
        elif $stopped_by_harness
        then "capture-timeout"
        else "watch-ended"
        end
      )
    }' \
    "$status_watch_evidence" >"$status_watch_evidence.tmp"
  mv "$status_watch_evidence.tmp" "$status_watch_evidence"

  local watched_death
  watched_death=$(jq -c . "$status_watch_evidence")
  if ! jq -e '.hard_process_death_confirmed == true' \
    <<<"$death_after" >/dev/null 2>&1 &&
    jq -e '.hard_process_death_confirmed == true' \
      <<<"$watched_death" >/dev/null; then
    death_after=$watched_death
  fi

  jq -n \
    --slurpfile before "$result_dir/hard-container-before.json" \
    --slurpfile process_incarnation \
      "$result_dir/process-incarnation-before.json" \
    --slurpfile command "$result_dir/process-trigger-command.json" \
    --slurpfile previous_logs \
      "$result_dir/previous-container-log-evidence.json" \
    --slurpfile live_logs "$live_log_evidence" \
    --slurpfile status_watch "$status_watch_evidence" \
    --arg trigger_kind "$trigger_kind" \
    --argjson previous_logs_status "$previous_logs_status" \
    --argjson after "$death_after" \
    '
      ($after != null and
       $after.hard_process_death_confirmed == true) as
        $identity_death_confirmed |
      ($status_watch[0].hard_process_death_confirmed == true) as
        $watch_identity_death_confirmed |
      (
        $process_incarnation[0].captured_and_bound == true and
        $command[0].kubectl_exec_status == 0 and
        $command[0].incarnation_match_required == true and
        $command[0].incarnation_token_echoed == true and
        $command[0].expected_process_token ==
          $process_incarnation[0].process_token
      ) as $trigger_succeeded |
      ($after != null and
       $after.original_termination_reason_recorded == true and
       $after.original_termination_not_oom == true) as
        $non_oom_termination_confirmed |
      # `std::process::abort()` calls libc abort.  A normal process is recorded
      # as SIGABRT/134.  The runtime is container PID 1, for which Linux can
      # suppress a default-fatal signal; glibc then executes its abort trap
      # fallback, recorded by containerd as 133.  Accept only those calibrated
      # abort outcomes, and still require the unique in-process marker below.
      ($after != null and
       ($after.original_terminated_signal == 6 or
        $after.original_terminated_exit_code == 134 or
        $after.original_terminated_exit_code == 133)) as
        $runtime_abort_exit_calibrated |
      (
        $live_logs[0].runtime_abort_marker_count <= 1 and
        $previous_logs[0].runtime_abort_marker_count <= 1 and
        (
          (
            $live_logs[0].attached_before_trigger == true and
            $live_logs[0].runtime_abort_marker_count == 1
          ) or
          (
            $previous_logs_status == 0 and
            $previous_logs[0].exact_original_container == true and
            $previous_logs[0].runtime_abort_marker_count == 1
          )
        )
      ) as
        $runtime_abort_marker_unique |
      (
        $live_logs[0].postgres_immediate_shutdown_log_lines > 0 or
        (
          $previous_logs_status == 0 and
          $previous_logs[0].exact_original_container == true and
          $previous_logs[0].postgres_immediate_shutdown_log_lines > 0
        )
      ) as $postgres_shutdown_log_confirmed |
      (if $trigger_kind == "runtime_self_abort"
       then (
         $identity_death_confirmed and
         $watch_identity_death_confirmed and
         $trigger_succeeded and
         $non_oom_termination_confirmed and
         $runtime_abort_exit_calibrated and
         $runtime_abort_marker_unique
       )
       else (
         $identity_death_confirmed and
         $watch_identity_death_confirmed and
         $trigger_succeeded and
         $non_oom_termination_confirmed and
         $postgres_shutdown_log_confirmed
       )
       end) as $cause_calibrated |
      {
        shutdown_mode: "hard",
        hard_process_death_required: true,
        before: $before[0],
        process_incarnation: $process_incarnation[0],
        process_trigger: $command[0],
        after: $after,
        container_status_watch: $status_watch[0],
        original_container_live_logs: $live_logs[0],
        previous_container_logs: $previous_logs[0],
        cause_calibration: {
          trigger_succeeded: $trigger_succeeded,
          identity_death_confirmed: $identity_death_confirmed,
          watch_identity_death_confirmed:
            $watch_identity_death_confirmed,
          process_incarnation_bound:
            ($process_incarnation[0].captured_and_bound == true),
          trigger_incarnation_token_echoed:
            ($command[0].incarnation_token_echoed == true),
          original_termination_reason_recorded:
            ($after != null and
             $after.original_termination_reason_recorded == true),
          original_termination_not_oom:
            ($after != null and
             $after.original_termination_not_oom == true),
          runtime_abort_exit_calibrated:
            (if $trigger_kind == "runtime_self_abort"
             then $runtime_abort_exit_calibrated else null end),
          runtime_abort_marker_unique:
            (if $trigger_kind == "runtime_self_abort"
             then $runtime_abort_marker_unique else null end),
          postgres_immediate_shutdown_log_evidence:
            (if $trigger_kind == "postgres_immediate_shutdown"
             then $postgres_shutdown_log_confirmed
             else null
             end),
          postgres_postmaster_change_required:
            ($trigger_kind == "postgres_immediate_shutdown"),
          postgres_postmaster_changed: null,
          calibrated: $cause_calibrated
        },
        hard_process_death_confirmed: $cause_calibrated
      }
    ' >"$result_dir/process-death-evidence.json"
  jq -e '.hard_process_death_confirmed == true' \
    "$result_dir/process-death-evidence.json" >/dev/null || {
    printf 'did not confirm death of container %s (%s) in Pod UID %s\n' \
      "$container_name" "$original_container_id" "$pod_uid" >&2
    return 1
  }
}

qualification_assert_faults_zero() {
  local destination=$1
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local release=${BENCH_RELEASE:-bench}
  local runtime_selector=${BENCH_RUNTIME_SELECTOR:-app.kubernetes.io/component=runtime}
  local runtime_container=${QUALIFICATION_RUNTIME_CONTAINER:-runtime}
  local prefix=${destination%.json}
  local helm_values_file="$prefix-helm-values.raw"
  local helm_stderr_file="$prefix-helm-values.stderr"
  local pods_file="$prefix-ready-pods.raw"
  local pods_stderr_file="$prefix-ready-pods.stderr"
  local helm_status
  local pods_status

  if helm get values "$release" -n "$namespace" -o json \
    >"$helm_values_file" 2>"$helm_stderr_file"; then
    helm_status=0
  else
    helm_status=$?
  fi
  if kubectl -n "$namespace" get pods -l "$runtime_selector" -o json \
    >"$pods_file" 2>"$pods_stderr_file"; then
    pods_status=0
  else
    pods_status=$?
  fi

  local helm_values=null
  local pods=null
  if ((helm_status == 0)); then
    if helm_values=$(jq -c . "$helm_values_file" 2>/dev/null); then
      :
    else
      helm_status=65
      helm_values=null
    fi
  fi
  if ((pods_status == 0)); then
    if pods=$(jq -c . "$pods_file" 2>/dev/null); then
      :
    else
      pods_status=65
      pods=null
    fi
  fi

  jq -n \
    --arg observed_at "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    --arg namespace "$namespace" \
    --arg release "$release" \
    --arg runtime_selector "$runtime_selector" \
    --arg runtime_container "$runtime_container" \
    --argjson helm_get_values_status "$helm_status" \
    --argjson kubectl_get_pods_status "$pods_status" \
    --argjson helm_values "$helm_values" \
    --argjson pods "$pods" '
      def number_or_null:
        if type == "number" then .
        elif type == "string" then (tonumber? // null)
        else null
        end;
      def fault_values($source):
        {
          admission_delay_ms:
            ($source.runtime.qualificationFaults.admissionDelayMs |
             number_or_null),
          post_commit_delay_ms:
            ($source.runtime.qualificationFaults.postCommitDelayMs |
             number_or_null),
          summary_delay_ms:
            ($source.runtime.qualificationFaults.summaryDelayMs |
             number_or_null)
        };
      def all_faults_zero($faults):
        $faults.admission_delay_ms == 0 and
        $faults.post_commit_delay_ms == 0 and
        $faults.summary_delay_ms == 0;
      def pod_fault_values($pod; $container):
        [$pod.spec.containers[]? | select(.name == $container)] as
          $containers |
        if ($containers | length) != 1 then null
        else
          ($containers[0].env // []) as $environment |
          {
            admission_delay_ms: (
              [$environment[] |
               select(.name ==
                 "INSIGHT_TERMINAL_QUALIFICATION_ADMISSION_DELAY_MS")] |
              if length == 1 then (.[0].value | number_or_null)
              else null end
            ),
            post_commit_delay_ms: (
              [$environment[] |
               select(.name ==
                 "INSIGHT_TERMINAL_QUALIFICATION_POST_COMMIT_DELAY_MS")] |
              if length == 1 then (.[0].value | number_or_null)
              else null end
            ),
            summary_delay_ms: (
              [$environment[] |
               select(.name ==
                 "INSIGHT_TERMINAL_QUALIFICATION_SUMMARY_DELAY_MS")] |
              if length == 1 then (.[0].value | number_or_null)
              else null end
            )
          }
        end;
      ($helm_values | fault_values(.)) as $helm_faults |
      ([
        $pods.items[]? |
        select(
          .metadata.deletionTimestamp == null and
          .status.phase == "Running" and
          any(.status.conditions[]?;
              .type == "Ready" and .status == "True")
        )
      ]) as $ready_pods |
      (if ($ready_pods | length) == 1
       then ($ready_pods[0] | pod_fault_values(.; $runtime_container))
       else null
       end) as $pod_faults |
      {
        observed_at: $observed_at,
        namespace: $namespace,
        release: $release,
        runtime_selector: $runtime_selector,
        runtime_container: $runtime_container,
        helm_get_values_status: $helm_get_values_status,
        kubectl_get_pods_status: $kubectl_get_pods_status,
        helm_faults: $helm_faults,
        ready_runtime_pod_count: ($ready_pods | length),
        ready_runtime_pod: (
          if ($ready_pods | length) == 1
          then {
            name: $ready_pods[0].metadata.name,
            uid: $ready_pods[0].metadata.uid,
            container_id: (
              [$ready_pods[0].status.containerStatuses[]? |
               select(.name == $runtime_container) |
               .containerID] |
              if length == 1 then .[0] else null end
            ),
            faults: $pod_faults
          }
          else null
          end
        ),
        all_zero: (
          $helm_get_values_status == 0 and
          $kubectl_get_pods_status == 0 and
          ($ready_pods | length) == 1 and
          $pod_faults != null and
          all_faults_zero($helm_faults) and
          all_faults_zero($pod_faults)
        )
      }
    ' >"$destination"

  jq -e '.all_zero == true' "$destination" >/dev/null || {
    printf 'qualification fault delays are not proven zero; see %s\n' \
      "$destination" >&2
    return 1
  }
}

runtime_pod_name() {
  require_command kubectl
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local selector=${BENCH_RUNTIME_SELECTOR:-app.kubernetes.io/component=runtime}
  kubectl -n "$namespace" get pods \
    -l "$selector" \
    --field-selector=status.phase=Running \
    -o jsonpath='{.items[0].metadata.name}'
}

capture_runtime_metrics() {
  local destination=$1
  api_curl "$BASE_URL/metrics" >"$destination"
}

capture_runtime_pod_state() {
  local destination=$1
  if [[ -n "${BENCH_RUNTIME_PID:-}" ]]; then
    printf '{"local":true,"pid":%s}\n' "$BENCH_RUNTIME_PID" >"$destination"
    return
  fi
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local pod
  pod=$(runtime_pod_name)
  [[ -n "$pod" ]] || {
    printf 'no running runtime pod found\n' >&2
    return 1
  }
  kubectl -n "$namespace" get pod "$pod" -o json >"$destination"
}

capture_runtime_topology() {
  local destination=$1
  if [[ -n "${BENCH_RUNTIME_PID:-}" ]]; then
    printf '{"local":true,"pid":%s,"desired_replicas":1,"ready_replicas":1,"pods":[{"uid":"local:%s","ready":true,"phase":"Running"}]}\n' \
      "$BENCH_RUNTIME_PID" "$BENCH_RUNTIME_PID" >"$destination"
    return
  fi

  require_command jq
  require_command kubectl
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local release=${BENCH_RELEASE:-bench}
  local selector=${BENCH_RUNTIME_SELECTOR:-app.kubernetes.io/component=runtime}
  local deployment=${BENCH_RUNTIME_DEPLOYMENT:-"${release}-insight-agent-platform"}
  local deployment_document
  local pods_document
  deployment_document=$(kubectl -n "$namespace" get deployment "$deployment" -o json)
  pods_document=$(kubectl -n "$namespace" get pods -l "$selector" -o json)
  jq -n \
    --argjson deployment "$deployment_document" \
    --argjson pod_list "$pods_document" '
      {
        local: false,
        deployment_name: $deployment.metadata.name,
        deployment_uid: $deployment.metadata.uid,
        desired_replicas: ($deployment.spec.replicas // 0),
        ready_replicas: ($deployment.status.readyReplicas // 0),
        available_replicas: ($deployment.status.availableReplicas // 0),
        pods: [
          $pod_list.items[] |
          {
            name: .metadata.name,
            uid: .metadata.uid,
            phase: .status.phase,
            deleting: (.metadata.deletionTimestamp != null),
            ready: (
              any(.status.conditions[]?;
                  .type == "Ready" and .status == "True")
            ),
            restart_count: (
              [.status.containerStatuses[]?.restartCount // 0] | add // 0
            )
          }
        ]
      }
    ' >"$destination"
}

capture_runtime_process_snapshot() {
  local destination=$1
  if [[ -n "${BENCH_RUNTIME_PID:-}" ]]; then
    local pid=$BENCH_RUNTIME_PID
    [[ -r "/proc/$pid/status" ]] || {
      printf 'BENCH_RUNTIME_PID %s has no readable /proc status\n' "$pid" >&2
      return 1
    }
    awk '
      /^(VmRSS|VmHWM|VmSize|VmPeak):/ {
        key=$1; sub(/:$/, "", key); print "status." key "_kb=" $2
      }
    ' "/proc/$pid/status" >"$destination"
    if [[ -r "/proc/$pid/smaps_rollup" ]]; then
      awk '
        /^(Pss|Rss):/ {
          key=$1; sub(/:$/, "", key); print "smaps." key "_kb=" $2
        }
      ' "/proc/$pid/smaps_rollup" >>"$destination"
    fi
    return
  fi

  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local pod
  pod=$(runtime_pod_name)
  [[ -n "$pod" ]] || {
    printf 'no running runtime pod found\n' >&2
    return 1
  }
  kubectl -n "$namespace" exec "$pod" -- sh -ec '
    awk '"'"'
      /^(VmRSS|VmHWM|VmSize|VmPeak):/ {
        key=$1; sub(/:$/, "", key); print "status." key "_kb=" $2
      }
    '"'"' /proc/1/status
    if [ -r /proc/1/smaps_rollup ]; then
      awk '"'"'
        /^(Pss|Rss):/ {
          key=$1; sub(/:$/, "", key); print "smaps." key "_kb=" $2
        }
      '"'"' /proc/1/smaps_rollup
    fi
    if [ -r /sys/fs/cgroup/memory.current ]; then
      printf "cgroup.memory_current_bytes="
      cat /sys/fs/cgroup/memory.current
    fi
    if [ -r /sys/fs/cgroup/memory.peak ]; then
      printf "cgroup.memory_peak_bytes="
      cat /sys/fs/cgroup/memory.peak
    fi
    if [ -r /sys/fs/cgroup/memory.events ]; then
      awk '"'"'{ print "cgroup." $1 "=" $2 }'"'"' /sys/fs/cgroup/memory.events
    fi
  ' >"$destination"
}

capture_artifact_bytes() {
  local destination=$1
  local artifact_root=${BENCH_ARTIFACT_ROOT:-/data/artifacts}
  if [[ -n "${BENCH_ARTIFACT_HOST_ROOT:-}" ]]; then
    du -sk "$BENCH_ARTIFACT_HOST_ROOT" |
      awk '{ print $1 * 1024 }' >"$destination"
    return
  fi
  local namespace=${BENCH_NAMESPACE:-insight-bench}
  local pod
  pod=$(runtime_pod_name)
  [[ -n "$pod" ]] || {
    printf 'no running runtime pod found\n' >&2
    return 1
  }
  kubectl -n "$namespace" exec "$pod" -- du -sk "$artifact_root" |
    awk '{ print $1 * 1024 }' >"$destination"
}
