#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
case "${1:-}" in
  model)
    target=phase3_model_turn
    scenario=model_worker_process_recovery
    ;;
  capability)
    target=phase3_invocation
    scenario=capability_workers_process_recovery
    : "${PLATFORM_MCP_HOST_BIN:?exact MCP Host executable is required for the complete remote recovery scenario}"
    ;;
  native-context)
    target=phase3_context
    scenario=production_native_context_worker_recovers_commit_window_process_loss
    ;;
  remote-context)
    target=phase3_context
    scenario=production_remote_context_worker_recovers_mtls_response_commit_window
    ;;
  model-tool-chain)
    target=phase3_model_turn
    scenario=production_workers_complete_model_tool_result_return_chain
    ;;
  *)
    echo "usage: qualify-platform-worker-recovery.sh <model|capability|native-context|remote-context|model-tool-chain>" >&2
    exit 2
    ;;
esac
[[ $# = 1 ]] || { echo "exactly one recovery scenario is required" >&2; exit 2; }
cd "$root"
# Each scenario validates its exact binary, PostgreSQL, TLS and provider fixture inputs.
# Missing inputs are an error, never a passed or silently skipped qualification.
cargo test --locked -p insight-platform-postgres --test "$target" "$scenario" -- --ignored --exact --nocapture
