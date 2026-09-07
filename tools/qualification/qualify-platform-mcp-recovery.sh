#!/usr/bin/env bash
set -euo pipefail
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
: "${PLATFORM_TEST_DATABASE_URL:?dedicated current-schema PostgreSQL fixture is required}"
require_binary() {
  local name=$1
  local binary=${!name:-}
  [[ "$binary" = /* && -f "$binary" && -x "$binary" ]] || { echo "$name must name the exact executable by absolute path" >&2; exit 2; }
}
case "${1:-}" in
  cleanup)
    target=phase4_mcp_oauth
    scenario=phase4_mcp_oauth_cleanup_process_recovers_egress_and_worker_kill
    require_binary PLATFORM_MCP_CLEANUP_WORKER_BIN
    ;;
  oauth-exchange)
    target=phase4_mcp_oauth
    scenario=phase4_mcp_oauth_callback_and_egress_recover_after_token_store_before_commit
    ;;
  subscription)
    target=phase4_mcp_subscription
    scenario=mcp_subscription_current_process_recovery
    for name in PLATFORM_MCP_DISCOVERY_WORKER_BIN PLATFORM_MCP_RESOURCE_HOST_BIN PLATFORM_MCP_SUBSCRIPTION_WORKER_BIN PLATFORM_SUBSCRIPTION_CONTEXT_WORKER_BIN PLATFORM_ARTIFACT_DATA_WORKER_BIN; do require_binary "$name"; done
    : "${PLATFORM_TEST_AWS_ENDPOINT:?dedicated AWS-compatible fixture endpoint is required}"
    : "${PLATFORM_TEST_S3_BUCKET:?versioned fixture bucket is required}"
    : "${PLATFORM_TEST_KMS_KEY_ID:?fixture KMS key identity is required}"
    ;;
  *) echo "usage: qualify-platform-mcp-recovery.sh <cleanup|oauth-exchange|subscription>" >&2; exit 2 ;;
esac
[[ $# = 1 ]] || { echo "exactly one recovery scenario is required" >&2; exit 2; }
cd "$root"
cargo test --locked -p insight-platform-postgres --test "$target" "$scenario" -- --ignored --exact --nocapture
