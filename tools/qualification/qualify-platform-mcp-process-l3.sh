#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
: "${PLATFORM_MCP_HOST_BIN:?absolute path to the exact MCP Host executable is required}"
[[ "$PLATFORM_MCP_HOST_BIN" = /* && -f "$PLATFORM_MCP_HOST_BIN" && -x "$PLATFORM_MCP_HOST_BIN" ]] || {
  echo "MCP process qualification requires an executable absolute binary path" >&2
  exit 2
}
cd "$root"
cargo test --locked -p insight-platform-mcp-service --test process_l3 \
  production_host_kill_and_restart_preserves_safe_replay_boundary -- --ignored --exact --nocapture
