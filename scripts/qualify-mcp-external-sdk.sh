#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
typescript_dir="${repo_root}/tests/interop/typescript"
go_dir="${repo_root}/tests/interop/go"
qualification_dir="${repo_root}/target/mcp-qualification"
go_cache="${qualification_dir}/go-build-cache"
go_fixture="${qualification_dir}/go-sdk-fixture"

node_bin="${INSIGHT_MCP_NODE:-$(command -v node || true)}"
pnpm_bin="${INSIGHT_MCP_PNPM:-$(command -v pnpm || true)}"

if [[ -z "${node_bin}" || ! -x "${node_bin}" ]]; then
  echo "Node.js is required for MCP external SDK qualification" >&2
  exit 1
fi
if [[ -z "${pnpm_bin}" || ! -x "${pnpm_bin}" ]]; then
  echo "pnpm is required for MCP external SDK qualification" >&2
  exit 1
fi
if ! command -v go >/dev/null 2>&1; then
  echo "Go is required for MCP external SDK qualification" >&2
  exit 1
fi

mkdir -p "${qualification_dir}" "${go_cache}"

(
  cd "${typescript_dir}"
  "${pnpm_bin}" install --frozen-lockfile --ignore-scripts
)

(
  cd "${go_dir}"
  GOCACHE="${go_cache}" go build -trimpath -o "${go_fixture}" .
)

started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
(
  cd "${repo_root}"
  INSIGHT_MCP_EXTERNAL_SDK_QUALIFY=1 \
  INSIGHT_MCP_NODE="${node_bin}" \
  INSIGHT_MCP_GO_FIXTURE_BIN="${go_fixture}" \
    cargo test -p insight-agent-platform --test mcp_external_sdk_interop -- --nocapture
)
completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

cat >"${qualification_dir}/report.json" <<EOF
{
  "profile": "mcp-2026-07-28-external-sdk-interop",
  "status": "passed",
  "started_at": "${started_at}",
  "completed_at": "${completed_at}",
  "typescript_sdk": "@modelcontextprotocol/sdk@1.30.0",
  "go_sdk": "github.com/modelcontextprotocol/go-sdk@91e4e1a0b8ca01cfa680f142815b1152a0513326",
  "matrix": {
    "platform-client-to-typescript-server": {
      "transports": ["stdio", "streamable_http"],
      "tasks": true
    },
    "platform-client-to-go-server": {
      "transports": ["stdio", "streamable_http"],
      "tasks": true
    },
    "typescript-client-to-platform-server": {
      "transports": ["streamable_http"],
      "tasks": true
    },
    "go-client-to-platform-server": {
      "transports": ["streamable_http"],
      "tasks": true
    }
  },
  "platform_server_profile_transport": "streamable_http"
}
EOF

echo "MCP external SDK qualification passed: ${qualification_dir}/report.json"
