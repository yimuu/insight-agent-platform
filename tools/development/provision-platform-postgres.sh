#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
if [[ -z "${PLATFORM_DATABASE_URL:-}" ]]; then
  printf 'PLATFORM_DATABASE_URL is required\n' >&2
  exit 2
fi
# All provisioning uses the owning runner: one transaction and full physical
# inventory verification before commit. It refuses an existing schema.
if [[ -n "${PLATFORM_SCHEMA_BIN:-}" ]]; then
  exec "$PLATFORM_SCHEMA_BIN" provision
fi
exec cargo run --quiet -p insight-platform-storage-tooling --bin platform-schema -- provision
