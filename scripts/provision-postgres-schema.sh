#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workspace_root=$(cd "$script_dir/.." && pwd)
schema_file="$workspace_root/database/durable/postgres/schema.sql"
database_url=${1:-${SCHEMA_PROVISIONER_POSTGRES_URL:-}}

if [[ ! -f "$schema_file" ]]; then
  printf 'PostgreSQL Schema file is missing: %s\n' "$schema_file" >&2
  exit 1
fi
if ! command -v psql >/dev/null 2>&1; then
  printf 'psql is required to provision the durable PostgreSQL Schema.\n' >&2
  exit 1
fi
if [[ -z "$database_url" ]]; then
  printf 'Usage: SCHEMA_PROVISIONER_POSTGRES_URL=postgres://... %s\n' "$0" >&2
  printf 'The provisioner URL must identify an empty target and a DDL-capable role.\n' >&2
  exit 1
fi

psql "$database_url" \
  --no-psqlrc \
  --set ON_ERROR_STOP=1 \
  --file "$schema_file"

printf 'Provisioned durable PostgreSQL Schema.\n'
