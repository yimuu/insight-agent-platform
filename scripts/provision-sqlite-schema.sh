#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
workspace_root=$(cd "$script_dir/.." && pwd)
schema_file="$workspace_root/database/durable/sqlite/schema.sql"
database_file=${1:-"$workspace_root/data/quickstart.sqlite3"}

if [[ ! -f "$schema_file" ]]; then
  printf 'SQLite Schema file is missing: %s\n' "$schema_file" >&2
  exit 1
fi
if ! command -v sqlite3 >/dev/null 2>&1; then
  printf 'sqlite3 is required to provision the durable SQLite Schema.\n' >&2
  exit 1
fi
if [[ -e "$database_file" ]]; then
  printf 'Refusing to provision a non-empty target path: %s\n' "$database_file" >&2
  printf 'Move or remove the pre-1.0 database explicitly, then retry.\n' >&2
  exit 1
fi

database_parent=$(dirname "$database_file")
mkdir -p "$database_parent"

cleanup_failed_install() {
  rm -f -- "$database_file"
}
trap cleanup_failed_install ERR

sqlite3 -bail "$database_file" < "$schema_file"

trap - ERR
printf 'Provisioned durable SQLite Schema at %s\n' "$database_file"
