#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

if [[ -z "${PLATFORM_DATABASE_URL:-}" ]]; then
  printf 'PLATFORM_DATABASE_URL is required\n' >&2
  exit 2
fi

if ! command -v psql >/dev/null 2>&1; then
  printf 'psql is required for Platform PostgreSQL provisioning\n' >&2
  exit 2
fi

contract_path="crates/platform-postgres/schema-contract.json"
baseline_path="crates/platform-postgres/migrations/0001_platform_baseline.sql"
bootstrap_path="crates/platform-postgres/bootstrap.sql"

baseline_checksum="$(python3 -c 'import hashlib, json, pathlib, sys
contract = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
baseline = pathlib.Path(sys.argv[2]).read_bytes()
expected = contract["migrations"][0]["checksum"]
actual = "sha256:" + hashlib.sha256(baseline).hexdigest()
if actual != expected:
    raise SystemExit(f"baseline checksum differs: expected {expected}, found {actual}")
print(actual)' "$contract_path" "$baseline_path")"

psql "$PLATFORM_DATABASE_URL" \
  --no-psqlrc \
  --set ON_ERROR_STOP=1 \
  --set "baseline_checksum=$baseline_checksum" \
  --file "$bootstrap_path"

printf 'Platform PostgreSQL baseline provisioned and recorded\n'
