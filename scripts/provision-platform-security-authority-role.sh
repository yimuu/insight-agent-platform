#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${PLATFORM_DATABASE_ADMIN_URL:-}" ]]; then
  printf 'PLATFORM_DATABASE_ADMIN_URL is required\n' >&2
  exit 2
fi

if [[ -z "${PLATFORM_SECURITY_AUTHORITY_ROLE:-}" ]]; then
  printf 'PLATFORM_SECURITY_AUTHORITY_ROLE is required\n' >&2
  exit 2
fi

if [[ ! "${PLATFORM_SECURITY_AUTHORITY_ROLE}" =~ ^[a-z][a-z0-9_]{0,62}$ ]]; then
  printf 'PLATFORM_SECURITY_AUTHORITY_ROLE has an invalid shape\n' >&2
  exit 2
fi

if ! command -v psql >/dev/null 2>&1; then
  printf 'psql is required for Platform role provisioning\n' >&2
  exit 2
fi

psql "${PLATFORM_DATABASE_ADMIN_URL}" \
  --no-psqlrc \
  --set ON_ERROR_STOP=1 \
  --set "security_authority_role=${PLATFORM_SECURITY_AUTHORITY_ROLE}" \
  --file crates/platform-postgres/security-authority-grants.sql

printf 'Platform Security Authority role grants applied\n'
