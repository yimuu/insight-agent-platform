#!/usr/bin/env bash
set -euo pipefail
: "${PLATFORM_DATABASE_ADMIN_URL:?PLATFORM_DATABASE_ADMIN_URL is required}"
: "${PLATFORM_OUTBOX_WORKER_ROLE:?PLATFORM_OUTBOX_WORKER_ROLE is required}"
if [[ ! "$PLATFORM_OUTBOX_WORKER_ROLE" =~ ^[a-z][a-z0-9_]{0,62}$ ]]; then
  printf 'Outbox role has an invalid shape\n' >&2
  exit 2
fi
command -v psql >/dev/null 2>&1 || { printf 'psql is required\n' >&2; exit 2; }
psql "$PLATFORM_DATABASE_ADMIN_URL" --no-psqlrc --set ON_ERROR_STOP=1 \
  --set "outbox_worker_role=$PLATFORM_OUTBOX_WORKER_ROLE" \
  --file crates/adapters/platform-postgres/outbox-role-grants.sql
