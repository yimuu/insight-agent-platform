#!/usr/bin/env bash
set -euo pipefail

required=(
  PLATFORM_DATABASE_ADMIN_URL
  PLATFORM_ARTIFACT_GATEWAY_ROLE
  PLATFORM_ARTIFACT_DATA_READER_ROLE
  PLATFORM_ARTIFACT_DATA_WORKER_ROLE
  PLATFORM_ARTIFACT_MAINTENANCE_ROLE
)
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    printf '%s is required\n' "$name" >&2
    exit 2
  fi
done
for role in "$PLATFORM_ARTIFACT_GATEWAY_ROLE" "$PLATFORM_ARTIFACT_DATA_READER_ROLE" "$PLATFORM_ARTIFACT_DATA_WORKER_ROLE" "$PLATFORM_ARTIFACT_MAINTENANCE_ROLE"; do
  if [[ ! "$role" =~ ^[a-z][a-z0-9_]{0,62}$ ]]; then
    printf 'Artifact PostgreSQL role has an invalid shape\n' >&2
    exit 2
  fi
done
if [[ "$PLATFORM_ARTIFACT_GATEWAY_ROLE" == "$PLATFORM_ARTIFACT_DATA_READER_ROLE" ||
      "$PLATFORM_ARTIFACT_GATEWAY_ROLE" == "$PLATFORM_ARTIFACT_DATA_WORKER_ROLE" ||
      "$PLATFORM_ARTIFACT_GATEWAY_ROLE" == "$PLATFORM_ARTIFACT_MAINTENANCE_ROLE" ||
      "$PLATFORM_ARTIFACT_DATA_READER_ROLE" == "$PLATFORM_ARTIFACT_DATA_WORKER_ROLE" ||
      "$PLATFORM_ARTIFACT_DATA_READER_ROLE" == "$PLATFORM_ARTIFACT_MAINTENANCE_ROLE" ||
      "$PLATFORM_ARTIFACT_DATA_WORKER_ROLE" == "$PLATFORM_ARTIFACT_MAINTENANCE_ROLE" ]]; then
  printf 'Artifact PostgreSQL roles must be mutually distinct\n' >&2
  exit 2
fi
command -v psql >/dev/null 2>&1 || { printf 'psql is required\n' >&2; exit 2; }

psql "$PLATFORM_DATABASE_ADMIN_URL" --no-psqlrc --set ON_ERROR_STOP=1 \
  --set "artifact_gateway_role=$PLATFORM_ARTIFACT_GATEWAY_ROLE" \
  --set "artifact_data_reader_role=$PLATFORM_ARTIFACT_DATA_READER_ROLE" \
  --set "artifact_data_worker_role=$PLATFORM_ARTIFACT_DATA_WORKER_ROLE" \
  --set "artifact_maintenance_role=$PLATFORM_ARTIFACT_MAINTENANCE_ROLE" \
  --file crates/platform-postgres/artifact-role-grants.sql

printf 'Platform Artifact role grants applied\n'
