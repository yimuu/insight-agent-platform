#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${PLATFORM_DATABASE_ADMIN_URL:-}" ]]; then
  printf 'PLATFORM_DATABASE_ADMIN_URL is required\n' >&2
  exit 2
fi

if [[ -z "${PLATFORM_ARTIFACT_BROKER_ROLE:-}" ]]; then
  printf 'PLATFORM_ARTIFACT_BROKER_ROLE is required\n' >&2
  exit 2
fi

if [[ ! "${PLATFORM_ARTIFACT_BROKER_ROLE}" =~ ^[a-z][a-z0-9_]{0,62}$ ]]; then
  printf 'PLATFORM_ARTIFACT_BROKER_ROLE has an invalid shape\n' >&2
  exit 2
fi

if ! command -v psql >/dev/null 2>&1; then
  printf 'psql is required for Platform role provisioning\n' >&2
  exit 2
fi

psql "${PLATFORM_DATABASE_ADMIN_URL}" \
  --no-psqlrc \
  --set ON_ERROR_STOP=1 \
  --set "artifact_broker_role=${PLATFORM_ARTIFACT_BROKER_ROLE}" \
  --file crates/platform-postgres/artifact-broker-grants.sql

printf 'Platform Artifact Broker role grants applied\n'
