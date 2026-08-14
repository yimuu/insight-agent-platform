\set ON_ERROR_STOP on

BEGIN;

DO $platform_bootstrap$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_namespace
        WHERE nspname = 'insight_platform'
    ) THEN
        RAISE EXCEPTION 'insight_platform schema already exists; baseline provisioning requires a fresh target';
    END IF;
END
$platform_bootstrap$;

CREATE SCHEMA insight_platform;

CREATE TABLE insight_platform.schema_migrations (
    version bigint PRIMARY KEY CHECK (version > 0),
    name text NOT NULL UNIQUE,
    checksum text NOT NULL CHECK (checksum ~ '^sha256:[0-9a-f]{64}$'),
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

\ir migrations/0001_platform_baseline.sql

INSERT INTO insight_platform.schema_migrations (version, name, checksum)
VALUES (1, 'platform_baseline', :'baseline_checksum');

COMMIT;
