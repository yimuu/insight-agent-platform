\set ON_ERROR_STOP on

-- Provisioning-only least-privilege grants for the independently deployed Artifact Broker.
-- The caller passes a pre-created NOLOGIN group role using `artifact_broker_role`.
-- This file creates no schema object and is not a migration.

BEGIN;

SELECT pg_catalog.set_config(
    'insight_platform.artifact_broker_role',
    :'artifact_broker_role',
    true
);

DO $artifact_broker_grants$
DECLARE
    target_role text := pg_catalog.current_setting(
        'insight_platform.artifact_broker_role',
        true
    );
BEGIN
    IF target_role IS NULL
       OR target_role !~ '^[a-z][a-z0-9_]{0,62}$'
       OR NOT EXISTS (
           SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = target_role
       ) THEN
        RAISE EXCEPTION 'artifact_broker_role must name an existing closed PostgreSQL role';
    END IF;

    EXECUTE pg_catalog.format('REVOKE ALL ON SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL TABLES IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA insight_platform FROM %I', target_role);

    EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA insight_platform TO %I', target_role);
    EXECUTE pg_catalog.format(
        'GRANT SELECT ON insight_platform.schema_migrations, insight_platform.invocations, insight_platform.jobs, insight_platform.run_values, insight_platform.artifact_links, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        target_role
    );
END
$artifact_broker_grants$;

COMMIT;
