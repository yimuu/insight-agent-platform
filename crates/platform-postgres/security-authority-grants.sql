\set ON_ERROR_STOP on

-- Provisioning-only least-privilege grants for the independently deployed Security Authority.
-- The caller must pass a pre-created NOLOGIN group role using psql variable
-- `security_authority_role`. Credentials/login membership remain an installation concern.
-- This file creates no schema object and is not a migration.

BEGIN;

SELECT pg_catalog.set_config(
    'insight_platform.security_authority_role',
    :'security_authority_role',
    true
);

DO $security_authority_grants$
DECLARE
    target_role text := pg_catalog.current_setting(
        'insight_platform.security_authority_role',
        true
    );
BEGIN
    IF target_role IS NULL
       OR target_role !~ '^[a-z][a-z0-9_]{0,62}$'
       OR NOT EXISTS (
           SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = target_role
       ) THEN
        RAISE EXCEPTION 'security_authority_role must name an existing closed PostgreSQL role';
    END IF;

    EXECUTE pg_catalog.format('REVOKE ALL ON SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL TABLES IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA insight_platform FROM %I', target_role);

    EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA insight_platform TO %I', target_role);

    -- Startup schema verification and exact current authority reads.
    EXECUTE pg_catalog.format(
        'GRANT SELECT ON insight_platform.schema_migrations, insight_platform.secret_bindings, insight_platform.principals, insight_platform.tenant_principals, insight_platform.receipts TO %I',
        target_role
    );

    -- Prepared winner registration only. No UPDATE/DELETE and no write privilege on any other
    -- business table are granted. PostgreSQL FK checks remain owned by the table owner.
    EXECUTE pg_catalog.format(
        'GRANT INSERT ON insight_platform.secret_bindings, insight_platform.receipts, insight_platform.events, insight_platform.outbox_events TO %I',
        target_role
    );
    EXECUTE pg_catalog.format(
        'GRANT UPDATE (state, disposition, response_reference_id, completed_at) ON insight_platform.receipts TO %I',
        target_role
    );

    -- Constraint helpers are the only schema functions needed by inserts.
    EXECUTE pg_catalog.format(
        'GRANT EXECUTE ON FUNCTION insight_platform.is_platform_id(text), insight_platform.is_sha256(text), insight_platform.is_bounded_object(jsonb, integer) TO %I',
        target_role
    );
END
$security_authority_grants$;

COMMIT;
