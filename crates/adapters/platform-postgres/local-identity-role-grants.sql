\set ON_ERROR_STOP on
BEGIN;
SELECT pg_catalog.set_config('insight_platform.local_identity_role', :'local_identity_role', true);
DO $identity_grants$
DECLARE target_role text := pg_catalog.current_setting('insight_platform.local_identity_role', true);
BEGIN
    IF target_role IS NULL OR target_role !~ '^[a-z][a-z0-9_]{0,62}$'
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=target_role) THEN
        RAISE EXCEPTION 'local_identity_role must name an existing PostgreSQL role';
    END IF;
    EXECUTE pg_catalog.format('REVOKE ALL ON SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL TABLES IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA insight_platform TO %I', target_role);
    EXECUTE pg_catalog.format('GRANT SELECT, INSERT, UPDATE ON insight_platform.local_console_owner TO %I', target_role);
    EXECUTE pg_catalog.format('GRANT SELECT, INSERT, DELETE ON insight_platform.local_console_sessions TO %I', target_role);
END
$identity_grants$;
COMMIT;
