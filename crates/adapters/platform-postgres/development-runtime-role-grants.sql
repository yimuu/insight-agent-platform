\set ON_ERROR_STOP on
-- Development composition only: shared business DML, never schema or role ownership.
BEGIN;
SELECT pg_catalog.set_config('insight_platform.development_runtime_role', :'development_runtime_role', true);
DO $runtime_grants$
DECLARE target_role text := pg_catalog.current_setting('insight_platform.development_runtime_role', true);
BEGIN
    IF target_role IS NULL OR target_role !~ '^[a-z][a-z0-9_]{0,62}$'
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=target_role) THEN
        RAISE EXCEPTION 'development_runtime_role must name an existing closed PostgreSQL role';
    END IF;
    EXECUTE pg_catalog.format('REVOKE ALL ON SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL TABLES IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA insight_platform TO %I', target_role);
    EXECUTE pg_catalog.format('GRANT SELECT, INSERT, UPDATE, DELETE ON
        insight_platform.artifact_blobs, insight_platform.artifact_links, insight_platform.artifacts,
        insight_platform.deployments, insight_platform.events, insight_platform.invocations,
        insight_platform.jobs, insight_platform.outbox_events, insight_platform.principals,
        insight_platform.quota_accounts, insight_platform.quota_ledger, insight_platform.receipts,
        insight_platform.resource_versions, insight_platform.resources, insight_platform.run_nodes,
        insight_platform.run_values, insight_platform.runs, insight_platform.scheduler_state,
        insight_platform.scheduler_tenant_state, insight_platform.secret_bindings, insight_platform.tasks,
        insight_platform.tenant_principals, insight_platform.tenants TO %I', target_role);
END
$runtime_grants$;
COMMIT;
