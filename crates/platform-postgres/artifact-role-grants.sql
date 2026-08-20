\set ON_ERROR_STOP on

-- Provisioning-only grants for the three Artifact process roles. Data Worker intentionally uses
-- a separate read-only pool for Sandbox materialization and a mutation pool for scan commits.
BEGIN;

SELECT pg_catalog.set_config('insight_platform.artifact_gateway_role', :'artifact_gateway_role', true);
SELECT pg_catalog.set_config('insight_platform.artifact_data_reader_role', :'artifact_data_reader_role', true);
SELECT pg_catalog.set_config('insight_platform.artifact_data_worker_role', :'artifact_data_worker_role', true);
SELECT pg_catalog.set_config('insight_platform.artifact_maintenance_role', :'artifact_maintenance_role', true);

DO $artifact_role_grants$
DECLARE
    gateway_role text := pg_catalog.current_setting('insight_platform.artifact_gateway_role', true);
    reader_role text := pg_catalog.current_setting('insight_platform.artifact_data_reader_role', true);
    worker_role text := pg_catalog.current_setting('insight_platform.artifact_data_worker_role', true);
    maintenance_role text := pg_catalog.current_setting('insight_platform.artifact_maintenance_role', true);
    target_role text;
BEGIN
    IF gateway_role = reader_role OR gateway_role = worker_role OR gateway_role = maintenance_role
       OR reader_role = worker_role OR reader_role = maintenance_role OR worker_role = maintenance_role THEN
        RAISE EXCEPTION 'Artifact PostgreSQL roles must be mutually distinct';
    END IF;
    FOREACH target_role IN ARRAY ARRAY[gateway_role, reader_role, worker_role, maintenance_role]
    LOOP
        IF target_role IS NULL OR target_role !~ '^[a-z][a-z0-9_]{0,62}$'
           OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = target_role) THEN
            RAISE EXCEPTION 'Artifact role must name an existing closed PostgreSQL role';
        END IF;
        EXECUTE pg_catalog.format('REVOKE ALL ON SCHEMA insight_platform FROM %I', target_role);
        EXECUTE pg_catalog.format('REVOKE ALL ON ALL TABLES IN SCHEMA insight_platform FROM %I', target_role);
        EXECUTE pg_catalog.format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA insight_platform FROM %I', target_role);
        EXECUTE pg_catalog.format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA insight_platform FROM %I', target_role);
        EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA insight_platform TO %I', target_role);
        EXECUTE pg_catalog.format('GRANT SELECT ON insight_platform.schema_migrations TO %I', target_role);
    END LOOP;

    EXECUTE pg_catalog.format(
        'GRANT SELECT ON insight_platform.invocations, insight_platform.jobs, insight_platform.run_values, insight_platform.artifact_links, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        reader_role
    );

    EXECUTE pg_catalog.format(
        'GRANT SELECT ON insight_platform.tenants, insight_platform.principals, insight_platform.tenant_principals, insight_platform.resources, insight_platform.resource_versions, insight_platform.quota_accounts, insight_platform.quota_ledger, insight_platform.jobs, insight_platform.tasks, insight_platform.events, insight_platform.receipts, insight_platform.outbox_events, insight_platform.artifact_links, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        gateway_role
    );
    EXECUTE pg_catalog.format(
        'GRANT INSERT ON insight_platform.quota_ledger, insight_platform.jobs, insight_platform.events, insight_platform.receipts, insight_platform.outbox_events, insight_platform.artifact_links, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        gateway_role
    );
    EXECUTE pg_catalog.format(
        'GRANT UPDATE ON insight_platform.quota_accounts, insight_platform.jobs, insight_platform.receipts, insight_platform.artifact_links, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        gateway_role
    );

    EXECUTE pg_catalog.format(
        'GRANT SELECT ON insight_platform.jobs, insight_platform.resources, insight_platform.resource_versions, insight_platform.events, insight_platform.receipts, insight_platform.outbox_events, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        worker_role
    );
    EXECUTE pg_catalog.format(
        'GRANT INSERT ON insight_platform.jobs, insight_platform.events, insight_platform.receipts, insight_platform.outbox_events TO %I',
        worker_role
    );
    EXECUTE pg_catalog.format(
        'GRANT UPDATE ON insight_platform.jobs, insight_platform.receipts, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        worker_role
    );

    EXECUTE pg_catalog.format(
        'GRANT SELECT ON insight_platform.jobs, insight_platform.events, insight_platform.receipts, insight_platform.outbox_events, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        maintenance_role
    );
    EXECUTE pg_catalog.format(
        'GRANT INSERT ON insight_platform.events, insight_platform.receipts, insight_platform.outbox_events TO %I',
        maintenance_role
    );
    EXECUTE pg_catalog.format(
        'GRANT UPDATE ON insight_platform.jobs, insight_platform.receipts, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        maintenance_role
    );
END
$artifact_role_grants$;

COMMIT;
