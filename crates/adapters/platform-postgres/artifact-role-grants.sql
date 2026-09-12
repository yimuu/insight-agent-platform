\set ON_ERROR_STOP on

-- Provisioning-only grants for the four Artifact database roles. Data Worker intentionally uses
-- a separate read-only materialization pool and a mutation pool for scan/stage commits.
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
    END LOOP;

    EXECUTE pg_catalog.format(
        'GRANT SELECT ON insight_platform.invocations, insight_platform.jobs, insight_platform.run_values, insight_platform.artifact_links, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        reader_role
    );
    EXECUTE pg_catalog.format('GRANT SELECT ON insight_platform.conversation_turns TO %I', reader_role);
    EXECUTE pg_catalog.format('GRANT SELECT (principal_id, state, version) ON insight_platform.principals TO %I', reader_role);
    EXECUTE pg_catalog.format('GRANT SELECT (tenant_id, principal_id, principal_kind, state, generation, version, permissions_schema_version, permissions, permissions_digest) ON insight_platform.tenant_principals TO %I', reader_role);
    EXECUTE pg_catalog.format('GRANT SELECT (tenant_id,run_id,artifact_id) ON insight_platform.run_values TO %I', gateway_role);
    EXECUTE pg_catalog.format('GRANT SELECT (tenant_id,run_id) ON insight_platform.conversation_turns TO %I', gateway_role);
    -- Scheduler TypedPlan/RunValue/Skill reads and the Skill's current Selection Policy closure.
    -- Preserve column-only reads: no Run current payload, table-wide reads, DML or row locks.
    EXECUTE pg_catalog.format(
        'GRANT SELECT (tenant_id, run_id, version, state, input_value_id, output_value_id, bindings_schema_version, bindings, bindings_digest) ON insight_platform.runs TO %I',
        reader_role
    );
    EXECUTE pg_catalog.format(
        'GRANT SELECT (tenant_id, resource_version_id, resource_id, resource_version_kind, content_digest, artifact_id, payload_schema_version, payload, payload_digest) ON insight_platform.resource_versions TO %I',
        reader_role
    );
    EXECUTE pg_catalog.format(
        'GRANT SELECT (tenant_id, deployment_id, resource_id, resource_version_id, payload_schema_version, bindings, bindings_digest) ON insight_platform.deployments TO %I',
        reader_role
    );
    EXECUTE pg_catalog.format(
        'GRANT SELECT (tenant_id, resource_id, resource_kind, lifecycle_state, gate_state) ON insight_platform.resources TO %I',
        reader_role
    );

    EXECUTE pg_catalog.format(
        'GRANT SELECT ON insight_platform.tenants, insight_platform.principals, insight_platform.tenant_principals, insight_platform.resources, insight_platform.resource_versions, insight_platform.deployments, insight_platform.quota_accounts, insight_platform.quota_ledger, insight_platform.jobs, insight_platform.tasks, insight_platform.events, insight_platform.receipts, insight_platform.outbox_events, insight_platform.artifact_links, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        gateway_role
    );
    EXECUTE pg_catalog.format(
        'GRANT INSERT ON insight_platform.quota_ledger, insight_platform.jobs, insight_platform.tasks, insight_platform.events, insight_platform.receipts, insight_platform.outbox_events, insight_platform.artifact_links, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        gateway_role
    );
    EXECUTE pg_catalog.format(
        'GRANT UPDATE ON insight_platform.quota_accounts, insight_platform.jobs, insight_platform.receipts, insight_platform.artifact_links, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        gateway_role
    );
    EXECUTE pg_catalog.format(
        'GRANT EXECUTE ON FUNCTION insight_platform.artifact_lock_scan_policy(text, text) TO %I',
        gateway_role
    );

    EXECUTE pg_catalog.format(
        'GRANT SELECT ON insight_platform.jobs, insight_platform.invocations, insight_platform.resources, insight_platform.resource_versions, insight_platform.events, insight_platform.receipts, insight_platform.outbox_events, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        worker_role
    );
    EXECUTE pg_catalog.format(
        'GRANT INSERT ON insight_platform.jobs, insight_platform.artifacts, insight_platform.artifact_blobs, insight_platform.events, insight_platform.receipts, insight_platform.outbox_events TO %I',
        worker_role
    );
    EXECUTE pg_catalog.format(
        'GRANT UPDATE ON insight_platform.jobs, insight_platform.receipts, insight_platform.artifacts, insight_platform.artifact_blobs TO %I',
        worker_role
    );
    EXECUTE pg_catalog.format(
        'GRANT EXECUTE ON FUNCTION insight_platform.artifact_lock_scan_policy(text, text) TO %I',
        worker_role
    );
    -- Existing MCP verification wake advances only its operation version/timestamp by exact CAS.
    EXECUTE pg_catalog.format(
        'GRANT UPDATE (version, updated_at) ON insight_platform.invocations TO %I',
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
    -- These workers advance the shared bounded scheduling cursor, but cannot provision
    -- tenants/partitions or mutate scheduling policy and identity business records.
    FOREACH target_role IN ARRAY ARRAY[worker_role, maintenance_role]
    LOOP
        EXECUTE pg_catalog.format('GRANT SELECT ON insight_platform.tenants, insight_platform.deployments, insight_platform.resources, insight_platform.resource_versions TO %I', target_role);
        EXECUTE pg_catalog.format('GRANT SELECT, UPDATE ON insight_platform.scheduler_state, insight_platform.scheduler_tenant_state TO %I', target_role);
    END LOOP;
END
$artifact_role_grants$;

COMMIT;
