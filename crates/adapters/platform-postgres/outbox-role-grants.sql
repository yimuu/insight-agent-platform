\set ON_ERROR_STOP on
-- Provisioning only. The runtime cannot alter roles, schema, domain rows or Event bodies.
BEGIN;
SELECT pg_catalog.set_config('insight_platform.outbox_worker_role', :'outbox_worker_role', true);
DO $outbox_grants$
DECLARE
    target_role text := pg_catalog.current_setting('insight_platform.outbox_worker_role', true);
BEGIN
    IF target_role IS NULL OR target_role !~ '^[a-z][a-z0-9_]{0,62}$'
       OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = target_role) THEN
        RAISE EXCEPTION 'outbox_worker_role must name an existing closed PostgreSQL role';
    END IF;
    EXECUTE pg_catalog.format('REVOKE ALL ON SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL TABLES IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA insight_platform FROM %I', target_role);
    EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA insight_platform TO %I', target_role);
    EXECUTE pg_catalog.format('GRANT SELECT ON insight_platform.outbox_events TO %I', target_role);
    EXECUTE pg_catalog.format(
        'GRANT SELECT (tenant_id, event_id, aggregate_id, aggregate_version, run_id, public_sequence, trace_id, occurred_at) ON insight_platform.events TO %I', target_role);
    EXECUTE pg_catalog.format(
        'GRANT UPDATE (state, publish_attempts, next_publish_at, claim_owner, claim_epoch, claim_expires_at, last_failure_code, published_at, updated_at) ON insight_platform.outbox_events TO %I', target_role);
END
$outbox_grants$;
COMMIT;
