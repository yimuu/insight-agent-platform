\set ON_ERROR_STOP on
BEGIN;
SELECT pg_catalog.set_config('insight_platform.history_maintenance_role', :'history_maintenance_role', true);
DO $history_grants$
DECLARE target_role text := pg_catalog.current_setting('insight_platform.history_maintenance_role', true);
BEGIN
    IF target_role IS NULL OR target_role !~ '^[a-z][a-z0-9_]{0,62}$' OR NOT EXISTS(SELECT 1 FROM pg_catalog.pg_roles WHERE rolname=target_role) THEN
        RAISE EXCEPTION 'history_maintenance_role must name an existing closed PostgreSQL role';
    END IF;
    EXECUTE pg_catalog.format('REVOKE ALL ON SCHEMA insight_platform FROM %I',target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL TABLES IN SCHEMA insight_platform FROM %I',target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA insight_platform FROM %I',target_role);
    EXECUTE pg_catalog.format('REVOKE ALL ON ALL FUNCTIONS IN SCHEMA insight_platform FROM %I',target_role);
    EXECUTE pg_catalog.format('GRANT USAGE ON SCHEMA insight_platform TO %I',target_role);
    EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION insight_platform.history_scan_runs(timestamptz,text,text,text,text,integer), insight_platform.history_lock_run(text,text), insight_platform.history_lock_event_prefix(text,text,bigint,bigint,integer), insight_platform.history_delete_prefix(text,text,bigint,bigint), insight_platform.history_scan_records(text,timestamptz,text,text,text,text,integer), insight_platform.history_lock_receipt(text,text), insight_platform.history_lock_owner(text,text), insight_platform.history_event_obligations(text,text,bigint), insight_platform.history_lock_event(text,text), insight_platform.history_delete_receipt(text,text,text), insight_platform.history_delete_published_outbox(text,text,timestamptz), insight_platform.history_delete_event(text,text,text), insight_platform.history_lock_task_chain(text,text), insight_platform.history_owner_delivery(text,text[]), insight_platform.history_retire_oauth_chain(text,text,bigint,text,text,text,text) TO %I',target_role);
END $history_grants$;
COMMIT;
