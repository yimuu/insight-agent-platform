\set ON_ERROR_STOP on

EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON)
SELECT
    admission.run_id,
    admission.tenant_id,
    admission.request_id,
    result.terminal_state,
    owner.lease_expires_at,
    CASE
        WHEN result.run_id IS NOT NULL THEN result.terminal_state
        WHEN owner.lease_expires_at > clock_timestamp() THEN 'active'
        ELSE 'interrupted'
    END AS derived_state
FROM terminal_run_admissions admission
LEFT JOIN terminal_run_results result
  ON result.run_id = admission.run_id
LEFT JOIN terminal_runtime_instances owner
  ON owner.instance_id = admission.owner_instance_id
 AND owner.owner_epoch = admission.owner_epoch
 AND owner.started_at >= pg_postmaster_start_time()
WHERE admission.run_id = :'run_id'
  AND admission.tenant_id = :'tenant_id';
