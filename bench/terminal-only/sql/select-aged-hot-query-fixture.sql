\set ON_ERROR_STOP on

SELECT jsonb_build_object(
    'run_id', admission.run_id,
    'tenant_id', admission.tenant_id,
    'request_id', admission.request_id,
    'owner_instance_id', admission.owner_instance_id,
    'owner_epoch', admission.owner_epoch,
    'accepted_at', admission.accepted_at,
    'result_present', (result.run_id IS NOT NULL),
    'owner_present', (owner.instance_id IS NOT NULL),
    'owner_registry_rows', (
        SELECT count(*) FROM terminal_runtime_instances
    )
)::text
FROM terminal_run_admissions admission
JOIN terminal_run_results result ON result.run_id = admission.run_id
LEFT JOIN terminal_runtime_instances owner
  ON owner.instance_id = admission.owner_instance_id
 AND owner.owner_epoch = admission.owner_epoch
 AND owner.started_at >= pg_postmaster_start_time()
WHERE (
    :'tenant_id' = ''
    OR admission.tenant_id = :'tenant_id'
)
ORDER BY admission.accepted_at DESC, admission.run_id DESC
LIMIT 1;
