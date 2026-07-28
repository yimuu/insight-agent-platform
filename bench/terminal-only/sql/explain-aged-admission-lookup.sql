\set ON_ERROR_STOP on

EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT JSON)
SELECT run_id
FROM terminal_run_admissions
WHERE tenant_id = :'tenant_id'
  AND request_id = :'request_id';
