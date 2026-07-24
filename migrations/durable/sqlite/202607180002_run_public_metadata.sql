-- Persist the Formal API transport identity required across ordinary restart.
-- The runtime applies this only when `workflow_runs.request_id` is absent.
ALTER TABLE workflow_runs ADD COLUMN request_id TEXT;
ALTER TABLE workflow_runs ADD COLUMN attachment TEXT;
ALTER TABLE workflow_runs ADD COLUMN started_at TEXT;

UPDATE workflow_runs
SET request_id = run_id,
    attachment = 'detached',
    started_at = CASE WHEN lifecycle = 'created' THEN NULL ELSE created_at END
WHERE request_id IS NULL OR attachment IS NULL;
