-- Persist the Formal API transport identity required across ordinary restart.
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS request_id TEXT;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS attachment TEXT;
ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;

UPDATE workflow_runs
SET request_id = COALESCE(request_id, run_id),
    attachment = COALESCE(attachment, 'detached'),
    started_at = COALESCE(
        started_at,
        CASE WHEN lifecycle = 'created' THEN NULL ELSE created_at END
    )
WHERE request_id IS NULL OR attachment IS NULL;

ALTER TABLE workflow_runs ALTER COLUMN request_id SET NOT NULL;
ALTER TABLE workflow_runs ALTER COLUMN attachment SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'workflow_runs_attachment_check'
    ) THEN
        ALTER TABLE workflow_runs
            ADD CONSTRAINT workflow_runs_attachment_check
            CHECK (attachment IN ('attached', 'detached'));
    END IF;
END;
$$;
