ALTER TABLE workflow_runs ADD COLUMN deadline_at TEXT;

CREATE INDEX idx_run_deadline_due
    ON workflow_runs(deadline_at, run_id)
    WHERE deadline_at IS NOT NULL
      AND lifecycle IN ('created', 'active', 'waiting', 'completing');
