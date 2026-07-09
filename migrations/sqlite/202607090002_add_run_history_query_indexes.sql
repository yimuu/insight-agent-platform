CREATE INDEX IF NOT EXISTS idx_runs_started_at_run_id ON runs(started_at DESC, run_id DESC);
CREATE INDEX IF NOT EXISTS idx_runs_status_started_at ON runs(status, started_at DESC, run_id DESC);
CREATE INDEX IF NOT EXISTS idx_runs_agent_status_started_at ON runs(agent_id, status, started_at DESC, run_id DESC);
