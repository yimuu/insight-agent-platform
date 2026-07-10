CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL DEFAULT '',
    agent_id TEXT NOT NULL,
    caller_service TEXT,
    tenant_id TEXT,
    user_id TEXT,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    input_summary TEXT NOT NULL,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_agent_started_at ON runs(agent_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_request_id ON runs(request_id);
CREATE INDEX IF NOT EXISTS idx_runs_caller_started_at ON runs(caller_service, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_tenant_started_at ON runs(tenant_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_runs_user_started_at ON runs(user_id, started_at DESC);

CREATE TABLE IF NOT EXISTS run_events (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    type TEXT NOT NULL,
    seq BIGINT NOT NULL,
    timestamp TEXT NOT NULL,
    code INTEGER NOT NULL,
    message TEXT NOT NULL,
    data TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_events_run_id ON run_events(run_id, seq, id);

CREATE TABLE IF NOT EXISTS step_outputs (
    run_id TEXT NOT NULL,
    step_id TEXT NOT NULL,
    output TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, step_id)
);
