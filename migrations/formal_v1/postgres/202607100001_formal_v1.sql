CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_version TEXT NOT NULL,
    attachment TEXT NOT NULL CHECK (attachment IN ('attached', 'detached')),
    status TEXT NOT NULL CHECK (
        status IN ('created', 'running', 'completed', 'failed', 'cancelled', 'interrupted')
    ),
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL,
    input_summary JSONB NOT NULL,
    output JSONB,
    error_code TEXT,
    error_message TEXT
);

CREATE INDEX idx_formal_runs_agent_updated
    ON runs(agent_id, updated_at DESC, run_id DESC);
CREATE INDEX idx_formal_runs_status_updated
    ON runs(status, updated_at DESC, run_id DESC);

CREATE TABLE run_events (
    run_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    node_id TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    data JSONB NOT NULL,
    UNIQUE (run_id, seq),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

CREATE INDEX idx_formal_run_events_replay ON run_events(run_id, seq);

CREATE TABLE node_outputs (
    run_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    output JSONB NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, node_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);
