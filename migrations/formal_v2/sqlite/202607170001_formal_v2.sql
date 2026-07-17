CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    agent_version TEXT NOT NULL,
    attachment TEXT NOT NULL CHECK (attachment IN ('attached', 'detached')),
    status TEXT NOT NULL CHECK (
        status IN ('created', 'running', 'completed', 'failed', 'cancelled', 'interrupted')
    ),
    started_at TEXT,
    ended_at TEXT,
    updated_at TEXT NOT NULL,
    input_summary TEXT NOT NULL CHECK (json_valid(input_summary)),
    output TEXT CHECK (output IS NULL OR json_valid(output)),
    error_kind TEXT CHECK (
        error_kind IS NULL OR error_kind IN ('workflow', 'operation', 'timeout', 'infrastructure')
    ),
    error_code TEXT,
    error_message TEXT,
    CHECK (
        (status IN ('created', 'running')
            AND ended_at IS NULL
            AND output IS NULL
            AND error_kind IS NULL
            AND error_code IS NULL
            AND error_message IS NULL)
        OR
        (status = 'completed'
            AND ended_at IS NOT NULL
            AND output IS NOT NULL
            AND error_kind IS NULL
            AND error_code IS NULL
            AND error_message IS NULL)
        OR
        (status = 'failed'
            AND ended_at IS NOT NULL
            AND output IS NULL
            AND error_kind IS NOT NULL
            AND error_code IS NOT NULL
            AND error_message IS NOT NULL)
        OR
        (status IN ('cancelled', 'interrupted')
            AND ended_at IS NOT NULL
            AND output IS NULL
            AND error_kind IS NULL
            AND error_code IS NOT NULL
            AND error_message IS NOT NULL)
    )
);

CREATE INDEX idx_formal_runs_agent_updated
    ON runs(agent_id, updated_at DESC, run_id DESC);
CREATE INDEX idx_formal_runs_status_updated
    ON runs(status, updated_at DESC, run_id DESC);

CREATE TABLE run_events (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    data TEXT NOT NULL CHECK (json_valid(data)),
    UNIQUE (run_id, seq),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
);

CREATE INDEX idx_formal_run_events_replay ON run_events(run_id, seq);
