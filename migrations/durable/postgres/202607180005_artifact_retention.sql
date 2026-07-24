-- Terminal Run artifact retention is distinct from orphan upload cleanup.
CREATE TABLE IF NOT EXISTS artifact_retention_releases (
    run_id TEXT PRIMARY KEY REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    transition_key TEXT NOT NULL UNIQUE,
    intent_hash TEXT NOT NULL CHECK (
        length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'
    ),
    event_id TEXT NOT NULL,
    event_seq BIGINT NOT NULL CHECK (event_seq >= 1),
    retain_until TIMESTAMPTZ NOT NULL,
    artifact_count BIGINT NOT NULL CHECK (artifact_count >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (run_id, event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_artifact_retention_due
    ON artifact_retention_releases(retain_until, run_id);
