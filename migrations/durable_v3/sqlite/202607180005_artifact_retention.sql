-- Terminal Run artifact retention is a separate lifecycle from orphan upload
-- cleanup. A release records when historical/recovery references may stop
-- protecting content-addressed objects; metadata rows remain auditable.
CREATE TABLE IF NOT EXISTS artifact_retention_releases (
    run_id TEXT PRIMARY KEY,
    transition_key TEXT NOT NULL UNIQUE,
    intent_hash TEXT NOT NULL CHECK (
        length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'
    ),
    event_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL CHECK (event_seq >= 1),
    retain_until TEXT NOT NULL,
    artifact_count INTEGER NOT NULL CHECK (artifact_count >= 0),
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id, event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_v3_artifact_retention_due
    ON artifact_retention_releases(retain_until, run_id);
