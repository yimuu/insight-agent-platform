-- Durable boundaries for response identity, item manifests, model usage,
-- and the final response snapshot. Transient body deltas are never stored here.

ALTER TABLE workflow_runs
    ADD COLUMN IF NOT EXISTS response_id TEXT
    GENERATED ALWAYS AS ('resp_' || run_id) STORED;

CREATE UNIQUE INDEX IF NOT EXISTS uq_workflow_runs_response_id
    ON workflow_runs(response_id);

CREATE TABLE IF NOT EXISTS response_public_items (
    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    activation_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    model_call_no INTEGER NOT NULL CHECK (model_call_no >= 1),
    item_ordinal INTEGER NOT NULL CHECK (item_ordinal >= 0),
    item_id TEXT NOT NULL CHECK (item_id <> ''),
    output_index INTEGER NOT NULL CHECK (output_index >= 0),
    node_id TEXT NOT NULL CHECK (node_id <> ''),
    item_kind TEXT NOT NULL CHECK (item_kind IN ('message', 'function_call')),
    item_status TEXT NOT NULL CHECK (
        item_status IN ('reserved', 'completed', 'incomplete', 'incomplete_unsealed')
    ),
    seal_index BIGINT CHECK (seal_index IS NULL OR seal_index >= 0),
    safe_item JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, activation_id, attempt_no, model_call_no, item_ordinal),
    UNIQUE (run_id, item_id),
    UNIQUE (run_id, output_index)
);

CREATE TABLE IF NOT EXISTS model_call_usage (
    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    activation_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    model_call_no INTEGER NOT NULL CHECK (model_call_no >= 1),
    task_id TEXT NOT NULL CHECK (task_id <> ''),
    lease_epoch BIGINT NOT NULL CHECK (lease_epoch >= 1),
    fencing_token TEXT NOT NULL CHECK (fencing_token <> ''),
    call_status TEXT NOT NULL CHECK (
        call_status IN ('started', 'completed', 'failed', 'abandoned')
    ),
    finish_reason TEXT CHECK (
        finish_reason IS NULL
        OR finish_reason IN ('stop', 'tool_calls', 'length', 'content_filter', 'invalid')
    ),
    usage JSONB,
    usage_complete BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, activation_id, attempt_no, model_call_no)
);

CREATE TABLE IF NOT EXISTS response_snapshots (
    run_id TEXT PRIMARY KEY REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    response_id TEXT NOT NULL REFERENCES workflow_runs(response_id) ON DELETE RESTRICT,
    terminal_kind TEXT NOT NULL CHECK (
        terminal_kind IN (
            'response.completed',
            'response.failed',
            'workflow.response.timed_out',
            'workflow.response.cancelled',
            'workflow.response.interrupted'
        )
    ),
    response_status TEXT NOT NULL CHECK (
        response_status IN ('completed', 'failed', 'cancelled', 'incomplete')
    ),
    response_payload JSONB NOT NULL,
    workflow_payload JSONB NOT NULL,
    public_item_manifest JSONB NOT NULL,
    usage JSONB,
    usage_status TEXT NOT NULL CHECK (
        usage_status IN ('complete', 'partial', 'unavailable')
    ),
    snapshot_hash TEXT NOT NULL CHECK (
        length(snapshot_hash) = 71 AND snapshot_hash LIKE 'sha256:%'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (response_id)
);
