-- Live response metadata remains durable only at item/model-call boundaries.
-- Transient body deltas are intentionally absent from this schema.

ALTER TABLE workflow_runs
    ADD COLUMN response_id TEXT
    GENERATED ALWAYS AS ('resp_' || run_id) STORED;

CREATE UNIQUE INDEX uq_v3_workflow_runs_response_id
    ON workflow_runs(response_id);

CREATE TABLE response_public_items (
    run_id TEXT NOT NULL,
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
    seal_index INTEGER CHECK (seal_index IS NULL OR seal_index >= 0),
    safe_item TEXT CHECK (safe_item IS NULL OR json_valid(safe_item)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, activation_id, attempt_no, model_call_no, item_ordinal),
    UNIQUE (run_id, item_id),
    UNIQUE (run_id, output_index),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT
);

CREATE TABLE model_call_usage (
    run_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    model_call_no INTEGER NOT NULL CHECK (model_call_no >= 1),
    task_id TEXT NOT NULL CHECK (task_id <> ''),
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch >= 1),
    fencing_token TEXT NOT NULL CHECK (fencing_token <> ''),
    call_status TEXT NOT NULL CHECK (
        call_status IN ('started', 'completed', 'failed', 'abandoned')
    ),
    finish_reason TEXT CHECK (
        finish_reason IS NULL
        OR finish_reason IN ('stop', 'tool_calls', 'length', 'content_filter', 'invalid')
    ),
    usage TEXT CHECK (usage IS NULL OR json_valid(usage)),
    usage_complete INTEGER NOT NULL DEFAULT 0 CHECK (usage_complete IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, activation_id, attempt_no, model_call_no),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT
);

CREATE TABLE response_snapshots (
    run_id TEXT PRIMARY KEY,
    response_id TEXT NOT NULL,
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
    response_payload TEXT NOT NULL CHECK (json_valid(response_payload)),
    workflow_payload TEXT NOT NULL CHECK (json_valid(workflow_payload)),
    public_item_manifest TEXT NOT NULL CHECK (json_valid(public_item_manifest)),
    usage TEXT CHECK (usage IS NULL OR json_valid(usage)),
    usage_status TEXT NOT NULL CHECK (
        usage_status IN ('complete', 'partial', 'unavailable')
    ),
    snapshot_hash TEXT NOT NULL CHECK (
        length(snapshot_hash) = 71 AND snapshot_hash LIKE 'sha256:%'
    ),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (response_id),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (response_id) REFERENCES workflow_runs(response_id) ON DELETE RESTRICT
);
