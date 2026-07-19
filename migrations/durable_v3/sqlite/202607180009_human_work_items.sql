-- First-class durable human-task queue for the single-runtime SQLite backend.
CREATE TABLE IF NOT EXISTS human_work_items (
    work_item_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    wait_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    signal_name TEXT NOT NULL,
    request_value TEXT NOT NULL CHECK (json_valid(request_value)),
    response_type TEXT NOT NULL CHECK (json_valid(response_type)),
    assignees TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(assignees)),
    candidate_groups TEXT NOT NULL DEFAULT '[]' CHECK (json_valid(candidate_groups)),
    claim_lease_ms INTEGER NOT NULL CHECK (claim_lease_ms BETWEEN 1 AND 2592000000),
    work_state TEXT NOT NULL CHECK (
        work_state IN ('open','claimed','completed','cancelled','expired')
    ),
    claim_fence INTEGER NOT NULL DEFAULT 0 CHECK (claim_fence >= 0),
    claimed_by TEXT,
    claim_request_id TEXT,
    claim_expires_at TEXT,
    completion_request_id TEXT,
    completion_payload TEXT CHECK (completion_payload IS NULL OR json_valid(completion_payload)),
    completion_payload_hash TEXT,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT,
    closed_at TEXT,
    UNIQUE (run_id, wait_id),
    UNIQUE (run_id, activation_id),
    FOREIGN KEY (run_id, wait_id)
        REFERENCES scheduler_wait_registrations(run_id, wait_id) ON DELETE RESTRICT,
    CHECK (
        (work_state = 'open' AND claimed_by IS NULL AND claim_expires_at IS NULL)
        OR (work_state = 'claimed' AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL)
        OR work_state IN ('completed','cancelled','expired')
    ),
    CHECK (
        (completion_request_id IS NULL AND completion_payload IS NULL
            AND completion_payload_hash IS NULL)
        OR (completion_request_id IS NOT NULL AND completion_payload IS NOT NULL
            AND completion_payload_hash IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_v3_human_work_items_queue
    ON human_work_items(work_state, claim_expires_at, created_at, work_item_id);
CREATE INDEX IF NOT EXISTS idx_v3_human_work_items_run
    ON human_work_items(run_id, work_state, work_item_id);

DROP TRIGGER IF EXISTS close_human_work_items_on_run_termination;
CREATE TRIGGER close_human_work_items_on_run_termination
AFTER UPDATE OF lifecycle, termination_intent_reason ON workflow_runs
WHEN NEW.lifecycle = 'terminating' AND OLD.lifecycle IS NOT NEW.lifecycle
BEGIN
    UPDATE human_work_items
    SET work_state = 'completed', completed_at = CURRENT_TIMESTAMP,
        claim_expires_at = NULL, projection_version = projection_version + 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE run_id = NEW.run_id AND work_state = 'claimed'
      AND completion_request_id IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM signals_inbox s
          WHERE s.run_id=human_work_items.run_id
            AND s.signal_id=human_work_items.signal_id
            AND s.signal_state='consumed'
      );
    UPDATE human_work_items
    SET work_state = CASE WHEN NEW.termination_intent_reason = 'timed_out'
                          THEN 'expired' ELSE 'cancelled' END,
        completion_request_id = NULL, completion_payload = NULL,
        completion_payload_hash = NULL, claim_expires_at = NULL,
        projection_version = projection_version + 1,
        updated_at = CURRENT_TIMESTAMP, closed_at = CURRENT_TIMESTAMP
    WHERE run_id = NEW.run_id AND work_state IN ('open','claimed');
END;

DROP TRIGGER IF EXISTS close_human_work_items_on_run_terminal;
CREATE TRIGGER close_human_work_items_on_run_terminal
AFTER UPDATE OF lifecycle ON workflow_runs
WHEN NEW.lifecycle IN ('succeeded','failed','cancelled','interrupted','timed_out')
     AND OLD.lifecycle IS NOT NEW.lifecycle
BEGIN
    UPDATE human_work_items
    SET work_state = 'completed', completed_at = CURRENT_TIMESTAMP,
        claim_expires_at = NULL, projection_version = projection_version + 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE run_id = NEW.run_id AND work_state = 'claimed'
      AND completion_request_id IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM signals_inbox s
          WHERE s.run_id=human_work_items.run_id
            AND s.signal_id=human_work_items.signal_id
            AND s.signal_state='consumed'
      );
    UPDATE human_work_items
    SET work_state = CASE WHEN NEW.lifecycle = 'timed_out'
                          THEN 'expired' ELSE 'cancelled' END,
        completion_request_id = NULL, completion_payload = NULL,
        completion_payload_hash = NULL, claim_expires_at = NULL,
        projection_version = projection_version + 1,
        updated_at = CURRENT_TIMESTAMP, closed_at = CURRENT_TIMESTAMP
    WHERE run_id = NEW.run_id AND work_state IN ('open','claimed');
END;
