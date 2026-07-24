-- First-class durable human-task queue. Completion reuses the scheduler's
-- signal/timer first-winner authority, while assignment and claim fencing are
-- independently queryable and CAS protected.
CREATE TABLE IF NOT EXISTS human_work_items (
    work_item_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    wait_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    signal_name TEXT NOT NULL,
    request_value JSONB NOT NULL,
    response_type JSONB NOT NULL,
    assignees JSONB NOT NULL DEFAULT '[]'::jsonb,
    candidate_groups JSONB NOT NULL DEFAULT '[]'::jsonb,
    claim_lease_ms BIGINT NOT NULL CHECK (claim_lease_ms BETWEEN 1 AND 2592000000),
    work_state TEXT NOT NULL CHECK (
        work_state IN ('open','claimed','completed','cancelled','expired')
    ),
    claim_fence BIGINT NOT NULL DEFAULT 0 CHECK (claim_fence >= 0),
    claimed_by TEXT,
    claim_request_id TEXT,
    claim_expires_at TIMESTAMPTZ,
    completion_request_id TEXT,
    completion_payload JSONB,
    completion_payload_hash TEXT,
    projection_version BIGINT NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    UNIQUE (run_id, wait_id),
    UNIQUE (run_id, activation_id),
    FOREIGN KEY (run_id, wait_id)
        REFERENCES scheduler_wait_registrations(run_id, wait_id) ON DELETE RESTRICT,
    CHECK (jsonb_typeof(assignees) = 'array'),
    CHECK (jsonb_typeof(candidate_groups) = 'array'),
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

CREATE INDEX IF NOT EXISTS idx_human_work_items_queue
    ON human_work_items(work_state, claim_expires_at, created_at, work_item_id);
CREATE INDEX IF NOT EXISTS idx_human_work_items_run
    ON human_work_items(run_id, work_state, work_item_id);

CREATE OR REPLACE FUNCTION close_human_work_items_for_run()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.lifecycle = 'terminating' AND OLD.lifecycle IS DISTINCT FROM NEW.lifecycle THEN
        UPDATE human_work_items h
        SET work_state = 'completed', completed_at = clock_timestamp(),
            claim_expires_at = NULL, projection_version = projection_version + 1,
            updated_at = clock_timestamp()
        WHERE h.run_id = NEW.run_id AND h.work_state = 'claimed'
          AND h.completion_request_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM signals_inbox s
              WHERE s.run_id=h.run_id AND s.signal_id=h.signal_id
                AND s.signal_state='consumed'
          );
        UPDATE human_work_items
        SET work_state = CASE WHEN NEW.termination_intent_reason = 'timed_out'
                              THEN 'expired' ELSE 'cancelled' END,
            completion_request_id = NULL, completion_payload = NULL,
            completion_payload_hash = NULL,
            claim_expires_at = NULL, projection_version = projection_version + 1,
            updated_at = clock_timestamp(), closed_at = clock_timestamp()
        WHERE run_id = NEW.run_id AND work_state IN ('open','claimed');
    ELSIF NEW.lifecycle IN ('succeeded','failed','cancelled','interrupted','timed_out')
          AND OLD.lifecycle IS DISTINCT FROM NEW.lifecycle THEN
        UPDATE human_work_items h
        SET work_state = 'completed', completed_at = clock_timestamp(),
            claim_expires_at = NULL, projection_version = projection_version + 1,
            updated_at = clock_timestamp()
        WHERE h.run_id = NEW.run_id AND h.work_state = 'claimed'
          AND h.completion_request_id IS NOT NULL
          AND EXISTS (
              SELECT 1 FROM signals_inbox s
              WHERE s.run_id=h.run_id AND s.signal_id=h.signal_id
                AND s.signal_state='consumed'
          );
        UPDATE human_work_items
        SET work_state = CASE WHEN NEW.lifecycle = 'timed_out'
                              THEN 'expired' ELSE 'cancelled' END,
            completion_request_id = NULL, completion_payload = NULL,
            completion_payload_hash = NULL,
            claim_expires_at = NULL, projection_version = projection_version + 1,
            updated_at = clock_timestamp(), closed_at = clock_timestamp()
        WHERE run_id = NEW.run_id AND work_state IN ('open','claimed');
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS close_human_work_items_on_run_state ON workflow_runs;
CREATE TRIGGER close_human_work_items_on_run_state
AFTER UPDATE OF lifecycle, termination_intent_reason ON workflow_runs
FOR EACH ROW EXECUTE FUNCTION close_human_work_items_for_run();
