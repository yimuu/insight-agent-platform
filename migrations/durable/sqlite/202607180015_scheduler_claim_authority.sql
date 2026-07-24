-- Persist the scheduler's claim purpose instead of trusting a worker-returned
-- in-memory claim. Existing live claims are conservatively treated as lease
-- loss finalizers; published result claims may only acknowledge.
ALTER TABLE task_outbox ADD COLUMN claim_mode TEXT CHECK (
    claim_mode IS NULL
    OR claim_mode IN ('execute', 'finalize_lease_loss', 'acknowledge')
);

UPDATE task_outbox
SET claim_mode = CASE
    WHEN task_state = 'claimed' THEN 'finalize_lease_loss'
    WHEN task_state = 'published' AND claim_token IS NOT NULL THEN 'acknowledge'
    ELSE NULL
END;
