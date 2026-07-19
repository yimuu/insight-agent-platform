-- Public delivery rows have mutable claim/publish fencing metadata, but their
-- canonical identity and redacted envelope are immutable from insertion.

DROP TRIGGER IF EXISTS public_event_outbox_insert_contract;
CREATE TRIGGER public_event_outbox_insert_contract
BEFORE INSERT ON public_event_outbox
FOR EACH ROW
WHEN NOT (
    (
        (NEW.is_terminal = 0 AND NEW.event_kind IN (
            'run.created','run.started','operation.started',
            'operation.completed','operation.failed'
        ))
        OR (NEW.is_terminal = 1 AND NEW.event_kind IN (
            'run.completed','run.failed','run.cancelled','run.interrupted'
        ))
    )
    AND (
        (NEW.publish_state = 'pending'
            AND NEW.claimed_by IS NULL AND NEW.claim_token IS NULL
            AND NEW.claim_expires_at IS NULL AND NEW.published_at IS NULL
            AND NEW.published_by IS NULL AND NEW.published_claim_token IS NULL
            AND NEW.notified_at IS NULL AND NEW.retain_until IS NULL)
        OR (NEW.publish_state = 'claimed'
            AND NEW.claimed_by IS NOT NULL AND NEW.claim_token IS NOT NULL
            AND NEW.claim_expires_at IS NOT NULL AND NEW.published_at IS NULL
            AND NEW.published_by IS NULL AND NEW.published_claim_token IS NULL
            AND NEW.notified_at IS NULL AND NEW.retain_until IS NULL)
        OR (NEW.publish_state = 'published'
            AND NEW.claimed_by IS NULL AND NEW.claim_token IS NULL
            AND NEW.claim_expires_at IS NULL AND NEW.published_at IS NOT NULL
            AND NEW.published_by IS NOT NULL AND NEW.published_claim_token IS NOT NULL
            AND NEW.notified_at IS NOT NULL
            AND ((NEW.is_terminal = 1 AND NEW.retain_until IS NULL)
                 OR (NEW.is_terminal = 0 AND NEW.retain_until IS NOT NULL)))
    )
)
BEGIN
    SELECT RAISE(ABORT, 'invalid public event delivery authority');
END;

DROP TRIGGER IF EXISTS public_event_outbox_update_contract;
CREATE TRIGGER public_event_outbox_update_contract
BEFORE UPDATE ON public_event_outbox
FOR EACH ROW
WHEN NOT (
    NEW.run_id IS OLD.run_id
    AND NEW.public_event_id IS OLD.public_event_id
    AND NEW.causation_event_id IS OLD.causation_event_id
    AND NEW.public_ordinal IS OLD.public_ordinal
    AND NEW.public_schema_version IS OLD.public_schema_version
    AND NEW.event_kind IS OLD.event_kind
    AND NEW.is_terminal IS OLD.is_terminal
    AND NEW.safe_envelope IS OLD.safe_envelope
    AND NEW.available_at IS OLD.available_at
    AND NEW.created_at IS OLD.created_at
    AND NEW.publish_attempts >= OLD.publish_attempts
    AND (
        (OLD.publish_state = 'pending' AND NEW.publish_state = 'claimed')
        OR (OLD.publish_state = 'claimed' AND NEW.publish_state = 'claimed')
        OR (OLD.publish_state = 'claimed' AND NEW.publish_state = 'published')
    )
)
BEGIN
    SELECT RAISE(ABORT, 'public event delivery authority is immutable');
END;
