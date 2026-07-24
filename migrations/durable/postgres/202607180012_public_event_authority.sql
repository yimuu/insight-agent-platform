-- Public delivery rows have mutable claim/publish fencing metadata, but their
-- canonical identity and redacted envelope are immutable from insertion.

ALTER TABLE public_event_outbox
    ADD CONSTRAINT ck_public_event_kind_terminal
    CHECK (
        (NOT is_terminal AND event_kind IN (
            'run.created','run.started','operation.started',
            'operation.completed','operation.failed'
        ))
        OR (is_terminal AND event_kind IN (
            'run.completed','run.failed','run.cancelled','run.interrupted'
        ))
    ),
    ADD CONSTRAINT ck_public_event_delivery_lifecycle
    CHECK (
        (publish_state = 'pending'
            AND claimed_by IS NULL AND claim_token IS NULL
            AND claim_expires_at IS NULL AND published_at IS NULL
            AND published_by IS NULL AND published_claim_token IS NULL
            AND notified_at IS NULL AND retain_until IS NULL)
        OR (publish_state = 'claimed'
            AND claimed_by IS NOT NULL AND claim_token IS NOT NULL
            AND claim_expires_at IS NOT NULL AND published_at IS NULL
            AND published_by IS NULL AND published_claim_token IS NULL
            AND notified_at IS NULL AND retain_until IS NULL)
        OR (publish_state = 'published'
            AND claimed_by IS NULL AND claim_token IS NULL
            AND claim_expires_at IS NULL AND published_at IS NOT NULL
            AND published_by IS NOT NULL AND published_claim_token IS NOT NULL
            AND notified_at IS NOT NULL
            AND ((is_terminal AND retain_until IS NULL)
                 OR (NOT is_terminal AND retain_until IS NOT NULL)))
    );

CREATE FUNCTION enforce_public_event_outbox_update_contract()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.run_id IS DISTINCT FROM OLD.run_id
       OR NEW.public_event_id IS DISTINCT FROM OLD.public_event_id
       OR NEW.causation_event_id IS DISTINCT FROM OLD.causation_event_id
       OR NEW.public_ordinal IS DISTINCT FROM OLD.public_ordinal
       OR NEW.public_schema_version IS DISTINCT FROM OLD.public_schema_version
       OR NEW.event_kind IS DISTINCT FROM OLD.event_kind
       OR NEW.is_terminal IS DISTINCT FROM OLD.is_terminal
       OR NEW.safe_envelope IS DISTINCT FROM OLD.safe_envelope
       OR NEW.available_at IS DISTINCT FROM OLD.available_at
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
       OR NEW.publish_attempts < OLD.publish_attempts
       OR NOT (
           (OLD.publish_state = 'pending' AND NEW.publish_state = 'claimed')
           OR (OLD.publish_state = 'claimed' AND NEW.publish_state = 'claimed')
           OR (OLD.publish_state = 'claimed' AND NEW.publish_state = 'published')
       ) THEN
        RAISE EXCEPTION 'public event delivery authority is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER public_event_outbox_update_contract
BEFORE UPDATE ON public_event_outbox
FOR EACH ROW EXECUTE FUNCTION enforce_public_event_outbox_update_contract();
