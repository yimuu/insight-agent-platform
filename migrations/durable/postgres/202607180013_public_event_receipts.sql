-- Public outbox bodies may expire, but transition replay identity may not.
-- This body-free receipt is the permanent public-event idempotency authority.

CREATE TABLE public_event_receipts (
    run_id TEXT NOT NULL,
    public_event_id TEXT NOT NULL,
    causation_event_id TEXT NOT NULL,
    public_ordinal INTEGER NOT NULL CHECK (public_ordinal > 0),
    public_schema_version INTEGER NOT NULL CHECK (public_schema_version >= 1),
    event_kind TEXT NOT NULL,
    is_terminal BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (run_id, public_event_id),
    UNIQUE (public_event_id),
    UNIQUE (run_id, causation_event_id, event_kind),
    UNIQUE (run_id, causation_event_id, public_ordinal),
    FOREIGN KEY (run_id, causation_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    CHECK (
        (NOT is_terminal AND event_kind IN (
            'run.created','run.started','operation.started',
            'operation.completed','operation.failed'
        ))
        OR (is_terminal AND event_kind IN (
            'run.completed','run.failed','run.cancelled','run.interrupted'
        ))
    )
);

INSERT INTO public_event_receipts (
    run_id,public_event_id,causation_event_id,public_ordinal,
    public_schema_version,event_kind,is_terminal,created_at
)
SELECT run_id,public_event_id,causation_event_id,public_ordinal,
       public_schema_version,event_kind,is_terminal,created_at
FROM public_event_outbox;

CREATE FUNCTION insert_public_event_receipt()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public_event_receipts (
        run_id,public_event_id,causation_event_id,public_ordinal,
        public_schema_version,event_kind,is_terminal,created_at
    ) VALUES (
        NEW.run_id,NEW.public_event_id,NEW.causation_event_id,NEW.public_ordinal,
        NEW.public_schema_version,NEW.event_kind,NEW.is_terminal,NEW.created_at
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER public_event_outbox_receipt_insert
AFTER INSERT ON public_event_outbox
FOR EACH ROW EXECUTE FUNCTION insert_public_event_receipt();

CREATE FUNCTION reject_public_event_receipt_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'public event receipt is immutable';
END;
$$;

CREATE TRIGGER public_event_receipt_update_forbidden
BEFORE UPDATE ON public_event_receipts
FOR EACH ROW EXECUTE FUNCTION reject_public_event_receipt_mutation();

CREATE TRIGGER public_event_receipt_delete_forbidden
BEFORE DELETE ON public_event_receipts
FOR EACH ROW EXECUTE FUNCTION reject_public_event_receipt_mutation();
