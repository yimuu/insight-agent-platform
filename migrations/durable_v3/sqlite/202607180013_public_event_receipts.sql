-- Public outbox bodies may expire, but transition replay identity may not.
-- This body-free receipt is the permanent public-event idempotency authority.

CREATE TABLE IF NOT EXISTS public_event_receipts (
    run_id TEXT NOT NULL,
    public_event_id TEXT NOT NULL,
    causation_event_id TEXT NOT NULL,
    public_ordinal INTEGER NOT NULL CHECK (public_ordinal > 0),
    public_schema_version INTEGER NOT NULL CHECK (public_schema_version >= 1),
    event_kind TEXT NOT NULL,
    is_terminal INTEGER NOT NULL CHECK (is_terminal IN (0, 1)),
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, public_event_id),
    UNIQUE (public_event_id),
    UNIQUE (run_id, causation_event_id, event_kind),
    UNIQUE (run_id, causation_event_id, public_ordinal),
    FOREIGN KEY (run_id, causation_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    CHECK (
        (is_terminal = 0 AND event_kind IN (
            'run.created','run.started','operation.started',
            'operation.completed','operation.failed'
        ))
        OR (is_terminal = 1 AND event_kind IN (
            'run.completed','run.failed','run.cancelled','run.interrupted'
        ))
    )
);

INSERT OR IGNORE INTO public_event_receipts (
    run_id,public_event_id,causation_event_id,public_ordinal,
    public_schema_version,event_kind,is_terminal,created_at
)
SELECT run_id,public_event_id,causation_event_id,public_ordinal,
       public_schema_version,event_kind,is_terminal,created_at
FROM public_event_outbox;

DROP TRIGGER IF EXISTS public_event_outbox_receipt_insert;
CREATE TRIGGER public_event_outbox_receipt_insert
AFTER INSERT ON public_event_outbox
FOR EACH ROW
BEGIN
    INSERT INTO public_event_receipts (
        run_id,public_event_id,causation_event_id,public_ordinal,
        public_schema_version,event_kind,is_terminal,created_at
    ) VALUES (
        NEW.run_id,NEW.public_event_id,NEW.causation_event_id,NEW.public_ordinal,
        NEW.public_schema_version,NEW.event_kind,NEW.is_terminal,NEW.created_at
    );
END;

DROP TRIGGER IF EXISTS public_event_receipt_update_forbidden;
CREATE TRIGGER public_event_receipt_update_forbidden
BEFORE UPDATE ON public_event_receipts
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'public event receipt is immutable');
END;

DROP TRIGGER IF EXISTS public_event_receipt_delete_forbidden;
CREATE TRIGGER public_event_receipt_delete_forbidden
BEFORE DELETE ON public_event_receipts
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'public event receipt cannot be deleted');
END;
