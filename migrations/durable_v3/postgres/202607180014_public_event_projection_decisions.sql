-- Every execution event makes one permanent public-projection decision.
-- The row starts as `none` and may be bound once by an outbox INSERT.

CREATE TABLE public_event_projection_decisions (
    run_id TEXT NOT NULL,
    execution_event_id TEXT NOT NULL,
    execution_seq BIGINT NOT NULL CHECK (execution_seq >= 1),
    execution_occurred_at TIMESTAMPTZ NOT NULL,
    execution_transition_key TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('none', 'public')),
    public_event_id TEXT,
    public_ordinal INTEGER CHECK (public_ordinal IS NULL OR public_ordinal > 0),
    public_schema_version INTEGER CHECK (
        public_schema_version IS NULL OR public_schema_version >= 1
    ),
    event_kind TEXT,
    is_terminal BOOLEAN,
    bound_at TIMESTAMPTZ,
    PRIMARY KEY (run_id, execution_event_id),
    UNIQUE (public_event_id),
    FOREIGN KEY (run_id, execution_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    CHECK (
        (decision = 'none'
            AND public_event_id IS NULL
            AND public_ordinal IS NULL
            AND public_schema_version IS NULL
            AND event_kind IS NULL
            AND is_terminal IS NULL
            AND bound_at IS NULL)
        OR
        (decision = 'public'
            AND public_event_id IS NOT NULL
            AND public_ordinal IS NOT NULL
            AND public_schema_version IS NOT NULL
            AND event_kind IS NOT NULL
            AND is_terminal IS NOT NULL
            AND bound_at IS NOT NULL)
    )
);

-- Multiple legacy receipts for one execution event fail the primary key.
INSERT INTO public_event_projection_decisions (
    run_id,execution_event_id,execution_seq,execution_occurred_at,
    execution_transition_key,decision,public_event_id,public_ordinal,
    public_schema_version,event_kind,is_terminal,bound_at
)
SELECT event.run_id,event.event_id,event.seq,event.occurred_at,event.transition_key,
       CASE WHEN receipt.public_event_id IS NULL THEN 'none' ELSE 'public' END,
       receipt.public_event_id,receipt.public_ordinal,receipt.public_schema_version,
       receipt.event_kind,receipt.is_terminal,receipt.created_at
FROM execution_events event
LEFT JOIN public_event_receipts receipt
  ON receipt.run_id=event.run_id
 AND receipt.causation_event_id=event.event_id;

CREATE INDEX idx_v3_public_projection_order
    ON public_event_projection_decisions(
        run_id,execution_seq,public_ordinal,public_event_id
    ) WHERE decision='public';

CREATE FUNCTION insert_public_projection_decision()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public_event_projection_decisions (
        run_id,execution_event_id,execution_seq,execution_occurred_at,
        execution_transition_key,decision
    ) VALUES (
        NEW.run_id,NEW.event_id,NEW.seq,NEW.occurred_at,NEW.transition_key,'none'
    );
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_event_public_projection_decision_insert
AFTER INSERT ON execution_events
FOR EACH ROW EXECUTE FUNCTION insert_public_projection_decision();

CREATE FUNCTION guard_public_projection_decision_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.decision='none'
       AND NEW.decision='public'
       AND NEW.run_id IS NOT DISTINCT FROM OLD.run_id
       AND NEW.execution_event_id IS NOT DISTINCT FROM OLD.execution_event_id
       AND NEW.execution_seq IS NOT DISTINCT FROM OLD.execution_seq
       AND NEW.execution_occurred_at IS NOT DISTINCT FROM OLD.execution_occurred_at
       AND NEW.execution_transition_key IS NOT DISTINCT FROM OLD.execution_transition_key
       AND EXISTS (
           SELECT 1 FROM public_event_outbox outbox
           WHERE outbox.run_id=NEW.run_id
             AND outbox.causation_event_id=NEW.execution_event_id
             AND outbox.public_event_id=NEW.public_event_id
             AND outbox.public_ordinal=NEW.public_ordinal
             AND outbox.public_schema_version=NEW.public_schema_version
             AND outbox.event_kind=NEW.event_kind
             AND outbox.is_terminal=NEW.is_terminal
             AND outbox.created_at=NEW.bound_at
       ) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'public projection decision is immutable'
        USING ERRCODE = 'check_violation';
END;
$$;

CREATE TRIGGER public_event_projection_decision_mutation_guard
BEFORE UPDATE OR DELETE ON public_event_projection_decisions
FOR EACH ROW EXECUTE FUNCTION guard_public_projection_decision_mutation();

CREATE FUNCTION bind_public_projection_decision()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE public_event_projection_decisions
    SET decision='public',
        public_event_id=NEW.public_event_id,
        public_ordinal=NEW.public_ordinal,
        public_schema_version=NEW.public_schema_version,
        event_kind=NEW.event_kind,
        is_terminal=NEW.is_terminal,
        bound_at=NEW.created_at
    WHERE run_id=NEW.run_id
      AND execution_event_id=NEW.causation_event_id
      AND decision='none';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'public projection decision bind failed'
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER public_event_outbox_projection_decision_bind
AFTER INSERT ON public_event_outbox
FOR EACH ROW EXECUTE FUNCTION bind_public_projection_decision();
