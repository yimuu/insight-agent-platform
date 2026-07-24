-- Close permanent public receipt provenance and replace backlog-wide public
-- claiming with one transactionally maintained delivery head per Run.
-- Full-history reads in this file are migration-time validation/rebuild only;
-- runtime triggers use the current head or a strict keyset successor.

-- A legitimately pruned outbox body is allowed: its exact permanent receipt
-- and projection decision remain authority. Any still-present outbox row must
-- agree with both permanent records byte-for-byte/field-for-field.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public_event_receipts receipt
        LEFT JOIN public_event_projection_decisions decision
          ON decision.run_id=receipt.run_id
         AND decision.execution_event_id=receipt.causation_event_id
        WHERE decision.decision IS DISTINCT FROM 'public'
           OR decision.public_event_id IS DISTINCT FROM receipt.public_event_id
           OR decision.public_ordinal IS DISTINCT FROM receipt.public_ordinal
           OR decision.public_schema_version IS DISTINCT FROM receipt.public_schema_version
           OR decision.event_kind IS DISTINCT FROM receipt.event_kind
           OR decision.is_terminal IS DISTINCT FROM receipt.is_terminal
           OR decision.bound_at IS DISTINCT FROM receipt.created_at
    ) OR EXISTS (
        SELECT 1
        FROM public_event_projection_decisions decision
        LEFT JOIN public_event_receipts receipt
          ON receipt.run_id=decision.run_id
         AND receipt.causation_event_id=decision.execution_event_id
        WHERE decision.decision='public'
          AND receipt.public_event_id IS NULL
    ) OR EXISTS (
        SELECT 1
        FROM public_event_outbox outbox
        LEFT JOIN public_event_projection_decisions decision
          ON decision.run_id=outbox.run_id
         AND decision.execution_event_id=outbox.causation_event_id
        LEFT JOIN public_event_receipts receipt
          ON receipt.run_id=outbox.run_id
         AND receipt.causation_event_id=outbox.causation_event_id
        WHERE decision.decision IS DISTINCT FROM 'public'
           OR decision.public_event_id IS DISTINCT FROM outbox.public_event_id
           OR decision.public_ordinal IS DISTINCT FROM outbox.public_ordinal
           OR decision.public_schema_version IS DISTINCT FROM outbox.public_schema_version
           OR decision.event_kind IS DISTINCT FROM outbox.event_kind
           OR decision.is_terminal IS DISTINCT FROM outbox.is_terminal
           OR decision.bound_at IS DISTINCT FROM outbox.created_at
           OR receipt.public_event_id IS DISTINCT FROM outbox.public_event_id
           OR receipt.public_ordinal IS DISTINCT FROM outbox.public_ordinal
           OR receipt.public_schema_version IS DISTINCT FROM outbox.public_schema_version
           OR receipt.event_kind IS DISTINCT FROM outbox.event_kind
           OR receipt.is_terminal IS DISTINCT FROM outbox.is_terminal
           OR receipt.created_at IS DISTINCT FROM outbox.created_at
    ) THEN
        RAISE EXCEPTION 'existing public event authority lacks provenance'
            USING ERRCODE='check_violation';
    END IF;
END;
$$;

ALTER TABLE public_event_receipts
    ADD CONSTRAINT uq_public_receipt_causation
    UNIQUE (run_id, causation_event_id);

CREATE FUNCTION guard_public_event_receipt_provenance()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public_event_outbox outbox
        JOIN public_event_projection_decisions decision
          ON decision.run_id=outbox.run_id
         AND decision.execution_event_id=outbox.causation_event_id
        WHERE outbox.run_id=NEW.run_id
          AND outbox.public_event_id=NEW.public_event_id
          AND outbox.causation_event_id=NEW.causation_event_id
          AND outbox.public_ordinal=NEW.public_ordinal
          AND outbox.public_schema_version=NEW.public_schema_version
          AND outbox.event_kind=NEW.event_kind
          AND outbox.is_terminal=NEW.is_terminal
          AND outbox.created_at=NEW.created_at
          AND decision.decision='public'
          AND decision.public_event_id=NEW.public_event_id
          AND decision.public_ordinal=NEW.public_ordinal
          AND decision.public_schema_version=NEW.public_schema_version
          AND decision.event_kind=NEW.event_kind
          AND decision.is_terminal=NEW.is_terminal
          AND decision.bound_at=NEW.created_at
    ) THEN
        RAISE EXCEPTION 'public event receipt lacks committed provenance'
            USING ERRCODE='check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER public_event_receipt_insert_provenance
BEFORE INSERT ON public_event_receipts
FOR EACH ROW EXECUTE FUNCTION guard_public_event_receipt_provenance();

CREATE TABLE public_event_delivery_heads (
    run_id TEXT NOT NULL PRIMARY KEY,
    head_state TEXT NOT NULL CHECK (head_state IN ('ready', 'drained')),
    public_event_id TEXT,
    execution_event_id TEXT,
    execution_seq BIGINT CHECK (execution_seq IS NULL OR execution_seq >= 1),
    public_ordinal INTEGER CHECK (public_ordinal IS NULL OR public_ordinal > 0),
    delivery_state TEXT CHECK (
        delivery_state IS NULL OR delivery_state IN ('pending', 'claimed')
    ),
    due_at TIMESTAMPTZ,
    UNIQUE (public_event_id),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, public_event_id)
        REFERENCES public_event_outbox(run_id, public_event_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, execution_event_id)
        REFERENCES public_event_projection_decisions(run_id, execution_event_id)
        ON DELETE RESTRICT,
    CHECK (
        (head_state='ready'
            AND public_event_id IS NOT NULL
            AND execution_event_id IS NOT NULL
            AND execution_seq IS NOT NULL
            AND public_ordinal IS NOT NULL
            AND delivery_state IS NOT NULL
            AND due_at IS NOT NULL)
        OR
        (head_state='drained'
            AND public_event_id IS NULL
            AND execution_event_id IS NULL
            AND execution_seq IS NULL
            AND public_ordinal IS NULL
            AND delivery_state IS NULL
            AND due_at IS NULL)
    )
);

CREATE INDEX idx_public_delivery_heads_due
    ON public_event_delivery_heads(
        due_at,run_id,execution_seq,public_ordinal,public_event_id
    ) WHERE head_state='ready';

-- Migration-time rebuild may inspect the legacy backlog once.
INSERT INTO public_event_delivery_heads (
    run_id,head_state,public_event_id,execution_event_id,
    execution_seq,public_ordinal,delivery_state,due_at
)
SELECT DISTINCT ON (decision.run_id)
       decision.run_id,'ready',decision.public_event_id,
       decision.execution_event_id,decision.execution_seq,
       decision.public_ordinal,outbox.publish_state,
       CASE outbox.publish_state
           WHEN 'pending' THEN outbox.available_at
           ELSE outbox.claim_expires_at
       END
FROM public_event_projection_decisions decision
JOIN public_event_outbox outbox
  ON outbox.run_id=decision.run_id
 AND outbox.causation_event_id=decision.execution_event_id
 AND outbox.public_event_id=decision.public_event_id
WHERE decision.decision='public'
  AND outbox.publish_state<>'published'
ORDER BY decision.run_id,decision.execution_seq,
         decision.public_ordinal,decision.public_event_id;

INSERT INTO public_event_delivery_heads (run_id,head_state)
SELECT authority.run_id,'drained'
FROM (
    SELECT run_id FROM public_event_receipts
    UNION
    SELECT run_id FROM public_event_outbox
) authority
WHERE NOT EXISTS (
    SELECT 1 FROM public_event_delivery_heads existing
    WHERE existing.run_id=authority.run_id
);

CREATE FUNCTION guard_public_event_delivery_head()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    candidate_valid BOOLEAN;
    old_published BOOLEAN;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'public event delivery head cannot be deleted'
            USING ERRCODE='check_violation';
    END IF;

    IF TG_OP='UPDATE' AND NEW.run_id IS DISTINCT FROM OLD.run_id THEN
        RAISE EXCEPTION 'public event delivery head Run is immutable'
            USING ERRCODE='check_violation';
    END IF;

    IF NEW.head_state='ready' THEN
        SELECT EXISTS (
            SELECT 1
            FROM public_event_projection_decisions decision
            JOIN public_event_outbox outbox
              ON outbox.run_id=decision.run_id
             AND outbox.causation_event_id=decision.execution_event_id
             AND outbox.public_event_id=decision.public_event_id
            WHERE decision.run_id=NEW.run_id
              AND decision.execution_event_id=NEW.execution_event_id
              AND decision.execution_seq=NEW.execution_seq
              AND decision.public_event_id=NEW.public_event_id
              AND decision.public_ordinal=NEW.public_ordinal
              AND decision.decision='public'
              AND outbox.public_ordinal=decision.public_ordinal
              AND outbox.publish_state=NEW.delivery_state
              AND outbox.publish_state<>'published'
              AND NEW.due_at=CASE outbox.publish_state
                  WHEN 'pending' THEN outbox.available_at
                  ELSE outbox.claim_expires_at
              END
        ) INTO candidate_valid;
        IF NOT candidate_valid THEN
            RAISE EXCEPTION 'invalid public event delivery head candidate'
                USING ERRCODE='check_violation';
        END IF;
    END IF;

    IF TG_OP='INSERT' THEN
        IF NEW.head_state<>'ready' THEN
            RAISE EXCEPTION 'invalid public event delivery head insert'
                USING ERRCODE='check_violation';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.head_state='drained' AND NEW.head_state='ready' THEN
        RETURN NEW;
    END IF;

    IF OLD.head_state='ready' AND NEW.head_state='ready'
       AND NEW.public_event_id=OLD.public_event_id
       AND NEW.execution_event_id=OLD.execution_event_id
       AND NEW.execution_seq=OLD.execution_seq
       AND NEW.public_ordinal=OLD.public_ordinal
       AND ((OLD.delivery_state='pending' AND NEW.delivery_state='claimed')
            OR (OLD.delivery_state='claimed' AND NEW.delivery_state='claimed')) THEN
        RETURN NEW;
    END IF;

    -- A concurrently inserted smaller key may replace the current key. Normal
    -- repository transitions are monotonic, but accepting only a smaller key
    -- remains order-safe and keeps the local upsert race-proof.
    IF OLD.head_state='ready' AND NEW.head_state='ready'
       AND (NEW.execution_seq,NEW.public_ordinal,NEW.public_event_id)
           < (OLD.execution_seq,OLD.public_ordinal,OLD.public_event_id) THEN
        RETURN NEW;
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM public_event_outbox published
        WHERE published.run_id=OLD.run_id
          AND published.public_event_id=OLD.public_event_id
          AND published.publish_state='published'
    ) INTO old_published;

    IF OLD.head_state='ready' AND OLD.delivery_state='claimed'
       AND old_published AND NEW.head_state='ready'
       AND (NEW.execution_seq,NEW.public_ordinal,NEW.public_event_id)
           > (OLD.execution_seq,OLD.public_ordinal,OLD.public_event_id)
       AND NOT EXISTS (
           SELECT 1
           FROM public_event_projection_decisions between_decision
           JOIN public_event_outbox between_outbox
             ON between_outbox.run_id=between_decision.run_id
            AND between_outbox.causation_event_id=between_decision.execution_event_id
            AND between_outbox.public_event_id=between_decision.public_event_id
           WHERE between_decision.run_id=OLD.run_id
             AND between_decision.decision='public'
             AND between_outbox.publish_state<>'published'
             AND (between_decision.execution_seq,between_decision.public_ordinal,
                  between_decision.public_event_id)
                 > (OLD.execution_seq,OLD.public_ordinal,OLD.public_event_id)
             AND (between_decision.execution_seq,between_decision.public_ordinal,
                  between_decision.public_event_id)
                 < (NEW.execution_seq,NEW.public_ordinal,NEW.public_event_id)
       ) THEN
        RETURN NEW;
    END IF;

    IF OLD.head_state='ready' AND OLD.delivery_state='claimed'
       AND old_published AND NEW.head_state='drained'
       AND NOT EXISTS (
           SELECT 1
           FROM public_event_projection_decisions later
           JOIN public_event_outbox later_outbox
             ON later_outbox.run_id=later.run_id
            AND later_outbox.causation_event_id=later.execution_event_id
            AND later_outbox.public_event_id=later.public_event_id
           WHERE later.run_id=OLD.run_id
             AND later.decision='public'
             AND later_outbox.publish_state<>'published'
             AND (later.execution_seq,later.public_ordinal,later.public_event_id)
                 > (OLD.execution_seq,OLD.public_ordinal,OLD.public_event_id)
       ) THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'invalid public event delivery head update'
        USING ERRCODE='check_violation';
END;
$$;

CREATE TRIGGER public_event_delivery_head_mutation_guard
BEFORE INSERT OR UPDATE OR DELETE ON public_event_delivery_heads
FOR EACH ROW EXECUTE FUNCTION guard_public_event_delivery_head();

-- One AFTER INSERT trigger establishes decision, permanent receipt and the
-- local head in a deterministic all-or-nothing chain.
DROP TRIGGER public_event_outbox_receipt_insert ON public_event_outbox;
DROP TRIGGER public_event_outbox_projection_decision_bind ON public_event_outbox;

CREATE FUNCTION establish_public_event_authority()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    affected_rows INTEGER;
BEGIN
    IF NEW.publish_state<>'pending' THEN
        RAISE EXCEPTION 'public outbox must enter pending'
            USING ERRCODE='check_violation';
    END IF;

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
    GET DIAGNOSTICS affected_rows=ROW_COUNT;
    IF affected_rows<>1 THEN
        RAISE EXCEPTION 'public projection decision bind failed'
            USING ERRCODE='check_violation';
    END IF;

    INSERT INTO public_event_receipts (
        run_id,public_event_id,causation_event_id,public_ordinal,
        public_schema_version,event_kind,is_terminal,created_at
    ) VALUES (
        NEW.run_id,NEW.public_event_id,NEW.causation_event_id,NEW.public_ordinal,
        NEW.public_schema_version,NEW.event_kind,NEW.is_terminal,NEW.created_at
    );

    INSERT INTO public_event_delivery_heads (
        run_id,head_state,public_event_id,execution_event_id,
        execution_seq,public_ordinal,delivery_state,due_at
    )
    SELECT NEW.run_id,'ready',NEW.public_event_id,NEW.causation_event_id,
           decision.execution_seq,NEW.public_ordinal,'pending',NEW.available_at
    FROM public_event_projection_decisions decision
    WHERE decision.run_id=NEW.run_id
      AND decision.execution_event_id=NEW.causation_event_id
      AND decision.decision='public'
    ON CONFLICT(run_id) DO UPDATE SET
        head_state=EXCLUDED.head_state,
        public_event_id=EXCLUDED.public_event_id,
        execution_event_id=EXCLUDED.execution_event_id,
        execution_seq=EXCLUDED.execution_seq,
        public_ordinal=EXCLUDED.public_ordinal,
        delivery_state=EXCLUDED.delivery_state,
        due_at=EXCLUDED.due_at
    WHERE public_event_delivery_heads.head_state='drained'
       OR (EXCLUDED.execution_seq,EXCLUDED.public_ordinal,EXCLUDED.public_event_id)
          < (public_event_delivery_heads.execution_seq,
             public_event_delivery_heads.public_ordinal,
             public_event_delivery_heads.public_event_id);
    RETURN NEW;
END;
$$;

CREATE TRIGGER public_event_outbox_authority_insert
AFTER INSERT ON public_event_outbox
FOR EACH ROW EXECUTE FUNCTION establish_public_event_authority();

CREATE FUNCTION synchronize_public_event_delivery_head()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    affected_rows INTEGER;
BEGIN
    IF NEW.publish_state<>'published' THEN
        UPDATE public_event_delivery_heads
        SET delivery_state=NEW.publish_state,
            due_at=CASE NEW.publish_state
                WHEN 'pending' THEN NEW.available_at
                ELSE NEW.claim_expires_at
            END
        WHERE run_id=NEW.run_id
          AND public_event_id=NEW.public_event_id;
        GET DIAGNOSTICS affected_rows=ROW_COUNT;
        IF affected_rows<>1 THEN
            RAISE EXCEPTION 'public claim does not own delivery head'
                USING ERRCODE='check_violation';
        END IF;
        RETURN NEW;
    END IF;

    -- Seek strictly after the published key. The projection-order index starts
    -- at OLD rather than scanning from the beginning of Run history.
    INSERT INTO public_event_delivery_heads (
        run_id,head_state,public_event_id,execution_event_id,
        execution_seq,public_ordinal,delivery_state,due_at
    )
    SELECT next.run_id,'ready',next.public_event_id,next.execution_event_id,
           next.execution_seq,next.public_ordinal,next_outbox.publish_state,
           CASE next_outbox.publish_state
               WHEN 'pending' THEN next_outbox.available_at
               ELSE next_outbox.claim_expires_at
           END
    FROM public_event_projection_decisions current
    JOIN public_event_projection_decisions next ON next.run_id=current.run_id
    JOIN public_event_outbox next_outbox
      ON next_outbox.run_id=next.run_id
     AND next_outbox.causation_event_id=next.execution_event_id
     AND next_outbox.public_event_id=next.public_event_id
    WHERE current.run_id=NEW.run_id
      AND current.execution_event_id=NEW.causation_event_id
      AND current.decision='public'
      AND next.decision='public'
      AND next_outbox.publish_state<>'published'
      AND (next.execution_seq,next.public_ordinal,next.public_event_id)
          > (current.execution_seq,current.public_ordinal,current.public_event_id)
    ORDER BY next.execution_seq,next.public_ordinal,next.public_event_id
    LIMIT 1
    ON CONFLICT(run_id) DO UPDATE SET
        head_state=EXCLUDED.head_state,
        public_event_id=EXCLUDED.public_event_id,
        execution_event_id=EXCLUDED.execution_event_id,
        execution_seq=EXCLUDED.execution_seq,
        public_ordinal=EXCLUDED.public_ordinal,
        delivery_state=EXCLUDED.delivery_state,
        due_at=EXCLUDED.due_at;

    UPDATE public_event_delivery_heads
    SET head_state='drained',public_event_id=NULL,execution_event_id=NULL,
        execution_seq=NULL,public_ordinal=NULL,delivery_state=NULL,due_at=NULL
    WHERE run_id=NEW.run_id
      AND public_event_id=NEW.public_event_id;

    IF EXISTS (
        SELECT 1 FROM public_event_delivery_heads head
        WHERE head.run_id=NEW.run_id
          AND head.head_state='ready'
          AND head.public_event_id=NEW.public_event_id
    ) THEN
        RAISE EXCEPTION 'published public event remained delivery head'
            USING ERRCODE='check_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER public_event_outbox_delivery_head_update
AFTER UPDATE OF publish_state,claim_expires_at ON public_event_outbox
FOR EACH ROW EXECUTE FUNCTION synchronize_public_event_delivery_head();
