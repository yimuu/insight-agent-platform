-- Close permanent public receipt provenance and replace backlog-wide public
-- claiming with one transactionally maintained delivery head per Run.
--
-- SQLite opens replay this file, so the migration is deliberately idempotent.
-- The only full-history reads below are migration-time validation/rebuilds.
-- Runtime triggers use the current head or a strict keyset successor.

BEGIN IMMEDIATE;

-- Validate the durable receipt/decision directory before adding the final
-- cardinality index. A legitimately pruned outbox body is allowed: its exact
-- receipt + projection decision pair remains permanent authority.
DROP TABLE IF EXISTS temp.v3_public_016_validation;
CREATE TEMP TABLE v3_public_016_validation (
    valid INTEGER NOT NULL CHECK (valid=1)
);
INSERT INTO v3_public_016_validation(valid)
SELECT 0
WHERE EXISTS (
    SELECT 1
    FROM public_event_receipts receipt
    LEFT JOIN public_event_projection_decisions decision
      ON decision.run_id=receipt.run_id
     AND decision.execution_event_id=receipt.causation_event_id
    WHERE decision.decision IS NOT 'public'
       OR decision.public_event_id IS NOT receipt.public_event_id
       OR decision.public_ordinal IS NOT receipt.public_ordinal
       OR decision.public_schema_version IS NOT receipt.public_schema_version
       OR decision.event_kind IS NOT receipt.event_kind
       OR decision.is_terminal IS NOT receipt.is_terminal
       OR decision.bound_at IS NOT receipt.created_at
)
OR EXISTS (
    SELECT 1
    FROM public_event_projection_decisions decision
    LEFT JOIN public_event_receipts receipt
      ON receipt.run_id=decision.run_id
     AND receipt.causation_event_id=decision.execution_event_id
    WHERE decision.decision='public'
      AND receipt.public_event_id IS NULL
)
OR EXISTS (
    SELECT 1
    FROM public_event_outbox outbox
    LEFT JOIN public_event_projection_decisions decision
      ON decision.run_id=outbox.run_id
     AND decision.execution_event_id=outbox.causation_event_id
    LEFT JOIN public_event_receipts receipt
      ON receipt.run_id=outbox.run_id
     AND receipt.causation_event_id=outbox.causation_event_id
    WHERE decision.decision IS NOT 'public'
       OR decision.public_event_id IS NOT outbox.public_event_id
       OR decision.public_ordinal IS NOT outbox.public_ordinal
       OR decision.public_schema_version IS NOT outbox.public_schema_version
       OR decision.event_kind IS NOT outbox.event_kind
       OR decision.is_terminal IS NOT outbox.is_terminal
       OR decision.bound_at IS NOT outbox.created_at
       OR receipt.public_event_id IS NOT outbox.public_event_id
       OR receipt.public_ordinal IS NOT outbox.public_ordinal
       OR receipt.public_schema_version IS NOT outbox.public_schema_version
       OR receipt.event_kind IS NOT outbox.event_kind
       OR receipt.is_terminal IS NOT outbox.is_terminal
       OR receipt.created_at IS NOT outbox.created_at
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_v3_public_receipt_causation
    ON public_event_receipts(run_id, causation_event_id);

DROP TRIGGER IF EXISTS public_event_receipt_insert_provenance;
CREATE TRIGGER public_event_receipt_insert_provenance
BEFORE INSERT ON public_event_receipts
FOR EACH ROW
WHEN NOT EXISTS (
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
)
BEGIN
    SELECT RAISE(ABORT, 'public event receipt lacks committed provenance');
END;

CREATE TABLE IF NOT EXISTS public_event_delivery_heads (
    run_id TEXT NOT NULL PRIMARY KEY,
    head_state TEXT NOT NULL CHECK (head_state IN ('ready', 'drained')),
    public_event_id TEXT,
    execution_event_id TEXT,
    execution_seq INTEGER CHECK (execution_seq IS NULL OR execution_seq >= 1),
    public_ordinal INTEGER CHECK (public_ordinal IS NULL OR public_ordinal > 0),
    delivery_state TEXT CHECK (
        delivery_state IS NULL OR delivery_state IN ('pending', 'claimed')
    ),
    due_at TEXT,
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

CREATE INDEX IF NOT EXISTS idx_v3_public_delivery_heads_due
    ON public_event_delivery_heads(
        due_at,run_id,execution_seq,public_ordinal,public_event_id
    ) WHERE head_state='ready';

-- A replayed migration must never silently repair or accept a corrupt head.
-- This scan is startup migration validation, not a runtime claim path.
DELETE FROM v3_public_016_validation;
INSERT INTO v3_public_016_validation(valid)
SELECT 0
WHERE EXISTS (
    SELECT 1
    FROM public_event_delivery_heads head
    WHERE NOT EXISTS (
        SELECT 1 FROM public_event_receipts receipt WHERE receipt.run_id=head.run_id
        UNION ALL
        SELECT 1 FROM public_event_outbox outbox WHERE outbox.run_id=head.run_id
    )
       OR (head.head_state='drained' AND EXISTS (
            SELECT 1
            FROM public_event_projection_decisions decision
            JOIN public_event_outbox outbox
              ON outbox.run_id=decision.run_id
             AND outbox.causation_event_id=decision.execution_event_id
             AND outbox.public_event_id=decision.public_event_id
            WHERE decision.run_id=head.run_id
              AND decision.decision='public'
              AND outbox.publish_state<>'published'
       ))
       OR (head.head_state='ready' AND (
            NOT EXISTS (
                SELECT 1
                FROM public_event_projection_decisions decision
                JOIN public_event_outbox outbox
                  ON outbox.run_id=decision.run_id
                 AND outbox.causation_event_id=decision.execution_event_id
                 AND outbox.public_event_id=decision.public_event_id
                WHERE decision.run_id=head.run_id
                  AND decision.execution_event_id=head.execution_event_id
                  AND decision.execution_seq=head.execution_seq
                  AND decision.public_event_id=head.public_event_id
                  AND decision.public_ordinal=head.public_ordinal
                  AND decision.decision='public'
                  AND outbox.public_ordinal=decision.public_ordinal
                  AND outbox.publish_state=head.delivery_state
                  AND outbox.publish_state<>'published'
                  AND head.due_at=CASE outbox.publish_state
                      WHEN 'pending' THEN STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.available_at)
                      ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.claim_expires_at)
                  END
            )
            OR EXISTS (
                SELECT 1
                FROM public_event_projection_decisions earlier
                JOIN public_event_outbox earlier_outbox
                  ON earlier_outbox.run_id=earlier.run_id
                 AND earlier_outbox.causation_event_id=earlier.execution_event_id
                 AND earlier_outbox.public_event_id=earlier.public_event_id
                WHERE earlier.run_id=head.run_id
                  AND earlier.decision='public'
                  AND earlier_outbox.publish_state<>'published'
                  AND (
                      earlier.execution_seq<head.execution_seq
                      OR (earlier.execution_seq=head.execution_seq
                          AND earlier.public_ordinal<head.public_ordinal)
                      OR (earlier.execution_seq=head.execution_seq
                          AND earlier.public_ordinal=head.public_ordinal
                          AND earlier.public_event_id<head.public_event_id)
                  )
            )
       ))
);

-- Remove the previous guards before the idempotent migration-time rebuild.
DROP TRIGGER IF EXISTS public_event_delivery_head_insert_guard;
DROP TRIGGER IF EXISTS public_event_delivery_head_update_guard;
DROP TRIGGER IF EXISTS public_event_delivery_head_delete_forbidden;

-- Migration-time rebuild may inspect the legacy backlog once. Existing rows
-- were validated above and are deliberately left unchanged on replay.
INSERT INTO public_event_delivery_heads (
    run_id,head_state,public_event_id,execution_event_id,
    execution_seq,public_ordinal,delivery_state,due_at
)
SELECT decision.run_id,'ready',decision.public_event_id,
       decision.execution_event_id,decision.execution_seq,
       decision.public_ordinal,outbox.publish_state,
       CASE outbox.publish_state
           WHEN 'pending' THEN STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.available_at)
           ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.claim_expires_at)
       END
FROM public_event_projection_decisions decision
JOIN public_event_outbox outbox
  ON outbox.run_id=decision.run_id
 AND outbox.causation_event_id=decision.execution_event_id
 AND outbox.public_event_id=decision.public_event_id
WHERE decision.decision='public'
  AND outbox.publish_state<>'published'
  AND NOT EXISTS (
      SELECT 1
      FROM public_event_projection_decisions earlier
      JOIN public_event_outbox earlier_outbox
        ON earlier_outbox.run_id=earlier.run_id
       AND earlier_outbox.causation_event_id=earlier.execution_event_id
       AND earlier_outbox.public_event_id=earlier.public_event_id
      WHERE earlier.run_id=decision.run_id
        AND earlier.decision='public'
        AND earlier_outbox.publish_state<>'published'
        AND (
            earlier.execution_seq<decision.execution_seq
            OR (earlier.execution_seq=decision.execution_seq
                AND earlier.public_ordinal<decision.public_ordinal)
            OR (earlier.execution_seq=decision.execution_seq
                AND earlier.public_ordinal=decision.public_ordinal
                AND earlier.public_event_id<decision.public_event_id)
        )
  )
ON CONFLICT(run_id) DO NOTHING;

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

-- INSERT is used only by the outbox INSERT trigger. It validates the local
-- candidate; the conflict arm below preserves the current smaller head.
CREATE TRIGGER public_event_delivery_head_insert_guard
BEFORE INSERT ON public_event_delivery_heads
FOR EACH ROW
WHEN NEW.head_state<>'ready' OR NOT EXISTS (
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
          WHEN 'pending' THEN STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.available_at)
          ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.claim_expires_at)
      END
)
BEGIN
    SELECT RAISE(ABORT, 'invalid public event delivery head insert');
END;

-- Runtime updates are a small local state machine:
--   ready(X) -> ready(X)       claim/reclaim
--   ready(X) -> ready(next X)  publish with strict keyset successor
--   ready(X) -> drained        publish with no successor
--   drained  -> ready(X)       first later outbox insert
--   ready(X) -> ready(earlier) defensive concurrent/out-of-order insert
CREATE TRIGGER public_event_delivery_head_update_guard
BEFORE UPDATE ON public_event_delivery_heads
FOR EACH ROW
WHEN NEW.run_id IS NOT OLD.run_id OR NOT (
    (NEW.head_state='ready'
        AND EXISTS (
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
                  WHEN 'pending' THEN STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.available_at)
                  ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.claim_expires_at)
              END
        )
        AND (
            OLD.head_state='drained'
            OR (
                OLD.head_state='ready'
                AND NEW.public_event_id=OLD.public_event_id
                AND NEW.execution_event_id=OLD.execution_event_id
                AND NEW.execution_seq=OLD.execution_seq
                AND NEW.public_ordinal=OLD.public_ordinal
                AND ((OLD.delivery_state='pending' AND NEW.delivery_state='claimed')
                     OR (OLD.delivery_state='claimed' AND NEW.delivery_state='claimed'))
            )
            OR (
                OLD.head_state='ready'
                AND (
                    NEW.execution_seq<OLD.execution_seq
                    OR (NEW.execution_seq=OLD.execution_seq
                        AND NEW.public_ordinal<OLD.public_ordinal)
                    OR (NEW.execution_seq=OLD.execution_seq
                        AND NEW.public_ordinal=OLD.public_ordinal
                        AND NEW.public_event_id<OLD.public_event_id)
                )
            )
            OR (
                OLD.head_state='ready'
                AND EXISTS (
                    SELECT 1 FROM public_event_outbox published
                    WHERE published.run_id=OLD.run_id
                      AND published.public_event_id=OLD.public_event_id
                      AND published.publish_state='published'
                )
                AND (
                    NEW.execution_seq>OLD.execution_seq
                    OR (NEW.execution_seq=OLD.execution_seq
                        AND NEW.public_ordinal>OLD.public_ordinal)
                    OR (NEW.execution_seq=OLD.execution_seq
                        AND NEW.public_ordinal=OLD.public_ordinal
                        AND NEW.public_event_id>OLD.public_event_id)
                )
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
                      AND (
                          between_decision.execution_seq>OLD.execution_seq
                          OR (between_decision.execution_seq=OLD.execution_seq
                              AND between_decision.public_ordinal>OLD.public_ordinal)
                          OR (between_decision.execution_seq=OLD.execution_seq
                              AND between_decision.public_ordinal=OLD.public_ordinal
                              AND between_decision.public_event_id>OLD.public_event_id)
                      )
                      AND (
                          between_decision.execution_seq<NEW.execution_seq
                          OR (between_decision.execution_seq=NEW.execution_seq
                              AND between_decision.public_ordinal<NEW.public_ordinal)
                          OR (between_decision.execution_seq=NEW.execution_seq
                              AND between_decision.public_ordinal=NEW.public_ordinal
                              AND between_decision.public_event_id<NEW.public_event_id)
                      )
                )
            )
        ))
    OR
    (NEW.head_state='drained'
        AND OLD.head_state='ready'
        AND OLD.delivery_state='claimed'
        AND EXISTS (
            SELECT 1 FROM public_event_outbox published
            WHERE published.run_id=OLD.run_id
              AND published.public_event_id=OLD.public_event_id
              AND published.publish_state='published'
        )
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
              AND (
                  later.execution_seq>OLD.execution_seq
                  OR (later.execution_seq=OLD.execution_seq
                      AND later.public_ordinal>OLD.public_ordinal)
                  OR (later.execution_seq=OLD.execution_seq
                      AND later.public_ordinal=OLD.public_ordinal
                      AND later.public_event_id>OLD.public_event_id)
              )
        ))
)
BEGIN
    SELECT RAISE(ABORT, 'invalid public event delivery head update');
END;

CREATE TRIGGER public_event_delivery_head_delete_forbidden
BEFORE DELETE ON public_event_delivery_heads
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'public event delivery head cannot be deleted');
END;

-- Replace the two independent AFTER INSERT triggers. Their relative firing
-- order is not a SQLite contract; one trigger establishes decision, receipt
-- and the local head atomically.
DROP TRIGGER IF EXISTS public_event_outbox_receipt_insert;
DROP TRIGGER IF EXISTS public_event_outbox_projection_decision_bind;
DROP TRIGGER IF EXISTS public_event_outbox_authority_insert;
CREATE TRIGGER public_event_outbox_authority_insert
AFTER INSERT ON public_event_outbox
FOR EACH ROW
BEGIN
    SELECT CASE WHEN NEW.publish_state<>'pending'
        THEN RAISE(ABORT, 'public outbox must enter pending') END;

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
    SELECT CASE WHEN changes()<>1
        THEN RAISE(ABORT, 'public projection decision bind failed') END;

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
           decision.execution_seq,NEW.public_ordinal,'pending',
           STRFTIME('%Y-%m-%dT%H:%M:%fZ',NEW.available_at)
    FROM public_event_projection_decisions decision
    WHERE decision.run_id=NEW.run_id
      AND decision.execution_event_id=NEW.causation_event_id
      AND decision.decision='public'
    ON CONFLICT(run_id) DO UPDATE SET
        head_state=excluded.head_state,
        public_event_id=excluded.public_event_id,
        execution_event_id=excluded.execution_event_id,
        execution_seq=excluded.execution_seq,
        public_ordinal=excluded.public_ordinal,
        delivery_state=excluded.delivery_state,
        due_at=excluded.due_at
    WHERE public_event_delivery_heads.head_state='drained'
       OR excluded.execution_seq<public_event_delivery_heads.execution_seq
       OR (excluded.execution_seq=public_event_delivery_heads.execution_seq
           AND excluded.public_ordinal<public_event_delivery_heads.public_ordinal)
       OR (excluded.execution_seq=public_event_delivery_heads.execution_seq
           AND excluded.public_ordinal=public_event_delivery_heads.public_ordinal
           AND excluded.public_event_id<public_event_delivery_heads.public_event_id);
END;

DROP TRIGGER IF EXISTS public_event_outbox_delivery_head_update;
CREATE TRIGGER public_event_outbox_delivery_head_update
AFTER UPDATE OF publish_state,claim_expires_at ON public_event_outbox
FOR EACH ROW
BEGIN
    -- Claim and reclaim only mutate the already-authoritative head.
    UPDATE public_event_delivery_heads
    SET delivery_state=NEW.publish_state,
        due_at=CASE NEW.publish_state
            WHEN 'pending' THEN STRFTIME('%Y-%m-%dT%H:%M:%fZ',NEW.available_at)
            ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ',NEW.claim_expires_at)
        END
    WHERE NEW.publish_state<>'published'
      AND run_id=NEW.run_id
      AND public_event_id=NEW.public_event_id;
    SELECT CASE WHEN NEW.publish_state<>'published' AND changes()<>1
        THEN RAISE(ABORT, 'public claim does not own delivery head') END;

    -- Publishing seeks strictly after OLD. The projection-order index makes
    -- this O(log n + 1); it never restarts at the beginning of Run history.
    INSERT INTO public_event_delivery_heads (
        run_id,head_state,public_event_id,execution_event_id,
        execution_seq,public_ordinal,delivery_state,due_at
    )
    SELECT next.run_id,'ready',next.public_event_id,next.execution_event_id,
           next.execution_seq,next.public_ordinal,next_outbox.publish_state,
           CASE next_outbox.publish_state
               WHEN 'pending' THEN STRFTIME('%Y-%m-%dT%H:%M:%fZ',next_outbox.available_at)
               ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ',next_outbox.claim_expires_at)
           END
    FROM public_event_projection_decisions current
    JOIN public_event_projection_decisions next ON next.run_id=current.run_id
    JOIN public_event_outbox next_outbox
      ON next_outbox.run_id=next.run_id
     AND next_outbox.causation_event_id=next.execution_event_id
     AND next_outbox.public_event_id=next.public_event_id
    WHERE NEW.publish_state='published'
      AND current.run_id=NEW.run_id
      AND current.execution_event_id=NEW.causation_event_id
      AND current.decision='public'
      AND next.decision='public'
      AND next_outbox.publish_state<>'published'
      AND (
          next.execution_seq>current.execution_seq
          OR (next.execution_seq=current.execution_seq
              AND next.public_ordinal>current.public_ordinal)
          OR (next.execution_seq=current.execution_seq
              AND next.public_ordinal=current.public_ordinal
              AND next.public_event_id>current.public_event_id)
      )
    ORDER BY next.execution_seq,next.public_ordinal,next.public_event_id
    LIMIT 1
    ON CONFLICT(run_id) DO UPDATE SET
        head_state=excluded.head_state,
        public_event_id=excluded.public_event_id,
        execution_event_id=excluded.execution_event_id,
        execution_seq=excluded.execution_seq,
        public_ordinal=excluded.public_ordinal,
        delivery_state=excluded.delivery_state,
        due_at=excluded.due_at;

    UPDATE public_event_delivery_heads
    SET head_state='drained',public_event_id=NULL,execution_event_id=NULL,
        execution_seq=NULL,public_ordinal=NULL,delivery_state=NULL,due_at=NULL
    WHERE NEW.publish_state='published'
      AND run_id=NEW.run_id
      AND public_event_id=NEW.public_event_id;

    SELECT CASE WHEN NEW.publish_state='published' AND EXISTS (
        SELECT 1 FROM public_event_delivery_heads head
        WHERE head.run_id=NEW.run_id
          AND (head.head_state='ready' AND head.public_event_id=NEW.public_event_id)
    ) THEN RAISE(ABORT, 'published public event remained delivery head') END;
END;

DROP TABLE v3_public_016_validation;
COMMIT;
