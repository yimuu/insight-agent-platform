-- Durable repository schema for a new, empty PostgreSQL 16 database/schema.
-- Provision this file before starting the service.

BEGIN;

-- Refuse to adopt a schema that already owns user objects, even when none of
-- their names collide with the durable repository. The target schema itself
-- must be created before this file is executed.
DO $durable_schema_target_must_be_empty$
DECLARE
    target_namespace OID;
BEGIN
    SELECT oid
    INTO target_namespace
    FROM pg_catalog.pg_namespace
    WHERE nspname = current_schema();

    IF target_namespace IS NULL THEN
        RAISE EXCEPTION 'durable Schema target does not exist'
            USING ERRCODE = '3F000';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_class
        WHERE relnamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_proc
        WHERE pronamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_type
        WHERE typnamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_operator
        WHERE oprnamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_opclass
        WHERE opcnamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_opfamily
        WHERE opfnamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_collation
        WHERE collnamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_conversion
        WHERE connamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_ts_config
        WHERE cfgnamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_ts_dict
        WHERE dictnamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_ts_parser
        WHERE prsnamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_ts_template
        WHERE tmplnamespace = target_namespace
        UNION ALL
        SELECT 1 FROM pg_catalog.pg_statistic_ext
        WHERE stxnamespace = target_namespace
    ) THEN
        RAISE EXCEPTION 'durable Schema provisioning requires an empty target schema'
            USING ERRCODE = '55000';
    END IF;
END;
$durable_schema_target_must_be_empty$;

-- Functions are declared before their referenced tables so the final schema
-- remains deterministic without a separate post-processing dependency pass.
SET LOCAL check_function_bodies = false;

CREATE FUNCTION notify_durable_work() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    target_namespace OID;
    old_row JSONB;
    new_row JSONB := to_jsonb(NEW);
    should_notify BOOLEAN := FALSE;
BEGIN
    old_row := CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) ELSE '{}'::jsonb END;

    -- The runtime marks its SQL connections with this fixed application name.
    -- Those commits already wake their process-local WorkCoordinator, while a
    -- bounded publisher sends at most one cross-process hint per debounce
    -- window after commit. Skipping row-triggered NOTIFY here removes the
    -- notification-ordering lock from every authoritative write transaction.
    -- Other repository writers retain commit-scoped trigger hints.
    IF current_setting('application_name', TRUE) =
       'insight-agent-platform-runtime' THEN
        RETURN NEW;
    END IF;

    -- Only transitions that make work immediately claimable need a low-latency
    -- hint. Claimed/running rows are not eligible until a database deadline;
    -- the coordinator's bounded deadline/safety scan owns that later wakeup.
    should_notify := CASE TG_TABLE_NAME
        WHEN 'task_outbox' THEN
            new_row->>'task_state' IN ('pending', 'published')
            AND (
                TG_OP = 'INSERT'
                OR old_row->>'task_state' IS DISTINCT FROM new_row->>'task_state'
                OR old_row->>'available_at' IS DISTINCT FROM new_row->>'available_at'
            )
        WHEN 'model_tool_calls' THEN
            new_row->>'call_status' = 'pending'
            AND (
                TG_OP = 'INSERT'
                OR old_row->>'call_status' IS DISTINCT FROM new_row->>'call_status'
                OR old_row->>'available_at' IS DISTINCT FROM new_row->>'available_at'
            )
        WHEN 'timers' THEN
            new_row->>'timer_state' = 'scheduled'
            AND (
                TG_OP = 'INSERT'
                OR old_row->>'timer_state' IS DISTINCT FROM new_row->>'timer_state'
                OR old_row->>'deadline_at' IS DISTINCT FROM new_row->>'deadline_at'
            )
        WHEN 'signals_inbox' THEN
            new_row->>'signal_state' = 'pending'
            AND (
                TG_OP = 'INSERT'
                OR old_row->>'signal_state' IS DISTINCT FROM new_row->>'signal_state'
            )
        WHEN 'wait_late_audit_outbox' THEN
            new_row->>'audit_state' = 'pending'
            AND (
                TG_OP = 'INSERT'
                OR old_row->>'audit_state' IS DISTINCT FROM new_row->>'audit_state'
                OR old_row->>'due_at' IS DISTINCT FROM new_row->>'due_at'
            )
        WHEN 'public_event_delivery_heads' THEN
            new_row->>'head_state' = 'ready'
            AND (
                TG_OP = 'INSERT'
                OR old_row->>'head_state' IS DISTINCT FROM new_row->>'head_state'
                OR old_row->>'due_at' IS DISTINCT FROM new_row->>'due_at'
                OR old_row->>'public_event_id' IS DISTINCT FROM new_row->>'public_event_id'
            )
        WHEN 'workflow_runs' THEN
            (
                new_row->>'lifecycle' = 'terminating'
                OR (
                    new_row->>'lifecycle' IN ('created', 'active', 'waiting')
                    AND new_row->>'admission_state' = 'open'
                )
            )
            AND (
                TG_OP = 'INSERT'
                OR old_row->>'lifecycle' IS DISTINCT FROM new_row->>'lifecycle'
                OR old_row->>'admission_state' IS DISTINCT FROM new_row->>'admission_state'
                OR old_row->>'projection_version' IS DISTINCT FROM new_row->>'projection_version'
            )
        ELSE FALSE
    END;
    IF NOT should_notify THEN
        RETURN NEW;
    END IF;

    -- One transaction can make several work classes eligible. A single
    -- payload-free all-class hint is enough: PostgreSQL state remains the
    -- authority and the coordinator coalesces before running bounded queries.
    -- The transaction-local flag also avoids repeated pg_notify() calls from
    -- multiple row triggers in the same commit.
    IF current_setting('iap.work_notify_emitted', TRUE) = '1' THEN
        RETURN NEW;
    END IF;
    PERFORM set_config('iap.work_notify_emitted', '1', TRUE);

    SELECT oid INTO target_namespace
    FROM pg_catalog.pg_namespace
    WHERE nspname=current_schema();
    IF target_namespace IS NULL THEN
        RAISE EXCEPTION 'durable Schema target does not exist'
            USING ERRCODE = '3F000';
    END IF;
    PERFORM pg_notify('iap_work_' || target_namespace::text, 'maintenance');
    RETURN NEW;
END;
$$;

CREATE FUNCTION bind_public_projection_decision() RETURNS trigger
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

CREATE FUNCTION close_human_work_items_for_run() RETURNS trigger
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

CREATE FUNCTION durable_reject_definition_public_metadata_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'published workflow definition public metadata is immutable'
        USING ERRCODE = 'check_violation';
END;
$$;

CREATE FUNCTION durable_reject_deployment_revision_update() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'deployment revisions are immutable'
        USING ERRCODE = 'check_violation';
END;
$$;

CREATE FUNCTION durable_reject_published_revision_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.revision_status = 'published' THEN
        RAISE EXCEPTION 'published workflow definition revisions are immutable'
            USING ERRCODE = 'check_violation';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION durable_validate_publication_head() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM workflow_definitions d
        JOIN deployment_revisions x
          ON x.definition_id = d.definition_id
        WHERE d.definition_id = NEW.definition_id
          AND d.agent_id = NEW.agent_id
          AND x.definition_revision_id = NEW.definition_revision_id
          AND x.deployment_revision_id = NEW.deployment_revision_id
    ) THEN
        RAISE EXCEPTION 'publication head agent does not own definition'
            USING ERRCODE = 'foreign_key_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE FUNCTION enforce_public_event_outbox_update_contract() RETURNS trigger
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

CREATE FUNCTION establish_public_event_authority() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    affected_rows INTEGER;
    authority_execution_seq BIGINT;
    current_head_state TEXT;
    current_public_event_id TEXT;
    current_execution_seq BIGINT;
    current_public_ordinal INTEGER;
BEGIN
    -- The non-locking ready-head fast path below relies on statement-fresh
    -- snapshots. Fail closed for direct callers and future repository paths
    -- that bypass the explicit READ COMMITTED transaction entry point.
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'public outbox writes require READ COMMITTED'
            USING ERRCODE='check_violation';
    END IF;

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

    SELECT decision.execution_seq
    INTO authority_execution_seq
    FROM public_event_projection_decisions decision
    WHERE decision.run_id=NEW.run_id
      AND decision.execution_event_id=NEW.causation_event_id
      AND decision.decision='public';
    IF authority_execution_seq IS NULL THEN
        RAISE EXCEPTION 'public projection decision authority is missing'
            USING ERRCODE='check_violation';
    END IF;

    LOOP
        -- Do not lock or update a ready head for the normal monotonic case.
        -- Event sequence allocation is serialized by workflow_runs, so a
        -- smaller key indicates a broken repository/backfill invariant.
        SELECT head_state,public_event_id,execution_seq,public_ordinal
        INTO current_head_state,current_public_event_id,
             current_execution_seq,current_public_ordinal
        FROM public_event_delivery_heads
        WHERE run_id=NEW.run_id;

        IF NOT FOUND THEN
            INSERT INTO public_event_delivery_heads (
                run_id,head_state,public_event_id,execution_event_id,
                execution_seq,public_ordinal,delivery_state,due_at
            ) VALUES (
                NEW.run_id,'ready',NEW.public_event_id,NEW.causation_event_id,
                authority_execution_seq,NEW.public_ordinal,'pending',NEW.available_at
            )
            ON CONFLICT(run_id) DO NOTHING;
            GET DIAGNOSTICS affected_rows=ROW_COUNT;
            IF affected_rows=1 THEN
                RETURN NEW;
            END IF;
            CONTINUE;
        END IF;

        IF current_head_state='ready' THEN
            IF (authority_execution_seq,NEW.public_ordinal,NEW.public_event_id)
               < (current_execution_seq,current_public_ordinal,current_public_event_id) THEN
                RAISE EXCEPTION 'public event key regressed for run %',NEW.run_id
                    USING ERRCODE='check_violation';
            END IF;
            RETURN NEW;
        END IF;

        UPDATE public_event_delivery_heads
        SET head_state='ready',
            public_event_id=NEW.public_event_id,
            execution_event_id=NEW.causation_event_id,
            execution_seq=authority_execution_seq,
            public_ordinal=NEW.public_ordinal,
            delivery_state='pending',
            due_at=NEW.available_at
        WHERE run_id=NEW.run_id
          AND head_state='drained';
        GET DIAGNOSTICS affected_rows=ROW_COUNT;
        IF affected_rows=1 THEN
            RETURN NEW;
        END IF;
    END LOOP;
END;
$$;

CREATE FUNCTION forbid_workflow_retrieval_publication_rewrite() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'workflow retrieval publication is immutable';
END;
$$;

CREATE FUNCTION guard_public_event_delivery_head() RETURNS trigger
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

CREATE FUNCTION guard_public_event_receipt_provenance() RETURNS trigger
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

CREATE FUNCTION guard_public_projection_decision_mutation() RETURNS trigger
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

CREATE FUNCTION insert_public_event_receipt() RETURNS trigger
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

CREATE FUNCTION insert_public_projection_decision() RETURNS trigger
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

CREATE FUNCTION permit_expired_public_event_prune() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF OLD.publish_state = 'published'
       AND NOT OLD.is_terminal
       AND OLD.retain_until IS NOT NULL
       AND OLD.retain_until <= clock_timestamp() THEN
        RETURN OLD;
    END IF;
    RAISE EXCEPTION 'public event delivery authority is not eligible for retention pruning';
END;
$$;

CREATE FUNCTION reject_artifact_store_authority_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'artifact store authority is immutable';
END;
$$;

CREATE FUNCTION reject_durable_authority_delete() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'durable delivery/idempotency authority cannot be deleted';
END;
$$;

CREATE FUNCTION reject_execution_event_projection_ledger_rewrite() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.projection_ledger_batch IS NOT NULL THEN
            RAISE EXCEPTION 'execution event projection ledger must be empty on insert'
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'execution event authority cannot be deleted'
            USING ERRCODE = 'check_violation';
    END IF;

    IF OLD.projection_ledger_batch IS NULL
       AND NEW.projection_ledger_batch IS NOT NULL
       AND NEW.run_id IS NOT DISTINCT FROM OLD.run_id
       AND NEW.seq IS NOT DISTINCT FROM OLD.seq
       AND NEW.event_id IS NOT DISTINCT FROM OLD.event_id
       AND NEW.schema_version IS NOT DISTINCT FROM OLD.schema_version
       AND NEW.kind IS NOT DISTINCT FROM OLD.kind
       AND NEW.transition_key IS NOT DISTINCT FROM OLD.transition_key
       AND NEW.intent_hash IS NOT DISTINCT FROM OLD.intent_hash
       AND NEW.node_id IS NOT DISTINCT FROM OLD.node_id
       AND NEW.scope_instance_id IS NOT DISTINCT FROM OLD.scope_instance_id
       AND NEW.activation_id IS NOT DISTINCT FROM OLD.activation_id
       AND NEW.attempt_no IS NOT DISTINCT FROM OLD.attempt_no
       AND NEW.causation_event_id IS NOT DISTINCT FROM OLD.causation_event_id
       AND NEW.projection_version_after IS NOT DISTINCT FROM OLD.projection_version_after
       AND NEW.safe_payload IS NOT DISTINCT FROM OLD.safe_payload
       AND NEW.occurred_at IS NOT DISTINCT FROM OLD.occurred_at
       AND jsonb_typeof(NEW.projection_ledger_batch) = 'object'
       AND NEW.projection_ledger_batch @> '{"schema_version":1}'::jsonb
       AND jsonb_typeof(NEW.projection_ledger_batch->'subject_count') = 'number'
       AND NEW.projection_ledger_batch->>'subject_count' ~ '^(0|[1-9][0-9]*)$'
       AND jsonb_typeof(NEW.projection_ledger_batch->'manifest_hash') = 'string'
       AND NEW.projection_ledger_batch->>'manifest_hash' ~ '^sha256:[0-9a-f]{64}$'
       AND jsonb_typeof(NEW.projection_ledger_batch->'subjects') = 'array'
       AND (SELECT COUNT(*) FROM jsonb_object_keys(NEW.projection_ledger_batch)) = 4
       AND NEW.projection_ledger_batch ?&
           ARRAY['schema_version', 'subject_count', 'manifest_hash', 'subjects'] THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'execution event authority is immutable'
        USING ERRCODE = 'check_violation';
END;
$_$;

CREATE FUNCTION reject_public_event_receipt_mutation() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'public event receipt is immutable';
END;
$$;

CREATE FUNCTION reject_transition_receipt_rewrite() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    RAISE EXCEPTION 'transition receipt is immutable';
END;
$$;

CREATE FUNCTION synchronize_public_event_delivery_head() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    affected_rows INTEGER;
    boundary_locked BOOLEAN := FALSE;
    next_public_event_id TEXT;
    next_execution_event_id TEXT;
    next_execution_seq BIGINT;
    next_public_ordinal INTEGER;
    next_delivery_state TEXT;
    next_due_at TIMESTAMPTZ;
BEGIN
    IF current_setting('transaction_isolation')<>'read committed' THEN
        RAISE EXCEPTION 'public outbox delivery updates require READ COMMITTED'
            USING ERRCODE='check_violation';
    END IF;

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

    LOOP
        -- Seek strictly after the published key. The projection-order index
        -- starts at the current event rather than scanning Run history.
        SELECT next.public_event_id,next.execution_event_id,next.execution_seq,
               next.public_ordinal,next_outbox.publish_state,
               CASE next_outbox.publish_state
                   WHEN 'pending' THEN next_outbox.available_at
                   ELSE next_outbox.claim_expires_at
               END
        INTO next_public_event_id,next_execution_event_id,next_execution_seq,
             next_public_ordinal,next_delivery_state,next_due_at
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
        LIMIT 1;

        IF FOUND THEN
            UPDATE public_event_delivery_heads
            SET head_state='ready',
                public_event_id=next_public_event_id,
                execution_event_id=next_execution_event_id,
                execution_seq=next_execution_seq,
                public_ordinal=next_public_ordinal,
                delivery_state=next_delivery_state,
                due_at=next_due_at
            WHERE run_id=NEW.run_id
              AND head_state='ready'
              AND public_event_id=NEW.public_event_id;
            GET DIAGNOSTICS affected_rows=ROW_COUNT;
            IF affected_rows<>1 THEN
                RAISE EXCEPTION 'published public event does not own delivery head'
                    USING ERRCODE='check_violation';
            END IF;
            RETURN NEW;
        END IF;

        IF boundary_locked THEN
            EXIT;
        END IF;

        -- Close the empty-successor race with an event writer. Writers already
        -- serialize on workflow_runs and do not touch a ready head for
        -- monotonic inserts, so this adds no hot-path head contention or writer
        -- wait edge back to the head.
        PERFORM 1
        FROM workflow_runs
        WHERE run_id=NEW.run_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'public event Run authority is missing'
                USING ERRCODE='foreign_key_violation';
        END IF;
        boundary_locked := TRUE;
        -- Under READ COMMITTED the next loop iteration gets a snapshot after
        -- the event writer that held workflow_runs has committed.
    END LOOP;

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

SET LOCAL default_tablespace = '';
SET LOCAL default_table_access_method = heap;

CREATE TABLE agent_publication_heads (
    agent_id text NOT NULL,
    definition_id text NOT NULL,
    definition_revision_id text NOT NULL,
    deployment_revision_id text NOT NULL,
    publication_origin text NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT agent_publication_heads_publication_origin_check CHECK ((publication_origin = ANY (ARRAY['built_in'::text, 'graph'::text])))
);

CREATE TABLE artifact_gc_claims (
    transition_key text NOT NULL,
    run_id text NOT NULL,
    artifact_id text NOT NULL,
    claim_token text NOT NULL,
    deletion_fence text NOT NULL,
    claim_expires_at timestamp with time zone NOT NULL,
    CONSTRAINT artifact_gc_claims_claim_token_check CHECK (((length(claim_token) = 71) AND (claim_token ~~ 'sha256:%'::text))),
    CONSTRAINT artifact_gc_claims_deletion_fence_check CHECK (((length(deletion_fence) = 71) AND (deletion_fence ~~ 'sha256:%'::text)))
);

CREATE TABLE artifact_gc_sweeps (
    transition_key text NOT NULL,
    intent_hash text NOT NULL,
    claimed_by text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT artifact_gc_sweeps_intent_hash_check CHECK (((length(intent_hash) = 71) AND (intent_hash ~~ 'sha256:%'::text)))
);

CREATE TABLE artifact_retention_releases (
    run_id text NOT NULL,
    transition_key text NOT NULL,
    intent_hash text NOT NULL,
    event_id text NOT NULL,
    event_seq bigint NOT NULL,
    retain_until timestamp with time zone NOT NULL,
    artifact_count bigint NOT NULL,
    created_at timestamp with time zone NOT NULL,
    registration_kind text DEFAULT 'legacy'::text NOT NULL,
    CONSTRAINT artifact_retention_releases_artifact_count_check CHECK ((artifact_count >= 0)),
    CONSTRAINT artifact_retention_releases_event_seq_check CHECK ((event_seq >= 1)),
    CONSTRAINT artifact_retention_releases_intent_hash_check CHECK (((length(intent_hash) = 71) AND (intent_hash ~~ 'sha256:%'::text))),
    CONSTRAINT artifact_retention_releases_registration_kind_check CHECK ((registration_kind = ANY (ARRAY['legacy'::text, 'terminal_atomic'::text])))
);

CREATE TABLE artifact_store_authority (
    singleton boolean DEFAULT true NOT NULL,
    backend text NOT NULL,
    namespace text NOT NULL,
    store_id text NOT NULL,
    bound_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT artifact_store_authority_backend_check CHECK ((backend = 'shared_filesystem'::text)),
    CONSTRAINT artifact_store_authority_namespace_check CHECK ((namespace ~ '^[A-Za-z0-9._-]{1,128}$'::text)),
    CONSTRAINT artifact_store_authority_singleton_check CHECK (singleton),
    CONSTRAINT artifact_store_authority_store_id_check CHECK ((store_id ~ '^artifact_store_[0-9a-f]{32}$'::text))
);

CREATE TABLE artifacts (
    run_id text NOT NULL,
    artifact_id text NOT NULL,
    content_hash text NOT NULL,
    size_bytes bigint NOT NULL,
    media_type text,
    storage_uri text NOT NULL,
    artifact_state text NOT NULL,
    verified_at timestamp with time zone,
    referenced_at timestamp with time zone,
    retain_until timestamp with time zone,
    deletion_fence text,
    deletion_claim_token text,
    deletion_claimed_by text,
    deletion_claim_request_key text,
    deletion_claimed_at timestamp with time zone,
    deletion_claim_expires_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT artifacts_artifact_state_check CHECK ((artifact_state = ANY (ARRAY['staged'::text, 'verified'::text, 'referenced'::text, 'deleting'::text, 'deleted'::text]))),
    CONSTRAINT artifacts_check CHECK ((((artifact_state = 'staged'::text) AND (verified_at IS NULL) AND (referenced_at IS NULL) AND (deletion_fence IS NULL) AND (deletion_claim_token IS NULL) AND (deletion_claimed_by IS NULL) AND (deletion_claim_request_key IS NULL) AND (deletion_claimed_at IS NULL) AND (deletion_claim_expires_at IS NULL)) OR ((artifact_state = 'verified'::text) AND (verified_at IS NOT NULL) AND (referenced_at IS NULL) AND (deletion_fence IS NULL) AND (deletion_claim_token IS NULL) AND (deletion_claimed_by IS NULL) AND (deletion_claim_request_key IS NULL) AND (deletion_claimed_at IS NULL) AND (deletion_claim_expires_at IS NULL)) OR ((artifact_state = 'referenced'::text) AND (verified_at IS NOT NULL) AND (referenced_at IS NOT NULL) AND (deletion_fence IS NULL) AND (deletion_claim_token IS NULL) AND (deletion_claimed_by IS NULL) AND (deletion_claim_request_key IS NULL) AND (deletion_claimed_at IS NULL) AND (deletion_claim_expires_at IS NULL)) OR ((artifact_state = ANY (ARRAY['deleting'::text, 'deleted'::text])) AND (referenced_at IS NULL) AND (deletion_fence IS NOT NULL) AND (deletion_claim_token IS NOT NULL) AND (deletion_claimed_by IS NOT NULL) AND (deletion_claim_request_key IS NOT NULL) AND (deletion_claimed_at IS NOT NULL) AND (deletion_claim_expires_at IS NOT NULL)))),
    CONSTRAINT artifacts_content_hash_check CHECK (((length(content_hash) = 71) AND (content_hash ~~ 'sha256:%'::text))),
    CONSTRAINT artifacts_deletion_claim_token_check CHECK (((deletion_claim_token IS NULL) OR ((length(deletion_claim_token) = 71) AND (deletion_claim_token ~~ 'sha256:%'::text)))),
    CONSTRAINT artifacts_deletion_fence_check CHECK (((deletion_fence IS NULL) OR ((length(deletion_fence) = 71) AND (deletion_fence ~~ 'sha256:%'::text)))),
    CONSTRAINT artifacts_size_bytes_check CHECK ((size_bytes >= 0))
);

CREATE TABLE control_tokens (
    run_id text NOT NULL,
    token_id text NOT NULL,
    current_scope_instance_id text NOT NULL,
    current_port_id text NOT NULL,
    source_activation_id text NOT NULL,
    source_port_id text NOT NULL,
    emission_slot text NOT NULL,
    emitted_by_transition_key text NOT NULL,
    provenance_frames jsonb NOT NULL,
    branch_activation_id text,
    selected_branch_port_id text,
    fork_group_id text,
    fork_leg_id text,
    token_state text NOT NULL,
    consumed_by_activation_id text,
    consumed_by_transition_key text,
    consumed_at timestamp with time zone,
    revoked_by_transition_key text,
    revoked_at timestamp with time zone,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT control_tokens_check CHECK ((((branch_activation_id IS NULL) AND (selected_branch_port_id IS NULL)) OR ((branch_activation_id IS NOT NULL) AND (selected_branch_port_id IS NOT NULL)))),
    CONSTRAINT control_tokens_check1 CHECK ((((fork_group_id IS NULL) AND (fork_leg_id IS NULL)) OR ((fork_group_id IS NOT NULL) AND (fork_leg_id IS NOT NULL)))),
    CONSTRAINT control_tokens_check2 CHECK ((((token_state = 'available'::text) AND (consumed_by_activation_id IS NULL) AND (consumed_by_transition_key IS NULL) AND (consumed_at IS NULL) AND (revoked_by_transition_key IS NULL) AND (revoked_at IS NULL)) OR ((token_state = 'consumed'::text) AND (consumed_by_activation_id IS NOT NULL) AND (consumed_by_transition_key IS NOT NULL) AND (consumed_at IS NOT NULL) AND (revoked_by_transition_key IS NULL) AND (revoked_at IS NULL)) OR ((token_state = 'revoked'::text) AND (consumed_by_activation_id IS NULL) AND (consumed_by_transition_key IS NULL) AND (consumed_at IS NULL) AND (revoked_by_transition_key IS NOT NULL) AND (revoked_at IS NOT NULL)))),
    CONSTRAINT control_tokens_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT control_tokens_token_state_check CHECK ((token_state = ANY (ARRAY['available'::text, 'consumed'::text, 'revoked'::text])))
);

CREATE TABLE control_transition_results (
    run_id text NOT NULL,
    transition_key text NOT NULL,
    intent_hash text NOT NULL,
    primary_event_id text NOT NULL,
    result_json jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT control_transition_results_intent_hash_check CHECK (((length(intent_hash) = 71) AND (intent_hash ~~ 'sha256:%'::text)))
);

CREATE TABLE deployment_revisions (
    definition_id text NOT NULL,
    definition_revision_id text NOT NULL,
    deployment_revision_id text NOT NULL,
    plan_hash text NOT NULL,
    binding_hash text NOT NULL,
    resolved_bindings jsonb NOT NULL,
    worker_contracts jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT deployment_revisions_binding_hash_check CHECK (((length(binding_hash) = 71) AND (binding_hash ~~ 'sha256:%'::text))),
    CONSTRAINT deployment_revisions_plan_hash_check CHECK (((length(plan_hash) = 71) AND (plan_hash ~~ 'sha256:%'::text)))
);

CREATE TABLE execution_events (
    run_id text NOT NULL,
    seq bigint NOT NULL,
    event_id text NOT NULL,
    schema_version integer NOT NULL,
    kind text NOT NULL,
    transition_key text NOT NULL,
    intent_hash text NOT NULL,
    node_id text,
    scope_instance_id text,
    activation_id text,
    attempt_no integer,
    causation_event_id text,
    projection_version_after bigint NOT NULL,
    safe_payload jsonb NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    projection_ledger_batch jsonb,
    CONSTRAINT execution_events_attempt_no_check CHECK (((attempt_no IS NULL) OR (attempt_no >= 1))),
    CONSTRAINT execution_events_check CHECK (((attempt_no IS NULL) OR (activation_id IS NOT NULL))),
    CONSTRAINT execution_events_check1 CHECK (((activation_id IS NULL) OR ((node_id IS NOT NULL) AND (scope_instance_id IS NOT NULL)))),
    CONSTRAINT execution_events_check2 CHECK (((node_id IS NULL) OR (scope_instance_id IS NOT NULL))),
    CONSTRAINT execution_events_check3 CHECK (((causation_event_id IS NULL) OR (causation_event_id <> event_id))),
    CONSTRAINT execution_events_intent_hash_check CHECK (((length(intent_hash) = 71) AND (intent_hash ~~ 'sha256:%'::text))),
    CONSTRAINT execution_events_kind_supported CHECK ((kind = ANY (ARRAY['run.created'::text, 'run.lifecycle_changed'::text, 'run.admission_changed'::text, 'run.termination_claimed'::text, 'scope.created'::text, 'scope.draining'::text, 'scope.settled'::text, 'activation.created'::text, 'activation.ready'::text, 'activation.leased'::text, 'activation.running'::text, 'activation.retry_wait'::text, 'activation.waiting'::text, 'activation.terminating'::text, 'activation.succeeded'::text, 'activation.failed'::text, 'activation.cancelled'::text, 'activation.timed_out'::text, 'attempt.created'::text, 'attempt.leased'::text, 'attempt.running'::text, 'attempt.succeeded'::text, 'attempt.failed'::text, 'attempt.timed_out'::text, 'attempt.abandoned'::text, 'attempt.cancelled'::text, 'effect.evidence_recorded'::text, 'control_token.emitted'::text, 'control_token.consumed'::text, 'control_token.revoked'::text, 'fork.created'::text, 'join.arrived'::text, 'join.completed'::text, 'signal.received'::text, 'signal.late'::text, 'timer.scheduled'::text, 'timer.fired'::text, 'timer.late'::text, 'projection.mutated'::text]))),
    CONSTRAINT execution_events_projection_version_after_check CHECK ((projection_version_after >= 0)),
    CONSTRAINT execution_events_schema_version_check CHECK ((schema_version >= 1)),
    CONSTRAINT execution_events_schema_version_supported CHECK ((schema_version = 2)),
    CONSTRAINT execution_events_seq_check CHECK ((seq >= 1))
);

CREATE TABLE fork_groups (
    run_id text NOT NULL,
    fork_group_id text NOT NULL,
    fork_activation_id text NOT NULL,
    parent_scope_instance_id text NOT NULL,
    join_activation_id text,
    join_mode text,
    failure_leg_id text,
    failure_settlement_class text,
    expected_legs integer NOT NULL,
    group_state text NOT NULL,
    admitted_legs integer DEFAULT 0 NOT NULL,
    settled_legs integer DEFAULT 0 NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    settled_at timestamp with time zone,
    CONSTRAINT fork_groups_admitted_legs_check CHECK ((admitted_legs >= 0)),
    CONSTRAINT fork_groups_check CHECK (((failure_leg_id IS NULL) = (failure_settlement_class IS NULL))),
    CONSTRAINT fork_groups_check1 CHECK (((settled_legs <= admitted_legs) AND (admitted_legs <= expected_legs))),
    CONSTRAINT fork_groups_check2 CHECK ((((group_state = ANY (ARRAY['open'::text, 'settling'::text])) AND (settled_at IS NULL)) OR ((group_state = ANY (ARRAY['settled'::text, 'cancelled'::text])) AND (settled_legs = admitted_legs) AND (settled_at IS NOT NULL)))),
    CONSTRAINT fork_groups_expected_legs_check CHECK ((expected_legs > 0)),
    CONSTRAINT fork_groups_failure_settlement_class_check CHECK (((failure_settlement_class IS NULL) OR (failure_settlement_class = ANY (ARRAY['safe_failure'::text, 'infrastructure_failure'::text, 'panic'::text, 'cancelled'::text, 'timed_out'::text])))),
    CONSTRAINT fork_groups_group_state_check CHECK ((group_state = ANY (ARRAY['open'::text, 'settling'::text, 'settled'::text, 'cancelled'::text]))),
    CONSTRAINT fork_groups_join_mode_check CHECK (((join_mode IS NULL) OR (join_mode = ANY (ARRAY['all_success'::text, 'all_settled'::text])))),
    CONSTRAINT fork_groups_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT fork_groups_settled_legs_check CHECK ((settled_legs >= 0))
);

CREATE TABLE fork_legs (
    run_id text NOT NULL,
    fork_group_id text NOT NULL,
    leg_id text NOT NULL,
    declaration_index integer NOT NULL,
    scope_instance_id text NOT NULL,
    child_activation_id text NOT NULL,
    token_id text NOT NULL,
    is_required boolean NOT NULL,
    leg_state text NOT NULL,
    settlement_class text,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    settled_at timestamp with time zone,
    CONSTRAINT fork_legs_check CHECK ((((leg_state = 'admitted'::text) AND (settlement_class IS NULL) AND (settled_at IS NULL)) OR ((leg_state = ANY (ARRAY['settled'::text, 'cancelled'::text])) AND (settlement_class IS NOT NULL) AND (settled_at IS NOT NULL)))),
    CONSTRAINT fork_legs_declaration_index_check CHECK ((declaration_index >= 0)),
    CONSTRAINT fork_legs_leg_state_check CHECK ((leg_state = ANY (ARRAY['admitted'::text, 'settled'::text, 'cancelled'::text]))),
    CONSTRAINT fork_legs_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT fork_legs_settlement_class_check CHECK (((settlement_class IS NULL) OR (settlement_class = ANY (ARRAY['succeeded'::text, 'safe_failure'::text, 'infrastructure_failure'::text, 'panic'::text, 'cancelled'::text, 'timed_out'::text]))))
);

CREATE TABLE graph_view_documents (
    definition_id text NOT NULL,
    definition_revision_id text NOT NULL,
    graph_document_id text NOT NULL,
    view_version bigint NOT NULL,
    view_document jsonb NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT graph_view_documents_graph_document_id_check CHECK ((graph_document_id <> ''::text)),
    CONSTRAINT graph_view_documents_view_version_check CHECK ((view_version >= 1))
);

CREATE TABLE human_work_items (
    work_item_id text NOT NULL,
    run_id text NOT NULL,
    wait_id text NOT NULL,
    activation_id text NOT NULL,
    signal_id text NOT NULL,
    signal_name text NOT NULL,
    request_value jsonb NOT NULL,
    response_type jsonb NOT NULL,
    assignees jsonb DEFAULT '[]'::jsonb NOT NULL,
    candidate_groups jsonb DEFAULT '[]'::jsonb NOT NULL,
    claim_lease_ms bigint NOT NULL,
    work_state text NOT NULL,
    claim_fence bigint DEFAULT 0 NOT NULL,
    claimed_by text,
    claim_request_id text,
    claim_expires_at timestamp with time zone,
    completion_request_id text,
    completion_payload jsonb,
    completion_payload_hash text,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    completed_at timestamp with time zone,
    closed_at timestamp with time zone,
    CONSTRAINT human_work_items_assignees_check CHECK ((jsonb_typeof(assignees) = 'array'::text)),
    CONSTRAINT human_work_items_candidate_groups_check CHECK ((jsonb_typeof(candidate_groups) = 'array'::text)),
    CONSTRAINT human_work_items_check CHECK ((((work_state = 'open'::text) AND (claimed_by IS NULL) AND (claim_expires_at IS NULL)) OR ((work_state = 'claimed'::text) AND (claimed_by IS NOT NULL) AND (claim_expires_at IS NOT NULL)) OR (work_state = ANY (ARRAY['completed'::text, 'cancelled'::text, 'expired'::text])))),
    CONSTRAINT human_work_items_check1 CHECK ((((completion_request_id IS NULL) AND (completion_payload IS NULL) AND (completion_payload_hash IS NULL)) OR ((completion_request_id IS NOT NULL) AND (completion_payload IS NOT NULL) AND (completion_payload_hash IS NOT NULL)))),
    CONSTRAINT human_work_items_claim_fence_check CHECK ((claim_fence >= 0)),
    CONSTRAINT human_work_items_claim_lease_ms_check CHECK (((claim_lease_ms >= 1) AND (claim_lease_ms <= '2592000000'::bigint))),
    CONSTRAINT human_work_items_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT human_work_items_work_state_check CHECK ((work_state = ANY (ARRAY['open'::text, 'claimed'::text, 'completed'::text, 'cancelled'::text, 'expired'::text])))
);

CREATE TABLE join_arrivals (
    run_id text NOT NULL,
    join_activation_id text NOT NULL,
    fork_group_id text NOT NULL,
    leg_id text NOT NULL,
    token_id text NOT NULL,
    arrival_transition_key text NOT NULL,
    arrival_event_id text NOT NULL,
    settlement_class text NOT NULL,
    value_payload_id text,
    value_artifact_id text,
    value_hash text,
    projection_version bigint DEFAULT 0 NOT NULL,
    arrived_at timestamp with time zone NOT NULL,
    CONSTRAINT join_arrivals_check CHECK ((((value_hash IS NULL) AND (value_payload_id IS NULL) AND (value_artifact_id IS NULL)) OR ((value_hash IS NOT NULL) AND ((((value_payload_id IS NOT NULL))::integer + ((value_artifact_id IS NOT NULL))::integer) = 1)))),
    CONSTRAINT join_arrivals_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT join_arrivals_settlement_class_check CHECK ((settlement_class = ANY (ARRAY['succeeded'::text, 'safe_failure'::text, 'infrastructure_failure'::text, 'panic'::text, 'cancelled'::text, 'timed_out'::text]))),
    CONSTRAINT join_arrivals_value_hash_check CHECK (((value_hash IS NULL) OR ((length(value_hash) = 71) AND (value_hash ~~ 'sha256:%'::text))))
);

CREATE TABLE model_call_usage (
    run_id text NOT NULL,
    activation_id text NOT NULL,
    attempt_no integer NOT NULL,
    model_call_no integer NOT NULL,
    task_id text NOT NULL,
    lease_epoch bigint NOT NULL,
    fencing_token text NOT NULL,
    call_status text NOT NULL,
    finish_reason text,
    usage jsonb,
    usage_complete boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT model_call_usage_attempt_no_check CHECK ((attempt_no >= 1)),
    CONSTRAINT model_call_usage_call_status_check CHECK ((call_status = ANY (ARRAY['started'::text, 'completed'::text, 'failed'::text, 'abandoned'::text]))),
    CONSTRAINT model_call_usage_fencing_token_check CHECK ((fencing_token <> ''::text)),
    CONSTRAINT model_call_usage_finish_reason_check CHECK (((finish_reason IS NULL) OR (finish_reason = ANY (ARRAY['stop'::text, 'tool_calls'::text, 'length'::text, 'content_filter'::text, 'invalid'::text])))),
    CONSTRAINT model_call_usage_lease_epoch_check CHECK ((lease_epoch >= 1)),
    CONSTRAINT model_call_usage_model_call_no_check CHECK ((model_call_no >= 1)),
    CONSTRAINT model_call_usage_task_id_check CHECK ((task_id <> ''::text))
);

CREATE TABLE model_tool_call_batches (
    run_id text NOT NULL,
    activation_id text NOT NULL,
    attempt_no integer NOT NULL,
    model_call_no integer NOT NULL,
    batch_status text NOT NULL,
    execution_status text DEFAULT 'checkpointed'::text NOT NULL,
    assistant_content text,
    parent_task_id text,
    parent_lease_epoch bigint,
    parent_fencing_token text,
    parent_claimed_by text,
    parent_claim_token text,
    parent_claim_expires_at timestamp with time zone,
    parent_task_projection_version bigint,
    continuation_status text DEFAULT 'checkpointed'::text NOT NULL,
    activated_at timestamp with time zone,
    completed_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    parent_operation_deadline timestamp with time zone,
    CONSTRAINT model_tool_call_batches_assistant_content_check CHECK (((assistant_content IS NULL) OR (octet_length(assistant_content) <= 1048576))),
    CONSTRAINT model_tool_call_batches_attempt_no_check CHECK ((attempt_no >= 1)),
    CONSTRAINT model_tool_call_batches_batch_status_check CHECK ((batch_status = 'checkpointed'::text)),
    CONSTRAINT model_tool_call_batches_check CHECK ((((execution_status = 'checkpointed'::text) AND (activated_at IS NULL)) OR ((execution_status <> 'checkpointed'::text) AND (activated_at IS NOT NULL)))),
    CONSTRAINT model_tool_call_batches_check1 CHECK (((execution_status = ANY (ARRAY['succeeded'::text, 'failed'::text, 'cancelled'::text])) = (completed_at IS NOT NULL))),
    CONSTRAINT model_tool_call_batches_check2 CHECK ((((continuation_status = 'checkpointed'::text) AND (parent_task_id IS NULL) AND (parent_lease_epoch IS NULL) AND (parent_fencing_token IS NULL) AND (parent_claimed_by IS NULL) AND (parent_claim_token IS NULL) AND (parent_claim_expires_at IS NULL) AND (parent_task_projection_version IS NULL)) OR ((continuation_status <> 'checkpointed'::text) AND (parent_task_id IS NOT NULL) AND (parent_lease_epoch IS NOT NULL) AND (parent_fencing_token IS NOT NULL) AND (parent_claimed_by IS NOT NULL) AND (parent_claim_token IS NOT NULL) AND (parent_claim_expires_at IS NOT NULL) AND (parent_task_projection_version IS NOT NULL)))),
    CONSTRAINT model_tool_call_batches_continuation_status_check CHECK ((continuation_status = ANY (ARRAY['checkpointed'::text, 'waiting_tools'::text, 'ready_continue'::text, 'ready_failed'::text, 'ready_cancelled'::text]))),
    CONSTRAINT model_tool_call_batches_execution_status_check CHECK ((execution_status = ANY (ARRAY['checkpointed'::text, 'active'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text]))),
    CONSTRAINT model_tool_call_batches_model_call_no_check CHECK ((model_call_no >= 1)),
    CONSTRAINT model_tool_call_batches_parent_lease_epoch_check CHECK (((parent_lease_epoch IS NULL) OR (parent_lease_epoch >= 1))),
    CONSTRAINT model_tool_call_batches_parent_task_projection_version_check CHECK (((parent_task_projection_version IS NULL) OR (parent_task_projection_version >= 1)))
);

CREATE TABLE model_tool_calls (
    run_id text NOT NULL,
    activation_id text NOT NULL,
    attempt_no integer NOT NULL,
    model_call_no integer NOT NULL,
    call_index integer NOT NULL,
    call_id text NOT NULL,
    tool_name text NOT NULL,
    arguments jsonb NOT NULL,
    tool_task_id text,
    effect_id text,
    action_id text,
    action_version text,
    action_descriptor_hash text,
    action_input_schema jsonb,
    action_output_schema jsonb,
    action_effect_policy jsonb,
    action_deployment_binding jsonb,
    effective_public_policy jsonb,
    response_item_id text,
    response_output_index integer,
    effect_idempotency text,
    cancellation text,
    max_attempts integer,
    initial_backoff_ms bigint,
    max_backoff_ms bigint,
    timeout_ms bigint,
    call_status text NOT NULL,
    tool_attempt_no integer,
    lease_epoch bigint,
    fencing_token text,
    effect_evidence text,
    available_at timestamp with time zone,
    claim_owner text,
    claim_token text,
    claim_expires_at timestamp with time zone,
    projection_version bigint DEFAULT 0 NOT NULL,
    lease_loss_count integer DEFAULT 0 NOT NULL,
    last_lease_loss_at timestamp with time zone,
    last_lease_loss_evidence text,
    result_json jsonb,
    failure_class text,
    failure_code text,
    failure_retryable boolean,
    last_commit_claim_token text,
    last_outcome_hash text,
    last_outcome_disposition text,
    last_outcome_attempt_no integer,
    last_outcome_lease_epoch bigint,
    last_outcome_fencing_token text,
    last_outcome_available_at timestamp with time zone,
    last_effect_evidence text,
    last_failure_class text,
    last_failure_code text,
    last_failure_retryable boolean,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    response_seal_index bigint,
    CONSTRAINT model_tool_calls_action_deployment_binding_check CHECK (((action_deployment_binding IS NULL) OR (jsonb_typeof(action_deployment_binding) = 'object'::text))),
    CONSTRAINT model_tool_calls_action_descriptor_hash_check CHECK (((action_descriptor_hash IS NULL) OR (action_descriptor_hash ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT model_tool_calls_action_effect_policy_check CHECK (((action_effect_policy IS NULL) OR (jsonb_typeof(action_effect_policy) = 'object'::text))),
    CONSTRAINT model_tool_calls_action_id_check CHECK (((action_id IS NULL) OR ((action_id <> ''::text) AND (octet_length(action_id) <= 128)))),
    CONSTRAINT model_tool_calls_action_version_check CHECK (((action_version IS NULL) OR ((action_version <> ''::text) AND (octet_length(action_version) <= 64)))),
    CONSTRAINT model_tool_calls_arguments_check CHECK (((jsonb_typeof(arguments) = 'object'::text) AND (octet_length((arguments)::text) <= 262144))),
    CONSTRAINT model_tool_calls_attempt_no_check CHECK ((attempt_no >= 1)),
    CONSTRAINT model_tool_calls_call_id_check CHECK (((call_id <> ''::text) AND (octet_length(call_id) <= 256))),
    CONSTRAINT model_tool_calls_call_index_check CHECK ((call_index >= 0)),
    CONSTRAINT model_tool_calls_call_status_check CHECK ((call_status = ANY (ARRAY['pending'::text, 'claimed'::text, 'running'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text]))),
    CONSTRAINT model_tool_calls_cancellation_check CHECK (((cancellation IS NULL) OR (cancellation = ANY (ARRAY['cooperative'::text, 'lease_only'::text])))),
    CONSTRAINT model_tool_calls_check CHECK ((((tool_task_id IS NULL) AND (effect_id IS NULL) AND (action_id IS NULL) AND (action_version IS NULL) AND (action_descriptor_hash IS NULL) AND (action_input_schema IS NULL) AND (action_output_schema IS NULL) AND (action_effect_policy IS NULL) AND (action_deployment_binding IS NULL) AND (effective_public_policy IS NULL) AND (response_item_id IS NULL) AND (response_output_index IS NULL) AND (effect_idempotency IS NULL) AND (cancellation IS NULL) AND (max_attempts IS NULL) AND (initial_backoff_ms IS NULL) AND (max_backoff_ms IS NULL) AND (timeout_ms IS NULL) AND (tool_attempt_no IS NULL) AND (lease_epoch IS NULL) AND (fencing_token IS NULL) AND (effect_evidence IS NULL) AND (available_at IS NULL) AND (projection_version = 0)) OR ((tool_task_id IS NOT NULL) AND (effect_id IS NOT NULL) AND (action_id IS NOT NULL) AND (action_version IS NOT NULL) AND (action_descriptor_hash IS NOT NULL) AND (action_input_schema IS NOT NULL) AND (action_output_schema IS NOT NULL) AND (action_effect_policy IS NOT NULL) AND (action_deployment_binding IS NOT NULL) AND (effective_public_policy IS NOT NULL) AND (effect_idempotency IS NOT NULL) AND (cancellation IS NOT NULL) AND (max_attempts IS NOT NULL) AND (initial_backoff_ms IS NOT NULL) AND (max_backoff_ms IS NOT NULL) AND (timeout_ms IS NOT NULL) AND (tool_attempt_no IS NOT NULL) AND (lease_epoch IS NOT NULL) AND (fencing_token IS NOT NULL) AND (effect_evidence IS NOT NULL) AND (projection_version >= 1)))),
    CONSTRAINT model_tool_calls_check1 CHECK ((((claim_owner IS NULL) AND (claim_token IS NULL) AND (claim_expires_at IS NULL)) OR ((claim_owner IS NOT NULL) AND (claim_token IS NOT NULL) AND (claim_expires_at IS NOT NULL)))),
    CONSTRAINT model_tool_calls_check2 CHECK (((response_item_id IS NULL) = (response_output_index IS NULL))),
    CONSTRAINT model_tool_calls_check3 CHECK ((((lease_loss_count = 0) AND (last_lease_loss_at IS NULL) AND (last_lease_loss_evidence IS NULL)) OR ((lease_loss_count > 0) AND (last_lease_loss_at IS NOT NULL) AND (last_lease_loss_evidence IS NOT NULL)))),
    CONSTRAINT model_tool_calls_check4 CHECK ((((last_commit_claim_token IS NULL) AND (last_outcome_hash IS NULL) AND (last_outcome_disposition IS NULL) AND (last_outcome_attempt_no IS NULL) AND (last_outcome_lease_epoch IS NULL) AND (last_outcome_fencing_token IS NULL) AND (last_outcome_available_at IS NULL) AND (last_effect_evidence IS NULL) AND (last_failure_class IS NULL) AND (last_failure_code IS NULL) AND (last_failure_retryable IS NULL)) OR ((last_commit_claim_token IS NOT NULL) AND (last_outcome_hash IS NOT NULL) AND (last_outcome_disposition IS NOT NULL) AND (last_outcome_attempt_no IS NOT NULL) AND (last_outcome_lease_epoch IS NOT NULL) AND (last_outcome_fencing_token IS NOT NULL) AND (last_effect_evidence IS NOT NULL)))),
    CONSTRAINT model_tool_calls_check5 CHECK ((((call_status = 'pending'::text) AND (claim_owner IS NULL) AND (result_json IS NULL) AND (failure_class IS NULL) AND (failure_code IS NULL) AND (failure_retryable IS NULL) AND (started_at IS NULL) AND (completed_at IS NULL) AND ((tool_task_id IS NULL) OR ((effect_evidence = 'not_started'::text) AND (available_at IS NOT NULL)))) OR ((call_status = 'claimed'::text) AND (claim_owner IS NOT NULL) AND (result_json IS NULL) AND (failure_code IS NULL) AND (effect_evidence = 'not_started'::text) AND (available_at IS NULL) AND (started_at IS NULL) AND (completed_at IS NULL)) OR ((call_status = 'running'::text) AND (claim_owner IS NOT NULL) AND (result_json IS NULL) AND (failure_code IS NULL) AND (effect_evidence = 'started'::text) AND (available_at IS NULL) AND (started_at IS NOT NULL) AND (completed_at IS NULL)) OR ((call_status = 'succeeded'::text) AND (claim_owner IS NOT NULL) AND (result_json IS NOT NULL) AND (failure_code IS NULL) AND (effect_evidence = 'committed'::text) AND (available_at IS NULL) AND (started_at IS NOT NULL) AND (completed_at IS NOT NULL)) OR ((call_status = ANY (ARRAY['failed'::text, 'cancelled'::text])) AND (result_json IS NULL) AND (failure_class IS NOT NULL) AND (failure_code IS NOT NULL) AND (failure_retryable IS NOT NULL) AND (effect_evidence = ANY (ARRAY['not_started'::text, 'started'::text, 'unknown'::text])) AND (available_at IS NULL) AND (completed_at IS NOT NULL)))),
    CONSTRAINT model_tool_calls_effect_evidence_check CHECK (((effect_evidence IS NULL) OR (effect_evidence = ANY (ARRAY['not_started'::text, 'started'::text, 'committed'::text, 'unknown'::text])))),
    CONSTRAINT model_tool_calls_effect_id_check CHECK (((effect_id IS NULL) OR (effect_id ~ '^effect_[0-9a-f]{64}$'::text))),
    CONSTRAINT model_tool_calls_effect_idempotency_check CHECK (((effect_idempotency IS NULL) OR (effect_idempotency = ANY (ARRAY['idempotent'::text, 'non_idempotent'::text])))),
    CONSTRAINT model_tool_calls_effective_public_policy_check CHECK (((effective_public_policy IS NULL) OR (jsonb_typeof(effective_public_policy) = 'object'::text))),
    CONSTRAINT model_tool_calls_failure_class_check CHECK (((failure_class IS NULL) OR (failure_class = ANY (ARRAY['safe'::text, 'infrastructure'::text, 'effect_outcome_unknown'::text])))),
    CONSTRAINT model_tool_calls_failure_code_check CHECK (((failure_code IS NULL) OR ((failure_code <> ''::text) AND (octet_length(failure_code) <= 128)))),
    CONSTRAINT model_tool_calls_fencing_token_check CHECK (((fencing_token IS NULL) OR (fencing_token <> ''::text))),
    CONSTRAINT model_tool_calls_initial_backoff_ms_check CHECK (((initial_backoff_ms IS NULL) OR (initial_backoff_ms >= 0))),
    CONSTRAINT model_tool_calls_last_effect_evidence_check CHECK (((last_effect_evidence IS NULL) OR (last_effect_evidence = ANY (ARRAY['not_started'::text, 'started'::text, 'committed'::text, 'unknown'::text])))),
    CONSTRAINT model_tool_calls_last_failure_class_check CHECK (((last_failure_class IS NULL) OR (last_failure_class = ANY (ARRAY['safe'::text, 'infrastructure'::text, 'effect_outcome_unknown'::text])))),
    CONSTRAINT model_tool_calls_last_failure_code_check CHECK (((last_failure_code IS NULL) OR ((last_failure_code <> ''::text) AND (octet_length(last_failure_code) <= 128)))),
    CONSTRAINT model_tool_calls_last_lease_loss_evidence_check CHECK (((last_lease_loss_evidence IS NULL) OR (last_lease_loss_evidence = ANY (ARRAY['not_started'::text, 'unknown'::text])))),
    CONSTRAINT model_tool_calls_last_outcome_attempt_no_check CHECK (((last_outcome_attempt_no IS NULL) OR (last_outcome_attempt_no >= 1))),
    CONSTRAINT model_tool_calls_last_outcome_disposition_check CHECK (((last_outcome_disposition IS NULL) OR (last_outcome_disposition = ANY (ARRAY['succeeded'::text, 'retry_scheduled'::text, 'failed'::text, 'cancelled'::text])))),
    CONSTRAINT model_tool_calls_last_outcome_hash_check CHECK (((last_outcome_hash IS NULL) OR (last_outcome_hash ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT model_tool_calls_last_outcome_lease_epoch_check CHECK (((last_outcome_lease_epoch IS NULL) OR (last_outcome_lease_epoch >= 1))),
    CONSTRAINT model_tool_calls_lease_epoch_check CHECK (((lease_epoch IS NULL) OR (lease_epoch >= 1))),
    CONSTRAINT model_tool_calls_lease_loss_count_check CHECK ((lease_loss_count >= 0)),
    CONSTRAINT model_tool_calls_max_attempts_check CHECK (((max_attempts IS NULL) OR (max_attempts >= 1))),
    CONSTRAINT model_tool_calls_max_backoff_ms_check CHECK (((max_backoff_ms IS NULL) OR (max_backoff_ms >= 0))),
    CONSTRAINT model_tool_calls_model_call_no_check CHECK ((model_call_no >= 1)),
    CONSTRAINT model_tool_calls_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT model_tool_calls_response_item_id_check CHECK (((response_item_id IS NULL) OR ((response_item_id <> ''::text) AND (octet_length(response_item_id) <= 256)))),
    CONSTRAINT model_tool_calls_response_output_index_check CHECK (((response_output_index IS NULL) OR (response_output_index >= 0))),
    CONSTRAINT model_tool_calls_response_seal_index_check CHECK (((response_seal_index IS NULL) OR (response_seal_index >= 3))),
    CONSTRAINT model_tool_calls_result_json_check CHECK (((result_json IS NULL) OR (octet_length((result_json)::text) <= 1048576))),
    CONSTRAINT model_tool_calls_timeout_ms_check CHECK (((timeout_ms IS NULL) OR (timeout_ms >= 1))),
    CONSTRAINT model_tool_calls_tool_attempt_no_check CHECK (((tool_attempt_no IS NULL) OR (tool_attempt_no >= 1))),
    CONSTRAINT model_tool_calls_tool_name_check CHECK (((tool_name <> ''::text) AND (octet_length(tool_name) <= 128))),
    CONSTRAINT model_tool_calls_tool_task_id_check CHECK (((tool_task_id IS NULL) OR (tool_task_id ~ '^task_[0-9a-f]{64}$'::text)))
);

CREATE TABLE node_activations (
    run_id text NOT NULL,
    activation_id text NOT NULL,
    scope_instance_id text NOT NULL,
    node_id text NOT NULL,
    stable_activation_key text NOT NULL,
    execution_kind text NOT NULL,
    lifecycle text NOT NULL,
    effect_id text NOT NULL,
    effect_idempotency text NOT NULL,
    effect_evidence text NOT NULL,
    last_attempt_no integer,
    last_lease_epoch bigint,
    current_attempt_no integer,
    current_lease_epoch bigint,
    current_fencing_token text,
    retry_budget_remaining integer NOT NULL,
    pending_retry_timer_id text,
    wait_registration_transition_key text,
    termination_intent_reason text,
    termination_intent_transition_key text,
    termination_intent_at timestamp with time zone,
    output_payload_id text,
    output_artifact_id text,
    output_value_hash text,
    winning_attempt_no integer,
    reused_from_run_id text,
    reused_from_activation_id text,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    terminal_at timestamp with time zone,
    CONSTRAINT node_activations_check CHECK ((((last_attempt_no IS NULL) AND (last_lease_epoch IS NULL)) OR ((last_attempt_no IS NOT NULL) AND (last_lease_epoch IS NOT NULL) AND (last_lease_epoch >= last_attempt_no)))),
    CONSTRAINT node_activations_check1 CHECK ((((lifecycle = ANY (ARRAY['leased'::text, 'running'::text])) AND (execution_kind = 'worker'::text) AND (current_attempt_no = last_attempt_no) AND (current_lease_epoch = last_lease_epoch) AND (current_fencing_token IS NOT NULL)) OR ((lifecycle <> ALL (ARRAY['leased'::text, 'running'::text])) AND (current_attempt_no IS NULL) AND (current_lease_epoch IS NULL) AND (current_fencing_token IS NULL)))),
    CONSTRAINT node_activations_check2 CHECK (((execution_kind = 'worker'::text) OR ((last_attempt_no IS NULL) AND (last_lease_epoch IS NULL) AND (current_attempt_no IS NULL) AND (current_lease_epoch IS NULL) AND (current_fencing_token IS NULL)))),
    CONSTRAINT node_activations_check3 CHECK (((lifecycle <> 'waiting'::text) OR (execution_kind = 'durable_wait'::text))),
    CONSTRAINT node_activations_check4 CHECK (((lifecycle <> 'waiting'::text) OR (wait_registration_transition_key IS NOT NULL))),
    CONSTRAINT node_activations_check5 CHECK ((((lifecycle = 'retry_wait'::text) AND (execution_kind = 'worker'::text) AND (pending_retry_timer_id IS NOT NULL)) OR ((lifecycle <> 'retry_wait'::text) AND (pending_retry_timer_id IS NULL)))),
    CONSTRAINT node_activations_check6 CHECK ((((termination_intent_reason IS NULL) AND (termination_intent_transition_key IS NULL) AND (termination_intent_at IS NULL)) OR ((termination_intent_reason IS NOT NULL) AND (termination_intent_transition_key IS NOT NULL) AND (termination_intent_at IS NOT NULL)))),
    CONSTRAINT node_activations_check7 CHECK ((((lifecycle = ANY (ARRAY['created'::text, 'ready'::text, 'leased'::text, 'running'::text, 'retry_wait'::text, 'waiting'::text])) AND (termination_intent_reason IS NULL) AND (terminal_at IS NULL) AND (output_payload_id IS NULL) AND (output_artifact_id IS NULL) AND (output_value_hash IS NULL)) OR ((lifecycle = 'terminating'::text) AND (termination_intent_reason IS NOT NULL) AND (terminal_at IS NULL) AND (output_payload_id IS NULL) AND (output_artifact_id IS NULL) AND (output_value_hash IS NULL)) OR ((lifecycle = 'succeeded'::text) AND (termination_intent_reason IS NULL) AND (terminal_at IS NOT NULL) AND (output_value_hash IS NOT NULL) AND ((((output_payload_id IS NOT NULL))::integer + ((output_artifact_id IS NOT NULL))::integer) = 1) AND (((execution_kind = 'worker'::text) AND (winning_attempt_no IS NOT NULL)) OR ((execution_kind <> 'worker'::text) AND (winning_attempt_no IS NULL)) OR ((reused_from_activation_id IS NOT NULL) AND (winning_attempt_no IS NULL)))) OR ((lifecycle = ANY (ARRAY['failed'::text, 'cancelled'::text, 'timed_out'::text])) AND (termination_intent_reason IS NOT NULL) AND (terminal_at IS NOT NULL) AND (output_payload_id IS NULL) AND (output_artifact_id IS NULL) AND (output_value_hash IS NULL)))),
    CONSTRAINT node_activations_check8 CHECK ((((reused_from_run_id IS NULL) AND (reused_from_activation_id IS NULL)) OR ((reused_from_run_id IS NOT NULL) AND (reused_from_activation_id IS NOT NULL) AND (lifecycle = 'succeeded'::text) AND (last_attempt_no IS NULL)))),
    CONSTRAINT node_activations_current_attempt_no_check CHECK (((current_attempt_no IS NULL) OR (current_attempt_no >= 1))),
    CONSTRAINT node_activations_current_lease_epoch_check CHECK (((current_lease_epoch IS NULL) OR (current_lease_epoch >= 1))),
    CONSTRAINT node_activations_effect_evidence_check CHECK ((effect_evidence = ANY (ARRAY['not_started'::text, 'started'::text, 'committed'::text, 'unknown'::text]))),
    CONSTRAINT node_activations_effect_idempotency_check CHECK ((effect_idempotency = ANY (ARRAY['idempotent'::text, 'non_idempotent'::text]))),
    CONSTRAINT node_activations_execution_kind_check CHECK ((execution_kind = ANY (ARRAY['worker'::text, 'scheduler_native'::text, 'durable_wait'::text]))),
    CONSTRAINT node_activations_last_attempt_no_check CHECK (((last_attempt_no IS NULL) OR (last_attempt_no >= 1))),
    CONSTRAINT node_activations_last_lease_epoch_check CHECK (((last_lease_epoch IS NULL) OR (last_lease_epoch >= 1))),
    CONSTRAINT node_activations_lifecycle_check CHECK ((lifecycle = ANY (ARRAY['created'::text, 'ready'::text, 'leased'::text, 'running'::text, 'retry_wait'::text, 'waiting'::text, 'terminating'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text, 'timed_out'::text]))),
    CONSTRAINT node_activations_output_value_hash_check CHECK (((output_value_hash IS NULL) OR ((length(output_value_hash) = 71) AND (output_value_hash ~~ 'sha256:%'::text)))),
    CONSTRAINT node_activations_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT node_activations_retry_budget_remaining_check CHECK ((retry_budget_remaining >= 0)),
    CONSTRAINT node_activations_winning_attempt_no_check CHECK (((winning_attempt_no IS NULL) OR (winning_attempt_no >= 1)))
);

CREATE TABLE node_attempts (
    run_id text NOT NULL,
    activation_id text NOT NULL,
    attempt_no integer NOT NULL,
    lease_epoch bigint NOT NULL,
    fencing_token text NOT NULL,
    effect_id text NOT NULL,
    lifecycle text NOT NULL,
    effect_evidence text NOT NULL,
    worker_id text,
    lease_expires_at timestamp with time zone,
    heartbeat_at timestamp with time zone,
    output_payload_id text,
    output_artifact_id text,
    output_value_hash text,
    failure_code text,
    completion_transition_key text,
    terminal_event_id text,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    started_at timestamp with time zone,
    terminal_at timestamp with time zone,
    CONSTRAINT node_attempts_attempt_no_check CHECK ((attempt_no >= 1)),
    CONSTRAINT node_attempts_check CHECK ((lease_epoch >= attempt_no)),
    CONSTRAINT node_attempts_check1 CHECK (((lifecycle = 'created'::text) OR ((worker_id IS NOT NULL) AND (lease_expires_at IS NOT NULL) AND (heartbeat_at IS NOT NULL)))),
    CONSTRAINT node_attempts_check2 CHECK ((((lifecycle = ANY (ARRAY['created'::text, 'leased'::text, 'running'::text])) AND (terminal_at IS NULL) AND (terminal_event_id IS NULL) AND (completion_transition_key IS NULL) AND (output_payload_id IS NULL) AND (output_artifact_id IS NULL) AND (output_value_hash IS NULL)) OR ((lifecycle = 'succeeded'::text) AND (terminal_at IS NOT NULL) AND (terminal_event_id IS NOT NULL) AND (completion_transition_key IS NOT NULL) AND (output_value_hash IS NOT NULL) AND ((((output_payload_id IS NOT NULL))::integer + ((output_artifact_id IS NOT NULL))::integer) = 1) AND (failure_code IS NULL)) OR ((lifecycle = ANY (ARRAY['failed'::text, 'timed_out'::text, 'abandoned'::text, 'cancelled'::text])) AND (terminal_at IS NOT NULL) AND (terminal_event_id IS NOT NULL) AND (completion_transition_key IS NOT NULL) AND (output_payload_id IS NULL) AND (output_artifact_id IS NULL) AND (output_value_hash IS NULL)))),
    CONSTRAINT node_attempts_check3 CHECK (((lifecycle <> 'abandoned'::text) OR (effect_evidence <> 'started'::text))),
    CONSTRAINT node_attempts_effect_evidence_check CHECK ((effect_evidence = ANY (ARRAY['not_started'::text, 'started'::text, 'committed'::text, 'unknown'::text]))),
    CONSTRAINT node_attempts_lifecycle_check CHECK ((lifecycle = ANY (ARRAY['created'::text, 'leased'::text, 'running'::text, 'succeeded'::text, 'failed'::text, 'timed_out'::text, 'abandoned'::text, 'cancelled'::text]))),
    CONSTRAINT node_attempts_output_value_hash_check CHECK (((output_value_hash IS NULL) OR ((length(output_value_hash) = 71) AND (output_value_hash ~~ 'sha256:%'::text)))),
    CONSTRAINT node_attempts_projection_version_check CHECK ((projection_version >= 0))
);

CREATE TABLE payloads (
    run_id text NOT NULL,
    payload_id text NOT NULL,
    content_hash text NOT NULL,
    canonical_bytes bigint NOT NULL,
    encoding text NOT NULL,
    inline_value jsonb,
    binary_value bytea,
    created_at timestamp with time zone NOT NULL,
    retain_until timestamp with time zone,
    CONSTRAINT payloads_canonical_bytes_check CHECK ((canonical_bytes >= 0)),
    CONSTRAINT payloads_check CHECK ((((encoding = 'json_jcs'::text) AND (inline_value IS NOT NULL) AND (binary_value IS NULL)) OR ((encoding = 'binary'::text) AND (inline_value IS NULL) AND (binary_value IS NOT NULL)))),
    CONSTRAINT payloads_content_hash_check CHECK (((length(content_hash) = 71) AND (content_hash ~~ 'sha256:%'::text))),
    CONSTRAINT payloads_encoding_check CHECK ((encoding = ANY (ARRAY['json_jcs'::text, 'binary'::text])))
);

CREATE TABLE projection_checkpoint_batches (
    run_id text NOT NULL,
    event_id text NOT NULL,
    checkpoint_schema_version integer NOT NULL,
    subject_count bigint NOT NULL,
    manifest_hash text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT projection_checkpoint_batches_checkpoint_schema_version_check CHECK ((checkpoint_schema_version >= 1)),
    CONSTRAINT projection_checkpoint_batches_manifest_hash_check CHECK (((length(manifest_hash) = 71) AND (manifest_hash ~~ 'sha256:%'::text))),
    CONSTRAINT projection_checkpoint_batches_subject_count_check CHECK ((subject_count >= 0))
);

CREATE TABLE projection_checkpoints (
    run_id text NOT NULL,
    event_id text NOT NULL,
    subject_kind text NOT NULL,
    subject_id text NOT NULL,
    checkpoint_schema_version integer NOT NULL,
    subject_projection_version bigint NOT NULL,
    projection_hash text NOT NULL,
    canonical_projection jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT projection_checkpoints_checkpoint_schema_version_check CHECK ((checkpoint_schema_version >= 1)),
    CONSTRAINT projection_checkpoints_projection_hash_check CHECK (((length(projection_hash) = 71) AND (projection_hash ~~ 'sha256:%'::text))),
    CONSTRAINT projection_checkpoints_subject_kind_check CHECK ((subject_kind = ANY (ARRAY['run'::text, 'scope'::text, 'activation'::text, 'attempt'::text, 'timer'::text, 'control'::text, 'fork'::text, 'join'::text, 'scheduler'::text, 'data_value'::text]))),
    CONSTRAINT projection_checkpoints_subject_projection_version_check CHECK ((subject_projection_version >= 0))
);

CREATE TABLE public_event_delivery_heads (
    run_id text NOT NULL,
    head_state text NOT NULL,
    public_event_id text,
    execution_event_id text,
    execution_seq bigint,
    public_ordinal integer,
    delivery_state text,
    due_at timestamp with time zone,
    CONSTRAINT public_event_delivery_heads_check CHECK ((((head_state = 'ready'::text) AND (public_event_id IS NOT NULL) AND (execution_event_id IS NOT NULL) AND (execution_seq IS NOT NULL) AND (public_ordinal IS NOT NULL) AND (delivery_state IS NOT NULL) AND (due_at IS NOT NULL)) OR ((head_state = 'drained'::text) AND (public_event_id IS NULL) AND (execution_event_id IS NULL) AND (execution_seq IS NULL) AND (public_ordinal IS NULL) AND (delivery_state IS NULL) AND (due_at IS NULL)))),
    CONSTRAINT public_event_delivery_heads_delivery_state_check CHECK (((delivery_state IS NULL) OR (delivery_state = ANY (ARRAY['pending'::text, 'claimed'::text])))),
    CONSTRAINT public_event_delivery_heads_execution_seq_check CHECK (((execution_seq IS NULL) OR (execution_seq >= 1))),
    CONSTRAINT public_event_delivery_heads_head_state_check CHECK ((head_state = ANY (ARRAY['ready'::text, 'drained'::text]))),
    CONSTRAINT public_event_delivery_heads_public_ordinal_check CHECK (((public_ordinal IS NULL) OR (public_ordinal > 0)))
);

CREATE TABLE public_event_outbox (
    run_id text NOT NULL,
    public_event_id text NOT NULL,
    causation_event_id text NOT NULL,
    public_ordinal integer NOT NULL,
    public_schema_version integer NOT NULL,
    event_kind text NOT NULL,
    is_terminal boolean NOT NULL,
    publish_state text NOT NULL,
    safe_envelope jsonb NOT NULL,
    available_at timestamp with time zone NOT NULL,
    claimed_by text,
    claim_token text,
    claim_expires_at timestamp with time zone,
    publish_attempts integer DEFAULT 0 NOT NULL,
    published_at timestamp with time zone,
    published_by text,
    published_claim_token text,
    notified_at timestamp with time zone,
    retain_until timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT ck_public_event_delivery_lifecycle CHECK ((((publish_state = 'pending'::text) AND (claimed_by IS NULL) AND (claim_token IS NULL) AND (claim_expires_at IS NULL) AND (published_at IS NULL) AND (published_by IS NULL) AND (published_claim_token IS NULL) AND (notified_at IS NULL) AND (retain_until IS NULL)) OR ((publish_state = 'claimed'::text) AND (claimed_by IS NOT NULL) AND (claim_token IS NOT NULL) AND (claim_expires_at IS NOT NULL) AND (published_at IS NULL) AND (published_by IS NULL) AND (published_claim_token IS NULL) AND (notified_at IS NULL) AND (retain_until IS NULL)) OR ((publish_state = 'published'::text) AND (claimed_by IS NULL) AND (claim_token IS NULL) AND (claim_expires_at IS NULL) AND (published_at IS NOT NULL) AND (published_by IS NOT NULL) AND (published_claim_token IS NOT NULL) AND (notified_at IS NOT NULL) AND ((is_terminal AND (retain_until IS NULL)) OR ((NOT is_terminal) AND (retain_until IS NOT NULL)))))),
    CONSTRAINT ck_public_event_kind_terminal CHECK ((((NOT is_terminal) AND (event_kind = ANY (ARRAY['run.created'::text, 'run.started'::text, 'operation.started'::text, 'operation.completed'::text, 'operation.failed'::text]))) OR (is_terminal AND (event_kind = ANY (ARRAY['run.completed'::text, 'run.failed'::text, 'run.cancelled'::text, 'run.interrupted'::text]))))),
    CONSTRAINT public_event_outbox_check CHECK ((((publish_state = 'pending'::text) AND (claimed_by IS NULL) AND (claim_token IS NULL) AND (claim_expires_at IS NULL) AND (published_at IS NULL) AND (published_by IS NULL) AND (published_claim_token IS NULL) AND (notified_at IS NULL) AND (retain_until IS NULL)) OR ((publish_state = 'claimed'::text) AND (claimed_by IS NOT NULL) AND (claim_token IS NOT NULL) AND (claim_expires_at IS NOT NULL) AND (published_at IS NULL) AND (published_by IS NULL) AND (published_claim_token IS NULL) AND (notified_at IS NULL) AND (retain_until IS NULL)) OR ((publish_state = 'published'::text) AND (published_at IS NOT NULL) AND (claimed_by IS NULL) AND (claim_token IS NULL) AND (claim_expires_at IS NULL) AND (published_by IS NOT NULL) AND (published_claim_token IS NOT NULL) AND (notified_at IS NOT NULL) AND ((is_terminal AND (retain_until IS NULL)) OR ((NOT is_terminal) AND (retain_until IS NOT NULL)))))),
    CONSTRAINT public_event_outbox_check1 CHECK ((((NOT is_terminal) AND (event_kind = ANY (ARRAY['run.created'::text, 'run.started'::text, 'operation.started'::text, 'operation.completed'::text, 'operation.failed'::text]))) OR (is_terminal AND (event_kind = ANY (ARRAY['run.completed'::text, 'run.failed'::text, 'run.cancelled'::text, 'run.interrupted'::text]))))),
    CONSTRAINT public_event_outbox_check2 CHECK ((((event_kind = 'run.created'::text) AND (public_ordinal = 10)) OR ((event_kind = 'run.started'::text) AND (public_ordinal = 20)) OR ((event_kind = 'operation.started'::text) AND (public_ordinal = 30)) OR ((event_kind = ANY (ARRAY['operation.completed'::text, 'operation.failed'::text])) AND (public_ordinal = 40)) OR ((event_kind = ANY (ARRAY['run.completed'::text, 'run.failed'::text, 'run.cancelled'::text, 'run.interrupted'::text])) AND (public_ordinal = 50)))),
    CONSTRAINT public_event_outbox_public_ordinal_check CHECK ((public_ordinal > 0)),
    CONSTRAINT public_event_outbox_public_schema_version_check CHECK ((public_schema_version >= 1)),
    CONSTRAINT public_event_outbox_publish_attempts_check CHECK ((publish_attempts >= 0)),
    CONSTRAINT public_event_outbox_publish_state_check CHECK ((publish_state = ANY (ARRAY['pending'::text, 'claimed'::text, 'published'::text])))
);

CREATE TABLE public_event_projection_decisions (
    run_id text NOT NULL,
    execution_event_id text NOT NULL,
    execution_seq bigint NOT NULL,
    execution_occurred_at timestamp with time zone NOT NULL,
    execution_transition_key text NOT NULL,
    decision text NOT NULL,
    public_event_id text,
    public_ordinal integer,
    public_schema_version integer,
    event_kind text,
    is_terminal boolean,
    bound_at timestamp with time zone,
    CONSTRAINT public_event_projection_decisions_check CHECK ((((decision = 'none'::text) AND (public_event_id IS NULL) AND (public_ordinal IS NULL) AND (public_schema_version IS NULL) AND (event_kind IS NULL) AND (is_terminal IS NULL) AND (bound_at IS NULL)) OR ((decision = 'public'::text) AND (public_event_id IS NOT NULL) AND (public_ordinal IS NOT NULL) AND (public_schema_version IS NOT NULL) AND (event_kind IS NOT NULL) AND (is_terminal IS NOT NULL) AND (bound_at IS NOT NULL)))),
    CONSTRAINT public_event_projection_decisions_decision_check CHECK ((decision = ANY (ARRAY['none'::text, 'public'::text]))),
    CONSTRAINT public_event_projection_decisions_execution_seq_check CHECK ((execution_seq >= 1)),
    CONSTRAINT public_event_projection_decisions_public_ordinal_check CHECK (((public_ordinal IS NULL) OR (public_ordinal > 0))),
    CONSTRAINT public_event_projection_decisions_public_schema_version_check CHECK (((public_schema_version IS NULL) OR (public_schema_version >= 1)))
);

CREATE TABLE public_event_receipts (
    run_id text NOT NULL,
    public_event_id text NOT NULL,
    causation_event_id text NOT NULL,
    public_ordinal integer NOT NULL,
    public_schema_version integer NOT NULL,
    event_kind text NOT NULL,
    is_terminal boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT public_event_receipts_check CHECK ((((NOT is_terminal) AND (event_kind = ANY (ARRAY['run.created'::text, 'run.started'::text, 'operation.started'::text, 'operation.completed'::text, 'operation.failed'::text]))) OR (is_terminal AND (event_kind = ANY (ARRAY['run.completed'::text, 'run.failed'::text, 'run.cancelled'::text, 'run.interrupted'::text]))))),
    CONSTRAINT public_event_receipts_public_ordinal_check CHECK ((public_ordinal > 0)),
    CONSTRAINT public_event_receipts_public_schema_version_check CHECK ((public_schema_version >= 1))
);

CREATE TABLE recovery_artifact_roots (
    run_id text NOT NULL,
    source_run_id text NOT NULL,
    artifact_run_id text NOT NULL,
    artifact_id text NOT NULL,
    source_activation_id text NOT NULL,
    created_by_transition_key text NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT recovery_artifact_roots_projection_version_check CHECK ((projection_version >= 0))
);

CREATE TABLE recovery_effect_roots (
    run_id text NOT NULL,
    source_run_id text NOT NULL,
    effect_run_id text NOT NULL,
    source_activation_id text NOT NULL,
    effect_id text NOT NULL,
    created_by_transition_key text NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT recovery_effect_roots_projection_version_check CHECK ((projection_version >= 0))
);

CREATE TABLE recovery_revision_roots (
    run_id text NOT NULL,
    root_role text NOT NULL,
    source_run_id text NOT NULL,
    definition_id text NOT NULL,
    definition_revision_id text NOT NULL,
    deployment_revision_id text NOT NULL,
    plan_hash text NOT NULL,
    binding_hash text NOT NULL,
    created_by_transition_key text NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT recovery_revision_roots_binding_hash_check CHECK (((length(binding_hash) = 71) AND (binding_hash ~~ 'sha256:%'::text))),
    CONSTRAINT recovery_revision_roots_plan_hash_check CHECK (((length(plan_hash) = 71) AND (plan_hash ~~ 'sha256:%'::text))),
    CONSTRAINT recovery_revision_roots_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT recovery_revision_roots_root_role_check CHECK ((root_role = ANY (ARRAY['source'::text, 'target'::text])))
);

CREATE TABLE recovery_transition_results (
    authority_run_id text NOT NULL,
    transition_key text NOT NULL,
    intent_hash text NOT NULL,
    primary_event_run_id text NOT NULL,
    primary_event_id text NOT NULL,
    result_json jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT recovery_transition_results_intent_hash_check CHECK (((length(intent_hash) = 71) AND (intent_hash ~~ 'sha256:%'::text)))
);

CREATE TABLE response_public_items (
    run_id text NOT NULL,
    activation_id text NOT NULL,
    attempt_no integer NOT NULL,
    model_call_no integer NOT NULL,
    item_ordinal integer NOT NULL,
    item_id text NOT NULL,
    output_index integer NOT NULL,
    node_id text NOT NULL,
    item_kind text NOT NULL,
    item_status text NOT NULL,
    seal_index bigint,
    safe_item jsonb,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT response_public_items_attempt_no_check CHECK ((attempt_no >= 1)),
    CONSTRAINT response_public_items_item_id_check CHECK ((item_id <> ''::text)),
    CONSTRAINT response_public_items_item_kind_check CHECK ((item_kind = ANY (ARRAY['message'::text, 'function_call'::text]))),
    CONSTRAINT response_public_items_item_ordinal_check CHECK ((item_ordinal >= 0)),
    CONSTRAINT response_public_items_item_status_check CHECK ((item_status = ANY (ARRAY['reserved'::text, 'completed'::text, 'incomplete'::text, 'incomplete_unsealed'::text]))),
    CONSTRAINT response_public_items_model_call_no_check CHECK ((model_call_no >= 1)),
    CONSTRAINT response_public_items_node_id_check CHECK ((node_id <> ''::text)),
    CONSTRAINT response_public_items_output_index_check CHECK ((output_index >= 0)),
    CONSTRAINT response_public_items_seal_index_check CHECK (((seal_index IS NULL) OR (seal_index >= 0)))
);

CREATE TABLE response_snapshots (
    run_id text NOT NULL,
    response_id text NOT NULL,
    terminal_kind text NOT NULL,
    response_status text NOT NULL,
    response_payload jsonb NOT NULL,
    workflow_payload jsonb NOT NULL,
    public_item_manifest jsonb NOT NULL,
    usage jsonb,
    usage_status text NOT NULL,
    snapshot_hash text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT response_snapshots_response_status_check CHECK ((response_status = ANY (ARRAY['completed'::text, 'failed'::text, 'cancelled'::text, 'incomplete'::text]))),
    CONSTRAINT response_snapshots_snapshot_hash_check CHECK (((length(snapshot_hash) = 71) AND (snapshot_hash ~~ 'sha256:%'::text))),
    CONSTRAINT response_snapshots_terminal_kind_check CHECK ((terminal_kind = ANY (ARRAY['response.completed'::text, 'response.failed'::text, 'workflow.response.timed_out'::text, 'workflow.response.cancelled'::text, 'workflow.response.interrupted'::text]))),
    CONSTRAINT response_snapshots_usage_status_check CHECK ((usage_status = ANY (ARRAY['complete'::text, 'partial'::text, 'unavailable'::text])))
);

CREATE TABLE run_migration_intents (
    run_id text NOT NULL,
    target_run_id text NOT NULL,
    target_definition_id text NOT NULL,
    target_definition_revision_id text NOT NULL,
    target_deployment_revision_id text NOT NULL,
    target_plan_hash text NOT NULL,
    target_binding_hash text NOT NULL,
    target_input jsonb NOT NULL,
    target_input_hash text NOT NULL,
    mapping_contracts jsonb NOT NULL,
    mapping_hash text NOT NULL,
    reuse_candidates jsonb NOT NULL,
    intent_transition_key text NOT NULL,
    intent_event_id text NOT NULL,
    intent_state text NOT NULL,
    final_transition_key text,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    target_timeout_ms bigint DEFAULT 300000 NOT NULL,
    CONSTRAINT run_migration_intents_check CHECK ((run_id <> target_run_id)),
    CONSTRAINT run_migration_intents_check1 CHECK ((((intent_state = 'pending'::text) AND (final_transition_key IS NULL) AND (completed_at IS NULL)) OR ((intent_state = 'completed'::text) AND (final_transition_key IS NOT NULL) AND (completed_at IS NOT NULL)))),
    CONSTRAINT run_migration_intents_intent_state_check CHECK ((intent_state = ANY (ARRAY['pending'::text, 'completed'::text]))),
    CONSTRAINT run_migration_intents_mapping_hash_check CHECK (((length(mapping_hash) = 71) AND (mapping_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_migration_intents_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT run_migration_intents_target_binding_hash_check CHECK (((length(target_binding_hash) = 71) AND (target_binding_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_migration_intents_target_input_hash_check CHECK (((length(target_input_hash) = 71) AND (target_input_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_migration_intents_target_plan_hash_check CHECK (((length(target_plan_hash) = 71) AND (target_plan_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_migration_intents_target_timeout_ms_check CHECK ((target_timeout_ms > 0))
);

CREATE TABLE run_recovery_lineage (
    run_id text NOT NULL,
    source_run_id text NOT NULL,
    lineage_kind text NOT NULL,
    source_generation bigint NOT NULL,
    target_generation bigint NOT NULL,
    source_definition_id text NOT NULL,
    source_definition_revision_id text NOT NULL,
    source_deployment_revision_id text NOT NULL,
    source_plan_hash text NOT NULL,
    source_binding_hash text NOT NULL,
    target_definition_id text NOT NULL,
    target_definition_revision_id text NOT NULL,
    target_deployment_revision_id text NOT NULL,
    target_plan_hash text NOT NULL,
    target_binding_hash text NOT NULL,
    source_checkpoint_hash text,
    created_by_transition_key text NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT run_recovery_lineage_check CHECK ((source_run_id <> run_id)),
    CONSTRAINT run_recovery_lineage_check1 CHECK ((((lineage_kind = 'continue_as_new'::text) AND (target_generation = (source_generation + 1)) AND (source_checkpoint_hash IS NULL)) OR ((lineage_kind = ANY (ARRAY['redrive'::text, 'migrate'::text])) AND (target_generation = 1) AND (source_checkpoint_hash IS NULL)) OR ((lineage_kind = 'fork'::text) AND (target_generation = 1) AND (source_checkpoint_hash IS NOT NULL)))),
    CONSTRAINT run_recovery_lineage_lineage_kind_check CHECK ((lineage_kind = ANY (ARRAY['redrive'::text, 'fork'::text, 'migrate'::text, 'continue_as_new'::text]))),
    CONSTRAINT run_recovery_lineage_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT run_recovery_lineage_source_binding_hash_check CHECK (((length(source_binding_hash) = 71) AND (source_binding_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_recovery_lineage_source_checkpoint_hash_check CHECK (((source_checkpoint_hash IS NULL) OR ((length(source_checkpoint_hash) = 71) AND (source_checkpoint_hash ~~ 'sha256:%'::text)))),
    CONSTRAINT run_recovery_lineage_source_generation_check CHECK ((source_generation >= 1)),
    CONSTRAINT run_recovery_lineage_source_plan_hash_check CHECK (((length(source_plan_hash) = 71) AND (source_plan_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_recovery_lineage_target_binding_hash_check CHECK (((length(target_binding_hash) = 71) AND (target_binding_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_recovery_lineage_target_generation_check CHECK ((target_generation >= 1)),
    CONSTRAINT run_recovery_lineage_target_plan_hash_check CHECK (((length(target_plan_hash) = 71) AND (target_plan_hash ~~ 'sha256:%'::text)))
);

CREATE TABLE run_reuse_candidates (
    run_id text NOT NULL,
    candidate_id text NOT NULL,
    target_scope_instance_id text NOT NULL,
    target_node_id text NOT NULL,
    stable_activation_key text NOT NULL,
    source_run_id text NOT NULL,
    source_activation_id text NOT NULL,
    source_control_provenance jsonb NOT NULL,
    definition_revision_id text NOT NULL,
    deployment_revision_id text NOT NULL,
    plan_hash text NOT NULL,
    binding_hash text NOT NULL,
    node_config_hash text NOT NULL,
    descriptor_hash text NOT NULL,
    input_value_hash text NOT NULL,
    output_value_hash text NOT NULL,
    output_schema_hash text NOT NULL,
    effect_policy_hash text NOT NULL,
    inherited_effect_id text NOT NULL,
    data_dependencies_hash text NOT NULL,
    created_by_transition_key text NOT NULL,
    candidate_state text NOT NULL,
    materialized_activation_id text,
    decision_transition_key text,
    rejection_reason text,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    decided_at timestamp with time zone,
    CONSTRAINT run_reuse_candidates_binding_hash_check CHECK (((length(binding_hash) = 71) AND (binding_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_reuse_candidates_candidate_state_check CHECK ((candidate_state = ANY (ARRAY['candidate'::text, 'materialized'::text, 'rejected'::text]))),
    CONSTRAINT run_reuse_candidates_check CHECK ((source_run_id <> run_id)),
    CONSTRAINT run_reuse_candidates_check1 CHECK ((((candidate_state = 'candidate'::text) AND (materialized_activation_id IS NULL) AND (decision_transition_key IS NULL) AND (rejection_reason IS NULL) AND (decided_at IS NULL)) OR ((candidate_state = 'materialized'::text) AND (materialized_activation_id IS NOT NULL) AND (decision_transition_key IS NOT NULL) AND (rejection_reason IS NULL) AND (decided_at IS NOT NULL)) OR ((candidate_state = 'rejected'::text) AND (materialized_activation_id IS NULL) AND (decision_transition_key IS NOT NULL) AND (rejection_reason IS NOT NULL) AND (decided_at IS NOT NULL)))),
    CONSTRAINT run_reuse_candidates_data_dependencies_hash_check CHECK (((length(data_dependencies_hash) = 71) AND (data_dependencies_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_reuse_candidates_descriptor_hash_check CHECK (((length(descriptor_hash) = 71) AND (descriptor_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_reuse_candidates_effect_policy_hash_check CHECK (((length(effect_policy_hash) = 71) AND (effect_policy_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_reuse_candidates_input_value_hash_check CHECK (((length(input_value_hash) = 71) AND (input_value_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_reuse_candidates_node_config_hash_check CHECK (((length(node_config_hash) = 71) AND (node_config_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_reuse_candidates_output_schema_hash_check CHECK (((length(output_schema_hash) = 71) AND (output_schema_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_reuse_candidates_output_value_hash_check CHECK (((length(output_value_hash) = 71) AND (output_value_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_reuse_candidates_plan_hash_check CHECK (((length(plan_hash) = 71) AND (plan_hash ~~ 'sha256:%'::text))),
    CONSTRAINT run_reuse_candidates_projection_version_check CHECK ((projection_version >= 0))
);

CREATE TABLE scheduler_checkpoints (
    run_id text NOT NULL,
    checkpoint_id text NOT NULL,
    content_hash text NOT NULL,
    checkpoint_kind text NOT NULL,
    transition_key text NOT NULL,
    intent_hash text NOT NULL,
    event_id text NOT NULL,
    checkpoint_schema_version integer NOT NULL,
    scheduler_projection_version bigint NOT NULL,
    fact_payload jsonb NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT scheduler_checkpoints_checkpoint_kind_check CHECK ((checkpoint_kind = ANY (ARRAY['planned_action'::text, 'task_started'::text, 'task_completed'::text, 'task_retry_scheduled'::text]))),
    CONSTRAINT scheduler_checkpoints_checkpoint_schema_version_check CHECK ((checkpoint_schema_version >= 1)),
    CONSTRAINT scheduler_checkpoints_content_hash_check CHECK (((length(content_hash) = 71) AND (content_hash ~~ 'sha256:%'::text))),
    CONSTRAINT scheduler_checkpoints_intent_hash_check CHECK (((length(intent_hash) = 71) AND (intent_hash ~~ 'sha256:%'::text))),
    CONSTRAINT scheduler_checkpoints_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT scheduler_checkpoints_scheduler_projection_version_check CHECK ((scheduler_projection_version >= 1))
);

CREATE TABLE scheduler_occurrence_values (
    run_id text NOT NULL,
    occurrence_key jsonb NOT NULL,
    port_id text NOT NULL,
    owner_activation_id text NOT NULL,
    runtime_value jsonb NOT NULL,
    value_ref jsonb NOT NULL,
    declared_type jsonb NOT NULL,
    storage_kind text NOT NULL,
    payload_id text,
    artifact_id text,
    content_hash text NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT scheduler_occurrence_values_check CHECK ((((storage_kind = 'inline'::text) AND (payload_id IS NOT NULL) AND (artifact_id IS NULL)) OR ((storage_kind = 'artifact'::text) AND (payload_id IS NULL) AND (artifact_id IS NOT NULL)))),
    CONSTRAINT scheduler_occurrence_values_content_hash_check CHECK (((length(content_hash) = 71) AND (content_hash ~~ 'sha256:%'::text))),
    CONSTRAINT scheduler_occurrence_values_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT scheduler_occurrence_values_storage_kind_check CHECK ((storage_kind = ANY (ARRAY['inline'::text, 'artifact'::text])))
);

CREATE TABLE scheduler_subflow_invocations (
    run_id text NOT NULL,
    child_run_id text NOT NULL,
    parent_activation_id text NOT NULL,
    node_id text NOT NULL,
    occurrence_key jsonb NOT NULL,
    invocation_scope_instance_id text NOT NULL,
    parent_scope_instance_id text NOT NULL,
    static_scope_id text NOT NULL,
    definition_revision_id text NOT NULL,
    deployment_revision_id text NOT NULL,
    plan_hash text NOT NULL,
    binding_hash text NOT NULL,
    interface_version text NOT NULL,
    output_contracts jsonb NOT NULL,
    invocation_state text NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    CONSTRAINT scheduler_subflow_invocations_binding_hash_check CHECK (((length(binding_hash) = 71) AND (binding_hash ~~ 'sha256:%'::text))),
    CONSTRAINT scheduler_subflow_invocations_check CHECK ((invocation_scope_instance_id <> parent_scope_instance_id)),
    CONSTRAINT scheduler_subflow_invocations_check1 CHECK ((((invocation_state = ANY (ARRAY['started'::text, 'cancellation_requested'::text])) AND (completed_at IS NULL)) OR ((invocation_state = 'completed'::text) AND (completed_at IS NOT NULL)))),
    CONSTRAINT scheduler_subflow_invocations_invocation_state_check CHECK ((invocation_state = ANY (ARRAY['started'::text, 'cancellation_requested'::text, 'completed'::text]))),
    CONSTRAINT scheduler_subflow_invocations_plan_hash_check CHECK (((length(plan_hash) = 71) AND (plan_hash ~~ 'sha256:%'::text))),
    CONSTRAINT scheduler_subflow_invocations_projection_version_check CHECK ((projection_version >= 0))
);

CREATE TABLE scheduler_values (
    run_id text NOT NULL,
    port_id text NOT NULL,
    owner_activation_id text NOT NULL,
    runtime_value jsonb NOT NULL,
    value_ref jsonb NOT NULL,
    declared_type jsonb NOT NULL,
    storage_kind text NOT NULL,
    payload_id text,
    artifact_id text,
    content_hash text NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT scheduler_values_check CHECK ((((storage_kind = 'inline'::text) AND (payload_id IS NOT NULL) AND (artifact_id IS NULL)) OR ((storage_kind = 'artifact'::text) AND (payload_id IS NULL) AND (artifact_id IS NOT NULL)))),
    CONSTRAINT scheduler_values_content_hash_check CHECK (((length(content_hash) = 71) AND (content_hash ~~ 'sha256:%'::text))),
    CONSTRAINT scheduler_values_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT scheduler_values_storage_kind_check CHECK ((storage_kind = ANY (ARRAY['inline'::text, 'artifact'::text])))
);

CREATE TABLE scheduler_wait_registrations (
    run_id text NOT NULL,
    wait_id text NOT NULL,
    activation_id text NOT NULL,
    node_id text NOT NULL,
    occurrence_key jsonb NOT NULL,
    signal_name text,
    signal_id text,
    timer_id text,
    due_at_ms bigint,
    payload_type jsonb,
    winner_kind text,
    winner_signal_id text,
    winner_timer_id text,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    resolved_at timestamp with time zone,
    CONSTRAINT scheduler_wait_registrations_check CHECK (((signal_name IS NULL) = (signal_id IS NULL))),
    CONSTRAINT scheduler_wait_registrations_check1 CHECK (((timer_id IS NULL) = (due_at_ms IS NULL))),
    CONSTRAINT scheduler_wait_registrations_check2 CHECK (((signal_id IS NOT NULL) OR (timer_id IS NOT NULL))),
    CONSTRAINT scheduler_wait_registrations_check3 CHECK ((((winner_kind IS NULL) AND (winner_signal_id IS NULL) AND (winner_timer_id IS NULL) AND (resolved_at IS NULL)) OR ((winner_kind = 'signal'::text) AND (winner_signal_id = signal_id) AND (winner_timer_id IS NULL) AND (resolved_at IS NOT NULL)) OR ((winner_kind = 'timer'::text) AND (winner_timer_id = timer_id) AND (winner_signal_id IS NULL) AND (resolved_at IS NOT NULL)) OR ((winner_kind = 'cancelled'::text) AND (winner_signal_id IS NULL) AND (winner_timer_id IS NULL) AND (resolved_at IS NOT NULL)))),
    CONSTRAINT scheduler_wait_registrations_due_at_ms_check CHECK (((due_at_ms IS NULL) OR (due_at_ms >= 0))),
    CONSTRAINT scheduler_wait_registrations_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT scheduler_wait_registrations_winner_kind_check CHECK (((winner_kind IS NULL) OR (winner_kind = ANY (ARRAY['signal'::text, 'timer'::text, 'cancelled'::text]))))
);

CREATE TABLE scope_instances (
    run_id text NOT NULL,
    scope_instance_id text NOT NULL,
    parent_scope_instance_id text,
    static_scope_id text NOT NULL,
    stable_dynamic_key text,
    scope_kind text NOT NULL,
    is_root boolean NOT NULL,
    lifecycle text NOT NULL,
    admission_state text NOT NULL,
    admitted_children bigint DEFAULT 0 NOT NULL,
    settled_children bigint DEFAULT 0 NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    settled_at timestamp with time zone,
    CONSTRAINT scope_instances_admission_state_check CHECK ((admission_state = ANY (ARRAY['open'::text, 'draining'::text, 'closed'::text]))),
    CONSTRAINT scope_instances_admitted_children_check CHECK ((admitted_children >= 0)),
    CONSTRAINT scope_instances_check CHECK (((settled_children >= 0) AND (settled_children <= admitted_children))),
    CONSTRAINT scope_instances_check1 CHECK (((is_root AND (scope_kind = 'root'::text) AND (parent_scope_instance_id IS NULL) AND (stable_dynamic_key IS NULL)) OR ((NOT is_root) AND (scope_kind <> 'root'::text) AND (parent_scope_instance_id IS NOT NULL) AND (stable_dynamic_key IS NOT NULL)))),
    CONSTRAINT scope_instances_check2 CHECK ((((lifecycle = ANY (ARRAY['active'::text, 'settling'::text])) AND (settled_at IS NULL)) OR ((lifecycle = ANY (ARRAY['settled'::text, 'cancelled'::text])) AND (admission_state = 'closed'::text) AND (settled_children = admitted_children) AND (settled_at IS NOT NULL)))),
    CONSTRAINT scope_instances_lifecycle_check CHECK ((lifecycle = ANY (ARRAY['active'::text, 'settling'::text, 'settled'::text, 'cancelled'::text]))),
    CONSTRAINT scope_instances_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT scope_instances_scope_kind_check CHECK ((scope_kind = ANY (ARRAY['root'::text, 'map_item'::text, 'loop_iteration'::text, 'subflow_invocation'::text, 'agent_loop_turn'::text, 'parallel_leg'::text])))
);

CREATE TABLE signals_inbox (
    run_id text NOT NULL,
    signal_id text NOT NULL,
    message_id text NOT NULL,
    intent_hash text NOT NULL,
    signal_name text NOT NULL,
    target_activation_id text NOT NULL,
    payload_id text NOT NULL,
    signal_state text NOT NULL,
    received_at timestamp with time zone NOT NULL,
    consumed_by_transition_key text,
    consumed_event_id text,
    terminal_at timestamp with time zone,
    projection_version bigint DEFAULT 0 NOT NULL,
    CONSTRAINT signals_inbox_check CHECK ((((signal_state = 'pending'::text) AND (consumed_by_transition_key IS NULL) AND (consumed_event_id IS NULL) AND (terminal_at IS NULL)) OR ((signal_state <> 'pending'::text) AND (consumed_by_transition_key IS NOT NULL) AND (consumed_event_id IS NOT NULL) AND (terminal_at IS NOT NULL)))),
    CONSTRAINT signals_inbox_intent_hash_check CHECK (((length(intent_hash) = 71) AND (intent_hash ~~ 'sha256:%'::text))),
    CONSTRAINT signals_inbox_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT signals_inbox_signal_state_check CHECK ((signal_state = ANY (ARRAY['pending'::text, 'consumed'::text, 'rejected'::text, 'expired'::text])))
);

CREATE TABLE wait_late_audit_outbox (
    run_id text NOT NULL,
    activation_id text NOT NULL,
    loser_kind text NOT NULL,
    loser_id text NOT NULL,
    due_at timestamp with time zone NOT NULL,
    audit_state text NOT NULL,
    claimed_by text,
    claim_token text,
    claim_expires_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    completed_event_id text,
    CONSTRAINT wait_late_audit_outbox_pkey PRIMARY KEY (run_id, loser_kind, loser_id),
    CONSTRAINT wait_late_audit_outbox_kind_check CHECK (loser_kind = ANY (ARRAY['timer'::text, 'signal'::text])),
    CONSTRAINT wait_late_audit_outbox_state_check CHECK (audit_state = ANY (ARRAY['pending'::text, 'claimed'::text, 'completed'::text])),
    CONSTRAINT wait_late_audit_outbox_lifecycle_check CHECK (
        ((audit_state = 'pending'::text) AND (claimed_by IS NULL) AND (claim_token IS NULL) AND (claim_expires_at IS NULL) AND (completed_event_id IS NULL))
        OR ((audit_state = 'claimed'::text) AND (claimed_by IS NOT NULL) AND (claim_token IS NOT NULL) AND (claim_expires_at IS NOT NULL) AND (completed_event_id IS NULL))
        OR ((audit_state = 'completed'::text) AND (claimed_by IS NULL) AND (claim_token IS NULL) AND (claim_expires_at IS NULL) AND (completed_event_id IS NOT NULL))
    )
);

CREATE TABLE task_outbox (
    run_id text NOT NULL,
    task_id text NOT NULL,
    activation_id text NOT NULL,
    attempt_no integer NOT NULL,
    lease_epoch bigint NOT NULL,
    fencing_token text NOT NULL,
    effect_id text NOT NULL,
    created_by_transition_key text NOT NULL,
    task_state text NOT NULL,
    task_envelope jsonb NOT NULL,
    available_at timestamp with time zone NOT NULL,
    claimed_by text,
    claim_token text,
    claim_expires_at timestamp with time zone,
    publish_attempts integer DEFAULT 0 NOT NULL,
    published_at timestamp with time zone,
    acked_at timestamp with time zone,
    last_error_code text,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    claim_mode text,
    CONSTRAINT ck_task_outbox_claim_mode CHECK (((claim_mode IS NULL) OR (claim_mode = ANY (ARRAY['execute'::text, 'finalize_lease_loss'::text, 'acknowledge'::text])))),
    CONSTRAINT task_outbox_attempt_no_check CHECK ((attempt_no >= 1)),
    CONSTRAINT task_outbox_check CHECK ((lease_epoch >= attempt_no)),
    CONSTRAINT task_outbox_check1 CHECK ((((task_state = 'pending'::text) AND (claimed_by IS NULL) AND (claim_token IS NULL) AND (claim_expires_at IS NULL)) OR ((task_state = 'claimed'::text) AND (claimed_by IS NOT NULL) AND (claim_token IS NOT NULL) AND (claim_expires_at IS NOT NULL)) OR (task_state = ANY (ARRAY['published'::text, 'acked'::text, 'dead'::text])))),
    CONSTRAINT task_outbox_check2 CHECK (((task_state <> 'published'::text) OR (published_at IS NOT NULL))),
    CONSTRAINT task_outbox_check3 CHECK (((task_state <> 'acked'::text) OR ((published_at IS NOT NULL) AND (acked_at IS NOT NULL)))),
    CONSTRAINT task_outbox_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT task_outbox_publish_attempts_check CHECK ((publish_attempts >= 0)),
    CONSTRAINT task_outbox_task_state_check CHECK ((task_state = ANY (ARRAY['pending'::text, 'claimed'::text, 'published'::text, 'acked'::text, 'dead'::text])))
);

CREATE TABLE timers (
    run_id text NOT NULL,
    timer_id text NOT NULL,
    activation_id text,
    timer_kind text NOT NULL,
    timer_state text NOT NULL,
    deadline_at timestamp with time zone NOT NULL,
    expected_attempt_no integer,
    expected_lease_epoch bigint,
    expected_fencing_token text,
    retry_budget_snapshot integer,
    created_by_transition_key text NOT NULL,
    fired_by_transition_key text,
    fired_event_id text,
    projection_version bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone NOT NULL,
    fired_at timestamp with time zone,
    CONSTRAINT timers_check CHECK ((((expected_attempt_no IS NULL) AND (expected_lease_epoch IS NULL) AND (expected_fencing_token IS NULL)) OR ((expected_attempt_no IS NOT NULL) AND (expected_lease_epoch IS NOT NULL) AND (expected_lease_epoch >= expected_attempt_no) AND (expected_fencing_token IS NOT NULL)))),
    CONSTRAINT timers_check1 CHECK ((((timer_kind = 'run_deadline'::text) AND (activation_id IS NULL)) OR ((timer_kind <> 'run_deadline'::text) AND (activation_id IS NOT NULL)))),
    CONSTRAINT timers_check2 CHECK (((timer_kind <> ALL (ARRAY['lease'::text, 'retry'::text])) OR ((expected_attempt_no IS NOT NULL) AND (retry_budget_snapshot IS NOT NULL)))),
    CONSTRAINT timers_check3 CHECK ((((timer_state = 'scheduled'::text) AND (fired_by_transition_key IS NULL) AND (fired_event_id IS NULL) AND (fired_at IS NULL)) OR ((timer_state = 'fired'::text) AND (fired_by_transition_key IS NOT NULL) AND (fired_event_id IS NOT NULL) AND (fired_at IS NOT NULL) AND (fired_at >= deadline_at)) OR ((timer_state = 'cancelled'::text) AND (fired_at IS NOT NULL)))),
    CONSTRAINT timers_expected_attempt_no_check CHECK (((expected_attempt_no IS NULL) OR (expected_attempt_no >= 1))),
    CONSTRAINT timers_expected_lease_epoch_check CHECK (((expected_lease_epoch IS NULL) OR (expected_lease_epoch >= 1))),
    CONSTRAINT timers_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT timers_retry_budget_snapshot_check CHECK (((retry_budget_snapshot IS NULL) OR (retry_budget_snapshot >= 0))),
    CONSTRAINT timers_timer_kind_check CHECK ((timer_kind = ANY (ARRAY['lease'::text, 'wait'::text, 'retry'::text, 'activation_timeout'::text, 'run_deadline'::text]))),
    CONSTRAINT timers_timer_state_check CHECK ((timer_state = ANY (ARRAY['scheduled'::text, 'fired'::text, 'cancelled'::text])))
);

CREATE TABLE workflow_definition_public_metadata (
    definition_id text NOT NULL,
    definition_revision_id text NOT NULL,
    display_name text NOT NULL,
    public_description text NOT NULL,
    CONSTRAINT workflow_definition_public_metadata_display_name_check CHECK (((display_name <> ''::text) AND (length(display_name) <= 256))),
    CONSTRAINT workflow_definition_public_metadata_public_description_check CHECK ((length(public_description) <= 4096))
);

CREATE TABLE workflow_definition_revisions (
    definition_id text NOT NULL,
    definition_revision_id text NOT NULL,
    revision_status text NOT NULL,
    author_document jsonb NOT NULL,
    canonical_plan jsonb NOT NULL,
    plan_hash text NOT NULL,
    compiler_version text NOT NULL,
    expression_engine_version text NOT NULL,
    descriptor_contracts jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL,
    published_at timestamp with time zone,
    CONSTRAINT workflow_definition_revisions_check CHECK ((((revision_status = 'draft'::text) AND (published_at IS NULL)) OR ((revision_status = 'published'::text) AND (published_at IS NOT NULL)))),
    CONSTRAINT workflow_definition_revisions_plan_hash_check CHECK (((length(plan_hash) = 71) AND (plan_hash ~~ 'sha256:%'::text))),
    CONSTRAINT workflow_definition_revisions_revision_status_check CHECK ((revision_status = ANY (ARRAY['draft'::text, 'published'::text])))
);

CREATE TABLE workflow_definitions (
    definition_id text NOT NULL,
    agent_id text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT workflow_definitions_check CHECK (((definition_id <> ''::text) AND (agent_id <> ''::text)))
);

CREATE TABLE workflow_retrieval_publications (
    run_id text NOT NULL,
    retrieval_id text NOT NULL,
    task_id text NOT NULL,
    activation_id text NOT NULL,
    node_id text NOT NULL,
    attempt_no integer NOT NULL,
    retrieval_resource_id text NOT NULL,
    retrieval_resource_version text NOT NULL,
    retrieval_descriptor_hash text NOT NULL,
    query_field text NOT NULL,
    effective_public_policy jsonb NOT NULL,
    effective_public_policy_hash text NOT NULL,
    public_projection jsonb,
    public_projection_hash text,
    completion_transition_key text NOT NULL,
    completion_intent_hash text NOT NULL,
    completion_event_id text NOT NULL,
    completion_event_seq bigint NOT NULL,
    publication_hash text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT workflow_retrieval_publicati_effective_public_policy_hash_check CHECK ((effective_public_policy_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT workflow_retrieval_publication_retrieval_resource_version_check CHECK (((retrieval_resource_version <> ''::text) AND (octet_length(retrieval_resource_version) <= 64))),
    CONSTRAINT workflow_retrieval_publications_attempt_no_check CHECK ((attempt_no >= 1)),
    CONSTRAINT workflow_retrieval_publications_check CHECK (((public_projection IS NULL) = (public_projection_hash IS NULL))),
    CONSTRAINT workflow_retrieval_publications_completion_event_seq_check CHECK ((completion_event_seq >= 1)),
    CONSTRAINT workflow_retrieval_publications_completion_intent_hash_check CHECK ((completion_intent_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT workflow_retrieval_publications_effective_public_policy_check CHECK (((jsonb_typeof(effective_public_policy) = 'object'::text) AND (octet_length((effective_public_policy)::text) <= 262144))),
    CONSTRAINT workflow_retrieval_publications_public_projection_check CHECK (((public_projection IS NULL) OR ((jsonb_typeof(public_projection) = 'object'::text) AND (octet_length((public_projection)::text) <= 1048576)))),
    CONSTRAINT workflow_retrieval_publications_public_projection_hash_check CHECK (((public_projection_hash IS NULL) OR (public_projection_hash ~ '^sha256:[0-9a-f]{64}$'::text))),
    CONSTRAINT workflow_retrieval_publications_publication_hash_check CHECK ((publication_hash ~ '^sha256:[0-9a-f]{64}$'::text)),
    CONSTRAINT workflow_retrieval_publications_query_field_check CHECK (((query_field <> ''::text) AND (octet_length(query_field) <= 128))),
    CONSTRAINT workflow_retrieval_publications_retrieval_descriptor_hash_check CHECK ((retrieval_descriptor_hash ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT workflow_retrieval_publications_retrieval_id_check CHECK ((retrieval_id ~ '^ret_[0-9a-f]{64}$'::text)),
    CONSTRAINT workflow_retrieval_publications_retrieval_resource_id_check CHECK (((retrieval_resource_id <> ''::text) AND (octet_length(retrieval_resource_id) <= 128))),
    CONSTRAINT workflow_retrieval_publications_task_id_check CHECK ((task_id ~ '^task_[0-9a-f]{64}$'::text))
);

CREATE TABLE workflow_runs (
    run_id text NOT NULL,
    definition_id text NOT NULL,
    definition_revision_id text NOT NULL,
    deployment_revision_id text NOT NULL,
    plan_hash text NOT NULL,
    binding_hash text NOT NULL,
    request_id text NOT NULL,
    attachment text NOT NULL,
    lifecycle text NOT NULL,
    admission_state text NOT NULL,
    termination_intent_reason text,
    termination_intent_transition_key text,
    termination_intent_at timestamp with time zone,
    input_payload_id text NOT NULL,
    output_payload_id text,
    output_artifact_id text,
    output_value_hash text,
    error_code text,
    terminal_event_id text,
    terminal_public_event_id text,
    parent_run_id text,
    lineage_kind text,
    generation bigint DEFAULT 1 NOT NULL,
    replacement_run_id text,
    next_event_seq bigint DEFAULT 1 NOT NULL,
    projection_version bigint DEFAULT 0 NOT NULL,
    scheduler_lease_epoch bigint DEFAULT 0 NOT NULL,
    scheduler_lease_owner text,
    scheduler_fencing_token text,
    scheduler_lease_expires_at timestamp with time zone,
    scheduler_heartbeat_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    started_at timestamp with time zone,
    updated_at timestamp with time zone NOT NULL,
    terminal_at timestamp with time zone,
    deadline_at timestamp with time zone,
    response_id text GENERATED ALWAYS AS (('resp_'::text || run_id)) STORED,
    artifact_reference_retention_seconds bigint DEFAULT 2592000 NOT NULL,
    CONSTRAINT workflow_runs_admission_state_check CHECK ((admission_state = ANY (ARRAY['open'::text, 'paused'::text, 'draining'::text, 'closed'::text]))),
    CONSTRAINT workflow_runs_artifact_reference_retention_seconds_check CHECK (((artifact_reference_retention_seconds >= 1) AND (artifact_reference_retention_seconds <= 315360000))),
    CONSTRAINT workflow_runs_attachment_check CHECK ((attachment = ANY (ARRAY['attached'::text, 'detached'::text]))),
    CONSTRAINT workflow_runs_binding_hash_check CHECK (((length(binding_hash) = 71) AND (binding_hash ~~ 'sha256:%'::text))),
    CONSTRAINT workflow_runs_check CHECK ((((termination_intent_reason IS NULL) AND (termination_intent_transition_key IS NULL) AND (termination_intent_at IS NULL)) OR ((termination_intent_reason IS NOT NULL) AND (termination_intent_transition_key IS NOT NULL) AND (termination_intent_at IS NOT NULL)))),
    CONSTRAINT workflow_runs_check1 CHECK ((((scheduler_lease_owner IS NULL) AND (scheduler_fencing_token IS NULL) AND (scheduler_lease_expires_at IS NULL) AND (scheduler_heartbeat_at IS NULL)) OR ((scheduler_lease_epoch > 0) AND (scheduler_lease_owner IS NOT NULL) AND (scheduler_fencing_token IS NOT NULL) AND (scheduler_lease_expires_at IS NOT NULL) AND (scheduler_heartbeat_at IS NOT NULL)))),
    CONSTRAINT workflow_runs_check2 CHECK ((((parent_run_id IS NULL) AND (lineage_kind IS NULL) AND (generation = 1)) OR ((parent_run_id IS NOT NULL) AND (lineage_kind IS NOT NULL)))),
    CONSTRAINT workflow_runs_check3 CHECK ((((lifecycle = ANY (ARRAY['created'::text, 'active'::text, 'waiting'::text])) AND (terminal_at IS NULL) AND (terminal_event_id IS NULL) AND (terminal_public_event_id IS NULL) AND (output_payload_id IS NULL) AND (output_artifact_id IS NULL) AND (output_value_hash IS NULL) AND (error_code IS NULL)) OR ((lifecycle = 'completing'::text) AND (admission_state = ANY (ARRAY['draining'::text, 'closed'::text])) AND (terminal_at IS NULL) AND (terminal_event_id IS NULL) AND (terminal_public_event_id IS NULL)) OR ((lifecycle = 'terminating'::text) AND (admission_state = ANY (ARRAY['draining'::text, 'closed'::text])) AND (termination_intent_reason IS NOT NULL) AND (terminal_at IS NULL) AND (terminal_event_id IS NULL) AND (terminal_public_event_id IS NULL)) OR ((lifecycle = 'succeeded'::text) AND (admission_state = 'closed'::text) AND (termination_intent_reason IS NULL) AND (terminal_at IS NOT NULL) AND (terminal_event_id IS NOT NULL) AND (terminal_public_event_id IS NOT NULL) AND (output_value_hash IS NOT NULL) AND ((((output_payload_id IS NOT NULL))::integer + ((output_artifact_id IS NOT NULL))::integer) = 1) AND (error_code IS NULL)) OR ((lifecycle = ANY (ARRAY['failed'::text, 'cancelled'::text, 'interrupted'::text, 'timed_out'::text])) AND (admission_state = 'closed'::text) AND (termination_intent_reason IS NOT NULL) AND (terminal_at IS NOT NULL) AND (terminal_event_id IS NOT NULL) AND (terminal_public_event_id IS NOT NULL) AND (output_payload_id IS NULL) AND (output_artifact_id IS NULL) AND (output_value_hash IS NULL) AND (error_code IS NOT NULL)))),
    CONSTRAINT workflow_runs_generation_check CHECK ((generation >= 1)),
    CONSTRAINT workflow_runs_lifecycle_check CHECK ((lifecycle = ANY (ARRAY['created'::text, 'active'::text, 'waiting'::text, 'completing'::text, 'terminating'::text, 'succeeded'::text, 'failed'::text, 'cancelled'::text, 'interrupted'::text, 'timed_out'::text]))),
    CONSTRAINT workflow_runs_lineage_kind_check CHECK (((lineage_kind IS NULL) OR (lineage_kind = ANY (ARRAY['generation'::text, 'redrive'::text, 'fork'::text, 'migrate'::text, 'subflow'::text])))),
    CONSTRAINT workflow_runs_next_event_seq_check CHECK ((next_event_seq >= 1)),
    CONSTRAINT workflow_runs_output_value_hash_check CHECK (((output_value_hash IS NULL) OR ((length(output_value_hash) = 71) AND (output_value_hash ~~ 'sha256:%'::text)))),
    CONSTRAINT workflow_runs_plan_hash_check CHECK (((length(plan_hash) = 71) AND (plan_hash ~~ 'sha256:%'::text))),
    CONSTRAINT workflow_runs_projection_version_check CHECK ((projection_version >= 0)),
    CONSTRAINT workflow_runs_request_id_check CHECK ((request_id <> ''::text)),
    CONSTRAINT workflow_runs_scheduler_lease_epoch_check CHECK ((scheduler_lease_epoch >= 0))
);

ALTER TABLE ONLY agent_publication_heads
    ADD CONSTRAINT agent_publication_heads_definition_id_key UNIQUE (definition_id);

ALTER TABLE ONLY agent_publication_heads
    ADD CONSTRAINT agent_publication_heads_pkey PRIMARY KEY (agent_id);

ALTER TABLE ONLY artifact_gc_claims
    ADD CONSTRAINT artifact_gc_claims_claim_token_key UNIQUE (claim_token);

ALTER TABLE ONLY artifact_gc_claims
    ADD CONSTRAINT artifact_gc_claims_pkey PRIMARY KEY (transition_key, run_id, artifact_id);

ALTER TABLE ONLY artifact_gc_sweeps
    ADD CONSTRAINT artifact_gc_sweeps_pkey PRIMARY KEY (transition_key);

ALTER TABLE ONLY artifact_retention_releases
    ADD CONSTRAINT artifact_retention_releases_pkey PRIMARY KEY (run_id);

ALTER TABLE ONLY artifact_retention_releases
    ADD CONSTRAINT artifact_retention_releases_transition_key_key UNIQUE (transition_key);

ALTER TABLE ONLY artifact_store_authority
    ADD CONSTRAINT artifact_store_authority_pkey PRIMARY KEY (singleton);

ALTER TABLE ONLY artifacts
    ADD CONSTRAINT artifacts_deletion_claim_token_key UNIQUE (deletion_claim_token);

ALTER TABLE ONLY artifacts
    ADD CONSTRAINT artifacts_pkey PRIMARY KEY (run_id, artifact_id);

ALTER TABLE ONLY artifacts
    ADD CONSTRAINT artifacts_run_id_content_hash_key UNIQUE (run_id, content_hash);

ALTER TABLE ONLY artifacts
    ADD CONSTRAINT artifacts_run_id_deletion_fence_key UNIQUE (run_id, deletion_fence);

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT control_tokens_pkey PRIMARY KEY (run_id, token_id);

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT control_tokens_run_id_consumed_by_transition_key_key UNIQUE (run_id, consumed_by_transition_key);

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT control_tokens_run_id_emitted_by_transition_key_emission_sl_key UNIQUE (run_id, emitted_by_transition_key, emission_slot);

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT control_tokens_run_id_source_activation_id_emission_slot_key UNIQUE (run_id, source_activation_id, emission_slot);

ALTER TABLE ONLY control_transition_results
    ADD CONSTRAINT control_transition_results_pkey PRIMARY KEY (run_id, transition_key);

ALTER TABLE ONLY control_transition_results
    ADD CONSTRAINT control_transition_results_run_id_primary_event_id_key UNIQUE (run_id, primary_event_id);

ALTER TABLE ONLY deployment_revisions
    ADD CONSTRAINT deployment_revisions_definition_id_definition_revision_id_d_key UNIQUE (definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash);

ALTER TABLE ONLY deployment_revisions
    ADD CONSTRAINT deployment_revisions_pkey PRIMARY KEY (definition_id, deployment_revision_id);

ALTER TABLE ONLY execution_events
    ADD CONSTRAINT execution_events_pkey PRIMARY KEY (run_id, seq);

ALTER TABLE ONLY execution_events
    ADD CONSTRAINT execution_events_run_id_event_id_key UNIQUE (run_id, event_id);

ALTER TABLE ONLY execution_events
    ADD CONSTRAINT execution_events_run_id_transition_key_key UNIQUE (run_id, transition_key);

ALTER TABLE ONLY fork_groups
    ADD CONSTRAINT fork_groups_pkey PRIMARY KEY (run_id, fork_group_id);

ALTER TABLE ONLY fork_legs
    ADD CONSTRAINT fork_legs_pkey PRIMARY KEY (run_id, fork_group_id, leg_id);

ALTER TABLE ONLY fork_legs
    ADD CONSTRAINT fork_legs_run_id_fork_group_id_declaration_index_key UNIQUE (run_id, fork_group_id, declaration_index);

ALTER TABLE ONLY fork_legs
    ADD CONSTRAINT fork_legs_run_id_fork_group_id_token_id_key UNIQUE (run_id, fork_group_id, token_id);

ALTER TABLE ONLY graph_view_documents
    ADD CONSTRAINT graph_view_documents_pkey PRIMARY KEY (definition_id, definition_revision_id);

ALTER TABLE ONLY human_work_items
    ADD CONSTRAINT human_work_items_pkey PRIMARY KEY (work_item_id);

ALTER TABLE ONLY human_work_items
    ADD CONSTRAINT human_work_items_run_id_activation_id_key UNIQUE (run_id, activation_id);

ALTER TABLE ONLY human_work_items
    ADD CONSTRAINT human_work_items_run_id_wait_id_key UNIQUE (run_id, wait_id);

ALTER TABLE ONLY join_arrivals
    ADD CONSTRAINT join_arrivals_pkey PRIMARY KEY (run_id, join_activation_id, fork_group_id, leg_id);

ALTER TABLE ONLY join_arrivals
    ADD CONSTRAINT join_arrivals_run_id_arrival_transition_key_key UNIQUE (run_id, arrival_transition_key);

ALTER TABLE ONLY join_arrivals
    ADD CONSTRAINT join_arrivals_run_id_fork_group_id_leg_id_key UNIQUE (run_id, fork_group_id, leg_id);

ALTER TABLE ONLY join_arrivals
    ADD CONSTRAINT join_arrivals_run_id_join_activation_id_token_id_key UNIQUE (run_id, join_activation_id, token_id);

ALTER TABLE ONLY model_call_usage
    ADD CONSTRAINT model_call_usage_pkey PRIMARY KEY (run_id, activation_id, attempt_no, model_call_no);

ALTER TABLE ONLY model_tool_call_batches
    ADD CONSTRAINT model_tool_call_batches_pkey PRIMARY KEY (run_id, activation_id, attempt_no, model_call_no);

ALTER TABLE ONLY model_tool_calls
    ADD CONSTRAINT model_tool_calls_effect_id_key UNIQUE (effect_id);

ALTER TABLE ONLY model_tool_calls
    ADD CONSTRAINT model_tool_calls_pkey PRIMARY KEY (run_id, activation_id, attempt_no, model_call_no, call_index);

ALTER TABLE ONLY model_tool_calls
    ADD CONSTRAINT model_tool_calls_run_id_activation_id_attempt_no_model_call_key UNIQUE (run_id, activation_id, attempt_no, model_call_no, call_id);

ALTER TABLE ONLY model_tool_calls
    ADD CONSTRAINT model_tool_calls_tool_task_id_key UNIQUE (tool_task_id);

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT node_activations_pkey PRIMARY KEY (run_id, activation_id);

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT node_activations_run_id_activation_id_effect_id_key UNIQUE (run_id, activation_id, effect_id);

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT node_activations_run_id_effect_id_key UNIQUE (run_id, effect_id);

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT node_activations_run_id_scope_instance_id_node_id_stable_ac_key UNIQUE (run_id, scope_instance_id, node_id, stable_activation_key);

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT node_activations_run_id_wait_registration_transition_key_key UNIQUE (run_id, wait_registration_transition_key);

ALTER TABLE ONLY node_attempts
    ADD CONSTRAINT node_attempts_pkey PRIMARY KEY (run_id, activation_id, attempt_no);

ALTER TABLE ONLY node_attempts
    ADD CONSTRAINT node_attempts_run_id_activation_id_attempt_no_lease_epoch_f_key UNIQUE (run_id, activation_id, attempt_no, lease_epoch, fencing_token);

ALTER TABLE ONLY node_attempts
    ADD CONSTRAINT node_attempts_run_id_activation_id_lease_epoch_key UNIQUE (run_id, activation_id, lease_epoch);

ALTER TABLE ONLY node_attempts
    ADD CONSTRAINT node_attempts_run_id_completion_transition_key_key UNIQUE (run_id, completion_transition_key);

ALTER TABLE ONLY node_attempts
    ADD CONSTRAINT node_attempts_run_id_fencing_token_key UNIQUE (run_id, fencing_token);

ALTER TABLE ONLY payloads
    ADD CONSTRAINT payloads_pkey PRIMARY KEY (run_id, payload_id);

ALTER TABLE ONLY payloads
    ADD CONSTRAINT payloads_run_id_content_hash_key UNIQUE (run_id, content_hash);

ALTER TABLE ONLY projection_checkpoint_batches
    ADD CONSTRAINT projection_checkpoint_batches_pkey PRIMARY KEY (run_id, event_id);

ALTER TABLE ONLY projection_checkpoints
    ADD CONSTRAINT projection_checkpoints_pkey PRIMARY KEY (run_id, event_id, subject_kind, subject_id);

ALTER TABLE ONLY projection_checkpoints
    ADD CONSTRAINT projection_checkpoints_run_id_subject_kind_subject_id_subje_key UNIQUE (run_id, subject_kind, subject_id, subject_projection_version);

ALTER TABLE ONLY public_event_delivery_heads
    ADD CONSTRAINT public_event_delivery_heads_pkey PRIMARY KEY (run_id);

ALTER TABLE ONLY public_event_delivery_heads
    ADD CONSTRAINT public_event_delivery_heads_public_event_id_key UNIQUE (public_event_id);

ALTER TABLE ONLY public_event_outbox
    ADD CONSTRAINT public_event_outbox_pkey PRIMARY KEY (run_id, public_event_id);

ALTER TABLE ONLY public_event_outbox
    ADD CONSTRAINT public_event_outbox_run_id_causation_event_id_event_kind_key UNIQUE (run_id, causation_event_id, event_kind);

ALTER TABLE ONLY public_event_outbox
    ADD CONSTRAINT public_event_outbox_run_id_causation_event_id_public_ordina_key UNIQUE (run_id, causation_event_id, public_ordinal);

ALTER TABLE ONLY public_event_projection_decisions
    ADD CONSTRAINT public_event_projection_decisions_pkey PRIMARY KEY (run_id, execution_event_id);

ALTER TABLE ONLY public_event_projection_decisions
    ADD CONSTRAINT public_event_projection_decisions_public_event_id_key UNIQUE (public_event_id);

ALTER TABLE ONLY public_event_receipts
    ADD CONSTRAINT public_event_receipts_pkey PRIMARY KEY (run_id, public_event_id);

ALTER TABLE ONLY public_event_receipts
    ADD CONSTRAINT public_event_receipts_public_event_id_key UNIQUE (public_event_id);

ALTER TABLE ONLY public_event_receipts
    ADD CONSTRAINT public_event_receipts_run_id_causation_event_id_event_kind_key UNIQUE (run_id, causation_event_id, event_kind);

ALTER TABLE ONLY public_event_receipts
    ADD CONSTRAINT public_event_receipts_run_id_causation_event_id_public_ordi_key UNIQUE (run_id, causation_event_id, public_ordinal);

ALTER TABLE ONLY recovery_artifact_roots
    ADD CONSTRAINT recovery_artifact_roots_pkey PRIMARY KEY (run_id, artifact_run_id, artifact_id);

ALTER TABLE ONLY recovery_effect_roots
    ADD CONSTRAINT recovery_effect_roots_pkey PRIMARY KEY (run_id, effect_run_id, source_activation_id, effect_id);

ALTER TABLE ONLY recovery_revision_roots
    ADD CONSTRAINT recovery_revision_roots_pkey PRIMARY KEY (run_id, root_role);

ALTER TABLE ONLY recovery_transition_results
    ADD CONSTRAINT recovery_transition_results_pkey PRIMARY KEY (authority_run_id, transition_key);

ALTER TABLE ONLY recovery_transition_results
    ADD CONSTRAINT recovery_transition_results_primary_event_run_id_primary_ev_key UNIQUE (primary_event_run_id, primary_event_id);

ALTER TABLE ONLY response_public_items
    ADD CONSTRAINT response_public_items_pkey PRIMARY KEY (run_id, activation_id, attempt_no, model_call_no, item_ordinal);

ALTER TABLE ONLY response_public_items
    ADD CONSTRAINT response_public_items_run_id_item_id_key UNIQUE (run_id, item_id);

ALTER TABLE ONLY response_public_items
    ADD CONSTRAINT response_public_items_run_id_output_index_key UNIQUE (run_id, output_index);

ALTER TABLE ONLY response_snapshots
    ADD CONSTRAINT response_snapshots_pkey PRIMARY KEY (run_id);

ALTER TABLE ONLY response_snapshots
    ADD CONSTRAINT response_snapshots_response_id_key UNIQUE (response_id);

ALTER TABLE ONLY run_migration_intents
    ADD CONSTRAINT run_migration_intents_final_transition_key_key UNIQUE (final_transition_key);

ALTER TABLE ONLY run_migration_intents
    ADD CONSTRAINT run_migration_intents_intent_transition_key_key UNIQUE (intent_transition_key);

ALTER TABLE ONLY run_migration_intents
    ADD CONSTRAINT run_migration_intents_pkey PRIMARY KEY (run_id);

ALTER TABLE ONLY run_migration_intents
    ADD CONSTRAINT run_migration_intents_target_run_id_key UNIQUE (target_run_id);

ALTER TABLE ONLY run_recovery_lineage
    ADD CONSTRAINT run_recovery_lineage_pkey PRIMARY KEY (run_id);

ALTER TABLE ONLY run_recovery_lineage
    ADD CONSTRAINT run_recovery_lineage_source_run_id_run_id_lineage_kind_key UNIQUE (source_run_id, run_id, lineage_kind);

ALTER TABLE ONLY run_reuse_candidates
    ADD CONSTRAINT run_reuse_candidates_pkey PRIMARY KEY (run_id, candidate_id);

ALTER TABLE ONLY run_reuse_candidates
    ADD CONSTRAINT run_reuse_candidates_run_id_created_by_transition_key_key UNIQUE (run_id, created_by_transition_key);

ALTER TABLE ONLY run_reuse_candidates
    ADD CONSTRAINT run_reuse_candidates_run_id_decision_transition_key_key UNIQUE (run_id, decision_transition_key);

ALTER TABLE ONLY run_reuse_candidates
    ADD CONSTRAINT run_reuse_candidates_run_id_target_scope_instance_id_target_key UNIQUE (run_id, target_scope_instance_id, target_node_id, stable_activation_key);

ALTER TABLE ONLY scheduler_checkpoints
    ADD CONSTRAINT scheduler_checkpoints_pkey PRIMARY KEY (run_id, checkpoint_id);

ALTER TABLE ONLY scheduler_checkpoints
    ADD CONSTRAINT scheduler_checkpoints_run_id_content_hash_key UNIQUE (run_id, content_hash);

ALTER TABLE ONLY scheduler_checkpoints
    ADD CONSTRAINT scheduler_checkpoints_run_id_event_id_key UNIQUE (run_id, event_id);

ALTER TABLE ONLY scheduler_checkpoints
    ADD CONSTRAINT scheduler_checkpoints_run_id_transition_key_key UNIQUE (run_id, transition_key);

ALTER TABLE ONLY scheduler_occurrence_values
    ADD CONSTRAINT scheduler_occurrence_values_pkey PRIMARY KEY (run_id, occurrence_key, port_id);

ALTER TABLE ONLY scheduler_subflow_invocations
    ADD CONSTRAINT scheduler_subflow_invocations_pkey PRIMARY KEY (run_id, child_run_id);

ALTER TABLE ONLY scheduler_subflow_invocations
    ADD CONSTRAINT scheduler_subflow_invocations_run_id_parent_activation_id_key UNIQUE (run_id, parent_activation_id);

ALTER TABLE ONLY scheduler_values
    ADD CONSTRAINT scheduler_values_pkey PRIMARY KEY (run_id, port_id);

ALTER TABLE ONLY scheduler_wait_registrations
    ADD CONSTRAINT scheduler_wait_registrations_pkey PRIMARY KEY (run_id, wait_id);

ALTER TABLE ONLY scheduler_wait_registrations
    ADD CONSTRAINT scheduler_wait_registrations_run_id_activation_id_key UNIQUE (run_id, activation_id);

ALTER TABLE ONLY scope_instances
    ADD CONSTRAINT scope_instances_pkey PRIMARY KEY (run_id, scope_instance_id);

ALTER TABLE ONLY scope_instances
    ADD CONSTRAINT scope_instances_run_id_parent_scope_instance_id_static_scop_key UNIQUE (run_id, parent_scope_instance_id, static_scope_id, stable_dynamic_key);

ALTER TABLE ONLY signals_inbox
    ADD CONSTRAINT signals_inbox_pkey PRIMARY KEY (run_id, signal_id);

ALTER TABLE ONLY signals_inbox
    ADD CONSTRAINT signals_inbox_run_id_consumed_by_transition_key_key UNIQUE (run_id, consumed_by_transition_key);

ALTER TABLE ONLY signals_inbox
    ADD CONSTRAINT signals_inbox_run_id_message_id_key UNIQUE (run_id, message_id);

ALTER TABLE ONLY task_outbox
    ADD CONSTRAINT task_outbox_pkey PRIMARY KEY (run_id, task_id);

ALTER TABLE ONLY task_outbox
    ADD CONSTRAINT task_outbox_run_id_activation_id_attempt_no_lease_epoch_key UNIQUE (run_id, activation_id, attempt_no, lease_epoch);

ALTER TABLE ONLY task_outbox
    ADD CONSTRAINT task_outbox_run_id_created_by_transition_key_key UNIQUE (run_id, created_by_transition_key);

ALTER TABLE ONLY timers
    ADD CONSTRAINT timers_pkey PRIMARY KEY (run_id, timer_id);

ALTER TABLE ONLY timers
    ADD CONSTRAINT timers_run_id_created_by_transition_key_key UNIQUE (run_id, created_by_transition_key);

ALTER TABLE ONLY timers
    ADD CONSTRAINT timers_run_id_fired_by_transition_key_key UNIQUE (run_id, fired_by_transition_key);

ALTER TABLE ONLY public_event_receipts
    ADD CONSTRAINT uq_public_receipt_causation UNIQUE (run_id, causation_event_id);

ALTER TABLE ONLY workflow_definition_public_metadata
    ADD CONSTRAINT workflow_definition_public_metadata_pkey PRIMARY KEY (definition_id, definition_revision_id);

ALTER TABLE ONLY workflow_definition_revisions
    ADD CONSTRAINT workflow_definition_revisions_definition_id_definition_revi_key UNIQUE (definition_id, definition_revision_id, plan_hash);

ALTER TABLE ONLY workflow_definition_revisions
    ADD CONSTRAINT workflow_definition_revisions_pkey PRIMARY KEY (definition_id, definition_revision_id);

ALTER TABLE ONLY workflow_definitions
    ADD CONSTRAINT workflow_definitions_pkey PRIMARY KEY (definition_id);

ALTER TABLE ONLY workflow_retrieval_publications
    ADD CONSTRAINT workflow_retrieval_publicatio_run_id_completion_transition__key UNIQUE (run_id, completion_transition_key);

ALTER TABLE ONLY workflow_retrieval_publications
    ADD CONSTRAINT workflow_retrieval_publications_pkey PRIMARY KEY (run_id, retrieval_id);

ALTER TABLE ONLY workflow_retrieval_publications
    ADD CONSTRAINT workflow_retrieval_publications_run_id_activation_id_key UNIQUE (run_id, activation_id);

ALTER TABLE ONLY workflow_retrieval_publications
    ADD CONSTRAINT workflow_retrieval_publications_run_id_completion_event_id_key UNIQUE (run_id, completion_event_id);

ALTER TABLE ONLY workflow_retrieval_publications
    ADD CONSTRAINT workflow_retrieval_publications_run_id_task_id_key UNIQUE (run_id, task_id);

ALTER TABLE ONLY workflow_runs
    ADD CONSTRAINT workflow_runs_pkey PRIMARY KEY (run_id);

CREATE INDEX idx_activation_dispatch ON node_activations USING btree (run_id, lifecycle, scope_instance_id, activation_id) WHERE (lifecycle = ANY (ARRAY['ready'::text, 'retry_wait'::text, 'waiting'::text]));

CREATE INDEX idx_activation_live_fence ON node_activations USING btree (run_id, activation_id, current_attempt_no, current_lease_epoch) WHERE (current_attempt_no IS NOT NULL);

CREATE INDEX idx_artifact_orphan_sweep ON artifacts USING btree (artifact_state, created_at, retain_until, run_id, artifact_id) WHERE (artifact_state = ANY (ARRAY['staged'::text, 'verified'::text, 'deleting'::text]));

CREATE INDEX idx_artifact_retention_due ON artifact_retention_releases USING btree (retain_until, run_id);

CREATE INDEX idx_attempt_lease_expiry ON node_attempts USING btree (lease_expires_at, run_id, activation_id, attempt_no) WHERE (lifecycle = ANY (ARRAY['leased'::text, 'running'::text]));

CREATE INDEX idx_control_tokens_available ON control_tokens USING btree (run_id, current_scope_instance_id, current_port_id, token_id) WHERE (token_state = 'available'::text);

CREATE INDEX idx_execution_events_activation ON execution_events USING btree (run_id, activation_id, seq) WHERE (activation_id IS NOT NULL);

CREATE INDEX idx_execution_events_rebuild ON execution_events USING btree (run_id, seq);

CREATE INDEX idx_graph_views_document ON graph_view_documents USING btree (graph_document_id);

CREATE INDEX idx_human_work_items_queue ON human_work_items USING btree (work_state, claim_expires_at, created_at, work_item_id);

CREATE INDEX idx_human_work_items_run ON human_work_items USING btree (run_id, work_state, work_item_id);

CREATE INDEX idx_model_tool_batches_parent_deadline ON model_tool_call_batches USING btree (execution_status, continuation_status, parent_operation_deadline);

CREATE INDEX idx_model_tool_calls_batch_status ON model_tool_calls USING btree (run_id, activation_id, attempt_no, model_call_no, call_status);

CREATE INDEX idx_model_tool_calls_claim ON model_tool_calls USING btree (available_at, run_id, tool_task_id) WHERE (call_status = 'pending'::text);

CREATE INDEX idx_model_tool_calls_reclaim ON model_tool_calls USING btree (claim_expires_at, run_id, activation_id, attempt_no, model_call_no, call_index) WHERE (call_status = ANY (ARRAY['claimed'::text, 'running'::text]));

CREATE INDEX idx_occurrence_values_owner ON scheduler_occurrence_values USING btree (run_id, owner_activation_id, occurrence_key, port_id);

CREATE INDEX idx_projection_checkpoint_rebuild ON projection_checkpoints USING btree (run_id, subject_kind, subject_id, subject_projection_version);

CREATE INDEX idx_public_delivery_heads_due ON public_event_delivery_heads USING btree (due_at, run_id, execution_seq, public_ordinal, public_event_id) WHERE (head_state = 'ready'::text);

CREATE INDEX idx_public_outbox_dispatch ON public_event_outbox USING btree (available_at, run_id, public_ordinal, public_event_id) WHERE (publish_state = 'pending'::text);

CREATE INDEX idx_public_outbox_reclaim ON public_event_outbox USING btree (claim_expires_at, run_id, public_ordinal, public_event_id) WHERE (publish_state = 'claimed'::text);

CREATE INDEX idx_public_outbox_retention ON public_event_outbox USING btree (retain_until, run_id, public_event_id) WHERE ((publish_state = 'published'::text) AND (NOT is_terminal) AND (retain_until IS NOT NULL));

CREATE INDEX idx_public_projection_order ON public_event_projection_decisions USING btree (run_id, execution_seq, public_ordinal, public_event_id) WHERE (decision = 'public'::text);

CREATE INDEX idx_reuse_candidates_pending ON run_reuse_candidates USING btree (run_id, target_scope_instance_id, target_node_id, candidate_id) WHERE (candidate_state = 'candidate'::text);

CREATE INDEX idx_run_deadline_due ON workflow_runs USING btree (deadline_at, run_id) WHERE ((deadline_at IS NOT NULL) AND (lifecycle = ANY (ARRAY['created'::text, 'active'::text, 'waiting'::text, 'completing'::text])));

CREATE INDEX idx_runs_dispatch ON workflow_runs USING btree (lifecycle, admission_state, updated_at, run_id) WHERE (lifecycle = ANY (ARRAY['active'::text, 'waiting'::text]));

CREATE INDEX idx_runs_recovery ON workflow_runs USING btree (updated_at, run_id) WHERE ((lifecycle = 'terminating'::text) OR ((lifecycle = ANY (ARRAY['created'::text, 'active'::text, 'waiting'::text])) AND (admission_state = 'open'::text)));

CREATE INDEX idx_runs_scheduler_lease ON workflow_runs USING btree (scheduler_lease_expires_at, run_id) WHERE (scheduler_lease_owner IS NOT NULL);

CREATE INDEX idx_scheduler_checkpoints_recovery ON scheduler_checkpoints USING btree (run_id, scheduler_projection_version, checkpoint_id);

CREATE INDEX idx_scheduler_values_owner ON scheduler_values USING btree (run_id, owner_activation_id, port_id);

CREATE INDEX idx_scope_parent ON scope_instances USING btree (run_id, parent_scope_instance_id, scope_instance_id);

CREATE INDEX idx_signals_pending ON signals_inbox USING btree (run_id, target_activation_id, received_at, signal_id) WHERE (signal_state = 'pending'::text);

CREATE INDEX idx_task_outbox_dispatch ON task_outbox USING btree (available_at, run_id, task_id) WHERE (task_state = 'pending'::text);

CREATE INDEX idx_task_outbox_acknowledge ON task_outbox USING btree (available_at, run_id, task_id) WHERE (task_state = 'published'::text);

CREATE INDEX idx_task_outbox_reclaim ON task_outbox USING btree (claim_expires_at, run_id, task_id) WHERE (task_state = 'claimed'::text);

CREATE INDEX idx_timers_due ON timers USING btree (deadline_at, run_id, timer_id) WHERE (timer_state = 'scheduled'::text);

CREATE INDEX idx_workflow_retrieval_publications_terminal ON workflow_retrieval_publications USING btree (run_id, activation_id, attempt_no, retrieval_id);

CREATE INDEX idx_wait_late_audit_pending ON wait_late_audit_outbox USING btree (due_at, run_id, loser_kind, loser_id) WHERE (audit_state = 'pending'::text);

CREATE INDEX idx_wait_late_audit_reclaim ON wait_late_audit_outbox USING btree (claim_expires_at, run_id, loser_kind, loser_id) WHERE (audit_state = 'claimed'::text);

CREATE UNIQUE INDEX uq_attempt_lease_timer ON timers USING btree (run_id, activation_id, expected_attempt_no, expected_lease_epoch) WHERE (timer_kind = 'lease'::text);

CREATE UNIQUE INDEX uq_public_outbox_claim_token ON public_event_outbox USING btree (claim_token) WHERE (claim_token IS NOT NULL);

CREATE UNIQUE INDEX uq_public_outbox_public_event_id ON public_event_outbox USING btree (public_event_id);

CREATE UNIQUE INDEX uq_public_outbox_published_claim_token ON public_event_outbox USING btree (published_claim_token) WHERE (published_claim_token IS NOT NULL);

CREATE UNIQUE INDEX uq_public_terminal_per_run ON public_event_outbox USING btree (run_id) WHERE is_terminal;

CREATE UNIQUE INDEX uq_root_scope_per_run ON scope_instances USING btree (run_id) WHERE is_root;

CREATE UNIQUE INDEX uq_workflow_runs_response_id ON workflow_runs USING btree (response_id);

CREATE UNIQUE INDEX uq_wait_late_audit_claim_token ON wait_late_audit_outbox USING btree (claim_token) WHERE (claim_token IS NOT NULL);

CREATE TRIGGER artifact_retention_release_delete_forbidden BEFORE DELETE ON artifact_retention_releases FOR EACH ROW EXECUTE FUNCTION reject_durable_authority_delete();

CREATE TRIGGER artifact_store_authority_immutable BEFORE DELETE OR UPDATE ON artifact_store_authority FOR EACH ROW EXECUTE FUNCTION reject_artifact_store_authority_mutation();

CREATE TRIGGER close_human_work_items_on_run_state AFTER UPDATE OF lifecycle, termination_intent_reason ON workflow_runs FOR EACH ROW EXECUTE FUNCTION close_human_work_items_for_run();

CREATE TRIGGER control_transition_result_delete_forbidden BEFORE DELETE ON control_transition_results FOR EACH ROW EXECUTE FUNCTION reject_durable_authority_delete();

CREATE TRIGGER control_transition_result_rewrite_forbidden BEFORE UPDATE ON control_transition_results FOR EACH ROW EXECUTE FUNCTION reject_transition_receipt_rewrite();

CREATE TRIGGER execution_event_projection_ledger_immutable BEFORE INSERT OR DELETE OR UPDATE ON execution_events FOR EACH ROW EXECUTE FUNCTION reject_execution_event_projection_ledger_rewrite();

CREATE TRIGGER execution_event_public_projection_decision_insert AFTER INSERT ON execution_events FOR EACH ROW EXECUTE FUNCTION insert_public_projection_decision();

CREATE TRIGGER model_tool_call_work_wakeup AFTER INSERT OR UPDATE OF call_status, available_at, claim_expires_at ON model_tool_calls FOR EACH ROW WHEN ((NEW.call_status = ANY (ARRAY['pending'::text, 'claimed'::text, 'running'::text]))) EXECUTE FUNCTION notify_durable_work();

CREATE TRIGGER public_event_delivery_work_wakeup AFTER INSERT OR UPDATE OF head_state, due_at ON public_event_delivery_heads FOR EACH ROW WHEN (NEW.head_state = 'ready'::text) EXECUTE FUNCTION notify_durable_work();

CREATE TRIGGER public_event_delivery_head_mutation_guard BEFORE INSERT OR DELETE OR UPDATE ON public_event_delivery_heads FOR EACH ROW EXECUTE FUNCTION guard_public_event_delivery_head();

CREATE TRIGGER public_event_outbox_authority_insert AFTER INSERT ON public_event_outbox FOR EACH ROW EXECUTE FUNCTION establish_public_event_authority();

CREATE TRIGGER public_event_outbox_delete_forbidden BEFORE DELETE ON public_event_outbox FOR EACH ROW EXECUTE FUNCTION permit_expired_public_event_prune();

CREATE TRIGGER public_event_outbox_delivery_head_update AFTER UPDATE OF publish_state, claim_expires_at ON public_event_outbox FOR EACH ROW EXECUTE FUNCTION synchronize_public_event_delivery_head();

CREATE TRIGGER public_event_outbox_update_contract BEFORE UPDATE ON public_event_outbox FOR EACH ROW EXECUTE FUNCTION enforce_public_event_outbox_update_contract();

CREATE TRIGGER public_event_projection_decision_mutation_guard BEFORE DELETE OR UPDATE ON public_event_projection_decisions FOR EACH ROW EXECUTE FUNCTION guard_public_projection_decision_mutation();

CREATE TRIGGER public_event_receipt_delete_forbidden BEFORE DELETE ON public_event_receipts FOR EACH ROW EXECUTE FUNCTION reject_public_event_receipt_mutation();

CREATE TRIGGER public_event_receipt_insert_provenance BEFORE INSERT ON public_event_receipts FOR EACH ROW EXECUTE FUNCTION guard_public_event_receipt_provenance();

CREATE TRIGGER public_event_receipt_update_forbidden BEFORE UPDATE ON public_event_receipts FOR EACH ROW EXECUTE FUNCTION reject_public_event_receipt_mutation();

CREATE TRIGGER recovery_transition_result_delete_forbidden BEFORE DELETE ON recovery_transition_results FOR EACH ROW EXECUTE FUNCTION reject_durable_authority_delete();

CREATE TRIGGER recovery_transition_result_rewrite_forbidden BEFORE UPDATE ON recovery_transition_results FOR EACH ROW EXECUTE FUNCTION reject_transition_receipt_rewrite();

CREATE TRIGGER run_recovery_work_wakeup AFTER INSERT OR UPDATE OF lifecycle, admission_state, projection_version ON workflow_runs FOR EACH ROW WHEN ((NEW.lifecycle = 'terminating'::text) OR ((NEW.lifecycle = ANY (ARRAY['created'::text, 'active'::text, 'waiting'::text])) AND (NEW.admission_state = 'open'::text))) EXECUTE FUNCTION notify_durable_work();

CREATE TRIGGER signal_ingress_work_wakeup AFTER INSERT OR UPDATE OF signal_state ON signals_inbox FOR EACH ROW WHEN (NEW.signal_state = 'pending'::text) EXECUTE FUNCTION notify_durable_work();

CREATE TRIGGER task_outbox_work_wakeup AFTER INSERT OR UPDATE OF task_state, available_at, claim_expires_at ON task_outbox FOR EACH ROW WHEN ((NEW.task_state = ANY (ARRAY['pending'::text, 'claimed'::text, 'published'::text]))) EXECUTE FUNCTION notify_durable_work();

CREATE TRIGGER timer_ingress_work_wakeup AFTER INSERT OR UPDATE OF timer_state, deadline_at ON timers FOR EACH ROW WHEN (NEW.timer_state = 'scheduled'::text) EXECUTE FUNCTION notify_durable_work();

CREATE TRIGGER wait_late_audit_work_wakeup AFTER INSERT OR UPDATE OF audit_state, due_at, claim_expires_at ON wait_late_audit_outbox FOR EACH ROW WHEN ((NEW.audit_state = ANY (ARRAY['pending'::text, 'claimed'::text]))) EXECUTE FUNCTION notify_durable_work();

CREATE TRIGGER trg_definition_public_metadata_immutable BEFORE DELETE OR UPDATE ON workflow_definition_public_metadata FOR EACH ROW EXECUTE FUNCTION durable_reject_definition_public_metadata_mutation();

CREATE TRIGGER trg_deployment_revision_immutable BEFORE UPDATE ON deployment_revisions FOR EACH ROW EXECUTE FUNCTION durable_reject_deployment_revision_update();

CREATE TRIGGER trg_publication_head_agent_matches_definition BEFORE INSERT OR UPDATE ON agent_publication_heads FOR EACH ROW EXECUTE FUNCTION durable_validate_publication_head();

CREATE TRIGGER trg_published_revision_immutable BEFORE DELETE OR UPDATE ON workflow_definition_revisions FOR EACH ROW EXECUTE FUNCTION durable_reject_published_revision_mutation();

CREATE TRIGGER workflow_retrieval_publication_immutable BEFORE DELETE OR UPDATE ON workflow_retrieval_publications FOR EACH ROW EXECUTE FUNCTION forbid_workflow_retrieval_publication_rewrite();

ALTER TABLE ONLY agent_publication_heads
    ADD CONSTRAINT agent_publication_heads_definition_id_deployment_revision__fkey FOREIGN KEY (definition_id, deployment_revision_id) REFERENCES deployment_revisions(definition_id, deployment_revision_id) ON DELETE RESTRICT;

ALTER TABLE ONLY artifact_gc_claims
    ADD CONSTRAINT artifact_gc_claims_run_id_artifact_id_fkey FOREIGN KEY (run_id, artifact_id) REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT;

ALTER TABLE ONLY artifact_gc_claims
    ADD CONSTRAINT artifact_gc_claims_transition_key_fkey FOREIGN KEY (transition_key) REFERENCES artifact_gc_sweeps(transition_key) ON DELETE RESTRICT;

ALTER TABLE ONLY artifact_retention_releases
    ADD CONSTRAINT artifact_retention_releases_run_id_event_id_fkey FOREIGN KEY (run_id, event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY artifact_retention_releases
    ADD CONSTRAINT artifact_retention_releases_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY artifacts
    ADD CONSTRAINT artifacts_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT control_tokens_run_id_branch_activation_id_fkey FOREIGN KEY (run_id, branch_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT control_tokens_run_id_consumed_by_activation_id_fkey FOREIGN KEY (run_id, consumed_by_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT control_tokens_run_id_current_scope_instance_id_fkey FOREIGN KEY (run_id, current_scope_instance_id) REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT;

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT control_tokens_run_id_emitted_by_transition_key_fkey FOREIGN KEY (run_id, emitted_by_transition_key) REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT;

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT control_tokens_run_id_fork_group_id_fkey FOREIGN KEY (run_id, fork_group_id) REFERENCES fork_groups(run_id, fork_group_id) ON DELETE RESTRICT;

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT control_tokens_run_id_source_activation_id_fkey FOREIGN KEY (run_id, source_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY control_transition_results
    ADD CONSTRAINT control_transition_results_run_id_primary_event_id_fkey FOREIGN KEY (run_id, primary_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY deployment_revisions
    ADD CONSTRAINT deployment_revisions_definition_id_definition_revision_id__fkey FOREIGN KEY (definition_id, definition_revision_id, plan_hash) REFERENCES workflow_definition_revisions(definition_id, definition_revision_id, plan_hash) ON DELETE RESTRICT;

ALTER TABLE ONLY execution_events
    ADD CONSTRAINT execution_events_run_id_activation_id_attempt_no_fkey FOREIGN KEY (run_id, activation_id, attempt_no) REFERENCES node_attempts(run_id, activation_id, attempt_no) ON DELETE RESTRICT;

ALTER TABLE ONLY execution_events
    ADD CONSTRAINT execution_events_run_id_activation_id_fkey FOREIGN KEY (run_id, activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY execution_events
    ADD CONSTRAINT execution_events_run_id_causation_event_id_fkey FOREIGN KEY (run_id, causation_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY execution_events
    ADD CONSTRAINT execution_events_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY execution_events
    ADD CONSTRAINT execution_events_run_id_scope_instance_id_fkey FOREIGN KEY (run_id, scope_instance_id) REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT;

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT fk_activation_current_attempt FOREIGN KEY (run_id, activation_id, current_attempt_no, current_lease_epoch, current_fencing_token) REFERENCES node_attempts(run_id, activation_id, attempt_no, lease_epoch, fencing_token) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT fk_activation_retry_timer FOREIGN KEY (run_id, pending_retry_timer_id) REFERENCES timers(run_id, timer_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT fk_activation_winning_attempt FOREIGN KEY (run_id, activation_id, winning_attempt_no) REFERENCES node_attempts(run_id, activation_id, attempt_no) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY node_attempts
    ADD CONSTRAINT fk_attempt_terminal_event FOREIGN KEY (run_id, terminal_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY workflow_runs
    ADD CONSTRAINT fk_run_input_payload FOREIGN KEY (run_id, input_payload_id) REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY workflow_runs
    ADD CONSTRAINT fk_run_output_artifact FOREIGN KEY (run_id, output_artifact_id) REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY workflow_runs
    ADD CONSTRAINT fk_run_output_payload FOREIGN KEY (run_id, output_payload_id) REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY workflow_runs
    ADD CONSTRAINT fk_run_terminal_event FOREIGN KEY (run_id, terminal_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY workflow_runs
    ADD CONSTRAINT fk_run_terminal_public_event FOREIGN KEY (run_id, terminal_public_event_id) REFERENCES public_event_outbox(run_id, public_event_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY control_tokens
    ADD CONSTRAINT fk_token_fork_leg FOREIGN KEY (run_id, fork_group_id, fork_leg_id) REFERENCES fork_legs(run_id, fork_group_id, leg_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY fork_groups
    ADD CONSTRAINT fork_groups_run_id_fork_activation_id_fkey FOREIGN KEY (run_id, fork_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY fork_groups
    ADD CONSTRAINT fork_groups_run_id_join_activation_id_fkey FOREIGN KEY (run_id, join_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY fork_groups
    ADD CONSTRAINT fork_groups_run_id_parent_scope_instance_id_fkey FOREIGN KEY (run_id, parent_scope_instance_id) REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT;

ALTER TABLE ONLY fork_legs
    ADD CONSTRAINT fork_legs_run_id_child_activation_id_fkey FOREIGN KEY (run_id, child_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY fork_legs
    ADD CONSTRAINT fork_legs_run_id_fork_group_id_fkey FOREIGN KEY (run_id, fork_group_id) REFERENCES fork_groups(run_id, fork_group_id) ON DELETE RESTRICT;

ALTER TABLE ONLY fork_legs
    ADD CONSTRAINT fork_legs_run_id_scope_instance_id_fkey FOREIGN KEY (run_id, scope_instance_id) REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT;

ALTER TABLE ONLY fork_legs
    ADD CONSTRAINT fork_legs_run_id_token_id_fkey FOREIGN KEY (run_id, token_id) REFERENCES control_tokens(run_id, token_id) ON DELETE RESTRICT;

ALTER TABLE ONLY graph_view_documents
    ADD CONSTRAINT graph_view_documents_definition_id_definition_revision_id_fkey FOREIGN KEY (definition_id, definition_revision_id) REFERENCES workflow_definition_revisions(definition_id, definition_revision_id) ON DELETE RESTRICT;

ALTER TABLE ONLY human_work_items
    ADD CONSTRAINT human_work_items_run_id_wait_id_fkey FOREIGN KEY (run_id, wait_id) REFERENCES scheduler_wait_registrations(run_id, wait_id) ON DELETE RESTRICT;

ALTER TABLE ONLY join_arrivals
    ADD CONSTRAINT join_arrivals_run_id_arrival_event_id_fkey FOREIGN KEY (run_id, arrival_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY join_arrivals
    ADD CONSTRAINT join_arrivals_run_id_fork_group_id_leg_id_fkey FOREIGN KEY (run_id, fork_group_id, leg_id) REFERENCES fork_legs(run_id, fork_group_id, leg_id) ON DELETE RESTRICT;

ALTER TABLE ONLY join_arrivals
    ADD CONSTRAINT join_arrivals_run_id_join_activation_id_fkey FOREIGN KEY (run_id, join_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY join_arrivals
    ADD CONSTRAINT join_arrivals_run_id_token_id_fkey FOREIGN KEY (run_id, token_id) REFERENCES control_tokens(run_id, token_id) ON DELETE RESTRICT;

ALTER TABLE ONLY join_arrivals
    ADD CONSTRAINT join_arrivals_run_id_value_artifact_id_fkey FOREIGN KEY (run_id, value_artifact_id) REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT;

ALTER TABLE ONLY join_arrivals
    ADD CONSTRAINT join_arrivals_run_id_value_payload_id_fkey FOREIGN KEY (run_id, value_payload_id) REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT;

ALTER TABLE ONLY model_call_usage
    ADD CONSTRAINT model_call_usage_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY model_tool_call_batches
    ADD CONSTRAINT model_tool_call_batches_run_id_activation_id_attempt_no_mo_fkey FOREIGN KEY (run_id, activation_id, attempt_no, model_call_no) REFERENCES model_call_usage(run_id, activation_id, attempt_no, model_call_no) ON DELETE RESTRICT;

ALTER TABLE ONLY model_tool_call_batches
    ADD CONSTRAINT model_tool_call_batches_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY model_tool_calls
    ADD CONSTRAINT model_tool_calls_run_id_activation_id_attempt_no_model_cal_fkey FOREIGN KEY (run_id, activation_id, attempt_no, model_call_no) REFERENCES model_tool_call_batches(run_id, activation_id, attempt_no, model_call_no) ON DELETE RESTRICT;

ALTER TABLE ONLY model_tool_calls
    ADD CONSTRAINT model_tool_calls_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT node_activations_reused_from_run_id_reused_from_activation_fkey FOREIGN KEY (reused_from_run_id, reused_from_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT node_activations_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT node_activations_run_id_output_artifact_id_fkey FOREIGN KEY (run_id, output_artifact_id) REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT;

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT node_activations_run_id_output_payload_id_fkey FOREIGN KEY (run_id, output_payload_id) REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT;

ALTER TABLE ONLY node_activations
    ADD CONSTRAINT node_activations_run_id_scope_instance_id_fkey FOREIGN KEY (run_id, scope_instance_id) REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT;

ALTER TABLE ONLY node_attempts
    ADD CONSTRAINT node_attempts_run_id_activation_id_effect_id_fkey FOREIGN KEY (run_id, activation_id, effect_id) REFERENCES node_activations(run_id, activation_id, effect_id) ON DELETE RESTRICT;

ALTER TABLE ONLY node_attempts
    ADD CONSTRAINT node_attempts_run_id_output_artifact_id_fkey FOREIGN KEY (run_id, output_artifact_id) REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT;

ALTER TABLE ONLY node_attempts
    ADD CONSTRAINT node_attempts_run_id_output_payload_id_fkey FOREIGN KEY (run_id, output_payload_id) REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT;

ALTER TABLE ONLY payloads
    ADD CONSTRAINT payloads_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY projection_checkpoint_batches
    ADD CONSTRAINT projection_checkpoint_batches_run_id_event_id_fkey FOREIGN KEY (run_id, event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY projection_checkpoints
    ADD CONSTRAINT projection_checkpoints_run_id_event_id_fkey FOREIGN KEY (run_id, event_id) REFERENCES projection_checkpoint_batches(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public_event_delivery_heads
    ADD CONSTRAINT public_event_delivery_heads_run_id_execution_event_id_fkey FOREIGN KEY (run_id, execution_event_id) REFERENCES public_event_projection_decisions(run_id, execution_event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public_event_delivery_heads
    ADD CONSTRAINT public_event_delivery_heads_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public_event_delivery_heads
    ADD CONSTRAINT public_event_delivery_heads_run_id_public_event_id_fkey FOREIGN KEY (run_id, public_event_id) REFERENCES public_event_outbox(run_id, public_event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public_event_outbox
    ADD CONSTRAINT public_event_outbox_run_id_causation_event_id_fkey FOREIGN KEY (run_id, causation_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public_event_projection_decisions
    ADD CONSTRAINT public_event_projection_decision_run_id_execution_event_id_fkey FOREIGN KEY (run_id, execution_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY public_event_receipts
    ADD CONSTRAINT public_event_receipts_run_id_causation_event_id_fkey FOREIGN KEY (run_id, causation_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_artifact_roots
    ADD CONSTRAINT recovery_artifact_roots_artifact_run_id_artifact_id_fkey FOREIGN KEY (artifact_run_id, artifact_id) REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_artifact_roots
    ADD CONSTRAINT recovery_artifact_roots_run_id_created_by_transition_key_fkey FOREIGN KEY (run_id, created_by_transition_key) REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_artifact_roots
    ADD CONSTRAINT recovery_artifact_roots_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_artifact_roots
    ADD CONSTRAINT recovery_artifact_roots_source_run_id_source_activation_id_fkey FOREIGN KEY (source_run_id, source_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_effect_roots
    ADD CONSTRAINT recovery_effect_roots_run_id_created_by_transition_key_fkey FOREIGN KEY (run_id, created_by_transition_key) REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_effect_roots
    ADD CONSTRAINT recovery_effect_roots_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_effect_roots
    ADD CONSTRAINT recovery_effect_roots_source_run_id_source_activation_id_e_fkey FOREIGN KEY (source_run_id, source_activation_id, effect_id) REFERENCES node_activations(run_id, activation_id, effect_id) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_revision_roots
    ADD CONSTRAINT recovery_revision_roots_definition_id_definition_revision__fkey FOREIGN KEY (definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) REFERENCES deployment_revisions(definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_revision_roots
    ADD CONSTRAINT recovery_revision_roots_run_id_created_by_transition_key_fkey FOREIGN KEY (run_id, created_by_transition_key) REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_revision_roots
    ADD CONSTRAINT recovery_revision_roots_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_revision_roots
    ADD CONSTRAINT recovery_revision_roots_source_run_id_fkey FOREIGN KEY (source_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_transition_results
    ADD CONSTRAINT recovery_transition_results_authority_run_id_fkey FOREIGN KEY (authority_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY recovery_transition_results
    ADD CONSTRAINT recovery_transition_results_primary_event_run_id_primary_e_fkey FOREIGN KEY (primary_event_run_id, primary_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY response_public_items
    ADD CONSTRAINT response_public_items_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY response_snapshots
    ADD CONSTRAINT response_snapshots_response_id_fkey FOREIGN KEY (response_id) REFERENCES workflow_runs(response_id) ON DELETE RESTRICT;

ALTER TABLE ONLY response_snapshots
    ADD CONSTRAINT response_snapshots_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY run_migration_intents
    ADD CONSTRAINT run_migration_intents_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY run_migration_intents
    ADD CONSTRAINT run_migration_intents_run_id_intent_event_id_fkey FOREIGN KEY (run_id, intent_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY run_migration_intents
    ADD CONSTRAINT run_migration_intents_target_definition_id_target_definiti_fkey FOREIGN KEY (target_definition_id, target_definition_revision_id, target_deployment_revision_id, target_plan_hash, target_binding_hash) REFERENCES deployment_revisions(definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) ON DELETE RESTRICT;

ALTER TABLE ONLY run_recovery_lineage
    ADD CONSTRAINT run_recovery_lineage_run_id_created_by_transition_key_fkey FOREIGN KEY (run_id, created_by_transition_key) REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT;

ALTER TABLE ONLY run_recovery_lineage
    ADD CONSTRAINT run_recovery_lineage_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY run_recovery_lineage
    ADD CONSTRAINT run_recovery_lineage_source_definition_id_source_definitio_fkey FOREIGN KEY (source_definition_id, source_definition_revision_id, source_deployment_revision_id, source_plan_hash, source_binding_hash) REFERENCES deployment_revisions(definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) ON DELETE RESTRICT;

ALTER TABLE ONLY run_recovery_lineage
    ADD CONSTRAINT run_recovery_lineage_source_run_id_fkey FOREIGN KEY (source_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY run_recovery_lineage
    ADD CONSTRAINT run_recovery_lineage_source_run_id_source_checkpoint_hash_fkey FOREIGN KEY (source_run_id, source_checkpoint_hash) REFERENCES scheduler_checkpoints(run_id, content_hash) ON DELETE RESTRICT;

ALTER TABLE ONLY run_recovery_lineage
    ADD CONSTRAINT run_recovery_lineage_target_definition_id_target_definitio_fkey FOREIGN KEY (target_definition_id, target_definition_revision_id, target_deployment_revision_id, target_plan_hash, target_binding_hash) REFERENCES deployment_revisions(definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) ON DELETE RESTRICT;

ALTER TABLE ONLY run_reuse_candidates
    ADD CONSTRAINT run_reuse_candidates_run_id_created_by_transition_key_fkey FOREIGN KEY (run_id, created_by_transition_key) REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT;

ALTER TABLE ONLY run_reuse_candidates
    ADD CONSTRAINT run_reuse_candidates_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY run_reuse_candidates
    ADD CONSTRAINT run_reuse_candidates_run_id_materialized_activation_id_fkey FOREIGN KEY (run_id, materialized_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY run_reuse_candidates
    ADD CONSTRAINT run_reuse_candidates_source_run_id_source_activation_id_fkey FOREIGN KEY (source_run_id, source_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_checkpoints
    ADD CONSTRAINT scheduler_checkpoints_run_id_event_id_fkey FOREIGN KEY (run_id, event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_checkpoints
    ADD CONSTRAINT scheduler_checkpoints_run_id_transition_key_fkey FOREIGN KEY (run_id, transition_key) REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_occurrence_values
    ADD CONSTRAINT scheduler_occurrence_values_run_id_artifact_id_fkey FOREIGN KEY (run_id, artifact_id) REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_occurrence_values
    ADD CONSTRAINT scheduler_occurrence_values_run_id_owner_activation_id_fkey FOREIGN KEY (run_id, owner_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_occurrence_values
    ADD CONSTRAINT scheduler_occurrence_values_run_id_payload_id_fkey FOREIGN KEY (run_id, payload_id) REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_subflow_invocations
    ADD CONSTRAINT scheduler_subflow_invocations_child_run_id_fkey FOREIGN KEY (child_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_subflow_invocations
    ADD CONSTRAINT scheduler_subflow_invocations_run_id_invocation_scope_inst_fkey FOREIGN KEY (run_id, invocation_scope_instance_id) REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_subflow_invocations
    ADD CONSTRAINT scheduler_subflow_invocations_run_id_parent_activation_id_fkey FOREIGN KEY (run_id, parent_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_subflow_invocations
    ADD CONSTRAINT scheduler_subflow_invocations_run_id_parent_scope_instance_fkey FOREIGN KEY (run_id, parent_scope_instance_id) REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_values
    ADD CONSTRAINT scheduler_values_run_id_artifact_id_fkey FOREIGN KEY (run_id, artifact_id) REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_values
    ADD CONSTRAINT scheduler_values_run_id_owner_activation_id_fkey FOREIGN KEY (run_id, owner_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_values
    ADD CONSTRAINT scheduler_values_run_id_payload_id_fkey FOREIGN KEY (run_id, payload_id) REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_wait_registrations
    ADD CONSTRAINT scheduler_wait_registrations_run_id_activation_id_fkey FOREIGN KEY (run_id, activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scheduler_wait_registrations
    ADD CONSTRAINT scheduler_wait_registrations_run_id_timer_id_fkey FOREIGN KEY (run_id, timer_id) REFERENCES timers(run_id, timer_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY scope_instances
    ADD CONSTRAINT scope_instances_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY scope_instances
    ADD CONSTRAINT scope_instances_run_id_parent_scope_instance_id_fkey FOREIGN KEY (run_id, parent_scope_instance_id) REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT;

ALTER TABLE ONLY signals_inbox
    ADD CONSTRAINT signals_inbox_run_id_consumed_event_id_fkey FOREIGN KEY (run_id, consumed_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY signals_inbox
    ADD CONSTRAINT signals_inbox_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY signals_inbox
    ADD CONSTRAINT signals_inbox_run_id_payload_id_fkey FOREIGN KEY (run_id, payload_id) REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT;

ALTER TABLE ONLY signals_inbox
    ADD CONSTRAINT signals_inbox_run_id_target_activation_id_fkey FOREIGN KEY (run_id, target_activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY task_outbox
    ADD CONSTRAINT task_outbox_run_id_activation_id_attempt_no_lease_epoch_fe_fkey FOREIGN KEY (run_id, activation_id, attempt_no, lease_epoch, fencing_token) REFERENCES node_attempts(run_id, activation_id, attempt_no, lease_epoch, fencing_token) ON DELETE RESTRICT;

ALTER TABLE ONLY timers
    ADD CONSTRAINT timers_run_id_activation_id_expected_attempt_no_expected_l_fkey FOREIGN KEY (run_id, activation_id, expected_attempt_no, expected_lease_epoch, expected_fencing_token) REFERENCES node_attempts(run_id, activation_id, attempt_no, lease_epoch, fencing_token) ON DELETE RESTRICT;

ALTER TABLE ONLY timers
    ADD CONSTRAINT timers_run_id_activation_id_fkey FOREIGN KEY (run_id, activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY timers
    ADD CONSTRAINT timers_run_id_fired_event_id_fkey FOREIGN KEY (run_id, fired_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY workflow_definition_public_metadata
    ADD CONSTRAINT workflow_definition_public_me_definition_id_definition_rev_fkey FOREIGN KEY (definition_id, definition_revision_id) REFERENCES workflow_definition_revisions(definition_id, definition_revision_id) ON DELETE RESTRICT;

ALTER TABLE ONLY workflow_definition_revisions
    ADD CONSTRAINT workflow_definition_revisions_definition_id_fkey FOREIGN KEY (definition_id) REFERENCES workflow_definitions(definition_id) ON DELETE RESTRICT;

ALTER TABLE ONLY workflow_retrieval_publications
    ADD CONSTRAINT workflow_retrieval_publicatio_run_id_activation_id_attempt_fkey FOREIGN KEY (run_id, activation_id, attempt_no) REFERENCES node_attempts(run_id, activation_id, attempt_no) ON DELETE RESTRICT;

ALTER TABLE ONLY workflow_retrieval_publications
    ADD CONSTRAINT workflow_retrieval_publications_run_id_completion_event_id_fkey FOREIGN KEY (run_id, completion_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

ALTER TABLE ONLY workflow_retrieval_publications
    ADD CONSTRAINT workflow_retrieval_publications_run_id_fkey FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY workflow_retrieval_publications
    ADD CONSTRAINT workflow_retrieval_publications_run_id_task_id_fkey FOREIGN KEY (run_id, task_id) REFERENCES task_outbox(run_id, task_id) ON DELETE RESTRICT;

ALTER TABLE ONLY workflow_runs
    ADD CONSTRAINT workflow_runs_definition_id_definition_revision_id_deploym_fkey FOREIGN KEY (definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) REFERENCES deployment_revisions(definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) ON DELETE RESTRICT;

ALTER TABLE ONLY workflow_runs
    ADD CONSTRAINT workflow_runs_parent_run_id_fkey FOREIGN KEY (parent_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT;

ALTER TABLE ONLY workflow_runs
    ADD CONSTRAINT workflow_runs_replacement_run_id_fkey FOREIGN KEY (replacement_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY wait_late_audit_outbox
    ADD CONSTRAINT wait_late_audit_outbox_activation_fkey FOREIGN KEY (run_id, activation_id) REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT;

ALTER TABLE ONLY wait_late_audit_outbox
    ADD CONSTRAINT wait_late_audit_outbox_event_fkey FOREIGN KEY (run_id, completed_event_id) REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT;

CREATE TABLE durable_schema_contract (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    contract_id TEXT NOT NULL,
    backend TEXT NOT NULL CHECK (backend IN ('postgres', 'sqlite')),
    installed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO durable_schema_contract (singleton, contract_id, backend)
VALUES (
    1,
    'durable-schema-cd9a5c3f-5f12-46d2-ab96-78820a13186f',
    'postgres'
);

COMMIT;
