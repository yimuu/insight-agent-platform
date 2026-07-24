-- `execution_events` is the immutable execution fact authority.  The sole
-- post-insert mutation is the transaction-local projection manifest fill:
-- NULL -> one canonical, closed v1 JSON object. JSONB supplies canonical
-- storage; the repository validates the complete closed manifest and hashes.

ALTER TABLE execution_events
    ADD CONSTRAINT execution_events_schema_version_supported
    CHECK (schema_version = 2) NOT VALID;

ALTER TABLE execution_events
    VALIDATE CONSTRAINT execution_events_schema_version_supported;

ALTER TABLE execution_events
    ADD CONSTRAINT execution_events_kind_supported
    CHECK (kind IN (
        'run.created', 'run.lifecycle_changed', 'run.admission_changed',
        'run.termination_claimed', 'scope.created', 'scope.draining', 'scope.settled',
        'activation.created', 'activation.ready', 'activation.leased',
        'activation.running', 'activation.retry_wait', 'activation.waiting',
        'activation.terminating', 'activation.succeeded', 'activation.failed',
        'activation.cancelled', 'activation.timed_out', 'attempt.created',
        'attempt.leased', 'attempt.running', 'attempt.succeeded', 'attempt.failed',
        'attempt.timed_out', 'attempt.abandoned', 'attempt.cancelled',
        'effect.evidence_recorded', 'control_token.emitted', 'control_token.consumed',
        'control_token.revoked', 'fork.created', 'join.arrived', 'join.completed',
        'signal.received', 'signal.late', 'timer.scheduled', 'timer.fired',
        'timer.late', 'projection.mutated'
    )) NOT VALID;

ALTER TABLE execution_events
    VALIDATE CONSTRAINT execution_events_kind_supported;

CREATE OR REPLACE FUNCTION reject_execution_event_projection_ledger_rewrite()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
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
$$;

DROP TRIGGER IF EXISTS execution_event_projection_ledger_immutable ON execution_events;

CREATE TRIGGER execution_event_projection_ledger_immutable
BEFORE INSERT OR UPDATE OR DELETE ON execution_events
FOR EACH ROW
EXECUTE FUNCTION reject_execution_event_projection_ledger_rewrite();
