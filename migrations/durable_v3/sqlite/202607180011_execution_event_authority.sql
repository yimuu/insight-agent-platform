-- `execution_events` is the immutable execution fact authority.  The sole
-- post-insert mutation is the transaction-local projection manifest fill:
-- NULL -> one canonical, closed v1 JSON object.

CREATE TRIGGER IF NOT EXISTS execution_event_schema_version_insert_supported
BEFORE INSERT ON execution_events
FOR EACH ROW
WHEN COALESCE((
    NEW.schema_version = 2
    AND NEW.projection_ledger_batch IS NULL
    AND NEW.kind IN (
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
    )
), 0) = 0
BEGIN
    SELECT RAISE(ABORT, 'invalid execution event insert authority');
END;

CREATE TRIGGER IF NOT EXISTS execution_event_schema_version_update_supported
BEFORE UPDATE OF schema_version ON execution_events
FOR EACH ROW
WHEN NEW.schema_version <> 2
BEGIN
    SELECT RAISE(ABORT, 'unsupported execution event schema version');
END;

CREATE TRIGGER IF NOT EXISTS execution_event_kind_update_supported
BEFORE UPDATE OF kind ON execution_events
FOR EACH ROW
WHEN NEW.kind NOT IN (
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
)
BEGIN
    SELECT RAISE(ABORT, 'unsupported execution event kind');
END;

-- Fail the forward migration rather than blessing an already-unknown row.
UPDATE execution_events
SET schema_version = schema_version
WHERE schema_version <> 2;

UPDATE execution_events
SET kind = kind
WHERE kind NOT IN (
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
);

DROP TRIGGER IF EXISTS execution_event_projection_ledger_immutable;

CREATE TRIGGER execution_event_projection_ledger_immutable
BEFORE UPDATE ON execution_events
FOR EACH ROW
WHEN COALESCE((
    OLD.projection_ledger_batch IS NULL
    AND NEW.projection_ledger_batch IS NOT NULL
    AND NEW.run_id IS OLD.run_id
    AND NEW.seq IS OLD.seq
    AND NEW.event_id IS OLD.event_id
    AND NEW.schema_version IS OLD.schema_version
    AND NEW.kind IS OLD.kind
    AND NEW.transition_key IS OLD.transition_key
    AND NEW.intent_hash IS OLD.intent_hash
    AND NEW.node_id IS OLD.node_id
    AND NEW.scope_instance_id IS OLD.scope_instance_id
    AND NEW.activation_id IS OLD.activation_id
    AND NEW.attempt_no IS OLD.attempt_no
    AND NEW.causation_event_id IS OLD.causation_event_id
    AND NEW.projection_version_after IS OLD.projection_version_after
    AND NEW.safe_payload IS OLD.safe_payload
    AND NEW.occurred_at IS OLD.occurred_at
    AND json_valid(NEW.projection_ledger_batch)
    AND json_type(NEW.projection_ledger_batch) = 'object'
    AND json_extract(NEW.projection_ledger_batch, '$.schema_version') = 1
    AND json_type(NEW.projection_ledger_batch, '$.subject_count') = 'integer'
    AND json_extract(NEW.projection_ledger_batch, '$.subject_count') >= 0
    AND json_type(NEW.projection_ledger_batch, '$.manifest_hash') = 'text'
    AND length(json_extract(NEW.projection_ledger_batch, '$.manifest_hash')) = 71
    AND json_extract(NEW.projection_ledger_batch, '$.manifest_hash') LIKE 'sha256:%'
    AND json_type(NEW.projection_ledger_batch, '$.subjects') = 'array'
    AND (SELECT COUNT(*) FROM json_each(NEW.projection_ledger_batch)) = 4
    AND NOT EXISTS (
        SELECT 1
        FROM json_each(NEW.projection_ledger_batch)
        WHERE key NOT IN ('schema_version', 'subject_count', 'manifest_hash', 'subjects')
    )
), 0) = 0
BEGIN
    SELECT RAISE(ABORT, 'execution event authority is immutable');
END;

CREATE TRIGGER IF NOT EXISTS execution_event_delete_forbidden
BEFORE DELETE ON execution_events
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'execution event authority cannot be deleted');
END;
