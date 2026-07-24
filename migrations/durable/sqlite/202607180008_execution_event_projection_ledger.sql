-- Execution events, not materialized checkpoint tables, are the projection
-- rebuild authority. The repository fills this once in the event transaction.
ALTER TABLE execution_events
    ADD COLUMN projection_ledger_batch TEXT
    CHECK (projection_ledger_batch IS NULL OR json_valid(projection_ledger_batch));

CREATE TRIGGER execution_event_projection_ledger_immutable
BEFORE UPDATE OF projection_ledger_batch ON execution_events
WHEN OLD.projection_ledger_batch IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'execution event projection ledger is immutable');
END;

-- Delivery and idempotency receipts are durable authorities, not rebuildable
-- materialized projections. Losing one could duplicate an external publish or
-- a recovery/control transition, so deletion fails closed.
CREATE TRIGGER public_event_outbox_delete_forbidden
BEFORE DELETE ON public_event_outbox
WHEN NOT (
    OLD.publish_state = 'published'
    AND OLD.is_terminal = 0
    AND OLD.retain_until IS NOT NULL
    AND julianday(OLD.retain_until) <= julianday('now')
)
BEGIN
    SELECT RAISE(ABORT, 'public event delivery authority is not eligible for retention pruning');
END;

CREATE TRIGGER control_transition_result_rewrite_forbidden
BEFORE UPDATE ON control_transition_results
BEGIN
    SELECT RAISE(ABORT, 'control transition receipt is immutable');
END;
CREATE TRIGGER control_transition_result_delete_forbidden
BEFORE DELETE ON control_transition_results
BEGIN
    SELECT RAISE(ABORT, 'control transition receipt cannot be deleted');
END;

CREATE TRIGGER recovery_transition_result_rewrite_forbidden
BEFORE UPDATE ON recovery_transition_results
BEGIN
    SELECT RAISE(ABORT, 'recovery transition receipt is immutable');
END;
CREATE TRIGGER recovery_transition_result_delete_forbidden
BEFORE DELETE ON recovery_transition_results
BEGIN
    SELECT RAISE(ABORT, 'recovery transition receipt cannot be deleted');
END;

CREATE TRIGGER artifact_retention_release_delete_forbidden
BEFORE DELETE ON artifact_retention_releases
BEGIN
    SELECT RAISE(ABORT, 'artifact retention release cannot be deleted');
END;
