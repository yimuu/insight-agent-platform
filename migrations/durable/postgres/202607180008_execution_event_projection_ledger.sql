-- Execution events, not materialized checkpoint tables, are the projection
-- rebuild authority. The repository fills this once in the event transaction.
ALTER TABLE execution_events
    ADD COLUMN projection_ledger_batch JSONB;

CREATE FUNCTION reject_execution_event_projection_ledger_rewrite()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.projection_ledger_batch IS NOT NULL
       AND NEW.projection_ledger_batch IS DISTINCT FROM OLD.projection_ledger_batch THEN
        RAISE EXCEPTION 'execution event projection ledger is immutable';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER execution_event_projection_ledger_immutable
BEFORE UPDATE OF projection_ledger_batch ON execution_events
FOR EACH ROW
EXECUTE FUNCTION reject_execution_event_projection_ledger_rewrite();

-- Delivery and idempotency receipts are durable authorities, not rebuildable
-- materialized projections. Losing one could duplicate an external publish or
-- a recovery/control transition, so deletion fails closed.
CREATE FUNCTION reject_durable_authority_delete()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'durable delivery/idempotency authority cannot be deleted';
END;
$$;

CREATE TRIGGER control_transition_result_delete_forbidden
BEFORE DELETE ON control_transition_results
FOR EACH ROW EXECUTE FUNCTION reject_durable_authority_delete();
CREATE TRIGGER recovery_transition_result_delete_forbidden
BEFORE DELETE ON recovery_transition_results
FOR EACH ROW EXECUTE FUNCTION reject_durable_authority_delete();
CREATE TRIGGER artifact_retention_release_delete_forbidden
BEFORE DELETE ON artifact_retention_releases
FOR EACH ROW EXECUTE FUNCTION reject_durable_authority_delete();

CREATE FUNCTION permit_expired_public_event_prune()
RETURNS TRIGGER
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

CREATE TRIGGER public_event_outbox_delete_forbidden
BEFORE DELETE ON public_event_outbox
FOR EACH ROW EXECUTE FUNCTION permit_expired_public_event_prune();

CREATE FUNCTION reject_transition_receipt_rewrite()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'transition receipt is immutable';
END;
$$;

CREATE TRIGGER control_transition_result_rewrite_forbidden
BEFORE UPDATE ON control_transition_results
FOR EACH ROW EXECUTE FUNCTION reject_transition_receipt_rewrite();
CREATE TRIGGER recovery_transition_result_rewrite_forbidden
BEFORE UPDATE ON recovery_transition_results
FOR EACH ROW EXECUTE FUNCTION reject_transition_receipt_rewrite();
