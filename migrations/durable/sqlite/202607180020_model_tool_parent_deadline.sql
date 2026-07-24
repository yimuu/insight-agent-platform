-- Freeze the parent LLM operation deadline into every activated tool batch.
-- The queue uses this database-clock authority while the parent task is
-- suspended, so waiting on tools can never buy a fresh LLM timeout budget.

ALTER TABLE model_tool_call_batches
    ADD COLUMN parent_operation_deadline TEXT;

CREATE INDEX IF NOT EXISTS idx_model_tool_batches_parent_deadline
    ON model_tool_call_batches(
        execution_status,
        continuation_status,
        parent_operation_deadline
    );
