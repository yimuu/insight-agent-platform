-- PostgreSQL authority equivalent of the SQLite parent-operation deadline.

ALTER TABLE model_tool_call_batches
    ADD COLUMN IF NOT EXISTS parent_operation_deadline TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_model_tool_batches_parent_deadline
    ON model_tool_call_batches(
        execution_status,
        continuation_status,
        parent_operation_deadline
    );
