-- Body-free function-call publication sequencing authority.
ALTER TABLE model_tool_calls
    ADD COLUMN IF NOT EXISTS response_seal_index BIGINT
    CHECK (response_seal_index IS NULL OR response_seal_index >= 3);
