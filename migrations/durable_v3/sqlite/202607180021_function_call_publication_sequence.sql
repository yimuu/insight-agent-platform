-- The completed seal watermark is body-free sequencing metadata. Keeping it
-- with the frozen tool task lets any later runtime publish the done tail
-- without persisting or replaying Provider argument fragments.
ALTER TABLE model_tool_calls
    ADD COLUMN response_seal_index INTEGER
    CHECK (response_seal_index IS NULL OR response_seal_index >= 3);
