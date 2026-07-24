ALTER TABLE run_migration_intents
    ADD COLUMN target_timeout_ms INTEGER NOT NULL DEFAULT 300000
    CHECK (target_timeout_ms > 0);
