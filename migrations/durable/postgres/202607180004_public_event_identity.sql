-- A PostgreSQL NOTIFY payload contains only public_event_id.  Make the
-- durable lookup by that ID authoritative across all Runs and runtimes.
CREATE UNIQUE INDEX IF NOT EXISTS uq_public_outbox_public_event_id
    ON public_event_outbox(public_event_id);
