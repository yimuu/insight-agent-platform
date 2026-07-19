-- Keep the SQLite contract structurally identical even though SQLite has no
-- cross-process LISTEN/NOTIFY stream.
CREATE UNIQUE INDEX IF NOT EXISTS uq_v3_public_outbox_public_event_id
    ON public_event_outbox(public_event_id);
