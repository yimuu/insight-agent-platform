-- PostgreSQL authority equivalent of the immutable Retrieval publication
-- decision ledger. Raw model input and raw retrieval results never enter it.

CREATE TABLE IF NOT EXISTS workflow_retrieval_publications (
    run_id TEXT NOT NULL,
    retrieval_id TEXT NOT NULL CHECK (retrieval_id ~ '^ret_[0-9a-f]{64}$'),
    task_id TEXT NOT NULL CHECK (task_id ~ '^task_[0-9a-f]{64}$'),
    activation_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    retrieval_resource_id TEXT NOT NULL CHECK (
        retrieval_resource_id <> '' AND octet_length(retrieval_resource_id) <= 128
    ),
    retrieval_resource_version TEXT NOT NULL CHECK (
        retrieval_resource_version <> '' AND octet_length(retrieval_resource_version) <= 64
    ),
    retrieval_descriptor_hash TEXT NOT NULL CHECK (
        retrieval_descriptor_hash ~ '^[0-9a-f]{64}$'
    ),
    query_field TEXT NOT NULL CHECK (
        query_field <> '' AND octet_length(query_field) <= 128
    ),
    effective_public_policy JSONB NOT NULL CHECK (
        jsonb_typeof(effective_public_policy) = 'object'
        AND octet_length(effective_public_policy::TEXT) <= 262144
    ),
    effective_public_policy_hash TEXT NOT NULL CHECK (
        effective_public_policy_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    public_projection JSONB CHECK (
        public_projection IS NULL
        OR (jsonb_typeof(public_projection) = 'object'
            AND octet_length(public_projection::TEXT) <= 1048576)
    ),
    public_projection_hash TEXT CHECK (
        public_projection_hash IS NULL
        OR public_projection_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    completion_transition_key TEXT NOT NULL,
    completion_intent_hash TEXT NOT NULL CHECK (
        completion_intent_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    completion_event_id TEXT NOT NULL,
    completion_event_seq BIGINT NOT NULL CHECK (completion_event_seq >= 1),
    publication_hash TEXT NOT NULL CHECK (
        publication_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, retrieval_id),
    UNIQUE (run_id, task_id),
    UNIQUE (run_id, activation_id),
    UNIQUE (run_id, completion_transition_key),
    UNIQUE (run_id, completion_event_id),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, task_id)
        REFERENCES task_outbox(run_id, task_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, activation_id, attempt_no)
        REFERENCES node_attempts(run_id, activation_id, attempt_no) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, completion_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    CHECK ((public_projection IS NULL) = (public_projection_hash IS NULL))
);

CREATE INDEX IF NOT EXISTS idx_workflow_retrieval_publications_terminal
    ON workflow_retrieval_publications(run_id, activation_id, attempt_no, retrieval_id);

CREATE OR REPLACE FUNCTION forbid_workflow_retrieval_publication_rewrite()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'workflow retrieval publication is immutable';
END;
$$;

DROP TRIGGER IF EXISTS workflow_retrieval_publication_immutable
    ON workflow_retrieval_publications;
CREATE TRIGGER workflow_retrieval_publication_immutable
BEFORE UPDATE OR DELETE ON workflow_retrieval_publications
FOR EACH ROW EXECUTE FUNCTION forbid_workflow_retrieval_publication_rewrite();
