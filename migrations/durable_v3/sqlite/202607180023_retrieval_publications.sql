-- One immutable publication decision for every successfully committed
-- first-class Retrieval activation. A private decision deliberately stores
-- no query, raw provider result, or public candidate: only NULL projection.

CREATE TABLE workflow_retrieval_publications (
    run_id TEXT NOT NULL,
    retrieval_id TEXT NOT NULL CHECK (
        length(retrieval_id) = 68 AND retrieval_id LIKE 'ret_%'
    ),
    task_id TEXT NOT NULL CHECK (
        length(task_id) = 69 AND task_id LIKE 'task_%'
    ),
    activation_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    retrieval_resource_id TEXT NOT NULL CHECK (
        retrieval_resource_id <> '' AND length(retrieval_resource_id) <= 128
    ),
    retrieval_resource_version TEXT NOT NULL CHECK (
        retrieval_resource_version <> '' AND length(retrieval_resource_version) <= 64
    ),
    retrieval_descriptor_hash TEXT NOT NULL CHECK (
        length(retrieval_descriptor_hash) = 64
    ),
    query_field TEXT NOT NULL CHECK (
        query_field <> '' AND length(query_field) <= 128
    ),
    effective_public_policy TEXT NOT NULL CHECK (
        json_valid(effective_public_policy)
        AND json_type(effective_public_policy) = 'object'
        AND length(effective_public_policy) <= 262144
    ),
    effective_public_policy_hash TEXT NOT NULL CHECK (
        length(effective_public_policy_hash) = 71
        AND effective_public_policy_hash LIKE 'sha256:%'
    ),
    public_projection TEXT CHECK (
        public_projection IS NULL
        OR (json_valid(public_projection)
            AND json_type(public_projection) = 'object'
            AND length(public_projection) <= 1048576)
    ),
    public_projection_hash TEXT CHECK (
        public_projection_hash IS NULL
        OR (length(public_projection_hash) = 71
            AND public_projection_hash LIKE 'sha256:%')
    ),
    completion_transition_key TEXT NOT NULL,
    completion_intent_hash TEXT NOT NULL CHECK (
        length(completion_intent_hash) = 71
        AND completion_intent_hash LIKE 'sha256:%'
    ),
    completion_event_id TEXT NOT NULL,
    completion_event_seq INTEGER NOT NULL CHECK (completion_event_seq >= 1),
    publication_hash TEXT NOT NULL CHECK (
        length(publication_hash) = 71 AND publication_hash LIKE 'sha256:%'
    ),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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

CREATE INDEX idx_workflow_retrieval_publications_terminal
    ON workflow_retrieval_publications(run_id, activation_id, attempt_no, retrieval_id);

CREATE TRIGGER workflow_retrieval_publication_update_forbidden
BEFORE UPDATE ON workflow_retrieval_publications
BEGIN
    SELECT RAISE(ABORT, 'workflow retrieval publication is immutable');
END;

CREATE TRIGGER workflow_retrieval_publication_delete_forbidden
BEFORE DELETE ON workflow_retrieval_publications
BEGIN
    SELECT RAISE(ABORT, 'workflow retrieval publication is immutable');
END;
