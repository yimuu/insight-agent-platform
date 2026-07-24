-- Canvas presentation state is mutable and intentionally separate from the
-- immutable author document / Canonical Plan revision.

CREATE TABLE IF NOT EXISTS graph_view_documents (
    definition_id TEXT NOT NULL,
    definition_revision_id TEXT NOT NULL,
    graph_document_id TEXT NOT NULL CHECK (graph_document_id <> ''),
    view_version INTEGER NOT NULL CHECK (view_version >= 1),
    view_document TEXT NOT NULL CHECK (json_valid(view_document)),
    updated_at TEXT NOT NULL,
    PRIMARY KEY (definition_id, definition_revision_id),
    FOREIGN KEY (definition_id, definition_revision_id)
        REFERENCES workflow_definition_revisions(definition_id, definition_revision_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_graph_views_document
    ON graph_view_documents(graph_document_id);
