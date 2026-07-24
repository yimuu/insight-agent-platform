-- Explicit durable public routing. Immutable deployment rows are an archive;
-- their insertion order is never a current-version contract.
CREATE TABLE workflow_definition_public_metadata (
    definition_id TEXT NOT NULL,
    definition_revision_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    public_description TEXT NOT NULL,
    PRIMARY KEY (definition_id, definition_revision_id),
    FOREIGN KEY (definition_id, definition_revision_id)
        REFERENCES workflow_definition_revisions(definition_id, definition_revision_id)
        ON DELETE RESTRICT,
    CHECK (display_name <> '' AND length(display_name) <= 256),
    CHECK (length(public_description) <= 4096)
);

CREATE FUNCTION durable_reject_definition_public_metadata_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'published workflow definition public metadata is immutable'
        USING ERRCODE = 'check_violation';
END;
$$;

CREATE TRIGGER trg_definition_public_metadata_immutable
    BEFORE UPDATE OR DELETE ON workflow_definition_public_metadata
    FOR EACH ROW EXECUTE FUNCTION durable_reject_definition_public_metadata_mutation();

CREATE TABLE agent_publication_heads (
    agent_id TEXT PRIMARY KEY,
    definition_id TEXT NOT NULL UNIQUE,
    definition_revision_id TEXT NOT NULL,
    deployment_revision_id TEXT NOT NULL,
    publication_origin TEXT NOT NULL CHECK (publication_origin IN ('built_in','graph')),
    updated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (definition_id, deployment_revision_id)
        REFERENCES deployment_revisions(definition_id, deployment_revision_id)
        ON DELETE RESTRICT
);

CREATE FUNCTION durable_validate_publication_head()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM workflow_definitions d
        JOIN deployment_revisions x
          ON x.definition_id = d.definition_id
        WHERE d.definition_id = NEW.definition_id
          AND d.agent_id = NEW.agent_id
          AND x.definition_revision_id = NEW.definition_revision_id
          AND x.deployment_revision_id = NEW.deployment_revision_id
    ) THEN
        RAISE EXCEPTION 'publication head agent does not own definition'
            USING ERRCODE = 'foreign_key_violation';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_publication_head_agent_matches_definition
    BEFORE INSERT OR UPDATE ON agent_publication_heads
    FOR EACH ROW EXECUTE FUNCTION durable_validate_publication_head();
