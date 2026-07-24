-- Freeze the referenced-Artifact hold at Run admission so a configuration
-- change or a delayed recovery cycle cannot change an admitted Run's policy.
ALTER TABLE workflow_runs
ADD COLUMN artifact_reference_retention_seconds INTEGER NOT NULL DEFAULT 2592000
    CHECK (
        artifact_reference_retention_seconds >= 1
        AND artifact_reference_retention_seconds <= 315360000
    );

-- Existing rows were registered by the compensating post-terminal path. New
-- Runs register their release in the terminal winner transaction itself.
ALTER TABLE artifact_retention_releases
ADD COLUMN registration_kind TEXT NOT NULL DEFAULT 'legacy'
    CHECK (registration_kind IN ('legacy', 'terminal_atomic'));
