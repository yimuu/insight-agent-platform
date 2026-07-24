CREATE TABLE IF NOT EXISTS artifact_store_authority (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    backend TEXT NOT NULL CHECK (backend = 'shared_filesystem'),
    namespace TEXT NOT NULL CHECK (
        length(namespace) BETWEEN 1 AND 128
        AND namespace NOT GLOB '*[^A-Za-z0-9._-]*'
    ),
    store_id TEXT NOT NULL CHECK (
        length(store_id) = 47
        AND store_id GLOB 'artifact_store_[0-9a-f]*'
        AND substr(store_id, 16) NOT GLOB '*[^0-9a-f]*'
    ),
    bound_at TEXT NOT NULL DEFAULT (STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TRIGGER IF NOT EXISTS artifact_store_authority_reject_update
BEFORE UPDATE ON artifact_store_authority
BEGIN
    SELECT RAISE(ABORT, 'artifact store authority is immutable');
END;

CREATE TRIGGER IF NOT EXISTS artifact_store_authority_reject_delete
BEFORE DELETE ON artifact_store_authority
BEGIN
    SELECT RAISE(ABORT, 'artifact store authority is immutable');
END;
