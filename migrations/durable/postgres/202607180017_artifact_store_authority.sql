CREATE TABLE IF NOT EXISTS artifact_store_authority (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    backend TEXT NOT NULL CHECK (backend = 'shared_filesystem'),
    namespace TEXT NOT NULL CHECK (
        namespace ~ '^[A-Za-z0-9._-]{1,128}$'
    ),
    store_id TEXT NOT NULL CHECK (
        store_id ~ '^artifact_store_[0-9a-f]{32}$'
    ),
    bound_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE OR REPLACE FUNCTION reject_artifact_store_authority_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $function$
BEGIN
    RAISE EXCEPTION 'artifact store authority is immutable';
END;
$function$;

DROP TRIGGER IF EXISTS artifact_store_authority_immutable
    ON artifact_store_authority;
CREATE TRIGGER artifact_store_authority_immutable
BEFORE UPDATE OR DELETE ON artifact_store_authority
FOR EACH ROW EXECUTE FUNCTION reject_artifact_store_authority_mutation();
