\set ON_ERROR_STOP on

DO $$
DECLARE
    fsync_setting text := current_setting('fsync');
    full_page_writes_setting text := current_setting('full_page_writes');
    synchronous_commit_setting text := current_setting('synchronous_commit');
BEGIN
    IF fsync_setting <> 'on' THEN
        RAISE EXCEPTION 'Gate invalid: fsync must be on, got %', fsync_setting;
    END IF;
    IF full_page_writes_setting <> 'on' THEN
        RAISE EXCEPTION 'Gate invalid: full_page_writes must be on, got %',
            full_page_writes_setting;
    END IF;
    IF synchronous_commit_setting NOT IN ('on', 'remote_apply') THEN
        RAISE EXCEPTION
            'Gate invalid: synchronous_commit may not be weakened, got %',
            synchronous_commit_setting;
    END IF;
    IF current_setting('track_io_timing') <> 'on' THEN
        RAISE EXCEPTION 'Gate invalid: track_io_timing must be on';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'
    ) THEN
        RAISE EXCEPTION 'Gate invalid: pg_stat_statements is required';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'pg_stat_statements'
          AND column_name = 'wal_bytes'
    ) THEN
        RAISE EXCEPTION
            'Gate invalid: pg_stat_statements.wal_bytes is required';
    END IF;
    IF current_setting('pg_stat_statements.track') <> 'all' THEN
        RAISE EXCEPTION
            'Gate invalid: pg_stat_statements.track must be all, got %',
            current_setting('pg_stat_statements.track');
    END IF;
    IF current_setting('pg_stat_statements.track_utility') <> 'on' THEN
        RAISE EXCEPTION
            'Gate invalid: pg_stat_statements.track_utility must be on, got %',
            current_setting('pg_stat_statements.track_utility');
    END IF;
    IF EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
          AND c.relname IN ('terminal_run_admissions', 'terminal_run_results')
          AND relpersistence <> 'p'
    ) THEN
        RAISE EXCEPTION
            'Gate invalid: admission/result relations must be permanent LOGGED tables';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM unnest(ARRAY[
            'terminal_run_admissions',
            'terminal_run_results',
            'terminal_content_deletion_jobs',
            'terminal_artifact_staging',
            'conversations',
            'conversation_messages',
            'conversation_summaries',
            'conversation_tombstones',
            'conversation_summary_jobs'
        ]::text[]) AS required(relname)
        LEFT JOIN pg_class c
          ON c.oid = to_regclass('public.' || required.relname)
        WHERE c.oid IS NULL OR c.relpersistence <> 'p'
    ) THEN
        RAISE EXCEPTION
            'Gate invalid: terminal/Conversation durable relations must be permanent LOGGED tables';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        WHERE c.oid = to_regclass('public.terminal_runtime_instances')
          AND c.relpersistence = 'u'
    ) THEN
        RAISE EXCEPTION
            'Gate invalid: terminal_runtime_instances must remain UNLOGGED';
    END IF;
END
$$;

SELECT 'PostgreSQL durability settings accepted';
