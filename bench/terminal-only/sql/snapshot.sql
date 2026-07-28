\set ON_ERROR_STOP on

-- Capture exact row counts for every permanent public table that a
-- terminal-only Run is not allowed to mutate.  The allowlist is intentionally
-- small: terminal-only lifecycle/conversation tables, the UNLOGGED owner table
-- (excluded by relpersistence), and the schema-contract metadata row.
CREATE TEMP TABLE terminal_qualification_forbidden_rows (
    table_name text PRIMARY KEY,
    row_count bigint NOT NULL
);

SELECT format(
    'INSERT INTO terminal_qualification_forbidden_rows ' ||
    'SELECT %L, count(*) FROM public.%I',
    class.relname,
    class.relname
)
FROM pg_class AS class
JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
WHERE namespace.nspname = 'public'
  AND class.relkind IN ('r', 'p')
  AND class.relpersistence = 'p'
  AND class.relname NOT IN (
      'durable_schema_contract',
      'terminal_run_admissions',
      'terminal_run_results',
      'terminal_content_deletion_jobs',
      'terminal_artifact_staging',
      'conversations',
      'conversation_messages',
      'conversation_summaries'
  )
ORDER BY class.relname
\gexec

\if :{?reset_statements}
-- Gate B defines its statement/WAL attribution interval here, after the
-- read-only census has completed and immediately before the boundary snapshot.
-- The matching after snapshot embeds the top statements in the same SQL
-- statement as pg_stat_wal, so there is no shell-side accounting gap.
SELECT pg_stat_statements_reset();
\endif

WITH
settings AS (
    SELECT jsonb_build_object(
        'fsync', current_setting('fsync'),
        'full_page_writes', current_setting('full_page_writes'),
        'synchronous_commit', current_setting('synchronous_commit'),
        'wal_level', current_setting('wal_level'),
        'track_io_timing', current_setting('track_io_timing'),
        'pg_stat_statements_track', current_setting('pg_stat_statements.track'),
        'pg_stat_statements_track_utility',
            current_setting('pg_stat_statements.track_utility'),
        'max_wal_size', current_setting('max_wal_size'),
        'wal_keep_size', current_setting('wal_keep_size'),
        'wal_keep_size_bytes',
            pg_size_bytes(current_setting('wal_keep_size'))::numeric,
        'checkpoint_timeout', current_setting('checkpoint_timeout'),
        'checkpoint_completion_target',
            current_setting('checkpoint_completion_target')
    ) AS value
),
qualification_relations AS (
    SELECT jsonb_object_agg(required.relname, COALESCE(class.relpersistence::text, 'missing')
                            ORDER BY required.relname) AS value
    FROM unnest(ARRAY[
        'terminal_run_admissions',
        'terminal_run_results',
        'terminal_content_deletion_jobs',
        'terminal_artifact_staging',
        'conversations',
        'conversation_messages',
        'conversation_summaries',
        'conversation_tombstones',
        'conversation_summary_jobs',
        'terminal_runtime_instances'
    ]::text[]) AS required(relname)
    LEFT JOIN pg_class AS class
      ON class.oid = to_regclass('public.' || required.relname)
),
statement_stats AS (
    SELECT jsonb_build_object(
        'stats_reset', stats_reset,
        'dealloc', dealloc,
        -- track=all exposes parent and nested statements. Adding both WAL
        -- counters would double count nested work already charged to its
        -- top-level statement, so only top-level WAL participates in SQL
        -- accounting. Nested WAL remains a separate diagnostic.
        'top_level_wal_bytes',
            (
                SELECT COALESCE(sum(wal_bytes), 0)::numeric
                FROM pg_stat_statements
                WHERE dbid = (
                    SELECT oid
                    FROM pg_database
                    WHERE datname = current_database()
                )
                  AND toplevel IS TRUE
            ),
        'top_level_calls',
            (
                SELECT COALESCE(sum(calls), 0)::numeric
                FROM pg_stat_statements
                WHERE dbid = (
                    SELECT oid
                    FROM pg_database
                    WHERE datname = current_database()
                )
                  AND toplevel IS TRUE
            ),
        'nested_wal_bytes',
            (
                SELECT COALESCE(sum(wal_bytes), 0)::numeric
                FROM pg_stat_statements
                WHERE dbid = (
                    SELECT oid
                    FROM pg_database
                    WHERE datname = current_database()
                )
                  AND toplevel IS FALSE
            ),
        'nested_calls',
            (
                SELECT COALESCE(sum(calls), 0)::numeric
                FROM pg_stat_statements
                WHERE dbid = (
                    SELECT oid
                    FROM pg_database
                    WHERE datname = current_database()
                )
                  AND toplevel IS FALSE
            )
    ) AS value
    FROM pg_stat_statements_info
),
top_wal_rows AS (
    SELECT
        queryid,
        toplevel,
        calls,
        rows,
        round(total_exec_time::numeric, 3) AS total_exec_ms,
        round(mean_exec_time::numeric, 3) AS mean_exec_ms,
        shared_blks_hit,
        shared_blks_read,
        temp_blks_read,
        temp_blks_written,
        wal_records,
        wal_fpi,
        wal_bytes,
        left(regexp_replace(query, E'[\n\r\t ]+', ' ', 'g'), 320) AS query
    FROM pg_stat_statements
    WHERE dbid = (
        SELECT oid FROM pg_database WHERE datname = current_database()
    )
      AND toplevel IS TRUE
    ORDER BY wal_bytes DESC, calls DESC, queryid
    LIMIT 30
),
top_wal AS (
    SELECT COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'queryid', queryid,
                'toplevel', toplevel,
                'calls', calls,
                'rows', rows,
                'total_exec_ms', total_exec_ms,
                'mean_exec_ms', mean_exec_ms,
                'shared_blks_hit', shared_blks_hit,
                'shared_blks_read', shared_blks_read,
                'temp_blks_read', temp_blks_read,
                'temp_blks_written', temp_blks_written,
                'wal_records', wal_records,
                'wal_fpi', wal_fpi,
                'wal_bytes', wal_bytes,
                'query', query
            )
            ORDER BY wal_bytes DESC, calls DESC, queryid
        ),
        '[]'::jsonb
    ) AS value
    FROM top_wal_rows
),
boundary AS (
    SELECT jsonb_build_object(
        'wal_insert_lsn', pg_current_wal_insert_lsn()::text,
        'postmaster_start_time', pg_postmaster_start_time(),
        'transaction_timestamp', transaction_timestamp(),
        'statement_timestamp', statement_timestamp()
    ) AS value
),
wal AS (
    SELECT jsonb_build_object(
        'wal_bytes', wal_bytes::numeric,
        'wal_records', wal_records::numeric,
        'wal_fpi', wal_fpi::numeric,
        'wal_buffers_full', wal_buffers_full::numeric,
        'stats_reset', stats_reset
    ) AS value
    FROM pg_stat_wal
),
bgwriter AS (
    SELECT jsonb_build_object(
        'checkpoints_timed', checkpoints_timed::numeric,
        'checkpoints_req', checkpoints_req::numeric,
        'checkpoint_write_time_ms', checkpoint_write_time::numeric,
        'checkpoint_sync_time_ms', checkpoint_sync_time::numeric,
        'buffers_checkpoint', buffers_checkpoint::numeric,
        'stats_reset', stats_reset
    ) AS value
    FROM pg_stat_bgwriter
),
database_stats AS (
    SELECT jsonb_build_object(
        'database_bytes', pg_database_size(current_database())::numeric,
        'xact_commit', xact_commit::numeric,
        'xact_rollback', xact_rollback::numeric,
        'temp_files', temp_files::numeric,
        'temp_bytes', temp_bytes::numeric,
        'deadlocks', deadlocks::numeric,
        'blks_read', blks_read::numeric,
        'blks_hit', blks_hit::numeric,
        'blk_read_time_ms', blk_read_time::numeric,
        'blk_write_time_ms', blk_write_time::numeric,
        'stats_reset', stats_reset
    ) AS value
    FROM pg_stat_database
    WHERE datname = current_database()
),
maintenance_stats AS (
    SELECT jsonb_build_object(
        -- pg_stat_user_tables has no independent reset timestamp.  Its
        -- counters belong to the current database statistics epoch, which is
        -- captured beside the per-table counters and checked for continuity by
        -- the fail-closed report evaluator.
        'stats_epoch',
            (
                SELECT stats_reset
                FROM pg_stat_database
                WHERE datname = current_database()
            ),
        'tables',
            COALESCE(
                jsonb_agg(
                    jsonb_build_object(
                        'schema_name', schemaname,
                        'table_name', relname,
                        'relation_id', relid,
                        'autovacuum_count', autovacuum_count::numeric,
                        'autoanalyze_count', autoanalyze_count::numeric,
                        'last_autovacuum', last_autovacuum,
                        'last_autoanalyze', last_autoanalyze
                    )
                    ORDER BY schemaname, relname, relid
                ),
                '[]'::jsonb
            )
    ) AS value
    FROM pg_stat_user_tables
    -- The snapshot itself creates a session-local census table above.
    -- Exclude pg_temp (and every non-public schema) so two independent psql
    -- sessions compare only stable application relations and OIDs.
    WHERE schemaname = 'public'
),
io_stats AS (
    SELECT jsonb_build_object(
        'reads', COALESCE(sum(reads), 0)::numeric,
        'read_time_ms', COALESCE(sum(read_time), 0)::numeric,
        'writes', COALESCE(sum(writes), 0)::numeric,
        'write_time_ms', COALESCE(sum(write_time), 0)::numeric,
        'writebacks', COALESCE(sum(writebacks), 0)::numeric,
        'writeback_time_ms', COALESCE(sum(writeback_time), 0)::numeric,
        'extends', COALESCE(sum(extends), 0)::numeric,
        'extend_time_ms', COALESCE(sum(extend_time), 0)::numeric,
        'fsyncs', COALESCE(sum(fsyncs), 0)::numeric,
        'fsync_time_ms', COALESCE(sum(fsync_time), 0)::numeric,
        'stats_reset',
            COALESCE(
                jsonb_agg(DISTINCT stats_reset ORDER BY stats_reset),
                '[]'::jsonb
            )
    ) AS value
    FROM pg_stat_io
),
terminal_rows AS (
    SELECT jsonb_build_object(
        'terminal_run_admissions',
            (SELECT count(*) FROM terminal_run_admissions),
        'terminal_run_results',
            (SELECT count(*) FROM terminal_run_results),
        'conversations',
            (SELECT count(*) FROM conversations),
        'conversation_messages',
            (SELECT count(*) FROM conversation_messages),
        'conversation_summaries',
            (SELECT count(*) FROM conversation_summaries)
    ) AS value
),
terminal_sizes AS (
    SELECT jsonb_build_object(
        'terminal_run_admissions',
            pg_total_relation_size('terminal_run_admissions'::regclass),
        'terminal_run_results',
            pg_total_relation_size('terminal_run_results'::regclass),
        'conversations',
            pg_total_relation_size('conversations'::regclass),
        'conversation_messages',
            pg_total_relation_size('conversation_messages'::regclass),
        'conversation_summaries',
            pg_total_relation_size('conversation_summaries'::regclass),
        'terminal_content_deletion_jobs',
            pg_total_relation_size('terminal_content_deletion_jobs'::regclass),
        'terminal_artifact_staging',
            pg_total_relation_size('terminal_artifact_staging'::regclass)
    ) AS value
),
ledger_rows AS (
    SELECT COALESCE(
        jsonb_object_agg(table_name, row_count ORDER BY table_name),
        '{}'::jsonb
    ) AS value
    FROM terminal_qualification_forbidden_rows
)
SELECT jsonb_build_object(
    'captured_at', clock_timestamp(),
    'server_version', current_setting('server_version'),
    'settings', settings.value,
    'qualification_relation_persistence', qualification_relations.value,
    'statement_stats', statement_stats.value,
    'top_wal_statements', top_wal.value,
    'boundary', boundary.value,
    'wal', wal.value,
    'bgwriter', bgwriter.value,
    'database', database_stats.value,
    'maintenance_stats', maintenance_stats.value,
    'io', io_stats.value,
    'terminal_rows', terminal_rows.value,
    'terminal_relation_bytes', terminal_sizes.value,
    'ledger_rows', ledger_rows.value,
    'forbidden_durable_rows', ledger_rows.value
)::text
FROM settings, qualification_relations, statement_stats, top_wal, boundary,
     wal, bgwriter, database_stats, maintenance_stats, io_stats, terminal_rows,
     terminal_sizes, ledger_rows;
