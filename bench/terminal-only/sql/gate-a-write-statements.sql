\set ON_ERROR_STOP on

WITH public_durable_tables AS (
    SELECT class.relname
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
),
statements AS (
    SELECT
        lower(ltrim(query)) AS normalized_query,
        calls::numeric AS calls,
        rows::numeric AS rows,
        wal_bytes::numeric AS wal_bytes
    FROM pg_stat_statements
    WHERE dbid = (
        SELECT oid FROM pg_database WHERE datname = current_database()
    )
),
forbidden_mutations AS (
    SELECT
        table_name.relname AS table_name,
        sum(statement.calls)::numeric AS calls,
        sum(statement.rows)::numeric AS rows
    FROM statements AS statement
    JOIN public_durable_tables AS table_name
      ON statement.normalized_query ~ (
          '(^|[[:space:](])(insert[[:space:]]+into|merge[[:space:]]+into|' ||
          'update|delete[[:space:]]+from|copy|' ||
          'truncate([[:space:]]+table)?)[[:space:]]+' ||
          '(only[[:space:]]+)?("?public"?[.])?"?' ||
          table_name.relname || '"?([[:space:](]|$)'
      )
    GROUP BY table_name.relname
),
classified AS (
    SELECT
        COALESCE(sum(calls) FILTER (
            WHERE normalized_query LIKE 'insert into terminal_run_admissions%'
        ), 0) AS admission_insert_calls,
        COALESCE(sum(rows) FILTER (
            WHERE normalized_query LIKE 'insert into terminal_run_admissions%'
        ), 0) AS admission_insert_rows,
        COALESCE(sum(calls) FILTER (
            WHERE normalized_query LIKE 'insert into terminal_run_results%'
        ), 0) AS result_insert_calls,
        COALESCE(sum(rows) FILTER (
            WHERE normalized_query LIKE 'insert into terminal_run_results%'
        ), 0) AS result_insert_rows,
        COALESCE(sum(calls) FILTER (
            WHERE normalized_query LIKE 'insert into conversation_messages%'
        ), 0) AS message_insert_calls,
        COALESCE(sum(rows) FILTER (
            WHERE normalized_query LIKE 'insert into conversation_messages%'
        ), 0) AS message_insert_rows,
        COALESCE(sum(calls) FILTER (
            WHERE normalized_query ~ (
                '(^|[[:space:](])(update|delete[[:space:]]+from|' ||
                'truncate([[:space:]]+table)?)[[:space:]]+' ||
                '(only[[:space:]]+)?("?public"?[.])?"?' ||
                'terminal_run_(admissions|results)"?([[:space:](]|$)'
            )
        ), 0) AS terminal_mutation_calls,
        COALESCE(sum(wal_bytes) FILTER (
            WHERE normalized_query LIKE 'insert into terminal_run_admissions%'
               OR normalized_query LIKE 'insert into terminal_run_results%'
               OR normalized_query LIKE 'insert into conversation_messages%'
        ), 0) AS core_insert_wal_bytes
    FROM statements
)
SELECT jsonb_build_object(
    'admission_insert_calls', admission_insert_calls,
    'admission_insert_rows', admission_insert_rows,
    'result_insert_calls', result_insert_calls,
    'result_insert_rows', result_insert_rows,
    'message_insert_calls', message_insert_calls,
    'message_insert_rows', message_insert_rows,
    'terminal_mutation_calls', terminal_mutation_calls,
    'forbidden_durable_table_count',
        (SELECT count(*) FROM public_durable_tables),
    'forbidden_durable_tables',
        (SELECT COALESCE(jsonb_agg(relname ORDER BY relname), '[]'::jsonb)
         FROM public_durable_tables),
    'forbidden_durable_mutation_calls',
        (SELECT COALESCE(sum(calls), 0) FROM forbidden_mutations),
    'forbidden_durable_mutations',
        (SELECT COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'table', table_name,
                    'calls', calls,
                    'rows', rows
                )
                ORDER BY table_name
            ),
            '[]'::jsonb
         )
         FROM forbidden_mutations),
    'core_insert_wal_bytes', core_insert_wal_bytes
)::text
FROM classified;
