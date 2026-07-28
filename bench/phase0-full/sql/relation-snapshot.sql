\set ON_ERROR_STOP on

CREATE TEMP TABLE phase0_full_relation_rows (
    table_name text PRIMARY KEY,
    row_count bigint NOT NULL
);

SELECT format(
    'INSERT INTO phase0_full_relation_rows ' ||
    'SELECT %L, count(*) FROM public.%I',
    class.relname,
    class.relname
)
FROM pg_class AS class
JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
WHERE namespace.nspname = 'public'
  AND class.relkind IN ('r', 'p')
  AND class.relpersistence = 'p'
ORDER BY class.relname
\gexec

WITH
tables AS (
    SELECT
        class.oid AS table_oid,
        namespace.nspname AS schema_name,
        class.relname AS table_name,
        class.relpersistence::text AS persistence,
        CASE
            WHEN class.relname = 'payloads' THEN 'payload'
            WHEN class.relname IN (
                'artifacts',
                'artifact_gc_claims',
                'artifact_gc_sweeps',
                'artifact_retention_releases',
                'artifact_store_authority',
                'recovery_artifact_roots',
                'terminal_artifact_staging',
                'terminal_content_deletion_jobs'
            ) THEN 'artifact_object_metadata'
            WHEN class.relname IN (
                'durable_schema_contract',
                'workflow_definitions',
                'workflow_definition_revisions',
                'deployment_revisions',
                'agent_publication_heads',
                'workflow_definition_public_metadata'
            ) THEN 'catalog'
            ELSE 'structural'
        END AS category,
        pg_relation_size(class.oid)::bigint AS heap_main_bytes,
        pg_table_size(class.oid)::bigint AS table_and_auxiliary_bytes,
        pg_indexes_size(class.oid)::bigint AS indexes_bytes,
        pg_total_relation_size(class.oid)::bigint AS total_bytes
    FROM pg_class AS class
    JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
    WHERE namespace.nspname = 'public'
      AND class.relkind IN ('r', 'p')
      AND class.relpersistence = 'p'
),
indexes AS (
    SELECT
        table_class.relname AS table_name,
        index_class.relname AS index_name,
        table_data.category,
        index_class.oid AS index_oid,
        pg_relation_size(index_class.oid)::bigint AS bytes
    FROM pg_index AS index_catalog
    JOIN pg_class AS index_class
      ON index_class.oid = index_catalog.indexrelid
    JOIN pg_class AS table_class
      ON table_class.oid = index_catalog.indrelid
    JOIN pg_namespace AS namespace
      ON namespace.oid = table_class.relnamespace
    JOIN tables AS table_data
      ON table_data.table_oid = table_class.oid
    WHERE namespace.nspname = 'public'
),
table_json AS (
    SELECT COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'schema_name', schema_name,
                'table_name', table_name,
                'table_oid', table_oid,
                'persistence', persistence,
                'category', category,
                'heap_main_bytes', heap_main_bytes,
                'table_and_auxiliary_bytes', table_and_auxiliary_bytes,
                'indexes_bytes', indexes_bytes,
                'total_bytes', total_bytes
            )
            ORDER BY table_name
        ),
        '[]'::jsonb
    ) AS value
    FROM tables
),
index_json AS (
    SELECT COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'table_name', table_name,
                'index_name', index_name,
                'index_oid', index_oid,
                'category', category,
                'bytes', bytes
            )
            ORDER BY table_name, index_name
        ),
        '[]'::jsonb
    ) AS value
    FROM indexes
),
category_names AS (
    SELECT unnest(ARRAY[
        'payload',
        'artifact_object_metadata',
        'structural',
        'catalog'
    ]::text[]) AS category
),
category_json AS (
    SELECT jsonb_object_agg(
        names.category,
        jsonb_build_object(
            'heap_main_bytes', COALESCE(sizes.heap_main_bytes, 0),
            'table_and_auxiliary_bytes',
                COALESCE(sizes.table_and_auxiliary_bytes, 0),
            'indexes_bytes', COALESCE(sizes.indexes_bytes, 0),
            'total_bytes', COALESCE(sizes.total_bytes, 0)
        )
        ORDER BY names.category
    ) AS value
    FROM category_names AS names
    LEFT JOIN (
        SELECT
            category,
            sum(heap_main_bytes)::bigint AS heap_main_bytes,
            sum(table_and_auxiliary_bytes)::bigint
                AS table_and_auxiliary_bytes,
            sum(indexes_bytes)::bigint AS indexes_bytes,
            sum(total_bytes)::bigint AS total_bytes
        FROM tables
        GROUP BY category
    ) AS sizes USING (category)
),
rows_json AS (
    SELECT jsonb_object_agg(table_name, row_count ORDER BY table_name) AS value
    FROM phase0_full_relation_rows
)
SELECT jsonb_build_object(
    'captured_at', clock_timestamp(),
    'database_name', current_database(),
    'tables', table_json.value,
    'indexes', index_json.value,
    'category_totals', category_json.value,
    'row_counts', rows_json.value
)::text
FROM table_json, index_json, category_json, rows_json;
