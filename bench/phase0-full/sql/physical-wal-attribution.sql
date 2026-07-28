\set ON_ERROR_STOP on

-- Attribute every record in the exact half-open LSN interval exactly once.
-- pg_get_wal_block_info supplies relation block references; records without a
-- block reference (for example COMMIT/CHECKPOINT) remain structural. Mixed or
-- no-longer-mappable relation records remain explicit unresolved categories.
WITH
records AS (
    SELECT
        start_lsn,
        end_lsn,
        resource_manager,
        record_type,
        record_length::bigint,
        main_data_length::bigint,
        fpi_length::bigint
    FROM pg_get_wal_records_info(
        :'start_lsn'::pg_lsn,
        :'end_lsn'::pg_lsn
    )
),
raw_blocks AS (
    SELECT
        block.start_lsn,
        block.block_id,
        block.reltablespace,
        block.reldatabase,
        block.relfilenode,
        block.resource_manager,
        block.record_type,
        block.block_data_length::bigint,
        block.block_fpi_length::bigint,
        CASE
            WHEN block.reldatabase = (
                SELECT oid FROM pg_database WHERE datname = current_database()
            )
            THEN pg_filenode_relation(
                block.reltablespace,
                block.relfilenode
            )
            ELSE NULL
        END AS relation_oid
    FROM pg_get_wal_block_info(
        :'start_lsn'::pg_lsn,
        :'end_lsn'::pg_lsn,
        false
    ) AS block
),
resolved_blocks AS (
    SELECT
        block.*,
        root_class.oid AS root_relation_oid,
        root_namespace.nspname AS root_schema_name,
        root_class.relname AS root_relation_name,
        CASE
            WHEN block.reldatabase = 0 THEN 'structural'
            WHEN block.reldatabase <> (
                SELECT oid FROM pg_database WHERE datname = current_database()
            ) THEN 'unmapped'
            WHEN block.relation_oid IS NULL OR root_class.oid IS NULL
                THEN 'unmapped'
            WHEN root_namespace.nspname = 'public'
             AND root_class.relname = 'payloads'
                THEN 'payload'
            WHEN root_namespace.nspname = 'public'
             AND root_class.relname IN (
                'artifacts',
                'artifact_gc_claims',
                'artifact_gc_sweeps',
                'artifact_retention_releases',
                'artifact_store_authority',
                'recovery_artifact_roots',
                'terminal_artifact_staging',
                'terminal_content_deletion_jobs'
             ) THEN 'artifact_object_metadata'
            ELSE 'structural'
        END AS block_category
    FROM raw_blocks AS block
    LEFT JOIN pg_class AS direct_class
      ON direct_class.oid = block.relation_oid
    LEFT JOIN pg_index AS index_catalog
      ON index_catalog.indexrelid = block.relation_oid
    LEFT JOIN pg_class AS indexed_table
      ON indexed_table.oid = index_catalog.indrelid
    LEFT JOIN pg_class AS direct_toast_owner
      ON direct_toast_owner.reltoastrelid = block.relation_oid
    LEFT JOIN pg_class AS indexed_toast_owner
      ON indexed_toast_owner.reltoastrelid = indexed_table.oid
    LEFT JOIN pg_class AS root_class
      ON root_class.oid = COALESCE(
          direct_toast_owner.oid,
          indexed_toast_owner.oid,
          indexed_table.oid,
          direct_class.oid
      )
    LEFT JOIN pg_namespace AS root_namespace
      ON root_namespace.oid = root_class.relnamespace
),
record_flags AS (
    SELECT
        record.start_lsn,
        count(block.block_id)::bigint AS block_reference_count,
        count(DISTINCT block.block_category)::bigint
            AS distinct_block_categories,
        bool_or(block.block_category = 'payload') AS has_payload,
        bool_or(block.block_category = 'artifact_object_metadata')
            AS has_artifact,
        bool_or(block.block_category = 'structural') AS has_structural,
        bool_or(block.block_category = 'unmapped') AS has_unmapped
    FROM records AS record
    LEFT JOIN resolved_blocks AS block USING (start_lsn)
    GROUP BY record.start_lsn
),
classified_records AS (
    SELECT
        record.*,
        flags.block_reference_count,
        CASE
            WHEN flags.block_reference_count = 0 THEN 'structural'
            WHEN flags.has_unmapped THEN 'unmapped'
            WHEN flags.distinct_block_categories > 1 THEN 'mixed'
            WHEN flags.has_payload THEN 'payload'
            WHEN flags.has_artifact THEN 'artifact_object_metadata'
            ELSE 'structural'
        END AS category
    FROM records AS record
    JOIN record_flags AS flags USING (start_lsn)
),
grouped AS (
    SELECT
        resource_manager,
        record_type,
        count(*)::bigint AS record_count,
        sum(record_length)::bigint AS record_length_bytes,
        sum(main_data_length)::bigint AS main_data_length_bytes,
        sum(fpi_length)::bigint AS fpi_length_bytes
    FROM classified_records
    GROUP BY resource_manager, record_type
),
category_names AS (
    SELECT unnest(ARRAY[
        'payload',
        'artifact_object_metadata',
        'structural',
        'mixed',
        'unmapped'
    ]::text[]) AS category
),
category_totals AS (
    SELECT
        names.category,
        COALESCE(counts.record_count, 0)::bigint AS record_count,
        COALESCE(counts.record_length_bytes, 0)::bigint
            AS record_length_bytes,
        COALESCE(counts.main_data_length_bytes, 0)::bigint
            AS main_data_length_bytes,
        COALESCE(counts.fpi_length_bytes, 0)::bigint AS fpi_length_bytes
    FROM category_names AS names
    LEFT JOIN (
        SELECT
            category,
            count(*)::bigint AS record_count,
            sum(record_length)::bigint AS record_length_bytes,
            sum(main_data_length)::bigint AS main_data_length_bytes,
            sum(fpi_length)::bigint AS fpi_length_bytes
        FROM classified_records
        GROUP BY category
    ) AS counts USING (category)
),
relation_block_groups AS (
    SELECT
        block_category AS category,
        COALESCE(root_schema_name, '<unmapped>') AS schema_name,
        COALESCE(root_relation_name, '<unmapped>') AS relation_name,
        resource_manager,
        record_type,
        count(*)::bigint AS block_reference_count,
        sum(block_data_length)::bigint AS block_data_length_bytes,
        sum(block_fpi_length)::bigint AS block_fpi_length_bytes
    FROM resolved_blocks
    GROUP BY
        block_category,
        COALESCE(root_schema_name, '<unmapped>'),
        COALESCE(root_relation_name, '<unmapped>'),
        resource_manager,
        record_type
),
groups_json AS (
    SELECT COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'resource_manager', resource_manager,
                'record_type', record_type,
                'record_count', record_count,
                'record_length_bytes', record_length_bytes,
                'main_data_length_bytes', main_data_length_bytes,
                'fpi_length_bytes', fpi_length_bytes
            )
            ORDER BY resource_manager, record_type
        ),
        '[]'::jsonb
    ) AS value
    FROM grouped
),
categories_json AS (
    SELECT jsonb_agg(
        jsonb_build_object(
            'category', category,
            'record_count', record_count,
            'record_length_bytes', record_length_bytes,
            'main_data_length_bytes', main_data_length_bytes,
            'fpi_length_bytes', fpi_length_bytes
        )
        ORDER BY category
    ) AS value
    FROM category_totals
),
relations_json AS (
    SELECT COALESCE(
        jsonb_agg(
            jsonb_build_object(
                'category', category,
                'schema_name', schema_name,
                'relation_name', relation_name,
                'resource_manager', resource_manager,
                'record_type', record_type,
                'block_reference_count', block_reference_count,
                'block_data_length_bytes', block_data_length_bytes,
                'block_fpi_length_bytes', block_fpi_length_bytes
            )
            ORDER BY
                category,
                schema_name,
                relation_name,
                resource_manager,
                record_type
        ),
        '[]'::jsonb
    ) AS value
    FROM relation_block_groups
),
totals AS (
    SELECT
        count(*)::bigint AS record_count,
        COALESCE(sum(record_length), 0)::bigint AS record_length_bytes,
        COALESCE(sum(main_data_length), 0)::bigint
            AS main_data_length_bytes,
        COALESCE(sum(fpi_length), 0)::bigint AS fpi_length_bytes
    FROM classified_records
)
SELECT jsonb_build_object(
    'extension', 'pg_walinspect',
    'extension_version',
        (
            SELECT extversion
            FROM pg_extension
            WHERE extname = 'pg_walinspect'
        ),
    'start_lsn', :'start_lsn',
    'end_lsn', :'end_lsn',
    'lsn_span_bytes',
        pg_wal_lsn_diff(
            :'end_lsn'::pg_lsn,
            :'start_lsn'::pg_lsn
        )::numeric,
    'groups', groups_json.value,
    'categories', categories_json.value,
    'relation_block_groups', relations_json.value,
    'totals', jsonb_build_object(
        'record_count', totals.record_count,
        'record_length_bytes', totals.record_length_bytes,
        'main_data_length_bytes', totals.main_data_length_bytes,
        'fpi_length_bytes', totals.fpi_length_bytes
    )
)::text
FROM groups_json, categories_json, relations_json, totals;
