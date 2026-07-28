\set ON_ERROR_STOP on

-- The caller supplies the exact pg_current_wal_insert_lsn() values embedded in
-- the before/after snapshots. pg_walinspect reads the physical records in that
-- half-open interval; it is an independent source view and is never added to
-- pg_stat_statements WAL.
WITH records AS (
    SELECT
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
grouped AS (
    SELECT
        resource_manager,
        record_type,
        count(*)::bigint AS record_count,
        sum(record_length)::bigint AS record_length_bytes,
        sum(main_data_length)::bigint AS main_data_length_bytes,
        sum(fpi_length)::bigint AS fpi_length_bytes
    FROM records
    GROUP BY resource_manager, record_type
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
totals AS (
    SELECT jsonb_build_object(
        'record_count', COALESCE(sum(record_count), 0)::bigint,
        'record_length_bytes', COALESCE(sum(record_length_bytes), 0)::bigint,
        'main_data_length_bytes',
            COALESCE(sum(main_data_length_bytes), 0)::bigint,
        'fpi_length_bytes', COALESCE(sum(fpi_length_bytes), 0)::bigint
    ) AS value
    FROM grouped
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
    'groups', groups_json.value,
    'totals', totals.value
)::text
FROM groups_json, totals;
