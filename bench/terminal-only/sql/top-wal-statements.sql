\set ON_ERROR_STOP on

SELECT
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
ORDER BY wal_bytes DESC, calls DESC
LIMIT 30;
