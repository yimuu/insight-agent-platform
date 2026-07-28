\set ON_ERROR_STOP on

CREATE TEMP TABLE aged_query_latencies (elapsed_ms double precision NOT NULL);

SELECT set_config('benchmark.conversation_id', :'conversation_id', false)
    AS benchmark_conversation_id \gset
SELECT set_config('benchmark.sample_count', :'sample_count', false)
    AS benchmark_sample_count \gset

DO $$
DECLARE
    started_at timestamptz;
    iteration integer;
BEGIN
    FOR iteration IN 1..20 LOOP
        PERFORM message_id
        FROM conversation_messages
        WHERE conversation_id = current_setting('benchmark.conversation_id')
        ORDER BY message_order DESC
        LIMIT 50;
    END LOOP;
    FOR iteration IN 1..current_setting('benchmark.sample_count')::integer LOOP
        started_at := clock_timestamp();
        PERFORM message_id, message_order, role, run_id, content_inline,
                content_ref, content_hash, created_at
        FROM conversation_messages
        WHERE conversation_id = current_setting('benchmark.conversation_id')
        ORDER BY message_order DESC
        LIMIT 50;
        INSERT INTO aged_query_latencies
        VALUES (
          extract(epoch FROM (clock_timestamp() - started_at)) * 1000.0
        );
    END LOOP;
END
$$;

SELECT jsonb_build_object(
    'samples', count(*),
    'p50_ms', percentile_cont(0.50) WITHIN GROUP (ORDER BY elapsed_ms),
    'p95_ms', percentile_cont(0.95) WITHIN GROUP (ORDER BY elapsed_ms),
    'p99_ms', percentile_cont(0.99) WITHIN GROUP (ORDER BY elapsed_ms),
    'max_ms', max(elapsed_ms)
)::text
FROM aged_query_latencies;
