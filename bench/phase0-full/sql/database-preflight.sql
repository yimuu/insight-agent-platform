\set ON_ERROR_STOP on

-- The namespace/PVC preflight proves this cluster was freshly provisioned.
-- This database-side census independently rejects any pre-existing workload
-- history. A full runtime may already have emitted empty artifact-GC sweep
-- intents while becoming Ready, so that one maintenance ledger is reported
-- but deliberately not treated as workload history.
CREATE TEMP TABLE phase0_full_workload_rows (
    table_name text PRIMARY KEY,
    row_count bigint NOT NULL
);

SELECT format(
    'INSERT INTO phase0_full_workload_rows ' ||
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
      'artifact_store_authority',
      'workflow_definitions',
      'workflow_definition_revisions',
      'deployment_revisions',
      'agent_publication_heads',
      'workflow_definition_public_metadata',
      'artifact_gc_sweeps'
  )
ORDER BY class.relname
\gexec

WITH
workload AS (
    SELECT
        COALESCE(sum(row_count), 0)::bigint AS total_rows,
        COALESCE(
            jsonb_object_agg(table_name, row_count ORDER BY table_name)
                FILTER (WHERE row_count <> 0),
            '{}'::jsonb
        ) AS nonzero_rows
    FROM phase0_full_workload_rows
),
catalog AS (
    SELECT jsonb_build_object(
        'workflow_definitions', (SELECT count(*) FROM workflow_definitions),
        'workflow_definition_revisions',
            (SELECT count(*) FROM workflow_definition_revisions),
        'deployment_revisions', (SELECT count(*) FROM deployment_revisions),
        'agent_publication_heads',
            (SELECT count(*) FROM agent_publication_heads),
        'workflow_definition_public_metadata',
            (SELECT count(*) FROM workflow_definition_public_metadata)
    ) AS counts
),
published_action AS (
    SELECT
        head.agent_id,
        head.definition_id,
        head.definition_revision_id,
        head.deployment_revision_id,
        policy.policy_count,
        policy.persistence_mode
    FROM agent_publication_heads AS head
    JOIN deployment_revisions AS revision
      ON revision.definition_id = head.definition_id
     AND revision.definition_revision_id = head.definition_revision_id
     AND revision.deployment_revision_id = head.deployment_revision_id
    CROSS JOIN LATERAL (
        SELECT
            count(*)::bigint AS policy_count,
            min(element->'deployment_policy'->>'persistence_mode')
                AS persistence_mode
        FROM jsonb_array_elements(revision.resolved_bindings) AS element
        WHERE element ? 'deployment_policy'
    ) AS policy
    WHERE head.agent_id = 'action_demo'
),
policy AS (
    SELECT jsonb_build_object(
        'published_agent_count',
            (SELECT count(*) FROM agent_publication_heads),
        'action_demo_rows', count(*),
        'policy_count', COALESCE(min(policy_count), 0),
        'persistence_mode', min(persistence_mode)
    ) AS value
    FROM published_action
),
settings AS (
    SELECT jsonb_build_object(
        'fsync', current_setting('fsync'),
        'full_page_writes', current_setting('full_page_writes'),
        'synchronous_commit', current_setting('synchronous_commit'),
        'track_io_timing', current_setting('track_io_timing'),
        'pg_stat_statements_track', current_setting('pg_stat_statements.track'),
        'pg_stat_statements_track_utility',
            current_setting('pg_stat_statements.track_utility'),
        'wal_keep_size', current_setting('wal_keep_size'),
        'wal_keep_size_bytes',
            pg_size_bytes(current_setting('wal_keep_size'))::numeric
    ) AS value
),
authority AS (
    SELECT count(*)::bigint AS row_count
    FROM artifact_store_authority
),
maintenance AS (
    SELECT count(*)::bigint AS artifact_gc_sweeps_rows
    FROM artifact_gc_sweeps
)
SELECT jsonb_build_object(
    'passed',
        workload.total_rows = 0
        AND catalog.counts = jsonb_build_object(
            'workflow_definitions', 1,
            'workflow_definition_revisions', 1,
            'deployment_revisions', 1,
            'agent_publication_heads', 1,
            'workflow_definition_public_metadata', 1
        )
        AND policy.value = jsonb_build_object(
            'published_agent_count', 1,
            'action_demo_rows', 1,
            'policy_count', 1,
            'persistence_mode', 'full'
        )
        AND authority.row_count = 1
        AND settings.value->>'fsync' = 'on'
        AND settings.value->>'full_page_writes' = 'on'
        AND settings.value->>'synchronous_commit' IN ('on', 'remote_apply')
        AND settings.value->>'track_io_timing' = 'on'
        AND settings.value->>'pg_stat_statements_track' = 'all'
        AND settings.value->>'pg_stat_statements_track_utility' = 'on'
        AND (settings.value->>'wal_keep_size_bytes')::numeric >= 8589934592,
    'captured_at', clock_timestamp(),
    'preexisting_workload_rows', workload.total_rows,
    'nonzero_workload_rows', workload.nonzero_rows,
    'catalog_counts', catalog.counts,
    'expected_catalog_count_each', 1,
    'deployment_policy', policy.value,
    'artifact_store_authority_rows', authority.row_count,
    'allowed_preworkload_maintenance', jsonb_build_object(
        'artifact_gc_sweeps', maintenance.artifact_gc_sweeps_rows
    ),
    'settings', settings.value,
    'minimum_wal_keep_size_bytes', 8589934592
)::text
FROM workload, catalog, policy, settings, authority, maintenance;
