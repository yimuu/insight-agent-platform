\set ON_ERROR_STOP on

-- A formal Gate B database may contain only the current seven deployment
-- catalog rows plus the one artifact-store authority row before warm-up.
-- Every historical Run/Conversation/ledger/GC row remains in this census,
-- including artifact_gc_sweeps.
CREATE TEMP TABLE gate_b_old_ledger_rows (
    table_name text PRIMARY KEY,
    row_count bigint NOT NULL
);

SELECT format(
    'INSERT INTO gate_b_old_ledger_rows ' ||
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
      'workflow_definition_public_metadata'
  )
ORDER BY class.relname
\gexec

WITH
old_ledgers AS (
    SELECT
        COALESCE(sum(row_count), 0)::bigint AS total_rows,
        COALESCE(
            jsonb_object_agg(table_name, row_count ORDER BY table_name)
                FILTER (WHERE row_count <> 0),
            '{}'::jsonb
        ) AS nonzero_rows
    FROM gate_b_old_ledger_rows
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
policies AS (
    SELECT
        count(*)::bigint AS deployment_count,
        count(*) FILTER (
            WHERE policy.policy_count = 1
              AND policy.persistence_mode = 'terminal_only'
        )::bigint AS terminal_only_count,
        count(*) FILTER (
            WHERE policy.policy_count <> 1
               OR policy.persistence_mode IS DISTINCT FROM 'terminal_only'
        )::bigint AS invalid_or_full_count
    FROM deployment_revisions AS revision
    CROSS JOIN LATERAL (
        SELECT
            count(*)::bigint AS policy_count,
            min(
                element->'deployment_policy'->>'persistence_mode'
            ) AS persistence_mode
        FROM jsonb_array_elements(revision.resolved_bindings) AS element
        WHERE element ? 'deployment_policy'
    ) AS policy
),
authority AS (
    SELECT count(*)::bigint AS row_count
    FROM artifact_store_authority
)
SELECT jsonb_build_object(
    'passed',
        old_ledgers.total_rows = 0
        AND catalog.counts = jsonb_build_object(
            'workflow_definitions', 7,
            'workflow_definition_revisions', 7,
            'deployment_revisions', 7,
            'agent_publication_heads', 7,
            'workflow_definition_public_metadata', 7
        )
        AND policies.deployment_count = 7
        AND policies.terminal_only_count = 7
        AND policies.invalid_or_full_count = 0
        AND authority.row_count = 1,
    'captured_at', clock_timestamp(),
    'old_ledger_total_rows', old_ledgers.total_rows,
    'nonzero_old_ledger_rows', old_ledgers.nonzero_rows,
    'catalog_counts', catalog.counts,
    'expected_catalog_count_each', 7,
    'deployment_policy', jsonb_build_object(
        'deployment_count', policies.deployment_count,
        'terminal_only_count', policies.terminal_only_count,
        'invalid_or_full_count', policies.invalid_or_full_count
    ),
    'artifact_store_authority_rows', authority.row_count,
    'artifact_gc_sweeps_rows',
        (
            SELECT row_count
            FROM gate_b_old_ledger_rows
            WHERE table_name = 'artifact_gc_sweeps'
        )
)::text
FROM old_ledgers, catalog, policies, authority;
