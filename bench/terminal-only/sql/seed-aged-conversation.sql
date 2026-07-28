\set ON_ERROR_STOP on

INSERT INTO conversations (
    conversation_id, tenant_id, user_id, agent_id, persistence_mode,
    deployment_revision_id, created_at, archived_at
) VALUES (
    :'conversation_id', :'tenant_id', :'user_id', :'agent_id',
    'terminal_only', :'deployment_revision_id', clock_timestamp(), NULL
);

INSERT INTO conversation_messages (
    message_id, conversation_id, role, run_id, content_inline, content_ref,
    content_hash, created_at
)
SELECT
    format('%s-message-%s', :'conversation_id', ordinal),
    :'conversation_id',
    'user',
    NULL,
    jsonb_build_object('text', format('aged fixture %s', ordinal)),
    NULL,
    'sha256:' || repeat('0', 64),
    clock_timestamp() - make_interval(secs => (:'message_count'::bigint - ordinal))
FROM generate_series(1, :'message_count'::bigint) AS ordinal;

ANALYZE conversation_messages;
