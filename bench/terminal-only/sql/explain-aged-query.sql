\set ON_ERROR_STOP on

EXPLAIN (ANALYZE, BUFFERS, SETTINGS, FORMAT TEXT)
SELECT message_id, message_order, role, run_id, content_inline, content_ref,
       content_hash, created_at
FROM conversation_messages
WHERE conversation_id = :'conversation_id'
ORDER BY message_order DESC
LIMIT 50;
