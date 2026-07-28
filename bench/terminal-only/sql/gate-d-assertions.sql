\set ON_ERROR_STOP on

WITH selected_conversations AS (
    SELECT conversation_id
    FROM conversations
    WHERE tenant_id = :'tenant_id'
),
selected_admissions AS (
    SELECT a.*
    FROM terminal_run_admissions a
    JOIN selected_conversations c USING (conversation_id)
),
ranked_admissions AS (
    SELECT a.*,
           row_number() OVER (
             PARTITION BY conversation_id ORDER BY accepted_at, run_id
           ) AS turn_number
    FROM selected_admissions a
),
selected_messages AS (
    SELECT m.*
    FROM conversation_messages m
    JOIN selected_conversations c USING (conversation_id)
),
selected_results AS (
    SELECT r.*
    FROM terminal_run_results r
    JOIN selected_admissions a USING (run_id)
)
SELECT jsonb_build_object(
    'conversations', (SELECT count(*) FROM selected_conversations),
    'admissions', (SELECT count(*) FROM selected_admissions),
    'results', (SELECT count(*) FROM selected_results),
    'succeeded_results', (
      SELECT count(*) FROM selected_results WHERE terminal_state='succeeded'
    ),
    'messages', (SELECT count(*) FROM selected_messages),
    'user_messages', (
      SELECT count(*) FROM selected_messages WHERE role='user'
    ),
    'distinct_admission_user_messages', (
      SELECT count(DISTINCT user_message_id) FROM selected_admissions
    ),
    'user_without_admission', (
      SELECT count(*)
      FROM selected_messages m
      LEFT JOIN selected_admissions a
        ON a.conversation_id=m.conversation_id
       AND a.user_message_id=m.message_id
      WHERE m.role='user' AND a.run_id IS NULL
    ),
    'assistant_messages', (
      SELECT count(*) FROM selected_messages WHERE role='assistant'
    ),
    'assistant_without_result', (
      SELECT count(*)
      FROM selected_messages m
      LEFT JOIN selected_results r ON r.run_id=m.run_id
      WHERE m.role='assistant' AND r.run_id IS NULL
    ),
    'result_without_assistant', (
      SELECT count(*)
      FROM selected_results r
      JOIN selected_admissions a USING (run_id)
      LEFT JOIN selected_messages m
        ON m.conversation_id=a.conversation_id
       AND m.run_id=a.run_id
       AND m.role='assistant'
      WHERE m.message_id IS NULL
    ),
    'admission_without_user', (
      SELECT count(*)
      FROM selected_admissions a
      LEFT JOIN selected_messages m
        ON m.conversation_id=a.conversation_id
       AND m.message_id=a.user_message_id
       AND m.role='user'
      WHERE m.message_id IS NULL
    ),
    'turn_order_violations', (
      SELECT count(*)
      FROM selected_admissions a
      JOIN selected_messages u
        ON u.message_id=a.user_message_id AND u.role='user'
      JOIN selected_messages assistant
        ON assistant.run_id=a.run_id AND assistant.role='assistant'
      WHERE assistant.message_order <= u.message_order
    ),
    'missing_context_hash_after_first_turn', (
      SELECT count(*)
      FROM ranked_admissions
      WHERE turn_number > 1 AND selected_context_hash IS NULL
    ),
    'conversations_with_summary', (
      SELECT count(DISTINCT s.conversation_id)
      FROM conversation_summaries s
      JOIN selected_conversations c USING (conversation_id)
    ),
    'max_messages_per_conversation', (
      SELECT COALESCE(max(message_count), 0)
      FROM (
        SELECT count(*) AS message_count
        FROM selected_messages
        GROUP BY conversation_id
      ) counts
    )
)::text;
