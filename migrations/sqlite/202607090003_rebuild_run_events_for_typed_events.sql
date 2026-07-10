DROP INDEX IF EXISTS idx_run_events_run_id;

ALTER TABLE run_events RENAME TO run_events_legacy;

CREATE TABLE run_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    type TEXT NOT NULL,
    seq INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    code INTEGER NOT NULL,
    message TEXT NOT NULL,
    data TEXT NOT NULL
);

INSERT INTO run_events (run_id, type, seq, timestamp, code, message, data)
SELECT
    legacy.run_id,
    CASE legacy.event
        WHEN 'run_started' THEN 'run.started'
        WHEN 'step_started' THEN 'step.started'
        WHEN 'token_delta' THEN 'content.delta'
        WHEN 'tool_call_started' THEN 'tool_call.started'
        WHEN 'tool_call_completed' THEN 'tool_call.completed'
        WHEN 'step_completed' THEN 'step.completed'
        WHEN 'run_completed' THEN 'run.completed'
        WHEN 'error' THEN 'run.failed'
        ELSE legacy.event
    END,
    ROW_NUMBER() OVER (PARTITION BY legacy.run_id ORDER BY legacy.id),
    legacy.timestamp,
    legacy.code,
    legacy.message,
    CASE legacy.event
        WHEN 'token_delta' THEN json_object('step_id', legacy.step_id, 'content', legacy.content)
        WHEN 'run_started' THEN json_object('status', 'running')
        WHEN 'step_started' THEN json_object('step_id', legacy.step_id, 'status', 'running')
        WHEN 'tool_call_started' THEN json_object('step_id', legacy.step_id, 'status', 'running')
        WHEN 'tool_call_completed' THEN json_object('step_id', legacy.step_id, 'status', 'completed')
        WHEN 'step_completed' THEN json_object('step_id', legacy.step_id, 'status', 'completed')
        WHEN 'run_completed' THEN json_object(
            'status', 'completed',
            'content', COALESCE(
                NULLIF(legacy.content, ''),
                (
                    SELECT group_concat(ordered.content, '')
                    FROM (
                        SELECT delta.content
                        FROM run_events_legacy AS delta
                        WHERE delta.run_id = legacy.run_id
                          AND delta.event = 'token_delta'
                          AND delta.id < legacy.id
                        ORDER BY delta.id
                    ) AS ordered
                ),
                ''
            ),
            'content_format', 'markdown',
            'output', json(legacy.result),
            'conversation', NULL
        )
        WHEN 'error' THEN CASE
            WHEN legacy.step_id IS NULL THEN json_object('status', 'failed')
            ELSE json_object('step_id', legacy.step_id, 'status', 'failed')
        END
        ELSE json_object('legacy_content', legacy.content, 'legacy_result', json(legacy.result))
    END
FROM run_events_legacy AS legacy;

DROP TABLE run_events_legacy;

CREATE INDEX IF NOT EXISTS idx_run_events_run_id ON run_events(run_id, seq, id);
