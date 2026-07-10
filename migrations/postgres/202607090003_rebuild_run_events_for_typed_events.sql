DROP INDEX IF EXISTS idx_run_events_run_id;

ALTER TABLE run_events RENAME TO run_events_legacy;

CREATE TABLE run_events (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    type TEXT NOT NULL,
    seq BIGINT NOT NULL,
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
        WHEN 'token_delta' THEN jsonb_build_object('step_id', legacy.step_id, 'content', legacy.content)::TEXT
        WHEN 'run_started' THEN jsonb_build_object('status', 'running')::TEXT
        WHEN 'step_started' THEN jsonb_build_object('step_id', legacy.step_id, 'status', 'running')::TEXT
        WHEN 'tool_call_started' THEN jsonb_build_object('step_id', legacy.step_id, 'status', 'running')::TEXT
        WHEN 'tool_call_completed' THEN jsonb_build_object('step_id', legacy.step_id, 'status', 'completed')::TEXT
        WHEN 'step_completed' THEN jsonb_build_object('step_id', legacy.step_id, 'status', 'completed')::TEXT
        WHEN 'run_completed' THEN jsonb_build_object(
            'status', 'completed',
            'content', COALESCE(
                NULLIF(legacy.content, ''),
                (
                    SELECT string_agg(delta.content, '' ORDER BY delta.id)
                    FROM run_events_legacy AS delta
                    WHERE delta.run_id = legacy.run_id
                      AND delta.event = 'token_delta'
                      AND delta.id < legacy.id
                ),
                ''
            ),
            'content_format', 'markdown',
            'output', legacy.result::JSONB,
            'conversation', NULL
        )::TEXT
        WHEN 'error' THEN CASE
            WHEN legacy.step_id IS NULL THEN jsonb_build_object('status', 'failed')::TEXT
            ELSE jsonb_build_object('step_id', legacy.step_id, 'status', 'failed')::TEXT
        END
        ELSE jsonb_build_object('legacy_content', legacy.content, 'legacy_result', legacy.result::JSONB)::TEXT
    END
FROM run_events_legacy AS legacy;

DROP TABLE run_events_legacy;

CREATE INDEX IF NOT EXISTS idx_run_events_run_id ON run_events(run_id, seq, id);
