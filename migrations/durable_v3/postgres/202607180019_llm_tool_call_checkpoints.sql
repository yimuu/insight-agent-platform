-- PostgreSQL authority equivalent of the SQLite model tool-task queue.

CREATE TABLE IF NOT EXISTS model_tool_call_batches (
    run_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    model_call_no INTEGER NOT NULL CHECK (model_call_no >= 1),
    batch_status TEXT NOT NULL CHECK (batch_status = 'checkpointed'),
    execution_status TEXT NOT NULL DEFAULT 'checkpointed' CHECK (
        execution_status IN ('checkpointed','active','succeeded','failed','cancelled')
    ),
    assistant_content TEXT CHECK (assistant_content IS NULL OR octet_length(assistant_content) <= 1048576),
    parent_task_id TEXT,
    parent_lease_epoch BIGINT CHECK (parent_lease_epoch IS NULL OR parent_lease_epoch >= 1),
    parent_fencing_token TEXT,
    parent_claimed_by TEXT,
    parent_claim_token TEXT,
    parent_claim_expires_at TIMESTAMPTZ,
    parent_task_projection_version BIGINT CHECK (
        parent_task_projection_version IS NULL OR parent_task_projection_version >= 1
    ),
    continuation_status TEXT NOT NULL DEFAULT 'checkpointed' CHECK (
        continuation_status IN (
            'checkpointed','waiting_tools','ready_continue','ready_failed','ready_cancelled'
        )
    ),
    activated_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, activation_id, attempt_no, model_call_no),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, activation_id, attempt_no, model_call_no)
        REFERENCES model_call_usage(run_id, activation_id, attempt_no, model_call_no)
        ON DELETE RESTRICT,
    CHECK ((execution_status = 'checkpointed' AND activated_at IS NULL)
        OR (execution_status <> 'checkpointed' AND activated_at IS NOT NULL)),
    CHECK ((execution_status IN ('succeeded','failed','cancelled')) = (completed_at IS NOT NULL)),
    CHECK ((continuation_status = 'checkpointed' AND parent_task_id IS NULL
            AND parent_lease_epoch IS NULL AND parent_fencing_token IS NULL
            AND parent_claimed_by IS NULL AND parent_claim_token IS NULL
            AND parent_claim_expires_at IS NULL AND parent_task_projection_version IS NULL)
        OR (continuation_status <> 'checkpointed' AND parent_task_id IS NOT NULL
            AND parent_lease_epoch IS NOT NULL AND parent_fencing_token IS NOT NULL
            AND parent_claimed_by IS NOT NULL AND parent_claim_token IS NOT NULL
            AND parent_claim_expires_at IS NOT NULL AND parent_task_projection_version IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS model_tool_calls (
    run_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    model_call_no INTEGER NOT NULL CHECK (model_call_no >= 1),
    call_index INTEGER NOT NULL CHECK (call_index >= 0),
    call_id TEXT NOT NULL CHECK (call_id <> '' AND octet_length(call_id) <= 256),
    tool_name TEXT NOT NULL CHECK (tool_name <> '' AND octet_length(tool_name) <= 128),
    arguments JSONB NOT NULL CHECK (
        jsonb_typeof(arguments) = 'object' AND octet_length(arguments::TEXT) <= 262144
    ),

    tool_task_id TEXT UNIQUE CHECK (
        tool_task_id IS NULL OR tool_task_id ~ '^task_[0-9a-f]{64}$'
    ),
    effect_id TEXT UNIQUE CHECK (
        effect_id IS NULL OR effect_id ~ '^effect_[0-9a-f]{64}$'
    ),
    action_id TEXT CHECK (action_id IS NULL OR (action_id <> '' AND octet_length(action_id) <= 128)),
    action_version TEXT CHECK (
        action_version IS NULL OR (action_version <> '' AND octet_length(action_version) <= 64)
    ),
    action_descriptor_hash TEXT CHECK (
        action_descriptor_hash IS NULL OR action_descriptor_hash ~ '^[0-9a-f]{64}$'
    ),
    action_input_schema JSONB,
    action_output_schema JSONB,
    action_effect_policy JSONB CHECK (
        action_effect_policy IS NULL OR jsonb_typeof(action_effect_policy) = 'object'
    ),
    action_deployment_binding JSONB CHECK (
        action_deployment_binding IS NULL OR jsonb_typeof(action_deployment_binding) = 'object'
    ),
    effective_public_policy JSONB CHECK (
        effective_public_policy IS NULL OR jsonb_typeof(effective_public_policy) = 'object'
    ),
    response_item_id TEXT CHECK (
        response_item_id IS NULL OR (response_item_id <> '' AND octet_length(response_item_id) <= 256)
    ),
    response_output_index INTEGER CHECK (
        response_output_index IS NULL OR response_output_index >= 0
    ),
    effect_idempotency TEXT CHECK (
        effect_idempotency IS NULL OR effect_idempotency IN ('idempotent','non_idempotent')
    ),
    cancellation TEXT CHECK (
        cancellation IS NULL OR cancellation IN ('cooperative','lease_only')
    ),
    max_attempts INTEGER CHECK (max_attempts IS NULL OR max_attempts >= 1),
    initial_backoff_ms BIGINT CHECK (initial_backoff_ms IS NULL OR initial_backoff_ms >= 0),
    max_backoff_ms BIGINT CHECK (max_backoff_ms IS NULL OR max_backoff_ms >= 0),
    timeout_ms BIGINT CHECK (timeout_ms IS NULL OR timeout_ms >= 1),

    call_status TEXT NOT NULL CHECK (
        call_status IN ('pending','claimed','running','succeeded','failed','cancelled')
    ),
    tool_attempt_no INTEGER CHECK (tool_attempt_no IS NULL OR tool_attempt_no >= 1),
    lease_epoch BIGINT CHECK (lease_epoch IS NULL OR lease_epoch >= 1),
    fencing_token TEXT CHECK (fencing_token IS NULL OR fencing_token <> ''),
    effect_evidence TEXT CHECK (
        effect_evidence IS NULL OR effect_evidence IN ('not_started','started','committed','unknown')
    ),
    available_at TIMESTAMPTZ,
    claim_owner TEXT,
    claim_token TEXT,
    claim_expires_at TIMESTAMPTZ,
    projection_version BIGINT NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    lease_loss_count INTEGER NOT NULL DEFAULT 0 CHECK (lease_loss_count >= 0),
    last_lease_loss_at TIMESTAMPTZ,
    last_lease_loss_evidence TEXT CHECK (
        last_lease_loss_evidence IS NULL OR last_lease_loss_evidence IN ('not_started','unknown')
    ),

    result_json JSONB CHECK (
        result_json IS NULL OR octet_length(result_json::TEXT) <= 1048576
    ),
    failure_class TEXT CHECK (
        failure_class IS NULL OR failure_class IN ('safe','infrastructure','effect_outcome_unknown')
    ),
    failure_code TEXT CHECK (
        failure_code IS NULL OR (failure_code <> '' AND octet_length(failure_code) <= 128)
    ),
    failure_retryable BOOLEAN,

    last_commit_claim_token TEXT,
    last_outcome_hash TEXT CHECK (
        last_outcome_hash IS NULL OR last_outcome_hash ~ '^sha256:[0-9a-f]{64}$'
    ),
    last_outcome_disposition TEXT CHECK (
        last_outcome_disposition IS NULL OR last_outcome_disposition IN (
            'succeeded','retry_scheduled','failed','cancelled'
        )
    ),
    last_outcome_attempt_no INTEGER CHECK (
        last_outcome_attempt_no IS NULL OR last_outcome_attempt_no >= 1
    ),
    last_outcome_lease_epoch BIGINT CHECK (
        last_outcome_lease_epoch IS NULL OR last_outcome_lease_epoch >= 1
    ),
    last_outcome_fencing_token TEXT,
    last_outcome_available_at TIMESTAMPTZ,
    last_effect_evidence TEXT CHECK (
        last_effect_evidence IS NULL OR last_effect_evidence IN (
            'not_started','started','committed','unknown'
        )
    ),
    last_failure_class TEXT CHECK (
        last_failure_class IS NULL OR last_failure_class IN (
            'safe','infrastructure','effect_outcome_unknown'
        )
    ),
    last_failure_code TEXT CHECK (
        last_failure_code IS NULL OR (last_failure_code <> '' AND octet_length(last_failure_code) <= 128)
    ),
    last_failure_retryable BOOLEAN,

    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    PRIMARY KEY (run_id, activation_id, attempt_no, model_call_no, call_index),
    UNIQUE (run_id, activation_id, attempt_no, model_call_no, call_id),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, activation_id, attempt_no, model_call_no)
        REFERENCES model_tool_call_batches(run_id, activation_id, attempt_no, model_call_no)
        ON DELETE RESTRICT,

    CHECK ((tool_task_id IS NULL AND effect_id IS NULL AND action_id IS NULL
            AND action_version IS NULL AND action_descriptor_hash IS NULL
            AND action_input_schema IS NULL AND action_output_schema IS NULL
            AND action_effect_policy IS NULL AND action_deployment_binding IS NULL
            AND effective_public_policy IS NULL AND response_item_id IS NULL
            AND response_output_index IS NULL AND effect_idempotency IS NULL
            AND cancellation IS NULL AND max_attempts IS NULL
            AND initial_backoff_ms IS NULL AND max_backoff_ms IS NULL AND timeout_ms IS NULL
            AND tool_attempt_no IS NULL AND lease_epoch IS NULL AND fencing_token IS NULL
            AND effect_evidence IS NULL AND available_at IS NULL AND projection_version = 0)
        OR (tool_task_id IS NOT NULL AND effect_id IS NOT NULL AND action_id IS NOT NULL
            AND action_version IS NOT NULL AND action_descriptor_hash IS NOT NULL
            AND action_input_schema IS NOT NULL AND action_output_schema IS NOT NULL
            AND action_effect_policy IS NOT NULL AND action_deployment_binding IS NOT NULL
            AND effective_public_policy IS NOT NULL AND effect_idempotency IS NOT NULL
            AND cancellation IS NOT NULL AND max_attempts IS NOT NULL
            AND initial_backoff_ms IS NOT NULL AND max_backoff_ms IS NOT NULL AND timeout_ms IS NOT NULL
            AND tool_attempt_no IS NOT NULL AND lease_epoch IS NOT NULL AND fencing_token IS NOT NULL
            AND effect_evidence IS NOT NULL AND projection_version >= 1)),
    CHECK ((claim_owner IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)
        OR (claim_owner IS NOT NULL AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)),
    CHECK ((response_item_id IS NULL) = (response_output_index IS NULL)),
    CHECK ((lease_loss_count = 0 AND last_lease_loss_at IS NULL AND last_lease_loss_evidence IS NULL)
        OR (lease_loss_count > 0 AND last_lease_loss_at IS NOT NULL
            AND last_lease_loss_evidence IS NOT NULL)),
    CHECK ((last_commit_claim_token IS NULL AND last_outcome_hash IS NULL
            AND last_outcome_disposition IS NULL AND last_outcome_attempt_no IS NULL
            AND last_outcome_lease_epoch IS NULL AND last_outcome_fencing_token IS NULL
            AND last_outcome_available_at IS NULL AND last_effect_evidence IS NULL
            AND last_failure_class IS NULL AND last_failure_code IS NULL
            AND last_failure_retryable IS NULL)
        OR (last_commit_claim_token IS NOT NULL AND last_outcome_hash IS NOT NULL
            AND last_outcome_disposition IS NOT NULL AND last_outcome_attempt_no IS NOT NULL
            AND last_outcome_lease_epoch IS NOT NULL AND last_outcome_fencing_token IS NOT NULL
            AND last_effect_evidence IS NOT NULL)),
    CHECK ((call_status = 'pending' AND claim_owner IS NULL AND result_json IS NULL
            AND failure_class IS NULL AND failure_code IS NULL AND failure_retryable IS NULL
            AND started_at IS NULL AND completed_at IS NULL
            AND (tool_task_id IS NULL OR (effect_evidence = 'not_started' AND available_at IS NOT NULL)))
        OR (call_status = 'claimed' AND claim_owner IS NOT NULL AND result_json IS NULL
            AND failure_code IS NULL AND effect_evidence = 'not_started'
            AND available_at IS NULL AND started_at IS NULL AND completed_at IS NULL)
        OR (call_status = 'running' AND claim_owner IS NOT NULL AND result_json IS NULL
            AND failure_code IS NULL AND effect_evidence = 'started'
            AND available_at IS NULL AND started_at IS NOT NULL AND completed_at IS NULL)
        OR (call_status = 'succeeded' AND claim_owner IS NOT NULL AND result_json IS NOT NULL
            AND failure_code IS NULL AND effect_evidence = 'committed'
            AND available_at IS NULL AND started_at IS NOT NULL AND completed_at IS NOT NULL)
        OR (call_status IN ('failed','cancelled')
            AND result_json IS NULL AND failure_class IS NOT NULL AND failure_code IS NOT NULL
            AND failure_retryable IS NOT NULL AND effect_evidence IN ('not_started','started','unknown')
            AND available_at IS NULL AND completed_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_model_tool_calls_claim
    ON model_tool_calls(call_status, available_at, run_id, tool_task_id);
CREATE INDEX IF NOT EXISTS idx_model_tool_calls_batch_status
    ON model_tool_calls(run_id, activation_id, attempt_no, model_call_no, call_status);
