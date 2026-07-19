-- DSL v3 durable execution kernel (SQLite single-process test double).
-- This intentionally mirrors the PostgreSQL identities and CAS columns, but
-- it is not a multi-runtime lease authority.

PRAGMA foreign_keys = ON;

CREATE TABLE workflow_definitions (
    definition_id TEXT PRIMARY KEY,
    agent_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    CHECK (definition_id <> '' AND agent_id <> '')
);

CREATE TABLE workflow_definition_revisions (
    definition_id TEXT NOT NULL,
    definition_revision_id TEXT NOT NULL,
    revision_status TEXT NOT NULL CHECK (revision_status IN ('draft', 'published')),
    author_document TEXT NOT NULL CHECK (json_valid(author_document)),
    canonical_plan TEXT NOT NULL CHECK (json_valid(canonical_plan)),
    plan_hash TEXT NOT NULL CHECK (length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
    compiler_version TEXT NOT NULL,
    expression_engine_version TEXT NOT NULL,
    descriptor_contracts TEXT NOT NULL CHECK (json_valid(descriptor_contracts)),
    created_at TEXT NOT NULL,
    published_at TEXT,
    PRIMARY KEY (definition_id, definition_revision_id),
    UNIQUE (definition_id, definition_revision_id, plan_hash),
    FOREIGN KEY (definition_id) REFERENCES workflow_definitions(definition_id) ON DELETE RESTRICT,
    CHECK (
        (revision_status = 'draft' AND published_at IS NULL)
        OR (revision_status = 'published' AND published_at IS NOT NULL)
    )
);

CREATE TABLE deployment_revisions (
    definition_id TEXT NOT NULL,
    definition_revision_id TEXT NOT NULL,
    deployment_revision_id TEXT NOT NULL,
    plan_hash TEXT NOT NULL CHECK (length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
    binding_hash TEXT NOT NULL CHECK (length(binding_hash) = 71 AND binding_hash LIKE 'sha256:%'),
    resolved_bindings TEXT NOT NULL CHECK (json_valid(resolved_bindings)),
    worker_contracts TEXT NOT NULL CHECK (json_valid(worker_contracts)),
    created_at TEXT NOT NULL,
    PRIMARY KEY (definition_id, deployment_revision_id),
    UNIQUE (
        definition_id,
        definition_revision_id,
        deployment_revision_id,
        plan_hash,
        binding_hash
    ),
    FOREIGN KEY (definition_id, definition_revision_id, plan_hash)
        REFERENCES workflow_definition_revisions(
            definition_id,
            definition_revision_id,
            plan_hash
    ) ON DELETE RESTRICT
);

CREATE TRIGGER trg_v3_published_revision_update_immutable
    BEFORE UPDATE ON workflow_definition_revisions
    FOR EACH ROW WHEN OLD.revision_status = 'published'
BEGIN
    SELECT RAISE(ABORT, 'published workflow definition revisions are immutable');
END;

CREATE TRIGGER trg_v3_published_revision_delete_immutable
    BEFORE DELETE ON workflow_definition_revisions
    FOR EACH ROW WHEN OLD.revision_status = 'published'
BEGIN
    SELECT RAISE(ABORT, 'published workflow definition revisions are immutable');
END;

CREATE TRIGGER trg_v3_deployment_revision_immutable
    BEFORE UPDATE ON deployment_revisions
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'deployment revisions are immutable');
END;

CREATE TABLE workflow_runs (
    run_id TEXT PRIMARY KEY,
    definition_id TEXT NOT NULL,
    definition_revision_id TEXT NOT NULL,
    deployment_revision_id TEXT NOT NULL,
    plan_hash TEXT NOT NULL CHECK (length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
    binding_hash TEXT NOT NULL CHECK (length(binding_hash) = 71 AND binding_hash LIKE 'sha256:%'),
    request_id TEXT NOT NULL CHECK (request_id <> ''),
    attachment TEXT NOT NULL CHECK (attachment IN ('attached', 'detached')),
    lifecycle TEXT NOT NULL CHECK (
        lifecycle IN (
            'created', 'active', 'waiting', 'completing', 'terminating',
            'succeeded', 'failed', 'cancelled', 'interrupted', 'timed_out'
        )
    ),
    admission_state TEXT NOT NULL CHECK (
        admission_state IN ('open', 'paused', 'draining', 'closed')
    ),
    termination_intent_reason TEXT,
    termination_intent_transition_key TEXT,
    termination_intent_at TEXT,
    input_payload_id TEXT NOT NULL,
    output_payload_id TEXT,
    output_artifact_id TEXT,
    output_value_hash TEXT CHECK (
        output_value_hash IS NULL
        OR (length(output_value_hash) = 71 AND output_value_hash LIKE 'sha256:%')
    ),
    error_code TEXT,
    terminal_event_id TEXT,
    terminal_public_event_id TEXT,
    parent_run_id TEXT,
    lineage_kind TEXT CHECK (
        lineage_kind IS NULL OR lineage_kind IN ('generation', 'redrive', 'fork', 'migrate', 'subflow')
    ),
    generation INTEGER NOT NULL DEFAULT 1 CHECK (generation >= 1),
    replacement_run_id TEXT,
    next_event_seq INTEGER NOT NULL DEFAULT 1 CHECK (next_event_seq >= 1),
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    scheduler_lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK (scheduler_lease_epoch >= 0),
    scheduler_lease_owner TEXT,
    scheduler_fencing_token TEXT,
    scheduler_lease_expires_at TEXT,
    scheduler_heartbeat_at TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    updated_at TEXT NOT NULL,
    terminal_at TEXT,
    FOREIGN KEY (
        definition_id,
        definition_revision_id,
        deployment_revision_id,
        plan_hash,
        binding_hash
    ) REFERENCES deployment_revisions(
        definition_id,
        definition_revision_id,
        deployment_revision_id,
        plan_hash,
        binding_hash
    ) ON DELETE RESTRICT,
    FOREIGN KEY (parent_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (replacement_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (run_id, input_payload_id) REFERENCES payloads(run_id, payload_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (run_id, output_payload_id) REFERENCES payloads(run_id, payload_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (run_id, output_artifact_id) REFERENCES artifacts(run_id, artifact_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (run_id, terminal_event_id) REFERENCES execution_events(run_id, event_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (run_id, terminal_public_event_id)
        REFERENCES public_event_outbox(run_id, public_event_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (termination_intent_reason IS NULL
            AND termination_intent_transition_key IS NULL
            AND termination_intent_at IS NULL)
        OR
        (termination_intent_reason IS NOT NULL
            AND termination_intent_transition_key IS NOT NULL
            AND termination_intent_at IS NOT NULL)
    ),
    CHECK (
        (scheduler_lease_owner IS NULL
            AND scheduler_fencing_token IS NULL
            AND scheduler_lease_expires_at IS NULL
            AND scheduler_heartbeat_at IS NULL)
        OR
        (scheduler_lease_epoch > 0
            AND scheduler_lease_owner IS NOT NULL
            AND scheduler_fencing_token IS NOT NULL
            AND scheduler_lease_expires_at IS NOT NULL
            AND scheduler_heartbeat_at IS NOT NULL)
    ),
    CHECK (
        (parent_run_id IS NULL AND lineage_kind IS NULL AND generation = 1)
        OR (parent_run_id IS NOT NULL AND lineage_kind IS NOT NULL)
    ),
    CHECK (
        (lifecycle IN ('created', 'active', 'waiting')
            AND terminal_at IS NULL
            AND terminal_event_id IS NULL
            AND terminal_public_event_id IS NULL
            AND output_payload_id IS NULL
            AND output_artifact_id IS NULL
            AND output_value_hash IS NULL
            AND error_code IS NULL)
        OR
        (lifecycle = 'completing'
            AND admission_state IN ('draining', 'closed')
            AND terminal_at IS NULL
            AND terminal_event_id IS NULL
            AND terminal_public_event_id IS NULL)
        OR
        (lifecycle = 'terminating'
            AND admission_state IN ('draining', 'closed')
            AND termination_intent_reason IS NOT NULL
            AND terminal_at IS NULL
            AND terminal_event_id IS NULL
            AND terminal_public_event_id IS NULL)
        OR
        (lifecycle = 'succeeded'
            AND admission_state = 'closed'
            AND termination_intent_reason IS NULL
            AND terminal_at IS NOT NULL
            AND terminal_event_id IS NOT NULL
            AND terminal_public_event_id IS NOT NULL
            AND output_value_hash IS NOT NULL
            AND ((output_payload_id IS NOT NULL) + (output_artifact_id IS NOT NULL)) = 1
            AND error_code IS NULL)
        OR
        (lifecycle IN ('failed', 'cancelled', 'interrupted', 'timed_out')
            AND admission_state = 'closed'
            AND termination_intent_reason IS NOT NULL
            AND terminal_at IS NOT NULL
            AND terminal_event_id IS NOT NULL
            AND terminal_public_event_id IS NOT NULL
            AND output_payload_id IS NULL
            AND output_artifact_id IS NULL
            AND output_value_hash IS NULL
            AND error_code IS NOT NULL)
    )
);

CREATE INDEX idx_v3_runs_dispatch
    ON workflow_runs(lifecycle, admission_state, updated_at, run_id)
    WHERE lifecycle IN ('active', 'waiting');

CREATE TABLE payloads (
    run_id TEXT NOT NULL,
    payload_id TEXT NOT NULL,
    content_hash TEXT NOT NULL CHECK (length(content_hash) = 71 AND content_hash LIKE 'sha256:%'),
    canonical_bytes INTEGER NOT NULL CHECK (canonical_bytes >= 0),
    encoding TEXT NOT NULL CHECK (encoding IN ('json_jcs', 'binary')),
    inline_value TEXT CHECK (inline_value IS NULL OR json_valid(inline_value)),
    binary_value BLOB,
    created_at TEXT NOT NULL,
    retain_until TEXT,
    PRIMARY KEY (run_id, payload_id),
    UNIQUE (run_id, content_hash),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    CHECK (
        (encoding = 'json_jcs' AND inline_value IS NOT NULL AND binary_value IS NULL)
        OR (encoding = 'binary' AND inline_value IS NULL AND binary_value IS NOT NULL)
    )
);

CREATE TABLE artifacts (
    run_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    content_hash TEXT NOT NULL CHECK (length(content_hash) = 71 AND content_hash LIKE 'sha256:%'),
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    media_type TEXT,
    storage_uri TEXT NOT NULL,
    artifact_state TEXT NOT NULL CHECK (
        artifact_state IN ('staged', 'verified', 'referenced', 'deleting', 'deleted')
    ),
    verified_at TEXT,
    referenced_at TEXT,
    retain_until TEXT,
    deletion_fence TEXT CHECK (
        deletion_fence IS NULL
        OR (length(deletion_fence) = 71 AND deletion_fence LIKE 'sha256:%')
    ),
    deletion_claim_token TEXT CHECK (
        deletion_claim_token IS NULL
        OR (length(deletion_claim_token) = 71 AND deletion_claim_token LIKE 'sha256:%')
    ),
    deletion_claimed_by TEXT,
    deletion_claim_request_key TEXT,
    deletion_claimed_at TEXT,
    deletion_claim_expires_at TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, artifact_id),
    UNIQUE (run_id, content_hash),
    UNIQUE (run_id, deletion_fence),
    UNIQUE (deletion_claim_token),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    CHECK (
        (artifact_state = 'staged' AND verified_at IS NULL AND referenced_at IS NULL
            AND deletion_fence IS NULL AND deletion_claim_token IS NULL
            AND deletion_claimed_by IS NULL AND deletion_claim_request_key IS NULL
            AND deletion_claimed_at IS NULL AND deletion_claim_expires_at IS NULL)
        OR (artifact_state = 'verified' AND verified_at IS NOT NULL AND referenced_at IS NULL
            AND deletion_fence IS NULL AND deletion_claim_token IS NULL
            AND deletion_claimed_by IS NULL AND deletion_claim_request_key IS NULL
            AND deletion_claimed_at IS NULL AND deletion_claim_expires_at IS NULL)
        OR (artifact_state = 'referenced' AND verified_at IS NOT NULL AND referenced_at IS NOT NULL
            AND deletion_fence IS NULL AND deletion_claim_token IS NULL
            AND deletion_claimed_by IS NULL AND deletion_claim_request_key IS NULL
            AND deletion_claimed_at IS NULL AND deletion_claim_expires_at IS NULL)
        OR (artifact_state IN ('deleting', 'deleted') AND referenced_at IS NULL
            AND deletion_fence IS NOT NULL AND deletion_claim_token IS NOT NULL
            AND deletion_claimed_by IS NOT NULL AND deletion_claim_request_key IS NOT NULL
            AND deletion_claimed_at IS NOT NULL AND deletion_claim_expires_at IS NOT NULL)
    )
);

CREATE INDEX idx_v3_artifact_orphan_sweep
    ON artifacts(artifact_state, created_at, retain_until, run_id, artifact_id)
    WHERE artifact_state IN ('staged', 'verified', 'deleting');

CREATE TABLE artifact_gc_sweeps (
    transition_key TEXT PRIMARY KEY,
    intent_hash TEXT NOT NULL CHECK (
        length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'
    ),
    claimed_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE artifact_gc_claims (
    transition_key TEXT NOT NULL,
    run_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    claim_token TEXT NOT NULL CHECK (
        length(claim_token) = 71 AND claim_token LIKE 'sha256:%'
    ),
    deletion_fence TEXT NOT NULL CHECK (
        length(deletion_fence) = 71 AND deletion_fence LIKE 'sha256:%'
    ),
    claim_expires_at TEXT NOT NULL,
    PRIMARY KEY (transition_key, run_id, artifact_id),
    UNIQUE (claim_token),
    FOREIGN KEY (transition_key) REFERENCES artifact_gc_sweeps(transition_key)
        ON DELETE RESTRICT,
    FOREIGN KEY (run_id, artifact_id) REFERENCES artifacts(run_id, artifact_id)
        ON DELETE RESTRICT
);

CREATE TABLE scope_instances (
    run_id TEXT NOT NULL,
    scope_instance_id TEXT NOT NULL,
    parent_scope_instance_id TEXT,
    static_scope_id TEXT NOT NULL,
    stable_dynamic_key TEXT,
    scope_kind TEXT NOT NULL CHECK (
        scope_kind IN (
            'root', 'map_item', 'loop_iteration', 'subflow_invocation',
            'agent_loop_turn', 'parallel_leg'
        )
    ),
    is_root INTEGER NOT NULL CHECK (is_root IN (0, 1)),
    lifecycle TEXT NOT NULL CHECK (lifecycle IN ('active', 'settling', 'settled', 'cancelled')),
    admission_state TEXT NOT NULL CHECK (admission_state IN ('open', 'draining', 'closed')),
    admitted_children INTEGER NOT NULL DEFAULT 0 CHECK (admitted_children >= 0),
    settled_children INTEGER NOT NULL DEFAULT 0 CHECK (
        settled_children >= 0 AND settled_children <= admitted_children
    ),
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    settled_at TEXT,
    PRIMARY KEY (run_id, scope_instance_id),
    UNIQUE (run_id, parent_scope_instance_id, static_scope_id, stable_dynamic_key),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, parent_scope_instance_id)
        REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
    CHECK (
        (is_root = 1
            AND scope_kind = 'root'
            AND parent_scope_instance_id IS NULL
            AND stable_dynamic_key IS NULL)
        OR
        (is_root = 0
            AND scope_kind <> 'root'
            AND parent_scope_instance_id IS NOT NULL
            AND stable_dynamic_key IS NOT NULL)
    ),
    CHECK (
        (lifecycle IN ('active', 'settling') AND settled_at IS NULL)
        OR
        (lifecycle IN ('settled', 'cancelled')
            AND admission_state = 'closed'
            AND settled_children = admitted_children
            AND settled_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_v3_root_scope_per_run
    ON scope_instances(run_id) WHERE is_root = 1;
CREATE INDEX idx_v3_scope_parent
    ON scope_instances(run_id, parent_scope_instance_id, scope_instance_id);

CREATE TABLE node_activations (
    run_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    scope_instance_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    stable_activation_key TEXT NOT NULL,
    execution_kind TEXT NOT NULL CHECK (
        execution_kind IN ('worker', 'scheduler_native', 'durable_wait')
    ),
    lifecycle TEXT NOT NULL CHECK (
        lifecycle IN (
            'created', 'ready', 'leased', 'running', 'retry_wait', 'waiting',
            'terminating', 'succeeded', 'failed', 'cancelled', 'timed_out'
        )
    ),
    effect_id TEXT NOT NULL,
    effect_idempotency TEXT NOT NULL CHECK (
        effect_idempotency IN ('idempotent', 'non_idempotent')
    ),
    effect_evidence TEXT NOT NULL CHECK (
        effect_evidence IN ('not_started', 'started', 'committed', 'unknown')
    ),
    last_attempt_no INTEGER CHECK (last_attempt_no IS NULL OR last_attempt_no >= 1),
    last_lease_epoch INTEGER CHECK (last_lease_epoch IS NULL OR last_lease_epoch >= 1),
    current_attempt_no INTEGER CHECK (current_attempt_no IS NULL OR current_attempt_no >= 1),
    current_lease_epoch INTEGER CHECK (current_lease_epoch IS NULL OR current_lease_epoch >= 1),
    current_fencing_token TEXT,
    retry_budget_remaining INTEGER NOT NULL CHECK (retry_budget_remaining >= 0),
    pending_retry_timer_id TEXT,
    wait_registration_transition_key TEXT,
    termination_intent_reason TEXT,
    termination_intent_transition_key TEXT,
    termination_intent_at TEXT,
    output_payload_id TEXT,
    output_artifact_id TEXT,
    output_value_hash TEXT CHECK (
        output_value_hash IS NULL
        OR (length(output_value_hash) = 71 AND output_value_hash LIKE 'sha256:%')
    ),
    winning_attempt_no INTEGER CHECK (winning_attempt_no IS NULL OR winning_attempt_no >= 1),
    reused_from_run_id TEXT,
    reused_from_activation_id TEXT,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    terminal_at TEXT,
    PRIMARY KEY (run_id, activation_id),
    UNIQUE (run_id, scope_instance_id, node_id, stable_activation_key),
    UNIQUE (run_id, effect_id),
    UNIQUE (run_id, activation_id, effect_id),
    UNIQUE (run_id, wait_registration_transition_key),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, scope_instance_id)
        REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, output_payload_id)
        REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, output_artifact_id)
        REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
    FOREIGN KEY (reused_from_run_id, reused_from_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (
        run_id,
        activation_id,
        current_attempt_no,
        current_lease_epoch,
        current_fencing_token
    ) REFERENCES node_attempts(
        run_id,
        activation_id,
        attempt_no,
        lease_epoch,
        fencing_token
    ) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (run_id, activation_id, winning_attempt_no)
        REFERENCES node_attempts(run_id, activation_id, attempt_no)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    FOREIGN KEY (run_id, pending_retry_timer_id) REFERENCES timers(run_id, timer_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (last_attempt_no IS NULL AND last_lease_epoch IS NULL)
        OR
        (last_attempt_no IS NOT NULL
            AND last_lease_epoch IS NOT NULL
            AND last_lease_epoch >= last_attempt_no)
    ),
    CHECK (
        (lifecycle IN ('leased', 'running')
            AND execution_kind = 'worker'
            AND current_attempt_no = last_attempt_no
            AND current_lease_epoch = last_lease_epoch
            AND current_fencing_token IS NOT NULL)
        OR
        (lifecycle NOT IN ('leased', 'running')
            AND current_attempt_no IS NULL
            AND current_lease_epoch IS NULL
            AND current_fencing_token IS NULL)
    ),
    CHECK (
        execution_kind = 'worker'
        OR (last_attempt_no IS NULL
            AND last_lease_epoch IS NULL
            AND current_attempt_no IS NULL
            AND current_lease_epoch IS NULL
            AND current_fencing_token IS NULL)
    ),
    CHECK (lifecycle <> 'waiting' OR execution_kind = 'durable_wait'),
    CHECK (lifecycle <> 'waiting' OR wait_registration_transition_key IS NOT NULL),
    CHECK (
        (lifecycle = 'retry_wait'
            AND execution_kind = 'worker'
            AND pending_retry_timer_id IS NOT NULL)
        OR (lifecycle <> 'retry_wait' AND pending_retry_timer_id IS NULL)
    ),
    CHECK (
        (termination_intent_reason IS NULL
            AND termination_intent_transition_key IS NULL
            AND termination_intent_at IS NULL)
        OR
        (termination_intent_reason IS NOT NULL
            AND termination_intent_transition_key IS NOT NULL
            AND termination_intent_at IS NOT NULL)
    ),
    CHECK (
        (lifecycle IN ('created', 'ready', 'leased', 'running', 'retry_wait', 'waiting')
            AND termination_intent_reason IS NULL
            AND terminal_at IS NULL
            AND output_payload_id IS NULL
            AND output_artifact_id IS NULL
            AND output_value_hash IS NULL)
        OR
        (lifecycle = 'terminating'
            AND termination_intent_reason IS NOT NULL
            AND terminal_at IS NULL
            AND output_payload_id IS NULL
            AND output_artifact_id IS NULL
            AND output_value_hash IS NULL)
        OR
        (lifecycle = 'succeeded'
            AND termination_intent_reason IS NULL
            AND terminal_at IS NOT NULL
            AND output_value_hash IS NOT NULL
            AND ((output_payload_id IS NOT NULL) + (output_artifact_id IS NOT NULL)) = 1
            AND (
                (execution_kind = 'worker' AND winning_attempt_no IS NOT NULL)
                OR (execution_kind <> 'worker' AND winning_attempt_no IS NULL)
                OR (reused_from_activation_id IS NOT NULL AND winning_attempt_no IS NULL)
            ))
        OR
        (lifecycle IN ('failed', 'cancelled', 'timed_out')
            AND termination_intent_reason IS NOT NULL
            AND terminal_at IS NOT NULL
            AND output_payload_id IS NULL
            AND output_artifact_id IS NULL
            AND output_value_hash IS NULL)
    ),
    CHECK (
        (reused_from_run_id IS NULL AND reused_from_activation_id IS NULL)
        OR
        (reused_from_run_id IS NOT NULL
            AND reused_from_activation_id IS NOT NULL
            AND lifecycle = 'succeeded'
            AND last_attempt_no IS NULL)
    )
);

CREATE INDEX idx_v3_activation_dispatch
    ON node_activations(run_id, lifecycle, scope_instance_id, activation_id)
    WHERE lifecycle IN ('ready', 'retry_wait', 'waiting');

CREATE TABLE node_attempts (
    run_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch >= attempt_no),
    fencing_token TEXT NOT NULL,
    effect_id TEXT NOT NULL,
    lifecycle TEXT NOT NULL CHECK (
        lifecycle IN (
            'created', 'leased', 'running', 'succeeded', 'failed',
            'timed_out', 'abandoned', 'cancelled'
        )
    ),
    effect_evidence TEXT NOT NULL CHECK (
        effect_evidence IN ('not_started', 'started', 'committed', 'unknown')
    ),
    worker_id TEXT,
    lease_expires_at TEXT,
    heartbeat_at TEXT,
    output_payload_id TEXT,
    output_artifact_id TEXT,
    output_value_hash TEXT CHECK (
        output_value_hash IS NULL
        OR (length(output_value_hash) = 71 AND output_value_hash LIKE 'sha256:%')
    ),
    failure_code TEXT,
    completion_transition_key TEXT,
    terminal_event_id TEXT,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    started_at TEXT,
    terminal_at TEXT,
    PRIMARY KEY (run_id, activation_id, attempt_no),
    UNIQUE (run_id, activation_id, attempt_no, lease_epoch, fencing_token),
    UNIQUE (run_id, activation_id, lease_epoch),
    UNIQUE (run_id, fencing_token),
    UNIQUE (run_id, completion_transition_key),
    FOREIGN KEY (run_id, activation_id, effect_id)
        REFERENCES node_activations(run_id, activation_id, effect_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, output_payload_id)
        REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, output_artifact_id)
        REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, terminal_event_id) REFERENCES execution_events(run_id, event_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        lifecycle = 'created'
        OR (worker_id IS NOT NULL AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL)
    ),
    CHECK (
        (lifecycle IN ('created', 'leased', 'running')
            AND terminal_at IS NULL
            AND terminal_event_id IS NULL
            AND completion_transition_key IS NULL
            AND output_payload_id IS NULL
            AND output_artifact_id IS NULL
            AND output_value_hash IS NULL)
        OR
        (lifecycle = 'succeeded'
            AND terminal_at IS NOT NULL
            AND terminal_event_id IS NOT NULL
            AND completion_transition_key IS NOT NULL
            AND output_value_hash IS NOT NULL
            AND ((output_payload_id IS NOT NULL) + (output_artifact_id IS NOT NULL)) = 1
            AND failure_code IS NULL)
        OR
        (lifecycle IN ('failed', 'timed_out', 'abandoned', 'cancelled')
            AND terminal_at IS NOT NULL
            AND terminal_event_id IS NOT NULL
            AND completion_transition_key IS NOT NULL
            AND output_payload_id IS NULL
            AND output_artifact_id IS NULL
            AND output_value_hash IS NULL)
    ),
    CHECK (lifecycle <> 'abandoned' OR effect_evidence <> 'started')
);

CREATE INDEX idx_v3_attempt_lease_expiry
    ON node_attempts(lease_expires_at, run_id, activation_id, attempt_no)
    WHERE lifecycle IN ('leased', 'running');

CREATE TABLE execution_events (
    run_id TEXT NOT NULL,
    seq INTEGER NOT NULL CHECK (seq >= 1),
    event_id TEXT NOT NULL,
    schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
    kind TEXT NOT NULL,
    transition_key TEXT NOT NULL,
    intent_hash TEXT NOT NULL CHECK (length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
    node_id TEXT,
    scope_instance_id TEXT,
    activation_id TEXT,
    attempt_no INTEGER CHECK (attempt_no IS NULL OR attempt_no >= 1),
    causation_event_id TEXT,
    projection_version_after INTEGER NOT NULL CHECK (projection_version_after >= 0),
    safe_payload TEXT NOT NULL CHECK (json_valid(safe_payload)),
    occurred_at TEXT NOT NULL,
    PRIMARY KEY (run_id, seq),
    UNIQUE (run_id, event_id),
    UNIQUE (run_id, transition_key),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, scope_instance_id)
        REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, activation_id, attempt_no)
        REFERENCES node_attempts(run_id, activation_id, attempt_no) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, causation_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    CHECK (attempt_no IS NULL OR activation_id IS NOT NULL),
    CHECK (activation_id IS NULL OR (node_id IS NOT NULL AND scope_instance_id IS NOT NULL)),
    CHECK (node_id IS NULL OR scope_instance_id IS NOT NULL),
    CHECK (causation_event_id IS NULL OR causation_event_id <> event_id)
);

CREATE INDEX idx_v3_execution_events_rebuild ON execution_events(run_id, seq);

CREATE TABLE artifact_retention_releases (
    run_id TEXT PRIMARY KEY,
    transition_key TEXT NOT NULL UNIQUE,
    intent_hash TEXT NOT NULL CHECK (
        length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'
    ),
    event_id TEXT NOT NULL,
    event_seq INTEGER NOT NULL CHECK (event_seq >= 1),
    retain_until TEXT NOT NULL,
    artifact_count INTEGER NOT NULL CHECK (artifact_count >= 0),
    created_at TEXT NOT NULL,
    FOREIGN KEY (run_id, event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT
);

CREATE INDEX idx_v3_artifact_retention_due
    ON artifact_retention_releases(retain_until, run_id);

-- A closed batch manifest records only rebuildable materialized projections,
-- including the empty set used by companion events. Inbox/outbox/claim and
-- completion authorities are deliberately outside this repair ledger.
CREATE TABLE projection_checkpoint_batches (
    run_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    checkpoint_schema_version INTEGER NOT NULL CHECK (checkpoint_schema_version >= 1),
    subject_count INTEGER NOT NULL CHECK (subject_count >= 0),
    manifest_hash TEXT NOT NULL CHECK (
        length(manifest_hash) = 71 AND manifest_hash LIKE 'sha256:%'
    ),
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, event_id),
    FOREIGN KEY (run_id, event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT
);

CREATE TABLE projection_checkpoints (
    run_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    subject_kind TEXT NOT NULL CHECK (
        subject_kind IN (
            'run', 'scope', 'activation', 'attempt', 'timer', 'control',
            'fork', 'join', 'scheduler', 'data_value'
        )
    ),
    subject_id TEXT NOT NULL,
    checkpoint_schema_version INTEGER NOT NULL CHECK (checkpoint_schema_version >= 1),
    subject_projection_version INTEGER NOT NULL CHECK (subject_projection_version >= 0),
    projection_hash TEXT NOT NULL CHECK (
        length(projection_hash) = 71 AND projection_hash LIKE 'sha256:%'
    ),
    canonical_projection TEXT NOT NULL CHECK (json_valid(canonical_projection)),
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, event_id, subject_kind, subject_id),
    UNIQUE (run_id, subject_kind, subject_id, subject_projection_version),
    FOREIGN KEY (run_id, event_id)
        REFERENCES projection_checkpoint_batches(run_id, event_id) ON DELETE RESTRICT
);

CREATE INDEX idx_v3_projection_checkpoint_rebuild
    ON projection_checkpoints(run_id, subject_kind, subject_id, subject_projection_version);

CREATE TABLE scheduler_checkpoints (
    run_id TEXT NOT NULL,
    checkpoint_id TEXT NOT NULL,
    content_hash TEXT NOT NULL CHECK (
        length(content_hash) = 71 AND content_hash LIKE 'sha256:%'
    ),
    checkpoint_kind TEXT NOT NULL CHECK (
        checkpoint_kind IN (
            'planned_action', 'task_started', 'task_completed', 'task_retry_scheduled'
        )
    ),
    transition_key TEXT NOT NULL,
    intent_hash TEXT NOT NULL CHECK (
        length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'
    ),
    event_id TEXT NOT NULL,
    checkpoint_schema_version INTEGER NOT NULL CHECK (checkpoint_schema_version >= 1),
    scheduler_projection_version INTEGER NOT NULL CHECK (scheduler_projection_version >= 1),
    fact_payload TEXT NOT NULL CHECK (json_valid(fact_payload)),
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, checkpoint_id),
    UNIQUE (run_id, content_hash),
    UNIQUE (run_id, transition_key),
    UNIQUE (run_id, event_id),
    FOREIGN KEY (run_id, event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, transition_key)
        REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT
);

CREATE INDEX idx_v3_scheduler_checkpoints_recovery
    ON scheduler_checkpoints(run_id, scheduler_projection_version, checkpoint_id);

CREATE TABLE scheduler_values (
    run_id TEXT NOT NULL,
    port_id TEXT NOT NULL,
    owner_activation_id TEXT NOT NULL,
    runtime_value TEXT NOT NULL CHECK (json_valid(runtime_value)),
    value_ref TEXT NOT NULL CHECK (json_valid(value_ref)),
    declared_type TEXT NOT NULL CHECK (json_valid(declared_type)),
    storage_kind TEXT NOT NULL CHECK (storage_kind IN ('inline', 'artifact')),
    payload_id TEXT,
    artifact_id TEXT,
    content_hash TEXT NOT NULL CHECK (
        length(content_hash) = 71 AND content_hash LIKE 'sha256:%'
    ),
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, port_id),
    FOREIGN KEY (run_id, owner_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, payload_id)
        REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, artifact_id)
        REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
    CHECK (
        (storage_kind = 'inline' AND payload_id IS NOT NULL AND artifact_id IS NULL)
        OR (storage_kind = 'artifact' AND payload_id IS NULL AND artifact_id IS NOT NULL)
    )
);

CREATE INDEX idx_v3_scheduler_values_owner
    ON scheduler_values(run_id, owner_activation_id, port_id);

-- Values are addressed by logical occurrence inside dynamic scopes.  The
-- port-only table above remains the convenient latest-value lookup; this is
-- the authoritative occurrence-aware projection used by the scheduler.
CREATE TABLE scheduler_occurrence_values (
    run_id TEXT NOT NULL,
    occurrence_key TEXT NOT NULL CHECK (json_valid(occurrence_key)),
    port_id TEXT NOT NULL,
    owner_activation_id TEXT NOT NULL,
    runtime_value TEXT NOT NULL CHECK (json_valid(runtime_value)),
    value_ref TEXT NOT NULL CHECK (json_valid(value_ref)),
    declared_type TEXT NOT NULL CHECK (json_valid(declared_type)),
    storage_kind TEXT NOT NULL CHECK (storage_kind IN ('inline', 'artifact')),
    payload_id TEXT,
    artifact_id TEXT,
    content_hash TEXT NOT NULL CHECK (
        length(content_hash) = 71 AND content_hash LIKE 'sha256:%'
    ),
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, occurrence_key, port_id),
    FOREIGN KEY (run_id, owner_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, payload_id)
        REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, artifact_id)
        REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
    CHECK (
        (storage_kind = 'inline' AND payload_id IS NOT NULL AND artifact_id IS NULL)
        OR (storage_kind = 'artifact' AND payload_id IS NULL AND artifact_id IS NOT NULL)
    )
);

CREATE INDEX idx_v3_occurrence_values_owner
    ON scheduler_occurrence_values(run_id, owner_activation_id, occurrence_key, port_id);

CREATE TABLE task_outbox (
    run_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    lease_epoch INTEGER NOT NULL CHECK (lease_epoch >= attempt_no),
    fencing_token TEXT NOT NULL,
    effect_id TEXT NOT NULL,
    created_by_transition_key TEXT NOT NULL,
    task_state TEXT NOT NULL CHECK (
        task_state IN ('pending', 'claimed', 'published', 'acked', 'dead')
    ),
    task_envelope TEXT NOT NULL CHECK (json_valid(task_envelope)),
    available_at TEXT NOT NULL,
    claimed_by TEXT,
    claim_token TEXT,
    claim_expires_at TEXT,
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    published_at TEXT,
    acked_at TEXT,
    last_error_code TEXT,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, task_id),
    UNIQUE (run_id, activation_id, attempt_no, lease_epoch),
    UNIQUE (run_id, created_by_transition_key),
    FOREIGN KEY (run_id, activation_id, attempt_no, lease_epoch, fencing_token)
        REFERENCES node_attempts(
            run_id,
            activation_id,
            attempt_no,
            lease_epoch,
            fencing_token
        ) ON DELETE RESTRICT,
    CHECK (
        (task_state = 'pending' AND claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)
        OR (task_state = 'claimed' AND claimed_by IS NOT NULL AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)
        OR (task_state IN ('published', 'acked', 'dead'))
    ),
    CHECK (task_state <> 'published' OR published_at IS NOT NULL),
    CHECK (task_state <> 'acked' OR (published_at IS NOT NULL AND acked_at IS NOT NULL))
);

CREATE INDEX idx_v3_task_outbox_dispatch
    ON task_outbox(available_at, run_id, task_id) WHERE task_state = 'pending';
CREATE INDEX idx_v3_task_outbox_reclaim
    ON task_outbox(claim_expires_at, run_id, task_id) WHERE task_state = 'claimed';

CREATE TABLE public_event_outbox (
    run_id TEXT NOT NULL,
    public_event_id TEXT NOT NULL,
    causation_event_id TEXT NOT NULL,
    public_ordinal INTEGER NOT NULL CHECK (public_ordinal > 0),
    public_schema_version INTEGER NOT NULL CHECK (public_schema_version >= 1),
    event_kind TEXT NOT NULL,
    is_terminal INTEGER NOT NULL CHECK (is_terminal IN (0, 1)),
    publish_state TEXT NOT NULL CHECK (publish_state IN ('pending', 'claimed', 'published')),
    safe_envelope TEXT NOT NULL CHECK (json_valid(safe_envelope)),
    available_at TEXT NOT NULL,
    claimed_by TEXT,
    claim_token TEXT,
    claim_expires_at TEXT,
    publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    published_at TEXT,
    published_by TEXT,
    published_claim_token TEXT,
    notified_at TEXT,
    retain_until TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, public_event_id),
    UNIQUE (run_id, causation_event_id, event_kind),
    UNIQUE (run_id, causation_event_id, public_ordinal),
    FOREIGN KEY (run_id, causation_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    CHECK (
        (publish_state = 'pending' AND claimed_by IS NULL AND claim_token IS NULL
            AND claim_expires_at IS NULL AND published_at IS NULL
            AND published_by IS NULL AND published_claim_token IS NULL
            AND notified_at IS NULL AND retain_until IS NULL)
        OR (publish_state = 'claimed' AND claimed_by IS NOT NULL AND claim_token IS NOT NULL
            AND claim_expires_at IS NOT NULL AND published_at IS NULL
            AND published_by IS NULL AND published_claim_token IS NULL
            AND notified_at IS NULL AND retain_until IS NULL)
        OR (publish_state = 'published' AND published_at IS NOT NULL
            AND claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL
            AND published_by IS NOT NULL AND published_claim_token IS NOT NULL
            AND notified_at IS NOT NULL
            AND ((is_terminal = 1 AND retain_until IS NULL)
                 OR (is_terminal = 0 AND retain_until IS NOT NULL)))
    ),
    CHECK (
        (is_terminal = 0 AND event_kind IN (
            'run.created','run.started','operation.started',
            'operation.completed','operation.failed'
        ))
        OR (is_terminal = 1 AND event_kind IN (
            'run.completed','run.failed','run.cancelled','run.interrupted'
        ))
    ),
    CHECK (
        (event_kind = 'run.created' AND public_ordinal = 10)
        OR (event_kind = 'run.started' AND public_ordinal = 20)
        OR (event_kind = 'operation.started' AND public_ordinal = 30)
        OR (event_kind IN ('operation.completed', 'operation.failed') AND public_ordinal = 40)
        OR (event_kind IN ('run.completed', 'run.failed', 'run.cancelled', 'run.interrupted')
            AND public_ordinal = 50)
    )
);

CREATE UNIQUE INDEX uq_v3_public_terminal_per_run
    ON public_event_outbox(run_id) WHERE is_terminal = 1;
CREATE UNIQUE INDEX uq_v3_public_outbox_public_event_id
    ON public_event_outbox(public_event_id);
CREATE INDEX idx_v3_public_outbox_dispatch
    ON public_event_outbox(available_at, run_id, public_ordinal, public_event_id)
    WHERE publish_state = 'pending';
CREATE INDEX idx_v3_public_outbox_reclaim
    ON public_event_outbox(claim_expires_at, run_id, public_ordinal, public_event_id)
    WHERE publish_state = 'claimed';
CREATE UNIQUE INDEX uq_v3_public_outbox_claim_token
    ON public_event_outbox(claim_token) WHERE claim_token IS NOT NULL;
CREATE UNIQUE INDEX uq_v3_public_outbox_published_claim_token
    ON public_event_outbox(published_claim_token)
    WHERE published_claim_token IS NOT NULL;

CREATE TABLE signals_inbox (
    run_id TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    intent_hash TEXT NOT NULL CHECK (length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
    signal_name TEXT NOT NULL,
    target_activation_id TEXT NOT NULL,
    payload_id TEXT NOT NULL,
    signal_state TEXT NOT NULL CHECK (
        signal_state IN ('pending', 'consumed', 'rejected', 'expired')
    ),
    received_at TEXT NOT NULL,
    consumed_by_transition_key TEXT,
    consumed_event_id TEXT,
    terminal_at TEXT,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    PRIMARY KEY (run_id, signal_id),
    UNIQUE (run_id, message_id),
    UNIQUE (run_id, consumed_by_transition_key),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, target_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, payload_id)
        REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, consumed_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    CHECK (
        (signal_state = 'pending'
            AND consumed_by_transition_key IS NULL
            AND consumed_event_id IS NULL
            AND terminal_at IS NULL)
        OR
        (signal_state <> 'pending'
            AND consumed_by_transition_key IS NOT NULL
            AND consumed_event_id IS NOT NULL
            AND terminal_at IS NOT NULL)
    )
);

CREATE INDEX idx_v3_signals_pending
    ON signals_inbox(run_id, target_activation_id, received_at, signal_id)
    WHERE signal_state = 'pending';

CREATE TABLE timers (
    run_id TEXT NOT NULL,
    timer_id TEXT NOT NULL,
    activation_id TEXT,
    timer_kind TEXT NOT NULL CHECK (timer_kind IN ('lease', 'wait', 'retry', 'activation_timeout', 'run_deadline')),
    timer_state TEXT NOT NULL CHECK (timer_state IN ('scheduled', 'fired', 'cancelled')),
    deadline_at TEXT NOT NULL,
    expected_attempt_no INTEGER CHECK (expected_attempt_no IS NULL OR expected_attempt_no >= 1),
    expected_lease_epoch INTEGER CHECK (expected_lease_epoch IS NULL OR expected_lease_epoch >= 1),
    expected_fencing_token TEXT,
    retry_budget_snapshot INTEGER CHECK (
        retry_budget_snapshot IS NULL OR retry_budget_snapshot >= 0
    ),
    created_by_transition_key TEXT NOT NULL,
    fired_by_transition_key TEXT,
    fired_event_id TEXT,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    fired_at TEXT,
    PRIMARY KEY (run_id, timer_id),
    UNIQUE (run_id, created_by_transition_key),
    UNIQUE (run_id, fired_by_transition_key),
    FOREIGN KEY (run_id, activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, activation_id, expected_attempt_no, expected_lease_epoch, expected_fencing_token)
        REFERENCES node_attempts(
            run_id,
            activation_id,
            attempt_no,
            lease_epoch,
            fencing_token
        ) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, fired_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    CHECK (
        (expected_attempt_no IS NULL
            AND expected_lease_epoch IS NULL
            AND expected_fencing_token IS NULL)
        OR
        (expected_attempt_no IS NOT NULL
            AND expected_lease_epoch IS NOT NULL
            AND expected_lease_epoch >= expected_attempt_no
            AND expected_fencing_token IS NOT NULL)
    ),
    CHECK (
        (timer_kind = 'run_deadline' AND activation_id IS NULL)
        OR (timer_kind <> 'run_deadline' AND activation_id IS NOT NULL)
    ),
    CHECK (
        timer_kind NOT IN ('lease', 'retry')
        OR (expected_attempt_no IS NOT NULL AND retry_budget_snapshot IS NOT NULL)
    ),
    CHECK (
        (timer_state = 'scheduled'
            AND fired_by_transition_key IS NULL
            AND fired_event_id IS NULL
            AND fired_at IS NULL)
        OR
        (timer_state = 'fired'
            AND fired_by_transition_key IS NOT NULL
            AND fired_event_id IS NOT NULL
            AND fired_at IS NOT NULL
            AND fired_at >= deadline_at)
        OR
        (timer_state = 'cancelled' AND fired_at IS NOT NULL)
    )
);

CREATE INDEX idx_v3_timers_due
    ON timers(deadline_at, run_id, timer_id) WHERE timer_state = 'scheduled';
CREATE UNIQUE INDEX uq_v3_attempt_lease_timer
    ON timers(run_id, activation_id, expected_attempt_no, expected_lease_epoch)
    WHERE timer_kind = 'lease';

CREATE TABLE scheduler_wait_registrations (
    run_id TEXT NOT NULL,
    wait_id TEXT NOT NULL,
    activation_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    occurrence_key TEXT NOT NULL CHECK (json_valid(occurrence_key)),
    signal_name TEXT,
    signal_id TEXT,
    timer_id TEXT,
    due_at_ms INTEGER CHECK (due_at_ms IS NULL OR due_at_ms >= 0),
    payload_type TEXT CHECK (payload_type IS NULL OR json_valid(payload_type)),
    winner_kind TEXT CHECK (
        winner_kind IS NULL OR winner_kind IN ('signal', 'timer', 'cancelled')
    ),
    winner_signal_id TEXT,
    winner_timer_id TEXT,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    PRIMARY KEY (run_id, wait_id),
    UNIQUE (run_id, activation_id),
    FOREIGN KEY (run_id, activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, timer_id)
        REFERENCES timers(run_id, timer_id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CHECK ((signal_name IS NULL) = (signal_id IS NULL)),
    CHECK ((timer_id IS NULL) = (due_at_ms IS NULL)),
    CHECK (signal_id IS NOT NULL OR timer_id IS NOT NULL),
    CHECK (
        (winner_kind IS NULL AND winner_signal_id IS NULL
            AND winner_timer_id IS NULL AND resolved_at IS NULL)
        OR (winner_kind = 'signal' AND winner_signal_id = signal_id
            AND winner_timer_id IS NULL AND resolved_at IS NOT NULL)
        OR (winner_kind = 'timer' AND winner_timer_id = timer_id
            AND winner_signal_id IS NULL AND resolved_at IS NOT NULL)
        OR (winner_kind = 'cancelled' AND winner_signal_id IS NULL
            AND winner_timer_id IS NULL AND resolved_at IS NOT NULL)
    )
);

CREATE TABLE scheduler_subflow_invocations (
    run_id TEXT NOT NULL,
    child_run_id TEXT NOT NULL,
    parent_activation_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    occurrence_key TEXT NOT NULL CHECK (json_valid(occurrence_key)),
    invocation_scope_instance_id TEXT NOT NULL,
    parent_scope_instance_id TEXT NOT NULL,
    static_scope_id TEXT NOT NULL,
    definition_revision_id TEXT NOT NULL,
    deployment_revision_id TEXT NOT NULL,
    plan_hash TEXT NOT NULL CHECK (length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
    binding_hash TEXT NOT NULL CHECK (length(binding_hash) = 71 AND binding_hash LIKE 'sha256:%'),
    interface_version TEXT NOT NULL,
    output_contracts TEXT NOT NULL CHECK (json_valid(output_contracts)),
    invocation_state TEXT NOT NULL CHECK (
        invocation_state IN ('started', 'cancellation_requested', 'completed')
    ),
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    completed_at TEXT,
    PRIMARY KEY (run_id, child_run_id),
    UNIQUE (run_id, parent_activation_id),
    FOREIGN KEY (run_id, parent_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, invocation_scope_instance_id)
        REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, parent_scope_instance_id)
        REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
    FOREIGN KEY (child_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    CHECK (invocation_scope_instance_id <> parent_scope_instance_id),
    CHECK (
        (invocation_state IN ('started', 'cancellation_requested') AND completed_at IS NULL)
        OR (invocation_state = 'completed' AND completed_at IS NOT NULL)
    )
);

CREATE TABLE fork_groups (
    run_id TEXT NOT NULL,
    fork_group_id TEXT NOT NULL,
    fork_activation_id TEXT NOT NULL,
    parent_scope_instance_id TEXT NOT NULL,
    join_activation_id TEXT,
    join_mode TEXT CHECK (join_mode IS NULL OR join_mode IN ('all_success', 'all_settled')),
    failure_leg_id TEXT,
    failure_settlement_class TEXT CHECK (
        failure_settlement_class IS NULL OR failure_settlement_class IN (
            'safe_failure', 'infrastructure_failure', 'panic', 'cancelled', 'timed_out'
        )
    ),
    expected_legs INTEGER NOT NULL CHECK (expected_legs > 0),
    group_state TEXT NOT NULL CHECK (group_state IN ('open', 'settling', 'settled', 'cancelled')),
    admitted_legs INTEGER NOT NULL DEFAULT 0 CHECK (admitted_legs >= 0),
    settled_legs INTEGER NOT NULL DEFAULT 0 CHECK (settled_legs >= 0),
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    settled_at TEXT,
    PRIMARY KEY (run_id, fork_group_id),
    FOREIGN KEY (run_id, fork_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, parent_scope_instance_id)
        REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, join_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    CHECK ((failure_leg_id IS NULL) = (failure_settlement_class IS NULL)),
    CHECK (settled_legs <= admitted_legs AND admitted_legs <= expected_legs),
    CHECK (
        (group_state IN ('open', 'settling') AND settled_at IS NULL)
        OR (group_state IN ('settled', 'cancelled')
            AND settled_legs = admitted_legs
            AND settled_at IS NOT NULL)
    )
);

CREATE TABLE control_tokens (
    run_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    current_scope_instance_id TEXT NOT NULL,
    current_port_id TEXT NOT NULL,
    source_activation_id TEXT NOT NULL,
    source_port_id TEXT NOT NULL,
    emission_slot TEXT NOT NULL,
    emitted_by_transition_key TEXT NOT NULL,
    provenance_frames TEXT NOT NULL CHECK (json_valid(provenance_frames)),
    branch_activation_id TEXT,
    selected_branch_port_id TEXT,
    fork_group_id TEXT,
    fork_leg_id TEXT,
    token_state TEXT NOT NULL CHECK (token_state IN ('available', 'consumed', 'revoked')),
    consumed_by_activation_id TEXT,
    consumed_by_transition_key TEXT,
    consumed_at TEXT,
    revoked_by_transition_key TEXT,
    revoked_at TEXT,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, token_id),
    UNIQUE (run_id, source_activation_id, emission_slot),
    UNIQUE (run_id, emitted_by_transition_key, emission_slot),
    UNIQUE (run_id, consumed_by_transition_key),
    FOREIGN KEY (run_id, current_scope_instance_id)
        REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, source_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, emitted_by_transition_key)
        REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, branch_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, fork_group_id)
        REFERENCES fork_groups(run_id, fork_group_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, consumed_by_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, fork_group_id, fork_leg_id)
        REFERENCES fork_legs(run_id, fork_group_id, leg_id)
        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CHECK (
        (branch_activation_id IS NULL AND selected_branch_port_id IS NULL)
        OR (branch_activation_id IS NOT NULL AND selected_branch_port_id IS NOT NULL)
    ),
    CHECK (
        (fork_group_id IS NULL AND fork_leg_id IS NULL)
        OR (fork_group_id IS NOT NULL AND fork_leg_id IS NOT NULL)
    ),
    CHECK (
        (token_state = 'available'
            AND consumed_by_activation_id IS NULL
            AND consumed_by_transition_key IS NULL
            AND consumed_at IS NULL
            AND revoked_by_transition_key IS NULL
            AND revoked_at IS NULL)
        OR
        (token_state = 'consumed'
            AND consumed_by_activation_id IS NOT NULL
            AND consumed_by_transition_key IS NOT NULL
            AND consumed_at IS NOT NULL
            AND revoked_by_transition_key IS NULL
            AND revoked_at IS NULL)
        OR
        (token_state = 'revoked'
            AND consumed_by_activation_id IS NULL
            AND consumed_by_transition_key IS NULL
            AND consumed_at IS NULL
            AND revoked_by_transition_key IS NOT NULL
            AND revoked_at IS NOT NULL)
    )
);

CREATE TABLE fork_legs (
    run_id TEXT NOT NULL,
    fork_group_id TEXT NOT NULL,
    leg_id TEXT NOT NULL,
    declaration_index INTEGER NOT NULL CHECK (declaration_index >= 0),
    scope_instance_id TEXT NOT NULL,
    child_activation_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    is_required INTEGER NOT NULL CHECK (is_required IN (0, 1)),
    leg_state TEXT NOT NULL CHECK (leg_state IN ('admitted', 'settled', 'cancelled')),
    settlement_class TEXT CHECK (
        settlement_class IS NULL OR settlement_class IN (
            'succeeded', 'safe_failure', 'infrastructure_failure', 'panic', 'cancelled', 'timed_out'
        )
    ),
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    settled_at TEXT,
    PRIMARY KEY (run_id, fork_group_id, leg_id),
    UNIQUE (run_id, fork_group_id, declaration_index),
    UNIQUE (run_id, fork_group_id, token_id),
    FOREIGN KEY (run_id, fork_group_id)
        REFERENCES fork_groups(run_id, fork_group_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, scope_instance_id)
        REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, child_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, token_id)
        REFERENCES control_tokens(run_id, token_id) ON DELETE RESTRICT,
    CHECK (
        (leg_state = 'admitted' AND settlement_class IS NULL AND settled_at IS NULL)
        OR (leg_state IN ('settled', 'cancelled')
            AND settlement_class IS NOT NULL
            AND settled_at IS NOT NULL)
    )
);

CREATE TABLE join_arrivals (
    run_id TEXT NOT NULL,
    join_activation_id TEXT NOT NULL,
    fork_group_id TEXT NOT NULL,
    leg_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    arrival_transition_key TEXT NOT NULL,
    arrival_event_id TEXT NOT NULL,
    settlement_class TEXT NOT NULL CHECK (
        settlement_class IN (
            'succeeded', 'safe_failure', 'infrastructure_failure', 'panic', 'cancelled', 'timed_out'
        )
    ),
    value_payload_id TEXT,
    value_artifact_id TEXT,
    value_hash TEXT CHECK (
        value_hash IS NULL OR (length(value_hash) = 71 AND value_hash LIKE 'sha256:%')
    ),
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    arrived_at TEXT NOT NULL,
    PRIMARY KEY (run_id, join_activation_id, fork_group_id, leg_id),
    UNIQUE (run_id, fork_group_id, leg_id),
    UNIQUE (run_id, arrival_transition_key),
    UNIQUE (run_id, join_activation_id, token_id),
    FOREIGN KEY (run_id, join_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, fork_group_id, leg_id)
        REFERENCES fork_legs(run_id, fork_group_id, leg_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, token_id)
        REFERENCES control_tokens(run_id, token_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, arrival_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, value_payload_id)
        REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, value_artifact_id)
        REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
    CHECK (
        (value_hash IS NULL AND value_payload_id IS NULL AND value_artifact_id IS NULL)
        OR
        (value_hash IS NOT NULL
            AND ((value_payload_id IS NOT NULL) + (value_artifact_id IS NOT NULL)) = 1)
    )
);

CREATE TABLE run_reuse_candidates (
    run_id TEXT NOT NULL,
    candidate_id TEXT NOT NULL,
    target_scope_instance_id TEXT NOT NULL,
    target_node_id TEXT NOT NULL,
    stable_activation_key TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    source_activation_id TEXT NOT NULL,
    source_control_provenance TEXT NOT NULL CHECK (json_valid(source_control_provenance)),
    definition_revision_id TEXT NOT NULL,
    deployment_revision_id TEXT NOT NULL,
    plan_hash TEXT NOT NULL CHECK (length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
    binding_hash TEXT NOT NULL CHECK (length(binding_hash) = 71 AND binding_hash LIKE 'sha256:%'),
    node_config_hash TEXT NOT NULL CHECK (length(node_config_hash) = 71 AND node_config_hash LIKE 'sha256:%'),
    descriptor_hash TEXT NOT NULL CHECK (length(descriptor_hash) = 71 AND descriptor_hash LIKE 'sha256:%'),
    input_value_hash TEXT NOT NULL CHECK (length(input_value_hash) = 71 AND input_value_hash LIKE 'sha256:%'),
    output_value_hash TEXT NOT NULL CHECK (length(output_value_hash) = 71 AND output_value_hash LIKE 'sha256:%'),
    output_schema_hash TEXT NOT NULL CHECK (length(output_schema_hash) = 71 AND output_schema_hash LIKE 'sha256:%'),
    effect_policy_hash TEXT NOT NULL CHECK (length(effect_policy_hash) = 71 AND effect_policy_hash LIKE 'sha256:%'),
    inherited_effect_id TEXT NOT NULL,
    data_dependencies_hash TEXT NOT NULL CHECK (
        length(data_dependencies_hash) = 71 AND data_dependencies_hash LIKE 'sha256:%'
    ),
    created_by_transition_key TEXT NOT NULL,
    candidate_state TEXT NOT NULL CHECK (
        candidate_state IN ('candidate', 'materialized', 'rejected')
    ),
    materialized_activation_id TEXT,
    decision_transition_key TEXT,
    rejection_reason TEXT,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    decided_at TEXT,
    PRIMARY KEY (run_id, candidate_id),
    UNIQUE (run_id, target_scope_instance_id, target_node_id, stable_activation_key),
    UNIQUE (run_id, created_by_transition_key),
    UNIQUE (run_id, decision_transition_key),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (source_run_id, source_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, created_by_transition_key)
        REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, materialized_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (source_run_id <> run_id),
    CHECK (
        (candidate_state = 'candidate'
            AND materialized_activation_id IS NULL
            AND decision_transition_key IS NULL
            AND rejection_reason IS NULL
            AND decided_at IS NULL)
        OR
        (candidate_state = 'materialized'
            AND materialized_activation_id IS NOT NULL
            AND decision_transition_key IS NOT NULL
            AND rejection_reason IS NULL
            AND decided_at IS NOT NULL)
        OR
        (candidate_state = 'rejected'
            AND materialized_activation_id IS NULL
            AND decision_transition_key IS NOT NULL
            AND rejection_reason IS NOT NULL
            AND decided_at IS NOT NULL)
    )
);

CREATE INDEX idx_v3_reuse_candidates_pending
    ON run_reuse_candidates(run_id, target_scope_instance_id, target_node_id, candidate_id)
    WHERE candidate_state = 'candidate';

CREATE TABLE control_transition_results (
    run_id TEXT NOT NULL,
    transition_key TEXT NOT NULL,
    intent_hash TEXT NOT NULL CHECK (length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
    primary_event_id TEXT NOT NULL,
    result_json TEXT NOT NULL CHECK (json_valid(result_json)),
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, transition_key),
    UNIQUE (run_id, primary_event_id),
    FOREIGN KEY (run_id, primary_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT
);

CREATE TABLE run_recovery_lineage (
    run_id TEXT PRIMARY KEY,
    source_run_id TEXT NOT NULL,
    lineage_kind TEXT NOT NULL CHECK (
        lineage_kind IN ('redrive', 'fork', 'migrate', 'continue_as_new')
    ),
    source_generation INTEGER NOT NULL CHECK (source_generation >= 1),
    target_generation INTEGER NOT NULL CHECK (target_generation >= 1),
    source_definition_id TEXT NOT NULL,
    source_definition_revision_id TEXT NOT NULL,
    source_deployment_revision_id TEXT NOT NULL,
    source_plan_hash TEXT NOT NULL CHECK (
        length(source_plan_hash) = 71 AND source_plan_hash LIKE 'sha256:%'
    ),
    source_binding_hash TEXT NOT NULL CHECK (
        length(source_binding_hash) = 71 AND source_binding_hash LIKE 'sha256:%'
    ),
    target_definition_id TEXT NOT NULL,
    target_definition_revision_id TEXT NOT NULL,
    target_deployment_revision_id TEXT NOT NULL,
    target_plan_hash TEXT NOT NULL CHECK (
        length(target_plan_hash) = 71 AND target_plan_hash LIKE 'sha256:%'
    ),
    target_binding_hash TEXT NOT NULL CHECK (
        length(target_binding_hash) = 71 AND target_binding_hash LIKE 'sha256:%'
    ),
    source_checkpoint_hash TEXT CHECK (
        source_checkpoint_hash IS NULL
        OR (length(source_checkpoint_hash) = 71 AND source_checkpoint_hash LIKE 'sha256:%')
    ),
    created_by_transition_key TEXT NOT NULL,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    UNIQUE (source_run_id, run_id, lineage_kind),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (source_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (source_run_id, source_checkpoint_hash)
        REFERENCES scheduler_checkpoints(run_id, content_hash) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, created_by_transition_key)
        REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT,
    FOREIGN KEY (
        source_definition_id, source_definition_revision_id,
        source_deployment_revision_id, source_plan_hash, source_binding_hash
    ) REFERENCES deployment_revisions(
        definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash
    ) ON DELETE RESTRICT,
    FOREIGN KEY (
        target_definition_id, target_definition_revision_id,
        target_deployment_revision_id, target_plan_hash, target_binding_hash
    ) REFERENCES deployment_revisions(
        definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash
    ) ON DELETE RESTRICT,
    CHECK (source_run_id <> run_id),
    CHECK (
        (lineage_kind = 'continue_as_new'
            AND target_generation = source_generation + 1
            AND source_checkpoint_hash IS NULL)
        OR (lineage_kind IN ('redrive', 'migrate')
            AND target_generation = 1
            AND source_checkpoint_hash IS NULL)
        OR (lineage_kind = 'fork'
            AND target_generation = 1
            AND source_checkpoint_hash IS NOT NULL)
    )
);

CREATE TABLE run_migration_intents (
    run_id TEXT PRIMARY KEY,
    target_run_id TEXT NOT NULL UNIQUE,
    target_definition_id TEXT NOT NULL,
    target_definition_revision_id TEXT NOT NULL,
    target_deployment_revision_id TEXT NOT NULL,
    target_plan_hash TEXT NOT NULL CHECK (
        length(target_plan_hash) = 71 AND target_plan_hash LIKE 'sha256:%'
    ),
    target_binding_hash TEXT NOT NULL CHECK (
        length(target_binding_hash) = 71 AND target_binding_hash LIKE 'sha256:%'
    ),
    target_input TEXT NOT NULL CHECK (json_valid(target_input)),
    target_input_hash TEXT NOT NULL CHECK (
        length(target_input_hash) = 71 AND target_input_hash LIKE 'sha256:%'
    ),
    mapping_contracts TEXT NOT NULL CHECK (json_valid(mapping_contracts)),
    mapping_hash TEXT NOT NULL CHECK (
        length(mapping_hash) = 71 AND mapping_hash LIKE 'sha256:%'
    ),
    reuse_candidates TEXT NOT NULL CHECK (json_valid(reuse_candidates)),
    intent_transition_key TEXT NOT NULL UNIQUE,
    intent_event_id TEXT NOT NULL,
    intent_state TEXT NOT NULL CHECK (intent_state IN ('pending', 'completed')),
    final_transition_key TEXT UNIQUE,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    completed_at TEXT,
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, intent_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
    FOREIGN KEY (
        target_definition_id, target_definition_revision_id,
        target_deployment_revision_id, target_plan_hash, target_binding_hash
    ) REFERENCES deployment_revisions(
        definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash
    ) ON DELETE RESTRICT,
    CHECK (run_id <> target_run_id),
    CHECK (
        (intent_state = 'pending' AND final_transition_key IS NULL AND completed_at IS NULL)
        OR (intent_state = 'completed'
            AND final_transition_key IS NOT NULL AND completed_at IS NOT NULL)
    )
);

CREATE TABLE recovery_revision_roots (
    run_id TEXT NOT NULL,
    root_role TEXT NOT NULL CHECK (root_role IN ('source', 'target')),
    source_run_id TEXT NOT NULL,
    definition_id TEXT NOT NULL,
    definition_revision_id TEXT NOT NULL,
    deployment_revision_id TEXT NOT NULL,
    plan_hash TEXT NOT NULL CHECK (length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
    binding_hash TEXT NOT NULL CHECK (
        length(binding_hash) = 71 AND binding_hash LIKE 'sha256:%'
    ),
    created_by_transition_key TEXT NOT NULL,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, root_role),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (source_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (
        definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash
    ) REFERENCES deployment_revisions(
        definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash
    ) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, created_by_transition_key)
        REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT
);

CREATE TABLE recovery_artifact_roots (
    run_id TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    artifact_run_id TEXT NOT NULL,
    artifact_id TEXT NOT NULL,
    source_activation_id TEXT NOT NULL,
    created_by_transition_key TEXT NOT NULL,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, artifact_run_id, artifact_id),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (source_run_id, source_activation_id)
        REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
    FOREIGN KEY (artifact_run_id, artifact_id)
        REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, created_by_transition_key)
        REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT
);

CREATE TABLE recovery_effect_roots (
    run_id TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    effect_run_id TEXT NOT NULL,
    source_activation_id TEXT NOT NULL,
    effect_id TEXT NOT NULL,
    created_by_transition_key TEXT NOT NULL,
    projection_version INTEGER NOT NULL DEFAULT 0 CHECK (projection_version >= 0),
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, effect_run_id, source_activation_id, effect_id),
    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (source_run_id, source_activation_id, effect_id)
        REFERENCES node_activations(run_id, activation_id, effect_id) ON DELETE RESTRICT,
    FOREIGN KEY (run_id, created_by_transition_key)
        REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT
);

CREATE TABLE recovery_transition_results (
    authority_run_id TEXT NOT NULL,
    transition_key TEXT NOT NULL,
    intent_hash TEXT NOT NULL CHECK (length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
    primary_event_run_id TEXT NOT NULL,
    primary_event_id TEXT NOT NULL,
    result_json TEXT NOT NULL CHECK (json_valid(result_json)),
    created_at TEXT NOT NULL,
    PRIMARY KEY (authority_run_id, transition_key),
    UNIQUE (primary_event_run_id, primary_event_id),
    FOREIGN KEY (authority_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
    FOREIGN KEY (primary_event_run_id, primary_event_id)
        REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT
);

-- The SQLite repository serializes writers in one process. It still performs
-- event, projection, timer/inbox, and outbox mutations in one transaction.
