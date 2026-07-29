-- Durable repository schema for a new, empty SQLite database.
-- Provision this file before starting the service.

PRAGMA foreign_keys = ON;
BEGIN IMMEDIATE;

-- SQLite evaluates CASE lazily. Feeding json() an intentionally invalid
-- document only on a non-empty target aborts this transaction without
-- creating a temporary or persistent validation object.
SELECT CASE
  WHEN EXISTS(
    SELECT 1
    FROM main.sqlite_schema
    WHERE name NOT LIKE 'sqlite_%'
  )
  THEN json('durable_schema_target_must_be_empty')
  ELSE 1
END;

CREATE TABLE workflow_definitions(
  definition_id TEXT NOT NULL PRIMARY KEY,
  agent_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK(definition_id <> '' AND agent_id <> '')
);
CREATE TABLE workflow_definition_revisions(
  definition_id TEXT NOT NULL,
  definition_revision_id TEXT NOT NULL,
  revision_status TEXT NOT NULL CHECK(revision_status IN('draft', 'published')),
  author_document TEXT NOT NULL CHECK(json_valid(author_document)),
  canonical_plan TEXT NOT NULL CHECK(json_valid(canonical_plan)),
  plan_hash TEXT NOT NULL CHECK(length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
  compiler_version TEXT NOT NULL,
  expression_engine_version TEXT NOT NULL,
  descriptor_contracts TEXT NOT NULL CHECK(json_valid(descriptor_contracts)),
  created_at TEXT NOT NULL,
  published_at TEXT,
  PRIMARY KEY(definition_id, definition_revision_id),
  UNIQUE(definition_id, definition_revision_id, plan_hash),
  FOREIGN KEY(definition_id) REFERENCES workflow_definitions(definition_id) ON DELETE RESTRICT,
  CHECK((revision_status = 'draft' AND published_at IS NULL)
OR(revision_status = 'published' AND published_at IS NOT NULL))
);
CREATE TABLE deployment_revisions(
  definition_id TEXT NOT NULL,
  definition_revision_id TEXT NOT NULL,
  deployment_revision_id TEXT NOT NULL,
  plan_hash TEXT NOT NULL CHECK(length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
  binding_hash TEXT NOT NULL CHECK(length(binding_hash) = 71 AND binding_hash LIKE 'sha256:%'),
  resolved_bindings TEXT NOT NULL CHECK(json_valid(resolved_bindings)),
  worker_contracts TEXT NOT NULL CHECK(json_valid(worker_contracts)),
  created_at TEXT NOT NULL,
  PRIMARY KEY(definition_id, deployment_revision_id),
  UNIQUE(definition_id,
definition_revision_id,
deployment_revision_id,
plan_hash,
binding_hash),
  FOREIGN KEY(definition_id, definition_revision_id, plan_hash)
  REFERENCES workflow_definition_revisions(definition_id,
definition_revision_id,
plan_hash) ON DELETE RESTRICT
);
CREATE TRIGGER trg_published_revision_update_immutable
    BEFORE UPDATE ON workflow_definition_revisions
    FOR EACH ROW WHEN OLD.revision_status = 'published'
BEGIN
    SELECT RAISE(ABORT, 'published workflow definition revisions are immutable');
END;
CREATE TRIGGER trg_published_revision_delete_immutable
    BEFORE DELETE ON workflow_definition_revisions
    FOR EACH ROW WHEN OLD.revision_status = 'published'
BEGIN
    SELECT RAISE(ABORT, 'published workflow definition revisions are immutable');
END;
CREATE TRIGGER trg_deployment_revision_immutable
    BEFORE UPDATE ON deployment_revisions
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'deployment revisions are immutable');
END;
CREATE TABLE workflow_runs(
  run_id TEXT NOT NULL PRIMARY KEY,
  definition_id TEXT NOT NULL,
  definition_revision_id TEXT NOT NULL,
  deployment_revision_id TEXT NOT NULL,
  plan_hash TEXT NOT NULL CHECK(length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
  binding_hash TEXT NOT NULL CHECK(length(binding_hash) = 71 AND binding_hash LIKE 'sha256:%'),
  request_id TEXT NOT NULL CHECK(request_id <> ''),
  attachment TEXT NOT NULL CHECK(attachment IN('attached', 'detached')),
  lifecycle TEXT NOT NULL CHECK(lifecycle IN('created', 'active', 'waiting', 'completing', 'terminating',
'succeeded', 'failed', 'cancelled', 'interrupted', 'timed_out')),
  admission_state TEXT NOT NULL CHECK(admission_state IN('open', 'paused', 'draining', 'closed')),
  termination_intent_reason TEXT,
  termination_intent_transition_key TEXT,
  termination_intent_at TEXT,
  input_payload_id TEXT NOT NULL,
  output_payload_id TEXT,
  output_artifact_id TEXT,
  output_value_hash TEXT CHECK(output_value_hash IS NULL
OR(length(output_value_hash) = 71 AND output_value_hash LIKE 'sha256:%')),
  error_code TEXT,
  terminal_event_id TEXT,
  terminal_public_event_id TEXT,
  parent_run_id TEXT,
  lineage_kind TEXT CHECK(lineage_kind IS NULL OR lineage_kind IN('generation', 'redrive', 'fork', 'migrate', 'subflow')),
  generation INTEGER NOT NULL DEFAULT 1 CHECK(generation >= 1),
  replacement_run_id TEXT,
  next_event_seq INTEGER NOT NULL DEFAULT 1 CHECK(next_event_seq >= 1),
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  scheduler_lease_epoch INTEGER NOT NULL DEFAULT 0 CHECK(scheduler_lease_epoch >= 0),
  scheduler_lease_owner TEXT,
  scheduler_fencing_token TEXT,
  scheduler_lease_expires_at TEXT,
  scheduler_heartbeat_at TEXT,
  created_at TEXT NOT NULL,
  started_at TEXT,
  updated_at TEXT NOT NULL,
  terminal_at TEXT,
  deadline_at TEXT,
  response_id TEXT
  GENERATED ALWAYS AS('resp_' || run_id) STORED,
  artifact_reference_retention_seconds INTEGER NOT NULL DEFAULT 2592000
  CHECK(artifact_reference_retention_seconds >= 1
AND artifact_reference_retention_seconds <= 315360000),
  FOREIGN KEY(definition_id,
definition_revision_id,
deployment_revision_id,
plan_hash,
binding_hash) REFERENCES deployment_revisions(definition_id,
definition_revision_id,
deployment_revision_id,
plan_hash,
binding_hash) ON DELETE RESTRICT,
  FOREIGN KEY(parent_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(replacement_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT
  DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(run_id, input_payload_id) REFERENCES payloads(run_id, payload_id)
  ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(run_id, output_payload_id) REFERENCES payloads(run_id, payload_id)
  ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(run_id, output_artifact_id) REFERENCES artifacts(run_id, artifact_id)
  ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(run_id, terminal_event_id) REFERENCES execution_events(run_id, event_id)
  ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(run_id, terminal_public_event_id)
  REFERENCES public_event_outbox(run_id, public_event_id)
  ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  CHECK((termination_intent_reason IS NULL
AND termination_intent_transition_key IS NULL
AND termination_intent_at IS NULL)
OR(termination_intent_reason IS NOT NULL
AND termination_intent_transition_key IS NOT NULL
AND termination_intent_at IS NOT NULL)),
  CHECK((scheduler_lease_owner IS NULL
AND scheduler_fencing_token IS NULL
AND scheduler_lease_expires_at IS NULL
AND scheduler_heartbeat_at IS NULL)
OR(scheduler_lease_epoch > 0
AND scheduler_lease_owner IS NOT NULL
AND scheduler_fencing_token IS NOT NULL
AND scheduler_lease_expires_at IS NOT NULL
AND scheduler_heartbeat_at IS NOT NULL)),
  CHECK((parent_run_id IS NULL AND lineage_kind IS NULL AND generation = 1)
OR(parent_run_id IS NOT NULL AND lineage_kind IS NOT NULL)),
  CHECK((lifecycle IN('created', 'active', 'waiting')
AND terminal_at IS NULL
AND terminal_event_id IS NULL
AND terminal_public_event_id IS NULL
AND output_payload_id IS NULL
AND output_artifact_id IS NULL
AND output_value_hash IS NULL
AND error_code IS NULL)
OR(lifecycle = 'completing'
AND admission_state IN('draining', 'closed')
AND terminal_at IS NULL
AND terminal_event_id IS NULL
AND terminal_public_event_id IS NULL)
OR(lifecycle = 'terminating'
AND admission_state IN('draining', 'closed')
AND termination_intent_reason IS NOT NULL
AND terminal_at IS NULL
AND terminal_event_id IS NULL
AND terminal_public_event_id IS NULL)
OR(lifecycle = 'succeeded'
AND admission_state = 'closed'
AND termination_intent_reason IS NULL
AND terminal_at IS NOT NULL
AND terminal_event_id IS NOT NULL
AND terminal_public_event_id IS NOT NULL
AND output_value_hash IS NOT NULL
AND((output_payload_id IS NOT NULL) +(output_artifact_id IS NOT NULL)) = 1
AND error_code IS NULL)
OR(lifecycle IN('failed', 'cancelled', 'interrupted', 'timed_out')
AND admission_state = 'closed'
AND termination_intent_reason IS NOT NULL
AND terminal_at IS NOT NULL
AND terminal_event_id IS NOT NULL
AND terminal_public_event_id IS NOT NULL
AND output_payload_id IS NULL
AND output_artifact_id IS NULL
AND output_value_hash IS NULL
AND error_code IS NOT NULL))
);
CREATE INDEX idx_runs_dispatch
ON workflow_runs(
  lifecycle,
  admission_state,
  updated_at,
  run_id
)
WHERE lifecycle IN('active', 'waiting');
CREATE INDEX idx_runs_recovery
ON workflow_runs(
  updated_at,
  run_id
)
WHERE lifecycle = 'terminating'
OR(lifecycle IN('created', 'active', 'waiting') AND admission_state = 'open');
CREATE TABLE payloads(
  run_id TEXT NOT NULL,
  payload_id TEXT NOT NULL,
  content_hash TEXT NOT NULL CHECK(length(content_hash) = 71 AND content_hash LIKE 'sha256:%'),
  canonical_bytes INTEGER NOT NULL CHECK(canonical_bytes >= 0),
  encoding TEXT NOT NULL CHECK(encoding IN('json_jcs', 'binary')),
  inline_value TEXT CHECK(inline_value IS NULL OR json_valid(inline_value)),
  binary_value BLOB,
  created_at TEXT NOT NULL,
  retain_until TEXT,
  PRIMARY KEY(run_id, payload_id),
  UNIQUE(run_id, content_hash),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  CHECK((encoding = 'json_jcs' AND inline_value IS NOT NULL AND binary_value IS NULL)
OR(encoding = 'binary' AND inline_value IS NULL AND binary_value IS NOT NULL))
);
CREATE TABLE artifacts(
  run_id TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  content_hash TEXT NOT NULL CHECK(length(content_hash) = 71 AND content_hash LIKE 'sha256:%'),
  size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
  media_type TEXT,
  storage_uri TEXT NOT NULL,
  artifact_state TEXT NOT NULL CHECK(artifact_state IN('staged', 'verified', 'referenced', 'deleting', 'deleted')),
  verified_at TEXT,
  referenced_at TEXT,
  retain_until TEXT,
  deletion_fence TEXT CHECK(deletion_fence IS NULL
OR(length(deletion_fence) = 71 AND deletion_fence LIKE 'sha256:%')),
  deletion_claim_token TEXT CHECK(deletion_claim_token IS NULL
OR(length(deletion_claim_token) = 71 AND deletion_claim_token LIKE 'sha256:%')),
  deletion_claimed_by TEXT,
  deletion_claim_request_key TEXT,
  deletion_claimed_at TEXT,
  deletion_claim_expires_at TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, artifact_id),
  UNIQUE(run_id, content_hash),
  UNIQUE(run_id, deletion_fence),
  UNIQUE(deletion_claim_token),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  CHECK((artifact_state = 'staged' AND verified_at IS NULL AND referenced_at IS NULL
AND deletion_fence IS NULL AND deletion_claim_token IS NULL
AND deletion_claimed_by IS NULL AND deletion_claim_request_key IS NULL
AND deletion_claimed_at IS NULL AND deletion_claim_expires_at IS NULL)
OR(artifact_state = 'verified' AND verified_at IS NOT NULL AND referenced_at IS NULL
AND deletion_fence IS NULL AND deletion_claim_token IS NULL
AND deletion_claimed_by IS NULL AND deletion_claim_request_key IS NULL
AND deletion_claimed_at IS NULL AND deletion_claim_expires_at IS NULL)
OR(artifact_state = 'referenced' AND verified_at IS NOT NULL AND referenced_at IS NOT NULL
AND deletion_fence IS NULL AND deletion_claim_token IS NULL
AND deletion_claimed_by IS NULL AND deletion_claim_request_key IS NULL
AND deletion_claimed_at IS NULL AND deletion_claim_expires_at IS NULL)
OR(artifact_state IN('deleting', 'deleted') AND referenced_at IS NULL
AND deletion_fence IS NOT NULL AND deletion_claim_token IS NOT NULL
AND deletion_claimed_by IS NOT NULL AND deletion_claim_request_key IS NOT NULL
AND deletion_claimed_at IS NOT NULL AND deletion_claim_expires_at IS NOT NULL))
);
CREATE INDEX idx_artifact_orphan_sweep
ON artifacts(
  artifact_state,
  created_at,
  retain_until,
  run_id,
  artifact_id
)
WHERE artifact_state IN('staged', 'verified', 'deleting');
CREATE TABLE artifact_gc_sweeps(
  transition_key TEXT NOT NULL PRIMARY KEY,
  intent_hash TEXT NOT NULL CHECK(length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
  claimed_by TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE artifact_gc_claims(
  transition_key TEXT NOT NULL,
  run_id TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  claim_token TEXT NOT NULL CHECK(length(claim_token) = 71 AND claim_token LIKE 'sha256:%'),
  deletion_fence TEXT NOT NULL CHECK(length(deletion_fence) = 71 AND deletion_fence LIKE 'sha256:%'),
  claim_expires_at TEXT NOT NULL,
  PRIMARY KEY(transition_key, run_id, artifact_id),
  UNIQUE(claim_token),
  FOREIGN KEY(transition_key) REFERENCES artifact_gc_sweeps(transition_key)
  ON DELETE RESTRICT,
  FOREIGN KEY(run_id, artifact_id) REFERENCES artifacts(run_id, artifact_id)
  ON DELETE RESTRICT
);
CREATE TABLE scope_instances(
  run_id TEXT NOT NULL,
  scope_instance_id TEXT NOT NULL,
  parent_scope_instance_id TEXT,
  static_scope_id TEXT NOT NULL,
  stable_dynamic_key TEXT,
  scope_kind TEXT NOT NULL CHECK(scope_kind IN('root', 'map_item', 'loop_iteration', 'subflow_invocation',
'agent_loop_turn', 'parallel_leg')),
  is_root INTEGER NOT NULL CHECK(is_root IN(0, 1)),
  lifecycle TEXT NOT NULL CHECK(lifecycle IN('active', 'settling', 'settled', 'cancelled')),
  admission_state TEXT NOT NULL CHECK(admission_state IN('open', 'draining', 'closed')),
  admitted_children INTEGER NOT NULL DEFAULT 0 CHECK(admitted_children >= 0),
  settled_children INTEGER NOT NULL DEFAULT 0 CHECK(settled_children >= 0 AND settled_children <= admitted_children),
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  settled_at TEXT,
  PRIMARY KEY(run_id, scope_instance_id),
  UNIQUE(run_id, parent_scope_instance_id, static_scope_id, stable_dynamic_key),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, parent_scope_instance_id)
  REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
  CHECK((is_root = 1
AND scope_kind = 'root'
AND parent_scope_instance_id IS NULL
AND stable_dynamic_key IS NULL)
OR(is_root = 0
AND scope_kind <> 'root'
AND parent_scope_instance_id IS NOT NULL
AND stable_dynamic_key IS NOT NULL)),
  CHECK((lifecycle IN('active', 'settling') AND settled_at IS NULL)
OR(lifecycle IN('settled', 'cancelled')
AND admission_state = 'closed'
AND settled_children = admitted_children
AND settled_at IS NOT NULL))
);
CREATE UNIQUE INDEX uq_root_scope_per_run
ON scope_instances(
  run_id
) WHERE is_root = 1;
CREATE INDEX idx_scope_parent
ON scope_instances(
  run_id,
  parent_scope_instance_id,
  scope_instance_id
);
CREATE TABLE node_activations(
  run_id TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  scope_instance_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  stable_activation_key TEXT NOT NULL,
  execution_kind TEXT NOT NULL CHECK(execution_kind IN('worker', 'scheduler_native', 'durable_wait')),
  lifecycle TEXT NOT NULL CHECK(lifecycle IN('created', 'ready', 'leased', 'running', 'retry_wait', 'waiting',
'terminating', 'succeeded', 'failed', 'cancelled', 'timed_out')),
  effect_id TEXT NOT NULL,
  effect_idempotency TEXT NOT NULL CHECK(effect_idempotency IN('idempotent', 'non_idempotent')),
  effect_evidence TEXT NOT NULL CHECK(effect_evidence IN('not_started', 'started', 'committed', 'unknown')),
  last_attempt_no INTEGER CHECK(last_attempt_no IS NULL OR last_attempt_no >= 1),
  last_lease_epoch INTEGER CHECK(last_lease_epoch IS NULL OR last_lease_epoch >= 1),
  current_attempt_no INTEGER CHECK(current_attempt_no IS NULL OR current_attempt_no >= 1),
  current_lease_epoch INTEGER CHECK(current_lease_epoch IS NULL OR current_lease_epoch >= 1),
  current_fencing_token TEXT,
  retry_budget_remaining INTEGER NOT NULL CHECK(retry_budget_remaining >= 0),
  pending_retry_timer_id TEXT,
  wait_registration_transition_key TEXT,
  termination_intent_reason TEXT,
  termination_intent_transition_key TEXT,
  termination_intent_at TEXT,
  output_payload_id TEXT,
  output_artifact_id TEXT,
  output_value_hash TEXT CHECK(output_value_hash IS NULL
OR(length(output_value_hash) = 71 AND output_value_hash LIKE 'sha256:%')),
  winning_attempt_no INTEGER CHECK(winning_attempt_no IS NULL OR winning_attempt_no >= 1),
  reused_from_run_id TEXT,
  reused_from_activation_id TEXT,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  terminal_at TEXT,
  PRIMARY KEY(run_id, activation_id),
  UNIQUE(run_id, scope_instance_id, node_id, stable_activation_key),
  UNIQUE(run_id, effect_id),
  UNIQUE(run_id, activation_id, effect_id),
  UNIQUE(run_id, wait_registration_transition_key),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, scope_instance_id)
  REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, output_payload_id)
  REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, output_artifact_id)
  REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
  FOREIGN KEY(reused_from_run_id, reused_from_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id,
activation_id,
current_attempt_no,
current_lease_epoch,
current_fencing_token) REFERENCES node_attempts(run_id,
activation_id,
attempt_no,
lease_epoch,
fencing_token) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(run_id, activation_id, winning_attempt_no)
  REFERENCES node_attempts(run_id, activation_id, attempt_no)
  ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY(run_id, pending_retry_timer_id) REFERENCES timers(run_id, timer_id)
  ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  CHECK((last_attempt_no IS NULL AND last_lease_epoch IS NULL)
OR(last_attempt_no IS NOT NULL
AND last_lease_epoch IS NOT NULL
AND last_lease_epoch >= last_attempt_no)),
  CHECK((lifecycle IN('leased', 'running')
AND execution_kind = 'worker'
AND current_attempt_no = last_attempt_no
AND current_lease_epoch = last_lease_epoch
AND current_fencing_token IS NOT NULL)
OR(lifecycle NOT IN('leased', 'running')
AND current_attempt_no IS NULL
AND current_lease_epoch IS NULL
AND current_fencing_token IS NULL)),
  CHECK(execution_kind = 'worker'
OR(last_attempt_no IS NULL
AND last_lease_epoch IS NULL
AND current_attempt_no IS NULL
AND current_lease_epoch IS NULL
AND current_fencing_token IS NULL)),
  CHECK(lifecycle <> 'waiting' OR execution_kind = 'durable_wait'),
  CHECK(lifecycle <> 'waiting' OR wait_registration_transition_key IS NOT NULL),
  CHECK((lifecycle = 'retry_wait'
AND execution_kind = 'worker'
AND pending_retry_timer_id IS NOT NULL)
OR(lifecycle <> 'retry_wait' AND pending_retry_timer_id IS NULL)),
  CHECK((termination_intent_reason IS NULL
AND termination_intent_transition_key IS NULL
AND termination_intent_at IS NULL)
OR(termination_intent_reason IS NOT NULL
AND termination_intent_transition_key IS NOT NULL
AND termination_intent_at IS NOT NULL)),
  CHECK((lifecycle IN('created', 'ready', 'leased', 'running', 'retry_wait', 'waiting')
AND termination_intent_reason IS NULL
AND terminal_at IS NULL
AND output_payload_id IS NULL
AND output_artifact_id IS NULL
AND output_value_hash IS NULL)
OR(lifecycle = 'terminating'
AND termination_intent_reason IS NOT NULL
AND terminal_at IS NULL
AND output_payload_id IS NULL
AND output_artifact_id IS NULL
AND output_value_hash IS NULL)
OR(lifecycle = 'succeeded'
AND termination_intent_reason IS NULL
AND terminal_at IS NOT NULL
AND output_value_hash IS NOT NULL
AND((output_payload_id IS NOT NULL) +(output_artifact_id IS NOT NULL)) = 1
AND((execution_kind = 'worker' AND winning_attempt_no IS NOT NULL)
OR(execution_kind <> 'worker' AND winning_attempt_no IS NULL)
OR(reused_from_activation_id IS NOT NULL AND winning_attempt_no IS NULL)))
OR(lifecycle IN('failed', 'cancelled', 'timed_out')
AND termination_intent_reason IS NOT NULL
AND terminal_at IS NOT NULL
AND output_payload_id IS NULL
AND output_artifact_id IS NULL
AND output_value_hash IS NULL)),
  CHECK((reused_from_run_id IS NULL AND reused_from_activation_id IS NULL)
OR(reused_from_run_id IS NOT NULL
AND reused_from_activation_id IS NOT NULL
AND lifecycle = 'succeeded'
AND last_attempt_no IS NULL))
);
CREATE INDEX idx_activation_dispatch
ON node_activations(
  run_id,
  lifecycle,
  scope_instance_id,
  activation_id
)
WHERE lifecycle IN('ready', 'retry_wait', 'waiting');
CREATE TABLE node_attempts(
  run_id TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL CHECK(attempt_no >= 1),
  lease_epoch INTEGER NOT NULL CHECK(lease_epoch >= attempt_no),
  fencing_token TEXT NOT NULL,
  effect_id TEXT NOT NULL,
  lifecycle TEXT NOT NULL CHECK(lifecycle IN('created', 'leased', 'running', 'succeeded', 'failed',
'timed_out', 'abandoned', 'cancelled')),
  effect_evidence TEXT NOT NULL CHECK(effect_evidence IN('not_started', 'started', 'committed', 'unknown')),
  worker_id TEXT,
  lease_expires_at TEXT,
  heartbeat_at TEXT,
  output_payload_id TEXT,
  output_artifact_id TEXT,
  output_value_hash TEXT CHECK(output_value_hash IS NULL
OR(length(output_value_hash) = 71 AND output_value_hash LIKE 'sha256:%')),
  failure_code TEXT,
  completion_transition_key TEXT,
  terminal_event_id TEXT,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  started_at TEXT,
  terminal_at TEXT,
  PRIMARY KEY(run_id, activation_id, attempt_no),
  UNIQUE(run_id, activation_id, attempt_no, lease_epoch, fencing_token),
  UNIQUE(run_id, activation_id, lease_epoch),
  UNIQUE(run_id, fencing_token),
  UNIQUE(run_id, completion_transition_key),
  FOREIGN KEY(run_id, activation_id, effect_id)
  REFERENCES node_activations(run_id, activation_id, effect_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, output_payload_id)
  REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, output_artifact_id)
  REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, terminal_event_id) REFERENCES execution_events(run_id, event_id)
  ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  CHECK(lifecycle = 'created'
OR(worker_id IS NOT NULL AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL)),
  CHECK((lifecycle IN('created', 'leased', 'running')
AND terminal_at IS NULL
AND terminal_event_id IS NULL
AND completion_transition_key IS NULL
AND output_payload_id IS NULL
AND output_artifact_id IS NULL
AND output_value_hash IS NULL)
OR(lifecycle = 'succeeded'
AND terminal_at IS NOT NULL
AND terminal_event_id IS NOT NULL
AND completion_transition_key IS NOT NULL
AND output_value_hash IS NOT NULL
AND((output_payload_id IS NOT NULL) +(output_artifact_id IS NOT NULL)) = 1
AND failure_code IS NULL)
OR(lifecycle IN('failed', 'timed_out', 'abandoned', 'cancelled')
AND terminal_at IS NOT NULL
AND terminal_event_id IS NOT NULL
AND completion_transition_key IS NOT NULL
AND output_payload_id IS NULL
AND output_artifact_id IS NULL
AND output_value_hash IS NULL)),
  CHECK(lifecycle <> 'abandoned' OR effect_evidence <> 'started')
);
CREATE INDEX idx_attempt_lease_expiry
ON node_attempts(
  lease_expires_at,
  run_id,
  activation_id,
  attempt_no
)
WHERE lifecycle IN('leased', 'running');
CREATE TABLE execution_events(
  run_id TEXT NOT NULL,
  seq INTEGER NOT NULL CHECK(seq >= 1),
  event_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL CHECK(schema_version >= 1),
  kind TEXT NOT NULL,
  transition_key TEXT NOT NULL,
  intent_hash TEXT NOT NULL CHECK(length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
  node_id TEXT,
  scope_instance_id TEXT,
  activation_id TEXT,
  attempt_no INTEGER CHECK(attempt_no IS NULL OR attempt_no >= 1),
  causation_event_id TEXT,
  projection_version_after INTEGER NOT NULL CHECK(projection_version_after >= 0),
  safe_payload TEXT NOT NULL CHECK(json_valid(safe_payload)),
  occurred_at TEXT NOT NULL,
  projection_ledger_batch TEXT
  CHECK(projection_ledger_batch IS NULL OR json_valid(projection_ledger_batch)),
  PRIMARY KEY(run_id, seq),
  UNIQUE(run_id, event_id),
  UNIQUE(run_id, transition_key),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, scope_instance_id)
  REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, activation_id, attempt_no)
  REFERENCES node_attempts(run_id, activation_id, attempt_no) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, causation_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  CHECK(attempt_no IS NULL OR activation_id IS NOT NULL),
  CHECK(activation_id IS NULL OR(node_id IS NOT NULL AND scope_instance_id IS NOT NULL)),
  CHECK(node_id IS NULL OR scope_instance_id IS NOT NULL),
  CHECK(causation_event_id IS NULL OR causation_event_id <> event_id)
);
CREATE INDEX idx_execution_events_rebuild ON execution_events(run_id, seq);
CREATE TABLE artifact_retention_releases(
  run_id TEXT NOT NULL PRIMARY KEY,
  transition_key TEXT NOT NULL UNIQUE,
  intent_hash TEXT NOT NULL CHECK(length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
  event_id TEXT NOT NULL,
  event_seq INTEGER NOT NULL CHECK(event_seq >= 1),
  retain_until TEXT NOT NULL,
  artifact_count INTEGER NOT NULL CHECK(artifact_count >= 0),
  created_at TEXT NOT NULL,
  registration_kind TEXT NOT NULL DEFAULT 'legacy'
  CHECK(registration_kind IN('legacy', 'terminal_atomic')),
  FOREIGN KEY(run_id, event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT
);
CREATE INDEX idx_artifact_retention_due
ON artifact_retention_releases(
  retain_until,
  run_id
);
CREATE TABLE projection_checkpoint_batches(
  run_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  checkpoint_schema_version INTEGER NOT NULL CHECK(checkpoint_schema_version >= 1),
  subject_count INTEGER NOT NULL CHECK(subject_count >= 0),
  manifest_hash TEXT NOT NULL CHECK(length(manifest_hash) = 71 AND manifest_hash LIKE 'sha256:%'),
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, event_id),
  FOREIGN KEY(run_id, event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT
);
CREATE TABLE projection_checkpoints(
  run_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  subject_kind TEXT NOT NULL CHECK(subject_kind IN('run', 'scope', 'activation', 'attempt', 'timer', 'control',
'fork', 'join', 'scheduler', 'data_value')),
  subject_id TEXT NOT NULL,
  checkpoint_schema_version INTEGER NOT NULL CHECK(checkpoint_schema_version >= 1),
  subject_projection_version INTEGER NOT NULL CHECK(subject_projection_version >= 0),
  projection_hash TEXT NOT NULL CHECK(length(projection_hash) = 71 AND projection_hash LIKE 'sha256:%'),
  canonical_projection TEXT NOT NULL CHECK(json_valid(canonical_projection)),
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, event_id, subject_kind, subject_id),
  UNIQUE(run_id, subject_kind, subject_id, subject_projection_version),
  FOREIGN KEY(run_id, event_id)
  REFERENCES projection_checkpoint_batches(run_id, event_id) ON DELETE RESTRICT
);
CREATE INDEX idx_projection_checkpoint_rebuild
ON projection_checkpoints(
  run_id,
  subject_kind,
  subject_id,
  subject_projection_version
);
CREATE TABLE scheduler_checkpoints(
  run_id TEXT NOT NULL,
  checkpoint_id TEXT NOT NULL,
  content_hash TEXT NOT NULL CHECK(length(content_hash) = 71 AND content_hash LIKE 'sha256:%'),
  checkpoint_kind TEXT NOT NULL CHECK(checkpoint_kind IN('planned_action', 'task_started', 'task_completed', 'task_retry_scheduled')),
  transition_key TEXT NOT NULL,
  intent_hash TEXT NOT NULL CHECK(length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
  event_id TEXT NOT NULL,
  checkpoint_schema_version INTEGER NOT NULL CHECK(checkpoint_schema_version >= 1),
  scheduler_projection_version INTEGER NOT NULL CHECK(scheduler_projection_version >= 1),
  fact_payload TEXT NOT NULL CHECK(json_valid(fact_payload)),
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, checkpoint_id),
  UNIQUE(run_id, content_hash),
  UNIQUE(run_id, transition_key),
  UNIQUE(run_id, event_id),
  FOREIGN KEY(run_id, event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, transition_key)
  REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT
);
CREATE INDEX idx_scheduler_checkpoints_recovery
ON scheduler_checkpoints(
  run_id,
  scheduler_projection_version,
  checkpoint_id
);
CREATE TABLE scheduler_values(
  run_id TEXT NOT NULL,
  port_id TEXT NOT NULL,
  owner_activation_id TEXT NOT NULL,
  runtime_value TEXT NOT NULL CHECK(json_valid(runtime_value)),
  value_ref TEXT NOT NULL CHECK(json_valid(value_ref)),
  declared_type TEXT NOT NULL CHECK(json_valid(declared_type)),
  storage_kind TEXT NOT NULL CHECK(storage_kind IN('inline', 'artifact')),
  payload_id TEXT,
  artifact_id TEXT,
  content_hash TEXT NOT NULL CHECK(length(content_hash) = 71 AND content_hash LIKE 'sha256:%'),
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(run_id, port_id),
  FOREIGN KEY(run_id, owner_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, payload_id)
  REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, artifact_id)
  REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
  CHECK((storage_kind = 'inline' AND payload_id IS NOT NULL AND artifact_id IS NULL)
OR(storage_kind = 'artifact' AND payload_id IS NULL AND artifact_id IS NOT NULL))
);
CREATE INDEX idx_scheduler_values_owner
ON scheduler_values(
  run_id,
  owner_activation_id,
  port_id
);
CREATE TABLE scheduler_occurrence_values(
  run_id TEXT NOT NULL,
  occurrence_key TEXT NOT NULL CHECK(json_valid(occurrence_key)),
  port_id TEXT NOT NULL,
  owner_activation_id TEXT NOT NULL,
  runtime_value TEXT NOT NULL CHECK(json_valid(runtime_value)),
  value_ref TEXT NOT NULL CHECK(json_valid(value_ref)),
  declared_type TEXT NOT NULL CHECK(json_valid(declared_type)),
  storage_kind TEXT NOT NULL CHECK(storage_kind IN('inline', 'artifact')),
  payload_id TEXT,
  artifact_id TEXT,
  content_hash TEXT NOT NULL CHECK(length(content_hash) = 71 AND content_hash LIKE 'sha256:%'),
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY(run_id, occurrence_key, port_id),
  FOREIGN KEY(run_id, owner_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, payload_id)
  REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, artifact_id)
  REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
  CHECK((storage_kind = 'inline' AND payload_id IS NOT NULL AND artifact_id IS NULL)
OR(storage_kind = 'artifact' AND payload_id IS NULL AND artifact_id IS NOT NULL))
);
CREATE INDEX idx_occurrence_values_owner
ON scheduler_occurrence_values(
  run_id,
  owner_activation_id,
  occurrence_key,
  port_id
);
CREATE TABLE task_outbox(
  run_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL CHECK(attempt_no >= 1),
  lease_epoch INTEGER NOT NULL CHECK(lease_epoch >= attempt_no),
  fencing_token TEXT NOT NULL,
  effect_id TEXT NOT NULL,
  created_by_transition_key TEXT NOT NULL,
  task_state TEXT NOT NULL CHECK(task_state IN('pending', 'claimed', 'published', 'acked', 'dead')),
  task_envelope TEXT NOT NULL CHECK(json_valid(task_envelope)),
  available_at TEXT NOT NULL,
  claimed_by TEXT,
  claim_token TEXT,
  claim_expires_at TEXT,
  publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK(publish_attempts >= 0),
  published_at TEXT,
  acked_at TEXT,
  last_error_code TEXT,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  claim_mode TEXT CHECK(claim_mode IS NULL
OR claim_mode IN('execute', 'finalize_lease_loss', 'acknowledge')),
  PRIMARY KEY(run_id, task_id),
  UNIQUE(run_id, activation_id, attempt_no, lease_epoch),
  UNIQUE(run_id, created_by_transition_key),
  FOREIGN KEY(run_id, activation_id, attempt_no, lease_epoch, fencing_token)
  REFERENCES node_attempts(run_id,
activation_id,
attempt_no,
lease_epoch,
fencing_token) ON DELETE RESTRICT,
  CHECK((task_state = 'pending' AND claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)
OR(task_state = 'claimed' AND claimed_by IS NOT NULL AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)
OR(task_state IN('published', 'acked', 'dead'))),
  CHECK(task_state <> 'published' OR published_at IS NOT NULL),
  CHECK(task_state <> 'acked' OR(published_at IS NOT NULL AND acked_at IS NOT NULL))
);
CREATE INDEX idx_task_outbox_dispatch
ON task_outbox(
  available_at,
  run_id,
  task_id
) WHERE task_state = 'pending';
CREATE INDEX idx_task_outbox_acknowledge
ON task_outbox(
  available_at,
  run_id,
  task_id
) WHERE task_state = 'published';
CREATE INDEX idx_task_outbox_reclaim
ON task_outbox(
  claim_expires_at,
  run_id,
  task_id
) WHERE task_state = 'claimed';
CREATE TABLE public_event_outbox(
  run_id TEXT NOT NULL,
  public_event_id TEXT NOT NULL,
  causation_event_id TEXT NOT NULL,
  public_ordinal INTEGER NOT NULL CHECK(public_ordinal > 0),
  public_schema_version INTEGER NOT NULL CHECK(public_schema_version >= 1),
  event_kind TEXT NOT NULL,
  is_terminal INTEGER NOT NULL CHECK(is_terminal IN(0, 1)),
  publish_state TEXT NOT NULL CHECK(publish_state IN('pending', 'claimed', 'published')),
  safe_envelope TEXT NOT NULL CHECK(json_valid(safe_envelope)),
  available_at TEXT NOT NULL,
  claimed_by TEXT,
  claim_token TEXT,
  claim_expires_at TEXT,
  publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK(publish_attempts >= 0),
  published_at TEXT,
  published_by TEXT,
  published_claim_token TEXT,
  notified_at TEXT,
  retain_until TEXT,
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, public_event_id),
  UNIQUE(run_id, causation_event_id, event_kind),
  UNIQUE(run_id, causation_event_id, public_ordinal),
  FOREIGN KEY(run_id, causation_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  CHECK((publish_state = 'pending' AND claimed_by IS NULL AND claim_token IS NULL
AND claim_expires_at IS NULL AND published_at IS NULL
AND published_by IS NULL AND published_claim_token IS NULL
AND notified_at IS NULL AND retain_until IS NULL)
OR(publish_state = 'claimed' AND claimed_by IS NOT NULL AND claim_token IS NOT NULL
AND claim_expires_at IS NOT NULL AND published_at IS NULL
AND published_by IS NULL AND published_claim_token IS NULL
AND notified_at IS NULL AND retain_until IS NULL)
OR(publish_state = 'published' AND published_at IS NOT NULL
AND claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL
AND published_by IS NOT NULL AND published_claim_token IS NOT NULL
AND notified_at IS NOT NULL
AND((is_terminal = 1 AND retain_until IS NULL)
OR(is_terminal = 0 AND retain_until IS NOT NULL)))),
  CHECK((is_terminal = 0 AND event_kind IN('run.created','run.started','operation.started',
'operation.completed','operation.failed'))
OR(is_terminal = 1 AND event_kind IN('run.completed','run.failed','run.cancelled','run.interrupted'))),
  CHECK((event_kind = 'run.created' AND public_ordinal = 10)
OR(event_kind = 'run.started' AND public_ordinal = 20)
OR(event_kind = 'operation.started' AND public_ordinal = 30)
OR(event_kind IN('operation.completed', 'operation.failed') AND public_ordinal = 40)
OR(event_kind IN('run.completed', 'run.failed', 'run.cancelled', 'run.interrupted')
AND public_ordinal = 50))
);
CREATE UNIQUE INDEX uq_public_terminal_per_run
ON public_event_outbox(
  run_id
) WHERE is_terminal = 1;
CREATE UNIQUE INDEX uq_public_outbox_public_event_id
ON public_event_outbox(
  public_event_id
);
CREATE INDEX idx_public_outbox_dispatch
ON public_event_outbox(
  available_at,
  run_id,
  public_ordinal,
  public_event_id
)
WHERE publish_state = 'pending';
CREATE INDEX idx_public_outbox_reclaim
ON public_event_outbox(
  claim_expires_at,
  run_id,
  public_ordinal,
  public_event_id
)
WHERE publish_state = 'claimed';
CREATE INDEX idx_public_outbox_retention
ON public_event_outbox(
  retain_until,
  run_id,
  public_event_id
)
WHERE publish_state = 'published'
  AND is_terminal = 0
  AND retain_until IS NOT NULL;
CREATE UNIQUE INDEX uq_public_outbox_claim_token
ON public_event_outbox(
  claim_token
) WHERE claim_token IS NOT NULL;
CREATE UNIQUE INDEX uq_public_outbox_published_claim_token
ON public_event_outbox(
  published_claim_token
)
WHERE published_claim_token IS NOT NULL;
CREATE TABLE signals_inbox(
  run_id TEXT NOT NULL,
  signal_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  intent_hash TEXT NOT NULL CHECK(length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
  signal_name TEXT NOT NULL,
  target_activation_id TEXT NOT NULL,
  payload_id TEXT NOT NULL,
  signal_state TEXT NOT NULL CHECK(signal_state IN('pending', 'consumed', 'rejected', 'expired')),
  received_at TEXT NOT NULL,
  consumed_by_transition_key TEXT,
  consumed_event_id TEXT,
  terminal_at TEXT,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  PRIMARY KEY(run_id, signal_id),
  UNIQUE(run_id, message_id),
  UNIQUE(run_id, consumed_by_transition_key),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, target_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, payload_id)
  REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, consumed_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  CHECK((signal_state = 'pending'
AND consumed_by_transition_key IS NULL
AND consumed_event_id IS NULL
AND terminal_at IS NULL)
OR(signal_state <> 'pending'
AND consumed_by_transition_key IS NOT NULL
AND consumed_event_id IS NOT NULL
AND terminal_at IS NOT NULL))
);
CREATE INDEX idx_signals_pending
ON signals_inbox(
  run_id,
  target_activation_id,
  received_at,
  signal_id
)
WHERE signal_state = 'pending';
CREATE TABLE wait_late_audit_outbox(
  run_id TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  loser_kind TEXT NOT NULL CHECK(loser_kind IN('timer', 'signal')),
  loser_id TEXT NOT NULL,
  due_at TEXT NOT NULL,
  audit_state TEXT NOT NULL CHECK(audit_state IN('pending', 'claimed', 'completed')),
  claimed_by TEXT,
  claim_token TEXT,
  claim_expires_at TEXT,
  created_at TEXT NOT NULL,
  completed_event_id TEXT,
  PRIMARY KEY(run_id, loser_kind, loser_id),
  FOREIGN KEY(run_id, activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, completed_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  CHECK(
    (audit_state='pending' AND claimed_by IS NULL AND claim_token IS NULL
      AND claim_expires_at IS NULL AND completed_event_id IS NULL)
    OR(audit_state='claimed' AND claimed_by IS NOT NULL AND claim_token IS NOT NULL
      AND claim_expires_at IS NOT NULL AND completed_event_id IS NULL)
    OR(audit_state='completed' AND claimed_by IS NULL AND claim_token IS NULL
      AND claim_expires_at IS NULL AND completed_event_id IS NOT NULL)
  )
);
CREATE INDEX idx_wait_late_audit_pending
ON wait_late_audit_outbox(due_at, run_id, loser_kind, loser_id)
WHERE audit_state='pending';
CREATE INDEX idx_wait_late_audit_reclaim
ON wait_late_audit_outbox(claim_expires_at, run_id, loser_kind, loser_id)
WHERE audit_state='claimed';
CREATE UNIQUE INDEX uq_wait_late_audit_claim_token
ON wait_late_audit_outbox(claim_token)
WHERE claim_token IS NOT NULL;
CREATE TABLE timers(
  run_id TEXT NOT NULL,
  timer_id TEXT NOT NULL,
  activation_id TEXT,
  timer_kind TEXT NOT NULL CHECK(timer_kind IN('lease', 'wait', 'retry', 'activation_timeout', 'run_deadline')),
  timer_state TEXT NOT NULL CHECK(timer_state IN('scheduled', 'fired', 'cancelled')),
  deadline_at TEXT NOT NULL,
  expected_attempt_no INTEGER CHECK(expected_attempt_no IS NULL OR expected_attempt_no >= 1),
  expected_lease_epoch INTEGER CHECK(expected_lease_epoch IS NULL OR expected_lease_epoch >= 1),
  expected_fencing_token TEXT,
  retry_budget_snapshot INTEGER CHECK(retry_budget_snapshot IS NULL OR retry_budget_snapshot >= 0),
  created_by_transition_key TEXT NOT NULL,
  fired_by_transition_key TEXT,
  fired_event_id TEXT,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  fired_at TEXT,
  PRIMARY KEY(run_id, timer_id),
  UNIQUE(run_id, created_by_transition_key),
  UNIQUE(run_id, fired_by_transition_key),
  FOREIGN KEY(run_id, activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, activation_id, expected_attempt_no, expected_lease_epoch, expected_fencing_token)
  REFERENCES node_attempts(run_id,
activation_id,
attempt_no,
lease_epoch,
fencing_token) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, fired_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  CHECK((expected_attempt_no IS NULL
AND expected_lease_epoch IS NULL
AND expected_fencing_token IS NULL)
OR(expected_attempt_no IS NOT NULL
AND expected_lease_epoch IS NOT NULL
AND expected_lease_epoch >= expected_attempt_no
AND expected_fencing_token IS NOT NULL)),
  CHECK((timer_kind = 'run_deadline' AND activation_id IS NULL)
OR(timer_kind <> 'run_deadline' AND activation_id IS NOT NULL)),
  CHECK(timer_kind NOT IN('lease', 'retry')
OR(expected_attempt_no IS NOT NULL AND retry_budget_snapshot IS NOT NULL)),
  CHECK((timer_state = 'scheduled'
AND fired_by_transition_key IS NULL
AND fired_event_id IS NULL
AND fired_at IS NULL)
OR(timer_state = 'fired'
AND fired_by_transition_key IS NOT NULL
AND fired_event_id IS NOT NULL
AND fired_at IS NOT NULL
AND fired_at >= deadline_at)
OR(timer_state = 'cancelled' AND fired_at IS NOT NULL))
);
CREATE INDEX idx_timers_due
ON timers(
  deadline_at,
  run_id,
  timer_id
) WHERE timer_state = 'scheduled';
CREATE UNIQUE INDEX uq_attempt_lease_timer
ON timers(
  run_id,
  activation_id,
  expected_attempt_no,
  expected_lease_epoch
)
WHERE timer_kind = 'lease';
CREATE TABLE scheduler_wait_registrations(
  run_id TEXT NOT NULL,
  wait_id TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  occurrence_key TEXT NOT NULL CHECK(json_valid(occurrence_key)),
  signal_name TEXT,
  signal_id TEXT,
  timer_id TEXT,
  due_at_ms INTEGER CHECK(due_at_ms IS NULL OR due_at_ms >= 0),
  payload_type TEXT CHECK(payload_type IS NULL OR json_valid(payload_type)),
  winner_kind TEXT CHECK(winner_kind IS NULL OR winner_kind IN('signal', 'timer', 'cancelled')),
  winner_signal_id TEXT,
  winner_timer_id TEXT,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  resolved_at TEXT,
  PRIMARY KEY(run_id, wait_id),
  UNIQUE(run_id, activation_id),
  FOREIGN KEY(run_id, activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, timer_id)
  REFERENCES timers(run_id, timer_id) ON DELETE RESTRICT
  DEFERRABLE INITIALLY DEFERRED,
  CHECK((signal_name IS NULL) =(signal_id IS NULL)),
  CHECK((timer_id IS NULL) =(due_at_ms IS NULL)),
  CHECK(signal_id IS NOT NULL OR timer_id IS NOT NULL),
  CHECK((winner_kind IS NULL AND winner_signal_id IS NULL
AND winner_timer_id IS NULL AND resolved_at IS NULL)
OR(winner_kind = 'signal' AND winner_signal_id = signal_id
AND winner_timer_id IS NULL AND resolved_at IS NOT NULL)
OR(winner_kind = 'timer' AND winner_timer_id = timer_id
AND winner_signal_id IS NULL AND resolved_at IS NOT NULL)
OR(winner_kind = 'cancelled' AND winner_signal_id IS NULL
AND winner_timer_id IS NULL AND resolved_at IS NOT NULL))
);
CREATE TABLE scheduler_subflow_invocations(
  run_id TEXT NOT NULL,
  child_run_id TEXT NOT NULL,
  parent_activation_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  occurrence_key TEXT NOT NULL CHECK(json_valid(occurrence_key)),
  invocation_scope_instance_id TEXT NOT NULL,
  parent_scope_instance_id TEXT NOT NULL,
  static_scope_id TEXT NOT NULL,
  definition_revision_id TEXT NOT NULL,
  deployment_revision_id TEXT NOT NULL,
  plan_hash TEXT NOT NULL CHECK(length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
  binding_hash TEXT NOT NULL CHECK(length(binding_hash) = 71 AND binding_hash LIKE 'sha256:%'),
  interface_version TEXT NOT NULL,
  output_contracts TEXT NOT NULL CHECK(json_valid(output_contracts)),
  invocation_state TEXT NOT NULL CHECK(invocation_state IN('started', 'cancellation_requested', 'completed')),
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  completed_at TEXT,
  PRIMARY KEY(run_id, child_run_id),
  UNIQUE(run_id, parent_activation_id),
  FOREIGN KEY(run_id, parent_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, invocation_scope_instance_id)
  REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, parent_scope_instance_id)
  REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
  FOREIGN KEY(child_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  CHECK(invocation_scope_instance_id <> parent_scope_instance_id),
  CHECK((invocation_state IN('started', 'cancellation_requested') AND completed_at IS NULL)
OR(invocation_state = 'completed' AND completed_at IS NOT NULL))
);
CREATE TABLE fork_groups(
  run_id TEXT NOT NULL,
  fork_group_id TEXT NOT NULL,
  fork_activation_id TEXT NOT NULL,
  parent_scope_instance_id TEXT NOT NULL,
  join_activation_id TEXT,
  join_mode TEXT CHECK(join_mode IS NULL OR join_mode IN('all_success', 'all_settled')),
  failure_leg_id TEXT,
  failure_settlement_class TEXT CHECK(failure_settlement_class IS NULL OR failure_settlement_class IN('safe_failure', 'infrastructure_failure', 'panic', 'cancelled', 'timed_out')),
  expected_legs INTEGER NOT NULL CHECK(expected_legs > 0),
  group_state TEXT NOT NULL CHECK(group_state IN('open', 'settling', 'settled', 'cancelled')),
  admitted_legs INTEGER NOT NULL DEFAULT 0 CHECK(admitted_legs >= 0),
  settled_legs INTEGER NOT NULL DEFAULT 0 CHECK(settled_legs >= 0),
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  settled_at TEXT,
  PRIMARY KEY(run_id, fork_group_id),
  FOREIGN KEY(run_id, fork_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, parent_scope_instance_id)
  REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, join_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  CHECK((failure_leg_id IS NULL) =(failure_settlement_class IS NULL)),
  CHECK(settled_legs <= admitted_legs AND admitted_legs <= expected_legs),
  CHECK((group_state IN('open', 'settling') AND settled_at IS NULL)
OR(group_state IN('settled', 'cancelled')
AND settled_legs = admitted_legs
AND settled_at IS NOT NULL))
);
CREATE TABLE control_tokens(
  run_id TEXT NOT NULL,
  token_id TEXT NOT NULL,
  current_scope_instance_id TEXT NOT NULL,
  current_port_id TEXT NOT NULL,
  source_activation_id TEXT NOT NULL,
  source_port_id TEXT NOT NULL,
  emission_slot TEXT NOT NULL,
  emitted_by_transition_key TEXT NOT NULL,
  provenance_frames TEXT NOT NULL CHECK(json_valid(provenance_frames)),
  branch_activation_id TEXT,
  selected_branch_port_id TEXT,
  fork_group_id TEXT,
  fork_leg_id TEXT,
  token_state TEXT NOT NULL CHECK(token_state IN('available', 'consumed', 'revoked')),
  consumed_by_activation_id TEXT,
  consumed_by_transition_key TEXT,
  consumed_at TEXT,
  revoked_by_transition_key TEXT,
  revoked_at TEXT,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, token_id),
  UNIQUE(run_id, source_activation_id, emission_slot),
  UNIQUE(run_id, emitted_by_transition_key, emission_slot),
  UNIQUE(run_id, consumed_by_transition_key),
  FOREIGN KEY(run_id, current_scope_instance_id)
  REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, source_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, emitted_by_transition_key)
  REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, branch_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, fork_group_id)
  REFERENCES fork_groups(run_id, fork_group_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, consumed_by_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, fork_group_id, fork_leg_id)
  REFERENCES fork_legs(run_id, fork_group_id, leg_id)
  ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
  CHECK((branch_activation_id IS NULL AND selected_branch_port_id IS NULL)
OR(branch_activation_id IS NOT NULL AND selected_branch_port_id IS NOT NULL)),
  CHECK((fork_group_id IS NULL AND fork_leg_id IS NULL)
OR(fork_group_id IS NOT NULL AND fork_leg_id IS NOT NULL)),
  CHECK((token_state = 'available'
AND consumed_by_activation_id IS NULL
AND consumed_by_transition_key IS NULL
AND consumed_at IS NULL
AND revoked_by_transition_key IS NULL
AND revoked_at IS NULL)
OR(token_state = 'consumed'
AND consumed_by_activation_id IS NOT NULL
AND consumed_by_transition_key IS NOT NULL
AND consumed_at IS NOT NULL
AND revoked_by_transition_key IS NULL
AND revoked_at IS NULL)
OR(token_state = 'revoked'
AND consumed_by_activation_id IS NULL
AND consumed_by_transition_key IS NULL
AND consumed_at IS NULL
AND revoked_by_transition_key IS NOT NULL
AND revoked_at IS NOT NULL))
);
CREATE TABLE fork_legs(
  run_id TEXT NOT NULL,
  fork_group_id TEXT NOT NULL,
  leg_id TEXT NOT NULL,
  declaration_index INTEGER NOT NULL CHECK(declaration_index >= 0),
  scope_instance_id TEXT NOT NULL,
  child_activation_id TEXT NOT NULL,
  token_id TEXT NOT NULL,
  is_required INTEGER NOT NULL CHECK(is_required IN(0, 1)),
  leg_state TEXT NOT NULL CHECK(leg_state IN('admitted', 'settled', 'cancelled')),
  settlement_class TEXT CHECK(settlement_class IS NULL OR settlement_class IN('succeeded', 'safe_failure', 'infrastructure_failure', 'panic', 'cancelled', 'timed_out')),
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  settled_at TEXT,
  PRIMARY KEY(run_id, fork_group_id, leg_id),
  UNIQUE(run_id, fork_group_id, declaration_index),
  UNIQUE(run_id, fork_group_id, token_id),
  FOREIGN KEY(run_id, fork_group_id)
  REFERENCES fork_groups(run_id, fork_group_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, scope_instance_id)
  REFERENCES scope_instances(run_id, scope_instance_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, child_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, token_id)
  REFERENCES control_tokens(run_id, token_id) ON DELETE RESTRICT,
  CHECK((leg_state = 'admitted' AND settlement_class IS NULL AND settled_at IS NULL)
OR(leg_state IN('settled', 'cancelled')
AND settlement_class IS NOT NULL
AND settled_at IS NOT NULL))
);
CREATE TABLE join_arrivals(
  run_id TEXT NOT NULL,
  join_activation_id TEXT NOT NULL,
  fork_group_id TEXT NOT NULL,
  leg_id TEXT NOT NULL,
  token_id TEXT NOT NULL,
  arrival_transition_key TEXT NOT NULL,
  arrival_event_id TEXT NOT NULL,
  settlement_class TEXT NOT NULL CHECK(settlement_class IN('succeeded', 'safe_failure', 'infrastructure_failure', 'panic', 'cancelled', 'timed_out')),
  value_payload_id TEXT,
  value_artifact_id TEXT,
  value_hash TEXT CHECK(value_hash IS NULL OR(length(value_hash) = 71 AND value_hash LIKE 'sha256:%')),
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  arrived_at TEXT NOT NULL,
  PRIMARY KEY(run_id, join_activation_id, fork_group_id, leg_id),
  UNIQUE(run_id, fork_group_id, leg_id),
  UNIQUE(run_id, arrival_transition_key),
  UNIQUE(run_id, join_activation_id, token_id),
  FOREIGN KEY(run_id, join_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, fork_group_id, leg_id)
  REFERENCES fork_legs(run_id, fork_group_id, leg_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, token_id)
  REFERENCES control_tokens(run_id, token_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, arrival_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, value_payload_id)
  REFERENCES payloads(run_id, payload_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, value_artifact_id)
  REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
  CHECK((value_hash IS NULL AND value_payload_id IS NULL AND value_artifact_id IS NULL)
OR(value_hash IS NOT NULL
AND((value_payload_id IS NOT NULL) +(value_artifact_id IS NOT NULL)) = 1))
);
CREATE TABLE run_reuse_candidates(
  run_id TEXT NOT NULL,
  candidate_id TEXT NOT NULL,
  target_scope_instance_id TEXT NOT NULL,
  target_node_id TEXT NOT NULL,
  stable_activation_key TEXT NOT NULL,
  source_run_id TEXT NOT NULL,
  source_activation_id TEXT NOT NULL,
  source_control_provenance TEXT NOT NULL CHECK(json_valid(source_control_provenance)),
  definition_revision_id TEXT NOT NULL,
  deployment_revision_id TEXT NOT NULL,
  plan_hash TEXT NOT NULL CHECK(length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
  binding_hash TEXT NOT NULL CHECK(length(binding_hash) = 71 AND binding_hash LIKE 'sha256:%'),
  node_config_hash TEXT NOT NULL CHECK(length(node_config_hash) = 71 AND node_config_hash LIKE 'sha256:%'),
  descriptor_hash TEXT NOT NULL CHECK(length(descriptor_hash) = 71 AND descriptor_hash LIKE 'sha256:%'),
  input_value_hash TEXT NOT NULL CHECK(length(input_value_hash) = 71 AND input_value_hash LIKE 'sha256:%'),
  output_value_hash TEXT NOT NULL CHECK(length(output_value_hash) = 71 AND output_value_hash LIKE 'sha256:%'),
  output_schema_hash TEXT NOT NULL CHECK(length(output_schema_hash) = 71 AND output_schema_hash LIKE 'sha256:%'),
  effect_policy_hash TEXT NOT NULL CHECK(length(effect_policy_hash) = 71 AND effect_policy_hash LIKE 'sha256:%'),
  inherited_effect_id TEXT NOT NULL,
  data_dependencies_hash TEXT NOT NULL CHECK(length(data_dependencies_hash) = 71 AND data_dependencies_hash LIKE 'sha256:%'),
  created_by_transition_key TEXT NOT NULL,
  candidate_state TEXT NOT NULL CHECK(candidate_state IN('candidate', 'materialized', 'rejected')),
  materialized_activation_id TEXT,
  decision_transition_key TEXT,
  rejection_reason TEXT,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  decided_at TEXT,
  PRIMARY KEY(run_id, candidate_id),
  UNIQUE(run_id, target_scope_instance_id, target_node_id, stable_activation_key),
  UNIQUE(run_id, created_by_transition_key),
  UNIQUE(run_id, decision_transition_key),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(source_run_id, source_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, created_by_transition_key)
  REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, materialized_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT
  DEFERRABLE INITIALLY DEFERRED,
  CHECK(source_run_id <> run_id),
  CHECK((candidate_state = 'candidate'
AND materialized_activation_id IS NULL
AND decision_transition_key IS NULL
AND rejection_reason IS NULL
AND decided_at IS NULL)
OR(candidate_state = 'materialized'
AND materialized_activation_id IS NOT NULL
AND decision_transition_key IS NOT NULL
AND rejection_reason IS NULL
AND decided_at IS NOT NULL)
OR(candidate_state = 'rejected'
AND materialized_activation_id IS NULL
AND decision_transition_key IS NOT NULL
AND rejection_reason IS NOT NULL
AND decided_at IS NOT NULL))
);
CREATE INDEX idx_reuse_candidates_pending
ON run_reuse_candidates(
  run_id,
  target_scope_instance_id,
  target_node_id,
  candidate_id
)
WHERE candidate_state = 'candidate';
CREATE TABLE control_transition_results(
  run_id TEXT NOT NULL,
  transition_key TEXT NOT NULL,
  intent_hash TEXT NOT NULL CHECK(length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
  primary_event_id TEXT NOT NULL,
  result_json TEXT NOT NULL CHECK(json_valid(result_json)),
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, transition_key),
  UNIQUE(run_id, primary_event_id),
  FOREIGN KEY(run_id, primary_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT
);
CREATE TABLE run_recovery_lineage(
  run_id TEXT NOT NULL PRIMARY KEY,
  source_run_id TEXT NOT NULL,
  lineage_kind TEXT NOT NULL CHECK(lineage_kind IN('redrive', 'fork', 'migrate', 'continue_as_new')),
  source_generation INTEGER NOT NULL CHECK(source_generation >= 1),
  target_generation INTEGER NOT NULL CHECK(target_generation >= 1),
  source_definition_id TEXT NOT NULL,
  source_definition_revision_id TEXT NOT NULL,
  source_deployment_revision_id TEXT NOT NULL,
  source_plan_hash TEXT NOT NULL CHECK(length(source_plan_hash) = 71 AND source_plan_hash LIKE 'sha256:%'),
  source_binding_hash TEXT NOT NULL CHECK(length(source_binding_hash) = 71 AND source_binding_hash LIKE 'sha256:%'),
  target_definition_id TEXT NOT NULL,
  target_definition_revision_id TEXT NOT NULL,
  target_deployment_revision_id TEXT NOT NULL,
  target_plan_hash TEXT NOT NULL CHECK(length(target_plan_hash) = 71 AND target_plan_hash LIKE 'sha256:%'),
  target_binding_hash TEXT NOT NULL CHECK(length(target_binding_hash) = 71 AND target_binding_hash LIKE 'sha256:%'),
  source_checkpoint_hash TEXT CHECK(source_checkpoint_hash IS NULL
OR(length(source_checkpoint_hash) = 71 AND source_checkpoint_hash LIKE 'sha256:%')),
  created_by_transition_key TEXT NOT NULL,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  UNIQUE(source_run_id, run_id, lineage_kind),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(source_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(source_run_id, source_checkpoint_hash)
  REFERENCES scheduler_checkpoints(run_id, content_hash) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, created_by_transition_key)
  REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT,
  FOREIGN KEY(source_definition_id, source_definition_revision_id,
source_deployment_revision_id, source_plan_hash, source_binding_hash) REFERENCES deployment_revisions(definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) ON DELETE RESTRICT,
  FOREIGN KEY(target_definition_id, target_definition_revision_id,
target_deployment_revision_id, target_plan_hash, target_binding_hash) REFERENCES deployment_revisions(definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) ON DELETE RESTRICT,
  CHECK(source_run_id <> run_id),
  CHECK((lineage_kind = 'continue_as_new'
AND target_generation = source_generation + 1
AND source_checkpoint_hash IS NULL)
OR(lineage_kind IN('redrive', 'migrate')
AND target_generation = 1
AND source_checkpoint_hash IS NULL)
OR(lineage_kind = 'fork'
AND target_generation = 1
AND source_checkpoint_hash IS NOT NULL))
);
CREATE TABLE run_migration_intents(
  run_id TEXT NOT NULL PRIMARY KEY,
  target_run_id TEXT NOT NULL UNIQUE,
  target_definition_id TEXT NOT NULL,
  target_definition_revision_id TEXT NOT NULL,
  target_deployment_revision_id TEXT NOT NULL,
  target_plan_hash TEXT NOT NULL CHECK(length(target_plan_hash) = 71 AND target_plan_hash LIKE 'sha256:%'),
  target_binding_hash TEXT NOT NULL CHECK(length(target_binding_hash) = 71 AND target_binding_hash LIKE 'sha256:%'),
  target_input TEXT NOT NULL CHECK(json_valid(target_input)),
  target_input_hash TEXT NOT NULL CHECK(length(target_input_hash) = 71 AND target_input_hash LIKE 'sha256:%'),
  mapping_contracts TEXT NOT NULL CHECK(json_valid(mapping_contracts)),
  mapping_hash TEXT NOT NULL CHECK(length(mapping_hash) = 71 AND mapping_hash LIKE 'sha256:%'),
  reuse_candidates TEXT NOT NULL CHECK(json_valid(reuse_candidates)),
  intent_transition_key TEXT NOT NULL UNIQUE,
  intent_event_id TEXT NOT NULL,
  intent_state TEXT NOT NULL CHECK(intent_state IN('pending', 'completed')),
  final_transition_key TEXT UNIQUE,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  completed_at TEXT,
  target_timeout_ms INTEGER NOT NULL DEFAULT 300000
  CHECK(target_timeout_ms > 0),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, intent_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  FOREIGN KEY(target_definition_id, target_definition_revision_id,
target_deployment_revision_id, target_plan_hash, target_binding_hash) REFERENCES deployment_revisions(definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) ON DELETE RESTRICT,
  CHECK(run_id <> target_run_id),
  CHECK((intent_state = 'pending' AND final_transition_key IS NULL AND completed_at IS NULL)
OR(intent_state = 'completed'
AND final_transition_key IS NOT NULL AND completed_at IS NOT NULL))
);
CREATE TABLE recovery_revision_roots(
  run_id TEXT NOT NULL,
  root_role TEXT NOT NULL CHECK(root_role IN('source', 'target')),
  source_run_id TEXT NOT NULL,
  definition_id TEXT NOT NULL,
  definition_revision_id TEXT NOT NULL,
  deployment_revision_id TEXT NOT NULL,
  plan_hash TEXT NOT NULL CHECK(length(plan_hash) = 71 AND plan_hash LIKE 'sha256:%'),
  binding_hash TEXT NOT NULL CHECK(length(binding_hash) = 71 AND binding_hash LIKE 'sha256:%'),
  created_by_transition_key TEXT NOT NULL,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, root_role),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(source_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) REFERENCES deployment_revisions(definition_id, definition_revision_id, deployment_revision_id, plan_hash, binding_hash) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, created_by_transition_key)
  REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT
);
CREATE TABLE recovery_artifact_roots(
  run_id TEXT NOT NULL,
  source_run_id TEXT NOT NULL,
  artifact_run_id TEXT NOT NULL,
  artifact_id TEXT NOT NULL,
  source_activation_id TEXT NOT NULL,
  created_by_transition_key TEXT NOT NULL,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, artifact_run_id, artifact_id),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(source_run_id, source_activation_id)
  REFERENCES node_activations(run_id, activation_id) ON DELETE RESTRICT,
  FOREIGN KEY(artifact_run_id, artifact_id)
  REFERENCES artifacts(run_id, artifact_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, created_by_transition_key)
  REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT
);
CREATE TABLE recovery_effect_roots(
  run_id TEXT NOT NULL,
  source_run_id TEXT NOT NULL,
  effect_run_id TEXT NOT NULL,
  source_activation_id TEXT NOT NULL,
  effect_id TEXT NOT NULL,
  created_by_transition_key TEXT NOT NULL,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, effect_run_id, source_activation_id, effect_id),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(source_run_id, source_activation_id, effect_id)
  REFERENCES node_activations(run_id, activation_id, effect_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, created_by_transition_key)
  REFERENCES execution_events(run_id, transition_key) ON DELETE RESTRICT
);
CREATE TABLE recovery_transition_results(
  authority_run_id TEXT NOT NULL,
  transition_key TEXT NOT NULL,
  intent_hash TEXT NOT NULL CHECK(length(intent_hash) = 71 AND intent_hash LIKE 'sha256:%'),
  primary_event_run_id TEXT NOT NULL,
  primary_event_id TEXT NOT NULL,
  result_json TEXT NOT NULL CHECK(json_valid(result_json)),
  created_at TEXT NOT NULL,
  PRIMARY KEY(authority_run_id, transition_key),
  UNIQUE(primary_event_run_id, primary_event_id),
  FOREIGN KEY(authority_run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(primary_event_run_id, primary_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT
);
CREATE INDEX idx_run_deadline_due
ON workflow_runs(
  deadline_at,
  run_id
)
WHERE deadline_at IS NOT NULL

    AND lifecycle IN('created', 'active', 'waiting', 'completing');
CREATE TABLE graph_view_documents(
  definition_id TEXT NOT NULL,
  definition_revision_id TEXT NOT NULL,
  graph_document_id TEXT NOT NULL CHECK(graph_document_id <> ''),
  view_version INTEGER NOT NULL CHECK(view_version >= 1),
  view_document TEXT NOT NULL CHECK(json_valid(view_document)),
  updated_at TEXT NOT NULL,
  PRIMARY KEY(definition_id, definition_revision_id),
  FOREIGN KEY(definition_id, definition_revision_id)
  REFERENCES workflow_definition_revisions(definition_id, definition_revision_id)
  ON DELETE RESTRICT
);
CREATE INDEX idx_graph_views_document
ON graph_view_documents(
  graph_document_id
);
CREATE TRIGGER public_event_outbox_delete_forbidden
BEFORE DELETE ON public_event_outbox
WHEN NOT (
    OLD.publish_state = 'published'
    AND OLD.is_terminal = 0
    AND OLD.retain_until IS NOT NULL
    AND julianday(OLD.retain_until) <= julianday('now')
)
BEGIN
    SELECT RAISE(ABORT, 'public event delivery authority is not eligible for retention pruning');
END;
CREATE TRIGGER control_transition_result_rewrite_forbidden
BEFORE UPDATE ON control_transition_results
BEGIN
    SELECT RAISE(ABORT, 'control transition receipt is immutable');
END;
CREATE TRIGGER control_transition_result_delete_forbidden
BEFORE DELETE ON control_transition_results
BEGIN
    SELECT RAISE(ABORT, 'control transition receipt cannot be deleted');
END;
CREATE TRIGGER recovery_transition_result_rewrite_forbidden
BEFORE UPDATE ON recovery_transition_results
BEGIN
    SELECT RAISE(ABORT, 'recovery transition receipt is immutable');
END;
CREATE TRIGGER recovery_transition_result_delete_forbidden
BEFORE DELETE ON recovery_transition_results
BEGIN
    SELECT RAISE(ABORT, 'recovery transition receipt cannot be deleted');
END;
CREATE TRIGGER artifact_retention_release_delete_forbidden
BEFORE DELETE ON artifact_retention_releases
BEGIN
    SELECT RAISE(ABORT, 'artifact retention release cannot be deleted');
END;
CREATE TABLE human_work_items(
  work_item_id TEXT NOT NULL PRIMARY KEY,
  run_id TEXT NOT NULL,
  wait_id TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  signal_id TEXT NOT NULL,
  signal_name TEXT NOT NULL,
  request_value TEXT NOT NULL CHECK(json_valid(request_value)),
  response_type TEXT NOT NULL CHECK(json_valid(response_type)),
  assignees TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(assignees)),
candidate_groups TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(candidate_groups)),
claim_lease_ms INTEGER NOT NULL CHECK(claim_lease_ms BETWEEN 1 AND 2592000000),
work_state TEXT NOT NULL CHECK(work_state IN('open','claimed','completed','cancelled','expired')),
claim_fence INTEGER NOT NULL DEFAULT 0 CHECK(claim_fence >= 0),
claimed_by TEXT,
claim_request_id TEXT,
claim_expires_at TEXT,
completion_request_id TEXT,
completion_payload TEXT CHECK(completion_payload IS NULL OR json_valid(completion_payload)),
completion_payload_hash TEXT,
projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
completed_at TEXT,
closed_at TEXT,
UNIQUE(run_id, wait_id),
UNIQUE(run_id, activation_id),
FOREIGN KEY(run_id, wait_id)
REFERENCES scheduler_wait_registrations(run_id, wait_id) ON DELETE RESTRICT,
CHECK((work_state = 'open' AND claimed_by IS NULL AND claim_expires_at IS NULL)
OR(work_state = 'claimed' AND claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL)
OR work_state IN('completed','cancelled','expired')),
CHECK((completion_request_id IS NULL AND completion_payload IS NULL
AND completion_payload_hash IS NULL)
OR(completion_request_id IS NOT NULL AND completion_payload IS NOT NULL
AND completion_payload_hash IS NOT NULL))
);
CREATE INDEX idx_human_work_items_queue
ON human_work_items(
  work_state,
  claim_expires_at,
  created_at,
  work_item_id
);
CREATE INDEX idx_human_work_items_run
ON human_work_items(
  run_id,
  work_state,
  work_item_id
);
CREATE TRIGGER close_human_work_items_on_run_termination
AFTER UPDATE OF lifecycle, termination_intent_reason ON workflow_runs
WHEN NEW.lifecycle = 'terminating' AND OLD.lifecycle IS NOT NEW.lifecycle
BEGIN
    UPDATE human_work_items
    SET work_state = 'completed', completed_at = CURRENT_TIMESTAMP,
        claim_expires_at = NULL, projection_version = projection_version + 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE run_id = NEW.run_id AND work_state = 'claimed'
      AND completion_request_id IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM signals_inbox s
          WHERE s.run_id=human_work_items.run_id
            AND s.signal_id=human_work_items.signal_id
            AND s.signal_state='consumed'
      );
    UPDATE human_work_items
    SET work_state = CASE WHEN NEW.termination_intent_reason = 'timed_out'
                          THEN 'expired' ELSE 'cancelled' END,
        completion_request_id = NULL, completion_payload = NULL,
        completion_payload_hash = NULL, claim_expires_at = NULL,
        projection_version = projection_version + 1,
        updated_at = CURRENT_TIMESTAMP, closed_at = CURRENT_TIMESTAMP
    WHERE run_id = NEW.run_id AND work_state IN ('open','claimed');
END;
CREATE TRIGGER close_human_work_items_on_run_terminal
AFTER UPDATE OF lifecycle ON workflow_runs
WHEN NEW.lifecycle IN ('succeeded','failed','cancelled','interrupted','timed_out')
     AND OLD.lifecycle IS NOT NEW.lifecycle
BEGIN
    UPDATE human_work_items
    SET work_state = 'completed', completed_at = CURRENT_TIMESTAMP,
        claim_expires_at = NULL, projection_version = projection_version + 1,
        updated_at = CURRENT_TIMESTAMP
    WHERE run_id = NEW.run_id AND work_state = 'claimed'
      AND completion_request_id IS NOT NULL
      AND EXISTS (
          SELECT 1 FROM signals_inbox s
          WHERE s.run_id=human_work_items.run_id
            AND s.signal_id=human_work_items.signal_id
            AND s.signal_state='consumed'
      );
    UPDATE human_work_items
    SET work_state = CASE WHEN NEW.lifecycle = 'timed_out'
                          THEN 'expired' ELSE 'cancelled' END,
        completion_request_id = NULL, completion_payload = NULL,
        completion_payload_hash = NULL, claim_expires_at = NULL,
        projection_version = projection_version + 1,
        updated_at = CURRENT_TIMESTAMP, closed_at = CURRENT_TIMESTAMP
    WHERE run_id = NEW.run_id AND work_state IN ('open','claimed');
END;
CREATE TABLE workflow_definition_public_metadata(
  definition_id TEXT NOT NULL,
  definition_revision_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  public_description TEXT NOT NULL,
  PRIMARY KEY(definition_id, definition_revision_id),
  FOREIGN KEY(definition_id, definition_revision_id)
  REFERENCES workflow_definition_revisions(definition_id, definition_revision_id)
  ON DELETE RESTRICT,
  CHECK(display_name <> '' AND length(display_name) <= 256),
  CHECK(length(public_description) <= 4096)
);
CREATE TRIGGER trg_definition_public_metadata_update_immutable
    BEFORE UPDATE ON workflow_definition_public_metadata
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'published workflow definition public metadata is immutable');
END;
CREATE TRIGGER trg_definition_public_metadata_delete_immutable
    BEFORE DELETE ON workflow_definition_public_metadata
    FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'published workflow definition public metadata is immutable');
END;
CREATE TABLE agent_publication_heads(
  agent_id TEXT NOT NULL PRIMARY KEY,
  definition_id TEXT NOT NULL UNIQUE,
  definition_revision_id TEXT NOT NULL,
  deployment_revision_id TEXT NOT NULL,
  publication_origin TEXT NOT NULL CHECK(publication_origin IN('built_in','graph')),
  updated_at TEXT NOT NULL,
  FOREIGN KEY(definition_id, deployment_revision_id)
  REFERENCES deployment_revisions(definition_id, deployment_revision_id)
  ON DELETE RESTRICT
);
CREATE TRIGGER trg_publication_head_agent_matches_definition_insert
    BEFORE INSERT ON agent_publication_heads
    FOR EACH ROW
    WHEN NOT EXISTS (
        SELECT 1 FROM workflow_definitions d
        JOIN deployment_revisions x
          ON x.definition_id = d.definition_id
        WHERE d.definition_id = NEW.definition_id
          AND d.agent_id = NEW.agent_id
          AND x.definition_revision_id = NEW.definition_revision_id
          AND x.deployment_revision_id = NEW.deployment_revision_id
    )
BEGIN
    SELECT RAISE(ABORT, 'publication head agent does not own definition');
END;
CREATE TRIGGER trg_publication_head_agent_matches_definition_update
    BEFORE UPDATE ON agent_publication_heads
    FOR EACH ROW
    WHEN NOT EXISTS (
        SELECT 1 FROM workflow_definitions d
        JOIN deployment_revisions x
          ON x.definition_id = d.definition_id
        WHERE d.definition_id = NEW.definition_id
          AND d.agent_id = NEW.agent_id
          AND x.definition_revision_id = NEW.definition_revision_id
          AND x.deployment_revision_id = NEW.deployment_revision_id
    )
BEGIN
    SELECT RAISE(ABORT, 'publication head agent does not own definition');
END;
CREATE TRIGGER execution_event_schema_version_insert_supported
BEFORE INSERT ON execution_events
FOR EACH ROW
WHEN COALESCE((
    NEW.schema_version = 2
    AND NEW.projection_ledger_batch IS NULL
    AND NEW.kind IN (
        'run.created', 'run.lifecycle_changed', 'run.admission_changed',
        'run.termination_claimed', 'scope.created', 'scope.draining', 'scope.settled',
        'activation.created', 'activation.ready', 'activation.leased',
        'activation.running', 'activation.retry_wait', 'activation.waiting',
        'activation.terminating', 'activation.succeeded', 'activation.failed',
        'activation.cancelled', 'activation.timed_out', 'attempt.created',
        'attempt.leased', 'attempt.running', 'attempt.succeeded', 'attempt.failed',
        'attempt.timed_out', 'attempt.abandoned', 'attempt.cancelled',
        'effect.evidence_recorded', 'control_token.emitted', 'control_token.consumed',
        'control_token.revoked', 'fork.created', 'join.arrived', 'join.completed',
        'signal.received', 'signal.late', 'timer.scheduled', 'timer.fired',
        'timer.late', 'projection.mutated'
    )
), 0) = 0
BEGIN
    SELECT RAISE(ABORT, 'invalid execution event insert authority');
END;
CREATE TRIGGER execution_event_schema_version_update_supported
BEFORE UPDATE OF schema_version ON execution_events
FOR EACH ROW
WHEN NEW.schema_version <> 2
BEGIN
    SELECT RAISE(ABORT, 'unsupported execution event schema version');
END;
CREATE TRIGGER execution_event_kind_update_supported
BEFORE UPDATE OF kind ON execution_events
FOR EACH ROW
WHEN NEW.kind NOT IN (
    'run.created', 'run.lifecycle_changed', 'run.admission_changed',
    'run.termination_claimed', 'scope.created', 'scope.draining', 'scope.settled',
    'activation.created', 'activation.ready', 'activation.leased',
    'activation.running', 'activation.retry_wait', 'activation.waiting',
    'activation.terminating', 'activation.succeeded', 'activation.failed',
    'activation.cancelled', 'activation.timed_out', 'attempt.created',
    'attempt.leased', 'attempt.running', 'attempt.succeeded', 'attempt.failed',
    'attempt.timed_out', 'attempt.abandoned', 'attempt.cancelled',
    'effect.evidence_recorded', 'control_token.emitted', 'control_token.consumed',
    'control_token.revoked', 'fork.created', 'join.arrived', 'join.completed',
    'signal.received', 'signal.late', 'timer.scheduled', 'timer.fired',
    'timer.late', 'projection.mutated'
)
BEGIN
    SELECT RAISE(ABORT, 'unsupported execution event kind');
END;
CREATE TRIGGER execution_event_projection_ledger_immutable
BEFORE UPDATE ON execution_events
FOR EACH ROW
WHEN COALESCE((
    OLD.projection_ledger_batch IS NULL
    AND NEW.projection_ledger_batch IS NOT NULL
    AND NEW.run_id IS OLD.run_id
    AND NEW.seq IS OLD.seq
    AND NEW.event_id IS OLD.event_id
    AND NEW.schema_version IS OLD.schema_version
    AND NEW.kind IS OLD.kind
    AND NEW.transition_key IS OLD.transition_key
    AND NEW.intent_hash IS OLD.intent_hash
    AND NEW.node_id IS OLD.node_id
    AND NEW.scope_instance_id IS OLD.scope_instance_id
    AND NEW.activation_id IS OLD.activation_id
    AND NEW.attempt_no IS OLD.attempt_no
    AND NEW.causation_event_id IS OLD.causation_event_id
    AND NEW.projection_version_after IS OLD.projection_version_after
    AND NEW.safe_payload IS OLD.safe_payload
    AND NEW.occurred_at IS OLD.occurred_at
    AND json_valid(NEW.projection_ledger_batch)
    AND json_type(NEW.projection_ledger_batch) = 'object'
    AND json_extract(NEW.projection_ledger_batch, '$.schema_version') = 1
    AND json_type(NEW.projection_ledger_batch, '$.subject_count') = 'integer'
    AND json_extract(NEW.projection_ledger_batch, '$.subject_count') >= 0
    AND json_type(NEW.projection_ledger_batch, '$.manifest_hash') = 'text'
    AND length(json_extract(NEW.projection_ledger_batch, '$.manifest_hash')) = 71
    AND json_extract(NEW.projection_ledger_batch, '$.manifest_hash') LIKE 'sha256:%'
    AND json_type(NEW.projection_ledger_batch, '$.subjects') = 'array'
    AND (SELECT COUNT(*) FROM json_each(NEW.projection_ledger_batch)) = 4
    AND NOT EXISTS (
        SELECT 1
        FROM json_each(NEW.projection_ledger_batch)
        WHERE key NOT IN ('schema_version', 'subject_count', 'manifest_hash', 'subjects')
    )
), 0) = 0
BEGIN
    SELECT RAISE(ABORT, 'execution event authority is immutable');
END;
CREATE TRIGGER execution_event_delete_forbidden
BEFORE DELETE ON execution_events
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'execution event authority cannot be deleted');
END;
CREATE TRIGGER public_event_outbox_insert_contract
BEFORE INSERT ON public_event_outbox
FOR EACH ROW
WHEN NOT (
    (
        (NEW.is_terminal = 0 AND NEW.event_kind IN (
            'run.created','run.started','operation.started',
            'operation.completed','operation.failed'
        ))
        OR (NEW.is_terminal = 1 AND NEW.event_kind IN (
            'run.completed','run.failed','run.cancelled','run.interrupted'
        ))
    )
    AND (
        (NEW.publish_state = 'pending'
            AND NEW.claimed_by IS NULL AND NEW.claim_token IS NULL
            AND NEW.claim_expires_at IS NULL AND NEW.published_at IS NULL
            AND NEW.published_by IS NULL AND NEW.published_claim_token IS NULL
            AND NEW.notified_at IS NULL AND NEW.retain_until IS NULL)
        OR (NEW.publish_state = 'claimed'
            AND NEW.claimed_by IS NOT NULL AND NEW.claim_token IS NOT NULL
            AND NEW.claim_expires_at IS NOT NULL AND NEW.published_at IS NULL
            AND NEW.published_by IS NULL AND NEW.published_claim_token IS NULL
            AND NEW.notified_at IS NULL AND NEW.retain_until IS NULL)
        OR (NEW.publish_state = 'published'
            AND NEW.claimed_by IS NULL AND NEW.claim_token IS NULL
            AND NEW.claim_expires_at IS NULL AND NEW.published_at IS NOT NULL
            AND NEW.published_by IS NOT NULL AND NEW.published_claim_token IS NOT NULL
            AND NEW.notified_at IS NOT NULL
            AND ((NEW.is_terminal = 1 AND NEW.retain_until IS NULL)
                 OR (NEW.is_terminal = 0 AND NEW.retain_until IS NOT NULL)))
    )
)
BEGIN
    SELECT RAISE(ABORT, 'invalid public event delivery authority');
END;
CREATE TRIGGER public_event_outbox_update_contract
BEFORE UPDATE ON public_event_outbox
FOR EACH ROW
WHEN NOT (
    NEW.run_id IS OLD.run_id
    AND NEW.public_event_id IS OLD.public_event_id
    AND NEW.causation_event_id IS OLD.causation_event_id
    AND NEW.public_ordinal IS OLD.public_ordinal
    AND NEW.public_schema_version IS OLD.public_schema_version
    AND NEW.event_kind IS OLD.event_kind
    AND NEW.is_terminal IS OLD.is_terminal
    AND NEW.safe_envelope IS OLD.safe_envelope
    AND NEW.available_at IS OLD.available_at
    AND NEW.created_at IS OLD.created_at
    AND NEW.publish_attempts >= OLD.publish_attempts
    AND (
        (OLD.publish_state = 'pending' AND NEW.publish_state = 'claimed')
        OR (OLD.publish_state = 'claimed' AND NEW.publish_state = 'claimed')
        OR (OLD.publish_state = 'claimed' AND NEW.publish_state = 'published')
    )
)
BEGIN
    SELECT RAISE(ABORT, 'public event delivery authority is immutable');
END;
CREATE TABLE public_event_receipts(
  run_id TEXT NOT NULL,
  public_event_id TEXT NOT NULL,
  causation_event_id TEXT NOT NULL,
  public_ordinal INTEGER NOT NULL CHECK(public_ordinal > 0),
  public_schema_version INTEGER NOT NULL CHECK(public_schema_version >= 1),
  event_kind TEXT NOT NULL,
  is_terminal INTEGER NOT NULL CHECK(is_terminal IN(0, 1)),
  created_at TEXT NOT NULL,
  PRIMARY KEY(run_id, public_event_id),
  UNIQUE(public_event_id),
  UNIQUE(run_id, causation_event_id, event_kind),
  UNIQUE(run_id, causation_event_id, public_ordinal),
  FOREIGN KEY(run_id, causation_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  CHECK((is_terminal = 0 AND event_kind IN('run.created','run.started','operation.started',
'operation.completed','operation.failed'))
OR(is_terminal = 1 AND event_kind IN('run.completed','run.failed','run.cancelled','run.interrupted')))
);
CREATE TRIGGER public_event_receipt_update_forbidden
BEFORE UPDATE ON public_event_receipts
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'public event receipt is immutable');
END;
CREATE TRIGGER public_event_receipt_delete_forbidden
BEFORE DELETE ON public_event_receipts
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'public event receipt cannot be deleted');
END;
CREATE TABLE public_event_projection_decisions(
  run_id TEXT NOT NULL,
  execution_event_id TEXT NOT NULL,
  execution_seq INTEGER NOT NULL CHECK(execution_seq >= 1),
  execution_occurred_at TEXT NOT NULL,
  execution_transition_key TEXT NOT NULL,
  decision TEXT NOT NULL CHECK(decision IN('none', 'public')),
  public_event_id TEXT,
  public_ordinal INTEGER CHECK(public_ordinal IS NULL OR public_ordinal > 0),
  public_schema_version INTEGER CHECK(public_schema_version IS NULL OR public_schema_version >= 1),
  event_kind TEXT,
  is_terminal INTEGER CHECK(is_terminal IS NULL OR is_terminal IN(0, 1)),
  bound_at TEXT,
  PRIMARY KEY(run_id, execution_event_id),
  UNIQUE(public_event_id),
  FOREIGN KEY(run_id, execution_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  CHECK((decision = 'none'
AND public_event_id IS NULL
AND public_ordinal IS NULL
AND public_schema_version IS NULL
AND event_kind IS NULL
AND is_terminal IS NULL
AND bound_at IS NULL)
OR(decision = 'public'
AND public_event_id IS NOT NULL
AND public_ordinal IS NOT NULL
AND public_schema_version IS NOT NULL
AND event_kind IS NOT NULL
AND is_terminal IS NOT NULL
AND bound_at IS NOT NULL))
);
CREATE INDEX idx_public_projection_order
ON public_event_projection_decisions(
  run_id,
  execution_seq,
  public_ordinal,
  public_event_id
) WHERE decision='public';
CREATE TRIGGER execution_event_public_projection_decision_insert
AFTER INSERT ON execution_events
FOR EACH ROW
BEGIN
    INSERT INTO public_event_projection_decisions (
        run_id,execution_event_id,execution_seq,execution_occurred_at,
        execution_transition_key,decision
    ) VALUES (
        NEW.run_id,NEW.event_id,NEW.seq,NEW.occurred_at,NEW.transition_key,'none'
    );
END;
CREATE TRIGGER public_event_projection_decision_update_guard
BEFORE UPDATE ON public_event_projection_decisions
FOR EACH ROW
WHEN NOT (
    OLD.decision='none'
    AND NEW.decision='public'
    AND NEW.run_id IS OLD.run_id
    AND NEW.execution_event_id IS OLD.execution_event_id
    AND NEW.execution_seq IS OLD.execution_seq
    AND NEW.execution_occurred_at IS OLD.execution_occurred_at
    AND NEW.execution_transition_key IS OLD.execution_transition_key
    AND EXISTS (
        SELECT 1 FROM public_event_outbox outbox
        WHERE outbox.run_id=NEW.run_id
          AND outbox.causation_event_id=NEW.execution_event_id
          AND outbox.public_event_id=NEW.public_event_id
          AND outbox.public_ordinal=NEW.public_ordinal
          AND outbox.public_schema_version=NEW.public_schema_version
          AND outbox.event_kind=NEW.event_kind
          AND outbox.is_terminal=NEW.is_terminal
          AND outbox.created_at=NEW.bound_at
    )
)
BEGIN
    SELECT RAISE(ABORT, 'public projection decision is immutable');
END;
CREATE TRIGGER public_event_projection_decision_delete_forbidden
BEFORE DELETE ON public_event_projection_decisions
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'public projection decision cannot be deleted');
END;
CREATE UNIQUE INDEX uq_public_receipt_causation
ON public_event_receipts(
  run_id,
  causation_event_id
);
CREATE TRIGGER public_event_receipt_insert_provenance
BEFORE INSERT ON public_event_receipts
FOR EACH ROW
WHEN NOT EXISTS (
    SELECT 1
    FROM public_event_outbox outbox
    JOIN public_event_projection_decisions decision
      ON decision.run_id=outbox.run_id
     AND decision.execution_event_id=outbox.causation_event_id
    WHERE outbox.run_id=NEW.run_id
      AND outbox.public_event_id=NEW.public_event_id
      AND outbox.causation_event_id=NEW.causation_event_id
      AND outbox.public_ordinal=NEW.public_ordinal
      AND outbox.public_schema_version=NEW.public_schema_version
      AND outbox.event_kind=NEW.event_kind
      AND outbox.is_terminal=NEW.is_terminal
      AND outbox.created_at=NEW.created_at
      AND decision.decision='public'
      AND decision.public_event_id=NEW.public_event_id
      AND decision.public_ordinal=NEW.public_ordinal
      AND decision.public_schema_version=NEW.public_schema_version
      AND decision.event_kind=NEW.event_kind
      AND decision.is_terminal=NEW.is_terminal
      AND decision.bound_at=NEW.created_at
)
BEGIN
    SELECT RAISE(ABORT, 'public event receipt lacks committed provenance');
END;
CREATE TABLE public_event_delivery_heads(
  run_id TEXT NOT NULL PRIMARY KEY,
  head_state TEXT NOT NULL CHECK(head_state IN('ready', 'drained')),
  public_event_id TEXT,
  execution_event_id TEXT,
  execution_seq INTEGER CHECK(execution_seq IS NULL OR execution_seq >= 1),
  public_ordinal INTEGER CHECK(public_ordinal IS NULL OR public_ordinal > 0),
  delivery_state TEXT CHECK(delivery_state IS NULL OR delivery_state IN('pending', 'claimed')),
  due_at TEXT,
  UNIQUE(public_event_id),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, public_event_id)
  REFERENCES public_event_outbox(run_id, public_event_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, execution_event_id)
  REFERENCES public_event_projection_decisions(run_id, execution_event_id)
  ON DELETE RESTRICT,
  CHECK((head_state='ready'
AND public_event_id IS NOT NULL
AND execution_event_id IS NOT NULL
AND execution_seq IS NOT NULL
AND public_ordinal IS NOT NULL
AND delivery_state IS NOT NULL
AND due_at IS NOT NULL)
OR(head_state='drained'
AND public_event_id IS NULL
AND execution_event_id IS NULL
AND execution_seq IS NULL
AND public_ordinal IS NULL
AND delivery_state IS NULL
AND due_at IS NULL))
);
CREATE INDEX idx_public_delivery_heads_due
ON public_event_delivery_heads(
  due_at,
  run_id,
  execution_seq,
  public_ordinal,
  public_event_id
) WHERE head_state='ready';
CREATE TRIGGER public_event_delivery_head_insert_guard
BEFORE INSERT ON public_event_delivery_heads
FOR EACH ROW
WHEN NEW.head_state<>'ready' OR NOT EXISTS (
    SELECT 1
    FROM public_event_projection_decisions decision
    JOIN public_event_outbox outbox
      ON outbox.run_id=decision.run_id
     AND outbox.causation_event_id=decision.execution_event_id
     AND outbox.public_event_id=decision.public_event_id
    WHERE decision.run_id=NEW.run_id
      AND decision.execution_event_id=NEW.execution_event_id
      AND decision.execution_seq=NEW.execution_seq
      AND decision.public_event_id=NEW.public_event_id
      AND decision.public_ordinal=NEW.public_ordinal
      AND decision.decision='public'
      AND outbox.public_ordinal=decision.public_ordinal
      AND outbox.publish_state=NEW.delivery_state
      AND outbox.publish_state<>'published'
      AND NEW.due_at=CASE outbox.publish_state
          WHEN 'pending' THEN STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.available_at)
          ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.claim_expires_at)
      END
)
BEGIN
    SELECT RAISE(ABORT, 'invalid public event delivery head insert');
END;
CREATE TRIGGER public_event_delivery_head_update_guard
BEFORE UPDATE ON public_event_delivery_heads
FOR EACH ROW
WHEN NEW.run_id IS NOT OLD.run_id OR NOT (
    (NEW.head_state='ready'
        AND EXISTS (
            SELECT 1
            FROM public_event_projection_decisions decision
            JOIN public_event_outbox outbox
              ON outbox.run_id=decision.run_id
             AND outbox.causation_event_id=decision.execution_event_id
             AND outbox.public_event_id=decision.public_event_id
            WHERE decision.run_id=NEW.run_id
              AND decision.execution_event_id=NEW.execution_event_id
              AND decision.execution_seq=NEW.execution_seq
              AND decision.public_event_id=NEW.public_event_id
              AND decision.public_ordinal=NEW.public_ordinal
              AND decision.decision='public'
              AND outbox.public_ordinal=decision.public_ordinal
              AND outbox.publish_state=NEW.delivery_state
              AND outbox.publish_state<>'published'
              AND NEW.due_at=CASE outbox.publish_state
                  WHEN 'pending' THEN STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.available_at)
                  ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ',outbox.claim_expires_at)
              END
        )
        AND (
            OLD.head_state='drained'
            OR (
                OLD.head_state='ready'
                AND NEW.public_event_id=OLD.public_event_id
                AND NEW.execution_event_id=OLD.execution_event_id
                AND NEW.execution_seq=OLD.execution_seq
                AND NEW.public_ordinal=OLD.public_ordinal
                AND ((OLD.delivery_state='pending' AND NEW.delivery_state='claimed')
                     OR (OLD.delivery_state='claimed' AND NEW.delivery_state='claimed'))
            )
            OR (
                OLD.head_state='ready'
                AND (
                    NEW.execution_seq<OLD.execution_seq
                    OR (NEW.execution_seq=OLD.execution_seq
                        AND NEW.public_ordinal<OLD.public_ordinal)
                    OR (NEW.execution_seq=OLD.execution_seq
                        AND NEW.public_ordinal=OLD.public_ordinal
                        AND NEW.public_event_id<OLD.public_event_id)
                )
            )
            OR (
                OLD.head_state='ready'
                AND EXISTS (
                    SELECT 1 FROM public_event_outbox published
                    WHERE published.run_id=OLD.run_id
                      AND published.public_event_id=OLD.public_event_id
                      AND published.publish_state='published'
                )
                AND (
                    NEW.execution_seq>OLD.execution_seq
                    OR (NEW.execution_seq=OLD.execution_seq
                        AND NEW.public_ordinal>OLD.public_ordinal)
                    OR (NEW.execution_seq=OLD.execution_seq
                        AND NEW.public_ordinal=OLD.public_ordinal
                        AND NEW.public_event_id>OLD.public_event_id)
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM public_event_projection_decisions between_decision
                    JOIN public_event_outbox between_outbox
                      ON between_outbox.run_id=between_decision.run_id
                     AND between_outbox.causation_event_id=between_decision.execution_event_id
                     AND between_outbox.public_event_id=between_decision.public_event_id
                    WHERE between_decision.run_id=OLD.run_id
                      AND between_decision.decision='public'
                      AND between_outbox.publish_state<>'published'
                      AND (
                          between_decision.execution_seq>OLD.execution_seq
                          OR (between_decision.execution_seq=OLD.execution_seq
                              AND between_decision.public_ordinal>OLD.public_ordinal)
                          OR (between_decision.execution_seq=OLD.execution_seq
                              AND between_decision.public_ordinal=OLD.public_ordinal
                              AND between_decision.public_event_id>OLD.public_event_id)
                      )
                      AND (
                          between_decision.execution_seq<NEW.execution_seq
                          OR (between_decision.execution_seq=NEW.execution_seq
                              AND between_decision.public_ordinal<NEW.public_ordinal)
                          OR (between_decision.execution_seq=NEW.execution_seq
                              AND between_decision.public_ordinal=NEW.public_ordinal
                              AND between_decision.public_event_id<NEW.public_event_id)
                      )
                )
            )
        ))
    OR
    (NEW.head_state='drained'
        AND OLD.head_state='ready'
        AND OLD.delivery_state='claimed'
        AND EXISTS (
            SELECT 1 FROM public_event_outbox published
            WHERE published.run_id=OLD.run_id
              AND published.public_event_id=OLD.public_event_id
              AND published.publish_state='published'
        )
        AND NOT EXISTS (
            SELECT 1
            FROM public_event_projection_decisions later
            JOIN public_event_outbox later_outbox
              ON later_outbox.run_id=later.run_id
             AND later_outbox.causation_event_id=later.execution_event_id
             AND later_outbox.public_event_id=later.public_event_id
            WHERE later.run_id=OLD.run_id
              AND later.decision='public'
              AND later_outbox.publish_state<>'published'
              AND (
                  later.execution_seq>OLD.execution_seq
                  OR (later.execution_seq=OLD.execution_seq
                      AND later.public_ordinal>OLD.public_ordinal)
                  OR (later.execution_seq=OLD.execution_seq
                      AND later.public_ordinal=OLD.public_ordinal
                      AND later.public_event_id>OLD.public_event_id)
              )
        ))
)
BEGIN
    SELECT RAISE(ABORT, 'invalid public event delivery head update');
END;
CREATE TRIGGER public_event_delivery_head_delete_forbidden
BEFORE DELETE ON public_event_delivery_heads
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'public event delivery head cannot be deleted');
END;
CREATE TRIGGER public_event_outbox_authority_insert
AFTER INSERT ON public_event_outbox
FOR EACH ROW
BEGIN
    SELECT CASE WHEN NEW.publish_state<>'pending'
        THEN RAISE(ABORT, 'public outbox must enter pending') END;

    UPDATE public_event_projection_decisions
    SET decision='public',
        public_event_id=NEW.public_event_id,
        public_ordinal=NEW.public_ordinal,
        public_schema_version=NEW.public_schema_version,
        event_kind=NEW.event_kind,
        is_terminal=NEW.is_terminal,
        bound_at=NEW.created_at
    WHERE run_id=NEW.run_id
      AND execution_event_id=NEW.causation_event_id
      AND decision='none';
    SELECT CASE WHEN changes()<>1
        THEN RAISE(ABORT, 'public projection decision bind failed') END;

    INSERT INTO public_event_receipts (
        run_id,public_event_id,causation_event_id,public_ordinal,
        public_schema_version,event_kind,is_terminal,created_at
    ) VALUES (
        NEW.run_id,NEW.public_event_id,NEW.causation_event_id,NEW.public_ordinal,
        NEW.public_schema_version,NEW.event_kind,NEW.is_terminal,NEW.created_at
    );

    INSERT INTO public_event_delivery_heads (
        run_id,head_state,public_event_id,execution_event_id,
        execution_seq,public_ordinal,delivery_state,due_at
    )
    SELECT NEW.run_id,'ready',NEW.public_event_id,NEW.causation_event_id,
           decision.execution_seq,NEW.public_ordinal,'pending',
           STRFTIME('%Y-%m-%dT%H:%M:%fZ',NEW.available_at)
    FROM public_event_projection_decisions decision
    WHERE decision.run_id=NEW.run_id
      AND decision.execution_event_id=NEW.causation_event_id
      AND decision.decision='public'
    ON CONFLICT(run_id) DO UPDATE SET
        head_state=excluded.head_state,
        public_event_id=excluded.public_event_id,
        execution_event_id=excluded.execution_event_id,
        execution_seq=excluded.execution_seq,
        public_ordinal=excluded.public_ordinal,
        delivery_state=excluded.delivery_state,
        due_at=excluded.due_at
    WHERE public_event_delivery_heads.head_state='drained'
       OR excluded.execution_seq<public_event_delivery_heads.execution_seq
       OR (excluded.execution_seq=public_event_delivery_heads.execution_seq
           AND excluded.public_ordinal<public_event_delivery_heads.public_ordinal)
       OR (excluded.execution_seq=public_event_delivery_heads.execution_seq
           AND excluded.public_ordinal=public_event_delivery_heads.public_ordinal
           AND excluded.public_event_id<public_event_delivery_heads.public_event_id);
END;
CREATE TRIGGER public_event_outbox_delivery_head_update
AFTER UPDATE OF publish_state,claim_expires_at ON public_event_outbox
FOR EACH ROW
BEGIN
    -- Claim and reclaim only mutate the already-authoritative head.
    UPDATE public_event_delivery_heads
    SET delivery_state=NEW.publish_state,
        due_at=CASE NEW.publish_state
            WHEN 'pending' THEN STRFTIME('%Y-%m-%dT%H:%M:%fZ',NEW.available_at)
            ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ',NEW.claim_expires_at)
        END
    WHERE NEW.publish_state<>'published'
      AND run_id=NEW.run_id
      AND public_event_id=NEW.public_event_id;
    SELECT CASE WHEN NEW.publish_state<>'published' AND changes()<>1
        THEN RAISE(ABORT, 'public claim does not own delivery head') END;

    -- Publishing seeks strictly after OLD. The projection-order index makes
    -- this O(log n + 1); it never restarts at the beginning of Run history.
    INSERT INTO public_event_delivery_heads (
        run_id,head_state,public_event_id,execution_event_id,
        execution_seq,public_ordinal,delivery_state,due_at
    )
    SELECT next.run_id,'ready',next.public_event_id,next.execution_event_id,
           next.execution_seq,next.public_ordinal,next_outbox.publish_state,
           CASE next_outbox.publish_state
               WHEN 'pending' THEN STRFTIME('%Y-%m-%dT%H:%M:%fZ',next_outbox.available_at)
               ELSE STRFTIME('%Y-%m-%dT%H:%M:%fZ',next_outbox.claim_expires_at)
           END
    FROM public_event_projection_decisions current
    JOIN public_event_projection_decisions next ON next.run_id=current.run_id
    JOIN public_event_outbox next_outbox
      ON next_outbox.run_id=next.run_id
     AND next_outbox.causation_event_id=next.execution_event_id
     AND next_outbox.public_event_id=next.public_event_id
    WHERE NEW.publish_state='published'
      AND current.run_id=NEW.run_id
      AND current.execution_event_id=NEW.causation_event_id
      AND current.decision='public'
      AND next.decision='public'
      AND next_outbox.publish_state<>'published'
      AND (
          next.execution_seq>current.execution_seq
          OR (next.execution_seq=current.execution_seq
              AND next.public_ordinal>current.public_ordinal)
          OR (next.execution_seq=current.execution_seq
              AND next.public_ordinal=current.public_ordinal
              AND next.public_event_id>current.public_event_id)
      )
    ORDER BY next.execution_seq,next.public_ordinal,next.public_event_id
    LIMIT 1
    ON CONFLICT(run_id) DO UPDATE SET
        head_state=excluded.head_state,
        public_event_id=excluded.public_event_id,
        execution_event_id=excluded.execution_event_id,
        execution_seq=excluded.execution_seq,
        public_ordinal=excluded.public_ordinal,
        delivery_state=excluded.delivery_state,
        due_at=excluded.due_at;

    UPDATE public_event_delivery_heads
    SET head_state='drained',public_event_id=NULL,execution_event_id=NULL,
        execution_seq=NULL,public_ordinal=NULL,delivery_state=NULL,due_at=NULL
    WHERE NEW.publish_state='published'
      AND run_id=NEW.run_id
      AND public_event_id=NEW.public_event_id;

    SELECT CASE WHEN NEW.publish_state='published' AND EXISTS (
        SELECT 1 FROM public_event_delivery_heads head
        WHERE head.run_id=NEW.run_id
          AND (head.head_state='ready' AND head.public_event_id=NEW.public_event_id)
    ) THEN RAISE(ABORT, 'published public event remained delivery head') END;
END;
CREATE TABLE artifact_store_authority(
  singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
  backend TEXT NOT NULL CHECK(backend = 'shared_filesystem'),
  namespace TEXT NOT NULL CHECK(length(namespace) BETWEEN 1 AND 128
AND namespace NOT GLOB '*[^A-Za-z0-9._-]*'),
store_id TEXT NOT NULL CHECK(length(store_id) = 47
AND store_id GLOB 'artifact_store_[0-9a-f]*'
AND substr(store_id, 16) NOT GLOB '*[^0-9a-f]*'),
bound_at TEXT NOT NULL DEFAULT(STRFTIME('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE TRIGGER artifact_store_authority_reject_update
BEFORE UPDATE ON artifact_store_authority
BEGIN
    SELECT RAISE(ABORT, 'artifact store authority is immutable');
END;
CREATE TRIGGER artifact_store_authority_reject_delete
BEFORE DELETE ON artifact_store_authority
BEGIN
    SELECT RAISE(ABORT, 'artifact store authority is immutable');
END;
CREATE UNIQUE INDEX uq_workflow_runs_response_id
ON workflow_runs(response_id);
CREATE TABLE response_public_items(
  run_id TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL CHECK(attempt_no >= 1),
  model_call_no INTEGER NOT NULL CHECK(model_call_no >= 1),
  item_ordinal INTEGER NOT NULL CHECK(item_ordinal >= 0),
  item_id TEXT NOT NULL CHECK(item_id <> ''),
  output_index INTEGER NOT NULL CHECK(output_index >= 0),
  node_id TEXT NOT NULL CHECK(node_id <> ''),
  item_kind TEXT NOT NULL CHECK(item_kind IN('message', 'function_call')),
  item_status TEXT NOT NULL CHECK(item_status IN('reserved', 'completed', 'incomplete', 'incomplete_unsealed')),
  seal_index INTEGER CHECK(seal_index IS NULL OR seal_index >= 0),
  safe_item TEXT CHECK(safe_item IS NULL OR json_valid(safe_item)),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(run_id, activation_id, attempt_no, model_call_no, item_ordinal),
  UNIQUE(run_id, item_id),
  UNIQUE(run_id, output_index),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT
);
CREATE TABLE model_call_usage(
  run_id TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL CHECK(attempt_no >= 1),
  model_call_no INTEGER NOT NULL CHECK(model_call_no >= 1),
  task_id TEXT NOT NULL CHECK(task_id <> ''),
  lease_epoch INTEGER NOT NULL CHECK(lease_epoch >= 1),
  fencing_token TEXT NOT NULL CHECK(fencing_token <> ''),
  call_status TEXT NOT NULL CHECK(call_status IN('started', 'completed', 'failed', 'abandoned')),
  finish_reason TEXT CHECK(finish_reason IS NULL
OR finish_reason IN('stop', 'tool_calls', 'length', 'content_filter', 'invalid')),
  usage TEXT CHECK(usage IS NULL OR json_valid(usage)),
  usage_complete INTEGER NOT NULL DEFAULT 0 CHECK(usage_complete IN(0, 1)),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(run_id, activation_id, attempt_no, model_call_no),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT
);
CREATE TABLE run_stream_snapshots(
  run_id TEXT NOT NULL PRIMARY KEY,
  protocol TEXT NOT NULL CHECK(protocol = 'run-stream/v1'),
  terminal_kind TEXT NOT NULL CHECK(terminal_kind IN('run.lifecycle.completed',
'run.lifecycle.failed',
'run.lifecycle.timed_out',
'run.lifecycle.cancelled',
'run.lifecycle.interrupted')),
  run_payload TEXT NOT NULL CHECK(json_valid(run_payload)),
  public_item_manifest TEXT NOT NULL CHECK(json_valid(public_item_manifest)),
  snapshot_hash TEXT NOT NULL CHECK(length(snapshot_hash) = 71 AND snapshot_hash LIKE 'sha256:%'),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT
);
CREATE TABLE model_tool_call_batches(
  run_id TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL CHECK(attempt_no >= 1),
  model_call_no INTEGER NOT NULL CHECK(model_call_no >= 1),
  batch_status TEXT NOT NULL CHECK(batch_status = 'checkpointed'),
  execution_status TEXT NOT NULL DEFAULT 'checkpointed' CHECK(execution_status IN('checkpointed','active','succeeded','failed','cancelled')),
  assistant_content TEXT CHECK(assistant_content IS NULL OR length(assistant_content) <= 1048576),
  parent_task_id TEXT,
  parent_lease_epoch INTEGER CHECK(parent_lease_epoch IS NULL OR parent_lease_epoch >= 1),
  parent_fencing_token TEXT,
  parent_claimed_by TEXT,
  parent_claim_token TEXT,
  parent_claim_expires_at TEXT,
  parent_task_projection_version INTEGER CHECK(parent_task_projection_version IS NULL OR parent_task_projection_version >= 1),
  continuation_status TEXT NOT NULL DEFAULT 'checkpointed' CHECK(continuation_status IN('checkpointed','waiting_tools','ready_continue','ready_failed','ready_cancelled')),
  activated_at TEXT,
  completed_at TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  parent_operation_deadline TEXT,
  PRIMARY KEY(run_id, activation_id, attempt_no, model_call_no),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, activation_id, attempt_no, model_call_no)
  REFERENCES model_call_usage(run_id, activation_id, attempt_no, model_call_no)
  ON DELETE RESTRICT,
  CHECK((execution_status = 'checkpointed' AND activated_at IS NULL)
OR(execution_status <> 'checkpointed' AND activated_at IS NOT NULL)),
  CHECK((execution_status IN('succeeded','failed','cancelled')) =(completed_at IS NOT NULL)),
  CHECK((continuation_status = 'checkpointed' AND parent_task_id IS NULL
AND parent_lease_epoch IS NULL AND parent_fencing_token IS NULL
AND parent_claimed_by IS NULL AND parent_claim_token IS NULL
AND parent_claim_expires_at IS NULL AND parent_task_projection_version IS NULL)
OR(continuation_status <> 'checkpointed' AND parent_task_id IS NOT NULL
AND parent_lease_epoch IS NOT NULL AND parent_fencing_token IS NOT NULL
AND parent_claimed_by IS NOT NULL AND parent_claim_token IS NOT NULL
AND parent_claim_expires_at IS NOT NULL AND parent_task_projection_version IS NOT NULL))
);
CREATE TABLE model_tool_calls(
  run_id TEXT NOT NULL,
  activation_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL CHECK(attempt_no >= 1),
  model_call_no INTEGER NOT NULL CHECK(model_call_no >= 1),
  call_index INTEGER NOT NULL CHECK(call_index >= 0),
  call_id TEXT NOT NULL CHECK(call_id <> '' AND length(call_id) <= 256),
  tool_name TEXT NOT NULL CHECK(tool_name <> '' AND length(tool_name) <= 128),
  arguments TEXT NOT NULL CHECK(json_valid(arguments) AND json_type(arguments) = 'object' AND length(arguments) <= 262144),
  tool_task_id TEXT UNIQUE CHECK(tool_task_id IS NULL OR(length(tool_task_id) = 69 AND tool_task_id LIKE 'task_%')),
  effect_id TEXT UNIQUE CHECK(effect_id IS NULL OR(length(effect_id) = 71 AND effect_id LIKE 'effect_%')),
  action_id TEXT CHECK(action_id IS NULL OR(action_id <> '' AND length(action_id) <= 128)),
  action_version TEXT CHECK(action_version IS NULL OR(action_version <> '' AND length(action_version) <= 64)),
  action_descriptor_hash TEXT CHECK(action_descriptor_hash IS NULL OR length(action_descriptor_hash) = 64),
  action_input_schema TEXT CHECK(action_input_schema IS NULL OR json_valid(action_input_schema)),
  action_output_schema TEXT CHECK(action_output_schema IS NULL OR json_valid(action_output_schema)),
  action_effect_policy TEXT CHECK(action_effect_policy IS NULL OR(json_valid(action_effect_policy)
AND json_type(action_effect_policy) = 'object')),
  action_deployment_binding TEXT CHECK(action_deployment_binding IS NULL OR(json_valid(action_deployment_binding)
AND json_type(action_deployment_binding) = 'object')),
  effective_public_policy TEXT CHECK(effective_public_policy IS NULL OR(json_valid(effective_public_policy)
AND json_type(effective_public_policy) = 'object')),
  response_item_id TEXT CHECK(response_item_id IS NULL OR(response_item_id <> '' AND length(response_item_id) <= 256)),
  response_output_index INTEGER CHECK(response_output_index IS NULL OR response_output_index >= 0),
  effect_idempotency TEXT CHECK(effect_idempotency IS NULL OR effect_idempotency IN('idempotent','non_idempotent')),
  cancellation TEXT CHECK(cancellation IS NULL OR cancellation IN('cooperative','lease_only')),
  max_attempts INTEGER CHECK(max_attempts IS NULL OR max_attempts >= 1),
  initial_backoff_ms INTEGER CHECK(initial_backoff_ms IS NULL OR initial_backoff_ms >= 0),
  max_backoff_ms INTEGER CHECK(max_backoff_ms IS NULL OR max_backoff_ms >= 0),
  timeout_ms INTEGER CHECK(timeout_ms IS NULL OR timeout_ms >= 1),
  call_status TEXT NOT NULL CHECK(call_status IN('pending','claimed','running','succeeded','failed','cancelled')),
  tool_attempt_no INTEGER CHECK(tool_attempt_no IS NULL OR tool_attempt_no >= 1),
  lease_epoch INTEGER CHECK(lease_epoch IS NULL OR lease_epoch >= 1),
  fencing_token TEXT CHECK(fencing_token IS NULL OR fencing_token <> ''),
  effect_evidence TEXT CHECK(effect_evidence IS NULL OR effect_evidence IN('not_started','started','committed','unknown')),
  available_at TEXT,
  claim_owner TEXT,
  claim_token TEXT,
  claim_expires_at TEXT,
  projection_version INTEGER NOT NULL DEFAULT 0 CHECK(projection_version >= 0),
  lease_loss_count INTEGER NOT NULL DEFAULT 0 CHECK(lease_loss_count >= 0),
  last_lease_loss_at TEXT,
  last_lease_loss_evidence TEXT CHECK(last_lease_loss_evidence IS NULL OR last_lease_loss_evidence IN('not_started','unknown')),
  result_json TEXT CHECK(result_json IS NULL OR(json_valid(result_json) AND length(result_json) <= 1048576)),
  failure_class TEXT CHECK(failure_class IS NULL OR failure_class IN('safe','infrastructure','effect_outcome_unknown')),
  failure_code TEXT CHECK(failure_code IS NULL OR(failure_code <> '' AND length(failure_code) <= 128)),
  failure_retryable INTEGER CHECK(failure_retryable IS NULL OR failure_retryable IN(0,1)),
  last_commit_claim_token TEXT,
  last_outcome_hash TEXT CHECK(last_outcome_hash IS NULL OR(length(last_outcome_hash) = 71
AND last_outcome_hash LIKE 'sha256:%')),
  last_outcome_disposition TEXT CHECK(last_outcome_disposition IS NULL OR last_outcome_disposition IN('succeeded','retry_scheduled','failed','cancelled')),
  last_outcome_attempt_no INTEGER CHECK(last_outcome_attempt_no IS NULL OR last_outcome_attempt_no >= 1),
  last_outcome_lease_epoch INTEGER CHECK(last_outcome_lease_epoch IS NULL OR last_outcome_lease_epoch >= 1),
  last_outcome_fencing_token TEXT,
  last_outcome_available_at TEXT,
  last_effect_evidence TEXT CHECK(last_effect_evidence IS NULL OR last_effect_evidence IN('not_started','started','committed','unknown')),
  last_failure_class TEXT CHECK(last_failure_class IS NULL OR last_failure_class IN('safe','infrastructure','effect_outcome_unknown')),
  last_failure_code TEXT CHECK(last_failure_code IS NULL OR(last_failure_code <> '' AND length(last_failure_code) <= 128)),
  last_failure_retryable INTEGER CHECK(last_failure_retryable IS NULL OR last_failure_retryable IN(0,1)),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  started_at TEXT,
  completed_at TEXT,
  response_seal_index INTEGER
  CHECK(response_seal_index IS NULL OR response_seal_index >= 3),
  PRIMARY KEY(run_id, activation_id, attempt_no, model_call_no, call_index),
  UNIQUE(run_id, activation_id, attempt_no, model_call_no, call_id),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, activation_id, attempt_no, model_call_no)
  REFERENCES model_tool_call_batches(run_id, activation_id, attempt_no, model_call_no)
  ON DELETE RESTRICT,
  CHECK((tool_task_id IS NULL AND effect_id IS NULL AND action_id IS NULL
AND action_version IS NULL AND action_descriptor_hash IS NULL
AND action_input_schema IS NULL AND action_output_schema IS NULL
AND action_effect_policy IS NULL AND action_deployment_binding IS NULL
AND effective_public_policy IS NULL AND response_item_id IS NULL
AND response_output_index IS NULL AND effect_idempotency IS NULL
AND cancellation IS NULL AND max_attempts IS NULL
AND initial_backoff_ms IS NULL AND max_backoff_ms IS NULL AND timeout_ms IS NULL
AND tool_attempt_no IS NULL AND lease_epoch IS NULL AND fencing_token IS NULL
AND effect_evidence IS NULL AND available_at IS NULL AND projection_version = 0)
OR(tool_task_id IS NOT NULL AND effect_id IS NOT NULL AND action_id IS NOT NULL
AND action_version IS NOT NULL AND action_descriptor_hash IS NOT NULL
AND action_input_schema IS NOT NULL AND action_output_schema IS NOT NULL
AND action_effect_policy IS NOT NULL AND action_deployment_binding IS NOT NULL
AND effective_public_policy IS NOT NULL AND effect_idempotency IS NOT NULL
AND cancellation IS NOT NULL AND max_attempts IS NOT NULL
AND initial_backoff_ms IS NOT NULL AND max_backoff_ms IS NOT NULL AND timeout_ms IS NOT NULL
AND tool_attempt_no IS NOT NULL AND lease_epoch IS NOT NULL AND fencing_token IS NOT NULL
AND effect_evidence IS NOT NULL AND projection_version >= 1)),
  CHECK((claim_owner IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)
OR(claim_owner IS NOT NULL AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)),
  CHECK((response_item_id IS NULL) =(response_output_index IS NULL)),
  CHECK((lease_loss_count = 0 AND last_lease_loss_at IS NULL AND last_lease_loss_evidence IS NULL)
OR(lease_loss_count > 0 AND last_lease_loss_at IS NOT NULL
AND last_lease_loss_evidence IS NOT NULL)),
  CHECK((last_commit_claim_token IS NULL AND last_outcome_hash IS NULL
AND last_outcome_disposition IS NULL AND last_outcome_attempt_no IS NULL
AND last_outcome_lease_epoch IS NULL AND last_outcome_fencing_token IS NULL
AND last_outcome_available_at IS NULL AND last_effect_evidence IS NULL
AND last_failure_class IS NULL AND last_failure_code IS NULL
AND last_failure_retryable IS NULL)
OR(last_commit_claim_token IS NOT NULL AND last_outcome_hash IS NOT NULL
AND last_outcome_disposition IS NOT NULL AND last_outcome_attempt_no IS NOT NULL
AND last_outcome_lease_epoch IS NOT NULL AND last_outcome_fencing_token IS NOT NULL
AND last_effect_evidence IS NOT NULL)),
  CHECK((call_status = 'pending' AND claim_owner IS NULL AND result_json IS NULL
AND failure_class IS NULL AND failure_code IS NULL AND failure_retryable IS NULL
AND completed_at IS NULL
AND(tool_task_id IS NOT NULL OR started_at IS NULL)
AND(tool_task_id IS NULL OR(effect_evidence = 'not_started' AND available_at IS NOT NULL)))
OR(call_status = 'claimed' AND claim_owner IS NOT NULL AND result_json IS NULL
AND failure_code IS NULL AND effect_evidence = 'not_started'
AND available_at IS NULL AND completed_at IS NULL)
OR(call_status = 'running' AND claim_owner IS NOT NULL AND result_json IS NULL
AND failure_code IS NULL AND effect_evidence = 'started'
AND available_at IS NULL AND started_at IS NOT NULL AND completed_at IS NULL)
OR(call_status = 'succeeded' AND claim_owner IS NOT NULL AND result_json IS NOT NULL
AND failure_code IS NULL AND effect_evidence = 'committed'
AND available_at IS NULL AND started_at IS NOT NULL AND completed_at IS NOT NULL)
OR(call_status IN('failed','cancelled')
AND result_json IS NULL AND failure_class IS NOT NULL AND failure_code IS NOT NULL
AND failure_retryable IS NOT NULL AND effect_evidence IN('not_started','started','unknown')
AND available_at IS NULL AND completed_at IS NOT NULL)),
  CHECK(started_at IS NULL OR completed_at IS NULL
        OR julianday(completed_at) >= julianday(started_at))
);
CREATE INDEX idx_model_tool_calls_claim
ON model_tool_calls(
  available_at,
  run_id,
  tool_task_id
) WHERE call_status = 'pending';
CREATE INDEX idx_model_tool_calls_reclaim
ON model_tool_calls(
  claim_expires_at,
  run_id,
  activation_id,
  attempt_no,
  model_call_no,
  call_index
) WHERE call_status IN('claimed', 'running');
CREATE INDEX idx_model_tool_calls_batch_status
ON model_tool_calls(
  run_id,
  activation_id,
  attempt_no,
  model_call_no,
  call_status
);
CREATE INDEX idx_model_tool_batches_parent_deadline
ON model_tool_call_batches(
  execution_status,
  continuation_status,
  parent_operation_deadline
);
CREATE TABLE workflow_retrieval_publications(
  run_id TEXT NOT NULL,
  retrieval_id TEXT NOT NULL CHECK(length(retrieval_id) = 68 AND retrieval_id LIKE 'ret_%'),
  task_id TEXT NOT NULL CHECK(length(task_id) = 69 AND task_id LIKE 'task_%'),
  activation_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL CHECK(attempt_no >= 1),
  retrieval_resource_id TEXT NOT NULL CHECK(retrieval_resource_id <> '' AND length(retrieval_resource_id) <= 128),
  retrieval_resource_version TEXT NOT NULL CHECK(retrieval_resource_version <> '' AND length(retrieval_resource_version) <= 64),
  retrieval_descriptor_hash TEXT NOT NULL CHECK(length(retrieval_descriptor_hash) = 64),
  query_field TEXT NOT NULL CHECK(query_field <> '' AND length(query_field) <= 128),
  effective_public_policy TEXT NOT NULL CHECK(json_valid(effective_public_policy)
AND json_type(effective_public_policy) = 'object'
AND length(effective_public_policy) <= 262144),
  effective_public_policy_hash TEXT NOT NULL CHECK(length(effective_public_policy_hash) = 71
AND effective_public_policy_hash LIKE 'sha256:%'),
  public_projection TEXT CHECK(public_projection IS NULL
OR(json_valid(public_projection)
AND json_type(public_projection) = 'object'
AND length(public_projection) <= 1048576)),
  public_projection_hash TEXT CHECK(public_projection_hash IS NULL
OR(length(public_projection_hash) = 71
AND public_projection_hash LIKE 'sha256:%')),
  completion_transition_key TEXT NOT NULL,
  completion_intent_hash TEXT NOT NULL CHECK(length(completion_intent_hash) = 71
AND completion_intent_hash LIKE 'sha256:%'),
  completion_event_id TEXT NOT NULL,
  completion_event_seq INTEGER NOT NULL CHECK(completion_event_seq >= 1),
  publication_hash TEXT NOT NULL CHECK(length(publication_hash) = 71 AND publication_hash LIKE 'sha256:%'),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY(run_id, retrieval_id),
  UNIQUE(run_id, task_id),
  UNIQUE(run_id, activation_id),
  UNIQUE(run_id, completion_transition_key),
  UNIQUE(run_id, completion_event_id),
  FOREIGN KEY(run_id) REFERENCES workflow_runs(run_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, task_id)
  REFERENCES task_outbox(run_id, task_id) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, activation_id, attempt_no)
  REFERENCES node_attempts(run_id, activation_id, attempt_no) ON DELETE RESTRICT,
  FOREIGN KEY(run_id, completion_event_id)
  REFERENCES execution_events(run_id, event_id) ON DELETE RESTRICT,
  CHECK((public_projection IS NULL) =(public_projection_hash IS NULL))
);
CREATE INDEX idx_workflow_retrieval_publications_terminal
ON workflow_retrieval_publications(
  run_id,
  activation_id,
  attempt_no,
  retrieval_id
);
CREATE TRIGGER workflow_retrieval_publication_update_forbidden
BEFORE UPDATE ON workflow_retrieval_publications
BEGIN
    SELECT RAISE(ABORT, 'workflow retrieval publication is immutable');
END;
CREATE TRIGGER workflow_retrieval_publication_delete_forbidden
BEFORE DELETE ON workflow_retrieval_publications
BEGIN
    SELECT RAISE(ABORT, 'workflow retrieval publication is immutable');
END;

CREATE TABLE terminal_runtime_instances(
  instance_id TEXT NOT NULL PRIMARY KEY,
  owner_epoch INTEGER NOT NULL CHECK(owner_epoch >= 1),
  endpoint TEXT NOT NULL CHECK(endpoint <> '' AND length(CAST(endpoint AS BLOB)) <= 4096),
  lease_expires_at TEXT NOT NULL,
  started_at TEXT NOT NULL,
  CHECK(instance_id <> '' AND length(CAST(instance_id AS BLOB)) <= 256),
  CHECK(lease_expires_at >= started_at)
);

CREATE TABLE terminal_run_admissions(
  run_id TEXT NOT NULL PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  definition_revision_id TEXT NOT NULL,
  deployment_revision_id TEXT NOT NULL,
  conversation_id TEXT,
  user_message_id TEXT,
  input_ref TEXT,
  input_hash TEXT NOT NULL,
  selected_context_hash TEXT,
  owner_instance_id TEXT NOT NULL,
  owner_epoch INTEGER NOT NULL CHECK(owner_epoch >= 1),
  accepted_at TEXT NOT NULL,
  UNIQUE(tenant_id, request_id),
  CHECK(run_id <> '' AND length(CAST(run_id AS BLOB)) <= 256),
  CHECK(tenant_id <> '' AND length(CAST(tenant_id AS BLOB)) <= 256),
  CHECK(request_id <> '' AND length(CAST(request_id AS BLOB)) <= 256),
  CHECK(agent_id <> '' AND length(CAST(agent_id AS BLOB)) <= 256),
  CHECK(definition_revision_id <> '' AND
        length(CAST(definition_revision_id AS BLOB)) <= 256),
  CHECK(deployment_revision_id <> '' AND
        length(CAST(deployment_revision_id AS BLOB)) <= 256),
  CHECK((conversation_id IS NULL) = (user_message_id IS NULL)),
  CHECK(length(input_hash) = 71 AND substr(input_hash, 1, 7) = 'sha256:' AND
        substr(input_hash, 8) NOT GLOB '*[^0-9a-f]*'),
  CHECK(selected_context_hash IS NULL OR
        (length(selected_context_hash) = 71 AND
         substr(selected_context_hash, 1, 7) = 'sha256:' AND
         substr(selected_context_hash, 8) NOT GLOB '*[^0-9a-f]*')),
  CHECK(owner_instance_id <> '' AND
        length(CAST(owner_instance_id AS BLOB)) <= 256)
);

CREATE INDEX idx_terminal_run_admissions_retention
ON terminal_run_admissions(accepted_at, run_id);

CREATE TABLE terminal_run_results(
  run_id TEXT NOT NULL PRIMARY KEY
    REFERENCES terminal_run_admissions(run_id) ON DELETE CASCADE,
  terminal_state TEXT NOT NULL
    CHECK(terminal_state IN('succeeded', 'failed', 'cancelled', 'timed_out')),
  response_id TEXT NOT NULL UNIQUE
    CHECK(response_id <> '' AND length(CAST(response_id AS BLOB)) <= 256),
  output_ref TEXT,
  output_hash TEXT CHECK(output_hash IS NULL OR
    (length(output_hash) = 71 AND substr(output_hash, 1, 7) = 'sha256:' AND
     substr(output_hash, 8) NOT GLOB '*[^0-9a-f]*')),
  error_code TEXT,
  usage_json TEXT CHECK(usage_json IS NULL OR json_valid(usage_json)),
  tool_results_json TEXT NOT NULL DEFAULT '[]'
    CHECK(json_valid(tool_results_json)
      AND json_type(tool_results_json) = 'array'
      AND length(CAST(tool_results_json AS BLOB)) <= 1048576),
  started_at TEXT NOT NULL,
  terminal_at TEXT NOT NULL,
  CHECK((output_ref IS NULL) = (output_hash IS NULL)),
  CHECK(terminal_at >= started_at)
);

CREATE TABLE terminal_content_deletion_jobs(
  deletion_job_id TEXT NOT NULL PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  content_ref TEXT NOT NULL,
  content_hash TEXT CHECK(content_hash IS NULL OR
    (length(content_hash) = 71 AND substr(content_hash, 1, 7) = 'sha256:' AND
     substr(content_hash, 8) NOT GLOB '*[^0-9a-f]*')),
  source_kind TEXT NOT NULL
    CHECK(source_kind IN(
      'terminal_run_retention',
      'conversation_privacy',
      'conversation_retention'
    )),
  source_id TEXT NOT NULL,
  job_state TEXT NOT NULL DEFAULT 'pending'
    CHECK(job_state IN('pending', 'claimed')),
  available_at TEXT NOT NULL,
  claim_token TEXT,
  claimed_by TEXT,
  claim_expires_at TEXT,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
  created_at TEXT NOT NULL,
  UNIQUE(tenant_id, content_ref, source_kind, source_id),
  CHECK(deletion_job_id <> '' AND length(CAST(deletion_job_id AS BLOB)) <= 256),
  CHECK(tenant_id <> '' AND length(CAST(tenant_id AS BLOB)) <= 256),
  CHECK(content_ref <> '' AND length(CAST(content_ref AS BLOB)) <= 16384),
  CHECK(source_id <> '' AND length(CAST(source_id AS BLOB)) <= 256),
  CHECK(
    (job_state = 'pending' AND claim_token IS NULL AND
     claimed_by IS NULL AND claim_expires_at IS NULL)
    OR
    (job_state = 'claimed' AND claim_token IS NOT NULL AND
     claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL)
  )
);

CREATE INDEX idx_terminal_content_deletion_jobs_pending
ON terminal_content_deletion_jobs(available_at, created_at, deletion_job_id)
WHERE job_state = 'pending';

CREATE INDEX idx_terminal_content_deletion_jobs_reclaim
ON terminal_content_deletion_jobs(claim_expires_at, deletion_job_id)
WHERE job_state = 'claimed';

CREATE TABLE terminal_artifact_staging(
  staging_id TEXT NOT NULL PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  content_ref TEXT NOT NULL UNIQUE,
  content_hash TEXT NOT NULL
    CHECK(length(content_hash) = 71 AND substr(content_hash, 1, 7) = 'sha256:' AND
          substr(content_hash, 8) NOT GLOB '*[^0-9a-f]*'),
  source_kind TEXT NOT NULL
    CHECK(source_kind IN(
      'run_output',
      'user_message',
      'assistant_message',
      'conversation_summary'
    )),
  source_id TEXT NOT NULL,
  staging_state TEXT NOT NULL DEFAULT 'pending'
    CHECK(staging_state IN('pending', 'claimed')),
  available_at TEXT NOT NULL,
  claim_token TEXT,
  claimed_by TEXT,
  claim_expires_at TEXT,
  attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
  created_at TEXT NOT NULL,
  UNIQUE(tenant_id, source_kind, source_id),
  CHECK(length(staging_id) = 79 AND
        substr(staging_id, 1, 15) = 'terminal_stage_' AND
        substr(staging_id, 16) NOT GLOB '*[^0-9a-f]*'),
  CHECK(tenant_id <> '' AND length(CAST(tenant_id AS BLOB)) <= 256),
  CHECK(content_ref <> '' AND length(CAST(content_ref AS BLOB)) <= 16384),
  CHECK(source_id <> '' AND length(CAST(source_id AS BLOB)) <= 512),
  CHECK(available_at >= created_at),
  CHECK(
    (staging_state = 'pending' AND claim_token IS NULL AND
     claimed_by IS NULL AND claim_expires_at IS NULL)
    OR
    (staging_state = 'claimed' AND claim_token IS NOT NULL AND
     claimed_by IS NOT NULL AND claim_expires_at IS NOT NULL)
  )
);

CREATE INDEX idx_terminal_artifact_staging_pending
ON terminal_artifact_staging(available_at, created_at, staging_id)
WHERE staging_state = 'pending';

CREATE INDEX idx_terminal_artifact_staging_reclaim
ON terminal_artifact_staging(claim_expires_at, staging_id)
WHERE staging_state = 'claimed';

CREATE TABLE conversations(
  conversation_id TEXT NOT NULL PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  persistence_mode TEXT NOT NULL CHECK(persistence_mode IN('full', 'terminal_only')),
  deployment_revision_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  archived_at TEXT,
  CHECK(conversation_id <> '' AND length(CAST(conversation_id AS BLOB)) <= 256),
  CHECK(tenant_id <> '' AND length(CAST(tenant_id AS BLOB)) <= 256),
  CHECK(user_id <> '' AND length(CAST(user_id AS BLOB)) <= 256),
  CHECK(agent_id <> '' AND length(CAST(agent_id AS BLOB)) <= 256),
  CHECK(deployment_revision_id <> '' AND
        length(CAST(deployment_revision_id AS BLOB)) <= 256),
  CHECK(archived_at IS NULL OR archived_at >= created_at)
);

CREATE INDEX idx_conversations_created_retention
ON conversations(created_at, conversation_id);

CREATE TABLE conversation_tombstones(
  conversation_id TEXT NOT NULL PRIMARY KEY,
  deleted_at TEXT NOT NULL,
  CHECK(conversation_id <> '' AND length(CAST(conversation_id AS BLOB)) <= 256)
);

CREATE TABLE conversation_messages(
  message_id TEXT NOT NULL PRIMARY KEY,
  conversation_id TEXT NOT NULL
    REFERENCES conversations(conversation_id) ON DELETE CASCADE,
  message_order INTEGER NOT NULL CHECK(message_order >= 1),
  role TEXT NOT NULL CHECK(role IN('user', 'assistant')),
  run_id TEXT,
  content_inline TEXT CHECK(content_inline IS NULL OR json_valid(content_inline)),
  content_ref TEXT,
  content_hash TEXT NOT NULL
    CHECK(length(content_hash) = 71 AND substr(content_hash, 1, 7) = 'sha256:' AND
          substr(content_hash, 8) NOT GLOB '*[^0-9a-f]*'),
  created_at TEXT NOT NULL,
  UNIQUE(conversation_id, message_order),
  CHECK(role <> 'assistant' OR run_id IS NOT NULL),
  CHECK((content_inline IS NULL) <> (content_ref IS NULL))
);

CREATE INDEX idx_conversation_messages_page
ON conversation_messages(conversation_id, message_order DESC);

CREATE UNIQUE INDEX uq_conversation_assistant_run
ON conversation_messages(conversation_id, run_id)
WHERE role = 'assistant';

CREATE TABLE conversation_summaries(
  conversation_id TEXT NOT NULL
    REFERENCES conversations(conversation_id) ON DELETE CASCADE,
  through_message_order INTEGER NOT NULL CHECK(through_message_order >= 1),
  summary_ref TEXT NOT NULL
    CHECK(summary_ref <> '' AND length(CAST(summary_ref AS BLOB)) <= 16384),
  summary_hash TEXT NOT NULL
    CHECK(length(summary_hash) = 71 AND substr(summary_hash, 1, 7) = 'sha256:' AND
          substr(summary_hash, 8) NOT GLOB '*[^0-9a-f]*'),
  model_revision TEXT NOT NULL
    CHECK(model_revision <> '' AND length(CAST(model_revision AS BLOB)) <= 256),
  created_at TEXT NOT NULL,
  PRIMARY KEY(conversation_id, through_message_order)
);

CREATE TABLE conversation_summary_jobs(
  conversation_id TEXT NOT NULL PRIMARY KEY
    REFERENCES conversations(conversation_id) ON DELETE CASCADE,
  claim_token TEXT NOT NULL UNIQUE,
  claimed_by TEXT NOT NULL,
  claim_expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  CHECK(claim_token <> '' AND length(CAST(claim_token AS BLOB)) <= 256),
  CHECK(claimed_by <> '' AND length(CAST(claimed_by AS BLOB)) <= 256),
  CHECK(claim_expires_at > created_at)
);

CREATE TABLE full_conversation_turns(
  tenant_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  run_id TEXT NOT NULL
    REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
  user_message_id TEXT NOT NULL UNIQUE,
  assistant_message_id TEXT NOT NULL UNIQUE,
  user_content_hash TEXT NOT NULL
    CHECK(length(user_content_hash) = 71 AND
          substr(user_content_hash, 1, 7) = 'sha256:' AND
          substr(user_content_hash, 8) NOT GLOB '*[^0-9a-f]*'),
  selected_context_hash TEXT NOT NULL
    CHECK(length(selected_context_hash) = 71 AND
          substr(selected_context_hash, 1, 7) = 'sha256:' AND
          substr(selected_context_hash, 8) NOT GLOB '*[^0-9a-f]*'),
  created_at TEXT NOT NULL,
  PRIMARY KEY(tenant_id, request_id),
  UNIQUE(run_id),
  CHECK(tenant_id <> '' AND length(CAST(tenant_id AS BLOB)) <= 256),
  CHECK(request_id <> '' AND length(CAST(request_id AS BLOB)) <= 256),
  CHECK(user_id <> '' AND length(CAST(user_id AS BLOB)) <= 256),
  CHECK(user_message_id <> '' AND length(CAST(user_message_id AS BLOB)) <= 256),
  CHECK(assistant_message_id <> '' AND length(CAST(assistant_message_id AS BLOB)) <= 256)
);

CREATE INDEX idx_full_conversation_turns_conversation
ON full_conversation_turns(conversation_id, created_at, run_id);

CREATE TABLE durable_schema_contract (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    contract_id TEXT NOT NULL,
    backend TEXT NOT NULL CHECK (backend IN ('postgres', 'sqlite')),
    installed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO durable_schema_contract (singleton, contract_id, backend)
VALUES (
    1,
    'durable-schema-ed759e21-5c5d-42e9-90d3-744029ea19b2',
    'sqlite'
);

COMMIT;
