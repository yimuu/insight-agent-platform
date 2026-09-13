-- Current PostgreSQL authority. Provision only into a fresh target.
-- Business semantics remain in owning Rust types; this file owns physical structure.
SET LOCAL search_path = pg_catalog;
CREATE SCHEMA insight_platform;

CREATE FUNCTION insight_platform.is_bounded_object(candidate jsonb, maximum_bytes integer) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN ((jsonb_typeof(candidate) = 'object'::text) AND (maximum_bytes > 0) AND (octet_length((candidate)::text) <= maximum_bytes));

CREATE FUNCTION insight_platform.is_platform_id(candidate text) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN (candidate ~ '^[a-z][a-z0-9]{1,7}_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'::text);

CREATE FUNCTION insight_platform.is_sha256(candidate text) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN (candidate ~ '^sha256:[0-9a-f]{64}$'::text);

CREATE FUNCTION insight_platform.is_trace_id(candidate text) RETURNS boolean
    LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
    RETURN ((candidate ~ '^[0-9a-f]{32}$'::text) AND (candidate <> '00000000000000000000000000000000'::text));

CREATE TABLE insight_platform.artifact_blobs (
    tenant_id text NOT NULL,
    blob_id text NOT NULL,
    backend text NOT NULL,
    storage_binding_digest text NOT NULL,
    security_domain_digest text NOT NULL,
    object_reference_ciphertext bytea NOT NULL,
    object_generation text,
    key_id text NOT NULL,
    encryption_domain_id text NOT NULL,
    content_digest text,
    size_bytes bigint,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    verified_at timestamp with time zone,
    deleted_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT artifact_blobs_backend_ck CHECK ((backend ~ '^[a-z][a-z0-9_.-]{0,63}$'::text)),
    CONSTRAINT artifact_blobs_digest_ck CHECK (((content_digest IS NULL) OR insight_platform.is_sha256(content_digest))),
    CONSTRAINT artifact_blobs_encryption_domain_ck CHECK (insight_platform.is_platform_id(encryption_domain_id)),
    CONSTRAINT artifact_blobs_id_ck CHECK (insight_platform.is_platform_id(blob_id)),
    CONSTRAINT artifact_blobs_key_id_ck CHECK (((octet_length(key_id) >= 1) AND (octet_length(key_id) <= 255))),
    CONSTRAINT artifact_blobs_object_ck CHECK (((octet_length(object_reference_ciphertext) >= 1) AND (octet_length(object_reference_ciphertext) <= 16384))),
    CONSTRAINT artifact_blobs_object_generation_ck CHECK (((object_generation IS NULL) OR ((octet_length(object_generation) >= 1) AND (octet_length(object_generation) <= 255)))),
    CONSTRAINT artifact_blobs_security_domain_ck CHECK (insight_platform.is_sha256(security_domain_digest)),
    CONSTRAINT artifact_blobs_size_ck CHECK (((size_bytes IS NULL) OR (size_bytes >= 0))),
    CONSTRAINT artifact_blobs_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT artifact_blobs_storage_binding_ck CHECK (insight_platform.is_sha256(storage_binding_digest)),
    CONSTRAINT artifact_blobs_time_ck CHECK (((updated_at >= created_at) AND ((verified_at IS NULL) OR (verified_at >= created_at)) AND ((deleted_at IS NULL) OR (deleted_at >= created_at)))),
    CONSTRAINT artifact_blobs_verified_shape_ck CHECK (((state <> 'verified'::text) OR ((object_generation IS NOT NULL) AND (content_digest IS NOT NULL) AND (size_bytes IS NOT NULL) AND (verified_at IS NOT NULL)))),
    CONSTRAINT artifact_blobs_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.artifact_links (
    tenant_id text NOT NULL,
    artifact_link_id text NOT NULL,
    link_kind text NOT NULL,
    owner_kind text NOT NULL,
    owner_id text NOT NULL,
    source_artifact_id text,
    target_artifact_id text,
    link_key_digest text NOT NULL,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    payload_schema_version integer DEFAULT 1 NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload_digest text NOT NULL,
    expires_at timestamp with time zone,
    released_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT artifact_links_id_ck CHECK (insight_platform.is_platform_id(artifact_link_id)),
    CONSTRAINT artifact_links_key_ck CHECK (insight_platform.is_sha256(link_key_digest)),
    CONSTRAINT artifact_links_kind_ck CHECK ((link_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT artifact_links_owner_id_ck CHECK (insight_platform.is_platform_id(owner_id)),
    CONSTRAINT artifact_links_owner_kind_ck CHECK ((owner_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT artifact_links_payload_ck CHECK (insight_platform.is_bounded_object(payload, 262144)),
    CONSTRAINT artifact_links_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT artifact_links_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT artifact_links_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT artifact_links_target_ck CHECK (((source_artifact_id IS NOT NULL) OR (target_artifact_id IS NOT NULL))),
    CONSTRAINT artifact_links_time_ck CHECK (((updated_at >= created_at) AND ((expires_at IS NULL) OR (expires_at >= created_at)) AND ((released_at IS NULL) OR (released_at >= created_at)))),
    CONSTRAINT artifact_links_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.artifacts (
    tenant_id text NOT NULL,
    artifact_id text NOT NULL,
    blob_id text,
    purpose text NOT NULL,
    classification text NOT NULL,
    expected_size_bytes bigint NOT NULL,
    expected_digest text,
    declared_media_type text,
    verified_media_type text,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    metadata_schema_version integer DEFAULT 1 NOT NULL,
    metadata jsonb DEFAULT '{}'::jsonb NOT NULL,
    metadata_digest text NOT NULL,
    retention_policy_revision_id text NOT NULL,
    retain_until timestamp with time zone NOT NULL,
    created_by text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    terminal_at timestamp with time zone,
    CONSTRAINT artifacts_classification_ck CHECK ((classification ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT artifacts_created_by_ck CHECK (insight_platform.is_platform_id(created_by)),
    CONSTRAINT artifacts_declared_media_type_ck CHECK (((declared_media_type IS NULL) OR ((octet_length(declared_media_type) >= 1) AND (octet_length(declared_media_type) <= 255)))),
    CONSTRAINT artifacts_expected_digest_ck CHECK (((expected_digest IS NULL) OR insight_platform.is_sha256(expected_digest))),
    CONSTRAINT artifacts_expected_size_ck CHECK ((expected_size_bytes >= 0)),
    CONSTRAINT artifacts_id_ck CHECK (insight_platform.is_platform_id(artifact_id)),
    CONSTRAINT artifacts_metadata_ck CHECK (insight_platform.is_bounded_object(metadata, 262144)),
    CONSTRAINT artifacts_metadata_digest_ck CHECK (insight_platform.is_sha256(metadata_digest)),
    CONSTRAINT artifacts_metadata_version_ck CHECK ((metadata_schema_version > 0)),
    CONSTRAINT artifacts_purpose_ck CHECK ((purpose ~ '^[a-z][a-z0-9_.:]{0,127}$'::text)),
    CONSTRAINT artifacts_retention_policy_id_ck CHECK (insight_platform.is_platform_id(retention_policy_revision_id)),
    CONSTRAINT artifacts_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT artifacts_time_ck CHECK (((updated_at >= created_at) AND (retain_until >= created_at) AND ((terminal_at IS NULL) OR (terminal_at >= created_at)))),
    CONSTRAINT artifacts_verified_media_type_ck CHECK (((verified_media_type IS NULL) OR ((octet_length(verified_media_type) >= 1) AND (octet_length(verified_media_type) <= 255)))),
    CONSTRAINT artifacts_verified_shape_ck CHECK (((state <> ALL (ARRAY['verified'::text, 'ready'::text])) OR ((blob_id IS NOT NULL) AND (verified_media_type IS NOT NULL)))),
    CONSTRAINT artifacts_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.deployments (
    tenant_id text NOT NULL,
    deployment_id text NOT NULL,
    resource_id text NOT NULL,
    resource_version_id text NOT NULL,
    environment text NOT NULL,
    bindings_digest text NOT NULL,
    payload_schema_version integer NOT NULL,
    bindings jsonb NOT NULL,
    created_by text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT deployments_bindings_ck CHECK (insight_platform.is_bounded_object(bindings, 1048576)),
    CONSTRAINT deployments_bindings_digest_ck CHECK (insight_platform.is_sha256(bindings_digest)),
    CONSTRAINT deployments_created_by_ck CHECK (insight_platform.is_platform_id(created_by)),
    CONSTRAINT deployments_environment_ck CHECK ((environment ~ '^[a-z][a-z0-9_.-]{0,63}$'::text)),
    CONSTRAINT deployments_id_ck CHECK (insight_platform.is_platform_id(deployment_id)),
    CONSTRAINT deployments_schema_version_ck CHECK ((payload_schema_version > 0))
);

CREATE TABLE insight_platform.events (
    tenant_id text,
    event_id text NOT NULL,
    aggregate_kind text NOT NULL,
    aggregate_id text NOT NULL,
    aggregate_version bigint,
    trace_id text NOT NULL,
    run_id text,
    public_sequence bigint,
    event_type text NOT NULL,
    visibility text NOT NULL,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    occurred_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT events_aggregate_id_ck CHECK (insight_platform.is_platform_id(aggregate_id)),
    CONSTRAINT events_aggregate_kind_ck CHECK ((aggregate_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT events_aggregate_version_ck CHECK (((aggregate_version IS NULL) OR (aggregate_version > 0))),
    CONSTRAINT events_id_ck CHECK (insight_platform.is_platform_id(event_id)),
    CONSTRAINT events_payload_ck CHECK (insight_platform.is_bounded_object(payload, 1048576)),
    CONSTRAINT events_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT events_public_sequence_ck CHECK (((public_sequence IS NULL) OR ((run_id IS NOT NULL) AND (public_sequence > 0)))),
    CONSTRAINT events_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT events_scope_ck CHECK (((tenant_id IS NOT NULL) OR ((run_id IS NULL) AND (public_sequence IS NULL) AND (aggregate_kind = ANY (ARRAY['principal'::text, 'installation_service'::text, 'release'::text]))))),
    CONSTRAINT events_trace_id_ck CHECK (insight_platform.is_trace_id(trace_id)),
    CONSTRAINT events_type_ck CHECK ((event_type ~ '^[a-z][a-z0-9_.]{0,127}$'::text)),
    CONSTRAINT events_visibility_ck CHECK ((visibility ~ '^[a-z][a-z0-9_]{0,63}$'::text))
);

CREATE TABLE insight_platform.invocations (
    tenant_id text NOT NULL,
    invocation_id text NOT NULL,
    trace_id text NOT NULL,
    invocation_kind text NOT NULL,
    owner_kind text NOT NULL,
    owner_id text NOT NULL,
    logical_key text NOT NULL,
    run_id text,
    node_id text,
    deployment_id text,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    input_value_id text,
    output_value_id text,
    effect_key_digest text,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    deadline timestamp with time zone NOT NULL,
    retry_at timestamp with time zone,
    started_at timestamp with time zone,
    terminal_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT invocations_effect_key_ck CHECK (((effect_key_digest IS NULL) OR insight_platform.is_sha256(effect_key_digest))),
    CONSTRAINT invocations_id_ck CHECK (insight_platform.is_platform_id(invocation_id)),
    CONSTRAINT invocations_kind_ck CHECK ((invocation_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT invocations_logical_key_ck CHECK (((octet_length(logical_key) >= 1) AND (octet_length(logical_key) <= 255))),
    CONSTRAINT invocations_owner_id_ck CHECK (insight_platform.is_platform_id(owner_id)),
    CONSTRAINT invocations_owner_kind_ck CHECK ((owner_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT invocations_payload_ck CHECK (insight_platform.is_bounded_object(payload, 1048576)),
    CONSTRAINT invocations_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT invocations_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT invocations_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT invocations_time_ck CHECK (((deadline >= created_at) AND (updated_at >= created_at) AND ((started_at IS NULL) OR (started_at >= created_at)) AND ((terminal_at IS NULL) OR (terminal_at >= created_at)))),
    CONSTRAINT invocations_trace_id_ck CHECK (insight_platform.is_trace_id(trace_id)),
    CONSTRAINT invocations_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.jobs (
    tenant_id text NOT NULL,
    job_id text NOT NULL,
    job_kind text NOT NULL,
    work_class text NOT NULL,
    owner_kind text NOT NULL,
    owner_id text NOT NULL,
    trace_id text NOT NULL,
    invocation_id text,
    run_id text,
    node_id text,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    attempt_no integer DEFAULT 0 NOT NULL,
    attempt_limit integer NOT NULL,
    lease_epoch bigint DEFAULT 0 NOT NULL,
    worker_id text,
    lease_token_digest text,
    lease_expires_at timestamp with time zone,
    heartbeat_at timestamp with time zone,
    scheduled_at timestamp with time zone NOT NULL,
    retry_at timestamp with time zone,
    deadline timestamp with time zone NOT NULL,
    priority smallint DEFAULT 0 NOT NULL,
    wake_kind text,
    wake_state text,
    wake_generation bigint DEFAULT 0 NOT NULL,
    request_digest text NOT NULL,
    result_digest text,
    effect_key_digest text,
    quota_reservation_id text,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    started_at timestamp with time zone,
    terminal_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    scheduler_partition_id smallint NOT NULL,
    execution_requirement_version integer NOT NULL,
    execution_requirement jsonb NOT NULL,
    execution_requirement_digest text NOT NULL,
    execution_requirement_family text GENERATED ALWAYS AS ((execution_requirement ->> 'family'::text)) STORED,
    execution_semantic_identity text GENERATED ALWAYS AS (
CASE (execution_requirement ->> 'family'::text)
    WHEN 'program'::text THEN (execution_requirement ->> 'program_semantic_identity'::text)
    WHEN 'agent_compilation'::text THEN (execution_requirement ->> 'compiler_semantic_identity'::text)
    WHEN 'domain_operation'::text THEN (execution_requirement ->> 'operation_abi_identity'::text)
    ELSE NULL::text
END) STORED,
    execution_ir_abi_version integer GENERATED ALWAYS AS (
CASE
    WHEN ((execution_requirement ->> 'family'::text) = 'program'::text) THEN ((execution_requirement ->> 'ir_abi_version'::text))::integer
    ELSE NULL::integer
END) STORED,
    execution_adapter_identity text GENERATED ALWAYS AS (
CASE
    WHEN (((execution_requirement ->> 'family'::text) = 'domain_operation'::text) AND (((execution_requirement -> 'requirements'::text) ->> 'kind'::text) = 'adapter'::text)) THEN ((execution_requirement -> 'requirements'::text) ->> 'protocol_adapter_identity'::text)
    ELSE NULL::text
END) STORED,
    attempt_build_digest text,
    CONSTRAINT jobs_attempt_build_ck CHECK (((attempt_build_digest IS NULL) OR insight_platform.is_sha256(attempt_build_digest))),
    CONSTRAINT jobs_attempt_ck CHECK (((attempt_no >= 0) AND (attempt_limit > 0) AND (attempt_no <= attempt_limit) AND (lease_epoch >= 0))),
    CONSTRAINT jobs_effect_key_ck CHECK (((effect_key_digest IS NULL) OR insight_platform.is_sha256(effect_key_digest))),
    CONSTRAINT jobs_execution_requirement_ck CHECK ((((execution_requirement_version IS NULL) AND (execution_requirement IS NULL) AND (execution_requirement_digest IS NULL)) OR ((execution_requirement_version IS NOT NULL) AND (execution_requirement_version = 1) AND (execution_requirement IS NOT NULL) AND insight_platform.is_bounded_object(execution_requirement, 16384) AND (execution_requirement_family IS NOT NULL) AND (execution_requirement_family = ANY (ARRAY['program'::text, 'agent_compilation'::text, 'domain_operation'::text])) AND (execution_semantic_identity IS NOT NULL) AND insight_platform.is_sha256(execution_semantic_identity) AND
CASE execution_requirement_family
    WHEN 'program'::text THEN ((execution_ir_abi_version IS NOT NULL) AND (execution_ir_abi_version > 0) AND (execution_adapter_identity IS NULL))
    WHEN 'agent_compilation'::text THEN ((execution_ir_abi_version IS NULL) AND (execution_adapter_identity IS NULL))
    WHEN 'domain_operation'::text THEN ((execution_ir_abi_version IS NULL) AND (((execution_requirement -> 'requirements'::text) ->> 'kind'::text) IS NOT NULL) AND
    CASE ((execution_requirement -> 'requirements'::text) ->> 'kind'::text)
        WHEN 'adapter'::text THEN ((execution_adapter_identity IS NOT NULL) AND insight_platform.is_sha256(execution_adapter_identity))
        WHEN 'validator'::text THEN (execution_adapter_identity IS NULL)
        WHEN 'control'::text THEN (execution_adapter_identity IS NULL)
        ELSE false
    END)
    ELSE false
END AND (execution_requirement_digest IS NOT NULL) AND insight_platform.is_sha256(execution_requirement_digest)))),
    CONSTRAINT jobs_id_ck CHECK (insight_platform.is_platform_id(job_id)),
    CONSTRAINT jobs_kind_ck CHECK ((job_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT jobs_lease_ck CHECK ((((worker_id IS NULL) AND (lease_token_digest IS NULL) AND (lease_expires_at IS NULL) AND (heartbeat_at IS NULL)) OR ((worker_id IS NOT NULL) AND (lease_token_digest IS NOT NULL) AND (lease_expires_at IS NOT NULL) AND (heartbeat_at IS NOT NULL)))),
    CONSTRAINT jobs_lease_token_ck CHECK (((lease_token_digest IS NULL) OR insight_platform.is_sha256(lease_token_digest))),
    CONSTRAINT jobs_owner_id_ck CHECK (insight_platform.is_platform_id(owner_id)),
    CONSTRAINT jobs_owner_kind_ck CHECK ((owner_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT jobs_payload_ck CHECK (insight_platform.is_bounded_object(payload, 1048576)),
    CONSTRAINT jobs_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT jobs_priority_ck CHECK (((priority >= '-32767'::integer) AND (priority <= 32767))),
    CONSTRAINT jobs_quota_reservation_ck CHECK (((quota_reservation_id IS NULL) OR insight_platform.is_platform_id(quota_reservation_id))),
    CONSTRAINT jobs_request_digest_ck CHECK (insight_platform.is_sha256(request_digest)),
    CONSTRAINT jobs_result_digest_ck CHECK (((result_digest IS NULL) OR insight_platform.is_sha256(result_digest))),
    CONSTRAINT jobs_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT jobs_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT jobs_time_ck CHECK (((deadline >= created_at) AND (updated_at >= created_at) AND ((started_at IS NULL) OR (started_at >= created_at)) AND ((terminal_at IS NULL) OR (terminal_at >= created_at)))),
    CONSTRAINT jobs_trace_id_ck CHECK (insight_platform.is_trace_id(trace_id)),
    CONSTRAINT jobs_version_ck CHECK ((version > 0)),
    CONSTRAINT jobs_wake_ck CHECK ((((wake_kind IS NULL) AND (wake_state IS NULL) AND (wake_generation = 0)) OR ((wake_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text) AND (wake_state ~ '^[a-z][a-z0-9_]{0,63}$'::text) AND (wake_generation > 0)))),
    CONSTRAINT jobs_work_class_ck CHECK ((work_class = ANY (ARRAY['registry_validation'::text, 'orchestration'::text, 'model'::text, 'capability_native'::text, 'capability_remote'::text, 'mcp'::text, 'context'::text, 'sandbox'::text, 'interaction'::text, 'artifact'::text, 'recovery'::text]))),
    CONSTRAINT jobs_worker_id_ck CHECK (((worker_id IS NULL) OR insight_platform.is_platform_id(worker_id)))
);

CREATE TABLE insight_platform.outbox_events (
    tenant_id text NOT NULL,
    outbox_id text NOT NULL,
    event_id text NOT NULL,
    trace_id text NOT NULL,
    state text DEFAULT 'pending'::text NOT NULL,
    publish_attempts integer DEFAULT 0 NOT NULL,
    next_publish_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    claim_owner text,
    claim_epoch bigint DEFAULT 0 NOT NULL,
    claim_expires_at timestamp with time zone,
    last_failure_code text,
    published_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT outbox_events_attempts_ck CHECK ((publish_attempts >= 0)),
    CONSTRAINT outbox_events_claim_ck CHECK ((((claim_owner IS NULL) AND (claim_expires_at IS NULL)) OR ((claim_owner IS NOT NULL) AND (claim_expires_at IS NOT NULL)))),
    CONSTRAINT outbox_events_claim_epoch_ck CHECK ((claim_epoch >= 0)),
    CONSTRAINT outbox_events_failure_ck CHECK (((last_failure_code IS NULL) OR (last_failure_code ~ '^[a-z][a-z0-9_.:]{0,127}$'::text))),
    CONSTRAINT outbox_events_id_ck CHECK (insight_platform.is_platform_id(outbox_id)),
    CONSTRAINT outbox_events_state_ck CHECK ((state = ANY (ARRAY['pending'::text, 'publishing'::text, 'retry'::text, 'published'::text, 'incompatible'::text]))),
    CONSTRAINT outbox_events_time_ck CHECK (((updated_at >= created_at) AND ((published_at IS NULL) OR (published_at >= created_at)))),
    CONSTRAINT outbox_events_trace_id_ck CHECK (insight_platform.is_trace_id(trace_id))
);

CREATE TABLE insight_platform.principals (
    principal_id text NOT NULL,
    state text NOT NULL,
    authentication_authority_digest text NOT NULL,
    subject_digest text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT principals_authority_digest_ck CHECK (insight_platform.is_sha256(authentication_authority_digest)),
    CONSTRAINT principals_id_ck CHECK (insight_platform.is_platform_id(principal_id)),
    CONSTRAINT principals_payload_ck CHECK (insight_platform.is_bounded_object(payload, 65536)),
    CONSTRAINT principals_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT principals_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT principals_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT principals_subject_digest_ck CHECK (insight_platform.is_sha256(subject_digest)),
    CONSTRAINT principals_time_ck CHECK ((updated_at >= created_at)),
    CONSTRAINT principals_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.quota_accounts (
    tenant_id text NOT NULL,
    quota_account_id text NOT NULL,
    scope_kind text NOT NULL,
    scope_id text NOT NULL,
    work_class text NOT NULL,
    metric text NOT NULL,
    limit_value bigint NOT NULL,
    reserved_value bigint DEFAULT 0 NOT NULL,
    used_value bigint DEFAULT 0 NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    payload_schema_version integer DEFAULT 1 NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT quota_accounts_id_ck CHECK (insight_platform.is_platform_id(quota_account_id)),
    CONSTRAINT quota_accounts_metric_ck CHECK ((metric ~ '^[a-z][a-z0-9_.]{0,127}$'::text)),
    CONSTRAINT quota_accounts_payload_ck CHECK (insight_platform.is_bounded_object(payload, 65536)),
    CONSTRAINT quota_accounts_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT quota_accounts_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT quota_accounts_scope_id_ck CHECK (insight_platform.is_platform_id(scope_id)),
    CONSTRAINT quota_accounts_scope_kind_ck CHECK ((scope_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT quota_accounts_time_ck CHECK ((updated_at >= created_at)),
    CONSTRAINT quota_accounts_values_ck CHECK (((limit_value >= 0) AND (reserved_value >= 0) AND (used_value >= 0) AND ((reserved_value + used_value) <= limit_value))),
    CONSTRAINT quota_accounts_version_ck CHECK ((version > 0)),
    CONSTRAINT quota_accounts_work_class_ck CHECK ((work_class ~ '^[a-z][a-z0-9_]{0,63}$'::text))
);

CREATE TABLE insight_platform.quota_ledger (
    tenant_id text NOT NULL,
    quota_entry_id text NOT NULL,
    quota_account_id text NOT NULL,
    correlation_id text NOT NULL,
    entry_kind text NOT NULL,
    reserved_amount bigint NOT NULL,
    used_amount bigint DEFAULT 0 NOT NULL,
    account_version bigint NOT NULL,
    request_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT quota_ledger_account_version_ck CHECK ((account_version > 0)),
    CONSTRAINT quota_ledger_amount_ck CHECK (((reserved_amount > 0) AND (used_amount >= 0) AND (used_amount <= reserved_amount) AND (((entry_kind = 'reserve'::text) AND (used_amount = 0)) OR (entry_kind = 'settle'::text)))),
    CONSTRAINT quota_ledger_correlation_ck CHECK (insight_platform.is_platform_id(correlation_id)),
    CONSTRAINT quota_ledger_id_ck CHECK (insight_platform.is_platform_id(quota_entry_id)),
    CONSTRAINT quota_ledger_kind_ck CHECK ((entry_kind = ANY (ARRAY['reserve'::text, 'settle'::text]))),
    CONSTRAINT quota_ledger_request_digest_ck CHECK (insight_platform.is_sha256(request_digest))
);

CREATE TABLE insight_platform.receipts (
    tenant_id text NOT NULL,
    receipt_id text NOT NULL,
    receipt_kind text NOT NULL,
    scope_kind text NOT NULL,
    scope_id text NOT NULL,
    dedupe_owner_id text NOT NULL,
    operation text NOT NULL,
    idempotency_key_digest text NOT NULL,
    request_digest text NOT NULL,
    state text NOT NULL,
    claim_generation bigint DEFAULT 1 NOT NULL,
    claim_owner text,
    claim_expires_at timestamp with time zone,
    disposition text,
    response_reference_id text,
    payload_schema_version integer DEFAULT 1 NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    completed_at timestamp with time zone,
    expires_at timestamp with time zone NOT NULL,
    CONSTRAINT receipts_claim_ck CHECK ((((claim_owner IS NULL) AND (claim_expires_at IS NULL)) OR ((claim_owner IS NOT NULL) AND (claim_expires_at IS NOT NULL)))),
    CONSTRAINT receipts_claim_generation_ck CHECK ((claim_generation > 0)),
    CONSTRAINT receipts_dedupe_owner_id_ck CHECK (insight_platform.is_platform_id(dedupe_owner_id)),
    CONSTRAINT receipts_disposition_ck CHECK (((disposition IS NULL) OR (disposition ~ '^[a-z][a-z0-9_.:]{0,127}$'::text))),
    CONSTRAINT receipts_id_ck CHECK (insight_platform.is_platform_id(receipt_id)),
    CONSTRAINT receipts_idempotency_ck CHECK (insight_platform.is_sha256(idempotency_key_digest)),
    CONSTRAINT receipts_kind_ck CHECK ((receipt_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT receipts_operation_ck CHECK ((operation ~ '^[a-z][a-z0-9_.]{0,127}$'::text)),
    CONSTRAINT receipts_payload_ck CHECK (insight_platform.is_bounded_object(payload, 262144)),
    CONSTRAINT receipts_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT receipts_request_digest_ck CHECK (insight_platform.is_sha256(request_digest)),
    CONSTRAINT receipts_response_reference_ck CHECK (((response_reference_id IS NULL) OR insight_platform.is_platform_id(response_reference_id))),
    CONSTRAINT receipts_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT receipts_scope_id_ck CHECK (insight_platform.is_platform_id(scope_id)),
    CONSTRAINT receipts_scope_kind_ck CHECK ((scope_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT receipts_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT receipts_time_ck CHECK (((expires_at > created_at) AND ((completed_at IS NULL) OR (completed_at >= created_at))))
);

CREATE TABLE insight_platform.resource_versions (
    tenant_id text NOT NULL,
    resource_version_id text NOT NULL,
    resource_id text NOT NULL,
    resource_version_kind text NOT NULL,
    revision_no bigint NOT NULL,
    content_digest text NOT NULL,
    artifact_id text,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    created_by text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT resource_versions_artifact_id_ck CHECK (((artifact_id IS NULL) OR insight_platform.is_platform_id(artifact_id))),
    CONSTRAINT resource_versions_content_digest_ck CHECK (insight_platform.is_sha256(content_digest)),
    CONSTRAINT resource_versions_created_by_ck CHECK (insight_platform.is_platform_id(created_by)),
    CONSTRAINT resource_versions_id_ck CHECK (insight_platform.is_platform_id(resource_version_id)),
    CONSTRAINT resource_versions_kind_ck CHECK ((resource_version_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT resource_versions_payload_ck CHECK (insight_platform.is_bounded_object(payload, 1048576)),
    CONSTRAINT resource_versions_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT resource_versions_revision_ck CHECK ((revision_no > 0)),
    CONSTRAINT resource_versions_schema_version_ck CHECK ((payload_schema_version > 0))
);

CREATE TABLE insight_platform.resources (
    tenant_id text NOT NULL,
    resource_id text NOT NULL,
    resource_kind text NOT NULL,
    lifecycle_state text NOT NULL,
    gate_state text NOT NULL,
    draft_generation bigint DEFAULT 1 NOT NULL,
    active_version_id text,
    active_deployment_id text,
    version bigint DEFAULT 1 NOT NULL,
    payload_schema_version integer DEFAULT 1 NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT resources_active_deployment_id_ck CHECK (((active_deployment_id IS NULL) OR insight_platform.is_platform_id(active_deployment_id))),
    CONSTRAINT resources_alias_ck CHECK ((NOT (payload ? 'alias') OR payload -> 'alias' = 'null'::jsonb OR (jsonb_typeof(payload -> 'alias') = 'string' AND octet_length(payload ->> 'alias') <= 64 AND (payload ->> 'alias') ~ '^[a-z][a-z0-9._-]{0,63}$'))),
    CONSTRAINT resources_active_target_ck CHECK (((active_version_id IS NULL) OR (active_deployment_id IS NULL))),
    CONSTRAINT resources_active_version_id_ck CHECK (((active_version_id IS NULL) OR insight_platform.is_platform_id(active_version_id))),
    CONSTRAINT resources_draft_generation_ck CHECK ((draft_generation > 0)),
    CONSTRAINT resources_gate_ck CHECK ((gate_state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT resources_id_ck CHECK (insight_platform.is_platform_id(resource_id)),
    CONSTRAINT resources_kind_ck CHECK ((resource_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT resources_lifecycle_ck CHECK ((lifecycle_state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT resources_payload_ck CHECK (insight_platform.is_bounded_object(payload, 262144)),
    CONSTRAINT resources_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT resources_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT resources_time_ck CHECK ((updated_at >= created_at)),
    CONSTRAINT resources_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.run_nodes (
    tenant_id text NOT NULL,
    node_id text NOT NULL,
    run_id text NOT NULL,
    parent_node_id text,
    record_kind text NOT NULL,
    scope_id text NOT NULL,
    plan_node_key text,
    activation_ordinal integer,
    related_run_id text,
    logical_key text NOT NULL,
    node_kind text NOT NULL,
    state text NOT NULL,
    generation bigint DEFAULT 1 NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    enqueue_round bigint,
    payload_schema_version integer DEFAULT 1 NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    payload_digest text NOT NULL,
    retry_at timestamp with time zone,
    deadline timestamp with time zone NOT NULL,
    started_at timestamp with time zone,
    terminal_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT run_nodes_enqueue_round_ck CHECK (((enqueue_round IS NULL) OR (enqueue_round >= 0))),
    CONSTRAINT run_nodes_generation_ck CHECK ((generation > 0)),
    CONSTRAINT run_nodes_id_ck CHECK (insight_platform.is_platform_id(node_id)),
    CONSTRAINT run_nodes_kind_ck CHECK ((node_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT run_nodes_logical_key_ck CHECK (((octet_length(logical_key) >= 1) AND (octet_length(logical_key) <= 255))),
    CONSTRAINT run_nodes_parent_id_ck CHECK (((parent_node_id IS NULL) OR insight_platform.is_platform_id(parent_node_id))),
    CONSTRAINT run_nodes_payload_ck CHECK (insight_platform.is_bounded_object(payload, 262144)),
    CONSTRAINT run_nodes_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT run_nodes_plan_identity_ck CHECK ((((record_kind = 'node_execution'::text) AND (plan_node_key IS NOT NULL) AND (activation_ordinal IS NOT NULL) AND (activation_ordinal > 0)) OR ((record_kind <> 'node_execution'::text) AND (plan_node_key IS NULL) AND (activation_ordinal IS NULL)))),
    CONSTRAINT run_nodes_plan_key_ck CHECK (((plan_node_key IS NULL) OR ((octet_length(plan_node_key) >= 1) AND (octet_length(plan_node_key) <= 128)))),
    CONSTRAINT run_nodes_record_kind_ck CHECK ((record_kind = ANY (ARRAY['node_execution'::text, 'scope_instance'::text, 'child_run_link'::text]))),
    CONSTRAINT run_nodes_related_run_ck CHECK (((related_run_id IS NULL) OR insight_platform.is_platform_id(related_run_id))),
    CONSTRAINT run_nodes_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT run_nodes_scope_id_ck CHECK (insight_platform.is_platform_id(scope_id)),
    CONSTRAINT run_nodes_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT run_nodes_time_ck CHECK (((deadline >= created_at) AND (updated_at >= created_at) AND ((started_at IS NULL) OR (started_at >= created_at)) AND ((terminal_at IS NULL) OR (terminal_at >= created_at)))),
    CONSTRAINT run_nodes_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.run_values (
    tenant_id text NOT NULL,
    value_id text NOT NULL,
    run_id text NOT NULL,
    node_id text,
    value_kind text NOT NULL,
    classification text NOT NULL,
    schema_digest text NOT NULL,
    content_digest text NOT NULL,
    inline_value jsonb,
    artifact_id text,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT run_values_classification_ck CHECK ((classification ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT run_values_content_digest_ck CHECK (insight_platform.is_sha256(content_digest)),
    CONSTRAINT run_values_id_ck CHECK (insight_platform.is_platform_id(value_id)),
    CONSTRAINT run_values_kind_ck CHECK ((value_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT run_values_schema_digest_ck CHECK (insight_platform.is_sha256(schema_digest)),
    CONSTRAINT run_values_storage_ck CHECK ((((inline_value IS NOT NULL) AND (artifact_id IS NULL) AND (octet_length((inline_value)::text) <= 1048576)) OR ((inline_value IS NULL) AND (artifact_id IS NOT NULL))))
);

CREATE TABLE insight_platform.runs (
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    root_run_id text NOT NULL,
    parent_run_id text,
    parent_node_id text,
    agent_deployment_id text NOT NULL,
    principal_id text NOT NULL,
    trace_id text NOT NULL,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    bindings_schema_version integer NOT NULL,
    bindings jsonb NOT NULL,
    bindings_digest text NOT NULL,
    current_schema_version integer NOT NULL,
    current_payload jsonb NOT NULL,
    current_payload_digest text NOT NULL,
    input_value_id text,
    output_value_id text,
    depth integer DEFAULT 0 NOT NULL,
    descendant_count integer DEFAULT 0 NOT NULL,
    active_work_count integer DEFAULT 0 NOT NULL,
    pause_generation bigint DEFAULT 0 NOT NULL,
    cancel_generation bigint DEFAULT 0 NOT NULL,
    timeout_generation bigint DEFAULT 0 NOT NULL,
    public_sequence bigint DEFAULT 0 NOT NULL,
    retry_at timestamp with time zone,
    deadline timestamp with time zone NOT NULL,
    started_at timestamp with time zone,
    terminal_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    execution_requirement_version integer NOT NULL,
    execution_requirement jsonb NOT NULL,
    execution_requirement_digest text NOT NULL,
    public_replay_floor bigint DEFAULT 0 NOT NULL,
    history_holds jsonb DEFAULT '{"holds": {}, "schema_version": 1}'::jsonb NOT NULL,
    CONSTRAINT runs_ancestry_ck CHECK (((depth >= 0) AND (depth <= 32) AND (descendant_count >= 0) AND (((depth = 0) AND (root_run_id = run_id) AND (parent_run_id IS NULL) AND (parent_node_id IS NULL)) OR ((depth > 0) AND (root_run_id <> run_id) AND (parent_run_id IS NOT NULL) AND (parent_node_id IS NOT NULL))))),
    CONSTRAINT runs_bindings_ck CHECK (insight_platform.is_bounded_object(bindings, 1048576)),
    CONSTRAINT runs_bindings_digest_ck CHECK (insight_platform.is_sha256(bindings_digest)),
    CONSTRAINT runs_bindings_version_ck CHECK ((bindings_schema_version > 0)),
    CONSTRAINT runs_counters_ck CHECK (((active_work_count >= 0) AND (pause_generation >= 0) AND (cancel_generation >= 0) AND (timeout_generation >= 0) AND (public_sequence >= 0))),
    CONSTRAINT runs_current_digest_ck CHECK (insight_platform.is_sha256(current_payload_digest)),
    CONSTRAINT runs_current_payload_ck CHECK (insight_platform.is_bounded_object(current_payload, 1048576)),
    CONSTRAINT runs_current_version_ck CHECK ((current_schema_version > 0)),
    CONSTRAINT runs_execution_requirement_ck CHECK ((((execution_requirement_version IS NULL) AND (execution_requirement IS NULL) AND (execution_requirement_digest IS NULL)) OR ((execution_requirement_version IS NOT NULL) AND (execution_requirement_version = 1) AND (execution_requirement IS NOT NULL) AND insight_platform.is_bounded_object(execution_requirement, 16384) AND ((execution_requirement ->> 'family'::text) IS NOT NULL) AND ((execution_requirement ->> 'family'::text) = 'program'::text) AND ((execution_requirement ->> 'definition_digest'::text) IS NOT NULL) AND insight_platform.is_sha256((execution_requirement ->> 'definition_digest'::text)) AND ((execution_requirement ->> 'program_semantic_identity'::text) IS NOT NULL) AND insight_platform.is_sha256((execution_requirement ->> 'program_semantic_identity'::text)) AND ((execution_requirement ->> 'ir_abi_version'::text) IS NOT NULL) AND (((execution_requirement ->> 'ir_abi_version'::text))::integer > 0) AND (execution_requirement_digest IS NOT NULL) AND insight_platform.is_sha256(execution_requirement_digest)))),
    CONSTRAINT runs_history_holds_ck CHECK ((insight_platform.is_bounded_object(history_holds, 16384) AND ((history_holds ->> 'schema_version'::text) IS NOT NULL) AND ((history_holds ->> 'schema_version'::text) = '1'::text) AND ((history_holds -> 'holds'::text) IS NOT NULL) AND (jsonb_typeof((history_holds -> 'holds'::text)) = 'object'::text))),
    CONSTRAINT runs_id_ck CHECK (insight_platform.is_platform_id(run_id)),
    CONSTRAINT runs_public_replay_floor_ck CHECK (((public_replay_floor >= 0) AND (public_replay_floor <= public_sequence))),
    CONSTRAINT runs_relation_id_ck CHECK ((insight_platform.is_platform_id(root_run_id) AND ((parent_run_id IS NULL) OR insight_platform.is_platform_id(parent_run_id)) AND ((parent_node_id IS NULL) OR insight_platform.is_platform_id(parent_node_id)))),
    CONSTRAINT runs_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT runs_time_ck CHECK (((deadline >= created_at) AND (updated_at >= created_at) AND ((started_at IS NULL) OR (started_at >= created_at)) AND ((terminal_at IS NULL) OR (terminal_at >= created_at)))),
    CONSTRAINT runs_trace_id_ck CHECK (insight_platform.is_trace_id(trace_id)),
    CONSTRAINT runs_value_id_ck CHECK ((((input_value_id IS NULL) OR insight_platform.is_platform_id(input_value_id)) AND ((output_value_id IS NULL) OR insight_platform.is_platform_id(output_value_id)))),
    CONSTRAINT runs_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.scheduler_state (
    work_class text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    current_round bigint DEFAULT 0 NOT NULL,
    cursor_tenant_id text,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    partition_id smallint NOT NULL,
    tenant_upper_bound text,
    CONSTRAINT scheduler_state_cursor_bound_ck CHECK (((cursor_tenant_id IS NULL) OR ((tenant_upper_bound IS NOT NULL) AND (cursor_tenant_id <= tenant_upper_bound)))),
    CONSTRAINT scheduler_state_cursor_ck CHECK (((cursor_tenant_id IS NULL) OR insight_platform.is_platform_id(cursor_tenant_id))),
    CONSTRAINT scheduler_state_partition_ck CHECK (((partition_id >= 0) AND (partition_id <= 255))),
    CONSTRAINT scheduler_state_round_ck CHECK ((current_round >= 0)),
    CONSTRAINT scheduler_state_upper_ck CHECK (((tenant_upper_bound IS NULL) OR insight_platform.is_platform_id(tenant_upper_bound))),
    CONSTRAINT scheduler_state_version_ck CHECK ((version > 0)),
    CONSTRAINT scheduler_state_work_class_ck CHECK ((work_class ~ '^[a-z][a-z0-9_]{0,63}$'::text))
);

CREATE TABLE insight_platform.scheduler_tenant_state (
    tenant_id text NOT NULL,
    work_class text NOT NULL,
    partition_id smallint NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    policy_version_id text,
    policy_version_digest text,
    rules_digest text,
    deficit bigint DEFAULT 0 NOT NULL,
    earliest_eligible_round bigint DEFAULT 0 NOT NULL,
    credited_round bigint,
    last_served_round bigint,
    successful_claims bigint DEFAULT 0 NOT NULL,
    job_creation_cutoff timestamp with time zone,
    job_upper_created_at timestamp with time zone,
    job_upper_id text,
    job_cursor_created_at timestamp with time zone,
    job_cursor_id text,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT scheduler_tenant_accounting_ck CHECK (((deficit >= 0) AND (earliest_eligible_round >= 0) AND (successful_claims >= 0) AND ((credited_round IS NULL) OR (credited_round >= 0)) AND ((last_served_round IS NULL) OR (last_served_round >= 0)))),
    CONSTRAINT scheduler_tenant_job_sweep_ck CHECK ((((job_creation_cutoff IS NULL) AND (job_upper_created_at IS NULL) AND (job_upper_id IS NULL) AND (job_cursor_created_at IS NULL) AND (job_cursor_id IS NULL)) OR ((job_creation_cutoff IS NOT NULL) AND (job_upper_created_at IS NOT NULL) AND (job_upper_created_at < job_creation_cutoff) AND (job_upper_id IS NOT NULL) AND insight_platform.is_platform_id(job_upper_id) AND (((job_cursor_created_at IS NULL) AND (job_cursor_id IS NULL)) OR ((job_cursor_created_at IS NOT NULL) AND (job_cursor_id IS NOT NULL) AND insight_platform.is_platform_id(job_cursor_id) AND (ROW(job_cursor_created_at, job_cursor_id) <= ROW(job_upper_created_at, job_upper_id))))))),
    CONSTRAINT scheduler_tenant_policy_ck CHECK ((((policy_version_id IS NULL) AND (policy_version_digest IS NULL) AND (rules_digest IS NULL) AND (deficit = 0)) OR ((policy_version_id IS NOT NULL) AND insight_platform.is_platform_id(policy_version_id) AND (policy_version_digest IS NOT NULL) AND insight_platform.is_sha256(policy_version_digest) AND (rules_digest IS NOT NULL) AND insight_platform.is_sha256(rules_digest)))),
    CONSTRAINT scheduler_tenant_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.secret_bindings (
    tenant_id text NOT NULL,
    secret_binding_id text NOT NULL,
    purpose text NOT NULL,
    provider text NOT NULL,
    state text NOT NULL,
    generation bigint DEFAULT 1 NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    opaque_reference_ciphertext bytea NOT NULL,
    key_id text NOT NULL,
    reference_digest text NOT NULL,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    revoked_at timestamp with time zone,
    CONSTRAINT secret_bindings_ciphertext_ck CHECK (((octet_length(opaque_reference_ciphertext) >= 1) AND (octet_length(opaque_reference_ciphertext) <= 16384))),
    CONSTRAINT secret_bindings_digest_ck CHECK (insight_platform.is_sha256(reference_digest)),
    CONSTRAINT secret_bindings_generation_ck CHECK ((generation > 0)),
    CONSTRAINT secret_bindings_id_ck CHECK (insight_platform.is_platform_id(secret_binding_id)),
    CONSTRAINT secret_bindings_key_id_ck CHECK (((octet_length(key_id) >= 1) AND (octet_length(key_id) <= 255))),
    CONSTRAINT secret_bindings_payload_ck CHECK (insight_platform.is_bounded_object(payload, 65536)),
    CONSTRAINT secret_bindings_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT secret_bindings_provider_ck CHECK ((provider ~ '^[a-z][a-z0-9_.-]{0,127}$'::text)),
    CONSTRAINT secret_bindings_purpose_ck CHECK ((purpose ~ '^[a-z][a-z0-9_.:]{0,127}$'::text)),
    CONSTRAINT secret_bindings_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT secret_bindings_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT secret_bindings_time_ck CHECK (((updated_at >= created_at) AND ((revoked_at IS NULL) OR (revoked_at >= created_at)))),
    CONSTRAINT secret_bindings_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.tasks (
    tenant_id text NOT NULL,
    task_id text NOT NULL,
    trace_id text NOT NULL,
    task_kind text NOT NULL,
    owner_kind text NOT NULL,
    owner_id text NOT NULL,
    run_id text,
    node_id text,
    invocation_id text,
    state text NOT NULL,
    generation bigint DEFAULT 1 NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    response_schema_digest text,
    principal_snapshot_schema_version integer NOT NULL,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    response_value_id text,
    deadline timestamp with time zone NOT NULL,
    responded_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    current_cleanup_job_id text,
    CONSTRAINT tasks_cleanup_id_ck CHECK (((current_cleanup_job_id IS NULL) OR insight_platform.is_platform_id(current_cleanup_job_id))),
    CONSTRAINT tasks_generation_ck CHECK ((generation > 0)),
    CONSTRAINT tasks_id_ck CHECK (insight_platform.is_platform_id(task_id)),
    CONSTRAINT tasks_kind_ck CHECK ((task_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT tasks_owner_id_ck CHECK (insight_platform.is_platform_id(owner_id)),
    CONSTRAINT tasks_owner_kind_ck CHECK ((owner_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT tasks_payload_ck CHECK (insight_platform.is_bounded_object(payload, 262144)),
    CONSTRAINT tasks_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT tasks_response_schema_ck CHECK (((response_schema_digest IS NULL) OR insight_platform.is_sha256(response_schema_digest))),
    CONSTRAINT tasks_schema_version_ck CHECK ((payload_schema_version > 0)),
    CONSTRAINT tasks_snapshot_version_ck CHECK ((principal_snapshot_schema_version > 0)),
    CONSTRAINT tasks_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT tasks_time_ck CHECK (((deadline >= created_at) AND (updated_at >= created_at) AND ((responded_at IS NULL) OR (responded_at >= created_at)))),
    CONSTRAINT tasks_trace_id_ck CHECK (insight_platform.is_trace_id(trace_id)),
    CONSTRAINT tasks_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.tenant_principals (
    tenant_id text NOT NULL,
    principal_id text NOT NULL,
    principal_kind text NOT NULL,
    state text NOT NULL,
    generation bigint DEFAULT 1 NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    permissions_schema_version integer DEFAULT 1 NOT NULL,
    permissions jsonb NOT NULL,
    permissions_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT tenant_principals_generation_ck CHECK ((generation > 0)),
    CONSTRAINT tenant_principals_kind_ck CHECK ((principal_kind ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT tenant_principals_permissions_ck CHECK (insight_platform.is_bounded_object(permissions, 65536)),
    CONSTRAINT tenant_principals_permissions_digest_ck CHECK (insight_platform.is_sha256(permissions_digest)),
    CONSTRAINT tenant_principals_schema_version_ck CHECK ((permissions_schema_version > 0)),
    CONSTRAINT tenant_principals_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT tenant_principals_time_ck CHECK ((updated_at >= created_at)),
    CONSTRAINT tenant_principals_version_ck CHECK ((version > 0))
);

CREATE TABLE insight_platform.tenants (
    tenant_id text NOT NULL,
    state text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    config_schema_version integer DEFAULT 1 NOT NULL,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    config_digest text NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    scheduler_partition_id smallint NOT NULL,
    CONSTRAINT tenants_config_ck CHECK (insight_platform.is_bounded_object(config, 65536)),
    CONSTRAINT tenants_config_digest_ck CHECK (insight_platform.is_sha256(config_digest)),
    CONSTRAINT tenants_config_version_ck CHECK ((config_schema_version > 0)),
    CONSTRAINT tenants_id_ck CHECK (insight_platform.is_platform_id(tenant_id)),
    CONSTRAINT tenants_scheduler_partition_ck CHECK (((scheduler_partition_id >= 0) AND (scheduler_partition_id <= 255))),
    CONSTRAINT tenants_state_ck CHECK ((state ~ '^[a-z][a-z0-9_]{0,63}$'::text)),
    CONSTRAINT tenants_time_ck CHECK ((updated_at >= created_at)),
    CONSTRAINT tenants_version_ck CHECK ((version > 0))
);

ALTER TABLE ONLY insight_platform.artifact_blobs
    ADD CONSTRAINT artifact_blobs_pkey PRIMARY KEY (tenant_id, blob_id);

ALTER TABLE ONLY insight_platform.artifact_links
    ADD CONSTRAINT artifact_links_key_uq UNIQUE (tenant_id, link_kind, owner_kind, owner_id, link_key_digest);

ALTER TABLE ONLY insight_platform.artifact_links
    ADD CONSTRAINT artifact_links_pkey PRIMARY KEY (tenant_id, artifact_link_id);

ALTER TABLE ONLY insight_platform.artifacts
    ADD CONSTRAINT artifacts_pkey PRIMARY KEY (tenant_id, artifact_id);

ALTER TABLE ONLY insight_platform.deployments
    ADD CONSTRAINT deployments_closure_uq UNIQUE (tenant_id, resource_id, resource_version_id, environment, bindings_digest);

ALTER TABLE ONLY insight_platform.deployments
    ADD CONSTRAINT deployments_pkey PRIMARY KEY (tenant_id, deployment_id);

ALTER TABLE ONLY insight_platform.deployments
    ADD CONSTRAINT deployments_resource_id_uq UNIQUE (tenant_id, resource_id, deployment_id);

ALTER TABLE ONLY insight_platform.events
    ADD CONSTRAINT events_aggregate_version_type_uq UNIQUE (tenant_id, aggregate_kind, aggregate_id, aggregate_version, event_type);

ALTER TABLE ONLY insight_platform.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (event_id);

ALTER TABLE ONLY insight_platform.events
    ADD CONSTRAINT events_public_sequence_uq UNIQUE (tenant_id, run_id, public_sequence);

ALTER TABLE ONLY insight_platform.events
    ADD CONSTRAINT events_tenant_event_trace_uq UNIQUE (tenant_id, event_id, trace_id);

ALTER TABLE ONLY insight_platform.events
    ADD CONSTRAINT events_tenant_event_uq UNIQUE (tenant_id, event_id);

ALTER TABLE ONLY insight_platform.invocations
    ADD CONSTRAINT invocations_owner_key_uq UNIQUE (tenant_id, owner_kind, owner_id, invocation_kind, logical_key);

ALTER TABLE ONLY insight_platform.invocations
    ADD CONSTRAINT invocations_pkey PRIMARY KEY (tenant_id, invocation_id);

ALTER TABLE ONLY insight_platform.jobs
    ADD CONSTRAINT jobs_pkey PRIMARY KEY (tenant_id, job_id);

ALTER TABLE ONLY insight_platform.outbox_events
    ADD CONSTRAINT outbox_events_event_uq UNIQUE (tenant_id, event_id);

ALTER TABLE ONLY insight_platform.outbox_events
    ADD CONSTRAINT outbox_events_pkey PRIMARY KEY (tenant_id, outbox_id);

ALTER TABLE ONLY insight_platform.principals
    ADD CONSTRAINT principals_external_identity_uq UNIQUE (authentication_authority_digest, subject_digest);

ALTER TABLE ONLY insight_platform.principals
    ADD CONSTRAINT principals_pkey PRIMARY KEY (principal_id);

ALTER TABLE ONLY insight_platform.quota_accounts
    ADD CONSTRAINT quota_accounts_pkey PRIMARY KEY (tenant_id, quota_account_id);

ALTER TABLE ONLY insight_platform.quota_accounts
    ADD CONSTRAINT quota_accounts_scope_uq UNIQUE (tenant_id, scope_kind, scope_id, work_class, metric);

ALTER TABLE ONLY insight_platform.quota_ledger
    ADD CONSTRAINT quota_ledger_pkey PRIMARY KEY (tenant_id, quota_entry_id);

ALTER TABLE ONLY insight_platform.quota_ledger
    ADD CONSTRAINT quota_ledger_replay_uq UNIQUE (tenant_id, quota_account_id, correlation_id, entry_kind);

ALTER TABLE ONLY insight_platform.receipts
    ADD CONSTRAINT receipts_idempotency_uq UNIQUE (tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id, operation, idempotency_key_digest);

ALTER TABLE ONLY insight_platform.receipts
    ADD CONSTRAINT receipts_pkey PRIMARY KEY (tenant_id, receipt_id);

ALTER TABLE ONLY insight_platform.resource_versions
    ADD CONSTRAINT resource_versions_pkey PRIMARY KEY (tenant_id, resource_version_id);

ALTER TABLE ONLY insight_platform.resource_versions
    ADD CONSTRAINT resource_versions_resource_id_uq UNIQUE (tenant_id, resource_id, resource_version_id);

ALTER TABLE ONLY insight_platform.resource_versions
    ADD CONSTRAINT resource_versions_revision_uq UNIQUE (tenant_id, resource_id, resource_version_kind, revision_no);

ALTER TABLE ONLY insight_platform.resources
    ADD CONSTRAINT resources_pkey PRIMARY KEY (tenant_id, resource_id);

ALTER TABLE ONLY insight_platform.run_nodes
    ADD CONSTRAINT run_nodes_pkey PRIMARY KEY (tenant_id, node_id);

ALTER TABLE ONLY insight_platform.run_nodes
    ADD CONSTRAINT run_nodes_run_logical_uq UNIQUE (tenant_id, run_id, logical_key);

ALTER TABLE ONLY insight_platform.run_nodes
    ADD CONSTRAINT run_nodes_run_node_uq UNIQUE (tenant_id, run_id, node_id);

ALTER TABLE ONLY insight_platform.run_values
    ADD CONSTRAINT run_values_pkey PRIMARY KEY (tenant_id, value_id);

ALTER TABLE ONLY insight_platform.runs
    ADD CONSTRAINT runs_execution_requirement_route_uq UNIQUE (tenant_id, run_id, execution_requirement_digest);

ALTER TABLE ONLY insight_platform.runs
    ADD CONSTRAINT runs_pkey PRIMARY KEY (tenant_id, run_id);

ALTER TABLE ONLY insight_platform.scheduler_state
    ADD CONSTRAINT scheduler_state_pkey PRIMARY KEY (work_class, partition_id);

ALTER TABLE ONLY insight_platform.scheduler_tenant_state
    ADD CONSTRAINT scheduler_tenant_state_pkey PRIMARY KEY (tenant_id, work_class);

ALTER TABLE ONLY insight_platform.secret_bindings
    ADD CONSTRAINT secret_bindings_pkey PRIMARY KEY (tenant_id, secret_binding_id);

ALTER TABLE ONLY insight_platform.tasks
    ADD CONSTRAINT tasks_pkey PRIMARY KEY (tenant_id, task_id);

ALTER TABLE ONLY insight_platform.tenant_principals
    ADD CONSTRAINT tenant_principals_pkey PRIMARY KEY (tenant_id, principal_id, principal_kind);

ALTER TABLE ONLY insight_platform.tenants
    ADD CONSTRAINT tenants_pkey PRIMARY KEY (tenant_id);

ALTER TABLE ONLY insight_platform.tenants
    ADD CONSTRAINT tenants_scheduler_route_uq UNIQUE (tenant_id, scheduler_partition_id);

CREATE UNIQUE INDEX artifact_blobs_content_uq ON insight_platform.artifact_blobs USING btree (tenant_id, backend, storage_binding_digest, encryption_domain_id, security_domain_digest, content_digest) WHERE ((state = 'verified'::text) AND (deleted_at IS NULL));

CREATE INDEX artifact_blobs_state_idx ON insight_platform.artifact_blobs USING btree (tenant_id, state, updated_at, blob_id);

CREATE INDEX artifact_links_source_idx ON insight_platform.artifact_links USING btree (tenant_id, source_artifact_id, link_kind, state) WHERE (released_at IS NULL);

CREATE INDEX artifact_links_target_idx ON insight_platform.artifact_links USING btree (tenant_id, target_artifact_id, link_kind, state) WHERE (released_at IS NULL);

CREATE INDEX artifacts_lookup_idx ON insight_platform.artifacts USING btree (tenant_id, state, purpose, artifact_id);

CREATE INDEX artifacts_retention_idx ON insight_platform.artifacts USING btree (tenant_id, state, retain_until, artifact_id) WHERE (terminal_at IS NULL);

CREATE INDEX events_aggregate_idx ON insight_platform.events USING btree (tenant_id, aggregate_kind, aggregate_id, aggregate_version, occurred_at, event_id);

CREATE INDEX events_run_idx ON insight_platform.events USING btree (tenant_id, run_id, public_sequence, occurred_at, event_id) WHERE (run_id IS NOT NULL);

CREATE INDEX invocations_drive_idx ON insight_platform.invocations USING btree (tenant_id, invocation_kind, state, COALESCE(retry_at, deadline), invocation_id) WHERE (terminal_at IS NULL);

CREATE INDEX jobs_claim_idx ON insight_platform.jobs USING btree (work_class, job_kind, state, COALESCE(retry_at, scheduled_at), priority DESC, job_id) WHERE ((terminal_at IS NULL) AND (worker_id IS NULL));

CREATE INDEX jobs_creation_sweep_idx ON insight_platform.jobs USING btree (work_class, scheduler_partition_id, tenant_id, created_at, job_id) WHERE (terminal_at IS NULL);

CREATE INDEX jobs_execution_route_idx ON insight_platform.jobs USING btree (work_class, scheduler_partition_id, execution_requirement_family, execution_semantic_identity, execution_ir_abi_version, execution_adapter_identity, tenant_id, created_at, job_id) WHERE (terminal_at IS NULL);

CREATE INDEX jobs_lease_idx ON insight_platform.jobs USING btree (lease_expires_at, tenant_id, job_id) WHERE ((terminal_at IS NULL) AND (lease_expires_at IS NOT NULL));

CREATE UNIQUE INDEX jobs_live_owner_uq ON insight_platform.jobs USING btree (tenant_id, work_class, owner_kind, owner_id) WHERE (terminal_at IS NULL);

CREATE INDEX jobs_partition_due_probe_idx ON insight_platform.jobs USING btree (work_class, scheduler_partition_id, tenant_id, COALESCE(retry_at, scheduled_at), priority DESC, job_id) WHERE ((terminal_at IS NULL) AND (worker_id IS NULL));

CREATE INDEX outbox_events_publish_idx ON insight_platform.outbox_events USING btree (next_publish_at, tenant_id, outbox_id) WHERE (published_at IS NULL);

CREATE INDEX quota_ledger_correlation_idx ON insight_platform.quota_ledger USING btree (tenant_id, correlation_id, created_at, quota_entry_id);

CREATE INDEX receipts_expiry_idx ON insight_platform.receipts USING btree (expires_at, tenant_id, receipt_id);

CREATE INDEX resources_registry_idx ON insight_platform.resources USING btree (tenant_id, resource_kind, lifecycle_state, resource_id);
CREATE UNIQUE INDEX resources_alias_uq ON insight_platform.resources USING btree (tenant_id, resource_kind, (payload ->> 'alias')) WHERE (payload ->> 'alias') IS NOT NULL;

CREATE UNIQUE INDEX run_nodes_child_run_uq ON insight_platform.run_nodes USING btree (tenant_id, related_run_id) WHERE ((record_kind = 'child_run_link'::text) AND (related_run_id IS NOT NULL));

CREATE INDEX run_nodes_drive_idx ON insight_platform.run_nodes USING btree (tenant_id, state, COALESCE(retry_at, deadline), COALESCE(enqueue_round, (0)::bigint), node_id) WHERE (terminal_at IS NULL);

CREATE INDEX run_values_read_idx ON insight_platform.run_values USING btree (tenant_id, run_id, created_at DESC, value_id DESC);

CREATE INDEX runs_children_read_idx ON insight_platform.runs USING btree (tenant_id, parent_run_id, created_at DESC, run_id DESC) WHERE (parent_run_id IS NOT NULL);

CREATE INDEX run_values_owner_idx ON insight_platform.run_values USING btree (tenant_id, run_id, node_id, value_id);

CREATE INDEX runs_drive_idx ON insight_platform.runs USING btree (tenant_id, state, COALESCE(retry_at, deadline), run_id) WHERE (terminal_at IS NULL);

CREATE INDEX scheduler_tenant_partition_idx ON insight_platform.scheduler_tenant_state USING btree (work_class, partition_id, tenant_id);

CREATE INDEX secret_bindings_lookup_idx ON insight_platform.secret_bindings USING btree (tenant_id, purpose, state, secret_binding_id);

CREATE INDEX tasks_due_idx ON insight_platform.tasks USING btree (tenant_id, state, deadline, task_id) WHERE (responded_at IS NULL);

CREATE INDEX tasks_inbox_created_idx ON insight_platform.tasks USING btree (tenant_id, created_at DESC, task_id DESC);

CREATE UNIQUE INDEX tasks_live_owner_uq ON insight_platform.tasks USING btree (tenant_id, task_kind, owner_kind, owner_id, generation) WHERE (responded_at IS NULL);

CREATE INDEX tenant_principals_lookup_idx ON insight_platform.tenant_principals USING btree (tenant_id, state, principal_kind, principal_id);

CREATE INDEX tenants_state_idx ON insight_platform.tenants USING btree (state, tenant_id);

ALTER TABLE ONLY insight_platform.artifact_blobs
    ADD CONSTRAINT artifact_blobs_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.artifact_links
    ADD CONSTRAINT artifact_links_source_fk FOREIGN KEY (tenant_id, source_artifact_id) REFERENCES insight_platform.artifacts(tenant_id, artifact_id);

ALTER TABLE ONLY insight_platform.artifact_links
    ADD CONSTRAINT artifact_links_target_fk FOREIGN KEY (tenant_id, target_artifact_id) REFERENCES insight_platform.artifacts(tenant_id, artifact_id);

ALTER TABLE ONLY insight_platform.artifact_links
    ADD CONSTRAINT artifact_links_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.artifacts
    ADD CONSTRAINT artifacts_blob_fk FOREIGN KEY (tenant_id, blob_id) REFERENCES insight_platform.artifact_blobs(tenant_id, blob_id);

ALTER TABLE ONLY insight_platform.artifacts
    ADD CONSTRAINT artifacts_created_by_fk FOREIGN KEY (created_by) REFERENCES insight_platform.principals(principal_id);

ALTER TABLE ONLY insight_platform.artifacts
    ADD CONSTRAINT artifacts_retention_policy_fk FOREIGN KEY (tenant_id, retention_policy_revision_id) REFERENCES insight_platform.resource_versions(tenant_id, resource_version_id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE ONLY insight_platform.artifacts
    ADD CONSTRAINT artifacts_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.deployments
    ADD CONSTRAINT deployments_resource_fk FOREIGN KEY (tenant_id, resource_id) REFERENCES insight_platform.resources(tenant_id, resource_id);

ALTER TABLE ONLY insight_platform.deployments
    ADD CONSTRAINT deployments_version_fk FOREIGN KEY (tenant_id, resource_id, resource_version_id) REFERENCES insight_platform.resource_versions(tenant_id, resource_id, resource_version_id);

ALTER TABLE ONLY insight_platform.events
    ADD CONSTRAINT events_run_fk FOREIGN KEY (tenant_id, run_id) REFERENCES insight_platform.runs(tenant_id, run_id);

ALTER TABLE ONLY insight_platform.events
    ADD CONSTRAINT events_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.invocations
    ADD CONSTRAINT invocations_deployment_fk FOREIGN KEY (tenant_id, deployment_id) REFERENCES insight_platform.deployments(tenant_id, deployment_id);

ALTER TABLE ONLY insight_platform.invocations
    ADD CONSTRAINT invocations_input_fk FOREIGN KEY (tenant_id, input_value_id) REFERENCES insight_platform.run_values(tenant_id, value_id);

ALTER TABLE ONLY insight_platform.invocations
    ADD CONSTRAINT invocations_node_fk FOREIGN KEY (tenant_id, node_id) REFERENCES insight_platform.run_nodes(tenant_id, node_id);

ALTER TABLE ONLY insight_platform.invocations
    ADD CONSTRAINT invocations_output_fk FOREIGN KEY (tenant_id, output_value_id) REFERENCES insight_platform.run_values(tenant_id, value_id);

ALTER TABLE ONLY insight_platform.invocations
    ADD CONSTRAINT invocations_run_fk FOREIGN KEY (tenant_id, run_id) REFERENCES insight_platform.runs(tenant_id, run_id);

ALTER TABLE ONLY insight_platform.invocations
    ADD CONSTRAINT invocations_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.jobs
    ADD CONSTRAINT jobs_invocation_fk FOREIGN KEY (tenant_id, invocation_id) REFERENCES insight_platform.invocations(tenant_id, invocation_id);

ALTER TABLE ONLY insight_platform.jobs
    ADD CONSTRAINT jobs_node_fk FOREIGN KEY (tenant_id, node_id) REFERENCES insight_platform.run_nodes(tenant_id, node_id);

ALTER TABLE ONLY insight_platform.jobs
    ADD CONSTRAINT jobs_run_execution_requirement_fk FOREIGN KEY (tenant_id, run_id, execution_requirement_digest) REFERENCES insight_platform.runs(tenant_id, run_id, execution_requirement_digest);

ALTER TABLE ONLY insight_platform.jobs
    ADD CONSTRAINT jobs_run_fk FOREIGN KEY (tenant_id, run_id) REFERENCES insight_platform.runs(tenant_id, run_id);

ALTER TABLE ONLY insight_platform.jobs
    ADD CONSTRAINT jobs_scheduler_route_fk FOREIGN KEY (tenant_id, scheduler_partition_id) REFERENCES insight_platform.tenants(tenant_id, scheduler_partition_id);

ALTER TABLE ONLY insight_platform.jobs
    ADD CONSTRAINT jobs_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.outbox_events
    ADD CONSTRAINT outbox_events_event_fk FOREIGN KEY (tenant_id, event_id, trace_id) REFERENCES insight_platform.events(tenant_id, event_id, trace_id);

ALTER TABLE ONLY insight_platform.quota_accounts
    ADD CONSTRAINT quota_accounts_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.quota_ledger
    ADD CONSTRAINT quota_ledger_account_fk FOREIGN KEY (tenant_id, quota_account_id) REFERENCES insight_platform.quota_accounts(tenant_id, quota_account_id);

ALTER TABLE ONLY insight_platform.receipts
    ADD CONSTRAINT receipts_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.resource_versions
    ADD CONSTRAINT resource_versions_artifact_fk FOREIGN KEY (tenant_id, artifact_id) REFERENCES insight_platform.artifacts(tenant_id, artifact_id);

ALTER TABLE ONLY insight_platform.resource_versions
    ADD CONSTRAINT resource_versions_resource_fk FOREIGN KEY (tenant_id, resource_id) REFERENCES insight_platform.resources(tenant_id, resource_id);

ALTER TABLE ONLY insight_platform.resources
    ADD CONSTRAINT resources_active_deployment_fk FOREIGN KEY (tenant_id, resource_id, active_deployment_id) REFERENCES insight_platform.deployments(tenant_id, resource_id, deployment_id);

ALTER TABLE ONLY insight_platform.resources
    ADD CONSTRAINT resources_active_version_fk FOREIGN KEY (tenant_id, resource_id, active_version_id) REFERENCES insight_platform.resource_versions(tenant_id, resource_id, resource_version_id);

ALTER TABLE ONLY insight_platform.resources
    ADD CONSTRAINT resources_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.run_nodes
    ADD CONSTRAINT run_nodes_parent_fk FOREIGN KEY (tenant_id, run_id, parent_node_id) REFERENCES insight_platform.run_nodes(tenant_id, run_id, node_id);

ALTER TABLE ONLY insight_platform.run_nodes
    ADD CONSTRAINT run_nodes_related_run_fk FOREIGN KEY (tenant_id, related_run_id) REFERENCES insight_platform.runs(tenant_id, run_id);

ALTER TABLE ONLY insight_platform.run_nodes
    ADD CONSTRAINT run_nodes_run_fk FOREIGN KEY (tenant_id, run_id) REFERENCES insight_platform.runs(tenant_id, run_id);

ALTER TABLE ONLY insight_platform.run_nodes
    ADD CONSTRAINT run_nodes_scope_fk FOREIGN KEY (tenant_id, run_id, scope_id) REFERENCES insight_platform.run_nodes(tenant_id, run_id, node_id);

ALTER TABLE ONLY insight_platform.run_values
    ADD CONSTRAINT run_values_artifact_fk FOREIGN KEY (tenant_id, artifact_id) REFERENCES insight_platform.artifacts(tenant_id, artifact_id);

ALTER TABLE ONLY insight_platform.run_values
    ADD CONSTRAINT run_values_node_fk FOREIGN KEY (tenant_id, node_id) REFERENCES insight_platform.run_nodes(tenant_id, node_id);

ALTER TABLE ONLY insight_platform.run_values
    ADD CONSTRAINT run_values_run_fk FOREIGN KEY (tenant_id, run_id) REFERENCES insight_platform.runs(tenant_id, run_id);

ALTER TABLE ONLY insight_platform.runs
    ADD CONSTRAINT runs_deployment_fk FOREIGN KEY (tenant_id, agent_deployment_id) REFERENCES insight_platform.deployments(tenant_id, deployment_id);

ALTER TABLE ONLY insight_platform.runs
    ADD CONSTRAINT runs_input_value_fk FOREIGN KEY (tenant_id, input_value_id) REFERENCES insight_platform.run_values(tenant_id, value_id);

ALTER TABLE ONLY insight_platform.runs
    ADD CONSTRAINT runs_output_value_fk FOREIGN KEY (tenant_id, output_value_id) REFERENCES insight_platform.run_values(tenant_id, value_id);

ALTER TABLE ONLY insight_platform.runs
    ADD CONSTRAINT runs_parent_fk FOREIGN KEY (tenant_id, parent_run_id) REFERENCES insight_platform.runs(tenant_id, run_id);

ALTER TABLE ONLY insight_platform.runs
    ADD CONSTRAINT runs_parent_node_fk FOREIGN KEY (tenant_id, parent_run_id, parent_node_id) REFERENCES insight_platform.run_nodes(tenant_id, run_id, node_id);

ALTER TABLE ONLY insight_platform.runs
    ADD CONSTRAINT runs_principal_fk FOREIGN KEY (principal_id) REFERENCES insight_platform.principals(principal_id);

ALTER TABLE ONLY insight_platform.runs
    ADD CONSTRAINT runs_root_fk FOREIGN KEY (tenant_id, root_run_id) REFERENCES insight_platform.runs(tenant_id, run_id);

ALTER TABLE ONLY insight_platform.runs
    ADD CONSTRAINT runs_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.scheduler_tenant_state
    ADD CONSTRAINT scheduler_tenant_partition_fk FOREIGN KEY (work_class, partition_id) REFERENCES insight_platform.scheduler_state(work_class, partition_id);

ALTER TABLE ONLY insight_platform.scheduler_tenant_state
    ADD CONSTRAINT scheduler_tenant_policy_fk FOREIGN KEY (tenant_id, policy_version_id) REFERENCES insight_platform.resource_versions(tenant_id, resource_version_id);

ALTER TABLE ONLY insight_platform.scheduler_tenant_state
    ADD CONSTRAINT scheduler_tenant_route_fk FOREIGN KEY (tenant_id, partition_id) REFERENCES insight_platform.tenants(tenant_id, scheduler_partition_id);

ALTER TABLE ONLY insight_platform.secret_bindings
    ADD CONSTRAINT secret_bindings_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.tasks
    ADD CONSTRAINT tasks_cleanup_job_fk FOREIGN KEY (tenant_id, current_cleanup_job_id) REFERENCES insight_platform.jobs(tenant_id, job_id);

ALTER TABLE ONLY insight_platform.tasks
    ADD CONSTRAINT tasks_invocation_fk FOREIGN KEY (tenant_id, invocation_id) REFERENCES insight_platform.invocations(tenant_id, invocation_id);

ALTER TABLE ONLY insight_platform.tasks
    ADD CONSTRAINT tasks_node_fk FOREIGN KEY (tenant_id, node_id) REFERENCES insight_platform.run_nodes(tenant_id, node_id);

ALTER TABLE ONLY insight_platform.tasks
    ADD CONSTRAINT tasks_response_value_fk FOREIGN KEY (tenant_id, response_value_id) REFERENCES insight_platform.run_values(tenant_id, value_id);

ALTER TABLE ONLY insight_platform.tasks
    ADD CONSTRAINT tasks_run_fk FOREIGN KEY (tenant_id, run_id) REFERENCES insight_platform.runs(tenant_id, run_id);

ALTER TABLE ONLY insight_platform.tasks
    ADD CONSTRAINT tasks_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

ALTER TABLE ONLY insight_platform.tenant_principals
    ADD CONSTRAINT tenant_principals_principal_fk FOREIGN KEY (principal_id) REFERENCES insight_platform.principals(principal_id);

ALTER TABLE ONLY insight_platform.tenant_principals
    ADD CONSTRAINT tenant_principals_tenant_fk FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id);

INSERT INTO insight_platform.scheduler_state (work_class, partition_id, current_round)
SELECT class.work_class, partition.partition_id, 0
FROM (VALUES ('registry_validation'), ('orchestration'), ('model'),
    ('capability_native'), ('capability_remote'), ('mcp'), ('context'),
    ('sandbox'), ('interaction'), ('artifact'), ('recovery')) AS class(work_class)
CROSS JOIN generate_series(0, 255) AS partition(partition_id);

-- Restricted maintenance projections. Application policy remains authoritative;
-- these primitives expose metadata and preserve physical lock/prefix atomicity.
CREATE FUNCTION insight_platform.history_scan_runs(p_cutoff timestamptz, p_upper_tenant text, p_upper_run text, p_after_tenant text, p_after_run text, p_limit integer)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $history$
DECLARE cutoff timestamptz; upper_tenant text; upper_run text; rows_json jsonb;
BEGIN
    IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 128 OR (p_upper_tenant IS NULL) <> (p_upper_run IS NULL)
       OR (p_after_tenant IS NULL) <> (p_after_run IS NULL)
       OR (p_upper_tenant IS NOT NULL AND (NOT insight_platform.is_platform_id(p_upper_tenant) OR left(p_upper_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_upper_run) OR left(p_upper_run,4)<>'run_'))
       OR (p_after_tenant IS NOT NULL AND (NOT insight_platform.is_platform_id(p_after_tenant) OR left(p_after_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_after_run) OR left(p_after_run,4)<>'run_')) THEN
        RAISE EXCEPTION 'invalid bounded history scan' USING ERRCODE='22023';
    END IF;
    cutoff := COALESCE(p_cutoff, clock_timestamp());
    IF cutoff > clock_timestamp() THEN RAISE EXCEPTION 'future history scan cutoff' USING ERRCODE='22023'; END IF;
    IF p_upper_tenant IS NULL THEN
        IF p_after_tenant IS NOT NULL OR p_cutoff IS NOT NULL THEN RAISE EXCEPTION 'partial history cursor' USING ERRCODE='22023'; END IF;
        SELECT r.tenant_id,r.run_id INTO upper_tenant,upper_run FROM insight_platform.runs r WHERE r.created_at<cutoff ORDER BY r.tenant_id DESC,r.run_id DESC LIMIT 1;
    ELSE
        IF p_cutoff IS NULL OR p_after_tenant IS NULL OR (p_after_tenant,p_after_run)>(p_upper_tenant,p_upper_run) THEN RAISE EXCEPTION 'invalid history cursor range' USING ERRCODE='22023'; END IF;
        upper_tenant:=p_upper_tenant; upper_run:=p_upper_run;
    END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(candidate) ORDER BY candidate.tenant_id,candidate.run_id),'[]'::jsonb) INTO rows_json FROM (
        SELECT r.tenant_id,r.run_id,r.public_sequence,r.public_replay_floor,r.terminal_at,r.active_work_count
        FROM insight_platform.runs r WHERE r.created_at<cutoff AND (r.tenant_id,r.run_id)<=(upper_tenant,upper_run)
          AND (p_after_tenant IS NULL OR (r.tenant_id,r.run_id)>(p_after_tenant,p_after_run)) ORDER BY r.tenant_id,r.run_id LIMIT p_limit
    ) candidate;
    RETURN jsonb_build_object('creation_cutoff',cutoff,'upper_tenant',upper_tenant,'upper_run',upper_run,'rows',rows_json);
END $history$;

CREATE TABLE insight_platform.conversations (
 tenant_id text NOT NULL, conversation_id text NOT NULL,
 agent_id text NOT NULL, agent_deployment_id text NOT NULL, deployment_digest text NOT NULL CHECK (insight_platform.is_sha256(deployment_digest)),
 input_field text NOT NULL CHECK (octet_length(input_field) BETWEEN 1 AND 128), input_schema_digest text NOT NULL CHECK (insight_platform.is_sha256(input_schema_digest)),
 title text NOT NULL CHECK (octet_length(title) BETWEEN 1 AND 160),
 created_by text NOT NULL CHECK (insight_platform.is_platform_id(created_by) AND left(created_by,4)='prn_'),
 version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
 turn_count integer NOT NULL DEFAULT 0 CHECK (turn_count BETWEEN 0 AND 128),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (tenant_id, conversation_id),
 CHECK (insight_platform.is_platform_id(conversation_id) AND left(conversation_id,4)='cnv_'),
 FOREIGN KEY (tenant_id) REFERENCES insight_platform.tenants(tenant_id),
 FOREIGN KEY (tenant_id, agent_id) REFERENCES insight_platform.resources(tenant_id, resource_id),
 FOREIGN KEY (tenant_id, agent_deployment_id) REFERENCES insight_platform.deployments(tenant_id, deployment_id)
);
CREATE INDEX conversations_listing ON insight_platform.conversations(tenant_id, created_at, conversation_id);
CREATE TABLE insight_platform.conversation_turns (
 tenant_id text NOT NULL, conversation_id text NOT NULL, ordinal integer NOT NULL CHECK (ordinal BETWEEN 1 AND 128),
 run_id text NOT NULL, history_through integer NOT NULL CHECK (history_through = ordinal - 1),
 conversation_version bigint NOT NULL CHECK (conversation_version > 1),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY (tenant_id, conversation_id, ordinal), UNIQUE (tenant_id, run_id),
 FOREIGN KEY (tenant_id, conversation_id) REFERENCES insight_platform.conversations(tenant_id, conversation_id),
 FOREIGN KEY (tenant_id, run_id) REFERENCES insight_platform.runs(tenant_id, run_id)
);

CREATE FUNCTION insight_platform.history_lock_run(p_tenant text,p_run text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $history$
DECLARE root record; snapshot jsonb; cleanup_states jsonb;
BEGIN
    IF p_tenant IS NULL OR p_run IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_run) OR left(p_run,4)<>'run_' THEN RAISE EXCEPTION 'invalid Run identity' USING ERRCODE='22023'; END IF;
    SELECT r.version,r.public_replay_floor,r.public_sequence,r.terminal_at,r.active_work_count,r.history_holds INTO root
        FROM insight_platform.runs r WHERE r.tenant_id=p_tenant AND r.run_id=p_run FOR UPDATE NOWAIT;
    IF NOT FOUND THEN RETURN NULL; END IF;
    SELECT COALESCE(jsonb_agg(to_jsonb(fact)),'[]'::jsonb) INTO cleanup_states FROM (
        SELECT DISTINCT cleanup.state,(cleanup.payload->>'deletion_proof' IS NOT NULL) AS has_deletion_proof
        FROM insight_platform.tasks task JOIN insight_platform.jobs cleanup ON cleanup.tenant_id=task.tenant_id AND cleanup.job_id=task.current_cleanup_job_id
        WHERE task.tenant_id=p_tenant AND task.run_id=p_run ORDER BY cleanup.state,has_deletion_proof LIMIT 32
    ) fact;
    snapshot:=jsonb_build_object('version',root.version,'public_replay_floor',root.public_replay_floor,'public_sequence',root.public_sequence,'terminal_at',root.terminal_at,'active_work_count',root.active_work_count,
        'hold_count',(SELECT count(*) FROM jsonb_object_keys(root.history_holds->'holds')) + (SELECT count(*) FROM insight_platform.conversation_turns WHERE tenant_id=p_tenant AND run_id=p_run),
        'live_jobs',(SELECT count(*) FROM (SELECT 1 FROM insight_platform.jobs j WHERE j.tenant_id=p_tenant AND j.run_id=p_run AND j.terminal_at IS NULL LIMIT 1) fact),
        'sandbox_cleanup_obligations',(SELECT count(*) FROM (SELECT 1 FROM insight_platform.jobs j WHERE j.tenant_id=p_tenant AND j.run_id=p_run AND j.job_kind='sandbox_capability_execution' AND j.payload#>>'{cleanup,required}' IS DISTINCT FROM 'false' LIMIT 1) fact),
        'live_invocations',(SELECT count(*) FROM (SELECT 1 FROM insight_platform.invocations i WHERE i.tenant_id=p_tenant AND i.run_id=p_run AND i.terminal_at IS NULL LIMIT 1) fact),
        'pending_tasks',(SELECT count(*) FROM (SELECT 1 FROM insight_platform.tasks t WHERE t.tenant_id=p_tenant AND t.run_id=p_run AND t.state='pending' LIMIT 1) fact),
        'cleanup_states',cleanup_states);
    RETURN snapshot;
END $history$;

CREATE FUNCTION insight_platform.history_lock_event_prefix(p_tenant text,p_run text,p_floor bigint,p_through bigint,p_limit integer)
RETURNS TABLE(event_id text,aggregate_id text,public_sequence bigint,occurred_at timestamptz,outbox_exists boolean,outbox_state text,published_at timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $history$
DECLARE item record; delivery record;
BEGIN
    IF p_tenant IS NULL OR p_run IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_run) OR left(p_run,4)<>'run_' OR p_floor IS NULL OR p_floor<0 OR p_through IS NULL OR p_through<p_floor OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 THEN RAISE EXCEPTION 'invalid bounded event prefix' USING ERRCODE='22023'; END IF;
    PERFORM 1 FROM insight_platform.runs r WHERE r.tenant_id=p_tenant AND r.run_id=p_run AND r.public_replay_floor=p_floor AND r.public_sequence>=p_through FOR UPDATE NOWAIT;
    IF NOT FOUND THEN RAISE EXCEPTION 'Run prefix fence changed' USING ERRCODE='40001'; END IF;
    FOR item IN SELECT e.event_id,e.aggregate_id,e.public_sequence,e.occurred_at FROM insight_platform.events e WHERE e.tenant_id=p_tenant AND e.run_id=p_run AND e.public_sequence>p_floor AND e.public_sequence<=p_through ORDER BY e.public_sequence LIMIT p_limit FOR UPDATE NOWAIT LOOP
        SELECT o.state,o.published_at INTO delivery FROM insight_platform.outbox_events o WHERE o.tenant_id=p_tenant AND o.event_id=item.event_id FOR UPDATE NOWAIT;
        RETURN QUERY SELECT item.event_id,item.aggregate_id,item.public_sequence,item.occurred_at,FOUND,delivery.state,delivery.published_at;
    END LOOP;
END $history$;

CREATE FUNCTION insight_platform.history_delete_prefix(p_tenant text,p_run text,p_floor bigint,p_through bigint)
RETURNS bigint LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $history$
DECLARE selected_ids text[]; selected_count bigint; first_sequence bigint; last_sequence bigint;
BEGIN
    IF p_tenant IS NULL OR p_run IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_run) OR left(p_run,4)<>'run_' OR p_floor IS NULL OR p_floor<0 OR p_through IS NULL OR p_through<=p_floor OR p_through-p_floor>1000 THEN RAISE EXCEPTION 'invalid bounded prefix deletion' USING ERRCODE='22023'; END IF;
    PERFORM 1 FROM insight_platform.runs r WHERE r.tenant_id=p_tenant AND r.run_id=p_run AND r.public_replay_floor=p_floor AND r.public_sequence>=p_through FOR UPDATE NOWAIT;
    IF NOT FOUND THEN RAISE EXCEPTION 'Run prefix fence changed' USING ERRCODE='40001'; END IF;
    SELECT array_agg(item.event_id),count(*),min(item.public_sequence),max(item.public_sequence) INTO selected_ids,selected_count,first_sequence,last_sequence FROM (
        SELECT e.event_id,e.public_sequence FROM insight_platform.events e WHERE e.tenant_id=p_tenant AND e.run_id=p_run AND e.public_sequence>p_floor AND e.public_sequence<=p_through ORDER BY e.public_sequence FOR UPDATE NOWAIT
    ) item;
    IF selected_count<>p_through-p_floor OR first_sequence<>p_floor+1 OR last_sequence<>p_through THEN RAISE EXCEPTION 'history deletion is not a continuous prefix' USING ERRCODE='40001'; END IF;
    PERFORM 1 FROM insight_platform.outbox_events o WHERE o.tenant_id=p_tenant AND o.event_id=ANY(selected_ids) FOR UPDATE NOWAIT;
    IF EXISTS(SELECT 1 FROM insight_platform.outbox_events o WHERE o.tenant_id=p_tenant AND o.event_id=ANY(selected_ids) AND o.published_at IS NULL) THEN RAISE EXCEPTION 'undelivered outbox obligation' USING ERRCODE='40001'; END IF;
    DELETE FROM insight_platform.outbox_events o WHERE o.tenant_id=p_tenant AND o.event_id=ANY(selected_ids);
    DELETE FROM insight_platform.events e WHERE e.tenant_id=p_tenant AND e.run_id=p_run AND e.event_id=ANY(selected_ids);
    UPDATE insight_platform.runs r SET public_replay_floor=p_through WHERE r.tenant_id=p_tenant AND r.run_id=p_run AND r.public_replay_floor=p_floor;
    IF NOT FOUND THEN RAISE EXCEPTION 'Run prefix floor changed' USING ERRCODE='40001'; END IF;
    RETURN p_through;
END $history$;
REVOKE ALL ON FUNCTION insight_platform.history_scan_runs(timestamptz,text,text,text,text,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_lock_run(text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_lock_event_prefix(text,text,bigint,bigint,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_delete_prefix(text,text,bigint,bigint) FROM PUBLIC;

-- Safe retirement metadata: the maintenance role receives no arbitrary payload.
CREATE FUNCTION insight_platform.history_scan_records(p_lane text,p_cutoff timestamptz,p_upper_tenant text,p_upper_id text,p_after_tenant text,p_after_id text,p_limit integer)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
DECLARE cutoff timestamptz; upper_tenant text; upper_id text; result jsonb; prefix text;
BEGIN
 IF p_lane IS NULL OR p_lane NOT IN ('receipt','event_delivery','oauth_task') OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 128 THEN RAISE EXCEPTION 'invalid retirement scan' USING ERRCODE='22023'; END IF;
 prefix:=CASE p_lane WHEN 'receipt' THEN 'rcp_' WHEN 'event_delivery' THEN 'evt_' ELSE 'int_' END;
 IF (p_upper_tenant IS NULL)<>(p_upper_id IS NULL) OR (p_after_tenant IS NULL)<>(p_after_id IS NULL)
 OR (p_upper_id IS NOT NULL AND (NOT insight_platform.is_platform_id(p_upper_tenant) OR left(p_upper_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_upper_id) OR left(p_upper_id,4)<>prefix))
 OR (p_after_id IS NOT NULL AND (NOT insight_platform.is_platform_id(p_after_tenant) OR left(p_after_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_after_id) OR left(p_after_id,4)<>prefix)) THEN RAISE EXCEPTION 'invalid retirement cursor identity' USING ERRCODE='22023'; END IF;
 cutoff:=COALESCE(p_cutoff,clock_timestamp());
 IF cutoff>clock_timestamp() THEN RAISE EXCEPTION 'future retirement cutoff' USING ERRCODE='22023'; END IF;
 IF p_upper_id IS NULL THEN
  IF p_cutoff IS NOT NULL OR p_after_id IS NOT NULL THEN RAISE EXCEPTION 'partial retirement cursor' USING ERRCODE='22023'; END IF;
  SELECT item.tenant_id,item.record_id INTO upper_tenant,upper_id FROM (
   SELECT r.tenant_id,r.receipt_id AS record_id,r.created_at FROM insight_platform.receipts r WHERE p_lane='receipt'
   UNION ALL SELECT e.tenant_id,e.event_id,e.occurred_at FROM insight_platform.events e WHERE p_lane='event_delivery' AND e.tenant_id IS NOT NULL
   UNION ALL SELECT t.tenant_id,t.task_id,t.created_at FROM insight_platform.tasks t WHERE p_lane='oauth_task' AND t.task_kind='external_authorization'
  ) item WHERE item.created_at<cutoff ORDER BY item.tenant_id DESC,item.record_id DESC LIMIT 1;
 ELSE
  IF p_cutoff IS NULL OR p_after_id IS NULL OR (p_after_tenant,p_after_id)>(p_upper_tenant,p_upper_id) THEN RAISE EXCEPTION 'invalid retirement cursor range' USING ERRCODE='22023'; END IF;
  upper_tenant:=p_upper_tenant; upper_id:=p_upper_id;
 END IF;
 SELECT COALESCE(jsonb_agg(jsonb_build_object('tenant_id',item.tenant_id,'record_id',item.record_id) ORDER BY item.tenant_id,item.record_id),'[]'::jsonb) INTO result FROM (
  SELECT candidate.tenant_id,candidate.record_id FROM (
   SELECT r.tenant_id,r.receipt_id AS record_id,r.created_at FROM insight_platform.receipts r WHERE p_lane='receipt'
   UNION ALL SELECT e.tenant_id,e.event_id,e.occurred_at FROM insight_platform.events e WHERE p_lane='event_delivery' AND e.tenant_id IS NOT NULL
   UNION ALL SELECT t.tenant_id,t.task_id,t.created_at FROM insight_platform.tasks t WHERE p_lane='oauth_task' AND t.task_kind='external_authorization'
  ) candidate WHERE candidate.created_at<cutoff AND (candidate.tenant_id,candidate.record_id)<=(upper_tenant,upper_id)
   AND (p_after_id IS NULL OR (candidate.tenant_id,candidate.record_id)>(p_after_tenant,p_after_id)) ORDER BY candidate.tenant_id,candidate.record_id LIMIT p_limit
 ) item;
 RETURN jsonb_build_object('schema_version',1,'creation_cutoff',cutoff,'upper_tenant',upper_tenant,'upper_id',upper_id,'rows',result);
END $retirement$;

CREATE FUNCTION insight_platform.history_lock_receipt(p_tenant text,p_receipt text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
DECLARE r record;
BEGIN
 IF p_tenant IS NULL OR p_receipt IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_receipt) OR left(p_receipt,4)<>'rcp_' THEN RAISE EXCEPTION 'invalid receipt identity' USING ERRCODE='22023'; END IF;
 SELECT receipt_id,receipt_kind,scope_kind,scope_id,response_reference_id,request_digest,state,created_at,completed_at,expires_at,claim_expires_at INTO r FROM insight_platform.receipts WHERE tenant_id=p_tenant AND receipt_id=p_receipt FOR UPDATE NOWAIT;
 IF NOT FOUND THEN RETURN NULL; END IF;
 RETURN to_jsonb(r);
END $retirement$;

CREATE FUNCTION insight_platform.history_lock_owner(p_tenant text,p_id text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
DECLARE r record; fact jsonb; related text[]:=ARRAY[]::text[]; root_kind text; holds bigint:=0; cleanup_required boolean:=false;
BEGIN
 IF p_tenant IS NULL OR p_id IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_id) THEN RAISE EXCEPTION 'invalid history owner identity' USING ERRCODE='22023'; END IF;
 SELECT state,terminal_at,deadline,active_work_count,history_holds FROM insight_platform.runs WHERE tenant_id=p_tenant AND run_id=p_id FOR UPDATE NOWAIT INTO r;
 IF FOUND THEN
  root_kind:='run'; SELECT (SELECT count(*) FROM jsonb_object_keys(r.history_holds->'holds')) + (SELECT count(*) FROM insight_platform.conversation_turns WHERE tenant_id=p_tenant AND run_id=p_id) INTO holds;
  fact:=jsonb_build_object('state',r.state,'terminal_at',r.terminal_at,'deadline',r.deadline,'active_work',r.active_work_count>0);
 ELSE
  SELECT job_kind,state,terminal_at,deadline,run_id,invocation_id,owner_id,payload#>>'{cleanup,required}' AS cleanup,payload#>>'{physical,cleanup_required}' AS physical_cleanup FROM insight_platform.jobs WHERE tenant_id=p_tenant AND job_id=p_id FOR UPDATE NOWAIT INTO r;
  IF FOUND THEN
   root_kind:='job'; related:=ARRAY[r.run_id,r.invocation_id,r.owner_id]; cleanup_required:=r.cleanup='true' OR r.physical_cleanup='true';
   fact:=jsonb_build_object('state',r.state,'terminal_at',r.terminal_at,'deadline',r.deadline,'active_work',r.terminal_at IS NULL);
  ELSE
   SELECT state,terminal_at,deadline,run_id,owner_id FROM insight_platform.invocations WHERE tenant_id=p_tenant AND invocation_id=p_id FOR UPDATE NOWAIT INTO r;
   IF FOUND THEN
    root_kind:='invocation'; related:=ARRAY[r.run_id,r.owner_id]; fact:=jsonb_build_object('state',r.state,'terminal_at',r.terminal_at,'deadline',r.deadline,'active_work',r.terminal_at IS NULL);
   ELSE
    SELECT state,responded_at,deadline,run_id,invocation_id,owner_id,current_cleanup_job_id FROM insight_platform.tasks WHERE tenant_id=p_tenant AND task_id=p_id FOR UPDATE NOWAIT INTO r;
    IF FOUND THEN
     root_kind:='task'; related:=ARRAY[r.run_id,r.invocation_id,r.owner_id,r.current_cleanup_job_id];
     fact:=jsonb_build_object('state',r.state,'terminal_at',r.responded_at,'deadline',r.deadline,'active_work',r.responded_at IS NULL);
     IF r.current_cleanup_job_id IS NOT NULL THEN
      SELECT (j.state<>'succeeded' OR j.payload->>'deletion_proof' IS NULL) INTO cleanup_required FROM insight_platform.jobs j WHERE j.tenant_id=p_tenant AND j.job_id=r.current_cleanup_job_id;
      cleanup_required:=COALESCE(cleanup_required,true);
     END IF;
    ELSE
     SELECT state,source_artifact_id,target_artifact_id,owner_id FROM insight_platform.artifact_links WHERE tenant_id=p_tenant AND artifact_link_id=p_id FOR UPDATE NOWAIT INTO r;
     IF FOUND THEN
      root_kind:='artifact_link'; related:=ARRAY[r.source_artifact_id,r.target_artifact_id,r.owner_id]; fact:=jsonb_build_object('state',r.state,'terminal_at',NULL,'deadline',NULL,'active_work',false);
     ELSE
      SELECT state FROM insight_platform.artifacts WHERE tenant_id=p_tenant AND artifact_id=p_id FOR UPDATE NOWAIT INTO r;
      IF FOUND THEN
       root_kind:='artifact'; fact:=jsonb_build_object('state',r.state,'terminal_at',NULL,'deadline',NULL,'active_work',false);
       SELECT count(*) INTO holds FROM (SELECT 1 FROM insight_platform.artifact_links l WHERE l.tenant_id=p_tenant AND l.target_artifact_id=p_id AND l.link_kind='hold' AND l.state='active' LIMIT 1) held;
      ELSE
       SELECT lifecycle_state FROM insight_platform.resources WHERE tenant_id=p_tenant AND resource_id=p_id FOR UPDATE NOWAIT INTO r;
       IF FOUND THEN root_kind:='resource'; fact:=jsonb_build_object('state',r.lifecycle_state,'terminal_at',NULL,'deadline',NULL,'active_work',false);
       ELSE
        SELECT resource_id FROM insight_platform.resource_versions WHERE tenant_id=p_tenant AND resource_version_id=p_id FOR UPDATE NOWAIT INTO r;
        IF FOUND THEN root_kind:='resource_version'; related:=ARRAY[r.resource_id];
        ELSE
         PERFORM 1 FROM insight_platform.tenant_principals WHERE tenant_id=p_tenant AND principal_id=p_id ORDER BY principal_kind FOR UPDATE NOWAIT;
         IF FOUND THEN root_kind:='tenant_principal';
         ELSE PERFORM 1 FROM insight_platform.tenants WHERE tenant_id=p_tenant AND tenant_id=p_id FOR UPDATE NOWAIT;
          IF FOUND THEN root_kind:='tenant'; ELSE root_kind:='absent'; END IF;
         END IF;
        END IF;
        fact:=jsonb_build_object('state',NULL,'terminal_at',NULL,'deadline',NULL,'active_work',false);
       END IF;
      END IF;
     END IF;
    END IF;
   END IF;
  END IF;
 END IF;
 IF root_kind='absent' THEN
  SELECT resource_id,resource_version_id FROM insight_platform.deployments WHERE tenant_id=p_tenant AND deployment_id=p_id FOR UPDATE NOWAIT INTO r;
  IF FOUND THEN root_kind:='deployment'; related:=ARRAY[r.resource_id,r.resource_version_id];
  ELSE
   PERFORM 1 FROM insight_platform.secret_bindings WHERE tenant_id=p_tenant AND secret_binding_id=p_id FOR UPDATE NOWAIT;
   IF FOUND THEN root_kind:='secret_binding';
   ELSE
    SELECT scope_id FROM insight_platform.quota_accounts WHERE tenant_id=p_tenant AND quota_account_id=p_id FOR UPDATE NOWAIT INTO r;
    IF FOUND THEN root_kind:='quota_account'; related:=ARRAY[r.scope_id];
    ELSE
     SELECT quota_account_id,correlation_id FROM insight_platform.quota_ledger WHERE tenant_id=p_tenant AND quota_entry_id=p_id FOR UPDATE NOWAIT INTO r;
     IF FOUND THEN root_kind:='quota_entry'; related:=ARRAY[r.quota_account_id,r.correlation_id];
     ELSE
      SELECT run_id,artifact_id FROM insight_platform.run_values WHERE tenant_id=p_tenant AND value_id=p_id FOR UPDATE NOWAIT INTO r;
      IF FOUND THEN root_kind:='run_value'; related:=ARRAY[r.run_id,r.artifact_id];
      ELSE
       SELECT run_id FROM insight_platform.run_nodes WHERE tenant_id=p_tenant AND node_id=p_id FOR UPDATE NOWAIT INTO r;
       IF FOUND THEN root_kind:='run_node'; related:=ARRAY[r.run_id]; END IF;
      END IF;
     END IF;
    END IF;
   END IF;
  END IF;
 END IF;
 RETURN fact || jsonb_build_object('schema_version',1,'object_kind',root_kind,'object_id',p_id,'related_ids',to_jsonb(array_remove(related,NULL)),'hold_count',holds,'cleanup_required',COALESCE(cleanup_required,false),
  'live_jobs',EXISTS(SELECT 1 FROM insight_platform.jobs j WHERE j.tenant_id=p_tenant AND j.owner_id=p_id AND j.terminal_at IS NULL),
  'live_invocations',EXISTS(SELECT 1 FROM insight_platform.invocations i WHERE i.tenant_id=p_tenant AND i.owner_id=p_id AND i.terminal_at IS NULL),
  'pending_tasks',EXISTS(SELECT 1 FROM insight_platform.tasks t WHERE t.tenant_id=p_tenant AND t.owner_id=p_id AND t.responded_at IS NULL));
END $retirement$;

CREATE FUNCTION insight_platform.history_event_obligations(p_tenant text,p_event text,p_receipt_seconds bigint)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
DECLARE e record; receipt_until timestamptz;
BEGIN
 IF p_tenant IS NULL OR p_event IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_event) OR left(p_event,4)<>'evt_' OR p_receipt_seconds IS NULL OR p_receipt_seconds NOT BETWEEN 1 AND 315576000 THEN RAISE EXCEPTION 'invalid event obligation target' USING ERRCODE='22023'; END IF;
 SELECT aggregate_id,occurred_at,event_type,run_id INTO e FROM insight_platform.events WHERE tenant_id=p_tenant AND event_id=p_event FOR UPDATE NOWAIT;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT max(greatest(r.expires_at,r.completed_at+make_interval(secs=>p_receipt_seconds::double precision),r.claim_expires_at)) INTO receipt_until FROM insight_platform.receipts r WHERE r.tenant_id=p_tenant AND (r.scope_id=e.aggregate_id OR r.response_reference_id=e.aggregate_id OR (e.run_id IS NOT NULL AND (r.scope_id=e.run_id OR r.response_reference_id=e.run_id))) AND r.created_at<=e.occurred_at;
 RETURN jsonb_build_object('receipt_until',receipt_until,
  'processing_receipt',EXISTS(SELECT 1 FROM insight_platform.receipts r WHERE r.tenant_id=p_tenant AND (r.scope_id=e.aggregate_id OR r.response_reference_id=e.aggregate_id OR (e.run_id IS NOT NULL AND (r.scope_id=e.run_id OR r.response_reference_id=e.run_id))) AND r.created_at<=e.occurred_at AND (r.completed_at IS NULL OR r.state<>'succeeded')),
  'source_reference',EXISTS(SELECT 1 FROM insight_platform.jobs j WHERE j.tenant_id=p_tenant AND j.job_kind='mcp_oauth_pkce_cleanup' AND j.payload->>'source_event_id'=p_event),
  'provenance_reference',EXISTS(SELECT 1 FROM insight_platform.artifact_links l WHERE l.tenant_id=p_tenant AND l.link_kind='provenance' AND l.payload->>'evidence_event_id'=p_event),
  'installation_reference',e.event_type='installation.development_profile_provisioned');
END $retirement$;

CREATE FUNCTION insight_platform.history_lock_event(p_tenant text,p_event text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
DECLARE e record; delivery record;
BEGIN
 IF p_tenant IS NULL OR p_event IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_event) OR left(p_event,4)<>'evt_' THEN RAISE EXCEPTION 'invalid Event identity' USING ERRCODE='22023'; END IF;
 SELECT event_id,aggregate_id,aggregate_kind,event_type,run_id,occurred_at,payload_digest FROM insight_platform.events WHERE tenant_id=p_tenant AND event_id=p_event FOR UPDATE NOWAIT INTO e;
 IF NOT FOUND THEN RETURN NULL; END IF;
 SELECT state,published_at FROM insight_platform.outbox_events WHERE tenant_id=p_tenant AND event_id=p_event FOR UPDATE NOWAIT INTO delivery;
 RETURN to_jsonb(e)||jsonb_build_object('outbox_exists',FOUND,'outbox_state',delivery.state,'published_at',delivery.published_at);
END $retirement$;

CREATE FUNCTION insight_platform.history_delete_receipt(p_tenant text,p_receipt text,p_digest text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
BEGIN
 IF p_tenant IS NULL OR p_receipt IS NULL OR p_digest IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_receipt) OR left(p_receipt,4)<>'rcp_' OR NOT insight_platform.is_sha256(p_digest) THEN RAISE EXCEPTION 'invalid receipt deletion' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM insight_platform.receipts WHERE tenant_id=p_tenant AND receipt_id=p_receipt FOR UPDATE NOWAIT;
 DELETE FROM insight_platform.receipts WHERE tenant_id=p_tenant AND receipt_id=p_receipt AND request_digest=p_digest AND state='succeeded' AND completed_at IS NOT NULL AND expires_at<clock_timestamp() AND (claim_expires_at IS NULL OR claim_expires_at<clock_timestamp());
 RETURN FOUND;
END $retirement$;
CREATE FUNCTION insight_platform.history_delete_published_outbox(p_tenant text,p_event text,p_published timestamptz)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
BEGIN
 IF p_tenant IS NULL OR p_event IS NULL OR p_published IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_event) OR left(p_event,4)<>'evt_' OR p_published>clock_timestamp() THEN RAISE EXCEPTION 'invalid published delivery deletion' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM insight_platform.outbox_events WHERE tenant_id=p_tenant AND event_id=p_event FOR UPDATE NOWAIT;
 DELETE FROM insight_platform.outbox_events WHERE tenant_id=p_tenant AND event_id=p_event AND state='published' AND published_at=p_published;
 RETURN FOUND;
END $retirement$;
CREATE FUNCTION insight_platform.history_delete_event(p_tenant text,p_event text,p_digest text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
BEGIN
 IF p_tenant IS NULL OR p_event IS NULL OR p_digest IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_event) OR left(p_event,4)<>'evt_' OR NOT insight_platform.is_sha256(p_digest) THEN RAISE EXCEPTION 'invalid event deletion' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM insight_platform.events WHERE tenant_id=p_tenant AND event_id=p_event FOR UPDATE NOWAIT;
 IF EXISTS(SELECT 1 FROM insight_platform.outbox_events WHERE tenant_id=p_tenant AND event_id=p_event) THEN RETURN false; END IF;
 DELETE FROM insight_platform.events WHERE tenant_id=p_tenant AND event_id=p_event AND run_id IS NULL AND public_sequence IS NULL AND payload_digest=p_digest;
 RETURN FOUND;
END $retirement$;

CREATE FUNCTION insight_platform.history_lock_task_chain(p_tenant text,p_task text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
DECLARE t record; j record; next_id text; chain jsonb:='[]'::jsonb; identity jsonb; link jsonb; i integer; valid_structure boolean:=true;
BEGIN
 IF p_tenant IS NULL OR p_task IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR NOT insight_platform.is_platform_id(p_task) OR left(p_task,4)<>'int_' THEN RAISE EXCEPTION 'invalid OAuth retirement identity' USING ERRCODE='22023'; END IF;
 PERFORM 1 FROM insight_platform.receipts r WHERE r.tenant_id=p_tenant AND (r.scope_id=p_task OR r.response_reference_id=p_task) ORDER BY r.receipt_id LIMIT 1 FOR UPDATE NOWAIT;
 SELECT task_kind,state,generation,version,owner_id,run_id,invocation_id,response_value_id,responded_at,updated_at,deadline,current_cleanup_job_id,
  payload#>>'{definition,binding,pkce_secret_binding,secret_binding_id}' AS secret_id,
  payload#>>'{definition,binding,pkce_secret_binding,binding_generation}' AS binding_generation
 INTO t FROM insight_platform.tasks WHERE tenant_id=p_tenant AND task_id=p_task FOR UPDATE NOWAIT;
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF t.task_kind<>'external_authorization' THEN RAISE EXCEPTION 'retirement target is not OAuth Task' USING ERRCODE='22023'; END IF;
 identity:=jsonb_build_object('tenant_id',p_tenant,'task_id',p_task,'task_generation',t.generation,'current_job_id',t.current_cleanup_job_id,
  'cause',CASE t.state WHEN 'responded' THEN 'authorized' WHEN 'declined' THEN 'declined' WHEN 'expired' THEN 'expired' ELSE NULL END,
  'hint',jsonb_build_object('schema_version',1,'secret_binding_id',t.secret_id,'binding_generation',t.binding_generation::bigint));
 next_id:=t.current_cleanup_job_id;
 FOR i IN 1..10 LOOP
  EXIT WHEN next_id IS NULL;
  SELECT job_id,job_kind,work_class,owner_id,state,created_at,terminal_at,deadline,payload_digest,payload INTO j FROM insight_platform.jobs WHERE tenant_id=p_tenant AND job_id=next_id FOR UPDATE NOWAIT;
  IF NOT FOUND THEN valid_structure:=false; EXIT; END IF;
  valid_structure:=valid_structure AND j.job_kind='mcp_oauth_pkce_cleanup' AND j.work_class='recovery' AND j.owner_id=p_task;
  link:=jsonb_build_object('job_id',j.job_id,'state',j.state,'created_at',j.created_at,'terminal_at',j.terminal_at,'deadline',j.deadline,'payload_digest',j.payload_digest,
   'payload',jsonb_build_object('schema_version',j.payload->'schema_version','tenant_id',j.payload->'tenant_id','task_id',j.payload->'task_id','task_generation',j.payload->'task_generation','cause',j.payload->'cause',
    'hint',jsonb_build_object('schema_version',j.payload#>'{hint,schema_version}','secret_binding_id',j.payload#>'{hint,secret_binding_id}','binding_generation',j.payload#>'{hint,binding_generation}'),
    'source_event_id',j.payload->'source_event_id','deletion_effect_identity',j.payload->'deletion_effect_identity','predecessor_job_id',j.payload->'predecessor_job_id',
    'recovery_evidence_digest',j.payload->'recovery_evidence_digest','deletion_proof',j.payload->'deletion_proof'));
  chain:=chain||jsonb_build_array(link); next_id:=j.payload->>'predecessor_job_id';
 END LOOP;
 PERFORM 1 FROM insight_platform.receipts r WHERE r.tenant_id=p_tenant AND (r.scope_id IN (SELECT value->>'job_id' FROM jsonb_array_elements(chain)) OR r.response_reference_id IN (SELECT value->>'job_id' FROM jsonb_array_elements(chain))) ORDER BY r.receipt_id LIMIT 1 FOR UPDATE NOWAIT;
 RETURN jsonb_build_object('schema_version',1,'state',t.state,'version',t.version,'owner_id',t.owner_id,'run_id',t.run_id,'invocation_id',t.invocation_id,'response_value_id',t.response_value_id,
  'responded_at',t.responded_at,'updated_at',t.updated_at,'deadline',t.deadline,'identity',identity,'chain',chain,'valid_structure',valid_structure AND next_id IS NULL,
  'receipt_reference',EXISTS(SELECT 1 FROM insight_platform.receipts r WHERE r.tenant_id=p_tenant AND (r.scope_id=p_task OR r.response_reference_id=p_task OR r.scope_id IN (SELECT value->>'job_id' FROM jsonb_array_elements(chain)) OR r.response_reference_id IN (SELECT value->>'job_id' FROM jsonb_array_elements(chain)))),
  'artifact_reference',EXISTS(SELECT 1 FROM insight_platform.artifact_links l WHERE l.tenant_id=p_tenant AND (l.owner_id=p_task OR l.owner_id IN (SELECT value->>'job_id' FROM jsonb_array_elements(chain)))),
  'other_job_reference',EXISTS(SELECT 1 FROM insight_platform.jobs job WHERE job.tenant_id=p_tenant AND job.owner_id=p_task AND job.job_id NOT IN (SELECT value->>'job_id' FROM jsonb_array_elements(chain))));
END $retirement$;

CREATE FUNCTION insight_platform.history_owner_delivery(p_tenant text,p_ids text[])
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
BEGIN
 IF p_tenant IS NULL OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant,4)<>'ten_' OR p_ids IS NULL OR cardinality(p_ids) NOT BETWEEN 1 AND 32 OR EXISTS(SELECT 1 FROM unnest(p_ids) item WHERE item IS NULL OR NOT insight_platform.is_platform_id(item)) THEN RAISE EXCEPTION 'invalid delivery owner set' USING ERRCODE='22023'; END IF;
 RETURN EXISTS(SELECT 1 FROM insight_platform.events e JOIN insight_platform.outbox_events o ON o.tenant_id=e.tenant_id AND o.event_id=e.event_id WHERE e.tenant_id=p_tenant AND e.aggregate_id=ANY(p_ids) AND o.state<>'published');
END $retirement$;

CREATE FUNCTION insight_platform.history_retire_oauth_chain(p_tenant text,p_task text,p_version bigint,p_current_job text,p_event text,p_outbox text,p_digest text)
RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $retirement$
DECLARE snapshot jsonb; evidence jsonb; trace text; ids text[]; count_deleted integer; now timestamptz:=clock_timestamp();
BEGIN
 IF p_version IS NULL OR p_version<1 OR p_current_job IS NULL OR NOT insight_platform.is_platform_id(p_current_job) OR left(p_current_job,4)<>'job_' OR p_event IS NULL OR NOT insight_platform.is_platform_id(p_event) OR left(p_event,4)<>'evt_' OR p_outbox IS NULL OR NOT insight_platform.is_platform_id(p_outbox) OR left(p_outbox,4)<>'obx_' OR p_digest IS NULL OR NOT insight_platform.is_sha256(p_digest) THEN RAISE EXCEPTION 'invalid cleanup retirement fence' USING ERRCODE='22023'; END IF;
 snapshot:=insight_platform.history_lock_task_chain(p_tenant,p_task);
 IF snapshot IS NULL THEN RETURN 0; END IF;
 IF (snapshot->>'version')::bigint<>p_version OR snapshot#>>'{identity,current_job_id}'<>p_current_job OR NOT (snapshot->>'valid_structure')::boolean OR (snapshot->>'receipt_reference')::boolean OR (snapshot->>'artifact_reference')::boolean OR (snapshot->>'other_job_reference')::boolean THEN RAISE EXCEPTION 'cleanup retirement fence changed' USING ERRCODE='40001'; END IF;
 IF snapshot->>'state' NOT IN ('responded','declined','expired') OR snapshot#>>'{chain,0,state}'<>'succeeded' OR (snapshot#>>'{chain,0,payload,deletion_proof}' IS NULL OR snapshot#>>'{chain,0,payload,deletion_proof}' NOT IN ('deleted','already_absent')) OR jsonb_array_length(snapshot->'chain') NOT BETWEEN 1 AND 9 THEN RAISE EXCEPTION 'cleanup proof missing' USING ERRCODE='22023'; END IF;
 SELECT array_agg(value->>'job_id') INTO ids FROM jsonb_array_elements(snapshot->'chain');
 SELECT trace_id INTO trace FROM insight_platform.tasks WHERE tenant_id=p_tenant AND task_id=p_task;
 evidence:=jsonb_build_object('schema_version',1,'identity',snapshot->'identity','chain',snapshot->'chain');
 INSERT INTO insight_platform.events(tenant_id,event_id,aggregate_kind,aggregate_id,aggregate_version,trace_id,event_type,visibility,payload_schema_version,payload,payload_digest,occurred_at)
 VALUES(p_tenant,p_event,'mcp_oauth_task',p_task,p_version,trace,'mcp.pkce.cleanup_retired','internal',1,evidence,p_digest,now);
 INSERT INTO insight_platform.outbox_events(tenant_id,outbox_id,event_id,trace_id,created_at,updated_at) VALUES(p_tenant,p_outbox,p_event,trace,now,now);
 DELETE FROM insight_platform.tasks WHERE tenant_id=p_tenant AND task_id=p_task AND version=p_version AND current_cleanup_job_id=p_current_job;
 IF NOT FOUND THEN RAISE EXCEPTION 'Task retirement fence changed' USING ERRCODE='40001'; END IF;
 DELETE FROM insight_platform.jobs WHERE tenant_id=p_tenant AND job_id=ANY(ids); GET DIAGNOSTICS count_deleted=ROW_COUNT;
 IF count_deleted<>cardinality(ids) THEN RAISE EXCEPTION 'cleanup chain changed' USING ERRCODE='40001'; END IF;
 RETURN count_deleted;
END $retirement$;

-- Physical serialization only. The caller owns current gate, digest and policy semantics.
CREATE FUNCTION insight_platform.artifact_lock_scan_policy(p_tenant text, p_revision text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $artifact_lock$
BEGIN
    IF p_tenant IS NULL OR octet_length(p_tenant) <> 40
       OR NOT insight_platform.is_platform_id(p_tenant) OR left(p_tenant, 4) <> 'ten_'
       OR p_revision IS NULL OR octet_length(p_revision) <> 41
       OR NOT insight_platform.is_platform_id(p_revision) OR left(p_revision, 5) <> 'prev_' THEN
        RAISE EXCEPTION 'invalid Artifact policy lock identity' USING ERRCODE = '22023';
    END IF;
    PERFORM 1 FROM insight_platform.resource_versions AS version
    JOIN insight_platform.resources AS resource
      ON resource.tenant_id = version.tenant_id AND resource.resource_id = version.resource_id
    WHERE version.tenant_id = p_tenant AND version.resource_version_id = p_revision
      AND version.resource_version_kind = 'policy_revision' AND resource.resource_kind = 'policy'
    FOR SHARE OF version, resource;
    RETURN FOUND;
END $artifact_lock$;
REVOKE ALL ON FUNCTION insight_platform.artifact_lock_scan_policy(text, text) FROM PUBLIC;

CREATE INDEX receipts_retirement_scope_idx ON insight_platform.receipts(tenant_id,scope_id,created_at,expires_at);
CREATE INDEX receipts_retirement_response_idx ON insight_platform.receipts(tenant_id,response_reference_id,created_at,expires_at) WHERE response_reference_id IS NOT NULL;
CREATE INDEX jobs_retirement_owner_idx ON insight_platform.jobs(tenant_id,owner_id,job_id);
CREATE INDEX jobs_cleanup_source_event_idx ON insight_platform.jobs(tenant_id,(payload->>'source_event_id')) WHERE job_kind='mcp_oauth_pkce_cleanup';
CREATE INDEX artifact_provenance_event_idx ON insight_platform.artifact_links(tenant_id,(payload->>'evidence_event_id')) WHERE link_kind='provenance';
CREATE INDEX artifact_links_retirement_owner_idx ON insight_platform.artifact_links(tenant_id,owner_id);
CREATE INDEX invocations_retirement_owner_idx ON insight_platform.invocations(tenant_id,owner_id) WHERE terminal_at IS NULL;
CREATE INDEX tasks_retirement_owner_idx ON insight_platform.tasks(tenant_id,owner_id) WHERE responded_at IS NULL;

REVOKE ALL ON FUNCTION insight_platform.history_scan_records(text,timestamptz,text,text,text,text,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_lock_receipt(text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_lock_owner(text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_event_obligations(text,text,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_lock_event(text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_delete_receipt(text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_delete_published_outbox(text,text,timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_delete_event(text,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_lock_task_chain(text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_owner_delivery(text,text[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION insight_platform.history_retire_oauth_chain(text,text,bigint,text,text,text,text) FROM PUBLIC;

-- Local identity credential/session lifecycle. No business permissions are copied here.
CREATE TABLE insight_platform.local_console_owner (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    principal_id text NOT NULL UNIQUE REFERENCES insight_platform.principals(principal_id),
    email text NOT NULL CHECK (length(email) BETWEEN 3 AND 254 AND email = lower(email)),
    display_name text NOT NULL CHECK (length(display_name) BETWEEN 1 AND 128),
    password_salt bytea NOT NULL CHECK (octet_length(password_salt) = 32),
    password_hash bytea NOT NULL CHECK (octet_length(password_hash) = 64),
    failed_attempts integer NOT NULL DEFAULT 0 CHECK (failed_attempts BETWEEN 0 AND 10),
    locked_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE insight_platform.local_console_sessions (
    session_digest text PRIMARY KEY CHECK (session_digest ~ '^[0-9a-f]{64}$'),
    owner boolean NOT NULL DEFAULT true REFERENCES insight_platform.local_console_owner(singleton),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    expires_at timestamptz NOT NULL,
    CHECK (expires_at > created_at AND expires_at <= created_at + interval '8 hours')
);
CREATE INDEX local_console_sessions_expiry_idx ON insight_platform.local_console_sessions(expires_at);
