CREATE FUNCTION insight_platform.is_platform_id(candidate text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
RETURN candidate ~ '^[a-z][a-z0-9]{1,7}_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$';

CREATE FUNCTION insight_platform.is_sha256(candidate text)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
RETURN candidate ~ '^sha256:[0-9a-f]{64}$';

CREATE FUNCTION insight_platform.is_bounded_object(candidate jsonb, maximum_bytes integer)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
RETURN jsonb_typeof(candidate) = 'object'
   AND maximum_bytes > 0
   AND octet_length(candidate::text) <= maximum_bytes;

CREATE TABLE insight_platform.tenants (
    tenant_id text PRIMARY KEY,
    state text NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    config_schema_version integer NOT NULL DEFAULT 1,
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    config_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT tenants_id_ck CHECK (insight_platform.is_platform_id(tenant_id)),
    CONSTRAINT tenants_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT tenants_version_ck CHECK (version > 0),
    CONSTRAINT tenants_config_version_ck CHECK (config_schema_version > 0),
    CONSTRAINT tenants_config_ck CHECK (insight_platform.is_bounded_object(config, 65536)),
    CONSTRAINT tenants_config_digest_ck CHECK (insight_platform.is_sha256(config_digest)),
    CONSTRAINT tenants_time_ck CHECK (updated_at >= created_at)
);

CREATE INDEX tenants_state_idx ON insight_platform.tenants (state, tenant_id);

CREATE TABLE insight_platform.principals (
    principal_id text PRIMARY KEY,
    state text NOT NULL,
    authentication_authority_digest text NOT NULL,
    subject_digest text NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT principals_id_ck CHECK (insight_platform.is_platform_id(principal_id)),
    CONSTRAINT principals_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT principals_authority_digest_ck CHECK (
        insight_platform.is_sha256(authentication_authority_digest)
    ),
    CONSTRAINT principals_subject_digest_ck CHECK (insight_platform.is_sha256(subject_digest)),
    CONSTRAINT principals_version_ck CHECK (version > 0),
    CONSTRAINT principals_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT principals_payload_ck CHECK (insight_platform.is_bounded_object(payload, 65536)),
    CONSTRAINT principals_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT principals_external_identity_uq UNIQUE (
        authentication_authority_digest, subject_digest
    ),
    CONSTRAINT principals_time_ck CHECK (updated_at >= created_at)
);

CREATE TABLE insight_platform.tenant_principals (
    tenant_id text NOT NULL,
    principal_id text NOT NULL,
    principal_kind text NOT NULL,
    state text NOT NULL,
    generation bigint NOT NULL DEFAULT 1,
    version bigint NOT NULL DEFAULT 1,
    permissions_schema_version integer NOT NULL DEFAULT 1,
    permissions jsonb NOT NULL,
    permissions_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, principal_id, principal_kind),
    CONSTRAINT tenant_principals_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT tenant_principals_principal_fk FOREIGN KEY (principal_id)
        REFERENCES insight_platform.principals (principal_id),
    CONSTRAINT tenant_principals_kind_ck CHECK (
        principal_kind ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    CONSTRAINT tenant_principals_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT tenant_principals_generation_ck CHECK (generation > 0),
    CONSTRAINT tenant_principals_version_ck CHECK (version > 0),
    CONSTRAINT tenant_principals_schema_version_ck CHECK (permissions_schema_version > 0),
    CONSTRAINT tenant_principals_permissions_ck CHECK (
        insight_platform.is_bounded_object(permissions, 65536)
    ),
    CONSTRAINT tenant_principals_permissions_digest_ck CHECK (
        insight_platform.is_sha256(permissions_digest)
    ),
    CONSTRAINT tenant_principals_time_ck CHECK (updated_at >= created_at)
);

CREATE INDEX tenant_principals_lookup_idx
    ON insight_platform.tenant_principals (tenant_id, state, principal_kind, principal_id);

CREATE TABLE insight_platform.secret_bindings (
    tenant_id text NOT NULL,
    secret_binding_id text NOT NULL,
    purpose text NOT NULL,
    provider text NOT NULL,
    state text NOT NULL,
    generation bigint NOT NULL DEFAULT 1,
    version bigint NOT NULL DEFAULT 1,
    opaque_reference_ciphertext bytea NOT NULL,
    key_id text NOT NULL,
    reference_digest text NOT NULL,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    revoked_at timestamptz,
    PRIMARY KEY (tenant_id, secret_binding_id),
    CONSTRAINT secret_bindings_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT secret_bindings_id_ck CHECK (insight_platform.is_platform_id(secret_binding_id)),
    CONSTRAINT secret_bindings_purpose_ck CHECK (purpose ~ '^[a-z][a-z0-9_.:]{0,127}$'),
    CONSTRAINT secret_bindings_provider_ck CHECK (provider ~ '^[a-z][a-z0-9_.-]{0,127}$'),
    CONSTRAINT secret_bindings_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT secret_bindings_generation_ck CHECK (generation > 0),
    CONSTRAINT secret_bindings_version_ck CHECK (version > 0),
    CONSTRAINT secret_bindings_ciphertext_ck CHECK (
        octet_length(opaque_reference_ciphertext) BETWEEN 1 AND 16384
    ),
    CONSTRAINT secret_bindings_key_id_ck CHECK (octet_length(key_id) BETWEEN 1 AND 255),
    CONSTRAINT secret_bindings_digest_ck CHECK (insight_platform.is_sha256(reference_digest)),
    CONSTRAINT secret_bindings_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT secret_bindings_payload_ck CHECK (
        insight_platform.is_bounded_object(payload, 65536)
    ),
    CONSTRAINT secret_bindings_payload_digest_ck CHECK (
        insight_platform.is_sha256(payload_digest)
    ),
    CONSTRAINT secret_bindings_time_ck CHECK (
        updated_at >= created_at AND (revoked_at IS NULL OR revoked_at >= created_at)
    )
);

CREATE INDEX secret_bindings_lookup_idx
    ON insight_platform.secret_bindings (tenant_id, purpose, state, secret_binding_id);

CREATE TABLE insight_platform.resources (
    tenant_id text NOT NULL,
    resource_id text NOT NULL,
    resource_kind text NOT NULL,
    lifecycle_state text NOT NULL,
    gate_state text NOT NULL,
    draft_generation bigint NOT NULL DEFAULT 1,
    active_version_id text,
    active_deployment_id text,
    version bigint NOT NULL DEFAULT 1,
    payload_schema_version integer NOT NULL DEFAULT 1,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, resource_id),
    CONSTRAINT resources_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT resources_id_ck CHECK (insight_platform.is_platform_id(resource_id)),
    CONSTRAINT resources_kind_ck CHECK (resource_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT resources_lifecycle_ck CHECK (lifecycle_state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT resources_gate_ck CHECK (gate_state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT resources_draft_generation_ck CHECK (draft_generation > 0),
    CONSTRAINT resources_version_ck CHECK (version > 0),
    CONSTRAINT resources_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT resources_payload_ck CHECK (insight_platform.is_bounded_object(payload, 262144)),
    CONSTRAINT resources_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT resources_active_target_ck CHECK (
        active_version_id IS NULL OR active_deployment_id IS NULL
    ),
    CONSTRAINT resources_active_version_id_ck CHECK (
        active_version_id IS NULL OR insight_platform.is_platform_id(active_version_id)
    ),
    CONSTRAINT resources_active_deployment_id_ck CHECK (
        active_deployment_id IS NULL OR insight_platform.is_platform_id(active_deployment_id)
    ),
    CONSTRAINT resources_time_ck CHECK (updated_at >= created_at)
);

CREATE INDEX resources_registry_idx
    ON insight_platform.resources (tenant_id, resource_kind, lifecycle_state, resource_id);

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
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, resource_version_id),
    CONSTRAINT resource_versions_resource_fk FOREIGN KEY (tenant_id, resource_id)
        REFERENCES insight_platform.resources (tenant_id, resource_id),
    CONSTRAINT resource_versions_id_ck CHECK (insight_platform.is_platform_id(resource_version_id)),
    CONSTRAINT resource_versions_kind_ck CHECK (
        resource_version_kind ~ '^[a-z][a-z0-9_]{0,63}$'
    ),
    CONSTRAINT resource_versions_revision_ck CHECK (revision_no > 0),
    CONSTRAINT resource_versions_content_digest_ck CHECK (insight_platform.is_sha256(content_digest)),
    CONSTRAINT resource_versions_artifact_id_ck CHECK (
        artifact_id IS NULL OR insight_platform.is_platform_id(artifact_id)
    ),
    CONSTRAINT resource_versions_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT resource_versions_payload_ck CHECK (
        insight_platform.is_bounded_object(payload, 1048576)
    ),
    CONSTRAINT resource_versions_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT resource_versions_created_by_ck CHECK (insight_platform.is_platform_id(created_by)),
    CONSTRAINT resource_versions_revision_uq UNIQUE (
        tenant_id, resource_id, resource_version_kind, revision_no
    ),
    CONSTRAINT resource_versions_content_uq UNIQUE (
        tenant_id, resource_id, resource_version_kind, content_digest
    ),
    CONSTRAINT resource_versions_resource_id_uq UNIQUE (
        tenant_id, resource_id, resource_version_id
    )
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
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, deployment_id),
    CONSTRAINT deployments_resource_fk FOREIGN KEY (tenant_id, resource_id)
        REFERENCES insight_platform.resources (tenant_id, resource_id),
    CONSTRAINT deployments_version_fk FOREIGN KEY (
        tenant_id, resource_id, resource_version_id
    ) REFERENCES insight_platform.resource_versions (
        tenant_id, resource_id, resource_version_id
    ),
    CONSTRAINT deployments_id_ck CHECK (insight_platform.is_platform_id(deployment_id)),
    CONSTRAINT deployments_environment_ck CHECK (environment ~ '^[a-z][a-z0-9_.-]{0,63}$'),
    CONSTRAINT deployments_bindings_digest_ck CHECK (insight_platform.is_sha256(bindings_digest)),
    CONSTRAINT deployments_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT deployments_bindings_ck CHECK (
        insight_platform.is_bounded_object(bindings, 1048576)
    ),
    CONSTRAINT deployments_created_by_ck CHECK (insight_platform.is_platform_id(created_by)),
    CONSTRAINT deployments_closure_uq UNIQUE (
        tenant_id, resource_id, resource_version_id, environment, bindings_digest
    ),
    CONSTRAINT deployments_resource_id_uq UNIQUE (
        tenant_id, resource_id, deployment_id
    )
);

ALTER TABLE insight_platform.resources
    ADD CONSTRAINT resources_active_version_fk
    FOREIGN KEY (tenant_id, resource_id, active_version_id)
    REFERENCES insight_platform.resource_versions (
        tenant_id, resource_id, resource_version_id
    );

ALTER TABLE insight_platform.resources
    ADD CONSTRAINT resources_active_deployment_fk
    FOREIGN KEY (tenant_id, resource_id, active_deployment_id)
    REFERENCES insight_platform.deployments (tenant_id, resource_id, deployment_id);

CREATE TABLE insight_platform.runs (
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    root_run_id text NOT NULL,
    parent_run_id text,
    parent_node_id text,
    agent_deployment_id text NOT NULL,
    principal_id text NOT NULL,
    state text NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    bindings_schema_version integer NOT NULL,
    bindings jsonb NOT NULL,
    bindings_digest text NOT NULL,
    current_schema_version integer NOT NULL,
    current_payload jsonb NOT NULL,
    current_payload_digest text NOT NULL,
    input_value_id text,
    output_value_id text,
    depth integer NOT NULL DEFAULT 0,
    descendant_count integer NOT NULL DEFAULT 0,
    active_work_count integer NOT NULL DEFAULT 0,
    pause_generation bigint NOT NULL DEFAULT 0,
    cancel_generation bigint NOT NULL DEFAULT 0,
    timeout_generation bigint NOT NULL DEFAULT 0,
    public_sequence bigint NOT NULL DEFAULT 0,
    retry_at timestamptz,
    deadline timestamptz NOT NULL,
    started_at timestamptz,
    terminal_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, run_id),
    CONSTRAINT runs_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT runs_deployment_fk FOREIGN KEY (tenant_id, agent_deployment_id)
        REFERENCES insight_platform.deployments (tenant_id, deployment_id),
    CONSTRAINT runs_principal_fk FOREIGN KEY (principal_id)
        REFERENCES insight_platform.principals (principal_id),
    CONSTRAINT runs_id_ck CHECK (insight_platform.is_platform_id(run_id)),
    CONSTRAINT runs_relation_id_ck CHECK (
        insight_platform.is_platform_id(root_run_id)
        AND (parent_run_id IS NULL OR insight_platform.is_platform_id(parent_run_id))
        AND (parent_node_id IS NULL OR insight_platform.is_platform_id(parent_node_id))
    ),
    CONSTRAINT runs_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT runs_version_ck CHECK (version > 0),
    CONSTRAINT runs_bindings_version_ck CHECK (bindings_schema_version > 0),
    CONSTRAINT runs_bindings_ck CHECK (insight_platform.is_bounded_object(bindings, 1048576)),
    CONSTRAINT runs_bindings_digest_ck CHECK (insight_platform.is_sha256(bindings_digest)),
    CONSTRAINT runs_current_version_ck CHECK (current_schema_version > 0),
    CONSTRAINT runs_current_payload_ck CHECK (
        insight_platform.is_bounded_object(current_payload, 1048576)
    ),
    CONSTRAINT runs_current_digest_ck CHECK (insight_platform.is_sha256(current_payload_digest)),
    CONSTRAINT runs_value_id_ck CHECK (
        (input_value_id IS NULL OR insight_platform.is_platform_id(input_value_id))
        AND (output_value_id IS NULL OR insight_platform.is_platform_id(output_value_id))
    ),
    CONSTRAINT runs_ancestry_ck CHECK (
        depth >= 0 AND depth <= 32 AND descendant_count >= 0
        AND ((depth = 0 AND root_run_id = run_id AND parent_run_id IS NULL AND parent_node_id IS NULL)
             OR (depth > 0 AND root_run_id <> run_id AND parent_run_id IS NOT NULL
                 AND parent_node_id IS NOT NULL))
    ),
    CONSTRAINT runs_counters_ck CHECK (
        active_work_count >= 0 AND pause_generation >= 0 AND cancel_generation >= 0
        AND timeout_generation >= 0 AND public_sequence >= 0
    ),
    CONSTRAINT runs_time_ck CHECK (
        deadline >= created_at AND updated_at >= created_at
        AND (started_at IS NULL OR started_at >= created_at)
        AND (terminal_at IS NULL OR terminal_at >= created_at)
    )
);

CREATE INDEX runs_drive_idx
    ON insight_platform.runs (tenant_id, state, COALESCE(retry_at, deadline), run_id)
    WHERE terminal_at IS NULL;

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
    generation bigint NOT NULL DEFAULT 1,
    version bigint NOT NULL DEFAULT 1,
    enqueue_round bigint,
    payload_schema_version integer NOT NULL DEFAULT 1,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload_digest text NOT NULL,
    retry_at timestamptz,
    deadline timestamptz NOT NULL,
    started_at timestamptz,
    terminal_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, node_id),
    CONSTRAINT run_nodes_run_fk FOREIGN KEY (tenant_id, run_id)
        REFERENCES insight_platform.runs (tenant_id, run_id),
    CONSTRAINT run_nodes_id_ck CHECK (insight_platform.is_platform_id(node_id)),
    CONSTRAINT run_nodes_parent_id_ck CHECK (
        parent_node_id IS NULL OR insight_platform.is_platform_id(parent_node_id)
    ),
    CONSTRAINT run_nodes_record_kind_ck CHECK (
        record_kind IN ('node_execution', 'scope_instance', 'child_run_link')
    ),
    CONSTRAINT run_nodes_scope_id_ck CHECK (insight_platform.is_platform_id(scope_id)),
    CONSTRAINT run_nodes_plan_identity_ck CHECK (
        (record_kind = 'node_execution' AND plan_node_key IS NOT NULL
            AND activation_ordinal IS NOT NULL AND activation_ordinal > 0)
        OR (record_kind <> 'node_execution' AND plan_node_key IS NULL
            AND activation_ordinal IS NULL)
    ),
    CONSTRAINT run_nodes_plan_key_ck CHECK (
        plan_node_key IS NULL OR octet_length(plan_node_key) BETWEEN 1 AND 128
    ),
    CONSTRAINT run_nodes_related_run_ck CHECK (
        related_run_id IS NULL OR insight_platform.is_platform_id(related_run_id)
    ),
    CONSTRAINT run_nodes_logical_key_ck CHECK (octet_length(logical_key) BETWEEN 1 AND 255),
    CONSTRAINT run_nodes_kind_ck CHECK (node_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT run_nodes_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT run_nodes_generation_ck CHECK (generation > 0),
    CONSTRAINT run_nodes_version_ck CHECK (version > 0),
    CONSTRAINT run_nodes_enqueue_round_ck CHECK (enqueue_round IS NULL OR enqueue_round >= 0),
    CONSTRAINT run_nodes_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT run_nodes_payload_ck CHECK (insight_platform.is_bounded_object(payload, 262144)),
    CONSTRAINT run_nodes_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT run_nodes_time_ck CHECK (
        deadline >= created_at AND updated_at >= created_at
        AND (started_at IS NULL OR started_at >= created_at)
        AND (terminal_at IS NULL OR terminal_at >= created_at)
    ),
    CONSTRAINT run_nodes_run_logical_uq UNIQUE (tenant_id, run_id, logical_key),
    CONSTRAINT run_nodes_run_node_uq UNIQUE (tenant_id, run_id, node_id),
    CONSTRAINT run_nodes_parent_fk FOREIGN KEY (tenant_id, run_id, parent_node_id)
        REFERENCES insight_platform.run_nodes (tenant_id, run_id, node_id),
    CONSTRAINT run_nodes_scope_fk FOREIGN KEY (tenant_id, run_id, scope_id)
        REFERENCES insight_platform.run_nodes (tenant_id, run_id, node_id),
    CONSTRAINT run_nodes_related_run_fk FOREIGN KEY (tenant_id, related_run_id)
        REFERENCES insight_platform.runs (tenant_id, run_id)
);

CREATE INDEX run_nodes_drive_idx
    ON insight_platform.run_nodes (
        tenant_id, state, COALESCE(retry_at, deadline), COALESCE(enqueue_round, 0), node_id
    )
    WHERE terminal_at IS NULL;
CREATE UNIQUE INDEX run_nodes_child_run_uq
    ON insight_platform.run_nodes (tenant_id, related_run_id)
    WHERE record_kind = 'child_run_link' AND related_run_id IS NOT NULL;

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
    version bigint NOT NULL DEFAULT 1,
    verified_at timestamptz,
    deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, blob_id),
    CONSTRAINT artifact_blobs_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT artifact_blobs_id_ck CHECK (insight_platform.is_platform_id(blob_id)),
    CONSTRAINT artifact_blobs_backend_ck CHECK (backend ~ '^[a-z][a-z0-9_.-]{0,63}$'),
    CONSTRAINT artifact_blobs_storage_binding_ck CHECK (
        insight_platform.is_sha256(storage_binding_digest)
    ),
    CONSTRAINT artifact_blobs_security_domain_ck CHECK (
        insight_platform.is_sha256(security_domain_digest)
    ),
    CONSTRAINT artifact_blobs_object_ck CHECK (
        octet_length(object_reference_ciphertext) BETWEEN 1 AND 16384
    ),
    CONSTRAINT artifact_blobs_object_generation_ck CHECK (
        object_generation IS NULL OR octet_length(object_generation) BETWEEN 1 AND 255
    ),
    CONSTRAINT artifact_blobs_key_id_ck CHECK (octet_length(key_id) BETWEEN 1 AND 255),
    CONSTRAINT artifact_blobs_encryption_domain_ck CHECK (
        insight_platform.is_platform_id(encryption_domain_id)
    ),
    CONSTRAINT artifact_blobs_digest_ck CHECK (
        content_digest IS NULL OR insight_platform.is_sha256(content_digest)
    ),
    CONSTRAINT artifact_blobs_size_ck CHECK (size_bytes IS NULL OR size_bytes >= 0),
    CONSTRAINT artifact_blobs_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT artifact_blobs_version_ck CHECK (version > 0),
    CONSTRAINT artifact_blobs_time_ck CHECK (
        updated_at >= created_at
        AND (verified_at IS NULL OR verified_at >= created_at)
        AND (deleted_at IS NULL OR deleted_at >= created_at)
    ),
    CONSTRAINT artifact_blobs_verified_shape_ck CHECK (
        state <> 'verified'
        OR (object_generation IS NOT NULL AND content_digest IS NOT NULL
            AND size_bytes IS NOT NULL AND verified_at IS NOT NULL)
    )
);

CREATE INDEX artifact_blobs_state_idx
    ON insight_platform.artifact_blobs (tenant_id, state, updated_at, blob_id);
CREATE UNIQUE INDEX artifact_blobs_content_uq
    ON insight_platform.artifact_blobs (
        tenant_id, backend, storage_binding_digest, encryption_domain_id,
        security_domain_digest, content_digest
    )
    WHERE state = 'verified' AND deleted_at IS NULL;

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
    version bigint NOT NULL DEFAULT 1,
    metadata_schema_version integer NOT NULL DEFAULT 1,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    metadata_digest text NOT NULL,
    retention_policy_revision_id text NOT NULL,
    retain_until timestamptz NOT NULL,
    created_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    terminal_at timestamptz,
    PRIMARY KEY (tenant_id, artifact_id),
    CONSTRAINT artifacts_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT artifacts_blob_fk FOREIGN KEY (tenant_id, blob_id)
        REFERENCES insight_platform.artifact_blobs (tenant_id, blob_id),
    CONSTRAINT artifacts_retention_policy_fk FOREIGN KEY (
        tenant_id, retention_policy_revision_id
    ) REFERENCES insight_platform.resource_versions (tenant_id, resource_version_id)
      DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT artifacts_created_by_fk FOREIGN KEY (created_by)
        REFERENCES insight_platform.principals (principal_id),
    CONSTRAINT artifacts_id_ck CHECK (insight_platform.is_platform_id(artifact_id)),
    CONSTRAINT artifacts_purpose_ck CHECK (purpose ~ '^[a-z][a-z0-9_.:]{0,127}$'),
    CONSTRAINT artifacts_classification_ck CHECK (classification ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT artifacts_expected_size_ck CHECK (expected_size_bytes >= 0),
    CONSTRAINT artifacts_expected_digest_ck CHECK (
        expected_digest IS NULL OR insight_platform.is_sha256(expected_digest)
    ),
    CONSTRAINT artifacts_declared_media_type_ck CHECK (
        declared_media_type IS NULL OR octet_length(declared_media_type) BETWEEN 1 AND 255
    ),
    CONSTRAINT artifacts_verified_media_type_ck CHECK (
        verified_media_type IS NULL OR octet_length(verified_media_type) BETWEEN 1 AND 255
    ),
    CONSTRAINT artifacts_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT artifacts_version_ck CHECK (version > 0),
    CONSTRAINT artifacts_metadata_version_ck CHECK (metadata_schema_version > 0),
    CONSTRAINT artifacts_metadata_ck CHECK (insight_platform.is_bounded_object(metadata, 262144)),
    CONSTRAINT artifacts_metadata_digest_ck CHECK (insight_platform.is_sha256(metadata_digest)),
    CONSTRAINT artifacts_retention_policy_id_ck CHECK (
        insight_platform.is_platform_id(retention_policy_revision_id)
    ),
    CONSTRAINT artifacts_created_by_ck CHECK (insight_platform.is_platform_id(created_by)),
    CONSTRAINT artifacts_verified_shape_ck CHECK (
        state NOT IN ('verified', 'ready')
        OR (blob_id IS NOT NULL AND verified_media_type IS NOT NULL)
    ),
    CONSTRAINT artifacts_time_ck CHECK (
        updated_at >= created_at
        AND retain_until >= created_at
        AND (terminal_at IS NULL OR terminal_at >= created_at)
    )
);

CREATE INDEX artifacts_lookup_idx
    ON insight_platform.artifacts (tenant_id, state, purpose, artifact_id);
CREATE INDEX artifacts_retention_idx
    ON insight_platform.artifacts (tenant_id, state, retain_until, artifact_id)
    WHERE terminal_at IS NULL;

ALTER TABLE insight_platform.resource_versions
    ADD CONSTRAINT resource_versions_artifact_fk
    FOREIGN KEY (tenant_id, artifact_id)
    REFERENCES insight_platform.artifacts (tenant_id, artifact_id);

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
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, value_id),
    CONSTRAINT run_values_run_fk FOREIGN KEY (tenant_id, run_id)
        REFERENCES insight_platform.runs (tenant_id, run_id),
    CONSTRAINT run_values_node_fk FOREIGN KEY (tenant_id, node_id)
        REFERENCES insight_platform.run_nodes (tenant_id, node_id),
    CONSTRAINT run_values_artifact_fk FOREIGN KEY (tenant_id, artifact_id)
        REFERENCES insight_platform.artifacts (tenant_id, artifact_id),
    CONSTRAINT run_values_id_ck CHECK (insight_platform.is_platform_id(value_id)),
    CONSTRAINT run_values_kind_ck CHECK (value_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT run_values_classification_ck CHECK (classification ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT run_values_schema_digest_ck CHECK (insight_platform.is_sha256(schema_digest)),
    CONSTRAINT run_values_content_digest_ck CHECK (insight_platform.is_sha256(content_digest)),
    CONSTRAINT run_values_storage_ck CHECK (
        (inline_value IS NOT NULL AND artifact_id IS NULL
            AND octet_length(inline_value::text) <= 1048576)
        OR (inline_value IS NULL AND artifact_id IS NOT NULL)
    )
);

CREATE INDEX run_values_owner_idx
    ON insight_platform.run_values (tenant_id, run_id, node_id, value_id);

ALTER TABLE insight_platform.runs
    ADD CONSTRAINT runs_root_fk FOREIGN KEY (tenant_id, root_run_id)
    REFERENCES insight_platform.runs (tenant_id, run_id);
ALTER TABLE insight_platform.runs
    ADD CONSTRAINT runs_parent_fk FOREIGN KEY (tenant_id, parent_run_id)
    REFERENCES insight_platform.runs (tenant_id, run_id);
ALTER TABLE insight_platform.runs
    ADD CONSTRAINT runs_parent_node_fk FOREIGN KEY (tenant_id, parent_run_id, parent_node_id)
    REFERENCES insight_platform.run_nodes (tenant_id, run_id, node_id);
ALTER TABLE insight_platform.runs
    ADD CONSTRAINT runs_input_value_fk FOREIGN KEY (tenant_id, input_value_id)
    REFERENCES insight_platform.run_values (tenant_id, value_id);
ALTER TABLE insight_platform.runs
    ADD CONSTRAINT runs_output_value_fk FOREIGN KEY (tenant_id, output_value_id)
    REFERENCES insight_platform.run_values (tenant_id, value_id);

CREATE TABLE insight_platform.invocations (
    tenant_id text NOT NULL,
    invocation_id text NOT NULL,
    invocation_kind text NOT NULL,
    owner_kind text NOT NULL,
    owner_id text NOT NULL,
    logical_key text NOT NULL,
    run_id text,
    node_id text,
    deployment_id text,
    state text NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    input_value_id text,
    output_value_id text,
    effect_key_digest text,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    deadline timestamptz NOT NULL,
    retry_at timestamptz,
    started_at timestamptz,
    terminal_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, invocation_id),
    CONSTRAINT invocations_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT invocations_run_fk FOREIGN KEY (tenant_id, run_id)
        REFERENCES insight_platform.runs (tenant_id, run_id),
    CONSTRAINT invocations_node_fk FOREIGN KEY (tenant_id, node_id)
        REFERENCES insight_platform.run_nodes (tenant_id, node_id),
    CONSTRAINT invocations_deployment_fk FOREIGN KEY (tenant_id, deployment_id)
        REFERENCES insight_platform.deployments (tenant_id, deployment_id),
    CONSTRAINT invocations_input_fk FOREIGN KEY (tenant_id, input_value_id)
        REFERENCES insight_platform.run_values (tenant_id, value_id),
    CONSTRAINT invocations_output_fk FOREIGN KEY (tenant_id, output_value_id)
        REFERENCES insight_platform.run_values (tenant_id, value_id),
    CONSTRAINT invocations_id_ck CHECK (insight_platform.is_platform_id(invocation_id)),
    CONSTRAINT invocations_kind_ck CHECK (invocation_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT invocations_owner_kind_ck CHECK (owner_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT invocations_owner_id_ck CHECK (insight_platform.is_platform_id(owner_id)),
    CONSTRAINT invocations_logical_key_ck CHECK (octet_length(logical_key) BETWEEN 1 AND 255),
    CONSTRAINT invocations_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT invocations_version_ck CHECK (version > 0),
    CONSTRAINT invocations_effect_key_ck CHECK (
        effect_key_digest IS NULL OR insight_platform.is_sha256(effect_key_digest)
    ),
    CONSTRAINT invocations_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT invocations_payload_ck CHECK (insight_platform.is_bounded_object(payload, 1048576)),
    CONSTRAINT invocations_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT invocations_time_ck CHECK (
        deadline >= created_at AND updated_at >= created_at
        AND (started_at IS NULL OR started_at >= created_at)
        AND (terminal_at IS NULL OR terminal_at >= created_at)
    ),
    CONSTRAINT invocations_owner_key_uq UNIQUE (
        tenant_id, owner_kind, owner_id, invocation_kind, logical_key
    )
);

CREATE INDEX invocations_drive_idx
    ON insight_platform.invocations (tenant_id, invocation_kind, state, COALESCE(retry_at, deadline), invocation_id)
    WHERE terminal_at IS NULL;

CREATE TABLE insight_platform.quota_accounts (
    tenant_id text NOT NULL,
    quota_account_id text NOT NULL,
    scope_kind text NOT NULL,
    scope_id text NOT NULL,
    work_class text NOT NULL,
    metric text NOT NULL,
    limit_value bigint NOT NULL,
    reserved_value bigint NOT NULL DEFAULT 0,
    used_value bigint NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 1,
    payload_schema_version integer NOT NULL DEFAULT 1,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, quota_account_id),
    CONSTRAINT quota_accounts_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT quota_accounts_id_ck CHECK (insight_platform.is_platform_id(quota_account_id)),
    CONSTRAINT quota_accounts_scope_kind_ck CHECK (scope_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT quota_accounts_scope_id_ck CHECK (insight_platform.is_platform_id(scope_id)),
    CONSTRAINT quota_accounts_work_class_ck CHECK (work_class ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT quota_accounts_metric_ck CHECK (metric ~ '^[a-z][a-z0-9_.]{0,127}$'),
    CONSTRAINT quota_accounts_values_ck CHECK (
        limit_value >= 0 AND reserved_value >= 0 AND used_value >= 0
        AND reserved_value + used_value <= limit_value
    ),
    CONSTRAINT quota_accounts_version_ck CHECK (version > 0),
    CONSTRAINT quota_accounts_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT quota_accounts_payload_ck CHECK (insight_platform.is_bounded_object(payload, 65536)),
    CONSTRAINT quota_accounts_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT quota_accounts_time_ck CHECK (updated_at >= created_at),
    CONSTRAINT quota_accounts_scope_uq UNIQUE (
        tenant_id, scope_kind, scope_id, work_class, metric
    )
);

CREATE TABLE insight_platform.quota_ledger (
    tenant_id text NOT NULL,
    quota_entry_id text NOT NULL,
    quota_account_id text NOT NULL,
    correlation_id text NOT NULL,
    entry_kind text NOT NULL,
    reserved_amount bigint NOT NULL,
    used_amount bigint NOT NULL DEFAULT 0,
    account_version bigint NOT NULL,
    request_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, quota_entry_id),
    CONSTRAINT quota_ledger_account_fk FOREIGN KEY (tenant_id, quota_account_id)
        REFERENCES insight_platform.quota_accounts (tenant_id, quota_account_id),
    CONSTRAINT quota_ledger_id_ck CHECK (insight_platform.is_platform_id(quota_entry_id)),
    CONSTRAINT quota_ledger_correlation_ck CHECK (insight_platform.is_platform_id(correlation_id)),
    CONSTRAINT quota_ledger_kind_ck CHECK (entry_kind IN ('reserve', 'settle')),
    CONSTRAINT quota_ledger_amount_ck CHECK (
        reserved_amount > 0 AND used_amount >= 0 AND used_amount <= reserved_amount
        AND ((entry_kind = 'reserve' AND used_amount = 0) OR entry_kind = 'settle')
    ),
    CONSTRAINT quota_ledger_account_version_ck CHECK (account_version > 0),
    CONSTRAINT quota_ledger_request_digest_ck CHECK (insight_platform.is_sha256(request_digest)),
    CONSTRAINT quota_ledger_replay_uq UNIQUE (
        tenant_id, quota_account_id, correlation_id, entry_kind
    )
);

CREATE INDEX quota_ledger_correlation_idx
    ON insight_platform.quota_ledger (tenant_id, correlation_id, created_at, quota_entry_id);

CREATE TABLE insight_platform.jobs (
    tenant_id text NOT NULL,
    job_id text NOT NULL,
    work_class text NOT NULL,
    owner_kind text NOT NULL,
    owner_id text NOT NULL,
    invocation_id text,
    run_id text,
    node_id text,
    state text NOT NULL,
    version bigint NOT NULL DEFAULT 1,
    attempt_no integer NOT NULL DEFAULT 0,
    attempt_limit integer NOT NULL,
    lease_epoch bigint NOT NULL DEFAULT 0,
    worker_id text,
    lease_token_digest text,
    lease_expires_at timestamptz,
    heartbeat_at timestamptz,
    scheduled_at timestamptz NOT NULL,
    retry_at timestamptz,
    deadline timestamptz NOT NULL,
    priority smallint NOT NULL DEFAULT 0,
    wake_kind text,
    wake_state text,
    wake_generation bigint NOT NULL DEFAULT 0,
    request_digest text NOT NULL,
    result_digest text,
    effect_key_digest text,
    quota_reservation_id text,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    started_at timestamptz,
    terminal_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, job_id),
    CONSTRAINT jobs_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT jobs_invocation_fk FOREIGN KEY (tenant_id, invocation_id)
        REFERENCES insight_platform.invocations (tenant_id, invocation_id),
    CONSTRAINT jobs_run_fk FOREIGN KEY (tenant_id, run_id)
        REFERENCES insight_platform.runs (tenant_id, run_id),
    CONSTRAINT jobs_node_fk FOREIGN KEY (tenant_id, node_id)
        REFERENCES insight_platform.run_nodes (tenant_id, node_id),
    CONSTRAINT jobs_id_ck CHECK (insight_platform.is_platform_id(job_id)),
    CONSTRAINT jobs_work_class_ck CHECK (work_class ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT jobs_owner_kind_ck CHECK (owner_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT jobs_owner_id_ck CHECK (insight_platform.is_platform_id(owner_id)),
    CONSTRAINT jobs_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT jobs_version_ck CHECK (version > 0),
    CONSTRAINT jobs_attempt_ck CHECK (
        attempt_no >= 0 AND attempt_limit > 0 AND attempt_no <= attempt_limit AND lease_epoch >= 0
    ),
    CONSTRAINT jobs_lease_ck CHECK (
        (worker_id IS NULL AND lease_token_digest IS NULL
            AND lease_expires_at IS NULL AND heartbeat_at IS NULL)
        OR (worker_id IS NOT NULL AND lease_token_digest IS NOT NULL
            AND lease_expires_at IS NOT NULL AND heartbeat_at IS NOT NULL)
    ),
    CONSTRAINT jobs_worker_id_ck CHECK (
        worker_id IS NULL OR insight_platform.is_platform_id(worker_id)
    ),
    CONSTRAINT jobs_lease_token_ck CHECK (
        lease_token_digest IS NULL OR insight_platform.is_sha256(lease_token_digest)
    ),
    CONSTRAINT jobs_priority_ck CHECK (priority BETWEEN -32767 AND 32767),
    CONSTRAINT jobs_wake_ck CHECK (
        (wake_kind IS NULL AND wake_state IS NULL AND wake_generation = 0)
        OR (wake_kind ~ '^[a-z][a-z0-9_]{0,63}$'
            AND wake_state ~ '^[a-z][a-z0-9_]{0,63}$' AND wake_generation > 0)
    ),
    CONSTRAINT jobs_request_digest_ck CHECK (insight_platform.is_sha256(request_digest)),
    CONSTRAINT jobs_result_digest_ck CHECK (
        result_digest IS NULL OR insight_platform.is_sha256(result_digest)
    ),
    CONSTRAINT jobs_effect_key_ck CHECK (
        effect_key_digest IS NULL OR insight_platform.is_sha256(effect_key_digest)
    ),
    CONSTRAINT jobs_quota_reservation_ck CHECK (
        quota_reservation_id IS NULL OR insight_platform.is_platform_id(quota_reservation_id)
    ),
    CONSTRAINT jobs_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT jobs_payload_ck CHECK (insight_platform.is_bounded_object(payload, 1048576)),
    CONSTRAINT jobs_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT jobs_time_ck CHECK (
        deadline >= created_at AND updated_at >= created_at
        AND (started_at IS NULL OR started_at >= created_at)
        AND (terminal_at IS NULL OR terminal_at >= created_at)
    )
);

CREATE UNIQUE INDEX jobs_live_owner_uq
    ON insight_platform.jobs (tenant_id, work_class, owner_kind, owner_id)
    WHERE terminal_at IS NULL;
CREATE INDEX jobs_claim_idx
    ON insight_platform.jobs (work_class, state, COALESCE(retry_at, scheduled_at), priority DESC, job_id)
    WHERE terminal_at IS NULL AND worker_id IS NULL;
CREATE INDEX jobs_lease_idx
    ON insight_platform.jobs (lease_expires_at, tenant_id, job_id)
    WHERE terminal_at IS NULL AND lease_expires_at IS NOT NULL;

CREATE TABLE insight_platform.tasks (
    tenant_id text NOT NULL,
    task_id text NOT NULL,
    task_kind text NOT NULL,
    owner_kind text NOT NULL,
    owner_id text NOT NULL,
    run_id text,
    node_id text,
    invocation_id text,
    state text NOT NULL,
    generation bigint NOT NULL DEFAULT 1,
    version bigint NOT NULL DEFAULT 1,
    response_schema_digest text,
    principal_snapshot_schema_version integer NOT NULL,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    response_value_id text,
    deadline timestamptz NOT NULL,
    responded_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, task_id),
    CONSTRAINT tasks_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT tasks_run_fk FOREIGN KEY (tenant_id, run_id)
        REFERENCES insight_platform.runs (tenant_id, run_id),
    CONSTRAINT tasks_node_fk FOREIGN KEY (tenant_id, node_id)
        REFERENCES insight_platform.run_nodes (tenant_id, node_id),
    CONSTRAINT tasks_invocation_fk FOREIGN KEY (tenant_id, invocation_id)
        REFERENCES insight_platform.invocations (tenant_id, invocation_id),
    CONSTRAINT tasks_response_value_fk FOREIGN KEY (tenant_id, response_value_id)
        REFERENCES insight_platform.run_values (tenant_id, value_id),
    CONSTRAINT tasks_id_ck CHECK (insight_platform.is_platform_id(task_id)),
    CONSTRAINT tasks_kind_ck CHECK (task_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT tasks_owner_kind_ck CHECK (owner_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT tasks_owner_id_ck CHECK (insight_platform.is_platform_id(owner_id)),
    CONSTRAINT tasks_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT tasks_generation_ck CHECK (generation > 0),
    CONSTRAINT tasks_version_ck CHECK (version > 0),
    CONSTRAINT tasks_response_schema_ck CHECK (
        response_schema_digest IS NULL OR insight_platform.is_sha256(response_schema_digest)
    ),
    CONSTRAINT tasks_snapshot_version_ck CHECK (principal_snapshot_schema_version > 0),
    CONSTRAINT tasks_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT tasks_payload_ck CHECK (insight_platform.is_bounded_object(payload, 262144)),
    CONSTRAINT tasks_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT tasks_time_ck CHECK (
        deadline >= created_at AND updated_at >= created_at
        AND (responded_at IS NULL OR responded_at >= created_at)
    )
);

CREATE UNIQUE INDEX tasks_live_owner_uq
    ON insight_platform.tasks (tenant_id, task_kind, owner_kind, owner_id, generation)
    WHERE responded_at IS NULL;
CREATE INDEX tasks_due_idx
    ON insight_platform.tasks (tenant_id, state, deadline, task_id)
    WHERE responded_at IS NULL;

CREATE TABLE insight_platform.events (
    tenant_id text,
    event_id text PRIMARY KEY,
    aggregate_kind text NOT NULL,
    aggregate_id text NOT NULL,
    aggregate_version bigint,
    run_id text,
    public_sequence bigint,
    event_type text NOT NULL,
    visibility text NOT NULL,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT events_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT events_run_fk FOREIGN KEY (tenant_id, run_id)
        REFERENCES insight_platform.runs (tenant_id, run_id),
    CONSTRAINT events_id_ck CHECK (insight_platform.is_platform_id(event_id)),
    CONSTRAINT events_aggregate_kind_ck CHECK (aggregate_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT events_aggregate_id_ck CHECK (insight_platform.is_platform_id(aggregate_id)),
    CONSTRAINT events_aggregate_version_ck CHECK (
        aggregate_version IS NULL OR aggregate_version > 0
    ),
    CONSTRAINT events_public_sequence_ck CHECK (
        public_sequence IS NULL OR (run_id IS NOT NULL AND public_sequence > 0)
    ),
    CONSTRAINT events_scope_ck CHECK (
        tenant_id IS NOT NULL
        OR (run_id IS NULL AND public_sequence IS NULL
            AND aggregate_kind IN ('principal', 'installation_service', 'release'))
    ),
    CONSTRAINT events_type_ck CHECK (event_type ~ '^[a-z][a-z0-9_.]{0,127}$'),
    CONSTRAINT events_visibility_ck CHECK (visibility ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT events_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT events_payload_ck CHECK (insight_platform.is_bounded_object(payload, 1048576)),
    CONSTRAINT events_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT events_tenant_event_uq UNIQUE (tenant_id, event_id),
    CONSTRAINT events_aggregate_version_type_uq UNIQUE (
        tenant_id, aggregate_kind, aggregate_id, aggregate_version, event_type
    ),
    CONSTRAINT events_public_sequence_uq UNIQUE (tenant_id, run_id, public_sequence)
);

CREATE INDEX events_aggregate_idx
    ON insight_platform.events (
        tenant_id, aggregate_kind, aggregate_id, aggregate_version, occurred_at, event_id
    );
CREATE INDEX events_run_idx
    ON insight_platform.events (tenant_id, run_id, public_sequence, occurred_at, event_id)
    WHERE run_id IS NOT NULL;

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
    claim_generation bigint NOT NULL DEFAULT 1,
    claim_owner text,
    claim_expires_at timestamptz,
    disposition text,
    response_reference_id text,
    payload_schema_version integer NOT NULL DEFAULT 1,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload_digest text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, receipt_id),
    CONSTRAINT receipts_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT receipts_id_ck CHECK (insight_platform.is_platform_id(receipt_id)),
    CONSTRAINT receipts_kind_ck CHECK (receipt_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT receipts_scope_kind_ck CHECK (scope_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT receipts_scope_id_ck CHECK (insight_platform.is_platform_id(scope_id)),
    CONSTRAINT receipts_dedupe_owner_id_ck CHECK (
        insight_platform.is_platform_id(dedupe_owner_id)
    ),
    CONSTRAINT receipts_operation_ck CHECK (operation ~ '^[a-z][a-z0-9_.]{0,127}$'),
    CONSTRAINT receipts_idempotency_ck CHECK (insight_platform.is_sha256(idempotency_key_digest)),
    CONSTRAINT receipts_request_digest_ck CHECK (insight_platform.is_sha256(request_digest)),
    CONSTRAINT receipts_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT receipts_claim_generation_ck CHECK (claim_generation > 0),
    CONSTRAINT receipts_claim_ck CHECK (
        (claim_owner IS NULL AND claim_expires_at IS NULL)
        OR (claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)
    ),
    CONSTRAINT receipts_disposition_ck CHECK (
        disposition IS NULL OR disposition ~ '^[a-z][a-z0-9_.:]{0,127}$'
    ),
    CONSTRAINT receipts_response_reference_ck CHECK (
        response_reference_id IS NULL OR insight_platform.is_platform_id(response_reference_id)
    ),
    CONSTRAINT receipts_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT receipts_payload_ck CHECK (insight_platform.is_bounded_object(payload, 262144)),
    CONSTRAINT receipts_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT receipts_time_ck CHECK (
        expires_at > created_at AND (completed_at IS NULL OR completed_at >= created_at)
    ),
    CONSTRAINT receipts_idempotency_uq UNIQUE (
        tenant_id, receipt_kind, scope_kind, scope_id, dedupe_owner_id,
        operation, idempotency_key_digest
    )
);

CREATE INDEX receipts_expiry_idx
    ON insight_platform.receipts (expires_at, tenant_id, receipt_id);

CREATE TABLE insight_platform.outbox_events (
    tenant_id text NOT NULL,
    outbox_id text NOT NULL,
    event_id text NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    publish_attempts integer NOT NULL DEFAULT 0,
    next_publish_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claim_owner text,
    claim_epoch bigint NOT NULL DEFAULT 0,
    claim_expires_at timestamptz,
    last_failure_code text,
    published_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, outbox_id),
    CONSTRAINT outbox_events_event_fk FOREIGN KEY (tenant_id, event_id)
        REFERENCES insight_platform.events (tenant_id, event_id),
    CONSTRAINT outbox_events_id_ck CHECK (insight_platform.is_platform_id(outbox_id)),
    CONSTRAINT outbox_events_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT outbox_events_attempts_ck CHECK (publish_attempts >= 0),
    CONSTRAINT outbox_events_claim_epoch_ck CHECK (claim_epoch >= 0),
    CONSTRAINT outbox_events_claim_ck CHECK (
        (claim_owner IS NULL AND claim_expires_at IS NULL)
        OR (claim_owner IS NOT NULL AND claim_expires_at IS NOT NULL)
    ),
    CONSTRAINT outbox_events_failure_ck CHECK (
        last_failure_code IS NULL OR last_failure_code ~ '^[a-z][a-z0-9_.:]{0,127}$'
    ),
    CONSTRAINT outbox_events_time_ck CHECK (
        updated_at >= created_at AND (published_at IS NULL OR published_at >= created_at)
    ),
    CONSTRAINT outbox_events_event_uq UNIQUE (tenant_id, event_id)
);

CREATE INDEX outbox_events_publish_idx
    ON insight_platform.outbox_events (next_publish_at, tenant_id, outbox_id)
    WHERE published_at IS NULL;

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
    version bigint NOT NULL DEFAULT 1,
    payload_schema_version integer NOT NULL DEFAULT 1,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload_digest text NOT NULL,
    expires_at timestamptz,
    released_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, artifact_link_id),
    CONSTRAINT artifact_links_tenant_fk FOREIGN KEY (tenant_id)
        REFERENCES insight_platform.tenants (tenant_id),
    CONSTRAINT artifact_links_source_fk FOREIGN KEY (tenant_id, source_artifact_id)
        REFERENCES insight_platform.artifacts (tenant_id, artifact_id),
    CONSTRAINT artifact_links_target_fk FOREIGN KEY (tenant_id, target_artifact_id)
        REFERENCES insight_platform.artifacts (tenant_id, artifact_id),
    CONSTRAINT artifact_links_id_ck CHECK (insight_platform.is_platform_id(artifact_link_id)),
    CONSTRAINT artifact_links_kind_ck CHECK (link_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT artifact_links_owner_kind_ck CHECK (owner_kind ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT artifact_links_owner_id_ck CHECK (insight_platform.is_platform_id(owner_id)),
    CONSTRAINT artifact_links_target_ck CHECK (
        source_artifact_id IS NOT NULL OR target_artifact_id IS NOT NULL
    ),
    CONSTRAINT artifact_links_key_ck CHECK (insight_platform.is_sha256(link_key_digest)),
    CONSTRAINT artifact_links_state_ck CHECK (state ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT artifact_links_version_ck CHECK (version > 0),
    CONSTRAINT artifact_links_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT artifact_links_payload_ck CHECK (insight_platform.is_bounded_object(payload, 262144)),
    CONSTRAINT artifact_links_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest)),
    CONSTRAINT artifact_links_time_ck CHECK (
        updated_at >= created_at
        AND (expires_at IS NULL OR expires_at >= created_at)
        AND (released_at IS NULL OR released_at >= created_at)
    ),
    CONSTRAINT artifact_links_key_uq UNIQUE (tenant_id, link_kind, owner_kind, owner_id, link_key_digest)
);

CREATE INDEX artifact_links_source_idx
    ON insight_platform.artifact_links (tenant_id, source_artifact_id, link_kind, state)
    WHERE released_at IS NULL;
CREATE INDEX artifact_links_target_idx
    ON insight_platform.artifact_links (tenant_id, target_artifact_id, link_kind, state)
    WHERE released_at IS NULL;

CREATE TABLE insight_platform.scheduler_state (
    work_class text PRIMARY KEY,
    version bigint NOT NULL DEFAULT 1,
    current_round bigint NOT NULL DEFAULT 0,
    cursor_tenant_id text,
    payload_schema_version integer NOT NULL,
    payload jsonb NOT NULL,
    payload_digest text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT scheduler_state_work_class_ck CHECK (work_class ~ '^[a-z][a-z0-9_]{0,63}$'),
    CONSTRAINT scheduler_state_version_ck CHECK (version > 0),
    CONSTRAINT scheduler_state_round_ck CHECK (current_round >= 0),
    CONSTRAINT scheduler_state_cursor_ck CHECK (
        cursor_tenant_id IS NULL OR insight_platform.is_platform_id(cursor_tenant_id)
    ),
    CONSTRAINT scheduler_state_schema_version_ck CHECK (payload_schema_version > 0),
    CONSTRAINT scheduler_state_payload_ck CHECK (
        insight_platform.is_bounded_object(payload, 1048576)
    ),
    CONSTRAINT scheduler_state_payload_digest_ck CHECK (insight_platform.is_sha256(payload_digest))
);
