use super::*;
use crate::provider_config::tests::bao_config;

fn tenant() -> ResourceId {
    "ten_0198f1c3-9a00-7c3e-b1f3-773c2836ae05".parse().unwrap()
}

#[test]
fn pinned_identity_binds_physical_provider_tenant_path_version_and_material() {
    let config = bao_config();
    let preparation = digest(b"preparation-metadata-only");
    let reference = OpenBaoOpaqueSecretReferenceV1::new(
        &config,
        &tenant(),
        &preparation,
        MaterialKind::ModelCredential,
    )
    .unwrap();
    let encoded = reference.encode().unwrap();
    let policy = SecretResolutionPolicy::Pinned {
        opaque_version_identity_digest: reference.version_digest().unwrap(),
    };
    OpenBaoOpaqueSecretReferenceV1::decode(&encoded, &config, &tenant())
        .unwrap()
        .validate_policy(&policy)
        .unwrap();
    assert_ne!(reference.version_digest().unwrap(), digest(b"1"));
    let foreign: ResourceId = "ten_0198f1c3-9a00-7c3e-b1f3-773c2836ae06".parse().unwrap();
    assert!(OpenBaoOpaqueSecretReferenceV1::decode(&encoded, &config, &foreign).is_err());
    let other = OpenBaoOpaqueSecretReferenceV1::new(
        &config,
        &foreign,
        &preparation,
        MaterialKind::ModelCredential,
    )
    .unwrap();
    assert_ne!(
        other.version_digest().unwrap(),
        reference.version_digest().unwrap()
    );
    let other_kind = OpenBaoOpaqueSecretReferenceV1::new(
        &config,
        &tenant(),
        &preparation,
        MaterialKind::McpOAuthToken,
    )
    .unwrap();
    assert!(other_kind.validate_policy(&policy).is_err());

    for (field, value) in [
        ("version", serde_json::json!(0)),
        ("version", serde_json::json!(2)),
        ("version", serde_json::json!(1.0)),
        ("schema_version", serde_json::json!(2)),
        (
            "relative_path",
            serde_json::json!("insight/prepared/../escape"),
        ),
        (
            "relative_path",
            serde_json::json!("https://elsewhere.test/secret"),
        ),
        ("relative_path", serde_json::json!("insight/readiness")),
        ("material_kind", serde_json::json!("raw")),
        (
            "provider_config_digest",
            serde_json::json!(digest(b"different-config")),
        ),
        (
            "kv_binding_digest",
            serde_json::json!(digest(b"different-mount")),
        ),
    ] {
        let mut value_json = serde_json::to_value(&reference).unwrap();
        value_json[field] = value;
        let bytes = OpaqueSecretReference::new(serde_json::to_vec(&value_json).unwrap()).unwrap();
        assert!(
            OpenBaoOpaqueSecretReferenceV1::decode(&bytes, &config, &tenant()).is_err(),
            "field {field}"
        );
    }
}

#[test]
fn role_local_transport_changes_preserve_physical_reference_but_identity_changes_do_not() {
    let config = bao_config();
    let reference = OpenBaoOpaqueSecretReferenceV1::new(
        &config,
        &tenant(),
        &digest(b"request"),
        MaterialKind::ModelCredential,
    )
    .unwrap();
    let encoded = reference.encode().unwrap();
    let mut credentials = config.clone();
    credentials.client.client_certificate_file = "/run/other/egress.pem".to_owned();
    credentials.client.client_private_key_file = "/run/other/key.pem".to_owned();
    credentials.client.auth_role = "replacement-leaf".to_owned();
    credentials.client.operation_timeout_milliseconds = 6000;
    assert_eq!(
        config.provider_config_digest,
        credentials.calculated_digest().unwrap()
    );
    credentials.validate().unwrap();
    let decoded =
        OpenBaoOpaqueSecretReferenceV1::decode(&encoded, &credentials, &tenant()).unwrap();
    assert_eq!(
        reference.version_digest().unwrap(),
        decoded.version_digest().unwrap()
    );
    type Mutation = fn(&mut OpenBaoSecretProviderConfigV1);
    let changes: &[Mutation] = &[
        |c| c.client.expected_cluster_id = "21eaa45d-f250-4cbd-a735-c8df5143be05".to_owned(),
        |c| c.kv.mount_accessor = "kv_replaced".to_owned(),
        |c| c.reference_key.key_version = 2,
        |c| c.secret_path_prefix = "new/prepared".to_owned(),
    ];
    for change in changes {
        let mut changed = config.clone();
        change(&mut changed);
        changed.kv.identity_digest = changed
            .kv
            .calculated_digest(&changed.client.expected_cluster_id)
            .unwrap();
        changed.reference_key.identity_digest = changed
            .reference_key
            .calculated_digest(&changed.client.expected_cluster_id)
            .unwrap();
        changed.provider_config_digest = changed.calculated_digest().unwrap();
        changed.validate().unwrap();
        assert!(OpenBaoOpaqueSecretReferenceV1::decode(&encoded, &changed, &tenant()).is_err());
    }
}

#[test]
fn secret_aad_contains_every_current_authority_dimension_and_domain() {
    let tenant = tenant();
    let binding: ResourceId = "sbd_0198f1c3-9a00-7c3e-b1f3-773c2836ae05".parse().unwrap();
    let provider = bao_config().provider_id;
    let context = aad(&tenant, &binding, &provider, 1, "openbao-actual-key").unwrap();
    let value: serde_json::Value = serde_json::from_slice(&context).unwrap();
    assert_eq!(value["domain"], "openbao_secret_reference_v1");
    assert_eq!(value["context"]["tenant_id"], tenant.to_string());
    assert_eq!(value["context"]["secret_binding_id"], binding.to_string());
    assert_eq!(value["context"]["provider_id"], provider.to_string());
    assert_eq!(value["context"]["binding_generation"], "1");
    assert_eq!(value["context"]["key_id"], "openbao-actual-key");
    assert_ne!(
        context,
        aad(&tenant, &binding, &provider, 2, "openbao-actual-key").unwrap()
    );
    assert!(aad(&tenant, &binding, &provider, 0, "openbao-actual-key").is_err());
    assert!(aad(&binding, &tenant, &provider, 1, "openbao-actual-key").is_err());
}

#[test]
fn physical_error_mapping_never_claims_an_unknown_write_or_delete_succeeded() {
    assert_eq!(
        prepare_error(BaoError::UnknownOutcome),
        SecretProviderPrepareError::WriteUncertain
    );
    assert_eq!(
        prepare_error(BaoError::Denied),
        SecretProviderPrepareError::Rejected
    );
    assert_eq!(
        delete_error(BaoError::NotFound),
        SecretProviderDeleteError::OutcomeUncertain
    );
    assert_eq!(
        delete_error(BaoError::UnknownOutcome),
        SecretProviderDeleteError::OutcomeUncertain
    );
    assert_eq!(
        delete_error(BaoError::Denied),
        SecretProviderDeleteError::Rejected
    );
    assert_eq!(
        resolve_error(BaoError::Denied),
        SecretProviderResolveError::Rejected
    );
}
