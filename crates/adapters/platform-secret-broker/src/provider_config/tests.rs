use super::*;

pub(crate) fn bao_config() -> OpenBaoSecretProviderConfigV1 {
    let digest: Sha256Digest = format!("sha256:{}", "1".repeat(64)).parse().unwrap();
    let client = BaoClientConfigV1 {
        schema_version: 1,
        endpoint: "https://bao.example.test".to_owned(),
        expected_cluster_id: "11eaa45d-f250-4cbd-a735-c8df5143be05".to_owned(),
        auth_mount: "cert".to_owned(),
        auth_mount_accessor: "auth_cert_fixture".to_owned(),
        auth_role: "egress".to_owned(),
        expected_token_policies: vec!["platform-egress".to_owned()],
        ca_file: "/run/secrets/bao-ca.pem".to_owned(),
        client_certificate_file: "/run/secrets/bao-egress.pem".to_owned(),
        client_private_key_file: "/run/secrets/bao-egress-key.pem".to_owned(),
        connect_timeout_milliseconds: 1000,
        operation_timeout_milliseconds: 5000,
        maximum_response_bytes: 256 * 1024,
    };
    let mut kv = KvV2BindingV1 {
        schema_version: 1,
        mount: "secrets".to_owned(),
        mount_accessor: "kv_fixture".to_owned(),
        identity_digest: digest.clone(),
    };
    kv.identity_digest = kv.calculated_digest(&client.expected_cluster_id).unwrap();
    let mut reference_key = TransitBindingV1 {
        schema_version: 1,
        mount: "transit".to_owned(),
        mount_accessor: "transit_fixture".to_owned(),
        name: "secret-references".to_owned(),
        key_version: 1,
        identity_digest: digest.clone(),
    };
    reference_key.identity_digest = reference_key
        .calculated_digest(&client.expected_cluster_id)
        .unwrap();
    let mut config = OpenBaoSecretProviderConfigV1 {
        schema_version: 1,
        provider_id: "spr_0198f1c3-9a00-7c3e-b1f3-773c2836ae05".parse().unwrap(),
        provider_config_digest: digest.clone(),
        client,
        kv,
        reference_key,
        secret_path_prefix: "insight/prepared".to_owned(),
        readiness: OpenBaoSecretReadinessV1 {
            relative_path: "insight/readiness".to_owned(),
            version: 1,
            content_digest: digest,
        },
    };
    config.provider_config_digest = config.calculated_digest().unwrap();
    config
}

#[test]
fn catalog_requires_explicit_physical_kind_and_complete_digest() {
    let catalog = SecretProviderCatalogConfigV2 {
        schema_version: 2,
        providers: vec![SecretProviderConfig::OpenBaoKvV2(Box::new(bao_config()))],
    };
    catalog.validate().unwrap();
    let value = serde_json::to_value(&catalog).unwrap();
    assert_eq!(value["providers"][0]["kind"], "openbao_kv_v2");
    assert_eq!(
        serde_json::from_value::<SecretProviderCatalogConfigV2>(value.clone()).unwrap(),
        catalog
    );
    for invalid in [
        serde_json::json!({"schema_version":1,"providers":value["providers"]}),
        serde_json::json!({"schema_version":2,"providers":[]}),
    ] {
        assert!(
            serde_json::from_value::<SecretProviderCatalogConfigV2>(invalid)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    let mut wrong_kind = value.clone();
    wrong_kind["providers"][0]["kind"] = serde_json::json!("auto");
    let mut extra_field = value.clone();
    extra_field["providers"][0]["config"]["fallback"] = serde_json::json!(true);
    let mut untagged = value;
    untagged["providers"][0]
        .as_object_mut()
        .unwrap()
        .remove("kind");
    for invalid in [wrong_kind, extra_field, untagged] {
        assert!(serde_json::from_value::<SecretProviderCatalogConfigV2>(invalid).is_err());
    }
}

#[test]
fn bao_catalog_rejects_duplicate_ids_and_overlapping_physical_namespaces() {
    let original = bao_config();
    let mut second = original.clone();
    let make = |other| SecretProviderCatalogConfigV2 {
        schema_version: 2,
        providers: vec![
            SecretProviderConfig::OpenBaoKvV2(Box::new(original.clone())),
            SecretProviderConfig::OpenBaoKvV2(Box::new(other)),
        ],
    };
    assert_eq!(
        make(second.clone()).validate(),
        Err(SecretProviderConfigError::DuplicateProvider)
    );
    second.provider_id = "spr_0198f1c3-9a00-7c3e-b1f3-773c2836ae06".parse().unwrap();
    second.secret_path_prefix = "insight/prepared/nested".to_owned();
    second.provider_config_digest = second.calculated_digest().unwrap();
    assert_eq!(
        make(second.clone()).validate(),
        Err(SecretProviderConfigError::DuplicateNamespace)
    );
    second.secret_path_prefix = "insight/prepared-other".to_owned();
    second.provider_config_digest = second.calculated_digest().unwrap();
    make(second).validate().unwrap();
}

#[test]
fn bao_catalog_refuses_invalid_physical_inputs_even_with_recomputed_config_digest() {
    type Mutation = fn(&mut OpenBaoSecretProviderConfigV1);
    let changes: &[Mutation] = &[
        |c| c.schema_version = 2,
        |c| c.provider_id = "ten_0198f1c3-9a00-7c3e-b1f3-773c2836ae05".parse().unwrap(),
        |c| c.client.endpoint = "http://bao.example.test".to_owned(),
        |c| c.client.expected_token_policies = vec!["root".to_owned()],
        |c| c.client.operation_timeout_milliseconds = 30_001,
        |c| c.reference_key.key_version = 2,
        |c| c.kv.mount_accessor = "kv_replaced".to_owned(),
        |c| c.secret_path_prefix = "insight/../other".to_owned(),
        |c| c.secret_path_prefix = "insight/%2fother".to_owned(),
        |c| c.secret_path_prefix = "x".repeat(129),
        |c| c.readiness.version = 0,
        |c| c.readiness.relative_path = "insight/prepared/canary".to_owned(),
    ];
    for (index, change) in changes.iter().enumerate() {
        let mut config = bao_config();
        change(&mut config);
        config.provider_config_digest = config.calculated_digest().unwrap();
        assert!(config.validate().is_err(), "mutation {index}");
    }
    let mut changed = bao_config();
    changed.secret_path_prefix = "different".to_owned();
    assert!(changed.validate().is_err());
}
