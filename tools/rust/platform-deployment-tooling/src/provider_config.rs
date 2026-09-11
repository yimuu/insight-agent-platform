//! Shared physical catalog rendering; caller supplies observed exact provider references.
use insight_platform_contracts::{canonical_digest, ResourceId, ResourceKind};
use insight_platform_deployment_contracts::{
    installation::{InstallationError, ServiceOrigin},
    installation_provider::{InstallationProviderReadyV1, OPENBAO_CANARY_PATH},
    openbao::{BaoClientConfigV1, TransitBindingV1},
};
pub const ARTIFACT_BUCKET: &str = "insight-platform-artifacts";
pub const SECRET_NAME_PREFIX: &str = "insight/platform/prepared";
#[derive(Clone, Copy)]
pub struct ProviderEndpoints<'a> {
    pub artifact: &'a ServiceOrigin,
    pub kms: &'a ServiceOrigin,
    pub secrets: &'a ServiceOrigin,
}
impl ProviderEndpoints<'_> {
    pub fn validate(&self) -> Result<(), InstallationError> {
        for origin in [self.artifact, self.kms, self.secrets] {
            origin.validate()?;
            if !origin.is_tls() {
                return Err(InstallationError::InvalidEndpoint);
            }
        }
        Ok(())
    }
}
pub fn artifact_provider_catalog(
    endpoints: ProviderEndpoints<'_>,
    bucket: &str,
    kms_key_arn: &str,
) -> Result<serde_json::Value, InstallationError> {
    endpoints.validate()?;
    if !(3..=63).contains(&bucket.len())
        || !bucket
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        || !bucket.as_bytes()[0].is_ascii_alphanumeric()
        || !bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric()
    {
        return Err(InstallationError::InvalidInput);
    }
    let kms_binding = serde_json::json!({
        "connect_timeout_milliseconds": 5000,
        "endpoint": endpoints.kms.as_str(),
        "key_id": kms_key_arn,
        "operation_timeout_milliseconds": 30000,
        "provider": "aws_kms",
        "region": "us-east-1",
        "schema_version": 1,
    });
    let kms_binding_digest =
        canonical_digest(&kms_binding).map_err(|_| InstallationError::InvalidInput)?;
    let storage_binding = serde_json::json!({
        "backend": "s3",
        "bucket": bucket,
        "connect_timeout_milliseconds": 5000,
        "endpoint": endpoints.artifact.as_str(),
        "force_path_style": true,
        "kms_binding_digest": kms_binding_digest,
        "maximum_object_bytes": 67108864,
        "operation_timeout_milliseconds": 30000,
        "region": "us-east-1",
        "schema_version": 1,
    });
    let storage_binding_digest =
        canonical_digest(&storage_binding).map_err(|_| InstallationError::InvalidInput)?;
    Ok(serde_json::json!({
        "schema_version": 2,
        "write_storage_binding_digest": storage_binding_digest,
        "s3_storage_bindings": [{
            "schema_version": 1,
            "storage_binding_digest": storage_binding_digest,
            "endpoint": endpoints.artifact.as_str(),
            "region": "us-east-1",
            "bucket": bucket,
            "force_path_style": true,
            "kms_binding_digest": kms_binding_digest,
            "connect_timeout_milliseconds": 5000,
            "operation_timeout_milliseconds": 30000,
            "maximum_object_bytes": 67108864,
        }],
        "reference_key_bindings": [{"kind":"aws_kms","config":{
            "schema_version": 1,
            "kms_binding_digest": kms_binding_digest,
            "endpoint": endpoints.kms.as_str(),
            "region": "us-east-1",
            "key_id": kms_key_arn,
            "connect_timeout_milliseconds": 5000,
            "operation_timeout_milliseconds": 30000,
        }}],
    }))
}

/// The physical key identity is independent of each consumer's private certificate path.
pub fn openbao_artifact_provider_catalog(
    artifact: &ServiceOrigin,
    bucket: &str,
    client: &BaoClientConfigV1,
    key: &TransitBindingV1,
) -> Result<serde_json::Value, InstallationError> {
    artifact.validate()?;
    key.validate_for(client)
        .map_err(|_| InstallationError::InvalidInput)?;
    if !artifact.is_tls()
        || !(3..=63).contains(&bucket.len())
        || !bucket
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        || !bucket.as_bytes()[0].is_ascii_alphanumeric()
        || !bucket.as_bytes()[bucket.len() - 1].is_ascii_alphanumeric()
    {
        return Err(InstallationError::InvalidInput);
    }
    let storage = serde_json::json!({
        "schema_version":1,"backend":"s3","endpoint":artifact.as_str(),
        "region":"us-east-1","bucket":bucket,"force_path_style":true,
        "kms_binding_digest":key.identity_digest,
        "connect_timeout_milliseconds":5000,"operation_timeout_milliseconds":30000,
        "maximum_object_bytes":67108864,
    });
    let storage_digest = canonical_digest(&storage).map_err(|_| InstallationError::InvalidInput)?;
    let mut binding = storage;
    binding
        .as_object_mut()
        .ok_or(InstallationError::InvalidInput)?
        .remove("backend");
    binding["storage_binding_digest"] = serde_json::json!(storage_digest);
    Ok(serde_json::json!({
        "schema_version":2,"write_storage_binding_digest":storage_digest,
        "s3_storage_bindings":[binding],
        "reference_key_bindings":[{"kind":"openbao_transit","config":{
            "schema_version":1,"kms_binding_digest":key.identity_digest,"client":client,"key":key,
        }}],
    }))
}

pub fn secret_provider_catalog(
    endpoints: ProviderEndpoints<'_>,
    kms_key_arn: &str,
    readiness_secret_arn: &str,
    provider_id: &ResourceId,
) -> Result<serde_json::Value, InstallationError> {
    endpoints.validate()?;
    if provider_id.kind() != ResourceKind::SecretProvider {
        return Err(InstallationError::InvalidInput);
    }
    let (authority, _) = readiness_secret_arn
        .split_once(":secret:")
        .ok_or(InstallationError::InvalidInput)?;
    let mut provider = serde_json::json!({
        "schema_version": 1,
        "provider_id": provider_id,
        "region": "us-east-1",
        "secrets_endpoint": endpoints.secrets.as_str(),
        "kms_endpoint": endpoints.kms.as_str(),
        "kms_key_arn": kms_key_arn,
        "secret_arn_prefix": format!("{authority}:secret:insight/platform/"),
        "secret_name_prefix": SECRET_NAME_PREFIX,
        "readiness_secret_id": readiness_secret_arn,
        "connect_timeout_milliseconds": 5000,
        "operation_timeout_milliseconds": 30000,
    });
    let digest = canonical_digest(&provider).map_err(|_| InstallationError::InvalidInput)?;
    provider
        .as_object_mut()
        .expect("local provider configuration is an object")
        .insert(
            "provider_config_digest".to_owned(),
            serde_json::Value::String(digest),
        );
    Ok(serde_json::json!({
        "schema_version": 2,
        "providers": [{"kind":"aws_secrets_manager","config":provider}],
    }))
}

/// The provider identity binds the observed physical namespace, not role-local file paths.
pub fn openbao_secret_provider_catalog(
    evidence: &InstallationProviderReadyV1,
    client: &BaoClientConfigV1,
    provider_id: &ResourceId,
) -> Result<serde_json::Value, InstallationError> {
    client
        .validate()
        .map_err(|_| InstallationError::InvalidInput)?;
    evidence
        .secrets
        .validate_for(client)
        .map_err(|_| InstallationError::InvalidInput)?;
    evidence
        .secret_key
        .validate_for(client)
        .map_err(|_| InstallationError::InvalidInput)?;
    if provider_id.kind() != ResourceKind::SecretProvider
        || client.expected_cluster_id != evidence.client.expected_cluster_id
    {
        return Err(InstallationError::InvalidInput);
    }
    let readiness = serde_json::json!({"relative_path":OPENBAO_CANARY_PATH,
        "version":evidence.canary_version,"content_digest":evidence.canary_digest});
    let digest = canonical_digest(&serde_json::json!({
        "schema_version":1,"provider":"openbao_kv_v2","provider_id":provider_id,
        "cluster_id":client.expected_cluster_id,"kv":evidence.secrets,
        "reference_key":evidence.secret_key,"secret_path_prefix":"prepared","readiness":readiness,
    }))
    .map_err(|_| InstallationError::InvalidInput)?;
    Ok(
        serde_json::json!({"schema_version":2,"providers":[{"kind":"openbao_kv_v2","config":{
            "schema_version":1,"provider_id":provider_id,"provider_config_digest":digest,
            "client":client,"kv":evidence.secrets,"reference_key":evidence.secret_key,
            "secret_path_prefix":"prepared","readiness":readiness,
        }}]}),
    )
}
