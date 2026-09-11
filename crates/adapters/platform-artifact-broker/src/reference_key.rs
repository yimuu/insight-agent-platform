//! Reference encryption is independent of S3 transport and role-local credentials.
use crate::{
    ArtifactExternalDependency, ArtifactExternalDependencyObserver,
    ArtifactObjectReferenceUnsealError, ArtifactProviderConfigError,
    ArtifactProviderReadinessError, ArtifactReferenceKeyBindingConfig, ArtifactUploadProviderError,
    DecryptedArtifactObjectReference,
};
use aws_config::BehaviorVersion;
use aws_sdk_kms::{primitives::Blob, types::EncryptionAlgorithmSpec, Client as KmsClient};
use insight_platform_openbao::{BaoClient, BaoError, SensitiveBytes, TransitBindingV1};
use std::{collections::HashMap, sync::Arc, time::Duration};

pub(crate) struct ArtifactReferenceKey {
    backend: ReferenceKeyBackend,
    pub(crate) key_id: Arc<str>,
    observer: Arc<dyn ArtifactExternalDependencyObserver>,
}

enum ReferenceKeyBackend {
    Aws(KmsClient),
    OpenBao {
        client: BaoClient,
        binding: TransitBindingV1,
    },
}

impl ArtifactReferenceKey {
    pub(crate) async fn install(
        config: ArtifactReferenceKeyBindingConfig,
        observer: Arc<dyn ArtifactExternalDependencyObserver>,
    ) -> Result<Self, ArtifactProviderConfigError> {
        let (key_id, backend) = match config {
            ArtifactReferenceKeyBindingConfig::AwsKms(binding) => {
                let shared = aws_config::defaults(BehaviorVersion::latest())
                    .region(aws_sdk_kms::config::Region::new(binding.region.clone()))
                    .load()
                    .await;
                let timeout = aws_sdk_kms::config::timeout::TimeoutConfig::builder()
                    .connect_timeout(Duration::from_millis(binding.connect_timeout_milliseconds))
                    .operation_timeout(Duration::from_millis(
                        binding.operation_timeout_milliseconds,
                    ))
                    .build();
                let client = KmsClient::from_conf(
                    aws_sdk_kms::Config::from(&shared)
                        .to_builder()
                        .endpoint_url(binding.endpoint)
                        .region(aws_sdk_kms::config::Region::new(binding.region))
                        .timeout_config(timeout)
                        .build(),
                );
                (binding.key_id, ReferenceKeyBackend::Aws(client))
            }
            ArtifactReferenceKeyBindingConfig::OpenBaoTransit(config) => {
                let client = BaoClient::install(config.client)
                    .map_err(|_| ArtifactProviderConfigError::InvalidKmsBinding)?;
                (
                    config.key.key_id(),
                    ReferenceKeyBackend::OpenBao {
                        client,
                        binding: config.key,
                    },
                )
            }
        };
        Ok(Self {
            backend,
            key_id: Arc::from(key_id),
            observer,
        })
    }

    pub(crate) async fn encrypt(
        &self,
        plaintext: Vec<u8>,
        context: HashMap<String, String>,
    ) -> Result<Vec<u8>, ArtifactUploadProviderError> {
        let result = async {
            match &self.backend {
                ReferenceKeyBackend::Aws(client) => {
                    let output = client
                        .encrypt()
                        .key_id(&*self.key_id)
                        .plaintext(Blob::new(plaintext))
                        .set_encryption_context(Some(context))
                        .encryption_algorithm(EncryptionAlgorithmSpec::SymmetricDefault)
                        .send()
                        .await
                        .map_err(|_| ArtifactUploadProviderError::KmsUnavailable)?;
                    if output.key_id() != Some(&*self.key_id)
                        || output.encryption_algorithm()
                            != Some(&EncryptionAlgorithmSpec::SymmetricDefault)
                    {
                        return Err(ArtifactUploadProviderError::InvalidEvidence);
                    }
                    output
                        .ciphertext_blob
                        .map(|bytes| bytes.into_inner())
                        .ok_or(ArtifactUploadProviderError::InvalidEvidence)
                }
                ReferenceKeyBackend::OpenBao { client, binding } => {
                    let plaintext = SensitiveBytes::new(plaintext)
                        .map_err(|_| ArtifactUploadProviderError::InvalidEvidence)?;
                    let aad = associated_data(&context)
                        .map_err(|_| ArtifactUploadProviderError::InvalidEvidence)?;
                    client
                        .encrypt(binding, plaintext.as_bytes(), &aad, deadline(client))
                        .await
                        .map(SensitiveBytes::into_bytes)
                        .map_err(map_upload_error)
                }
            }
        }
        .await;
        crate::aws::observe_external(
            &self.observer,
            ArtifactExternalDependency::Kms,
            result.is_ok(),
        );
        result
    }

    pub(crate) async fn decrypt(
        &self,
        ciphertext: &[u8],
        context: HashMap<String, String>,
    ) -> Result<DecryptedArtifactObjectReference, ArtifactObjectReferenceUnsealError> {
        let result = async {
            match &self.backend {
                ReferenceKeyBackend::Aws(client) => {
                    let output = client
                        .decrypt()
                        .ciphertext_blob(Blob::new(ciphertext))
                        .key_id(&*self.key_id)
                        .set_encryption_context(Some(context))
                        .encryption_algorithm(EncryptionAlgorithmSpec::SymmetricDefault)
                        .send()
                        .await
                        .map_err(|error| match error.as_service_error() {
                            Some(service)
                                if service.is_incorrect_key_exception()
                                    || service.is_invalid_ciphertext_exception()
                                    || service.is_invalid_grant_token_exception()
                                    || service.is_invalid_key_usage_exception()
                                    || service.is_not_found_exception()
                                    || service.is_disabled_exception()
                                    || service.is_kms_invalid_state_exception() =>
                            {
                                ArtifactObjectReferenceUnsealError::Rejected
                            }
                            _ => ArtifactObjectReferenceUnsealError::Unavailable,
                        })?;
                    if output.key_id() != Some(&*self.key_id)
                        || output.encryption_algorithm()
                            != Some(&EncryptionAlgorithmSpec::SymmetricDefault)
                        || output.ciphertext_for_recipient().is_some()
                    {
                        if let Some(plaintext) = output.plaintext {
                            let mut bytes = plaintext.into_inner();
                            bytes.fill(0);
                        }
                        return Err(ArtifactObjectReferenceUnsealError::InvalidEvidence);
                    }
                    DecryptedArtifactObjectReference::new(
                        output
                            .plaintext
                            .ok_or(ArtifactObjectReferenceUnsealError::InvalidEvidence)?
                            .into_inner(),
                    )
                }
                ReferenceKeyBackend::OpenBao { client, binding } => {
                    let aad = associated_data(&context)
                        .map_err(|_| ArtifactObjectReferenceUnsealError::InvalidEvidence)?;
                    let bytes = client
                        .decrypt(binding, ciphertext, &aad, deadline(client))
                        .await
                        .map_err(|error| match error {
                            BaoError::Unavailable | BaoError::UnknownOutcome => {
                                ArtifactObjectReferenceUnsealError::Unavailable
                            }
                            BaoError::InvalidEvidence => {
                                ArtifactObjectReferenceUnsealError::InvalidEvidence
                            }
                            _ => ArtifactObjectReferenceUnsealError::Rejected,
                        })?;
                    DecryptedArtifactObjectReference::new(bytes.into_bytes())
                }
            }
        }
        .await;
        crate::aws::observe_external(
            &self.observer,
            ArtifactExternalDependency::Kms,
            result.is_ok(),
        );
        result
    }

    pub(crate) async fn check_readiness(&self) -> Result<(), ArtifactProviderReadinessError> {
        let result = async {
            match &self.backend {
                ReferenceKeyBackend::Aws(client) => {
                    let output = client
                        .describe_key()
                        .key_id(&*self.key_id)
                        .send()
                        .await
                        .map_err(|_| ArtifactProviderReadinessError::KmsUnavailable)?;
                    let metadata = output
                        .key_metadata()
                        .ok_or(ArtifactProviderReadinessError::KmsInvalidEvidence)?;
                    if metadata.arn() != Some(&*self.key_id)
                        || !metadata.enabled()
                        || metadata.key_state() != Some(&aws_sdk_kms::types::KeyState::Enabled)
                        || metadata.key_usage()
                            != Some(&aws_sdk_kms::types::KeyUsageType::EncryptDecrypt)
                        || metadata.key_spec()
                            != Some(&aws_sdk_kms::types::KeySpec::SymmetricDefault)
                    {
                        Err(ArtifactProviderReadinessError::KmsInvalidEvidence)
                    } else {
                        Ok(())
                    }
                }
                ReferenceKeyBackend::OpenBao { client, binding } => client
                    .check_transit(binding, deadline(client))
                    .await
                    .map_err(|error| match error {
                        BaoError::Unavailable | BaoError::UnknownOutcome => {
                            ArtifactProviderReadinessError::KmsUnavailable
                        }
                        _ => ArtifactProviderReadinessError::KmsInvalidEvidence,
                    }),
            }
        }
        .await;
        crate::aws::observe_external(
            &self.observer,
            ArtifactExternalDependency::Kms,
            result.is_ok(),
        );
        result
    }
}

fn deadline(client: &BaoClient) -> tokio::time::Instant {
    tokio::time::Instant::now()
        + Duration::from_millis(client.config().operation_timeout_milliseconds)
}

fn associated_data(context: &HashMap<String, String>) -> Result<Vec<u8>, ()> {
    serde_jcs::to_vec(&serde_json::json!({
        "domain":"artifact_object_reference", "schema_version":1, "context":context,
    }))
    .map_err(|_| ())
}

fn map_upload_error(error: BaoError) -> ArtifactUploadProviderError {
    match error {
        BaoError::Unavailable | BaoError::UnknownOutcome => {
            ArtifactUploadProviderError::KmsUnavailable
        }
        _ => ArtifactUploadProviderError::InvalidEvidence,
    }
}
