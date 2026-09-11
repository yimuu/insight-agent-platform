//! OpenBao KV v2 stores immutable prepared versions; Transit seals only their opaque references.
#[cfg(test)]
mod physical_tests;
mod reference;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_tests;

use super::*;
use crate::prepared::{
    check_import_permit, decode_prepared, deterministic_binding_id, encode_prepared, exact_binding,
    reference_encryption_context, validate_seal_identity, PreparedMcpOAuthPkce,
    PreparedMcpOAuthToken, PreparedModelCredential, PreparedSecretEnvelope, SecretBytes,
};
use insight_platform_contracts::{
    ModelCredentialImportAuthorizationV1, ModelCredentialImportPermitV1, SensitiveModelApiKey,
};
use insight_platform_egress::{McpOAuthTokenSet, SensitiveMcpOAuthPkceVerifier};
use insight_platform_mcp_host::{
    SensitiveMcpOAuthNonce, SensitiveOAuthValue, MCP_OAUTH_PKCE_SECRET_PURPOSE,
};
use insight_platform_openbao::{BaoClient, BaoError, BaoSecretPath, SensitiveBytes};
use reference::{prepared_path, MaterialKind, OpenBaoOpaqueSecretReferenceV1};
use tokio::time::Instant;

type ImportPermit<'a> = Option<(
    &'a ModelCredentialImportAuthorizationV1,
    &'a ModelCredentialImportPermitV1,
)>;

#[derive(Clone)]
pub(super) struct OpenBaoProvider {
    config: Arc<OpenBaoSecretProviderConfigV1>,
    client: BaoClient,
    observer: Arc<dyn SecretExternalDependencyObserver>,
}

impl OpenBaoProvider {
    pub(super) fn install(
        config: OpenBaoSecretProviderConfigV1,
        observer: Arc<dyn SecretExternalDependencyObserver>,
    ) -> Result<Self, SecretProviderConfigError> {
        config.validate()?;
        let client = BaoClient::install(config.client.clone())
            .map_err(|_| SecretProviderConfigError::InvalidProvider)?;
        Ok(Self {
            config: Arc::new(config),
            client,
            observer,
        })
    }

    fn deadline(&self) -> Instant {
        Instant::now() + Duration::from_millis(self.config.client.operation_timeout_milliseconds)
    }

    fn observe<T>(&self, dependency: SecretExternalDependency, result: &Result<T, BaoError>) {
        self.observer.observe(
            dependency,
            if result.is_ok() {
                SecretExternalDependencyOutcome::Success
            } else {
                SecretExternalDependencyOutcome::Failure
            },
        );
    }

    pub(super) async fn check_readiness(&self) -> Result<(), BaoError> {
        let deadline = self.deadline();
        let result = self
            .client
            .check_transit(&self.config.reference_key, deadline)
            .await;
        self.observe(SecretExternalDependency::Kms, &result);
        result?;
        let result = self.client.check_kv(&self.config.kv, deadline).await;
        self.observe(SecretExternalDependency::Secret, &result);
        result?;
        let path = BaoSecretPath::parse(&self.config.readiness.relative_path)?;
        let result = self
            .client
            .read_exact(
                &self.config.kv,
                &path,
                self.config.readiness.version,
                deadline,
            )
            .await;
        self.observe(SecretExternalDependency::Secret, &result);
        let read = result?;
        if read.version != self.config.readiness.version
            || digest(read.bytes.as_bytes()) != self.config.readiness.content_digest
        {
            return Err(BaoError::InvalidEvidence);
        }
        Ok(())
    }

    async fn load_prepared(
        &self,
        path: &BaoSecretPath,
        permit: ImportPermit<'_>,
        deadline: Instant,
    ) -> Result<Option<PreparedSecretEnvelope>, SecretProviderPrepareError> {
        check_import_permit(permit)?;
        let result = self
            .client
            .read_exact(&self.config.kv, path, 1, deadline)
            .await;
        self.observe(SecretExternalDependency::Secret, &result);
        match result {
            Ok(read) if read.version == 1 => decode_prepared(read.bytes.as_bytes())
                .map(Some)
                .map_err(resolve_prepare),
            Ok(_) => Err(SecretProviderPrepareError::Rejected),
            Err(BaoError::NotFound) => {
                // A missing data response can be a tombstone, not permission to recreate a key.
                check_import_permit(permit)?;
                let metadata = self.client.metadata(&self.config.kv, path, deadline).await;
                self.observe(SecretExternalDependency::Secret, &metadata);
                match metadata {
                    Err(BaoError::NotFound) => Ok(None),
                    Ok(_) => Err(SecretProviderPrepareError::Rejected),
                    Err(error) => Err(prepare_error(error)),
                }
            }
            Err(error) => Err(prepare_error(error)),
        }
    }

    async fn create_or_load(
        &self,
        path: &BaoSecretPath,
        proposed: PreparedSecretEnvelope,
        permit: ImportPermit<'_>,
        deadline: Instant,
    ) -> Result<PreparedSecretEnvelope, SecretProviderPrepareError> {
        if let Some(existing) = self.load_prepared(path, permit, deadline).await? {
            return Ok(existing);
        }
        check_import_permit(permit)?;
        let encoded = SensitiveBytes::new(encode_prepared(&proposed)?)
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        let result = self
            .client
            .create_only(&self.config.kv, path, encoded.as_bytes(), deadline)
            .await;
        self.observe(SecretExternalDependency::Secret, &result);
        match result {
            Ok(1) => {}
            Ok(_) => return Err(SecretProviderPrepareError::WriteUncertain),
            Err(BaoError::Conflict | BaoError::UnknownOutcome) => {}
            Err(error) => return Err(prepare_error(error)),
        }
        // A successful write is also read back: its response does not prove the winning bytes.
        check_import_permit(permit).map_err(|_| SecretProviderPrepareError::WriteUncertain)?;
        self.load_prepared(path, permit, deadline)
            .await
            // Denied/expired/malformed readback does not prove the preceding write was absent.
            // Only a successfully decoded winner is available to the caller's content checks.
            .map_err(|_| SecretProviderPrepareError::WriteUncertain)?
            .ok_or(SecretProviderPrepareError::WriteUncertain)
    }

    fn prepared(
        &self,
        reference: &OpenBaoOpaqueSecretReferenceV1,
        binding: ResourceId,
        preparation: &Sha256Digest,
    ) -> Result<ProviderPreparedSecretVersion, SecretProviderPrepareError> {
        Ok(ProviderPreparedSecretVersion {
            secret_binding_id: binding,
            provider_id: self.config.provider_id.clone(),
            opaque_reference: reference.encode().map_err(resolve_prepare)?,
            opaque_version_identity_digest: reference.version_digest().map_err(resolve_prepare)?,
            storage_evidence_digest: reference
                .evidence_digest(preparation)
                .map_err(resolve_prepare)?,
        })
    }

    fn token_result(
        &self,
        preparation: &McpOAuthTokenPreparation,
        stored: PreparedSecretEnvelope,
    ) -> Result<ProviderStoredMcpOAuthTokenSecret, SecretProviderPrepareError> {
        let PreparedSecretEnvelope::McpOAuthToken(token) = stored else {
            return Err(SecretProviderPrepareError::Rejected);
        };
        token.validate_for(preparation)?;
        let reference = OpenBaoOpaqueSecretReferenceV1::new(
            &self.config,
            &preparation.tenant_id,
            &preparation.preparation_digest,
            MaterialKind::McpOAuthToken,
        )
        .map_err(resolve_prepare)?;
        let prepared = self.prepared(
            &reference,
            deterministic_binding_id(&preparation.task_id, &preparation.preparation_digest)?,
            &preparation.preparation_digest,
        )?;
        let exact = exact_binding(
            prepared.secret_binding_id.clone(),
            self.config.provider_id.clone(),
            preparation.token_credential_purpose.clone(),
            prepared.opaque_version_identity_digest.clone(),
        )?;
        Ok(ProviderStoredMcpOAuthTokenSecret {
            stored: StoredMcpOAuthTokenSecret {
                schema_version: 1,
                preparation_digest: token.preparation_digest,
                token_secret_binding: exact,
                granted_scopes: token.granted_scopes,
                audience_identity_digest: token.audience_identity_digest,
                issuer_identity_digest: token.issuer_identity_digest,
                subject_identity_digest: token.subject_identity_digest,
                verification_evidence_digest: token.verification_evidence_digest,
                expires_at: token.expires_at,
                storage_evidence_digest: prepared.storage_evidence_digest.clone(),
            },
            prepared_secret: prepared,
        })
    }
}

#[async_trait]
impl InstalledSecretProvider for OpenBaoProvider {
    fn provider_id(&self) -> &ResourceId {
        &self.config.provider_id
    }

    async fn resolve(
        &self,
        tenant_id: &ResourceId,
        reference: &OpaqueSecretReference,
        policy: &SecretResolutionPolicy,
    ) -> Result<ProviderSecretMaterial, SecretProviderResolveError> {
        let reference = OpenBaoOpaqueSecretReferenceV1::decode(reference, &self.config, tenant_id)?;
        reference.validate_policy(policy)?;
        let path = BaoSecretPath::parse(&reference.relative_path)
            .map_err(|_| SecretProviderResolveError::Rejected)?;
        let result = self
            .client
            .read_exact(&self.config.kv, &path, reference.version, self.deadline())
            .await;
        self.observe(SecretExternalDependency::Secret, &result);
        let read = result.map_err(resolve_error)?;
        if read.version != reference.version {
            return Err(SecretProviderResolveError::InvalidEvidence);
        }
        let envelope = decode_prepared(read.bytes.as_bytes())?;
        let bytes = match (reference.material_kind, envelope) {
            (MaterialKind::ModelCredential, PreparedSecretEnvelope::ModelCredential(value)) => {
                if value.schema_version != 1
                    || !value.identity.validate()
                    || value.identity.tenant_id != *tenant_id
                    || value.identity.provider_id != self.config.provider_id
                    || prepared_path(
                        &self.config,
                        tenant_id,
                        &value
                            .identity
                            .preparation_digest()
                            .map_err(|_| SecretProviderResolveError::InvalidEvidence)?,
                    )
                    .map_err(|_| SecretProviderResolveError::InvalidEvidence)?
                    .as_str()
                        != reference.relative_path
                {
                    return Err(SecretProviderResolveError::InvalidEvidence);
                }
                let key = SensitiveModelApiKey::new(value.api_key.decode()?)
                    .map_err(|_| SecretProviderResolveError::InvalidEvidence)?;
                key.expose().to_vec()
            }
            (MaterialKind::McpOAuthPkce, PreparedSecretEnvelope::McpOAuthPkce(value)) => {
                if value.schema_version != 1
                    || value.tenant_id != *tenant_id
                    || prepared_path(&self.config, tenant_id, &value.preparation_digest)?.as_str()
                        != reference.relative_path
                {
                    return Err(SecretProviderResolveError::InvalidEvidence);
                }
                value.pkce_verifier.decode()?
            }
            (MaterialKind::McpOAuthToken, PreparedSecretEnvelope::McpOAuthToken(value)) => {
                if value.schema_version != 1
                    || prepared_path(&self.config, tenant_id, &value.preparation_digest)?.as_str()
                        != reference.relative_path
                {
                    return Err(SecretProviderResolveError::InvalidEvidence);
                }
                value.access_token.decode()?
            }
            _ => return Err(SecretProviderResolveError::InvalidEvidence),
        };
        ProviderSecretMaterial::new(reference.version_digest()?, bytes)
    }

    async fn delete_exact(
        &self,
        tenant_id: &ResourceId,
        reference: &OpaqueSecretReference,
        policy: &SecretResolutionPolicy,
    ) -> Result<SecretProviderDeleteDisposition, SecretProviderDeleteError> {
        let reference = OpenBaoOpaqueSecretReferenceV1::decode(reference, &self.config, tenant_id)
            .map_err(|_| SecretProviderDeleteError::Rejected)?;
        reference
            .validate_policy(policy)
            .map_err(|_| SecretProviderDeleteError::Rejected)?;
        let path = BaoSecretPath::parse(&reference.relative_path)
            .map_err(|_| SecretProviderDeleteError::Rejected)?;
        let deadline = self.deadline();
        let metadata = self.client.metadata(&self.config.kv, &path, deadline).await;
        self.observe(SecretExternalDependency::Secret, &metadata);
        let metadata = metadata.map_err(delete_error)?;
        if metadata.current_version != reference.version {
            return Err(SecretProviderDeleteError::Rejected);
        }
        let version = metadata
            .versions
            .get(&reference.version)
            .ok_or(SecretProviderDeleteError::OutcomeUncertain)?;
        if version.destroyed {
            return Ok(SecretProviderDeleteDisposition::AlreadyAbsent);
        }
        let result = self
            .client
            .destroy_exact(&self.config.kv, &path, reference.version, deadline)
            .await;
        self.observe(SecretExternalDependency::Secret, &result);
        result.map_err(delete_error)?;
        Ok(SecretProviderDeleteDisposition::Deleted)
    }

    async fn prepare_or_load_model_credential(
        &self,
        request: &ModelCredentialImportAuthorizationV1,
        permit: &ModelCredentialImportPermitV1,
        key: &SensitiveModelApiKey,
    ) -> Result<ProviderPreparedSecretVersion, SecretProviderPrepareError> {
        check_import_permit(Some((request, permit)))?;
        let identity = &request.identity;
        if identity.provider_id != self.config.provider_id {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let preparation = identity
            .preparation_digest()
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        let reference = OpenBaoOpaqueSecretReferenceV1::new(
            &self.config,
            &identity.tenant_id,
            &preparation,
            MaterialKind::ModelCredential,
        )
        .map_err(resolve_prepare)?;
        let path = BaoSecretPath::parse(&reference.relative_path)
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        let remaining = (permit.valid_until - Utc::now())
            .to_std()
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        let deadline = self.deadline().min(Instant::now() + remaining);
        let stored = self
            .create_or_load(
                &path,
                PreparedSecretEnvelope::ModelCredential(PreparedModelCredential {
                    schema_version: 1,
                    identity: identity.clone(),
                    api_key: SecretBytes::encode(key.expose()),
                }),
                Some((request, permit)),
                deadline,
            )
            .await?;
        let PreparedSecretEnvelope::ModelCredential(stored) = stored else {
            return Err(SecretProviderPrepareError::Rejected);
        };
        let stored_key =
            SensitiveModelApiKey::new(stored.api_key.decode().map_err(resolve_prepare)?)
                .map_err(|_| SecretProviderPrepareError::Rejected)?;
        if stored.schema_version != 1
            || stored.identity != *identity
            || stored_key.expose() != key.expose()
        {
            return Err(SecretProviderPrepareError::Rejected);
        }
        self.prepared(
            &reference,
            identity
                .secret_binding_id()
                .map_err(|_| SecretProviderPrepareError::Rejected)?,
            &preparation,
        )
    }

    async fn prepare_or_load_mcp_oauth_transient(
        &self,
        candidate: NewMcpOAuthTransientSecretBundle,
    ) -> Result<ProviderStoredMcpOAuthTransientSecretBundle, SecretProviderPrepareError> {
        candidate
            .validate()
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        if candidate.pkce_secret_provider_id != self.config.provider_id {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let reference = OpenBaoOpaqueSecretReferenceV1::new(
            &self.config,
            &candidate.tenant_id,
            &candidate.preparation_digest,
            MaterialKind::McpOAuthPkce,
        )
        .map_err(resolve_prepare)?;
        let path = BaoSecretPath::parse(&reference.relative_path)
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        let proposed = PreparedSecretEnvelope::McpOAuthPkce(PreparedMcpOAuthPkce {
            schema_version: 1,
            tenant_id: candidate.tenant_id.clone(),
            task_id: candidate.task_id.clone(),
            authorization_binding_id: candidate.authorization_binding_id.clone(),
            mcp_deployment: candidate.mcp_deployment.clone(),
            preparation_digest: candidate.preparation_digest.clone(),
            callback_binding_digest: candidate.callback_binding_digest.clone(),
            expires_at: candidate.expires_at,
            state: SecretBytes::encode(candidate.state.as_bytes()),
            nonce: SecretBytes::encode(candidate.nonce.as_bytes()),
            pkce_verifier: SecretBytes::encode(candidate.pkce_verifier.expose()),
        });
        let stored = self
            .create_or_load(&path, proposed, None, self.deadline())
            .await?;
        let PreparedSecretEnvelope::McpOAuthPkce(stored) = stored else {
            return Err(SecretProviderPrepareError::Rejected);
        };
        stored.validate_for_transient(&candidate)?;
        let prepared = self.prepared(
            &reference,
            deterministic_binding_id(&candidate.task_id, &candidate.preparation_digest)?,
            &candidate.preparation_digest,
        )?;
        let exact = exact_binding(
            prepared.secret_binding_id.clone(),
            self.config.provider_id.clone(),
            MCP_OAUTH_PKCE_SECRET_PURPOSE
                .parse()
                .map_err(|_| SecretProviderPrepareError::Rejected)?,
            prepared.opaque_version_identity_digest.clone(),
        )?;
        Ok(ProviderStoredMcpOAuthTransientSecretBundle {
            stored: StoredMcpOAuthTransientSecretBundle {
                schema_version: 1,
                tenant_id: stored.tenant_id,
                task_id: stored.task_id,
                authorization_binding_id: stored.authorization_binding_id,
                mcp_deployment: stored.mcp_deployment,
                pkce_secret_provider_id: self.config.provider_id.clone(),
                preparation_digest: stored.preparation_digest,
                callback_binding_digest: stored.callback_binding_digest,
                expires_at: stored.expires_at,
                state: SensitiveOAuthValue::from_decoded(
                    stored.state.decode().map_err(resolve_prepare)?,
                    insight_platform_mcp_host::MAX_MCP_OAUTH_STATE_BYTES,
                )
                .map_err(|_| SecretProviderPrepareError::Rejected)?,
                nonce: SensitiveMcpOAuthNonce::new(stored.nonce.decode().map_err(resolve_prepare)?)
                    .map_err(|_| SecretProviderPrepareError::Rejected)?,
                pkce_verifier: SensitiveMcpOAuthPkceVerifier::new(
                    stored.pkce_verifier.decode().map_err(resolve_prepare)?,
                )
                .map_err(|_| SecretProviderPrepareError::Rejected)?,
                pkce_secret_binding: exact,
                storage_evidence_digest: prepared.storage_evidence_digest.clone(),
            },
            prepared_secret: prepared,
        })
    }

    async fn load_prepared_mcp_oauth_token(
        &self,
        preparation: &McpOAuthTokenPreparation,
    ) -> Result<Option<ProviderStoredMcpOAuthTokenSecret>, SecretProviderPrepareError> {
        preparation
            .validate_at(Utc::now())
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        if preparation.token_secret_provider_id != self.config.provider_id {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let path = prepared_path(
            &self.config,
            &preparation.tenant_id,
            &preparation.preparation_digest,
        )
        .map_err(resolve_prepare)?;
        self.load_prepared(&path, None, self.deadline())
            .await?
            .map(|stored| self.token_result(preparation, stored))
            .transpose()
    }

    async fn prepare_or_load_mcp_oauth_token(
        &self,
        preparation: &McpOAuthTokenPreparation,
        tokens: &McpOAuthTokenSet,
        verified: &VerifiedMcpOAuthToken,
    ) -> Result<ProviderStoredMcpOAuthTokenSecret, SecretProviderPrepareError> {
        preparation
            .validate_at(Utc::now())
            .map_err(|_| SecretProviderPrepareError::Rejected)?;
        if preparation.token_secret_provider_id != self.config.provider_id {
            return Err(SecretProviderPrepareError::Rejected);
        }
        let path = prepared_path(
            &self.config,
            &preparation.tenant_id,
            &preparation.preparation_digest,
        )
        .map_err(resolve_prepare)?;
        let proposed = PreparedSecretEnvelope::McpOAuthToken(PreparedMcpOAuthToken {
            schema_version: 1,
            preparation_digest: preparation.preparation_digest.clone(),
            access_token: SecretBytes::encode(tokens.access_token.expose()),
            refresh_token: tokens
                .refresh_token
                .as_ref()
                .map(|v| SecretBytes::encode(v.expose())),
            id_token: tokens
                .id_token
                .as_ref()
                .map(|v| SecretBytes::encode(v.expose())),
            granted_scopes: verified.granted_scopes.clone(),
            audience_identity_digest: verified.audience_identity_digest.clone(),
            issuer_identity_digest: verified.issuer_identity_digest.clone(),
            subject_identity_digest: verified.subject_identity_digest.clone(),
            verification_evidence_digest: verified.verification_evidence_digest.clone(),
            expires_at: verified.expires_at,
        });
        let stored = self
            .create_or_load(&path, proposed, None, self.deadline())
            .await?;
        self.token_result(preparation, stored)
    }
}

fn aad(
    tenant: &ResourceId,
    binding: &ResourceId,
    provider: &ResourceId,
    generation: u64,
    key_id: &str,
) -> Result<Vec<u8>, SecretReferenceSealError> {
    validate_seal_identity(tenant, binding, provider, generation)?;
    let context = reference_encryption_context(tenant, binding, provider, generation, key_id);
    serde_jcs::to_vec(
        &serde_json::json!({"domain":"openbao_secret_reference_v1", "context":context}),
    )
    .map_err(|_| SecretReferenceSealError::InvalidEvidence)
}

#[async_trait]
impl SecretReferenceSealer for OpenBaoProvider {
    async fn seal(
        &self,
        tenant: &ResourceId,
        binding: &ResourceId,
        provider: &ResourceId,
        generation: u64,
        reference: &OpaqueSecretReference,
    ) -> Result<SealedSecretReference, SecretReferenceSealError> {
        if provider != &self.config.provider_id {
            return Err(SecretReferenceSealError::Rejected);
        }
        OpenBaoOpaqueSecretReferenceV1::decode(reference, &self.config, tenant)
            .map_err(|_| SecretReferenceSealError::InvalidEvidence)?;
        let key_id = self.config.reference_key.key_id();
        let context = aad(tenant, binding, provider, generation, &key_id)?;
        let result = self
            .client
            .encrypt(
                &self.config.reference_key,
                reference.expose(),
                &context,
                self.deadline(),
            )
            .await;
        self.observe(SecretExternalDependency::Kms, &result);
        let encrypted = result.map_err(seal_error)?;
        Ok(SealedSecretReference {
            encrypted_reference: EncryptedOpaqueReference::new(encrypted.into_bytes())
                .map_err(|_| SecretReferenceSealError::InvalidEvidence)?,
            key_id,
            reference_digest: digest(reference.expose()),
        })
    }
}

#[async_trait]
impl SecretReferenceUnsealer for OpenBaoProvider {
    async fn unseal(
        &self,
        record: &SecretBindingResolutionRecord,
    ) -> Result<OpaqueSecretReference, SecretReferenceUnsealError> {
        record
            .validate()
            .map_err(|_| SecretReferenceUnsealError::InvalidEvidence)?;
        if record.provider_id != self.config.provider_id
            || record.key_id != self.config.reference_key.key_id()
        {
            return Err(SecretReferenceUnsealError::Rejected);
        }
        let context = aad(
            &record.tenant_id,
            &record.secret_binding_id,
            &record.provider_id,
            record.generation,
            &record.key_id,
        )
        .map_err(|_| SecretReferenceUnsealError::InvalidEvidence)?;
        let result = self
            .client
            .decrypt(
                &self.config.reference_key,
                record.encrypted_reference.as_bytes(),
                &context,
                self.deadline(),
            )
            .await;
        self.observe(SecretExternalDependency::Kms, &result);
        let plaintext = result.map_err(|e| match seal_error(e) {
            SecretReferenceSealError::Unavailable => SecretReferenceUnsealError::Unavailable,
            SecretReferenceSealError::Rejected => SecretReferenceUnsealError::Rejected,
            SecretReferenceSealError::InvalidEvidence => {
                SecretReferenceUnsealError::InvalidEvidence
            }
        })?;
        let reference = OpaqueSecretReference::new(plaintext.into_bytes())?;
        if digest(reference.expose()) != record.reference_digest {
            return Err(SecretReferenceUnsealError::InvalidEvidence);
        }
        OpenBaoOpaqueSecretReferenceV1::decode(&reference, &self.config, &record.tenant_id)
            .map_err(|_| SecretReferenceUnsealError::InvalidEvidence)?;
        Ok(reference)
    }
}

fn prepare_error(error: BaoError) -> SecretProviderPrepareError {
    match error {
        BaoError::Unavailable => SecretProviderPrepareError::Unavailable,
        BaoError::UnknownOutcome => SecretProviderPrepareError::WriteUncertain,
        _ => SecretProviderPrepareError::Rejected,
    }
}
fn resolve_prepare(error: SecretProviderResolveError) -> SecretProviderPrepareError {
    match error {
        SecretProviderResolveError::Unavailable => SecretProviderPrepareError::Unavailable,
        _ => SecretProviderPrepareError::Rejected,
    }
}
fn resolve_error(error: BaoError) -> SecretProviderResolveError {
    match error {
        BaoError::Unavailable | BaoError::UnknownOutcome => SecretProviderResolveError::Unavailable,
        BaoError::NotFound => SecretProviderResolveError::NotFound,
        BaoError::InvalidEvidence => SecretProviderResolveError::InvalidEvidence,
        _ => SecretProviderResolveError::Rejected,
    }
}
fn delete_error(error: BaoError) -> SecretProviderDeleteError {
    match error {
        BaoError::Unavailable => SecretProviderDeleteError::Unavailable,
        BaoError::UnknownOutcome | BaoError::NotFound => {
            SecretProviderDeleteError::OutcomeUncertain
        }
        _ => SecretProviderDeleteError::Rejected,
    }
}
fn seal_error(error: BaoError) -> SecretReferenceSealError {
    match error {
        BaoError::Unavailable | BaoError::UnknownOutcome => SecretReferenceSealError::Unavailable,
        BaoError::InvalidEvidence => SecretReferenceSealError::InvalidEvidence,
        _ => SecretReferenceSealError::Rejected,
    }
}
