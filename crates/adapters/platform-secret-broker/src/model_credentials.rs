use super::*;
use insight_platform_contracts::{
    ModelCredentialImportAuthorizationV1, ModelCredentialImportError as Failure,
    SensitiveModelApiKey,
};
use insight_platform_security::ModelCredentialImporter;

#[async_trait]
impl ModelCredentialImporter for BrokeredPreparedSecretStore {
    async fn import_model_credential(
        &self,
        request: ModelCredentialImportAuthorizationV1,
        key: SensitiveModelApiKey,
    ) -> Result<ExactSecretBindingRef, Failure> {
        let now = Utc::now();
        if !request.validate_at(now) || key.expose().len() > self.limits.maximum_material_bytes {
            return Err(Failure::Rejected);
        }
        let _capacity = self
            .in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| Failure::TemporarilyUnavailable)?;
        let duration = (request.deadline - now)
            .to_std()
            .map_err(|_| Failure::Rejected)?
            .min(self.limits.resolution_timeout);
        // Timeout can occur after an external write or registration commit. Retain the operation.
        timeout(duration, self.import_model_credential_inner(&request, &key))
            .await
            .map_err(|_| Failure::OutcomeUnknown)?
    }
}
impl BrokeredPreparedSecretStore {
    async fn import_model_credential_inner(
        &self,
        request: &ModelCredentialImportAuthorizationV1,
        key: &SensitiveModelApiKey,
    ) -> Result<ExactSecretBindingRef, Failure> {
        let identity = &request.identity;
        let provider = self
            .providers
            .get(&identity.provider_id)
            .ok_or(Failure::Rejected)?;
        let authority = self.import_authority.as_ref().ok_or(Failure::Rejected)?;
        let permit = authority.authorize_model_credential_import(request).await?;
        if !permit.validate_for(request, Utc::now()) {
            return Err(Failure::Rejected);
        }
        let prepared = provider
            .prepare_or_load_model_credential(request, &permit, key)
            .await
            .map_err(|error| match error {
                SecretProviderPrepareError::Rejected => Failure::Rejected,
                SecretProviderPrepareError::Unavailable => Failure::TemporarilyUnavailable,
                SecretProviderPrepareError::WriteUncertain => Failure::OutcomeUnknown,
            })?;
        if prepared.secret_binding_id != identity.secret_binding_id()?
            || prepared.provider_id != identity.provider_id
        {
            return Err(Failure::Rejected);
        }
        let binding = ExactSecretBindingRef::build(
            prepared.secret_binding_id.clone(),
            1,
            identity.provider_id.clone(),
            identity.purpose.clone(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: prepared.opaque_version_identity_digest.clone(),
            },
        )
        .map_err(|_| Failure::Rejected)?;
        validate_provider_prepared_secret(
            &identity.tenant_id,
            &identity.purpose,
            &binding,
            &prepared,
        )
        .map_err(|_| Failure::Rejected)?;
        // Same authorization deadline before the distinct KMS boundary, without extending it.
        if !permit.validate_for(request, Utc::now()) {
            return Err(Failure::Rejected);
        }
        let sealed = self
            .sealer
            .seal(
                &identity.tenant_id,
                &prepared.secret_binding_id,
                &identity.provider_id,
                1,
                &prepared.opaque_reference,
            )
            .await
            .map_err(|error| match error {
                SecretReferenceSealError::Unavailable => Failure::TemporarilyUnavailable,
                _ => Failure::Rejected,
            })?;
        if sealed.reference_digest != digest(prepared.opaque_reference.expose()) {
            return Err(Failure::Rejected);
        }
        if !permit.validate_for(request, Utc::now()) {
            return Err(Failure::Rejected);
        }
        let preparation_digest = identity.preparation_digest()?;
        let mut command = RegisterPreparedSecretBinding {
            audit: CommandAudit {
                trace: insight_platform_contracts::TraceIdentityV1::generate(),
                tenant_id: identity.tenant_id.clone(),
                principal_id: self.service_principal_id.clone(),
                principal_kind: PrincipalKind::ServiceIdentity,
                receipt_id: new_resource_id(ResourceKind::Receipt)
                    .map_err(|_| Failure::Rejected)?,
                event_id: new_resource_id(ResourceKind::Event).map_err(|_| Failure::Rejected)?,
                outbox_id: new_resource_id(ResourceKind::OutboxEvent)
                    .map_err(|_| Failure::Rejected)?,
                idempotency_key_digest: preparation_digest.clone(),
                request_digest: preparation_digest.clone(),
                receipt_expires_at: Utc::now() + MAX_PREPARED_SECRET_TTL,
            },
            preparation_digest,
            secret_binding_id: prepared.secret_binding_id,
            purpose: identity.purpose.clone(),
            provider_id: identity.provider_id.clone(),
            encrypted_reference: sealed.encrypted_reference,
            key_id: sealed.key_id,
            reference_digest: sealed.reference_digest,
            opaque_version_identity_digest: prepared.opaque_version_identity_digest,
            provider_storage_evidence_digest: prepared.storage_evidence_digest,
            delegated_import: Some(identity.clone()),
        };
        command.audit.request_digest = command
            .semantic_request_digest()
            .map_err(|_| Failure::Rejected)?;
        let outcome =
            self.registration.register_prepared(command).await.map_err(
                |failure| match failure {
                    PreparedSecretBindingRegistrationError::Rejected => Failure::Rejected,
                    // The commit result can be lost. Only the same operation may be retried.
                    PreparedSecretBindingRegistrationError::TemporarilyUnavailable => {
                        Failure::OutcomeUnknown
                    }
                },
            )?;
        if outcome.exact_binding != binding {
            return Err(Failure::Rejected);
        }
        Ok(binding)
    }
}
