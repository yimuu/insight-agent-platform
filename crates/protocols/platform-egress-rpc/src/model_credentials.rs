use super::*;
use insight_platform_contracts::{
    ExactSecretBindingRef, ModelCredentialImportAuthorizationV1,
    ModelCredentialImportError as Failure, SensitiveModelApiKey, MAX_MODEL_API_KEY_BYTES,
};
use insight_platform_security::ModelCredentialImporter;
const IMPORT: &str = "model_credential.import/v1";
const OUTCOME: &str = "model_credential.import_outcome/v1";
fn import_limits() -> EgressInternalRpcLimits {
    EgressInternalRpcLimits {
        maximum_metadata_bytes: 4096,
        maximum_payload_bytes: MAX_MODEL_API_KEY_BYTES,
    }
}
/// Ensures even invalid private wire payloads are cleared before their allocation is released.
struct SensitiveEnvelope(ClosedEgressEnvelope);
impl Drop for SensitiveEnvelope {
    fn drop(&mut self) {
        self.0.payload.fill(0);
    }
}

#[async_trait]
impl ModelCredentialImporter for EgressBrokerGrpcClient {
    async fn import_model_credential(
        &self,
        request: ModelCredentialImportAuthorizationV1,
        key: SensitiveModelApiKey,
    ) -> Result<ExactSecretBindingRef, Failure> {
        let now = Utc::now();
        if !request.validate_at(now) {
            return Err(Failure::Rejected);
        }
        let mut wire = Request::new(
            encode_metadata_payload(&request, key.expose().to_vec(), IMPORT, import_limits())
                .map_err(|_| Failure::Rejected)?,
        );
        wire.set_timeout(
            (request.deadline - now)
                .to_std()
                .map_err(|_| Failure::Rejected)?,
        );
        let response = self.client.clone().import_model_credential(wire).await;
        observe_egress_rpc(&self.dependency_observer, response.is_ok());
        // A transport failure may hide a committed external or PostgreSQL result.
        let response = response.map_err(|status| match status.code() {
            tonic::Code::PermissionDenied
            | tonic::Code::Unauthenticated
            | tonic::Code::InvalidArgument => Failure::Rejected,
            _ => Failure::OutcomeUnknown,
        })?;
        let outcome = decode_metadata::<UnaryOutcome<ExactSecretBindingRef, Failure>>(
            response.into_inner(),
            OUTCOME,
            self.limits,
        )
        .map_err(|_| Failure::OutcomeUnknown)?;
        match outcome {
            UnaryOutcome::Failed(error) => Err(error),
            UnaryOutcome::Succeeded(binding) => {
                validate_binding(&request, &binding).map_err(|_| Failure::OutcomeUnknown)?;
                Ok(binding)
            }
        }
    }
}
fn validate_binding(
    request: &ModelCredentialImportAuthorizationV1,
    binding: &ExactSecretBindingRef,
) -> Result<(), Failure> {
    if binding.validate().is_err()
        || binding.secret_binding_id != request.identity.secret_binding_id()?
        || binding.provider_id != request.identity.provider_id
        || binding.purpose != request.identity.purpose
        || binding.binding_generation != 1
        || !matches!(
            binding.resolution_policy,
            SecretResolutionPolicy::Pinned { .. }
        )
    {
        return Err(Failure::Rejected);
    }
    Ok(())
}

pub(super) async fn serve_import(
    importer: Option<&dyn ModelCredentialImporter>,
    request: Request<ClosedEgressEnvelope>,
    limits: EgressInternalRpcLimits,
) -> Result<Response<ClosedEgressEnvelope>, Status> {
    // The whole private body remains zeroing even for the wrong authenticated role.
    let authorized = require_role(&request, EgressCallerRole::Gateway);
    let trace = trace_context(&request);
    let mut wire = SensitiveEnvelope(request.into_inner());
    authorized?;
    let trace = trace?;
    let metadata: ModelCredentialImportAuthorizationV1 =
        decode_metadata_payload_ref(&wire.0, IMPORT, import_limits())
            .map_err(|_| Status::invalid_argument("invalid credential import"))?;
    if !metadata.validate_at(Utc::now()) {
        return Err(Status::invalid_argument("invalid credential import"));
    }
    let key = SensitiveModelApiKey::new(std::mem::take(&mut wire.0.payload))
        .map_err(|_| Status::invalid_argument("invalid credential import"))?;
    let importer =
        importer.ok_or_else(|| Status::unavailable("credential importer unavailable"))?;
    let identity = metadata.clone();
    let outcome = match scope_trace(trace, importer.import_model_credential(metadata, key)).await {
        Ok(binding) if validate_binding(&identity, &binding).is_ok() => {
            UnaryOutcome::Succeeded(binding)
        }
        Ok(_) => UnaryOutcome::Failed(Failure::OutcomeUnknown),
        Err(failure) => UnaryOutcome::Failed(failure),
    };
    Ok(Response::new(encode_metadata(&outcome, OUTCOME, limits)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{PrincipalKind, TraceFlags, TraceIdentityV1};
    fn metadata() -> ModelCredentialImportAuthorizationV1 {
        ModelCredentialImportAuthorizationV1 {
            schema_version: 1,
            deadline: Utc::now() + Duration::seconds(25),
            identity: insight_platform_contracts::ModelCredentialImportIdentityV1 {
                schema_version: 1,
                operation_id: "a8376371-3d45-4ef6-8c8c-eb1a895fa99c".parse().unwrap(),
                tenant_id: "ten_0198f1c3-8f49-7c3e-b1f3-773c28367d10".parse().unwrap(),
                principal_id: "prn_0198f1c3-8f49-7c3e-b1f3-773c28367d12".parse().unwrap(),
                principal_kind: PrincipalKind::TenantAdmin,
                provider_id: "spr_0198f1c3-8f49-7c3e-b1f3-773c28367d15".parse().unwrap(),
                purpose: "model_api_key".parse().unwrap(),
            },
        }
    }
    struct Importer(std::sync::atomic::AtomicUsize);
    #[async_trait]
    impl ModelCredentialImporter for Importer {
        async fn import_model_credential(
            &self,
            request: ModelCredentialImportAuthorizationV1,
            key: SensitiveModelApiKey,
        ) -> Result<ExactSecretBindingRef, Failure> {
            assert_eq!(key.expose(), b"private-payload-canary");
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            ExactSecretBindingRef::build(
                request.identity.secret_binding_id()?,
                1,
                request.identity.provider_id,
                request.identity.purpose,
                SecretResolutionPolicy::Pinned {
                    opaque_version_identity_digest: raw_digest(b"version").unwrap(),
                },
            )
            .map_err(|_| Failure::Rejected)
        }
    }
    fn request(
        envelope: ClosedEgressEnvelope,
        role: EgressCallerRole,
    ) -> Request<ClosedEgressEnvelope> {
        let mut request = Request::new(envelope);
        request.extensions_mut().insert(role);
        request.extensions_mut().insert(
            ExecutionTraceContext::start(TraceIdentityV1::generate(), TraceFlags::NotSampled)
                .unwrap(),
        );
        request
    }
    #[tokio::test]
    async fn private_import_wire_is_role_bound_and_payload_is_never_metadata() {
        let importer = Importer(std::sync::atomic::AtomicUsize::new(0));
        let metadata = metadata();
        let envelope = encode_metadata_payload(
            &metadata,
            b"private-payload-canary".to_vec(),
            IMPORT,
            import_limits(),
        )
        .unwrap();
        assert!(!String::from_utf8_lossy(&envelope.canonical_metadata_json)
            .contains("private-payload-canary"));
        for role in [
            EgressCallerRole::ModelWorker,
            EgressCallerRole::McpCallback,
            EgressCallerRole::CapabilityWorker,
        ] {
            assert_eq!(
                serve_import(
                    Some(&importer),
                    request(envelope.clone(), role),
                    import_limits()
                )
                .await
                .unwrap_err()
                .code(),
                tonic::Code::PermissionDenied
            );
        }
        let mut swapped = envelope.clone();
        swapped.payload = b"other-input".to_vec();
        assert_eq!(
            serve_import(
                Some(&importer),
                request(swapped, EgressCallerRole::Gateway),
                import_limits()
            )
            .await
            .unwrap_err()
            .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(importer.0.load(std::sync::atomic::Ordering::SeqCst), 0);
        let response = serve_import(
            Some(&importer),
            request(envelope, EgressCallerRole::Gateway),
            import_limits(),
        )
        .await
        .unwrap()
        .into_inner();
        assert!(response.payload.is_empty());
        assert!(!String::from_utf8_lossy(&response.canonical_metadata_json)
            .contains("private-payload-canary"));
        assert_eq!(importer.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        let UnaryOutcome::Succeeded(binding) = decode_metadata::<
            UnaryOutcome<ExactSecretBindingRef, Failure>,
        >(response, OUTCOME, import_limits())
        .unwrap() else {
            panic!("import failed")
        };
        validate_binding(&metadata, &binding).unwrap();
    }
}
