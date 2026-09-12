use super::*;
use insight_platform_contracts::{
    ModelConnectionError as Failure, ModelConnectionObservationV1,
    ModelConnectionOutcome as Outcome, ModelConnectionProbeAuthorizationV1,
    MODEL_PROBE_CONNECT_MILLISECONDS, MODEL_PROBE_MAXIMUM_RESPONSE_BYTES,
};
use insight_platform_security::ModelConnectionProbe;

#[async_trait]
impl ModelConnectionProbe for ReqwestModelProviderEgressBroker {
    async fn probe_model_connection(
        &self,
        request: ModelConnectionProbeAuthorizationV1,
    ) -> Result<ModelConnectionObservationV1, Failure> {
        let now = Utc::now();
        if !request.validate_at(now) {
            return Err(Failure::Rejected);
        }
        let authority = self.probe_authority.as_ref().ok_or(Failure::Unavailable)?;
        let _global = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Failure::Unavailable)?;
        let _diagnostic = self
            .probe_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Failure::Unavailable)?;
        let timeout = (request.deadline_at() - now)
            .to_std()
            .map_err(|_| Failure::Rejected)?;
        // The monotonic outer timeout covers authorization, DNS, Secret, HTTP and body reads.
        let deadline = tokio::time::Instant::now() + timeout;
        let permit = tokio::time::timeout_at(
            deadline,
            authority.authorize_model_connection_probe(&request),
        )
        .await
        .map_err(|_| Failure::Unavailable)??;
        if !permit.validate_for(&request, Utc::now()) {
            return Err(Failure::Rejected);
        }
        let target = &permit.target;
        let provider = &target.provider;
        let candidate = match &self.catalog.routing {
            insight_platform_contracts::ModelEgressRoutingV1::Fixed { destinations } => {
                destinations
                    .iter()
                    .find(|entry| {
                        entry.protocol == target.protocol && entry.endpoint == provider.endpoint
                    })
                    .cloned()
            }
            insight_platform_contracts::ModelEgressRoutingV1::PublicHttps { grant } => grant
                .destination(
                    target.protocol,
                    provider.endpoint.clone(),
                    provider.region.clone(),
                ),
        };
        let entry = candidate
            .filter(|e| {
                e.endpoint_identity_digest == provider.endpoint_identity_digest
                    && e.network_policy == provider.network_policy
                    && e.tls_policy == provider.tls_policy
                    && e.trust_policy == provider.trust_policy
                    && e.data_policy == provider.data_policy
                    && e.region == provider.region
                    && provider.secret_bindings.len() == 1
                    && provider.secret_bindings[0].purpose == e.credential_purpose
            })
            .ok_or(Failure::Rejected)?;
        entry.validate().map_err(|_| Failure::Rejected)?;
        let body = insight_platform_model_adapters::model_connection_request(target)?;
        let cancellation = CancellationToken::new();
        let operation = async {
            let (dns_host, addresses) = self
                .resolve_addresses(&entry, &cancellation, request.deadline_at())
                .await
                .map_err(|_| Outcome::TransportUnavailable)?;
            if !permit.validate_for(&request, Utc::now()) {
                return Ok(Err(Failure::Rejected));
            }
            let binding = &provider.secret_bindings[0];
            let credential = match self.secrets.resolve(&request.tenant_id, binding).await {
                Ok(value) => value,
                Err(SecretMaterialResolutionError::Unavailable) => {
                    return Ok(Err(Failure::Unavailable))
                }
                Err(
                    SecretMaterialResolutionError::NotFound
                    | SecretMaterialResolutionError::Revoked
                    | SecretMaterialResolutionError::InvalidEvidence,
                ) => return Ok(Err(Failure::Rejected)),
            };
            if !credential.validate_for(binding, self.limits.maximum_secret_material_bytes)
                || credential.binding_generation != target.credential_generation
            {
                return Ok(Err(Failure::Rejected));
            }
            // This is a second current-authority RPC after the physical read, not a cached permit.
            let current = match authority.authorize_model_connection_probe(&request).await {
                Ok(p) => p,
                Err(e) => return Ok(Err(e)),
            };
            if !current.validate_for(&request, Utc::now())
                || current.target_digest != permit.target_digest
            {
                return Ok(Err(Failure::Rejected));
            }
            let mut headers = provider_headers(target.protocol, Some(&credential))
                .map_err(|_| Outcome::CredentialsRejected)?;
            headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
            let remaining = (request.deadline_at() - Utc::now())
                .to_std()
                .map_err(|_| Outcome::TimedOut)?;
            let total_timeout = remaining.min(Duration::from_millis(
                target.request_limits.total_timeout_milliseconds,
            ));
            let response_limit = MODEL_PROBE_MAXIMUM_RESPONSE_BYTES
                .min(target.request_limits.maximum_response_bytes as usize);
            let response = self
                .transport
                .open(PinnedHttpRequest {
                    url: endpoint_url(&entry).map_err(|_| Outcome::TransportUnavailable)?,
                    dns_host,
                    addresses,
                    headers,
                    body,
                    connect_timeout: Duration::from_millis(
                        target
                            .request_limits
                            .connect_timeout_milliseconds
                            .min(MODEL_PROBE_CONNECT_MILLISECONDS),
                    )
                    .min(total_timeout),
                    total_timeout,
                    maximum_response_bytes: response_limit as u64,
                    deadline: request.deadline_at(),
                    cancellation: cancellation.clone(),
                    trusted_root_pem: entry.trusted_root_pem.clone(),
                })
                .await
                .map_err(|_| Outcome::TransportUnavailable)?;
            let outcome = match response.status_code {
                401 | 403 => Outcome::CredentialsRejected,
                404 => Outcome::ModelUnavailable,
                429 => Outcome::RateLimited,
                500..=599 => Outcome::ProviderUnavailable,
                200..=299 => {
                    if !response
                        .content_type
                        .split(';')
                        .next()
                        .is_some_and(|s| s.trim().eq_ignore_ascii_case("application/json"))
                    {
                        return Err(Outcome::InvalidResponse);
                    }
                    let mut stream = response.body;
                    let mut bytes = Vec::new();
                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk.map_err(|_| Outcome::TransportUnavailable)?;
                        if chunk.len() > response_limit.saturating_sub(bytes.len()) {
                            bytes.fill(0);
                            return Err(Outcome::InvalidResponse);
                        }
                        bytes.extend_from_slice(&chunk);
                    }
                    let valid = insight_platform_model_adapters::model_connection_response(
                        target.protocol,
                        &bytes,
                    );
                    bytes.fill(0);
                    if valid {
                        Outcome::ResponseReceived
                    } else {
                        Outcome::InvalidResponse
                    }
                }
                _ => Outcome::InvalidResponse,
            };
            Ok(Ok(outcome))
        };
        let outcome = match tokio::time::timeout_at(deadline, operation).await {
            Ok(Ok(Ok(outcome))) | Ok(Err(outcome)) => outcome,
            Ok(Ok(Err(e))) => return Err(e),
            Err(_) => {
                cancellation.cancel();
                Outcome::TimedOut
            }
        };
        Ok(target.observation(outcome, Utc::now()))
    }
}
