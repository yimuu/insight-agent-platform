use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header::AUTHORIZATION, HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    ApiProblem, ApiProblemCode, AuthnStrength, PermissionSet, PrincipalKind, PrincipalSnapshot,
    ResourceId, ResourceKind, Sha256Digest, MAX_FIELD_ERRORS, MAX_SAFE_TEXT_BYTES,
};
use std::sync::Arc;

const MAX_BEARER_TOKEN_BYTES: usize = 16_384;
const MAX_CREDENTIAL_LIFETIME_SECONDS: i64 = 86_400;
const RESERVED_IDENTITY_HEADERS: &[&str] = &[
    "x-platform-tenant-id",
    "x-platform-principal-id",
    "x-platform-principal-kind",
    "x-platform-credential-digest",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedExternalCredential {
    pub tenant_id: ResourceId,
    pub authentication_authority_digest: Sha256Digest,
    pub subject_digest: Sha256Digest,
    pub credential_digest: Sha256Digest,
    pub principal_kind: PrincipalKind,
    pub authn_strength: AuthnStrength,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

impl VerifiedExternalCredential {
    fn validate_at(&self, now: DateTime<Utc>) -> Result<(), AuthenticationError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.principal_kind == PrincipalKind::InstallationOperator
            || self.issued_at > now
            || self.expires_at <= now
            || self.expires_at <= self.issued_at
            || (self.expires_at - self.issued_at).num_seconds() > MAX_CREDENTIAL_LIFETIME_SECONDS
        {
            return Err(AuthenticationError::Unauthenticated);
        }
        Ok(())
    }
}

pub trait ExternalCredentialVerifier: Send + Sync {
    fn verify(
        &self,
        bearer_token: &str,
        now: DateTime<Utc>,
    ) -> Result<VerifiedExternalCredential, AuthenticationError>;
}

/// Port implemented by the public Gateway composition. The adapter rebinds verified external
/// identity evidence to current active tenant membership without accepting a principal ID.
#[async_trait]
pub trait ExternalPrincipalBindingAuthority: Send + Sync {
    async fn resolve_external_principal(
        &self,
        tenant_id: ResourceId,
        authentication_authority_digest: Sha256Digest,
        subject_digest: Sha256Digest,
        asserted_principal_kind: PrincipalKind,
    ) -> Result<PrincipalSnapshot, AuthenticationError>;
}

pub trait AuthenticationClock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemAuthenticationClock;

impl AuthenticationClock for SystemAuthenticationClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedPrincipal {
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub permissions: PermissionSet,
    pub authn_strength: AuthnStrength,
    pub principal_version: u64,
    pub binding_generation: u64,
    pub binding_version: u64,
    pub credential_digest: Sha256Digest,
    pub credential_expires_at: DateTime<Utc>,
    pub trace: insight_platform_contracts::TraceIdentityV1,
}

impl AuthenticatedPrincipal {
    pub fn validate(&self) -> Result<(), AuthenticationError> {
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.principal_id.kind() != ResourceKind::Principal
            || self.principal_kind == PrincipalKind::InstallationOperator
            || self.principal_version == 0
            || self.binding_generation == 0
            || self.binding_version == 0
            || self.trace.validate().is_err()
        {
            return Err(AuthenticationError::Unauthenticated);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationError {
    InvalidRequest,
    Unauthenticated,
    Unavailable,
}

#[derive(Clone)]
pub struct PublicAuthenticationState {
    verifier: Arc<dyn ExternalCredentialVerifier>,
    bindings: Arc<dyn ExternalPrincipalBindingAuthority>,
    clock: Arc<dyn AuthenticationClock>,
}

impl PublicAuthenticationState {
    pub fn new(
        verifier: Arc<dyn ExternalCredentialVerifier>,
        bindings: Arc<dyn ExternalPrincipalBindingAuthority>,
        clock: Arc<dyn AuthenticationClock>,
    ) -> Self {
        Self {
            verifier,
            bindings,
            clock,
        }
    }

    pub async fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedPrincipal, AuthenticationError> {
        if RESERVED_IDENTITY_HEADERS
            .iter()
            .any(|header| headers.contains_key(*header))
        {
            return Err(AuthenticationError::InvalidRequest);
        }
        let mut authorization_values = headers.get_all(AUTHORIZATION).iter();
        let authorization = authorization_values
            .next()
            .ok_or(AuthenticationError::Unauthenticated)?;
        if authorization_values.next().is_some() {
            return Err(AuthenticationError::InvalidRequest);
        }
        let authorization = authorization
            .to_str()
            .map_err(|_| AuthenticationError::Unauthenticated)?;
        let token = authorization
            .strip_prefix("Bearer ")
            .ok_or(AuthenticationError::Unauthenticated)?;
        if token.is_empty()
            || token.len() > MAX_BEARER_TOKEN_BYTES
            || !token.is_ascii()
            || token.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(AuthenticationError::Unauthenticated);
        }
        let now = self.clock.now();
        let verified = self.verifier.verify(token, now)?;
        verified.validate_at(now)?;
        let snapshot = self
            .bindings
            .resolve_external_principal(
                verified.tenant_id.clone(),
                verified.authentication_authority_digest.clone(),
                verified.subject_digest.clone(),
                verified.principal_kind,
            )
            .await?;
        let principal = AuthenticatedPrincipal {
            tenant_id: snapshot.tenant_id,
            principal_id: snapshot.principal_id,
            principal_kind: snapshot.principal_kind,
            permissions: snapshot.permissions,
            authn_strength: verified.authn_strength,
            principal_version: snapshot.principal_version,
            binding_generation: snapshot.binding_generation,
            binding_version: snapshot.binding_version,
            credential_digest: verified.credential_digest,
            credential_expires_at: verified.expires_at,
            trace: crate::trace::current_trace_context().identity,
        };
        principal.validate()?;
        Ok(principal)
    }
}

pub async fn authenticate_public_request(
    State(state): State<PublicAuthenticationState>,
    mut request: Request,
    next: Next,
) -> Response {
    match state.authenticate(request.headers()).await {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(error) => authentication_problem(error),
    }
}

fn authentication_problem(error: AuthenticationError) -> Response {
    let (status, code, title, retryable) = match error {
        AuthenticationError::InvalidRequest => (
            StatusCode::BAD_REQUEST,
            ApiProblemCode::InvalidRequest,
            "The authentication request is ambiguous.",
            false,
        ),
        AuthenticationError::Unauthenticated => (
            StatusCode::UNAUTHORIZED,
            ApiProblemCode::Unauthenticated,
            "Authentication is required.",
            false,
        ),
        AuthenticationError::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            ApiProblemCode::TemporarilyUnavailable,
            "Authentication is temporarily unavailable.",
            true,
        ),
    };
    let request_id = ResourceId::from_uuid_v7(ResourceKind::ServerRequest, uuid::Uuid::now_v7())
        .expect("UUID v7 generator must produce a valid server request identity");
    let problem = ApiProblem {
        type_uri: format!("https://insight.platform/problems/{}", code.as_str()),
        title: title.to_owned(),
        status: status.as_u16(),
        code,
        detail: None,
        request_id,
        trace_id: crate::trace::current_trace_id(),
        retryable,
        retry_after_ms: retryable.then_some(1_000),
        field_errors: Vec::new(),
    };
    debug_assert!(problem
        .validate(MAX_SAFE_TEXT_BYTES, MAX_FIELD_ERRORS)
        .is_ok());
    let mut response = (status, Json(problem)).into_response();
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store, private, max-age=0"),
    );
    if retryable {
        response.headers_mut().insert(
            axum::http::header::RETRY_AFTER,
            axum::http::HeaderValue::from_static("1"),
        );
    }
    response
}

pub fn authenticated_principal(request: &Request<Body>) -> Option<&AuthenticatedPrincipal> {
    request.extensions().get::<AuthenticatedPrincipal>()
}

pub fn principal_extension(principal: AuthenticatedPrincipal) -> Extension<AuthenticatedPrincipal> {
    Extension(principal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::http::HeaderValue;
    use insight_platform_contracts::{Permission, PrincipalSnapshot};
    use std::sync::Mutex;

    fn id(kind: ResourceKind, suffix: u16) -> ResourceId {
        format!(
            "{}_0198f1cc-32e4-75e1-a9e8-d95ca0f8{suffix:04x}",
            kind.descriptor().prefix
        )
        .parse()
        .unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    struct FixedClock(DateTime<Utc>);

    impl AuthenticationClock for FixedClock {
        fn now(&self) -> DateTime<Utc> {
            self.0
        }
    }

    struct FixedVerifier {
        expected_token: &'static str,
        result: Result<VerifiedExternalCredential, AuthenticationError>,
    }

    impl ExternalCredentialVerifier for FixedVerifier {
        fn verify(
            &self,
            bearer_token: &str,
            _now: DateTime<Utc>,
        ) -> Result<VerifiedExternalCredential, AuthenticationError> {
            if bearer_token != self.expected_token {
                return Err(AuthenticationError::Unauthenticated);
            }
            self.result.clone()
        }
    }

    struct FixedBindings {
        requests: Mutex<Vec<(ResourceId, Sha256Digest, Sha256Digest, PrincipalKind)>>,
        result: Result<PrincipalSnapshot, AuthenticationError>,
    }

    #[async_trait]
    impl ExternalPrincipalBindingAuthority for FixedBindings {
        async fn resolve_external_principal(
            &self,
            tenant_id: ResourceId,
            authentication_authority_digest: Sha256Digest,
            subject_digest: Sha256Digest,
            asserted_principal_kind: PrincipalKind,
        ) -> Result<PrincipalSnapshot, AuthenticationError> {
            self.requests.lock().unwrap().push((
                tenant_id,
                authentication_authority_digest,
                subject_digest,
                asserted_principal_kind,
            ));
            self.result.clone()
        }
    }

    fn fixture() -> (
        PublicAuthenticationState,
        Arc<FixedBindings>,
        DateTime<Utc>,
        ResourceId,
        ResourceId,
    ) {
        let now = Utc::now();
        let tenant_id = id(ResourceKind::Tenant, 1);
        let principal_id = id(ResourceKind::Principal, 2);
        let permissions = PermissionSet::new(vec![Permission::OperationRead]).unwrap();
        let snapshot = PrincipalSnapshot::build(
            tenant_id.clone(),
            principal_id.clone(),
            PrincipalKind::TenantAdmin,
            permissions,
            3,
            4,
            5,
        )
        .unwrap();
        let verifier = Arc::new(FixedVerifier {
            expected_token: "signed-token",
            result: Ok(VerifiedExternalCredential {
                tenant_id: tenant_id.clone(),
                authentication_authority_digest: digest('a'),
                subject_digest: digest('b'),
                credential_digest: digest('c'),
                principal_kind: PrincipalKind::TenantAdmin,
                authn_strength: AuthnStrength::MultiFactor,
                issued_at: now - chrono::Duration::minutes(1),
                expires_at: now + chrono::Duration::minutes(10),
            }),
        });
        let bindings = Arc::new(FixedBindings {
            requests: Mutex::new(Vec::new()),
            result: Ok(snapshot),
        });
        let state =
            PublicAuthenticationState::new(verifier, bindings.clone(), Arc::new(FixedClock(now)));
        (state, bindings, now, tenant_id, principal_id)
    }

    #[tokio::test]
    async fn verified_subject_is_rebound_to_current_database_principal() {
        let (state, bindings, _, tenant_id, principal_id) = fixture();
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer signed-token"),
        );

        let principal = state.authenticate(&headers).await.unwrap();

        assert_eq!(principal.tenant_id, tenant_id);
        assert_eq!(principal.principal_id, principal_id);
        assert_eq!(principal.principal_version, 3);
        assert_eq!(principal.binding_generation, 4);
        assert_eq!(principal.binding_version, 5);
        assert!(principal.permissions.contains(Permission::OperationRead));
        let requests = bindings.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].1, digest('a'));
        assert_eq!(requests[0].2, digest('b'));
    }

    #[tokio::test]
    async fn reserved_identity_and_duplicate_authorization_headers_fail_before_verification() {
        let (state, bindings, _, _, _) = fixture();
        let mut forged = HeaderMap::new();
        forged.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer signed-token"),
        );
        forged.insert(
            "x-platform-tenant-id",
            HeaderValue::from_static("ten_0198f1cc-32e4-75e1-a9e8-d95ca0f80099"),
        );
        assert_eq!(
            state.authenticate(&forged).await,
            Err(AuthenticationError::InvalidRequest)
        );

        let mut duplicate = HeaderMap::new();
        duplicate.append(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer signed-token"),
        );
        duplicate.append(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer other-token"),
        );
        assert_eq!(
            state.authenticate(&duplicate).await,
            Err(AuthenticationError::InvalidRequest)
        );
        assert!(bindings.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn malformed_or_expired_credentials_do_not_reach_binding_authority() {
        let (state, bindings, _, _, _) = fixture();
        for value in ["Basic signed-token", "Bearer ", "Bearer signed\ntoken"] {
            let mut headers = HeaderMap::new();
            if let Ok(value) = HeaderValue::from_str(value) {
                headers.insert(AUTHORIZATION, value);
                assert_eq!(
                    state.authenticate(&headers).await,
                    Err(AuthenticationError::Unauthenticated)
                );
            }
        }
        assert!(bindings.requests.lock().unwrap().is_empty());
    }
}
