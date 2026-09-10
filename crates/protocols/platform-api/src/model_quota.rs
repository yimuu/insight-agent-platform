//! Allocation commands project the existing Model quota accounts.
use super::*;
use axum::http::header;
use insight_platform_contracts::validate_model_quota_etag;
pub use insight_platform_contracts::{ModelQuotaViewV1, SetModelQuotaRequestV1};
#[derive(Debug, Clone)]
pub struct ReadModelQuotaIntent {
    pub principal: AuthenticatedPrincipal,
    pub model_deployment_id: ResourceId,
    pub deadline: DateTime<Utc>,
}
#[derive(Debug, Clone)]
pub struct SetModelQuotaIntent {
    pub principal: AuthenticatedPrincipal,
    pub request: SetModelQuotaRequestV1,
    pub expected_etag: String,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}
pub(super) async fn read_model_quota(
    State(state): State<ResourceHttpState>,
    Path(raw): Path<String>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let Ok(model_deployment_id) = ResourceId::parse_expected(&raw, ResourceKind::ModelDeployment)
    else {
        return problem(ResourceApplicationError::Invalid);
    };
    let tenant = principal.tenant_id.clone();
    let id = model_deployment_id.clone();
    let result = state
        .application
        .read_model_quota(ReadModelQuotaIntent {
            principal,
            model_deployment_id,
            deadline: state.clock.now()
                + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
        })
        .await;
    response(result, &tenant, &id)
}
pub(super) async fn set_model_quota(
    State(state): State<ResourceHttpState>,
    Path(raw): Path<String>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let Ok(id) = ResourceId::parse_expected(&raw, ResourceKind::ModelDeployment) else {
        return problem(ResourceApplicationError::Invalid);
    };
    let request = match parse_strict_json(
        &body,
        JsonLimits {
            max_bytes: 2048,
            max_depth: 3,
            max_properties_per_object: 3,
            max_items_per_array: 1,
            max_string_bytes: 128,
        },
    )
    .ok()
    .and_then(|value| serde_json::from_value::<SetModelQuotaRequestV1>(value).ok())
    {
        Some(request)
            if request.validate().is_ok() && request.model_deployment.deployment_id == id =>
        {
            request
        }
        _ => return problem(ResourceApplicationError::Invalid),
    };
    if !headers.contains_key(header::IF_MATCH) {
        return problem(ResourceApplicationError::PreconditionRequired);
    }
    let expected_etag = match headers.get(header::IF_MATCH).and_then(|v| v.to_str().ok()) {
        Some(value)
            if headers.get_all(header::IF_MATCH).iter().count() == 1
                && validate_model_quota_etag(value).is_ok() =>
        {
            value.to_owned()
        }
        _ => return problem(ResourceApplicationError::Invalid),
    };
    let idempotency_key_digest = match idempotency_key_digest_for_operation(
        &headers,
        &principal,
        RegistryResourceKind::ModelProfile,
        "model.quota.set",
        Some(&id),
    ) {
        Ok(value) => value,
        Err(error) => return problem(error),
    };
    let request_digest = match insight_platform_registry::model_quota::model_quota_request_digest(
        &principal.tenant_id,
        &principal.principal_id,
        &request,
        &expected_etag,
        &idempotency_key_digest,
    ) {
        Ok(value) => value,
        Err(_) => return problem(ResourceApplicationError::Invalid),
    };
    let tenant = principal.tenant_id.clone();
    let result = state
        .application
        .set_model_quota(SetModelQuotaIntent {
            principal,
            request,
            expected_etag,
            idempotency_key_digest,
            request_digest,
            deadline: state.clock.now()
                + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
        })
        .await;
    response(result, &tenant, &id)
}
fn response(
    result: Result<ModelQuotaViewV1, ResourceApplicationError>,
    tenant: &ResourceId,
    id: &ResourceId,
) -> Response {
    match result {
        Ok(view)
            if view.validate().is_ok()
                && view.tenant_id == *tenant
                && view.model_deployment.deployment_id == *id =>
        {
            let Ok(etag) = HeaderValue::from_str(&view.etag) else {
                return problem(ResourceApplicationError::Internal);
            };
            let mut response = (StatusCode::OK, Json(view)).into_response();
            response.headers_mut().insert(header::ETAG, etag);
            no_store(response)
        }
        Ok(_) => problem(ResourceApplicationError::Internal),
        Err(error) => problem(error),
    }
}
