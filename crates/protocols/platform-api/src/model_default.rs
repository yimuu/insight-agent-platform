//! Tenant-owned default Model pointer; Registry remains the deployment authority.
use super::*;
use insight_platform_contracts::ExactDeploymentRef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelDefaultViewV1 {
    pub schema_version: u16,
    pub tenant_id: ResourceId,
    pub default_model: Option<ExactDeploymentRef>,
    pub version: u64,
    pub etag: String,
}

impl ModelDefaultViewV1 {
    pub fn validate(&self) -> Result<(), ResourceApplicationError> {
        if self.schema_version != 1
            || self.tenant_id.kind() != ResourceKind::Tenant
            || self.version == 0
            || self.version > i64::MAX as u64
            || self.etag != resource_etag(&self.tenant_id, self.version)
            || !valid_model(&self.default_model)
        {
            return Err(ResourceApplicationError::Internal);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetModelDefaultRequestV1 {
    pub schema_version: u16,
    // A required nullable field distinguishes an explicit clear from a missing intent.
    #[serde(deserialize_with = "required_model")]
    pub default_model: Option<ExactDeploymentRef>,
}

fn required_model<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<ExactDeploymentRef>, D::Error> {
    Option::<ExactDeploymentRef>::deserialize(deserializer)
}

fn valid_model(model: &Option<ExactDeploymentRef>) -> bool {
    model.as_ref().is_none_or(|model| {
        model.resource_kind == ResourceKind::ModelDeployment && model.validate().is_ok()
    })
}

#[derive(Debug, Clone)]
pub struct ReadModelDefaultIntent {
    pub principal: AuthenticatedPrincipal,
    pub deadline: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SetModelDefaultIntent {
    pub principal: AuthenticatedPrincipal,
    pub expected_tenant_version: u64,
    pub default_model: Option<ExactDeploymentRef>,
    pub idempotency_key_digest: Sha256Digest,
    pub request_digest: Sha256Digest,
    pub deadline: DateTime<Utc>,
}

pub(super) async fn read_model_default(
    State(state): State<ResourceHttpState>,
    principal: Option<Extension<AuthenticatedPrincipal>>,
) -> Response {
    let Some(Extension(principal)) = principal else {
        return problem(ResourceApplicationError::Unauthenticated);
    };
    if principal.validate().is_err() {
        return problem(ResourceApplicationError::Unauthenticated);
    }
    let tenant = principal.tenant_id.clone();
    let result = state
        .application
        .read_model_default(ReadModelDefaultIntent {
            principal,
            deadline: state.clock.now()
                + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
        })
        .await;
    model_default_response(result, &tenant)
}

pub(super) async fn set_model_default(
    State(state): State<ResourceHttpState>,
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
    let value = match parse_strict_json(
        &body,
        JsonLimits {
            max_bytes: 2048,
            max_depth: 3,
            max_properties_per_object: 3,
            max_items_per_array: 1,
            max_string_bytes: 128,
        },
    ) {
        Ok(value) => value,
        Err(_) => return problem(ResourceApplicationError::Invalid),
    };
    let request: SetModelDefaultRequestV1 = match serde_json::from_value(value) {
        Ok(request) => request,
        Err(_) => return problem(ResourceApplicationError::Invalid),
    };
    if request.schema_version != 1 || !valid_model(&request.default_model) {
        return problem(ResourceApplicationError::Invalid);
    }
    let expected_tenant_version = match expected_resource_version(&headers, &principal.tenant_id) {
        Ok(version) if version <= i64::MAX as u64 => version,
        Ok(_) => return problem(ResourceApplicationError::Invalid),
        Err(error) => return problem(error),
    };
    let idempotency_key_digest = match idempotency_key_digest_for_operation(
        &headers,
        &principal,
        RegistryResourceKind::ModelProfile,
        "model.default.set",
        Some(&principal.tenant_id),
    ) {
        Ok(value) => value,
        Err(error) => return problem(error),
    };
    let request_digest = match digest(&serde_json::json!({
        "schema_version": 1, "operation": "model.default.set",
        "tenant_id": principal.tenant_id, "principal_id": principal.principal_id,
        "expected_tenant_version": expected_tenant_version,
        "default_model": request.default_model,
        "idempotency_key_digest": idempotency_key_digest,
    })) {
        Ok(value) => value,
        Err(error) => return problem(error),
    };
    let tenant = principal.tenant_id.clone();
    let result = state
        .application
        .set_model_default(SetModelDefaultIntent {
            principal,
            expected_tenant_version,
            default_model: request.default_model,
            idempotency_key_digest,
            request_digest,
            deadline: state.clock.now()
                + Duration::milliseconds(RESOURCE_COMMAND_DEADLINE_MILLISECONDS),
        })
        .await;
    model_default_response(result, &tenant)
}

fn model_default_response(
    result: Result<ModelDefaultViewV1, ResourceApplicationError>,
    tenant: &ResourceId,
) -> Response {
    match result {
        Ok(view) if view.validate().is_ok() && &view.tenant_id == tenant => {
            let Ok(etag) = HeaderValue::from_str(&view.etag) else {
                return problem(ResourceApplicationError::Internal);
            };
            let mut response = (StatusCode::OK, Json(view)).into_response();
            response.headers_mut().insert("etag", etag);
            no_store(response)
        }
        Ok(_) => problem(ResourceApplicationError::Internal),
        Err(error) => problem(error),
    }
}
