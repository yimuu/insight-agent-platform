use crate::{
    canonical_digest, AdministrativeGate, AuthnStrength, ExactDeploymentRef, Permission,
    PrincipalBindingState, PrincipalKind, ResourceId, ResourceKind, Sha256Digest, TraceIdentityV1,
};
use chrono::{DateTime, Utc};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::{collections::BTreeSet, error::Error, fmt, str::FromStr};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantConfig {
    pub scheduling_policy: Option<ExactDeploymentRef>,
    pub artifact_retention_policy: Option<ExactDeploymentRef>,
    pub artifact_io_policy: Option<ExactDeploymentRef>,
}

impl TenantConfig {
    pub fn validate(&self) -> Result<(), SecurityContractError> {
        for policy in [
            &self.scheduling_policy,
            &self.artifact_retention_policy,
            &self.artifact_io_policy,
        ]
        .into_iter()
        .flatten()
        {
            policy
                .validate()
                .map_err(|_| SecurityContractError::InvalidTenantConfig)?;
            if policy.resource_kind != ResourceKind::PolicyDeployment {
                return Err(SecurityContractError::InvalidTenantConfig);
            }
        }
        if self.artifact_retention_policy == self.artifact_io_policy
            && self.artifact_retention_policy.is_some()
        {
            return Err(SecurityContractError::InvalidTenantConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct PermissionSet(Vec<Permission>);

impl PermissionSet {
    pub fn new(values: Vec<Permission>) -> Result<Self, SecurityContractError> {
        let mut unique = BTreeSet::new();
        for value in values {
            if !unique.insert(value) {
                return Err(SecurityContractError::DuplicatePermission(value));
            }
        }
        Ok(Self(unique.into_iter().collect()))
    }

    pub fn contains(&self, permission: Permission) -> bool {
        self.0.binary_search(&permission).is_ok()
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = Permission> + '_ {
        self.0.iter().copied()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Immutable authorization evidence embedded in the aggregate that consumed it.
///
/// A principal snapshot deliberately has no standalone identity or lifecycle. The repository
/// derives it from the exact active principal and tenant binding rows in the command transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalSnapshot {
    pub schema_version: u32,
    pub tenant_id: ResourceId,
    pub principal_id: ResourceId,
    pub principal_kind: PrincipalKind,
    pub permissions: PermissionSet,
    pub principal_version: u64,
    pub binding_generation: u64,
    pub binding_version: u64,
    pub permissions_digest: Sha256Digest,
    pub canonical_digest: Sha256Digest,
}

impl PrincipalSnapshot {
    pub fn build(
        tenant_id: ResourceId,
        principal_id: ResourceId,
        principal_kind: PrincipalKind,
        permissions: PermissionSet,
        principal_version: u64,
        binding_generation: u64,
        binding_version: u64,
    ) -> Result<Self, SecurityContractError> {
        validate_principal_snapshot_authority(
            &tenant_id,
            &principal_id,
            principal_kind,
            principal_version,
            binding_generation,
            binding_version,
        )?;
        let permissions_digest = principal_permissions_digest(&permissions)?;
        let unsigned = UnsignedPrincipalSnapshot {
            schema_version: 1,
            tenant_id: &tenant_id,
            principal_id: &principal_id,
            principal_kind,
            permissions: &permissions,
            principal_version,
            binding_generation,
            binding_version,
            permissions_digest: &permissions_digest,
        };
        let canonical_digest = snapshot_digest(&unsigned)?;
        Ok(Self {
            schema_version: 1,
            tenant_id,
            principal_id,
            principal_kind,
            permissions,
            principal_version,
            binding_generation,
            binding_version,
            permissions_digest,
            canonical_digest,
        })
    }

    pub fn validate(&self) -> Result<(), SecurityContractError> {
        if self.schema_version != 1 {
            return Err(SecurityContractError::InvalidPrincipalSnapshot);
        }
        validate_principal_snapshot_authority(
            &self.tenant_id,
            &self.principal_id,
            self.principal_kind,
            self.principal_version,
            self.binding_generation,
            self.binding_version,
        )?;
        if principal_permissions_digest(&self.permissions)? != self.permissions_digest {
            return Err(SecurityContractError::InvalidPrincipalSnapshot);
        }
        let unsigned = UnsignedPrincipalSnapshot {
            schema_version: self.schema_version,
            tenant_id: &self.tenant_id,
            principal_id: &self.principal_id,
            principal_kind: self.principal_kind,
            permissions: &self.permissions,
            principal_version: self.principal_version,
            binding_generation: self.binding_generation,
            binding_version: self.binding_version,
            permissions_digest: &self.permissions_digest,
        };
        if snapshot_digest(&unsigned)? != self.canonical_digest {
            return Err(SecurityContractError::InvalidPrincipalSnapshot);
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct PrincipalPermissionsDocument<'a> {
    schema_version: u32,
    permissions: &'a PermissionSet,
}

#[derive(Serialize)]
struct UnsignedPrincipalSnapshot<'a> {
    schema_version: u32,
    tenant_id: &'a ResourceId,
    principal_id: &'a ResourceId,
    principal_kind: PrincipalKind,
    permissions: &'a PermissionSet,
    principal_version: u64,
    binding_generation: u64,
    binding_version: u64,
    permissions_digest: &'a Sha256Digest,
}

fn validate_principal_snapshot_authority(
    tenant_id: &ResourceId,
    principal_id: &ResourceId,
    principal_kind: PrincipalKind,
    principal_version: u64,
    binding_generation: u64,
    binding_version: u64,
) -> Result<(), SecurityContractError> {
    if tenant_id.kind() != ResourceKind::Tenant
        || principal_id.kind() != ResourceKind::Principal
        || principal_kind == PrincipalKind::InstallationOperator
        || principal_version == 0
        || binding_generation == 0
        || binding_version == 0
    {
        return Err(SecurityContractError::InvalidPrincipalSnapshot);
    }
    Ok(())
}

fn principal_permissions_digest(
    permissions: &PermissionSet,
) -> Result<Sha256Digest, SecurityContractError> {
    let value = serde_json::to_value(PrincipalPermissionsDocument {
        schema_version: 1,
        permissions,
    })
    .map_err(|_| SecurityContractError::InvalidPrincipalSnapshot)?;
    canonical_digest(&value)
        .map_err(|_| SecurityContractError::InvalidPrincipalSnapshot)?
        .parse()
        .map_err(|_| SecurityContractError::InvalidPrincipalSnapshot)
}

fn snapshot_digest<T: Serialize>(value: &T) -> Result<Sha256Digest, SecurityContractError> {
    let value =
        serde_json::to_value(value).map_err(|_| SecurityContractError::InvalidPrincipalSnapshot)?;
    canonical_digest(&value)
        .map_err(|_| SecurityContractError::InvalidPrincipalSnapshot)?
        .parse()
        .map_err(|_| SecurityContractError::InvalidPrincipalSnapshot)
}

impl<'de> Deserialize<'de> for PermissionSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(Vec::<Permission>::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PrincipalScope {
    Installation,
    Tenant { tenant_id: ResourceId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrincipalContext {
    pub principal_id: ResourceId,
    pub scope: PrincipalScope,
    pub principal_kind: PrincipalKind,
    pub permissions: PermissionSet,
    pub authn_strength: AuthnStrength,
    pub binding_generation: u64,
    pub expires_at: DateTime<Utc>,
    pub trace: TraceIdentityV1,
}

impl PrincipalContext {
    pub fn validate(&self) -> Result<(), SecurityContractError> {
        if self.principal_id.kind() != ResourceKind::Principal
            || self.binding_generation == 0
            || self.trace.validate().is_err()
        {
            return Err(SecurityContractError::InvalidPrincipalContext);
        }
        match (&self.scope, self.principal_kind) {
            (
                PrincipalScope::Installation,
                PrincipalKind::InstallationOperator | PrincipalKind::ServiceIdentity,
            ) => Ok(()),
            (PrincipalScope::Tenant { tenant_id }, PrincipalKind::InstallationOperator)
                if tenant_id.kind() == ResourceKind::Tenant =>
            {
                Err(SecurityContractError::InvalidPrincipalScope)
            }
            (PrincipalScope::Tenant { tenant_id }, _)
                if tenant_id.kind() == ResourceKind::Tenant =>
            {
                Ok(())
            }
            _ => Err(SecurityContractError::InvalidPrincipalScope),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationPrincipalBinding {
    pub principal_kind: PrincipalKind,
    pub permissions: PermissionSet,
    pub state: PrincipalBindingState,
    pub generation: u64,
}

impl InstallationPrincipalBinding {
    pub fn validate(&self) -> Result<(), SecurityContractError> {
        if self.generation == 0
            || !matches!(
                self.principal_kind,
                PrincipalKind::InstallationOperator | PrincipalKind::ServiceIdentity
            )
        {
            return Err(SecurityContractError::InvalidInstallationBinding);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalBindingsPayload {
    pub installation_bindings: Vec<InstallationPrincipalBinding>,
}

impl PrincipalBindingsPayload {
    pub fn validate(&self) -> Result<(), SecurityContractError> {
        let mut kinds = BTreeSet::new();
        for binding in &self.installation_bindings {
            binding.validate()?;
            if !kinds.insert(binding.principal_kind) {
                return Err(SecurityContractError::DuplicateInstallationBinding(
                    binding.principal_kind,
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantPrincipalPayload {
    pub permissions: PermissionSet,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct SecretPurpose(String);

impl SecretPurpose {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl FromStr for SecretPurpose {
    type Err = SecurityContractError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut bytes = value.bytes();
        let Some(first) = bytes.next() else {
            return Err(SecurityContractError::InvalidSecretPurpose);
        };
        if value.len() > 128
            || !first.is_ascii_lowercase()
            || !bytes.all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'_' | b'.' | b':')
            })
        {
            return Err(SecurityContractError::InvalidSecretPurpose);
        }
        Ok(Self(value.to_owned()))
    }
}

impl<'de> Deserialize<'de> for SecretPurpose {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SecretResolutionPolicy {
    Pinned {
        opaque_version_identity_digest: Sha256Digest,
    },
    FollowProviderRotation {
        rotation_policy_revision_id: ResourceId,
    },
}

impl SecretResolutionPolicy {
    pub fn validate(&self) -> Result<(), SecurityContractError> {
        if let Self::FollowProviderRotation {
            rotation_policy_revision_id,
        } = self
        {
            if rotation_policy_revision_id.kind() != ResourceKind::PolicyRevision {
                return Err(SecurityContractError::InvalidRotationPolicy);
            }
        }
        Ok(())
    }
}

/// Immutable, non-secret credential authority frozen into a Deployment closure.
///
/// The public management request identifies a `SecretBinding` only by ID. The repository derives
/// this value from the active binding in the Deployment creation transaction. Keeping the complete
/// policy alongside its digest makes the execution closure self-contained without exposing an
/// opaque reference or secret value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactSecretBindingRef {
    pub secret_binding_id: ResourceId,
    pub binding_generation: u64,
    pub provider_id: ResourceId,
    pub purpose: SecretPurpose,
    pub resolution_policy: SecretResolutionPolicy,
    pub resolution_policy_digest: Sha256Digest,
}

impl ExactSecretBindingRef {
    pub fn build(
        secret_binding_id: ResourceId,
        binding_generation: u64,
        provider_id: ResourceId,
        purpose: SecretPurpose,
        resolution_policy: SecretResolutionPolicy,
    ) -> Result<Self, SecurityContractError> {
        let resolution_policy_digest = resolution_policy_digest(&resolution_policy)?;
        let reference = Self {
            secret_binding_id,
            binding_generation,
            provider_id,
            purpose,
            resolution_policy,
            resolution_policy_digest,
        };
        reference.validate()?;
        Ok(reference)
    }

    pub fn validate(&self) -> Result<(), SecurityContractError> {
        if self.secret_binding_id.kind() != ResourceKind::SecretBinding
            || self.binding_generation == 0
            || self.provider_id.kind() != ResourceKind::SecretProvider
        {
            return Err(SecurityContractError::InvalidExactSecretBinding);
        }
        self.resolution_policy.validate()?;
        if resolution_policy_digest(&self.resolution_policy)? != self.resolution_policy_digest {
            return Err(SecurityContractError::InvalidExactSecretBinding);
        }
        Ok(())
    }

    /// Checks a non-secret resolver result against the Deployment's frozen rotation semantics.
    pub fn permits_resolved_generation(
        &self,
        secret_binding_id: &ResourceId,
        purpose: &SecretPurpose,
        binding_generation: u64,
    ) -> bool {
        if self.validate().is_err()
            || &self.secret_binding_id != secret_binding_id
            || &self.purpose != purpose
        {
            return false;
        }
        match &self.resolution_policy {
            SecretResolutionPolicy::Pinned { .. } => binding_generation == self.binding_generation,
            SecretResolutionPolicy::FollowProviderRotation { .. } => {
                binding_generation >= self.binding_generation
            }
        }
    }
}

pub fn resolution_policy_digest(
    policy: &SecretResolutionPolicy,
) -> Result<Sha256Digest, SecurityContractError> {
    policy.validate()?;
    let value = serde_json::to_value(policy)
        .map_err(|_| SecurityContractError::InvalidExactSecretBinding)?;
    canonical_digest(&value)
        .map_err(|_| SecurityContractError::InvalidExactSecretBinding)?
        .parse()
        .map_err(|_| SecurityContractError::InvalidExactSecretBinding)
}

pub fn exact_secret_binding_purposes_match(
    bindings: &[ExactSecretBindingRef],
    required_purposes: &[SecretPurpose],
) -> bool {
    bindings.len() == required_purposes.len()
        && bindings
            .iter()
            .map(|binding| &binding.purpose)
            .eq(required_purposes.iter())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretBindingPayload {
    pub provider_id: ResourceId,
    pub resolution_policy: SecretResolutionPolicy,
}

impl SecretBindingPayload {
    pub fn validate(&self) -> Result<(), SecurityContractError> {
        if self.provider_id.kind() != ResourceKind::SecretProvider {
            return Err(SecurityContractError::InvalidSecretProvider);
        }
        self.resolution_policy.validate()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct AuthorizationRequest<'a> {
    pub now: DateTime<Utc>,
    pub tenant_id: Option<&'a ResourceId>,
    pub resource_tenant_id: Option<&'a ResourceId>,
    pub required_permission: Permission,
    pub resource_gate: AdministrativeGate,
    pub policy_allows: bool,
}

pub fn authorize(
    principal: &PrincipalContext,
    request: AuthorizationRequest<'_>,
) -> Result<(), AuthorizationError> {
    principal
        .validate()
        .map_err(|_| AuthorizationError::Unauthenticated)?;
    if principal.expires_at <= request.now {
        return Err(AuthorizationError::Unauthenticated);
    }
    if let (Some(request_tenant), Some(resource_tenant)) =
        (request.tenant_id, request.resource_tenant_id)
    {
        if request_tenant != resource_tenant {
            return Err(AuthorizationError::ResourceNotFound);
        }
    }
    match (&principal.scope, request.tenant_id) {
        (PrincipalScope::Installation, None) => {}
        (PrincipalScope::Tenant { tenant_id }, Some(request_tenant))
            if tenant_id == request_tenant => {}
        _ => return Err(AuthorizationError::PermissionDenied),
    }
    if !principal.permissions.contains(request.required_permission) {
        return Err(AuthorizationError::PermissionDenied);
    }
    if request.resource_gate == AdministrativeGate::Suspended {
        return Err(AuthorizationError::ResourceSuspended);
    }
    if !request.policy_allows {
        return Err(AuthorizationError::PolicyDenied);
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationError {
    Unauthenticated,
    ResourceNotFound,
    PermissionDenied,
    ResourceSuspended,
    PolicyDenied,
}

impl AuthorizationError {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unauthenticated => "unauthenticated",
            Self::ResourceNotFound => "resource_not_found",
            Self::PermissionDenied => "permission_denied",
            Self::ResourceSuspended => "resource_suspended",
            Self::PolicyDenied => "policy_denied",
        }
    }
}

impl fmt::Display for AuthorizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Error for AuthorizationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SecurityContractError {
    InvalidTenantConfig,
    InvalidPrincipalSnapshot,
    DuplicatePermission(Permission),
    InvalidPrincipalContext,
    InvalidPrincipalScope,
    InvalidInstallationBinding,
    DuplicateInstallationBinding(PrincipalKind),
    InvalidSecretPurpose,
    InvalidSecretProvider,
    InvalidRotationPolicy,
    InvalidExactSecretBinding,
}

impl fmt::Display for SecurityContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTenantConfig => formatter.write_str("tenant config is invalid"),
            Self::InvalidPrincipalSnapshot => {
                formatter.write_str("principal snapshot is invalid or non-canonical")
            }
            Self::DuplicatePermission(permission) => {
                write!(formatter, "permission {permission} is duplicated")
            }
            Self::InvalidPrincipalContext => formatter.write_str("principal context is invalid"),
            Self::InvalidPrincipalScope => formatter.write_str("principal scope and kind disagree"),
            Self::InvalidInstallationBinding => {
                formatter.write_str("installation principal binding is invalid")
            }
            Self::DuplicateInstallationBinding(kind) => {
                write!(formatter, "installation binding kind {kind} is duplicated")
            }
            Self::InvalidSecretPurpose => formatter.write_str("secret purpose is invalid"),
            Self::InvalidSecretProvider => {
                formatter.write_str("secret provider ID has the wrong kind")
            }
            Self::InvalidRotationPolicy => {
                formatter.write_str("rotation policy revision ID has the wrong kind")
            }
            Self::InvalidExactSecretBinding => {
                formatter.write_str("exact secret binding reference is invalid or non-canonical")
            }
        }
    }
}

impl Error for SecurityContractError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const PRINCIPAL_ID: &str = "prn_0198f1c3-8f49-7c3e-b1f3-773c28367b7e";
    const TENANT_A: &str = "ten_0198f1c3-8f49-7c3e-b1f3-773c28367b7f";
    const TENANT_B: &str = "ten_0198f1c3-8f49-7c3e-b1f3-773c28367b80";

    fn tenant_context() -> PrincipalContext {
        PrincipalContext {
            principal_id: PRINCIPAL_ID.parse().unwrap(),
            scope: PrincipalScope::Tenant {
                tenant_id: TENANT_A.parse().unwrap(),
            },
            principal_kind: PrincipalKind::AgentRunner,
            permissions: PermissionSet::new(vec![Permission::AgentRun]).unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            binding_generation: 1,
            expires_at: Utc::now() + Duration::from_secs(60),
            trace: TraceIdentityV1::generate(),
        }
    }

    fn exact_policy(suffix: &str, marker: char) -> ExactDeploymentRef {
        ExactDeploymentRef::new(
            format!("pdep_0198f1c3-8f49-7c3e-b1f3-773c2836{suffix}")
                .parse()
                .unwrap(),
            format!("sha256:{}", marker.to_string().repeat(64))
                .parse()
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn tenant_config_owns_distinct_exact_artifact_policy_bindings() {
        let retention = exact_policy("7b81", 'a');
        let artifact_io = exact_policy("7b82", 'b');
        let config = TenantConfig {
            scheduling_policy: None,
            artifact_retention_policy: Some(retention.clone()),
            artifact_io_policy: Some(artifact_io.clone()),
        };
        config.validate().unwrap();

        let wire = serde_json::to_value(&config).unwrap();
        assert_eq!(
            wire["artifact_retention_policy"]["deployment_id"],
            retention.deployment_id.to_string()
        );
        assert_eq!(
            wire["artifact_io_policy"]["deployment_digest"],
            artifact_io.deployment_digest.to_string()
        );
        assert!(serde_json::from_value::<TenantConfig>(serde_json::json!({
            "scheduling_policy": null,
            "artifact_retention_policy": retention,
            "artifact_io_policy": artifact_io,
            "fallback_to_any_active_policy": true
        }))
        .is_err());
    }

    #[test]
    fn tenant_config_rejects_one_deployment_in_both_artifact_policy_slots() {
        let policy = exact_policy("7b83", 'c');
        assert_eq!(
            TenantConfig {
                scheduling_policy: None,
                artifact_retention_policy: Some(policy.clone()),
                artifact_io_policy: Some(policy),
            }
            .validate(),
            Err(SecurityContractError::InvalidTenantConfig)
        );
    }

    #[test]
    fn permissions_are_canonical_and_duplicates_fail_closed() {
        let permissions =
            PermissionSet::new(vec![Permission::AgentRun, Permission::AgentRead]).unwrap();
        assert_eq!(
            permissions.iter().collect::<Vec<_>>(),
            vec![Permission::AgentRead, Permission::AgentRun]
        );
        assert!(PermissionSet::new(vec![Permission::AgentRun, Permission::AgentRun]).is_err());
        assert!(serde_json::from_str::<PermissionSet>("[\"agent.run\",\"agent.run\"]").is_err());
    }

    #[test]
    fn principal_snapshot_is_embedded_canonical_authority_not_an_identity() {
        let snapshot = PrincipalSnapshot::build(
            TENANT_A.parse().unwrap(),
            PRINCIPAL_ID.parse().unwrap(),
            PrincipalKind::AgentRunner,
            PermissionSet::new(vec![Permission::RuntimeControl, Permission::AgentRun]).unwrap(),
            3,
            5,
            7,
        )
        .unwrap();
        snapshot.validate().unwrap();
        assert_eq!(snapshot.schema_version, 1);
        let wire = serde_json::to_value(&snapshot).unwrap();
        assert!(wire.get("principal_snapshot_id").is_none());

        let mut forged_permissions = snapshot.clone();
        forged_permissions.permissions =
            PermissionSet::new(vec![Permission::RuntimeControl]).unwrap();
        assert_eq!(
            forged_permissions.validate(),
            Err(SecurityContractError::InvalidPrincipalSnapshot)
        );

        let mut forged_digest = snapshot;
        forged_digest.canonical_digest = format!("sha256:{}", "0".repeat(64)).parse().unwrap();
        assert_eq!(
            forged_digest.validate(),
            Err(SecurityContractError::InvalidPrincipalSnapshot)
        );
    }

    #[test]
    fn tenant_scope_is_not_client_overridable() {
        let principal = tenant_context();
        let tenant_a: ResourceId = TENANT_A.parse().unwrap();
        let tenant_b: ResourceId = TENANT_B.parse().unwrap();
        assert_eq!(
            authorize(
                &principal,
                AuthorizationRequest {
                    now: Utc::now(),
                    tenant_id: Some(&tenant_b),
                    resource_tenant_id: Some(&tenant_a),
                    required_permission: Permission::AgentRun,
                    resource_gate: AdministrativeGate::Enabled,
                    policy_allows: true,
                }
            ),
            Err(AuthorizationError::ResourceNotFound)
        );
    }

    #[test]
    fn operator_cannot_use_a_tenant_command() {
        let tenant: ResourceId = TENANT_A.parse().unwrap();
        let operator = PrincipalContext {
            principal_id: PRINCIPAL_ID.parse().unwrap(),
            scope: PrincipalScope::Installation,
            principal_kind: PrincipalKind::InstallationOperator,
            permissions: PermissionSet::new(vec![Permission::InstallationManage]).unwrap(),
            authn_strength: AuthnStrength::PhishingResistant,
            binding_generation: 1,
            expires_at: Utc::now() + Duration::from_secs(60),
            trace: TraceIdentityV1::generate(),
        };
        assert_eq!(
            authorize(
                &operator,
                AuthorizationRequest {
                    now: Utc::now(),
                    tenant_id: Some(&tenant),
                    resource_tenant_id: Some(&tenant),
                    required_permission: Permission::AgentRun,
                    resource_gate: AdministrativeGate::Enabled,
                    policy_allows: true,
                }
            ),
            Err(AuthorizationError::PermissionDenied)
        );
    }

    #[test]
    fn resource_gate_and_policy_are_independent_denials() {
        let principal = tenant_context();
        let tenant: ResourceId = TENANT_A.parse().unwrap();
        let mut request = AuthorizationRequest {
            now: Utc::now(),
            tenant_id: Some(&tenant),
            resource_tenant_id: Some(&tenant),
            required_permission: Permission::AgentRun,
            resource_gate: AdministrativeGate::Suspended,
            policy_allows: true,
        };
        assert_eq!(
            authorize(&principal, request),
            Err(AuthorizationError::ResourceSuspended)
        );
        request.resource_gate = AdministrativeGate::Enabled;
        request.policy_allows = false;
        assert_eq!(
            authorize(&principal, request),
            Err(AuthorizationError::PolicyDenied)
        );
    }

    #[test]
    fn secret_payload_has_no_place_for_a_secret_value() {
        let payload = SecretBindingPayload {
            provider_id: "spr_0198f1c3-8f49-7c3e-b1f3-773c28367b81".parse().unwrap(),
            resolution_policy: SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: format!("sha256:{}", "a".repeat(64))
                    .parse()
                    .unwrap(),
            },
        };
        payload.validate().unwrap();
        let wire = serde_json::to_string(&payload).unwrap();
        assert!(!wire.contains("secret_value"));
        assert!(serde_json::from_str::<SecretBindingPayload>(
            r#"{"provider_id":"spr_0198f1c3-8f49-7c3e-b1f3-773c28367b81","resolution_policy":{"kind":"pinned","opaque_version_identity_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"secret_value":"canary"}"#
        )
        .is_err());
    }

    #[test]
    fn exact_secret_binding_is_canonical_and_freezes_rotation_semantics() {
        let binding_id: ResourceId = "sbd_0198f1c3-8f49-7c3e-b1f3-773c28367b82".parse().unwrap();
        let provider_id: ResourceId = "spr_0198f1c3-8f49-7c3e-b1f3-773c28367b83".parse().unwrap();
        let purpose: SecretPurpose = "provider.api_key".parse().unwrap();
        let pinned = ExactSecretBindingRef::build(
            binding_id.clone(),
            7,
            provider_id.clone(),
            purpose.clone(),
            SecretResolutionPolicy::Pinned {
                opaque_version_identity_digest: format!("sha256:{}", "b".repeat(64))
                    .parse()
                    .unwrap(),
            },
        )
        .unwrap();
        assert!(pinned.permits_resolved_generation(&binding_id, &purpose, 7));
        assert!(!pinned.permits_resolved_generation(&binding_id, &purpose, 8));
        let mut forged = pinned.clone();
        forged.resolution_policy_digest = format!("sha256:{}", "0".repeat(64)).parse().unwrap();
        assert_eq!(
            forged.validate(),
            Err(SecurityContractError::InvalidExactSecretBinding)
        );

        let following = ExactSecretBindingRef::build(
            binding_id.clone(),
            7,
            provider_id,
            purpose.clone(),
            SecretResolutionPolicy::FollowProviderRotation {
                rotation_policy_revision_id: "prev_0198f1c3-8f49-7c3e-b1f3-773c28367b84"
                    .parse()
                    .unwrap(),
            },
        )
        .unwrap();
        assert!(following.permits_resolved_generation(&binding_id, &purpose, 8));
        assert!(!following.permits_resolved_generation(&binding_id, &purpose, 6));
        assert!(!serde_json::to_string(&following)
            .unwrap()
            .contains("secret_value"));
    }
}
