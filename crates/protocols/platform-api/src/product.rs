use crate::authentication::AuthenticatedPrincipal;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, AgentProductState, AgentRequiredFeature, ExactDeploymentRef,
    ExactPolicyBinding, ExactVersionRef, OpaqueListCursor, ResourceId, ResourceKind, RunState,
    Sha256Digest, UtcTimestamp,
};
use ring::hmac;
use serde::{Deserialize, Serialize};

pub const PRODUCT_LIST_MAX_PAGE_SIZE: u16 = 50;
pub const PRODUCT_LIST_DEFAULT_PAGE_SIZE: u16 = 25;
pub const PRODUCT_LIST_CURSOR_TTL_SECONDS: i64 = 900;
pub const DEFAULT_AGENT_MODEL_REFERENCE: &str = "project/default";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentModelLoopLimitsV1 {
    pub maximum_rounds: u16,
    pub maximum_capability_calls: u32,
    pub maximum_parallel_calls_per_round: u16,
    pub token_budget: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentAuthoringModelBindingV1 {
    /// A directly usable project reference for the shared Agent compiler, not a Registry alias.
    pub alias: String,
    pub deployment: ExactDeploymentRef,
    pub selection_policy: ExactPolicyBinding,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentAuthoringProfileV1 {
    pub schema_version: u32,
    pub default_deadline_seconds: u32,
    pub default_environment: String,
    pub policy_versions: Vec<ExactVersionRef>,
    pub deployment_policies: Vec<ExactPolicyBinding>,
    pub execution_profile: ExactPolicyBinding,
    pub model_loop: AgentModelLoopLimitsV1,
    pub models: Vec<AgentAuthoringModelBindingV1>,
    pub profile_digest: Sha256Digest,
}

impl AgentAuthoringProfileV1 {
    pub fn build(
        execution_profile: ExactPolicyBinding,
        models: Vec<AgentAuthoringModelBindingV1>,
    ) -> Result<Self, ListError> {
        Self::build_for_installation("development".to_owned(), execution_profile, models)
    }

    pub fn build_for_installation(
        environment: String,
        execution_profile: ExactPolicyBinding,
        models: Vec<AgentAuthoringModelBindingV1>,
    ) -> Result<Self, ListError> {
        let mut profile = Self {
            schema_version: 1,
            default_deadline_seconds: 120,
            default_environment: environment,
            policy_versions: vec![execution_profile.revision.clone()],
            deployment_policies: vec![execution_profile.clone()],
            execution_profile,
            model_loop: AgentModelLoopLimitsV1 {
                maximum_rounds: 1,
                maximum_capability_calls: 0,
                maximum_parallel_calls_per_round: 0,
                token_budget: 10_240,
            },
            models,
            profile_digest:
                "sha256:0000000000000000000000000000000000000000000000000000000000000000"
                    .parse()
                    .map_err(|_| ListError::Invalid)?,
        };
        profile.profile_digest = profile.expected_digest()?;
        profile.validate()?;
        Ok(profile)
    }

    pub fn validate(&self) -> Result<(), ListError> {
        if self.schema_version != 1
            || self.default_deadline_seconds == 0
            || self.default_deadline_seconds > 3_600
            || !valid_environment(&self.default_environment)
            || self.policy_versions.is_empty()
            || self.policy_versions.len() > 16
            || self.deployment_policies.is_empty()
            || self.deployment_policies.len() > 16
            || self.models.len() > 16
            || self.model_loop.maximum_rounds == 0
            || insight_platform_plan::validate_model_loop_tool_budget(
                self.model_loop.maximum_capability_calls,
                self.model_loop.maximum_parallel_calls_per_round,
            )
            .is_err()
            || self.model_loop.token_budget == 0
            || self.execution_profile.validate().is_err()
            || self.policy_versions.iter().any(|revision| {
                revision.resource_kind != ResourceKind::PolicyRevision
                    || revision.validate().is_err()
            })
            || self
                .deployment_policies
                .iter()
                .any(|binding| binding.validate().is_err())
            || self
                .models
                .windows(2)
                .any(|pair| pair[0].alias >= pair[1].alias)
            || self.models.iter().any(|binding| {
                !valid_authoring_model_reference(&binding.alias)
                    || binding.deployment.resource_kind != ResourceKind::ModelDeployment
                    || binding.deployment.validate().is_err()
                    || binding.selection_policy.validate().is_err()
            })
            || self.expected_digest()? != self.profile_digest
        {
            return Err(ListError::Invalid);
        }
        Ok(())
    }

    fn expected_digest(&self) -> Result<Sha256Digest, ListError> {
        canonical_digest(&serde_json::json!({
            "default_deadline_seconds": self.default_deadline_seconds,
            "default_environment": self.default_environment,
            "deployment_policies": self.deployment_policies,
            "execution_profile": self.execution_profile,
            "model_loop": self.model_loop,
            "models": self.models,
            "policy_versions": self.policy_versions,
            "schema_version": self.schema_version,
        }))
        .map_err(|_| ListError::Invalid)?
        .parse()
        .map_err(|_| ListError::Invalid)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentListFiltersV1 {
    pub state: Option<AgentProductState>,
    pub environment: Option<String>,
}

impl AgentListFiltersV1 {
    pub fn validate(&self) -> Result<(), ListError> {
        if self
            .environment
            .as_deref()
            .is_some_and(|value| !valid_environment(value))
        {
            return Err(ListError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunListFiltersV1 {
    pub agent_id: Option<ResourceId>,
    pub state: Option<RunState>,
    pub created_after: Option<UtcTimestamp>,
    pub created_before: Option<UtcTimestamp>,
}

impl RunListFiltersV1 {
    pub fn validate(&self) -> Result<(), ListError> {
        if self
            .agent_id
            .as_ref()
            .is_some_and(|id| id.kind() != ResourceKind::Agent)
        {
            return Err(ListError::Invalid);
        }
        let after = self
            .created_after
            .as_ref()
            .map(parse_timestamp)
            .transpose()?;
        let before = self
            .created_before
            .as_ref()
            .map(parse_timestamp)
            .transpose()?;
        if matches!((after, before), (Some(after), Some(before)) if after >= before) {
            return Err(ListError::Invalid);
        }
        Ok(())
    }

    /// Validate against an actual database snapshot, never an HTTP clock approximation.
    pub fn validate_at(&self, snapshot_at: DateTime<Utc>) -> Result<(), ListError> {
        self.validate()?;
        if self
            .created_before
            .as_ref()
            .map(parse_timestamp)
            .transpose()?
            .is_some_and(|before| before > snapshot_at)
        {
            return Err(ListError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListPageV1<T> {
    pub schema_version: u32,
    pub items: Vec<T>,
    pub next_cursor: Option<OpaqueListCursor>,
}

impl<T> ListPageV1<T> {
    pub fn new(items: Vec<T>, next_cursor: Option<OpaqueListCursor>) -> Result<Self, ListError> {
        if items.len() > usize::from(PRODUCT_LIST_MAX_PAGE_SIZE) {
            return Err(ListError::Invalid);
        }
        Ok(Self {
            schema_version: 1,
            items,
            next_cursor,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentSummaryV1 {
    pub schema_version: u32,
    pub name: String,
    pub display_name: String,
    pub agent_id: ResourceId,
    pub active_deployment: Option<ExactDeploymentRef>,
    pub state: AgentProductState,
    pub environment: Option<String>,
    pub updated_at: UtcTimestamp,
    pub published_at: Option<UtcTimestamp>,
    pub required_features: Vec<AgentRequiredFeature>,
    pub latest_run_state: Option<RunState>,
}

impl AgentSummaryV1 {
    pub fn validate(&self) -> Result<(), ListError> {
        if self.schema_version != 1
            || self.agent_id.kind() != ResourceKind::Agent
            || self.active_deployment.as_ref().is_some_and(|value| {
                value.resource_kind != ResourceKind::AgentDeployment || value.validate().is_err()
            })
            || !valid_authoring_name(&self.name)
            || !valid_display_name(&self.display_name)
            || self
                .environment
                .as_deref()
                .is_some_and(|value| !valid_environment(value))
            || !strictly_sorted_unique(&self.required_features)
        {
            return Err(ListError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunSummaryV1 {
    pub schema_version: u32,
    pub run_id: ResourceId,
    pub agent_name: String,
    pub agent_id: ResourceId,
    pub state: RunState,
    pub started_at: Option<UtcTimestamp>,
    pub terminal_at: Option<UtcTimestamp>,
    pub waiting_task_count: u32,
    pub result_available: bool,
}

impl RunSummaryV1 {
    pub fn validate(&self) -> Result<(), ListError> {
        if self.schema_version != 1
            || self.run_id.kind() != ResourceKind::Run
            || self.agent_id.kind() != ResourceKind::Agent
            || !valid_authoring_name(&self.agent_name)
        {
            return Err(ListError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListRoutePurpose {
    Conversations,
    ConversationTurns,
    AuthoringDependencies,
    RunValues,
    ChildRuns,
    Agents,
    Runs,
    Tasks,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListCursorContext {
    pub purpose: ListRoutePurpose,
    pub filter_digest: Sha256Digest,
    pub page_size: u16,
}

impl ListCursorContext {
    pub fn validate(&self) -> Result<(), ListError> {
        if self.page_size == 0 || self.page_size > PRODUCT_LIST_MAX_PAGE_SIZE {
            return Err(ListError::Invalid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ListKeysetBoundary {
    Conversation {
        created_at: UtcTimestamp,
        conversation_id: ResourceId,
    },
    ConversationTurn {
        created_at: UtcTimestamp,
        ordinal: u32,
    },
    ChildRun {
        created_at: UtcTimestamp,
        run_id: ResourceId,
    },
    RunValue {
        created_at: UtcTimestamp,
        value_id: ResourceId,
    },
    AuthoringDependency {
        created_at: UtcTimestamp,
        deployment_id: ResourceId,
    },
    Task {
        created_at: UtcTimestamp,
        task_id: ResourceId,
    },
    Agent {
        updated_at: UtcTimestamp,
        agent_id: ResourceId,
    },
    Run {
        created_at: UtcTimestamp,
        run_id: ResourceId,
    },
}

impl ListKeysetBoundary {
    fn validates_for(&self, purpose: ListRoutePurpose) -> bool {
        matches!((purpose,self),(ListRoutePurpose::Conversations,Self::Conversation{conversation_id,..}) if conversation_id.kind()==ResourceKind::Conversation)
            || matches!((purpose,self),(ListRoutePurpose::ConversationTurns,Self::ConversationTurn{ordinal,..}) if *ordinal>0 && *ordinal<=128)
            || matches!(
                (purpose, self),
                (
                    ListRoutePurpose::Agents,
                    Self::Agent { agent_id, .. }
                ) if agent_id.kind() == ResourceKind::Agent
            )
            || matches!(
                (purpose, self),
                (ListRoutePurpose::Runs, Self::Run { run_id, .. })
                    if run_id.kind() == ResourceKind::Run
            )
            || matches!((purpose,self),(ListRoutePurpose::ChildRuns,Self::ChildRun{run_id,..}) if run_id.kind()==ResourceKind::Run)
            || matches!((purpose,self),(ListRoutePurpose::RunValues,Self::RunValue{value_id,..}) if value_id.kind()==ResourceKind::RunValue)
            || matches!((purpose,self),(ListRoutePurpose::AuthoringDependencies,Self::AuthoringDependency{deployment_id,..}) if deployment_id.kind().is_deployment())
            || matches!((purpose, self), (ListRoutePurpose::Tasks, Self::Task { task_id, .. }) if matches!(task_id.kind(), ResourceKind::Interaction | ResourceKind::ApprovalTask))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedListCursor {
    pub snapshot_at: DateTime<Utc>,
    pub boundary: ListKeysetBoundary,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthorityListPage<T> {
    /// The durable owner's database timestamp, preserved across continuation pages. It is
    /// compared with database timestamps, never with the HTTP process clock.
    pub snapshot_at: DateTime<Utc>,
    pub items: Vec<T>,
    pub next_boundary: Option<ListKeysetBoundary>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListError {
    Invalid,
    Expired,
}

pub trait ListCursorCodec: Send + Sync {
    fn encode(
        &self,
        principal: &AuthenticatedPrincipal,
        context: &ListCursorContext,
        snapshot_at: DateTime<Utc>,
        boundary: ListKeysetBoundary,
        expires_at: DateTime<Utc>,
    ) -> Result<OpaqueListCursor, ListError>;

    fn decode(
        &self,
        cursor: &str,
        principal: &AuthenticatedPrincipal,
        context: &ListCursorContext,
        now: DateTime<Utc>,
    ) -> Result<DecodedListCursor, ListError>;
}

#[derive(Debug, Clone)]
pub struct HmacListCursorCodec {
    key: hmac::Key,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ListCursorClaims {
    schema_version: u32,
    purpose: ListRoutePurpose,
    tenant_id: ResourceId,
    principal_scope_digest: Sha256Digest,
    filter_digest: Sha256Digest,
    page_size: u16,
    snapshot_at: UtcTimestamp,
    boundary: ListKeysetBoundary,
    /// Fixed by the first HTTP request's clock; independent of the database snapshot clock.
    expires_at_epoch_seconds: i64,
}

impl HmacListCursorCodec {
    pub fn install(key: &[u8]) -> Result<Self, ListError> {
        if !(32..=64).contains(&key.len()) {
            return Err(ListError::Invalid);
        }
        Ok(Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, key),
        })
    }
}

impl ListCursorCodec for HmacListCursorCodec {
    fn encode(
        &self,
        principal: &AuthenticatedPrincipal,
        context: &ListCursorContext,
        snapshot_at: DateTime<Utc>,
        boundary: ListKeysetBoundary,
        expires_at: DateTime<Utc>,
    ) -> Result<OpaqueListCursor, ListError> {
        context.validate()?;
        if principal.validate().is_err() || !boundary.validates_for(context.purpose) {
            return Err(ListError::Invalid);
        }
        let claims = ListCursorClaims {
            schema_version: 1,
            purpose: context.purpose,
            tenant_id: principal.tenant_id.clone(),
            principal_scope_digest: principal_scope_digest(principal)?,
            filter_digest: context.filter_digest.clone(),
            page_size: context.page_size,
            snapshot_at: UtcTimestamp::from_datetime(snapshot_at),
            boundary,
            expires_at_epoch_seconds: expires_at.timestamp(),
        };
        let payload = serde_jcs::to_vec(&claims).map_err(|_| ListError::Invalid)?;
        let signature = hmac::sign(&self.key, &payload);
        OpaqueListCursor::new(format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(payload),
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        ))
        .map_err(|_| ListError::Invalid)
    }

    fn decode(
        &self,
        cursor: &str,
        principal: &AuthenticatedPrincipal,
        context: &ListCursorContext,
        now: DateTime<Utc>,
    ) -> Result<DecodedListCursor, ListError> {
        context.validate()?;
        if principal.validate().is_err() {
            return Err(ListError::Invalid);
        }
        let (payload, signature) = cursor.split_once('.').ok_or(ListError::Invalid)?;
        if signature.contains('.') {
            return Err(ListError::Invalid);
        }
        let payload = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| ListError::Invalid)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| ListError::Invalid)?;
        hmac::verify(&self.key, &payload, &signature).map_err(|_| ListError::Invalid)?;
        let claims: ListCursorClaims =
            serde_json::from_slice(&payload).map_err(|_| ListError::Invalid)?;
        if claims.schema_version != 1
            || claims.purpose != context.purpose
            || claims.tenant_id != principal.tenant_id
            || claims.principal_scope_digest != principal_scope_digest(principal)?
            || claims.filter_digest != context.filter_digest
            || claims.page_size != context.page_size
            || !claims.boundary.validates_for(context.purpose)
        {
            return Err(ListError::Invalid);
        }
        if claims.expires_at_epoch_seconds <= now.timestamp() {
            return Err(ListError::Expired);
        }
        let expires_at = DateTime::from_timestamp(claims.expires_at_epoch_seconds, 0)
            .ok_or(ListError::Invalid)?;
        let snapshot_at = DateTime::parse_from_rfc3339(claims.snapshot_at.as_str())
            .map_err(|_| ListError::Invalid)?
            .with_timezone(&Utc);
        let boundary_at = match &claims.boundary {
            ListKeysetBoundary::Agent { updated_at, .. } => updated_at,
            ListKeysetBoundary::Conversation { created_at, .. }
            | ListKeysetBoundary::ConversationTurn { created_at, .. }
            | ListKeysetBoundary::Run { created_at, .. }
            | ListKeysetBoundary::Task { created_at, .. }
            | ListKeysetBoundary::AuthoringDependency { created_at, .. }
            | ListKeysetBoundary::ChildRun { created_at, .. }
            | ListKeysetBoundary::RunValue { created_at, .. } => created_at,
        };
        let boundary_at = DateTime::parse_from_rfc3339(boundary_at.as_str())
            .map_err(|_| ListError::Invalid)?
            .with_timezone(&Utc);
        if boundary_at > snapshot_at {
            return Err(ListError::Invalid);
        }
        Ok(DecodedListCursor {
            snapshot_at,
            boundary: claims.boundary,
            expires_at,
        })
    }
}

pub fn list_filter_digest<T: Serialize>(filters: &T) -> Result<Sha256Digest, ListError> {
    canonical_digest(&serde_json::to_value(filters).map_err(|_| ListError::Invalid)?)
        .map_err(|_| ListError::Invalid)?
        .parse()
        .map_err(|_| ListError::Invalid)
}

fn principal_scope_digest(principal: &AuthenticatedPrincipal) -> Result<Sha256Digest, ListError> {
    canonical_digest(&serde_json::json!({
        "binding_generation": principal.binding_generation,
        "binding_version": principal.binding_version,
        "permissions": principal.permissions,
        "principal_id": principal.principal_id,
        "principal_kind": principal.principal_kind,
        "principal_version": principal.principal_version,
        "schema_version": 1,
        "tenant_id": principal.tenant_id,
    }))
    .map_err(|_| ListError::Invalid)?
    .parse()
    .map_err(|_| ListError::Invalid)
}

fn valid_authoring_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && value.len() <= 63
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn valid_display_name(value: &str) -> bool {
    !value.is_empty() && value.chars().count() <= 255 && !value.chars().any(char::is_control)
}

fn valid_authoring_model_reference(value: &str) -> bool {
    value.strip_prefix("project/").is_some_and(|name| {
        let mut bytes = name.bytes();
        name.len() <= 63
            && bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
            && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    })
}

fn valid_environment(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && value.len() <= 64
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.' | b'-')
        })
}

fn parse_timestamp(value: &UtcTimestamp) -> Result<DateTime<Utc>, ListError> {
    DateTime::parse_from_rfc3339(value.as_str())
        .map(|value| value.with_timezone(&Utc))
        .map_err(|_| ListError::Invalid)
}

fn strictly_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        AuthnStrength, Permission, PermissionSet, PrincipalKind, TraceIdentityV1,
    };

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

    fn principal() -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            tenant_id: id(ResourceKind::Tenant, 1),
            principal_id: id(ResourceKind::Principal, 2),
            principal_kind: PrincipalKind::AgentRunner,
            permissions: PermissionSet::new(vec![Permission::AgentRead, Permission::RuntimeRead])
                .unwrap(),
            authn_strength: AuthnStrength::MultiFactor,
            principal_version: 3,
            binding_generation: 4,
            binding_version: 5,
            credential_digest: digest('a'),
            credential_expires_at: Utc::now() + chrono::Duration::hours(1),
            trace: TraceIdentityV1::generate(),
        }
    }

    fn policy_binding(suffix: u16, marker: char) -> ExactPolicyBinding {
        ExactPolicyBinding {
            deployment: ExactDeploymentRef::new(
                id(ResourceKind::PolicyDeployment, suffix),
                digest(marker),
            )
            .unwrap(),
            revision: ExactVersionRef::new(
                id(ResourceKind::PolicyRevision, suffix),
                digest(marker),
            )
            .unwrap(),
        }
    }

    #[test]
    fn authoring_model_references_are_directly_usable_project_names() {
        let binding = |alias: String| AgentAuthoringModelBindingV1 {
            alias,
            deployment: ExactDeploymentRef::new(id(ResourceKind::ModelDeployment, 20), digest('a'))
                .unwrap(),
            selection_policy: policy_binding(21, 'b'),
        };
        for reference in [
            "project/default".to_owned(),
            "project/a".to_owned(),
            format!("project/{}", "a".repeat(63)),
        ] {
            let profile = AgentAuthoringProfileV1::build(
                policy_binding(22, 'c'),
                vec![binding(reference.clone())],
            )
            .unwrap();
            assert_eq!(profile.models[0].alias, reference);
        }
        for reference in [
            "default".to_owned(),
            "project/".to_owned(),
            "project/a.b".to_owned(),
            "project/a_b".to_owned(),
            "project/A".to_owned(),
            "project/a/b".to_owned(),
            "project/中文".to_owned(),
            format!("project/{}", "a".repeat(64)),
            id(ResourceKind::ModelDeployment, 23).to_string(),
        ] {
            assert_eq!(
                AgentAuthoringProfileV1::build(policy_binding(22, 'c'), vec![binding(reference)]),
                Err(ListError::Invalid)
            );
        }
    }

    #[test]
    fn authoring_profile_tool_budgets_are_paired_and_default_to_zero() {
        let original = AgentAuthoringProfileV1::build(policy_binding(10, 'b'), vec![]).unwrap();
        assert_eq!(original.model_loop.maximum_capability_calls, 0);
        assert_eq!(original.model_loop.maximum_parallel_calls_per_round, 0);
        for (total, parallel, accepted) in [
            (0, 0, true),
            (1, 1, true),
            (8, 2, true),
            (0, 1, false),
            (1, 0, false),
            (1, 2, false),
        ] {
            let mut profile = original.clone();
            profile.model_loop.maximum_capability_calls = total;
            profile.model_loop.maximum_parallel_calls_per_round = parallel;
            profile.profile_digest = profile.expected_digest().unwrap();
            assert_eq!(profile.validate().is_ok(), accepted);
        }
    }

    #[test]
    fn agent_authoring_profile_is_closed_bounded_and_digest_protected() {
        let mut profile = AgentAuthoringProfileV1::build(policy_binding(10, 'b'), vec![]).unwrap();
        assert_eq!(profile.validate(), Ok(()));

        profile.default_deadline_seconds = 3_601;
        assert_eq!(profile.validate(), Err(ListError::Invalid));

        let profile = AgentAuthoringProfileV1::build(policy_binding(11, 'c'), vec![]).unwrap();
        let mut value = serde_json::to_value(profile).unwrap();
        value["credential"] = serde_json::json!("forbidden");
        assert!(serde_json::from_value::<AgentAuthoringProfileV1>(value).is_err());
    }

    #[test]
    fn cursor_binds_route_tenant_principal_filter_page_snapshot_and_expiry() {
        let codec = HmacListCursorCodec::install(&[7_u8; 32]).unwrap();
        let clock_now = DateTime::parse_from_rfc3339("2026-09-05T12:34:56.123456789Z")
            .unwrap()
            .with_timezone(&Utc);
        let snapshot_at = parse_timestamp(&UtcTimestamp::from_datetime(clock_now)).unwrap();
        assert_ne!(snapshot_at, clock_now);
        let context = ListCursorContext {
            purpose: ListRoutePurpose::Agents,
            filter_digest: digest('b'),
            page_size: 25,
        };
        let boundary = ListKeysetBoundary::Agent {
            updated_at: UtcTimestamp::from_datetime(clock_now - chrono::Duration::seconds(1)),
            agent_id: id(ResourceKind::Agent, 3),
        };
        let cursor = codec
            .encode(
                &principal(),
                &context,
                clock_now,
                boundary.clone(),
                clock_now + chrono::Duration::minutes(15),
            )
            .unwrap();
        assert_eq!(
            codec.decode(cursor.as_str(), &principal(), &context, clock_now),
            Ok(DecodedListCursor {
                snapshot_at,
                boundary,
                expires_at: DateTime::from_timestamp(
                    (clock_now + chrono::Duration::minutes(15)).timestamp(),
                    0,
                )
                .unwrap(),
            })
        );

        let mut wrong_context = context.clone();
        wrong_context.purpose = ListRoutePurpose::Runs;
        assert_eq!(
            codec.decode(cursor.as_str(), &principal(), &wrong_context, clock_now),
            Err(ListError::Invalid)
        );
        wrong_context = context.clone();
        wrong_context.filter_digest = digest('c');
        assert_eq!(
            codec.decode(cursor.as_str(), &principal(), &wrong_context, clock_now),
            Err(ListError::Invalid)
        );
        wrong_context = context.clone();
        wrong_context.page_size = 24;
        assert_eq!(
            codec.decode(cursor.as_str(), &principal(), &wrong_context, clock_now),
            Err(ListError::Invalid)
        );

        let mut wrong_principal = principal();
        wrong_principal.binding_version += 1;
        assert_eq!(
            codec.decode(cursor.as_str(), &wrong_principal, &context, clock_now),
            Err(ListError::Invalid)
        );
        assert_eq!(
            codec.decode(
                cursor.as_str(),
                &principal(),
                &context,
                clock_now + chrono::Duration::minutes(15),
            ),
            Err(ListError::Expired)
        );
    }

    #[test]
    fn cursor_keeps_independent_database_time_and_fixed_http_expiry() {
        let codec = HmacListCursorCodec::install(&[7; 32]).unwrap();
        let now = DateTime::parse_from_rfc3339("2026-09-07T12:00:00.000000Z")
            .unwrap()
            .with_timezone(&Utc);
        let expires_at = now + chrono::Duration::seconds(PRODUCT_LIST_CURSOR_TTL_SECONDS);
        let context = ListCursorContext {
            purpose: ListRoutePurpose::Tasks,
            filter_digest: digest('b'),
            page_size: 1,
        };
        for offset in [
            chrono::Duration::milliseconds(25),
            chrono::Duration::hours(1),
        ] {
            let snapshot = now + offset;
            let boundary = ListKeysetBoundary::Task {
                created_at: UtcTimestamp::from_datetime(snapshot),
                task_id: id(ResourceKind::Interaction, 3),
            };
            let cursor = codec
                .encode(&principal(), &context, snapshot, boundary, expires_at)
                .unwrap();
            let decoded = codec
                .decode(cursor.as_str(), &principal(), &context, now)
                .unwrap();
            assert_eq!(decoded.snapshot_at, snapshot);
            assert_eq!(decoded.expires_at, expires_at);
            let continued = codec
                .encode(
                    &principal(),
                    &context,
                    decoded.snapshot_at,
                    decoded.boundary,
                    decoded.expires_at,
                )
                .unwrap();
            assert!(codec
                .decode(
                    continued.as_str(),
                    &principal(),
                    &context,
                    expires_at - chrono::Duration::seconds(1)
                )
                .is_ok());
            assert_eq!(
                codec.decode(continued.as_str(), &principal(), &context, expires_at),
                Err(ListError::Expired)
            );

            let invalid = codec
                .encode(
                    &principal(),
                    &context,
                    snapshot,
                    ListKeysetBoundary::Task {
                        created_at: UtcTimestamp::from_datetime(
                            snapshot + chrono::Duration::milliseconds(1),
                        ),
                        task_id: id(ResourceKind::Interaction, 3),
                    },
                    expires_at,
                )
                .unwrap();
            assert_eq!(
                codec.decode(invalid.as_str(), &principal(), &context, now),
                Err(ListError::Invalid)
            );
        }
    }

    #[test]
    fn product_summaries_and_pages_are_closed_and_bounded() {
        let summary = AgentSummaryV1 {
            schema_version: 1,
            name: "support-agent".to_owned(),
            display_name: "Support Agent".to_owned(),
            agent_id: id(ResourceKind::Agent, 1),
            active_deployment: None,
            state: AgentProductState::Ready,
            environment: Some("development".to_owned()),
            updated_at: UtcTimestamp::from_datetime(Utc::now()),
            published_at: None,
            required_features: vec![AgentRequiredFeature::Model],
            latest_run_state: Some(RunState::Succeeded),
        };
        assert_eq!(summary.validate(), Ok(()));
        assert!(ListPageV1::new(vec![summary], None).is_ok());
        assert!(serde_json::from_value::<AgentSummaryV1>(serde_json::json!({
            "schema_version": 1,
            "name": "support-agent",
            "display_name": "Support Agent",
            "agent_id": id(ResourceKind::Agent, 1),
            "state": "ready",
            "environment": null,
            "updated_at": UtcTimestamp::from_datetime(Utc::now()),
            "published_at": null,
            "required_features": [],
            "latest_run_state": null,
            "etag": "forbidden"
        }))
        .is_err());
    }
}
