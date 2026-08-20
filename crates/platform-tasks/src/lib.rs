//! Pure state decisions for the shared durable Task authority.
//!
//! Approval, interaction, and human work keep their domain-specific meaning while sharing one
//! generation/version first-winner primitive. Repositories provide database time and persist an
//! accepted decision together with its owner wake, Receipt, Event, and Outbox.

use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    Effect, InteractionKind, InteractionSchemaDocument, McpOAuthTaskBinding, PrincipalSnapshot,
    ResourceId, ResourceKind, Sha256Digest,
};
use serde::{Deserialize, Serialize};
use std::{error::Error, fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskKind {
    Approval,
    InteractionForm,
    InteractionUrlConsent,
    InteractionBusinessInput,
    ExternalAuthorization,
    HumanWork,
}

impl TaskKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Approval => "approval",
            Self::InteractionForm => "interaction_form",
            Self::InteractionUrlConsent => "interaction_url_consent",
            Self::InteractionBusinessInput => "interaction_business_input",
            Self::ExternalAuthorization => "external_authorization",
            Self::HumanWork => "human_work",
        }
    }

    pub const fn task_id_kind(self) -> ResourceKind {
        match self {
            Self::Approval => ResourceKind::ApprovalTask,
            Self::InteractionForm
            | Self::InteractionUrlConsent
            | Self::InteractionBusinessInput
            | Self::ExternalAuthorization
            | Self::HumanWork => ResourceKind::Interaction,
        }
    }

    pub const fn responder_permission(self) -> insight_platform_contracts::Permission {
        match self {
            Self::Approval => insight_platform_contracts::Permission::ApprovalRespond,
            Self::InteractionForm
            | Self::InteractionUrlConsent
            | Self::InteractionBusinessInput
            | Self::HumanWork => insight_platform_contracts::Permission::InteractionRespond,
            Self::ExternalAuthorization => insight_platform_contracts::Permission::McpWrite,
        }
    }
}

impl fmt::Display for TaskKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TaskKind {
    type Err = TaskError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "approval" => Ok(Self::Approval),
            "interaction_form" => Ok(Self::InteractionForm),
            "interaction_url_consent" => Ok(Self::InteractionUrlConsent),
            "interaction_business_input" => Ok(Self::InteractionBusinessInput),
            "external_authorization" => Ok(Self::ExternalAuthorization),
            "human_work" => Ok(Self::HumanWork),
            _ => Err(TaskError::InvalidKind),
        }
    }
}

impl From<InteractionKind> for TaskKind {
    fn from(kind: InteractionKind) -> Self {
        match kind {
            InteractionKind::Form => Self::InteractionForm,
            InteractionKind::UrlConsent => Self::InteractionUrlConsent,
            InteractionKind::BusinessInput => Self::InteractionBusinessInput,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Pending,
    Responded,
    Declined,
    Approved,
    Rejected,
    Cancelled,
    Expired,
}

impl TaskState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Responded => "responded",
            Self::Declined => "declined",
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Cancelled => "cancelled",
            Self::Expired => "expired",
        }
    }

    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Pending)
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for TaskState {
    type Err = TaskError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "responded" => Ok(Self::Responded),
            "declined" => Ok(Self::Declined),
            "approved" => Ok(Self::Approved),
            "rejected" => Ok(Self::Rejected),
            "cancelled" => Ok(Self::Cancelled),
            "expired" => Ok(Self::Expired),
            _ => Err(TaskError::InvalidState),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskDefinition {
    Approval {
        owner_version: u64,
        owner_snapshot_digest: Sha256Digest,
        effect: Effect,
        input_digest: Sha256Digest,
        policy_revision_id: ResourceId,
        approver_rule_digest: Sha256Digest,
        safe_prompt_key: String,
    },
    Interaction {
        interaction_kind: InteractionKind,
        eligible_principal_rule_digest: Sha256Digest,
        safe_prompt_key: String,
    },
    CapabilityInput {
        owner_version: u64,
        owner_snapshot_digest: Sha256Digest,
        job_id: ResourceId,
        wake_generation: u64,
        opaque_state_digest: Sha256Digest,
        interaction_kind: InteractionKind,
        response_schema: InteractionSchemaDocument,
        eligible_principal_rule_digest: Sha256Digest,
        exact_eligible_principal_id: Option<ResourceId>,
        safe_prompt_key: String,
    },
    #[serde(rename = "mcp_oauth_authorization")]
    McpOAuthAuthorization {
        binding: Box<McpOAuthTaskBinding>,
        safe_prompt_key: String,
    },
    HumanWork {
        eligible_principal_rule_digest: Sha256Digest,
        safe_prompt_key: String,
    },
}

impl TaskDefinition {
    pub const fn task_kind(&self) -> TaskKind {
        match self {
            Self::Approval { .. } => TaskKind::Approval,
            Self::Interaction {
                interaction_kind, ..
            }
            | Self::CapabilityInput {
                interaction_kind, ..
            } => match interaction_kind {
                InteractionKind::Form => TaskKind::InteractionForm,
                InteractionKind::UrlConsent => TaskKind::InteractionUrlConsent,
                InteractionKind::BusinessInput => TaskKind::InteractionBusinessInput,
            },
            Self::McpOAuthAuthorization { .. } => TaskKind::ExternalAuthorization,
            Self::HumanWork { .. } => TaskKind::HumanWork,
        }
    }

    pub fn validate(&self) -> Result<(), TaskError> {
        let (safe_prompt_key, policy_revision_id, owner_version, job_binding) = match self {
            Self::Approval {
                safe_prompt_key,
                policy_revision_id,
                owner_version,
                ..
            } => (
                safe_prompt_key,
                Some(policy_revision_id),
                Some(*owner_version),
                None,
            ),
            Self::Interaction {
                safe_prompt_key, ..
            }
            | Self::HumanWork {
                safe_prompt_key, ..
            } => (safe_prompt_key, None, None, None),
            Self::CapabilityInput {
                safe_prompt_key,
                owner_version,
                job_id,
                wake_generation,
                ..
            } => (
                safe_prompt_key,
                None,
                Some(*owner_version),
                Some((job_id, *wake_generation)),
            ),
            Self::McpOAuthAuthorization {
                safe_prompt_key, ..
            } => (safe_prompt_key, None, None, None),
        };
        if !is_stable_key(safe_prompt_key)
            || owner_version == Some(0)
            || policy_revision_id.is_some_and(|id| id.kind() != ResourceKind::PolicyRevision)
            || job_binding.is_some_and(|(job_id, wake_generation)| {
                job_id.kind() != ResourceKind::Job || wake_generation == 0
            })
        {
            return Err(TaskError::InvalidDefinition);
        }
        if let Self::CapabilityInput {
            response_schema,
            exact_eligible_principal_id,
            ..
        } = self
        {
            response_schema
                .validate()
                .map_err(|_| TaskError::InvalidDefinition)?;
            if exact_eligible_principal_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::Principal)
            {
                return Err(TaskError::InvalidDefinition);
            }
        }
        if let Self::McpOAuthAuthorization { binding, .. } = self {
            binding
                .validate()
                .map_err(|_| TaskError::InvalidDefinition)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskResolution {
    pub state: TaskState,
    pub principal: Option<PrincipalSnapshot>,
    pub response_value_id: Option<ResourceId>,
    pub response_schema_digest: Option<Sha256Digest>,
}

impl TaskResolution {
    fn validate_for(&self, kind: TaskKind) -> Result<(), TaskError> {
        let human_decision = matches!(
            self.state,
            TaskState::Responded | TaskState::Declined | TaskState::Approved | TaskState::Rejected
        );
        if !self.state.is_terminal()
            || human_decision != self.principal.is_some()
            || self
                .principal
                .as_ref()
                .is_some_and(|principal| principal.validate().is_err())
            || (kind == TaskKind::ExternalAuthorization
                && (self.response_value_id.is_some() || self.response_schema_digest.is_some()))
            || (kind != TaskKind::ExternalAuthorization
                && (self.state == TaskState::Responded)
                    != (self.response_value_id.is_some() && self.response_schema_digest.is_some()))
            || self
                .response_value_id
                .as_ref()
                .is_some_and(|id| id.kind() != ResourceKind::RunValue)
            || (kind == TaskKind::Approval
                && !matches!(
                    self.state,
                    TaskState::Approved
                        | TaskState::Rejected
                        | TaskState::Cancelled
                        | TaskState::Expired
                ))
            || (kind != TaskKind::Approval
                && !matches!(
                    self.state,
                    TaskState::Responded
                        | TaskState::Declined
                        | TaskState::Cancelled
                        | TaskState::Expired
                ))
        {
            return Err(TaskError::InvalidResolution);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPayload {
    pub definition: TaskDefinition,
    pub created_by: PrincipalSnapshot,
    pub resolution: Option<TaskResolution>,
}

impl TaskPayload {
    pub fn validate(&self) -> Result<(), TaskError> {
        self.definition.validate()?;
        self.created_by
            .validate()
            .map_err(|_| TaskError::InvalidDefinition)?;
        if let Some(resolution) = &self.resolution {
            resolution.validate_for(self.definition.task_kind())?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskProjection {
    pub tenant_id: ResourceId,
    pub task_id: ResourceId,
    pub kind: TaskKind,
    pub state: TaskState,
    pub generation: u64,
    pub version: u64,
    pub response_schema_digest: Option<Sha256Digest>,
    pub payload: TaskPayload,
    pub response_value_id: Option<ResourceId>,
    pub deadline: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

impl TaskProjection {
    pub fn validate(&self) -> Result<(), TaskError> {
        self.payload.validate()?;
        if self.tenant_id.kind() != ResourceKind::Tenant
            || self.task_id.kind() != self.kind.task_id_kind()
            || self.kind != self.payload.definition.task_kind()
            || self.generation == 0
            || self.version == 0
            || (self.state == TaskState::Pending)
                != (self.resolved_at.is_none()
                    && self.payload.resolution.is_none()
                    && self.response_value_id.is_none())
            || self.state.is_terminal()
                != (self.resolved_at.is_some() && self.payload.resolution.is_some())
            || self.response_value_id
                != self
                    .payload
                    .resolution
                    .as_ref()
                    .and_then(|resolution| resolution.response_value_id.clone())
        {
            return Err(TaskError::InvalidProjection);
        }
        if let Some(resolution) = &self.payload.resolution {
            if resolution.state != self.state {
                return Err(TaskError::InvalidProjection);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ResolveTask {
    pub expected_generation: u64,
    pub expected_version: u64,
    pub target: TaskState,
    pub principal: Option<PrincipalSnapshot>,
    pub response_value_id: Option<ResourceId>,
    pub response_schema_digest: Option<Sha256Digest>,
}

pub fn decide_resolution(
    current: &TaskProjection,
    command: ResolveTask,
    database_now: DateTime<Utc>,
) -> Result<TaskProjection, TaskError> {
    current.validate()?;
    if current.state != TaskState::Pending
        || current.generation != command.expected_generation
        || current.version != command.expected_version
    {
        return Err(TaskError::FirstWinnerLost);
    }
    if command.target == TaskState::Expired {
        if database_now < current.deadline {
            return Err(TaskError::DeadlineNotReached);
        }
    } else if command.target != TaskState::Cancelled && database_now >= current.deadline {
        return Err(TaskError::DeadlineExceeded);
    }
    if command.target == TaskState::Responded
        && current.kind != TaskKind::ExternalAuthorization
        && (current.response_schema_digest.is_none()
            || current.response_schema_digest != command.response_schema_digest)
    {
        return Err(TaskError::ResponseSchemaMismatch);
    }
    let resolution = TaskResolution {
        state: command.target,
        principal: command.principal,
        response_value_id: command.response_value_id,
        response_schema_digest: command.response_schema_digest,
    };
    resolution.validate_for(current.kind)?;
    let mut next = current.clone();
    next.state = command.target;
    next.version = next
        .version
        .checked_add(1)
        .ok_or(TaskError::CounterOverflow)?;
    next.response_value_id = resolution.response_value_id.clone();
    next.payload.resolution = Some(resolution);
    next.resolved_at = Some(database_now);
    next.validate()?;
    Ok(next)
}

fn is_stable_key(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskError {
    InvalidKind,
    InvalidState,
    InvalidDefinition,
    InvalidResolution,
    InvalidProjection,
    FirstWinnerLost,
    DeadlineNotReached,
    DeadlineExceeded,
    ResponseSchemaMismatch,
    CounterOverflow,
}

impl fmt::Display for TaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidKind => "Task kind is invalid",
            Self::InvalidState => "Task state is invalid",
            Self::InvalidDefinition => "Task definition is invalid",
            Self::InvalidResolution => "Task resolution is invalid",
            Self::InvalidProjection => "Task projection is invalid",
            Self::FirstWinnerLost => "Task resolution lost the generation/version first-winner",
            Self::DeadlineNotReached => "Task expiry deadline has not been reached",
            Self::DeadlineExceeded => "Task response deadline has been exceeded",
            Self::ResponseSchemaMismatch => "Task response schema does not match",
            Self::CounterOverflow => "Task counter overflowed",
        })
    }
}

impl Error for TaskError {}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use insight_platform_contracts::{
        ExactDeploymentRef, ExactSecretBindingRef, ExactVersionRef, McpOAuthTaskBinding,
        Permission, PermissionSet, PrincipalKind, SecretResolutionPolicy,
    };

    fn id(value: &str) -> ResourceId {
        value.parse().unwrap()
    }

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn principal() -> PrincipalSnapshot {
        PrincipalSnapshot::build(
            id("ten_0198f1c5-0787-75e1-a9e8-d95ca0f39001"),
            id("prn_0198f1c5-0787-75e1-a9e8-d95ca0f39002"),
            PrincipalKind::HumanApprover,
            PermissionSet::new(vec![Permission::InteractionRespond]).unwrap(),
            1,
            1,
            1,
        )
        .unwrap()
    }

    fn pending(now: DateTime<Utc>) -> TaskProjection {
        let principal = principal();
        TaskProjection {
            tenant_id: principal.tenant_id.clone(),
            task_id: id("int_0198f1c5-0787-75e1-a9e8-d95ca0f39003"),
            kind: TaskKind::InteractionForm,
            state: TaskState::Pending,
            generation: 1,
            version: 1,
            response_schema_digest: Some(digest('a')),
            payload: TaskPayload {
                definition: TaskDefinition::Interaction {
                    interaction_kind: InteractionKind::Form,
                    eligible_principal_rule_digest: digest('b'),
                    safe_prompt_key: "provide_business_input".to_owned(),
                },
                created_by: principal,
                resolution: None,
            },
            response_value_id: None,
            deadline: now + Duration::minutes(5),
            resolved_at: None,
        }
    }

    fn oauth_pending(now: DateTime<Utc>) -> TaskProjection {
        let principal = principal();
        TaskProjection {
            tenant_id: principal.tenant_id.clone(),
            task_id: id("int_0198f1c5-0787-75e1-a9e8-d95ca0f39005"),
            kind: TaskKind::ExternalAuthorization,
            state: TaskState::Pending,
            generation: 1,
            version: 1,
            response_schema_digest: None,
            payload: TaskPayload {
                definition: TaskDefinition::McpOAuthAuthorization {
                    binding: Box::new(McpOAuthTaskBinding {
                        authorization_binding_id: id("mab_0198f1c5-0787-75e1-a9e8-d95ca0f39006"),
                        mcp_deployment: ExactDeploymentRef::new(
                            id("mcdep_0198f1c5-0787-75e1-a9e8-d95ca0f39007"),
                            digest('b'),
                        )
                        .unwrap(),
                        auth_policy: ExactVersionRef::new(
                            id("prev_0198f1c5-0787-75e1-a9e8-d95ca0f3900a"),
                            digest('a'),
                        )
                        .unwrap(),
                        principal_binding_generation: 1,
                        audience_identity_digest: digest('c'),
                        requested_scopes: vec!["tools.call".to_owned()],
                        token_credential_purpose: "mcp.oauth".parse().unwrap(),
                        state_digest: digest('d'),
                        nonce_digest: digest('e'),
                        callback_binding_digest: digest('f'),
                        pkce_secret_binding: Box::new(
                            ExactSecretBindingRef::build(
                                id("sbd_0198f1c5-0787-75e1-a9e8-d95ca0f39008"),
                                1,
                                id("spr_0198f1c5-0787-75e1-a9e8-d95ca0f39009"),
                                "mcp.oauth.pkce".parse().unwrap(),
                                SecretResolutionPolicy::Pinned {
                                    opaque_version_identity_digest: digest('1'),
                                },
                            )
                            .unwrap(),
                        ),
                        expected_authorization_generation: None,
                        expected_authorization_version: None,
                    }),
                    safe_prompt_key: "authorize_mcp_server".to_owned(),
                },
                created_by: principal,
                resolution: None,
            },
            response_value_id: None,
            deadline: now + Duration::minutes(5),
            resolved_at: None,
        }
    }

    #[test]
    fn response_cancel_and_expiry_are_generation_first_winner() {
        let now = DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let pending = pending(now);
        let responded = decide_resolution(
            &pending,
            ResolveTask {
                expected_generation: 1,
                expected_version: 1,
                target: TaskState::Responded,
                principal: Some(principal()),
                response_value_id: Some(id("val_0198f1c5-0787-75e1-a9e8-d95ca0f39004")),
                response_schema_digest: Some(digest('a')),
            },
            now + Duration::seconds(1),
        )
        .unwrap();
        assert_eq!(responded.state, TaskState::Responded);
        assert_eq!(
            decide_resolution(
                &responded,
                ResolveTask {
                    expected_generation: 1,
                    expected_version: 1,
                    target: TaskState::Cancelled,
                    principal: None,
                    response_value_id: None,
                    response_schema_digest: None,
                },
                now + Duration::seconds(2),
            ),
            Err(TaskError::FirstWinnerLost)
        );
        assert_eq!(
            decide_resolution(
                &pending,
                ResolveTask {
                    expected_generation: 1,
                    expected_version: 1,
                    target: TaskState::Expired,
                    principal: None,
                    response_value_id: None,
                    response_schema_digest: None,
                },
                now + Duration::minutes(4),
            ),
            Err(TaskError::DeadlineNotReached)
        );
    }

    #[test]
    fn external_authorization_resolves_without_persisting_a_response_value() {
        let now = DateTime::parse_from_rfc3339("2026-08-09T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let pending = oauth_pending(now);
        pending.validate().unwrap();
        let completed = decide_resolution(
            &pending,
            ResolveTask {
                expected_generation: 1,
                expected_version: 1,
                target: TaskState::Responded,
                principal: Some(principal()),
                response_value_id: None,
                response_schema_digest: None,
            },
            now + Duration::seconds(1),
        )
        .unwrap();
        assert_eq!(completed.state, TaskState::Responded);
        assert!(completed.response_value_id.is_none());
    }
}
