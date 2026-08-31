use crate::authentication::AuthenticatedPrincipal;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use insight_platform_contracts::{
    canonical_digest, AgentProductState, AgentRequiredFeature, OpaqueListCursor, ResourceId,
    ResourceKind, RunState, Sha256Digest, UtcTimestamp,
};
use ring::hmac;
use serde::{Deserialize, Serialize};

pub const PRODUCT_LIST_MAX_PAGE_SIZE: u16 = 50;
pub const PRODUCT_LIST_DEFAULT_PAGE_SIZE: u16 = 25;
pub const PRODUCT_LIST_CURSOR_TTL_SECONDS: i64 = 900;

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
    pub fn validate_at(&self, snapshot_at: DateTime<Utc>) -> Result<(), ListError> {
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
        if before.is_some_and(|value| value > snapshot_at)
            || matches!((after, before), (Some(after), Some(before)) if after >= before)
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
    Agents,
    Runs,
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
        matches!(
            (purpose, self),
            (
                ListRoutePurpose::Agents,
                Self::Agent { agent_id, .. }
            ) if agent_id.kind() == ResourceKind::Agent
        ) || matches!(
            (purpose, self),
            (ListRoutePurpose::Runs, Self::Run { run_id, .. })
                if run_id.kind() == ResourceKind::Run
        )
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
        if principal.validate().is_err()
            || !boundary.validates_for(context.purpose)
            || expires_at <= snapshot_at
        {
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
            ListKeysetBoundary::Run { created_at, .. } => created_at,
        };
        let boundary_at = DateTime::parse_from_rfc3339(boundary_at.as_str())
            .map_err(|_| ListError::Invalid)?
            .with_timezone(&Utc);
        if snapshot_at > now || boundary_at > snapshot_at {
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

    #[test]
    fn cursor_binds_route_tenant_principal_filter_page_snapshot_and_expiry() {
        let codec = HmacListCursorCodec::install(&[7_u8; 32]).unwrap();
        let now = Utc::now();
        let context = ListCursorContext {
            purpose: ListRoutePurpose::Agents,
            filter_digest: digest('b'),
            page_size: 25,
        };
        let boundary = ListKeysetBoundary::Agent {
            updated_at: UtcTimestamp::from_datetime(now - chrono::Duration::seconds(1)),
            agent_id: id(ResourceKind::Agent, 3),
        };
        let cursor = codec
            .encode(
                &principal(),
                &context,
                now,
                boundary.clone(),
                now + chrono::Duration::minutes(15),
            )
            .unwrap();
        assert_eq!(
            codec.decode(cursor.as_str(), &principal(), &context, now),
            Ok(DecodedListCursor {
                snapshot_at: now,
                boundary,
                expires_at: DateTime::from_timestamp(
                    (now + chrono::Duration::minutes(15)).timestamp(),
                    0,
                )
                .unwrap(),
            })
        );

        let mut wrong_context = context.clone();
        wrong_context.purpose = ListRoutePurpose::Runs;
        assert_eq!(
            codec.decode(cursor.as_str(), &principal(), &wrong_context, now),
            Err(ListError::Invalid)
        );
        wrong_context = context.clone();
        wrong_context.filter_digest = digest('c');
        assert_eq!(
            codec.decode(cursor.as_str(), &principal(), &wrong_context, now),
            Err(ListError::Invalid)
        );
        wrong_context = context.clone();
        wrong_context.page_size = 24;
        assert_eq!(
            codec.decode(cursor.as_str(), &principal(), &wrong_context, now),
            Err(ListError::Invalid)
        );

        let mut wrong_principal = principal();
        wrong_principal.binding_version += 1;
        assert_eq!(
            codec.decode(cursor.as_str(), &wrong_principal, &context, now),
            Err(ListError::Invalid)
        );
        assert_eq!(
            codec.decode(
                cursor.as_str(),
                &principal(),
                &context,
                now + chrono::Duration::minutes(15),
            ),
            Err(ListError::Expired)
        );
    }

    #[test]
    fn product_summaries_and_pages_are_closed_and_bounded() {
        let summary = AgentSummaryV1 {
            schema_version: 1,
            name: "support-agent".to_owned(),
            display_name: "Support Agent".to_owned(),
            agent_id: id(ResourceKind::Agent, 1),
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
