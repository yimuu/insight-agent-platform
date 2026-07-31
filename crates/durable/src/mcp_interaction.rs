//! Backend-neutral durable MCP interaction authority.

use std::collections::BTreeSet;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_engine::{schema::compile_schema_2020, TransitionOutcome};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{RepositoryError, RepositoryErrorExt as _};

const MAX_LABEL_BYTES: usize = 256;
const MAX_MESSAGE_BYTES: usize = 4 * 1024;
const MAX_SCHEMA_BYTES: usize = 64 * 1024;
const MAX_CIPHERTEXT_BYTES: usize = 256 * 1024;
const MAX_FORM_FIELDS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct McpInteractionId(String);

impl McpInteractionId {
    pub fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        validate_label(&value)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpInteractionPrincipal {
    tenant_id: String,
    user_id: String,
}

impl McpInteractionPrincipal {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        let tenant_id = tenant_id.into();
        let user_id = user_id.into();
        validate_label(&tenant_id)?;
        validate_label(&user_id)?;
        Ok(Self { tenant_id, user_id })
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    pub fn user_id(&self) -> &str {
        &self.user_id
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpInteractionRequest {
    Form {
        message: String,
        requested_schema: Value,
    },
    Url {
        message: String,
        scheme: String,
        host: String,
        port: Option<u16>,
    },
    Approval {
        message: String,
        effect: String,
    },
    Authorization {
        message: String,
        required_scopes: Vec<String>,
        step_up: bool,
    },
}

impl McpInteractionRequest {
    pub fn validate(&self) -> Result<(), RepositoryError> {
        match self {
            Self::Form {
                message,
                requested_schema,
            } => {
                validate_message(message)?;
                validate_form_schema(requested_schema)
            }
            Self::Url {
                message,
                scheme,
                host,
                port: _,
            } => {
                validate_message(message)?;
                if scheme != "https"
                    || host.is_empty()
                    || host.len() > 253
                    || !host.is_ascii()
                    || host.chars().any(char::is_control)
                    || host.contains('@')
                    || host.contains('/')
                    || host.contains('?')
                    || host.contains('#')
                {
                    return Err(RepositoryError::invalid_data());
                }
                Ok(())
            }
            Self::Approval { message, effect } => {
                validate_message(message)?;
                if !matches!(effect.as_str(), "pure" | "read_only" | "mutating") {
                    return Err(RepositoryError::invalid_data());
                }
                Ok(())
            }
            Self::Authorization {
                message,
                required_scopes,
                step_up: _,
            } => {
                validate_message(message)?;
                if required_scopes.is_empty()
                    || required_scopes.len() > 128
                    || required_scopes.windows(2).any(|pair| pair[0] >= pair[1])
                    || required_scopes.iter().any(|scope| {
                        scope.is_empty()
                            || scope.len() > 256
                            || !scope.is_ascii()
                            || !scope
                                .bytes()
                                .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
                    })
                {
                    return Err(RepositoryError::invalid_data());
                }
                Ok(())
            }
        }
    }

    pub fn mode(&self) -> McpInteractionMode {
        match self {
            Self::Form { .. } => McpInteractionMode::Form,
            Self::Url { .. } => McpInteractionMode::Url,
            Self::Approval { .. } => McpInteractionMode::Approval,
            Self::Authorization { .. } => McpInteractionMode::Authorization,
        }
    }

    pub fn validate_response(&self, response: &Value) -> Result<(), RepositoryError> {
        match self {
            Self::Form {
                requested_schema, ..
            } => {
                let validator = compile_schema_2020(requested_schema)
                    .map_err(|_| RepositoryError::invalid_data())?;
                if !validator.is_valid(response) {
                    return Err(RepositoryError::invalid_data());
                }
                Ok(())
            }
            Self::Url { .. } | Self::Approval { .. } => {
                if response != &Value::Object(serde_json::Map::new()) {
                    return Err(RepositoryError::invalid_data());
                }
                Ok(())
            }
            // OAuth authorization is completed only by the state-bound
            // callback and credential-generation authority. A generic
            // interaction response must never be able to assert success.
            Self::Authorization { .. } => Err(RepositoryError::invalid_data()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpInteractionMode {
    Form,
    Url,
    Approval,
    Authorization,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpInteractionState {
    Requested,
    Responded,
    Retrying,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpInteractionOutcome {
    Accepted,
    Declined,
    Cancelled,
    Expired,
    RunTerminal,
    RetryCompleted,
    RetryFailed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpInteraction {
    interaction_id: McpInteractionId,
    principal: McpInteractionPrincipal,
    run_id: String,
    operation_id: String,
    server_id: String,
    binding_hash: String,
    logical_request_key: String,
    generation: u32,
    request: McpInteractionRequest,
    state: McpInteractionState,
    outcome: Option<McpInteractionOutcome>,
    version: u64,
    deadline: DateTime<Utc>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    closed_at: Option<DateTime<Utc>>,
}

impl McpInteraction {
    pub fn interaction_id(&self) -> &McpInteractionId {
        &self.interaction_id
    }

    pub fn principal(&self) -> &McpInteractionPrincipal {
        &self.principal
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    pub fn server_id(&self) -> &str {
        &self.server_id
    }

    pub fn binding_hash(&self) -> &str {
        &self.binding_hash
    }

    pub fn logical_request_key(&self) -> &str {
        &self.logical_request_key
    }

    pub fn generation(&self) -> u32 {
        self.generation
    }

    pub fn request(&self) -> &McpInteractionRequest {
        &self.request
    }

    pub fn state(&self) -> McpInteractionState {
        self.state
    }

    pub fn outcome(&self) -> Option<McpInteractionOutcome> {
        self.outcome
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn deadline(&self) -> DateTime<Utc> {
        self.deadline
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    pub fn closed_at(&self) -> Option<DateTime<Utc>> {
        self.closed_at
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct McpSecretCiphertext(String);

impl McpSecretCiphertext {
    pub fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        if !value.starts_with("enc:v1:")
            || value.len() <= "enc:v1:".len()
            || value.len() > MAX_CIPHERTEXT_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self(value))
    }

    pub fn expose_ciphertext(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for McpSecretCiphertext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("McpSecretCiphertext(REDACTED)")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpSecretPurpose {
    ElicitationRequest,
    ElicitationResponse,
    OauthTransaction,
    AccessToken,
    RefreshToken,
    RemoteTaskId,
    RemoteTaskPayload,
    ServerContinuation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpSecretScope {
    tenant_id: String,
    user_id: String,
    server_id: String,
    authority_id: String,
    purpose: McpSecretPurpose,
}

impl McpSecretScope {
    pub fn new(
        principal: &McpInteractionPrincipal,
        server_id: impl Into<String>,
        authority_id: impl Into<String>,
        purpose: McpSecretPurpose,
    ) -> Result<Self, RepositoryError> {
        let server_id = server_id.into();
        let authority_id = authority_id.into();
        validate_label(&server_id)?;
        validate_label(&authority_id)?;
        Ok(Self {
            tenant_id: principal.tenant_id.clone(),
            user_id: principal.user_id.clone(),
            server_id,
            authority_id,
            purpose,
        })
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpProtectedSecret {
    ciphertext: McpSecretCiphertext,
    content_hash: String,
}

impl McpProtectedSecret {
    pub fn new(
        ciphertext: McpSecretCiphertext,
        content_hash: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        let content_hash = content_hash.into();
        validate_lower_sha256(&content_hash)?;
        Ok(Self {
            ciphertext,
            content_hash,
        })
    }

    pub fn ciphertext(&self) -> &McpSecretCiphertext {
        &self.ciphertext
    }

    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    pub fn into_parts(self) -> (McpSecretCiphertext, String) {
        (self.ciphertext, self.content_hash)
    }
}

pub trait McpSecretProtector: Send + Sync {
    fn seal(
        &self,
        scope: &McpSecretScope,
        plaintext: &[u8],
    ) -> Result<McpProtectedSecret, RepositoryError>;

    fn open(
        &self,
        scope: &McpSecretScope,
        protected: &McpProtectedSecret,
    ) -> Result<Vec<u8>, RepositoryError>;
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CreateMcpInteractionCommand {
    interaction: McpInteraction,
    request_secret: McpSecretCiphertext,
    request_secret_hash: String,
}

impl CreateMcpInteractionCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        interaction_id: McpInteractionId,
        principal: McpInteractionPrincipal,
        run_id: impl Into<String>,
        operation_id: impl Into<String>,
        server_id: impl Into<String>,
        binding_hash: impl Into<String>,
        logical_request_key: impl Into<String>,
        generation: u32,
        request: McpInteractionRequest,
        request_secret: McpSecretCiphertext,
        request_secret_hash: impl Into<String>,
        deadline: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let run_id = run_id.into();
        let operation_id = operation_id.into();
        let server_id = server_id.into();
        let binding_hash = binding_hash.into();
        let logical_request_key = logical_request_key.into();
        let request_secret_hash = request_secret_hash.into();
        for value in [&run_id, &operation_id, &server_id, &logical_request_key] {
            validate_label(value)?;
        }
        validate_lower_sha256(&binding_hash)?;
        validate_lower_sha256(&request_secret_hash)?;
        request.validate()?;
        if generation == 0 || deadline <= now {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            interaction: McpInteraction {
                interaction_id,
                principal,
                run_id,
                operation_id,
                server_id,
                binding_hash,
                logical_request_key,
                generation,
                request,
                state: McpInteractionState::Requested,
                outcome: None,
                version: 1,
                deadline,
                created_at: now,
                updated_at: now,
                closed_at: None,
            },
            request_secret,
            request_secret_hash,
        })
    }

    pub fn interaction(&self) -> &McpInteraction {
        &self.interaction
    }

    pub fn request_secret(&self) -> &McpSecretCiphertext {
        &self.request_secret
    }

    pub fn request_secret_hash(&self) -> &str {
        &self.request_secret_hash
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpInteractionDisposition {
    Accept,
    Decline,
    Cancel,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResolveMcpInteractionCommand {
    interaction_id: McpInteractionId,
    principal: McpInteractionPrincipal,
    request_id: String,
    expected_version: u64,
    disposition: McpInteractionDisposition,
    response_secret: Option<McpSecretCiphertext>,
    response_hash: Option<String>,
    responded_at: DateTime<Utc>,
}

impl ResolveMcpInteractionCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        interaction: &McpInteraction,
        principal: McpInteractionPrincipal,
        request_id: impl Into<String>,
        expected_version: u64,
        disposition: McpInteractionDisposition,
        response: Option<&Value>,
        response_secret: Option<McpSecretCiphertext>,
        response_hash: Option<String>,
        responded_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let request_id = request_id.into();
        validate_label(&request_id)?;
        if expected_version == 0 || responded_at > interaction.deadline {
            return Err(RepositoryError::invalid_data());
        }
        match disposition {
            McpInteractionDisposition::Accept => {
                let response = response.ok_or_else(RepositoryError::invalid_data)?;
                interaction.request.validate_response(response)?;
                if response_secret.is_none()
                    || response_hash
                        .as_ref()
                        .is_none_or(|hash| validate_lower_sha256(hash).is_err())
                {
                    return Err(RepositoryError::invalid_data());
                }
            }
            McpInteractionDisposition::Decline | McpInteractionDisposition::Cancel => {
                if response.is_some() || response_secret.is_some() || response_hash.is_some() {
                    return Err(RepositoryError::invalid_data());
                }
            }
        }
        Ok(Self {
            interaction_id: interaction.interaction_id.clone(),
            principal,
            request_id,
            expected_version,
            disposition,
            response_secret,
            response_hash,
            responded_at,
        })
    }

    pub fn interaction_id(&self) -> &McpInteractionId {
        &self.interaction_id
    }

    pub fn principal(&self) -> &McpInteractionPrincipal {
        &self.principal
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn expected_version(&self) -> u64 {
        self.expected_version
    }

    pub fn disposition(&self) -> McpInteractionDisposition {
        self.disposition
    }

    pub fn response_secret(&self) -> Option<&McpSecretCiphertext> {
        self.response_secret.as_ref()
    }

    pub fn response_hash(&self) -> Option<&str> {
        self.response_hash.as_deref()
    }

    pub fn responded_at(&self) -> DateTime<Utc> {
        self.responded_at
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TransitionMcpInteractionCommand {
    interaction_id: McpInteractionId,
    request_id: String,
    expected_version: u64,
    outcome: Option<McpInteractionOutcome>,
    transitioned_at: DateTime<Utc>,
}

impl TransitionMcpInteractionCommand {
    pub fn begin_retry(
        interaction_id: McpInteractionId,
        request_id: impl Into<String>,
        expected_version: u64,
        transitioned_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        Self::new(
            interaction_id,
            request_id,
            expected_version,
            None,
            transitioned_at,
        )
    }

    pub fn close(
        interaction_id: McpInteractionId,
        request_id: impl Into<String>,
        expected_version: u64,
        outcome: McpInteractionOutcome,
        transitioned_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        if outcome == McpInteractionOutcome::Accepted {
            return Err(RepositoryError::invalid_data());
        }
        Self::new(
            interaction_id,
            request_id,
            expected_version,
            Some(outcome),
            transitioned_at,
        )
    }

    fn new(
        interaction_id: McpInteractionId,
        request_id: impl Into<String>,
        expected_version: u64,
        outcome: Option<McpInteractionOutcome>,
        transitioned_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let request_id = request_id.into();
        validate_label(&request_id)?;
        if expected_version == 0 {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            interaction_id,
            request_id,
            expected_version,
            outcome,
            transitioned_at,
        })
    }

    pub fn interaction_id(&self) -> &McpInteractionId {
        &self.interaction_id
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub fn expected_version(&self) -> u64 {
        self.expected_version
    }

    pub fn outcome(&self) -> Option<McpInteractionOutcome> {
        self.outcome
    }

    pub fn transitioned_at(&self) -> DateTime<Utc> {
        self.transitioned_at
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct McpInteractionSecretAuthority {
    pub request_secret: McpSecretCiphertext,
    pub request_hash: String,
    pub response_secret: Option<McpSecretCiphertext>,
    pub response_hash: Option<String>,
}

impl std::fmt::Debug for McpInteractionSecretAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("McpInteractionSecretAuthority")
            .field("request_secret", &"REDACTED")
            .field("request_hash", &self.request_hash)
            .field(
                "response_secret",
                &self.response_secret.as_ref().map(|_| "REDACTED"),
            )
            .field("response_hash", &self.response_hash)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct McpInteractionListFilter {
    pub run_id: Option<String>,
    pub state: Option<McpInteractionState>,
    pub after_interaction_id: Option<String>,
}

#[async_trait]
pub trait McpInteractionDurableRepository: Send + Sync {
    async fn load_mcp_run_principal(
        &self,
        run_id: &str,
    ) -> Result<Option<McpInteractionPrincipal>, RepositoryError>;

    async fn create_mcp_interaction(
        &self,
        command: CreateMcpInteractionCommand,
    ) -> Result<TransitionOutcome<McpInteraction>, RepositoryError>;

    async fn load_mcp_interaction(
        &self,
        interaction_id: &McpInteractionId,
    ) -> Result<Option<McpInteraction>, RepositoryError>;

    async fn list_mcp_interactions(
        &self,
        principal: &McpInteractionPrincipal,
        filter: &McpInteractionListFilter,
        limit: u32,
    ) -> Result<Vec<McpInteraction>, RepositoryError>;

    async fn resolve_mcp_interaction(
        &self,
        command: ResolveMcpInteractionCommand,
    ) -> Result<TransitionOutcome<McpInteraction>, RepositoryError>;

    async fn transition_mcp_interaction(
        &self,
        command: TransitionMcpInteractionCommand,
    ) -> Result<TransitionOutcome<McpInteraction>, RepositoryError>;

    async fn load_mcp_interaction_secret(
        &self,
        interaction_id: &McpInteractionId,
    ) -> Result<Option<McpInteractionSecretAuthority>, RepositoryError>;

    async fn list_mcp_interactions_ready_for_retry(
        &self,
        limit: u32,
    ) -> Result<Vec<McpInteraction>, RepositoryError>;
}

#[doc(hidden)]
pub mod adapter {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    pub fn interaction_from_storage(
        interaction_id: McpInteractionId,
        principal: McpInteractionPrincipal,
        run_id: String,
        operation_id: String,
        server_id: String,
        binding_hash: String,
        logical_request_key: String,
        generation: u32,
        request: McpInteractionRequest,
        state: McpInteractionState,
        outcome: Option<McpInteractionOutcome>,
        version: u64,
        deadline: DateTime<Utc>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
        closed_at: Option<DateTime<Utc>>,
    ) -> McpInteraction {
        McpInteraction {
            interaction_id,
            principal,
            run_id,
            operation_id,
            server_id,
            binding_hash,
            logical_request_key,
            generation,
            request,
            state,
            outcome,
            version,
            deadline,
            created_at,
            updated_at,
            closed_at,
        }
    }
}

fn validate_form_schema(schema: &Value) -> Result<(), RepositoryError> {
    if serde_jcs::to_vec(schema)
        .map_err(|_| RepositoryError::canonicalization())?
        .len()
        > MAX_SCHEMA_BYTES
        || schema.get("type").and_then(Value::as_str) != Some("object")
        || schema.get("additionalProperties").and_then(Value::as_bool) != Some(false)
        || contains_forbidden_schema_keyword(schema)
    {
        return Err(RepositoryError::invalid_data());
    }
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .ok_or_else(RepositoryError::invalid_data)?;
    if properties.is_empty() || properties.len() > MAX_FORM_FIELDS {
        return Err(RepositoryError::invalid_data());
    }
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_owned)
                        .ok_or_else(RepositoryError::invalid_data)
                })
                .collect::<Result<BTreeSet<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    if required.iter().any(|field| !properties.contains_key(field)) {
        return Err(RepositoryError::invalid_data());
    }
    for (name, property) in properties {
        if forbidden_secret_field(name) || !valid_form_property(property) {
            return Err(RepositoryError::invalid_data());
        }
    }
    compile_schema_2020(schema).map_err(|_| RepositoryError::invalid_data())?;
    Ok(())
}

fn valid_form_property(property: &Value) -> bool {
    let Some(object) = property.as_object() else {
        return false;
    };
    let allowed = BTreeSet::from([
        "type",
        "title",
        "description",
        "default",
        "enum",
        "items",
        "minLength",
        "maxLength",
        "minimum",
        "maximum",
        "multipleOf",
        "format",
        "minItems",
        "maxItems",
        "uniqueItems",
    ]);
    if object.keys().any(|key| !allowed.contains(key.as_str())) {
        return false;
    }
    match object.get("type").and_then(Value::as_str) {
        Some("string") => {
            object.get("format").is_none_or(|format| {
                format
                    .as_str()
                    .is_some_and(|value| matches!(value, "email" | "uri" | "date" | "date-time"))
            }) && primitive_enum(object.get("enum"), false)
        }
        Some("number") | Some("integer") | Some("boolean") => {
            object.get("format").is_none() && primitive_enum(object.get("enum"), false)
        }
        Some("array") => {
            object.get("uniqueItems").and_then(Value::as_bool) == Some(true)
                && object
                    .get("items")
                    .and_then(Value::as_object)
                    .is_some_and(|items| {
                        items.len() == 1 && primitive_enum(items.get("enum"), true)
                    })
        }
        _ => false,
    }
}

fn primitive_enum(value: Option<&Value>, required: bool) -> bool {
    match value {
        None => !required,
        Some(Value::Array(values)) => {
            !values.is_empty()
                && values.len() <= 128
                && values.iter().all(|value| {
                    matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_))
                })
        }
        Some(_) => false,
    }
}

fn contains_forbidden_schema_keyword(value: &Value) -> bool {
    match value {
        Value::Array(values) => values.iter().any(contains_forbidden_schema_keyword),
        Value::Object(values) => {
            values.keys().any(|key| {
                matches!(
                    key.as_str(),
                    "$ref"
                        | "$dynamicRef"
                        | "$recursiveRef"
                        | "allOf"
                        | "anyOf"
                        | "oneOf"
                        | "not"
                        | "if"
                        | "then"
                        | "else"
                        | "pattern"
                        | "patternProperties"
                        | "dependentSchemas"
                )
            }) || values.values().any(contains_forbidden_schema_keyword)
        }
        _ => false,
    }
}

fn forbidden_secret_field(value: &str) -> bool {
    let normalized = value.to_ascii_lowercase().replace('-', "_");
    [
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "payment",
        "credit_card",
        "card_number",
        "cvv",
    ]
    .iter()
    .any(|needle| normalized.contains(needle))
}

fn validate_message(value: &str) -> Result<(), RepositoryError> {
    if value.is_empty()
        || value.len() > MAX_MESSAGE_BYTES
        || value
            .chars()
            .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

fn validate_label(value: &str) -> Result<(), RepositoryError> {
    if value.is_empty()
        || value.len() > MAX_LABEL_BYTES
        || value
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

fn validate_lower_sha256(value: &str) -> Result<(), RepositoryError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RepositoryError::invalid_data());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Duration;
    use serde_json::json;

    use super::*;

    fn form() -> McpInteractionRequest {
        McpInteractionRequest::Form {
            message: "Choose a repository".to_owned(),
            requested_schema: json!({
                "$schema":"https://json-schema.org/draft/2020-12/schema",
                "type":"object",
                "properties":{
                    "repository":{"type":"string","enum":["one","two"]},
                    "depth":{"type":"integer","minimum":1,"maximum":10}
                },
                "required":["repository"],
                "additionalProperties":false
            }),
        }
    }

    #[test]
    fn form_profile_rejects_nested_and_secret_fields() {
        assert!(form().validate().is_ok());
        let nested = McpInteractionRequest::Form {
            message: "Nested".to_owned(),
            requested_schema: json!({
                "type":"object",
                "properties":{"nested":{"type":"object"}},
                "required":[],
                "additionalProperties":false
            }),
        };
        assert!(nested.validate().is_err());
        let secret = McpInteractionRequest::Form {
            message: "Secret".to_owned(),
            requested_schema: json!({
                "type":"object",
                "properties":{"api_token":{"type":"string"}},
                "required":[],
                "additionalProperties":false
            }),
        };
        assert!(secret.validate().is_err());
    }

    #[test]
    fn authorization_interaction_is_scope_bounded_and_cannot_self_assert_success() {
        let request = McpInteractionRequest::Authorization {
            message: "Authorize the calendar server".to_owned(),
            required_scopes: vec!["calendar.read".to_owned(), "calendar.write".to_owned()],
            step_up: true,
        };
        assert!(request.validate().is_ok());
        assert_eq!(request.mode(), McpInteractionMode::Authorization);
        assert!(request
            .validate_response(&Value::Object(serde_json::Map::new()))
            .is_err());

        let duplicated = McpInteractionRequest::Authorization {
            message: "Authorize".to_owned(),
            required_scopes: vec!["calendar.read".to_owned(), "calendar.read".to_owned()],
            step_up: false,
        };
        assert!(duplicated.validate().is_err());
    }

    #[test]
    fn commands_keep_ciphertext_redacted_and_validate_response() {
        let now = DateTime::parse_from_rfc3339("2026-07-30T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let command = CreateMcpInteractionCommand::new(
            McpInteractionId::new("mcpint_1").unwrap(),
            McpInteractionPrincipal::new("tenant-a", "user-a").unwrap(),
            "run-a",
            "operation-a",
            "engineering",
            "a".repeat(64),
            "run-a:operation-a:1:tools/call",
            1,
            form(),
            McpSecretCiphertext::new("enc:v1:opaque-request").unwrap(),
            "b".repeat(64),
            now + Duration::minutes(5),
            now,
        )
        .unwrap();
        let interaction = command.interaction();
        let response = json!({"repository":"one","depth":2});
        let resolve = ResolveMcpInteractionCommand::new(
            interaction,
            interaction.principal().clone(),
            "request-1",
            1,
            McpInteractionDisposition::Accept,
            Some(&response),
            Some(McpSecretCiphertext::new("enc:v1:opaque-response").unwrap()),
            Some("c".repeat(64)),
            now + Duration::seconds(1),
        )
        .unwrap();
        assert_eq!(resolve.expected_version(), 1);
        assert!(!format!("{command:?}").contains("opaque-request"));
    }
}
