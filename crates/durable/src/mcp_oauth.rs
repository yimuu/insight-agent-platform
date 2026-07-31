//! Backend-neutral durable authority for MCP OAuth transactions and credentials.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use insight_engine::TransitionOutcome;
use serde::{Deserialize, Serialize};

use super::{
    McpInteractionPrincipal, McpSecretCiphertext, RepositoryError, RepositoryErrorExt as _,
};

const MAX_LABEL_BYTES: usize = 1024;
const MAX_SCOPE_COUNT: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct McpOAuthTransactionId(String);

impl McpOAuthTransactionId {
    pub fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        validate_label(&value, 256)?;
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpOAuthTransactionState {
    Pending,
    Consumed,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthTransaction {
    transaction_id: McpOAuthTransactionId,
    principal: McpInteractionPrincipal,
    server_id: String,
    issuer: String,
    resource: String,
    client_id: String,
    redirect_uri: String,
    scopes: Vec<String>,
    state_hash: String,
    state: McpOAuthTransactionState,
    version: u64,
    expires_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    consumed_at: Option<DateTime<Utc>>,
}

impl McpOAuthTransaction {
    pub fn transaction_id(&self) -> &McpOAuthTransactionId {
        &self.transaction_id
    }
    pub fn principal(&self) -> &McpInteractionPrincipal {
        &self.principal
    }
    pub fn server_id(&self) -> &str {
        &self.server_id
    }
    pub fn issuer(&self) -> &str {
        &self.issuer
    }
    pub fn resource(&self) -> &str {
        &self.resource
    }
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    pub fn redirect_uri(&self) -> &str {
        &self.redirect_uri
    }
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }
    pub fn state_hash(&self) -> &str {
        &self.state_hash
    }
    pub fn state(&self) -> McpOAuthTransactionState {
        self.state
    }
    pub fn version(&self) -> u64 {
        self.version
    }
    pub fn expires_at(&self) -> DateTime<Utc> {
        self.expires_at
    }
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }
    pub fn consumed_at(&self) -> Option<DateTime<Utc>> {
        self.consumed_at
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CreateMcpOAuthTransactionCommand {
    transaction: McpOAuthTransaction,
    transaction_secret: McpSecretCiphertext,
    transaction_secret_hash: String,
}

impl CreateMcpOAuthTransactionCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        transaction_id: McpOAuthTransactionId,
        principal: McpInteractionPrincipal,
        server_id: impl Into<String>,
        issuer: impl Into<String>,
        resource: impl Into<String>,
        client_id: impl Into<String>,
        redirect_uri: impl Into<String>,
        mut scopes: Vec<String>,
        state_hash: impl Into<String>,
        transaction_secret: McpSecretCiphertext,
        transaction_secret_hash: impl Into<String>,
        expires_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let server_id = server_id.into();
        let issuer = issuer.into();
        let resource = resource.into();
        let client_id = client_id.into();
        let redirect_uri = redirect_uri.into();
        let state_hash = state_hash.into();
        let transaction_secret_hash = transaction_secret_hash.into();
        for value in [&server_id, &issuer, &resource, &client_id, &redirect_uri] {
            validate_label(value, MAX_LABEL_BYTES)?;
        }
        scopes.sort();
        scopes.dedup();
        validate_scopes(&scopes)?;
        validate_hash(&state_hash)?;
        validate_hash(&transaction_secret_hash)?;
        if expires_at <= now {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            transaction: McpOAuthTransaction {
                transaction_id,
                principal,
                server_id,
                issuer,
                resource,
                client_id,
                redirect_uri,
                scopes,
                state_hash,
                state: McpOAuthTransactionState::Pending,
                version: 1,
                expires_at,
                created_at: now,
                consumed_at: None,
            },
            transaction_secret,
            transaction_secret_hash,
        })
    }

    pub fn transaction(&self) -> &McpOAuthTransaction {
        &self.transaction
    }
    pub fn transaction_secret(&self) -> &McpSecretCiphertext {
        &self.transaction_secret
    }
    pub fn transaction_secret_hash(&self) -> &str {
        &self.transaction_secret_hash
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConsumeMcpOAuthTransactionCommand {
    transaction_id: McpOAuthTransactionId,
    principal: McpInteractionPrincipal,
    request_id: String,
    expected_version: u64,
    callback_state_hash: String,
    consumed_at: DateTime<Utc>,
}

impl ConsumeMcpOAuthTransactionCommand {
    pub fn new(
        transaction_id: McpOAuthTransactionId,
        principal: McpInteractionPrincipal,
        request_id: impl Into<String>,
        expected_version: u64,
        callback_state_hash: impl Into<String>,
        consumed_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let request_id = request_id.into();
        let callback_state_hash = callback_state_hash.into();
        validate_label(&request_id, 256)?;
        validate_hash(&callback_state_hash)?;
        if expected_version == 0 {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            transaction_id,
            principal,
            request_id,
            expected_version,
            callback_state_hash,
            consumed_at,
        })
    }
    pub fn transaction_id(&self) -> &McpOAuthTransactionId {
        &self.transaction_id
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
    pub fn callback_state_hash(&self) -> &str {
        &self.callback_state_hash
    }
    pub fn consumed_at(&self) -> DateTime<Utc> {
        self.consumed_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpOAuthCredential {
    principal: McpInteractionPrincipal,
    server_id: String,
    issuer: String,
    client_id: String,
    resource: String,
    scopes: Vec<String>,
    token_type: String,
    generation: u64,
    access_expires_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
}

impl McpOAuthCredential {
    pub fn principal(&self) -> &McpInteractionPrincipal {
        &self.principal
    }
    pub fn server_id(&self) -> &str {
        &self.server_id
    }
    pub fn issuer(&self) -> &str {
        &self.issuer
    }
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    pub fn resource(&self) -> &str {
        &self.resource
    }
    pub fn scopes(&self) -> &[String] {
        &self.scopes
    }
    pub fn token_type(&self) -> &str {
        &self.token_type
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn access_expires_at(&self) -> Option<DateTime<Utc>> {
        self.access_expires_at
    }
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }
    pub fn revoked_at(&self) -> Option<DateTime<Utc>> {
        self.revoked_at
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StoreMcpOAuthCredentialCommand {
    credential: McpOAuthCredential,
    expected_generation: Option<u64>,
    refresh_lease_owner: Option<String>,
    request_id: String,
    access_token: McpSecretCiphertext,
    access_token_hash: String,
    refresh_token: Option<McpSecretCiphertext>,
    refresh_token_hash: Option<String>,
}

impl StoreMcpOAuthCredentialCommand {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        principal: McpInteractionPrincipal,
        server_id: impl Into<String>,
        issuer: impl Into<String>,
        client_id: impl Into<String>,
        resource: impl Into<String>,
        mut scopes: Vec<String>,
        token_type: impl Into<String>,
        generation: u64,
        expected_generation: Option<u64>,
        request_id: impl Into<String>,
        access_token: McpSecretCiphertext,
        access_token_hash: impl Into<String>,
        refresh_token: Option<McpSecretCiphertext>,
        refresh_token_hash: Option<String>,
        access_expires_at: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let server_id = server_id.into();
        let issuer = issuer.into();
        let client_id = client_id.into();
        let resource = resource.into();
        let token_type = token_type.into();
        let request_id = request_id.into();
        let access_token_hash = access_token_hash.into();
        for value in [&server_id, &issuer, &client_id, &resource] {
            validate_label(value, MAX_LABEL_BYTES)?;
        }
        validate_label(&request_id, 256)?;
        scopes.sort();
        scopes.dedup();
        validate_scopes(&scopes)?;
        validate_hash(&access_token_hash)?;
        if token_type != "Bearer"
            || generation == 0
            || expected_generation.is_some_and(|value| value == 0)
            || refresh_token.is_some() != refresh_token_hash.is_some()
            || refresh_token_hash
                .as_ref()
                .is_some_and(|hash| validate_hash(hash).is_err())
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            credential: McpOAuthCredential {
                principal,
                server_id,
                issuer,
                client_id,
                resource,
                scopes,
                token_type,
                generation,
                access_expires_at,
                updated_at: now,
                revoked_at: None,
            },
            expected_generation,
            refresh_lease_owner: None,
            request_id,
            access_token,
            access_token_hash,
            refresh_token,
            refresh_token_hash,
        })
    }

    pub fn credential(&self) -> &McpOAuthCredential {
        &self.credential
    }
    pub fn expected_generation(&self) -> Option<u64> {
        self.expected_generation
    }
    /// Fences a credential rotation to the refresh request that produced it.
    ///
    /// Authorization-code exchanges intentionally leave this unset. Refresh
    /// callers must set it after claiming and durably dispatching the matching
    /// refresh lease.
    pub fn with_refresh_lease_owner(
        mut self,
        owner: impl Into<String>,
    ) -> Result<Self, RepositoryError> {
        let owner = owner.into();
        validate_label(&owner, 256)?;
        if self.expected_generation.is_none() {
            return Err(RepositoryError::invalid_data());
        }
        self.refresh_lease_owner = Some(owner);
        Ok(self)
    }
    pub fn refresh_lease_owner(&self) -> Option<&str> {
        self.refresh_lease_owner.as_deref()
    }
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn access_token(&self) -> &McpSecretCiphertext {
        &self.access_token
    }
    pub fn access_token_hash(&self) -> &str {
        &self.access_token_hash
    }
    pub fn refresh_token(&self) -> Option<&McpSecretCiphertext> {
        self.refresh_token.as_ref()
    }
    pub fn refresh_token_hash(&self) -> Option<&str> {
        self.refresh_token_hash.as_deref()
    }
}

/// Atomically installs the credential returned by a successful authorization
/// code exchange and consumes the single-use callback transaction.
///
/// The token endpoint call necessarily happens before this command. Keeping
/// both local authority changes in one repository transaction prevents a
/// callback from becoming consumed without its credential, or a credential
/// from becoming visible while the callback remains replayable.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CompleteMcpOAuthCallbackCommand {
    consume: ConsumeMcpOAuthTransactionCommand,
    credential: StoreMcpOAuthCredentialCommand,
}

impl CompleteMcpOAuthCallbackCommand {
    pub fn new(
        consume: ConsumeMcpOAuthTransactionCommand,
        credential: StoreMcpOAuthCredentialCommand,
    ) -> Result<Self, RepositoryError> {
        if consume.principal() != credential.credential().principal()
            || consume.consumed_at() != credential.credential().updated_at()
        {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            consume,
            credential,
        })
    }

    pub fn consume(&self) -> &ConsumeMcpOAuthTransactionCommand {
        &self.consume
    }

    pub fn credential(&self) -> &StoreMcpOAuthCredentialCommand {
        &self.credential
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthCallbackCompletion {
    pub transaction: McpOAuthTransaction,
    pub credential: McpOAuthCredential,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClaimMcpOAuthRefreshCommand {
    principal: McpInteractionPrincipal,
    server_id: String,
    expected_generation: u64,
    owner: String,
    now: DateTime<Utc>,
    lease_expires_at: DateTime<Utc>,
}

impl ClaimMcpOAuthRefreshCommand {
    pub fn new(
        principal: McpInteractionPrincipal,
        server_id: impl Into<String>,
        expected_generation: u64,
        owner: impl Into<String>,
        now: DateTime<Utc>,
        lease_expires_at: DateTime<Utc>,
    ) -> Result<Self, RepositoryError> {
        let server_id = server_id.into();
        let owner = owner.into();
        validate_label(&server_id, MAX_LABEL_BYTES)?;
        validate_label(&owner, 256)?;
        if expected_generation == 0 || lease_expires_at <= now {
            return Err(RepositoryError::invalid_data());
        }
        Ok(Self {
            principal,
            server_id,
            expected_generation,
            owner,
            now,
            lease_expires_at,
        })
    }

    pub fn principal(&self) -> &McpInteractionPrincipal {
        &self.principal
    }
    pub fn server_id(&self) -> &str {
        &self.server_id
    }
    pub fn expected_generation(&self) -> u64 {
        self.expected_generation
    }
    pub fn owner(&self) -> &str {
        &self.owner
    }
    pub fn now(&self) -> DateTime<Utc> {
        self.now
    }
    pub fn lease_expires_at(&self) -> DateTime<Utc> {
        self.lease_expires_at
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthTransactionSecret {
    pub transaction_secret: McpSecretCiphertext,
    pub transaction_secret_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpOAuthCredentialSecret {
    pub access_token: McpSecretCiphertext,
    pub access_token_hash: String,
    pub refresh_token: Option<McpSecretCiphertext>,
    pub refresh_token_hash: Option<String>,
}

#[async_trait]
pub trait McpOAuthDurableRepository: Send + Sync {
    async fn create_mcp_oauth_transaction(
        &self,
        command: CreateMcpOAuthTransactionCommand,
    ) -> Result<TransitionOutcome<McpOAuthTransaction>, RepositoryError>;
    async fn load_mcp_oauth_transaction(
        &self,
        transaction_id: &McpOAuthTransactionId,
    ) -> Result<Option<McpOAuthTransaction>, RepositoryError>;
    async fn load_mcp_oauth_transaction_secret(
        &self,
        transaction_id: &McpOAuthTransactionId,
    ) -> Result<Option<McpOAuthTransactionSecret>, RepositoryError>;
    async fn consume_mcp_oauth_transaction(
        &self,
        command: ConsumeMcpOAuthTransactionCommand,
    ) -> Result<TransitionOutcome<McpOAuthTransaction>, RepositoryError>;
    async fn store_mcp_oauth_credential(
        &self,
        command: StoreMcpOAuthCredentialCommand,
    ) -> Result<TransitionOutcome<McpOAuthCredential>, RepositoryError>;
    async fn complete_mcp_oauth_callback(
        &self,
        command: CompleteMcpOAuthCallbackCommand,
    ) -> Result<TransitionOutcome<McpOAuthCallbackCompletion>, RepositoryError>;
    async fn load_mcp_oauth_credential(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
    ) -> Result<Option<McpOAuthCredential>, RepositoryError>;
    async fn load_mcp_oauth_credential_secret(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
    ) -> Result<Option<McpOAuthCredentialSecret>, RepositoryError>;
    async fn claim_mcp_oauth_refresh(
        &self,
        command: ClaimMcpOAuthRefreshCommand,
    ) -> Result<bool, RepositoryError>;
    /// Marks the point after which the authorization server may have consumed
    /// or rotated the refresh token. This transition is deliberately durable
    /// before the network request is sent.
    async fn mark_mcp_oauth_refresh_dispatched(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        generation: u64,
        owner: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError>;
    /// Scrubs a credential whose dispatched refresh did not reach a fenced
    /// local commit. The only safe recovery is a new authorization flow.
    async fn quarantine_mcp_oauth_refresh(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        generation: u64,
        owner: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, RepositoryError>;
    async fn release_mcp_oauth_refresh(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        generation: u64,
        owner: &str,
    ) -> Result<bool, RepositoryError>;
    /// Atomically expires and cryptographically scrubs a bounded batch of
    /// pending authorization transactions whose callback deadline elapsed.
    async fn expire_mcp_oauth_transactions(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, RepositoryError>;
    async fn delete_mcp_oauth_credential(
        &self,
        principal: &McpInteractionPrincipal,
        server_id: &str,
        request_id: &str,
        now: DateTime<Utc>,
    ) -> Result<TransitionOutcome<McpOAuthCredential>, RepositoryError>;
}

#[doc(hidden)]
pub mod adapter {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    pub fn transaction_from_storage(
        transaction_id: McpOAuthTransactionId,
        principal: McpInteractionPrincipal,
        server_id: String,
        issuer: String,
        resource: String,
        client_id: String,
        redirect_uri: String,
        scopes: Vec<String>,
        state_hash: String,
        state: McpOAuthTransactionState,
        version: u64,
        expires_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
        consumed_at: Option<DateTime<Utc>>,
    ) -> McpOAuthTransaction {
        McpOAuthTransaction {
            transaction_id,
            principal,
            server_id,
            issuer,
            resource,
            client_id,
            redirect_uri,
            scopes,
            state_hash,
            state,
            version,
            expires_at,
            created_at,
            consumed_at,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn credential_from_storage(
        principal: McpInteractionPrincipal,
        server_id: String,
        issuer: String,
        client_id: String,
        resource: String,
        scopes: Vec<String>,
        token_type: String,
        generation: u64,
        access_expires_at: Option<DateTime<Utc>>,
        updated_at: DateTime<Utc>,
        revoked_at: Option<DateTime<Utc>>,
    ) -> McpOAuthCredential {
        McpOAuthCredential {
            principal,
            server_id,
            issuer,
            client_id,
            resource,
            scopes,
            token_type,
            generation,
            access_expires_at,
            updated_at,
            revoked_at,
        }
    }
}

fn validate_label(value: &str, max: usize) -> Result<(), RepositoryError> {
    if value.is_empty() || value.len() > max || value.chars().any(char::is_control) {
        Err(RepositoryError::invalid_data())
    } else {
        Ok(())
    }
}

fn validate_hash(value: &str) -> Result<(), RepositoryError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(RepositoryError::invalid_data())
    }
}

fn validate_scopes(scopes: &[String]) -> Result<(), RepositoryError> {
    if scopes.len() > MAX_SCOPE_COUNT
        || scopes.iter().any(|scope| {
            scope.is_empty()
                || scope.len() > 256
                || !scope
                    .bytes()
                    .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
        })
    {
        Err(RepositoryError::invalid_data())
    } else {
        Ok(())
    }
}
