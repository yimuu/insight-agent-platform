//! One-shot installation state. No serving process mounts this directory.
use crate::{
    identity::{jwks_for_key_pair, pkcs1_private_key_from_pkcs8, sign_session},
    private_state::InstallationDirectory,
    tls,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_deployment_contracts::installation::*;
use insight_platform_deployment_contracts::installation_provider::*;
use rcgen::{KeyPair, PKCS_RSA_SHA256};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, path::Path};

const PRIVATE_IDENTITY_FILE: &str = "identity-private.json";
const PROGRESS_FILE: &str = "progress.json";
const PROVIDER_FILE: &str = "provider-initialization.json";
const PROVIDER_START_FILE: &str = "provider-start-requested.json";
/// Never implement Debug: this envelope contains deployment-only keys and credentials.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateIdentity {
    schema_version: u32,
    input: InstallationInputV1,
    identity: InstallationIdentityV1,
    issuer_key_pem: String,
    authority_key_pem: String,
    authority_certificate_pem: String,
    database_passwords: BTreeMap<String, String>,
    role_material: BTreeMap<String, String>,
}
impl PrivateIdentity {
    fn validate(&self, input: &InstallationInputV1) -> Result<(), InstallationError> {
        self.identity.validate()?;
        crate::role_material::validate_credentials(&input.network, &input.credentials)?;
        if self.schema_version != INSTALLATION_VERSION
            || self.input.digest()? != input.digest()?
            || self.identity.input_digest != input.digest()?
        {
            return Err(InstallationError::IdentityDrift);
        }
        let issuer = KeyPair::from_pem(&self.issuer_key_pem)
            .map_err(|_| InstallationError::CredentialInvalid)?;
        let jwks = jwks_for_key_pair(&issuer, &self.identity.session.key_id)?;
        if digest(&jwks)? != self.identity.jwks_digest
            || bytes_digest(self.authority_certificate_pem.as_bytes())?
                != self.identity.certificate_authority_digest
        {
            return Err(InstallationError::IdentityDrift);
        }
        let authority = KeyPair::from_pem(&self.authority_key_pem)
            .map_err(|_| InstallationError::CredentialInvalid)?;
        use rcgen::PublicKeyData as _;
        use x509_parser::prelude::FromDer as _;
        let (_, pem) = x509_parser::pem::parse_x509_pem(self.authority_certificate_pem.as_bytes())
            .map_err(|_| InstallationError::CredentialInvalid)?;
        let (_, certificate) = x509_parser::certificate::X509Certificate::from_der(&pem.contents)
            .map_err(|_| InstallationError::CredentialInvalid)?;
        if certificate.public_key().raw != authority.subject_public_key_info() {
            return Err(InstallationError::IdentityDrift);
        }
        if self.database_passwords.len() != DATABASE_CREDENTIALS.len()
            || !DATABASE_CREDENTIALS.iter().all(|name| {
                self.database_passwords.get(*name).is_some_and(|value| {
                    value.len() == 32 && value.bytes().all(|b| b.is_ascii_hexdigit())
                })
            })
        {
            return Err(InstallationError::CredentialInvalid);
        }
        Ok(())
    }
}
const DATABASE_CREDENTIALS: &[&str] = &[
    "postgres-admin-password",
    "runtime-password",
    "outbox-password",
    "history-password",
    "security-authority-password",
    "artifact-gateway-password",
    "artifact-data-reader-password",
    "artifact-data-worker-password",
    "artifact-maintenance-password",
];

pub struct PreparedInstallation {
    directory: InstallationDirectory,
    material: PrivateIdentity,
    progress: InstallationProgressV1,
}
impl PreparedInstallation {
    pub fn prepare(input: &InstallationInputV1, root: &Path) -> Result<Self, InstallationError> {
        input.validate()?;
        validate_remote_context_destinations(&input.remote_context_destinations)?;
        let directory = InstallationDirectory::open(root, true)?;
        let material = match directory.read(PRIVATE_IDENTITY_FILE, INSTALLATION_MAX_BYTES)? {
            Some(bytes) => decode_private(&bytes)?,
            None => {
                directory.require_uninitialized()?;
                // No external effect is possible before this complete identity is durable.
                let material = generate(input)?;
                directory.write_immutable(PRIVATE_IDENTITY_FILE, &encode(&material)?)?;
                material
            }
        };
        material.validate(input)?;
        let encoded_progress = directory.read(PROGRESS_FILE, INSTALLATION_MAX_BYTES)?;
        let already_prepared = encoded_progress.is_some();
        let progress = match encoded_progress {
            Some(bytes) => decode_progress(&bytes)?,
            None => InstallationProgressV1 {
                schema_version: INSTALLATION_VERSION,
                input_digest: input.digest()?,
                identity_digest: material.identity.digest()?,
                phase: InstallationPhase::Prepared,
            },
        };
        progress.validate_for(input, &material.identity)?;
        let prepared = Self {
            directory,
            material,
            progress,
        };
        if already_prepared {
            prepared.verify_public_identity()?;
        } else {
            prepared.persist_public_identity()?;
        }
        prepared
            .directory
            .write_immutable(PROGRESS_FILE, &encode(&prepared.progress)?)?;
        prepared.prepare_provider_files()?;
        Ok(prepared)
    }
    pub fn open(input: &InstallationInputV1, root: &Path) -> Result<Self, InstallationError> {
        input.validate()?;
        Self::open_directory(input, InstallationDirectory::open(root, false)?)
    }
    pub fn open_read_only(
        input: &InstallationInputV1,
        root: &Path,
    ) -> Result<Self, InstallationError> {
        input.validate()?;
        Self::open_directory(input, InstallationDirectory::open_read_only(root)?)
    }
    fn open_directory(
        input: &InstallationInputV1,
        directory: InstallationDirectory,
    ) -> Result<Self, InstallationError> {
        input.validate()?;
        validate_remote_context_destinations(&input.remote_context_destinations)?;
        let material = decode_private(
            &directory
                .read(PRIVATE_IDENTITY_FILE, INSTALLATION_MAX_BYTES)?
                .ok_or(InstallationError::Incomplete)?,
        )?;
        material.validate(input)?;
        let progress = decode_progress(
            &directory
                .read(PROGRESS_FILE, INSTALLATION_MAX_BYTES)?
                .ok_or(InstallationError::Incomplete)?,
        )?;
        progress.validate_for(input, &material.identity)?;
        let prepared = Self {
            directory,
            material,
            progress,
        };
        prepared.verify_public_identity()?;
        Ok(prepared)
    }
    pub fn public_trust(
        &self,
    ) -> Result<
        insight_platform_deployment_contracts::public_trust::InstallationPublicTrustV1,
        InstallationError,
    > {
        use insight_platform_deployment_contracts::public_trust::InstallationPublicTrustV1;
        if self.progress.phase != InstallationPhase::Ready {
            return Err(InstallationError::Incomplete);
        }
        self.verify_public_identity()?;
        let result = InstallationPublicTrustV1 {
            schema_version: 1,
            input_digest: self.progress.input_digest.clone(),
            identity_digest: self.progress.identity_digest.clone(),
            certificate_pem: self.material.authority_certificate_pem.clone(),
            certificate_sha256: self.material.identity.certificate_authority_digest.clone(),
        };
        result.validate_for(&self.material.input, &self.material.identity)?;
        Ok(result)
    }
    pub fn directory(&self) -> &InstallationDirectory {
        &self.directory
    }
    pub fn identity(&self) -> &InstallationIdentityV1 {
        &self.material.identity
    }
    pub fn progress(&self) -> &InstallationProgressV1 {
        &self.progress
    }
    pub fn input(&self) -> &InstallationInputV1 {
        &self.material.input
    }
    pub fn provider_documents(
        &self,
    ) -> Result<crate::openbao_profile::OpenBaoDocuments, InstallationError> {
        let mut certificates = BTreeMap::new();
        for role in OpenBaoInstallationRole::ALL {
            let name = crate::openbao_profile::certificate_file(*role);
            let bytes = self
                .directory
                .read(&name, 16_384)?
                .ok_or(InstallationError::Incomplete)?;
            certificates.insert(
                *role,
                String::from_utf8(bytes).map_err(|_| InstallationError::CredentialInvalid)?,
            );
        }
        crate::openbao_profile::render(self.input(), self.identity(), &certificates)
    }
    fn prepare_provider_files(&self) -> Result<(), InstallationError> {
        let documents = self.provider_documents()?;
        let prior = self.directory.read(PROVIDER_FILE, INSTALLATION_MAX_BYTES)?;
        let never_requested = self
            .directory
            .read(PROVIDER_START_FILE, INSTALLATION_MAX_BYTES)?
            .is_none();
        let may_create = self.progress.phase == InstallationPhase::Prepared
            && never_requested
            && prior
                .as_deref()
                .map(InstallationProviderInitializationV1::decode)
                .transpose()?
                .is_none_or(|state| state.state == InstallationProviderStateV1::Prepared);
        for (name, bytes) in [
            ("openbao-initialize.json", documents.initialize.as_slice()),
            ("openbao-serve.json", documents.serve.as_slice()),
            ("openbao-canary.json", documents.canary.as_slice()),
        ] {
            match self.directory.read(name, INSTALLATION_MAX_BYTES)? {
                Some(actual) if actual == bytes => (),
                None if may_create => self.directory.write_immutable(name, bytes)?,
                _ => return Err(InstallationError::ConfigurationDrift),
            }
        }
        match prior {
            Some(bytes) => InstallationProviderInitializationV1::decode(&bytes)?.validate_for(
                self.input(),
                self.identity(),
                &documents.configuration_digest,
            ),
            None => {
                // A missing mutable journal never grants a second first-start permission.
                if !may_create {
                    return Err(InstallationError::Incomplete);
                }
                self.directory.write_immutable(
                    PROVIDER_FILE,
                    &encode(&InstallationProviderInitializationV1 {
                        schema_version: PROVIDER_INITIALIZATION_VERSION,
                        input_digest: self.input().digest()?,
                        identity_digest: self.identity().digest()?,
                        configuration_digest: documents.configuration_digest,
                        state: InstallationProviderStateV1::Prepared,
                    })?,
                )
            }
        }
    }
    pub fn provider_state(
        &self,
    ) -> Result<InstallationProviderInitializationV1, InstallationError> {
        let documents = self.provider_documents()?;
        for (name, bytes) in [
            ("openbao-initialize.json", &documents.initialize),
            ("openbao-serve.json", &documents.serve),
            ("openbao-canary.json", &documents.canary),
        ] {
            if self.directory.read(name, INSTALLATION_MAX_BYTES)?.as_ref() != Some(bytes) {
                return Err(InstallationError::ConfigurationDrift);
            }
        }
        let state = InstallationProviderInitializationV1::decode(
            &self
                .directory
                .read(PROVIDER_FILE, INSTALLATION_MAX_BYTES)?
                .ok_or(InstallationError::Incomplete)?,
        )?;
        state.validate_for(
            self.input(),
            self.identity(),
            &documents.configuration_digest,
        )?;
        Ok(state)
    }
    /// The installation filesystem lock remains held across this atomic permission hand-off.
    pub fn request_provider_start(&self) -> Result<InstallationProviderStart, InstallationError> {
        let mut state = self.provider_state()?;
        if state.state == InstallationProviderStateV1::Prepared {
            if self
                .directory
                .read(PROVIDER_START_FILE, INSTALLATION_MAX_BYTES)?
                .is_some()
            {
                return Err(InstallationError::ExternalOutcomeUnknown);
            }
            // This immutable marker closes response loss between the two durable file writes.
            self.directory.write_immutable(PROVIDER_START_FILE, &encode(&serde_json::json!({
                "schema_version":1,"input_digest":state.input_digest,
                "identity_digest":state.identity_digest,"configuration_digest":state.configuration_digest,
            }))?)?;
        }
        let permission = state.request_start()?;
        self.directory.replace(PROVIDER_FILE, &encode(&state)?)?;
        Ok(permission)
    }
    /// Caller obtains every field through authenticated exact provider readback before this call.
    pub fn complete_provider(
        &self,
        evidence: InstallationProviderReadyV1,
    ) -> Result<(), InstallationError> {
        let documents = self.provider_documents()?;
        let expected = parse_strict_json(&documents.canary, INSTALLATION_LIMITS)
            .map_err(|_| InstallationError::InvalidInput)?;
        if evidence.canary_digest != digest(&expected)? {
            return Err(InstallationError::ConfigurationDrift);
        }
        let mut state = self.provider_state()?;
        let already_ready = matches!(
            state.state,
            InstallationProviderStateV1::ProviderReady { .. }
        );
        state.complete(self.input(), evidence)?;
        if already_ready {
            Ok(())
        } else {
            self.directory.replace(PROVIDER_FILE, &encode(&state)?)
        }
    }
    /// Advance only after the caller has observed the exact owning setup stage's success.
    pub fn complete_phase(&mut self, next: InstallationPhase) -> Result<(), InstallationError> {
        let expected = match self.progress.phase {
            InstallationPhase::Prepared => InstallationPhase::DependenciesVerified,
            InstallationPhase::DependenciesVerified => InstallationPhase::SchemaVerified,
            InstallationPhase::SchemaVerified => InstallationPhase::RolesProvisioned,
            InstallationPhase::RolesProvisioned => InstallationPhase::AuthorityBootstrapped,
            InstallationPhase::AuthorityBootstrapped => InstallationPhase::Ready,
            InstallationPhase::Ready => return Err(InstallationError::Conflict),
        };
        if next != expected {
            return Err(InstallationError::Conflict);
        }
        if !matches!(
            self.provider_state()?.state,
            InstallationProviderStateV1::ProviderReady { .. }
        ) {
            return Err(InstallationError::Incomplete);
        }
        let progress = InstallationProgressV1 {
            phase: next,
            ..self.progress.clone()
        };
        self.directory.replace(PROGRESS_FILE, &encode(&progress)?)?;
        self.progress = progress;
        Ok(())
    }
    /// Input to the deterministic renderer only; the renderer selects each role's minimum files.
    pub fn renderer_private_files(&self) -> Result<BTreeMap<String, Vec<u8>>, InstallationError> {
        self.verify_public_identity()?;
        let mut files = self
            .material
            .database_passwords
            .iter()
            .map(|(name, value)| (name.clone(), value.as_bytes().to_vec()))
            .collect::<BTreeMap<_, _>>();
        for (name, value) in &self.material.role_material {
            files.insert(
                name.clone(),
                BASE64
                    .decode(value)
                    .map_err(|_| InstallationError::CredentialInvalid)?,
            );
        }
        files.insert(
            "ca.pem".into(),
            self.material.authority_certificate_pem.as_bytes().to_vec(),
        );
        Ok(files)
    }
    pub fn jwks(&self) -> Result<Value, InstallationError> {
        let key = KeyPair::from_pem(&self.material.issuer_key_pem)
            .map_err(|_| InstallationError::CredentialInvalid)?;
        jwks_for_key_pair(&key, &self.material.identity.session.key_id)
    }
    pub fn prepare_artifact_authority(&self,storage:Sha256Digest,allow_create:bool)->Result<(insight_platform_deployment_contracts::development::DevelopmentArtifactAuthorityConfigV1,Sha256Digest),InstallationError>{
        const FILE: &str = "artifact-bootstrap.json";
        let prior = self.directory.read(FILE, INSTALLATION_MAX_BYTES)?;
        let preserved = prior
            .as_deref()
            .map(|bytes| {
                let value = parse_strict_json(bytes, INSTALLATION_LIMITS)
                    .map_err(|_| InstallationError::InvalidInput)?;
                serde_json::from_value(value).map_err(|_| InstallationError::InvalidInput)
            })
            .transpose()?;
        if preserved.is_none()
            && (!allow_create || self.progress.phase != InstallationPhase::Prepared)
        {
            return Err(InstallationError::Incomplete);
        }
        let config = crate::bootstrap::artifact_authority(
            storage,
            self.identity().artifact_encryption_domain_id.clone(),
            preserved.as_ref(),
        )?;
        let value = serde_json::to_value(&config).map_err(|_| InstallationError::InvalidInput)?;
        let digest = digest(&value)?;
        if prior.is_none() {
            self.directory.write_immutable(FILE, &encode(&config)?)?;
        }
        Ok((config, digest))
    }
    fn public_files(&self) -> Result<BTreeMap<&'static str, Vec<u8>>, InstallationError> {
        let issuer = KeyPair::from_pem(&self.material.issuer_key_pem)
            .map_err(|_| InstallationError::CredentialInvalid)?;
        Ok(BTreeMap::from([
            ("input.json", encode(&self.material.input)?),
            ("identity.json", encode(&self.material.identity)?),
            ("bootstrap.json", encode(&self.material.identity.bootstrap)?),
            (
                "jwks.json",
                encode(&jwks_for_key_pair(
                    &issuer,
                    &self.material.identity.session.key_id,
                )?)?,
            ),
            (
                "ca.pem",
                self.material.authority_certificate_pem.as_bytes().to_vec(),
            ),
        ]))
    }
    fn persist_public_identity(&self) -> Result<(), InstallationError> {
        for (name, bytes) in self.public_files()? {
            self.directory.write_immutable(name, &bytes)?;
        }
        for (name, value) in &self.material.database_passwords {
            self.directory.write_immutable(name, value.as_bytes())?;
        }
        for (name, value) in &self.material.role_material {
            self.directory.write_immutable(
                name,
                &BASE64
                    .decode(value)
                    .map_err(|_| InstallationError::CredentialInvalid)?,
            )?;
        }
        Ok(())
    }
    fn verify_public_identity(&self) -> Result<(), InstallationError> {
        for (name, bytes) in self.public_files()? {
            if self
                .directory
                .read(name, INSTALLATION_MAX_BYTES)?
                .as_deref()
                != Some(bytes.as_slice())
            {
                return Err(InstallationError::ConfigurationDrift);
            }
        }
        for (name, value) in &self.material.database_passwords {
            if self.directory.read(name, 32)?.as_deref() != Some(value.as_bytes()) {
                return Err(InstallationError::CredentialInvalid);
            }
        }
        for (name, value) in &self.material.role_material {
            if self
                .directory
                .read(name, INSTALLATION_MAX_BYTES)?
                .as_deref()
                != Some(
                    BASE64
                        .decode(value)
                        .map_err(|_| InstallationError::CredentialInvalid)?
                        .as_slice(),
                )
            {
                return Err(InstallationError::CredentialInvalid);
            }
        }
        Ok(())
    }
    /// Session issuance never bootstraps or changes current principal permissions.
    pub fn issue_session(&self, issued_at: u64) -> Result<std::path::PathBuf, InstallationError> {
        if self.progress.phase != InstallationPhase::Ready {
            return Err(InstallationError::Incomplete);
        }
        self.verify_public_identity()?;
        let key = KeyPair::from_pem(&self.material.issuer_key_pem)
            .map_err(|_| InstallationError::CredentialInvalid)?;
        let der = key.serialize_der();
        let token = sign_session(
            &self.material.identity.session,
            pkcs1_private_key_from_pkcs8(&der).ok_or(InstallationError::CredentialInvalid)?,
            issued_at,
        )?;
        self.directory
            .replace("session-token", format!("{token}\n").as_bytes())?;
        self.directory.path("session-token")
    }
}
fn decode_private(bytes: &[u8]) -> Result<PrivateIdentity, InstallationError> {
    let value = parse_strict_json(bytes, INSTALLATION_LIMITS)
        .map_err(|_| InstallationError::InvalidInput)?;
    serde_json::from_value(value).map_err(|_| InstallationError::InvalidInput)
}
fn decode_progress(bytes: &[u8]) -> Result<InstallationProgressV1, InstallationError> {
    let value = parse_strict_json(bytes, INSTALLATION_LIMITS)
        .map_err(|_| InstallationError::InvalidInput)?;
    serde_json::from_value(value).map_err(|_| InstallationError::InvalidInput)
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>, InstallationError> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|_| InstallationError::InvalidInput)?;
    if bytes.len() > INSTALLATION_MAX_BYTES {
        return Err(InstallationError::InvalidInput);
    }
    Ok(bytes)
}
fn digest(value: &Value) -> Result<Sha256Digest, InstallationError> {
    canonical_digest(value)
        .map_err(|_| InstallationError::InvalidInput)?
        .parse()
        .map_err(|_| InstallationError::InvalidInput)
}
fn bytes_digest(value: &[u8]) -> Result<Sha256Digest, InstallationError> {
    format!("sha256:{}", crate::lower_hex(&Sha256::digest(value)))
        .parse()
        .map_err(|_| InstallationError::InvalidInput)
}
fn tag_digest(tag: &str, value: &str) -> Result<Sha256Digest, InstallationError> {
    digest(&json!({"schema_version":1,"tag":tag,"value":value}))
}
fn fresh(kind: ResourceKind) -> Result<ResourceId, InstallationError> {
    ResourceId::from_uuid_v7(kind, uuid::Uuid::now_v7())
        .map_err(|_| InstallationError::InvalidInput)
}
fn generate(input: &InstallationInputV1) -> Result<PrivateIdentity, InstallationError> {
    let nonce = uuid::Uuid::now_v7();
    let issuer = format!("https://local.insight.platform/{nonce}");
    let subject = format!("administrator:{nonce}");
    let authority = tag_digest("oidc_authentication_authority_v1", &issuer)?;
    let principal = |subject: &str| -> Result<BootstrapPrincipalV1, InstallationError> {
        Ok(BootstrapPrincipalV1 {
            principal_id: fresh(ResourceKind::Principal)?,
            authentication_authority_digest: authority.clone(),
            subject_digest: tag_digest("oidc_subject_v1", subject)?,
        })
    };
    let bootstrap = InstallationAdministratorBootstrapV1 {
        schema_version: INSTALLATION_VERSION,
        environment_class: "development".into(),
        installation: principal(&format!("bootstrap:{nonce}"))?,
        installation_request_id: fresh(ResourceKind::ServerRequest)?,
        installation_evidence_digest: tag_digest(
            "local_installation_bootstrap_evidence_v1",
            &issuer,
        )?,
        tenant_id: fresh(ResourceKind::Tenant)?,
        administrator: principal(&subject)?,
        registry_validator: principal(&format!("registry-validator:{nonce}"))?,
        egress_broker: principal(&format!("egress-broker:{nonce}"))?,
    };
    let session = LocalSessionIdentityV1 {
        schema_version: INSTALLATION_VERSION,
        issuer,
        audience: "insight.platform/v1".into(),
        key_id: format!("local-oidc-{nonce}"),
        tenant_id: bootstrap.tenant_id.clone(),
        subject,
        principal_kind: LocalSessionKind::TenantAdmin,
    };
    let key = KeyPair::generate_for(&PKCS_RSA_SHA256)
        .map_err(|_| InstallationError::CredentialInvalid)?;
    let ca = tls::create_authority()?;
    let identity = InstallationIdentityV1 {
        schema_version: INSTALLATION_VERSION,
        installation_id: fresh(ResourceKind::InstallationService)?,
        input_digest: input.digest()?,
        jwks_digest: digest(&jwks_for_key_pair(&key, &session.key_id)?)?,
        certificate_authority_digest: bytes_digest(ca.certificate_pem.as_bytes())?,
        session,
        bootstrap,
        artifact_encryption_domain_id: fresh(ResourceKind::EncryptionDomain)?,
        secret_provider_id: fresh(ResourceKind::SecretProvider)?,
    };
    identity.validate()?;
    let authority_key =
        KeyPair::from_pem(&ca.private_key_pem).map_err(|_| InstallationError::CredentialInvalid)?;
    let issuer = rcgen::Issuer::new(tls::authority_parameters()?, authority_key);
    let mut role_material = crate::role_material::generate_leaf_files(&input.network, &issuer)?;
    for name in [
        "cursor-key",
        "mcp-state-key",
        "mcp-oauth-state-key",
        crate::openbao_profile::OPENBAO_SEAL_FILE,
    ] {
        let mut bytes = vec![0; 32];
        getrandom::fill(&mut bytes).map_err(|_| InstallationError::CredentialInvalid)?;
        role_material.insert(name.into(), bytes);
    }
    for name in [
        "s3-initializer-credentials",
        "s3-artifact-gateway-credentials",
        "s3-artifact-data-credentials",
        "s3-artifact-maintenance-credentials",
    ] {
        let mut access = [0; 16];
        let mut secret = [0; 32];
        getrandom::fill(&mut access).map_err(|_| InstallationError::CredentialInvalid)?;
        getrandom::fill(&mut secret).map_err(|_| InstallationError::CredentialInvalid)?;
        role_material.insert(
            name.into(),
            format!(
                "[default]\naws_access_key_id={}\naws_secret_access_key={}\n",
                crate::lower_hex(&access),
                crate::lower_hex(&secret)
            )
            .into_bytes(),
        );
        access.fill(0);
        secret.fill(0);
    }
    Ok(PrivateIdentity {
        schema_version: INSTALLATION_VERSION,
        input: input.clone(),
        identity,
        issuer_key_pem: key.serialize_pem(),
        authority_key_pem: ca.private_key_pem,
        authority_certificate_pem: ca.certificate_pem,
        role_material: role_material
            .into_iter()
            .map(|(name, bytes)| (name, BASE64.encode(bytes)))
            .collect(),
        database_passwords: DATABASE_CREDENTIALS
            .iter()
            .map(|name| {
                (
                    (*name).to_owned(),
                    uuid::Uuid::new_v4().simple().to_string(),
                )
            })
            .collect(),
    })
}

pub fn read_remote_context_destinations(
    path: &std::path::Path,
) -> Result<Vec<insight_platform_contracts::InstalledRemoteContextDestinationV1>, InstallationError>
{
    use std::io::Read as _;
    use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
    if !path.is_absolute()
        || path.components().any(|part| {
            matches!(
                part,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
    {
        return Err(InstallationError::InvalidPath);
    }
    for ancestor in path.ancestors().skip(1) {
        if !std::fs::symlink_metadata(ancestor)
            .map_err(|_| InstallationError::InvalidPath)?
            .is_dir()
        {
            return Err(InstallationError::InvalidPath);
        }
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| InstallationError::InvalidInput)?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.len() > INSTALLATION_MAX_BYTES as u64
    {
        return Err(InstallationError::InvalidInput);
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| InstallationError::InvalidInput)?;
    let opened = file
        .metadata()
        .map_err(|_| InstallationError::InvalidInput)?;
    if !opened.is_file()
        || opened.dev() != metadata.dev()
        || opened.ino() != metadata.ino()
        || opened.nlink() != 1
    {
        return Err(InstallationError::InvalidInput);
    }
    let mut bytes = Vec::new();
    file.take(INSTALLATION_MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| InstallationError::InvalidInput)?;
    let value = insight_platform_contracts::parse_strict_json(
        &bytes,
        insight_platform_deployment_contracts::installation::INSTALLATION_LIMITS,
    )
    .map_err(|_| InstallationError::InvalidInput)?;
    let destinations: Vec<insight_platform_contracts::InstalledRemoteContextDestinationV1> =
        serde_json::from_value(value).map_err(|_| InstallationError::InvalidInput)?;
    validate_remote_context_destinations(&destinations)?;
    Ok(destinations)
}

/// The installation entry declares the complete base topology before creating private state.
pub fn validate_remote_context_destinations(
    destinations: &[insight_platform_contracts::InstalledRemoteContextDestinationV1],
) -> Result<(), InstallationError> {
    use x509_parser::prelude::FromDer as _;
    for destination in destinations {
        // Foundation owns the transport shape. The installation producer additionally parses
        // public certificates, without creating a trust store or contacting the destination.
        if !destination.validate_shape() {
            return Err(InstallationError::InvalidInput);
        }
        let mut remaining = destination.trusted_root_pem.as_bytes();
        let mut count = 0;
        while !remaining.is_empty() {
            remaining = remaining.trim_ascii();
            if remaining.is_empty() {
                break;
            }
            if !remaining.starts_with(b"-----BEGIN CERTIFICATE-----") {
                return Err(InstallationError::InvalidInput);
            }
            let (rest, pem) = x509_parser::pem::parse_x509_pem(remaining)
                .map_err(|_| InstallationError::InvalidInput)?;
            if pem.label != "CERTIFICATE" {
                return Err(InstallationError::InvalidInput);
            }
            let (trailing, _) = x509_parser::certificate::X509Certificate::from_der(&pem.contents)
                .map_err(|_| InstallationError::InvalidInput)?;
            if !trailing.is_empty() || rest.len() >= remaining.len() {
                return Err(InstallationError::InvalidInput);
            }
            count += 1;
            remaining = rest;
        }
        if count == 0 {
            return Err(InstallationError::InvalidInput);
        }
    }
    Ok(())
}

/// Select a physical destination before preparation and derive its one existing worker role.
/// The input remains the frozen owner; this never updates a prepared installation.
pub fn with_remote_context_destinations(
    mut input: InstallationInputV1,
    destinations: Vec<insight_platform_contracts::InstalledRemoteContextDestinationV1>,
) -> Result<InstallationInputV1, InstallationError> {
    use InstallationProcess as P;
    input.validate()?;
    validate_remote_context_destinations(&destinations)?;
    if input
        .network
        .processes
        .iter()
        .any(|entry| entry.process == P::ContextRemote)
    {
        return Err(InstallationError::InvalidRoleClosure);
    }
    if !destinations.is_empty() {
        let (observability_address, paths) = match input.network.topology {
            InstallationTopology::Compose | InstallationTopology::KubernetesLocal => (
                ([0, 0, 0, 0], 9090).into(),
                RolePathsV1 {
                    process: P::ContextRemote,
                    configuration_directory: "/run/insight/role/config".into(),
                    credential_directory: "/run/insight/role/credentials".into(),
                    temporary_directory: "/var/lib/insight".into(),
                },
            ),
            InstallationTopology::Native => {
                let base = input
                    .paths
                    .iter()
                    .find(|paths| paths.process == P::ContextNative)
                    .ok_or(InstallationError::InvalidRoleClosure)?;
                let roles = Path::new(&base.configuration_directory)
                    .parent()
                    .and_then(Path::parent)
                    .ok_or(InstallationError::InvalidPath)?;
                let temporary = Path::new(&base.temporary_directory)
                    .parent()
                    .ok_or(InstallationError::InvalidPath)?;
                let port = input
                    .network
                    .database
                    .port
                    .checked_add(17 + 2 * P::BASE.len() as u16)
                    .ok_or(InstallationError::InvalidEndpoint)?;
                (
                    ([127, 0, 0, 1], port).into(),
                    RolePathsV1 {
                        process: P::ContextRemote,
                        configuration_directory: roles
                            .join(P::ContextRemote.name())
                            .join("config")
                            .to_string_lossy()
                            .into_owned(),
                        credential_directory: roles
                            .join(P::ContextRemote.name())
                            .join("credentials")
                            .to_string_lossy()
                            .into_owned(),
                        temporary_directory: temporary
                            .join(P::ContextRemote.name())
                            .to_string_lossy()
                            .into_owned(),
                    },
                )
            }
        };
        input.network.processes.push(ProcessNetworkV1 {
            process: P::ContextRemote,
            listen_address: None,
            observability_address,
            service_origin: None,
        });
        input.paths.push(paths);
    }
    input.remote_context_destinations = destinations;
    input.credentials = crate::role_material::credentials(&input.network);
    input.validate()?;
    Ok(input)
}

pub fn compose_input(
    name: &str,
    package_digest: Sha256Digest,
) -> Result<InstallationInputV1, InstallationError> {
    use InstallationProcess as P;
    let processes = InstallationProcess::BASE
        .iter()
        .map(|process| {
            let port = match process {
                P::GatewayManagement => Some((8081, false)),
                P::GatewayRuntime => Some((8080, false)),
                P::ArtifactGateway => Some((8443, true)),
                P::ArtifactData => Some((8444, true)),
                P::SecurityAuthority => Some((8445, true)),
                P::EgressBroker => Some((8446, true)),
                _ => None,
            };
            Ok(ProcessNetworkV1 {
                process: *process,
                listen_address: port
                    .map(|(port, _)| std::net::SocketAddr::from(([0, 0, 0, 0], port))),
                observability_address: std::net::SocketAddr::from((
                    [0, 0, 0, 0],
                    port.filter(|_| matches!(process, P::GatewayManagement | P::GatewayRuntime))
                        .map(|(port, _)| port)
                        .unwrap_or(9090),
                )),
                service_origin: port
                    .map(|(port, tls)| {
                        ServiceOrigin::parse(&format!(
                            "{}://{}:{port}",
                            if tls { "https" } else { "http" },
                            process.name()
                        ))
                    })
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, InstallationError>>()?;
    let mut input = InstallationInputV1 {
        schema_version: INSTALLATION_VERSION,
        name: name.into(),
        environment_class: "development".into(),
        package_digest,
        authentication: InstallationAuth::LocalTenantAdmin,
        network: NetworkTopologyV1 {
            topology: InstallationTopology::Compose,
            processes,
            database: DatabaseEndpointV1 {
                host: "postgres".into(),
                port: 5432,
                database: "insight_platform".into(),
            },
            nats_host: "nats".into(),
            nats_port: 4222,
            console_origin: ServiceOrigin::parse("http://127.0.0.1:8088")?,
            providers: ProviderNetworkV1::S3OpenBao {
                artifact: ServiceOrigin::parse("https://s3.localhost:8333")?,
                openbao: ServiceOrigin::parse("https://openbao:8200")?,
            },
        },
        paths: InstallationProcess::BASE
            .iter()
            .map(|process| RolePathsV1 {
                process: *process,
                configuration_directory: "/run/insight/role/config".into(),
                credential_directory: "/run/insight/role/credentials".into(),
                temporary_directory: "/var/lib/insight".into(),
            })
            .collect(),
        credentials: CredentialReferencesV1 { files: vec![] },
        model_destinations: Vec::new(),
        remote_context_destinations: Vec::new(),
    };
    input.credentials = crate::role_material::credentials(&input.network);
    input.validate()?;
    Ok(input)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (tempfile::TempDir, InstallationInputV1, std::path::PathBuf) {
        let temp = tempfile::tempdir().unwrap();
        let parent = std::fs::canonicalize(temp.path()).unwrap();
        let root = parent.join("installation");
        let input = compose_input(
            "installation-test",
            format!("sha256:{}", "a".repeat(64)).parse().unwrap(),
        )
        .unwrap();
        (temp, input, root)
    }
    fn state_snapshot(root: &Path) -> BTreeMap<String, Sha256Digest> {
        std::fs::read_dir(root)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (
                    entry.file_name().into_string().unwrap(),
                    bytes_digest(&std::fs::read(entry.path()).unwrap()).unwrap(),
                )
            })
            .collect()
    }
    #[test]
    fn public_trust_requires_ready_and_read_only_state_rejects_every_write_path() {
        use insight_platform_deployment_contracts::public_trust::InstallationPublicTrustV1;
        let (_temp, input, root) = fixture();
        let mut prepared = PreparedInstallation::prepare(&input, &root).unwrap();
        assert_eq!(
            prepared.public_trust().unwrap_err(),
            InstallationError::Incomplete
        );
        // Unit fixture advances only the local phase; real provider/PG qualification is separate.
        prepared.progress.phase = InstallationPhase::Ready;
        prepared
            .directory
            .replace(PROGRESS_FILE, &encode(&prepared.progress).unwrap())
            .unwrap();
        let identity = prepared.identity().clone();
        drop(prepared);
        let before = state_snapshot(&root);
        let prepared = PreparedInstallation::open_read_only(&input, &root).unwrap();
        assert!(matches!(
            PreparedInstallation::open(&input, &root),
            Err(InstallationError::Conflict)
        ));
        let trust = prepared.public_trust().unwrap();
        let encoded = serde_json::to_vec(&trust).unwrap();
        let decoded = InstallationPublicTrustV1::decode(&encoded).unwrap();
        decoded.validate_for(&input, &identity).unwrap();
        assert!(!trust.certificate_pem.contains("PRIVATE KEY"));
        assert_eq!(
            prepared.directory.replace("forbidden", b"write"),
            Err(InstallationError::InvalidInput)
        );
        assert_eq!(
            prepared.directory.write_immutable("forbidden", b"write"),
            Err(InstallationError::InvalidInput)
        );
        assert_eq!(
            prepared.issue_session(1000),
            Err(InstallationError::InvalidInput)
        );
        let mut wrong_input = input.clone();
        wrong_input.name = "different-installation".into();
        assert!(decoded.validate_for(&wrong_input, &identity).is_err());
        let mut wrong_identity = identity.clone();
        wrong_identity.certificate_authority_digest = input.package_digest.clone();
        assert!(decoded.validate_for(&input, &wrong_identity).is_err());
        drop(prepared);
        assert_eq!(state_snapshot(&root), before);
        std::fs::write(root.join("ca.pem"), b"foreign certificate").unwrap();
        assert!(PreparedInstallation::open_read_only(&input, &root).is_err());
        assert!(std::fs::read(root.join("ca.pem")).unwrap() == b"foreign certificate");
    }
    #[test]
    fn provider_start_is_durable_and_missing_journal_never_recreates_permission() {
        let (_temp, input, root) = fixture();
        let prepared = PreparedInstallation::prepare(&input, &root).unwrap();
        let initialize = prepared
            .directory
            .read("openbao-initialize.json", INSTALLATION_MAX_BYTES)
            .unwrap()
            .unwrap();
        let serve = prepared
            .directory
            .read("openbao-serve.json", INSTALLATION_MAX_BYTES)
            .unwrap()
            .unwrap();
        assert!(serde_json::from_slice::<Value>(&initialize)
            .unwrap()
            .get("initialize")
            .is_some());
        assert!(serde_json::from_slice::<Value>(&serve)
            .unwrap()
            .get("initialize")
            .is_none());
        assert_eq!(
            prepared.request_provider_start(),
            Ok(InstallationProviderStart::InitializeOnce)
        );
        drop(prepared);
        let prepared = PreparedInstallation::open(&input, &root).unwrap();
        assert_eq!(
            prepared.request_provider_start(),
            Err(InstallationError::ExternalOutcomeUnknown)
        );
        assert_eq!(
            prepared
                .directory
                .read("openbao-initialize.json", INSTALLATION_MAX_BYTES)
                .unwrap()
                .unwrap(),
            initialize
        );
        assert_eq!(
            prepared
                .directory
                .read("openbao-serve.json", INSTALLATION_MAX_BYTES)
                .unwrap()
                .unwrap(),
            serve
        );
        drop(prepared);
        std::fs::remove_file(root.join(PROVIDER_FILE)).unwrap();
        assert!(matches!(
            PreparedInstallation::prepare(&input, &root),
            Err(InstallationError::Incomplete)
        ));
        assert!(!root.join(PROVIDER_FILE).exists());
    }
    #[test]
    fn first_start_permission_lost_before_mutable_journal_commit_stays_unknown() {
        let (_temp, input, root) = fixture();
        let prepared = PreparedInstallation::prepare(&input, &root).unwrap();
        // Inject a crash after the immutable effect marker and before the mutable phase write.
        prepared
            .directory
            .write_immutable(PROVIDER_START_FILE, b"{}")
            .unwrap();
        assert_eq!(
            prepared.request_provider_start(),
            Err(InstallationError::ExternalOutcomeUnknown)
        );
        assert_eq!(
            prepared.provider_state().unwrap().state,
            InstallationProviderStateV1::Prepared
        );
    }
    #[test]
    fn prepare_replay_keeps_identity_and_credentials_and_session_requires_ready() {
        let (_temp, input, root) = fixture();
        let first = PreparedInstallation::prepare(&input, &root).unwrap();
        let identity = first.identity().digest().unwrap();
        let secret = std::fs::read(root.join("identity-private.json")).unwrap();
        assert_eq!(
            first.issue_session(1000).unwrap_err(),
            InstallationError::Incomplete
        );
        assert!(matches!(
            PreparedInstallation::open(&input, &root),
            Err(InstallationError::Conflict)
        ));
        drop(first);
        let replay = PreparedInstallation::prepare(&input, &root).unwrap();
        assert_eq!(replay.identity().digest().unwrap(), identity);
        assert_eq!(
            std::fs::read(root.join("identity-private.json")).unwrap(),
            secret
        );
        drop(replay);
        let mut changed = input.clone();
        changed.name = "other-name".into();
        assert!(matches!(
            PreparedInstallation::prepare(&changed, &root),
            Err(InstallationError::IdentityDrift)
        ));
        assert!(PreparedInstallation::open(&input, &root).is_ok());
    }
    #[test]
    fn private_state_drift_and_foreign_material_are_never_repaired() {
        let (_temp, input, root) = fixture();
        drop(PreparedInstallation::prepare(&input, &root).unwrap());
        let public = root.join("identity.json");
        std::fs::write(&public, b"{\"foreign\":true}").unwrap();
        assert!(matches!(
            PreparedInstallation::open(&input, &root),
            Err(InstallationError::ConfigurationDrift)
        ));
        assert!(PreparedInstallation::prepare(&input, &root).is_err());
        assert_eq!(std::fs::read(public).unwrap(), b"{\"foreign\":true}");
    }
    #[cfg(unix)]
    #[test]
    fn linked_or_exposed_private_state_is_rejected() {
        use std::os::unix::fs::{symlink, PermissionsExt as _};
        let (temp, input, root) = fixture();
        drop(PreparedInstallation::prepare(&input, &root).unwrap());
        let key = root.join("identity-private.json");
        let linked = temp.path().join("second-identity-link");
        std::fs::hard_link(&key, &linked).unwrap();
        assert!(matches!(
            PreparedInstallation::open(&input, &root),
            Err(InstallationError::CredentialInvalid)
        ));
        std::fs::remove_file(linked).unwrap();
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(matches!(
            PreparedInstallation::open(&input, &root),
            Err(InstallationError::CredentialInvalid)
        ));
        std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
        let alias = temp.path().join("alias");
        symlink(&root, &alias).unwrap();
        assert!(matches!(
            PreparedInstallation::open(&input, &alias),
            Err(InstallationError::InvalidPath)
        ));
    }
}
