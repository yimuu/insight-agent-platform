//! One-shot S3 bucket configuration. The caller owns the exclusive private directory lock.
//! A Requested phase authorizes only exact readback, never another external write.
use insight_platform_contracts::{parse_strict_json, JsonLimits, ResourceId, Sha256Digest};
use insight_platform_deployment_contracts::installation::{
    InstallationError as Error, InstallationIdentityV1, InstallationInputV1,
};
use insight_platform_deployment_tooling::{
    private_state::InstallationDirectory,
    s3_profile::{S3IdentityRole, S3RoleCredentials},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, ffi::OsStr, time::Duration};

const JOURNAL_FILE: &str = "s3-setup.json";
const JOURNAL_LIMIT: usize = 16_384;
const REGION: &str = "us-east-1";
const OWNER_TAG: &str = "insight-installation-id";
const IDENTITY_TAG: &str = "insight-installation-identity";
const CORS_ID: &str = "insight-installation-upload-v1";
const INITIAL_OBSERVATION_TIMEOUT: Duration = Duration::from_secs(30);
const LIMITS: JsonLimits = JsonLimits {
    max_bytes: JOURNAL_LIMIT,
    max_depth: 4,
    max_properties_per_object: 12,
    max_items_per_array: 4,
    max_string_bytes: 256,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Mode {
    Provision,
    Verify,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Planned,
    CreateRequested,
    Created,
    TagRequested,
    Tagged,
    VersionRequested,
    Versioned,
    CorsRequested,
    Configured,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    input_digest: Sha256Digest,
    identity_digest: Sha256Digest,
    installation_id: ResourceId,
    credentials_digest: Sha256Digest,
    bucket: String,
    phase: Phase,
}

fn bytes_digest(bytes: &[u8]) -> Result<Sha256Digest, Error> {
    format!(
        "sha256:{}",
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
    .parse()
    .map_err(|_| Error::InvalidInput)
}

impl Journal {
    fn create(
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
        credentials_digest: Sha256Digest,
    ) -> Result<Self, Error> {
        Ok(Self {
            schema_version: 1,
            input_digest: input.digest()?,
            identity_digest: identity.digest()?,
            installation_id: identity.installation_id.clone(),
            credentials_digest,
            bucket: format!(
                "insight-platform-artifacts-{}",
                identity.installation_id.uuid().simple()
            ),
            phase: Phase::Planned,
        })
    }
    fn validate(
        &self,
        input: &InstallationInputV1,
        identity: &InstallationIdentityV1,
        credentials: &Sha256Digest,
    ) -> Result<(), Error> {
        if self.schema_version != 1
            || self.input_digest != input.digest()?
            || self.identity_digest != identity.digest()?
            || self.installation_id != identity.installation_id
            || identity.input_digest != self.input_digest
            || self.credentials_digest != *credentials
            || self.bucket
                != format!(
                    "insight-platform-artifacts-{}",
                    identity.installation_id.uuid().simple()
                )
        {
            return Err(Error::IdentityDrift);
        }
        Ok(())
    }
    fn tags(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (OWNER_TAG.into(), self.installation_id.to_string()),
            (IDENTITY_TAG.into(), self.identity_digest.to_string()),
        ])
    }
    fn save(&self, directory: &InstallationDirectory) -> Result<(), Error> {
        let bytes = serde_json::to_vec(self).map_err(|_| Error::InvalidInput)?;
        if bytes.len() > JOURNAL_LIMIT {
            return Err(Error::InvalidInput);
        }
        directory.replace(JOURNAL_FILE, &bytes)
    }
    fn advance(&mut self, directory: &InstallationDirectory, phase: Phase) -> Result<(), Error> {
        self.phase = phase;
        self.save(directory)
    }
}

#[derive(Clone, PartialEq, Eq)]
struct Cors {
    id: String,
    origins: Vec<String>,
    methods: Vec<String>,
    headers: Vec<String>,
    expose: Vec<String>,
    max_age: Option<i32>,
}
impl Cors {
    fn expected(input: &InstallationInputV1) -> Self {
        Self {
            id: CORS_ID.into(),
            origins: vec![input.network.console_origin.as_str().into()],
            methods: vec!["PUT".into()],
            headers: vec!["content-type".into()],
            expose: vec![],
            max_age: Some(0),
        }
    }
}
#[derive(Clone)]
struct Bucket {
    tags: Option<BTreeMap<String, String>>,
    version: Option<String>,
    cors: Option<Vec<Cors>>,
}
trait Api {
    async fn bucket(&self, name: &str) -> Result<Option<Bucket>, Error>;
    async fn create_bucket(&self, name: &str) -> Result<(), Error>;
    async fn tag_bucket(&self, name: &str, tags: &BTreeMap<String, String>) -> Result<(), Error>;
    async fn version_bucket(&self, name: &str) -> Result<(), Error>;
    async fn cors_bucket(&self, name: &str, cors: &Cors) -> Result<(), Error>;
}

pub(crate) fn tls_environment(
    directory: &InstallationDirectory,
    identity: &InstallationIdentityV1,
    certificate_file: Option<&OsStr>,
    certificate_directory: Option<&OsStr>,
) -> Result<(), Error> {
    if certificate_file != Some(directory.path("ca.pem")?.as_os_str())
        || certificate_directory != Some(OsStr::new("/etc/ssl/certs"))
    {
        return Err(Error::CredentialInvalid);
    }
    let ca = directory.read("ca.pem", 16_384)?.ok_or(Error::Incomplete)?;
    if bytes_digest(&ca)? != identity.certificate_authority_digest {
        return Err(Error::IdentityDrift);
    }
    Ok(())
}

pub async fn ensure_s3(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    directory: &InstallationDirectory,
    mode: Mode,
) -> Result<String, Error> {
    input.validate()?;
    identity.validate()?;
    if identity.input_digest != input.digest()? {
        return Err(Error::IdentityDrift);
    }
    tls_environment(
        directory,
        identity,
        std::env::var_os("SSL_CERT_FILE").as_deref(),
        std::env::var_os("SSL_CERT_DIR").as_deref(),
    )?;
    for name in [
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
        "AWS_CA_BUNDLE",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        if std::env::var_os(name).is_some() {
            return Err(Error::CredentialInvalid);
        }
    }
    let raw = zeroize::Zeroizing::new(
        directory
            .read(S3IdentityRole::Initializer.credential_filename(), 256)?
            .ok_or(Error::Incomplete)?,
    );
    let credentials = S3RoleCredentials::decode(&raw)?;
    let sdk = Sdk::new(input, &credentials);
    ensure_with_api(input, identity, directory, bytes_digest(&raw)?, mode, &sdk).await
}

fn verify(state: &Journal, bucket: &Bucket, cors: &Cors) -> Result<(), Error> {
    if bucket.tags.as_ref() != Some(&state.tags()) {
        return Err(Error::ForeignState);
    }
    if bucket.version.as_deref() != Some("Enabled")
        || bucket.cors.as_deref() != Some(std::slice::from_ref(cors))
    {
        return Err(Error::ConfigurationDrift);
    }
    Ok(())
}

async fn ensure_with_api<A: Api>(
    input: &InstallationInputV1,
    identity: &InstallationIdentityV1,
    directory: &InstallationDirectory,
    credentials: Sha256Digest,
    mode: Mode,
    api: &A,
) -> Result<String, Error> {
    input.validate()?;
    identity.validate()?;
    let mut state = match directory.read(JOURNAL_FILE, JOURNAL_LIMIT)? {
        Some(bytes) => serde_json::from_value::<Journal>(
            parse_strict_json(&bytes, LIMITS).map_err(|_| Error::InvalidInput)?,
        )
        .map_err(|_| Error::InvalidInput)?,
        None if mode == Mode::Provision => {
            let state = Journal::create(input, identity, credentials.clone())?;
            state.validate(input, identity, &credentials)?;
            state.save(directory)?;
            state
        }
        None => return Err(Error::Incomplete),
    };
    state.validate(input, identity, &credentials)?;
    if mode == Mode::Verify && state.phase != Phase::Configured {
        return Err(Error::Incomplete);
    }
    let expected = Cors::expected(input);
    let mut bucket = observe_initial_bucket(api, &state.bucket).await?;
    if mode == Mode::Verify || state.phase == Phase::Configured {
        verify(&state, &bucket.ok_or(Error::ConfigurationDrift)?, &expected)?;
        return Ok(state.bucket);
    }
    if state.phase == Phase::Planned {
        if bucket.is_some() {
            return Err(Error::ForeignState);
        }
        state.advance(directory, Phase::CreateRequested)?;
        api.create_bucket(&state.bucket).await?;
        state.advance(directory, Phase::Created)?;
        bucket = api.bucket(&state.bucket).await?;
    } else if state.phase == Phase::CreateRequested {
        // CreateBucket cannot carry our tags atomically. Untagged or absent after response loss
        // is not ownership evidence, even for this nonce: never recreate or adopt it.
        if bucket.as_ref().and_then(|value| value.tags.as_ref()) != Some(&state.tags()) {
            return Err(Error::ExternalOutcomeUnknown);
        }
        state.advance(directory, Phase::Tagged)?;
    }
    let mut bucket = bucket.ok_or(Error::ExternalOutcomeUnknown)?;
    if matches!(state.phase, Phase::Created | Phase::TagRequested) {
        match &bucket.tags {
            Some(tags) if tags != &state.tags() => return Err(Error::ForeignState),
            Some(_) => {}
            None if state.phase == Phase::TagRequested => {
                return Err(Error::ExternalOutcomeUnknown)
            }
            None => {
                state.advance(directory, Phase::TagRequested)?;
                api.tag_bucket(&state.bucket, &state.tags()).await?;
            }
        }
        bucket = api
            .bucket(&state.bucket)
            .await?
            .ok_or(Error::ExternalOutcomeUnknown)?;
        if bucket.tags.as_ref() != Some(&state.tags()) {
            return Err(Error::ForeignState);
        }
        state.advance(directory, Phase::Tagged)?;
    }
    if bucket.tags.as_ref() != Some(&state.tags()) {
        return Err(Error::ForeignState);
    }
    if matches!(state.phase, Phase::Tagged | Phase::VersionRequested) {
        match bucket.version.as_deref() {
            Some("Enabled") => {}
            Some(_) => return Err(Error::ConfigurationDrift),
            None if state.phase == Phase::VersionRequested => {
                return Err(Error::ExternalOutcomeUnknown)
            }
            None => {
                state.advance(directory, Phase::VersionRequested)?;
                api.version_bucket(&state.bucket).await?;
            }
        }
        bucket = api
            .bucket(&state.bucket)
            .await?
            .ok_or(Error::ExternalOutcomeUnknown)?;
        if bucket.version.as_deref() != Some("Enabled") {
            return Err(Error::ConfigurationDrift);
        }
        state.advance(directory, Phase::Versioned)?;
    }
    if matches!(state.phase, Phase::Versioned | Phase::CorsRequested) {
        match &bucket.cors {
            Some(cors) if cors.as_slice() != std::slice::from_ref(&expected) => {
                return Err(Error::ConfigurationDrift)
            }
            Some(_) => {}
            None if state.phase == Phase::CorsRequested => {
                return Err(Error::ExternalOutcomeUnknown)
            }
            None => {
                state.advance(directory, Phase::CorsRequested)?;
                api.cors_bucket(&state.bucket, &expected).await?;
            }
        }
        bucket = api
            .bucket(&state.bucket)
            .await?
            .ok_or(Error::ExternalOutcomeUnknown)?;
        verify(&state, &bucket, &expected)?;
        state.advance(directory, Phase::Configured)?;
    }
    verify(&state, &bucket, &expected)?;
    Ok(state.bucket)
}

async fn observe_initial_bucket<A: Api>(api: &A, name: &str) -> Result<Option<Bucket>, Error> {
    let deadline = tokio::time::Instant::now() + INITIAL_OBSERVATION_TIMEOUT;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::PrerequisiteUnavailable);
        }
        // Only the first read waits for startup. Writes and their exact readbacks retain the
        // original journal rules and never enter this loop, including after response loss.
        let result = tokio::time::timeout_at(deadline, api.bucket(name))
            .await
            .map_err(|_| Error::PrerequisiteUnavailable)?;
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::PrerequisiteUnavailable);
        }
        match result {
            Err(Error::PrerequisiteUnavailable) => {
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + Duration::from_secs(1)),
                )
                .await;
            }
            result => return result,
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    HeadBucket,
    GetBucketTagging,
    GetBucketVersioning,
    GetBucketCors,
    CreateBucket,
    PutBucketTagging,
    PutBucketVersioning,
    PutBucketCors,
}
impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::HeadBucket => "s3_head_bucket",
            Self::GetBucketTagging => "s3_get_bucket_tagging",
            Self::GetBucketVersioning => "s3_get_bucket_versioning",
            Self::GetBucketCors => "s3_get_bucket_cors",
            Self::CreateBucket => "s3_create_bucket",
            Self::PutBucketTagging => "s3_put_bucket_tagging",
            Self::PutBucketVersioning => "s3_put_bucket_versioning",
            Self::PutBucketCors => "s3_put_bucket_cors",
        }
    }
}
fn diagnostic<E, R>(operation: Operation, error: &aws_sdk_s3::error::SdkError<E, R>) -> String {
    use aws_sdk_s3::error::SdkError;
    let failure = match error {
        SdkError::TimeoutError(_) => "timeout",
        SdkError::DispatchFailure(context) if context.is_timeout() => "timeout",
        SdkError::DispatchFailure(_) => "dispatch",
        SdkError::ServiceError(_) => "service",
        SdkError::ResponseError(_) => "invalid_response",
        _ => "unknown",
    };
    format!(
        "installation_s3 operation={} failure={failure}",
        operation.name()
    )
}
fn failure<E, R>(
    operation: Operation,
    error: &aws_sdk_s3::error::SdkError<E, R>,
    outcome: Error,
) -> Error {
    eprintln!("{}", diagnostic(operation, error));
    outcome
}

struct Sdk {
    client: aws_sdk_s3::Client,
}
impl Sdk {
    fn new(input: &InstallationInputV1, credentials: &S3RoleCredentials) -> Self {
        let credentials = credentials.with_keys(|access, secret| {
            aws_sdk_s3::config::Credentials::new(
                access,
                secret,
                None,
                None,
                "installation-s3-initializer",
            )
        });
        Self {
            client: aws_sdk_s3::Client::from_conf(
                aws_sdk_s3::Config::builder()
                    .behavior_version_latest()
                    .region(aws_sdk_s3::config::Region::new(REGION))
                    .credentials_provider(credentials)
                    .retry_config(aws_config::retry::RetryConfig::standard().with_max_attempts(1))
                    .timeout_config(
                        aws_config::timeout::TimeoutConfig::builder()
                            .connect_timeout(Duration::from_secs(3))
                            .read_timeout(Duration::from_secs(10))
                            .operation_timeout(Duration::from_secs(10))
                            .operation_attempt_timeout(Duration::from_secs(10))
                            .build(),
                    )
                    .endpoint_url(input.network.providers.artifact().as_str())
                    .force_path_style(true)
                    .build(),
            ),
        }
    }
}

impl Api for Sdk {
    async fn bucket(&self, name: &str) -> Result<Option<Bucket>, Error> {
        use aws_sdk_s3::error::ProvideErrorMetadata as _;
        match self.client.head_bucket().bucket(name).send().await {
            Ok(_) => {}
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_not_found()) =>
            {
                return Ok(None)
            }
            Err(error) => {
                return Err(failure(
                    Operation::HeadBucket,
                    &error,
                    Error::PrerequisiteUnavailable,
                ))
            }
        }
        let tags = match self.client.get_bucket_tagging().bucket(name).send().await {
            Ok(output) => {
                if !matches!(output.tag_set().len(), 0 | 2) {
                    return Err(Error::ForeignState);
                }
                let mut tags = BTreeMap::new();
                for tag in output.tag_set() {
                    if !matches!(tag.key(), OWNER_TAG | IDENTITY_TAG)
                        || tag.value().len() > 256
                        || tags
                            .insert(tag.key().to_owned(), tag.value().to_owned())
                            .is_some()
                    {
                        return Err(Error::ForeignState);
                    }
                }
                (!tags.is_empty()).then_some(tags)
            }
            Err(error)
                if error.as_service_error().and_then(|error| error.code())
                    == Some("NoSuchTagSet") =>
            {
                None
            }
            Err(error) => {
                return Err(failure(
                    Operation::GetBucketTagging,
                    &error,
                    Error::PrerequisiteUnavailable,
                ))
            }
        };
        let version = self
            .client
            .get_bucket_versioning()
            .bucket(name)
            .send()
            .await
            .map_err(|error| {
                failure(
                    Operation::GetBucketVersioning,
                    &error,
                    Error::PrerequisiteUnavailable,
                )
            })?;
        if version
            .mfa_delete()
            .is_some_and(|value| value.as_str() != "Disabled")
        {
            return Err(Error::ConfigurationDrift);
        }
        let cors = match self.client.get_bucket_cors().bucket(name).send().await {
            Ok(output) => {
                if output.cors_rules().len() != 1 {
                    return Err(Error::ConfigurationDrift);
                }
                let rule = &output.cors_rules()[0];
                if rule.id().is_some_and(|id| id.len() > 128)
                    || rule.allowed_origins().len() != 1
                    || rule.allowed_methods().len() != 1
                    || rule.allowed_headers().len() != 1
                    || !rule.expose_headers().is_empty()
                    || rule
                        .allowed_origins()
                        .iter()
                        .chain(rule.allowed_methods())
                        .chain(rule.allowed_headers())
                        .any(|value| value.len() > 2048)
                {
                    return Err(Error::ConfigurationDrift);
                }
                Some(vec![Cors {
                    id: rule.id().unwrap_or_default().into(),
                    origins: rule.allowed_origins().into(),
                    methods: rule.allowed_methods().into(),
                    headers: rule.allowed_headers().into(),
                    expose: rule.expose_headers().into(),
                    max_age: rule.max_age_seconds(),
                }])
            }
            Err(error)
                if error.as_service_error().and_then(|error| error.code())
                    == Some("NoSuchCORSConfiguration") =>
            {
                None
            }
            Err(error) => {
                return Err(failure(
                    Operation::GetBucketCors,
                    &error,
                    Error::PrerequisiteUnavailable,
                ))
            }
        };
        Ok(Some(Bucket {
            tags,
            version: version.status().map(|value| value.as_str().into()),
            cors,
        }))
    }
    async fn create_bucket(&self, name: &str) -> Result<(), Error> {
        self.client
            .create_bucket()
            .bucket(name)
            .send()
            .await
            .map_err(|error| {
                failure(
                    Operation::CreateBucket,
                    &error,
                    Error::ExternalOutcomeUnknown,
                )
            })?;
        Ok(())
    }
    async fn tag_bucket(&self, name: &str, tags: &BTreeMap<String, String>) -> Result<(), Error> {
        let tags = tags
            .iter()
            .map(|(key, value)| {
                aws_sdk_s3::types::Tag::builder()
                    .key(key)
                    .value(value)
                    .build()
                    .map_err(|_| Error::InvalidInput)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let tagging = aws_sdk_s3::types::Tagging::builder()
            .set_tag_set(Some(tags))
            .build()
            .map_err(|_| Error::InvalidInput)?;
        self.client
            .put_bucket_tagging()
            .bucket(name)
            .tagging(tagging)
            .send()
            .await
            .map_err(|error| {
                failure(
                    Operation::PutBucketTagging,
                    &error,
                    Error::ExternalOutcomeUnknown,
                )
            })?;
        Ok(())
    }
    async fn version_bucket(&self, name: &str) -> Result<(), Error> {
        self.client
            .put_bucket_versioning()
            .bucket(name)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .map_err(|error| {
                failure(
                    Operation::PutBucketVersioning,
                    &error,
                    Error::ExternalOutcomeUnknown,
                )
            })?;
        Ok(())
    }
    async fn cors_bucket(&self, name: &str, cors: &Cors) -> Result<(), Error> {
        let rule = aws_sdk_s3::types::CorsRule::builder()
            .id(&cors.id)
            .set_allowed_origins(Some(cors.origins.clone()))
            .set_allowed_methods(Some(cors.methods.clone()))
            .set_allowed_headers(Some(cors.headers.clone()))
            .set_max_age_seconds(cors.max_age)
            .build()
            .map_err(|_| Error::InvalidInput)?;
        let configuration = aws_sdk_s3::types::CorsConfiguration::builder()
            .cors_rules(rule)
            .build()
            .map_err(|_| Error::InvalidInput)?;
        self.client
            .put_bucket_cors()
            .bucket(name)
            .cors_configuration(configuration)
            .send()
            .await
            .map_err(|error| {
                failure(
                    Operation::PutBucketCors,
                    &error,
                    Error::ExternalOutcomeUnknown,
                )
            })?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "s3_setup_tests.rs"]
mod tests;
