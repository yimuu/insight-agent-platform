use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use insight_platform_contracts::{canonical_json, parse_strict_json, JsonLimits};
use ring::signature;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::BTreeSet,
    env, fmt, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use uuid::Uuid;

use crate::dev_profile;

const DEFAULT_RELEASE_BASE_URL: &str = "https://github.com/yimuu/insight-agent-platform/releases";
const MAX_BUNDLE_BYTES: usize = 1_048_576;
const MAX_SIGNATURE_BYTES: usize = 16_384;
const MAX_CLI_BYTES: u64 = 268_435_456;
const REQUIRED_TARGETS: [&str; 4] = [
    "aarch64-apple-darwin",
    "aarch64-unknown-linux-gnu",
    "x86_64-apple-darwin",
    "x86_64-unknown-linux-gnu",
];

#[derive(Debug)]
pub struct ReleaseError(String);

impl ReleaseError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ReleaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ReleaseError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseArtifactV1 {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseCliV1 {
    pub target: String,
    pub archive: ReleaseArtifactV1,
    pub binary: ReleaseArtifactV1,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseImagePlatformV1 {
    pub platform: String,
    pub digest: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseImageV1 {
    pub name: String,
    pub subject: String,
    pub index_digest: String,
    pub platforms: Vec<ReleaseImagePlatformV1>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReleaseBundleV1 {
    pub schema_version: u32,
    pub version: String,
    pub git_commit: String,
    pub created_at: String,
    pub contract_digest: String,
    pub profile_schema_digest: String,
    pub development_profile_digest: String,
    pub console: ReleaseArtifactV1,
    pub cli: Vec<ReleaseCliV1>,
    pub images: Vec<ReleaseImageV1>,
    pub metadata: Vec<ReleaseArtifactV1>,
}

#[derive(Debug, Clone)]
pub struct VerifiedRelease {
    pub bundle_digest: String,
    pub version: String,
    pub runtime_image: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReleaseSignatureV1 {
    schema_version: u32,
    algorithm: String,
    key_id: String,
    signature: String,
}

#[derive(Debug, Serialize)]
struct VersionOutput<'a> {
    schema_version: u32,
    version: &'a str,
    target: &'a str,
    git_commit: &'a str,
    release_bundle_digest: &'a str,
}

pub fn validate_exact_version(value: &str) -> Result<(), ReleaseError> {
    let parsed = Version::parse(value)
        .map_err(|_| ReleaseError::new("--version must be an exact semantic version"))?;
    if parsed.to_string() != value || !parsed.pre.is_empty() || !parsed.build.is_empty() {
        return Err(ReleaseError::new(
            "--version must be a normalized release version without pre-release or build metadata",
        ));
    }
    Ok(())
}

pub fn version_output(json: bool) -> String {
    let value = VersionOutput {
        schema_version: 1,
        version: env!("CARGO_PKG_VERSION"),
        target: current_target().unwrap_or("unsupported"),
        git_commit: option_env!("INSIGHT_RELEASE_GIT_COMMIT").unwrap_or("development"),
        release_bundle_digest: option_env!("INSIGHT_RELEASE_BUNDLE_DIGEST")
            .unwrap_or("development"),
    };
    if json {
        return serde_json::to_string_pretty(&value).expect("version output is serializable")
            + "\n";
    }
    format!(
        "insight {}\ntarget {}\ngit_commit {}\nrelease_bundle {}\n",
        value.version, value.target, value.git_commit, value.release_bundle_digest
    )
}

pub fn check_for_update() -> Result<String, ReleaseError> {
    let bundle = fetch_verified_bundle(None)?;
    let current = Version::parse(env!("CARGO_PKG_VERSION"))
        .map_err(|error| ReleaseError::new(format!("invalid embedded CLI version: {error}")))?;
    let available = Version::parse(&bundle.version)
        .map_err(|error| ReleaseError::new(format!("invalid verified release version: {error}")))?;
    if available > current {
        Ok(format!(
            "update available: {} (current {})\napply with: insight update apply --version {}\n",
            available, current, available
        ))
    } else {
        Ok(format!("insight {} is up to date\n", current))
    }
}

pub fn apply_update(version: &str) -> Result<String, ReleaseError> {
    validate_exact_version(version)?;
    let bundle = fetch_verified_bundle(Some(version))?;
    if bundle.version != version {
        return Err(ReleaseError::new(format!(
            "verified ReleaseBundle version {} does not match requested version {version}",
            bundle.version
        )));
    }
    let target = current_target().ok_or_else(|| {
        ReleaseError::new("this operating system and architecture has no supported CLI release")
    })?;
    let cli = bundle
        .cli
        .iter()
        .find(|candidate| candidate.target == target)
        .ok_or_else(|| ReleaseError::new(format!("release {version} does not contain {target}")))?;
    let base = release_asset_base(Some(version))?;
    let maximum = usize::try_from(cli.binary.bytes)
        .ok()
        .filter(|value| *value <= MAX_CLI_BYTES as usize)
        .ok_or_else(|| ReleaseError::new("CLI binary exceeds the release size limit"))?;
    let bytes = fetch_bytes(&asset_url(&base, &cli.binary.path)?, maximum)?;
    let current_executable = env::current_exe().map_err(|error| {
        ReleaseError::new(format!("cannot locate current insight binary: {error}"))
    })?;
    install_verified_binary(&current_executable, &bytes, &cli.binary)?;
    Ok(format!(
        "updated insight to {version} for {target}\nrestart the local profile explicitly with: insight stop && insight dev\n"
    ))
}

fn fetch_verified_bundle(version: Option<&str>) -> Result<ReleaseBundleV1, ReleaseError> {
    let base = release_asset_base(version)?;
    let bundle = fetch_bytes(&asset_url(&base, "release-bundle.json")?, MAX_BUNDLE_BYTES)?;
    let detached = fetch_bytes(
        &asset_url(&base, "release-bundle.signature.json")?,
        MAX_SIGNATURE_BYTES,
    )?;
    verify_release_bundle(&bundle, &detached, &embedded_public_key()?)
}

pub fn load_current_release(
    cache_root: &Path,
    offline: bool,
) -> Result<VerifiedRelease, ReleaseError> {
    // Establish the compiled organization trust root before doing any network or Docker work.
    // Development/source builds deliberately have no release trust root and must fail closed
    // without contacting the release service.
    let public_key = embedded_public_key()?;
    let version = env!("CARGO_PKG_VERSION");
    let cache = cache_root.join("releases").join(version);
    let bundle_path = cache.join("release-bundle.json");
    let signature_path = cache.join("release-bundle.signature.json");
    let (bundle_bytes, signature_bytes) = if offline {
        (
            fs::read(&bundle_path).map_err(|_| {
                ReleaseError::new(format!(
                    "offline release cache misses {}; reconnect and run `insight dev` once",
                    bundle_path.display()
                ))
            })?,
            fs::read(&signature_path).map_err(|_| {
                ReleaseError::new(format!(
                    "offline release cache misses {}; reconnect and run `insight dev` once",
                    signature_path.display()
                ))
            })?,
        )
    } else {
        let base = release_asset_base(Some(version))?;
        let bundle = fetch_bytes(&asset_url(&base, "release-bundle.json")?, MAX_BUNDLE_BYTES)?;
        let signature = fetch_bytes(
            &asset_url(&base, "release-bundle.signature.json")?,
            MAX_SIGNATURE_BYTES,
        )?;
        (bundle, signature)
    };
    let bundle = verify_release_bundle(&bundle_bytes, &signature_bytes, &public_key)?;
    if bundle.version != version {
        return Err(ReleaseError::new(format!(
            "verified release {} does not match CLI version {version}",
            bundle.version
        )));
    }
    if bundle.development_profile_digest
        != dev_profile::registry_content_digest().map_err(ReleaseError::new)?
        || bundle.profile_schema_digest != dev_profile::registry_schema_digest()
    {
        return Err(ReleaseError::new(
            "CLI embedded profile/schema does not match the verified ReleaseBundle",
        ));
    }
    verify_current_executable(&bundle)?;
    if !offline {
        write_release_cache(&cache, &bundle_bytes, &signature_bytes)?;
    }
    let runtime = bundle
        .images
        .iter()
        .find(|image| image.name == "runtime")
        .ok_or_else(|| ReleaseError::new("verified release has no runtime image"))?;
    Ok(VerifiedRelease {
        bundle_digest: digest_bytes(&bundle_bytes),
        version: bundle.version,
        runtime_image: format!("{}@{}", runtime.subject, runtime.index_digest),
    })
}

fn verify_current_executable(bundle: &ReleaseBundleV1) -> Result<(), ReleaseError> {
    let target = current_target().ok_or_else(|| {
        ReleaseError::new("this operating system and architecture has no supported CLI release")
    })?;
    let expected = bundle
        .cli
        .iter()
        .find(|entry| entry.target == target)
        .ok_or_else(|| ReleaseError::new(format!("verified release does not contain {target}")))?;
    let path = env::current_exe().map_err(|error| {
        ReleaseError::new(format!("cannot locate current insight binary: {error}"))
    })?;
    let metadata = fs::symlink_metadata(&path).map_err(|error| {
        ReleaseError::new(format!("cannot inspect current insight binary: {error}"))
    })?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() != expected.binary.bytes
        || metadata.len() > MAX_CLI_BYTES
    {
        return Err(ReleaseError::new(
            "current insight binary does not match the verified release metadata",
        ));
    }
    let bytes = fs::read(&path).map_err(|error| {
        ReleaseError::new(format!("cannot verify current insight binary: {error}"))
    })?;
    if digest_bytes(&bytes) != expected.binary.sha256 {
        return Err(ReleaseError::new(
            "current insight binary digest does not match the verified ReleaseBundle",
        ));
    }
    Ok(())
}

fn write_release_cache(cache: &Path, bundle: &[u8], signature: &[u8]) -> Result<(), ReleaseError> {
    fs::create_dir_all(cache)
        .map_err(|error| ReleaseError::new(format!("cannot create release cache: {error}")))?;
    write_cache_file(&cache.join("release-bundle.json"), bundle)?;
    write_cache_file(&cache.join("release-bundle.signature.json"), signature)
}

fn write_cache_file(path: &Path, bytes: &[u8]) -> Result<(), ReleaseError> {
    let temporary = path.with_extension(format!("tmp-{}", Uuid::now_v7()));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| ReleaseError::new(format!("cannot stage release cache: {error}")))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| ReleaseError::new(format!("cannot persist release cache: {error}")))?;
        fs::rename(&temporary, path)
            .map_err(|error| ReleaseError::new(format!("cannot replace release cache: {error}")))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn release_asset_base(version: Option<&str>) -> Result<String, ReleaseError> {
    let root =
        env::var("INSIGHT_UPDATE_BASE_URL").unwrap_or_else(|_| DEFAULT_RELEASE_BASE_URL.to_owned());
    let root = root.trim_end_matches('/');
    let parsed = reqwest::Url::parse(root)
        .map_err(|error| ReleaseError::new(format!("invalid update base URL: {error}")))?;
    if parsed.scheme() != "https" || parsed.host_str().is_none() {
        return Err(ReleaseError::new(
            "update base URL must be an absolute HTTPS URL",
        ));
    }
    Ok(match version {
        Some(version) => format!("{root}/download/v{version}"),
        None => format!("{root}/latest/download"),
    })
}

fn asset_url(base: &str, path: &str) -> Result<String, ReleaseError> {
    if !safe_asset_path(path) {
        return Err(ReleaseError::new(format!(
            "unsafe release asset path {path:?}"
        )));
    }
    Ok(format!("{base}/{path}"))
}

fn fetch_bytes(url: &str, maximum: usize) -> Result<Vec<u8>, ReleaseError> {
    let client = reqwest::blocking::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(60))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.url().scheme() != "https" {
                attempt.error("release redirect must preserve HTTPS")
            } else if attempt.previous().len() >= 3 {
                attempt.error("release redirect limit exceeded")
            } else {
                attempt.follow()
            }
        }))
        .build()
        .map_err(|error| ReleaseError::new(format!("cannot initialize update client: {error}")))?;
    let response = client
        .get(url)
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .map_err(|error| ReleaseError::new(format!("cannot download {url}: {error}")))?;
    if response
        .content_length()
        .is_some_and(|length| length > maximum as u64)
    {
        return Err(ReleaseError::new(format!(
            "release asset exceeds the {maximum}-byte limit"
        )));
    }
    let mut bytes = Vec::new();
    response
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| ReleaseError::new(format!("cannot read {url}: {error}")))?;
    if bytes.len() > maximum {
        return Err(ReleaseError::new(format!(
            "release asset exceeds the {maximum}-byte limit"
        )));
    }
    Ok(bytes)
}

fn embedded_public_key() -> Result<Vec<u8>, ReleaseError> {
    let encoded = option_env!("INSIGHT_RELEASE_PUBLIC_KEY_BASE64").ok_or_else(|| {
        ReleaseError::new(
            "this development build has no release trust root; install an official signed CLI",
        )
    })?;
    let key = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ReleaseError::new("embedded release trust root is invalid"))?;
    if key.len() != 32 {
        return Err(ReleaseError::new(
            "embedded Ed25519 release trust root must be 32 bytes",
        ));
    }
    Ok(key)
}

pub fn verify_release_bundle(
    bytes: &[u8],
    detached: &[u8],
    public_key: &[u8],
) -> Result<ReleaseBundleV1, ReleaseError> {
    let value = parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: MAX_BUNDLE_BYTES,
            max_depth: 12,
            max_properties_per_object: 32,
            max_items_per_array: 64,
            max_string_bytes: 2_048,
        },
    )
    .map_err(|error| ReleaseError::new(format!("ReleaseBundle is not strict JSON: {error}")))?;
    let canonical = canonical_json(&value)
        .map_err(|error| ReleaseError::new(format!("ReleaseBundle is not canonical: {error}")))?;
    if canonical != bytes {
        return Err(ReleaseError::new(
            "ReleaseBundle bytes are not the canonical signed representation",
        ));
    }
    let signature_value = parse_strict_json(
        detached,
        JsonLimits {
            max_bytes: MAX_SIGNATURE_BYTES,
            max_depth: 4,
            max_properties_per_object: 8,
            max_items_per_array: 1,
            max_string_bytes: 1_024,
        },
    )
    .map_err(|error| ReleaseError::new(format!("release signature is invalid JSON: {error}")))?;
    let detached: ReleaseSignatureV1 = serde_json::from_value(signature_value)
        .map_err(|error| ReleaseError::new(format!("release signature is not closed: {error}")))?;
    if detached.schema_version != 1 || detached.algorithm != "ed25519" {
        return Err(ReleaseError::new("unsupported release signature contract"));
    }
    if public_key.len() != 32 || detached.key_id != digest_bytes(public_key) {
        return Err(ReleaseError::new(
            "release signature key does not match the embedded trust root",
        ));
    }
    let signature_bytes = URL_SAFE_NO_PAD
        .decode(&detached.signature)
        .map_err(|_| ReleaseError::new("release signature is not valid base64url"))?;
    signature::UnparsedPublicKey::new(&signature::ED25519, public_key)
        .verify(bytes, &signature_bytes)
        .map_err(|_| ReleaseError::new("ReleaseBundle signature verification failed"))?;
    let bundle: ReleaseBundleV1 = serde_json::from_value(value)
        .map_err(|error| ReleaseError::new(format!("ReleaseBundle is not closed: {error}")))?;
    validate_bundle(&bundle)?;
    Ok(bundle)
}

fn validate_bundle(bundle: &ReleaseBundleV1) -> Result<(), ReleaseError> {
    if bundle.schema_version != 1 {
        return Err(ReleaseError::new(
            "unsupported ReleaseBundle schema_version",
        ));
    }
    validate_exact_version(&bundle.version)?;
    if bundle.git_commit.len() != 40 || !is_lower_hex(&bundle.git_commit) {
        return Err(ReleaseError::new(
            "ReleaseBundle git_commit must be exact lowercase SHA-1",
        ));
    }
    if bundle.created_at.len() != 27
        || !bundle.created_at.ends_with('Z')
        || !bundle.created_at.is_ascii()
    {
        return Err(ReleaseError::new(
            "ReleaseBundle created_at must be UTC microsecond time",
        ));
    }
    for digest in [
        &bundle.contract_digest,
        &bundle.profile_schema_digest,
        &bundle.development_profile_digest,
    ] {
        validate_digest(digest)?;
    }
    validate_artifact(&bundle.console)?;
    let targets = bundle
        .cli
        .iter()
        .map(|entry| entry.target.as_str())
        .collect::<BTreeSet<_>>();
    if targets != BTreeSet::from(REQUIRED_TARGETS) || bundle.cli.len() != REQUIRED_TARGETS.len() {
        return Err(ReleaseError::new(
            "ReleaseBundle must contain each supported CLI target exactly once",
        ));
    }
    for entry in &bundle.cli {
        if entry.archive.path != format!("insight-{}-{}.tar.gz", bundle.version, entry.target)
            || entry.binary.path != format!("insight-{}-{}", bundle.version, entry.target)
        {
            return Err(ReleaseError::new(format!(
                "CLI asset names for {} do not match the exact release",
                entry.target
            )));
        }
        validate_artifact(&entry.archive)?;
        validate_artifact(&entry.binary)?;
        if entry.binary.bytes > MAX_CLI_BYTES {
            return Err(ReleaseError::new(
                "CLI binary exceeds the release size limit",
            ));
        }
    }
    let image_names = bundle
        .images
        .iter()
        .map(|image| image.name.as_str())
        .collect::<BTreeSet<_>>();
    if image_names != BTreeSet::from(["console", "runtime", "sandbox_guest"])
        || bundle.images.len() != 3
    {
        return Err(ReleaseError::new(
            "ReleaseBundle must bind runtime, sandbox_guest, and console images exactly once",
        ));
    }
    for image in &bundle.images {
        validate_digest(&image.index_digest)?;
        if image.subject.contains(":latest") || image.subject.contains(":candidate-") {
            return Err(ReleaseError::new(
                "release image subject uses a mutable tag",
            ));
        }
        let mut platforms = BTreeSet::new();
        for platform in &image.platforms {
            if !matches!(platform.platform.as_str(), "linux/amd64" | "linux/arm64")
                || !platforms.insert(platform.platform.as_str())
            {
                return Err(ReleaseError::new("release image platform set is invalid"));
            }
            validate_digest(&platform.digest)?;
        }
        if platforms != BTreeSet::from(["linux/amd64", "linux/arm64"]) {
            return Err(ReleaseError::new(
                "each release image must bind linux/amd64 and linux/arm64 child digests",
            ));
        }
    }
    let metadata = bundle
        .metadata
        .iter()
        .map(|artifact| artifact.path.as_str())
        .collect::<BTreeSet<_>>();
    let required_metadata = BTreeSet::from([
        "build-provenance.intoto.jsonl",
        "cli.spdx.json",
        "console.spdx.json",
        "development-profile-v1.json",
        "release-performance.json",
        "runtime.spdx.json",
        "sandbox-guest.spdx.json",
    ]);
    if bundle.metadata.len() != metadata.len() || !required_metadata.is_subset(&metadata) {
        return Err(ReleaseError::new(
            "ReleaseBundle metadata must uniquely bind profile, SBOM, provenance, and performance evidence",
        ));
    }
    for artifact in &bundle.metadata {
        validate_artifact(artifact)?;
    }
    Ok(())
}

fn validate_artifact(artifact: &ReleaseArtifactV1) -> Result<(), ReleaseError> {
    if !safe_asset_path(&artifact.path) || artifact.bytes == 0 {
        return Err(ReleaseError::new("release artifact metadata is invalid"));
    }
    validate_digest(&artifact.sha256)
}

fn validate_digest(value: &str) -> Result<(), ReleaseError> {
    if value.len() != 71 || !value.starts_with("sha256:") || !is_lower_hex(&value[7..]) {
        return Err(ReleaseError::new(format!(
            "invalid exact SHA-256 digest {value:?}"
        )));
    }
    Ok(())
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn safe_asset_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 255
        && value.is_ascii()
        && !value.contains('/')
        && !value.contains('\\')
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn digest_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", lower_hex(&Sha256::digest(bytes)))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        value.push(HEX[(byte >> 4) as usize] as char);
        value.push(HEX[(byte & 15) as usize] as char);
    }
    value
}

fn current_target() -> Option<&'static str> {
    match (env::consts::ARCH, env::consts::OS) {
        ("aarch64", "macos") => Some("aarch64-apple-darwin"),
        ("x86_64", "macos") => Some("x86_64-apple-darwin"),
        ("aarch64", "linux") => Some("aarch64-unknown-linux-gnu"),
        ("x86_64", "linux") => Some("x86_64-unknown-linux-gnu"),
        _ => None,
    }
}

fn install_verified_binary(
    current: &Path,
    bytes: &[u8],
    artifact: &ReleaseArtifactV1,
) -> Result<(), ReleaseError> {
    if bytes.len() as u64 != artifact.bytes || digest_bytes(bytes) != artifact.sha256 {
        return Err(ReleaseError::new(
            "downloaded CLI size or digest does not match the verified ReleaseBundle",
        ));
    }
    let metadata = fs::symlink_metadata(current)
        .map_err(|error| ReleaseError::new(format!("cannot inspect current CLI: {error}")))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(ReleaseError::new(
            "current CLI path must be a regular file, not a symlink",
        ));
    }
    let parent = current
        .parent()
        .ok_or_else(|| ReleaseError::new("current CLI has no installation directory"))?;
    let temporary = temporary_path(parent);
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| ReleaseError::new(format!("cannot stage CLI update: {error}")))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| ReleaseError::new(format!("cannot persist CLI update: {error}")))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            fs::set_permissions(&temporary, fs::Permissions::from_mode(0o755)).map_err(
                |error| ReleaseError::new(format!("cannot mark CLI update executable: {error}")),
            )?;
        }
        fs::rename(&temporary, current).map_err(|error| {
            ReleaseError::new(format!("cannot atomically replace CLI: {error}"))
        })?;
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| ReleaseError::new(format!("cannot sync CLI directory: {error}")))?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn temporary_path(parent: &Path) -> PathBuf {
    parent.join(format!(".insight-update-{}.tmp", Uuid::now_v7()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::signature::{Ed25519KeyPair, KeyPair as _};
    use serde_json::json;
    use tempfile::tempdir;

    fn digest(label: &str) -> String {
        digest_bytes(label.as_bytes())
    }

    fn artifact(path: String, label: &str) -> ReleaseArtifactV1 {
        ReleaseArtifactV1 {
            path,
            bytes: label.len() as u64,
            sha256: digest(label),
        }
    }

    fn bundle() -> ReleaseBundleV1 {
        let version = "1.2.3";
        ReleaseBundleV1 {
            schema_version: 1,
            version: version.to_owned(),
            git_commit: "a".repeat(40),
            created_at: "2026-09-01T00:00:00.000000Z".to_owned(),
            contract_digest: digest("contract"),
            profile_schema_digest: digest("profile-schema"),
            development_profile_digest: digest("profile"),
            console: artifact("console.tar.gz".to_owned(), "console"),
            cli: REQUIRED_TARGETS
                .iter()
                .map(|target| ReleaseCliV1 {
                    target: (*target).to_owned(),
                    archive: artifact(
                        format!("insight-{version}-{target}.tar.gz"),
                        &format!("archive-{target}"),
                    ),
                    binary: artifact(
                        format!("insight-{version}-{target}"),
                        &format!("binary-{target}"),
                    ),
                })
                .collect(),
            images: ["console", "runtime", "sandbox_guest"]
                .iter()
                .map(|name| ReleaseImageV1 {
                    name: (*name).to_owned(),
                    subject: format!("ghcr.io/example/{name}:v{version}"),
                    index_digest: digest(&format!("index-{name}")),
                    platforms: ["linux/amd64", "linux/arm64"]
                        .iter()
                        .map(|platform| ReleaseImagePlatformV1 {
                            platform: (*platform).to_owned(),
                            digest: digest(&format!("{name}-{platform}")),
                        })
                        .collect(),
                })
                .collect(),
            metadata: [
                "build-provenance.intoto.jsonl",
                "cli.spdx.json",
                "console.spdx.json",
                "development-profile-v1.json",
                "release-performance.json",
                "runtime.spdx.json",
                "sandbox-guest.spdx.json",
            ]
            .iter()
            .map(|path| artifact((*path).to_owned(), path))
            .collect(),
        }
    }

    fn signed(bundle: &ReleaseBundleV1) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let bytes = canonical_json(&serde_json::to_value(bundle).unwrap()).unwrap();
        let key = Ed25519KeyPair::from_seed_unchecked(&[7_u8; 32]).unwrap();
        let public = key.public_key().as_ref().to_vec();
        let detached = serde_json::to_vec(&json!({
            "schema_version": 1,
            "algorithm": "ed25519",
            "key_id": digest_bytes(&public),
            "signature": URL_SAFE_NO_PAD.encode(key.sign(&bytes).as_ref()),
        }))
        .unwrap();
        (bytes, detached, public)
    }

    #[test]
    fn signed_bundle_closes_all_release_subjects() {
        let expected = bundle();
        let (bytes, detached, public) = signed(&expected);
        assert_eq!(
            verify_release_bundle(&bytes, &detached, &public).unwrap(),
            expected
        );
    }

    #[test]
    fn tamper_wrong_arch_and_mutable_tag_fail_closed() {
        let expected = bundle();
        let (mut bytes, detached, public) = signed(&expected);
        bytes[10] ^= 1;
        assert!(verify_release_bundle(&bytes, &detached, &public).is_err());

        let mut wrong_arch = bundle();
        wrong_arch.cli.pop();
        let (bytes, detached, public) = signed(&wrong_arch);
        assert!(verify_release_bundle(&bytes, &detached, &public).is_err());

        let mut mutable = bundle();
        mutable.images[0].subject = "ghcr.io/example/console:latest".to_owned();
        let (bytes, detached, public) = signed(&mutable);
        assert!(verify_release_bundle(&bytes, &detached, &public).is_err());
    }

    #[test]
    fn duplicate_json_key_and_noncanonical_bundle_fail_closed() {
        let (_, detached, public) = signed(&bundle());
        assert!(verify_release_bundle(
            br#"{"schema_version":1,"schema_version":1}"#,
            &detached,
            &public
        )
        .is_err());
        let pretty = serde_json::to_vec_pretty(&bundle()).unwrap();
        assert!(verify_release_bundle(&pretty, &detached, &public).is_err());
    }

    #[test]
    fn atomic_install_never_replaces_on_digest_mismatch() {
        let directory = tempdir().unwrap();
        let current = directory.path().join("insight");
        fs::write(&current, b"old").unwrap();
        let metadata = artifact("insight".to_owned(), "new");
        assert!(install_verified_binary(&current, b"tampered", &metadata).is_err());
        assert_eq!(fs::read(&current).unwrap(), b"old");
        install_verified_binary(&current, b"new", &metadata).unwrap();
        assert_eq!(fs::read(&current).unwrap(), b"new");
    }

    #[test]
    fn exact_release_version_rejects_latest_and_prereleases() {
        assert!(validate_exact_version("1.2.3").is_ok());
        for invalid in ["latest", "v1.2.3", "1.2.3-beta.1", "1.2.3+build"] {
            assert!(validate_exact_version(invalid).is_err(), "{invalid}");
        }
    }
}
