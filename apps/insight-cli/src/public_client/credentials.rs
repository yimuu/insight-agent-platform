//! Shared private token reading. Unverified claim checks only reject obvious misuse.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use insight_platform_contracts::{parse_strict_json, JsonLimits, ResourceId, ResourceKind};
use std::path::Path;

pub(super) fn reject_obvious_session_mismatch(
    token: &str,
    expected: &ResourceId,
) -> Result<(), String> {
    let invalid = || {
        "session token does not match the configured Tenant or has expired; explicitly renew the installation session".to_owned()
    };
    let mut parts = token.split('.');
    let header = parts.next().filter(|s| !s.is_empty()).ok_or_else(invalid)?;
    let payload = parts.next().filter(|s| !s.is_empty()).ok_or_else(invalid)?;
    let signature = parts.next().filter(|s| !s.is_empty()).ok_or_else(invalid)?;
    if parts.next().is_some()
        || expected.kind() != ResourceKind::Tenant
        || ![header, payload, signature].iter().all(|part| {
            part.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        })
    {
        return Err(invalid());
    }
    let mut bytes = URL_SAFE_NO_PAD.decode(payload).map_err(|_| invalid())?;
    let parsed = parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: 49_152,
            max_depth: 2,
            max_properties_per_object: 16,
            max_items_per_array: 1,
            max_string_bytes: 4096,
        },
    );
    bytes.fill(0);
    let value = parsed.map_err(|_| invalid())?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| invalid())?
        .as_secs();
    if value.get("tenant_id").and_then(|v| v.as_str()) != Some(expected.to_string().as_str())
        || value
            .get("exp")
            .and_then(|v| v.as_u64())
            .is_none_or(|expiry| expiry <= now)
    {
        return Err(invalid());
    }
    Ok(())
}

pub(super) fn read_private_token(path: &Path) -> Result<String, String> {
    use std::io::Read;
    let metadata = std::fs::symlink_metadata(path).map_err(|_| "cannot read session token file")?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > 65_537
    {
        return Err("session token must be a bounded private regular file".to_owned());
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        if metadata.nlink() != 1 || metadata.mode() & 0o777 != 0o600 {
            return Err("session token permissions must be 0600 with one link".to_owned());
        }
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|_| "cannot safely open session token")?;
    let opened = file
        .metadata()
        .map_err(|_| "cannot inspect session token")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if opened.ino() != metadata.ino()
            || opened.dev() != metadata.dev()
            || opened.nlink() != 1
            || opened.mode() & 0o777 != 0o600
        {
            return Err("session token changed during read".to_owned());
        }
    }
    let mut value = Vec::new();
    file.take(65_538)
        .read_to_end(&mut value)
        .map_err(|_| "cannot read session token")?;
    if value.last() == Some(&b'\n') {
        value.pop();
    }
    if value.is_empty() || value.len() > 65_536 || !value.iter().all(u8::is_ascii_graphic) {
        value.fill(0);
        return Err("session token file is invalid".to_owned());
    }
    String::from_utf8(value).map_err(|_| "session token encoding is invalid".to_owned())
}
