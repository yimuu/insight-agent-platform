//! Explicit invocation-local trust; never a persisted model or credential fact.
use std::{fs::OpenOptions, io::Read, path::Path};

pub(super) fn read_bundle(path: &Path) -> Result<Vec<reqwest::Certificate>, String> {
    let before = std::fs::symlink_metadata(path).map_err(|_| "cannot inspect public CA bundle")?;
    if !before.is_file() || before.len() == 0 || before.len() > 65_536 {
        return Err("public CA bundle must be a bounded regular file".into());
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|_| "cannot open public CA bundle")?;
    let after = file
        .metadata()
        .map_err(|_| "cannot inspect opened CA bundle")?;
    if !after.is_file() {
        return Err("public CA bundle must be regular".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if before.ino() != after.ino() || before.dev() != after.dev() || after.nlink() != 1 {
            return Err("public CA bundle changed during opening or has multiple links".into());
        }
    }
    let mut bytes = Vec::new();
    file.take(65_537)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read public CA bundle")?;
    if bytes.len() > 65_536 {
        return Err("public CA bundle exceeds its byte bound".into());
    }
    parse_bundle(&bytes)
}

fn parse_bundle(bytes: &[u8]) -> Result<Vec<reqwest::Certificate>, String> {
    let mut text = std::str::from_utf8(bytes)
        .map_err(|_| "public CA bundle must be PEM")?
        .trim();
    let mut count = 0;
    while !text.is_empty() {
        text = text
            .strip_prefix("-----BEGIN CERTIFICATE-----")
            .ok_or("CA bundle accepts only public certificate PEM blocks")?;
        let (content, rest) = text
            .split_once("-----END CERTIFICATE-----")
            .ok_or("CA bundle certificate is incomplete")?;
        if content.trim().is_empty()
            || !content.bytes().all(|b| {
                b.is_ascii_alphanumeric()
                    || b.is_ascii_whitespace()
                    || matches!(b, b'+' | b'/' | b'=')
            })
        {
            return Err("CA bundle certificate encoding is invalid".into());
        }
        count += 1;
        if count > 16 {
            return Err("CA bundle contains too many certificates".into());
        }
        text = rest.trim();
    }
    if count == 0 {
        return Err("CA bundle contains no certificates".into());
    }
    let roots = reqwest::Certificate::from_pem_bundle(bytes)
        .map_err(|_| "CA bundle certificate is invalid")?;
    if roots.len() != count {
        return Err("CA bundle certificate count is invalid".into());
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn public_trust_rejects_private_material_and_ambiguous_files() {
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let public = certificate.cert.pem();
        assert_eq!(parse_bundle(public.as_bytes()).unwrap().len(), 1);
        for bytes in [
            Vec::new(),
            b"not PEM".to_vec(),
            certificate.signing_key.serialize_pem().into_bytes(),
            format!("{public}garbage").into_bytes(),
            public.repeat(17).into_bytes(),
        ] {
            assert!(parse_bundle(&bytes).is_err());
        }
        #[cfg(unix)]
        {
            let directory =
                std::env::temp_dir().join(format!("insight-model-trust-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&directory).unwrap();
            let file = directory.join("public.pem");
            std::fs::write(&file, public).unwrap();
            assert_eq!(read_bundle(&file).unwrap().len(), 1);
            let link = directory.join("link.pem");
            std::os::unix::fs::symlink(&file, &link).unwrap();
            assert!(read_bundle(&link).is_err());
            std::fs::remove_file(link).unwrap();
            std::fs::hard_link(&file, directory.join("other.pem")).unwrap();
            assert!(read_bundle(&file).is_err());
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}
