use crate::{
    client::{encode, limits},
    sensitive::SensitiveJson,
    BaoClient, BaoError, BaoSecretPath, KvV2BindingV1, SensitiveBytes, MAX_PROVIDER_REQUEST_BYTES,
};
use insight_platform_contracts::parse_strict_json;
use reqwest::Method;
use serde_json::Value;
use std::collections::BTreeMap;
use tokio::time::Instant;

#[derive(Debug)]
pub struct KvRead {
    pub version: u64,
    pub bytes: SensitiveBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvVersionMetadata {
    pub destroyed: bool,
    pub deletion_time: String,
    pub created_time: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvMetadata {
    pub current_version: u64,
    pub versions: BTreeMap<u64, KvVersionMetadata>,
}

impl BaoClient {
    pub async fn check_kv(
        &self,
        binding: &KvV2BindingV1,
        caller_deadline: Instant,
    ) -> Result<(), BaoError> {
        binding.validate_for(self.config())?;
        let deadline = self.deadline(caller_deadline)?;
        self.check_mount(
            &binding.mount,
            &binding.mount_accessor,
            "kv",
            Some("2"),
            deadline,
        )
        .await
    }

    pub async fn read_exact(
        &self,
        binding: &KvV2BindingV1,
        path: &BaoSecretPath,
        version: u64,
        caller_deadline: Instant,
    ) -> Result<KvRead, BaoError> {
        check_version(version)?;
        let deadline = self.deadline(caller_deadline)?;
        self.check_kv(binding, deadline).await?;
        let response = self
            .authorized(
                Method::GET,
                &format!("{}/data/{}?version={version}", binding.mount, path.as_str()),
                None,
                false,
                deadline,
            )
            .await?;
        decode_read(&response.0, version)
    }

    /// Only creation of one dedicated path. No caller-supplied CAS, update or latest write exists.
    pub async fn create_only(
        &self,
        binding: &KvV2BindingV1,
        path: &BaoSecretPath,
        json: &[u8],
        caller_deadline: Instant,
    ) -> Result<u64, BaoError> {
        if json.len() > 64 * 1024 {
            return Err(BaoError::InvalidConfig);
        }
        let value = parse_strict_json(json, limits(MAX_PROVIDER_REQUEST_BYTES))
            .map_err(|_| BaoError::InvalidConfig)?;
        let mut value = SensitiveJson(value);
        if !value.0.is_object() {
            return Err(BaoError::InvalidConfig);
        }
        let body = encode(serde_json::json!({ "options": {"cas": 0}, "data": value.0.take() }))?;
        let deadline = self.deadline(caller_deadline)?;
        self.check_kv(binding, deadline).await?;
        let response = self
            .authorized(
                Method::POST,
                &format!("{}/data/{}", binding.mount, path.as_str()),
                Some(body),
                true,
                deadline,
            )
            .await?;
        // A response to a possibly executed write cannot prove absence on malformed evidence.
        if response.0.pointer("/data/version").and_then(Value::as_u64) != Some(1) {
            return Err(BaoError::UnknownOutcome);
        }
        Ok(1)
    }

    pub async fn metadata(
        &self,
        binding: &KvV2BindingV1,
        path: &BaoSecretPath,
        caller_deadline: Instant,
    ) -> Result<KvMetadata, BaoError> {
        let deadline = self.deadline(caller_deadline)?;
        self.check_kv(binding, deadline).await?;
        self.metadata_after_identity(binding, path, deadline).await
    }

    async fn metadata_after_identity(
        &self,
        binding: &KvV2BindingV1,
        path: &BaoSecretPath,
        deadline: Instant,
    ) -> Result<KvMetadata, BaoError> {
        let response = self
            .authorized(
                Method::GET,
                &format!("{}/metadata/{}", binding.mount, path.as_str()),
                None,
                false,
                deadline,
            )
            .await?;
        decode_metadata(&response.0)
    }

    /// Permanent removal of exactly one version, retaining the provider's tombstone.
    /// A success return includes the subsequent exact metadata observation, not only HTTP 204.
    pub async fn destroy_exact(
        &self,
        binding: &KvV2BindingV1,
        path: &BaoSecretPath,
        version: u64,
        caller_deadline: Instant,
    ) -> Result<(), BaoError> {
        check_version(version)?;
        let deadline = self.deadline(caller_deadline)?;
        self.check_kv(binding, deadline).await?;
        let before = self
            .metadata_after_identity(binding, path, deadline)
            .await?;
        let existing = before
            .versions
            .get(&version)
            .ok_or(BaoError::InvalidEvidence)?;
        if existing.destroyed {
            return Ok(());
        }
        let body = encode(serde_json::json!({"versions": [version]}))?;
        let result = self
            .authorized(
                Method::POST,
                &format!("{}/destroy/{}", binding.mount, path.as_str()),
                Some(body),
                true,
                deadline,
            )
            .await;
        match result {
            Ok(_) | Err(BaoError::UnknownOutcome) => {}
            Err(error) => return Err(error),
        }
        let observed = self
            .metadata_after_identity(binding, path, deadline)
            .await
            .map_err(|_| BaoError::UnknownOutcome)?;
        if observed
            .versions
            .get(&version)
            .is_none_or(|entry| !entry.destroyed)
        {
            return Err(BaoError::UnknownOutcome);
        }
        Ok(())
    }
}

fn check_version(version: u64) -> Result<(), BaoError> {
    if (1..=i32::MAX as u64).contains(&version) {
        Ok(())
    } else {
        Err(BaoError::InvalidConfig)
    }
}

fn decode_read(value: &Value, expected_version: u64) -> Result<KvRead, BaoError> {
    let data = value.get("data").ok_or(BaoError::InvalidEvidence)?;
    let metadata = data.get("metadata").ok_or(BaoError::InvalidEvidence)?;
    let flags = decode_version_metadata(metadata)?;
    if metadata.get("version").and_then(Value::as_u64) != Some(expected_version)
        || flags.destroyed
        || !flags.deletion_time.is_empty()
    {
        return Err(BaoError::InvalidEvidence);
    }
    let payload = data.get("data").ok_or(BaoError::InvalidEvidence)?;
    if !payload.is_object() {
        return Err(BaoError::InvalidEvidence);
    }
    Ok(KvRead {
        version: expected_version,
        bytes: SensitiveBytes::new(
            serde_jcs::to_vec(payload).map_err(|_| BaoError::InvalidEvidence)?,
        )?,
    })
}

fn decode_version_metadata(value: &Value) -> Result<KvVersionMetadata, BaoError> {
    let destroyed = value
        .get("destroyed")
        .and_then(Value::as_bool)
        .ok_or(BaoError::InvalidEvidence)?;
    let deletion_time = value
        .get("deletion_time")
        .and_then(Value::as_str)
        .ok_or(BaoError::InvalidEvidence)?;
    let created_time = value
        .get("created_time")
        .and_then(Value::as_str)
        .ok_or(BaoError::InvalidEvidence)?;
    if !valid_timestamp(created_time)
        || (!deletion_time.is_empty() && !valid_timestamp(deletion_time))
    {
        return Err(BaoError::InvalidEvidence);
    }
    Ok(KvVersionMetadata {
        destroyed,
        deletion_time: deletion_time.to_owned(),
        created_time: created_time.to_owned(),
    })
}

fn valid_timestamp(value: &str) -> bool {
    value.len() <= 64 && chrono::DateTime::parse_from_rfc3339(value).is_ok()
}

fn decode_metadata(value: &Value) -> Result<KvMetadata, BaoError> {
    let data = value.get("data").ok_or(BaoError::InvalidEvidence)?;
    let current_version = data
        .get("current_version")
        .and_then(Value::as_u64)
        .ok_or(BaoError::InvalidEvidence)?;
    check_version(current_version).map_err(|_| BaoError::InvalidEvidence)?;
    let entries = data
        .get("versions")
        .and_then(Value::as_object)
        .ok_or(BaoError::InvalidEvidence)?;
    if entries.is_empty() || entries.len() > 128 {
        return Err(BaoError::InvalidEvidence);
    }
    let mut versions = BTreeMap::new();
    for (version, data) in entries {
        let parsed: u64 = version.parse().map_err(|_| BaoError::InvalidEvidence)?;
        check_version(parsed).map_err(|_| BaoError::InvalidEvidence)?;
        if version != &parsed.to_string() || parsed > current_version {
            return Err(BaoError::InvalidEvidence);
        }
        versions.insert(parsed, decode_version_metadata(data)?);
    }
    if !versions.contains_key(&current_version) {
        return Err(BaoError::InvalidEvidence);
    }
    Ok(KvMetadata {
        current_version,
        versions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn flags() -> Value {
        json!({"created_time":"2026-09-09T00:00:00Z","deletion_time":"","destroyed":false})
    }

    #[test]
    fn exact_read_never_treats_wrong_version_or_tombstones_as_secret_values() {
        let mut metadata = flags();
        metadata["version"] = json!(1);
        let good = json!({"data":{"data":{"test":"canary"},"metadata":metadata}});
        assert_eq!(
            decode_read(&good, 1).unwrap().bytes.as_bytes(),
            br#"{"test":"canary"}"#
        );
        for (field, value) in [
            ("version", json!(2)),
            ("destroyed", json!(true)),
            ("deletion_time", json!("2026-09-09T00:00:00Z")),
            ("created_time", json!("invalid")),
        ] {
            let mut changed = good.clone();
            changed["data"]["metadata"][field] = value;
            assert_eq!(
                decode_read(&changed, 1).unwrap_err(),
                BaoError::InvalidEvidence
            );
        }
    }

    #[test]
    fn metadata_requires_exact_positive_versions_and_explicit_destroy_evidence() {
        let good = json!({"data":{"current_version":1,"versions":{"1":flags()}}});
        assert!(!decode_metadata(&good).unwrap().versions[&1].destroyed);
        for version in ["0", "01", "2", "-1", "2147483648"] {
            let changed = json!({"data":{"current_version":1,"versions":{version:flags()}}});
            assert_eq!(decode_metadata(&changed), Err(BaoError::InvalidEvidence));
        }
        let mut destroyed = good.clone();
        destroyed["data"]["versions"]["1"]["destroyed"] = json!(true);
        assert!(decode_metadata(&destroyed).unwrap().versions[&1].destroyed);
        destroyed["data"]["versions"]["1"]["destroyed"] = Value::Null;
        assert_eq!(decode_metadata(&destroyed), Err(BaoError::InvalidEvidence));
    }
}
