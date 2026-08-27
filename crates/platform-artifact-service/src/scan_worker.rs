use chrono::{DateTime, Duration as ChronoDuration, Utc};
use insight_platform_artifact_broker::{ArtifactScanReadError, BrokeredArtifactScannerReader};
use insight_platform_artifacts::{
    ArtifactBackendFailure, ArtifactBlobBackend, ArtifactBlobDeletionEvidence,
    ArtifactScanDisposition, ArtifactScanEvidence, ArtifactScanEvidenceDraft, ArtifactScanRequest,
    ArtifactScanner, ArtifactWorkerService, DeleteArtifactBlobGeneration,
};
use insight_platform_contracts::{
    parse_strict_json, JsonLimits, ResourceId, ResourceKind, Sha256Digest,
};
use insight_platform_jobs::JobFence as DomainJobFence;
use insight_platform_postgres::{
    artifact_repository::{ArtifactExecutionSlot, StartedArtifactExecution},
    repository::{
        ArtifactWorkerRole, ClaimArtifactJobs, JobFence as RepositoryJobFence, PgRepository,
    },
};
use serde::Deserialize;
use sha2::{Digest as _, Sha256};
use std::{sync::Arc, time::Duration};
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScanWorkerConfig {
    pub scanner_contract_digest: Sha256Digest,
    pub ruleset_digest: Sha256Digest,
    pub claim_batch: u16,
    pub lease_milliseconds: i64,
    pub receipt_ttl_milliseconds: i64,
    pub poll_milliseconds: u64,
}

impl ScanWorkerConfig {
    pub(crate) fn validate(&self) -> Result<(), ()> {
        if self.claim_batch == 0
            || self.claim_batch > 32
            || !(1_000..=120_000).contains(&self.lease_milliseconds)
            || !(1_000..=3_600_000).contains(&self.receipt_ttl_milliseconds)
            || !(10..=60_000).contains(&self.poll_milliseconds)
        {
            return Err(());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct IntegrityArtifactScanner {
    reader: Arc<BrokeredArtifactScannerReader>,
    scanner_contract_digest: Sha256Digest,
    ruleset_digest: Sha256Digest,
}

impl IntegrityArtifactScanner {
    pub(crate) fn new(
        reader: Arc<BrokeredArtifactScannerReader>,
        config: &ScanWorkerConfig,
    ) -> Self {
        Self {
            reader,
            scanner_contract_digest: config.scanner_contract_digest.clone(),
            ruleset_digest: config.ruleset_digest.clone(),
        }
    }
}

impl ArtifactScanner for IntegrityArtifactScanner {
    async fn scan(
        &self,
        request: ArtifactScanRequest,
    ) -> Result<ArtifactScanEvidence, ArtifactBackendFailure> {
        if request.job.scanner_contract_digest != self.scanner_contract_digest
            || request.job.ruleset_digest != self.ruleset_digest
        {
            return Err(backend_failure(false, "scanner_profile_not_installed"));
        }
        let read = self
            .reader
            .read_for_scan(&request)
            .await
            .map_err(map_read_failure)?;
        let (verified_media_type, disposition, reason_class) =
            inspect_content(read.bytes(), read.declared_media_type());
        let observed_at = request.observed_at;
        let expires_at = observed_at
            .checked_add_signed(ChronoDuration::milliseconds(
                i64::try_from(request.job.evidence_ttl_milliseconds)
                    .map_err(|_| backend_failure(false, "scanner_evidence_ttl_invalid"))?,
            ))
            .ok_or_else(|| backend_failure(false, "scanner_evidence_ttl_invalid"))?;
        ArtifactScanEvidenceDraft {
            schema_version: 1,
            scan_kind: request.job.scan_kind,
            scan_job_id: request.job_id,
            scan_policy_revision: request.job.scan_policy_revision,
            scanner_contract_digest: request.job.scanner_contract_digest,
            ruleset_digest: request.job.ruleset_digest,
            object_generation: request.job.object_generation,
            content_digest: read.content_digest().clone(),
            size_bytes: u64::try_from(read.bytes().len())
                .map_err(|_| backend_failure(false, "scanner_size_invalid"))?,
            verified_media_type,
            disposition,
            reason_class,
            observed_at,
            expires_at,
        }
        .seal()
        .map_err(|_| backend_failure(false, "scanner_evidence_invalid"))
    }
}

fn inspect_content(
    bytes: &[u8],
    declared_media_type: Option<&str>,
) -> (String, ArtifactScanDisposition, Option<String>) {
    const EICAR: &[u8] = b"EICAR-STANDARD-ANTIVIRUS-TEST-FILE";
    let executable =
        bytes.starts_with(b"\x7fELF") || bytes.starts_with(b"MZ") || bytes.starts_with(b"#!");
    let detected = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        "image/jpeg"
    } else if bytes.starts_with(b"%PDF-") {
        "application/pdf"
    } else if bytes.starts_with(b"PK\x03\x04") {
        "application/zip"
    } else if parse_strict_json(
        bytes,
        JsonLimits {
            max_bytes: bytes.len().max(1),
            max_depth: 64,
            max_items_per_array: 65_536,
            max_properties_per_object: 65_536,
            max_string_bytes: bytes.len().max(1),
        },
    )
    .is_ok()
    {
        "application/json"
    } else if std::str::from_utf8(bytes).is_ok() && !bytes.contains(&0) {
        "text/plain"
    } else {
        "application/octet-stream"
    };
    if bytes.windows(EICAR.len()).any(|window| window == EICAR) || executable {
        return (
            detected.to_owned(),
            ArtifactScanDisposition::Rejected,
            Some("prohibited_executable_or_malware".to_owned()),
        );
    }
    if detected == "application/zip" {
        return (
            detected.to_owned(),
            ArtifactScanDisposition::Quarantined,
            Some("archive_requires_published_scanner".to_owned()),
        );
    }
    if declared_media_type
        .is_some_and(|declared| declared != detected && declared != "application/octet-stream")
    {
        return (
            detected.to_owned(),
            ArtifactScanDisposition::Quarantined,
            Some("declared_media_type_mismatch".to_owned()),
        );
    }
    (detected.to_owned(), ArtifactScanDisposition::Verified, None)
}

fn map_read_failure(error: ArtifactScanReadError) -> ArtifactBackendFailure {
    match error {
        ArtifactScanReadError::Unavailable => {
            backend_failure(true, "scanner_dependency_unavailable")
        }
        ArtifactScanReadError::NotFound => backend_failure(false, "artifact_object_not_found"),
        ArtifactScanReadError::Denied => backend_failure(false, "artifact_scan_authority_denied"),
        ArtifactScanReadError::TooLarge => backend_failure(false, "artifact_scan_too_large"),
        ArtifactScanReadError::Integrity => {
            backend_failure(false, "artifact_object_evidence_invalid")
        }
    }
}

fn backend_failure(retryable: bool, reason_class: &str) -> ArtifactBackendFailure {
    ArtifactBackendFailure {
        retryable,
        reason_class: reason_class.to_owned(),
    }
}

#[derive(Clone, Copy)]
struct UnavailableBlobBackend;

impl ArtifactBlobBackend for UnavailableBlobBackend {
    async fn delete_generation(
        &self,
        _request: DeleteArtifactBlobGeneration,
    ) -> Result<ArtifactBlobDeletionEvidence, ArtifactBackendFailure> {
        Err(backend_failure(false, "data_worker_delete_forbidden"))
    }
}

pub(crate) async fn run_scan_worker(
    repository: Arc<PgRepository>,
    scanner: IntegrityArtifactScanner,
    config: ScanWorkerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), String> {
    let worker_id = new_id(ResourceKind::WorkerProcessGeneration)?;
    loop {
        if *shutdown.borrow() {
            return Ok(());
        }
        let tokens = (0..config.claim_batch)
            .map(|_| fresh_digest("artifact-data-worker-lease"))
            .collect::<Result<Vec<_>, _>>()?;
        let claimed = repository
            .claim_artifact_jobs(ClaimArtifactJobs {
                role: ArtifactWorkerRole::DataWorker,
                worker_id: worker_id.clone(),
                limit: config.claim_batch,
                lease_milliseconds: config.lease_milliseconds,
                lease_token_digests: tokens,
            })
            .await
            .map_err(|_| "Artifact scan claim failed".to_owned())?;
        let mut attempts = tokio::task::JoinSet::new();
        for claimed_job in claimed {
            let repository = Arc::clone(&repository);
            let scanner = scanner.clone();
            let config = config.clone();
            let worker_id = worker_id.clone();
            attempts.spawn(async move {
                execute_claimed_scan(repository, scanner, config, worker_id, claimed_job).await
            });
        }
        while let Some(result) = attempts.join_next().await {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(reason)) => eprintln!("Artifact Data Worker attempt ended: {reason}"),
                Err(_) => eprintln!("Artifact Data Worker attempt task failed"),
            }
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { return Ok(()); }
            }
            _ = tokio::time::sleep(Duration::from_millis(config.poll_milliseconds)) => {}
        }
    }
}

async fn execute_claimed_scan(
    repository: Arc<PgRepository>,
    scanner: IntegrityArtifactScanner,
    config: ScanWorkerConfig,
    worker_id: ResourceId,
    claimed: insight_platform_postgres::repository::JobRecord,
) -> Result<(), String> {
    let token: Sha256Digest = claimed
        .lease_token_digest
        .as_deref()
        .ok_or_else(|| "claimed Artifact Job has no token".to_owned())?
        .parse()
        .map_err(|_| "claimed Artifact Job token is invalid".to_owned())?;
    let started = repository
        .start_job(RepositoryJobFence {
            tenant_id: claimed.tenant_id.clone(),
            job_id: claimed.job_id.clone(),
            worker_id: worker_id.clone(),
            lease_epoch: claimed.lease_epoch,
            expected_job_version: claimed.version,
            lease_token_digest: token.clone(),
        })
        .await
        .map_err(|_| "Artifact scan start lost its fence".to_owned())?;
    let tenant_id: ResourceId = started
        .tenant_id
        .parse()
        .map_err(|_| "Artifact scan tenant identity is corrupt".to_owned())?;
    let job_id: ResourceId = started
        .job_id
        .parse()
        .map_err(|_| "Artifact scan Job identity is corrupt".to_owned())?;
    let fence = DomainJobFence {
        expected_version: u64::try_from(started.version)
            .map_err(|_| "Artifact scan Job version is corrupt".to_owned())?,
        worker_process_generation_id: worker_id,
        lease_generation: u64::try_from(started.lease_epoch)
            .map_err(|_| "Artifact scan lease generation is corrupt".to_owned())?,
        token_digest: token,
    };
    let receipt_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(repository.pool())
        .await
        .map_err(|_| "Artifact database clock unavailable".to_owned())?;
    let receipt_limit = receipt_now
        .checked_add_signed(ChronoDuration::milliseconds(
            config.receipt_ttl_milliseconds,
        ))
        .ok_or_else(|| "Artifact receipt expiry overflowed".to_owned())?;
    let receipt_expires_at: DateTime<Utc> = started.deadline.min(receipt_limit);
    let execution = repository
        .load_started_artifact_execution(
            ArtifactWorkerRole::DataWorker,
            tenant_id,
            job_id,
            fence,
            ArtifactExecutionSlot {
                receipt_id: new_id(ResourceKind::Receipt)?,
                event_id: new_id(ResourceKind::Event)?,
                outbox_id: new_id(ResourceKind::OutboxEvent)?,
                duplicate_blob_cleanup_job_id: new_id(ResourceKind::Job)?,
                receipt_expires_at,
            },
        )
        .await
        .map_err(|_| "Artifact scan authority changed before execution".to_owned())?;
    let StartedArtifactExecution::Scan(execution) = execution else {
        return Err("Data Worker received non-scan Artifact work".to_owned());
    };
    let service =
        ArtifactWorkerService::new(scanner, UnavailableBlobBackend, (*repository).clone());
    let execution_now: DateTime<Utc> = sqlx::query_scalar("SELECT clock_timestamp()")
        .fetch_one(repository.pool())
        .await
        .map_err(|_| "Artifact database clock unavailable".to_owned())?;
    service
        .execute_scan(execution, execution_now)
        .await
        .map(|_| ())
        .map_err(|_| "Artifact scan did not commit".to_owned())
}

fn new_id(kind: ResourceKind) -> Result<ResourceId, String> {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7())
        .map_err(|_| "Artifact worker identity generation failed".to_owned())
}

fn fresh_digest(domain: &str) -> Result<Sha256Digest, String> {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    hasher.update(Uuid::new_v4().as_bytes());
    let value = hasher.finalize();
    format!("sha256:{}", lower_hex(&value))
        .parse()
        .map_err(|_| "Artifact worker token generation failed".to_owned())
}

fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scanner_rejects_executables_and_quarantines_unbounded_archives() {
        assert_eq!(
            inspect_content(b"\x7fELFpayload", None).1,
            ArtifactScanDisposition::Rejected
        );
        assert_eq!(
            inspect_content(b"PK\x03\x04payload", Some("application/zip")).1,
            ArtifactScanDisposition::Quarantined
        );
        assert_eq!(
            inspect_content(br#"{"safe":true}"#, Some("application/json")).1,
            ArtifactScanDisposition::Verified
        );
    }
}
