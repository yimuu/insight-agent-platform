mod support;

use chrono::{Duration, TimeZone, Utc};
use insight_durable::{
    AcknowledgeFileDeletionCommand, ClaimFileDeletionsCommand, CompleteFileCommand,
    CreateFileCommand, FileDurableRepository, FileQuery, FileStatus,
};
use insight_storage::SqliteDurableRepository;

fn query() -> FileQuery {
    FileQuery {
        file_id: "file_gc_contract".to_owned(),
        tenant_id: "tenant-a".to_owned(),
        user_id: "user-a".to_owned(),
    }
}

fn create_command(
    file_id: &str,
    idempotency_key: Option<&str>,
    request_hash_byte: char,
    now: chrono::DateTime<Utc>,
) -> CreateFileCommand {
    CreateFileCommand {
        file_id: file_id.to_owned(),
        tenant_id: "tenant-a".to_owned(),
        user_id: "user-a".to_owned(),
        filename: "report.png".to_owned(),
        media_type: "image/png".to_owned(),
        expected_size_bytes: 8,
        checksum_sha256: None,
        object_key: format!("files/private/{file_id}/content"),
        idempotency_key: idempotency_key.map(str::to_owned),
        request_hash: format!("sha256:{}", request_hash_byte.to_string().repeat(64)),
        created_at: now,
        upload_expires_at: now + Duration::hours(1),
    }
}

#[tokio::test]
async fn file_metadata_is_principal_scoped_idempotent_and_covers_terminal_states() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("durable.sqlite3");
    support::provision_sqlite_database(&database).await;
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 8, 5, 9, 0, 0).unwrap();

    let created = repository
        .create_file(create_command("file_original", Some("upload-1"), 'a', now))
        .await
        .unwrap();
    assert!(!created.replayed);
    let replay = repository
        .create_file(create_command(
            "file_other_candidate",
            Some("upload-1"),
            'a',
            now,
        ))
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.file.file_id, "file_original");
    assert!(repository
        .create_file(create_command("file_conflict", Some("upload-1"), 'b', now))
        .await
        .is_err());
    assert!(repository
        .get_file(FileQuery {
            file_id: "file_original".to_owned(),
            tenant_id: "tenant-a".to_owned(),
            user_id: "user-b".to_owned(),
        })
        .await
        .unwrap()
        .is_none());

    let ready = repository
        .complete_file(CompleteFileCommand {
            query: FileQuery {
                file_id: "file_original".to_owned(),
                tenant_id: "tenant-a".to_owned(),
                user_id: "user-a".to_owned(),
            },
            actual_size_bytes: 8,
            object_etag: "etag-ready".to_owned(),
            object_version_id: Some("version-ready".to_owned()),
            ready_at: now + Duration::minutes(1),
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ready.status, FileStatus::Ready);

    repository
        .create_file(create_command("file_expired", None, 'c', now))
        .await
        .unwrap();
    let expired = repository
        .expire_pending_files(now + Duration::hours(2), 10)
        .await
        .unwrap();
    assert_eq!(expired.len(), 1);
    assert_eq!(expired[0].status, FileStatus::Expired);

    repository
        .create_file(create_command(
            "file_failed",
            None,
            'd',
            now + Duration::hours(3),
        ))
        .await
        .unwrap();
    let failed = repository
        .fail_file(
            FileQuery {
                file_id: "file_failed".to_owned(),
                tenant_id: "tenant-a".to_owned(),
                user_id: "user-a".to_owned(),
            },
            now + Duration::hours(3),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(failed.status, FileStatus::Failed);
}

#[tokio::test]
async fn file_deletion_claim_survives_restart_and_fences_stale_ack() {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("durable.sqlite3");
    support::provision_sqlite_database(&database).await;
    let repository = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let now = Utc.with_ymd_and_hms(2026, 8, 5, 10, 0, 0).unwrap();
    repository
        .create_file(CreateFileCommand {
            file_id: query().file_id,
            tenant_id: query().tenant_id,
            user_id: query().user_id,
            filename: "report.png".to_owned(),
            media_type: "image/png".to_owned(),
            expected_size_bytes: 8,
            checksum_sha256: None,
            object_key: "files/private/file_gc_contract/content".to_owned(),
            idempotency_key: Some("create-gc-contract".to_owned()),
            request_hash: format!("sha256:{}", "a".repeat(64)),
            created_at: now,
            upload_expires_at: now + Duration::hours(1),
        })
        .await
        .unwrap();
    repository
        .complete_file(CompleteFileCommand {
            query: query(),
            actual_size_bytes: 8,
            object_etag: "etag-v1".to_owned(),
            object_version_id: Some("version-v1".to_owned()),
            ready_at: now + Duration::minutes(1),
        })
        .await
        .unwrap();
    repository
        .begin_file_delete(query(), now + Duration::minutes(2))
        .await
        .unwrap();

    let first = repository
        .claim_file_deletions(ClaimFileDeletionsCommand {
            observed_at: now + Duration::minutes(2),
            claim_expires_at: now + Duration::minutes(3),
            limit: 10,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(first.deletion_fence, 1);
    assert!(repository
        .claim_file_deletions(ClaimFileDeletionsCommand {
            observed_at: now + Duration::seconds(150),
            claim_expires_at: now + Duration::minutes(4),
            limit: 10,
        })
        .await
        .unwrap()
        .is_empty());

    drop(repository);
    let restarted = SqliteDurableRepository::connect_path(&database)
        .await
        .unwrap();
    let second = restarted
        .claim_file_deletions(ClaimFileDeletionsCommand {
            observed_at: now + Duration::minutes(3),
            claim_expires_at: now + Duration::minutes(4),
            limit: 10,
        })
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert_eq!(second.deletion_fence, 2);
    assert_ne!(second.claim_token, first.claim_token);

    assert!(!restarted
        .acknowledge_file_deletion(AcknowledgeFileDeletionCommand {
            file_id: first.file.file_id,
            claim_token: first.claim_token,
            deletion_fence: first.deletion_fence,
            deleted_at: now + Duration::minutes(3),
        })
        .await
        .unwrap());
    assert!(restarted
        .acknowledge_file_deletion(AcknowledgeFileDeletionCommand {
            file_id: second.file.file_id,
            claim_token: second.claim_token,
            deletion_fence: second.deletion_fence,
            deleted_at: now + Duration::minutes(3),
        })
        .await
        .unwrap());
    assert_eq!(
        restarted.get_file(query()).await.unwrap().unwrap().status,
        FileStatus::Deleted
    );
}
