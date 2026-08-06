//! Closed S3 contract exercised against a real RustFS instance.
//!
//! This is ignored in the default hermetic suite. Release qualification runs
//! it explicitly with `RUSTFS_CONTRACT_*` credentials and a pre-created bucket.
//! An isolated disposable instance may opt into fixture-only bucket creation.

use std::{fs::File, path::PathBuf, sync::Arc, time::Duration};

use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::{BehaviorVersion, Region},
    Client,
};
use aws_smithy_http_client::Builder as SmithyHttpClientBuilder;
use insight_durable::{FileDurableRepository, FileQuery, FileStatus};
use insight_storage::file_service::{
    CreateFileRequest, FilePrincipal, FileService, FileServiceConfig,
};
use insight_storage::s3_storage::{S3Storage, S3StorageConfig};
use insight_storage::SqliteDurableRepository;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required"))
}

fn storage() -> S3Storage {
    let endpoint = required("RUSTFS_CONTRACT_ENDPOINT");
    let presign_ttl = std::env::var("RUSTFS_CONTRACT_PRESIGN_TTL_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(60);
    S3Storage::new(S3StorageConfig {
        public_endpoint: std::env::var("RUSTFS_CONTRACT_PUBLIC_ENDPOINT")
            .unwrap_or_else(|_| endpoint.clone()),
        endpoint,
        region: std::env::var("RUSTFS_CONTRACT_REGION").unwrap_or_else(|_| "us-east-1".to_owned()),
        bucket: required("RUSTFS_CONTRACT_BUCKET"),
        force_path_style: true,
        access_key: required("RUSTFS_CONTRACT_ACCESS_KEY"),
        secret_key: required("RUSTFS_CONTRACT_SECRET_KEY"),
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(30),
        presign_upload_ttl: Duration::from_secs(presign_ttl),
        presign_download_ttl: Duration::from_secs(presign_ttl),
        artifact_namespace: "rustfs-contract".to_owned(),
        artifact_inline_threshold_bytes: 1024,
    })
    .unwrap()
}

fn administrative_client() -> Client {
    let credentials = Credentials::new(
        required("RUSTFS_CONTRACT_ACCESS_KEY"),
        required("RUSTFS_CONTRACT_SECRET_KEY"),
        None,
        None,
        "rustfs-contract-qualification",
    );
    let mut config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .endpoint_url(required("RUSTFS_CONTRACT_ENDPOINT"))
        .region(Region::new(
            std::env::var("RUSTFS_CONTRACT_REGION").unwrap_or_else(|_| "us-east-1".to_owned()),
        ))
        .credentials_provider(credentials)
        .force_path_style(true);
    if required("RUSTFS_CONTRACT_ENDPOINT").starts_with("http://") {
        config = config.http_client(SmithyHttpClientBuilder::new().build_http());
    }
    Client::from_conf(config.build())
}

async fn create_bucket_for_isolated_qualification() {
    if std::env::var("RUSTFS_CONTRACT_CREATE_BUCKET").as_deref() != Ok("1") {
        return;
    }
    let _ = administrative_client()
        .create_bucket()
        .bucket(required("RUSTFS_CONTRACT_BUCKET"))
        .send()
        .await;
}

async fn send_presigned(
    client: &reqwest::Client,
    request: insight_storage::s3_storage::PresignedS3Request,
    body: Option<Vec<u8>>,
) -> reqwest::Response {
    let method = reqwest::Method::from_bytes(request.method.as_bytes()).unwrap();
    let mut builder = client.request(method, request.url);
    for (name, value) in request.headers {
        if !name.eq_ignore_ascii_case("host") {
            builder = builder.header(name, value);
        }
    }
    if let Some(body) = body {
        builder = builder.body(body);
    }
    builder.send().await.unwrap()
}

async fn exercise_file_service(storage: S3Storage) {
    let temporary = tempfile::tempdir().unwrap();
    let database = temporary.path().join("file-service.sqlite3");
    File::create(&database).unwrap();
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&database)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    sqlx::raw_sql(include_str!("../database/durable/sqlite/schema.sql"))
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let repository = Arc::new(
        SqliteDurableRepository::connect_path(&database)
            .await
            .unwrap(),
    );
    let service = FileService::new(
        repository.clone(),
        Arc::new(storage),
        FileServiceConfig {
            max_file_bytes: 1024,
            max_files_per_invocation: 4,
            max_total_file_bytes_per_invocation: 2048,
            pending_upload_ttl: Duration::from_secs(60),
            deletion_claim_ttl: Duration::from_secs(60),
        },
    )
    .unwrap();
    let principal = FilePrincipal::new("tenant-contract", "user-contract").unwrap();
    let bytes = b"rustfs-file-service".to_vec();
    let checksum = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let created = service
        .create_file(
            &principal,
            CreateFileRequest {
                filename: "scan.png".to_owned(),
                media_type: "image/png".to_owned(),
                size_bytes: u64::try_from(bytes.len()).unwrap(),
                sha256: Some(checksum),
            },
            Some("rustfs-file-service-create"),
        )
        .await
        .unwrap();
    let file_id = created.file.file_id.clone();
    let upload = created
        .upload
        .expect("pending File must return a PUT capability");
    assert!(send_presigned(
        &reqwest::Client::new(),
        insight_storage::s3_storage::PresignedS3Request {
            method: upload.method,
            url: upload.url,
            headers: upload.headers,
        },
        Some(bytes.clone()),
    )
    .await
    .status()
    .is_success());
    let completed = service.complete_file(&principal, &file_id).await.unwrap();
    assert_eq!(completed.status, "ready");
    let replayed = service
        .create_file(
            &principal,
            CreateFileRequest {
                filename: "scan.png".to_owned(),
                media_type: "image/png".to_owned(),
                size_bytes: u64::try_from(bytes.len()).unwrap(),
                sha256: Some(
                    Sha256::digest(&bytes)
                        .iter()
                        .map(|byte| format!("{byte:02x}"))
                        .collect(),
                ),
            },
            Some("rustfs-file-service-create"),
        )
        .await
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.file.file_id, file_id);
    assert!(replayed.upload.is_none());
    assert_eq!(
        service
            .resolve_files(&principal, std::slice::from_ref(&file_id))
            .await
            .unwrap()
            .len(),
        1
    );
    let download = service.create_download(&principal, &file_id).await.unwrap();
    let response = send_presigned(
        &reqwest::Client::new(),
        insight_storage::s3_storage::PresignedS3Request {
            method: download.method,
            url: download.url,
            headers: download.headers,
        },
        None,
    )
    .await;
    assert!(response.status().is_success());
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes.as_slice());
    let other = FilePrincipal::new("tenant-contract", "other-user").unwrap();
    assert_eq!(
        service.get_file(&other, &file_id).await.unwrap_err().code(),
        "FILE_NOT_FOUND"
    );
    service.delete_file(&principal, &file_id).await.unwrap();
    assert_eq!(
        service
            .get_file(&principal, &file_id)
            .await
            .unwrap_err()
            .code(),
        "FILE_NOT_FOUND"
    );
    assert_eq!(service.gc_once(10).await.unwrap(), 1);
    assert_eq!(
        repository
            .get_file(FileQuery {
                file_id,
                tenant_id: principal.tenant_id,
                user_id: principal.user_id,
            })
            .await
            .unwrap()
            .unwrap()
            .status,
        FileStatus::Deleted
    );
}

#[tokio::test]
#[ignore = "requires a real RustFS instance and qualification bucket"]
async fn rustfs_supports_the_closed_file_service_s3_contract() {
    create_bucket_for_isolated_qualification().await;
    let storage = storage();
    storage.check_readiness().await.unwrap();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let key = format!("contract/{}/content", Uuid::new_v4().simple());
    let bytes = b"rustfs-s3-contract".to_vec();
    let checksum = Sha256::digest(&bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    let upload = storage
        .presign_put(
            &key,
            u64::try_from(bytes.len()).unwrap(),
            "image/png",
            Some(&checksum),
        )
        .await
        .unwrap();
    let signed_headers = upload
        .headers
        .keys()
        .map(|header| header.to_ascii_lowercase())
        .collect::<Vec<_>>();
    assert!(signed_headers.iter().any(|header| header == "content-type"));
    assert!(signed_headers
        .iter()
        .any(|header| header == "content-length"));
    assert!(signed_headers
        .iter()
        .any(|header| header == "if-none-match"));
    assert!(signed_headers
        .iter()
        .any(|header| header == "x-amz-meta-sha256"));
    assert!(signed_headers
        .iter()
        .any(|header| header == "x-amz-checksum-sha256"));
    let upload_response = send_presigned(&client, upload, Some(bytes.clone())).await;
    assert!(upload_response.status().is_success());

    let overwrite = storage
        .presign_put(
            &key,
            u64::try_from(bytes.len()).unwrap(),
            "image/png",
            Some(&checksum),
        )
        .await
        .unwrap();
    assert!(!send_presigned(&client, overwrite, Some(bytes.clone()))
        .await
        .status()
        .is_success());

    let checksum_key = format!("contract/{}/checksum", Uuid::new_v4().simple());
    let checksum_upload = storage
        .presign_put(
            &checksum_key,
            u64::try_from(bytes.len()).unwrap(),
            "image/png",
            Some(&checksum),
        )
        .await
        .unwrap();
    let mut tampered = bytes.clone();
    tampered[0] ^= 0xff;
    assert!(!send_presigned(&client, checksum_upload, Some(tampered))
        .await
        .status()
        .is_success());
    assert!(storage.head_object(&checksum_key).await.is_err());

    let metadata = storage.head_object(&key).await.unwrap();
    assert_eq!(metadata.size_bytes, u64::try_from(bytes.len()).unwrap());
    assert_eq!(metadata.media_type.as_deref(), Some("image/png"));
    assert_eq!(metadata.checksum_sha256.as_deref(), Some(checksum.as_str()));
    let etag = metadata.etag.clone().expect("RustFS must return an ETag");
    assert_eq!(storage.get_bytes(&key, bytes.len()).await.unwrap(), bytes);
    assert_eq!(storage.get_bytes_range(&key, 7, 2).await.unwrap(), b"s3");

    let download = storage.presign_get(&key).await.unwrap();
    let response = send_presigned(&client, download, None).await;
    assert!(response.status().is_success());
    assert_eq!(response.bytes().await.unwrap().as_ref(), bytes.as_slice());

    assert!(storage
        .delete_object_if_identity(&key, Some("\"wrong-etag\""), None)
        .await
        .is_err());
    storage
        .delete_object_if_identity(&key, Some(&etag), metadata.version_id.as_deref())
        .await
        .unwrap();
    assert!(storage.head_object(&key).await.is_err());

    let ttl = storage.presign_upload_ttl();
    if ttl <= Duration::from_secs(5) {
        let expired_key = format!("contract/{}/expired", Uuid::new_v4().simple());
        let expired = storage
            .presign_put(
                &expired_key,
                u64::try_from(bytes.len()).unwrap(),
                "image/png",
                Some(&checksum),
            )
            .await
            .unwrap();
        tokio::time::sleep(ttl + Duration::from_secs(2)).await;
        assert!(!send_presigned(&client, expired, Some(bytes.clone()))
            .await
            .status()
            .is_success());
        assert!(storage.head_object(&expired_key).await.is_err());
    }
    exercise_file_service(storage).await;
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestartState {
    key: String,
    etag: Option<String>,
    version_id: Option<String>,
    checksum_sha256: Option<String>,
    size_bytes: u64,
}

fn restart_state_path() -> PathBuf {
    PathBuf::from(required("RUSTFS_CONTRACT_RESTART_STATE_FILE"))
}

#[tokio::test]
#[ignore = "requires seed, RustFS restart, and verify qualification phases"]
async fn rustfs_preserves_object_identity_across_restart() {
    create_bucket_for_isolated_qualification().await;
    let storage = storage();
    storage.check_readiness().await.unwrap();
    let state_path = restart_state_path();
    match required("RUSTFS_CONTRACT_RESTART_PHASE").as_str() {
        "seed" => {
            let key = format!("contract-restart/{}/content", Uuid::new_v4().simple());
            let bytes = b"rustfs-restart-persistence-contract".to_vec();
            let metadata = storage
                .put_bytes(&key, bytes, Some("application/octet-stream"))
                .await
                .unwrap();
            let state = RestartState {
                key,
                etag: metadata.etag,
                version_id: metadata.version_id,
                checksum_sha256: metadata.checksum_sha256,
                size_bytes: metadata.size_bytes,
            };
            std::fs::write(&state_path, serde_json::to_vec(&state).unwrap()).unwrap();
        }
        "verify" => {
            let state: RestartState =
                serde_json::from_slice(&std::fs::read(&state_path).unwrap()).unwrap();
            let metadata = storage.head_object(&state.key).await.unwrap();
            assert_eq!(metadata.etag, state.etag);
            assert_eq!(metadata.version_id, state.version_id);
            assert_eq!(metadata.checksum_sha256, state.checksum_sha256);
            assert_eq!(metadata.size_bytes, state.size_bytes);
            assert_eq!(
                storage
                    .get_bytes(&state.key, usize::try_from(state.size_bytes).unwrap())
                    .await
                    .unwrap(),
                b"rustfs-restart-persistence-contract"
            );
            storage
                .delete_object_if_identity(
                    &state.key,
                    state.etag.as_deref(),
                    state.version_id.as_deref(),
                )
                .await
                .unwrap();
            assert!(storage.head_object(&state.key).await.is_err());
            std::fs::remove_file(state_path).unwrap();
        }
        phase => panic!("unsupported RUSTFS_CONTRACT_RESTART_PHASE '{phase}'"),
    }
}
