use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use insight_api::v1::{build_file_router, ApiAuth, FileApiState};
use insight_engine::file_store::{
    CreateFileRequest, CreateFileResponse, FileDownloadCapability, FilePrincipal, FileServiceApi,
    FileServiceError, FileUploadCapability, PublicFile,
};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tower::ServiceExt as _;

#[derive(Default)]
struct FixtureFileService {
    create: Mutex<Option<CreateFileRequest>>,
}

fn public_file() -> PublicFile {
    PublicFile {
        file_id: "file_api_fixture".to_owned(),
        filename: "a.png".to_owned(),
        media_type: "image/png".to_owned(),
        size_bytes: 24,
        status: "pending_upload".to_owned(),
        created_at: chrono::Utc::now(),
    }
}

#[async_trait::async_trait]
impl FileServiceApi for FixtureFileService {
    async fn create_file(
        &self,
        principal: &FilePrincipal,
        request: CreateFileRequest,
        idempotency_key: Option<&str>,
    ) -> Result<CreateFileResponse, FileServiceError> {
        assert_eq!(principal.tenant_id, "tenant-a");
        assert_eq!(principal.user_id, "user-a");
        assert_eq!(idempotency_key, Some("upload-a"));
        let mut previous = self.create.lock().await;
        let replayed = match previous.as_ref() {
            Some(previous) if previous == &request => true,
            Some(_) => {
                return Err(FileServiceError::new(
                    "IDEMPOTENCY_KEY_REUSED",
                    "Idempotency key was already used for a different request",
                ))
            }
            None => {
                *previous = Some(request);
                false
            }
        };
        Ok(CreateFileResponse {
            file: public_file(),
            upload: Some(FileUploadCapability {
                method: "PUT".to_owned(),
                url: "https://files.example.test/upload".to_owned(),
                headers: Default::default(),
                expires_at: chrono::Utc::now(),
            }),
            replayed,
        })
    }

    async fn complete_file(
        &self,
        _principal: &FilePrincipal,
        _file_id: &str,
    ) -> Result<PublicFile, FileServiceError> {
        Ok(public_file())
    }

    async fn get_file(
        &self,
        principal: &FilePrincipal,
        file_id: &str,
    ) -> Result<PublicFile, FileServiceError> {
        if principal.user_id != "user-a" || file_id != "file_api_fixture" {
            return Err(FileServiceError::not_found());
        }
        Ok(public_file())
    }

    async fn create_download(
        &self,
        _principal: &FilePrincipal,
        _file_id: &str,
    ) -> Result<FileDownloadCapability, FileServiceError> {
        Ok(FileDownloadCapability {
            method: "GET".to_owned(),
            url: "https://files.example.test/download".to_owned(),
            headers: Default::default(),
            expires_at: chrono::Utc::now(),
        })
    }

    async fn delete_file(
        &self,
        _principal: &FilePrincipal,
        _file_id: &str,
    ) -> Result<(), FileServiceError> {
        Ok(())
    }
}

fn router() -> Router {
    build_file_router(FileApiState {
        auth: ApiAuth::disabled(),
        service: Arc::new(FixtureFileService::default()),
    })
}

fn create_request(body: Value, idempotency_key: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/files")
        .header(header::CONTENT_TYPE, "application/json")
        .header("x-tenant-id", "tenant-a")
        .header("x-user-id", "user-a")
        .header("idempotency-key", idempotency_key)
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap()
}

#[tokio::test]
async fn file_api_is_strict_principal_scoped_and_idempotent() {
    let router = router();
    let body = json!({
        "filename":"a.png",
        "media_type":"image/png",
        "size_bytes":24
    });
    let created = router
        .clone()
        .oneshot(create_request(body.clone(), "upload-a"))
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    assert_eq!(
        created.headers()[header::CACHE_CONTROL],
        "private, no-store"
    );
    assert!(created.headers().contains_key("x-request-id"));
    let created: Value =
        serde_json::from_slice(&to_bytes(created.into_body(), 64 * 1024).await.unwrap()).unwrap();
    let file_id = created["data"]["file"]["file_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(created["data"]["replayed"], false);
    assert_eq!(created["data"]["upload"]["method"], "PUT");

    let replay = router
        .clone()
        .oneshot(create_request(body, "upload-a"))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let replay: Value =
        serde_json::from_slice(&to_bytes(replay.into_body(), 64 * 1024).await.unwrap()).unwrap();
    assert_eq!(replay["data"]["file"]["file_id"], file_id);
    assert_eq!(replay["data"]["replayed"], true);

    let conflict = router
        .clone()
        .oneshot(create_request(
            json!({"filename":"a.png","media_type":"image/png","size_bytes":25}),
            "upload-a",
        ))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);

    let cross_principal = Request::builder()
        .method("GET")
        .uri(format!("/v1/files/{file_id}"))
        .header("x-tenant-id", "tenant-a")
        .header("x-user-id", "user-b")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        router
            .clone()
            .oneshot(cross_principal)
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );

    let alias = router
        .oneshot(create_request(json!({"content":"not accepted"}), "alias"))
        .await
        .unwrap();
    assert_eq!(alias.status(), StatusCode::BAD_REQUEST);
}
