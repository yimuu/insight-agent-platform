//! HTTP exposure of the bounded process metrics and readiness projection.
use axum::{
    extract::{Extension, Request},
    http::{header::CACHE_CONTROL, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use insight_platform_observability::ProcessHttpMetrics;
use std::{sync::Arc, time::Instant};
pub fn process_observability_router(metrics: Arc<ProcessHttpMetrics>) -> Router {
    Router::new()
        .route("/livez", get(live))
        .route("/readyz", get(ready))
        .route("/metrics", get(metrics_response))
        .layer(middleware::from_fn(observe_request))
        .layer(Extension(metrics))
}

async fn live() -> Response {
    no_store(StatusCode::OK, "live")
}

async fn ready(Extension(metrics): Extension<Arc<ProcessHttpMetrics>>) -> Response {
    if metrics.is_ready() {
        no_store(StatusCode::OK, "ready")
    } else {
        no_store(StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

async fn metrics_response(Extension(metrics): Extension<Arc<ProcessHttpMetrics>>) -> Response {
    let mut response = metrics.render_prometheus().into_response();
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn observe_request(request: Request, next: Next) -> Response {
    let metrics = request
        .extensions()
        .get::<Arc<ProcessHttpMetrics>>()
        .cloned()
        .expect("process metrics Extension is installed");
    let operation = match request.uri().path() {
        "/livez" => "live",
        "/readyz" => "ready",
        "/metrics" => "metrics",
        _ => "other",
    };
    let started = Instant::now();
    let response = next.run(request).await;
    metrics.observe(operation, response.status().as_u16(), started.elapsed());
    response
}

fn no_store(status: StatusCode, body: &'static str) -> Response {
    let mut response = (status, body).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use insight_platform_observability::PROCESS_OBSERVABILITY_OPERATIONS;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio_util::sync::CancellationToken;
    use tower::ServiceExt as _;
    #[tokio::test]
    async fn process_router_exposes_fail_closed_readiness_and_bounded_metrics() {
        let metrics = Arc::new(
            ProcessHttpMetrics::install("scheduler-recovery", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap(),
        );
        let router = process_observability_router(Arc::clone(&metrics));
        let request = |path| {
            axum::http::Request::builder()
                .uri(path)
                .body(Body::empty())
                .unwrap()
        };
        let response = router.clone().oneshot(request("/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        metrics.mark_ready();
        let response = router.oneshot(request("/metrics")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
    }

    async fn scrape_over_tcp(address: std::net::SocketAddr, request: &str) -> String {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        String::from_utf8(response).unwrap()
    }

    #[tokio::test]
    async fn real_tcp_scrape_is_bounded_and_payload_canaries_are_absent() {
        const PAYLOAD_CANARY: &str = "payload-canary-4e27b98f";
        const IDENTITY_CANARY: &str = "tenant-canary-a309dcd1";
        const TRACESTATE_CANARY: &str = "vendor=trace-canary-49fa124a";
        const BAGGAGE_CANARY: &str = "private=baggage-canary-ff8ad715";

        let metrics = Arc::new(
            ProcessHttpMetrics::install("scheduler-recovery", PROCESS_OBSERVABILITY_OPERATIONS)
                .unwrap(),
        );
        metrics.mark_ready();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cancellation = CancellationToken::new();
        let server_cancellation = cancellation.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, process_observability_router(metrics))
                .with_graceful_shutdown(server_cancellation.cancelled_owned())
                .await
                .unwrap();
        });

        let canary_request = format!(
            "GET /{PAYLOAD_CANARY} HTTP/1.1\r\nHost: {IDENTITY_CANARY}\r\ntracestate: {TRACESTATE_CANARY}\r\nbaggage: {BAGGAGE_CANARY}\r\nConnection: close\r\n\r\n"
        );
        let canary_response = scrape_over_tcp(address, &canary_request).await;
        assert!(canary_response.starts_with("HTTP/1.1 404"));

        let scrape = scrape_over_tcp(
            address,
            "GET /metrics HTTP/1.1\r\nHost: prometheus\r\nConnection: close\r\n\r\n",
        )
        .await;
        assert!(scrape.starts_with("HTTP/1.1 200"));
        assert!(scrape.contains("content-type: text/plain; version=0.0.4; charset=utf-8"));
        assert!(scrape.contains(
            "insight_platform_http_requests_total{component_role=\"scheduler-recovery\",operation=\"other\",outcome=\"rejected\"} 1"
        ));
        for forbidden in [
            PAYLOAD_CANARY,
            IDENTITY_CANARY,
            TRACESTATE_CANARY,
            BAGGAGE_CANARY,
            "tracestate",
            "baggage",
        ] {
            assert!(!scrape.contains(forbidden), "scrape leaked {forbidden}");
        }

        cancellation.cancel();
        server.await.unwrap();
    }
}
