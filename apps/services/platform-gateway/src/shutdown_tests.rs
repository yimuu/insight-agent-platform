use super::*;
use std::{
    future::{Future, IntoFuture},
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    process::{Child, Command, Stdio},
    task::Poll,
};

struct Fixture {
    child: Child,
    directory: PathBuf,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn actual_sigterm_drains_http_listener_and_reaps_owned_gateway_fixture() {
    let directory =
        std::env::temp_dir().join(format!("insight-gateway-term-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory).unwrap();
    let ready = directory.join("ready");
    let child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "shutdown_tests::gateway_sigterm_child",
            "--ignored",
        ])
        .env("INSIGHT_GATEWAY_TERM_READY", &ready)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut fixture = Fixture { child, directory };
    let deadline = Instant::now() + Duration::from_secs(5);
    let address: SocketAddr = loop {
        if let Ok(address) = std::fs::read_to_string(&ready) {
            if let Ok(address) = address.parse() {
                break address;
            }
        }
        assert!(fixture.child.try_wait().unwrap().is_none());
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut socket = TcpStream::connect_timeout(&address, Duration::from_secs(1)).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    socket
        .write_all(b"GET /readyz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut response = String::new();
    socket.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with("ready"));
    drop(socket);
    assert!(Command::new("/bin/kill")
        .args(["-TERM", &fixture.child.id().to_string()])
        .status()
        .unwrap()
        .success());
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = fixture.child.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(fixture.child.wait().unwrap().success());
    assert!(TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err());
}

#[tokio::test]
#[ignore = "isolated signal helper executed by actual_sigterm_drains_http_listener_and_reaps_owned_gateway_fixture"]
async fn gateway_sigterm_child() {
    let path = std::env::var_os("INSIGHT_GATEWAY_TERM_READY").expect("parent fixture path");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut signal = Box::pin(shutdown_signal());
    // Register both signal handlers before telling the parent it may send SIGTERM.
    std::future::poll_fn(|context| {
        assert!(signal.as_mut().poll(context).is_pending());
        Poll::Ready(())
    })
    .await;
    std::fs::write(path, address.to_string()).unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        axum::serve(
            listener,
            Router::new().route("/readyz", axum::routing::get(|| async { "ready" })),
        )
        .with_graceful_shutdown(async move {
            signal.await.unwrap();
        })
        .into_future(),
    )
    .await
    .unwrap()
    .unwrap();
}
