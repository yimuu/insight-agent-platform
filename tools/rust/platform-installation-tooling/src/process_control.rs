//! Scoped cancellation for the one-shot owner and its directly owned child handles.
use insight_platform_deployment_contracts::installation::InstallationError as Error;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, OnceLock,
};
use std::time::Duration;
use tokio::sync::{watch, Notify};

static CONTROL: OnceLock<Arc<Control>> = OnceLock::new();
const IDLE: u8 = 0;
const ACTIVE: u8 = 1;
const CLEANUP_UNKNOWN: u8 = 2;

pub struct Control {
    cancelled: watch::Sender<bool>,
    child: AtomicU8,
    changed: Notify,
}
impl Control {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            cancelled: watch::channel(false).0,
            child: AtomicU8::new(IDLE),
            changed: Notify::new(),
        })
    }
    fn cancel(&self) {
        self.cancelled.send_replace(true);
    }
    fn lease(self: &Arc<Self>) -> Result<Lease, Error> {
        if *self.cancelled.borrow() {
            return Err(Error::ExternalOutcomeUnknown);
        }
        self.child
            .compare_exchange(IDLE, ACTIVE, Ordering::SeqCst, Ordering::SeqCst)
            .map_err(|_| Error::Conflict)?;
        Ok(Lease {
            control: Arc::clone(self),
            reaped: false,
        })
    }
}
struct Lease {
    control: Arc<Control>,
    reaped: bool,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.control.child.store(
            if self.reaped { IDLE } else { CLEANUP_UNKNOWN },
            Ordering::SeqCst,
        );
        self.control.changed.notify_one();
    }
}

/// Register both handlers before polling any installer work. On cancellation the original future
/// keeps running only until its registered direct child has completed kill and bounded wait/reap.
pub async fn run<F>(work: F) -> Result<(), Error>
where
    F: std::future::Future<Output = Result<(), Error>>,
{
    let control = Control::new();
    CONTROL
        .set(Arc::clone(&control))
        .map_err(|_| Error::Conflict)?;
    #[cfg(unix)]
    let signal = {
        use tokio::signal::unix::{signal, SignalKind};
        let mut interrupt =
            signal(SignalKind::interrupt()).map_err(|_| Error::PrerequisiteUnavailable)?;
        let mut terminate =
            signal(SignalKind::terminate()).map_err(|_| Error::PrerequisiteUnavailable)?;
        async move {
            tokio::select! { _ = interrupt.recv() => (), _ = terminate.recv() => () }
        }
    };
    #[cfg(not(unix))]
    let signal = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    tokio::pin!(work);
    tokio::select! {
        biased;
        _ = signal => {
            control.cancel();
            loop {
                if control.child.load(Ordering::SeqCst) != ACTIVE {
                    return Err(Error::ExternalOutcomeUnknown);
                }
                tokio::select! {
                    biased;
                    _ = control.changed.notified() => (),
                    _ = &mut work => return Err(Error::ExternalOutcomeUnknown),
                }
            }
        }
        result = &mut work => result,
    }
}

pub enum ChildOutcome {
    Exited(std::process::ExitStatus),
    Interrupted,
}

pub async fn execute(
    command: tokio::process::Command,
    deadline: Duration,
) -> Result<ChildOutcome, Error> {
    execute_with(
        command,
        deadline,
        CONTROL.get().cloned().unwrap_or_else(Control::new),
    )
    .await
}
async fn execute_with(
    mut command: tokio::process::Command,
    deadline: Duration,
    control: Arc<Control>,
) -> Result<ChildOutcome, Error> {
    // No await exists between cancellation check, registration and spawn. The main task therefore
    // cannot observe IDLE after a child exists but before its cleanup obligation is registered.
    let mut lease = control.lease()?;
    let mut child = match command.kill_on_drop(true).spawn() {
        Ok(child) => child,
        Err(_) => {
            lease.reaped = true;
            return Err(Error::PrerequisiteUnavailable);
        }
    };
    let mut cancellation = control.cancelled.subscribe();
    let cancelled = async {
        loop {
            if *cancellation.borrow_and_update() {
                break;
            }
            if cancellation.changed().await.is_err() {
                break;
            }
        }
    };
    let status = tokio::select! {
        biased;
        _ = cancelled => None,
        result = tokio::time::timeout(deadline, child.wait()) => match result {
            Ok(Ok(status)) => Some(status),
            _ => None,
        },
    };
    if let Some(status) = status {
        lease.reaped = true;
        return Ok(ChildOutcome::Exited(status));
    }
    // Try wait also closes the race in which the child exited just before cancellation.
    if child
        .try_wait()
        .map_err(|_| Error::ExternalOutcomeUnknown)?
        .is_none()
    {
        child
            .start_kill()
            .map_err(|_| Error::ExternalOutcomeUnknown)?;
    }
    tokio::time::timeout(Duration::from_secs(5), child.wait())
        .await
        .map_err(|_| Error::ExternalOutcomeUnknown)?
        .map_err(|_| Error::ExternalOutcomeUnknown)?;
    lease.reaped = true;
    Ok(ChildOutcome::Interrupted)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::process::Stdio;
    #[tokio::test]
    async fn cancellation_reaps_exact_child_before_releasing_lease_and_forbids_new_spawn() {
        let temporary = tempfile::tempdir().unwrap();
        let file = temporary.path().join("child.pid");
        let control = Control::new();
        let mut command = tokio::process::Command::new("/bin/sh");
        command
            .args([
                "-c",
                "printf '%s' \"$$\" > \"$1\"; exec /bin/sleep 60",
                "fixture",
            ])
            .arg(&file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child_control = Arc::clone(&control);
        let work = tokio::spawn(async move {
            execute_with(command, Duration::from_secs(60), child_control).await
        });
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        let pid = loop {
            if let Ok(text) = std::fs::read_to_string(&file) {
                if let Ok(pid) = text.parse::<u32>() {
                    break pid;
                }
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert_eq!(control.child.load(Ordering::SeqCst), ACTIVE);
        control.cancel();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(6), work)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            ChildOutcome::Interrupted
        ));
        assert_eq!(control.child.load(Ordering::SeqCst), IDLE);
        assert!(!std::process::Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success());
        let mut forbidden = tokio::process::Command::new("/usr/bin/touch");
        let marker = temporary.path().join("must-not-exist");
        forbidden.arg(&marker);
        assert!(matches!(
            execute_with(forbidden, Duration::from_secs(1), control).await,
            Err(Error::ExternalOutcomeUnknown)
        ));
        assert!(!marker.exists());
    }
    #[tokio::test]
    #[ignore = "only spawned by the parent signal boundary test with a private fixture path"]
    async fn signal_fixture() {
        let file = std::path::PathBuf::from(
            std::env::var_os("INSIGHT_INSTALLER_SIGNAL_FIXTURE").expect("explicit fixture"),
        );
        assert!(file.is_absolute());
        let result = super::run(async {
            let mut command = tokio::process::Command::new("/bin/sh");
            command
                .args([
                    "-c",
                    "printf '%s' \"$$\" > \"$1\"; exec /bin/sleep 60",
                    "fixture",
                ])
                .arg(&file)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let _ = execute(command, Duration::from_secs(60)).await?;
            Err(Error::ExternalOutcomeUnknown)
        })
        .await;
        assert_eq!(result, Err(Error::ExternalOutcomeUnknown));
        assert_eq!(CONTROL.get().unwrap().child.load(Ordering::SeqCst), IDLE);
    }

    #[tokio::test]
    async fn actual_termination_signal_waits_for_child_reap_before_parent_exit() {
        let temporary = tempfile::tempdir().unwrap();
        let file = temporary.path().join("child.pid");
        let mut parent = tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process_control::tests::signal_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env_clear()
            .env("INSIGHT_INSTALLER_SIGNAL_FIXTURE", &file)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let pid = loop {
            if let Ok(text) = std::fs::read_to_string(&file) {
                if let Ok(pid) = text.parse::<u32>() {
                    break pid;
                }
            }
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        };
        assert!(std::process::Command::new("/bin/kill")
            .args(["-TERM", &parent.id().unwrap().to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success());
        assert!(tokio::time::timeout(Duration::from_secs(6), parent.wait())
            .await
            .unwrap()
            .unwrap()
            .success());
        assert!(!std::process::Command::new("/bin/kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success());
    }
}
