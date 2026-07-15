use std::{
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::FutureExt;
use sqlx::{migrate::Migrator, Connection, PgConnection};
use tokio::{
    sync::{oneshot, watch},
    task::JoinHandle,
    time::{interval, timeout, MissedTickBehavior},
};
use uuid::Uuid;

use crate::history::repository::HistoryError;

const ADVISORY_LOCK_NAMESPACE: i64 = 0x4941_5001;
const MONITOR_INTERVAL: Duration = Duration::from_secs(1);
const OWNERSHIP_ACTIVE: u8 = 0;
const OWNERSHIP_RELEASING: u8 = 1;
const OWNERSHIP_LOST: u8 = 2;
const OWNERSHIP_RELEASED: u8 = 3;

#[derive(Clone)]
pub(super) struct OwnershipToken {
    owner_id: Arc<str>,
    generation: i64,
    schema_oid: i64,
}

impl OwnershipToken {
    pub(super) fn schema_oid(&self) -> i64 {
        self.schema_oid
    }

    pub(super) fn matches(&self, stored: Option<&(Option<String>, i64)>) -> bool {
        matches!(
            stored,
            Some((Some(owner_id), generation))
                if owner_id == self.owner_id.as_ref() && *generation == self.generation
        )
    }

    pub(super) fn matches_health(&self, stored: Option<&(Option<String>, i64, i64)>) -> bool {
        matches!(
            stored,
            Some((Some(owner_id), generation, schema_oid))
                if owner_id == self.owner_id.as_ref()
                    && *generation == self.generation
                    && *schema_oid == self.schema_oid
        )
    }
}

#[derive(Clone)]
pub(super) struct OwnershipState {
    inner: Arc<OwnershipStateInner>,
}

struct OwnershipStateInner {
    status: AtomicU8,
    loss_changed: watch::Sender<bool>,
}

impl OwnershipState {
    fn new() -> Self {
        let (loss_changed, _receiver) = watch::channel(false);
        Self {
            inner: Arc::new(OwnershipStateInner {
                status: AtomicU8::new(OWNERSHIP_ACTIVE),
                loss_changed,
            }),
        }
    }

    pub(super) fn mark_lost(&self) {
        loop {
            let status = self.inner.status.load(Ordering::Acquire);
            if matches!(status, OWNERSHIP_LOST | OWNERSHIP_RELEASED) {
                return;
            }
            if self
                .inner
                .status
                .compare_exchange(status, OWNERSHIP_LOST, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                self.inner.loss_changed.send_replace(true);
                return;
            }
        }
    }

    pub(super) fn ensure_current(&self) -> Result<(), HistoryError> {
        if self.inner.status.load(Ordering::Acquire) != OWNERSHIP_ACTIVE {
            Err(ownership_lost())
        } else {
            Ok(())
        }
    }

    fn is_lost(&self) -> bool {
        self.inner.status.load(Ordering::Acquire) == OWNERSHIP_LOST
    }

    fn subscribe_loss(&self) -> watch::Receiver<bool> {
        self.inner.loss_changed.subscribe()
    }

    fn begin_release(&self) -> Result<(), HistoryError> {
        self.inner
            .status
            .compare_exchange(
                OWNERSHIP_ACTIVE,
                OWNERSHIP_RELEASING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| ownership_lost())
    }

    fn mark_released(&self) -> Result<(), HistoryError> {
        self.inner
            .status
            .compare_exchange(
                OWNERSHIP_RELEASING,
                OWNERSHIP_RELEASED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(|_| ownership_lost())
    }
}

/// Retains the dedicated PostgreSQL session that owns one Formal V1 store.
#[doc(hidden)]
pub struct PostgresStoreOwner {
    state: OwnershipState,
    release: Option<oneshot::Sender<()>>,
    monitor: Option<JoinHandle<Result<(), HistoryError>>>,
    backend_pid: i32,
}

impl PostgresStoreOwner {
    pub fn subscribe_loss(&self) -> watch::Receiver<bool> {
        self.state.subscribe_loss()
    }

    pub fn is_lost(&self) -> bool {
        self.state.is_lost()
    }

    #[doc(hidden)]
    pub fn backend_pid(&self) -> i32 {
        self.backend_pid
    }

    pub async fn release(mut self) -> Result<(), HistoryError> {
        let release_started = self.state.begin_release().is_ok();
        let sent = self
            .release
            .take()
            .map(|release| release.send(()).is_ok())
            .unwrap_or(false);
        let result = match self.monitor.take() {
            Some(monitor) => monitor.await.map_err(|_| ownership_lost())?,
            None => Err(ownership_lost()),
        };
        if !release_started || !sent || self.state.is_lost() {
            return Err(ownership_lost());
        }
        result
    }
}

pub(super) async fn acquire(
    database_url: &str,
    migrator: &'static Migrator,
    operation_timeout: Duration,
    probe_timeout: Duration,
) -> Result<(OwnershipToken, OwnershipState, PostgresStoreOwner), HistoryError> {
    let mut connection = PgConnection::connect(database_url)
        .await
        .map_err(|error| init_error("failed to initialize PostgreSQL ownership", error))?;
    let schema_oid = current_schema_oid(&mut connection)
        .await
        .map_err(|error| init_error("failed to resolve PostgreSQL history schema", error))?;
    let lock_key = advisory_lock_key(schema_oid)?;
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(lock_key)
        .fetch_one(&mut connection)
        .await
        .map_err(|error| init_error("failed to acquire PostgreSQL history ownership", error))?;
    if !acquired {
        return Err(HistoryError::new(
            "HISTORY_STORE_ALREADY_OWNED",
            "PostgreSQL history store is already owned",
        ));
    }

    migrator
        .run(&mut connection)
        .await
        .map_err(|error| init_error("failed to migrate PostgreSQL history", error))?;

    let owner_id = Uuid::new_v4().to_string();
    let generation = timeout(
        operation_timeout,
        claim_generation(&mut connection, &owner_id),
    )
    .await
    .map_err(|_| {
        HistoryError::new(
            "HISTORY_INIT_FAILED",
            "PostgreSQL history ownership claim timed out",
        )
    })??;
    let backend_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut connection)
        .await
        .map_err(|error| init_error("failed to initialize PostgreSQL ownership", error))?;

    let token = OwnershipToken {
        owner_id: Arc::from(owner_id),
        generation,
        schema_oid,
    };
    let state = OwnershipState::new();
    let (release, release_received) = oneshot::channel();
    let monitor_state = state.clone();
    let monitor_token = token.clone();
    let monitor = tokio::spawn(async move {
        let monitored = AssertUnwindSafe(monitor_connection(
            connection,
            monitor_token,
            lock_key,
            probe_timeout,
            release_received,
            monitor_state.clone(),
        ))
        .catch_unwind()
        .await;
        match monitored {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                monitor_state.mark_lost();
                Err(error)
            }
            Err(_) => {
                monitor_state.mark_lost();
                Err(ownership_lost())
            }
        }
    });

    let owner = PostgresStoreOwner {
        state: state.clone(),
        release: Some(release),
        monitor: Some(monitor),
        backend_pid,
    };
    Ok((token, state, owner))
}

async fn current_schema_oid(connection: &mut PgConnection) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT oid::bigint FROM pg_namespace WHERE nspname = current_schema()")
        .fetch_optional(connection)
        .await?
        .ok_or_else(|| {
            sqlx::Error::Protocol("PostgreSQL current schema is unavailable".to_string())
        })
}

fn advisory_lock_key(schema_oid: i64) -> Result<i64, HistoryError> {
    let schema_oid = u32::try_from(schema_oid).map_err(|_| {
        HistoryError::new(
            "HISTORY_INIT_FAILED",
            "PostgreSQL history schema identity is invalid",
        )
    })?;
    Ok((ADVISORY_LOCK_NAMESPACE << 32) | i64::from(schema_oid))
}

async fn claim_generation(
    connection: &mut PgConnection,
    owner_id: &str,
) -> Result<i64, HistoryError> {
    let mut transaction = connection
        .begin()
        .await
        .map_err(|error| init_error("failed to claim PostgreSQL history ownership", error))?;
    let stored: Option<(i64, Option<String>)> = sqlx::query_as(
        "SELECT generation, owner_id
         FROM runtime_ownership
         WHERE singleton = 1
         FOR UPDATE",
    )
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|error| init_error("failed to claim PostgreSQL history ownership", error))?;
    let Some((generation, stored_owner)) = stored else {
        return Err(HistoryError::new(
            "HISTORY_INIT_FAILED",
            "PostgreSQL history ownership metadata is invalid",
        ));
    };
    if generation < 0 || (generation == 0) != stored_owner.is_none() {
        return Err(HistoryError::new(
            "HISTORY_INIT_FAILED",
            "PostgreSQL history ownership metadata is invalid",
        ));
    }
    let next = generation.checked_add(1).ok_or_else(|| {
        HistoryError::new(
            "HISTORY_INIT_FAILED",
            "PostgreSQL history ownership generation overflowed",
        )
    })?;
    let updated = sqlx::query(
        "UPDATE runtime_ownership
         SET generation = $1, owner_id = $2, claimed_at = CURRENT_TIMESTAMP
         WHERE singleton = 1",
    )
    .bind(next)
    .bind(owner_id)
    .execute(&mut *transaction)
    .await
    .map_err(|error| init_error("failed to claim PostgreSQL history ownership", error))?;
    if updated.rows_affected() != 1 {
        return Err(HistoryError::new(
            "HISTORY_INIT_FAILED",
            "PostgreSQL history ownership metadata is invalid",
        ));
    }
    transaction
        .commit()
        .await
        .map_err(|error| init_error("failed to claim PostgreSQL history ownership", error))?;
    Ok(next)
}

async fn monitor_connection(
    connection: PgConnection,
    token: OwnershipToken,
    lock_key: i64,
    probe_timeout: Duration,
    mut release: oneshot::Receiver<()>,
    state: OwnershipState,
) -> Result<(), HistoryError> {
    let mut monitored = MonitoredConnection::new(connection, state.clone());
    let mut probes = interval(MONITOR_INTERVAL);
    probes.set_missed_tick_behavior(MissedTickBehavior::Delay);
    probes.tick().await;
    loop {
        tokio::select! {
            biased;
            command = &mut release => {
                if command.is_err() {
                    return Err(ownership_lost());
                }
                let unlocked = timeout(
                    probe_timeout,
                    release_current(monitored.connection_mut(), lock_key, &token),
                )
                .await
                .map_err(|_| ownership_lost())?
                .map_err(|_| ownership_lost())?;
                if !unlocked {
                    return Err(ownership_lost());
                }
                state.mark_released()?;
                monitored.disarm();
                return Ok(());
            }
            _ = probes.tick() => {
                let current = timeout(
                    probe_timeout,
                    probe_current(monitored.connection_mut(), &token),
                )
                    .await
                    .map_err(|_| ownership_lost())?
                    .map_err(|_| ownership_lost())?;
                if !current {
                    return Err(ownership_lost());
                }
            }
        }
    }
}

async fn release_current(
    connection: &mut PgConnection,
    lock_key: i64,
    token: &OwnershipToken,
) -> Result<bool, sqlx::Error> {
    let unlocked: Option<bool> = sqlx::query_scalar(
        "SELECT CASE
             WHEN EXISTS (
                 SELECT 1
                 FROM runtime_ownership ownership
                 JOIN pg_namespace namespace ON namespace.nspname = current_schema()
                 WHERE ownership.singleton = 1
                   AND ownership.owner_id = $2
                   AND ownership.generation = $3
                   AND namespace.oid::bigint = $4
             )
             THEN pg_advisory_unlock($1)
         END",
    )
    .bind(lock_key)
    .bind(token.owner_id.as_ref())
    .bind(token.generation)
    .bind(token.schema_oid)
    .fetch_one(connection)
    .await?;
    Ok(unlocked == Some(true))
}

async fn probe_current(
    connection: &mut PgConnection,
    token: &OwnershipToken,
) -> Result<bool, sqlx::Error> {
    let stored = sqlx::query_as::<_, (Option<String>, i64, i64)>(
        "SELECT ownership.owner_id, ownership.generation, namespace.oid::bigint
         FROM runtime_ownership ownership
         JOIN pg_namespace namespace ON namespace.nspname = current_schema()
         WHERE ownership.singleton = 1",
    )
    .fetch_optional(connection)
    .await?;
    Ok(token.matches_health(stored.as_ref()))
}

struct MonitoredConnection {
    connection: PgConnection,
    state: OwnershipState,
    armed: bool,
}

impl MonitoredConnection {
    fn new(connection: PgConnection, state: OwnershipState) -> Self {
        Self {
            connection,
            state,
            armed: true,
        }
    }

    fn connection_mut(&mut self) -> &mut PgConnection {
        &mut self.connection
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for MonitoredConnection {
    fn drop(&mut self) {
        if self.armed {
            self.state.mark_lost();
        }
    }
}

pub(super) fn ownership_lost() -> HistoryError {
    HistoryError::new(
        "HISTORY_OWNERSHIP_LOST",
        "PostgreSQL history store ownership was lost",
    )
}

fn init_error<E>(message: &'static str, source: E) -> HistoryError
where
    E: std::error::Error + Send + Sync + 'static,
{
    HistoryError::with_source("HISTORY_INIT_FAILED", message, source)
}
