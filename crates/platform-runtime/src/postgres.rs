//! Role-scoped PostgreSQL connection bulkheads.

use insight_platform_contracts::{HardLimitProfile, LimitUnit};
use insight_platform_postgres::repository::PgRepository;
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    PgPool,
};
use std::{error::Error, fmt, str::FromStr, time::Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresConnectionBulkheadConfig {
    pub worker_role: String,
    pub business_max_connections: u32,
    pub critical_control_reserved_connections: u32,
    pub process_connection_budget: u32,
    pub acquire_timeout: Duration,
    pub statement_timeout: Duration,
    pub idle_timeout: Option<Duration>,
    pub max_lifetime: Option<Duration>,
}

impl PostgresConnectionBulkheadConfig {
    pub fn validate(&self, profile: &HardLimitProfile) -> Result<(), PostgresBulkheadError> {
        profile
            .validate()
            .map_err(|failure| PostgresBulkheadError::InvalidProfile(failure.to_string()))?;
        let role_is_valid = !self.worker_role.is_empty()
            && self.worker_role.len() <= 64
            && self.worker_role.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b".-_".contains(&byte)
            });
        let local_total = self
            .business_max_connections
            .checked_add(self.critical_control_reserved_connections)
            .ok_or(PostgresBulkheadError::ConnectionBudgetOverflow)?;
        let profile_connection_budget =
            u32::try_from(profile.control_data.database_connections.q1_default)
                .map_err(|_| PostgresBulkheadError::ConnectionBudgetOverflow)?;
        let statement_milliseconds = u64::try_from(self.statement_timeout.as_millis())
            .map_err(|_| PostgresBulkheadError::StatementTimeoutOutsideProfile)?;
        if !role_is_valid
            || self.business_max_connections == 0
            || self.critical_control_reserved_connections == 0
            || self.process_connection_budget == 0
            || local_total > self.process_connection_budget
            || self.process_connection_budget > profile_connection_budget
            || self.acquire_timeout.is_zero()
            || self.statement_timeout.is_zero()
            || profile.control_data.database_connections.unit != LimitUnit::Connections
            || profile.control_data.transaction_milliseconds.unit != LimitUnit::Milliseconds
            || statement_milliseconds > profile.control_data.transaction_milliseconds.hard_max
            || self.idle_timeout.is_some_and(|value| value.is_zero())
            || self.max_lifetime.is_some_and(|value| value.is_zero())
        {
            return Err(PostgresBulkheadError::InvalidConfig);
        }
        Ok(())
    }

    pub fn configured_connection_total(&self) -> u32 {
        self.business_max_connections
            .saturating_add(self.critical_control_reserved_connections)
    }
}

#[derive(Clone)]
pub struct PostgresConnectionBulkheads {
    config: PostgresConnectionBulkheadConfig,
    business: PgPool,
    critical_control: PgPool,
}

impl PostgresConnectionBulkheads {
    pub async fn connect(
        database_url: &str,
        config: PostgresConnectionBulkheadConfig,
        profile: &HardLimitProfile,
    ) -> Result<Self, PostgresBulkheadError> {
        config.validate(profile)?;
        let base = PgConnectOptions::from_str(database_url)
            .map_err(|_| PostgresBulkheadError::InvalidDatabaseUrl)?;
        let business_options = base
            .clone()
            .application_name(&format!("{}.business", config.worker_role));
        let critical_options =
            base.application_name(&format!("{}.critical-control", config.worker_role));
        let statement_timeout = duration_milliseconds(config.statement_timeout)?;
        let business = pool_options(
            config.business_max_connections,
            config.acquire_timeout,
            config.idle_timeout,
            config.max_lifetime,
            statement_timeout,
        )
        .connect_with(business_options)
        .await
        .map_err(PostgresBulkheadError::Connect)?;
        let critical_control = match pool_options(
            config.critical_control_reserved_connections,
            config.acquire_timeout,
            config.idle_timeout,
            config.max_lifetime,
            statement_timeout,
        )
        .connect_with(critical_options)
        .await
        {
            Ok(pool) => pool,
            Err(failure) => {
                business.close().await;
                return Err(PostgresBulkheadError::Connect(failure));
            }
        };
        Ok(Self {
            config,
            business,
            critical_control,
        })
    }

    pub fn config(&self) -> &PostgresConnectionBulkheadConfig {
        &self.config
    }

    pub fn business_pool(&self) -> &PgPool {
        &self.business
    }

    pub fn critical_control_pool(&self) -> &PgPool {
        &self.critical_control
    }

    pub fn business_repository(&self) -> PgRepository {
        PgRepository::new(self.business.clone())
    }

    pub fn critical_control_repository(&self) -> PgRepository {
        PgRepository::new(self.critical_control.clone())
    }

    pub fn snapshot(&self) -> PostgresConnectionBulkheadSnapshot {
        PostgresConnectionBulkheadSnapshot {
            worker_role: self.config.worker_role.clone(),
            business_max_connections: self.config.business_max_connections,
            critical_control_reserved_connections: self
                .config
                .critical_control_reserved_connections,
            business_open_connections: self.business.size(),
            business_idle_connections: self.business.num_idle(),
            critical_control_open_connections: self.critical_control.size(),
            critical_control_idle_connections: self.critical_control.num_idle(),
        }
    }

    pub async fn close(self) {
        tokio::join!(self.business.close(), self.critical_control.close());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresConnectionBulkheadSnapshot {
    pub worker_role: String,
    pub business_max_connections: u32,
    pub critical_control_reserved_connections: u32,
    pub business_open_connections: u32,
    pub business_idle_connections: usize,
    pub critical_control_open_connections: u32,
    pub critical_control_idle_connections: usize,
}

fn pool_options(
    max_connections: u32,
    acquire_timeout: Duration,
    idle_timeout: Option<Duration>,
    max_lifetime: Option<Duration>,
    statement_timeout_milliseconds: i64,
) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(0)
        .acquire_timeout(acquire_timeout)
        .idle_timeout(idle_timeout)
        .max_lifetime(max_lifetime)
        .after_connect(move |connection, _metadata| {
            Box::pin(async move {
                sqlx::query("SELECT set_config('statement_timeout', $1, false)")
                    .bind(format!("{statement_timeout_milliseconds}ms"))
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
}

fn duration_milliseconds(duration: Duration) -> Result<i64, PostgresBulkheadError> {
    i64::try_from(duration.as_millis())
        .map_err(|_| PostgresBulkheadError::StatementTimeoutOutsideProfile)
}

#[derive(Debug)]
pub enum PostgresBulkheadError {
    InvalidProfile(String),
    InvalidConfig,
    ConnectionBudgetOverflow,
    StatementTimeoutOutsideProfile,
    InvalidDatabaseUrl,
    Connect(sqlx::Error),
}

impl fmt::Display for PostgresBulkheadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidProfile(message) => {
                write!(formatter, "invalid HardLimitProfile: {message}")
            }
            Self::InvalidConfig => formatter.write_str("PostgreSQL connection bulkhead is invalid"),
            Self::ConnectionBudgetOverflow => {
                formatter.write_str("PostgreSQL connection budget exceeds its representation")
            }
            Self::StatementTimeoutOutsideProfile => {
                formatter.write_str("PostgreSQL statement timeout is outside the platform profile")
            }
            Self::InvalidDatabaseUrl => formatter.write_str("PostgreSQL connection URL is invalid"),
            Self::Connect(_) => formatter.write_str("PostgreSQL role pool could not connect"),
        }
    }
}

impl Error for PostgresBulkheadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Connect(failure) => Some(failure),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::checked_in_hard_limit_profile;

    fn config() -> PostgresConnectionBulkheadConfig {
        PostgresConnectionBulkheadConfig {
            worker_role: "orchestration.primary".to_owned(),
            business_max_connections: 8,
            critical_control_reserved_connections: 2,
            process_connection_budget: 12,
            acquire_timeout: Duration::from_secs(2),
            statement_timeout: Duration::from_secs(30),
            idle_timeout: Some(Duration::from_secs(60)),
            max_lifetime: Some(Duration::from_secs(600)),
        }
    }

    #[test]
    fn critical_control_connections_are_a_positive_independent_reserve() {
        let profile = checked_in_hard_limit_profile();
        let config = config();
        config.validate(&profile).unwrap();
        assert_eq!(config.configured_connection_total(), 10);

        let mut no_reserve = config.clone();
        no_reserve.critical_control_reserved_connections = 0;
        assert!(no_reserve.validate(&profile).is_err());

        let mut oversubscribed = config;
        oversubscribed.process_connection_budget = 9;
        assert!(oversubscribed.validate(&profile).is_err());
    }

    #[test]
    fn statement_timeout_cannot_exceed_the_versioned_hard_limit() {
        let profile = checked_in_hard_limit_profile();
        let mut config = config();
        config.statement_timeout =
            Duration::from_millis(profile.control_data.transaction_milliseconds.hard_max + 1);
        assert!(config.validate(&profile).is_err());
    }
}
