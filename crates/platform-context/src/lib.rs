//! Pure ContextQuery, citation, observation, and Text2SQL read-only contracts.
//!
//! This crate performs no I/O. Repositories supply exact frozen facts and database-observed time;
//! the decisions returned here are persisted in caller-owned transactions.

#![allow(async_fn_in_trait)]

mod remote;
mod state;
mod text2sql;
mod types;

pub use remote::*;
pub use state::*;
pub use text2sql::*;
pub use types::*;

use std::{error::Error, fmt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContextQueryError {
    InvalidIdentity,
    InvalidLimits,
    InvalidRequest,
    InvalidBinding,
    InvalidDatasetView,
    InvalidGrant,
    InvalidAdmission,
    AdmissionRejected,
    InvalidJob,
    InvalidOutcome,
    InvalidObservation,
    InvalidCitation,
    InvalidTextToSql,
    InvalidTransition,
    StaleFence,
    FirstWinnerLost,
    Canonicalization,
    CounterOverflow,
}

impl fmt::Display for ContextQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidIdentity => "Context identity is invalid",
            Self::InvalidLimits => "Context limits are invalid",
            Self::InvalidRequest => "Context request is invalid",
            Self::InvalidBinding => "Context binding is invalid",
            Self::InvalidDatasetView => "Context dataset view is invalid",
            Self::InvalidGrant => "Context data access grant is invalid",
            Self::InvalidAdmission => "Context admission snapshot is invalid",
            Self::AdmissionRejected => "Context admission facts reject the query",
            Self::InvalidJob => "Context Job projection is invalid",
            Self::InvalidOutcome => "Context backend outcome is invalid",
            Self::InvalidObservation => "Context observation is invalid",
            Self::InvalidCitation => "Context citation is invalid",
            Self::InvalidTextToSql => "Text2SQL read-only contract is invalid",
            Self::InvalidTransition => "Context state transition is invalid",
            Self::StaleFence => "Context Job lease fence is stale",
            Self::FirstWinnerLost => "Context command lost the first-winner race",
            Self::Canonicalization => "Context canonical serialization failed",
            Self::CounterOverflow => "Context counter overflowed",
        })
    }
}

impl Error for ContextQueryError {}
