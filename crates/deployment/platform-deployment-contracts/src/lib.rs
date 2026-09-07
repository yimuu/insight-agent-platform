//! Versioned deployment and qualification artifact contracts.
//! These values are release evidence, never business state or promotion authority.

pub mod qualification;
pub use qualification::*;

pub mod outbox;

pub mod schema;
pub mod workers;

pub mod history;
pub mod recovery;

pub mod development;
