//! CR-216 OpenSandbox-only execution contracts and Dispatcher.
//!
//! Durable business state remains the shared Job/Invocation/RunValue authority. OpenSandbox owns
//! physical lifecycle only; no legacy backend selector or execution fallback is compiled here.

pub mod dispatcher;
pub mod opensandbox;
