use std::{error::Error, fmt};

/// Stable, body-free error returned by the v3 execution model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelError {
    code: &'static str,
    message: String,
}

impl ModelError {
    pub(crate) fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for ModelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for ModelError {}
