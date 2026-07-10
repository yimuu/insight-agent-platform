pub mod compiled;
pub mod compiler;
pub mod raw;

use std::{error::Error, fmt};

pub use raw::{parse_raw_agent, DurationSpec, EmitPolicy, RawAgent, RawInput, RawNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileError {
    code: &'static str,
    message: String,
}

impl CompileError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn yaml(message: impl Into<String>) -> Self {
        Self::new("DSL_YAML_INVALID", message)
    }

    pub fn unsupported_version(version: u32) -> Self {
        Self::new(
            "DSL_VERSION_UNSUPPORTED",
            format!("unsupported agent DSL version {version}; expected version 1"),
        )
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for CompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for CompileError {}
