pub mod vnext;

use std::{error::Error, fmt};

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
