//! Crash-safe local journal for one public Run control action.

use crate::run::RunViewV1;
use insight_platform_contracts::ResourceId;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use uuid::Uuid;

const JOURNAL_KIND: &str = "insight.platform.run-control-journal/v1";
const CURSOR_JOURNAL_KIND: &str = "insight.platform.run-cursor-journal/v1";
const MAX_JOURNAL_BYTES: u64 = 65_536;

#[derive(Debug)]
pub enum RunJournalError {
    Io { path: String, detail: String },
    Invalid(String),
}

impl fmt::Display for RunJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, detail } => {
                write!(
                    formatter,
                    "cannot persist Run control journal at {path}: {detail}"
                )
            }
            Self::Invalid(detail) => write!(formatter, "Run control journal is invalid: {detail}"),
        }
    }
}

impl std::error::Error for RunJournalError {}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunControlJournalV1 {
    pub schema_version: u16,
    pub kind: String,
    pub run_id: ResourceId,
    pub action: String,
    pub receipt: String,
    pub if_match: String,
    pub result: Option<RunViewV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunCursorJournalV1 {
    pub schema_version: u16,
    pub kind: String,
    pub run_id: ResourceId,
    pub cursor: Option<String>,
    pub last_sequence: u64,
}

impl RunCursorJournalV1 {
    pub fn new(run_id: ResourceId) -> Self {
        Self {
            schema_version: 1,
            kind: CURSOR_JOURNAL_KIND.to_owned(),
            run_id,
            cursor: None,
            last_sequence: 0,
        }
    }

    pub fn validate(&self) -> Result<(), RunJournalError> {
        if self.schema_version != 1
            || self.kind != CURSOR_JOURNAL_KIND
            || self.run_id.kind() != insight_platform_contracts::ResourceKind::Run
            || self.cursor.as_ref().is_some_and(|cursor| {
                cursor.is_empty()
                    || cursor.len() > 4_096
                    || !cursor.is_ascii()
                    || cursor.bytes().any(|byte| byte.is_ascii_control())
            })
            || (self.cursor.is_none() && self.last_sequence != 0)
        {
            return Err(RunJournalError::Invalid(
                "Run cursor identity, cursor, or sequence is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

impl RunControlJournalV1 {
    pub fn new(run_id: ResourceId, action: String, receipt: String, if_match: String) -> Self {
        Self {
            schema_version: 1,
            kind: JOURNAL_KIND.to_owned(),
            run_id,
            action,
            receipt,
            if_match,
            result: None,
        }
    }

    pub fn validate(&self) -> Result<(), RunJournalError> {
        if self.schema_version != 1
            || self.kind != JOURNAL_KIND
            || !matches!(self.action.as_str(), "pause" | "resume" | "cancel")
            || self.receipt.is_empty()
            || self.receipt.len() > 255
            || !self.receipt.is_ascii()
            || self.receipt.bytes().any(|byte| byte.is_ascii_control())
            || !valid_etag(&self.if_match)
            || self
                .result
                .as_ref()
                .is_some_and(|result| result.run_id != self.run_id)
        {
            return Err(RunJournalError::Invalid(
                "identity, action, Receipt, If-Match, or result closure is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

pub fn journal_path(directory: &Path, run_id: &ResourceId, action: &str) -> PathBuf {
    directory.join(format!("{run_id}-{action}.json"))
}

pub fn load(path: &Path) -> Result<Option<RunControlJournalV1>, RunJournalError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(path, error)),
    };
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_JOURNAL_BYTES {
        return Err(RunJournalError::Invalid(
            "journal is not a bounded regular file".to_owned(),
        ));
    }
    require_private_permissions(path, &metadata)?;
    let bytes = fs::read(path).map_err(|error| io_error(path, error))?;
    let journal = serde_json::from_slice::<RunControlJournalV1>(&bytes)
        .map_err(|_| RunJournalError::Invalid("journal is not closed JSON".to_owned()))?;
    journal.validate()?;
    Ok(Some(journal))
}

pub fn save(path: &Path, journal: &RunControlJournalV1) -> Result<(), RunJournalError> {
    journal.validate()?;
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|_| RunJournalError::Invalid("journal cannot be serialized".to_owned()))?;
    save_bytes(path, &bytes)
}

pub fn cursor_journal_path(directory: &Path, run_id: &ResourceId) -> PathBuf {
    directory.join(format!("{run_id}-events.json"))
}

pub fn load_cursor(path: &Path) -> Result<Option<RunCursorJournalV1>, RunJournalError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(path, error)),
    };
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_JOURNAL_BYTES {
        return Err(RunJournalError::Invalid(
            "cursor journal is not a bounded regular file".to_owned(),
        ));
    }
    require_private_permissions(path, &metadata)?;
    let bytes = fs::read(path).map_err(|error| io_error(path, error))?;
    let journal = serde_json::from_slice::<RunCursorJournalV1>(&bytes)
        .map_err(|_| RunJournalError::Invalid("cursor journal is not closed JSON".to_owned()))?;
    journal.validate()?;
    Ok(Some(journal))
}

pub fn save_cursor(path: &Path, journal: &RunCursorJournalV1) -> Result<(), RunJournalError> {
    journal.validate()?;
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|_| RunJournalError::Invalid("cursor journal cannot be serialized".to_owned()))?;
    save_bytes(path, &bytes)
}

fn save_bytes(path: &Path, bytes: &[u8]) -> Result<(), RunJournalError> {
    let directory = path.parent().ok_or_else(|| {
        RunJournalError::Invalid("journal path has no parent directory".to_owned())
    })?;
    fs::create_dir_all(directory).map_err(|error| io_error(directory, error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error(directory, error))?;
    }
    if bytes.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(RunJournalError::Invalid(
            "journal exceeds its size limit".to_owned(),
        ));
    }
    let temporary = path.with_extension(format!("{}.tmp", Uuid::now_v7()));
    let result = (|| {
        #[cfg(unix)]
        use std::os::unix::fs::OpenOptionsExt as _;

        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options
            .open(&temporary)
            .map_err(|error| io_error(&temporary, error))?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| io_error(&temporary, error))?;
        fs::rename(&temporary, path).map_err(|error| io_error(path, error))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn require_private_permissions(
    path: &Path,
    metadata: &fs::Metadata,
) -> Result<(), RunJournalError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(RunJournalError::Invalid(format!(
                "journal {} grants group or other access",
                path.display()
            )));
        }
    }
    Ok(())
}

fn valid_etag(value: &str) -> bool {
    value.len() >= 3
        && value.len() <= 128
        && value.starts_with('"')
        && value.ends_with('"')
        && value.is_ascii()
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn io_error(path: &Path, error: std::io::Error) -> RunJournalError {
    RunJournalError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}
