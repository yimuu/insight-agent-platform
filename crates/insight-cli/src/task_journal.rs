//! Crash-safe local journal for one public Task mutation.

use crate::task::{SubmitTaskInputV1, TaskViewV1};
use insight_platform_contracts::ResourceId;
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use uuid::Uuid;

const JOURNAL_KIND: &str = "insight.platform.task-control-journal/v1";
const MAX_JOURNAL_BYTES: u64 = 131_072;

#[derive(Debug)]
pub enum TaskJournalError {
    Io { path: String, detail: String },
    Invalid(String),
}

impl fmt::Display for TaskJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, detail } => {
                write!(formatter, "cannot persist Task journal at {path}: {detail}")
            }
            Self::Invalid(detail) => write!(formatter, "Task journal is invalid: {detail}"),
        }
    }
}

impl std::error::Error for TaskJournalError {}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TaskControlJournalV1 {
    pub schema_version: u16,
    pub kind: String,
    pub task_id: ResourceId,
    pub action: String,
    pub receipt: String,
    pub if_match: String,
    pub input: Option<SubmitTaskInputV1>,
    pub result: Option<TaskViewV1>,
}

impl TaskControlJournalV1 {
    pub fn new(
        task_id: ResourceId,
        action: String,
        receipt: String,
        if_match: String,
        input: Option<SubmitTaskInputV1>,
    ) -> Self {
        Self {
            schema_version: 1,
            kind: JOURNAL_KIND.to_owned(),
            task_id,
            action,
            receipt,
            if_match,
            input,
            result: None,
        }
    }

    fn validate(&self) -> Result<(), TaskJournalError> {
        if self.schema_version != 1
            || self.kind != JOURNAL_KIND
            || !matches!(
                self.action.as_str(),
                "submit-input" | "approve" | "reject" | "cancel"
            )
            || (self.action == "submit-input") != self.input.is_some()
            || self.receipt.is_empty()
            || self.receipt.len() > 255
            || !self.receipt.is_ascii()
            || self.receipt.bytes().any(|byte| byte.is_ascii_control())
            || !valid_etag(&self.if_match)
            || self
                .result
                .as_ref()
                .is_some_and(|result| result.task_id != self.task_id)
        {
            return Err(TaskJournalError::Invalid(
                "identity, action, input, Receipt, If-Match, or result is invalid".to_owned(),
            ));
        }
        Ok(())
    }
}

pub fn journal_path(directory: &Path, task_id: &ResourceId, action: &str) -> PathBuf {
    directory.join(format!("{task_id}-{action}.json"))
}

pub fn load(path: &Path) -> Result<Option<TaskControlJournalV1>, TaskJournalError> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error(path, error)),
    };
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_JOURNAL_BYTES {
        return Err(TaskJournalError::Invalid(
            "journal is not a bounded regular file".to_owned(),
        ));
    }
    let journal = serde_json::from_slice::<TaskControlJournalV1>(
        &fs::read(path).map_err(|error| io_error(path, error))?,
    )
    .map_err(|_| TaskJournalError::Invalid("journal is not closed JSON".to_owned()))?;
    journal.validate()?;
    Ok(Some(journal))
}

pub fn save(path: &Path, journal: &TaskControlJournalV1) -> Result<(), TaskJournalError> {
    journal.validate()?;
    let directory = path
        .parent()
        .ok_or_else(|| TaskJournalError::Invalid("journal path has no parent".to_owned()))?;
    fs::create_dir_all(directory).map_err(|error| io_error(directory, error))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error(directory, error))?;
    }
    let bytes = serde_json::to_vec_pretty(journal)
        .map_err(|_| TaskJournalError::Invalid("journal cannot be serialized".to_owned()))?;
    if bytes.len() as u64 > MAX_JOURNAL_BYTES {
        return Err(TaskJournalError::Invalid(
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
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|error| io_error(&temporary, error))?;
        fs::rename(&temporary, path).map_err(|error| io_error(path, error))
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn valid_etag(value: &str) -> bool {
    value.len() >= 3
        && value.len() <= 128
        && value.starts_with('"')
        && value.ends_with('"')
        && value.is_ascii()
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn io_error(path: &Path, error: std::io::Error) -> TaskJournalError {
    TaskJournalError::Io {
        path: path.display().to_string(),
        detail: error.to_string(),
    }
}
