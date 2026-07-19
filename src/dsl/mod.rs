pub mod v3;

use std::{error::Error, fmt};

/// One segment in an authored DSL path.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DslPathSegment {
    Key(String),
    Index(usize),
}

/// A structured path into an authored DSL document.
///
/// Paths intentionally do not retain source values. Display uses a stable
/// JSONPath-like representation solely for diagnostics and developer tools.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DslPath(Vec<DslPathSegment>);

impl DslPath {
    pub fn root() -> Self {
        Self::default()
    }

    pub fn from_segments(segments: impl IntoIterator<Item = DslPathSegment>) -> Self {
        Self(segments.into_iter().collect())
    }

    pub fn child_key(&self, key: impl Into<String>) -> Self {
        let mut segments = self.0.clone();
        segments.push(DslPathSegment::Key(key.into()));
        Self(segments)
    }

    pub fn child_index(&self, index: usize) -> Self {
        let mut segments = self.0.clone();
        segments.push(DslPathSegment::Index(index));
        Self(segments)
    }

    pub fn segments(&self) -> &[DslPathSegment] {
        &self.0
    }

    pub fn is_root(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn from_serde_path(path: &str) -> Option<Self> {
        if path == "." {
            return Some(Self::root());
        }

        let mut segments = Vec::new();
        let mut remaining = path.strip_prefix('.').unwrap_or(path);
        while !remaining.is_empty() {
            if let Some(after_open) = remaining.strip_prefix('[') {
                let (index, after_close) = after_open.split_once(']')?;
                segments.push(DslPathSegment::Index(index.parse().ok()?));
                remaining = after_close.strip_prefix('.').unwrap_or(after_close);
                continue;
            }

            let boundary = remaining
                .char_indices()
                .find_map(|(index, character)| matches!(character, '.' | '[').then_some(index))
                .unwrap_or(remaining.len());
            if boundary == 0 {
                return None;
            }
            let key = &remaining[..boundary];
            if key == "?" {
                return None;
            }
            segments.push(DslPathSegment::Key(key.to_string()));
            remaining = &remaining[boundary..];
            remaining = remaining.strip_prefix('.').unwrap_or(remaining);
        }
        Some(Self(segments))
    }
}

impl fmt::Display for DslPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("$")?;
        for segment in &self.0 {
            match segment {
                DslPathSegment::Key(key) if is_simple_path_key(key) => {
                    write!(formatter, ".{key}")?;
                }
                DslPathSegment::Key(key) => {
                    let encoded = serde_json::to_string(key).map_err(|_| fmt::Error)?;
                    write!(formatter, "[{encoded}]")?;
                }
                DslPathSegment::Index(index) => write!(formatter, "[{index}]")?,
            }
        }
        Ok(())
    }
}

fn is_simple_path_key(key: &str) -> bool {
    let mut characters = key.chars();
    characters
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric())
}

/// A half-open byte span plus 1-based Unicode-scalar line and column pairs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSpan {
    byte_start: u64,
    byte_end: u64,
    line_start: u32,
    column_start: u32,
    line_end: u32,
    column_end: u32,
}

impl SourceSpan {
    pub fn new(
        byte_start: u64,
        byte_end: u64,
        line_start: u32,
        column_start: u32,
        line_end: u32,
        column_end: u32,
    ) -> Self {
        Self {
            byte_start,
            byte_end,
            line_start,
            column_start,
            line_end,
            column_end,
        }
    }

    pub fn byte_start(&self) -> u64 {
        self.byte_start
    }

    pub fn byte_end(&self) -> u64 {
        self.byte_end
    }

    pub fn line_start(&self) -> u32 {
        self.line_start
    }

    pub fn column_start(&self) -> u32 {
        self.column_start
    }

    pub fn line_end(&self) -> u32 {
        self.line_end
    }

    pub fn column_end(&self) -> u32 {
        self.column_end
    }

    #[cfg(test)]
    pub(crate) fn document(source: &str) -> Self {
        let (line_end, column_end) = source_position(source, source.len());
        Self::new(0, source.len() as u64, 1, 1, line_end, column_end)
    }

    pub(crate) fn range(source: &str, byte_start: usize, byte_end: usize) -> Self {
        let byte_start = floor_char_boundary(source, byte_start.min(source.len()));
        let byte_end = floor_char_boundary(source, byte_end.clamp(byte_start, source.len()));
        let (line_start, column_start) = source_position(source, byte_start);
        let (line_end, column_end) = source_position(source, byte_end);
        Self::new(
            byte_start as u64,
            byte_end as u64,
            line_start,
            column_start,
            line_end,
            column_end,
        )
    }

    pub(crate) fn point(source: &str, byte_index: usize) -> Self {
        let byte_start = floor_char_boundary(source, byte_index.min(source.len()));
        let byte_end = source[byte_start..]
            .chars()
            .next()
            .map_or(byte_start, |character| byte_start + character.len_utf8());
        Self::range(source, byte_start, byte_end)
    }
}

fn floor_char_boundary(source: &str, mut byte_index: usize) -> usize {
    while !source.is_char_boundary(byte_index) {
        byte_index -= 1;
    }
    byte_index
}

fn source_position(source: &str, byte_index: usize) -> (u32, u32) {
    let mut line = 1u32;
    let mut column = 1u32;
    for character in source[..byte_index].chars() {
        if character == '\n' {
            line = line.saturating_add(1);
            column = 1;
        } else {
            column = column.saturating_add(1);
        }
    }
    (line, column)
}

/// A sanitized parse failure with optional authored location information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DslParseError {
    code: &'static str,
    message: String,
    path: Option<DslPath>,
    span: Option<SourceSpan>,
}

impl DslParseError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            path: None,
            span: None,
        }
    }

    pub fn at(mut self, path: DslPath, span: Option<SourceSpan>) -> Self {
        self.path = Some(path);
        self.span = span;
        self
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn path(&self) -> Option<&DslPath> {
        self.path.as_ref()
    }

    pub fn span(&self) -> Option<SourceSpan> {
        self.span
    }
}

impl fmt::Display for DslParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for DslParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompileError {
    code: &'static str,
    message: String,
    diagnostics: Option<Box<CompileDiagnostics>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct CompileDiagnostics {
    agent_id: Option<String>,
    step_id: Option<String>,
    path: Option<DslPath>,
    span: Option<SourceSpan>,
    decoded_template_span: Option<SourceSpan>,
}

impl CompileError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            diagnostics: None,
        }
    }

    pub fn at(mut self, path: DslPath, span: SourceSpan) -> Self {
        let diagnostics = self.diagnostics_mut();
        diagnostics.path = Some(path);
        diagnostics.span = Some(span);
        self
    }

    pub fn with_path(mut self, path: DslPath) -> Self {
        self.diagnostics_mut().path = Some(path);
        self
    }

    pub fn with_agent_id(mut self, agent_id: impl Into<String>) -> Self {
        self.diagnostics_mut().agent_id = Some(agent_id.into());
        self
    }

    pub fn with_step_id(mut self, step_id: impl Into<String>) -> Self {
        self.diagnostics_mut().step_id = Some(step_id.into());
        self
    }

    pub fn with_span(mut self, span: SourceSpan) -> Self {
        self.diagnostics_mut().span = Some(span);
        self
    }

    /// Attach a location in the decoded template source. This coordinate
    /// system is intentionally independent from the authored YAML/JSON span.
    pub fn with_decoded_template_span(mut self, span: SourceSpan) -> Self {
        self.diagnostics_mut().decoded_template_span = Some(span);
        self
    }

    fn diagnostics_mut(&mut self) -> &mut CompileDiagnostics {
        self.diagnostics
            .get_or_insert_with(|| Box::new(CompileDiagnostics::default()))
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn agent_id(&self) -> Option<&str> {
        self.diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.agent_id.as_deref())
    }

    pub fn step_id(&self) -> Option<&str> {
        self.diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.step_id.as_deref())
    }

    pub fn path(&self) -> Option<&DslPath> {
        self.diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.path.as_ref())
    }

    pub fn span(&self) -> Option<SourceSpan> {
        self.diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.span)
    }

    pub fn decoded_template_span(&self) -> Option<SourceSpan> {
        self.diagnostics
            .as_deref()
            .and_then(|diagnostics| diagnostics.decoded_template_span)
    }
}

impl fmt::Display for CompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for CompileError {}

impl From<DslParseError> for CompileError {
    fn from(error: DslParseError) -> Self {
        let diagnostics = (error.path.is_some() || error.span.is_some()).then(|| {
            Box::new(CompileDiagnostics {
                path: error.path,
                span: error.span,
                ..CompileDiagnostics::default()
            })
        });
        Self {
            code: error.code,
            message: error.message,
            diagnostics,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CompileError, DslPath, DslPathSegment, SourceSpan};

    #[test]
    fn dsl_paths_are_structured_and_render_stably() {
        let path = DslPath::from_segments([
            DslPathSegment::Key("workflow".to_string()),
            DslPathSegment::Key("steps".to_string()),
            DslPathSegment::Index(2),
            DslPathSegment::Key("$value".to_string()),
        ]);

        assert_eq!(path.to_string(), "$.workflow.steps[2][\"$value\"]");
        assert_eq!(
            DslPath::from_serde_path("workflow.steps[2].result"),
            Some(
                DslPath::root()
                    .child_key("workflow")
                    .child_key("steps")
                    .child_index(2)
                    .child_key("result")
            )
        );
    }

    #[test]
    fn source_spans_use_utf8_bytes_and_unicode_scalar_columns() {
        let source = "名: 值\nnext: true\n";
        let byte_index = source.find('值').unwrap();
        let span = SourceSpan::point(source, byte_index);

        assert_eq!(span.byte_start(), byte_index as u64);
        assert_eq!(span.byte_end(), (byte_index + '值'.len_utf8()) as u64);
        assert_eq!((span.line_start(), span.column_start()), (1, 4));
        assert_eq!((span.line_end(), span.column_end()), (1, 5));

        let document = SourceSpan::document(source);
        assert_eq!(document.byte_end(), source.len() as u64);
        assert_eq!((document.line_end(), document.column_end()), (3, 1));
    }

    #[test]
    fn compile_error_location_is_structured_but_not_rendered_with_source() {
        let path = DslPath::root().child_key("workflow");
        let span = SourceSpan::new(3, 4, 2, 1, 2, 2);
        let decoded_template_span = SourceSpan::new(8, 9, 3, 4, 3, 5);
        let error = CompileError::new("TEST", "safe message")
            .at(path.clone(), span)
            .with_decoded_template_span(decoded_template_span);

        assert_eq!(error.path(), Some(&path));
        assert_eq!(error.span(), Some(span));
        assert_eq!(error.decoded_template_span(), Some(decoded_template_span));
        assert_eq!(error.to_string(), "safe message");
    }

    #[test]
    fn compile_error_keeps_optional_diagnostics_out_of_the_result_inline_size() {
        assert!(std::mem::size_of::<CompileError>() <= 64);
    }
}
