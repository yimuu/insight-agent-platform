use insight_platform_contracts::{
    canonical_digest, ExactVersionRef, ResourceKind, Sha256Digest, SkillInstructionAudience,
    SkillInstructionPhase, SkillInstructionSection, SkillPackageEntryKind, SkillPackageManifest,
    SkillResourceSpec, MAX_SKILL_PACKAGE_BYTES,
};
use sha2::{Digest as _, Sha256};
use std::{fmt::Write as _, sync::Arc};

use crate::{
    ArtifactObjectReadAuthorityError, SchedulerSkillPackageLease, SchedulerSkillPackageReadError,
    SchedulerSkillPackageReader, SchedulerSkillPackageRequestResolver,
};

pub const SKILL_PACKAGE_FRAME_MAGIC: &[u8; 24] = b"INSIGHT-SKILL-PACKAGE/1\n";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillPackageContent {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedSkillInstruction {
    pub section_id: String,
    pub phase: SkillInstructionPhase,
    pub audience: SkillInstructionAudience,
    pub source_path: String,
    pub content_digest: Sha256Digest,
    pub data_classification: insight_platform_contracts::DataClassification,
    pub max_tokens: u32,
    pub text: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillPackageFrameError {
    InvalidManifest,
    InvalidFrame,
    TooLarge,
    Integrity,
    InstructionNotFound,
    InvalidInstructionText,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchedulerSkillInstructionMaterializeError {
    Unavailable,
    Denied,
    NotFound,
    TooLarge,
    Integrity,
}

pub struct BrokeredSchedulerSkillInstructionMaterializer {
    resolver: Arc<dyn SchedulerSkillPackageRequestResolver>,
    reader: Arc<dyn SchedulerSkillPackageReader>,
}

impl BrokeredSchedulerSkillInstructionMaterializer {
    pub fn new(
        resolver: Arc<dyn SchedulerSkillPackageRequestResolver>,
        reader: Arc<dyn SchedulerSkillPackageReader>,
    ) -> Self {
        Self { resolver, reader }
    }

    pub async fn materialize(
        &self,
        lease: SchedulerSkillPackageLease,
        skill_revision: &ExactVersionRef,
        skill: &SkillResourceSpec,
        section_id: &str,
    ) -> Result<MaterializedSkillInstruction, SchedulerSkillInstructionMaterializeError> {
        self.materialize_all(lease, skill_revision, skill)
            .await?
            .into_iter()
            .find(|materialized| materialized.section_id == section_id)
            .ok_or(SchedulerSkillInstructionMaterializeError::NotFound)
    }

    pub async fn materialize_all(
        &self,
        lease: SchedulerSkillPackageLease,
        skill_revision: &ExactVersionRef,
        skill: &SkillResourceSpec,
    ) -> Result<Vec<MaterializedSkillInstruction>, SchedulerSkillInstructionMaterializeError> {
        if skill_revision.resource_kind != ResourceKind::SkillRevision
            || skill.authoring_package.artifact.media_type()
                != insight_platform_contracts::SKILL_PACKAGE_MEDIA_TYPE
        {
            return Err(SchedulerSkillInstructionMaterializeError::Integrity);
        }
        let request = self
            .resolver
            .resolve_skill_package_read(lease)
            .await
            .map_err(map_authority_error)?;
        if request.skill_revision_id != skill_revision.revision_id
            || request.manifest_digest != skill.manifest.canonical_digest
            || request.artifact != skill.authoring_package.artifact
        {
            return Err(SchedulerSkillInstructionMaterializeError::Integrity);
        }
        let bytes = self
            .reader
            .read_exact(request)
            .await
            .map_err(map_read_error)?;
        skill
            .instruction_sections
            .iter()
            .map(|section| materialize_skill_instruction(&bytes, &skill.manifest, section))
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_frame_error)
    }
}

pub fn encode_canonical_skill_package(
    manifest: &SkillPackageManifest,
    contents: &[SkillPackageContent],
) -> Result<Vec<u8>, SkillPackageFrameError> {
    validate_manifest(manifest)?;
    if contents.len() != manifest.entries.len() {
        return Err(SkillPackageFrameError::InvalidFrame);
    }
    let frame_capacity = frame_length(manifest)?;
    let mut frame = Vec::with_capacity(frame_capacity);
    frame.extend_from_slice(SKILL_PACKAGE_FRAME_MAGIC);
    frame.extend_from_slice(
        &u32::try_from(contents.len())
            .map_err(|_| SkillPackageFrameError::TooLarge)?
            .to_be_bytes(),
    );
    for (entry, content) in manifest.entries.iter().zip(contents) {
        if entry.path != content.path
            || usize::try_from(entry.byte_length).ok() != Some(content.bytes.len())
            || digest_bytes(&content.bytes) != entry.content_digest
        {
            return Err(SkillPackageFrameError::Integrity);
        }
        let path = content.path.as_bytes();
        frame.extend_from_slice(
            &u16::try_from(path.len())
                .map_err(|_| SkillPackageFrameError::TooLarge)?
                .to_be_bytes(),
        );
        frame.extend_from_slice(&entry.byte_length.to_be_bytes());
        frame.extend_from_slice(path);
        frame.extend_from_slice(&content.bytes);
    }
    if frame.len() != frame_capacity {
        return Err(SkillPackageFrameError::Integrity);
    }
    Ok(frame)
}

pub fn materialize_skill_instruction(
    frame: &[u8],
    manifest: &SkillPackageManifest,
    section: &SkillInstructionSection,
) -> Result<MaterializedSkillInstruction, SkillPackageFrameError> {
    validate_manifest(manifest)?;
    if frame.len() != frame_length(manifest)? || !frame.starts_with(SKILL_PACKAGE_FRAME_MAGIC) {
        return Err(SkillPackageFrameError::InvalidFrame);
    }
    let mut cursor = SKILL_PACKAGE_FRAME_MAGIC.len();
    let entry_count = read_u32(frame, &mut cursor)?;
    if usize::try_from(entry_count).ok() != Some(manifest.entries.len()) {
        return Err(SkillPackageFrameError::InvalidFrame);
    }
    let mut instruction = None;
    for entry in &manifest.entries {
        let path_length = usize::from(read_u16(frame, &mut cursor)?);
        let content_length = read_u64(frame, &mut cursor)?;
        let path = take(frame, &mut cursor, path_length)?;
        let content_length_usize =
            usize::try_from(content_length).map_err(|_| SkillPackageFrameError::TooLarge)?;
        let content = take(frame, &mut cursor, content_length_usize)?;
        if path != entry.path.as_bytes()
            || content_length != entry.byte_length
            || digest_bytes(content) != entry.content_digest
        {
            return Err(SkillPackageFrameError::Integrity);
        }
        if entry.path == section.body.path {
            if entry.kind != SkillPackageEntryKind::Instruction
                || entry.content_digest != section.body.content_digest
                || entry.data_classification != section.data_classification
            {
                return Err(SkillPackageFrameError::Integrity);
            }
            let start = usize::try_from(section.body.byte_offset)
                .map_err(|_| SkillPackageFrameError::TooLarge)?;
            let length = usize::try_from(section.body.byte_length)
                .map_err(|_| SkillPackageFrameError::TooLarge)?;
            let end = start
                .checked_add(length)
                .ok_or(SkillPackageFrameError::TooLarge)?;
            let slice = content
                .get(start..end)
                .ok_or(SkillPackageFrameError::Integrity)?;
            let text = std::str::from_utf8(slice)
                .map_err(|_| SkillPackageFrameError::InvalidInstructionText)?;
            if text.contains('\0') {
                return Err(SkillPackageFrameError::InvalidInstructionText);
            }
            instruction = Some(text.to_owned());
        }
    }
    if cursor != frame.len() {
        return Err(SkillPackageFrameError::InvalidFrame);
    }
    let text = instruction.ok_or(SkillPackageFrameError::InstructionNotFound)?;
    Ok(MaterializedSkillInstruction {
        section_id: section.section_id.clone(),
        phase: section.phase,
        audience: section.audience,
        source_path: section.body.path.clone(),
        content_digest: section.body.content_digest.clone(),
        data_classification: section.data_classification,
        max_tokens: section.max_tokens,
        text,
    })
}

fn map_authority_error(
    error: ArtifactObjectReadAuthorityError,
) -> SchedulerSkillInstructionMaterializeError {
    match error {
        ArtifactObjectReadAuthorityError::Unavailable => {
            SchedulerSkillInstructionMaterializeError::Unavailable
        }
        ArtifactObjectReadAuthorityError::NotFound => {
            SchedulerSkillInstructionMaterializeError::NotFound
        }
        ArtifactObjectReadAuthorityError::Denied => {
            SchedulerSkillInstructionMaterializeError::Denied
        }
        ArtifactObjectReadAuthorityError::InvalidEvidence => {
            SchedulerSkillInstructionMaterializeError::Integrity
        }
    }
}

fn map_read_error(
    error: SchedulerSkillPackageReadError,
) -> SchedulerSkillInstructionMaterializeError {
    match error {
        SchedulerSkillPackageReadError::Unavailable => {
            SchedulerSkillInstructionMaterializeError::Unavailable
        }
        SchedulerSkillPackageReadError::Denied => SchedulerSkillInstructionMaterializeError::Denied,
        SchedulerSkillPackageReadError::NotFound => {
            SchedulerSkillInstructionMaterializeError::NotFound
        }
        SchedulerSkillPackageReadError::TooLarge => {
            SchedulerSkillInstructionMaterializeError::TooLarge
        }
        SchedulerSkillPackageReadError::Integrity => {
            SchedulerSkillInstructionMaterializeError::Integrity
        }
    }
}

fn map_frame_error(error: SkillPackageFrameError) -> SchedulerSkillInstructionMaterializeError {
    match error {
        SkillPackageFrameError::TooLarge => SchedulerSkillInstructionMaterializeError::TooLarge,
        SkillPackageFrameError::InstructionNotFound => {
            SchedulerSkillInstructionMaterializeError::NotFound
        }
        SkillPackageFrameError::InvalidManifest
        | SkillPackageFrameError::InvalidFrame
        | SkillPackageFrameError::Integrity
        | SkillPackageFrameError::InvalidInstructionText => {
            SchedulerSkillInstructionMaterializeError::Integrity
        }
    }
}

fn validate_manifest(manifest: &SkillPackageManifest) -> Result<(), SkillPackageFrameError> {
    if manifest.schema_version != 1
        || manifest.entries.is_empty()
        || !manifest
            .entries
            .windows(2)
            .all(|pair| pair[0].path < pair[1].path)
    {
        return Err(SkillPackageFrameError::InvalidManifest);
    }
    let expanded = manifest
        .entries
        .iter()
        .try_fold(0_u64, |total, entry| total.checked_add(entry.byte_length));
    if expanded != Some(manifest.total_byte_length)
        || manifest.total_byte_length == 0
        || manifest.total_byte_length > MAX_SKILL_PACKAGE_BYTES
    {
        return Err(SkillPackageFrameError::InvalidManifest);
    }
    let digest: Sha256Digest = canonical_digest(&serde_json::json!({
        "entries": manifest.entries,
        "schema_version": manifest.schema_version,
        "total_byte_length": manifest.total_byte_length,
    }))
    .map_err(|_| SkillPackageFrameError::InvalidManifest)?
    .parse()
    .map_err(|_| SkillPackageFrameError::InvalidManifest)?;
    if digest != manifest.canonical_digest {
        return Err(SkillPackageFrameError::InvalidManifest);
    }
    Ok(())
}

fn frame_length(manifest: &SkillPackageManifest) -> Result<usize, SkillPackageFrameError> {
    let mut total = SKILL_PACKAGE_FRAME_MAGIC.len() + size_of::<u32>();
    for entry in &manifest.entries {
        total = total
            .checked_add(size_of::<u16>() + size_of::<u64>())
            .and_then(|value| value.checked_add(entry.path.len()))
            .and_then(|value| {
                usize::try_from(entry.byte_length)
                    .ok()
                    .and_then(|length| value.checked_add(length))
            })
            .ok_or(SkillPackageFrameError::TooLarge)?;
    }
    Ok(total)
}

fn read_u16(frame: &[u8], cursor: &mut usize) -> Result<u16, SkillPackageFrameError> {
    let bytes: [u8; 2] = take(frame, cursor, 2)?
        .try_into()
        .map_err(|_| SkillPackageFrameError::InvalidFrame)?;
    Ok(u16::from_be_bytes(bytes))
}

fn read_u32(frame: &[u8], cursor: &mut usize) -> Result<u32, SkillPackageFrameError> {
    let bytes: [u8; 4] = take(frame, cursor, 4)?
        .try_into()
        .map_err(|_| SkillPackageFrameError::InvalidFrame)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(frame: &[u8], cursor: &mut usize) -> Result<u64, SkillPackageFrameError> {
    let bytes: [u8; 8] = take(frame, cursor, 8)?
        .try_into()
        .map_err(|_| SkillPackageFrameError::InvalidFrame)?;
    Ok(u64::from_be_bytes(bytes))
}

fn take<'a>(
    frame: &'a [u8],
    cursor: &mut usize,
    length: usize,
) -> Result<&'a [u8], SkillPackageFrameError> {
    let end = cursor
        .checked_add(length)
        .ok_or(SkillPackageFrameError::TooLarge)?;
    let bytes = frame
        .get(*cursor..end)
        .ok_or(SkillPackageFrameError::InvalidFrame)?;
    *cursor = end;
    Ok(bytes)
}

fn digest_bytes(bytes: &[u8]) -> Sha256Digest {
    let mut value = String::with_capacity(71);
    value.push_str("sha256:");
    for byte in Sha256::digest(bytes) {
        write!(&mut value, "{byte:02x}").unwrap();
    }
    value.parse().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use insight_platform_contracts::{
        canonical_digest, DataClassification, SkillArtifactSliceRef, SkillPackageEntry,
    };

    fn manifest(contents: &[SkillPackageContent]) -> SkillPackageManifest {
        let entries = contents
            .iter()
            .map(|content| SkillPackageEntry {
                path: content.path.clone(),
                kind: if content.path == "skill.json" {
                    SkillPackageEntryKind::Manifest
                } else {
                    SkillPackageEntryKind::Instruction
                },
                media_type: if content.path == "skill.json" {
                    "application/json".to_owned()
                } else {
                    "text/markdown".to_owned()
                },
                byte_length: u64::try_from(content.bytes.len()).unwrap(),
                content_digest: digest_bytes(&content.bytes),
                data_classification: DataClassification::Internal,
                executable: false,
            })
            .collect::<Vec<_>>();
        let total_byte_length = entries.iter().map(|entry| entry.byte_length).sum();
        let canonical_digest: Sha256Digest = canonical_digest(&serde_json::json!({
            "entries": entries,
            "schema_version": 1,
            "total_byte_length": total_byte_length,
        }))
        .unwrap()
        .parse()
        .unwrap();
        SkillPackageManifest {
            schema_version: 1,
            entries,
            total_byte_length,
            canonical_digest,
        }
    }

    fn section(manifest: &SkillPackageManifest) -> SkillInstructionSection {
        let entry = &manifest.entries[0];
        SkillInstructionSection {
            section_id: "review".to_owned(),
            phase: SkillInstructionPhase::Validation,
            audience: SkillInstructionAudience::Validator,
            body: SkillArtifactSliceRef {
                path: entry.path.clone(),
                content_digest: entry.content_digest.clone(),
                byte_offset: 7,
                byte_length: 7,
            },
            max_tokens: 8,
            data_classification: entry.data_classification,
        }
    }

    #[test]
    fn canonical_frame_materializes_exact_utf8_instruction_slice() {
        let contents = vec![
            SkillPackageContent {
                path: "instructions/review.md".to_owned(),
                bytes: b"Review bounded output".to_vec(),
            },
            SkillPackageContent {
                path: "skill.json".to_owned(),
                bytes: b"{}".to_vec(),
            },
        ];
        let manifest = manifest(&contents);
        let frame = encode_canonical_skill_package(&manifest, &contents).unwrap();
        let materialized =
            materialize_skill_instruction(&frame, &manifest, &section(&manifest)).unwrap();
        assert_eq!(materialized.text, "bounded");
        assert_eq!(materialized.source_path, "instructions/review.md");
    }

    #[test]
    fn canonical_frame_rejects_truncation_trailing_bytes_and_entry_drift() {
        let contents = vec![
            SkillPackageContent {
                path: "instructions/review.md".to_owned(),
                bytes: b"Review bounded output".to_vec(),
            },
            SkillPackageContent {
                path: "skill.json".to_owned(),
                bytes: b"{}".to_vec(),
            },
        ];
        let manifest = manifest(&contents);
        let frame = encode_canonical_skill_package(&manifest, &contents).unwrap();
        let instruction = section(&manifest);

        assert_eq!(
            materialize_skill_instruction(&frame[..frame.len() - 1], &manifest, &instruction),
            Err(SkillPackageFrameError::InvalidFrame)
        );
        let mut trailing = frame.clone();
        trailing.push(0);
        assert_eq!(
            materialize_skill_instruction(&trailing, &manifest, &instruction),
            Err(SkillPackageFrameError::InvalidFrame)
        );
        let mut drifted = frame;
        *drifted.last_mut().unwrap() ^= 1;
        assert_eq!(
            materialize_skill_instruction(&drifted, &manifest, &instruction),
            Err(SkillPackageFrameError::Integrity)
        );
    }
}
