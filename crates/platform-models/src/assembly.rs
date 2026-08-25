use crate::{
    CanonicalMessage, CanonicalMessagePart, CanonicalMessageRole, ModelContentSource,
    PromptAssemblyPhase,
};
use insight_platform_contracts::{canonical_digest, DataClassification, Sha256Digest};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptAssemblyBlock {
    pub phase: PromptAssemblyPhase,
    pub ordinal: u32,
    pub source_kind: String,
    pub source_id: String,
    pub source_digest: Sha256Digest,
    pub classification: DataClassification,
    pub byte_budget: u32,
    pub token_budget: u32,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromptSourceMapEntry {
    pub phase: PromptAssemblyPhase,
    pub ordinal: u32,
    pub source_kind: String,
    pub source_id: String,
    pub source_digest: Sha256Digest,
    pub content_digest: Sha256Digest,
    pub classification: DataClassification,
    pub byte_budget: u32,
    pub token_budget: u32,
    pub included_bytes: u32,
    pub estimated_tokens: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AssembledPrompt {
    pub messages: Vec<CanonicalMessage>,
    pub source_map: Vec<PromptSourceMapEntry>,
    pub source_map_digest: Sha256Digest,
    pub classification: DataClassification,
    pub total_bytes: u32,
    pub total_estimated_tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PromptAssemblyError {
    Empty,
    InvalidSource,
    InvalidPhase,
    NonCanonicalOrdinal,
    BlockBudgetExceeded,
    TotalBudgetExceeded,
    Canonicalization,
}

pub fn assemble_prompt_messages(
    mut blocks: Vec<PromptAssemblyBlock>,
    maximum_bytes: u32,
    maximum_tokens: u64,
) -> Result<AssembledPrompt, PromptAssemblyError> {
    if blocks.is_empty() || maximum_bytes == 0 || maximum_tokens == 0 {
        return Err(PromptAssemblyError::Empty);
    }
    blocks.sort_by(|left, right| {
        (left.phase, left.ordinal, left.source_id.as_str()).cmp(&(
            right.phase,
            right.ordinal,
            right.source_id.as_str(),
        ))
    });
    let mut positions = BTreeSet::new();
    let mut phases = BTreeSet::new();
    let mut messages = Vec::with_capacity(blocks.len());
    let mut source_map = Vec::with_capacity(blocks.len());
    let mut total_bytes = 0_u32;
    let mut total_tokens = 0_u64;
    let mut classification = DataClassification::Public;

    for block in blocks {
        if block.phase == PromptAssemblyPhase::CapabilityToolResult {
            return Err(PromptAssemblyError::InvalidPhase);
        }
        phases.insert(block.phase);
        if !positions.insert((block.phase, block.ordinal)) {
            return Err(PromptAssemblyError::NonCanonicalOrdinal);
        }
        if block.source_kind.is_empty()
            || block.source_id.is_empty()
            || block.text.is_empty()
            || block.text.contains('\0')
            || block.byte_budget == 0
            || block.token_budget == 0
        {
            return Err(PromptAssemblyError::InvalidSource);
        }
        let included_bytes = u32::try_from(block.text.len())
            .map_err(|_| PromptAssemblyError::BlockBudgetExceeded)?;
        let estimated_tokens = included_bytes
            .checked_add(3)
            .ok_or(PromptAssemblyError::BlockBudgetExceeded)?
            / 4;
        let estimated_tokens = estimated_tokens.max(1);
        if included_bytes > block.byte_budget || estimated_tokens > block.token_budget {
            return Err(PromptAssemblyError::BlockBudgetExceeded);
        }
        total_bytes = total_bytes
            .checked_add(included_bytes)
            .ok_or(PromptAssemblyError::TotalBudgetExceeded)?;
        total_tokens = total_tokens
            .checked_add(u64::from(estimated_tokens))
            .ok_or(PromptAssemblyError::TotalBudgetExceeded)?;
        if total_bytes > maximum_bytes || total_tokens > maximum_tokens {
            return Err(PromptAssemblyError::TotalBudgetExceeded);
        }
        if block.classification.rank() > classification.rank() {
            classification = block.classification;
        }
        let content_digest = digest_bytes(block.text.as_bytes())?;
        let trusted_instruction = matches!(
            block.phase,
            PromptAssemblyPhase::PlatformSafety
                | PromptAssemblyPhase::AgentContract
                | PromptAssemblyPhase::PlanNodeInstruction
        );
        let role = if trusted_instruction {
            CanonicalMessageRole::Platform
        } else {
            CanonicalMessageRole::User
        };
        let source = ModelContentSource {
            source_kind: block.source_kind.clone(),
            source_id: block.source_id.clone(),
            source_digest: block.source_digest.clone(),
            content_digest: content_digest.clone(),
            assembly_phase: block.phase,
            ordinal: block.ordinal,
            byte_budget: block.byte_budget,
            token_budget: block.token_budget,
            trusted_instruction,
        };
        messages.push(CanonicalMessage {
            role,
            parts: vec![CanonicalMessagePart::Text(block.text)],
            classification: block.classification,
            source,
        });
        source_map.push(PromptSourceMapEntry {
            phase: block.phase,
            ordinal: block.ordinal,
            source_kind: block.source_kind,
            source_id: block.source_id,
            source_digest: block.source_digest,
            content_digest,
            classification: block.classification,
            byte_budget: block.byte_budget,
            token_budget: block.token_budget,
            included_bytes,
            estimated_tokens,
        });
    }
    for required in [
        PromptAssemblyPhase::PlatformSafety,
        PromptAssemblyPhase::AgentContract,
        PromptAssemblyPhase::PlanNodeInstruction,
        PromptAssemblyPhase::UserInput,
    ] {
        if !phases.contains(&required) {
            return Err(PromptAssemblyError::InvalidPhase);
        }
    }
    let source_map_digest: Sha256Digest = canonical_digest(
        &serde_json::to_value(&source_map).map_err(|_| PromptAssemblyError::Canonicalization)?,
    )
    .map_err(|_| PromptAssemblyError::Canonicalization)?
    .parse()
    .map_err(|_| PromptAssemblyError::Canonicalization)?;
    Ok(AssembledPrompt {
        messages,
        source_map,
        source_map_digest,
        classification,
        total_bytes,
        total_estimated_tokens: total_tokens,
    })
}

fn digest_bytes(bytes: &[u8]) -> Result<Sha256Digest, PromptAssemblyError> {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(71);
    encoded.push_str("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").map_err(|_| PromptAssemblyError::Canonicalization)?;
    }
    encoded
        .parse()
        .map_err(|_| PromptAssemblyError::Canonicalization)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn digest(character: char) -> Sha256Digest {
        format!("sha256:{}", character.to_string().repeat(64))
            .parse()
            .unwrap()
    }

    fn block(
        phase: PromptAssemblyPhase,
        ordinal: u32,
        id: &str,
        text: &str,
    ) -> PromptAssemblyBlock {
        PromptAssemblyBlock {
            phase,
            ordinal,
            source_kind: "fixture".to_owned(),
            source_id: id.to_owned(),
            source_digest: digest('a'),
            classification: DataClassification::Internal,
            byte_budget: 1_024,
            token_budget: 256,
            text: text.to_owned(),
        }
    }

    #[test]
    fn assembly_is_phase_and_ordinal_deterministic() {
        let input = vec![
            block(PromptAssemblyPhase::UserInput, 0, "input", "question"),
            block(PromptAssemblyPhase::RequiredSkill, 1, "skill-b", "method b"),
            block(PromptAssemblyPhase::PlatformSafety, 0, "safety", "be safe"),
            block(PromptAssemblyPhase::RequiredSkill, 0, "skill-a", "method a"),
            block(
                PromptAssemblyPhase::AgentContract,
                0,
                "agent",
                "agent contract",
            ),
            block(
                PromptAssemblyPhase::PlanNodeInstruction,
                0,
                "node",
                "do work",
            ),
        ];
        let first = assemble_prompt_messages(input.clone(), 8_192, 2_048).unwrap();
        let mut reversed = input;
        reversed.reverse();
        let second = assemble_prompt_messages(reversed, 8_192, 2_048).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.messages[0].role, CanonicalMessageRole::Platform);
        assert_eq!(first.messages[3].role, CanonicalMessageRole::User);
        assert!(!first.messages[3].source.trusted_instruction);
    }

    #[test]
    fn assembly_rejects_ordinal_collisions_and_budget_overflow() {
        let collision = vec![
            block(PromptAssemblyPhase::RequiredSkill, 0, "a", "a"),
            block(PromptAssemblyPhase::RequiredSkill, 0, "b", "b"),
        ];
        assert_eq!(
            assemble_prompt_messages(collision, 8_192, 2_048),
            Err(PromptAssemblyError::NonCanonicalOrdinal)
        );
        let mut oversized = block(PromptAssemblyPhase::UserInput, 0, "input", "12345");
        oversized.byte_budget = 4;
        assert_eq!(
            assemble_prompt_messages(vec![oversized], 8_192, 2_048),
            Err(PromptAssemblyError::BlockBudgetExceeded)
        );
    }
}
