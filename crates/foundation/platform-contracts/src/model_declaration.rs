//! Exact source bytes for an operator-declared model/provider authoring Artifact.
//! This binds a declaration to its owning Resource semantics without asserting protocol success.
use crate::{
    canonical_digest, ModelContractError, ModelEvidenceBasis, ModelProfileResourceSpec,
    ModelProviderResourceSpec, Sha256Digest,
};
use serde_json::{json, Value};

fn definition<T: serde::Serialize>(spec: &T) -> Result<Value, ModelContractError> {
    let mut value = serde_json::to_value(spec).map_err(|_| ModelContractError::InvalidJson)?;
    let object = value
        .as_object_mut()
        .ok_or(ModelContractError::InvalidJson)?;
    object.remove("authoring_package");
    object.remove("contract_digest");
    if let Some(evidence) = object
        .get_mut("catalog_evidence")
        .and_then(Value::as_object_mut)
    {
        evidence.remove("artifact");
        evidence.remove("source_digest");
    }
    Ok(value)
}

pub fn model_provider_declaration(
    spec: &ModelProviderResourceSpec,
) -> Result<Value, ModelContractError> {
    Ok(
        json!({"schema_version": 1, "kind": "insight.model-provider-declaration/v1", "definition": definition(spec)?}),
    )
}

pub fn model_profile_declaration(
    spec: &ModelProfileResourceSpec,
) -> Result<Value, ModelContractError> {
    Ok(
        json!({"schema_version": 1, "kind": "insight.model-profile-declaration/v1", "definition": definition(spec)?}),
    )
}

fn value_digest(value: &Value) -> Result<Sha256Digest, ModelContractError> {
    canonical_digest(value)
        .map_err(|_| ModelContractError::InvalidJson)?
        .parse()
        .map_err(|_| ModelContractError::InvalidJson)
}

pub fn validate_model_provider_declaration(
    spec: &ModelProviderResourceSpec,
) -> Result<(), ModelContractError> {
    let expected = value_digest(&model_provider_declaration(spec)?)?;
    if spec.contract_digest != expected
        || spec.authoring_package.manifest_digest != expected
        || spec.authoring_package.artifact.content_digest() != &expected
    {
        return Err(ModelContractError::InvalidEvidence);
    }
    Ok(())
}

pub fn validate_model_profile_declaration(
    spec: &ModelProfileResourceSpec,
) -> Result<(), ModelContractError> {
    if spec.catalog_evidence.basis != ModelEvidenceBasis::OperatorDeclaration {
        return Ok(());
    }
    let expected = value_digest(&model_profile_declaration(spec)?)?;
    if spec.catalog_evidence.source_digest != expected
        || spec.contract_digest != expected
        || spec.authoring_package.manifest_digest != expected
        || spec.authoring_package.artifact.content_digest() != &expected
        || spec.catalog_evidence.artifact != spec.authoring_package.artifact
    {
        return Err(ModelContractError::InvalidEvidence);
    }
    Ok(())
}
