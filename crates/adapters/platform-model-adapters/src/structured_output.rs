//! Adapter encoding of the already frozen structured-output contract. Text mode never injects
//! instructions: prompt construction and its accounting belong before ModelTurn admission.
use super::{permanent, rejected, ModelAdapterExecutionRequest, ModelAdapterFailure};
use insight_platform_contracts::{
    canonical_json, parse_strict_json, ClosedJsonValue, MODEL_JSON_LIMITS,
};
use insight_platform_models::CanonicalMessagePart;

pub(crate) fn use_native_structured_output(
    request: &ModelAdapterExecutionRequest,
) -> Result<bool, ModelAdapterFailure> {
    let response = &request.request.response_contract;
    let Some(schema) = &response.structured_schema else {
        return Ok(false);
    };
    let contract = &request.profile.structured_output;
    let encoded = canonical_json(&schema.schema)
        .map_err(|_| rejected("model_structured_output_schema_invalid"))?;
    if encoded.len() > contract.maximum_schema_bytes as usize {
        return Err(rejected("model_structured_output_schema_too_large"));
    }
    if contract.native {
        return Ok(true);
    }
    if !contract.textual_json_fallback
        || !request.request.tools.is_empty()
        || response.allow_tool_intents
        || response.allow_message_with_tool_intents
        || request
            .request
            .messages
            .iter()
            .flat_map(|message| &message.parts)
            .any(|part| !matches!(part, CanonicalMessagePart::Text(_)))
    {
        return Err(rejected("model_textual_json_profile_not_supported"));
    }
    Ok(false)
}

pub(crate) fn parse_structured_output(
    request: &ModelAdapterExecutionRequest,
    text: &str,
    safe_code: &'static str,
) -> Result<Option<ClosedJsonValue>, ModelAdapterFailure> {
    let Some(schema) = &request.request.response_contract.structured_schema else {
        return Ok(None);
    };
    let mut limits = MODEL_JSON_LIMITS;
    limits.max_bytes = limits
        .max_bytes
        .min(request.profile.structured_output.maximum_output_bytes as usize);
    let value = parse_strict_json(text.as_bytes(), limits).map_err(|_| permanent(safe_code))?;
    schema
        .validate_instance(&value)
        .map_err(|_| permanent(safe_code))?;
    ClosedJsonValue::build(schema.canonical_digest.clone(), value)
        .map(Some)
        .map_err(|_| permanent(safe_code))
}
