//! Machine representation of the closed public model setup compiler boundary. Deployment field
//! schemas are derived from the existing deployment producer, omitting only the unallocated self ID.
use serde_json::{json, Value};
fn object(properties: Value) -> Value {
    json!({"type":"object","additionalProperties":false,"required":properties.as_object().unwrap().keys().collect::<Vec<_>>(),"properties":properties})
}
fn reference(name: &str) -> Value {
    json!({"$ref":format!("#/$defs/{name}")})
}
fn digest() -> Value {
    json!({"$ref":"nominal/digest.schema.json"})
}
fn id(prefix: &str) -> Value {
    json!({"type":"string","pattern":format!("^{prefix}_[0-9a-f]{{8}}-[0-9a-f]{{4}}-7[0-9a-f]{{3}}-[89ab][0-9a-f]{{3}}-[0-9a-f]{{12}}$")})
}
fn nullable(value: Value) -> Value {
    json!({"oneOf":[value,{"type":"null"}]})
}
fn exact(prefix: &str, kind: &str) -> Value {
    object(
        json!({"deployment_id":id(prefix),"resource_kind":{"const":kind},"deployment_digest":digest()}),
    )
}

pub(super) fn schema() -> Value {
    let closure = super::deployment_closure_schema();
    let alias = json!({"type":"string","pattern":"^[a-z][a-z0-9._-]{0,63}$","maxLength":64});
    let name =
        json!({"type":"string","minLength":1,"maxLength":255,"x-insight-max-utf8-bytes":255});
    let environment = json!({"type":"string","pattern":"^[a-z0-9_-]{1,64}$","maxLength":64});
    let mut provider = closure["$defs"]["ModelProviderDeploymentClosure"].clone();
    let mut model = closure["$defs"]["ModelDeploymentClosure"].clone();
    for (schema, field) in [
        (&mut provider, "provider_revision"),
        (&mut model, "profile_revision"),
    ] {
        let bindings = &mut schema["properties"]["bindings"];
        bindings["properties"]
            .as_object_mut()
            .unwrap()
            .remove(field);
        bindings["required"]
            .as_array_mut()
            .unwrap()
            .retain(|item| item.as_str() != Some(field));
    }
    let source = object(
        json!({"schema_version":{"const":1},"alias":alias,"display_name":name,
        "destination_digest":digest(),"credential":reference("ExactSecretBindingRef")}),
    );
    let basic = object(
        json!({"schema_version":{"const":1},"alias":alias,"display_name":name,"source":exact("mpdep","model_provider_deployment"),
        "model":{"type":"string","minLength":1,"maxLength":255,"x-insight-max-utf8-bytes":insight_platform_contracts::MAX_PROVIDER_MODEL_IDENTITY_BYTES},"maximum_input_tokens":{"type":"integer","minimum":1,"maximum":8192},
        "maximum_output_tokens":{"type":"integer","minimum":1,"maximum":2048},"declared_at":{"type":"string","format":"date-time"}}),
    );
    let input = json!({"oneOf":[object(json!({"kind":{"const":"source"},"configuration":source})),object(json!({"kind":{"const":"model"},"configuration":basic}))]});
    let declaration = object(
        json!({"schema_version":{"const":1},"content":{},"content_digest":digest(),"size_bytes":{"type":"integer","minimum":1,"maximum":65536}}),
    );
    let declare = object(
        json!({"schema_version":{"const":1},"installation_digest":digest(),"input":reference("ModelConfigurationInputV1")}),
    );
    let compile = object(
        json!({"schema_version":{"const":1},"installation_digest":digest(),"input":reference("ModelConfigurationInputV1"),"artifact":{"$ref":"nominal/artifact-ref.schema.json"}}),
    );
    let mut summary = object(
        json!({"resource_id":{"$ref":"resource-id.schema.json"},"resource_kind":{"enum":["model_provider","model_profile"]},
        "alias":nullable(alias),"display_name":name,"version":{"type":"integer","minimum":1,"maximum":i64::MAX},"etag":{"type":"string","maxLength":128},
        "lifecycle_state":{"enum":["active","archived","deleted"]},"gate_state":{"enum":["enabled","suspended"]},
        "active_deployment":nullable(json!({"oneOf":[exact("mpdep","model_provider_deployment"),exact("mdep","model_deployment")]}))}),
    );
    // Closed states come from their owning enum registry, not a second spelling list.
    summary["properties"]["lifecycle_state"] = json!({"enum":insight_platform_contracts::EntityLifecycle::ALL.iter().map(|state|state.as_str()).collect::<Vec<_>>()});
    summary["properties"]["gate_state"] = json!({"enum":insight_platform_contracts::AdministrativeGate::ALL.iter().map(|state|state.as_str()).collect::<Vec<_>>()});
    let destination = object(
        json!({"destination_digest":digest(),"endpoint_identity_digest":digest(),"base_url":{"type":"string","minLength":1,"maxLength":8448},
        "protocol":{"enum":["open_ai_responses","anthropic_messages"]},"region":{"type":"string","minLength":1,"maxLength":32}}),
    );
    let quota_value = json!({"type":"integer","minimum":0,"maximum":insight_platform_contracts::MAX_MODEL_QUOTA_VALUE});
    let quota_limits =
        object(json!({"requests":quota_value,"tokens":quota_value,"cost_microunits":quota_value}));
    let quota_counter =
        object(json!({"limit":quota_value,"reserved":quota_value,"used":quota_value}));
    let quota_etag = json!({"type":"string","pattern":"^\"model-quota-[0-9a-f]{64}\"$"});
    json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$id":"urn:insight:platform:v1:model-configuration",
    "$defs":{
        "ModelQuotaDeploymentId":id("mdep"),
        "ModelQuotaLimitsV1":quota_limits,
        "ModelQuotaViewV1":object(json!({"schema_version":{"const":1},"tenant_id":id("ten"),"model_deployment":exact("mdep","model_deployment"),"etag":quota_etag,
            "allocation":nullable(object(json!({"limits":reference("ModelQuotaLimitsV1"),"reserved":reference("ModelQuotaLimitsV1"),"used":reference("ModelQuotaLimitsV1")}))),"tenant_concurrency":quota_counter})),
        "SetModelQuotaRequestV1":object(json!({"schema_version":{"const":1},"model_deployment":exact("mdep","model_deployment"),"limits":reference("ModelQuotaLimitsV1")})),
        "ExactVersionRef":closure["$defs"]["ExactVersionRef"],"ExactDeploymentRef":closure["$defs"]["ExactDeploymentRef"],"ExactSecretBindingRef":closure["$defs"]["ExactSecretBindingRef"],
        "ModelConfigurationInputV1":input,"DeclareModelConfigurationRequestV1":declare,"CompileModelConfigurationRequestV1":compile,
        "ModelConfigurationDeclarationV1":declaration,
        "CompiledModelConfigurationV1":object(json!({"schema_version":{"const":1},"draft":{"$ref":"../openapi.yaml#/components/schemas/ResourceDraftPayload"},"environment":environment,
            "declaration":reference("ModelConfigurationDeclarationV1"),"deployment":{"oneOf":[provider,model]}})),
        "ModelConfigurationCatalogViewV1":object(json!({"schema_version":{"const":1},"installation_digest":digest(),"environment":environment,"secret_provider_id":id("spr"),
            "destinations":{"type":"array","minItems":1,"maxItems":64,"items":destination},"maximum_classification":{"const":"internal"}})),
        "ModelConfigurationResourceSummaryV1":summary,
        "ModelConfigurationResourcePageV1":object(json!({"schema_version":{"const":1},"items":{"type":"array","maxItems":25,"items":reference("ModelConfigurationResourceSummaryV1")},"next_after":nullable(json!({"$ref":"resource-id.schema.json"}))}))
    }})
}
