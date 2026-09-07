//! Verify deployment configurations against physical executable bytes and compiled owner catalogs.
use insight_platform_contracts::{
    canonical_digest, parse_strict_json, JsonLimits, Sha256Digest, WorkerExecutionCapabilities,
    WorkerExecutionCapability, WorkerManifest,
};
use insight_platform_deployment_contracts::workers::{
    WorkerExecutableEvidenceEntryV1, WorkerExecutableEvidenceV1, WORKER_EXECUTABLES,
};
use insight_platform_worker::execution::{executable_digest, validate_worker_catalog};
use serde_json::Value;
use std::{collections::BTreeSet, io::Read, path::Path};

fn read(path: &Path) -> Result<Value, String> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?
        .take(4_194_305)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    parse_strict_json(
        &bytes,
        JsonLimits {
            max_bytes: 4_194_304,
            max_depth: 32,
            max_properties_per_object: 128,
            max_items_per_array: 4096,
            max_string_bytes: 262_144,
        },
    )
    .map_err(|error| error.to_string())
}
fn field_digest(config: &Value, pointer: &str) -> Result<Sha256Digest, String> {
    config
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing closed deployment input {pointer}"))?
        .parse()
        .map_err(|_| format!("invalid deployment digest {pointer}"))
}
fn merge_adapters(
    adapters: impl IntoIterator<Item = WorkerExecutionCapability>,
) -> Result<WorkerExecutionCapabilities, String> {
    let mut catalog = insight_platform_plan::execution::program_execution_capabilities();
    for adapter in adapters {
        if !catalog.capabilities.contains(&adapter) {
            catalog.capabilities.push(adapter);
        }
    }
    catalog.validate().map_err(|error| error.to_string())?;
    Ok(catalog)
}
pub fn catalog(binary: &str, config: &Value) -> Result<WorkerExecutionCapabilities, String> {
    let single = |capability| WorkerExecutionCapabilities {
        schema_version: 1,
        capabilities: vec![capability],
    };
    Ok(match binary {
        "platform-orchestration-worker" => {
            insight_platform_plan::execution::program_execution_capabilities()
        }
        "platform-sandbox-dispatcher" => merge_adapters([
            insight_platform_sandbox::contracts::sandbox_cleanup_execution_capability(),
        ])?,
        "platform-remote-context-worker" => {
            let protocol = insight_platform_context::remote_context_protocol_contract_digest();
            let mapping = insight_platform_context::remote_context_result_mapping_digest();
            if field_digest(config, "/protocol_contract_digest")? != protocol
                || field_digest(config, "/result_mapping_digest")? != mapping
            {
                return Err("Remote Context config declares unsupported wire semantics".to_owned());
            }
            merge_adapters([
                insight_platform_contracts::remote_context_adapter_execution_capability(
                    &protocol, &mapping,
                )
                .map_err(|error| error.to_string())?,
            ])?
        }
        "platform-model-worker" => {
            let installed: Vec<insight_platform_contracts::InstalledModelAdapter> =
                serde_json::from_value(config["installed_adapters"].clone())
                    .map_err(|error| error.to_string())?;
            if installed.iter().any(|adapter| {
                !matches!(
                    adapter.qualified_name.as_str(),
                    "anthropic.messages/2023-06-01" | "openai.responses/v1"
                )
            }) {
                return Err(
                    "Model config names an adapter absent from the physical worker".to_owned(),
                );
            }
            merge_adapters(
                installed
                    .iter()
                    .map(insight_platform_contracts::model_adapter_execution_capability)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())?,
            )?
        }
        "platform-capability-native-worker" => {
            let installed = config["installed_adapters"]
                .as_array()
                .ok_or("missing native Capability adapters")?;
            for adapter in installed {
                if adapter["adapter_id"] != insight_platform_contracts::BUILTIN_ECHO_ADAPTER_ID
                    || adapter["adapter_version"]
                        != insight_platform_contracts::BUILTIN_ECHO_ADAPTER_VERSION
                    || adapter["entrypoint_id"]
                        != insight_platform_contracts::BUILTIN_ECHO_ENTRYPOINT_ID
                    || field_digest(adapter, "/module_digest")?
                        != insight_platform_contracts::builtin_echo_module_digest()
                {
                    return Err("native Capability module absent from physical worker".to_owned());
                }
            }

            merge_adapters(
                installed
                    .iter()
                    .map(|adapter| {
                        insight_platform_contracts::native_capability_adapter_execution_capability(
                            &field_digest(adapter, "/module_digest")?,
                        )
                        .map_err(|error| error.to_string())
                    })
                    .collect::<Result<Vec<_>, String>>()?,
            )?
        }
        "platform-context-worker" => merge_adapters([
            insight_platform_contracts::native_context_adapter_execution_capability(
                &field_digest(config, "/native_catalog/adapter_contract_digest")?,
                &field_digest(config, "/native_catalog/installed_adapter_digest")?,
            )
            .map_err(|error| error.to_string())?,
        ])?,
        "platform-capability-remote-worker" => {
            let mut adapters = Vec::new();
            for kind in ["http", "grpc", "mcp"] {
                let key = format!("installed_{kind}_codecs");
                for codec in config[&key]
                    .as_array()
                    .ok_or("missing remote Capability codec set")?
                {
                    let fixed = builtin_worker_protocols();
                    for (field, expected) in fixed["codecs"][kind]
                        .as_object()
                        .ok_or("invalid compiled codec identity")?
                    {
                        if codec.get(field) != Some(expected) {
                            return Err(format!("unsupported compiled {kind} codec {field}"));
                        }
                    }

                    let descriptor: insight_platform_contracts::InstalledCapabilityCodecRef = serde_json::from_value(serde_json::json!({
                        "schema_version": 1, "backend_kind": kind, "codec_id": codec["codec_id"], "codec_version": codec["codec_version"],
                        "module_digest": codec["module_digest"], "worker_protocol_version": codec["worker_protocol_version"], "descriptor_digest": codec["descriptor_digest"],
                    })).map_err(|error| error.to_string())?;
                    adapters.push(
                        insight_platform_contracts::remote_capability_adapter_execution_capability(
                            &descriptor,
                        )
                        .map_err(|error| error.to_string())?,
                    );
                }
            }
            merge_adapters(adapters)?
        }
        "platform-registry-validation-worker" => WorkerExecutionCapabilities {
            schema_version: 1,
            capabilities: vec![
                WorkerExecutionCapability::AgentCompilation {
                    compiler_semantic_identity:
                        insight_platform_agent_compiler::compiler_semantic_identity(),
                },
                insight_platform_registry::registry_resource_validation_execution_capability(),
            ],
        },
        "platform-subscription-context-worker" => {
            single(insight_platform_context::execution::subscription_execution_capability())
        }
        "platform-context-dataset-worker" => {
            let sources = config
                .get("sources")
                .and_then(Value::as_array)
                .filter(|sources| !sources.is_empty() && sources.len() <= 64)
                .ok_or("missing bounded Context Dataset sources")?;
            let contracts = sources
                .iter()
                .map(|source| field_digest(source, "/binding/adapter_contract_digest"))
                .collect::<Result<BTreeSet<_>, _>>()?;
            WorkerExecutionCapabilities {
                schema_version: 1,
                capabilities: contracts
                    .iter()
                    .map(insight_platform_context::execution::dataset_execution_capability)
                    .collect(),
            }
        }
        "platform-artifact-data-worker" => {
            let scanner =
                insight_platform_artifacts::execution::integrity_scanner_contract_digest();
            if field_digest(config, "/scan_worker/scanner_contract_digest")? != scanner {
                return Err("Artifact scanner is absent from the physical worker".to_owned());
            }
            insight_platform_artifacts::execution::data_worker_execution_capabilities(&scanner)
        }
        "platform-artifact-maintenance" => {
            insight_platform_artifacts::execution::maintenance_execution_capabilities()
        }
        "platform-mcp-discovery-worker" => {
            insight_platform_mcp_host::execution::discovery_execution_capabilities()
        }
        "platform-mcp-cleanup-worker" => {
            single(insight_platform_mcp_host::mcp_oauth_cleanup_execution_capability())
        }
        "platform-mcp-subscription-worker" => {
            insight_platform_mcp_host::execution::subscription_execution_capabilities()
        }
        _ => return Err("unknown physical worker executable".to_owned()),
    })
}
fn closed_directory(directory: &Path) -> Result<(), String> {
    let expected = WORKER_EXECUTABLES
        .iter()
        .map(|worker| format!("{}.json", worker.binary))
        .collect::<BTreeSet<_>>();
    let observed = std::fs::read_dir(directory)
        .map_err(|error| error.to_string())?
        .map(|entry| {
            let entry = entry.map_err(|error| error.to_string())?;
            if !entry
                .file_type()
                .map_err(|error| error.to_string())?
                .is_file()
            {
                return Err("worker closure entries must be physical JSON files".to_owned());
            }
            entry
                .file_name()
                .into_string()
                .map_err(|_| "worker closure filename must be UTF8".to_owned())
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if observed != expected {
        return Err(
            "worker closure directory contains missing or extra executable records".to_owned(),
        );
    }
    Ok(())
}
pub fn validate(
    manifests: &Path,
    configurations: &Path,
    binaries: &Path,
    image: Sha256Digest,
) -> Result<WorkerExecutableEvidenceV1, String> {
    closed_directory(manifests)?;
    closed_directory(configurations)?;
    let mut entries = Vec::new();
    for worker in WORKER_EXECUTABLES {
        let manifest_json = read(&manifests.join(format!("{}.json", worker.binary)))?;
        let manifest: WorkerManifest =
            serde_json::from_value(manifest_json.clone()).map_err(|error| error.to_string())?;
        let config = read(&configurations.join(format!("{}.json", worker.binary)))?;
        if config.pointer(worker.manifest_pointer) != Some(&manifest_json) {
            return Err(format!(
                "{} process config differs from its exact deployment manifest",
                worker.binary
            ));
        }
        validate_worker_catalog(
            &manifest,
            &catalog(worker.binary, &config)?,
            worker.worker_role,
            worker.work_class,
        )
        .map_err(|error| format!("{}: {error}", worker.binary))?;
        let actual =
            executable_digest(&binaries.join(worker.binary)).map_err(|error| error.to_string())?;
        if actual != manifest.worker_build_digest {
            return Err(format!(
                "{} executable bytes differ from deployment manifest",
                worker.binary
            ));
        }
        entries.push(WorkerExecutableEvidenceEntryV1 {
            binary: worker.binary.to_owned(),
            worker_manifest_digest: manifest
                .canonical_digest()
                .map_err(|error| error.to_string())?,
            worker_build_digest: actual,
            process_config_digest: canonical_digest(&config)
                .map_err(|error| error.to_string())?
                .parse()
                .map_err(|_| "invalid process digest")?,
        });
    }
    let evidence = WorkerExecutableEvidenceV1 {
        schema_version: 1,
        runtime_image_digest: image,
        workers: entries,
    };
    evidence.validate().map_err(str::to_owned)?;
    Ok(evidence)
}

/// Deployment assembly reads these identities from the same owning protocol functions as startup.
pub fn builtin_worker_protocols() -> Value {
    use insight_platform_contracts::*;
    let common = serde_json::json!({"codec_id": BUILTIN_JSON_CODEC_ID, "codec_version": BUILTIN_JSON_CODEC_VERSION,
        "module_digest": builtin_json_codec_module_digest(), "worker_protocol_version": WORKER_PROTOCOL_VERSION});
    let mut http = common.clone();
    let mut grpc = common.clone();
    let mut mcp = common;
    for (field, digest) in [
        (
            "protocol_contract_digest",
            builtin_json_http_protocol_contract_digest(),
        ),
        (
            "request_mapping_digest",
            builtin_json_http_request_mapping_digest(),
        ),
        (
            "response_mapping_digest",
            builtin_json_http_response_mapping_digest(),
        ),
        (
            "error_mapping_digest",
            builtin_json_http_error_mapping_digest(),
        ),
    ] {
        http[field] = serde_json::json!(digest);
    }
    for (field, digest) in [
        (
            "protobuf_contract_digest",
            builtin_json_grpc_protobuf_contract_digest(),
        ),
        (
            "request_mapping_digest",
            builtin_json_grpc_request_mapping_digest(),
        ),
        (
            "response_mapping_digest",
            builtin_json_grpc_response_mapping_digest(),
        ),
        (
            "error_mapping_digest",
            builtin_json_grpc_error_mapping_digest(),
        ),
    ] {
        grpc[field] = serde_json::json!(digest);
    }
    mcp["output_mapping_digest"] = serde_json::json!(builtin_json_mcp_output_mapping_digest());
    serde_json::json!({"integrity_scanner_contract_digest":insight_platform_artifacts::execution::integrity_scanner_contract_digest(),"native_capability":{"adapter_id":BUILTIN_ECHO_ADAPTER_ID,"adapter_version":BUILTIN_ECHO_ADAPTER_VERSION,"entrypoint_id":BUILTIN_ECHO_ENTRYPOINT_ID,"module_digest":builtin_echo_module_digest()}, "remote_context":{"protocol_contract_digest":insight_platform_context::remote_context_protocol_contract_digest(), "result_mapping_digest":insight_platform_context::remote_context_result_mapping_digest()}, "codecs":{"http":http,"grpc":grpc,"mcp":mcp}})
}
