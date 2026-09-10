use crate::{jetstream_setup, model_setup, openbao_setup, s3_setup, storage_setup};
use insight_platform_contracts::{canonical_digest, Sha256Digest};
use insight_platform_deployment_contracts::{
    installation::*,
    installation_provider::{InstallationProviderStateV1, OpenBaoInstallationRole},
};
use insight_platform_deployment_tooling::{
    installation::PreparedInstallation,
    model_profile, openbao_profile, provider_config,
    renderer::{self, InstallationRenderInputs},
    role_output::{self, RoleOutputMode, RoleOutputOwnership},
    worker_profile::WorkerBuilds,
};
use std::path::Path;

fn invalid() -> InstallationError {
    InstallationError::ConfigurationDrift
}
fn canonical(value: &impl serde::Serialize) -> Result<Sha256Digest, InstallationError> {
    canonical_digest(&serde_json::to_value(value).map_err(|_| invalid())?)
        .map_err(|_| invalid())?
        .parse()
        .map_err(|_| invalid())
}
fn ownership(input: &InstallationInputV1) -> RoleOutputOwnership {
    if input.network.topology == InstallationTopology::Native {
        RoleOutputOwnership::NativeCurrentUser
    } else {
        RoleOutputOwnership::ServingUsers
    }
}

pub fn prepare(
    input: &InstallationInputV1,
    state: &Path,
    output: &Path,
) -> Result<PreparedInstallation, InstallationError> {
    let prepared = PreparedInstallation::prepare(input, state)?;
    role_output::publish_dependency_outputs(
        input,
        prepared.identity(),
        prepared.directory(),
        &output.join("dependencies"),
        RoleOutputMode::Provision,
        ownership(input),
    )?;
    role_output::initialize_nats_data_directory(
        input,
        prepared.identity(),
        prepared.directory(),
        &output.join("nats-data"),
        RoleOutputMode::Provision,
        ownership(input),
    )?;
    provider_data(&prepared, output, RoleOutputMode::Provision)?;
    Ok(prepared)
}

pub async fn configure(
    input: &InstallationInputV1,
    state: &Path,
    output: &Path,
    binaries: &Path,
    verify: bool,
) -> Result<PreparedInstallation, InstallationError> {
    let mut prepared = PreparedInstallation::open(input, state)?;
    if verify && prepared.progress().phase != InstallationPhase::Ready {
        return Err(InstallationError::Incomplete);
    }
    let verify = verify || prepared.progress().phase == InstallationPhase::Ready;
    let s3_mode = if verify {
        s3_setup::Mode::Verify
    } else {
        s3_setup::Mode::Provision
    };
    let storage_mode = if verify {
        storage_setup::Mode::Verify
    } else {
        storage_setup::Mode::Provision
    };
    let model_mode = if verify {
        model_setup::Mode::Verify
    } else {
        model_setup::Mode::Provision
    };
    role_output::publish_dependency_outputs(
        input,
        prepared.identity(),
        prepared.directory(),
        &output.join("dependencies"),
        RoleOutputMode::Verify,
        ownership(input),
    )?;
    role_output::initialize_nats_data_directory(
        input,
        prepared.identity(),
        prepared.directory(),
        &output.join("nats-data"),
        RoleOutputMode::Verify,
        ownership(input),
    )?;
    provider_data(&prepared, output, RoleOutputMode::Verify)?;
    if !matches!(
        prepared.provider_state()?.state,
        InstallationProviderStateV1::ProviderReady { .. }
    ) {
        return Err(InstallationError::Incomplete);
    }
    let provider_ready = openbao_setup::observe(&prepared, state).await?;
    prepared.complete_provider(provider_ready.clone())?;
    let bucket =
        s3_setup::ensure_s3(input, prepared.identity(), prepared.directory(), s3_mode).await?;
    let staging_client = openbao_profile::role_client(
        &provider_ready,
        OpenBaoInstallationRole::ArtifactGateway,
        state,
    )?;
    let artifact_catalog = provider_config::openbao_artifact_provider_catalog(
        input.network.providers.artifact(),
        &bucket,
        &staging_client,
        &provider_ready.artifact_key,
    )?;
    let storage = artifact_catalog
        .get("write_storage_binding_digest")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(invalid)?
        .parse()
        .map_err(|_| invalid())?;
    let (artifact_authority, artifact_digest) =
        prepared.prepare_artifact_authority(storage, !verify)?;
    jetstream_setup::ensure_stream(
        input,
        prepared.identity(),
        prepared.directory(),
        binaries,
        if verify {
            jetstream_setup::Mode::Verify
        } else {
            jetstream_setup::Mode::Provision
        },
    )
    .await?;
    if prepared.progress().phase == InstallationPhase::Prepared {
        prepared.complete_phase(InstallationPhase::DependenciesVerified)?;
    }
    for (stage, previous, next) in [
        (
            storage_setup::StorageStage::Schema,
            InstallationPhase::DependenciesVerified,
            InstallationPhase::SchemaVerified,
        ),
        (
            storage_setup::StorageStage::DatabaseRoles,
            InstallationPhase::SchemaVerified,
            InstallationPhase::RolesProvisioned,
        ),
        (
            storage_setup::StorageStage::Bootstrap,
            InstallationPhase::RolesProvisioned,
            InstallationPhase::AuthorityBootstrapped,
        ),
    ] {
        storage_setup::run_storage_stage(
            &storage_setup::StorageInputs {
                input,
                identity: prepared.identity(),
                directory: prepared.directory(),
                binary_directory: binaries,
                artifact_bootstrap: Some(storage_setup::ArtifactBootstrapFile {
                    name: "artifact-bootstrap.json",
                    digest: &artifact_digest,
                }),
            },
            stage,
            storage_mode,
        )
        .await?;
        if prepared.progress().phase == previous {
            prepared.complete_phase(next)?;
        }
    }
    let selected = input
        .network
        .processes
        .iter()
        .map(|process| process.process)
        .collect::<Vec<_>>();
    let builds = WorkerBuilds::read_processes(binaries, &selected)
        .map_err(|_| InstallationError::PrerequisiteUnavailable)?;
    let model_catalog = if input.model_destinations.is_empty() {
        None
    } else {
        let storage_inputs = storage_setup::StorageInputs {
            input,
            identity: prepared.identity(),
            directory: prepared.directory(),
            binary_directory: binaries,
            artifact_bootstrap: Some(storage_setup::ArtifactBootstrapFile {
                name: "artifact-bootstrap.json",
                digest: &artifact_digest,
            }),
        };
        let artifact = storage_setup::run_model_inputs(&storage_inputs, storage_mode).await?;
        let seed = model_setup::prepare_seed(
            input,
            prepared.identity(),
            prepared.directory(),
            &artifact,
            artifact_authority
                .retention_policy
                .minimum_retention_seconds,
            model_mode,
        )?;
        let model = model_setup::ensure_model_object(
            input,
            prepared.identity(),
            prepared.directory(),
            seed,
            artifact_catalog.clone(),
            model_mode,
        )
        .await?;
        storage_setup::run_model_policy_stage(
            &storage_inputs,
            &model.seed.canonical_digest().map_err(|_| invalid())?,
            &canonical(&model.material)?,
            storage_mode,
        )
        .await?;
        model_profile::installation_catalog(input, prepared.identity(), &model.built, &builds)?
    };
    let rendered = renderer::render_installation(InstallationRenderInputs {
        input,
        identity: prepared.identity(),
        builds: &builds,
        binary_directory: binaries,
        jwks: &prepared.jwks()?,
        provider_ready: &provider_ready,
        artifact_bucket: &bucket,
        artifact_bootstrap: &artifact_authority,
        model_installation: model_catalog.as_ref(),
        capability_protocol_profile: None,
        private_files: &prepared.renderer_private_files()?,
    })?;
    role_output::publish_role_outputs(
        input,
        prepared.identity(),
        &rendered,
        prepared.directory(),
        &output.join("roles"),
        if verify {
            RoleOutputMode::Verify
        } else {
            RoleOutputMode::Provision
        },
        ownership(input),
    )?;
    if prepared.progress().phase == InstallationPhase::AuthorityBootstrapped {
        prepared.complete_phase(InstallationPhase::Ready)?;
    }
    if prepared.progress().phase != InstallationPhase::Ready {
        return Err(InstallationError::Incomplete);
    }
    Ok(prepared)
}

fn provider_data(
    prepared: &PreparedInstallation,
    output: &Path,
    mode: RoleOutputMode,
) -> Result<(), InstallationError> {
    use role_output::DependencyDataDirectory as D;
    for (name, kind) in [("s3-data", D::S3), ("openbao-data", D::OpenBao)] {
        role_output::initialize_dependency_data_directory(
            prepared.input(),
            prepared.identity(),
            prepared.directory(),
            &output.join(name),
            mode,
            ownership(prepared.input()),
            kind,
        )?;
    }
    if prepared.input().network.topology == InstallationTopology::Native {
        role_output::initialize_dependency_data_directory(
            prepared.input(),
            prepared.identity(),
            prepared.directory(),
            &output.join("postgres-data"),
            mode,
            ownership(prepared.input()),
            D::Postgres,
        )?;
    }
    Ok(())
}
