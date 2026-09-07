//! Closed Job kind, work class and owner combinations used by runtime validation.

use crate::{JobKind, ResourceKind, WorkClass};

pub const EXECUTION_WORK_OWNER_PAIRS: &[(WorkClass, ResourceKind)] = &[
    (WorkClass::RegistryValidation, ResourceKind::Job),
    (WorkClass::Orchestration, ResourceKind::NodeExecution),
    (WorkClass::Model, ResourceKind::ModelTurn),
    (
        WorkClass::CapabilityNative,
        ResourceKind::CapabilityInvocation,
    ),
    (
        WorkClass::CapabilityRemote,
        ResourceKind::CapabilityInvocation,
    ),
    (WorkClass::Mcp, ResourceKind::McpOperation),
    (WorkClass::Context, ResourceKind::ContextQuery),
    (WorkClass::Context, ResourceKind::ContextDataset),
    (WorkClass::Context, ResourceKind::McpOperation),
    (WorkClass::Sandbox, ResourceKind::Job),
    (WorkClass::Interaction, ResourceKind::Interaction),
    (WorkClass::Artifact, ResourceKind::Artifact),
    (WorkClass::Artifact, ResourceKind::InternalBlob),
    (WorkClass::Recovery, ResourceKind::Run),
    (WorkClass::Recovery, ResourceKind::NodeExecution),
    (WorkClass::Recovery, ResourceKind::CapabilityInvocation),
    (WorkClass::Recovery, ResourceKind::ContextQuery),
    (WorkClass::Recovery, ResourceKind::McpOperation),
    (WorkClass::Recovery, ResourceKind::ModelTurn),
    (WorkClass::Recovery, ResourceKind::Job),
    (WorkClass::Recovery, ResourceKind::Interaction),
];

pub const JOB_KIND_WORK_OWNER_TRIPLES: &[(JobKind, WorkClass, ResourceKind)] = &[
    (
        JobKind::RegistryValidation,
        WorkClass::RegistryValidation,
        ResourceKind::Job,
    ),
    (
        JobKind::OrchestrationNode,
        WorkClass::Orchestration,
        ResourceKind::NodeExecution,
    ),
    (
        JobKind::ModelTurn,
        WorkClass::Model,
        ResourceKind::ModelTurn,
    ),
    (
        JobKind::CapabilityInvocation,
        WorkClass::CapabilityNative,
        ResourceKind::CapabilityInvocation,
    ),
    (
        JobKind::CapabilityInvocation,
        WorkClass::CapabilityRemote,
        ResourceKind::CapabilityInvocation,
    ),
    (
        JobKind::McpDiscovery,
        WorkClass::Mcp,
        ResourceKind::McpOperation,
    ),
    (
        JobKind::McpSubscription,
        WorkClass::Mcp,
        ResourceKind::McpOperation,
    ),
    (
        JobKind::ContextQueryNative,
        WorkClass::Context,
        ResourceKind::ContextQuery,
    ),
    (
        JobKind::ContextQueryRemote,
        WorkClass::Context,
        ResourceKind::ContextQuery,
    ),
    (
        JobKind::ContextDatasetBuild,
        WorkClass::Context,
        ResourceKind::ContextDataset,
    ),
    (
        JobKind::ContextSubscriptionRefresh,
        WorkClass::Context,
        ResourceKind::McpOperation,
    ),
    (
        JobKind::SandboxCapabilityExecution,
        WorkClass::Sandbox,
        ResourceKind::Job,
    ),
    (
        JobKind::Interaction,
        WorkClass::Interaction,
        ResourceKind::Interaction,
    ),
    (
        JobKind::ArtifactScan,
        WorkClass::Artifact,
        ResourceKind::Artifact,
    ),
    (
        JobKind::ArtifactRescan,
        WorkClass::Artifact,
        ResourceKind::Artifact,
    ),
    (
        JobKind::ArtifactDelete,
        WorkClass::Artifact,
        ResourceKind::Artifact,
    ),
    (
        JobKind::ArtifactBlobCleanup,
        WorkClass::Artifact,
        ResourceKind::InternalBlob,
    ),
    (JobKind::Recovery, WorkClass::Recovery, ResourceKind::Run),
    (
        JobKind::Recovery,
        WorkClass::Recovery,
        ResourceKind::NodeExecution,
    ),
    (
        JobKind::Recovery,
        WorkClass::Recovery,
        ResourceKind::CapabilityInvocation,
    ),
    (
        JobKind::Recovery,
        WorkClass::Recovery,
        ResourceKind::ContextQuery,
    ),
    (
        JobKind::Recovery,
        WorkClass::Recovery,
        ResourceKind::McpOperation,
    ),
    (
        JobKind::Recovery,
        WorkClass::Recovery,
        ResourceKind::ModelTurn,
    ),
    (JobKind::Recovery, WorkClass::Recovery, ResourceKind::Job),
    (
        JobKind::McpOAuthPkceCleanup,
        WorkClass::Recovery,
        ResourceKind::Interaction,
    ),
];

pub const fn is_job_kind_work_owner_triple(
    job_kind: JobKind,
    work_class: WorkClass,
    owner_kind: ResourceKind,
) -> bool {
    let mut index = 0;
    while index < JOB_KIND_WORK_OWNER_TRIPLES.len() {
        let candidate = JOB_KIND_WORK_OWNER_TRIPLES[index];
        if candidate.0 as u8 == job_kind as u8
            && candidate.1 as u8 == work_class as u8
            && candidate.2 as u8 == owner_kind as u8
        {
            return true;
        }
        index += 1;
    }
    false
}

pub const fn is_execution_work_owner_pair(work_class: WorkClass, owner_kind: ResourceKind) -> bool {
    let mut index = 0;
    while index < EXECUTION_WORK_OWNER_PAIRS.len() {
        let candidate = EXECUTION_WORK_OWNER_PAIRS[index];
        if candidate.0 as u8 == work_class as u8 && candidate.1 as u8 == owner_kind as u8 {
            return true;
        }
        index += 1;
    }
    false
}
