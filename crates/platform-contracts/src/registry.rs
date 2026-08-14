use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::{error::Error, fmt, str::FromStr};

use crate::{id::ResourceKind, limits::LimitUnit};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownRegistryValue {
    registry: &'static str,
    value: String,
}

impl UnknownRegistryValue {
    fn new(registry: &'static str, value: impl Into<String>) -> Self {
        Self {
            registry,
            value: value.into(),
        }
    }
}

impl fmt::Display for UnknownRegistryValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown value {:?} for closed {} registry",
            self.value, self.registry
        )
    }
}

impl Error for UnknownRegistryValue {}

macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident, $registry:literal {
            $($variant:ident => $wire:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $wire),+
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = UnknownRegistryValue;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($wire => Ok(Self::$variant)),+,
                    _ => Err(UnknownRegistryValue::new($registry, value)),
                }
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                value.parse().map_err(de::Error::custom)
            }
        }
    };
}

string_enum! {
    pub enum PrincipalKind, "principal kind" {
        InstallationOperator => "installation_operator",
        TenantAdmin => "tenant_admin",
        AgentAuthor => "agent_author",
        AgentRunner => "agent_runner",
        HumanApprover => "human_approver",
        ServiceIdentity => "service_identity"
    }
}

string_enum! {
    pub enum AuthnStrength, "authentication strength" {
        SingleFactor => "single_factor",
        MultiFactor => "multi_factor",
        PhishingResistant => "phishing_resistant",
        WorkloadIdentity => "workload_identity"
    }
}

string_enum! {
    pub enum LockRank, "lock rank" {
        CommandReceipt => "command_receipt",
        SchedulerFairness => "scheduler_fairness",
        TenantQuotaPolicy => "tenant_quota_policy",
        ParentRootAggregate => "parent_root_aggregate",
        ChildLeafAggregate => "child_leaf_aggregate",
        JobFence => "job_fence",
        PublicRunStreamHead => "public_run_stream_head",
        AppendOnlyProjectionOutbox => "append_only_projection_outbox"
    }
}

impl LockRank {
    pub const fn ordinal(self) -> u8 {
        match self {
            Self::CommandReceipt => 10,
            Self::SchedulerFairness => 15,
            Self::TenantQuotaPolicy => 20,
            Self::ParentRootAggregate => 30,
            Self::ChildLeafAggregate => 40,
            Self::JobFence => 50,
            Self::PublicRunStreamHead => 60,
            Self::AppendOnlyProjectionOutbox => 70,
        }
    }
}

string_enum! {
    pub enum WorkClass, "work class" {
        RegistryValidation => "registry_validation",
        Orchestration => "orchestration",
        Model => "model",
        CapabilityNative => "capability_native",
        CapabilityRemote => "capability_remote",
        Mcp => "mcp",
        Context => "context",
        Sandbox => "sandbox",
        Interaction => "interaction",
        Artifact => "artifact",
        Recovery => "recovery"
    }
}

string_enum! {
    pub enum PlanNodeKind, "plan node kind" {
        Start => "start",
        Compute => "compute",
        Branch => "branch",
        Fork => "fork",
        Join => "join",
        Map => "map",
        Loop => "loop",
        ErrorBoundary => "error_boundary",
        ModelLoop => "model_loop",
        CapabilityCall => "capability_call",
        ContextQuery => "context_query",
        ChildAgentCall => "child_agent_call",
        HumanTask => "human_task",
        TimerWait => "timer_wait",
        SignalWait => "signal_wait",
        Return => "return",
        Raise => "raise"
    }
}

string_enum! {
    pub enum ScopeKind, "scope kind" {
        Root => "root",
        BranchLeg => "branch_leg",
        ParallelLeg => "parallel_leg",
        MapItem => "map_item",
        LoopIteration => "loop_iteration",
        ModelRound => "model_round",
        ErrorBoundary => "error_boundary"
    }
}

string_enum! {
    pub enum WakeContractKind, "wake contract kind" {
        Timer => "timer",
        Signal => "signal",
        HumanTask => "human_task",
        Approval => "approval",
        RemoteInvocation => "remote_invocation",
        ChildRun => "child_run",
        RetryDeadline => "retry_deadline"
    }
}

string_enum! {
    pub enum InteractionKind, "interaction kind" {
        Form => "form",
        UrlConsent => "url_consent",
        BusinessInput => "business_input"
    }
}

string_enum! {
    pub enum SchedulerPriority, "scheduler priority" {
        Low => "low",
        Normal => "normal",
        High => "high",
        CriticalControl => "critical_control"
    }
}

string_enum! {
    pub enum ServiceClass, "public service class" {
        Low => "low",
        Normal => "normal",
        High => "high"
    }
}

string_enum! {
    pub enum ArtifactPurpose, "artifact purpose" {
        AuthoringDocument => "authoring_document",
        InterfaceContract => "interface_contract",
        TypedPlan => "typed_plan",
        Package => "package",
        Sbom => "sbom",
        BackendBinding => "backend_binding",
        ModelGenerationDefaults => "model_generation_defaults",
        RunInput => "run_input",
        RunOutput => "run_output",
        CapabilityInput => "capability_input",
        CapabilityOutput => "capability_output",
        ContextSource => "context_source",
        ContextDerived => "context_derived",
        McpResource => "mcp_resource",
        SandboxInput => "sandbox_input",
        SandboxOutput => "sandbox_output",
        Diagnostic => "diagnostic",
        Export => "export"
    }
}

string_enum! {
    pub enum ArtifactReferenceKind, "artifact reference kind" {
        Definition => "definition",
        Input => "input",
        Output => "output",
        Evidence => "evidence",
        Package => "package",
        Attachment => "attachment",
        Result => "result",
        Provenance => "provenance"
    }
}

string_enum! {
    pub enum ArtifactGrantOperation, "artifact grant operation" {
        ReadWhole => "read_whole",
        ReadRange => "read_range",
        WriteStaging => "write_staging",
        CommitStaging => "commit_staging"
    }
}

string_enum! {
    pub enum ArtifactWorkloadAudience, "artifact workload audience" {
        Principal => "principal",
        Runtime => "runtime",
        RegistryWorker => "registry_worker",
        CapabilityWorker => "capability_worker",
        ContextWorker => "context_worker",
        ModelWorker => "model_worker",
        McpHost => "mcp_host",
        SandboxGateway => "sandbox_gateway",
        ArtifactWorker => "artifact_worker"
    }
}

string_enum! {
    pub enum BlobIntegrityState, "blob integrity state" {
        Staging => "staging",
        Verified => "verified",
        Corrupt => "corrupt",
        Deleting => "deleting",
        Deleted => "deleted"
    }
}

string_enum! {
    pub enum ManagementOperationKind, "management operation kind" {
        Validation => "validation",
        Import => "import",
        Discovery => "discovery",
        Build => "build",
        ArtifactUpload => "artifact_upload",
        ArtifactVerify => "artifact_verify",
        ArtifactRescan => "artifact_rescan",
        ArtifactDelete => "artifact_delete",
        Export => "export"
    }
}

string_enum! {
    pub enum AgentAuthoringMode, "agent authoring mode" {
        Structured => "structured",
        Graph => "graph"
    }
}

string_enum! {
    pub enum DependencySlotKind, "dependency slot kind" {
        Model => "model",
        Capability => "capability",
        Context => "context",
        ChildAgent => "child_agent",
        Skill => "skill"
    }
}

string_enum! {
    pub enum CapabilityBackendKind, "capability backend kind" {
        Native => "native",
        Http => "http",
        Grpc => "grpc",
        Mcp => "mcp",
        Sandbox => "sandbox"
    }
}

string_enum! {
    pub enum CapabilityIdempotencyKind, "capability idempotency kind" {
        Intrinsic => "intrinsic",
        CallerKey => "caller_key",
        ReconcileBeforeRetry => "reconcile_before_retry",
        None => "none"
    }
}

string_enum! {
    pub enum CapabilityCancellationKind, "capability cancellation kind" {
        Unsupported => "unsupported",
        BestEffort => "best_effort",
        Confirmed => "confirmed"
    }
}

string_enum! {
    pub enum CapabilityProgressMode, "capability progress mode" {
        None => "none",
        Events => "events"
    }
}

string_enum! {
    pub enum CapabilityProgressDurability, "capability progress durability" {
        None => "none",
        LiveOnly => "live_only",
        CoarseDurable => "coarse_durable"
    }
}

string_enum! {
    pub enum SkillInstructionPhase, "skill instruction phase" {
        TaskUnderstanding => "task_understanding",
        Planning => "planning",
        ToolUse => "tool_use",
        Validation => "validation",
        OutputComposition => "output_composition"
    }
}

string_enum! {
    pub enum SkillInstructionAudience, "skill instruction audience" {
        Planner => "planner",
        ToolUser => "tool_user",
        Validator => "validator",
        Composer => "composer"
    }
}

string_enum! {
    pub enum SkillRequirementKind, "skill requirement kind" {
        Capability => "capability",
        Context => "context",
        ModelFeature => "model_feature"
    }
}

string_enum! {
    pub enum SkillPackageEntryKind, "skill package entry kind" {
        Manifest => "manifest",
        Instruction => "instruction",
        Reference => "reference",
        Example => "example",
        Asset => "asset"
    }
}

string_enum! {
    pub enum SkillSelectionMode, "skill selection mode" {
        Required => "required",
        PlanSelected => "plan_selected",
        PolicySelected => "policy_selected",
        ModelProposed => "model_proposed"
    }
}

string_enum! {
    pub enum ContextBackendKind, "context backend kind" {
        ManagedIndex => "managed_index",
        RemoteSearch => "remote_search",
        McpResources => "mcp_resources",
        SqlCatalog => "sql_catalog",
        ArtifactCollection => "artifact_collection",
        NativeCatalog => "native_catalog"
    }
}

string_enum! {
    pub enum ContextConsistencyMode, "context consistency mode" {
        PinnedGeneration => "pinned_generation",
        PinAtRunAdmission => "pin_at_run_admission",
        LatestAtQueryStart => "latest_at_query_start",
        ExternalObservation => "external_observation"
    }
}

string_enum! {
    pub enum ContextCitationStrength, "context citation strength" {
        Exact => "exact",
        ObservationOnly => "observation_only"
    }
}

string_enum! {
    pub enum ContextBackendOutcomeKind, "context backend outcome kind" {
        Completed => "completed",
        Deferred => "deferred",
        RetryableFailure => "retryable_failure",
        PermanentFailure => "permanent_failure"
    }
}

string_enum! {
    pub enum McpTransportKind, "mcp transport kind" {
        StreamableHttp => "streamable_http",
        ManagedStdio => "managed_stdio"
    }
}

string_enum! {
    pub enum McpAuthorizationPrincipalKind, "mcp authorization principal kind" {
        PerUser => "per_user",
        ServiceIdentity => "service_identity"
    }
}

string_enum! {
    pub enum McpOAuthClientAuthenticationKind, "mcp oauth client authentication kind" {
        None => "none",
        ClientSecretBasic => "client_secret_basic"
    }
}

string_enum! {
    pub enum ModelIdentityStability, "model identity stability" {
        Pinned => "pinned",
        ExternallyMutable => "externally_mutable"
    }
}

string_enum! {
    pub enum ModelModality, "model modality" {
        Text => "text",
        Image => "image",
        Audio => "audio",
        Document => "document"
    }
}

string_enum! {
    pub enum SandboxRuntimeFamily, "sandbox runtime family" {
        Python => "python",
        NodeJs => "node_js",
        WasmWasi => "wasm_wasi",
        ReviewedShell => "reviewed_shell",
        ManagedMcpServer => "managed_mcp_server"
    }
}

string_enum! {
    pub enum SandboxIsolationClass, "sandbox isolation class" {
        Wasm => "wasm",
        SandboxedContainer => "sandboxed_container",
        MicroVm => "micro_vm"
    }
}

impl SandboxIsolationClass {
    pub const fn security_rank(self) -> u8 {
        match self {
            Self::Wasm => 1,
            Self::SandboxedContainer => 2,
            Self::MicroVm => 3,
        }
    }
}

string_enum! {
    pub enum SandboxAbiVersion, "sandbox ABI version" {
        V1 => "v1"
    }
}

string_enum! {
    pub enum SandboxCleanupPolicy, "sandbox cleanup policy" {
        SingleUseDestroy => "single_use_destroy"
    }
}

string_enum! {
    pub enum SandboxEntrypointKind, "sandbox entrypoint kind" {
        PythonModule => "python_module",
        NodeModule => "node_module",
        WasmExport => "wasm_export",
        ReviewedExecutable => "reviewed_executable",
        ManagedMcpServer => "managed_mcp_server"
    }
}

string_enum! {
    pub enum QuotaAccountingMode, "quota accounting mode" {
        Leased => "leased",
        Consumable => "consumable",
        Reclaimable => "reclaimable"
    }
}

string_enum! {
    pub enum QuotaScopeKind, "quota scope kind" {
        Tenant => "tenant",
        AgentDeployment => "agent_deployment",
        WorkClass => "work_class",
        CapabilityDeployment => "capability_deployment",
        ModelDeployment => "model_deployment",
        ContextDeployment => "context_deployment",
        McpDeployment => "mcp_deployment",
        SandboxProfileRevision => "sandbox_profile_revision",
        Run => "run",
        Principal => "principal"
    }
}

string_enum! {
    pub enum QuotaWindowKind, "quota window kind" {
        Current => "current",
        Run => "run",
        UtcDay => "utc_day",
        UtcMonth => "utc_month",
        Lifetime => "lifetime"
    }
}

string_enum! {
    /// Each wire value is the one authoritative HardLimitProfile field path.
    pub enum QuotaDimension, "quota dimension" {
        TenantActiveRuns => "run_scheduler.active_runs_per_tenant",
        TenantWaitingRuns => "run_scheduler.waiting_runs_per_tenant",
        AgentConcurrentRuns => "durable_quota.agent_concurrent_runs",
        WorkClassConcurrentOperations => "durable_quota.work_class_concurrent_operations",
        CapabilityConcurrentInvocations => "durable_quota.capability_concurrent_invocations",
        SandboxConcurrentExecutions => "durable_quota.sandbox_concurrent_executions",
        SandboxCpuSeconds => "durable_quota.sandbox_cpu_seconds",
        SandboxMemoryMebibytes => "durable_quota.sandbox_memory_mebibytes",
        SandboxOutputBytes => "durable_quota.sandbox_output_bytes",
        ModelTokens => "durable_quota.model_tokens",
        ModelCostMicrounits => "durable_quota.model_cost_microunits",
        ModelRequests => "durable_quota.model_requests",
        ContextQueries => "durable_quota.context_queries",
        ContextResultBytes => "durable_quota.context_result_bytes",
        McpSessions => "model_context_mcp.mcp_sessions_per_tenant",
        McpTasks => "model_context_mcp.mcp_tasks_per_session",
        McpSubscriptions => "model_context_mcp.mcp_subscriptions_per_session",
        ArtifactCount => "durable_quota.artifact_count",
        ArtifactLogicalBytes => "artifact.tenant_total_bytes",
        ArtifactPhysicalBytes => "durable_quota.artifact_physical_bytes",
        ArtifactStagingBytes => "durable_quota.artifact_staging_bytes",
        ArtifactUploads => "durable_quota.artifact_uploads",
        HumanTasksPending => "durable_quota.human_tasks_pending"
    }
}

impl QuotaDimension {
    pub const fn accounting_mode(self) -> QuotaAccountingMode {
        match self {
            Self::TenantActiveRuns
            | Self::TenantWaitingRuns
            | Self::AgentConcurrentRuns
            | Self::WorkClassConcurrentOperations
            | Self::CapabilityConcurrentInvocations
            | Self::SandboxConcurrentExecutions
            | Self::SandboxMemoryMebibytes
            | Self::McpSessions
            | Self::McpTasks
            | Self::McpSubscriptions
            | Self::ArtifactStagingBytes
            | Self::ArtifactUploads
            | Self::HumanTasksPending => QuotaAccountingMode::Leased,
            Self::SandboxCpuSeconds
            | Self::SandboxOutputBytes
            | Self::ModelTokens
            | Self::ModelCostMicrounits
            | Self::ModelRequests
            | Self::ContextQueries
            | Self::ContextResultBytes => QuotaAccountingMode::Consumable,
            Self::ArtifactCount | Self::ArtifactLogicalBytes | Self::ArtifactPhysicalBytes => {
                QuotaAccountingMode::Reclaimable
            }
        }
    }

    pub const fn unit(self) -> LimitUnit {
        match self {
            Self::TenantActiveRuns
            | Self::TenantWaitingRuns
            | Self::AgentConcurrentRuns
            | Self::WorkClassConcurrentOperations
            | Self::CapabilityConcurrentInvocations
            | Self::SandboxConcurrentExecutions
            | Self::ModelRequests
            | Self::ContextQueries
            | Self::McpTasks
            | Self::McpSubscriptions
            | Self::ArtifactCount
            | Self::ArtifactUploads
            | Self::HumanTasksPending => LimitUnit::Count,
            Self::SandboxCpuSeconds => LimitUnit::Seconds,
            Self::SandboxMemoryMebibytes => LimitUnit::Mebibytes,
            Self::SandboxOutputBytes
            | Self::ContextResultBytes
            | Self::ArtifactLogicalBytes
            | Self::ArtifactPhysicalBytes
            | Self::ArtifactStagingBytes => LimitUnit::Bytes,
            Self::ModelTokens => LimitUnit::Tokens,
            Self::ModelCostMicrounits => LimitUnit::CurrencyMicrounits,
            Self::McpSessions => LimitUnit::Connections,
        }
    }
}

string_enum! {
    pub enum PolicyKind, "policy kind" {
        Authorization => "authorization",
        Approval => "approval",
        DataFlow => "data_flow",
        Declassification => "declassification",
        Network => "network",
        Tls => "tls",
        Trust => "trust",
        Retry => "retry",
        Budget => "budget",
        Quota => "quota",
        Selection => "selection",
        Scheduling => "scheduling",
        Execution => "execution",
        Resource => "resource",
        Isolation => "isolation",
        Parser => "parser",
        Chunker => "chunker",
        Ranking => "ranking",
        Retention => "retention",
        ArtifactIo => "artifact_io",
        SecretResolution => "secret_resolution",
        PublicProjection => "public_projection",
        Protocol => "protocol",
        McpAuth => "mcp_auth"
    }
}

string_enum! {
    pub enum PolicyReferenceRole, "policy reference role" {
        Authorization => "authorization",
        Approval => "approval",
        Data => "data",
        Declassification => "declassification",
        Network => "network",
        Tls => "tls",
        Trust => "trust",
        Isolation => "isolation",
        Retry => "retry",
        Budget => "budget",
        Quota => "quota",
        Selection => "selection",
        Scheduling => "scheduling",
        Execution => "execution",
        Resource => "resource",
        Parser => "parser",
        Chunker => "chunker",
        Ranking => "ranking",
        Retention => "retention",
        ArtifactIo => "artifact_io",
        SecretRotation => "secret_rotation",
        PublicProjection => "public_projection",
        Protocol => "protocol",
        AuthProfile => "auth_profile"
    }
}

impl PolicyReferenceRole {
    pub const fn expected_kind(self) -> PolicyKind {
        match self {
            Self::Authorization => PolicyKind::Authorization,
            Self::Approval => PolicyKind::Approval,
            Self::Data => PolicyKind::DataFlow,
            Self::Declassification => PolicyKind::Declassification,
            Self::Network => PolicyKind::Network,
            Self::Tls => PolicyKind::Tls,
            Self::Trust => PolicyKind::Trust,
            Self::Isolation => PolicyKind::Isolation,
            Self::Retry => PolicyKind::Retry,
            Self::Budget => PolicyKind::Budget,
            Self::Quota => PolicyKind::Quota,
            Self::Selection => PolicyKind::Selection,
            Self::Scheduling => PolicyKind::Scheduling,
            Self::Execution => PolicyKind::Execution,
            Self::Resource => PolicyKind::Resource,
            Self::Parser => PolicyKind::Parser,
            Self::Chunker => PolicyKind::Chunker,
            Self::Ranking => PolicyKind::Ranking,
            Self::Retention => PolicyKind::Retention,
            Self::ArtifactIo => PolicyKind::ArtifactIo,
            Self::SecretRotation => PolicyKind::SecretResolution,
            Self::PublicProjection => PolicyKind::PublicProjection,
            Self::Protocol => PolicyKind::Protocol,
            Self::AuthProfile => PolicyKind::McpAuth,
        }
    }
}

string_enum! {
    pub enum Permission, "permission" {
        InstallationManage => "installation.manage",
        InstallationSupport => "installation.support",
        TenantRead => "tenant.read",
        TenantManage => "tenant.manage",
        TenantEmergencyStop => "tenant.emergency_stop",
        AgentRead => "agent.read",
        AgentWrite => "agent.write",
        AgentPublish => "agent.publish",
        AgentDeploy => "agent.deploy",
        AgentActivate => "agent.activate",
        AgentRun => "agent.run",
        SkillRead => "skill.read",
        SkillWrite => "skill.write",
        SkillPublish => "skill.publish",
        SkillBind => "skill.bind",
        SkillActivate => "skill.activate",
        CapabilityRead => "capability.read",
        CapabilityWrite => "capability.write",
        CapabilityPublish => "capability.publish",
        CapabilityDeploy => "capability.deploy",
        CapabilityActivate => "capability.activate",
        CapabilityBind => "capability.bind",
        CapabilityInvoke => "capability.invoke",
        ContextRead => "context.read",
        ContextWrite => "context.write",
        ContextPublish => "context.publish",
        ContextDeploy => "context.deploy",
        ContextActivate => "context.activate",
        ContextQuery => "context.query",
        ContextBuildDataset => "context.build_dataset",
        McpRead => "mcp.read",
        McpWrite => "mcp.write",
        McpDiscover => "mcp.discover",
        McpImport => "mcp.import",
        McpPublish => "mcp.publish",
        McpDeploy => "mcp.deploy",
        McpActivate => "mcp.activate",
        McpInvoke => "mcp.invoke",
        ModelRead => "model.read",
        ModelWrite => "model.write",
        ModelDiscover => "model.discover",
        ModelImport => "model.import",
        ModelPublish => "model.publish",
        ModelDeploy => "model.deploy",
        ModelActivate => "model.activate",
        ModelInvoke => "model.invoke",
        SandboxRead => "sandbox.read",
        SandboxWrite => "sandbox.write",
        SandboxBuild => "sandbox.build",
        SandboxPublish => "sandbox.publish",
        SandboxActivate => "sandbox.activate",
        SandboxExecute => "sandbox.execute",
        ArtifactRead => "artifact.read",
        ArtifactWrite => "artifact.write",
        ArtifactDelete => "artifact.delete",
        ArtifactHold => "artifact.hold",
        ArtifactRescan => "artifact.rescan",
        ApprovalRead => "approval.read",
        ApprovalRespond => "approval.respond",
        InteractionRead => "interaction.read",
        InteractionRespond => "interaction.respond",
        PolicyRead => "policy.read",
        PolicyWrite => "policy.write",
        PolicyPublish => "policy.publish",
        PolicyActivate => "policy.activate",
        OperationRead => "operation.read",
        OperationCancel => "operation.cancel",
        RuntimeRead => "runtime.read",
        RuntimeControl => "runtime.control",
        RuntimeSignal => "runtime.signal",
        SecretInspect => "secret.inspect",
        SecretBind => "secret.bind",
        SecretRotate => "secret.rotate",
        SecretRevoke => "secret.revoke"
    }
}

string_enum! {
    pub enum Effect, "effect" {
        Pure => "pure",
        ReadOnly => "read_only",
        IdempotentWrite => "idempotent_write",
        NonIdempotentWrite => "non_idempotent_write",
        Irreversible => "irreversible"
    }
}

impl Effect {
    pub const fn risk_rank(self) -> u8 {
        match self {
            Self::Pure => 0,
            Self::ReadOnly => 1,
            Self::IdempotentWrite => 2,
            Self::NonIdempotentWrite => 3,
            Self::Irreversible => 4,
        }
    }
}

string_enum! {
    pub enum DataClassification, "data classification" {
        Public => "public",
        Internal => "internal",
        Confidential => "confidential",
        Restricted => "restricted"
    }
}

impl DataClassification {
    pub const fn rank(self) -> u8 {
        match self {
            Self::Public => 0,
            Self::Internal => 1,
            Self::Confidential => 2,
            Self::Restricted => 3,
        }
    }

    pub const fn join(self, other: Self) -> Self {
        if self.rank() >= other.rank() {
            self
        } else {
            other
        }
    }
}

string_enum! {
    pub enum CodeTrustClass, "code trust class" {
        BuiltIn => "built_in",
        ReviewedPublished => "reviewed_published",
        TenantPublished => "tenant_published",
        ModelGenerated => "model_generated"
    }
}

string_enum! {
    pub enum FailureClass, "failure class" {
        Validation => "validation",
        Authorization => "authorization",
        Policy => "policy",
        Quota => "quota",
        Deadline => "deadline",
        Dependency => "dependency",
        External => "external",
        Resource => "resource",
        Cancelled => "cancelled",
        UncertainEffect => "uncertain_effect",
        Platform => "platform"
    }
}

string_enum! {
    pub enum Retryability, "retryability" {
        Never => "never",
        SafeWithinPolicy => "safe_within_policy",
        ReconcileBeforeRetry => "reconcile_before_retry"
    }
}

string_enum! {
    pub enum FailureSource, "failure source" {
        Agent => "agent",
        Plan => "plan",
        Capability => "capability",
        Context => "context",
        Model => "model",
        ChildAgent => "child_agent",
        Artifact => "artifact",
        Interaction => "interaction",
        Dependency => "dependency",
        Platform => "platform"
    }
}

string_enum! {
    pub enum PlatformFailureCode, "platform failure code" {
        AgentInputInvalid => "agent_input_invalid",
        AgentOutputInvalid => "agent_output_invalid",
        PlanInvariantFailed => "plan_invariant_failed",
        BudgetExhausted => "budget_exhausted",
        DeadlineExceeded => "deadline_exceeded",
        CapabilityFailed => "capability_failed",
        ContextQueryFailed => "context_query_failed",
        ModelTurnFailed => "model_turn_failed",
        ChildAgentFailed => "child_agent_failed",
        ArtifactUnavailable => "artifact_unavailable",
        InteractionFailed => "interaction_failed",
        DependencyUnavailable => "dependency_unavailable",
        ContentRejected => "content_rejected",
        UncertainEffect => "uncertain_effect",
        PlatformInvariantFailed => "platform_invariant_failed"
    }
}

string_enum! {
    pub enum ApiProblemCode, "API problem code" {
        InvalidRequest => "invalid_request",
        SchemaValidationFailed => "schema_validation_failed",
        Unauthenticated => "unauthenticated",
        PermissionDenied => "permission_denied",
        ResourceNotFound => "resource_not_found",
        EtagMismatch => "etag_mismatch",
        IdempotencyConflict => "idempotency_conflict",
        InvalidStateTransition => "invalid_state_transition",
        PolicyDenied => "policy_denied",
        ApprovalRequired => "approval_required",
        QuotaExceeded => "quota_exceeded",
        RateLimited => "rate_limited",
        ResourceSuspended => "resource_suspended",
        SecretUnavailable => "secret_unavailable",
        NetworkDenied => "network_denied",
        IsolationUnavailable => "isolation_unavailable",
        ContentRejected => "content_rejected",
        CursorInvalid => "cursor_invalid",
        CursorExpired => "cursor_expired",
        RunNotTerminal => "run_not_terminal",
        OperationNotTerminal => "operation_not_terminal",
        DeadlineExceeded => "deadline_exceeded",
        TemporarilyUnavailable => "temporarily_unavailable",
        InternalError => "internal_error"
    }
}

string_enum! {
    pub enum EventDurability, "event durability" {
        Snapshot => "snapshot",
        Durable => "durable",
        LiveOnly => "live_only"
    }
}

string_enum! {
    pub enum CursorPurpose, "cursor purpose" {
        List => "list",
        RunEvent => "run_event"
    }
}

string_enum! {
    pub enum PublicRunEventSourceKind, "public Run event source kind" {
        Run => "run",
        RunControl => "run_control",
        NodeExecution => "node_execution",
        SkillActivation => "skill_activation",
        ModelTurn => "model_turn",
        CapabilityInvocation => "capability_invocation",
        ContextQuery => "context_query",
        ChildRunLink => "child_run_link",
        Interaction => "interaction",
        ApprovalTask => "approval_task"
    }
}

impl PublicRunEventSourceKind {
    pub const fn resource_kind(self) -> ResourceKind {
        match self {
            Self::Run | Self::RunControl => ResourceKind::Run,
            Self::NodeExecution => ResourceKind::NodeExecution,
            Self::SkillActivation => ResourceKind::SkillActivation,
            Self::ModelTurn => ResourceKind::ModelTurn,
            Self::CapabilityInvocation => ResourceKind::CapabilityInvocation,
            Self::ContextQuery => ResourceKind::ContextQuery,
            Self::ChildRunLink => ResourceKind::ChildRunLink,
            Self::Interaction => ResourceKind::Interaction,
            Self::ApprovalTask => ResourceKind::ApprovalTask,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CursorPurposeMismatch {
    pub expected: CursorPurpose,
    pub actual: CursorPurpose,
}

impl fmt::Display for CursorPurposeMismatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "cursor purpose mismatch: expected {}, got {}",
            self.expected, self.actual
        )
    }
}

impl Error for CursorPurposeMismatch {}

pub fn require_cursor_purpose(
    actual: CursorPurpose,
    expected: CursorPurpose,
) -> Result<(), CursorPurposeMismatch> {
    if actual == expected {
        Ok(())
    } else {
        Err(CursorPurposeMismatch { expected, actual })
    }
}

string_enum! {
    pub enum PublicRunEventType, "public Run event type" {
        RunSnapshot => "run.snapshot",
        RunQueued => "run.queued",
        RunStarted => "run.started",
        RunWaiting => "run.waiting",
        RunPaused => "run.paused",
        RunResumed => "run.resumed",
        RunCancelling => "run.cancelling",
        RunCompleted => "run.completed",
        RunFailed => "run.failed",
        RunCancelled => "run.cancelled",
        RunTimedOut => "run.timed_out",
        NodeStarted => "node.started",
        NodeCompleted => "node.completed",
        NodeFailed => "node.failed",
        NodeCancelled => "node.cancelled",
        NodeTimedOut => "node.timed_out",
        SkillSelected => "skill.selected",
        SkillActivated => "skill.activated",
        SkillRejected => "skill.rejected",
        ModelStarted => "model.started",
        ModelDelta => "model.delta",
        ModelToolIntent => "model.tool_intent",
        ModelCompleted => "model.completed",
        ModelFailed => "model.failed",
        ModelCancelled => "model.cancelled",
        ModelTimedOut => "model.timed_out",
        CapabilityStarted => "capability.started",
        CapabilityWaiting => "capability.waiting",
        CapabilityInputRequired => "capability.input_required",
        CapabilityProgress => "capability.progress",
        CapabilityCompleted => "capability.completed",
        CapabilityFailed => "capability.failed",
        CapabilityCancelled => "capability.cancelled",
        CapabilityTimedOut => "capability.timed_out",
        ContextStarted => "context.started",
        ContextCompleted => "context.completed",
        ContextFailed => "context.failed",
        ContextCancelled => "context.cancelled",
        ContextTimedOut => "context.timed_out",
        ChildStarted => "child.started",
        ChildWaiting => "child.waiting",
        ChildProgress => "child.progress",
        ChildCompleted => "child.completed",
        ChildFailed => "child.failed",
        ChildCancelled => "child.cancelled",
        ChildTimedOut => "child.timed_out",
        InteractionRequired => "interaction.required",
        InteractionResolved => "interaction.resolved",
        ApprovalRequired => "approval.required",
        ApprovalResolved => "approval.resolved",
        StreamLiveGap => "stream.live_gap"
    }
}

impl PublicRunEventType {
    pub const fn allowed_durability(self) -> &'static [EventDurability] {
        use EventDurability::{Durable, LiveOnly, Snapshot};
        match self {
            Self::RunSnapshot => &[Snapshot],
            Self::ModelDelta | Self::StreamLiveGap => &[LiveOnly],
            Self::CapabilityProgress | Self::ChildProgress => &[Durable, LiveOnly],
            _ => &[Durable],
        }
    }

    pub const fn durable_source_kind(self) -> Option<PublicRunEventSourceKind> {
        use PublicRunEventSourceKind::{
            ApprovalTask, CapabilityInvocation, ChildRunLink, ContextQuery, Interaction, ModelTurn,
            NodeExecution, Run, RunControl, SkillActivation,
        };
        match self {
            Self::RunSnapshot | Self::ModelDelta | Self::StreamLiveGap => None,
            Self::RunPaused | Self::RunResumed => Some(RunControl),
            Self::RunQueued
            | Self::RunStarted
            | Self::RunWaiting
            | Self::RunCancelling
            | Self::RunCompleted
            | Self::RunFailed
            | Self::RunCancelled
            | Self::RunTimedOut => Some(Run),
            Self::NodeStarted
            | Self::NodeCompleted
            | Self::NodeFailed
            | Self::NodeCancelled
            | Self::NodeTimedOut => Some(NodeExecution),
            Self::SkillSelected | Self::SkillActivated | Self::SkillRejected => {
                Some(SkillActivation)
            }
            Self::ModelStarted
            | Self::ModelToolIntent
            | Self::ModelCompleted
            | Self::ModelFailed
            | Self::ModelCancelled
            | Self::ModelTimedOut => Some(ModelTurn),
            Self::CapabilityStarted
            | Self::CapabilityWaiting
            | Self::CapabilityInputRequired
            | Self::CapabilityProgress
            | Self::CapabilityCompleted
            | Self::CapabilityFailed
            | Self::CapabilityCancelled
            | Self::CapabilityTimedOut => Some(CapabilityInvocation),
            Self::ContextStarted
            | Self::ContextCompleted
            | Self::ContextFailed
            | Self::ContextCancelled
            | Self::ContextTimedOut => Some(ContextQuery),
            Self::ChildStarted
            | Self::ChildWaiting
            | Self::ChildProgress
            | Self::ChildCompleted
            | Self::ChildFailed
            | Self::ChildCancelled
            | Self::ChildTimedOut => Some(ChildRunLink),
            Self::InteractionRequired | Self::InteractionResolved => Some(Interaction),
            Self::ApprovalRequired | Self::ApprovalResolved => Some(ApprovalTask),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventEnvelopeError {
    DurabilityNotAllowed,
    InvalidIdentityShape,
    SnapshotTypeRequired,
}

impl fmt::Display for EventEnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::DurabilityNotAllowed => "event type does not allow the selected durability",
            Self::InvalidIdentityShape => "event identity fields do not match durability",
            Self::SnapshotTypeRequired => "snapshot durability is reserved for run.snapshot",
        })
    }
}

impl Error for EventEnvelopeError {}

pub fn validate_public_event_envelope(
    event_type: PublicRunEventType,
    durability: EventDurability,
    event_id_present: bool,
    sequence_present: bool,
    cursor_present: bool,
) -> Result<(), EventEnvelopeError> {
    if !event_type.allowed_durability().contains(&durability) {
        return Err(EventEnvelopeError::DurabilityNotAllowed);
    }
    match durability {
        EventDurability::Snapshot => {
            if event_type != PublicRunEventType::RunSnapshot {
                return Err(EventEnvelopeError::SnapshotTypeRequired);
            }
            if event_id_present || sequence_present || !cursor_present {
                return Err(EventEnvelopeError::InvalidIdentityShape);
            }
        }
        EventDurability::Durable => {
            if !event_id_present || !sequence_present || !cursor_present {
                return Err(EventEnvelopeError::InvalidIdentityShape);
            }
        }
        EventDurability::LiveOnly => {
            if event_id_present || sequence_present || cursor_present {
                return Err(EventEnvelopeError::InvalidIdentityShape);
            }
        }
    }
    Ok(())
}

pub fn is_valid_declared_failure_code(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_lowercase() || value.len() > 64 {
        return false;
    }
    chars.all(|character| {
        character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
    }) && !PlatformFailureCode::ALL
        .iter()
        .any(|code| code.as_str() == value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_isolation_security_rank_is_explicit_and_strict() {
        assert!(
            SandboxIsolationClass::Wasm.security_rank()
                < SandboxIsolationClass::SandboxedContainer.security_rank()
        );
        assert!(
            SandboxIsolationClass::SandboxedContainer.security_rank()
                < SandboxIsolationClass::MicroVm.security_rank()
        );
    }

    #[test]
    fn permission_registry_is_unique_and_rejects_wildcards() {
        let values = Permission::ALL
            .iter()
            .map(|item| item.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(values.len(), Permission::ALL.len());
        assert!(!values.iter().any(|permission| permission.contains('*')));
        assert!("secret.read".parse::<Permission>().is_err());
        assert!("runtime.run".parse::<Permission>().is_err());
    }

    #[test]
    fn every_policy_reference_role_has_one_expected_kind() {
        let roles = PolicyReferenceRole::ALL
            .iter()
            .map(|role| role.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(roles.len(), PolicyReferenceRole::ALL.len());
        assert!(PolicyReferenceRole::ALL
            .iter()
            .all(|role| PolicyKind::ALL.contains(&role.expected_kind())));
    }

    #[test]
    fn risk_and_classification_orders_only_tighten() {
        assert!(Effect::Irreversible.risk_rank() > Effect::ReadOnly.risk_rank());
        assert_eq!(
            DataClassification::Internal.join(DataClassification::Restricted),
            DataClassification::Restricted
        );
    }

    #[test]
    fn declared_codes_are_bounded_and_cannot_shadow_platform_codes() {
        assert!(is_valid_declared_failure_code("inventory_empty"));
        assert!(!is_valid_declared_failure_code("InventoryEmpty"));
        assert!(!is_valid_declared_failure_code("deadline_exceeded"));
        assert!(!is_valid_declared_failure_code(&"x".repeat(65)));
    }

    #[test]
    fn cursor_purposes_are_not_interchangeable() {
        assert!(require_cursor_purpose(CursorPurpose::List, CursorPurpose::List).is_ok());
        assert!(require_cursor_purpose(CursorPurpose::List, CursorPurpose::RunEvent).is_err());
    }

    #[test]
    fn lock_rank_registry_is_strictly_ordered() {
        assert!(LockRank::ALL
            .windows(2)
            .all(|pair| pair[0].ordinal() < pair[1].ordinal()));
    }

    #[test]
    fn work_class_and_quota_registries_are_unique_and_closed() {
        let work_classes = WorkClass::ALL
            .iter()
            .map(|value| value.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(work_classes.len(), WorkClass::ALL.len());

        let dimensions = QuotaDimension::ALL
            .iter()
            .map(|value| value.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(dimensions.len(), QuotaDimension::ALL.len());
        assert!(QuotaDimension::ALL
            .iter()
            .all(|dimension| dimension.as_str().contains('.')));
        assert_eq!(
            QuotaDimension::ArtifactLogicalBytes.accounting_mode(),
            QuotaAccountingMode::Reclaimable
        );
        assert_eq!(
            QuotaDimension::ModelCostMicrounits.unit(),
            LimitUnit::CurrencyMicrounits
        );
    }

    #[test]
    fn artifact_and_management_operation_registries_are_unique_and_closed() {
        fn unique<T>(values: &[T], wire: impl Fn(&T) -> &'static str) -> bool {
            values
                .iter()
                .map(wire)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                == values.len()
        }

        assert!(unique(ArtifactPurpose::ALL, |value| value.as_str()));
        assert!(unique(ArtifactReferenceKind::ALL, |value| value.as_str()));
        assert!(unique(ArtifactGrantOperation::ALL, |value| value.as_str()));
        assert!(unique(ArtifactWorkloadAudience::ALL, |value| value.as_str()));
        assert!(unique(BlobIntegrityState::ALL, |value| value.as_str()));
        assert!(unique(ManagementOperationKind::ALL, |value| value.as_str()));
        assert!("artifact_scan".parse::<ManagementOperationKind>().is_err());
        assert!("read".parse::<ArtifactGrantOperation>().is_err());
    }
}
