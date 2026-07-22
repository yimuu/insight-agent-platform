//! Compatibility facade for durable repository models.

pub use insight_durable::{
    CommitReceipt, CreateRunCommand, DurableResponseSnapshot, PlanInstallOutcome,
    PlanPublicationOutcome, PublicEventIntent, PublicRunAttachment, PublicationHead,
    PublicationOrigin, PublishVersionedPlanCommand, ResponseTerminalKind, ResponseUsageStatus,
    RunProjection, RunTransitionCommand, VersionedPlan, VersionedPlanCatalog,
};
