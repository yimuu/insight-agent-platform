use serde::{Deserialize, Serialize};
use std::{error::Error, fmt};

pub const HARD_LIMIT_PROFILE_VERSION: u32 = 4;
pub const Q1_SANDBOX_RUNTIME_BUNDLE_BYTES: u64 = 33_554_432;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HardLimitProfile {
    pub profile_id: String,
    pub profile_version: u32,
    pub api: ApiLimits,
    pub registry_plan: RegistryPlanLimits,
    pub run_scheduler: RunSchedulerLimits,
    pub model_context_mcp: ModelContextMcpLimits,
    pub capability_sandbox: CapabilitySandboxLimits,
    pub artifact: ArtifactLimits,
    pub durable_quota: DurableQuotaLimits,
    pub control_data: ControlDataLimits,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limit {
    pub unit: LimitUnit,
    pub hard_max: u64,
    pub q1_default: u64,
    pub overflow_outcome: OverflowOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LimitUnit {
    Bytes,
    Count,
    Depth,
    Items,
    Milliseconds,
    Seconds,
    Connections,
    RequestsPerSecond,
    Tokens,
    Millicpu,
    Mebibytes,
    CurrencyMicrounits,
    Pids,
    Files,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OverflowOutcome {
    InvalidRequest,
    QuotaExceeded,
    RateLimited,
    ContentRejected,
    BudgetExhausted,
    DeadlineExceeded,
    DurableQueue,
    DropLiveThenClose,
    PromoteToArtifact,
    TemporarilyUnavailable,
}

macro_rules! limit_family {
    ($name:ident { $($field:ident),+ $(,)? }) => {
        #[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct $name {
            $(pub $field: Limit),+
        }

        impl $name {
            fn visit(&self, family: &str, visitor: &mut impl FnMut(&str, &Limit)) {
                $(visitor(concat!(stringify!($field)), &self.$field);)+
                let _ = family;
            }
        }
    };
}

limit_family! {
    ApiLimits {
        header_bytes,
        url_bytes,
        compressed_body_bytes,
        decoded_body_bytes,
        json_depth,
        json_properties,
        json_items,
        list_page_items,
        sse_event_bytes,
        sse_buffer_events,
        sse_connections_per_tenant
    }
}

limit_family! {
    RegistryPlanLimits {
        draft_bytes,
        package_bytes,
        schema_bytes,
        definitions,
        plan_nodes,
        plan_edges,
        branch_legs,
        map_items,
        loop_iterations,
        model_rounds,
        dependency_closure
    }
}

limit_family! {
    RunSchedulerLimits {
        active_runs_per_tenant,
        waiting_runs_per_tenant,
        descendants_per_run,
        ready_rows_per_run,
        inline_value_bytes,
        value_refs_per_run,
        claim_batch,
        attempts_per_work,
        lease_milliseconds,
        heartbeat_milliseconds,
        deferred_poll_base_milliseconds,
        deferred_poll_max_milliseconds,
        wake_contracts_per_run,
        interactions_per_run
    }
}

limit_family! {
    ModelContextMcpLimits {
        request_bytes,
        response_bytes,
        delta_bytes,
        tokens_per_turn,
        tool_calls_per_turn,
        context_candidates,
        context_items,
        context_pages,
        mcp_sessions_per_tenant,
        mcp_tasks_per_session,
        mcp_subscriptions_per_session
    }
}

limit_family! {
    CapabilitySandboxLimits {
        input_bytes,
        runtime_bundle_bytes,
        output_bytes,
        progress_events,
        queue_depth,
        cpu_millicores,
        memory_mebibytes,
        pids,
        files,
        io_bytes,
        wall_seconds,
        cleanup_seconds
    }
}

limit_family! {
    ArtifactLimits {
        single_bytes,
        tenant_total_bytes,
        multipart_parts,
        references_per_artifact,
        grants_per_artifact,
        scan_expansion_bytes,
        list_page_items,
        objects_per_operation,
        staging_seconds,
        retention_batch
    }
}

limit_family! {
    DurableQuotaLimits {
        agent_concurrent_runs,
        work_class_concurrent_operations,
        capability_concurrent_invocations,
        sandbox_concurrent_executions,
        sandbox_cpu_seconds,
        sandbox_memory_mebibytes,
        sandbox_output_bytes,
        model_tokens,
        model_cost_microunits,
        model_requests,
        context_queries,
        context_result_bytes,
        artifact_count,
        artifact_physical_bytes,
        artifact_staging_bytes,
        artifact_uploads,
        human_tasks_pending,
        human_task_retention_seconds
    }
}

limit_family! {
    ControlDataLimits {
        database_connections,
        transaction_milliseconds,
        outbox_batch,
        callback_batch,
        recovery_batch,
        recovery_shards,
        nats_payload_bytes,
        telemetry_buffer_events,
        metric_label_values,
        audit_batch
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LimitProfileError {
    pub field: String,
    pub message: &'static str,
}

impl fmt::Display for LimitProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid hard limit {}: {}",
            self.field, self.message
        )
    }
}

impl Error for LimitProfileError {}

impl HardLimitProfile {
    pub fn validate(&self) -> Result<(), LimitProfileError> {
        if self.profile_id != "q1-50" {
            return Err(LimitProfileError {
                field: "profile_id".to_owned(),
                message: "the initial profile ID must be q1-50",
            });
        }
        if self.profile_version != HARD_LIMIT_PROFILE_VERSION {
            return Err(LimitProfileError {
                field: "profile_version".to_owned(),
                message: "profile version does not match the current closed contract",
            });
        }
        if self.capability_sandbox.runtime_bundle_bytes
            != (Limit {
                unit: LimitUnit::Bytes,
                hard_max: crate::MAX_SANDBOX_RUNTIME_BUNDLE_BYTES,
                q1_default: Q1_SANDBOX_RUNTIME_BUNDLE_BYTES,
                overflow_outcome: OverflowOutcome::ContentRejected,
            })
        {
            return Err(LimitProfileError {
                field: "capability_sandbox.runtime_bundle_bytes".to_owned(),
                message: "runtime bundle limit does not match the current closed contract",
            });
        }

        let mut failure = None;
        {
            let mut inspect = |family: &str, name: &str, limit: &Limit| {
                if failure.is_some() {
                    return;
                }
                if limit.hard_max == 0 || limit.q1_default == 0 {
                    failure = Some(LimitProfileError {
                        field: format!("{family}.{name}"),
                        message: "hard maximum and Q1 default must be positive",
                    });
                } else if limit.q1_default > limit.hard_max {
                    failure = Some(LimitProfileError {
                        field: format!("{family}.{name}"),
                        message: "Q1 default cannot exceed hard maximum",
                    });
                }
            };
            self.api
                .visit("api", &mut |name, value| inspect("api", name, value));
            self.registry_plan
                .visit("registry_plan", &mut |name, value| {
                    inspect("registry_plan", name, value)
                });
            self.run_scheduler
                .visit("run_scheduler", &mut |name, value| {
                    inspect("run_scheduler", name, value)
                });
            self.model_context_mcp
                .visit("model_context_mcp", &mut |name, value| {
                    inspect("model_context_mcp", name, value)
                });
            self.capability_sandbox
                .visit("capability_sandbox", &mut |name, value| {
                    inspect("capability_sandbox", name, value)
                });
            self.artifact.visit("artifact", &mut |name, value| {
                inspect("artifact", name, value)
            });
            self.durable_quota
                .visit("durable_quota", &mut |name, value| {
                    inspect("durable_quota", name, value)
                });
            self.control_data.visit("control_data", &mut |name, value| {
                inspect("control_data", name, value)
            });
        }
        if failure.is_none()
            && (self.run_scheduler.deferred_poll_base_milliseconds.unit != LimitUnit::Milliseconds
                || self.run_scheduler.deferred_poll_max_milliseconds.unit
                    != LimitUnit::Milliseconds
                || self.run_scheduler.deferred_poll_base_milliseconds.hard_max
                    > self.run_scheduler.deferred_poll_max_milliseconds.hard_max
                || self
                    .run_scheduler
                    .deferred_poll_base_milliseconds
                    .q1_default
                    > self.run_scheduler.deferred_poll_max_milliseconds.q1_default)
        {
            failure = Some(LimitProfileError {
                field: "run_scheduler.deferred_poll_backoff".to_owned(),
                message:
                    "poll backoff limits must be millisecond values with base no greater than max",
            });
        }
        if failure.is_none()
            && (self.run_scheduler.lease_milliseconds.unit != LimitUnit::Milliseconds
                || self.run_scheduler.heartbeat_milliseconds.unit != LimitUnit::Milliseconds
                || self
                    .run_scheduler
                    .heartbeat_milliseconds
                    .hard_max
                    .saturating_mul(3)
                    >= self.run_scheduler.lease_milliseconds.hard_max
                || self
                    .run_scheduler
                    .heartbeat_milliseconds
                    .q1_default
                    .saturating_mul(3)
                    >= self.run_scheduler.lease_milliseconds.q1_default)
        {
            failure = Some(LimitProfileError {
                field: "run_scheduler.heartbeat_lease_ratio".to_owned(),
                message:
                    "heartbeat must be a millisecond value strictly below one third of the lease",
            });
        }
        if failure.is_none()
            && (self.control_data.recovery_batch.unit != LimitUnit::Items
                || self.control_data.recovery_shards.unit != LimitUnit::Count
                || self.control_data.recovery_shards.hard_max > u16::MAX.into())
        {
            failure = Some(LimitProfileError {
                field: "control_data.recovery_scan".to_owned(),
                message: "recovery batch must be items and shard count must fit u16",
            });
        }
        failure.map_or(Ok(()), Err)
    }
}

pub fn checked_in_hard_limit_profile() -> HardLimitProfile {
    serde_json::from_str(include_str!(
        "../../../contracts/platform-v1/limits/q1-50.json"
    ))
    .expect("checked-in HardLimitProfile is covered by contract tests")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::Path};

    fn q1_profile_bytes() -> Vec<u8> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(Path::parent)
            .unwrap();
        fs::read(root.join("contracts/platform-v1/limits/q1-50.json")).unwrap()
    }

    #[test]
    fn checked_in_q1_profile_is_closed_and_valid() {
        let bytes = q1_profile_bytes();
        let profile: HardLimitProfile = serde_json::from_slice(&bytes).unwrap();
        profile.validate().unwrap();
        assert_eq!(
            profile.capability_sandbox.runtime_bundle_bytes.hard_max,
            crate::MAX_SANDBOX_RUNTIME_BUNDLE_BYTES
        );
        assert_eq!(profile, checked_in_hard_limit_profile());
    }

    #[test]
    fn runtime_bundle_limit_and_profile_version_are_exact() {
        let bytes = q1_profile_bytes();
        let profile: HardLimitProfile = serde_json::from_slice(&bytes).unwrap();

        let mut old_version = profile.clone();
        old_version.profile_version = HARD_LIMIT_PROFILE_VERSION - 1;
        assert_eq!(old_version.validate().unwrap_err().field, "profile_version");

        let mutations = [
            Limit {
                unit: LimitUnit::Count,
                ..profile.capability_sandbox.runtime_bundle_bytes.clone()
            },
            Limit {
                hard_max: crate::MAX_SANDBOX_RUNTIME_BUNDLE_BYTES - 1,
                ..profile.capability_sandbox.runtime_bundle_bytes.clone()
            },
            Limit {
                q1_default: Q1_SANDBOX_RUNTIME_BUNDLE_BYTES - 1,
                ..profile.capability_sandbox.runtime_bundle_bytes.clone()
            },
            Limit {
                overflow_outcome: OverflowOutcome::QuotaExceeded,
                ..profile.capability_sandbox.runtime_bundle_bytes.clone()
            },
        ];
        for mutation in mutations {
            let mut changed = profile.clone();
            changed.capability_sandbox.runtime_bundle_bytes = mutation;
            assert_eq!(
                changed.validate().unwrap_err().field,
                "capability_sandbox.runtime_bundle_bytes"
            );
        }
    }

    #[test]
    fn deferred_poll_backoff_requires_an_ordered_millisecond_range() {
        let bytes = q1_profile_bytes();
        let mut profile: HardLimitProfile = serde_json::from_slice(&bytes).unwrap();
        profile
            .run_scheduler
            .deferred_poll_max_milliseconds
            .q1_default = profile
            .run_scheduler
            .deferred_poll_base_milliseconds
            .q1_default
            - 1;
        let failure = profile.validate().expect_err("reversed poll range");
        assert_eq!(failure.field, "run_scheduler.deferred_poll_backoff");
    }

    #[test]
    fn heartbeat_must_leave_strict_lease_expiry_margin() {
        let bytes = q1_profile_bytes();
        let mut profile: HardLimitProfile = serde_json::from_slice(&bytes).unwrap();
        profile.run_scheduler.heartbeat_milliseconds.q1_default =
            profile.run_scheduler.lease_milliseconds.q1_default / 3;
        let failure = profile.validate().expect_err("one-third heartbeat");
        assert_eq!(failure.field, "run_scheduler.heartbeat_lease_ratio");
    }

    #[test]
    fn recovery_scan_requires_item_batches_and_bounded_shards() {
        let bytes = q1_profile_bytes();
        let mut profile: HardLimitProfile = serde_json::from_slice(&bytes).unwrap();
        profile.control_data.recovery_shards.hard_max = u64::from(u16::MAX) + 1;
        let failure = profile
            .validate()
            .expect_err("oversized recovery shard count");
        assert_eq!(failure.field, "control_data.recovery_scan");
    }
}
