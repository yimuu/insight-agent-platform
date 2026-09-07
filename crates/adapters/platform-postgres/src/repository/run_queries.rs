//! PostgreSQL run queries. Shared locks and atomicity remain in this adapter.
use super::*;

impl PgRepository {
    pub async fn read_run_definition_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        run_id: &ResourceId,
    ) -> Result<insight_platform_orchestrator::store::RunDefinitionRecord, RepositoryError> {
        if run_id.kind() != ResourceKind::Run {
            return Err(RepositoryError::NotFound("run"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::RuntimeRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        let run = load_run(&mut transaction, tenant_id, run_id).await?;
        let row = sqlx::query("SELECT resource_id,bindings_digest,payload_schema_version,bindings FROM insight_platform.deployments WHERE tenant_id=$1 AND deployment_id=$2")
            .bind(tenant_id.to_string()).bind(run.bindings.agent.deployment_id.to_string())
            .fetch_optional(&mut *transaction).await?.ok_or(RepositoryError::NotFound("Run definition"))?;
        let payload = payload_from_row(
            &row,
            "payload_schema_version",
            "bindings",
            "bindings_digest",
        )?;
        let DeploymentClosure::Agent(closure) = decode_deployment_closure(&payload)? else {
            return Err(RepositoryError::CorruptRow(
                "Run definition is not Agent".into(),
            ));
        };
        let agent_id = ResourceId::parse_expected(
            &row.try_get::<String, _>("resource_id")?,
            ResourceKind::Agent,
        )
        .map_err(|_| RepositoryError::CorruptRow("Run Agent identity is invalid".into()))?;
        if payload.digest != run.bindings.agent.deployment_digest.as_str()
            || closure.interface != run.bindings.agent_interface
            || closure.plan != run.bindings.plan
        {
            return Err(RepositoryError::CorruptRow(
                "Run definition differs from frozen bindings".into(),
            ));
        }
        let result = insight_platform_orchestrator::store::RunDefinitionRecord {
            run_id: run_id.clone(),
            agent_id,
            agent_deployment: run.bindings.agent,
            agent_interface: run.bindings.agent_interface,
            plan: run.bindings.plan,
        };
        transaction.commit().await?;
        Ok(result)
    }

    pub async fn read_run_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        run_id: &ResourceId,
    ) -> Result<RunRecord, RepositoryError> {
        if run_id.kind() != ResourceKind::Run {
            return Err(RepositoryError::NotFound("run"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::RuntimeRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        let run = load_run(&mut transaction, tenant_id, run_id).await?;
        transaction.commit().await?;
        Ok(run)
    }

    pub async fn resolve_signal_wake_target_for_principal(
        &self,
        request: &ResolveOrchestrationSignalTarget,
    ) -> Result<OrchestrationSignalWakeTarget, RepositoryError> {
        if request.tenant_id.kind() != ResourceKind::Tenant
            || request.run_id.kind() != ResourceKind::Run
            || !valid_orchestration_signal_key(&request.signal_key)
        {
            return Err(RepositoryError::InvalidInput(
                "Run signal target is invalid".to_owned(),
            ));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            &request.tenant_id,
            &request.principal_id,
            request.principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::AgentRun) {
            return Err(RepositoryError::PermissionDenied);
        }
        load_run(&mut transaction, &request.tenant_id, &request.run_id).await?;
        let operation = format!("orchestration.job.wake.{}", WakeSource::Signal.as_str());
        let replay = sqlx::query(
            r#"
            SELECT receipt.request_digest, receipt.state, job.job_id,
                   receipt.payload_schema_version, receipt.payload, receipt.payload_digest
            FROM insight_platform.receipts AS receipt
            JOIN insight_platform.jobs AS job
              ON job.tenant_id = receipt.tenant_id AND job.job_id = receipt.scope_id
            WHERE receipt.tenant_id = $1
              AND receipt.receipt_kind = 'job_commit'
              AND receipt.scope_kind = 'job'
              AND receipt.scope_id = receipt.dedupe_owner_id
              AND receipt.operation = $2
              AND receipt.idempotency_key_digest = $3
              AND job.run_id = $4
            ORDER BY receipt.created_at
            LIMIT 2
            "#,
        )
        .bind(request.tenant_id.to_string())
        .bind(&operation)
        .bind(request.idempotency_key_digest.to_string())
        .bind(request.run_id.to_string())
        .fetch_all(&mut *transaction)
        .await?;
        if replay.len() > 1 {
            return Err(RepositoryError::CorruptRow(
                "Run signal idempotency key resolved to multiple Jobs".to_owned(),
            ));
        }
        if let Some(row) = replay.into_iter().next() {
            if row.try_get::<String, _>("request_digest")? != request.request_digest.to_string() {
                return Err(RepositoryError::IdempotencyConflict);
            }
            if row.try_get::<String, _>("state")? != "succeeded" {
                return Err(RepositoryError::Conflict("Run signal receipt"));
            }
            let stored =
                payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
            let evidence: OrchestrationWakeReceiptPayload =
                decode_typed_payload(&stored, "orchestration wake Receipt")?;
            let job_id = row.try_get::<String, _>("job_id")?;
            if evidence.job_id.to_string() != job_id
                || evidence.job_id.kind() != ResourceKind::Job
                || evidence.expected_job_version <= 0
                || evidence.expected_wake_generation == 0
                || evidence.source != WakeSource::Signal.as_str()
                || evidence.signal_key.as_deref() != Some(request.signal_key.as_str())
            {
                return Err(RepositoryError::CorruptRow(
                    "Run signal Receipt wake evidence is invalid".to_owned(),
                ));
            }
            let target = OrchestrationSignalWakeTarget {
                job_id: evidence.job_id,
                job_version: evidence.expected_job_version,
                wake_generation: evidence.expected_wake_generation,
            };
            transaction.commit().await?;
            return Ok(target);
        }
        let rows = sqlx::query(
            r#"
            SELECT job.job_id, job.version, job.wake_generation
            FROM insight_platform.jobs AS job
            JOIN insight_platform.run_nodes AS node
              ON node.tenant_id = job.tenant_id AND node.node_id = job.node_id
            WHERE job.tenant_id = $1 AND job.run_id = $2
              AND job.work_class = 'orchestration'
              AND job.owner_kind = 'node_execution'
              AND job.state = 'waiting' AND job.wake_kind = 'signal'
              AND job.worker_id IS NULL AND job.terminal_at IS NULL
              AND node.record_kind = 'node_execution'
              AND node.node_kind = 'signal_wait' AND node.state = 'waiting'
              AND node.terminal_at IS NULL
              AND node.payload #>> '{signal_key}' = $3
            ORDER BY job.job_id
            LIMIT 2
            "#,
        )
        .bind(request.tenant_id.to_string())
        .bind(request.run_id.to_string())
        .bind(&request.signal_key)
        .fetch_all(&mut *transaction)
        .await?;
        if rows.len() != 1 {
            return Err(if rows.is_empty() {
                RepositoryError::NotFound("pending Run signal")
            } else {
                RepositoryError::Conflict("ambiguous pending Run signal")
            });
        }
        let target = orchestration_signal_target_from_row(
            rows.into_iter()
                .next()
                .expect("one Signal target exists after exact cardinality check"),
        )?;
        transaction.commit().await?;
        Ok(target)
    }

    pub async fn read_public_run_events_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        run_id: &ResourceId,
        position: PublicRunReadPosition,
        limit: u16,
    ) -> Result<PublicRunEventPage, RepositoryError> {
        if run_id.kind() != ResourceKind::Run || limit == 0 || limit > 128 {
            return Err(RepositoryError::InvalidInput(
                "public Run event query is invalid".to_owned(),
            ));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::RuntimeRead) {
            return Err(RepositoryError::PermissionDenied);
        }
        let run = load_run(&mut transaction, tenant_id, run_id).await?;
        let replay_floor = u64::try_from(run.public_replay_floor)
            .map_err(|_| RepositoryError::CorruptRow("negative replay floor".into()))?;
        let high_water_sequence = u64::try_from(run.public_sequence)
            .map_err(|_| RepositoryError::CorruptRow("negative public sequence".into()))?;
        let after_sequence = position
            .resolve(replay_floor, high_water_sequence)
            .map_err(|error| match error {
                PublicReplayError::HistoryGap { replay_floor } => {
                    RepositoryError::PublicHistoryGap { replay_floor }
                }
                PublicReplayError::CursorAhead => RepositoryError::InvalidInput(
                    "public Run event cursor is ahead of authority".into(),
                ),
                PublicReplayError::CorruptWatermarks => {
                    RepositoryError::CorruptRow("public Run watermarks are invalid".into())
                }
            })?;
        let rows = sqlx::query(
            r#"
            SELECT event_id, aggregate_kind, aggregate_id, aggregate_version, trace_id,
                   public_sequence, event_type, payload, occurred_at
            FROM insight_platform.events
            WHERE tenant_id = $1 AND run_id = $2 AND visibility = 'public'
              AND public_sequence > $3
            ORDER BY public_sequence
            LIMIT $4
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(run_id.to_string())
        .bind(after_sequence as i64)
        .bind(i64::from(limit))
        .fetch_all(&mut *transaction)
        .await?;
        let events = rows
            .into_iter()
            .map(public_run_event_from_row)
            .collect::<Result<Vec<_>, _>>()?;
        transaction.commit().await?;
        Ok(PublicRunEventPage {
            schema_version: PUBLIC_RUN_EVENT_PAGE_VERSION,
            events,
            replay_floor,
            high_water_sequence,
            started_after_sequence: after_sequence,
            truncated: matches!(position, PublicRunReadPosition::Initial) && replay_floor > 0,
        })
    }

    pub async fn read_run_result_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        run_id: &ResourceId,
    ) -> Result<RunResultRecord, RepositoryError> {
        self.read_public_run_value(tenant_id, principal_id, principal_kind, run_id, None)
            .await
    }

    pub async fn read_run_value_content_for_principal(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        run_id: &ResourceId,
        value_id: &ResourceId,
    ) -> Result<RunResultRecord, RepositoryError> {
        self.read_public_run_value(
            tenant_id,
            principal_id,
            principal_kind,
            run_id,
            Some(value_id),
        )
        .await
    }

    async fn read_public_run_value(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        run_id: &ResourceId,
        selected_value_id: Option<&ResourceId>,
    ) -> Result<RunResultRecord, RepositoryError> {
        if run_id.kind() != ResourceKind::Run
            || selected_value_id.is_some_and(|id| id.kind() != ResourceKind::RunValue)
        {
            return Err(RepositoryError::NotFound("run result"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::RuntimeRead)
            || !insight_platform_contracts::permits_content_disclosure(
                &principal,
                insight_platform_contracts::ExecutionAuthorizationPurpose::ContentDisclosure,
            )
        {
            return Err(RepositoryError::PermissionDenied);
        }
        let run = load_run(&mut transaction, tenant_id, run_id).await?;
        let state: RunState = run
            .state
            .parse::<RunState>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        if selected_value_id.is_none()
            && !matches!(
                state,
                RunState::Succeeded | RunState::Failed | RunState::Cancelled | RunState::TimedOut
            )
        {
            return Err(RepositoryError::Conflict("run result is not terminal"));
        }
        let output_value_id = selected_value_id
            .map(ToString::to_string)
            .or(run.output_value_id)
            .ok_or(RepositoryError::NotFound("run output value"))?;
        let row = sqlx::query(
            r#"
            SELECT value.value_id, value.node_id, value.value_kind,
                   value.classification, value.schema_digest,
                   value.content_digest, value.inline_value, value.artifact_id,
                   artifact.state AS artifact_state,
                   artifact.verified_media_type, blob.size_bytes
            FROM insight_platform.run_values AS value
            LEFT JOIN insight_platform.artifacts AS artifact
              ON artifact.tenant_id = value.tenant_id AND artifact.artifact_id = value.artifact_id
            LEFT JOIN insight_platform.artifact_blobs AS blob
              ON blob.tenant_id = artifact.tenant_id AND blob.blob_id = artifact.blob_id
            WHERE value.tenant_id = $1 AND value.run_id = $2 AND value.value_id = $3
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(run_id.to_string())
        .bind(&output_value_id)
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("run output value"))?;
        let value_id: ResourceId = row
            .try_get::<String, _>("value_id")?
            .parse::<ResourceId>()
            .map_err(|_| RepositoryError::CorruptRow("RunValue ID is invalid".to_owned()))?;
        let classification: DataClassification = row
            .try_get::<String, _>("classification")?
            .parse::<DataClassification>()
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let schema_digest: Sha256Digest = row
            .try_get::<String, _>("schema_digest")?
            .parse()
            .map_err(|_| {
                RepositoryError::CorruptRow("RunValue schema digest is invalid".to_owned())
            })?;
        let content_digest: Sha256Digest = row
            .try_get::<String, _>("content_digest")?
            .parse()
            .map_err(|_| {
                RepositoryError::CorruptRow("RunValue content digest is invalid".to_owned())
            })?;
        let inline: Option<Value> = row.try_get("inline_value")?;
        let artifact_id: Option<String> = row.try_get("artifact_id")?;
        let value = match (inline, artifact_id) {
            (Some(value), None) => {
                let observed: Sha256Digest = canonical_digest(&value)
                    .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?
                    .parse()
                    .map_err(|_| {
                        RepositoryError::CorruptRow("RunValue digest is invalid".to_owned())
                    })?;
                if observed != content_digest {
                    return Err(RepositoryError::CorruptRow(
                        "RunValue content digest differs".to_owned(),
                    ));
                }
                ValueRef::Inline { value }
            }
            (None, Some(artifact_id))
                if row
                    .try_get::<Option<String>, _>("artifact_state")?
                    .as_deref()
                    == Some("ready") =>
            {
                let artifact = ArtifactRef::new(
                    artifact_id.parse().map_err(|_| {
                        RepositoryError::CorruptRow("Artifact ID is invalid".to_owned())
                    })?,
                    content_digest.clone(),
                    u64::try_from(row.try_get::<i64, _>("size_bytes")?).map_err(|_| {
                        RepositoryError::CorruptRow("Artifact size is invalid".to_owned())
                    })?,
                    row.try_get::<String, _>("verified_media_type")?,
                    classification,
                    None,
                )
                .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
                ValueRef::Artifact { artifact }
            }
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "RunValue storage shape is invalid".to_owned(),
                ))
            }
        };
        transaction.commit().await?;
        Ok(RunResultRecord {
            run_id: run_id.clone(),
            value_id,
            classification,
            schema_digest,
            content_digest,
            value,
        })
    }

    pub async fn resolve_root_run_target(
        &self,
        tenant_id: &ResourceId,
        agent_id: &ResourceId,
    ) -> Result<ResolvedRootRunTarget, RepositoryError> {
        if tenant_id.kind() != ResourceKind::Tenant || agent_id.kind() != ResourceKind::Agent {
            return Err(RepositoryError::NotFound("active agent deployment"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let row = sqlx::query(
            r#"
            SELECT deployment.tenant_id, deployment.deployment_id, deployment.resource_id,
                   deployment.resource_version_id, deployment.environment,
                   deployment.bindings_digest, deployment.payload_schema_version,
                   deployment.bindings, deployment.created_by, deployment.created_at
            FROM insight_platform.resources AS resource
            JOIN insight_platform.deployments AS deployment
              ON deployment.tenant_id = resource.tenant_id
             AND deployment.resource_id = resource.resource_id
             AND deployment.deployment_id = resource.active_deployment_id
            WHERE resource.tenant_id = $1 AND resource.resource_id = $2
              AND resource.resource_kind = 'agent'
              AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(agent_id.to_string())
        .fetch_optional(&mut *transaction)
        .await?
        .ok_or(RepositoryError::NotFound("active agent deployment"))?;
        let deployment = deployment_from_row(row)?;
        let closure = match decode_deployment_closure(&deployment.bindings)? {
            DeploymentClosure::Agent(closure) => closure,
            _ => {
                return Err(RepositoryError::CorruptRow(
                    "active Agent Deployment has a non-Agent closure".to_owned(),
                ));
            }
        };
        let deployment_id: ResourceId = deployment.deployment_id.parse().map_err(|_| {
            RepositoryError::CorruptRow("Agent Deployment ID is invalid".to_owned())
        })?;
        let deployment_digest: Sha256Digest = deployment.bindings.digest.parse().map_err(|_| {
            RepositoryError::CorruptRow("Agent Deployment digest is invalid".to_owned())
        })?;
        let agent = ExactDeploymentRef::new(deployment_id, deployment_digest)
            .map_err(|failure| RepositoryError::CorruptRow(failure.to_string()))?;
        let mut context_dataset_views = Vec::new();
        for binding in closure.slots.iter().filter_map(|slot| match &slot.target {
            FrozenSlotTarget::Context { binding } => Some(binding.as_ref()),
            _ => None,
        }) {
            let insight_platform_contracts::ContextConsistencyPolicy::PinAtRunAdmission {
                dataset_id,
            } = &binding.consistency
            else {
                continue;
            };
            let generation = sqlx::query(
                r#"
                SELECT version.resource_version_id, version.content_digest
                FROM insight_platform.resources AS resource
                JOIN insight_platform.resource_versions AS version
                  ON version.tenant_id = resource.tenant_id
                 AND version.resource_id = resource.resource_id
                 AND version.resource_version_id = resource.active_version_id
                WHERE resource.tenant_id = $1 AND resource.resource_id = $2
                  AND resource.resource_kind = 'context_dataset'
                  AND resource.lifecycle_state = 'active' AND resource.gate_state = 'enabled'
                  AND version.resource_version_kind = 'dataset_generation'
                "#,
            )
            .bind(tenant_id.to_string())
            .bind(dataset_id.to_string())
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or(RepositoryError::NotFound(
                "active Context Dataset Generation",
            ))?;
            context_dataset_views.push(insight_platform_contracts::RunContextDatasetView {
                context_binding_id: binding.context_binding_id.clone(),
                context_binding_digest: binding.binding_digest.clone(),
                generation: ExactDatasetGenerationRef {
                    dataset_id: dataset_id.clone(),
                    generation_id: generation
                        .try_get::<String, _>("resource_version_id")?
                        .parse()
                        .map_err(|_| {
                            RepositoryError::CorruptRow(
                                "Dataset Generation ID is invalid".to_owned(),
                            )
                        })?,
                    generation_digest: generation
                        .try_get::<String, _>("content_digest")?
                        .parse()
                        .map_err(|_| {
                        RepositoryError::CorruptRow(
                            "Dataset Generation digest is invalid".to_owned(),
                        )
                    })?,
                },
            });
        }
        transaction.commit().await?;
        Ok(ResolvedRootRunTarget {
            agent,
            closure,
            context_dataset_views,
        })
    }

    pub async fn read_root_run_admission_replay(
        &self,
        tenant_id: &ResourceId,
        principal_id: &ResourceId,
        principal_kind: PrincipalKind,
        agent_id: &ResourceId,
        idempotency_key_digest: &Sha256Digest,
        request_digest: &Sha256Digest,
    ) -> Result<Option<RunRecord>, RepositoryError> {
        if agent_id.kind() != ResourceKind::Agent {
            return Err(RepositoryError::NotFound("run admission receipt"));
        }
        let mut transaction = begin_read_only_repeatable(&self.pool).await?;
        let principal = load_current_principal_snapshot(
            &mut transaction,
            tenant_id,
            principal_id,
            principal_kind,
        )
        .await?;
        if !principal.permissions.contains(Permission::AgentRun) {
            return Err(RepositoryError::PermissionDenied);
        }
        let row = sqlx::query(
            r#"
            SELECT request_digest, state, payload_schema_version, payload, payload_digest
            FROM insight_platform.receipts
            WHERE tenant_id = $1 AND receipt_kind = 'command'
              AND scope_kind = 'run_admission' AND scope_id = $2
              AND dedupe_owner_id = $3 AND operation = 'run.admit'
              AND idempotency_key_digest = $4
            "#,
        )
        .bind(tenant_id.to_string())
        .bind(agent_id.to_string())
        .bind(principal_id.to_string())
        .bind(idempotency_key_digest.to_string())
        .fetch_optional(&mut *transaction)
        .await?;
        let Some(row) = row else {
            transaction.commit().await?;
            return Ok(None);
        };
        if row.try_get::<String, _>("request_digest")? != request_digest.to_string() {
            return Err(RepositoryError::IdempotencyConflict);
        }
        if row.try_get::<String, _>("state")? != "succeeded" {
            return Err(RepositoryError::Conflict("run admission receipt"));
        }
        let payload =
            payload_from_row(&row, "payload_schema_version", "payload", "payload_digest")?;
        let result: RunAdmissionReceiptResult =
            decode_versioned_payload(&payload, "run admission Receipt result")?;
        validate_run_admission_receipt_result(&result, tenant_id)?;
        transaction.commit().await?;
        Ok(Some(result.run))
    }
}
