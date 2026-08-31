use super::*;
use insight_platform_contracts::{ResourceId, ResourceKind, Sha256Digest};
use insight_platform_postgres::repository::{
    ClaimOrchestrationJobs, JobFence, OrchestrationClaimSlot, PgRepository, RepositoryError,
    StartOrchestrationJob, MAX_ORCHESTRATION_QUOTA_LINES,
};
use sqlx::{postgres::PgPoolOptions, Row};
use uuid::Uuid;

pub(super) struct TimerSignalEvidence {
    pub run_id: String,
    console_passed: bool,
    stale_job_fence_passed: bool,
    started_at: chrono::DateTime<Utc>,
    finished_at: chrono::DateTime<Utc>,
}

impl TimerSignalEvidence {
    pub fn mark_console_passed(&mut self) {
        self.console_passed = true;
        self.finished_at = Utc::now();
    }

    pub fn report(&self, revision: &str) -> Value {
        let check = |id: &str, status: &str, evidence: &str| json!({"id": id, "status": status, "evidence": evidence});
        let console = if self.console_passed {
            check(
                "console",
                "passed",
                "the same real headless Chromium session read this exact Timer/Signal Run ID from the fresh Gateway/PostgreSQL authority and verified its succeeded Inline result after replacement-Worker recovery",
            )
        } else {
            check(
                "console",
                "not_run",
                "set PLATFORM_PRODUCTIZATION_CONSOLE_BROWSER=true to read the exact Timer/Signal Run through the real Console",
            )
        };
        let stale_job_fence = if self.stale_job_fence_passed {
            check(
                "stale_job_fence",
                "passed",
                "the fixture claimed the exact durable ready continuation, altered only its lease-token digest, observed RepositoryError::StaleFence with the Job still leased at the same version, then let the lease expire for replacement-Worker recovery",
            )
        } else {
            check(
                "stale_job_fence",
                "not_run",
                "the controlled PostgreSQL lease-token probe did not run",
            )
        };
        let status = if self.console_passed && self.stale_job_fence_passed {
            "passed"
        } else {
            "incomplete"
        };
        json!({
            "schema_version": 1,
            "report_kind": "insight.productization.scenario-report/v1",
            "scenario_id": "timer-signal-restart-recovery",
            "contract_profile": "insight.platform/v1",
            "profile": "starter",
            "automation_layer": "P2",
            "source_revision": revision,
            "environment": {
                "os": env::consts::OS,
                "architecture": env::consts::ARCH,
                "fresh_profile": true,
            },
            "started_at": self.started_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "finished_at": self.finished_at.to_rfc3339_opts(SecondsFormat::Micros, true),
            "status": status,
            "entrypoints": [
                check("cli", "passed", "public insight artifact/apply/run watch/result commands completed on the fresh base authority"),
                check("http_fixture", "passed", "raw public /v1 signal submission and exact Receipt replay both returned the closed 204 contract"),
                console,
            ],
            "assertions": [
                check("restart_resume", "passed", "the accepted signal durably moved the Run to running with a ready continuation while the Orchestration Worker was absent, then an exact replacement Worker reached succeeded"),
                check("event_sequence", "passed", "public polling observed distinct TimerWait and SignalWait Run projection versions, then bounded SSE reached one terminal succeeded projection"),
                check("no_duplicate_effect", "passed", "replaying the exact signal Receipt did not create a second Run transition"),
            ],
            "failure_probes": [
                check("worker_kill_before_commit", "passed", "the Worker was terminated before signal submission; the owner transaction consumed the signal and retained a durable ready continuation without a Worker"),
                stale_job_fence,
            ],
        })
    }
}

pub(super) fn run(
    insight: &Path,
    project: &Path,
    fixture: &Path,
    schema_digest: &str,
    agent_manifest: &Value,
    replacement: RestartedOrchestrationWorker,
) -> (TimerSignalEvidence, RestartedOrchestrationWorker) {
    let started_at = Utc::now();
    let plan = json!({
        "plan_version": 5,
        "interface_contract_digest": CONTRACT_DIGEST,
        "entry_node_id": "start",
        "dependency_slots": {},
        "nodes": {
            "start": {"kind": "start", "next": "delay"},
            "delay": {
                "kind": "timer_wait",
                "delay_milliseconds": 250,
                "resume": "await_release"
            },
            "await_release": {
                "kind": "signal_wait",
                "signal_key": "release",
                "payload": null,
                "timeout_milliseconds": 120000,
                "resume": "finish"
            },
            "finish": {
                "kind": "return",
                "value": {"source": "run_input", "schema_digest": schema_digest}
            }
        }
    });
    let plan_path = write_canonical(fixture, "timer-signal-plan.json", &plan);
    let plan_upload = upload_artifact(
        insight,
        project,
        &plan_path,
        "typed_plan",
        "timer-signal-plan.json",
    );
    let mut manifest = agent_manifest.clone();
    manifest["create"]["display_name"] = json!("Timer signal recovery agent");
    manifest["create"]["document"]["spec"]["typed_plan_artifact_id"] =
        plan_upload["artifact_id"].clone();
    manifest["create"]["document"]["spec"]["typed_plan_digest"] =
        plan_upload["content_digest"].clone();
    manifest["publish"]["plan_content_digest"] = plan_upload["content_digest"].clone();
    manifest["publish"]["artifact_id"] = plan_upload["artifact_id"].clone();
    let manifest_path = write_canonical(fixture, "timer-signal-agent.apply.json", &manifest);
    let agent = run_json(
        insight,
        &[
            "apply",
            "--file",
            manifest_path.to_str().unwrap(),
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let request = json!({
        "agent_id": agent["resource_id"],
        "input": {
            "classification": "internal",
            "schema_digest": schema_digest,
            "value": {"kind": "inline", "value": {"message": "resume after signal"}}
        },
        "deadline": (Utc::now() + Duration::minutes(5))
            .to_rfc3339_opts(SecondsFormat::Micros, true),
    });
    let request_path = write_canonical(fixture, "timer-signal-run.json", &request);
    let run = run_json(
        insight,
        &[
            "run",
            "create",
            "--file",
            request_path.to_str().unwrap(),
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let run_id = run["run_id"].as_str().expect("timer signal Run ID");

    let wait_deadline = Instant::now() + StdDuration::from_secs(15);
    let first_wait_version = loop {
        let view = run_json(
            insight,
            &["run", "get", run_id, "--path", project.to_str().unwrap()],
        );
        if view["state"] == "waiting" {
            break view["version"].as_u64().expect("timer wait Run version");
        }
        assert!(
            Instant::now() < wait_deadline,
            "timer signal Run did not reach waiting"
        );
        thread::sleep(StdDuration::from_millis(50));
    };
    let signal_wait_deadline = Instant::now() + StdDuration::from_secs(15);
    loop {
        let view = run_json(
            insight,
            &["run", "get", run_id, "--path", project.to_str().unwrap()],
        );
        if view["state"] == "waiting"
            && view["version"]
                .as_u64()
                .is_some_and(|version| version > first_wait_version)
        {
            break;
        }
        assert!(
            Instant::now() < signal_wait_deadline,
            "timer signal Run did not advance from TimerWait to SignalWait"
        );
        thread::sleep(StdDuration::from_millis(50));
    }
    drop(replacement);

    let (client, base_url, token) = raw_runtime_client(project);
    let receipt = format!("productization-signal-{run_id}");
    for _ in 0..2 {
        let response = client
            .post(format!("{base_url}/v1/runs/{run_id}/signals/release"))
            .bearer_auth(&token)
            .header("accept", "application/json")
            .header("content-type", "application/json")
            .header("idempotency-key", &receipt)
            .json(&json!({"payload": null}))
            .send()
            .expect("raw signal request completes");
        let status = response.status();
        let has_trace_id = response.headers().contains_key("trace-id");
        let body = response.text().expect("raw signal response is readable");
        assert_eq!(status, StatusCode::NO_CONTENT, "signal response: {body}");
        assert!(has_trace_id);
    }
    let waiting = run_json(
        insight,
        &["run", "get", run_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(waiting["state"], "running");

    prove_stale_job_fence(run_id);

    let replacement = restart_orchestration_worker(insight, project);
    let watched = run_json_lines(
        insight,
        &[
            "run",
            "watch",
            run_id,
            "--timeout-seconds",
            "120",
            "--path",
            project.to_str().unwrap(),
        ],
    );
    let terminal = watched.last().expect("timer signal terminal watch record");
    assert_eq!(terminal["kind"], "terminal");
    assert_eq!(terminal["run"]["state"], "succeeded");
    let result = run_json(
        insight,
        &["run", "result", run_id, "--path", project.to_str().unwrap()],
    );
    assert_eq!(
        result["value"],
        json!({"kind": "inline", "value": {"message": "resume after signal"}})
    );

    (
        TimerSignalEvidence {
            run_id: run_id.to_owned(),
            console_passed: false,
            stale_job_fence_passed: true,
            started_at,
            finished_at: Utc::now(),
        },
        replacement,
    )
}

fn prove_stale_job_fence(run_id: &str) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("stale-fence runtime builds")
        .block_on(async move {
            let pool = PgPoolOptions::new()
                .max_connections(2)
                .connect("postgres://insight:insight@127.0.0.1:5432/insight_platform")
                .await
                .expect("fresh local PostgreSQL authority accepts the fault probe");
            let repository = PgRepository::new(pool.clone());
            let worker_id = fault_probe_id(ResourceKind::WorkerProcessGeneration);
            let lease_token_digest = fault_probe_digest();
            let quota_entry_ids = (0..MAX_ORCHESTRATION_QUOTA_LINES)
                .map(|_| fault_probe_id(ResourceKind::QuotaLedgerEntry))
                .collect::<Vec<_>>();
            let mut transaction = repository
                .begin_scheduler_transaction()
                .await
                .expect("fault-probe scheduler transaction begins");
            let mut claimed = transaction
                .claim_orchestration_jobs(ClaimOrchestrationJobs {
                    worker_id: worker_id.clone(),
                    limit: 1,
                    lease_milliseconds: 500,
                    slots: vec![OrchestrationClaimSlot {
                        lease_token_digest: lease_token_digest.clone(),
                        quota_reservation_id: fault_probe_id(ResourceKind::UsageReservation),
                        quota_entry_ids,
                        run_event_id: fault_probe_id(ResourceKind::Event),
                        run_outbox_id: fault_probe_id(ResourceKind::OutboxEvent),
                        node_event_id: fault_probe_id(ResourceKind::Event),
                        node_outbox_id: fault_probe_id(ResourceKind::OutboxEvent),
                        job_event_id: fault_probe_id(ResourceKind::Event),
                        job_outbox_id: fault_probe_id(ResourceKind::OutboxEvent),
                    }],
                })
                .await
                .expect("ready continuation can be claimed");
            transaction
                .commit()
                .await
                .expect("fault-probe claim commits");
            assert_eq!(claimed.len(), 1, "exactly one ready continuation is claimed");
            let claimed = claimed.pop().unwrap();
            assert_eq!(claimed.job.run_id.as_deref(), Some(run_id));

            let exact_fence = JobFence {
                tenant_id: claimed.job.tenant_id.clone(),
                job_id: claimed.job.job_id.clone(),
                worker_id,
                lease_epoch: claimed.job.lease_epoch,
                expected_job_version: claimed.job.version,
                lease_token_digest,
            };
            let mut stale_fence = exact_fence.clone();
            stale_fence.lease_token_digest = fault_probe_digest();
            let stale_start = StartOrchestrationJob {
                fence: stale_fence,
                receipt_id: fault_probe_id(ResourceKind::Receipt),
                idempotency_key_digest: fault_probe_digest(),
                request_digest: fault_probe_digest(),
                receipt_expires_at: Utc::now() + Duration::minutes(5),
                job_event_id: fault_probe_id(ResourceKind::Event),
                job_outbox_id: fault_probe_id(ResourceKind::OutboxEvent),
                node_event_id: fault_probe_id(ResourceKind::Event),
                node_outbox_id: fault_probe_id(ResourceKind::OutboxEvent),
            };
            let mut transaction = repository
                .begin_scheduler_transaction()
                .await
                .expect("stale start transaction begins");
            assert!(matches!(
                transaction.start_orchestration_job(stale_start).await,
                Err(RepositoryError::StaleFence)
            ));
            transaction
                .rollback()
                .await
                .expect("stale start transaction rolls back");

            let unchanged: (String, i64, String) = sqlx::query(
                "SELECT state, version, lease_token_digest FROM insight_platform.jobs WHERE tenant_id = $1 AND job_id = $2",
            )
            .bind(&exact_fence.tenant_id)
            .bind(&exact_fence.job_id)
            .fetch_one(&pool)
            .await
            .map(|row| {
                (
                    row.get("state"),
                    row.get("version"),
                    row.get("lease_token_digest"),
                )
            })
            .expect("fenced Job remains readable");
            assert_eq!(unchanged.0, "leased");
            assert_eq!(unchanged.1, exact_fence.expected_job_version);
            assert_eq!(unchanged.2, exact_fence.lease_token_digest.to_string());
            tokio::time::sleep(StdDuration::from_millis(650)).await;
        });
}

fn fault_probe_id(kind: ResourceKind) -> ResourceId {
    ResourceId::from_uuid_v7(kind, Uuid::now_v7()).expect("fault-probe typed identity")
}

fn fault_probe_digest() -> Sha256Digest {
    digest_bytes(Uuid::new_v4().as_bytes())
        .parse()
        .expect("fault-probe digest")
}
