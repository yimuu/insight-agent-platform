#!/usr/bin/env python3
"""Static contract checks for the qualification harness wiring."""

from __future__ import annotations

import os
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "bench" / "terminal-only"


def text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def helm_enabled_agents(relative: str) -> tuple[str, ...]:
    """Read the closed `agents.enabled` list from one checked-in values file."""
    lines = text(relative).splitlines()
    agents_index = lines.index("agents:")
    enabled_index = lines.index("  enabled:", agents_index + 1)
    enabled: list[str] = []
    for line in lines[enabled_index + 1 :]:
        if line.startswith("    - "):
            enabled.append(line.removeprefix("    - ").strip())
            continue
        if line.startswith("    #") or not line.strip():
            continue
        break
    return tuple(enabled)


class HarnessStaticTests(unittest.TestCase):
    def test_composite_child_shell_harnesses_are_executable(self) -> None:
        for name in (
            "run-gate-c.sh",
            "run-commit-before-sse.sh",
            "run-context-summary.sh",
            "run-summary-worker-crash.sh",
            "run-stream-scaling.sh",
            "run-privacy-delete.sh",
            "run-aged-query.sh",
            "preflight-fresh-qualification.sh",
            "validate-fresh-qualification.sh",
        ):
            with self.subTest(name=name):
                self.assertTrue(os.access(BENCH / name, os.X_OK))

    def test_gate_b_formal_duration_sampling_topology_and_row_closure(self) -> None:
        runner = text("bench/terminal-only/run-gate-b.sh")
        report = text("bench/terminal-only/report.py")
        workload = text("bench/terminal-only/k6/terminal-runs.js")
        for token in (
            "measured_duration=2h",
            "arrival_rate=10",
            "runtime_sample_interval_seconds=1",
            "capture_gate_b_before_snapshot",
            "capture_runtime_topology",
            "extract_embedded_top_wal_csv",
            "capture_physical_wal_records",
            "extract_physical_wal_csv",
            "GATE_B_PREFLIGHT_EVIDENCE",
            "capture_gate_b_database_preflight",
            "statistics-reset-before-warmup.json",
            "warmup-closure-evidence.json",
            "--physical-wal",
            "--physical-wal-csv",
            "--warmup",
            "--warmup-seconds",
            "--warmup-expected-arrivals",
            "--expected-arrivals",
            "--sample-interval-seconds",
        ):
            self.assertIn(token, runner)
        for token in (
            "expected_arrivals == 72_000",
            "scheduled == expected_arrivals",
            "iterations == expected_arrivals",
            "admission_row_delta == accepted",
            "result_row_delta == observed",
            "validate_runtime_samples",
            "validate_runtime_topology",
            "validate_statement_boundary",
            "validate_embedded_top_wal",
            "validate_maintenance_stats",
            "validate_sql_wal_diagnostics",
            "validate_physical_wal_evidence",
            "validate_gate_b_warmup",
            "validate_gate_b_preflight",
            "Gate B pg_stat_reset epoch",
            "top_level_statement_wal_bytes_delta",
            "nested_statement_wal_bytes_delta",
            "raw_top30_to_total_ratio",
            "raw_top_level_to_total_ratio",
            "top30_top_level_coverage",
            "physical_record_coverage",
        ):
            self.assertIn(token, report)
        for token in (
            'executor: "shared-iterations"',
            "exec.scenario.iterationInTest",
            "exec.scenario.startTime",
            "terminal_run_arrivals_scheduled",
            "terminal_run_arrival_lateness",
            "terminal_run_arrivals_late",
        ):
            self.assertIn(token, workload)
        self.assertNotIn('executor: "constant-arrival-rate"', workload)
        snapshot = text("bench/terminal-only/sql/snapshot.sql")
        for token in (
            "maintenance_stats",
            "pg_stat_user_tables",
            "autovacuum_count",
            "autoanalyze_count",
            "last_autovacuum",
            "last_autoanalyze",
            "AND toplevel IS TRUE",
            "AND toplevel IS FALSE",
            "wal_keep_size_bytes",
        ):
            self.assertIn(token, snapshot)
        self.assertIn("WHERE schemaname = 'public'", snapshot)
        physical = text("bench/terminal-only/sql/physical-wal-records.sql")
        for token in (
            "pg_get_wal_records_info",
            "resource_manager",
            "record_type",
            "record_length",
            "main_data_length",
            "fpi_length",
            "extversion",
        ):
            self.assertIn(token, physical)

    def test_all_qualification_relations_have_durability_checks(self) -> None:
        snapshot = text("bench/terminal-only/sql/snapshot.sql")
        assertion = text("bench/terminal-only/sql/assert-durability.sql")
        report = text("bench/terminal-only/report.py")
        relations = (
            "terminal_run_admissions",
            "terminal_run_results",
            "terminal_content_deletion_jobs",
            "terminal_artifact_staging",
            "conversations",
            "conversation_messages",
            "conversation_summaries",
            "conversation_tombstones",
            "conversation_summary_jobs",
        )
        for relation in relations:
            with self.subTest(relation=relation):
                self.assertIn(relation, snapshot)
                self.assertIn(relation, assertion)
                self.assertIn(relation, report)
        self.assertIn("('on', 'remote_apply')", assertion)
        self.assertIn("pg_stat_statements.track must be all", assertion)
        self.assertIn("pg_stat_statements.track_utility must be on", assertion)

    def test_snapshot_report_statement_schema_is_exact_and_public_only(self) -> None:
        snapshot = text("bench/terminal-only/sql/snapshot.sql")
        report = text("bench/terminal-only/report.py")
        for field in (
            "top_level_wal_bytes",
            "top_level_calls",
            "nested_wal_bytes",
            "nested_calls",
        ):
            self.assertIn(field, snapshot)
            self.assertIn(field, report)
        self.assertNotIn("'tracked_wal_bytes'", snapshot)
        self.assertNotIn('"tracked_wal_bytes"', report)
        self.assertIn("FROM pg_stat_user_tables", snapshot)
        self.assertIn("WHERE schemaname = 'public'", snapshot)

    def test_gate_b_freshness_and_wal_retention_are_fail_closed(self) -> None:
        preflight = text(
            "bench/terminal-only/preflight-fresh-qualification.sh"
        )
        validation = text(
            "bench/terminal-only/validate-fresh-qualification.sh"
        )
        database = text(
            "bench/terminal-only/sql/gate-b-database-preflight.sql"
        )
        values = text("deploy/helm/insight-agent-platform/values.yaml")
        c1 = text(
            "deploy/helm/insight-agent-platform/values-benchmark-c1.yaml"
        )
        statefulset = text(
            "deploy/helm/insight-agent-platform/templates/"
            "postgresql-statefulset.yaml"
        )
        for token in (
            "namespace_absent_before_preflight",
            "matching_helm_releases_before_preflight",
            "matching_pvcs_before_preflight",
            "namespace_uid",
            "qualification-preflight-id",
        ):
            self.assertIn(token, preflight)
        for token in (
            "namespace_uid",
            "status.phase == \"Bound\"",
            "(.items | length) == 2",
            "postgres_pvc_size",
            "artifact_pvc_size",
        ):
            self.assertIn(token, validation)
        for token in (
            "artifact_gc_sweeps",
            "old_ledger_total_rows",
            "deployment_revisions",
            "expected_catalog_count_each",
            "invalid_or_full_count",
            "'terminal_only'",
        ):
            self.assertIn(token, database)
        self.assertIn("walKeepSize: 0", values)
        self.assertIn("walKeepSize: 3GB", c1)
        self.assertIn(
            "wal_keep_size={{ .Values.postgresql.walKeepSize }}",
            statefulset,
        )

    def test_privacy_harness_is_synchronized_encrypted_and_fail_closed(self) -> None:
        privacy = text("bench/terminal-only/run-privacy-delete.sh")
        for token in (
            "privacy_stream_probe.py",
            "encrypted_artifact_probe.py",
            "IAPTEA01",
            "tenant-encryption-report.json",
            "target-ref-needles.txt",
            "messages-after-delete.json",
            "stream-database-after-delete.json",
            "secret_key_material_saved: false",
            "stream-api-service.json",
            "stream-api-port-forward.log",
            "stream-api-port-forward-cleanup.json",
            "stream-api-transport.json",
            "stop_privacy_stream_port_forward",
            "trap stop_privacy_stream_port_forward EXIT",
            "--address 127.0.0.1",
            '"service/$stream_api_service" "0:$stream_api_target_port"',
            '--base-url "$stream_probe_base_url"',
            '"app.kubernetes.io/instance"',
            '"app.kubernetes.io/component"',
            ".cleanup_confirmed == true and .reaped == true",
        ):
            self.assertIn(token, privacy)
        self.assertNotIn("stream_deltas_at_delete", privacy)

    def test_context_summary_separates_raw_envelope_from_authenticated_semantics(
        self,
    ) -> None:
        context = text("bench/terminal-only/run-context-summary.sh")
        for token in (
            "encrypted_artifact_probe.py",
            "latest-summary-object.bin",
            "latest-summary-envelope.json",
            ".framing_complete == true",
            'rm -f "$latest_summary_raw"',
            "jq -e '.summary | select(. != null)'",
            "authenticated_conversation_context_probe",
            "semantic_plaintext_hash_verified_by_artifact_store: true",
            "wait_for_summary_idle_stable",
            "terminal_run_active",
            "summary-settle-before-probe.json",
            "summary-settle-before-missing-object.json",
            "capture_latest_summary_row",
            "missing-object-target.json",
            "missing-object-latest-row-after-delete.json",
            'remove_artifact_object "$fault_summary_ref"',
            "latest_database_row_unchanged_after_delete",
            "object_absent_before_fault_turn: true",
            "summary-settle-after-missing-object.json",
        ):
            self.assertIn(token, context)
        self.assertNotIn("latest_object_hash=", context)
        self.assertNotIn("file_sha256()", context)
        self.assertNotIn("latest-summary-object.json", context)
        self.assertNotIn("TENANT_ARTIFACT_KEYRING_SECRET", context)

    def test_stream_and_durable_content_are_calibrated(self) -> None:
        stream = text("bench/terminal-only/k6/conversation-stream.js")
        runner = text("bench/terminal-only/run-stream-scaling.sh")
        for token in (
            "concatenatedDeltas",
            "terminalResult === concatenatedDeltas",
            "durableOutput === terminalResult",
            "assistant.content === terminalResult",
            "conversation_stream_calibrated",
        ):
            self.assertIn(token, stream)
        self.assertIn("conversation_stream_calibrated", runner)

    def test_gate_d_retries_only_explicit_capacity_rejections(self) -> None:
        workload = text("bench/terminal-only/k6/conversations.js")
        runner = text("bench/terminal-only/run-gate-d.sh")
        for token in (
            "postTurnWithCapacityRetry",
            'response.status !== 429',
            'errorCode(response) !== "RUN_CAPACITY_EXCEEDED"',
            "conversation_turn_attempts",
            "conversation_turn_capacity_rejected",
            "conversation_turn_fresh_acceptance",
            "Retry-After",
            "/^[1-9][0-9]*$/",
            "capacityRetryMaxAttempts",
            "remainingSeconds",
            "turnData.replayed === false",
            "capacityRetryTimeout",
            "conversation_turn_rejected",
        ):
            self.assertIn(token, workload)
        for token in (
            "conversation_turn_attempts.values.count",
            "conversation_turn_capacity_rejected.values.count",
            "http_req_failed.values.passes",
            "all_http_non_successes_were_capacity_rejections",
            "harness_invariant_same_request_and_payload_retry",
            "strict_positive_integer_retry_after_required",
            "statistics-reset-before-gate-d.json",
            "SELECT pg_stat_reset();",
            "database_stats_reset_after",
        ):
            self.assertIn(token, runner)

    def test_failure_harnesses_prove_active_population_and_new_identity(self) -> None:
        gate_c = text("bench/terminal-only/run-gate-c.sh")
        commit_sse = text("bench/terminal-only/run-commit-before-sse.sh")
        summary_crash = text("bench/terminal-only/run-summary-worker-crash.sh")
        gate_c_suite = text("bench/terminal-only/run-gate-c-suite.sh")
        library = text("bench/terminal-only/lib.sh")
        runtime_main = text("src/main.rs")
        for token in (
            "active_before_kill == run_count",
            "killed_pod_uid",
            "replacement_pod_uid",
            "qualification_trigger_container_death",
            "runtime_self_abort",
            "postgres_immediate_shutdown",
            "process-death-evidence.json",
            "pg_postmaster_start_time()",
            "postgres_postmaster_before",
            "postgres_postmaster_after",
        ):
            self.assertIn(token, gate_c)
        for token in (
            "hard-container-before.json",
            "original_container_id",
            "original_restart_count",
            "kill -USR2 1",
            'kill -QUIT "$postmaster_pid"',
            "trigger_signal",
            "terminal_signal",
            "current_terminated_signal",
            "last_terminated_signal",
            "original_terminated_signal",
            "original_terminated_exit_code",
            "original_termination_not_oom",
            "$after.original_terminated_signal == 6",
            "$after.original_terminated_exit_code == 134 or",
            "$after.original_terminated_exit_code == 133",
            "$command[0].kubectl_exec_status == 0",
            "process-incarnation-before.json",
            "process_start_time_ticks",
            "incarnation_match_required",
            "incarnation_token_echoed",
            "container-status-watch.tsv",
            "container-status-watch-attach.json",
            "kubernetes_pod_status_watch",
            "attach_resource_version",
            "attach_line_number",
            "original_terminated_finished_at",
            "watch_identity_death_confirmed",
            "qualification_cleanup_process_death_backgrounds",
            "previous-container.log",
            "runtime_abort_marker_count == 1",
            "postgres_immediate_shutdown_log_evidence",
            ".original_container_terminated and",
            "hard_process_death_confirmed",
            "process-death-evidence.json",
            "qualification_assert_faults_zero",
            "INSIGHT_TERMINAL_QUALIFICATION_ADMISSION_DELAY_MS",
            "INSIGHT_TERMINAL_QUALIFICATION_POST_COMMIT_DELAY_MS",
            "INSIGHT_TERMINAL_QUALIFICATION_SUMMARY_DELAY_MS",
        ):
            self.assertIn(token, library)
        for token in (
            "INSIGHT_QUALIFICATION_ENABLED",
            "QualificationSelfAbortControl",
            "SignalKind::user_defined2()",
            "QUALIFICATION_SELF_ABORT_HANDOFF_DELAY",
            "std::process::abort()",
        ):
            self.assertIn(token, runtime_main)

        for harness in (gate_c, commit_sse, summary_crash):
            trigger_index = harness.index("qualification_trigger_container_death")
            delete_index = harness.index("delete pod", trigger_index)
            self.assertLess(trigger_index, delete_index)
            self.assertIn("process-death-evidence.json", harness)
            self.assertNotIn("--grace-period=0 --force", harness)
            self.assertNotIn("kill -KILL 1", harness)
        for token in (
            "--max-time",
            "killed_pod_uid",
            "replacement_pod_uid",
            "replacement_pod_uid_changed",
            "runtime_self_abort",
        ):
            self.assertIn(token, commit_sse)
        for token in (
            "runtime_self_abort",
            "old_uid",
            "replacement_uid",
            "process_death",
            "context_window_oracle.py",
            "recovery-candidate-page.json",
            "expected-bounded-recovery-tail.json",
        ):
            self.assertIn(token, summary_crash)
        self.assertLess(
            gate_c.index("trap cleanup_gate_c_background EXIT"),
            gate_c.index("k6 run"),
        )
        self.assertIn("stop_gate_c_k6", gate_c)
        self.assertLess(
            commit_sse.index("trap cleanup_commit_stream EXIT"),
            commit_sse.index("curl --silent"),
        )
        self.assertIn("stop_commit_stream", commit_sse)
        for harness in (gate_c_suite, summary_crash):
            self.assertIn("qualification_assert_faults_zero", harness)
            self.assertIn("original_status", harness)
            self.assertIn("reset_status", harness)
            self.assertIn("fault-zero-", harness)
            self.assertNotIn("set_summary_delay 0 cleanup >/dev/null 2>&1 || true", harness)
        self.assertIn("reset_faults final", gate_c_suite)
        self.assertIn("reset_summary_fault clear-summary-delay", summary_crash)

    def test_aged_population_uses_observed_row_count(self) -> None:
        aged = text("bench/terminal-only/run-aged-query.sh")
        self.assertIn("actual_message_count=$(postgres_command", aged)
        self.assertIn("actual_message_count == message_count", aged)
        self.assertIn("--argjson messages \"$actual_message_count\"", aged)

    def test_aged_hot_queries_fail_closed_on_inefficient_plans(self) -> None:
        aged = text("bench/terminal-only/run-aged-query.sh")
        selector = text(
            "bench/terminal-only/sql/select-aged-hot-query-fixture.sql"
        )
        admission = text(
            "bench/terminal-only/sql/explain-aged-admission-lookup.sql"
        )
        derived = text(
            "bench/terminal-only/sql/explain-aged-derived-run-lookup.sql"
        )
        for token in (
            "gate_d_tenant_id",
            "hot_query_fixture_source=gate_d_batch",
            "no completed Gate D admission found",
            "terminal_run_admissions_tenant_request_key",
            "terminal_run_admissions_pkey",
            "terminal_run_results_pkey",
            '"Node Type" == "Seq Scan"',
            '"Relation Name" != "terminal_runtime_instances"',
            "owner_registry_rows <= 1",
            '"Actual Rows" // 0',
            '"Actual Loops" // 0',
            '"Rows Removed by Filter" // 0',
            "AGED_HOT_QUERY_PLAN_MAX_MS",
            "hot_query_plans",
            "no_growing_relation_seq_scan",
            "bounded_owner_registry_policy_passed",
        ):
            self.assertIn(token, aged)
        self.assertNotIn("enable_seqscan", aged)
        self.assertIn("JOIN terminal_run_results", selector)
        self.assertIn("terminal_runtime_instances", selector)
        self.assertNotIn("INSERT INTO terminal_run_admissions", selector)
        for query in (admission, derived):
            self.assertIn("ANALYZE, BUFFERS, SETTINGS, FORMAT JSON", query)
            self.assertNotIn("enable_seqscan", query)
        self.assertIn("tenant_id = :'tenant_id'", admission)
        self.assertIn("request_id = :'request_id'", admission)
        self.assertIn("LEFT JOIN terminal_run_results", derived)
        self.assertIn("LEFT JOIN terminal_runtime_instances", derived)
        self.assertIn("admission.run_id = :'run_id'", derived)
        self.assertIn("admission.tenant_id = :'tenant_id'", derived)

    def test_fresh_namespace_creates_or_validates_secret_before_fixture(self) -> None:
        deployment = text("bench/terminal-only/deploy-stream-fixture.sh")
        overlay = text(
            "deploy/helm/insight-agent-platform/"
            "values-terminal-only-qualification.yaml"
        )
        namespace_index = deployment.index('kubectl create namespace "$namespace"')
        secret_index = deployment.index("create secret generic")
        configmap_index = deployment.index("create configmap")
        self.assertLess(namespace_index, secret_index)
        self.assertLess(secret_index, configmap_index)
        self.assertIn("TENANT_ARTIFACT_KEYRING_SECRET", deployment)
        self.assertIn("TENANT_ARTIFACT_KEY_VERSION", deployment)
        self.assertIn("existingSecret: terminal-tenant-keyring", overlay)
        self.assertIn("activeKeyVersion: qualification-v1", overlay)
        self.assertIn("artifacts:\n  tenantEncryption:", overlay)
        self.assertIn("    size: 2Gi", overlay)
        self.assertIn("postgresql:\n  persistence:\n    enabled: true\n    size: 8Gi", overlay)
        self.assertIn("terminationGracePeriodSeconds: 40", overlay)
        self.assertIn(
            "terminationGracePeriodSeconds: "
            "{{ .Values.runtime.terminationGracePeriodSeconds }}",
            text("deploy/helm/insight-agent-platform/templates/deployment.yaml"),
        )
        self.assertIn(
            "must exceed runtime.shutdownHardDeadline",
            text("deploy/helm/insight-agent-platform/templates/configmap.yaml"),
        )

    def test_qualification_catalog_is_terminal_only_without_disabling_production_gc(
        self,
    ) -> None:
        qualification_values = (
            "deploy/helm/insight-agent-platform/"
            "values-terminal-only-qualification.yaml"
        )
        ordinary_values = "deploy/helm/insight-agent-platform/values.yaml"
        qualification_agents = helm_enabled_agents(qualification_values)

        self.assertEqual(
            qualification_agents,
            (
                "action_demo",
                "conversation_demo",
                "conversation_context_probe",
                "terminal_commit_fixture",
                "terminal_failure_fixture",
                "terminal_llm_failure_fixture",
                "terminal_stream_fixture",
            ),
        )
        self.assertIn("defaultPersistenceMode: terminal_only", text(qualification_values))
        self.assertNotIn("benchmark_wait", qualification_agents)

        # The ordinary full deployment and its durable signal-wait fixture
        # remain unchanged; only the terminal-only qualification overlay
        # excludes the full Agent.
        self.assertIn("benchmark_wait", helm_enabled_agents(ordinary_values))
        benchmark_wait = text("agents/benchmark_wait/agent.yaml")
        self.assertIn("persistence_mode: full", benchmark_wait)

        # Production artifact GC remains configured and the runtime still
        # starts its pump whenever the durable coordinator is active.
        configmap = text(
            "deploy/helm/insight-agent-platform/templates/configmap.yaml"
        )
        run_service = text("crates/runtime/src/run_service.rs")
        self.assertIn("gc_interval: 1m", configmap)
        self.assertIn("tasks.push(spawn_artifact_gc_pump", run_service)


if __name__ == "__main__":
    unittest.main()
