#!/usr/bin/env python3
"""Static wiring checks for the independent Phase 0 full harness."""

from __future__ import annotations

import os
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PHASE0 = ROOT / "bench" / "phase0-full"


def text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


class Phase0FullStaticTests(unittest.TestCase):
    def test_harness_is_independent_fixed_and_formal(self) -> None:
        runner = text("bench/phase0-full/run-phase0-full.sh")
        workload = text("bench/phase0-full/k6/full-runs.js")
        for token in (
            "warmup_duration=1m",
            "measured_duration=10m",
            "arrival_rate=10",
            "agent_id=action_demo",
            "PHASE0_FULL_PREFLIGHT_EVIDENCE",
            "capture_gate_b_before_snapshot",
            "physical-wal-attribution.sql",
            "statistics-reset-before-warmup.json",
            "warmup-closure-evidence.json",
            '--warmup "$result_dir/warmup-summary.json"',
            "--qualification",
            "phase0-full-report.json",
        ):
            self.assertIn(token, runner)
        self.assertIn(
            'text: "phase0 full WAL baseline fixture"',
            workload,
        )
        self.assertNotIn(
            "qualification ${profile} ${__VU}/${__ITER}",
            workload,
        )
        for token in (
            'executor: "shared-iterations"',
            "exec.scenario.iterationInTest",
            "exec.scenario.startTime",
            "phase0_full_arrivals_scheduled",
            "phase0_full_arrival_lateness",
            "phase0_full_arrivals_late",
        ):
            self.assertIn(token, workload)
        self.assertNotIn('executor: "constant-arrival-rate"', workload)
        for token in (
            "database_stats_reset_after",
            "closed_before_measured_lsn_boundary",
            "excluded_from_measured_lsn_interval",
        ):
            self.assertIn(token, text("bench/phase0-full/report.py"))

    def test_physical_wal_is_exact_and_relation_attributed(self) -> None:
        sql = text("bench/phase0-full/sql/physical-wal-attribution.sql")
        evaluator = text("bench/phase0-full/report.py")
        for token in (
            "pg_get_wal_records_info",
            "pg_get_wal_block_info",
            "pg_filenode_relation",
            "direct_toast_owner",
            "indexed_toast_owner",
            "payload",
            "artifact_object_metadata",
            "structural",
            "mixed",
            "unmapped",
            "pg_wal_lsn_diff",
        ):
            self.assertIn(token, sql)
        for token in (
            "0.95 <= pg_stat_wal_coverage <= 1.05",
            "0.95 <= lsn_coverage <= 1.05",
            "explained_coverage >= 0.95",
            "nested_diagnostic_not_added_to_top_level",
            "71033480938",
            "71801",
        ):
            self.assertIn(token, evaluator)

    def test_overlay_and_validator_reserve_full_interval(self) -> None:
        overlay = text(
            "deploy/archive/helm/insight-agent-platform/"
            "values-phase0-full-baseline.yaml"
        )
        values = text("deploy/archive/helm/insight-agent-platform/values.yaml")
        configmap = text(
            "deploy/archive/helm/insight-agent-platform/templates/configmap.yaml"
        )
        validator = text("bench/phase0-full/validate-fresh-deployment.sh")
        for token in (
            "defaultPersistenceMode: full",
            "enabled: false",
            "maxWalSize: 4GB",
            "walKeepSize: 8GB",
            "size: 24Gi",
        ):
            self.assertIn(token, overlay)
        for token in (
            '.agents.enabled == ["action_demo"]',
            '.runtime.defaultPersistenceMode == "full"',
            '.postgresql.persistence.size == "24Gi"',
            '.postgresql.walKeepSize == "8GB"',
            "namespace_absent_before_preflight",
            "status.phase == \"Bound\"",
        ):
            self.assertIn(token, validator)
        self.assertIn('helm list -n "$namespace" -o json', validator)
        self.assertNotIn('helm list -n "$namespace" --all', validator)
        self.assertIn("defaultBaseUrl: https://models.example.invalid/v1", values)
        for token in (
            "default_placeholder_model:",
            "base_url: {{ .Values.models.defaultBaseUrl | quote }}",
            "placeholder-chat-model",
        ):
            self.assertIn(token, configmap)

    def test_relation_snapshot_reports_tables_indexes_rows_and_split(self) -> None:
        sql = text("bench/phase0-full/sql/relation-snapshot.sql")
        for token in (
            "pg_relation_size",
            "pg_table_size",
            "pg_indexes_size",
            "pg_total_relation_size",
            "row_counts",
            "payload",
            "artifact_object_metadata",
            "structural",
        ):
            self.assertIn(token, sql)

    def test_report_template_state_non_gate_semantics(self) -> None:
        readme = text("bench/phase0-full/README.md")
        template = text("bench/phase0-full/report-template.md")
        for token in (
            "does not apply terminal-only forbidden-ledger or WAL ceilings",
            "1 minute warm-up",
            "exact LSN interval",
            "24Gi",
            "8GB",
        ):
            self.assertIn(token, readme)
        for token in (
            "top-level pgss top-30 WAL",
            "all top-level pgss WAL",
            "payload-relation WAL",
            "structural WAL",
            "95%–105%",
        ):
            self.assertIn(token, template)

    def test_shell_entrypoints_are_executable(self) -> None:
        for name in ("run-phase0-full.sh", "validate-fresh-deployment.sh"):
            with self.subTest(name=name):
                self.assertTrue(os.access(PHASE0 / name, os.X_OK))


if __name__ == "__main__":
    unittest.main()
