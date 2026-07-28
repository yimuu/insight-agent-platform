#!/usr/bin/env python3
"""Fixture and negative tests for the fail-closed evidence evaluator."""

from __future__ import annotations

import importlib.util
import io
import json
import math
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from typing import Any


REPORT_PATH = Path(__file__).with_name("report.py")
SPEC = importlib.util.spec_from_file_location("terminal_only_report", REPORT_PATH)
assert SPEC is not None and SPEC.loader is not None
REPORT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REPORT)


def snapshot(captured_at: str) -> dict[str, Any]:
    document: dict[str, Any] = {
        "captured_at": captured_at,
        "settings": {
            "fsync": "on",
            "full_page_writes": "on",
            "synchronous_commit": "on",
            "track_io_timing": "on",
            "pg_stat_statements_track": "all",
            "pg_stat_statements_track_utility": "on",
            "wal_keep_size": "3GB",
            "wal_keep_size_bytes": 3 * 1024**3,
        },
        "qualification_relation_persistence": {
            **{
                name: "p"
                for name in REPORT.REQUIRED_LOGGED_QUALIFICATION_RELATIONS
            },
            "terminal_runtime_instances": "u",
        },
        "statement_stats": {
            "stats_reset": "2026-07-27T00:00:00Z",
            "dealloc": 0,
            "top_level_wal_bytes": 0,
            "top_level_calls": 0,
            "nested_wal_bytes": 0,
            "nested_calls": 0,
        },
        "maintenance_stats": {
            "stats_epoch": "2026-07-27T00:00:00Z",
            "tables": [
                {
                    "schema_name": "public",
                    "table_name": "terminal_run_admissions",
                    "relation_id": 16_384,
                    "autovacuum_count": 0,
                    "autoanalyze_count": 0,
                    "last_autovacuum": None,
                    "last_autoanalyze": None,
                }
            ],
        },
        "top_wal_statements": [],
        "boundary": {
            "wal_insert_lsn": "0/1000000",
            "postmaster_start_time": "2026-07-27T00:00:00Z",
            "transaction_timestamp": captured_at,
            "statement_timestamp": captured_at,
        },
        "terminal_rows": {
            "terminal_run_admissions": 10,
            "terminal_run_results": 10,
            "conversation_messages": 20,
        },
        "terminal_relation_bytes": {"admissions": 100, "results": 100},
        "forbidden_durable_rows": {
            name: 0 for name in REPORT.REQUIRED_FORBIDDEN_DURABLE_TABLES
        },
    }
    for section, fields in REPORT.MONOTONIC_SNAPSHOT_FIELDS.items():
        document[section] = {
            **{field: 10 for field in fields},
            "stats_reset": (
                ["2026-07-27T00:00:00Z"]
                if section == "io"
                else "2026-07-27T00:00:00Z"
            ),
        }
    return document


def pod_document(uid: str = "fixture-pod-uid") -> dict[str, Any]:
    return {
        "metadata": {"uid": uid},
        "status": {
            "containerStatuses": [
                {"restartCount": 0, "state": {}, "lastState": {}}
            ]
        },
    }


def topology_document(
    uid: str = "fixture-pod-uid",
    *,
    pod_count: int = 1,
) -> dict[str, Any]:
    return {
        "desired_replicas": 1,
        "ready_replicas": 1,
        "available_replicas": 1,
        "pods": [
            {
                "name": f"runtime-{index}",
                "uid": uid if index == 0 else f"{uid}-{index}",
                "phase": "Running",
                "ready": True,
                "deleting": False,
                "restart_count": 0,
            }
            for index in range(pod_count)
        ],
    }


def write_gate_b_fixture(root: Path) -> SimpleNamespace:
    before = snapshot("2026-07-27T00:00:01Z")
    after = snapshot("2026-07-27T00:00:02Z")
    after["wal"]["wal_bytes"] = 1010
    after["terminal_rows"]["terminal_run_admissions"] = 20
    after["terminal_rows"]["terminal_run_results"] = 20
    after["terminal_relation_bytes"] = {"admissions": 200, "results": 200}
    after["boundary"]["wal_insert_lsn"] = "0/2000000"
    after["statement_stats"]["top_level_wal_bytes"] = 950
    after["statement_stats"]["top_level_calls"] = 10
    after["statement_stats"]["nested_wal_bytes"] = 200
    after["statement_stats"]["nested_calls"] = 10
    after["top_wal_statements"] = [
        {
            "queryid": 1,
            "toplevel": True,
            "calls": 10,
            "rows": 10,
            "total_exec_ms": 10,
            "mean_exec_ms": 1,
            "shared_blks_hit": 0,
            "shared_blks_read": 0,
            "temp_blks_read": 0,
            "temp_blks_written": 0,
            "wal_records": 10,
            "wal_fpi": 0,
            "wal_bytes": 950,
            "query": "insert fixture",
        }
    ]
    summary = {
        "state": {"testRunDurationMs": 1000},
        "metrics": {
            "iterations": {"values": {"count": 10}},
            "terminal_run_arrivals_scheduled": {
                "values": {"count": 10}
            },
            "terminal_run_arrival_lateness": {
                "values": {"p(95)": 2, "p(99)": 3, "max": 4}
            },
            "terminal_run_accepted": {"values": {"count": 10}},
            "terminal_run_terminal_observed": {"values": {"count": 10}},
            "terminal_run_succeeded": {"values": {"count": 10}},
            "terminal_run_lifecycle_duration": {
                "values": {"p(95)": 100, "p(99)": 200}
            },
        },
    }
    runtime_before = "\n".join(
        (
            "terminal_run_admissions_total 10",
            "terminal_run_results_total 10",
            "terminal_run_interrupted_total 0",
            "terminal_run_terminal_commit_retries_total 0",
            "terminal_run_active 0",
        )
    )
    runtime_after = "\n".join(
        (
            "terminal_run_admissions_total 20",
            "terminal_run_results_total 20",
            "terminal_run_interrupted_total 0",
            "terminal_run_terminal_commit_retries_total 0",
            "terminal_run_active 0",
        )
    )
    documents = {
        "before.json": before,
        "after.json": after,
        "warmup.json": summary,
        "summary.json": summary,
        "infrastructure.json": {"passed": True},
        "database-preflight.json": {"passed": True},
        "statistics-reset.json": {
            "operation": "pg_stat_reset",
            "database_stats_reset_before": None,
            "database_stats_reset_after": "2026-07-27T00:00:00Z",
            "passed": True,
        },
        "pod-before.json": pod_document(),
        "pod-after.json": pod_document(),
        "topology-before.json": topology_document(),
        "topology-after.json": topology_document(),
        "physical.json": {
            "extension": "pg_walinspect",
            "extension_version": "1.1",
            "start_lsn": "0/1000000",
            "end_lsn": "0/2000000",
            "groups": [
                {
                    "resource_manager": "Heap",
                    "record_type": "INSERT",
                    "record_count": 10,
                    "record_length_bytes": 970,
                    "main_data_length_bytes": 300,
                    "fpi_length_bytes": 400,
                }
            ],
            "totals": {
                "record_count": 10,
                "record_length_bytes": 970,
                "main_data_length_bytes": 300,
                "fpi_length_bytes": 400,
            },
        },
    }
    for name, document in documents.items():
        (root / name).write_text(json.dumps(document), encoding="utf-8")
    text_files = {
        "runtime-before.prom": runtime_before,
        "runtime-after.prom": runtime_after,
        "runtime-samples.prom": (
            "# sample_epoch_seconds 1\nterminal_run_active 0\n"
        ),
        "process-before.txt": "",
        "process-after.txt": "",
        "artifact-before.txt": "100\n",
        "artifact-after.txt": "200\n",
        "top.csv": (
            "queryid,toplevel,calls,wal_bytes,query\n"
            "1,true,10,950,insert fixture\n"
        ),
        "physical.csv": (
            "resource_manager,record_type,record_count,record_length_bytes,"
            "main_data_length_bytes,fpi_length_bytes\n"
            "Heap,INSERT,10,970,300,400\n"
        ),
    }
    for name, content in text_files.items():
        (root / name).write_text(content, encoding="utf-8")
    return SimpleNamespace(
        before=str(root / "before.json"),
        after=str(root / "after.json"),
        warmup=str(root / "warmup.json"),
        k6=str(root / "summary.json"),
        runtime_before=str(root / "runtime-before.prom"),
        runtime_after=str(root / "runtime-after.prom"),
        runtime_samples=str(root / "runtime-samples.prom"),
        process_before=str(root / "process-before.txt"),
        process_after=str(root / "process-after.txt"),
        pod_before=str(root / "pod-before.json"),
        pod_after=str(root / "pod-after.json"),
        topology_before=str(root / "topology-before.json"),
        topology_after=str(root / "topology-after.json"),
        artifact_before=str(root / "artifact-before.txt"),
        artifact_after=str(root / "artifact-after.txt"),
        top_wal=str(root / "top.csv"),
        physical_wal=str(root / "physical.json"),
        physical_wal_csv=str(root / "physical.csv"),
        infrastructure_freshness=str(root / "infrastructure.json"),
        database_preflight=str(root / "database-preflight.json"),
        statistics_reset=str(root / "statistics-reset.json"),
        output=str(root / "report.json"),
        warmup_seconds=1.0,
        warmup_expected_arrivals=10,
        expected_seconds=1.0,
        expected_arrivals=10,
        sample_interval_seconds=1.0,
        qualification=False,
    )


def evaluate_gate_b_silently(args: SimpleNamespace) -> tuple[int, dict[str, Any]]:
    with redirect_stdout(io.StringIO()):
        status = REPORT.evaluate_gate_b(args)
    return status, json.loads(Path(args.output).read_text(encoding="utf-8"))


class ReportFixtureTests(unittest.TestCase):
    def test_postgres_trimmed_fractional_timestamps_are_valid(self) -> None:
        for digits in range(1, 7):
            with self.subTest(fractional_digits=digits):
                failures: list[str] = []
                timestamp = REPORT.parse_timestamp(
                    "2026-07-27T22:42:46."
                    + ("8" * digits)
                    + "+00:00",
                    "captured_at",
                    failures,
                )
                self.assertIsNotNone(timestamp)
                self.assertEqual(failures, [])

    def test_gate_a_fixture_passes_without_transaction_count_inference(self) -> None:
        before = snapshot("2026-07-27T00:00:01Z")
        after = snapshot("2026-07-27T00:00:02Z")
        before["terminal_rows"].update(
            terminal_run_admissions=10,
            terminal_run_results=9,
            conversation_messages=20,
        )
        after["terminal_rows"].update(
            terminal_run_admissions=11,
            terminal_run_results=10,
            conversation_messages=20,
        )
        statements = {
            "admission_insert_calls": 1,
            "admission_insert_rows": 1,
            "result_insert_calls": 1,
            "result_insert_rows": 1,
            "message_insert_calls": 0,
            "message_insert_rows": 0,
            "terminal_mutation_calls": 0,
            "forbidden_durable_table_count": len(
                REPORT.REQUIRED_FORBIDDEN_DURABLE_TABLES
            ),
            "forbidden_durable_tables": sorted(
                REPORT.REQUIRED_FORBIDDEN_DURABLE_TABLES
            ),
            "forbidden_durable_mutation_calls": 0,
            "forbidden_durable_mutations": [],
            "core_insert_wal_bytes": 100,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, document in (
                ("before.json", before),
                ("after.json", after),
                ("statements.json", statements),
            ):
                (root / name).write_text(json.dumps(document), encoding="utf-8")
            args = SimpleNamespace(
                before=str(root / "before.json"),
                after=str(root / "after.json"),
                statements=str(root / "statements.json"),
                output=str(root / "report.json"),
                conversation=False,
            )

            with redirect_stdout(io.StringIO()):
                self.assertEqual(REPORT.evaluate_gate_a(args), 0)
            result = json.loads((root / "report.json").read_text(encoding="utf-8"))

        self.assertTrue(result["passed"])
        self.assertNotIn(
            "inferred_core_write_transactions",
            result["write_statement_evidence"],
        )
        self.assertIn("repository_contract", result["write_statement_evidence"])

    def test_gate_b_fixture_passes_and_embeds_boundary_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            args = write_gate_b_fixture(Path(directory))
            status, result = evaluate_gate_b_silently(args)

        self.assertEqual(status, 0)
        self.assertTrue(result["passed"])
        self.assertEqual(
            result["postgres"]["sql_wal_diagnostics"][
                "raw_top30_to_total_ratio"
            ],
            0.95,
        )
        self.assertEqual(
            result["postgres"]["physical_wal_attribution"][
                "physical_record_coverage"
            ],
            0.97,
        )
        self.assertEqual(
            result["postgres"]["terminal_admission_row_delta"],
            result["runs"]["accepted"],
        )
        self.assertEqual(
            result["postgres"]["terminal_result_row_delta"],
            result["runs"]["terminal_observed"],
        )
        self.assertEqual(result["runtime"]["identity_before"], "pod:fixture-pod-uid")
        self.assertEqual(
            result["runtime"]["topology"]["unique_pod_uid_before"],
            "fixture-pod-uid",
        )
        self.assertEqual(
            result["postgres"]["measurement_boundary"]["statement_stats_reset"],
            "2026-07-27T00:00:00Z",
        )

    def test_gate_b_rejects_terminal_row_delta_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = write_gate_b_fixture(root)
            after = json.loads((root / "after.json").read_text(encoding="utf-8"))
            after["terminal_rows"]["terminal_run_admissions"] = 19
            after["terminal_rows"]["terminal_run_results"] = 18
            (root / "after.json").write_text(json.dumps(after), encoding="utf-8")
            status, result = evaluate_gate_b_silently(args)

        self.assertEqual(status, 1)
        self.assertTrue(
            any("admissions row delta" in failure for failure in result["failures"])
        )
        self.assertTrue(
            any("results row delta" in failure for failure in result["failures"])
        )

    def test_gate_b_rejects_incomplete_sampling(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = write_gate_b_fixture(root)
            args.expected_seconds = 10.0
            status, result = evaluate_gate_b_silently(args)

        self.assertEqual(status, 1)
        self.assertTrue(
            any("sampling is incomplete" in failure for failure in result["failures"])
        )
        self.assertTrue(
            any("sampling spans" in failure for failure in result["failures"])
        )

    def test_gate_b_rejects_unclosed_warmup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = write_gate_b_fixture(root)
            warmup = json.loads(
                (root / "warmup.json").read_text(encoding="utf-8")
            )
            warmup["metrics"]["terminal_run_terminal_observed"]["values"][
                "count"
            ] = 9
            warmup["metrics"]["terminal_run_succeeded"]["values"]["count"] = 9
            (root / "warmup.json").write_text(
                json.dumps(warmup),
                encoding="utf-8",
            )
            status, result = evaluate_gate_b_silently(args)

        self.assertEqual(status, 1)
        self.assertFalse(result["passed"])
        self.assertTrue(
            any(
                "warm-up accepted closure is 9/10" in failure
                for failure in result["failures"]
            )
        )

    def test_gate_b_reset_epoch_must_match_measured_before_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = write_gate_b_fixture(root)
            reset = json.loads(
                (root / "statistics-reset.json").read_text(encoding="utf-8")
            )
            reset["database_stats_reset_after"] = "2026-07-27T00:00:01Z"
            (root / "statistics-reset.json").write_text(
                json.dumps(reset),
                encoding="utf-8",
            )
            args.qualification = True
            status, result = evaluate_gate_b_silently(args)

        self.assertEqual(status, 1)
        self.assertFalse(result["passed"])
        self.assertTrue(
            any(
                "does not match the measured before" in failure
                for failure in result["failures"]
            )
        )

    def test_qualification_rejects_wrong_arrival_closure_and_long_duration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = write_gate_b_fixture(root)
            summary = json.loads((root / "summary.json").read_text(encoding="utf-8"))
            summary["state"]["testRunDurationMs"] = 7_321_000
            (root / "summary.json").write_text(json.dumps(summary), encoding="utf-8")
            args.expected_seconds = 7200.0
            args.expected_arrivals = 71_999
            args.qualification = True
            status, result = evaluate_gate_b_silently(args)

        self.assertEqual(status, 1)
        self.assertTrue(
            any("no more than 7320s" in failure for failure in result["failures"])
        )
        self.assertTrue(
            any("expected exactly 72000" in failure for failure in result["failures"])
        )
        self.assertTrue(
            any(
                "exact scheduled arrivals" in failure
                for failure in result["failures"]
            )
        )

    def test_snapshot_continuity_and_complete_denylist_pass(self) -> None:
        before = snapshot("2026-07-27T00:00:01Z")
        after = snapshot("2026-07-27T00:00:02Z")
        failures: list[str] = []

        deltas = REPORT.validate_snapshot_pair(before, after, failures)
        rows = REPORT.validate_forbidden_durable_rows(before, after, failures)

        self.assertEqual(failures, [])
        self.assertTrue(
            all(
                value == 0
                for section in deltas.values()
                for value in section.values()
            )
        )
        self.assertEqual(set(rows), REPORT.REQUIRED_FORBIDDEN_DURABLE_TABLES)

    def test_snapshot_rejects_resets_missing_tables_and_weak_durability(self) -> None:
        before = snapshot("2026-07-27T00:00:01Z")
        after = snapshot("2026-07-27T00:00:02Z")
        after["wal"]["stats_reset"] = "2026-07-27T00:00:02Z"
        after["database"]["xact_commit"] = 9
        after["settings"]["synchronous_commit"] = "remote_write"
        after["qualification_relation_persistence"]["conversation_messages"] = "u"
        del after["forbidden_durable_rows"]["payloads"]
        failures: list[str] = []

        REPORT.validate_snapshot_pair(before, after, failures)
        REPORT.validate_forbidden_durable_rows(before, after, failures)

        self.assertTrue(any("wal stats_reset" in item for item in failures))
        self.assertTrue(any("database.xact_commit" in item for item in failures))
        self.assertTrue(any("synchronous_commit" in item for item in failures))
        self.assertTrue(any("conversation_messages" in item for item in failures))
        self.assertTrue(any("table set changed" in item for item in failures))

    def test_statement_boundary_and_embedded_top_wal_fail_closed(self) -> None:
        before = snapshot("2026-07-27T00:00:01Z")
        after = snapshot("2026-07-27T00:00:02Z")
        after["statement_stats"]["stats_reset"] = "2026-07-27T00:00:02Z"
        after["boundary"]["postmaster_start_time"] = "2026-07-27T00:00:01Z"
        failures: list[str] = []
        REPORT.validate_statement_boundary(before, after, failures)
        REPORT.validate_embedded_top_wal(
            {"top_wal_statements": [{"toplevel": True, "wal_bytes": 90}]},
            {"top_statement_rows": 1, "top_statement_wal_bytes": 95},
            failures,
        )

        self.assertTrue(any("stats_reset changed" in item for item in failures))
        self.assertTrue(any("postmaster identity changed" in item for item in failures))
        self.assertTrue(any("CSV bytes differ" in item for item in failures))

    def test_statement_boundary_rejects_eviction_and_tracks_top_level_sql(
        self,
    ) -> None:
        before = snapshot("2026-07-27T00:00:01Z")
        after = snapshot("2026-07-27T00:00:02Z")
        after["boundary"]["wal_insert_lsn"] = "0/1000100"
        after["statement_stats"]["dealloc"] = 1
        after["statement_stats"]["top_level_wal_bytes"] = 98
        failures: list[str] = []

        boundary = REPORT.validate_statement_boundary(before, after, failures)

        self.assertEqual(
            boundary["top_level_statement_wal_bytes_delta"],
            98,
        )
        self.assertTrue(any("deallocated" in item for item in failures))

    def test_equal_garbage_and_reversed_wal_boundaries_fail_closed(self) -> None:
        cases = (
            ("0/1000000", "0/1000000", "strictly greater"),
            ("garbage", "0/1000001", "canonical"),
            ("0/1000002", "0/1000001", "strictly greater"),
        )
        for before_lsn, after_lsn, expected in cases:
            with self.subTest(before=before_lsn, after=after_lsn):
                before = snapshot("2026-07-27T00:00:01Z")
                after = snapshot("2026-07-27T00:00:02Z")
                before["boundary"]["wal_insert_lsn"] = before_lsn
                after["boundary"]["wal_insert_lsn"] = after_lsn
                failures: list[str] = []
                REPORT.validate_statement_boundary(before, after, failures)
                self.assertTrue(any(expected in item for item in failures))

    def test_statement_and_capture_timestamp_order_is_strict(self) -> None:
        before = snapshot("2026-07-27T00:00:01Z")
        after = snapshot("2026-07-27T00:00:02Z")
        after["boundary"]["wal_insert_lsn"] = "0/1000001"
        before["boundary"]["statement_timestamp"] = "2026-07-27T00:00:01.5Z"
        failures: list[str] = []

        REPORT.validate_statement_boundary(before, after, failures)

        self.assertTrue(any("boundary timestamps" in item for item in failures))

    def test_runtime_topology_rejects_multiple_pods_and_uid_change(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "before.json").write_text(
                json.dumps(topology_document(pod_count=2)),
                encoding="utf-8",
            )
            (root / "after.json").write_text(
                json.dumps(topology_document(uid="replacement")),
                encoding="utf-8",
            )
            failures: list[str] = []
            REPORT.validate_runtime_topology(
                str(root / "before.json"),
                str(root / "after.json"),
                failures,
            )

        self.assertTrue(any("Pod set has 2 Pods" in item for item in failures))
        self.assertTrue(any("Pod UID changed" in item for item in failures))

    def test_required_k6_metrics_never_silently_default(self) -> None:
        failures: list[str] = []
        missing = REPORT.k6_metric({}, "required_counter", "count", failures)
        infinite = REPORT.k6_metric(
            {"metrics": {"trend": {"values": {"p(95)": math.inf}}}},
            "trend",
            "p(95)",
            failures,
        )

        self.assertEqual(missing, 0)
        self.assertEqual(infinite, 0)
        self.assertEqual(len(failures), 2)
        self.assertIn("missing", failures[0])
        self.assertIn("not finite", failures[1])

    def test_top_wal_accounting_reports_coverage_and_rejects_bad_values(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "top.csv"
            csv_path.write_text(
                "toplevel,calls,wal_bytes,query\n"
                "true,1,60,insert one\ntrue,1,35,insert two\n",
                encoding="utf-8",
            )
            failures: list[str] = []
            accounting = REPORT.top_wal_accounting(
                str(csv_path),
                100,
                failures,
                top_level_statement_wal_bytes=95,
            )
            self.assertEqual(failures, [])
            self.assertEqual(accounting["raw_top30_to_total_ratio"], 0.95)
            self.assertEqual(accounting["top30_top_level_coverage"], 1.0)
            self.assertEqual(accounting["positive_residual_wal_bytes"], 5)

            failures = []
            accounting = REPORT.top_wal_accounting(
                str(csv_path),
                100,
                failures,
                top_level_statement_wal_bytes=98,
            )
            self.assertEqual(failures, [])
            self.assertEqual(accounting["raw_top30_to_total_ratio"], 0.95)
            self.assertEqual(accounting["raw_top_level_to_total_ratio"], 0.98)
            self.assertEqual(accounting["other_top_level_statement_wal_bytes"], 3)
            self.assertEqual(accounting["positive_residual_wal_bytes"], 2)

            csv_path.write_text(
                "toplevel,calls,wal_bytes,query\ntrue,1,NaN,bad\n",
                encoding="utf-8",
            )
            failures = []
            REPORT.top_wal_accounting(
                str(csv_path),
                100,
                failures,
                top_level_statement_wal_bytes=1,
            )
            self.assertTrue(any("non-finite" in item for item in failures))

    def test_one_byte_sql_plus_one_vacuum_cannot_claim_million_byte_wal(
        self,
    ) -> None:
        before = snapshot("2026-07-27T00:00:01Z")
        after = snapshot("2026-07-27T00:00:02Z")
        after_row = after["maintenance_stats"]["tables"][0]
        after_row["autovacuum_count"] = 1
        after_row["last_autovacuum"] = "2026-07-27T00:00:01.500Z"
        failures: list[str] = []
        evidence = REPORT.validate_maintenance_stats(before, after, failures)
        self.assertEqual(failures, [])
        self.assertTrue(evidence["maintenance_observed"])
        self.assertTrue(evidence["correlation_evidence_valid"])
        self.assertEqual(evidence["autovacuum_delta"], 1)
        self.assertNotIn("wal_bytes", evidence)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            physical = {
                "extension": "pg_walinspect",
                "extension_version": "1.1",
                "start_lsn": "0/1",
                "end_lsn": "0/2",
                "groups": [
                    {
                        "resource_manager": "Heap",
                        "record_type": "INSERT",
                        "record_count": 1,
                        "record_length_bytes": 1,
                        "main_data_length_bytes": 0,
                        "fpi_length_bytes": 0,
                    }
                ],
                "totals": {
                    "record_count": 1,
                    "record_length_bytes": 1,
                    "main_data_length_bytes": 0,
                    "fpi_length_bytes": 0,
                },
            }
            (root / "physical.json").write_text(
                json.dumps(physical),
                encoding="utf-8",
            )
            (root / "physical.csv").write_text(
                "resource_manager,record_type,record_count,"
                "record_length_bytes,main_data_length_bytes,fpi_length_bytes\n"
                "Heap,INSERT,1,1,0,0\n",
                encoding="utf-8",
            )
            failures = []
            REPORT.validate_physical_wal_evidence(
                str(root / "physical.json"),
                str(root / "physical.csv"),
                "0/1",
                "0/2",
                1_000_000,
                failures,
            )
        self.assertTrue(
            any("physical record coverage" in item for item in failures)
        )

    def test_top30_one_all_top_level_hundred_fails_sql_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            csv_path = Path(directory) / "top.csv"
            csv_path.write_text(
                "toplevel,wal_bytes\ntrue,1\n",
                encoding="utf-8",
            )
            failures: list[str] = []
            accounting = REPORT.top_wal_accounting(
                str(csv_path),
                1_000_000,
                failures,
                top_level_statement_wal_bytes=100,
            )
            REPORT.validate_sql_wal_diagnostics(accounting, failures)

        self.assertEqual(accounting["raw_top30_to_total_ratio"], 0.000001)
        self.assertEqual(accounting["raw_top_level_to_total_ratio"], 0.0001)
        self.assertEqual(accounting["top30_top_level_coverage"], 0.01)
        self.assertTrue(
            any("top-30 covers only" in item for item in failures)
        )

    def test_physical_group_totals_and_csv_must_match_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            physical = {
                "extension": "pg_walinspect",
                "extension_version": "1.1",
                "start_lsn": "0/1",
                "end_lsn": "0/2",
                "groups": [
                    {
                        "resource_manager": "Heap",
                        "record_type": "INSERT",
                        "record_count": 1,
                        "record_length_bytes": 100,
                        "main_data_length_bytes": 20,
                        "fpi_length_bytes": 30,
                    }
                ],
                "totals": {
                    "record_count": 1,
                    "record_length_bytes": 99,
                    "main_data_length_bytes": 20,
                    "fpi_length_bytes": 30,
                },
            }
            (root / "physical.json").write_text(
                json.dumps(physical),
                encoding="utf-8",
            )
            (root / "physical.csv").write_text(
                "resource_manager,record_type,record_count,"
                "record_length_bytes,main_data_length_bytes,fpi_length_bytes\n"
                "Heap,INSERT,1,101,20,30\n",
                encoding="utf-8",
            )
            failures: list[str] = []
            REPORT.validate_physical_wal_evidence(
                str(root / "physical.json"),
                str(root / "physical.csv"),
                "0/1",
                "0/2",
                100,
                failures,
            )

        self.assertTrue(any("grouped record_length_bytes" in item for item in failures))
        self.assertTrue(any("exact mechanical projection" in item for item in failures))

    def test_maintenance_epoch_negative_delta_and_stale_timestamp_fail_closed(
        self,
    ) -> None:
        before = snapshot("2026-07-27T00:00:01Z")
        after = snapshot("2026-07-27T00:00:02Z")
        before_row = before["maintenance_stats"]["tables"][0]
        after_row = after["maintenance_stats"]["tables"][0]
        before_row["autovacuum_count"] = 2
        before_row["last_autovacuum"] = "2026-07-27T00:00:00.500Z"
        after_row["autovacuum_count"] = 1
        after_row["last_autovacuum"] = "2026-07-27T00:00:00.500Z"
        after["maintenance_stats"]["stats_epoch"] = "2026-07-27T00:00:01Z"
        failures: list[str] = []

        evidence = REPORT.validate_maintenance_stats(before, after, failures)

        self.assertFalse(evidence["stats_epoch_continuous"])
        self.assertFalse(evidence["counter_deltas_nonnegative"])
        self.assertFalse(evidence["correlation_evidence_valid"])
        self.assertTrue(any("stats epoch changed" in item for item in failures))
        self.assertTrue(any("delta is negative" in item for item in failures))

        # A positive counter delta is also insufficient if the matching
        # maintenance timestamp did not advance.
        after["maintenance_stats"]["stats_epoch"] = before["maintenance_stats"][
            "stats_epoch"
        ]
        after["maintenance_stats"]["tables"][0]["autovacuum_count"] = 3
        failures = []
        evidence = REPORT.validate_maintenance_stats(before, after, failures)
        self.assertFalse(evidence["timestamps_coherent"])
        self.assertFalse(evidence["correlation_evidence_valid"])
        self.assertTrue(any("coherent last_autovacuum" in item for item in failures))

    def test_top_level_sql_over_attribution_still_fails(self) -> None:
        failures: list[str] = []
        REPORT.validate_sql_wal_diagnostics(
            {
                "top_statement_rows": 1,
                "top_level_statement_wal_bytes": 1051,
                "top30_top_level_coverage": 1.0,
                "raw_top_level_to_total_ratio": 1.051,
            },
            failures,
        )

        self.assertTrue(any("top-level SQL WAL exceeds" in item for item in failures))


if __name__ == "__main__":
    unittest.main()
