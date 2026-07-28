#!/usr/bin/env python3
"""Synthetic fail-closed tests for the Phase 0 full evaluator."""

from __future__ import annotations

import copy
import csv
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "bench" / "phase0-full" / "report.py"


def snapshot(before: bool) -> Dict[str, Any]:
    wal_base = 100
    return {
        "captured_at": "2026-07-28T00:00:00+00:00",
        "settings": {
            "fsync": "on",
            "full_page_writes": "on",
            "synchronous_commit": "on",
            "track_io_timing": "on",
            "pg_stat_statements_track": "all",
            "pg_stat_statements_track_utility": "on",
            "wal_keep_size_bytes": 8589934592,
        },
        "statement_stats": {
            "stats_reset": "2026-07-28T00:00:00+00:00",
            "dealloc": 0,
            "top_level_wal_bytes": 0 if before else 900000,
            "top_level_calls": 0 if before else 120000,
            "nested_wal_bytes": 0 if before else 500000,
            "nested_calls": 0 if before else 90000,
        },
        "top_wal_statements": [] if before else [
            {
                "queryid": 1,
                "toplevel": True,
                "calls": 6000,
                "rows": 6000,
                "total_exec_ms": 1000,
                "mean_exec_ms": 0.1,
                "shared_blks_hit": 1,
                "shared_blks_read": 0,
                "temp_blks_read": 0,
                "temp_blks_written": 0,
                "wal_records": 10000,
                "wal_fpi": 100,
                "wal_bytes": 800000,
                "query": "INSERT INTO execution_events VALUES (...)",
            }
        ],
        "boundary": {
            "wal_insert_lsn": "0/10000000" if before else "0/100F4240",
            "postmaster_start_time": "2026-07-28T00:00:00+00:00",
            "transaction_timestamp": "2026-07-28T00:00:00+00:00",
            "statement_timestamp": "2026-07-28T00:00:00+00:00",
        },
        "wal": {
            "wal_bytes": wal_base if before else wal_base + 1000000,
            "wal_records": 10 if before else 10010,
            "wal_fpi": 1 if before else 101,
            "wal_buffers_full": 0,
            "stats_reset": "2026-07-27T00:00:00+00:00",
        },
        "bgwriter": {
            "checkpoints_timed": 1,
            "checkpoints_req": 0 if before else 1,
            "checkpoint_write_time_ms": 0 if before else 10,
            "checkpoint_sync_time_ms": 0 if before else 1,
            "buffers_checkpoint": 0 if before else 100,
            "stats_reset": "2026-07-27T00:00:00+00:00",
        },
        "database": {
            "database_bytes": 100000 if before else 300000,
            "xact_commit": 10 if before else 60010,
            "xact_rollback": 0,
            "temp_files": 0,
            "temp_bytes": 0,
            "deadlocks": 0,
            "blks_read": 1 if before else 100,
            "blks_hit": 10 if before else 10000,
            "blk_read_time_ms": 1 if before else 5,
            "blk_write_time_ms": 1 if before else 10,
            "stats_reset": "2026-07-27T00:00:00+00:00",
        },
        "io": {
            "reads": 1 if before else 2,
            "read_time_ms": 1 if before else 2,
            "writes": 1 if before else 100,
            "write_time_ms": 1 if before else 20,
            "writebacks": 0,
            "writeback_time_ms": 0,
            "extends": 0 if before else 10,
            "extend_time_ms": 0 if before else 2,
            "fsyncs": 0 if before else 20,
            "fsync_time_ms": 0 if before else 3,
            "stats_reset": ["2026-07-27T00:00:00+00:00"],
        },
    }


def relation_snapshot(before: bool) -> Dict[str, Any]:
    definitions = [
        ("workflow_runs", "structural", 6000),
        ("payloads", "payload", 12000),
        ("execution_events", "structural", 24000),
        ("projection_checkpoints", "structural", 30000),
        ("scheduler_checkpoints", "structural", 12000),
        ("public_event_outbox", "structural", 18000),
        ("artifacts", "artifact_object_metadata", 1000),
        ("durable_schema_contract", "catalog", 0),
    ]
    tables = []
    rows = {}
    for ordinal, (name, category, row_delta) in enumerate(definitions, 1):
        base = ordinal * 8192
        growth = row_delta * 10 if not before else 0
        tables.append(
            {
                "schema_name": "public",
                "table_name": name,
                "table_oid": ordinal,
                "persistence": "p",
                "category": category,
                "heap_main_bytes": base + growth // 2,
                "table_and_auxiliary_bytes": base + growth * 3 // 4,
                "indexes_bytes": base + growth // 4,
                "total_bytes": base * 2 + growth,
            }
        )
        rows[name] = ordinal * 10 + (row_delta if not before else 0)
    indexes = [
        {
            "table_name": "workflow_runs",
            "index_name": "workflow_runs_pkey",
            "index_oid": 100,
            "category": "structural",
            "bytes": 8192 if before else 40960,
        },
        {
            "table_name": "payloads",
            "index_name": "payloads_pkey",
            "index_oid": 101,
            "category": "payload",
            "bytes": 8192 if before else 24576,
        },
    ]
    return {
        "captured_at": "2026-07-28T00:00:00+00:00",
        "database_name": "insight_agent_platform",
        "tables": tables,
        "indexes": indexes,
        "category_totals": {},
        "row_counts": rows,
    }


def physical() -> Dict[str, Any]:
    categories = [
        ("payload", 1000, 100000, 40000, 10000),
        ("artifact_object_metadata", 100, 10000, 5000, 1000),
        ("structural", 8000, 860000, 450000, 88000),
        ("mixed", 50, 5000, 2500, 500),
        ("unmapped", 50, 5000, 2500, 500),
    ]
    totals = {
        "record_count": sum(row[1] for row in categories),
        "record_length_bytes": sum(row[2] for row in categories),
        "main_data_length_bytes": sum(row[3] for row in categories),
        "fpi_length_bytes": sum(row[4] for row in categories),
    }
    return {
        "extension": "pg_walinspect",
        "extension_version": "1.1",
        "start_lsn": "0/10000000",
        "end_lsn": "0/100F4240",
        "lsn_span_bytes": 1000000,
        "groups": [
            {
                "resource_manager": "Heap",
                "record_type": "INSERT",
                **totals,
            }
        ],
        "categories": [
            {
                "category": name,
                "record_count": count,
                "record_length_bytes": length,
                "main_data_length_bytes": main,
                "fpi_length_bytes": fpi,
            }
            for name, count, length, main, fpi in categories
        ],
        "relation_block_groups": [
            {
                "category": "payload",
                "schema_name": "public",
                "relation_name": "payloads",
                "resource_manager": "Heap",
                "record_type": "INSERT",
                "block_reference_count": 100,
                "block_data_length_bytes": 10000,
                "block_fpi_length_bytes": 1000,
            }
        ],
        "totals": totals,
    }


def k6_summary(
    iterations: int = 6000,
    duration_ms: int = 600000,
) -> Dict[str, Any]:
    def counter(value: int) -> Dict[str, Any]:
        return {"values": {"count": value}}

    return {
        "state": {"testRunDurationMs": duration_ms},
        "metrics": {
            "iterations": counter(iterations),
            "phase0_full_arrivals_scheduled": counter(iterations),
            "phase0_full_arrival_lateness": {
                "values": {"p(95)": 2, "p(99)": 3, "max": 4}
            },
            "phase0_full_run_accepted": counter(iterations),
            "phase0_full_run_terminal_observed": counter(iterations),
            "phase0_full_run_succeeded": counter(iterations),
            "phase0_full_run_lifecycle_duration": {
                "values": {"p(95)": 500, "p(99)": 1000}
            },
        },
    }


def profile() -> Dict[str, Any]:
    return {
        "profile": "qualification",
        "persistence_mode": "full",
        "agent_id": "action_demo",
        "warmup_duration": "1m",
        "warmup_seconds": 60,
        "duration": "10m",
        "expected_seconds": 600,
        "arrival_rate_per_second": 10,
        "expected_arrivals": 6000,
        "preallocated_vus": 20,
        "max_vus": 50,
        "fixture": {
            "request_body": {"text": "phase0 full WAL baseline fixture"},
            "variable_fields": ["x-request-id"],
        },
        "minimum_wal_keep_size_bytes": 8589934592,
    }


class Phase0FullReportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.documents = {
            "before.json": snapshot(True),
            "after.json": snapshot(False),
            "relations-before.json": relation_snapshot(True),
            "relations-after.json": relation_snapshot(False),
            "physical.json": physical(),
            "warmup.json": k6_summary(600, 60000),
            "summary.json": k6_summary(),
            "profile.json": profile(),
            "infrastructure.json": {
                "passed": True,
                "deployment": {"persistence_mode": "full"},
                "storage": {
                    "postgres_pvc_size": "24Gi",
                    "postgres_wal_keep_size": "8GB",
                },
            },
            "database-preflight.json": {
                "passed": True,
                "preexisting_workload_rows": 0,
                "deployment_policy": {"persistence_mode": "full"},
            },
            "statistics-reset.json": {
                "operation": "pg_stat_reset",
                "database_stats_reset_before": None,
                "database_stats_reset_after": "2026-07-27T00:00:00+00:00",
                "passed": True,
            },
            "pod-before.json": {"metadata": {"uid": "runtime-uid"}},
            "pod-after.json": {"metadata": {"uid": "runtime-uid"}},
            "topology-before.json": {
                "desired_replicas": 1,
                "ready_replicas": 1,
                "pods": [
                    {
                        "uid": "runtime-uid",
                        "phase": "Running",
                        "ready": True,
                        "deleting": False,
                        "restart_count": 0,
                    }
                ],
            },
            "topology-after.json": {
                "desired_replicas": 1,
                "ready_replicas": 1,
                "pods": [
                    {
                        "uid": "runtime-uid",
                        "phase": "Running",
                        "ready": True,
                        "deleting": False,
                        "restart_count": 0,
                    }
                ],
            },
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def run_report(
        self,
        mutations: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Tuple[subprocess.CompletedProcess, Dict[str, Any]]:
        documents = copy.deepcopy(self.documents)
        for name, value in (mutations or {}).items():
            documents[name] = value
        for name, value in documents.items():
            (self.root / name).write_text(
                json.dumps(value),
                encoding="utf-8",
            )
        (self.root / "artifact-before.txt").write_text("100\n", encoding="utf-8")
        (self.root / "artifact-after.txt").write_text("110\n", encoding="utf-8")
        with (self.root / "top-wal.csv").open(
            "w",
            newline="",
            encoding="utf-8",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "queryid",
                    "toplevel",
                    "calls",
                    "rows",
                    "total_exec_ms",
                    "mean_exec_ms",
                    "shared_blks_hit",
                    "shared_blks_read",
                    "temp_blks_read",
                    "temp_blks_written",
                    "wal_records",
                    "wal_fpi",
                    "wal_bytes",
                    "query",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "queryid": 1,
                    "toplevel": "true",
                    "calls": 6000,
                    "rows": 6000,
                    "total_exec_ms": 1000,
                    "mean_exec_ms": 0.1,
                    "shared_blks_hit": 1,
                    "shared_blks_read": 0,
                    "temp_blks_read": 0,
                    "temp_blks_written": 0,
                    "wal_records": 10000,
                    "wal_fpi": 100,
                    "wal_bytes": 800000,
                    "query": "INSERT INTO execution_events VALUES (...)",
                }
            )
        output = self.root / "report.json"
        command = [
            "python3",
            str(REPORT),
            "--before",
            str(self.root / "before.json"),
            "--after",
            str(self.root / "after.json"),
            "--relations-before",
            str(self.root / "relations-before.json"),
            "--relations-after",
            str(self.root / "relations-after.json"),
            "--physical-wal",
            str(self.root / "physical.json"),
            "--top-wal",
            str(self.root / "top-wal.csv"),
            "--warmup",
            str(self.root / "warmup.json"),
            "--k6",
            str(self.root / "summary.json"),
            "--profile",
            str(self.root / "profile.json"),
            "--artifact-before",
            str(self.root / "artifact-before.txt"),
            "--artifact-after",
            str(self.root / "artifact-after.txt"),
            "--pod-before",
            str(self.root / "pod-before.json"),
            "--pod-after",
            str(self.root / "pod-after.json"),
            "--topology-before",
            str(self.root / "topology-before.json"),
            "--topology-after",
            str(self.root / "topology-after.json"),
            "--infrastructure-freshness",
            str(self.root / "infrastructure.json"),
            "--database-preflight",
            str(self.root / "database-preflight.json"),
            "--statistics-reset",
            str(self.root / "statistics-reset.json"),
            "--qualification",
            "--output",
            str(output),
        ]
        result = subprocess.run(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        return result, json.loads(output.read_text(encoding="utf-8"))

    def test_valid_qualification_reports_independent_physical_split(self) -> None:
        result, report = self.run_report()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(report["passed"])
        attribution = report["postgresql"]["physical_wal_attribution"]
        self.assertEqual(
            attribution["coverage"]["record_bytes_to_pg_stat_wal"],
            0.98,
        )
        self.assertGreaterEqual(
            attribution["coverage"]["classified_payload_or_structural"],
            0.95,
        )
        self.assertEqual(
            attribution["wal_split"]["payload_object_wal_bytes"],
            110000,
        )
        self.assertEqual(
            report["storage_growth"]["payload_object"]["artifact_store_bytes"],
            10,
        )
        self.assertEqual(
            report["historical_full_2h_reference"]["total_wal_bytes"],
            71033480938,
        )
        self.assertEqual(
            report["postgresql"]["top_level_statement_attribution"][
                "nested_diagnostic_not_added_to_top_level"
            ]["wal_bytes"],
            500000,
        )
        self.assertEqual(report["warmup"]["accepted"], 600)
        self.assertTrue(
            report["warmup"]["closed_before_measured_lsn_boundary"]
        )

    def test_physical_boundary_below_95_percent_fails_closed(self) -> None:
        after = snapshot(False)
        after["wal"]["wal_bytes"] = 1100100
        result, report = self.run_report({"after.json": after})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any("exact-LSN physical record bytes cover" in item for item in report["failures"])
        )

    def test_unresolved_physical_classification_above_five_percent_fails(self) -> None:
        evidence = physical()
        by_name = {row["category"]: row for row in evidence["categories"]}
        by_name["structural"]["record_length_bytes"] -= 100000
        by_name["mixed"]["record_length_bytes"] += 100000
        result, report = self.run_report({"physical.json": evidence})
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(
            any("record attribution covers" in item for item in report["failures"])
        )

    def test_weakened_durability_fails_closed(self) -> None:
        after = snapshot(False)
        after["settings"]["fsync"] = "off"
        result, report = self.run_report({"after.json": after})
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(any("fsync" in item for item in report["failures"]))

    def test_formal_duration_cannot_be_shortened(self) -> None:
        changed = profile()
        changed["duration"] = "9m"
        changed["expected_seconds"] = 540
        changed["expected_arrivals"] = 5400
        result, report = self.run_report({"profile.json": changed})
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(
            any("duration is 540s" in item for item in report["failures"])
        )

    def test_unclosed_warmup_fails_before_baseline_can_qualify(self) -> None:
        warmup = k6_summary(600, 60000)
        warmup["metrics"]["phase0_full_run_terminal_observed"] = {
            "values": {"count": 599}
        }
        warmup["metrics"]["phase0_full_run_succeeded"] = {
            "values": {"count": 599}
        }
        result, report = self.run_report({"warmup.json": warmup})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any("accepted closure is 599/600" in item for item in report["failures"])
        )

    def test_missing_database_statistics_epoch_fails_qualification(self) -> None:
        reset = copy.deepcopy(self.documents["statistics-reset.json"])
        reset["database_stats_reset_after"] = None
        reset["passed"] = False
        result, report = self.run_report({"statistics-reset.json": reset})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any("statistics-reset evidence" in item for item in report["failures"])
        )

    def test_reset_epoch_must_match_measured_before_snapshot(self) -> None:
        reset = copy.deepcopy(self.documents["statistics-reset.json"])
        reset["database_stats_reset_after"] = "2026-07-27T00:00:01+00:00"
        result, report = self.run_report({"statistics-reset.json": reset})
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(report["passed"])
        self.assertTrue(
            any(
                "does not match the measured before" in item
                for item in report["failures"]
            )
        )


if __name__ == "__main__":
    unittest.main()
