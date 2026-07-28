#!/usr/bin/env python3
"""Behavioral tests for the fail-closed process-death evidence helpers."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LIBRARY = ROOT / "bench" / "terminal-only" / "lib.sh"


def capture_watch(rows: list[str], attach_line: int = 1) -> tuple[int, dict]:
    with tempfile.TemporaryDirectory() as temporary_directory:
        temporary = Path(temporary_directory)
        watch_file = temporary / "watch.tsv"
        evidence_file = temporary / "evidence.json"
        watch_file.write_text("\n".join(rows) + "\n", encoding="utf-8")
        completed = subprocess.run(
            [
                "bash",
                "-c",
                (
                    'source "$1"; '
                    "qualification_capture_watched_container_death "
                    '"$2" "$3" pod uid runtime old 0 100 "$4"'
                ),
                "bash",
                str(LIBRARY),
                str(watch_file),
                str(evidence_file),
                str(attach_line),
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        return completed.returncode, json.loads(
            evidence_file.read_text(encoding="utf-8")
        )


class WatchedContainerDeathTests(unittest.TestCase):
    def test_exact_post_attach_restart_is_captured(self) -> None:
        return_code, evidence = capture_watch(
            [
                "100|uid|old|0|||||",
                "101|uid|old|0|||||",
                "102|uid|new|1|old|133||Error|2026-07-28T00:00:00Z",
                "103|uid|newer|2|new|1||Error|2026-07-28T00:00:01Z",
            ]
        )
        self.assertEqual(return_code, 0)
        self.assertTrue(evidence["hard_process_death_confirmed"])
        self.assertEqual(evidence["resource_version"], "102")
        self.assertEqual(evidence["restart_count"], 1)
        self.assertEqual(evidence["original_terminated_exit_code"], 133)
        self.assertEqual(
            evidence["original_terminated_finished_at"],
            "2026-07-28T00:00:00Z",
        )

    def test_pre_attach_injection_and_restart_jump_fail_closed(self) -> None:
        return_code, evidence = capture_watch(
            [
                "99|uid|new|1|old|133||Error|2026-07-28T00:00:00Z",
                "100|uid|old|0|||||",
                "102|uid|newer|2|old|133||Error|2026-07-28T00:00:01Z",
            ],
            attach_line=2,
        )
        self.assertNotEqual(return_code, 0)
        self.assertFalse(evidence["hard_process_death_confirmed"])

    def test_truncated_termination_row_fails_closed(self) -> None:
        return_code, evidence = capture_watch(
            [
                "100|uid|old|0|||||",
                "102|uid|new|1|old|133||Error|",
            ]
        )
        self.assertNotEqual(return_code, 0)
        self.assertFalse(evidence["exact_original_termination_captured"])

    def test_return_trap_reaps_background_processes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            pid_file = Path(temporary_directory) / "pid"
            completed = subprocess.run(
                [
                    "bash",
                    "-c",
                    """
                      source "$1"
                      exercise_cleanup() {
                        local status_watch_pid=
                        local live_log_pid=
                        local saved_return_trap=
                        local saved_int_trap=
                        local saved_term_trap=
                        local trigger_background_cleanup_active=true
                        sleep 30 &
                        status_watch_pid=$!
                        printf '%s\n' "$status_watch_pid" >"$1"
                        trap qualification_cleanup_process_death_backgrounds RETURN
                        return 1
                      }
                      exercise_cleanup "$2" || true
                      background_pid=$(cat "$2")
                      ! kill -0 "$background_pid" 2>/dev/null
                    """,
                    "bash",
                    str(LIBRARY),
                    str(pid_file),
                ],
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
