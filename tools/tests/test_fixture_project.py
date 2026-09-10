from pathlib import Path
import json
import io
import os
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from tools.qualification.fixture_project import cleanup, cleanup_all, export_logs, identity


class FixtureProjectTests(unittest.TestCase):
    def setUp(self):
        self.owner = tempfile.TemporaryDirectory()
        self.addCleanup(self.owner.cleanup)
        self.root = Path(self.owner.name).resolve()
        self.project = self.root / "fixture"
        self.project.mkdir(mode=0o700)
        self.identity = identity(self.project)

    def initialize(self, journal=True):
        state = self.project / ".insight"
        state.mkdir()
        (state / "project.json").write_text(json.dumps({"project_name": "fixture"}))
        (state / "runtime").mkdir()
        (state / "runtime/compose.yaml").write_text("services: {}")
        if journal:
            (state / "runtime/processes.json").write_text("{}")

    def clean(self, **kwargs):
        return cleanup(self.project, self.identity, "insight", "fixture", **kwargs)

    @patch("tools.qualification.fixture_project.subprocess.run")
    def test_success_stops_then_resets_and_removes_only_owned_directory(self, run):
        self.initialize()
        other = self.root / "unrelated"
        other.mkdir()
        (self.project / "outside-link").symlink_to(other, target_is_directory=True)
        self.assertEqual(self.clean(status=0, keep_failed=True), 0)
        self.assertEqual([call.args[0][1:3] for call in run.call_args_list], [["qualification-aws", "stop"], ["qualification-aws", "reset"]])
        self.assertEqual(run.call_args_list[1].args[0][-2:], ["--confirm", "fixture"])
        self.assertFalse(self.project.exists())
        self.assertTrue(other.is_dir())

    @patch("tools.qualification.fixture_project.subprocess.run")
    def test_partial_startup_resets_without_process_journal(self, run):
        self.initialize(journal=False)
        self.assertEqual(self.clean(status=17), 17)
        self.assertEqual(run.call_args.args[0][1:3], ["qualification-aws", "reset"])
        self.assertFalse(self.project.exists())

    def test_failure_is_retained_only_with_explicit_flag(self):
        self.assertEqual(self.clean(status=17, keep_failed=True), 17)
        self.assertTrue(self.project.is_dir())
        self.assertEqual(self.clean(status=17), 17)
        self.assertFalse(self.project.exists())

    @patch("tools.qualification.fixture_project.subprocess.run")
    def test_changed_identity_or_name_cannot_call_destructive_cli(self, run):
        with self.assertRaisesRegex(ValueError, "identity changed"):
            cleanup(self.project, "0:0", "insight", "fixture", 0)
        self.initialize()
        with self.assertRaisesRegex(ValueError, "name changed"):
            cleanup(self.project, self.identity, "insight", "another", 0)
        run.assert_not_called()

    @patch("tools.qualification.fixture_project.subprocess.run")
    def test_failed_teardown_retains_ownership_for_retry(self, run):
        self.initialize()
        run.side_effect = subprocess.CalledProcessError(1, ["insight", "qualification-aws", "stop"])
        with self.assertRaises(subprocess.CalledProcessError):
            self.clean(status=0)
        self.assertTrue(self.project.exists())
        self.assertEqual(run.call_count, 1)

    def test_log_export_is_bounded_and_excludes_secrets_before_cleanup(self):
        logs = self.project / ".insight/runtime/logs"
        logs.mkdir(parents=True)
        (logs / "gateway.log").write_bytes(b"x" * (2 * 1024 * 1024))
        (logs / "config.json").write_text("secret")
        destination = self.root / "diagnostics"
        self.assertEqual(self.clean(status=1, logs=destination), 1)
        self.assertFalse(self.project.exists())
        self.assertEqual([path.name for path in destination.iterdir()], ["gateway.log"])
        self.assertEqual((destination / "gateway.log").stat().st_size, 1024 * 1024)

    def test_symlink_log_is_not_followed_and_cleanup_still_runs(self):
        logs = self.project / ".insight/runtime/logs"
        logs.mkdir(parents=True)
        secret = self.root / "secret"
        secret.write_text("private")
        (logs / "gateway.log").symlink_to(secret)
        with self.assertRaisesRegex(ValueError, "diagnostics failed"):
            self.clean(status=0, logs=self.root / "diagnostics")
        self.assertFalse(self.project.exists())
        self.assertEqual(secret.read_text(), "private")

    def test_symlink_project_and_nested_diagnostics_are_rejected(self):
        alias = self.root / "alias"
        alias.symlink_to(self.project, target_is_directory=True)
        with self.assertRaises(ValueError):
            identity(alias)
        with self.assertRaises(ValueError):
            export_logs(self.project, self.project / "diagnostics")

    @patch("tools.qualification.fixture_project.subprocess.run")
    def test_init_only_cleans_but_installed_missing_compose_is_retained(self, run):
        self.initialize(journal=False)
        (self.project / ".insight/runtime/compose.yaml").unlink()
        profile = self.project / ".insight/runtime/profile.json"
        profile.write_text("{}")
        with self.assertRaisesRegex(ValueError, "installed runtime"):
            self.clean(status=1)
        run.assert_not_called()
        profile.unlink()
        self.assertEqual(self.clean(status=1), 1)
        run.assert_not_called()
        self.assertFalse(self.project.exists())

    @patch("tools.qualification.fixture_project.subprocess.run")
    def test_export_contains_shutdown_logs_and_stop_failure_keeps_project(self, run):
        self.initialize()
        logs = self.project / ".insight/runtime/logs"
        logs.mkdir()
        def stop_then_fail(*args, **kwargs):
            (logs / "gateway.log").write_text("last drain diagnostic")
            raise subprocess.CalledProcessError(1, ["insight", "qualification-aws", "stop"])
        run.side_effect = stop_then_fail
        destination = self.root / "diagnostics"
        with self.assertRaises(subprocess.CalledProcessError):
            self.clean(status=1, logs=destination)
        self.assertEqual((destination / "gateway.log").read_text(), "last drain diagnostic")
        self.assertTrue(self.project.exists())

    def test_nonlogs_do_not_hide_valid_logs_and_hardlinks_are_rejected(self):
        logs = self.project / ".insight/runtime/logs"
        logs.mkdir(parents=True)
        for number in range(65):
            (logs / f"a-{number}.json").write_text("ignored")
        (logs / "z-worker.log").write_text("diagnostic")
        destination = self.root / "diagnostics"
        export_logs(self.project, destination)
        self.assertEqual((destination / "z-worker.log").read_text(), "diagnostic")
        secret = self.root / "secret"
        secret.write_text("private")
        os.link(secret, logs / "another.log")
        with self.assertRaisesRegex(ValueError, "one link"):
            export_logs(self.project, self.root / "hardlink-diagnostics")

    def test_retention_reports_export_failure_without_changing_original_status(self):
        destination = self.root / "diagnostics"
        destination.mkdir()
        with patch("sys.stderr", new_callable=io.StringIO) as error:
            self.assertEqual(self.clean(status=17, keep_failed=True, logs=destination), 17)
        self.assertIn("diagnostics failed", error.getvalue())
        self.assertTrue(self.project.exists())

    @patch("tools.qualification.fixture_project.cleanup")
    def test_cleanup_failure_cannot_skip_an_independent_fixture(self, clean):
        second = self.root / "second"
        second.mkdir()
        clean.side_effect = [ValueError("first teardown failed"), 1]
        with self.assertRaisesRegex(RuntimeError, "1 fixture cleanup failures"):
            cleanup_all([(self.project, self.identity), (second, identity(second))], "insight", "fixture")
        self.assertEqual(clean.call_count, 2)
        self.assertEqual(clean.call_args.args[0], second)


if __name__ == "__main__":
    unittest.main()
