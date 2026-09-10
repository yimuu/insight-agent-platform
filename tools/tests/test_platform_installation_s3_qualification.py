"""Pure safety checks for the task-only S3 physical qualification harness."""
import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("s3_qualification", ROOT / "tools/qualification/qualify-platform-installation-s3.py")
HARNESS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HARNESS)


class S3QualificationTests(unittest.TestCase):
    def test_environment_has_no_ambient_aws_proxy_or_provider_overrides(self):
        with patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "never-pass", "WEED_S3_CONFIG": "never-pass",
                                     "HTTPS_PROXY": "never-pass", "INSIGHT_S3_FIXTURE_INPUT": "never-pass",
                                     "PATH": "/usr/bin"}, clear=True):
            self.assertEqual(HARNESS.environment(), {"PATH": "/usr/bin"})

    def test_server_closes_default_exposures_and_uses_explicit_tls(self):
        arguments = HARNESS.server_arguments()
        for argument in ["-ip.bind=127.0.0.1", "-s3.ip.bind=0.0.0.0", "-s3.port.https=0",
                         "-s3.iam=false", "-iam=false", "-s3.port.iceberg=0", "-s3.port.lance=0",
                         "-s3.autoCreateBucket=false", "-s3.allowDeleteBucketNotEmpty=false",
                         "-master.telemetry=false", "-s3.allowedOrigins=",
                         "-s3.key.file=/run/insight/s3/server.key", "-s3.cert.file=/run/insight/s3/server.crt"]:
            self.assertIn(argument, arguments)
        self.assertNotIn("-s3.iam=true", arguments)

    def test_cleanup_requires_exact_nonce_and_image(self):
        metadata = {"Config": {"Labels": {HARNESS.LABEL: "own"}, "Image": HARNESS.IMAGE}}
        self.assertTrue(HARNESS.owned(metadata, "own", container=True))
        self.assertFalse(HARNESS.owned(metadata, "foreign", container=True))
        metadata["Config"]["Image"] = "chrislusf/seaweedfs:latest"
        self.assertFalse(HARNESS.owned(metadata, "own", container=True))
        self.assertFalse(HARNESS.owned({}, "own", container=False))

    def test_private_write_never_overwrites_file_or_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "private"
            HARNESS.write_private(path, b"fixture-canary")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                HARNESS.write_private(path, b"changed")
            link = root / "link"
            link.symlink_to(path)
            with self.assertRaises(FileExistsError):
                HARNESS.write_private(link, b"changed")
            self.assertEqual(path.read_bytes(), b"fixture-canary")

    def test_sdk_failure_only_releases_closed_codes(self):
        for diagnostic, expected in [("conditional_concurrency", "sdk_conditional_concurrency"),
                                     ("fixture_secret_canary", "sdk_failed")]:
            with self.subTest(diagnostic=diagnostic), tempfile.TemporaryDirectory() as temporary:
                fixture = HARNESS.Fixture.__new__(HARNESS.Fixture)
                fixture.root = Path(temporary)
                fixture.input = {}
                fixture.endpoint = "https://localhost.localstack.cloud:12345"
                fixture.executable = Path("/explicit/test-only-executable")
                def run(arguments, **kwargs):
                    self.assertEqual(arguments[0], str(fixture.executable))
                    self.assertNotIn("shell", kwargs)
                    self.assertEqual(kwargs["timeout"], 180)
                    self.assertEqual(kwargs["stdin"], subprocess.DEVNULL)
                    input_file = Path(kwargs["env"]["INSIGHT_S3_FIXTURE_INPUT"])
                    self.assertEqual(input_file.stat().st_mode & 0o777, 0o600)
                    kwargs["stdout"].write(('S3 physical qualification failed with safe code: "' + diagnostic + '"\nraw-sensitive-output').encode())
                    kwargs["stdout"].flush()
                    return subprocess.CompletedProcess(arguments, 1)
                with patch.object(HARNESS.subprocess, "run", side_effect=run):
                    with self.assertRaisesRegex(HARNESS.QualificationError, "^" + expected + "$"):
                        fixture.sdk("seed")

    def test_generation_recreation_uses_same_volume_and_new_container(self):
        fixture = HARNESS.Fixture.__new__(HARNESS.Fixture)
        fixture.current = "own-server-1"
        fixture.containers = [fixture.current]
        old = {"Id": "old-id", "State": {"Running": False, "OOMKilled": False, "ExitCode": 0}}
        commands = []
        with patch.object(fixture, "inspect", side_effect=[old, old, {"Id": "new-id"}]), \
                patch.object(fixture, "start") as start, patch.object(fixture, "check"), \
                patch.object(HARNESS, "command", side_effect=lambda arguments, *args: commands.append(arguments)):
            fixture.controlled_recreate()
        self.assertEqual(commands, [["docker", "stop", "--time", "30", "old-id"], ["docker", "rm", "old-id"]])
        start.assert_called_once_with(2)

    def test_forced_kill_is_not_qualified_as_controlled_shutdown(self):
        fixture = HARNESS.Fixture.__new__(HARNESS.Fixture)
        fixture.current = "own-server"
        metadata = {"Id": "own-id", "State": {"Running": False, "OOMKilled": False, "ExitCode": 137}}
        with patch.object(fixture, "inspect", return_value=metadata), patch.object(HARNESS, "command"), \
                patch.object(fixture, "start") as start:
            with self.assertRaisesRegex(HARNESS.QualificationError, "controlled_shutdown_failed"):
                fixture.controlled_recreate()
        start.assert_not_called()

    def test_unexpected_exception_and_incomplete_checks_never_report_pass(self):
        for failure in (TypeError("never emit private exception text"), None):
            with self.subTest(failure=bool(failure)), tempfile.TemporaryDirectory() as temporary:
                report = Path(temporary) / "report.json"
                with patch.object(HARNESS, "Fixture") as constructor, \
                        patch("sys.argv", ["qualification", "--report", str(report)]), \
                        contextlib.redirect_stdout(io.StringIO()):
                    fixture = constructor.return_value
                    fixture.phase = "start_1"
                    fixture.nonce = "own"
                    fixture.checks = []
                    fixture.safe_failure_diagnostics.return_value = {"server": "not_started"}
                    fixture.start.side_effect = failure
                    # An otherwise successful mock still cannot manufacture the required evidence file.
                    self.assertEqual(HARNESS.main(), 1)
                    fixture.close.assert_called_once()
                value = json.loads(report.read_text())
                self.assertFalse(value["passed"])
                self.assertTrue(value["cleanup"])
                self.assertNotIn("private exception", report.read_text())

    def test_sdk_references_publish_private_once_and_never_repair_drift(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = HARNESS.Fixture.__new__(HARNESS.Fixture)
            fixture.root = Path(temporary)
            fixture.root.chmod(0o700)
            fixture.current = "own-server"
            fixture.endpoint = "https://localhost.localstack.cloud:12345"
            fixture.prefix = "own-bucket"
            fixture.input = {"access_key": "a" * 32, "secret_key": "b" * 64}
            fixture.references_published = False
            references = fixture.sdk_references()
            path = Path(references["credentials_file"])
            before = path.read_bytes()
            before_time = path.stat().st_mtime_ns
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            self.assertEqual(set(references), {"endpoint", "bucket", "ca_file", "credentials_file"})
            self.assertNotIn("b" * 64, json.dumps(references))
            self.assertEqual(fixture.sdk_references(), references)
            self.assertEqual(path.stat().st_mtime_ns, before_time)
            path.chmod(0o644)
            with self.assertRaisesRegex(HARNESS.QualificationError, "sdk_references_drift"):
                fixture.sdk_references()
            self.assertEqual(path.stat().st_mode & 0o777, 0o644)
            path.chmod(0o600)
            path.write_bytes(b"changed")
            with self.assertRaisesRegex(HARNESS.QualificationError, "sdk_references_drift"):
                fixture.sdk_references()
            self.assertEqual(path.read_bytes(), b"changed")
            path.unlink()
            with self.assertRaisesRegex(HARNESS.QualificationError, "sdk_references_drift"):
                fixture.sdk_references()
            self.assertFalse(path.exists())
            self.assertIn(b"aws_secret_access_key=", before)

    def test_sdk_references_reject_symlink_and_hardlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            fixture = HARNESS.Fixture.__new__(HARNESS.Fixture)
            fixture.root = Path(temporary)
            fixture.root.chmod(0o700)
            fixture.current = "own-server"
            fixture.endpoint = "https://localhost.localstack.cloud:12345"
            fixture.prefix = "own-bucket"
            fixture.input = {"access_key": "a" * 32, "secret_key": "b" * 64}
            fixture.references_published = False
            references = fixture.sdk_references()
            path = Path(references["credentials_file"])
            alias = fixture.root / "alias"
            os.link(path, alias)
            with self.assertRaisesRegex(HARNESS.QualificationError, "sdk_references_drift"):
                fixture.sdk_references()
            path.unlink()
            path.symlink_to(alias)
            with self.assertRaisesRegex(HARNESS.QualificationError, "sdk_references_drift"):
                fixture.sdk_references()


if __name__ == "__main__":
    unittest.main()
