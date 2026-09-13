"""The pre-Ready qualification must reject safely without swallowing known owner failures."""
import importlib.util
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("compose_qualification", ROOT / "tools/qualification/qualify-platform-installation-compose.py")
qualification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qualification)


class PreReadyTrustCommandTests(unittest.TestCase):
    def probe(self, error, output="", code=1):
        commands = []
        with mock.patch.object(qualification, "trust_state_snapshot", return_value="unchanged"), mock.patch.object(qualification, "command", side_effect=lambda args, **_: commands.append(args)):
            qualification.public_trust_rejects_before_ready("fixture", "fixture-image", Path("/input.json"))
        command = commands[0]
        self.assertIn("--read-only", command)
        self.assertEqual(command[command.index("--cap-drop") + 1], "ALL")
        self.assertNotIn("--cap-add", command)
        self.assertTrue(all("readonly" in value for value in command if value.startswith("type=")))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            owner = root / "owner.py"
            owner.write_text(f"import sys\nsys.stdout.write({output!r})\nsys.stderr.write({error!r})\nraise SystemExit({code})\n")
            script = command[-1].replace("/usr/local/bin/platform-installation", f"{shlex.quote(sys.executable)} {shlex.quote(str(owner))}")
            script = script.replace("/tmp/public.json", str(root / "public.json")).replace("/tmp/error", str(root / "error"))
            return subprocess.run(["/bin/sh", "-ec", script], capture_output=True, timeout=5)

    def test_only_empty_exact_incomplete_is_expected(self):
        self.assertEqual(self.probe("installation Incomplete\n").returncode, 0)
        for error, output, code in [("installation Incomplete\n", "secret-canary", 1), ("", "", 0), ("installation Incomplete extra", "", 1)]:
            result = self.probe(error, output, code)
            self.assertNotEqual(result.returncode, 0)
            self.assertNotIn(b"secret-canary", result.stdout + result.stderr)

    def test_known_input_failure_is_visible_but_unknown_text_is_never_forwarded(self):
        result = self.probe("installation InvalidInput\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr, b"installation InvalidInput\n")
        result = self.probe("secret-provider-canary\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr, b"installation ExternalOutcomeUnknown\n")


if __name__ == "__main__":
    unittest.main()
